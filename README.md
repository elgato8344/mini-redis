# mini-redis

**A crash-safe, single-file Redis work-alike in pure Python — standard library only.**

`mini-redis` is a clean, readable implementation of an in-memory, key-value database speaking the Redis Serialization Protocol (RESP2). It pairs an in-memory data store with an append-only **Write-Ahead Log (WAL)** featuring CRC32 checksums, background group commits, crash recovery, atomic log compaction, dual-tier key expiration, LRU eviction, multi-key transactions with optimistic concurrency control (`WATCH`), and pattern-matching Pub/Sub.

The entire server lives in a single Python file (~2,000 lines), accompanied by a standalone terminal REPL client (`mini_cli.py`). It requires **zero third-party dependencies**.

> **Disclaimer**: `mini-redis` is designed for educational exploration, systems architecture study, interview preparation, and local testing. It is **not** a production drop-in replacement for Redis.

---

## Table of Contents

- [What It Is (and What It Is Not)](#what-it-is-and-what-it-is-not)
- [How It Works: Architecture Deep Dive](#how-it-works-architecture-deep-dive)
  - [In-Memory Storage & Versioning](#in-memory-storage--versioning)
  - [The Write-Ahead Log (WAL) Format](#the-write-ahead-log-wal-format)
  - [Group-Commit Writer Pipeline](#group-commit-writer-pipeline)
  - [Durability & Fsync Policies](#durability--fsync-policies)
  - [Crash Recovery & Torn-Write Detection](#crash-recovery--torn-write-detection)
  - [Atomic Compaction](#atomic-compaction)
  - [Transactional Execution & Optimistic Locking](#transactional-execution--optimistic-locking)
  - [Expiration: Passive & Active Sweeps](#expiration-passive--active-sweeps)
  - [LRU Eviction](#lru-eviction)
  - [Pub/Sub Architecture](#pubsub-architecture)
- [Command Reference](#command-reference)
- [Getting Started & Usage Guide](#getting-started--usage-guide)
  - [Running the Server (`mini_redis.py`)](#running-the-server-mini_redispy)
  - [Using the Native REPL (`mini_cli.py`)](#using-the-native-repl-mini_clipy)
  - [Using Standard Tools (`redis-cli`, `nc`, `redis-py`)](#using-standard-tools-redis-cli-nc-redis-py)
- [Project Structure](#project-structure)
- [Educational Takeaways & Design Decisions](#educational-takeaways--design-decisions)

---

## What It Is (and What It Is Not)

### What It Does
* **Full RESP2 Wire Protocol**: Accepts standard multibulk binary arrays and inline plain-text commands over TCP.
* **Core Data Types**: Strings (with integer increments and appends), Lists (double-ended queues), and Hashes (field-value maps).
* **Guaranteed Durability**: Writes are logged to a WAL with configurable `fsync` policies before transactions commit.
* **Group Commit Throughput**: Concurrent writes are coalesced across event loop turns and written via a dedicated I/O writer thread.
* **ACID Transaction Semantics**: Implements `MULTI`, `EXEC`, `DISCARD`, and `WATCH` with optimistic key version tracking and atomic rollback on WAL failure.
* **Non-Blocking Pub/Sub**: Channel and glob pattern (`*`, `?`, `[...]`) fan-out over asynchronous queues.
* **Atomic Compaction**: Rewrites the log into a minimal snapshot using atomic filesystem renames (`os.replace`) and directory syncs.
* **Safety Rails**: Limits on bulk string lengths, command buffers, connection count, and idle socket timeouts.

### What It Does Not Do
* **No Multi-Threading for Queries**: Like canonical Redis, request handling is executed on a single thread event loop. While I/O is offloaded, query throughput is CPU-bound by the Python interpreter.
* **No Complex Data Structures**: No Sets, Sorted Sets (ZSet), Bitmaps, HyperLogLogs, or Streams.
* **No Clustering or Replication**: Single-node only; does not implement Redis Sentinel or Redis Cluster protocols.
* **No Authentication / Encryption**: No `AUTH`, ACLs, or TLS termination. Do not expose this server to untrusted networks.
* **No Database Selection**: Operates exclusively on logical database `0` (no `SELECT`).
* **Key-Count Eviction**: Eviction (`--max-keys`) is strictly based on the number of keys rather than byte-accurate memory allocation (`maxmemory`).

---

## How It Works: Architecture Deep Dive

```text
                             ┌──────────────────────────────────────────┐
                             │              `mini_redis.py`             │
                             │                                          │
[ Client: redis-cli / REPL ] │ ┌────────────────────────────────────┐   │
              │              │ │ asyncio TCP Server Loop            │   │
        RESP2 │              │ └──────────────────┬─────────────────┘   │
              ▼              │                    │                     │
┌──────────────────────────┐ │                    ▼                     │
│      parse_one_resp()    │ │       [ Transaction Dispatch ]           │
│ (Multibulk + Inline Text)│ │        (Snapshot & Spec Check)           │
└─────────────┬────────────┘ │                    │                     │
              │              │        ┌───────────┴───────────┐         │
              ▼              │        ▼                       ▼         │
   ┌────────────────────┐    │   ┌───────────┐           ┌───────────┐  │
   │ Pipeline Execution ├────┼──►│ Memory    │           │ _Txn Log  │  │
   └────────────────────┘    │   │ Mutate &  │           │ Collector │  │
                             │   │ Version++ │           └─────┬─────┘  │
                             │   └───────────┘                 │        │
                             │                                 ▼        │
                             │                     ┌──────────────────┐ │
                             │                     │ _append_batch()  │ │
                             │                     └──────────┬───────┘ │
                             │                                │         │
                             │                  Thread-Safe   ▼         │
                             │                  Queue: ┌──────────────┐ │
                             │                         │ _wal_queue   │ │
                             │                         └──────┬───────┘ │
                             │                                │         │
                             │                                ▼         │
                             │                     ┌──────────────────┐ │
                             │                     │ _wal_writer_loop │ │
                             │                     │ (Dedicated Thread│ │
                             │                     └──────────┬───────┘ │
                             │                                │         │
                             │                `fdatasync()`   ▼         │
                             │                         ┌──────────────┐ │
                             │                         │   wal.log    │ │
                             │                         └──────────────┘ │
                             └──────────────────────────────────────────┘
```
### In-Memory Storage & Versioning
State is maintained inside a `MiniRedisEngine` instance:
* **Keyspace**: A `collections.OrderedDict` mapping str keys to values (str, collections.deque, or dict). Every read/write marks the key as most recently used (move_to_end).
* **Optimistic Versioning**: A monotonically increasing `_global_version` counter advances on every write. A dictionary `_key_versions[key]` records the version of every modified or deleted key.
* **O(1) SCAN Support**: Key references are maintained in both an append-only `_keys_list` and a fast-lookup `_keys_set`. This avoids $O(N)$ linear scans during key insertions while providing stable integer cursors for `SCAN`.

### The Write-Ahead Log (WAL) Format
Every mutating operation generates a structured record written as a single line:
```code
<8-hex-crc32> <json-payload>\n
```
Example:
```code
4f2d80ac ["SET","user:100","alice"]
76d91a2e ["BATCH",[["SET","counter","1"],["PEXPIREAT","counter",1771439283111]]]
3a01cf66 ["LPUSH","tasks","job_1"]
```
* **CRC32 Checksum**: Guarantees bit-level integrity. Valid JSON that was partially overwritten or bit-flipped will be detected and flagged as corruption.
* **Deterministic Absolute Timestamps**: Expirations are logged as absolute epoch milliseconds (`PEXPIREAT`). This ensures that replaying logs during recovery produces the exact same expiration state regardless of restart latency.
* **Atomic BATCH Records**: Multi-statement transactions, multi-key operations (`MSET`, `DEL`), and key-expiry bindings are emitted as a single BATCH log line.

### Group-Commit Writer Pipeline
To avoid blocking the `asyncio` event loop on disk I/O, disk writes are decoupled:
1. `run_transaction` generates encoded log lines and calls `_append_batch()`.
2. `_append_batch()` pushes a work package containing payload bytes, an `asyncio.Future`, and a fsync flag to a synchronized `queue.Queue`.
3. A background daemon thread (`_wal_writer_loop`) continuously drains the queue. If multiple requests are waiting, it coalesces them into a single write buffer.
4. If any batch item demands durability (e.g., `fsync="always"`), `fdatasync()` is issued once for the entire consolidated buffer.
5. Upon successful sync, the futures are resolved via `loop.call_soon_threadsafe()`.

### Durability & Fsync Policies
Configured via the `--fsync` argument:
* **`always`**: Every write batch triggers an immediate `fdatasync()` on the writer thread before returning success to the client. Guarantees zero data loss at the cost of disk I/O latency.
* **`everysec`**: *(default)*: Writes are flushed to the OS page cache immediately; a maintenance task calls `fdatasync()` once every second. At most 1 second of writes can be lost upon power failure.
* **`no`**: Data is flushed to the operating system buffer cache without explicit disk syncs, relying on the OS kernel's writeback schedule.

### Crash Recovery & Torn-Write Detection
During startup, `_recover()` scans `wal.log` sequentially:
1. * **CRC Verification**: Every line is re-checked against its CRC32 checksum.
2. * **Torn-Write Handling**: If the process crashed mid-write, the final line in the log file may be incomplete. If a checksum error occurs on the **final record** of the file, the server truncates the file back to the last valid byte offset and starts normally.
3. * **Mid-Log Corruption Detection**: If a corrupt record is encountered and followed by subsequent data, `MiniRedisEngine` raises `WALCorruptError` and terminates immediately. It will not guess ordering or risk data drift.
4. * **Stale Compaction Cleanup**: If an interrupted compaction left a `wal.log.tmp` artifact behind, it is removed before recovery begins.

### Atomic Compaction
As mutations occur, old keys are overwritten or deleted, creating dead records in the log. Compaction collapses the log back into a minimal snapshot:
1. * **Trigger Condition**: When `wal_ops >= compact_min_ops` and `wal_ops > compact_ratio * live_keys`, the background maintenance loop initiates compaction.
2. * **Snapshot Export**: A complete snapshot of live, non-expired keys is converted into compact commands (`SET`, multi-item `RPUSH`, multi-field `HSET`, and `PEXPIREAT`).
3. * **Flushed Output**: The snapshot is serialized to `wal.log.tmp` with freshly calculated CRCs, followed by an explicit `fdatasync()`.
4. * **Atomic Swap**: The writer thread safely closes the current log handle and performs an atomic os.replace("`wal.log.tmp`", "`wal.log`"). On POSIX platforms, the parent directory is also fsynced via `_fsync_dir()` to ensure directory entry durability.

### Transactional Execution & Optimistic Locking
`mini-redis` implements strict serializable isolation across transactions:
* **Atomic Execution**: When `EXEC` runs, all queued operations are executed within the engine's write lock. If an unexpected runtime error occurs mid-execution, memory state is rolled back using an in-memory deep snapshot.
* **Optimistic Key Watching (`WATCH`)**: When a client watches keys, `WATCH` key caches the current version from `_key_versions`. When `EXEC` is called, all watched key versions are compared. If another transaction or command modified or deleted any watched key, the transaction aborts and returns (`nil`).
* **Pipelined Transactions**: Regular pipelined commands are executed within a single transaction turn, guaranteeing atomic multi-command execution without requiring explicit `MULTI`/`EXEC` wrapping.

### Expiration: Passive & Active Sweeps
Keys can have millisecond-precision time-to-live values:
* **Passive (Lazy) Expiration**: Whenever a key is accessed (`GET`, `HGET`, `EXISTS`, etc.), `_check_expired()` evaluates whether the key's deadline has passed. If expired, the key is excised and a `DEL` record is logged to the WAL before returning nil.
* **Active Expiration**: A min-heap (`_expiries_heap`) tracks `(expire_timestamp, key)` pairs. The maintenance loop executes `sweep_expired()` every second, popping expired keys in O(logN) time and batch-deleting them from the keyspace.

### LRU Eviction
When initialized with `--max-keys <limit>`, `_evict_lru_if_needed()` monitors collection size:
* Because `_data` is an `OrderedDict` with accesses moved to the end, the head of the dictionary always represents the least-recently used key.
* If insertion of a new key would exceed `max_keys`, the oldest key is popped via `popitem(last=False)`.
* Evictions are logged to the WAL as `DEL` commands, ensuring memory reclamation persists across restarts.

### Pub/Sub Architecture
* Connections subscribing via `SUBSCRIBE` or `PSUBSCRIBE` transition into a dedicated Pub/Sub mode.
* Each subscriber connection owns an asynchronous `asyncio.Queue` (bounded at 1,024 messages).
* `PUBLISH` matches exact channel names and evaluates glob expressions via `fnmatch.fnmatchcase`.
* Subscribed clients only accept Pub/Sub management commands (`SUBSCRIBE`, `PSUBSCRIBE`, `UNSUBSCRIBE`, `PUNSUBSCRIBE`, `PING`, `QUIT`).
* Incoming socket data is read concurrently with message queues, preventing network buffer deadlocks.

---

### Command Reference


| Category         | Commands                                                                                                                                                                                           |
|:-----------------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Strings**      | `SET key value [EX s] [PX ms] [NX\|XX] [KEEPTTL]`<br>`GET key`<br>`MSET key value [key value ...]`<br>`GETDEL key`<br>`APPEND key value`<br>`INCR key · DECR key · INCRBY key delta`               |
| **Lists**        | `LPUSH key value [value ...] · RPUSH key value [value ...]`<br>`LPOP key [count] · RPOP key [count]`<br>`LLEN key`<br>`LRANGE key start stop`                                                      |
| **Hashes**       | `HSET key field value [field value ...]`<br>`HGET key field`<br>`HDEL key field [field ...]`<br>`HGETALL key`<br>`HLEN key`                                                                        |
| **Keyspace**     | `DEL key [key ...]`<br>`EXISTS key [key ...]`<br>`DBSIZE`<br>`KEYS pattern`<br>`SCAN cursor [MATCH pattern] [COUNT count]`                                                                         |
| **Expiry**       | `EXPIRE key seconds`<br>`PEXPIRE key milliseconds`<br>`TTL key · PTTL key`<br>`PERSIST key`                                                                                                        |
| **Transactions** | `MULTI`<br>`EXEC`<br>`DISCARD`<br>`WATCH key [key ...]`<br>`UNWATCH`                                                                                                                               |
| **Pub/Sub**      | `SUBSCRIBE channel [channel ...]`<br>`PSUBSCRIBE pattern [pattern ...]`<br>`UNSUBSCRIBE [channel ...]`<br>`PUNSUBSCRIBE [pattern ...]`<br>`PUBLISH channel message`<br>`PUBSUB CHANNELS [pattern]` |
| **Server**       | `PING [message]`<br>`ECHO message<br>INFO [section]`<br>`CONFIG GET maxkeys · CONFIG SET maxkeys value`<br>`COMPACT`<br>`COMMAND (stub)`<br>`QUIT`                                                 |

## Getting Started & Usage Guide

### Running the Server (`mini_redis.py`)

Run the server with default parameters (listening on `127.0.0.1:6379`, WAL saved to `wal.log`, `fsync` set to `everysec`):

```bash
python3 mini_redis.py
```
**Command-Line Flags**
```text
usage: mini_redis.py [-h] [--host HOST] [--port PORT] [--wal WAL]
                     [--fsync {always,everysec,no}] [--max-keys MAX_KEYS]

Mini-Redis server (RESP + WAL)

options:
  -h, --help            show this help message and exit
  --host HOST           bind interface (default: 127.0.0.1)
  --port PORT           listening port (default: 6379)
  --wal WAL             write-ahead log path (default: wal.log)
  --fsync {always,everysec,no}
                        durability policy (default: everysec)
  --max-keys MAX_KEYS   LRU eviction threshold; 0 disables eviction (default: 0)
```
**Examples**

1. **Maximum durability**:
    ```bash
   python3 mini_redis.py --fsync always --wal /var/data/redis.wal
   ```
2. **Fixed memory cache with LRU eviction**:
    ```bash
   python3 mini_redis.py --max-keys 50000 --fsync no
   ```
   
### Using the Native REPL (`mini_cli.py`)

`mini_cli.py` is a terminal client built using standard library `cmd.Cmd` and `readline`. It provides syntax coloring, auto-completion, round-trip latency reporting, sticky `SCAN` cursors, and streaming Pub/Sub.

Launch the CLI:
  ```bash
  python3 mini_cli.py --host 127.0.0.1 --port 6379
  ```
  **Running the Interactive Demo**
  To run an automated test script demonstrating strings, counters, lists, hashes, transactions and scan cursors:
  ```bash
  python3 mini_cli.py --demo
  ```
  **Interactive CLI Features**
  * **Tab Completion**: Commands auto-complete when pressing `<TAB>`.
  * **Quoting Support**: Preserves Windows-style backslashes while properly stripping exterior quotes:
    ```text
    127.0.0.1:6379> SET user:path "C:\Program Files\App"
    "OK"
    (0.24 ms)
    127.0.0.1:6379> GET user:path
    "C:\Program Files\App"
    (0.18 ms)
    ```
  * **Streaming Pub/Sub**:
    ```text
    127.0.0.1:6379> SUBSCRIBE news alerts
    1) "subscribe"
    2) "news"
    3) (integer) 1
    1) "subscribe"
    2) "alerts"
    3) (integer) 2
    subscribed — messages stream below; Ctrl+C to stop reading.
    ```
### Using Standard Tools (`redis-cli`, `nc`, `redis-py`)

Because the server speaks official RESP2, existing Redis tools work without modification.

**Standard `redis-cli`**
  ```bash
redis-cli -p 6379 SET message "Hello from official CLI"
redis-cli -p 6379 GET message
redis-cli -p 6379 HSET user:1 name "Alice" age 30
redis-cli -p 6379 HGETALL user:1
  ```
**Raw Netcat/Telnet (Inline Commands)**
  ```bash
$ nc 127.0.0.1 6379
SET greeting "Hello world"
+OK
GET greeting
$11
Hello world
QUIT
+OK
  ```
**Python (`redis-py`)**
  ```python
import redis

r = redis.Redis(host="127.0.0.1", port=6379, decode_responses=True)

# Basic Strings & Expiration
r.set("session:token", "abc123xyz", ex=300)
print(r.get("session:token"))
print("TTL:", r.ttl("session:token"))

# Lists
r.rpush("queue:jobs", "job_1", "job_2")
print("Popped:", r.lpop("queue:jobs"))

# Transactions with Optimistic Locking
with r.pipeline() as pipe:
    while True:
        try:
            pipe.watch("counter")
            val = int(pipe.get("counter") or 0)
            pipe.multi()
            pipe.set("counter", val + 1)
            pipe.execute()
            break
        except redis.WatchError:
            continue

print("Final Counter:", r.get("counter"))
  ```


---

## Project Structure

```code
.
├── mini_redis.py  # Complete database engine, WAL manager, RESP parser, and TCP server
├── mini_cli.py    # Standalone terminal REPL client with command completion and colorizer
├── README.md      # Architecture documentation and manual
└── wal.log        # Write-ahead log generated at runtime (auto-compacted)
```


---

## Educational Takeaways & Design Decisions

1. **Why JSON for the WAL?**<br>Using plain JSON strings prefixed with CRC32 makes the write-ahead log immediately inspectable with standard tools (`cat`, `less`, `jq`). Binary protocols like RDB are more compact, but JSON makes observing state transitions, checkpoints, and torn-write recoveries intuitive.
2. **Why Not Use Threads for Query Processing?**<br>Concurrency in databases introduces race conditions across shared state. By pinning request execution to a single event loop while offloading synchronous file flushes (`fsync`) to a writer thread, `mini-redis` mirror real-world designs (like Redis and SQLite WAL mode) that maintain a single writer pipeline for predictable ordering.
3. **Fail-Safe Integrity**:<br>Silently ignoring corrupted log lines causes cascading state drift. `mini-redis` distinguishes between torn writes at EOF (safe to truncate) and corrupted records followed by valid data (which halts startup). This highlights the importance of fault domains in storage engine design.


---

## Future Ideas

* **Master/Primary &#8594; Replica WAL streaming**: Stream new WAL records over a dedicated replication socket to follower nodes in real time to enable read-scaling and hot failover.
* **RDB-Style Snapshots (`BGSAVE`)**: Fork-and-save memory state alongside the WAL for faster boot times on large datasets.
* **Sorted Sets (ZSets) via Skip Lists or Bisect**: Implement `ZADD`, `ZRANGE`, and `ZSCORE` using standard library binary search or a pure-Python skip list to support real-time leaderboards and priority queues.
* **Byte-budget compaction trigger**: Trigger compaction based on on-disk file size thresholds (e.g, when `wal.log` exceeds $N$ MB) in addition to operation counts to keep disk footprints strictly bounded.
* **Lightweight Authentication (`AUTH`)**: Add a simple password requirement via a `--requirepass` flag and `AUTH` command to protect instances from unauthorized access on shared networks.
* **Blocking List Operations (`BLPOP` / `BRPOP`)**: Allow clients to block asynchronously with a timeout waiting for keys to be pushed, turning lists into true background job queues without polling.
* **Full Binary-Safe Payload Encoding**: Transition the WAL payload serialization from JSON strings to Base64 or raw length-prefixed bytes to natively support arbitrary binary blobs and images.


---
