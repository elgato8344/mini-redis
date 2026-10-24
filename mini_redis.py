#!/usr/bin/env python3
from __future__ import annotations
import argparse
import asyncio
import collections
import copy
import fnmatch
import heapq
import json
import logging
import os
import queue
import shlex
import threading
import time
import zlib

log = logging.getLogger("mini-redis")

# ---------------------------------------------------------------- config ----
FSYNC_POLICIES = ("always", "everysec", "no")
DEFAULT_COMPACT_MIN_OPS = 10_000
DEFAULT_COMPACT_RATIO = 2.0
MAINTENANCE_INTERVAL = 1.0
MAX_ARGS = 100_000
MAX_BULK = 8 * 1024 * 1024
MAX_COMMAND_SIZE = 64 * 1024 * 1024  # per-command byte cap
MAX_CLIENTS = 1000
CLIENT_TIMEOUT = 300.0


# ------------------------------------------------------------- exceptions ----
class WALCorruptError(Exception):
    """A bad record exists somewhere other than the final WAL line."""


class CompactionInProgress(Exception):
    pass


class ProtocolError(Exception):
    pass


class Incomplete(Exception):
    """A command is only partially buffered."""


# ------------------------------------------------------------ RESP output ----
OK = b"+OK\r\n"
QUEUED = b"+QUEUED\r\n"
NIL = b"$-1\r\n"


def err(message: str) -> bytes:
    message = message.replace("\r", " ").replace("\n", " ")
    return b"-" + message.encode("utf-8", "replace") + b"\r\n"


def integer(n: int) -> bytes:
    return b":%d\r\n" % n


def bulk_bytes(data: bytes) -> bytes:
    return b"$%d\r\n%s\r\n" % (len(data), data)


def bulk(value: str | None) -> bytes:
    if value is None:
        return NIL
    return bulk_bytes(value.encode("utf-8", "surrogateescape"))


def array(items: list) -> bytes:
    return b"*%d\r\n" % len(items) + b"".join(items)


def _s(arg: bytes) -> str:
    return arg.decode("utf-8", "surrogateescape")


def _strip_quotes(tok: str) -> str:
    if len(tok) >= 2 and ((tok[0] == '"' and tok[-1] == '"') or (tok[0] == "'" and tok[-1] == "'")):
        return tok[1:-1]
    return tok


# ----------------------------------------------------------------- engine ----
def _line(*record) -> str:
    payload = json.dumps(record, separators=(",", ":"))
    crc = zlib.crc32(payload.encode("ascii")) & 0xFFFFFFFF
    return f"{crc:08x} {payload}\n"


def _fsync_dir(path: str) -> None:
    if os.name == "nt":
        return
    try:
        fd = os.open(os.path.dirname(os.path.abspath(path)), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def _now_ms() -> int:
    return int(time.time() * 1000)


class _Txn:
    """Accumulates WAL lines + mutated keys for one atomic execution."""
    __slots__ = ("wal", "changed")

    def __init__(self):
        self.wal: list[str] = []
        self.changed: set[str] = set()

    def log(self, *record) -> None:
        self.wal.append(_line(*record))


class MiniRedisEngine:
    """Thread-safe in-memory store backed by a write-ahead log."""

    def __init__(
            self,
            path: str = "wal.log",
            fsync: str = "everysec",
            compact_min_ops: int = DEFAULT_COMPACT_MIN_OPS,
            compact_ratio: float = DEFAULT_COMPACT_RATIO,
            max_keys: int = 0,
    ):
        if fsync not in FSYNC_POLICIES:
            raise ValueError(f"fsync must be one of {FSYNC_POLICIES}")
        self.path = path
        self.fsync_policy = fsync
        self.compact_min_ops = compact_min_ops
        self.compact_ratio = compact_ratio
        self.max_keys = max_keys  # 0 = unlimited (LRU off)

        self._data: collections.OrderedDict = collections.OrderedDict()
        self._expiries: dict[str, int] = {}
        self._expiries_heap: list[tuple[int, str]] = []  # min-heap for O(log N) sweeps
        self._keys_list: list[str] = []  # Append-only list for SCAN cursor
        self._keys_set: set[str] = set()  # O(1) membership check to avoid O(N) list scans
        self._key_versions: dict[str, int] = {}
        self._global_version = 0
        self.evicted_count = 0
        self._type_counts = {"string": 0, "list": 0, "hash": 0}

        # pub/sub
        self._channels: dict[str, set[asyncio.Queue]] = {}
        self._patterns: list[tuple[str, asyncio.Queue]] = []

        self._lock = threading.RLock()
        self._wal_io_lock = threading.RLock()
        self._transaction_lock = asyncio.Lock()

        self._wal = None
        self._wal_size = 0
        self._wal_ops = 0
        self._dirty = False
        self._compacting = False
        self._closed = False
        self._wal_failed = False

        self.start_time = time.time()
        self.last_compact_time = 0.0
        self.connected_clients = 0

        # group-commit writer
        self._wal_queue: queue.Queue = queue.Queue()
        self._writer_thread = threading.Thread(target=self._wal_writer_loop, daemon=True)
        self._writer_thread.start()

        self._recover()
        self._open_wal()

    # ------------------------------------------------------------ recovery ----
    def _update_type_count(self, key: str, old_v: any, new_v: any) -> None:
        if old_v is not None:
            if isinstance(old_v, str):
                self._type_counts["string"] -= 1
            elif isinstance(old_v, collections.deque):
                self._type_counts["list"] -= 1
            elif isinstance(old_v, dict):
                self._type_counts["hash"] -= 1
        if new_v is not None:
            if isinstance(new_v, str):
                self._type_counts["string"] += 1
            elif isinstance(new_v, collections.deque):
                self._type_counts["list"] += 1
            elif isinstance(new_v, dict):
                self._type_counts["hash"] += 1

    def _apply(self, rec) -> None:
        if not isinstance(rec, list) or not rec:
            raise ValueError(f"bad record {rec!r}")
        op = rec[0]

        if op == "BATCH" and len(rec) == 2 and isinstance(rec[1], list):
            # Validate ALL subrecords before applying any to prevent partial application
            for sub in rec[1]:
                if not isinstance(sub, list) or not sub:
                    raise ValueError(f"bad batch record {sub!r}")
            for sub in rec[1]:
                self._apply(sub)
            return

        if op == "SET" and len(rec) == 3 and isinstance(rec[1], str) and isinstance(rec[2], str):
            k, v = rec[1], rec[2]
            old_v = self._data.get(k)
            self._update_type_count(k, old_v, v)
            self._data[k] = v
            if k not in self._keys_set:
                self._keys_set.add(k)
                self._keys_list.append(k)
        elif op == "DEL" and len(rec) == 2 and isinstance(rec[1], str):
            k = rec[1]
            old_v = self._data.pop(k, None)
            self._update_type_count(k, old_v, None)
            self._expiries.pop(k, None)
        elif op == "PEXPIREAT" and len(rec) == 3 and isinstance(rec[1], str) and isinstance(rec[2], int):
            if rec[1] in self._data:
                self._expiries[rec[1]] = rec[2]
                heapq.heappush(self._expiries_heap, (rec[2], rec[1]))
            else:
                self._expiries.pop(rec[1], None)
        elif op == "PERSIST" and len(rec) == 2 and isinstance(rec[1], str):
            self._expiries.pop(rec[1], None)
        elif op == "LPUSH" and len(rec) == 3:
            k, v = rec[1], rec[2]
            dq = self._data.get(k)
            is_new = dq is None
            if is_new:
                dq = collections.deque()
                self._update_type_count(k, None, dq)
                self._data[k] = dq
                if k not in self._keys_set:
                    self._keys_set.add(k)
                    self._keys_list.append(k)
            elif not isinstance(dq, collections.deque):
                raise ValueError("type mismatch in WAL")
            dq.appendleft(v)
        elif op == "RPUSH" and len(rec) == 3:
            k, v = rec[1], rec[2]
            dq = self._data.get(k)
            is_new = dq is None
            if is_new:
                dq = collections.deque()
                self._update_type_count(k, None, dq)
                self._data[k] = dq
                if k not in self._keys_set:
                    self._keys_set.add(k)
                    self._keys_list.append(k)
            elif not isinstance(dq, collections.deque):
                raise ValueError("type mismatch in WAL")
            dq.append(v)
        elif op == "LPOP" and len(rec) == 3 and isinstance(rec[2], int):
            k, count = rec[1], rec[2]
            dq = self._data.get(k)
            if isinstance(dq, collections.deque):
                for _ in range(count):
                    if not dq: break
                    dq.popleft()
                if not dq:
                    old_v = self._data.pop(k, None)
                    self._update_type_count(k, old_v, None)
                    self._expiries.pop(k, None)
        elif op == "RPOP" and len(rec) == 3 and isinstance(rec[2], int):
            k, count = rec[1], rec[2]
            dq = self._data.get(k)
            if isinstance(dq, collections.deque):
                for _ in range(count):
                    if not dq: break
                    dq.pop()
                if not dq:
                    old_v = self._data.pop(k, None)
                    self._update_type_count(k, old_v, None)
                    self._expiries.pop(k, None)
        elif op == "HSET" and len(rec) == 4:
            k, f, v = rec[1], rec[2], rec[3]
            h = self._data.get(k)
            is_new = h is None
            if is_new:
                h = {}
                self._update_type_count(k, None, h)
                self._data[k] = h
                if k not in self._keys_set:
                    self._keys_set.add(k)
                    self._keys_list.append(k)
            elif not isinstance(h, dict):
                raise ValueError("type mismatch in WAL")
            h[f] = v
        elif op == "HDEL" and len(rec) == 3:
            k, f = rec[1], rec[2]
            h = self._data.get(k)
            if isinstance(h, dict):
                h.pop(f, None)
                if not h:
                    old_v = self._data.pop(k, None)
                    self._update_type_count(k, old_v, None)
                    self._expiries.pop(k, None)
        else:
            raise ValueError(f"bad record {rec!r}")

    def _recover(self) -> None:
        tmp = self.path + ".tmp"
        if os.path.exists(tmp):
            os.remove(tmp)
            log.info("Removed stale %s", tmp)
        if not os.path.exists(self.path):
            return

        offset = 0
        bad_line = None
        ops = 0
        with open(self.path, "rb") as f:
            for lineno, raw in enumerate(f, 1):
                stripped = raw.strip()
                if bad_line is not None:
                    if stripped:
                        raise WALCorruptError(
                            f"{self.path}: corrupt record at line {bad_line} is followed by more data")
                    continue
                if stripped:
                    try:
                        space_idx = raw.find(b" ")
                        if space_idx != 8:
                            raise ValueError("bad crc format")
                        expected_crc = int(raw[:8], 16)
                        payload = raw[9:].strip()
                        if expected_crc != (zlib.crc32(payload) & 0xFFFFFFFF):
                            raise ValueError("crc mismatch")
                        record = json.loads(payload)

                        # Snapshot for atomic rollback on failure
                        before = (
                            copy.deepcopy(self._data), dict(self._expiries),
                            list(self._expiries_heap), list(self._keys_list),
                            set(self._keys_set), dict(self._key_versions),
                            self._global_version, dict(self._type_counts), self.evicted_count,
                        )
                        try:
                            self._apply(record)
                        except Exception:
                            (self._data, self._expiries, self._expiries_heap,
                             self._keys_list, self._keys_set, self._key_versions,
                             self._global_version, self._type_counts, self.evicted_count) = before
                            raise
                    except Exception:
                        bad_line = lineno
                        continue
                ops += 1
                offset += len(raw)

        if bad_line is not None:
            log.warning("Dropping bad last WAL record (line %d)", bad_line)
            with open(self.path, "r+b") as f:
                f.truncate(offset)

        self._wal_ops = ops
        self._keys_list = list(self._data.keys())
        self._keys_set = set(self._data.keys())
        for k in self._data:
            self._global_version += 1
            self._key_versions[k] = self._global_version

        for k, exp in self._expiries.items():
            heapq.heappush(self._expiries_heap, (exp, k))

        log.info("Recovered %d keys from %d WAL records", len(self._data), ops)

    def _open_wal(self) -> None:
        size = os.path.getsize(self.path) if os.path.exists(self.path) else 0
        if size:
            with open(self.path, "rb") as f:
                f.seek(-1, os.SEEK_END)
                last = f.read(1)
                if last != b"\n":
                    with open(self.path, "ab") as f:
                        f.write(b"\n")
                    size += 1
        self._wal = open(self.path, "ab")
        self._wal_size = size

    # ------------------------------------------------------ group commit ----
    @staticmethod
    def _complete_future(loop, future, error=None):
        if future.cancelled() or future.done():
            return
        if error is None:
            loop.call_soon_threadsafe(future.set_result, True)
        else:
            loop.call_soon_threadsafe(future.set_exception, error)

    def _wal_writer_loop(self):
        stashed_item = None
        while True:
            if stashed_item is not None:
                item = stashed_item
                stashed_item = None
            else:
                try:
                    item = self._wal_queue.get(timeout=1.0)
                except queue.Empty:
                    if self._closed:
                        return
                    continue

            if item.get("stop"):
                self._wal_queue.task_done()
                return

            if item.get("swap"):
                tmp_path, final_path = item["swap"]
                try:
                    with self._wal_io_lock:
                        if self._wal is not None:
                            self._wal.flush()
                            self._wal.close()
                            self._wal = None
                        os.replace(tmp_path, final_path)
                        _fsync_dir(final_path)
                        self._open_wal()
                        self._dirty = False
                        self._wal_ops = item.get("wal_ops", 0)
                        self.last_compact_time = time.time()
                except Exception as e:
                    if "callback" in item:
                        item["callback"](e)
                else:
                    if "callback" in item:
                        item["callback"](None)
                self._wal_queue.task_done()
                continue

            batch = [item]
            while True:
                try:
                    next_item = self._wal_queue.get_nowait()
                except queue.Empty:
                    break
                if next_item.get("stop") or next_item.get("swap"):
                    stashed_item = next_item
                    break
                batch.append(next_item)

            data = b"".join(it["data"] for it in batch)
            needs_fsync = any(it["fsync"] for it in batch)
            ops_count = sum(it["ops"] for it in batch)

            try:
                with self._wal_io_lock:
                    if self._wal is None:
                        self._open_wal()
                    self._wal.write(data)
                    self._wal.flush()
                    if needs_fsync:
                        if hasattr(os, "fdatasync"):
                            os.fdatasync(self._wal.fileno())
                        else:
                            os.fsync(self._wal.fileno())

                    self._wal_size += len(data)
                    self._wal_ops += ops_count
                    self._dirty = not needs_fsync

                for it in batch:
                    fut = it.get("future")
                    loop = it.get("loop")
                    if fut is not None and loop is not None:
                        self._complete_future(loop, fut)
            except Exception as e:
                log.exception("WAL write failed")
                with self._wal_io_lock:
                    self._discard_partial_write()
                for it in batch:
                    fut = it.get("future")
                    loop = it.get("loop")
                    if fut is not None and loop is not None:
                        self._complete_future(loop, fut, e)
            finally:
                for it in batch:
                    self._wal_queue.task_done()

    def _append_batch(self, lines: list[str]) -> asyncio.Future | None:
        if not lines:
            return None
        if self._closed or self._wal_failed:
            raise OSError("engine is closed or WAL has failed")

        data = "".join(lines).encode("ascii")
        try:
            loop = asyncio.get_running_loop()
            future = loop.create_future()
        except RuntimeError:
            loop = None
            future = None

        self._wal_queue.put({
            "data": data,
            "fsync": self.fsync_policy == "always",
            "ops": len(lines),
            "future": future,
            "loop": loop,
        })
        return future

    def _discard_partial_write(self) -> None:
        try:
            if self._wal is not None:
                self._wal.close()
        except OSError:
            pass
        self._wal = None
        try:
            os.truncate(self.path, self._wal_size)
        except OSError:
            pass

    def _restore_snapshot(self, snapshot) -> None:
        (self._data, self._expiries, self._expiries_heap, self._keys_list,
         self._keys_set, self._key_versions, self._global_version,
         self._type_counts, self.evicted_count) = snapshot

    # ----------------------------------------------- transactional core ----
    async def run_transaction(self, cmds: list[list[bytes]], watched: dict[str, int] | None = None):
        async with self._transaction_lock:
            with self._lock:
                if self._wal_failed:
                    return [err("ERR WAL is in a failed state")], False

                if watched:
                    for k, v in watched.items():
                        if self._key_versions.get(k, 0) != v:
                            return None, False

                snapshot = (
                    copy.deepcopy(self._data), dict(self._expiries),
                    list(self._expiries_heap), list(self._keys_list),
                    set(self._keys_set), dict(self._key_versions),
                    self._global_version, dict(self._type_counts), self.evicted_count,
                )

                txn = _Txn()
                replies = []
                try:
                    for cmd in cmds:
                        replies.append(self._dispatch(cmd, txn))
                except Exception:
                    log.exception("command failed mid-transaction")
                    self._restore_snapshot(snapshot)
                    return [err("ERR internal error")], False

                for k in txn.changed:
                    self._global_version += 1
                    self._key_versions[k] = self._global_version

                if len(txn.wal) > 1:
                    batch_records = []
                    for line in txn.wal:
                        space_idx = line.find(" ")
                        payload = line[space_idx + 1:].strip()
                        batch_records.append(json.loads(payload))
                    lines_to_write = [_line("BATCH", batch_records)]
                else:
                    lines_to_write = txn.wal

                try:
                    fut = self._append_batch(lines_to_write)
                except Exception as e:
                    self._restore_snapshot(snapshot)
                    return [err(f"ERR WAL write failed: {e}")], False

                # Await OUTSIDE the lock to allow concurrency, but STILL rollback if it fails
                if fut is not None:
                    try:
                        await fut
                    except Exception as e:
                        with self._lock:
                            self._wal_failed = True
                            self._restore_snapshot(snapshot)
                        return [err(f"ERR WAL write failed: {e}")], False

                return replies, True

    def _dispatch(self, args: list[bytes], txn: _Txn) -> bytes:
        if not args:
            return b""
        try:
            name = args[0].decode("utf-8", "replace").upper()
        except Exception:
            return err("ERR bad command name")

        spec = COMMANDS.get(name)
        if spec is None:
            return err(f"ERR unknown command '{name[:64]}'")

        low, high, handler = spec
        n = len(args) - 1
        if n < low or (high is not None and n > high):
            return err(f"ERR wrong number of arguments for '{name.lower()}' command")

        try:
            return handler(self, args[1:], txn)
        except ValueError as e:
            return err(f"ERR {e}")
        except OSError as e:
            log.error("%s failed: %s", name, e)
            return err(f"ERR {name.lower()} failed: {e.strerror or e}")
        except Exception:
            log.exception("Internal error running %s", name)
            return err("ERR internal error")

    # ------------------------------------------------------ keyspace core ----
    def _bump(self, txn: _Txn, key: str) -> None:
        txn.changed.add(key)

    def _evict_lru_if_needed(self, txn: _Txn) -> None:
        if self.max_keys <= 0:
            return
        while len(self._data) >= self.max_keys:
            evicted_key, old_v = self._data.popitem(last=False)
            self._update_type_count(evicted_key, old_v, None)
            self._expiries.pop(evicted_key, None)
            self._bump(txn, evicted_key)
            txn.log("DEL", evicted_key)
            self.evicted_count += 1
            log.info("LRU evicted '%s' (max-keys=%d)", evicted_key, self.max_keys)

    def _check_expired(self, key: str, txn: _Txn) -> bool:
        exp = self._expiries.get(key)
        if exp is not None and exp <= _now_ms():
            old_v = self._data.pop(key, None)
            self._update_type_count(key, old_v, None)
            self._expiries.pop(key, None)
            self._bump(txn, key)
            txn.log("DEL", key)
            return True
        return False

    def delete_keys(self, keys, txn: _Txn | None = None) -> int:
        own = txn is None
        if own:
            txn = _Txn()

        with self._lock if own else _nullcontext():
            present = []
            for k in dict.fromkeys(keys):
                if k in self._data and not self._check_expired(k, txn):
                    present.append(k)

            if present:
                txn.log("BATCH", [["DEL", k] for k in present])
                for k in present:
                    old_v = self._data.pop(k, None)
                    self._update_type_count(k, old_v, None)
                    self._expiries.pop(k, None)
                    self._bump(txn, k)

            if own:
                for changed_key in txn.changed:
                    self._global_version += 1
                    self._key_versions[changed_key] = self._global_version
                self._append_batch(txn.wal)
            return len(present)

    # --------------------------------------------------------------- pub/sub ----
    def publish(self, channel: str, message: str) -> int:
        with self._lock:
            subs = list(self._channels.get(channel, set()))
            patterns = [(p, q) for p, q in self._patterns if fnmatch.fnmatchcase(channel, p)]
            count = 0
            for q in subs:
                try:
                    q.put_nowait(("message", channel, message))
                    count += 1
                except asyncio.QueueFull:
                    pass
            for p, q in patterns:
                try:
                    q.put_nowait(("pmessage", p, channel, message))
                    count += 1
                except asyncio.QueueFull:
                    pass
            return count

    def channel_names(self, pattern: str = "*") -> list[str]:
        with self._lock:
            return [c for c in self._channels if fnmatch.fnmatchcase(c, pattern)]

    # ------------------------------------------------------------- commands ----
    def c_ping(self, a, txn):
        return bulk_bytes(a[0]) if a else b"+PONG\r\n"

    def c_echo(self, a, txn):
        return bulk_bytes(a[0])

    def c_set(self, a, txn):
        key, val = _s(a[0]), _s(a[1])
        ex_ms = None
        nx = xx = keepttl = False
        i = 2
        while i < len(a):
            opt = _s(a[i]).upper()
            if opt == "EX" and i + 1 < len(a):
                try:
                    sec = int(_s(a[i + 1]))
                    if sec <= 0:
                        return err("ERR invalid expire time in 'set' command")
                    ex_ms = _now_ms() + sec * 1000
                except ValueError:
                    return err("ERR value is not an integer or out of range")
                i += 2
            elif opt == "PX" and i + 1 < len(a):
                try:
                    ms = int(_s(a[i + 1]))
                    if ms <= 0:
                        return err("ERR invalid expire time in 'set' command")
                    ex_ms = _now_ms() + ms
                except ValueError:
                    return err("ERR value is not an integer or out of range")
                i += 2
            elif opt == "NX":
                nx = True
                i += 1
            elif opt == "XX":
                xx = True
                i += 1
            elif opt == "KEEPTTL":
                keepttl = True
                i += 1
            else:
                return err("ERR syntax error")

        if nx and xx:
            return err("ERR XX and NX options at the same time are not compatible")

        self._check_expired(key, txn)
        exists = key in self._data

        if nx and exists:
            return NIL
        if xx and not exists:
            return NIL

        ops = [["SET", key, val]]
        if ex_ms is not None:
            ops.append(["PEXPIREAT", key, ex_ms])
        elif exists and key in self._expiries and not keepttl:
            ops.append(["PERSIST", key])

        txn.log("BATCH", ops)

        is_new = key not in self._data
        if is_new:
            self._evict_lru_if_needed(txn)

        old_v = self._data.get(key)
        self._update_type_count(key, old_v, val)
        self._data[key] = val

        if is_new:
            if key not in self._keys_set:
                self._keys_set.add(key)
                self._keys_list.append(key)
        self._data.move_to_end(key)

        if ex_ms is not None:
            self._expiries[key] = ex_ms
            heapq.heappush(self._expiries_heap, (ex_ms, key))
        elif not keepttl:
            self._expiries.pop(key, None)

        self._bump(txn, key)
        return OK

    def c_mset(self, a, txn):
        if len(a) % 2 != 0:
            return err("ERR wrong number of arguments for 'mset' command")
        ops = []
        pairs = []
        for i in range(0, len(a), 2):
            k, v = _s(a[i]), _s(a[i + 1])
            ops.append(["SET", k, v])
            if k in self._expiries:
                ops.append(["PERSIST", k])
            pairs.append((k, v))

        txn.log("BATCH", ops)

        for k, v in pairs:
            is_new = k not in self._data
            if is_new:
                self._evict_lru_if_needed(txn)
            old_v = self._data.get(k)
            self._update_type_count(k, old_v, v)
            self._data[k] = v
            if is_new:
                if k not in self._keys_set:
                    self._keys_set.add(k)
                    self._keys_list.append(k)
            self._data.move_to_end(k)
            self._expiries.pop(k, None)
            self._bump(txn, k)
        return OK

    def c_get(self, a, txn):
        key = _s(a[0])
        if self._check_expired(key, txn):
            return NIL
        v = self._data.get(key)
        if v is None:
            return NIL
        if not isinstance(v, str):
            return err("WRONGTYPE Operation against a key holding the wrong kind of value")
        self._data.move_to_end(key)
        return bulk(v)

    def c_getdel(self, a, txn):
        key = _s(a[0])
        if self._check_expired(key, txn) or key not in self._data:
            return NIL
        v = self._data.get(key)
        if not isinstance(v, str):
            return err("WRONGTYPE Operation against a key holding the wrong kind of value")
        txn.log("DEL", key)
        old_v = self._data.pop(key, None)
        self._update_type_count(key, old_v, None)
        self._expiries.pop(key, None)
        self._bump(txn, key)
        return bulk(v)

    def c_del(self, a, txn):
        return integer(self.delete_keys([_s(x) for x in a], txn))

    def c_exists(self, a, txn):
        count = 0
        for x in a:
            k = _s(x)
            if k in self._data and not self._check_expired(k, txn):
                count += 1
                self._data.move_to_end(k)
        return integer(count)

    def c_dbsize(self, a, txn):
        return integer(len(self._data))

    def c_keys(self, a, txn):
        pattern = _s(a[0])
        out = []
        for k in list(self._data.keys()):
            if self._check_expired(k, txn):
                continue
            if fnmatch.fnmatchcase(k, pattern):
                out.append(k)
        return array([bulk(k) for k in out])

    def c_scan(self, a, txn):
        try:
            cursor = int(_s(a[0]))
        except ValueError:
            return err("ERR invalid cursor")
        pattern, count = "*", 10
        i = 1
        while i < len(a):
            opt = _s(a[i]).upper()
            if opt == "MATCH" and i + 1 < len(a):
                pattern = _s(a[i + 1])
                i += 2
            elif opt == "COUNT" and i + 1 < len(a):
                try:
                    count = max(1, int(_s(a[i + 1])))
                except ValueError:
                    return err("ERR value is not an integer or out of range")
                i += 2
            else:
                return err("ERR syntax error")

        if cursor < 0:
            return err("ERR invalid cursor")

        res = []
        c = cursor
        inspected = 0
        while c < len(self._keys_list) and inspected < count:
            k = self._keys_list[c]
            c += 1
            inspected += 1
            if k in self._data and not self._check_expired(k, txn):
                if fnmatch.fnmatchcase(k, pattern):
                    res.append(k)

        next_cursor = c if c < len(self._keys_list) else 0
        return array([bulk(str(next_cursor)), array([bulk(k) for k in res])])

    # ---- counters / appends ----
    def c_incr_common(self, a, txn, delta):
        key = _s(a[0])
        self._check_expired(key, txn)
        if key in self._data:
            val = self._data[key]
            if not isinstance(val, str):
                return err("WRONGTYPE Operation against a key holding the wrong kind of value")
        else:
            val = "0"

        try:
            if val != str(int(val)):
                raise ValueError()
            new_val = int(val) + delta
        except (TypeError, ValueError):
            return err("ERR value is not an integer or out of range")

        s = str(new_val)
        txn.log("SET", key, s)

        is_new = key not in self._data
        if is_new:
            self._evict_lru_if_needed(txn)

        old_v = self._data.get(key)
        self._update_type_count(key, old_v, s)
        self._data[key] = s
        if is_new:
            if key not in self._keys_set:
                self._keys_set.add(key)
                self._keys_list.append(key)
        self._data.move_to_end(key)
        self._bump(txn, key)
        return integer(new_val)

    def c_incr(self, a, txn):
        return self.c_incr_common(a, txn, 1)

    def c_decr(self, a, txn):
        return self.c_incr_common(a, txn, -1)

    def c_incrby(self, a, txn):
        try:
            delta = int(_s(a[1]))
        except ValueError:
            return err("ERR value is not an integer or out of range")
        return self.c_incr_common(a, txn, delta)

    def c_append(self, a, txn):
        key, val = _s(a[0]), _s(a[1])
        self._check_expired(key, txn)

        old = self._data.get(key, "")
        if not isinstance(old, str):
            return err("WRONGTYPE Operation against a key holding the wrong kind of value")

        new_val = old + val
        txn.log("SET", key, new_val)

        is_new = key not in self._data
        if is_new:
            self._evict_lru_if_needed(txn)

        self._update_type_count(key, old, new_val)
        self._data[key] = new_val
        if is_new:
            if key not in self._keys_set:
                self._keys_set.add(key)
                self._keys_list.append(key)
        self._data.move_to_end(key)
        self._bump(txn, key)
        return integer(len(new_val.encode("utf-8")))

    # ---- expiry ----
    def c_expire_common(self, a, txn, ms_delta):
        key = _s(a[0])
        try:
            amount = int(_s(a[1]))
        except ValueError:
            return err("ERR value is not an integer or out of range")

        if self._check_expired(key, txn) or key not in self._data:
            return integer(0)

        # In Redis, non-positive expire time immediately deletes the key
        if amount <= 0:
            self.delete_keys([key], txn)
            return integer(1)

        when = _now_ms() + ms_delta(amount)
        txn.log("PEXPIREAT", key, when)
        self._expiries[key] = when
        heapq.heappush(self._expiries_heap, (when, key))
        self._bump(txn, key)
        return integer(1)

    def c_expire(self, a, txn):
        return self.c_expire_common(a, txn, lambda s: s * 1000)

    def c_pexpire(self, a, txn):
        return self.c_expire_common(a, txn, lambda ms: ms)

    def c_ttl(self, a, txn):
        key = _s(a[0])
        if self._check_expired(key, txn) or key not in self._data:
            return integer(-2)
        exp = self._expiries.get(key)
        if exp is None:
            return integer(-1)
        diff = exp - _now_ms()
        return integer(max(0, (diff + 999) // 1000))

    def c_pttl(self, a, txn):
        key = _s(a[0])
        if self._check_expired(key, txn) or key not in self._data:
            return integer(-2)
        exp = self._expiries.get(key)
        if exp is None:
            return integer(-1)
        return integer(max(0, exp - _now_ms()))

    def c_persist(self, a, txn):
        key = _s(a[0])
        if self._check_expired(key, txn) or key not in self._data:
            return integer(0)
        if key not in self._expiries:
            return integer(0)
        txn.log("PERSIST", key)
        self._expiries.pop(key, None)
        self._bump(txn, key)
        return integer(1)

    # ---- lists ----
    def c_lpush(self, a, txn):
        return self._push(a, txn, left=True)

    def c_rpush(self, a, txn):
        return self._push(a, txn, left=False)

    def _push(self, a, txn, left):
        key = _s(a[0])
        self._check_expired(key, txn)

        dq = self._data.get(key)
        is_new = dq is None
        if is_new:
            self._evict_lru_if_needed(txn)
            dq = collections.deque()
            self._update_type_count(key, None, dq)
            self._data[key] = dq
            if key not in self._keys_set:
                self._keys_set.add(key)
                self._keys_list.append(key)
        elif not isinstance(dq, collections.deque):
            return err("WRONGTYPE Operation against a key holding the wrong kind of value")

        op = "LPUSH" if left else "RPUSH"
        for v in a[1:]:
            vs = _s(v)
            (dq.appendleft if left else dq.append)(vs)
            txn.log(op, key, vs)

        self._data.move_to_end(key)
        self._bump(txn, key)
        return integer(len(dq))

    def c_lpop(self, a, txn):
        return self._pop(a, txn, left=True)

    def c_rpop(self, a, txn):
        return self._pop(a, txn, left=False)

    def _pop(self, a, txn, left):
        key = _s(a[0])
        count = 1
        want_many = False
        if len(a) > 1:
            try:
                count = max(0, int(_s(a[1])))
            except ValueError:
                return err("ERR value is not an integer or out of range")
            want_many = True

        if self._check_expired(key, txn):
            return array([]) if want_many else NIL

        v = self._data.get(key)
        if v is None:
            return array([]) if want_many else NIL
        if not isinstance(v, collections.deque):
            return err("WRONGTYPE Operation against a key holding the wrong kind of value")

        n = min(count, len(v))
        popped = [(v.popleft if left else v.pop)() for _ in range(n)]

        if popped:
            txn.log("LPOP" if left else "RPOP", key, n)
            self._bump(txn, key)
            if not v:
                old_v = self._data.pop(key, None)
                self._update_type_count(key, old_v, None)
                self._expiries.pop(key, None)
            else:
                self._data.move_to_end(key)

        if want_many:
            return array([bulk(x) for x in popped])
        return bulk(popped[0]) if popped else NIL

    def c_llen(self, a, txn):
        key = _s(a[0])
        if self._check_expired(key, txn):
            return integer(0)
        v = self._data.get(key)
        if v is None:
            return integer(0)
        if not isinstance(v, collections.deque):
            return err("WRONGTYPE Operation against a key holding the wrong kind of value")
        return integer(len(v))

    def c_lrange(self, a, txn):
        key = _s(a[0])
        try:
            start, stop = int(_s(a[1])), int(_s(a[2]))
        except ValueError:
            return err("ERR value is not an integer or out of range")

        if self._check_expired(key, txn):
            return array([])
        v = self._data.get(key)
        if v is None:
            return array([])
        if not isinstance(v, collections.deque):
            return err("WRONGTYPE Operation against a key holding the wrong kind of value")

        n = len(v)
        if start < 0: start += n
        if stop < 0: stop += n
        start = max(0, start)

        if stop < 0 or start >= n or start > stop:
            return array([])

        stop = min(stop, n - 1)
        items = list(v)[start:stop + 1]
        self._data.move_to_end(key)
        return array([bulk(x) for x in items])

    # ---- hashes ----
    def c_hset(self, a, txn):
        key = _s(a[0])
        rest = a[1:]
        if len(rest) == 0 or len(rest) % 2 != 0:
            return err("ERR wrong number of arguments for 'hset' command")

        self._check_expired(key, txn)
        h = self._data.get(key)
        is_new = h is None
        if is_new:
            self._evict_lru_if_needed(txn)
            h = {}
            self._update_type_count(key, None, h)
            self._data[key] = h
            if key not in self._keys_set:
                self._keys_set.add(key)
                self._keys_list.append(key)
        elif not isinstance(h, dict):
            return err("WRONGTYPE Operation against a key holding the wrong kind of value")

        added = 0
        for i in range(0, len(rest), 2):
            f, v = _s(rest[i]), _s(rest[i + 1])
            if f not in h:
                added += 1
            h[f] = v
            txn.log("HSET", key, f, v)

        self._data.move_to_end(key)
        self._bump(txn, key)
        return integer(added)

    def c_hget(self, a, txn):
        key, field = _s(a[0]), _s(a[1])
        if self._check_expired(key, txn):
            return NIL
        h = self._data.get(key)
        if h is None:
            return NIL
        if not isinstance(h, dict):
            return err("WRONGTYPE Operation against a key holding the wrong kind of value")
        self._data.move_to_end(key)
        return bulk(h.get(field))

    def c_hdel(self, a, txn):
        key = _s(a[0])
        fields = [_s(x) for x in a[1:]]
        if self._check_expired(key, txn):
            return integer(0)
        h = self._data.get(key)
        if h is None:
            return integer(0)
        if not isinstance(h, dict):
            return err("WRONGTYPE Operation against a key holding the wrong kind of value")

        removed = 0
        for f in fields:
            if f in h:
                del h[f]
                removed += 1
                txn.log("HDEL", key, f)

        if removed:
            self._bump(txn, key)
            if not h:
                old_v = self._data.pop(key, None)
                self._update_type_count(key, old_v, None)
                self._expiries.pop(key, None)
            else:
                self._data.move_to_end(key)
        return integer(removed)

    def c_hgetall(self, a, txn):
        key = _s(a[0])
        if self._check_expired(key, txn):
            return array([])
        h = self._data.get(key)
        if h is None:
            return array([])
        if not isinstance(h, dict):
            return err("WRONGTYPE Operation against a key holding the wrong kind of value")
        self._data.move_to_end(key)
        items = []
        for f, v in h.items():
            items.append(bulk(f))
            items.append(bulk(v))
        return array(items)

    def c_hlen(self, a, txn):
        key = _s(a[0])
        if self._check_expired(key, txn):
            return integer(0)
        h = self._data.get(key)
        if h is None:
            return integer(0)
        if not isinstance(h, dict):
            return err("WRONGTYPE Operation against a key holding the wrong kind of value")
        return integer(len(h))

    # ---- info / misc ----
    def c_info(self, a, txn):
        uptime = int(time.time() - self.start_time)
        info = (
            "# Server\r\n"
            "redis_version:mini-redis-2\r\n"
            f"uptime_in_seconds:{uptime}\r\n"
            f"connected_clients:{self.connected_clients}\r\n"
            f"max_keys:{self.max_keys}\r\n"
            "# Keyspace\r\n"
            f"db0:keys={len(self._data)},expires={len(self._expiries)},"
            f"strings={self._type_counts['string']},lists={self._type_counts['list']},hashes={self._type_counts['hash']}\r\n"
            "# WAL\r\n"
            f"wal_size:{self._wal_size}\r\n"
            f"wal_ops:{self._wal_ops}\r\n"
            f"fsync_policy:{self.fsync_policy}\r\n"
            f"last_compaction:{self.last_compact_time:.3f}\r\n"
            "# Stats\r\n"
            f"evicted_keys:{self.evicted_count}\r\n"
            f"watched_keys:{len(self._key_versions)}\r\n"
        )
        return bulk(info)

    def c_config(self, a, txn):
        sub = _s(a[0]).upper()
        if sub == "GET" and len(a) == 2:
            param = _s(a[1]).upper()
            if param == "MAXKEYS":
                return array([bulk("maxkeys"), bulk(str(self.max_keys))])
            return array([])
        if sub == "SET" and len(a) == 3:
            param = _s(a[1]).upper()
            if param == "MAXKEYS":
                try:
                    self.max_keys = max(0, int(_s(a[2])))
                except ValueError:
                    return err("ERR Invalid argument")
                return OK
            return err("ERR Unsupported CONFIG parameter")
        return err("ERR Unknown CONFIG subcommand")

    def c_compact(self, a, txn):
        raise CompactionInProgress()

    def c_pubsub(self, a, txn):
        sub = _s(a[0]).upper()
        if sub == "CHANNELS":
            pat = _s(a[1]) if len(a) > 1 else "*"
            return array([bulk(c) for c in self.channel_names(pat)])
        return err("ERR Unknown PUBSUB subcommand")

    def c_unwatch(self, a, txn):
        return OK

    def c_command(self, a, txn):
        return b"*0\r\n"

    # ------------------------------------------------- maintenance methods ----
    def sync(self) -> None:
        with self._wal_io_lock:
            if self._dirty and self._wal is not None:
                self._wal.flush()
                if hasattr(os, "fdatasync"):
                    os.fdatasync(self._wal.fileno())
                else:
                    os.fsync(self._wal.fileno())
                self._dirty = False

    def sweep_expired(self) -> int:
        now = _now_ms()
        expired = []
        with self._lock:
            while self._expiries_heap and self._expiries_heap[0][0] <= now:
                exp, k = heapq.heappop(self._expiries_heap)
                if k in self._data and self._expiries.get(k) == exp:
                    expired.append(k)
            if expired:
                self.delete_keys(expired)
        return len(expired)

    def needs_compaction(self) -> bool:
        with self._lock:
            return (
                    not self._compacting
                    and self._wal_ops >= self.compact_min_ops
                    and self._wal_ops > self.compact_ratio * max(len(self._data), 1)
            )

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True

        self._wal_queue.put({"stop": True})
        self._wal_queue.join()
        self._writer_thread.join(timeout=5.0)

        with self._lock, self._wal_io_lock:
            if self._wal is not None:
                try:
                    self._wal.flush()
                    if hasattr(os, "fdatasync"):
                        os.fdatasync(self._wal.fileno())
                    else:
                        os.fsync(self._wal.fileno())
                finally:
                    self._wal.close()
                    self._wal = None


class _nullcontext:
    def __enter__(self): return None

    def __exit__(self, *exc): return False


# name -> (min args, max args or None, handler)
COMMANDS = {
    "PING": (0, 1, MiniRedisEngine.c_ping),
    "ECHO": (1, 1, MiniRedisEngine.c_echo),
    "SET": (2, None, MiniRedisEngine.c_set),
    "MSET": (2, None, MiniRedisEngine.c_mset),
    "GET": (1, 1, MiniRedisEngine.c_get),
    "GETDEL": (1, 1, MiniRedisEngine.c_getdel),
    "DEL": (1, None, MiniRedisEngine.c_del),
    "EXISTS": (1, None, MiniRedisEngine.c_exists),
    "DBSIZE": (0, 0, MiniRedisEngine.c_dbsize),
    "KEYS": (1, 1, MiniRedisEngine.c_keys),
    "SCAN": (1, None, MiniRedisEngine.c_scan),
    "INCR": (1, 1, MiniRedisEngine.c_incr),
    "DECR": (1, 1, MiniRedisEngine.c_decr),
    "INCRBY": (2, 2, MiniRedisEngine.c_incrby),
    "APPEND": (2, 2, MiniRedisEngine.c_append),
    "EXPIRE": (2, 2, MiniRedisEngine.c_expire),
    "PEXPIRE": (2, 2, MiniRedisEngine.c_pexpire),
    "TTL": (1, 1, MiniRedisEngine.c_ttl),
    "PTTL": (1, 1, MiniRedisEngine.c_pttl),
    "PERSIST": (1, 1, MiniRedisEngine.c_persist),
    "LPUSH": (2, None, MiniRedisEngine.c_lpush),
    "RPUSH": (2, None, MiniRedisEngine.c_rpush),
    "LPOP": (1, 2, MiniRedisEngine.c_lpop),
    "RPOP": (1, 2, MiniRedisEngine.c_rpop),
    "LLEN": (1, 1, MiniRedisEngine.c_llen),
    "LRANGE": (3, 3, MiniRedisEngine.c_lrange),
    "HSET": (3, None, MiniRedisEngine.c_hset),
    "HGET": (2, 2, MiniRedisEngine.c_hget),
    "HDEL": (2, None, MiniRedisEngine.c_hdel),
    "HGETALL": (1, 1, MiniRedisEngine.c_hgetall),
    "HLEN": (1, 1, MiniRedisEngine.c_hlen),
    "INFO": (0, None, MiniRedisEngine.c_info),
    "CONFIG": (2, 3, MiniRedisEngine.c_config),
    "COMPACT": (0, 0, MiniRedisEngine.c_compact),
    "PUBSUB": (1, 2, MiniRedisEngine.c_pubsub),
    "UNWATCH": (0, 0, MiniRedisEngine.c_unwatch),
    "COMMAND": (0, None, MiniRedisEngine.c_command),
}


# ------------------------------------------------------------- RESP input ----
def parse_one_resp(buffer: bytes):
    if not buffer:
        raise Incomplete()
    if buffer.startswith(b"*"):
        crlf = buffer.find(b"\r\n")
        if crlf == -1:
            raise Incomplete()
        try:
            count = int(buffer[1:crlf])
        except ValueError:
            raise ProtocolError("invalid multibulk length") from None
        if count < 0 or count > MAX_ARGS:
            raise ProtocolError("invalid multibulk length")

        pos = crlf + 2
        args: list[bytes] = []
        for _ in range(count):
            if pos >= len(buffer) or buffer[pos:pos + 1] != b"$":
                if pos >= len(buffer):
                    raise Incomplete()
                raise ProtocolError("expected '$' in multibulk")
            crlf2 = buffer.find(b"\r\n", pos)
            if crlf2 == -1:
                raise Incomplete()
            try:
                length = int(buffer[pos + 1:crlf2])
            except ValueError:
                raise ProtocolError("invalid bulk length") from None
            if length < 0 or length > MAX_BULK:
                raise ProtocolError("invalid bulk length")

            end = crlf2 + 2 + length + 2
            if end > MAX_COMMAND_SIZE:
                raise ProtocolError("command too large")
            if end > len(buffer):
                raise Incomplete()
            if buffer[end - 2:end] != b"\r\n":
                raise ProtocolError("bulk string not terminated by CRLF")

            args.append(buffer[crlf2 + 2:crlf2 + 2 + length])
            pos = end

        return args, pos

    # Inline command parsing: support both \r\n and \n without eating across lines
    lf = buffer.find(b"\n")
    if lf == -1:
        raise Incomplete()
    if lf > 0 and buffer[lf - 1:lf] == b"\r":
        line = buffer[:lf - 1]
    else:
        line = buffer[:lf]
    consumed = lf + 1

    if not line.strip():
        return [], consumed

    try:
        raw_tokens = shlex.split(line.decode("utf-8", "surrogateescape"), posix=False)
        args = [_strip_quotes(t).encode("utf-8", "surrogateescape") for t in raw_tokens]
    except ValueError:
        raise ProtocolError("unbalanced quotes in request") from None

    if consumed > MAX_COMMAND_SIZE:
        raise ProtocolError("command too large")
    return args, consumed


# ----------------------------------------------------------- compaction ----
def run_compaction(db: MiniRedisEngine) -> int:
    tmp = db.path + ".tmp"
    with db._lock:
        if db._closed:
            raise RuntimeError("engine is closed")
        if db._compacting:
            raise CompactionInProgress()
        db._compacting = True

        snapshot = {}
        expiries = {}
        for k, v in db._data.items():
            exp = db._expiries.get(k)
            if exp is not None and exp <= _now_ms():
                continue
            if isinstance(v, collections.deque):
                snapshot[k] = list(v)
            elif isinstance(v, dict):
                snapshot[k] = dict(v)
            else:
                snapshot[k] = v
            if exp is not None:
                expiries[k] = exp

        # Compact key lists and prune stale watched key versions
        db._keys_list = list(db._data.keys())
        db._keys_set = set(db._data.keys())
        db._key_versions = {k: v for k, v in db._key_versions.items() if k in db._data}

        swap_done = threading.Event()
        swap_error = None

        def swap_callback(error=None):
            nonlocal swap_error
            swap_error = error
            swap_done.set()

    try:
        with open(tmp, "wb") as f:
            for k, v in snapshot.items():
                if isinstance(v, str):
                    rec = ["SET", k, v]
                elif isinstance(v, list):
                    rec = ["BATCH", [["RPUSH", k, x] for x in v]]
                elif isinstance(v, dict):
                    rec = ["BATCH", [["HSET", k, field, hv] for field, hv in v.items()]]
                else:
                    continue

                if k in expiries:
                    if rec[0] == "BATCH":
                        rec[1].append(["PEXPIREAT", k, expiries[k]])
                    else:
                        rec = ["BATCH", [rec, ["PEXPIREAT", k, expiries[k]]]]

                payload = json.dumps(rec, separators=(",", ":"))
                crc = zlib.crc32(payload.encode("ascii")) & 0xFFFFFFFF
                f.write(f"{crc:08x} {payload}\n".encode("ascii"))
            f.flush()
            if hasattr(os, "fdatasync"):
                os.fdatasync(f.fileno())
            else:
                os.fsync(f.fileno())

        db._wal_queue.put({
            "swap": (tmp, db.path),
            "callback": swap_callback,
            "wal_ops": len(snapshot)
        })

        swap_done.wait()
        if swap_error:
            raise swap_error

        log.info("WAL compaction finished (%d keys)", len(snapshot))
        return len(snapshot)

    except BaseException:
        try:
            os.remove(tmp)
        except OSError:
            pass
        raise
    finally:
        with db._lock:
            db._compacting = False


async def compact_serialized(db: MiniRedisEngine) -> int:
    async with db._transaction_lock:
        await asyncio.to_thread(db._wal_queue.join)
        return await asyncio.to_thread(run_compaction, db)


# -------------------------------------------------------- client handling ----
async def read_buffered(reader: asyncio.StreamReader, buf: bytearray, timeout: float | None):
    try:
        chunk = await asyncio.wait_for(reader.read(65536), timeout=timeout)
    except asyncio.TimeoutError:
        return False
    if not chunk:
        return False
    buf.extend(chunk)
    if len(buf) > MAX_COMMAND_SIZE:
        raise ProtocolError("command too large")
    return True


def make_queue() -> asyncio.Queue:
    return asyncio.Queue(maxsize=1024)


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, db: MiniRedisEngine) -> None:
    peer = writer.get_extra_info("peername")
    log.info("Client connected: %s", peer)
    db.connected_clients += 1

    mode = "normal"
    tx_queue: list[list[bytes]] = []
    watched: dict[str, int] = {}
    buf = bytearray()

    msg_q = make_queue()
    channel_qs: dict[str, asyncio.Queue] = {}
    pattern_qs: dict[str, asyncio.Queue] = {}
    reader_task: asyncio.Task | None = None

    def sub_reply(kind: bytes, ch: str, count: int) -> bytes:
        return array([bulk_bytes(kind), bulk(ch), integer(count)])

    def add_channels(names: list[str]) -> list[bytes]:
        out = []
        for ch in names:
            if ch not in channel_qs:
                channel_qs[ch] = msg_q
                with db._lock:
                    db._channels.setdefault(ch, set()).add(msg_q)
                out.append(sub_reply(b"subscribe", ch, len(channel_qs) + len(pattern_qs)))
        return out

    def remove_channels(names: list[str]) -> list[bytes]:
        out = []
        for ch in names:
            if ch in channel_qs:
                del channel_qs[ch]
                with db._lock:
                    subs = db._channels.get(ch)
                    if subs is not None:
                        subs.discard(msg_q)
                        if not subs:
                            del db._channels[ch]
                out.append(sub_reply(b"unsubscribe", ch, len(channel_qs) + len(pattern_qs)))
        return out

    def add_patterns(pats: list[str]) -> list[bytes]:
        out = []
        for p in pats:
            if p not in pattern_qs:
                pattern_qs[p] = msg_q
                with db._lock:
                    db._patterns.append((p, msg_q))
                out.append(sub_reply(b"psubscribe", p, len(channel_qs) + len(pattern_qs)))
        return out

    def remove_patterns(pats: list[str]) -> list[bytes]:
        out = []
        for p in pats:
            if p in pattern_qs:
                del pattern_qs[p]
                with db._lock:
                    db._patterns[:] = [(pp, q) for pp, q in db._patterns if not (pp == p and q is msg_q)]
                out.append(sub_reply(b"punsubscribe", p, len(channel_qs) + len(pattern_qs)))
        return out

    async def pubsub_reader() -> None:
        rbuf = bytearray(buf)
        buf.clear()
        try:
            while True:
                if not await read_buffered(reader, rbuf, timeout=None):
                    await msg_q.put(("eof", None))
                    return
                while True:
                    try:
                        cmd, consumed = parse_one_resp(bytes(rbuf))
                    except Incomplete:
                        break
                    except ProtocolError:
                        await msg_q.put(("eof", None))
                        return
                    del rbuf[:consumed]
                    if not cmd:
                        continue
                    await msg_q.put(("cmd", cmd))
        except asyncio.CancelledError:
            raise
        except Exception:
            await msg_q.put(("eof", None))

    async def run_pubsub_command(cmd: list[bytes]) -> bool:
        nonlocal mode
        name = cmd[0].upper()
        if name == b"PING":
            writer.write(bulk_bytes(cmd[1]) if len(cmd) > 1 else b"+PONG\r\n")
        elif name in (b"QUIT", b"EXIT"):
            writer.write(OK)
            return False
        elif name == b"SUBSCRIBE":
            for r in add_channels([_s(x) for x in cmd[1:]]):
                writer.write(r)
        elif name == b"UNSUBSCRIBE":
            names = [_s(x) for x in cmd[1:]] or list(channel_qs.keys())
            for r in remove_channels(names):
                writer.write(r)
            if not channel_qs and not pattern_qs:
                mode = "normal"
                return True
        elif name == b"PSUBSCRIBE":
            for r in add_patterns([_s(x) for x in cmd[1:]]):
                writer.write(r)
        elif name == b"PUNSUBSCRIBE":
            pats = [_s(x) for x in cmd[1:]] or list(pattern_qs.keys())
            for r in remove_patterns(pats):
                writer.write(r)
            if not channel_qs and not pattern_qs:
                mode = "normal"
                return True
        elif name == b"PUBSUB" and len(cmd) > 1 and cmd[1].upper() == b"CHANNELS":
            pat = _s(cmd[2]) if len(cmd) > 2 else "*"
            writer.write(array([bulk(c) for c in db.channel_names(pat)]))
        else:
            writer.write(err("ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT / PUBSUB allowed in this context"))
        await writer.drain()
        return True

    def cleanup_pubsub():
        with db._lock:
            for ch, subs in list(db._channels.items()):
                subs.discard(msg_q)
                if not subs:
                    del db._channels[ch]
            db._patterns[:] = [(p, q) for p, q in db._patterns if q is not msg_q]

    try:
        while True:
            if mode == "pubsub":
                if reader_task is None:
                    reader_task = asyncio.create_task(pubsub_reader())
                item = await msg_q.get()
                kind = item[0]
                if kind == "eof":
                    return
                if kind == "cmd":
                    if not await run_pubsub_command(item[1]):
                        return
                if mode != "pubsub":
                    # Client unsubscribed from all channels/patterns
                    if reader_task is not None:
                        reader_task.cancel()
                        reader_task = None
                elif kind in ("message", "pmessage"):
                    if kind == "message":
                        _, ch, payload = item
                        writer.write(array([bulk("message"), bulk(ch), bulk(payload)]))
                    else:
                        _, pat, ch, payload = item
                        writer.write(array([bulk("pmessage"), bulk(pat), bulk(ch), bulk(payload)]))
                    await writer.drain()
                continue

            if not await read_buffered(reader, buf, CLIENT_TIMEOUT):
                break

            pipeline: list[list[bytes]] = []
            while True:
                try:
                    cmd, consumed = parse_one_resp(bytes(buf))
                except Incomplete:
                    break
                except ProtocolError as e:
                    writer.write(err(f"ERR Protocol error: {e}"))
                    await writer.drain()
                    return
                del buf[:consumed]
                if cmd:
                    pipeline.append(cmd)

            if not pipeline:
                continue

            replies: list[bytes] = []
            normal_cmds: list[list[bytes]] = []
            enter_pubsub = False
            disconnect = False

            async def flush_normal_cmds():
                nonlocal normal_cmds
                if normal_cmds:
                    resp, ok = await db.run_transaction(normal_cmds)
                    if ok:
                        replies.extend(resp)
                    else:
                        replies.extend([err("ERR internal error")] * len(normal_cmds))
                    normal_cmds = []

            for cmd in pipeline:
                name = cmd[0].upper()
                if name in (b"QUIT", b"EXIT"):
                    await flush_normal_cmds()
                    replies.append(OK)
                    disconnect = True
                    break

                if mode == "multi":
                    if name == b"EXEC":
                        await flush_normal_cmds()
                        mode = "normal"
                        resp, ok = await db.run_transaction(tx_queue, watched)
                        replies.append(NIL if (not ok and resp is None) else array(list(resp) if ok else resp))
                        tx_queue, watched = [], {}
                        continue
                    if name == b"DISCARD":
                        await flush_normal_cmds()
                        mode = "normal"
                        tx_queue, watched = [], {}
                        replies.append(OK)
                        continue
                    if name in (b"WATCH", b"MULTI"):
                        await flush_normal_cmds()
                        replies.append(err(f"ERR {name.decode()} inside MULTI is not allowed"))
                        continue

                    tx_queue.append(cmd)
                    replies.append(QUEUED)
                    continue

                # Normal mode
                if name == b"MULTI":
                    await flush_normal_cmds()
                    mode = "multi"
                    tx_queue = []
                    replies.append(OK)
                    continue
                if name == b"WATCH":
                    await flush_normal_cmds()
                    with db._lock:
                        for k in cmd[1:]:
                            ks = _s(k)
                            watched[ks] = db._key_versions.get(ks, 0)
                    replies.append(OK)
                    continue
                if name == b"UNWATCH":
                    await flush_normal_cmds()
                    watched.clear()
                    replies.append(OK)
                    continue
                if name in (b"EXEC", b"DISCARD"):
                    await flush_normal_cmds()
                    replies.append(err(f"ERR {name.decode()} without MULTI"))
                    continue
                if name in (b"SUBSCRIBE", b"PSUBSCRIBE", b"PUBLISH", b"COMPACT"):
                    await flush_normal_cmds()
                    if name == b"SUBSCRIBE":
                        replies.extend(add_channels([_s(x) for x in cmd[1:]]))
                        enter_pubsub = True
                    elif name == b"PSUBSCRIBE":
                        replies.extend(add_patterns([_s(x) for x in cmd[1:]]))
                        enter_pubsub = True
                    elif name == b"PUBLISH":
                        replies.append(integer(db.publish(_s(cmd[1]), _s(cmd[2]))))
                    elif name == b"COMPACT":
                        try:
                            await compact_serialized(db)
                            replies.append(OK)
                        except CompactionInProgress:
                            replies.append(err("ERR compaction already in progress"))
                        except Exception as e:
                            log.exception("compaction failed")
                            replies.append(err(f"ERR compact failed: {e}"))
                    continue

                normal_cmds.append(cmd)

            await flush_normal_cmds()

            for r in replies:
                writer.write(r)
            await writer.drain()

            if disconnect:
                break
            if enter_pubsub:
                mode = "pubsub"

    except ConnectionError:
        pass
    except Exception:
        log.exception("Error handling client %s", peer)
    finally:
        if reader_task is not None:
            reader_task.cancel()
        cleanup_pubsub()
        db.connected_clients -= 1
        log.info("Client disconnected: %s", peer)
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass


# ----------------------------------------------------------- maintenance ----
async def maintenance_loop(db: MiniRedisEngine) -> None:
    while True:
        await asyncio.sleep(MAINTENANCE_INTERVAL)
        try:
            async with db._transaction_lock:
                if db.fsync_policy == "everysec":
                    await asyncio.to_thread(db.sync)
                await asyncio.to_thread(db.sweep_expired)
                # Expiry deletions enqueue WAL records; drain them before a possible snapshot rewrite.
                await asyncio.to_thread(db._wal_queue.join)

                if db.needs_compaction():
                    log.info("WAL is mostly stale records; compacting")
                    await asyncio.to_thread(run_compaction, db)
        except CompactionInProgress:
            pass
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Maintenance task failed")


# ------------------------------------------------------------------ main ----
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Mini-Redis server (RESP + WAL)")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--wal", default="wal.log", help="write-ahead log path")
    parser.add_argument("--fsync", choices=FSYNC_POLICIES, default="everysec")
    parser.add_argument("--max-keys", type=int, default=0, help="LRU eviction threshold (0 = unlimited)")
    return parser.parse_args()


async def main(args: argparse.Namespace) -> None:
    db = MiniRedisEngine(args.wal, fsync=args.fsync, max_keys=args.max_keys)
    maintenance = asyncio.create_task(maintenance_loop(db), name="maintenance")
    clients: set[asyncio.StreamWriter] = set()

    async def on_connect(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        if len(clients) >= MAX_CLIENTS:
            writer.write(err("ERR max number of clients reached"))
            await writer.drain()
            writer.close()
            return
        clients.add(writer)
        try:
            await handle_client(reader, writer, db)
        finally:
            clients.discard(writer)

    server = await asyncio.start_server(on_connect, args.host, args.port)
    log.info("Mini-Redis listening on %s:%d (wal=%s, fsync=%s, max-keys=%s)",
             args.host, args.port, args.wal, args.fsync, args.max_keys or "off")
    try:
        await asyncio.Event().wait()
    finally:
        server.close()
        for writer in list(clients):
            writer.close()
        try:
            await asyncio.wait_for(server.wait_closed(), timeout=5)
        except asyncio.TimeoutError:
            log.warning("Some clients did not disconnect in time")
        maintenance.cancel()
        await asyncio.gather(maintenance, return_exceptions=True)
        db.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    try:
        asyncio.run(main(parse_args()))
    except KeyboardInterrupt:
        log.info("Shutting down Mini-Redis.")
