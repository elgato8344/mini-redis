#!/usr/bin/env python3
"""mini_cli.py — native Redis REPL built on cmd.Cmd + readline (stdlib only).
Usage:
python3 mini_cli.py [--host 127.0.0.1] [--port 6379] [--demo]
"""
from __future__ import annotations
import argparse
import cmd
import os
import shlex
import socket
import sys
import time

try:
    import readline  # noqa: F401  (enables history + arrow keys in cmd.Cmd)
except ImportError:
    pass

# ------------------------------------------------------------- ANSI colors ----
IS_TTY = sys.stdout.isatty()
NO_COLOR = os.environ.get("NO_COLOR")
USE_COLOR = IS_TTY and not NO_COLOR

class C:
    if USE_COLOR:
        RESET = "\x1b[0m"
        GREEN = "\x1b[32m"
        RED = "\x1b[31m"
        CYAN = "\x1b[36m"
        YELLOW = "\x1b[33m"
        MAGENTA = "\x1b[35m"
        DIM = "\x1b[2m"
        BOLD = "\x1b[1m"
    else:
        RESET = GREEN = RED = CYAN = YELLOW = MAGENTA = DIM = BOLD = ""

def escape_control(v: str) -> str:
    return v.replace("\\", "\\\\").replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t")

def _strip_quotes(tok: str) -> str:
    if len(tok) >= 2 and ((tok[0] == '"' and tok[-1] == '"') or (tok[0] == "'" and tok[-1] == "'")):
        return tok[1:-1]
    return tok

# ---------------------------------------------------------------- RESP wire ----
def encode_command(parts: list[str]) -> bytes:
    out = [b"*%d\r\n" % len(parts)]
    for p in parts:
        b = p.encode("utf-8", "surrogateescape")
        out.append(b"$%d\r\n%s\r\n" % (len(b), b))
    return b"".join(out)

class RespReader:
    """Buffered RESP2 response parser over a socket."""
    def __init__(self, sock: socket.socket):
        self.sock = sock
        self.buf = bytearray()
        self.pos = 0

    def _fill(self) -> None:
        data = self.sock.recv(65536)
        if not data:
            raise ConnectionError("server closed the connection")
        self.buf.extend(data)

    def _readline(self) -> bytes:
        while True:
            idx = self.buf.find(b"\r\n", self.pos)
            if idx != -1:
                line = bytes(self.buf[self.pos:idx])
                self.pos = idx + 2
                # Amortized O(1) buffer compaction to prevent unbounded growth
                if self.pos > 65536:
                    self.buf = self.buf[self.pos:]
                    self.pos = 0
                return line
            self._fill()

    def _readn(self, n: int) -> bytes:
        while len(self.buf) - self.pos < n:
            self._fill()
        data = bytes(self.buf[self.pos:self.pos+n])
        self.pos += n
        if self.pos > 65536:
            self.buf = self.buf[self.pos:]
            self.pos = 0
        return data

    def read_reply(self):
        line = self._readline()
        t, rest = line[:1], line[1:]
        if t == b"+":
            return "simple", rest.decode("utf-8", "surrogateescape")
        if t == b"-":
            return "error", rest.decode("utf-8", "surrogateescape")
        if t == b":":
            return "integer", int(rest)
        if t == b"$":
            n = int(rest)
            if n == -1:
                return "bulk", None
            raw = self._readn(n + 2)
            if raw[-2:] != b"\r\n":
                raise ValueError("malformed RESP bulk string terminator")
            data = raw[:-2]
            return "bulk", data.decode("utf-8", "surrogateescape")
        if t == b"*":
            n = int(rest)
            if n == -1:
                return "array", None
            return "array", [self.read_reply() for _ in range(n)]
        raise ValueError(f"bad RESP type byte: {t!r}")

def pretty(reply, indent: int = 0) -> str:
    pad = " " * indent
    kind, v = reply
    if kind == "simple":
        return f"{pad}{C.GREEN}{v}{C.RESET}"
    if kind == "error":
        return f"{pad}{C.RED}{v}{C.RESET}"
    if kind == "integer":
        return f"{pad}{C.CYAN}(integer) {v}{C.RESET}"
    if kind == "bulk":
        if v is None:
            return f"{pad}{C.DIM}(nil){C.RESET}"
        return f'{pad}{C.YELLOW}"{escape_control(v)}"{C.RESET}'
    if kind == "array":
        if v is None:
            return f"{pad}{C.DIM}(nil){C.RESET}"
        if not v:
            return f"{pad}{C.DIM}(empty array){C.RESET}"
        lines = []
        for i, item in enumerate(v, 1):
            first = pretty(item, indent + 2)
            lines.append(f"{pad}{C.DIM}{i}){C.RESET} {first}")
        return "\n".join(lines)
    return f"{pad}{v}"

# --------------------------------------------------------------------- REPL ----
WORDS = [
    "PING", "ECHO", "SET", "GET", "DEL", "EXISTS", "DBSIZE", "KEYS", "SCAN",
    "MSET", "INCR", "DECR", "INCRBY", "APPEND", "GETDEL",
    "EXPIRE", "PEXPIRE", "TTL", "PTTL", "PERSIST",
    "LPUSH", "RPUSH", "LPOP", "RPOP", "LLEN", "LRANGE",
    "HSET", "HGET", "HDEL", "HGETALL", "HLEN",
    "MULTI", "EXEC", "DISCARD", "WATCH", "UNWATCH",
    "SUBSCRIBE", "PSUBSCRIBE", "UNSUBSCRIBE", "PUBLISH", "PUBSUB",
    "INFO", "CONFIG", "COMPACT", "QUIT",
]

class MiniRedisCLI(cmd.Cmd):
    intro = (f"{C.BOLD}mini-redis REPL{C.RESET} — "
             f"type a command ({C.CYAN}SET k v{C.RESET}), "
             f"{C.CYAN}HELP{C.RESET} for help, {C.CYAN}QUIT{C.RESET} to exit.")

    def __init__(self, host: str, port: int):
        super().__init__()
        self.host, self.port = host, port
        self.prompt = f"{C.MAGENTA}{host}:{port}>{C.RESET} "
        self.sock = socket.create_connection((host, port))
        self.reader = RespReader(self.sock)
        self.tx = False
        self.scan_cursor = "0"

    def send(self, parts: list[str]):
        t0 = time.perf_counter()
        self.sock.sendall(encode_command(parts))
        reply = self.reader.read_reply()
        dt = (time.perf_counter() - t0) * 1000
        print(pretty(reply))
        print(f"{C.DIM}({dt:.2f} ms){C.RESET}")
        return reply

    def send_capture(self, parts: list[str]):
        self.sock.sendall(encode_command(parts))
        return self.reader.read_reply()

    def default(self, line: str):
        if not line.strip():
            return
        try:
            # posix=False preserves backslashes (e.g., Windows paths like C:\temp)
            # Enclosing matching quotes are stripped so literal quotes are not retained
            raw_parts = shlex.split(line, posix=False)
            parts = [_strip_quotes(p) for p in raw_parts]
        except ValueError:
            print(f"{C.RED}ERR unbalanced quotes{C.RESET}")
            return

        try:
            name = parts[0].upper()
            if name == "HELP":
                self.do_help(" ".join(parts[1:]))
                return

            if self.tx and name not in ("EXEC", "DISCARD", "MULTI", "WATCH", "UNWATCH"):
                self.send(parts)
                return

            if name == "MULTI":
                self.tx = True
            elif name in ("EXEC", "DISCARD"):
                self.tx = False
            elif name == "SCAN":
                if len(parts) == 1 or parts[1].upper() in ("MATCH", "COUNT"):
                    parts.insert(1, self.scan_cursor)
                reply = self.send(parts)
                if (reply[0] == "array" and reply[1] is not None
                    and len(reply[1]) == 2 and reply[1][0][0] == "bulk"):
                    self.scan_cursor = str(reply[1][0][1])
                    print(f"{C.DIM}cursor → {self.scan_cursor}{C.RESET}")
                return
            elif name in ("SUBSCRIBE", "PSUBSCRIBE"):
                # Send subscribe command and read the immediate confirmation(s)
                self.sock.sendall(encode_command(parts))
                # Number of confirmations corresponds to number of channels/patterns requested
                num_subs = max(1, len(parts) - 1)
                for _ in range(num_subs):
                    print(pretty(self.reader.read_reply()))
                print(f"{C.DIM}subscribed — messages stream below; Ctrl+C to stop reading.{C.RESET}")
                try:
                    while True:
                        print(pretty(self.reader.read_reply()))
                except KeyboardInterrupt:
                    print(f"\n{C.DIM}(stopped reading; UNSUBSCRIBE to leave pubsub){C.RESET}")
                return
            elif name in ("QUIT", "EXIT"):
                try:
                    self.send_capture(["QUIT"])
                except OSError:
                    pass
                print("bye")
                return True

            self.send(parts)
        except (ConnectionError, OSError) as e:
            print(f"{C.RED}connection lost: {e}{C.RESET}")
            return True

    def emptyline(self):
        pass

    def completenames(self, text, *ignored):
        return [w for w in WORDS if w.startswith(text.upper())]

    def completedefault(self, text, line, *ignored):
        if " " not in line:
            return [w + " " for w in WORDS if w.startswith(text.upper())]
        return []

    def do_help(self, arg):
        print(f"""{C.BOLD}mini-redis commands{C.RESET}
{C.CYAN}strings{C.RESET}   SET k v [EX s|PX ms|NX|XX|KEEPTTL] · GET · MSET · APPEND · GETDEL
            INCR · DECR · INCRBY n
{C.CYAN}expiry{C.RESET}    EXPIRE k s · PEXPIRE k ms · TTL · PTTL · PERSIST
{C.CYAN}lists{C.RESET}     LPUSH/RPUSH k v... · LPOP/RPOP k [count] · LLEN · LRANGE k a b
{C.CYAN}hashes{C.RESET}    HSET k f v [f v...] · HGET · HDEL · HGETALL · HLEN
{C.CYAN}scan{C.RESET}      KEYS pattern · SCAN [cursor] [MATCH p] [COUNT n]  (cursor is sticky)
{C.CYAN}tx{C.RESET}        MULTI · EXEC · DISCARD · WATCH k... · UNWATCH
{C.CYAN}pubsub{C.RESET}    SUBSCRIBE ch... · PSUBSCRIBE p... · PUBLISH ch msg · PUBSUB CHANNELS
{C.CYAN}server{C.RESET}    INFO · CONFIG GET/SET maxkeys · COMPACT · DBSIZE · PING
""")

    def do_quit(self, arg):
        return True

    def do_exit(self, arg):
        return True

    def do_EOF(self, arg):
        print()
        return True

# ------------------------------------------------------------------- demo ----
def run_demo(cli: MiniRedisCLI):
    steps = [
        ["SET", "user:1", "alice"],
        ["SET", "user:1", "alice2", "EX", "30"],
        ["TTL", "user:1"],
        ["INCR", "hits"],
        ["INCRBY", "hits", "5"],
        ["SET", "mystr", "hello"],
        ["APPEND", "mystr", " world"],
        ["LPUSH", "queue", "a", "b", "c"],
        ["LRANGE", "queue", "0", "-1"],
        ["RPOP", "queue"],
        ["HSET", "profile", "name", "alice", "lang", "python"],
        ["HGETALL", "profile"],
        ["WATCH", "user:1"],
        ["MULTI"],
        ["SET", "user:1", "in-txn"],
        ["INCR", "hits"],
        ["EXEC"],
        ["GET", "user:1"],
        ["SCAN", "0", "MATCH", "*", "COUNT", "50"],
        ["INFO"],
    ]
    for parts in steps:
        print(f"{C.BOLD}{C.MAGENTA}> {' '.join(parts)}{C.RESET}")
        try:
            cli.send(parts)
        except (ConnectionError, OSError) as e:
            print(f"{C.RED}connection lost: {e}{C.RESET}")
            return

def main():
    ap = argparse.ArgumentParser(description="mini-redis REPL client")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=6379)
    ap.add_argument("--demo", action="store_true", help="run a scripted demo and exit")
    args = ap.parse_args()

    try:
        cli = MiniRedisCLI(args.host, args.port)
    except OSError as e:
        print(f"{C.RED}Could not connect to {args.host}:{args.port}: {e}{C.RESET}")
        sys.exit(1)

    if args.demo:
        run_demo(cli)
        return

    try:
        cli.cmdloop()
    except KeyboardInterrupt:
        print()

if __name__ == "__main__":
    main()
