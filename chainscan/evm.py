"""ChainAgent: scans every unique 0x address on one EVM chain.

One eth_call (no `to`) runs BalanceScanner's constructor over up to a few thousand wallets and
every configured asset, and returns only non-zero balances. Requests carry 20 bytes per wallet.
Batches that run out of gas or are too large are halved automatically, and the agent learns a
smaller batch size for the rest of the run. Each 1/256th bucket is checkpointed when it finishes.
"""
import asyncio
import json
import os
import time

import aiohttp

from .pool import EndpointPool
from .scanner_bytecode import SCANNER_INITCODE
from .util import fmt_units, log, touch

SPLIT_HINTS = ("gas", "out of", "too large", "exceed", "size", "limit exceeded", "body", "timeout",
               "timed out", "deadline", "payload")
RATE_HINTS = ("rate", "too many", "429", "throttl", "capacity", "compute units", "quota")
STATE_HINTS = ("missing trie node", "header not found", "state", "pruned", "unknown block", "not available")


class SplitNeeded(Exception):
    pass


class RpcFailed(Exception):
    pass


def _bidx(name):
    try:
        return int(name.split("_b")[-1])
    except ValueError:
        return None


def _w32(n: int) -> bytes:
    return n.to_bytes(32, "big")


class ChainAgent:
    def __init__(self, ccfg, run_dir, session: aiohttp.ClientSession):
        self.c = ccfg
        self.key = ccfg.key
        self.session = session
        self.pool = EndpointPool(ccfg.endpoints)
        self.out_dir = os.path.join(run_dir, "results", self.key)
        self.fail_path = os.path.join(run_dir, "failed", f"{self.key}.txt")
        os.makedirs(self.out_dir, exist_ok=True)
        os.makedirs(os.path.dirname(self.fail_path), exist_ok=True)
        toks = [bytes(12) + (bytes.fromhex(a.address[2:]) if a.address else bytes(20)) for a in ccfg.assets]
        nt = len(toks)
        # abi.encode(address[] tokens, bytes packed): head, tokens array, then the packed bytes
        self.prefix = SCANNER_INITCODE + _w32(0x40) + _w32(0x40 + 32 + 32 * nt) + _w32(nt) + b"".join(toks)
        self.apc = ccfg.addresses_per_call()
        self.apc_max = self.apc
        self.ok_streak = 0
        self.block = None
        self.blocks_used = []
        self.last_repin = 0.0
        self.req_id = 0
        # counters read by the supervisor
        self.total = 0
        self.done = 0
        self.funded_entries = 0
        self.failed = 0
        self.calls = 0
        self.splits = 0
        self.repins = 0
        self.started = None
        self.finished = None
        self.status = "waiting"
        self.buckets_total = 0
        self.buckets_done = 0
        # per-bucket state for the live view: . empty, 0 waiting, 1 scanning, 2 done
        self.grid = bytearray(b"." * 256)
        self.grid_funded = [0] * 256

    # ---------- rpc ----------
    async def _post(self, ep, method, params):
        self.req_id += 1
        body = {"jsonrpc": "2.0", "id": self.req_id, "method": method, "params": params}
        headers = {"content-type": "application/json", **ep.headers}
        async with self.session.post(ep.url, json=body, headers=headers,
                                     timeout=aiohttp.ClientTimeout(total=self.c.timeout_s)) as r:
            text = await r.text()
            if r.status == 429:
                raise RpcFailed("429 rate limited")
            if r.status == 413:
                raise SplitNeeded("413 payload too large")
            if r.status >= 400:
                raise RpcFailed(f"HTTP {r.status}: {text[:200]}")
        j = json.loads(text)
        if isinstance(j, list):
            j = j[0]
        if j.get("error"):
            raise RpcFailed(str(j["error"].get("message", j["error"]))[:300])
        return j["result"]

    async def _simple(self, method, params):
        last = None
        for _ in range(len(self.pool.eps) * 3):
            ep = await self.pool.acquire()
            t0 = time.time()
            try:
                res = await self._post(ep, method, params)
                await self.pool.release(ep, "ok", time.time() - t0)
                return res
            except Exception as e:  # noqa: BLE001
                last = e
                await self.pool.release(ep, "error")
                await asyncio.sleep(0.3)
        raise RpcFailed(f"{method} failed on every endpoint: {last}")

    async def pin_block(self):
        bn = int(await self._simple("eth_blockNumber", []), 16)
        self.block = max(0, bn - 2)
        self.blocks_used.append(self.block)
        self.last_repin = time.time()

    async def _scan_call(self, chunk: bytes):
        n = len(chunk) // 20
        pad = (-len(chunk)) % 32
        data = self.prefix + _w32(len(chunk)) + chunk + bytes(pad)
        tx = {"data": "0x" + data.hex()}
        if self.c.call_gas:
            tx["gas"] = hex(int(self.c.call_gas))
        last = None
        hard = rl = 0
        while hard < 6 and rl < 60:
            ep = await self.pool.acquire()
            t0 = time.time()
            outcome = "error"
            try:
                res = await self._post(ep, "eth_call", [tx, hex(self.block)])
                outcome = "ok"
                self.calls += 1
                return self._decode(res, n)
            except SplitNeeded:
                outcome = "neutral"
                raise
            except asyncio.TimeoutError:
                outcome = "neutral"
                raise SplitNeeded("timeout")
            except (RpcFailed, aiohttp.ClientError, json.JSONDecodeError, ValueError) as e:
                msg = str(e).lower()
                last = e
                if any(h in msg for h in RATE_HINTS):
                    outcome = "rate_limited"
                    rl += 1
                elif any(h in msg for h in STATE_HINTS):
                    outcome = "neutral"
                    if time.time() - self.last_repin > 10:
                        self.repins += 1
                        log(f"{self.key}: node lost block {self.block} state, moving to latest")
                        try:
                            await self.pool.release(ep, outcome)
                            outcome = None
                            await self.pin_block()
                        except Exception:  # noqa: BLE001
                            pass
                elif any(h in msg for h in SPLIT_HINTS) and n > 1:
                    outcome = "neutral"
                    raise SplitNeeded(msg)
                else:
                    hard += 1
            finally:
                if outcome is not None:
                    await self.pool.release(ep, outcome, time.time() - t0)
            await asyncio.sleep(min(2.0, 0.1 * (hard + 1) + 0.05 * rl))
        if rl >= 60:
            raise RpcFailed(f"still rate limited after {rl} retries")
        raise SplitNeeded(f"gave up after retries: {last}")

    def _decode(self, res_hex: str, n: int):
        b = bytes.fromhex(res_hex[2:] if res_hex.startswith("0x") else res_hex)
        if len(b) % 37:
            raise ValueError(f"bad scanner output length {len(b)}")
        out = []
        for p in range(0, len(b), 37):
            idx = int.from_bytes(b[p:p + 4], "big")
            t = b[p + 4]
            if idx >= n or (t & 0x7F) >= len(self.c.assets):
                raise ValueError("scanner output out of range")
            out.append((idx, t & 0x7F, bool(t & 0x80), int.from_bytes(b[p + 5:p + 37], "big")))
        return out

    # ---------- batching ----------
    async def _process(self, st, chunk: bytes):
        n = len(chunk) // 20
        if n > self.apc_max:  # queued before the agent learned a smaller safe size: re-cut it
            k = self.apc * 20
            for i in range(0, len(chunk), k):
                await self._process(st, chunk[i:i + k])
            return
        try:
            entries = await self._scan_call(chunk)
        except RpcFailed:  # throttled for too long: keep the batch for a retry run, don't split
            st["failed"].append(chunk)
            self.failed += n
            self.done += n
            return
        except SplitNeeded:
            if n <= self.c.min_addresses_per_call:
                st["failed"].append(chunk)
                self.failed += n
                self.done += n
                return
            self.splits += 1
            self.ok_streak = 0
            # remember that this size is too big so batches never grow back past it
            self.apc_max = max(self.c.min_addresses_per_call, min(self.apc_max, int(n * 0.8)))
            self.apc = max(self.c.min_addresses_per_call, min(self.apc, n // 2, self.apc_max))
            h = (n // 2) * 20
            await self._process(st, chunk[:h])
            await self._process(st, chunk[h:])
            return
        self.ok_streak += 1
        if self.ok_streak >= 40 and self.apc < self.apc_max:
            self.apc = min(self.apc_max, int(self.apc * 1.15) + 1)
            self.ok_streak = 0
        bad = set()
        for idx, t, failed, bal in entries:
            addr = chunk[idx * 20:(idx + 1) * 20]
            if failed:
                bad.add(idx)
                continue
            a = self.c.assets[t]
            st["rows"].append(f"0x{addr.hex()},{self.key},{a.symbol},{bal},{fmt_units(bal, a.decimals)}\n")
            self.funded_entries += 1
        for idx in bad:
            st["failed"].append(chunk[idx * 20:(idx + 1) * 20])
        self.failed += len(bad)
        self.done += n

    def _finish(self, st):
        b = st["bucket"]
        with open(os.path.join(self.out_dir, f"{b}.csv"), "w") as f:
            f.writelines(st["rows"])
        if st["failed"]:
            with open(self.fail_path, "a") as f:
                for ch in st["failed"]:
                    for i in range(0, len(ch), 20):
                        f.write("0x" + ch[i:i + 20].hex() + "\n")
        touch(os.path.join(self.out_dir, f"{b}.done"))
        self.buckets_done += 1
        i = _bidx(b)
        if i is not None:
            self.grid[i] = ord("2")
            self.grid_funded[i] = len(st["rows"])

    async def run(self, bucket_files):
        todo = []
        for p in bucket_files:
            b = os.path.basename(p).split(".")[0]
            size = os.path.getsize(p) // 20
            self.total += size
            self.buckets_total += 1
            i = _bidx(b)
            if os.path.exists(os.path.join(self.out_dir, f"{b}.done")):
                self.done += size
                self.buckets_done += 1
                if i is not None:
                    self.grid[i] = ord("2")
                    try:
                        with open(os.path.join(self.out_dir, f"{b}.csv")) as f:
                            self.grid_funded[i] = sum(1 for _ in f)
                    except OSError:
                        pass
            elif size:
                todo.append((b, p))
                if i is not None:
                    self.grid[i] = ord("0")
        self.started = time.time()
        if not todo:
            self.status = "done"
            self.finished = time.time()
            return
        self.status = "pinning block"
        await self.pin_block()
        log(f"{self.key}: {len(todo)} buckets to scan at block {self.block}, "
            f"{self.apc} addresses per call, {self.pool.capacity} max parallel calls")
        self.status = "scanning"
        workers = max(1, self.pool.capacity)
        q = asyncio.Queue(maxsize=workers * 2)

        async def producer():
            for b, p in todo:
                data = open(p, "rb").read()
                i = _bidx(b)
                if i is not None:
                    self.grid[i] = ord("1")
                st = {"bucket": b, "rows": [], "failed": [], "pending": 0, "produced": False}
                n = len(data) // 20
                i = 0
                while i < n:
                    k = self.apc
                    st["pending"] += 1
                    await q.put((st, data[i * 20:(i + k) * 20]))
                    i += k
                st["produced"] = True
                if st["pending"] == 0:
                    self._finish(st)
            for _ in range(workers):
                await q.put(None)

        async def worker():
            while True:
                item = await q.get()
                if item is None:
                    return
                st, chunk = item
                try:
                    await self._process(st, chunk)
                except Exception as e:  # noqa: BLE001  never let one batch kill the run
                    log(f"{self.key}: batch error {e!r}; recorded as failed")
                    st["failed"].append(chunk)
                    self.failed += len(chunk) // 20
                    self.done += len(chunk) // 20
                st["pending"] -= 1
                if st["produced"] and st["pending"] == 0:
                    self._finish(st)

        await asyncio.gather(producer(), *[worker() for _ in range(workers)])
        self.status = "done"
        self.finished = time.time()

    def snapshot(self):
        el = (self.finished or time.time()) - (self.started or time.time())
        return {"chain": self.key, "name": self.c.name, "status": self.status, "total": self.total,
                "done": self.done, "funded_entries": self.funded_entries, "failed": self.failed,
                "calls": self.calls, "splits": self.splits, "addresses_per_call": self.apc,
                "block": self.block, "blocks_used": self.blocks_used, "repins": self.repins,
                "buckets": f"{self.buckets_done}/{self.buckets_total}", "seconds": round(el, 1),
                "grid": self.grid.decode(), "grid_funded": self.grid_funded,
                "endpoints": self.pool.snapshot()}
