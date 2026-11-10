"""BtcAgent: Bitcoin balances for every unique Bitcoin address in the input.

mode: snapshot  (the fast path, minutes for crores of addresses)
    Join the input against a UTXO-set dump you export from your own Bitcoin Core node
    (`bitcoin-cli dumptxoutset` + bitcoin-utxo-dump, or any CSV with address + amount columns).
    DuckDB does the join on disk; no network calls at all.

mode: esplora   (no node needed, slower)
    GET {base}/address/{addr} against one or more Esplora servers (self-hosted electrs/mempool,
    Blockstream, or a paid provider). Uses the same adaptive EndpointPool as the EVM agents.
"""
import asyncio
import json
import os
import time

import aiohttp

from .pool import EndpointPool
from .util import fmt_units, log, touch


class BtcAgent:
    def __init__(self, bcfg, run_dir, session=None):
        self.c = bcfg
        self.key = "btc"
        self.session = session
        self.out_dir = os.path.join(run_dir, "results", "btc")
        self.fail_path = os.path.join(run_dir, "failed", "btc.txt")
        os.makedirs(self.out_dir, exist_ok=True)
        os.makedirs(os.path.dirname(self.fail_path), exist_ok=True)
        self.pool = EndpointPool(bcfg.esplora) if bcfg.mode == "esplora" and bcfg.esplora else None
        self.total = self.done = self.funded_entries = self.failed = self.calls = 0
        self.started = self.finished = None
        self.status = "waiting"
        self.grid = None
        self.grid_funded = None

    async def run(self, bucket_files):
        self.started = time.time()
        for p in bucket_files:
            with open(p, "rb") as f:
                self.total += sum(1 for line in f if line.strip())
        if self.c.mode == "snapshot":
            await asyncio.to_thread(self._snapshot, bucket_files)
        elif self.c.mode == "esplora":
            if not self.pool:
                raise SystemExit("btc.mode is esplora but no btc.esplora URLs are configured")
            await self._esplora(bucket_files)
        else:
            raise SystemExit(f"unknown btc.mode {self.c.mode}")
        self.status = "done"
        self.finished = time.time()

    # ---------- snapshot join ----------
    def _snapshot(self, bucket_files):
        import duckdb
        done_mark = os.path.join(self.out_dir, "snapshot.done")
        if os.path.exists(done_mark):
            self.done = self.total
            return
        if not self.c.snapshot_path or not os.path.exists(self.c.snapshot_path):
            raise SystemExit(f"btc snapshot file not found: {self.c.snapshot_path!r}")
        self.status = "joining snapshot"
        log(f"btc: joining {self.total:,} addresses against {self.c.snapshot_path}")
        con = duckdb.connect()
        con.execute("PRAGMA threads=%d" % (os.cpu_count() or 2))
        con.execute(f"PRAGMA memory_limit='{os.environ.get('CHAINSCAN_DUCKDB_MEMORY', '3GB')}'")
        tmpd = os.path.join(self.out_dir, "duckdb_tmp")
        os.makedirs(tmpd, exist_ok=True)
        con.execute(f"PRAGMA temp_directory='{tmpd}'")
        files = [p.replace("'", "''") for p in bucket_files]
        lst = "[" + ",".join(f"'{p}'" for p in files) + "]"
        con.execute(f"CREATE TABLE inp AS SELECT DISTINCT column0 AS address FROM read_csv({lst}, "
                    "header=false, columns={'column0':'VARCHAR'})")
        acol, mcol = self.c.snapshot_address_column, self.c.snapshot_amount_column
        mult = "1" if self.c.snapshot_amount_unit == "sats" else "100000000"
        snap = self.c.snapshot_path.replace("'", "''")
        out = os.path.join(self.out_dir, "snapshot.csv").replace("'", "''")
        if self.c.snapshot_path.endswith(".parquet"):   # built by btcsource.build: address, sats
            source = f"SELECT address, CAST(sats AS BIGINT) AS sats FROM read_parquet('{snap}')"
        else:                                           # raw CSV/TSV dump: aggregate on the fly
            source = f"""SELECT CASE WHEN lower("{acol}") LIKE 'bc1%' THEN lower("{acol}") ELSE "{acol}" END AS address,
                       SUM(CAST(ROUND(CAST("{mcol}" AS DOUBLE) * {mult}) AS HUGEINT)) AS sats
                FROM read_csv('{snap}', header=true, auto_detect=true) GROUP BY 1"""
        con.execute(f"""
            COPY (
              WITH s AS ({source})
              SELECT i.address, 'btc' AS chain, 'BTC' AS asset, CAST(s.sats AS VARCHAR) AS raw,
                     CAST(s.sats // 100000000 AS VARCHAR) ||
                     CASE WHEN s.sats % 100000000 = 0 THEN ''
                          ELSE '.' || rtrim(lpad(CAST(s.sats % 100000000 AS VARCHAR), 8, '0'), '0') END AS balance
              FROM inp i JOIN s USING (address) WHERE s.sats > 0
            ) TO '{out}' (HEADER false, QUOTE '')""")
        # streamed by DuckDB on disk, so even crores of matches never sit in Python memory
        self.funded_entries = con.execute(
            f"SELECT count(*) FROM read_csv('{out}', header=false, columns={{'a':'VARCHAR','c':'VARCHAR',"
            "'s':'VARCHAR','r':'VARCHAR','b':'VARCHAR'})").fetchone()[0]
        self.done = self.total
        touch(done_mark)

    # ---------- esplora ----------
    async def _get_balance(self, addr: str):
        last = None
        for attempt in range(6):
            ep = await self.pool.acquire()
            t0 = time.time()
            outcome = "error"
            try:
                url = ep.url.rstrip("/") + "/address/" + addr
                async with self.session.get(url, headers=ep.headers,
                                            timeout=aiohttp.ClientTimeout(total=self.c.timeout_s)) as r:
                    if r.status == 429:
                        outcome = "rate_limited"
                        raise RuntimeError("429")
                    if r.status == 400:  # Esplora answers 400 for an invalid address
                        outcome = "ok"
                        return None
                    if r.status >= 400:
                        raise RuntimeError(f"HTTP {r.status}")
                    j = json.loads(await r.text())
                cs = j["chain_stats"]
                outcome = "ok"
                self.calls += 1
                return int(cs["funded_txo_sum"]) - int(cs["spent_txo_sum"])
            except Exception as e:  # noqa: BLE001
                last = e
            finally:
                await self.pool.release(ep, outcome, time.time() - t0)
            await asyncio.sleep(min(2.0, 0.2 * (attempt + 1)))
        raise RuntimeError(f"esplora failed: {last}")

    async def _esplora(self, bucket_files):
        self.status = "querying esplora"
        workers = max(1, self.pool.capacity)
        n = max([int(os.path.basename(p).split(".")[0].split("_b")[-1]) for p in bucket_files] + [63]) + 1
        self.grid = bytearray(b"." * n)
        self.grid_funded = [0] * n
        for p in bucket_files:
            self.grid[int(os.path.basename(p).split(".")[0].split("_b")[-1])] = ord("0")
        for p in bucket_files:
            b = os.path.basename(p).split(".")[0]
            bi = int(b.split("_b")[-1])
            addrs = [a.strip() for a in open(p) if a.strip()]
            if os.path.exists(os.path.join(self.out_dir, f"{b}.done")):
                self.done += len(addrs)
                self.grid[bi] = ord("2")
                continue
            self.grid[bi] = ord("1")
            rows, failed = [], []
            q = asyncio.Queue()
            for a in addrs:
                q.put_nowait(a)

            async def worker():
                while not q.empty():
                    a = q.get_nowait()
                    try:
                        bal = await self._get_balance(a)
                        if bal is None:
                            failed.append(a)
                            self.failed += 1
                        elif bal > 0:
                            rows.append(f"{a},btc,BTC,{bal},{fmt_units(bal, 8)}\n")
                            self.funded_entries += 1
                    except Exception:  # noqa: BLE001
                        failed.append(a)
                        self.failed += 1
                    self.done += 1

            await asyncio.gather(*[worker() for _ in range(min(workers, len(addrs)) or 1)])
            with open(os.path.join(self.out_dir, f"{b}.csv"), "w") as f:
                f.writelines(rows)
            if failed:
                with open(self.fail_path, "a") as f:
                    f.write("\n".join(failed) + "\n")
            touch(os.path.join(self.out_dir, f"{b}.done"))
            self.grid[bi] = ord("2")
            self.grid_funded[bi] = len(rows)

    def snapshot(self):
        el = (self.finished or time.time()) - (self.started or time.time())
        return {"chain": "btc", "name": "Bitcoin", "status": self.status, "mode": self.c.mode,
                "total": self.total, "done": self.done, "funded_entries": self.funded_entries,
                "failed": self.failed, "calls": self.calls, "seconds": round(el, 1),
                "grid": self.grid.decode() if self.grid else None, "grid_funded": self.grid_funded,
                "endpoints": self.pool.snapshot() if self.pool else []}
