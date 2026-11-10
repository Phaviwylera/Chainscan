"""WriterAgent: merges per-bucket results into the files you actually open.

funded.csv       one row per (address, chain, asset) with a non-zero balance
funded_wide.csv  one row per address, one column per chain:asset
summary.json     totals, counts, blocks, timings, failures, endpoint stats
failed/*.txt     addresses whose lookup failed; feed them back in as an input file to retry
"""
import glob
import os

from .util import fmt_int, fmt_units, log, write_json_atomic

COLS = ("{'address':'VARCHAR','chain':'VARCHAR','asset':'VARCHAR',"
        "'balance_raw':'VARCHAR','balance':'VARCHAR'}")


class WriterAgent:
    def __init__(self, cfg, run_dir):
        self.cfg = cfg
        self.dir = run_dir

    def run(self, ingest_meta, progress):
        funded = os.path.join(self.dir, "funded.csv")
        decimals = {(c.key, a.symbol): a.decimals for c in self.cfg.chains for a in c.assets}
        decimals[("btc", "BTC")] = 8
        # stream the per-bucket files into funded.csv; nothing is held in memory, so crores of
        # funded rows (e.g. a whole-chain dump) are fine
        with open(funded, "w") as out:
            out.write("address,chain,asset,balance_raw,balance\n")
            for p in sorted(glob.glob(os.path.join(self.dir, "results", "*", "*.csv"))):
                with open(p) as f:
                    for line in f:
                        if line.count(",") == 4:
                            out.write(line)
        con = self._db()
        src = funded.replace("'", "''")
        rows = con.execute(f"""SELECT chain, asset, count(*), CAST(sum(CAST(balance_raw AS HUGEINT)) AS VARCHAR)
                               FROM read_csv('{src}', header=true, columns={COLS}) GROUP BY 1, 2 ORDER BY 1, 2""").fetchall()
        wallets = dict(con.execute(f"""SELECT chain, count(DISTINCT address)
                                       FROM read_csv('{src}', header=true, columns={COLS}) GROUP BY 1""").fetchall())
        assets = [{"chain": ch, "asset": sym, "funded_addresses": int(n),
                   "total": fmt_units(int(tot), decimals.get((ch, sym), 18))} for ch, sym, n, tot in rows]
        self._wide(con, funded, sum(r[2] for r in rows))
        failed = {}
        for p in glob.glob(os.path.join(self.dir, "failed", "*.txt")):
            with open(p) as f:
                failed[os.path.basename(p)[:-4]] = sum(1 for _ in f)
        summary = {"ingest": ingest_meta, "assets": assets,
                   "funded_wallets_per_chain": {k: int(v) for k, v in wallets.items()},
                   "failed_lookups": failed, "progress": progress,
                   "files": {"funded": "funded.csv", "funded_wide": "funded_wide.csv", "failed": "failed/"}}
        write_json_atomic(os.path.join(self.dir, "summary.json"), summary)
        log("results:")
        for a in assets:
            log(f"  {a['chain']:<5} {a['asset']:<6} {fmt_int(a['funded_addresses']):>12} funded   total {a['total']}")
        if failed:
            log(f"  failed lookups: {failed}  (rerun with failed/<chain>.txt as input to retry)")
        log(f"wrote {funded}")
        return summary

    def _db(self):
        import duckdb
        con = duckdb.connect()
        tmpd = os.path.join(self.dir, "duckdb_tmp")
        os.makedirs(tmpd, exist_ok=True)
        con.execute(f"PRAGMA temp_directory='{tmpd}'")
        con.execute(f"PRAGMA memory_limit='{os.environ.get('CHAINSCAN_DUCKDB_MEMORY', '3GB')}'")
        return con

    def _wide(self, con, funded, n):
        dst = os.path.join(self.dir, "funded_wide.csv")
        if not n:
            open(dst, "w").write("address\n")
            return
        src = funded.replace("'", "''")
        con.execute(f"""COPY (
            PIVOT (SELECT address, chain || ':' || asset AS k, balance
                   FROM read_csv('{src}', header=true, columns={COLS}))
            ON k USING first(balance) GROUP BY address ORDER BY address
        ) TO '{dst.replace("'", "''")}' (HEADER true)""")
