"""WriterAgent: merges per-bucket results into the files you actually open.

funded.csv       one row per (address, chain, asset) with a non-zero balance
funded_wide.csv  one row per address, one column per chain:asset
summary.json     totals, counts, blocks, timings, failures, endpoint stats
failed/*.txt     addresses whose lookup failed; feed them back in as an input file to retry
"""
import glob
import os

from .util import fmt_int, fmt_units, log, write_json_atomic


class WriterAgent:
    def __init__(self, cfg, run_dir):
        self.cfg = cfg
        self.dir = run_dir

    def run(self, ingest_meta, progress):
        funded = os.path.join(self.dir, "funded.csv")
        totals, counts, wallets = {}, {}, {}
        decimals = {(c.key, a.symbol): a.decimals for c in self.cfg.chains for a in c.assets}
        decimals[("btc", "BTC")] = 8
        with open(funded, "w") as out:
            out.write("address,chain,asset,balance_raw,balance\n")
            for p in sorted(glob.glob(os.path.join(self.dir, "results", "*", "*.csv"))):
                with open(p) as f:
                    for line in f:
                        parts = line.rstrip("\n").split(",")
                        if len(parts) != 5:
                            continue
                        out.write(line)
                        addr, chain, sym, raw, _ = parts
                        k = (chain, sym)
                        totals[k] = totals.get(k, 0) + int(raw)
                        counts[k] = counts.get(k, 0) + 1
                        wallets.setdefault(chain, set()).add(addr)
        self._wide(funded)
        assets = []
        for (chain, sym), raw in sorted(totals.items()):
            assets.append({"chain": chain, "asset": sym, "funded_addresses": counts[(chain, sym)],
                           "total": fmt_units(raw, decimals.get((chain, sym), 18))})
        failed = {}
        for p in glob.glob(os.path.join(self.dir, "failed", "*.txt")):
            with open(p) as f:
                failed[os.path.basename(p)[:-4]] = sum(1 for _ in f)
        summary = {"ingest": ingest_meta, "assets": assets,
                   "funded_wallets_per_chain": {k: len(v) for k, v in wallets.items()},
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

    def _wide(self, funded):
        try:
            import duckdb
        except ImportError:
            return
        con = duckdb.connect()
        src = funded.replace("'", "''")
        dst = os.path.join(self.dir, "funded_wide.csv").replace("'", "''")
        n = con.execute(f"SELECT count(*) FROM read_csv('{src}', header=true, "
                        "columns={'address':'VARCHAR','chain':'VARCHAR','asset':'VARCHAR',"
                        "'balance_raw':'VARCHAR','balance':'VARCHAR'})").fetchone()[0]
        if not n:
            open(os.path.join(self.dir, "funded_wide.csv"), "w").write("address\n")
            return
        con.execute(f"""COPY (
            PIVOT (SELECT address, chain || ':' || asset AS k, balance
                   FROM read_csv('{src}', header=true, columns={{'address':'VARCHAR','chain':'VARCHAR',
                   'asset':'VARCHAR','balance_raw':'VARCHAR','balance':'VARCHAR'}}))
            ON k USING first(balance) GROUP BY address ORDER BY address
        ) TO '{dst}' (HEADER true)""")
