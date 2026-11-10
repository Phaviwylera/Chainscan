"""Bitcoin balance source: turn an address/balance dump into a fast lookup table.

Blockchair publishes a daily dump of every Bitcoin address with a balance
(blockchair_bitcoin_addresses_and_balance_<date>.tsv, columns: address, balance in satoshis).
Any similar TSV/CSV works: the address column is the one named "address" (or the first text
column) and the balance column the one named "balance" (or the first numeric column after it).

The dump is converted once into Parquet (address -> sats), after which a BTC scan is a DuckDB
join that takes seconds for crores of addresses, with no API calls at all.

  python -m chainscan btc-source blockchair_....tsv /data/btc/snapshot.parquet
"""
import json
import os
import time

from .util import fmt_units, log, write_json_atomic


def _q(p):
    return p.replace("'", "''")


def build(src: str, out_parquet: str, meta_path: str = None) -> dict:
    import duckdb
    t0 = time.time()
    os.makedirs(os.path.dirname(out_parquet) or ".", exist_ok=True)
    tmpdir = os.path.join(os.path.dirname(out_parquet) or ".", "duckdb_tmp")
    os.makedirs(tmpdir, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"PRAGMA threads={os.cpu_count() or 2}")
    con.execute(f"PRAGMA temp_directory='{_q(tmpdir)}'")
    con.execute(f"PRAGMA memory_limit='{os.environ.get('CHAINSCAN_DUCKDB_MEMORY', '3GB')}'")
    cols = con.execute(f"DESCRIBE SELECT * FROM read_csv('{_q(src)}', header=true, auto_detect=true, "
                       "sample_size=20000)").fetchall()
    names = [c[0] for c in cols]
    types = {c[0]: c[1].upper() for c in cols}
    low = {n.lower(): n for n in names}
    acol = low.get("address") or next((n for n in names if "CHAR" in types[n] or "STRING" in types[n]), names[0])
    num = [n for n in names if n != acol and any(t in types[n] for t in ("INT", "DECIMAL", "DOUBLE", "FLOAT", "NUMERIC"))]
    bcol = low.get("balance") or (num[0] if num else names[1])
    log(f"btc source: {src} -> address column '{acol}', balance column '{bcol}' (satoshis)")
    tmp = out_parquet + ".tmp"
    con.execute(f"""
        COPY (
          SELECT CASE WHEN lower("{acol}") LIKE 'bc1%' THEN lower("{acol}") ELSE "{acol}" END AS address,
                 CAST(SUM(CAST("{bcol}" AS BIGINT)) AS BIGINT) AS sats
          FROM read_csv('{_q(src)}', header=true, auto_detect=true, sample_size=20000)
          WHERE "{acol}" IS NOT NULL
          GROUP BY 1
        ) TO '{_q(tmp)}' (FORMAT PARQUET, COMPRESSION ZSTD)""")
    os.replace(tmp, out_parquet)
    rows, funded, total = con.execute(
        f"SELECT count(*), count(*) FILTER (WHERE sats > 0), CAST(sum(CAST(sats AS HUGEINT)) AS VARCHAR) FROM read_parquet('{_q(out_parquet)}')").fetchone()
    meta = {"state": "ready", "source_name": os.path.basename(src), "rows": int(rows), "funded": int(funded),
            "total_btc": fmt_units(int(total or 0), 8), "address_column": acol, "balance_column": bcol,
            "parquet_bytes": os.path.getsize(out_parquet), "built_at": time.time(), "seconds": round(time.time() - t0, 1)}
    if meta_path:
        old = {}
        try:
            old = json.load(open(meta_path))
        except (OSError, ValueError):
            pass
        write_json_atomic(meta_path, {**old, **meta})
    log(f"btc source: {rows:,} addresses ({funded:,} with a balance, {meta['total_btc']} BTC) in {meta['seconds']}s")
    return meta
