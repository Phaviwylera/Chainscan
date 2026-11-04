"""chainscan command line.

  python -m chainscan scan addresses.txt -c config.yaml --run-dir runs/oct
  python -m chainscan report runs/oct -c config.yaml        # AnalystAgent (Claude on Bedrock)

Re-running the same scan command resumes: finished buckets are skipped.
"""
import argparse
import asyncio
import os
import shutil
import time

import aiohttp

from . import config as cfgmod
from .analyst import AnalystAgent
from .btc import BtcAgent
from .evm import ChainAgent
from .ingest import IngestAgent
from .supervisor import SupervisorAgent
from .util import fmt_int, log
from .writer import WriterAgent


async def _scan_async(cfg, run_dir, ingest, want_btc, only):
    conn = aiohttp.TCPConnector(limit=0, ttl_dns_cache=300, enable_cleanup_closed=True)
    async with aiohttp.ClientSession(connector=conn) as session:
        agents = []
        evm_buckets = ingest.evm_buckets()
        for c in cfg.chains:
            if only and c.key not in only:
                continue
            agents.append(ChainAgent(c, run_dir, session))
        btc_agent = None
        if want_btc and (not only or "btc" in only):
            btc_agent = BtcAgent(cfg.btc, run_dir, session)
            agents.append(btc_agent)
        sup = SupervisorAgent(agents, run_dir, cfg.progress_every_s)
        sup_task = asyncio.create_task(sup.run())
        jobs = []
        for a in agents:
            if a is btc_agent:
                jobs.append(a.run(ingest.btc_buckets()))
            else:
                jobs.append(a.run(evm_buckets))
        results = await asyncio.gather(*jobs, return_exceptions=True)
        progress = sup.stop()
        await sup_task
        sup.print_line(progress)
        for a, r in zip(agents, results):
            if isinstance(r, BaseException):
                log(f"{a.key}: agent stopped with error: {r!r}")
        return progress


def cmd_scan(args):
    cfg = cfgmod.load(args.config)
    run_dir = args.run_dir
    if args.fresh and os.path.exists(run_dir):
        shutil.rmtree(run_dir)
    os.makedirs(run_dir, exist_ok=True)
    t0 = time.time()
    want_btc = cfg.btc.enabled
    ingest = IngestAgent(cfg, run_dir)
    first_col = args.first_column or cfg.first_column
    meta = ingest.run(args.inputs, want_btc, first_col)
    only = set(args.only.split(",")) if args.only else None
    progress = asyncio.run(_scan_async(cfg, run_dir, ingest, want_btc, only))
    summary = WriterAgent(cfg, run_dir).run(meta, progress)
    el = time.time() - t0
    n = meta["evm_unique"] + meta["btc_unique"]
    log(f"done: {fmt_int(n)} unique addresses in {el:.1f}s ({fmt_int(int(n / max(el, 1e-6)))} addresses/s overall)")
    if cfg.analyst.enabled:
        try:
            AnalystAgent(cfg.analyst, run_dir).run()
        except Exception as e:  # noqa: BLE001
            log(f"analyst: skipped ({e})")
    return summary


def cmd_report(args):
    cfg = cfgmod.load(args.config)
    AnalystAgent(cfg.analyst, args.run_dir).run()


def main():
    p = argparse.ArgumentParser(prog="chainscan", description="Bulk multi-chain balance scanner")
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("scan", help="scan one or more address files")
    s.add_argument("inputs", nargs="+", help="text/CSV files of any size containing addresses")
    s.add_argument("-c", "--config", default="config.yaml")
    s.add_argument("--run-dir", default="runs/latest")
    s.add_argument("--fresh", action="store_true", help="discard checkpoints in run-dir and start over")
    s.add_argument("--only", help="comma list of chain keys to scan, e.g. eth,btc")
    s.add_argument("--first-column", action="store_true",
                   help="use only the first value on each line (lines start with the address; the rest is ignored)")
    s.set_defaults(fn=cmd_scan)
    r = sub.add_parser("report", help="AnalystAgent: Claude on Bedrock writes report.md for a finished run")
    r.add_argument("run_dir")
    r.add_argument("-c", "--config", default="config.yaml")
    r.set_defaults(fn=cmd_report)
    w = sub.add_parser("web", help="run the web UI + job runner (Render, Docker, any server)")
    w.set_defaults(fn=lambda a: __import__("chainscan.web", fromlist=["main"]).main())
    args = p.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
