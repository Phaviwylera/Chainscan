"""SupervisorAgent: watches every agent, prints a live line per chain, writes progress.json.

progress.json is rewritten every couple of seconds, so a dashboard, a cron check or the
AnalystAgent can read the run's state while it is still going.
"""
import asyncio
import os
import time

from .util import fmt_int, fmt_secs, write_json_atomic


class SupervisorAgent:
    def __init__(self, agents, run_dir, every_s=2.0):
        self.agents = agents
        self.path = os.path.join(run_dir, "progress.json")
        self.every = every_s
        self.t0 = time.time()
        self._stop = asyncio.Event()
        self._last = {}

    def state(self):
        out = []
        for a in self.agents:
            s = a.snapshot()
            el = max(1e-6, s["seconds"])
            rate = s["done"] / el if el > 0.5 else 0
            # rate over the last interval reads better than the lifetime average
            prev = self._last.get(s["chain"])
            now = time.time()
            if prev:
                dt = now - prev[1]
                if dt > 0.2:
                    rate = (s["done"] - prev[0]) / dt
            self._last[s["chain"]] = (s["done"], now)
            left = s["total"] - s["done"]
            s["rate_per_s"] = round(rate)
            s["eta_s"] = round(left / rate) if rate > 0 and left > 0 else (0 if left <= 0 else None)
            s["pct"] = round(100 * s["done"] / s["total"], 2) if s["total"] else 100.0
            out.append(s)
        return {"elapsed_s": round(time.time() - self.t0, 1), "chains": out, "updated": time.time()}

    def print_line(self, st):
        for s in st["chains"]:
            eps = s.get("endpoints") or []
            ep_txt = " ".join(f"{e['endpoint'].split('.')[0]}:{e['inflight']}/{int(e['limit'])}"
                              + ("(parked)" if e["parked"] else "") for e in eps[:4])
            print(f"  {s['chain']:<5} {s['status']:<16} {fmt_int(s['done'])}/{fmt_int(s['total'])} "
                  f"{s['pct']:5.1f}%  {fmt_int(s['rate_per_s'])}/s  ETA {fmt_secs(s['eta_s'])}  "
                  f"funded {fmt_int(s['funded_entries'])}  failed {fmt_int(s['failed'])}  {ep_txt}", flush=True)

    async def run(self):
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.every)
            except asyncio.TimeoutError:
                pass
            st = self.state()
            write_json_atomic(self.path, st)
            if not self._stop.is_set():
                self.print_line(st)

    def stop(self):
        self._stop.set()
        st = self.state()
        write_json_atomic(self.path, st)
        return st
