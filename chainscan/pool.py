"""EndpointPool: shares work across several RPC/API endpoints and adapts to each one's limits.

Each endpoint has a concurrency limit that grows by +1 per window of successes and halves on a
rate limit or timeout (AIMD, the same idea TCP uses). Rate-limited endpoints cool down briefly.
An endpoint that keeps failing is parked for 30 s and then tried again. Work goes to the endpoint
with the most spare capacity, so a paid RPC carries most of the load while free ones help.
"""
import asyncio
import time


class EP:
    def __init__(self, cfg, idx):
        self.url = cfg.url
        self.headers = cfg.headers
        self.max = max(1, cfg.max_concurrency)
        self.limit = float(max(2, self.max // 4))
        self.inflight = 0
        self.ok = 0
        self.err = 0
        self.rate_limited = 0
        self.consec_err = 0
        self.cool_until = 0.0
        self.lat = 0.0
        self.idx = idx
        self.last_cut = 0.0
        self.recent_rl = []
        self.max_batch = 10 ** 9   # learned: largest batch this endpoint accepts (gas cap, body size, timeouts)
        self.park_level = 0

    def label(self):
        u = self.url.split("//")[-1].split("/")[0]
        return u[:32]

    def snapshot(self):
        return {"endpoint": self.label(), "limit": round(self.limit, 1), "max": self.max,
                "max_batch": self.max_batch if self.max_batch < 10 ** 9 else None,
                "inflight": self.inflight, "ok": self.ok, "errors": self.err,
                "rate_limited": self.rate_limited, "latency_ms": round(self.lat * 1000),
                "parked": self.cool_until > time.time()}


class EndpointPool:
    def __init__(self, endpoint_cfgs):
        self.eps = [EP(c, i) for i, c in enumerate(endpoint_cfgs)]
        self.cond = asyncio.Condition()

    @property
    def capacity(self):
        return sum(e.max for e in self.eps)

    async def acquire(self) -> EP:
        async with self.cond:
            while True:
                now = time.time()
                best, score = None, None
                lats = [e.lat for e in self.eps if e.ok and e.lat > 0]
                fastest = min(lats) if lats else 0
                for e in self.eps:
                    lim = int(e.limit)
                    # an endpoint that is many times slower than the best one only gets a single call,
                    # so it can't hold back the end of a scan
                    if fastest and e.lat > 4.0 and e.lat > 5 * fastest:
                        lim = min(lim, 1)
                    if e.cool_until > now or e.inflight >= lim:
                        continue
                    s = e.inflight / e.limit
                    if score is None or s < score:
                        best, score = e, s
                if best:
                    best.inflight += 1
                    return best
                try:
                    await asyncio.wait_for(self.cond.wait(), timeout=0.25)
                except asyncio.TimeoutError:
                    pass

    async def release(self, e: EP, outcome: str, latency: float = 0.0):
        """outcome: ok | rate_limited | error | neutral (request was fine but data needed a split)"""
        async with self.cond:
            e.inflight -= 1
            if outcome in ("ok", "neutral"):
                e.consec_err = 0
                if latency > 0:
                    e.lat = latency if e.lat == 0 else e.lat * 0.9 + latency * 0.1
                if outcome == "ok":
                    e.ok += 1
                    e.park_level = 0
                    e.limit = min(e.max, e.limit + 1.0 / e.limit)
            elif outcome == "rate_limited":
                # Many in-flight requests fail together when a provider throttles. Cut the limit
                # at most once per round trip, otherwise one burst collapses it to 1.
                now = time.time()
                e.rate_limited += 1
                if now - e.last_cut > max(0.5, e.lat * 1.5):
                    e.limit = max(1.0, e.limit * 0.6)
                    e.last_cut = now
                e.recent_rl = [t for t in e.recent_rl if now - t < 2.0] + [now]
                if len(e.recent_rl) >= max(8, int(e.limit)):  # sustained throttling: back off briefly
                    e.cool_until = now + 1.0
                    e.recent_rl = []
            else:
                now = time.time()
                e.err += 1
                e.consec_err += 1
                if now - e.last_cut > max(0.5, e.lat * 1.5):
                    e.limit = max(1.0, e.limit * 0.7)
                    e.last_cut = now
                if e.consec_err >= 8:  # keeps failing: park it, longer each time (30 s, 60 s, ... 10 min)
                    e.cool_until = now + min(600.0, 30.0 * 2 ** e.park_level)
                    e.park_level += 1
                    e.consec_err = 0
            self.cond.notify_all()

    def snapshot(self):
        return [e.snapshot() for e in self.eps]
