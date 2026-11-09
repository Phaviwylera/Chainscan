"""EndpointPool: shares work across several RPC/API endpoints and adapts to each one's limits.

Two levels of adaptive limits (AIMD: +1 per window of successes, cut on throttling):
  * per endpoint, for that URL's own behaviour (latency, batch size, errors)
  * per provider ACCOUNT, shared by every chain agent. All Alchemy URLs (every chain, every key on
    the same account) draw from one budget, so 7 chains x 2 keys back off together instead of
    14 endpoints each hammering the same account limit and getting throttled to a standstill.
Endpoints that keep failing are paused briefly (at most 60 s) and retried.
"""
import asyncio
import os
import time

_COND = None             # one condition for every pool: a release on one chain can wake another
_PROVIDERS = {}


_COND_LOOP = None


def _cond():
    global _COND, _COND_LOOP
    loop = asyncio.get_running_loop()
    if _COND is None or _COND_LOOP is not loop:   # a new asyncio.run() gets fresh shared state
        _COND, _COND_LOOP = asyncio.Condition(), loop
        _PROVIDERS.clear()
    return _COND


def provider_of(url: str) -> str:
    """Group endpoints that share a rate limit: same provider domain (alchemy.com, publicnode.com...)."""
    host = url.split("//")[-1].split("/")[0].split(":")[0]
    if host.replace(".", "").isdigit() or host == "localhost":
        return host
    parts = host.split(".")
    return ".".join(parts[-2:])


class Provider:
    def __init__(self, name):
        self.name = name
        self.max = int(os.environ.get("CHAINSCAN_PROVIDER_MAX", "64"))
        self.limit = float(min(self.max, int(os.environ.get("CHAINSCAN_PROVIDER_START", "12"))))
        self.inflight = 0
        self.last_cut = 0.0
        self.ok = 0
        self.rate_limited = 0
        self.lat = 0.0


def _provider(url):
    name = provider_of(url)
    p = _PROVIDERS.get(name)
    if p is None:
        p = _PROVIDERS[name] = Provider(name)
    return p


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
        self.max_batch = 10 ** 9   # learned: largest batch this endpoint accepts (gas cap, body size, timeouts)
        self.park_level = 0
        self.prov = _provider(cfg.url)

    def label(self):
        u = self.url.split("//")[-1].split("/")[0]
        return u[:32]

    def snapshot(self):
        return {"endpoint": self.label(), "limit": round(self.limit, 1), "max": self.max,
                "max_batch": self.max_batch if self.max_batch < 10 ** 9 else None,
                "inflight": self.inflight, "ok": self.ok, "errors": self.err,
                "rate_limited": self.rate_limited, "latency_ms": round(self.lat * 1000),
                "parked": self.cool_until > time.time(),
                "provider": self.prov.name, "provider_limit": round(self.prov.limit, 1),
                "provider_inflight": self.prov.inflight}


def _cut(obj, factor, now, lat):
    # many in-flight calls fail together when a provider throttles: cut once per round trip
    if now - obj.last_cut > max(0.5, lat * 1.5):
        obj.limit = max(1.0, obj.limit * factor)
        obj.last_cut = now


class EndpointPool:
    def __init__(self, endpoint_cfgs):
        try:
            _cond()
        except RuntimeError:
            pass
        self.eps = [EP(c, i) for i, c in enumerate(endpoint_cfgs)]

    @property
    def cond(self):
        return _cond()

    @property
    def capacity(self):
        return sum(e.max for e in self.eps)

    async def acquire(self) -> EP:
        cond = _cond()
        async with cond:
            while True:
                now = time.time()
                best, score = None, None
                lats = [e.lat for e in self.eps if e.ok and e.lat > 0]
                fastest = min(lats) if lats else 0
                for e in self.eps:
                    lim = int(e.limit)
                    # an endpoint many times slower than the best one only gets a single call
                    if fastest and e.lat > 4.0 and e.lat > 5 * fastest:
                        lim = min(lim, 1)
                    if e.cool_until > now or e.inflight >= lim or e.prov.inflight >= int(e.prov.limit):
                        continue
                    s = max(e.inflight / e.limit, e.prov.inflight / e.prov.limit)
                    if score is None or s < score:
                        best, score = e, s
                if best:
                    best.inflight += 1
                    best.prov.inflight += 1
                    return best
                try:
                    await asyncio.wait_for(cond.wait(), timeout=0.25)
                except asyncio.TimeoutError:
                    pass

    async def release(self, e: EP, outcome: str, latency: float = 0.0):
        """outcome: ok | rate_limited | error | neutral (request was fine but data needed a split)"""
        cond = _cond()
        async with cond:
            e.inflight -= 1
            p = e.prov
            p.inflight -= 1
            now = time.time()
            if outcome in ("ok", "neutral"):
                e.consec_err = 0
                if latency > 0:
                    e.lat = latency if e.lat == 0 else e.lat * 0.9 + latency * 0.1
                    p.lat = latency if p.lat == 0 else p.lat * 0.95 + latency * 0.05
                if outcome == "ok":
                    e.ok += 1
                    p.ok += 1
                    e.park_level = 0
                    e.limit = min(e.max, e.limit + 1.0 / e.limit)
                    p.limit = min(p.max, p.limit + 1.0 / p.limit)
            elif outcome == "rate_limited":
                e.rate_limited += 1
                p.rate_limited += 1
                _cut(e, 0.7, now, e.lat)
                _cut(p, 0.7, now, p.lat)        # the whole account backs off, every chain together
            else:
                e.err += 1
                e.consec_err += 1
                _cut(e, 0.7, now, e.lat)
                if e.consec_err >= 8:  # keeps failing: pause 15 s, 30 s, 60 s at most, then retry
                    e.cool_until = now + min(60.0, 15.0 * 2 ** e.park_level)
                    e.park_level += 1
                    e.consec_err = 0
            cond.notify_all()

    def snapshot(self):
        return [e.snapshot() for e in self.eps]
