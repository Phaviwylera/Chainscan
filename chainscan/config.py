"""Config loading. Everything has a default except RPC URLs, which you supply."""
import os
from dataclasses import dataclass, field
from typing import List, Optional

import yaml


@dataclass
class Endpoint:
    url: str
    max_concurrency: int = 16
    headers: dict = field(default_factory=dict)


@dataclass
class Asset:
    symbol: str
    decimals: int
    address: Optional[str] = None  # None = native coin

    @property
    def native(self) -> bool:
        return self.address is None


@dataclass
class ChainCfg:
    key: str
    name: str
    endpoints: List[Endpoint]
    assets: List[Asset]
    gas_budget: int = 40_000_000      # gas one eth_call may use; most nodes cap at 50M
    gas_per_asset: int = 4_500        # rough cost of one balance lookup, used to size batches
    max_addresses_per_call: int = 3_000
    min_addresses_per_call: int = 25  # below this a failing batch is recorded as failed
    call_gas: Optional[int] = None    # pass an explicit gas value in eth_call (some providers need it)
    timeout_s: float = 60.0
    explorer: str = ""

    def addresses_per_call(self) -> int:
        per = self.gas_per_asset * max(1, len(self.assets)) + 400
        return max(self.min_addresses_per_call, min(self.max_addresses_per_call, self.gas_budget // per))


@dataclass
class BtcCfg:
    enabled: bool = False
    mode: str = "snapshot"            # snapshot | esplora
    snapshot_path: str = ""
    snapshot_address_column: str = "address"
    snapshot_amount_column: str = "amount"
    snapshot_amount_unit: str = "sats"  # sats | btc
    esplora: List[Endpoint] = field(default_factory=list)
    timeout_s: float = 30.0


@dataclass
class AnalystCfg:
    enabled: bool = False
    region: str = "ap-south-1"
    model_id: str = ""
    max_tokens: int = 2500


@dataclass
class Config:
    chains: List[ChainCfg]
    btc: BtcCfg
    analyst: AnalystCfg
    ingest_workers: int = 0           # 0 = one per CPU
    ingest_chunk_mb: int = 64
    btc_buckets: int = 64
    progress_every_s: float = 2.0


def _endpoints(lst) -> List[Endpoint]:
    out = []
    for e in lst or []:
        if isinstance(e, str):
            e = {"url": e}
        out.append(Endpoint(url=e["url"], max_concurrency=int(e.get("max_concurrency", 16)),
                            headers=e.get("headers") or {}))
    return out


def _endpoints_env(lst):
    """Expand ${ENV_VARS} in RPC URLs and drop entries whose variable is not set."""
    out = []
    for e in lst or []:
        e = {"url": e} if isinstance(e, str) else dict(e)
        url = os.path.expandvars(str(e.get("url", "")))
        if not url or "${" in url or url.startswith("$"):
            continue
        e["url"] = url
        out.append(e)
    return out


def load(path: str) -> Config:
    with open(path) as f:
        raw = yaml.safe_load(f) or {}
    for c in (raw.get("chains") or {}).values():
        c["rpcs"] = _endpoints_env(c.get("rpcs"))
    if raw.get("btc"):
        raw["btc"]["esplora"] = _endpoints_env(raw["btc"].get("esplora"))
        if raw["btc"].get("snapshot_path"):
            raw["btc"]["snapshot_path"] = os.path.expandvars(raw["btc"]["snapshot_path"])
    a = raw.get("analyst") or {}
    for k in ("model_id", "region"):
        if isinstance(a.get(k), str):
            v = os.path.expandvars(a[k])
            a[k] = "" if "${" in v else v
    if not a.get("region"):
        a.pop("region", None)
    if os.environ.get("BEDROCK_MODEL_ID") and a.get("model_id"):
        a["enabled"] = True
    chains = []
    for key, c in (raw.get("chains") or {}).items():
        if c.get("enabled", True) is False:
            continue
        assets = [Asset(symbol=a["symbol"], decimals=int(a.get("decimals", 18)), address=a.get("address"))
                  for a in c.get("assets", [])]
        eps = _endpoints(c.get("rpcs"))
        if not eps:
            raise SystemExit(f"config: chain '{key}' has no rpcs")
        if not assets:
            raise SystemExit(f"config: chain '{key}' has no assets")
        chains.append(ChainCfg(
            key=key, name=c.get("name", key), endpoints=eps, assets=assets,
            gas_budget=int(c.get("gas_budget", 40_000_000)),
            gas_per_asset=int(c.get("gas_per_asset", 4_500)),
            max_addresses_per_call=int(c.get("max_addresses_per_call", 3_000)),
            min_addresses_per_call=int(c.get("min_addresses_per_call", 25)),
            call_gas=c.get("call_gas"), timeout_s=float(c.get("timeout_s", 60)),
            explorer=c.get("explorer", "")))
    b = raw.get("btc") or {}
    btc = BtcCfg(enabled=bool(b.get("enabled", False)), mode=b.get("mode", "snapshot"),
                 snapshot_path=b.get("snapshot_path", ""),
                 snapshot_address_column=b.get("snapshot_address_column", "address"),
                 snapshot_amount_column=b.get("snapshot_amount_column", "amount"),
                 snapshot_amount_unit=b.get("snapshot_amount_unit", "sats"),
                 esplora=_endpoints(b.get("esplora")), timeout_s=float(b.get("timeout_s", 30)))
    a = raw.get("analyst") or {}
    analyst = AnalystCfg(enabled=bool(a.get("enabled", False)), region=a.get("region", "ap-south-1"),
                         model_id=a.get("model_id", ""), max_tokens=int(a.get("max_tokens", 2500)))
    ing = raw.get("ingest") or {}
    return Config(chains=chains, btc=btc, analyst=analyst,
                  ingest_workers=int(os.environ.get("CHAINSCAN_INGEST_WORKERS") or ing.get("workers", 0)),
                  ingest_chunk_mb=int(os.environ.get("CHAINSCAN_CHUNK_MB") or ing.get("chunk_mb", 64)),
                  btc_buckets=int(ing.get("btc_buckets", 64)),
                  progress_every_s=float(raw.get("progress_every_s", 2.0)))
