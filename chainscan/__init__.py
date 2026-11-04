"""chainscan: bulk multi-chain balance scanner built from cooperating agents.

Agents
  IngestAgent      streams any-size input, extracts 0x and Bitcoin addresses, buckets + dedupes on disk
  ChainAgent       one per EVM chain; batches 20-byte wallets into deployless eth_calls
  BtcAgent         Bitcoin balances from a UTXO snapshot join (fast) or an Esplora API
  EndpointPool     adaptive per-RPC concurrency (AIMD), cooldown on rate limits, failover
  SupervisorAgent  live progress, ETA, progress.json, endpoint health
  WriterAgent      merges results into funded.csv / funded_wide.csv / summary.json
  AnalystAgent     optional: Claude on Amazon Bedrock writes a run report (never in the hot path)
"""
__version__ = "0.1.0"
