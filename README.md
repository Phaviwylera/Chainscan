# chainscan

Bulk balance scanner for crores of wallet addresses across Ethereum, BNB Chain, any other EVM
chain, and Bitcoin. Point it at a text or CSV file of any size (10 GB is fine). It extracts every
address, removes duplicates and returns every non-zero coin and token balance.

```
python -m chainscan scan wallets.csv -c config.yaml --run-dir runs/nov
```

## Measured performance

| Test | Result |
|---|---|
| 3 crore addresses, 1.35 GB file, 4 assets, mock node at 0.5 s per batch, 160 parallel calls | **86 s end to end** (ingest 17 s, scan 66 s), 0 failures |
| 22.1 crore addresses, **10 GB file** | ingest + dedupe 2.5 min; scan ~6.5 min at ~6.9 lakh addresses/s; killed at 36% and resumed with exact results |
| Correctness: 2 local EVM chains, real ERC-20s, BTC snapshot, noisy CSV | 15,143 of 15,143 balances exact, 0 extra, 0 missing |
| Flaky provider: 15% rate limits plus an out-of-gas cap below the batch size | 10 lakh addresses, 0 failures; batch size learned automatically |

These ran on a 2-core cloud box. The mock node shows what the pipeline can push. On a real
provider, throughput is set by how many parallel `eth_call`s it allows and how fast its nodes run
them:

```
addresses/s per chain ≈ parallel calls ÷ seconds per call × addresses per call
                      ≈ 100 ÷ 0.8 s × 2,200  ≈ 2.7 lakh/s  →  3 crore in under 2 minutes
```

With a single free public RPC you'll get a small fraction of that. For crores, use a paid plan
(Alchemy, QuickNode, Chainstack, dRPC), several providers listed together, or your own node
(Reth/Erigon/Geth for ETH, a BSC node for BNB), which has no limits.

## How the agents work together

```
                 ┌──────────────── SupervisorAgent ────────────────┐
                 │ live progress, ETA, endpoint health, progress.json │
                 └──────────────────────┬───────────────────────────┘
 wallets.csv ──► IngestAgent ──► 256 deduped 0x buckets ──► ChainAgent eth ──┐
 (any size)      (1 process per    + 64 BTC buckets    ├──► ChainAgent bsc ──┼─► WriterAgent ─► funded.csv
                  CPU, streaming)                      ├──► ChainAgent ...  ──┤                 funded_wide.csv
                                                       └──► BtcAgent ─────────┘                 summary.json
                                     each ChainAgent ⇄ EndpointPool (adaptive per-RPC limits)
                                                                       AnalystAgent (Claude on Bedrock) ─► report.md
```

- **IngestAgent** splits the file into byte ranges, one worker process per CPU, and streams each
  range in 64 MB chunks, so memory stays flat for any file size. It pulls out `0x` addresses
  (stored as 20 raw bytes) and Bitcoin addresses (`1…`, `3…`, `bc1…`) from any text or CSV layout,
  buckets them, then dedupes each bucket.
- **ChainAgent** (one per EVM chain, all running at once) uses a **deployless scanner contract**
  (`contracts/BalanceScanner.sol`). One `eth_call` with no `to` runs the contract's constructor
  over about 2,200 wallets × every configured asset and returns **only the non-zero balances**.
  Requests carry 20 bytes per wallet instead of Multicall's ~192 bytes per lookup, and since
  most wallets in a big list are empty, responses are tiny. No contract is deployed and no gas
  is paid. Every chain in one scan reads one pinned block (2 behind the tip).
- **EndpointPool** spreads calls over every RPC you list. Each endpoint's concurrency grows while
  it is healthy and is cut back when it throttles (cut at most once per round trip, so one burst
  of 429s doesn't collapse it). An endpoint that keeps failing is parked for 30 s.
- **Self-healing batches.** If a batch runs out of gas or is too large, it's halved and retried, and
  the agent remembers the safe size for the rest of the run. Throttled batches are retried, never
  split. If a node drops the pinned block's state, the agent moves to the latest block and
  records both block numbers in `summary.json`.
- **Checkpoints.** Each finished bucket is marked done. Run the same command again after a crash
  or Ctrl-C and it carries on where it stopped; the ingest pass is reused too.
- **BtcAgent** has two modes:
  - `snapshot` (the fast path): joins your addresses against a UTXO-set dump from your own
    Bitcoin Core node, using DuckDB. No API calls, so crores take minutes.
    `bitcoin-cli dumptxoutset utxo.dat` → [bitcoin-utxo-dump](https://github.com/in3rsha/bitcoin-utxo-dump)
    `-f address,amount` → `utxo.csv`.
  - `esplora`: asks Esplora servers per address. Fine for lakhs. For crores, run your own
    electrs/mempool backend, because public APIs will throttle.
- **WriterAgent** merges everything into the output files below.
- **AnalystAgent** (optional) sends the finished `summary.json` and the largest balances to
  Claude on Amazon Bedrock and writes `report.md`: failures, endpoint behaviour, settings to
  change for the next run, and the biggest holders. It deliberately stays out of the per-address
  path. An LLM call per wallet would be thousands of times slower and far too expensive at
  3 crore, and balance lookups need exact deterministic code.

## Setup

```bash
pip install -r requirements.txt
cp config.example.yaml config.yaml      # add your RPC URLs and the assets you want
python -m chainscan scan wallets.csv -c config.yaml --run-dir runs/first
python -m chainscan report runs/first -c config.yaml   # optional, needs AWS credentials + analyst.model_id
```

Docker: `docker build -t chainscan . && docker run -v $PWD:/work chainscan scan /work/wallets.csv -c /work/config.yaml --run-dir /work/runs/first`

Options: `--only eth,btc` scans just those chains; `--fresh` throws away the checkpoints;
`--first-column` uses only the address at the start of each line and ignores the rest of the line
(the website has this on by default). Without it, addresses are picked up anywhere in the text.

## Output (in the run directory)

| File | What it holds |
|---|---|
| `funded.csv` | `address,chain,asset,balance_raw,balance` for every non-zero balance (exact integers) |
| `funded_wide.csv` | one row per funded address, one column per `chain:asset` |
| `summary.json` | counts, totals per asset, duplicates, malformed rows, blocks, timings, endpoint stats |
| `progress.json` | live state, rewritten every few seconds while scanning |
| `failed/<chain>.txt` | addresses whose lookup failed; pass this file back in as input to retry |
| `report.md` | AnalystAgent's run report, when enabled |

## Tuning

- **Faster:** add endpoints, or raise `max_concurrency` on a paid one. The pool finds each
  endpoint's real limit by itself. Watch the `endpoint:inflight/limit` column in the live output.
- **Out-of-gas errors from your provider:** lower `gas_budget` (default 40M per call). Batches
  are sized as `gas_budget ÷ (gas_per_asset × assets)`, so each extra asset makes batches smaller.
- **More tokens:** add them under `assets`. Cost grows with wallets × assets. This checks the
  tokens you list; finding *every* token a wallet holds needs an indexer instead.
- **Machine:** ingest scales with CPU cores (about 100 MB/s on 2 cores). Scanning mostly waits on
  the network, so 2–4 cores and 4 GB RAM is plenty. Scratch disk needed ≈ 0.5 × input size.

## Limits to know

- Mixed-case `0x` addresses aren't EIP-55 checksum-verified during bulk ingest (checking 3 crore
  keccak hashes would dominate ingest time). Every well-formed 40-hex address is scanned.
- Bitcoin addresses are format-checked, not base58/bech32 checksum-checked. A malformed one just
  comes back with no balance (snapshot) or as failed (esplora).
- Snapshot BTC balances are as of your node's dump height. EVM balances are as of the pinned block.

## Tests

```bash
python tests/e2e.py --random 60000 --funded 3000 --btc 50000   # needs node + `npm i ganache@7.9.2 solc@0.8.24 js-sha3`
python tests/gen_big.py big.txt --n 30000000                   # 3 crore address file for load tests
python tests/mock_rpc.py --latency 0.5                         # fake node; add --rate-limit 0.15 --gas-cap-n 700 to stress
```

`contracts/` holds the scanner source. `tools/compile.js` recompiles it into `chainscan/scanner_bytecode.py`.
