"""End-to-end test on two local EVM chains (ganache) + a synthetic Bitcoin UTXO snapshot.

  python tests/e2e.py --random 200000 --funded 3000

Deploys real ERC-20 contracts, funds a known set of wallets with native coin and tokens, mixes
them into a noisy input file (CSV junk, duplicates, mixed case, malformed hex, BTC addresses),
runs `chainscan scan`, and checks every reported balance against what was funded.
"""
import argparse
import csv
import json
import os
import random
import subprocess
import sys
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
TOOLS = os.environ.get("CHAINSCAN_TOOLS", "/home/claude/tools")
TOKEN = json.load(open(os.path.join(HERE, "TestToken.json")))
B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
B32 = "023456789acdefghjklmnpqrstuvwxyz"


def rpc(port, method, params):
    req = urllib.request.Request(f"http://127.0.0.1:{port}", data=json.dumps(
        {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode(),
        headers={"content-type": "application/json"})
    j = json.load(urllib.request.urlopen(req, timeout=120))
    if "error" in j:
        raise RuntimeError(j["error"])
    return j["result"]


def start_chain(port):
    p = subprocess.Popen(["node", os.path.join(TOOLS, "node_modules/ganache/dist/node/cli.js"),
                          "--server.port", str(port), "--wallet.totalAccounts", "1",
                          "--wallet.defaultBalance", "100000000", "--miner.callGasLimit", "0x2FAF080",
                          "--miner.blockGasLimit", "0x1C9C3800", "--logging.quiet", "--chain.vmErrorsOnRPCResponse"],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(120):
        try:
            rpc(port, "eth_blockNumber", [])
            return p
        except Exception:
            time.sleep(0.5)
    raise RuntimeError("ganache did not start")


def send(port, frm, data, to=None):
    tx = {"from": frm, "data": data, "gas": "0x1C9C380"}
    if to:
        tx["to"] = to
    h = rpc(port, "eth_sendTransaction", [tx])
    rc = rpc(port, "eth_getTransactionReceipt", [h])
    if rc["status"] != "0x1":
        raise RuntimeError("tx failed")
    return rc


def enc_mint(addrs, amts):
    sel = MINT_SEL
    n = len(addrs)
    w = lambda x: x.to_bytes(32, "big")
    head = w(0x40) + w(0x40 + 32 + 32 * n)
    a = w(n) + b"".join(bytes(12) + bytes.fromhex(x[2:]) for x in addrs)
    b = w(n) + b"".join(w(v) for v in amts)
    return "0x" + sel + (head + a + b).hex()


def rand_addr(rng):
    return "0x" + rng.randbytes(20).hex()


def rand_btc(rng):
    k = rng.random()
    if k < 0.4:
        return "1" + "".join(rng.choice(B58) for _ in range(33))
    if k < 0.7:
        return "3" + "".join(rng.choice(B58) for _ in range(33))
    return "bc1q" + "".join(rng.choice(B32) for _ in range(38))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--random", type=int, default=200000)
    ap.add_argument("--funded", type=int, default=3000)
    ap.add_argument("--btc", type=int, default=50000)
    ap.add_argument("--work", default="/home/claude/e2e")
    a = ap.parse_args()
    rng = random.Random(7)
    os.makedirs(a.work, exist_ok=True)

    global MINT_SEL  # keccak selector, computed with js-sha3 from the node toolchain
    MINT_SEL = subprocess.check_output(["node", "-e",
        "const k=require(process.argv[1]);console.log(k.keccak256('mintBatch(address[],uint256[])').slice(0,8))",
        os.path.join(TOOLS, "node_modules/js-sha3")]).decode().strip()

    chains = {"eth": 18545, "bsc": 18546}
    procs = [start_chain(p) for p in chains.values()]
    try:
        funded = [rand_addr(rng) for _ in range(a.funded)]
        expected = {}  # (addr, chain, asset) -> raw
        for chain, port in chains.items():
            frm = rpc(port, "eth_accounts", [])[0]
            toks = []
            for sym in ("USDT", "WBTC"):
                rc = send(port, frm, TOKEN["bytecode"])
                toks.append((sym, rc["contractAddress"]))
            # native balances for a third of the wallets, tokens for overlapping subsets
            native_sym = "ETH" if chain == "eth" else "BNB"
            for i, ad in enumerate(funded):
                if i % 3 == 0:
                    v = rng.randrange(1, 10 ** 21)
                    rpc(port, "evm_setAccountBalance", [ad, hex(v)])
                    expected[(ad, chain, native_sym)] = v
            for ti, (sym, taddr) in enumerate(toks):
                sel = [ad for i, ad in enumerate(funded) if (i + ti) % 2 == (0 if chain == "eth" else 1)]
                amts = [rng.randrange(1, 10 ** 24) for _ in sel]
                for k in range(0, len(sel), 400):
                    send(port, frm, enc_mint(sel[k:k + 400], amts[k:k + 400]), to=taddr)
                for ad, v in zip(sel, amts):
                    expected[(ad, chain, sym)] = v
            for _ in range(4):  # the scanner reads 2 blocks behind the tip
                rpc(port, "evm_mine", [])
            chains[chain] = (port, toks, native_sym)

        # BTC snapshot: some input addresses funded, plus unrelated rows; multiple UTXOs per address
        btc_in = [rand_btc(rng) for _ in range(a.btc)]
        snap = os.path.join(a.work, "utxo.csv")
        with open(snap, "w") as f:
            f.write("count,txid,vout,amount,type,address\n")
            for i, ad in enumerate(btc_in):
                if i % 7 == 0:
                    tot = 0
                    for u in range(1 + i % 3):
                        v = rng.randrange(546, 10 ** 9)
                        tot += v
                        f.write(f"{i},{rng.randbytes(32).hex()},{u},{v},p2pkh,{ad}\n")
                    expected[(ad, "btc", "BTC")] = tot
            for _ in range(a.btc * 2):
                f.write(f"0,{rng.randbytes(32).hex()},0,{rng.randrange(546, 10**9)},p2pkh,{rand_btc(rng)}\n")

        # noisy input: CSV with junk, duplicates, mixed case, malformed 0x
        inp = os.path.join(a.work, "input.csv")
        t0 = time.time()
        with open(inp, "w") as f:
            f.write("id,wallet,note\n")
            pool = funded + [rand_addr(rng) for _ in range(a.random)]
            for i, ad in enumerate(pool):
                s = ad.upper().replace("0X", "0x") if i % 11 == 0 else ad
                f.write(f"{i},{s},row {i}\n")
                if i % 50 == 0:
                    f.write(f"dup,{ad},again\n")
                if i % 997 == 0:
                    f.write(f"bad,0x{rng.randbytes(10).hex()},short\n")
            for ad in btc_in:
                f.write(f"btc,{ad},\n")
        print(f"input: {os.path.getsize(inp) / 1e6:.1f} MB, {len(pool):,} 0x + {len(btc_in):,} BTC, built in {time.time() - t0:.1f}s")

        cfg = os.path.join(a.work, "config.yaml")
        lines = ["chains:"]
        for chain, (port, toks, nsym) in chains.items():
            lines += [f"  {chain}:", f"    name: {chain}", "    rpcs:",
                      f"      - {{url: 'http://127.0.0.1:{port}', max_concurrency: 4}}",
                      "    assets:", f"      - {{symbol: {nsym}, decimals: 18}}"]
            for sym, taddr in toks:
                lines.append(f"      - {{symbol: {sym}, decimals: {6 if sym == 'USDT' else 8}, address: '{taddr}'}}")
        lines += ["btc:", "  enabled: true", "  mode: snapshot", f"  snapshot_path: {snap}",
                  "progress_every_s: 3"]
        open(cfg, "w").write("\n".join(lines) + "\n")

        run_dir = os.path.join(a.work, "run")
        t0 = time.time()
        subprocess.check_call([sys.executable, "-m", "chainscan", "scan", inp, "-c", cfg,
                               "--run-dir", run_dir, "--fresh"], cwd=ROOT)
        el = time.time() - t0

        got = {}
        with open(os.path.join(run_dir, "funded.csv")) as f:
            for r in csv.DictReader(f):
                got[(r["address"], r["chain"], r["asset"])] = int(r["balance_raw"])
        missing = [k for k in expected if k not in got]
        wrong = [k for k in expected if k in got and got[k] != expected[k]]
        extra = [k for k in got if k not in expected]
        summ = json.load(open(os.path.join(run_dir, "summary.json")))
        print(f"\nscan took {el:.1f}s; expected {len(expected):,} funded entries, got {len(got):,}")
        print(f"missing={len(missing)} wrong={len(wrong)} extra={len(extra)} failed={summ['failed_lookups']}")
        ing = summ["ingest"]
        print(f"ingest: evm_unique={ing['evm_unique']:,} dups={ing['evm_duplicates']:,} "
              f"malformed={ing['invalid_hex']:,} btc_unique={ing['btc_unique']:,}")
        ok = not missing and not wrong and not extra and ing["evm_unique"] == len(set(pool))
        print("PASS" if ok else "FAIL")
        sys.exit(0 if ok else 1)
    finally:
        for p in procs:
            p.terminate()


if __name__ == "__main__":
    main()
