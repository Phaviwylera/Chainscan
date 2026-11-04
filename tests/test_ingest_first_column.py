"""Checks first-column ingest against a plain line-by-line reference.

Every line starts with an address and carries decoy text after it (other 0x and BTC addresses,
hashes, words). Tiny chunks and several workers force lines to straddle chunk and range edges.

  CHAINSCAN_CHUNK_MB=1 python tests/test_ingest_first_column.py
"""
import glob
import os
import random
import re
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from chainscan.config import AnalystCfg, BtcCfg, Config  # noqa: E402
from chainscan.ingest import IngestAgent  # noqa: E402

B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
B32 = "023456789acdefghjklmnpqrstuvwxyz"
BTC_OK = re.compile(r"^(bc1[02-9ac-hj-np-z]{11,71}|[13][1-9A-HJ-NP-Za-km-z]{25,34})$")


def rbtc(r):
    return ("1" + "".join(r.choice(B58) for _ in range(33))) if r.random() < .5 else \
        ("bc1q" + "".join(r.choice(B32) for _ in range(38)))


def reference(path):
    evm, btc = set(), set()
    with open(path, "rb") as f:
        for raw in f:
            line = raw.decode()
            if line.startswith("﻿"):
                line = line[1:]
            tok = re.split(r"[\s,;|]", line.lstrip(" \t\"'"), maxsplit=1)[0].strip("\"'")
            if tok.lower().startswith("0x") and re.fullmatch(r"0x[0-9a-fA-F]{40}", tok):
                evm.add(tok[2:].lower())
            elif BTC_OK.match(tok):
                btc.add(tok.lower() if tok.startswith("bc1") else tok)
    return evm, btc


def main():
    r = random.Random(11)
    work = tempfile.mkdtemp(prefix="cs_fc_")
    path = os.path.join(work, "in.txt")
    with open(path, "w", newline="") as f:
        f.write("﻿0x" + r.randbytes(20).hex() + " first line after a BOM\n")
        f.write("address,private_note,balance\n# comment line with 0x" + r.randbytes(20).hex() + "\n")
        for i in range(400_000):
            decoy = f"0x{r.randbytes(20).hex()} {rbtc(r)} 0x{r.randbytes(32).hex()} note {i}"
            k = r.random()
            if k < 0.45:
                a = "0x" + r.randbytes(20).hex()
                a = a.upper().replace("0X", "0x") if i % 9 == 0 else a
            elif k < 0.7:
                a = rbtc(r)
            elif k < 0.75:
                a = "0x" + r.randbytes(12).hex()  # malformed
            elif k < 0.8:
                a = "nothing here"
            else:
                a = '"0x' + r.randbytes(20).hex() + '"'
            sep = r.choice([" ", "\t", ",", " | ", ";"])
            end = "\r\n" if i % 5 == 0 else "\n"
            f.write(f"{a}{sep}{decoy}{end}")
            if i % 40 == 0:
                f.write(f"{a}{sep}duplicate{end}")
        f.write("0x" + r.randbytes(20).hex() + " last line, no newline")
    want_evm, want_btc = reference(path)
    cfg = Config(chains=[], btc=BtcCfg(enabled=True), analyst=AnalystCfg(), ingest_workers=3,
                 ingest_chunk_mb=int(os.environ.get("CHAINSCAN_CHUNK_MB", "1")))
    ag = IngestAgent(cfg, os.path.join(work, "run"))
    meta = ag.run([path], True, first_col=True)
    got_evm = set()
    for p in glob.glob(os.path.join(ag.dir, "evm", "u_b*.bin")):
        d = open(p, "rb").read()
        got_evm |= {d[i:i + 20].hex() for i in range(0, len(d), 20)}
    got_btc = set()
    for p in glob.glob(os.path.join(ag.dir, "btc", "u_b*.txt")):
        got_btc |= {ln.strip() for ln in open(p) if ln.strip()}
    print(f"file {os.path.getsize(path) / 1e6:.1f} MB; evm want {len(want_evm):,} got {len(got_evm):,}; "
          f"btc want {len(want_btc):,} got {len(got_btc):,}; lines without address {meta.get('lines_without_address')}")
    ok = got_evm == want_evm and got_btc == want_btc
    if not ok:
        print("evm missing", list(want_evm - got_evm)[:3], "extra", list(got_evm - want_evm)[:3])
        print("btc missing", list(want_btc - got_btc)[:3], "extra", list(got_btc - want_btc)[:3])
    shutil.rmtree(work)
    print("PASS" if ok else "FAIL")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
