"""IngestAgent: turns an input file of any size into deduplicated on-disk buckets.

* The file is split into byte ranges, one per CPU; each worker streams its range in 64 MB chunks,
  so memory use is flat whether the file is 10 MB or 10 GB.
* 0x addresses are stored as 20 raw bytes, bucketed by their first byte (256 buckets).
* Bitcoin addresses (1..., 3..., bc1...) are bucketed by crc32 into text files.
* A second pass dedupes each bucket independently (numpy sort on 20-byte records).
Buckets are also the unit of checkpointing for the chain agents, so a crashed run resumes.
"""
import binascii
import glob
import os
import re
import time
import zlib
from concurrent.futures import ProcessPoolExecutor

import numpy as np

from .util import fmt_int, log, write_json_atomic

HEX_RE = re.compile(rb"0x([0-9a-fA-F]+)")
BTC_RE = re.compile(rb"(?<![0-9A-Za-z])(bc1[02-9ac-hj-np-z]{11,71}|[13][1-9A-HJ-NP-Za-km-z]{25,34})(?![0-9A-Za-z])")
# first-column mode: only the first value on each line counts (optional BOM, spaces or a quote first)
_LEAD = rb"(?m)^(?:\xef\xbb\xbf)?[ \t\"']*"
HEX_FIRST_RE = re.compile(_LEAD + rb"0x([0-9a-fA-F]+)")
BTC_FIRST_RE = re.compile(_LEAD + rb"(bc1[02-9ac-hj-np-z]{11,71}|[13][1-9A-HJ-NP-Za-km-z]{25,34})(?![0-9A-Za-z])")
TOKEN = frozenset(b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
EVM_BUCKETS = 256
# how much parsed data a worker holds before writing it to bucket files (lower on small instances)
FLUSH_BYTES = int(os.environ.get("CHAINSCAN_FLUSH_MB", "256")) * 1024 * 1024


def _flush(outdir, wid, evm_parts, btc_parts, nbtc):
    if evm_parts:
        arr = np.concatenate(evm_parts)
        first = arr[:, 0]
        order = np.argsort(first, kind="stable")
        arr = arr[order]
        counts = np.bincount(first, minlength=EVM_BUCKETS)
        pos = 0
        for b in np.nonzero(counts)[0]:
            c = counts[b]
            with open(os.path.join(outdir, "evm", f"w{wid:02d}_b{b:03d}.bin"), "ab") as f:
                f.write(arr[pos:pos + c].tobytes())
            pos += c
    if btc_parts:
        by = {}
        for a in btc_parts:
            by.setdefault(zlib.crc32(a) % nbtc, []).append(a)
        for b, lst in by.items():
            with open(os.path.join(outdir, "btc", f"w{wid:02d}_b{b:03d}.txt"), "ab") as f:
                f.write(b"\n".join(lst) + b"\n")


def _scan_range(args):
    path, start, end, outdir, wid, chunk, want_btc, nbtc, first_col = args
    stats = {"bytes": 0, "lines": 0, "evm_raw": 0, "invalid_hex": 0, "btc_raw": 0}
    hex_re, btc_re = (HEX_FIRST_RE, BTC_FIRST_RE) if first_col else (HEX_RE, BTC_RE)
    with open(path, "rb") as f:
        if start > 0:
            f.seek(start - 1)
            prev = f.read(1)
            if first_col:
                if prev != b"\n":  # mid-line: the previous worker owns this line
                    f.readline()
                    start = f.tell()
            elif prev and prev[0] in TOKEN:  # mid-token: the previous worker owns it
                while True:
                    b = f.read(1)
                    start += 1
                    if not b or b[0] not in TOKEN:
                        break
        f.seek(start)
        pos, carry = start, b""
        evm_parts, btc_parts, pending = [], [], 0
        while pos < end:
            data = f.read(min(chunk, end - pos))
            if not data:
                break
            pos += len(data)
            buf = carry + data
            if pos < end:
                if first_col:  # cut at the last full line so every buffer starts at a line start
                    i = buf.rfind(b"\n")
                    if i < 0 and len(buf) < 4 * chunk:
                        carry = buf
                        stats["bytes"] += len(data)
                        continue
                else:
                    i = len(buf) - 1
                    lim = max(0, len(buf) - 4096)
                    while i > lim and buf[i] in TOKEN:
                        i -= 1
                carry, buf = buf[i + 1:], buf[:i + 1]
            else:  # finish the line / token that runs past our range end
                carry = b""
                if first_col:
                    if not buf.endswith(b"\n"):
                        buf += f.readline()
                else:
                    while True:
                        more = f.read(256)
                        if not more:
                            break
                        j = 0
                        while j < len(more) and more[j] in TOKEN:
                            j += 1
                        buf += more[:j]
                        if j < len(more):
                            break
            stats["bytes"] += len(data)
            stats["lines"] += buf.count(b"\n")
            hexes = hex_re.findall(buf)
            good = [h for h in hexes if len(h) == 40]
            stats["invalid_hex"] += len(hexes) - len(good)
            if good:
                raw = binascii.unhexlify(b"".join(good))
                evm_parts.append(np.frombuffer(raw, dtype=np.uint8).reshape(-1, 20))
                stats["evm_raw"] += len(good)
                pending += len(good) * 20
            if want_btc:
                found = btc_re.findall(buf)
                for a in found:
                    btc_parts.append(a.lower() if a[:3] == b"bc1" else a)
                stats["btc_raw"] += len(found)
                pending += len(found) * 40
            if pending > FLUSH_BYTES:
                _flush(outdir, wid, evm_parts, btc_parts, nbtc)
                evm_parts, btc_parts, pending = [], [], 0
        _flush(outdir, wid, evm_parts, btc_parts, nbtc)
    return stats


def _dedupe_evm(args):
    outdir, b = args
    files = sorted(glob.glob(os.path.join(outdir, "evm", f"w*_b{b:03d}.bin")))
    if not files:
        return 0, 0
    data = b"".join(open(p, "rb").read() for p in files)
    arr = np.frombuffer(data, dtype="V20")
    uniq = np.unique(arr)
    with open(os.path.join(outdir, "evm", f"u_b{b:03d}.bin"), "wb") as f:
        f.write(uniq.tobytes())
    for p in files:
        os.remove(p)
    return len(arr), len(uniq)


def _dedupe_btc(args):
    outdir, b = args
    files = sorted(glob.glob(os.path.join(outdir, "btc", f"w*_b{b:03d}.txt")))
    if not files:
        return 0, 0
    seen, n = set(), 0
    for p in files:
        for line in open(p, "rb"):
            line = line.strip()
            if line:
                n += 1
                seen.add(line)
    with open(os.path.join(outdir, "btc", f"u_b{b:03d}.txt"), "wb") as f:
        f.write(b"\n".join(sorted(seen)) + b"\n")
    for p in files:
        os.remove(p)
    return n, len(seen)


class IngestAgent:
    def __init__(self, cfg, run_dir):
        self.cfg = cfg
        self.dir = os.path.join(run_dir, "ingest")
        self.meta_path = os.path.join(self.dir, "ingest.json")

    def evm_buckets(self):
        return sorted(glob.glob(os.path.join(self.dir, "evm", "u_b*.bin")))

    def btc_buckets(self):
        return sorted(glob.glob(os.path.join(self.dir, "btc", "u_b*.txt")))

    def run(self, inputs, want_btc, first_col=False):
        sig = [(os.path.abspath(p), os.path.getsize(p), int(os.path.getmtime(p))) for p in inputs]
        if os.path.exists(self.meta_path):
            import json
            meta = json.load(open(self.meta_path))
            if (meta.get("inputs") == [list(s) for s in sig] and meta.get("btc") == want_btc
                    and meta.get("first_column", False) == first_col):
                log(f"ingest: reusing earlier pass ({fmt_int(meta['evm_unique'])} 0x, "
                    f"{fmt_int(meta['btc_unique'])} BTC unique)")
                return meta
        for sub in ("evm", "btc"):
            d = os.path.join(self.dir, sub)
            os.makedirs(d, exist_ok=True)
            for p in glob.glob(os.path.join(d, "*")):
                os.remove(p)

        workers = self.cfg.ingest_workers or os.cpu_count() or 2
        chunk = self.cfg.ingest_chunk_mb * 1024 * 1024
        total = sum(s[1] for s in sig)
        log(f"ingest: reading {len(inputs)} file(s), {total / 1e9:.2f} GB, {workers} workers, "
            f"{'first value on each line only' if first_col else 'addresses anywhere in the text'}")
        t0 = time.time()
        jobs, wid = [], 0
        for p, size, _ in sig:
            parts = max(1, min(workers * 2, size // (16 * 1024 * 1024) or 1))
            step = -(-size // parts)
            for i in range(parts):
                s, e = i * step, min(size, (i + 1) * step)
                if s < e:
                    jobs.append((p, s, e, self.dir, wid, chunk, want_btc, self.cfg.btc_buckets, first_col))
                    wid += 1
        stats = {"bytes": 0, "lines": 0, "evm_raw": 0, "invalid_hex": 0, "btc_raw": 0}
        with ProcessPoolExecutor(max_workers=workers) as ex:
            for st in ex.map(_scan_range, jobs):
                for k in stats:
                    stats[k] += st[k]
        t1 = time.time()
        log(f"ingest: parsed {total / 1e9:.2f} GB in {t1 - t0:.1f}s "
            f"({total / 1e6 / max(t1 - t0, 1e-9):.0f} MB/s); deduping")
        evm_unique = btc_unique = 0
        with ProcessPoolExecutor(max_workers=workers) as ex:
            for _, u in ex.map(_dedupe_evm, [(self.dir, b) for b in range(EVM_BUCKETS)]):
                evm_unique += u
            if want_btc:
                for _, u in ex.map(_dedupe_btc, [(self.dir, b) for b in range(self.cfg.btc_buckets)]):
                    btc_unique += u
        meta = {"inputs": [list(s) for s in sig], "btc": want_btc, "first_column": first_col, **stats,
                "evm_unique": evm_unique, "evm_duplicates": stats["evm_raw"] - evm_unique,
                "btc_unique": btc_unique, "btc_duplicates": stats["btc_raw"] - btc_unique,
                "seconds": round(time.time() - t0, 2)}
        write_json_atomic(self.meta_path, meta)
        log(f"ingest: {fmt_int(evm_unique)} unique 0x, {fmt_int(btc_unique)} unique BTC, "
            f"{fmt_int(stats['invalid_hex'])} malformed 0x tokens, {meta['seconds']}s total")
        if first_col:
            skipped = stats["lines"] - stats["evm_raw"] - stats["btc_raw"] - stats["invalid_hex"]
            meta["lines_without_address"] = max(0, skipped)
            write_json_atomic(self.meta_path, meta)
            log(f"ingest: {fmt_int(stats['lines'])} lines, {fmt_int(max(0, skipped))} did not start with an address")
        return meta
