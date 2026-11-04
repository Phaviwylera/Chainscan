"""Write a large address file fast (numpy), e.g. 3 crore lines of 0x addresses.

  python tests/gen_big.py /data/big.txt --n 30000000
"""
import argparse
import os

import numpy as np

HEX = np.frombuffer(b"0123456789abcdef", dtype=np.uint8)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("path")
    ap.add_argument("--n", type=int, default=30_000_000)
    ap.add_argument("--dup-every", type=int, default=20, help="repeat every Nth address once")
    a = ap.parse_args()
    step = 2_000_000
    with open(a.path, "wb") as f:
        left = a.n
        while left > 0:
            k = min(step, left)
            raw = np.frombuffer(os.urandom(20 * k), dtype=np.uint8).reshape(k, 20)
            if a.dup_every:
                raw = np.concatenate([raw, raw[::a.dup_every]])
            m = raw.shape[0]
            out = np.empty((m, 43), dtype=np.uint8)
            out[:, 0] = ord("0")
            out[:, 1] = ord("x")
            out[:, 2:42:2] = HEX[raw >> 4]
            out[:, 3:43:2] = HEX[raw & 15]
            out[:, 42] = ord("\n")
            f.write(out.tobytes())
            left -= k
    print(f"wrote {a.path}: {os.path.getsize(a.path) / 1e9:.2f} GB")


if __name__ == "__main__":
    main()
