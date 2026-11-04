import json
import os
import time


def fmt_units(raw: int, decimals: int) -> str:
    """Exact decimal string for an integer token amount."""
    if decimals == 0:
        return str(raw)
    q, r = divmod(raw, 10 ** decimals)
    frac = f"{r:0{decimals}d}".rstrip("0")
    return f"{q}.{frac}" if frac else str(q)


def fmt_int(n: int) -> str:
    """Indian digit grouping: 3,00,00,000."""
    s = str(int(n))
    if len(s) <= 3:
        return s
    head, tail = s[:-3], s[-3:]
    parts = []
    while len(head) > 2:
        parts.insert(0, head[-2:])
        head = head[:-2]
    if head:
        parts.insert(0, head)
    return ",".join(parts) + "," + tail


def fmt_secs(s: float) -> str:
    if s is None or s != s or s == float("inf"):
        return "--"
    s = int(s)
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m{s % 60:02d}s"
    return f"{s // 3600}h{(s % 3600) // 60:02d}m"


def write_json_atomic(path: str, obj) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(obj, f, indent=2, default=str)
    os.replace(tmp, path)


def touch(path: str) -> None:
    with open(path, "w") as f:
        f.write(str(time.time()))


def log(msg: str) -> None:
    print(time.strftime("%H:%M:%S"), msg, flush=True)
