"""Fast fake RPC for throughput tests: answers scanner eth_calls like a node would.

  python tests/mock_rpc.py --port 19000 --latency 0.5 --funded-every 200

Every `funded-every`-th wallet in a batch gets a balance for asset 0. `--latency` emulates the
time a real node spends executing a few thousand balance lookups.
"""
import argparse
import asyncio

from aiohttp import web


def make_app(latency, every, nt, gas_cap_n=0, rate_limit=0.0, fail_all=False):
    import random
    async def handle(req):
        body = await req.read()
        if fail_all and b"eth_blockNumber" in body:
            return web.json_response({"jsonrpc": "2.0", "id": 1, "error": {"code": -32600, "message": "not enabled"}}, status=403)
        if b"eth_blockNumber" in body:
            return web.json_response({"jsonrpc": "2.0", "id": 1, "result": "0x1000000"})
        i = body.find(b'"data": "0x')
        if i < 0:
            i = body.find(b'"data":"0x')
        j = body.find(b'"', i + 11)
        hexlen = j - (i + 11)
        # scanner prefix is fixed-size: initcode + 3 words + nt words + length word
        n = max(0, (hexlen // 2 - PREFIX - 32) // 20)
        if fail_all:
            return web.json_response({'jsonrpc': '2.0', 'id': 1, 'error': {'code': -32600, 'message': 'BNB_MAINNET is not enabled for this app'}}, status=403)
        if rate_limit and random.random() < rate_limit:
            return web.json_response({'jsonrpc': '2.0', 'id': 1, 'error': {'code': 429, 'message': 'Too many requests, rate limit exceeded'}})
        if gas_cap_n and n > gas_cap_n:
            return web.json_response({'jsonrpc': '2.0', 'id': 1, 'error': {'code': -32000, 'message': 'gas required exceeds allowance (50000000)'}})
        await asyncio.sleep(latency)
        out = bytearray()
        for k in range(0, n, every):
            out += k.to_bytes(4, "big") + bytes([0]) + (10 ** 18 + k).to_bytes(32, "big")
        return web.json_response({"jsonrpc": "2.0", "id": 1, "result": "0x" + out.hex()})

    app = web.Application(client_max_size=64 * 1024 * 1024)
    app.router.add_post("/", handle)
    return app


if __name__ == "__main__":
    import os
    import sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from chainscan.scanner_bytecode import SCANNER_INITCODE
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=19000)
    ap.add_argument("--latency", type=float, default=0.5)
    ap.add_argument("--funded-every", type=int, default=200)
    ap.add_argument("--assets", type=int, default=4)
    ap.add_argument("--gas-cap-n", type=int, default=0, help="reject batches larger than this with an out-of-gas error")
    ap.add_argument("--fail-all", action="store_true", help="reject every call, like a network not enabled on the key")
    ap.add_argument("--rate-limit", type=float, default=0.0, help="fraction of calls answered with a 429-style error")
    a = ap.parse_args()
    PREFIX = len(SCANNER_INITCODE) + 32 * 3 + 32 * a.assets
    web.run_app(make_app(a.latency, a.funded_every, a.assets, a.gas_cap_n, a.rate_limit, a.fail_all), port=a.port, print=None, access_log=None)
