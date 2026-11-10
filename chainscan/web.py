"""Web front end for running chainscan on a server (Render, any VM, Docker).

  python -m chainscan web          # listens on $PORT (default 10000)

Upload a file, paste addresses, or give a link to a large file (10 GB is fine: the server
downloads it itself). Jobs run one at a time in a subprocess, so the page stays responsive.
Every API call needs the ACCESS_TOKEN set in the environment.
"""
import asyncio
import json
import os
import secrets
import shutil
import signal
import sys
import time

import aiohttp
from aiohttp import web

from . import config as cfgmod

DATA = os.environ.get("DATA_DIR") or ("/data" if os.path.isdir("/data") and os.access("/data", os.W_OK) else "/tmp/chainscan")
CONFIG = os.environ.get("CHAINSCAN_CONFIG", os.path.join(os.path.dirname(os.path.dirname(__file__)), "config.render.yaml"))
TOKEN = os.environ.get("ACCESS_TOKEN", "")
UPLOADS = os.path.join(DATA, "uploads")
RUNS = os.path.join(DATA, "runs")
JOBS_FILE = os.path.join(DATA, "jobs.json")
OUT_FILES = ("funded.csv", "funded_wide.csv", "summary.json", "report.md", "progress.json")

BTC_DIR = os.path.join(DATA, "btc")
BTC_PARQUET = os.path.join(BTC_DIR, "snapshot.parquet")
BTC_META = os.path.join(BTC_DIR, "meta.json")
btc_task = {"task": None}

jobs = {}           # id -> dict
queue = asyncio.Queue()
running = {"id": None, "proc": None}


def save_jobs():
    tmp = JOBS_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(jobs, f)
    os.replace(tmp, JOBS_FILE)


def new_id():
    return time.strftime("%Y%m%d-%H%M%S-") + secrets.token_hex(3)


def safe_id(s):
    if not s or not all(c.isalnum() or c in "-_" for c in s):
        raise web.HTTPBadRequest(text="bad id")
    return s


@web.middleware
async def auth(request, handler):
    if request.path in ("/", "/healthz") or request.path.startswith("/static/"):
        return await handler(request)
    if not TOKEN:
        return web.json_response({"error": "ACCESS_TOKEN is not set on the server"}, status=503)
    got = request.headers.get("X-Access-Token") or request.query.get("token", "")
    if not secrets.compare_digest(got, TOKEN):
        return web.json_response({"error": "wrong access token"}, status=401)
    return await handler(request)


# ---------------- inputs ----------------
async def upload(request):
    """multipart field 'file' (streamed to disk) or JSON {"text": "..."} for pasted lists."""
    os.makedirs(UPLOADS, exist_ok=True)
    uid = new_id()
    path = os.path.join(UPLOADS, uid + ".txt")
    if request.content_type.startswith("multipart/"):
        reader = await request.multipart()
        field = await reader.next()
        while field is not None and field.name != "file":
            field = await reader.next()
        if field is None:
            raise web.HTTPBadRequest(text="no file field")
        name = field.filename or "upload"
        size = 0
        with open(path, "wb") as f:
            while True:
                chunk = await field.read_chunk(4 * 1024 * 1024)
                if not chunk:
                    break
                f.write(chunk)
                size += len(chunk)
    else:
        body = await request.json()
        text = body.get("text", "")
        name = "pasted.txt"
        with open(path, "w") as f:
            f.write(text)
        size = os.path.getsize(path)
    return web.json_response({"upload_id": uid, "name": name, "bytes": size})


# ---------------- Bitcoin balance source (address -> balance dump, e.g. Blockchair) ----------------
def btc_meta():
    m = read_json(BTC_META) or {}
    m["ready"] = os.path.exists(BTC_PARQUET) and m.get("state") == "ready"
    return m


def _btc_set(**kw):
    os.makedirs(BTC_DIR, exist_ok=True)
    m = read_json(BTC_META) or {}
    m.update(kw, updated=time.time())
    tmp = BTC_META + ".tmp"
    with open(tmp, "w") as f:
        json.dump(m, f)
    os.replace(tmp, BTC_META)


async def _btc_build(src, name):
    _btc_set(state="building", source_name=name, error=None, progress=None)
    log = open(os.path.join(BTC_DIR, "build.log"), "ab")
    proc = await asyncio.create_subprocess_exec(sys.executable, "-m", "chainscan", "btc-source", src, BTC_PARQUET,
                                                "--meta", BTC_META, stdout=log, stderr=asyncio.subprocess.STDOUT)
    rc = await proc.wait()
    log.close()
    if rc != 0:
        _btc_set(state="failed", error=f"could not read the file (exit {rc}); see btc/build.log on the server")
    else:
        _btc_set(source_name=name)
    try:
        os.remove(src)   # the Parquet table holds everything; free the space
    except OSError:
        pass


async def btc_source_get(request):
    return web.json_response(btc_meta())


async def btc_source_upload(request):
    if btc_task["task"] and not btc_task["task"].done():
        raise web.HTTPConflict(text="a balance file is already being processed")
    os.makedirs(BTC_DIR, exist_ok=True)
    reader = await request.multipart()
    field = await reader.next()
    while field is not None and field.name != "file":
        field = await reader.next()
    if field is None:
        raise web.HTTPBadRequest(text="no file field")
    name = os.path.basename(field.filename or "balances.tsv")
    ext = ".gz" if name.endswith(".gz") else (".csv" if name.endswith(".csv") else ".tsv")
    path = os.path.join(BTC_DIR, "source" + (".tsv" + ext if ext == ".gz" else ext))
    _btc_set(state="uploading", source_name=name, error=None)
    n = 0
    with open(path + ".part", "wb") as f:
        while True:
            chunk = await field.read_chunk(8 * 1024 * 1024)
            if not chunk:
                break
            f.write(chunk)
            n += len(chunk)
    os.replace(path + ".part", path)
    _btc_set(source_bytes=n)
    btc_task["task"] = asyncio.create_task(_btc_build(path, name))
    return web.json_response(btc_meta())


async def btc_source_url(request):
    if btc_task["task"] and not btc_task["task"].done():
        raise web.HTTPConflict(text="a balance file is already being processed")
    body = await request.json()
    url = str(body.get("url", ""))
    if not url.startswith(("http://", "https://")):
        raise web.HTTPBadRequest(text="url must be http(s)")
    name = url.split("?")[0].split("/")[-1] or "balances.tsv"
    path = os.path.join(BTC_DIR, "source.tsv.gz" if name.endswith(".gz") else "source.tsv")

    async def run():
        os.makedirs(BTC_DIR, exist_ok=True)
        _btc_set(state="downloading", source_name=name, error=None, downloaded=0)
        try:
            timeout = aiohttp.ClientTimeout(total=None, sock_read=600)
            async with aiohttp.ClientSession(timeout=timeout) as sess:
                async with sess.get(url) as r:
                    if r.status >= 400:
                        raise RuntimeError(f"HTTP {r.status}")
                    total, got, last = int(r.headers.get("content-length") or 0), 0, 0.0
                    with open(path, "wb") as f:
                        async for chunk in r.content.iter_chunked(4 * 1024 * 1024):
                            f.write(chunk)
                            got += len(chunk)
                            if time.time() - last > 2:
                                _btc_set(downloaded=got, download_total=total)
                                last = time.time()
            _btc_set(downloaded=got, source_bytes=got)
        except Exception as e:  # noqa: BLE001
            _btc_set(state="failed", error=f"download failed: {e}")
            return
        await _btc_build(path, name)

    btc_task["task"] = asyncio.create_task(run())
    return web.json_response(btc_meta())


# ---------------- packed uploads (browser reads the file, sends only addresses) ----------------
PART_KINDS = ("evm", "btc")


def _pdir(uid):
    return os.path.join(UPLOADS, safe_id(uid) + ".packed")


async def packed_create(request):
    body = await request.json()
    uid = new_id()
    d = _pdir(uid)
    os.makedirs(d, exist_ok=True)
    meta = {"upload_id": uid, "name": str(body.get("name", ""))[:200], "file_bytes": int(body.get("file_bytes", 0)),
            "first_column": bool(body.get("first_column", True)), "created": time.time(),
            "key": str(body.get("key", ""))[:300]}
    with open(os.path.join(d, "upload.json"), "w") as f:
        json.dump(meta, f)
    return web.json_response(meta)


async def packed_find(request):
    """Find an unfinished upload of the same file so the browser can resume it."""
    key = request.query.get("key", "")
    if not key or not os.path.isdir(UPLOADS):
        return web.json_response(None)
    for name in sorted(os.listdir(UPLOADS), reverse=True):
        if not name.endswith(".packed"):
            continue
        d = os.path.join(UPLOADS, name)
        meta = read_json(os.path.join(d, "upload.json")) or {}
        if meta.get("key") == key and not os.path.exists(os.path.join(d, "manifest.json")):
            return web.json_response(packed_state(meta["upload_id"]))
    return web.json_response(None)


def packed_state(uid):
    d = _pdir(uid)
    meta = read_json(os.path.join(d, "upload.json")) or {}
    parts = {k: [] for k in PART_KINDS}
    nbytes = 0
    for f in os.listdir(d):
        if f.endswith(".part"):
            kind, seq = f[:-5].split("_")
            parts[kind].append(int(seq))
            nbytes += os.path.getsize(os.path.join(d, f))
    return {**meta, "parts": {k: sorted(v) for k, v in parts.items()}, "received_bytes": nbytes,
            "finished": os.path.exists(os.path.join(d, "manifest.json"))}


async def packed_get(request):
    uid = safe_id(request.match_info["id"])
    if not os.path.isdir(_pdir(uid)):
        raise web.HTTPNotFound()
    return web.json_response(packed_state(uid))


async def packed_put(request):
    uid = safe_id(request.match_info["id"])
    kind = request.match_info["kind"]
    seq = int(request.match_info["seq"])
    d = _pdir(uid)
    if kind not in PART_KINDS or seq < 0 or not os.path.isdir(d):
        raise web.HTTPBadRequest(text="bad part")
    if os.path.exists(os.path.join(d, "manifest.json")):
        raise web.HTTPConflict(text="upload already finished")
    final = os.path.join(d, f"{kind}_{seq:06d}.part")
    tmp = final + f".{secrets.token_hex(4)}.tmp"
    n = 0
    with open(tmp, "wb") as f:
        async for chunk in request.content.iter_chunked(1024 * 1024):
            f.write(chunk)
            n += len(chunk)
    if kind == "evm" and n % 20:
        os.remove(tmp)
        raise web.HTTPBadRequest(text="evm part must be a multiple of 20 bytes")
    os.replace(tmp, final)  # re-sending a part just overwrites it
    return web.json_response({"ok": True, "bytes": n})


async def packed_finish(request):
    uid = safe_id(request.match_info["id"])
    d = _pdir(uid)
    body = await request.json()
    st = packed_state(uid)
    want = body.get("counts") or {}
    missing = {k: [i for i in range(int(want.get(k, 0))) if i not in set(st["parts"][k])] for k in PART_KINDS}
    if any(missing.values()):
        return web.json_response({"error": "parts missing", "missing": missing}, status=409)
    stats = {k: int(v) for k, v in (body.get("stats") or {}).items() if isinstance(v, (int, float))}
    stats["file_bytes"] = st.get("file_bytes", 0)
    with open(os.path.join(d, "manifest.json"), "w") as f:
        json.dump({"upload_id": uid, "stats": stats, "counts": want, "finished_at": time.time()}, f)
    return web.json_response({**packed_state(uid), "stats": stats})


async def download_url(job):
    """Fetch a big input file server-side (S3/GCS presigned URL, Drive direct link, any HTTP URL)."""
    os.makedirs(UPLOADS, exist_ok=True)
    path = os.path.join(UPLOADS, job["id"] + "-url.txt")
    job["status"] = "downloading"
    job["downloaded"] = 0
    timeout = aiohttp.ClientTimeout(total=None, sock_read=300)
    async with aiohttp.ClientSession(timeout=timeout) as s:
        async with s.get(job["url"]) as r:
            if r.status >= 400:
                raise RuntimeError(f"download failed: HTTP {r.status}")
            job["download_total"] = int(r.headers.get("content-length") or 0)
            with open(path, "wb") as f:
                async for chunk in r.content.iter_chunked(4 * 1024 * 1024):
                    f.write(chunk)
                    job["downloaded"] += len(chunk)
    return path


# ---------------- jobs ----------------
async def create_job(request):
    body = await request.json()
    jid = new_id()
    job = {"id": jid, "created": time.time(), "status": "queued", "name": body.get("name") or "",
           "chains": [c for c in (body.get("chains") or []) if isinstance(c, str)],
           "upload_id": None, "url": None, "input": None,
           "first_column": bool(body.get("first_column", True))}
    if body.get("upload_id"):
        p = os.path.join(UPLOADS, safe_id(body["upload_id"]) + ".txt")
        if not os.path.exists(p):
            p = _pdir(body["upload_id"])
            if not os.path.exists(os.path.join(p, "manifest.json")):
                raise web.HTTPBadRequest(text="upload not found or not finished")
            job["packed"] = read_json(os.path.join(p, "manifest.json"))
        job["upload_id"] = body["upload_id"]
        job["input"] = p
        job["name"] = job["name"] or body.get("upload_name", "")
    elif body.get("url"):
        if not str(body["url"]).startswith(("http://", "https://")):
            raise web.HTTPBadRequest(text="url must be http(s)")
        job["url"] = body["url"]
        job["name"] = job["name"] or body["url"].split("?")[0].split("/")[-1][:60]
    else:
        raise web.HTTPBadRequest(text="give upload_id or url")
    jobs[jid] = job
    save_jobs()
    await queue.put(jid)
    return web.json_response(job)


async def run_job(jid):
    job = jobs[jid]
    run_dir = os.path.join(RUNS, jid)
    os.makedirs(run_dir, exist_ok=True)
    try:
        if job.get("url") and not job.get("input"):
            job["input"] = await download_url(job)
            save_jobs()
        job["status"] = "running"
        job["started"] = time.time()
        save_jobs()
        cmd = [sys.executable, "-m", "chainscan", "scan", job["input"], "-c", CONFIG, "--run-dir", run_dir]
        if job["chains"]:
            cmd += ["--only", ",".join(job["chains"])]
        if job.get("first_column"):
            cmd.append("--first-column")
        log = open(os.path.join(run_dir, "log.txt"), "ab")
        env = dict(os.environ)
        if btc_meta().get("ready"):   # Bitcoin balances come from the uploaded dump: a join, no API calls
            env.update(BTC_SNAPSHOT_PATH=BTC_PARQUET, CHAINSCAN_BTC_MODE="snapshot")
            job["btc_source"] = btc_meta().get("source_name")
        proc = await asyncio.create_subprocess_exec(*cmd, stdout=log, stderr=asyncio.subprocess.STDOUT,
                                                    start_new_session=True, env=env)
        running.update(id=jid, proc=proc)
        rc = await proc.wait()
        log.close()
        if job["status"] == "stopping":
            job["status"] = "stopped"
        else:
            job["status"] = "done" if rc == 0 else "failed"
            if rc != 0:
                job["error"] = f"scanner exited with code {rc}; see log"
    except Exception as e:  # noqa: BLE001
        job["status"] = "failed"
        job["error"] = str(e)[:500]
    finally:
        job["finished"] = time.time()
        running.update(id=None, proc=None)
        save_jobs()


async def worker(app):
    while True:
        jid = await queue.get()
        if jid in jobs and jobs[jid]["status"] in ("queued", "interrupted"):
            await run_job(jid)


def read_json(p):
    try:
        with open(p) as f:
            return json.load(f)
    except Exception:  # noqa: BLE001
        return None


def tail(p, n=40):
    try:
        with open(p, "rb") as f:
            f.seek(0, 2)
            size = f.tell()
            f.seek(max(0, size - 16000))
            lines = f.read().decode("utf-8", "replace").splitlines()
        return [ln for ln in lines if not ln.startswith("  ")][-n:]
    except OSError:
        return []


async def list_jobs(request):
    out = sorted(jobs.values(), key=lambda j: -j["created"])[:50]
    return web.json_response(out)


async def get_job(request):
    jid = safe_id(request.match_info["id"])
    if jid not in jobs:
        raise web.HTTPNotFound()
    d = os.path.join(RUNS, jid)
    files = [f for f in OUT_FILES if os.path.exists(os.path.join(d, f))]
    for p in sorted(os.listdir(os.path.join(d, "failed"))) if os.path.isdir(os.path.join(d, "failed")) else []:
        files.append("failed/" + p)
    return web.json_response({"job": jobs[jid], "progress": read_json(os.path.join(d, "progress.json")),
                              "summary": read_json(os.path.join(d, "summary.json")),
                              "ingest": read_json(os.path.join(d, "ingest", "ingest.json")),
                              "ingest_progress": read_json(os.path.join(d, "ingest", "progress.json")),
                              "log": tail(os.path.join(d, "log.txt")), "files": files})


async def get_file(request):
    jid = safe_id(request.match_info["id"])
    name = request.match_info["name"]
    allowed = set(OUT_FILES) | {"log.txt"}
    if name not in allowed and not (name.startswith("failed/") and name.count("/") == 1 and ".." not in name):
        raise web.HTTPNotFound()
    p = os.path.join(RUNS, jid, name)
    if not os.path.exists(p):
        raise web.HTTPNotFound()
    return web.FileResponse(p, headers={"Content-Disposition": f'attachment; filename="{jid}-{name.replace("/", "-")}"'})


async def stop_job(request):
    jid = safe_id(request.match_info["id"])
    job = jobs.get(jid)
    if not job:
        raise web.HTTPNotFound()
    if running["id"] == jid and running["proc"]:
        job["status"] = "stopping"
        try:
            os.killpg(running["proc"].pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    elif job["status"] == "queued":
        job["status"] = "stopped"
    save_jobs()
    return web.json_response(job)


async def resume_job(request):
    jid = safe_id(request.match_info["id"])
    job = jobs.get(jid)
    if not job or job["status"] not in ("stopped", "failed", "interrupted"):
        raise web.HTTPBadRequest(text="only stopped, failed or interrupted jobs can resume")
    job["status"] = "queued"
    job.pop("error", None)
    save_jobs()
    await queue.put(jid)
    return web.json_response(job)


async def delete_job(request):
    jid = safe_id(request.match_info["id"])
    job = jobs.get(jid)
    if not job or job["status"] in ("running", "downloading", "stopping"):
        raise web.HTTPBadRequest(text="stop the job first")
    shutil.rmtree(os.path.join(RUNS, jid), ignore_errors=True)
    for p in (job.get("input"),):
        if p and p.startswith(UPLOADS) and os.path.exists(p):
            shutil.rmtree(p, ignore_errors=True) if os.path.isdir(p) else os.remove(p)
    jobs.pop(jid)
    save_jobs()
    return web.json_response({"deleted": jid})


async def info(request):
    try:
        cfg = cfgmod.load(CONFIG)
        chains = [{"key": c.key, "name": c.name, "assets": [a.symbol for a in c.assets],
                   "endpoints": [e.url.split("//")[-1].split("/")[0] for e in c.endpoints]} for c in cfg.chains]
        btc = {"enabled": cfg.btc.enabled, "mode": cfg.btc.mode,
               "endpoints": [e.url.split("//")[-1].split("/")[0] for e in cfg.btc.esplora]}
    except SystemExit as e:
        return web.json_response({"error": str(e)}, status=500)
    du = shutil.disk_usage(DATA)
    return web.json_response({"chains": chains, "btc": btc, "data_dir": DATA,
                              "disk_free_gb": round(du.free / 1e9, 1), "disk_total_gb": round(du.total / 1e9, 1),
                              "cpus": os.cpu_count(), "analyst": bool(cfg.analyst.enabled)})


async def index(request):
    return web.FileResponse(os.path.join(os.path.dirname(__file__), "static", "index.html"),
                            headers={"Cache-Control": "no-cache"})


async def static_file(request):
    name = request.match_info["name"]
    if name not in ("reader.js", "scan3d.js"):
        raise web.HTTPNotFound()
    return web.FileResponse(os.path.join(os.path.dirname(__file__), "static", name),
                            headers={"Cache-Control": "no-cache"})


async def healthz(request):
    return web.Response(text="ok")


async def on_start(app):
    os.makedirs(UPLOADS, exist_ok=True)
    os.makedirs(RUNS, exist_ok=True)
    if os.path.exists(JOBS_FILE):
        jobs.update(read_json(JOBS_FILE) or {})
        for j in jobs.values():  # a restart killed anything in flight; it can resume from checkpoints
            if j["status"] in ("running", "downloading", "stopping"):
                j["status"] = "interrupted"
            if j["status"] == "queued":
                await queue.put(j["id"])
        save_jobs()
    app["worker"] = asyncio.create_task(worker(app))


def main():
    app = web.Application(middlewares=[auth], client_max_size=1024 ** 4)
    app.router.add_get("/", index)
    app.router.add_get("/healthz", healthz)
    app.router.add_get("/static/{name}", static_file)
    app.router.add_get("/api/info", info)
    app.router.add_get("/api/btc-source", btc_source_get)
    app.router.add_post("/api/btc-source", btc_source_upload)
    app.router.add_post("/api/btc-source/url", btc_source_url)
    app.router.add_post("/api/upload", upload)
    app.router.add_post("/api/packed", packed_create)
    app.router.add_get("/api/packed/find", packed_find)
    app.router.add_get("/api/packed/{id}", packed_get)
    app.router.add_put("/api/packed/{id}/{kind}/{seq}", packed_put)
    app.router.add_post("/api/packed/{id}/finish", packed_finish)
    app.router.add_get("/api/jobs", list_jobs)
    app.router.add_post("/api/jobs", create_job)
    app.router.add_get("/api/jobs/{id}", get_job)
    app.router.add_post("/api/jobs/{id}/stop", stop_job)
    app.router.add_post("/api/jobs/{id}/resume", resume_job)
    app.router.add_delete("/api/jobs/{id}", delete_job)
    app.router.add_get("/api/jobs/{id}/files/{name:.+}", get_file)
    app.on_startup.append(on_start)
    port = int(os.environ.get("PORT", "10000"))
    print(f"chainscan web on :{port}, data in {DATA}, config {CONFIG}", flush=True)
    web.run_app(app, port=port, access_log=None)
