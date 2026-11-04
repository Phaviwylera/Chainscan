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
    if request.path in ("/", "/healthz"):
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
           "upload_id": None, "url": None, "input": None}
    if body.get("upload_id"):
        p = os.path.join(UPLOADS, safe_id(body["upload_id"]) + ".txt")
        if not os.path.exists(p):
            raise web.HTTPBadRequest(text="upload not found")
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
        log = open(os.path.join(run_dir, "log.txt"), "ab")
        proc = await asyncio.create_subprocess_exec(*cmd, stdout=log, stderr=asyncio.subprocess.STDOUT,
                                                    start_new_session=True)
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
            os.remove(p)
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
    return web.FileResponse(os.path.join(os.path.dirname(__file__), "static", "index.html"))


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
    app.router.add_get("/api/info", info)
    app.router.add_post("/api/upload", upload)
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
