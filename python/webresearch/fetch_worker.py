"""Standalone wreq fetch worker, run under the wreq venv as a subprocess.

Usage: <wenv>/bin/python fetch_worker.py --url <url> --out <dir> [--profiles Safari18_5 Safari26 ...]

Rotates through the fingerprint pool until a 200 (or all fail). Deterministic,
single URL per invocation. Writes the fetched body to a temp file (never stdout —
pages can be MBs) and prints one JSON line to stdout. The fetch is
content-type aware: HTML is saved as UTF-8 text (html_path), PDFs are saved as
raw binary bytes (pdf_path); everything else goes through the HTML path.

  {"ok":true,"status":200,"profile":...,"final_url":...,"content_type":...,
   "html_path":..., "bytes":N}          # HTML / non-PDF
  {"ok":true,"status":200,"profile":...,"final_url":...,"content_type":"application/pdf",
   "pdf_path":..., "bytes":N}           # PDF (raw bytes)
  {"ok":false,"last":{"status":<code> or "error":...}}
"""
import argparse
import asyncio
import json
import os
import re
import tempfile
from datetime import timedelta

DEFAULT_PROFILES = ["Safari18_5", "Safari26", "Chrome131", "Firefox150"]


def _plat_for(prof: str):
    from wreq.emulation import Platform
    return Platform.MacOS if "Safari" in prof else Platform.Linux


async def _run(url: str, outdir: str, profiles):
    from wreq import Client
    from wreq.emulation import Emulation, Profile
    last = None
    for name in profiles:
        prof = getattr(Profile, name, None)
        if prof is None:
            continue
        try:
            c = Client(emulation=Emulation(profile=prof, platform=_plat_for(name)))
            resp = await c.get(url, timeout=timedelta(seconds=60), default_headers=False)
            # resp.status is an opaque StatusCode; str() is "403 Forbidden"
            sc = getattr(resp.status, "value", None)
            if sc is None:
                m = re.match(r"(\d+)", str(resp.status))
                sc = int(m.group(1)) if m else str(resp.status)
            if sc != 200:
                last = {"status": sc, "profile": name}
                continue
            # content-type may be bytes (e.g. b'text/html'); normalize to str.
            ct = (resp.headers.get("content-type") or b"")
            if isinstance(ct, bytes):
                ct = ct.decode("ascii", "replace")
            ct = ct.lower()
            if "application/pdf" in ct:
                # PDF: keep raw bytes (never decode binary as UTF-8).
                payload = await resp.bytes()
                fd, tpath = tempfile.mkstemp(suffix=".pdf", prefix="wr_", dir=outdir)
                with os.fdopen(fd, "wb") as f:
                    f.write(payload)
                print(json.dumps({
                    "ok": True, "status": 200, "profile": name,
                    "final_url": str(resp.url), "content_type": ct,
                    "pdf_path": tpath, "bytes": len(payload),
                }))
                return
            body = await resp.text()
            fd, tpath = tempfile.mkstemp(suffix=".html", prefix="wr_", dir=outdir)
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(body)
            print(json.dumps({
                "ok": True, "status": 200, "profile": name,
                "final_url": str(resp.url), "content_type": ct,
                "html_path": tpath, "bytes": len(body.encode("utf-8")),
            }))
            return
        except Exception as e:  # network errors, connection reset, etc.
            last = {"error": f"{type(e).__name__}: {e}", "profile": name}
    print(json.dumps({"ok": False, "last": last}))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--out", default=tempfile.gettempdir())
    ap.add_argument("--profiles", nargs="*", default=DEFAULT_PROFILES)
    a = ap.parse_args()
    asyncio.run(_run(a.url, a.out, a.profiles or DEFAULT_PROFILES))


if __name__ == "__main__":
    main()
