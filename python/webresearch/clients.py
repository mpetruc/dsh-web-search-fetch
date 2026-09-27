"""web-research clients — Hister, SearXNG, reranker, and the wreq fetch worker.

Stdlib-only so it runs under any interpreter (Hermes venv, wreq venv, bare
python). All HTTP via urllib. Every function raises on hard failure; the core
pipeline layer owns retries/fallbacks.
"""
from __future__ import annotations

import base64
import json
import os
import re
import subprocess
import urllib.error
import urllib.parse
import urllib.request
from http.cookiejar import CookieJar
from typing import Any, Dict, List, Optional
import re
import time as _time

try:
    from .errors import (
        ConfigError, SearxNGSearchError, HisterSearchError, DocumentReadError,
        IngestError, RerankError,
    )
except ImportError:  # standalone / plain `import clients` fallback (tests)
    from errors import (
        ConfigError, SearxNGSearchError, HisterSearchError, DocumentReadError,
        IngestError, RerankError,
    )


# Unified recency filter and its SearXNG ``time_range`` translation.
# Mirrors web-search-plus's unified ``freshness`` param.
FRESHNESS_MAP = {"day": "day", "week": "week", "month": "month", "year": "year"}


def map_freshness(freshness) -> tuple:
    """Map a unified freshness value (``day``/``week``/``month``/``year``) to a
    SearXNG ``time_range``. Returns ``(time_range_or_None, applied: bool)`` so
    callers can report whether the filter was natively honored. Invalid or
    missing values are not silently dropped — they report ``applied=False``."""
    if not freshness:
        return None, False
    tr = FRESHNESS_MAP.get(str(freshness).strip().lower())
    return (tr, True) if tr else (None, False)


def env(name: str, default: str = "") -> str:
    return os.getenv(name, default) or default


def _http_get_json(url: str, headers: Dict[str, str], timeout: float = 20.0) -> Any:
    req = urllib.request.Request(url, headers=headers, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        body = r.read().decode("utf-8", "replace")
    return json.loads(body)


def _tolerant_json(text: str) -> Any:
    """Parse JSON that may contain trailing commas (hister CLI -f json emits these)."""
    text = re.sub(r",(\s*[\]}])", r"\1", text)
    return json.loads(text)


# ---------------------------------------------------------------------------
# Hister
# ---------------------------------------------------------------------------
class HisterClient:
    def __init__(self, base_url: str, token: str, hister_bin: str = "hister", log=None):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.hister_bin = hister_bin
        self.log = log

    def _msg(self, msg: str) -> None:
        if self.log:
            self.log(msg)

    def _auth(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"}

    # -- search: proven reliable via the CLI (raw /search HTTP 500s w/ bearer) --
    def search(self, query: str, limit: int = 10) -> List[Dict[str, str]]:
        cmd = [
            self.hister_bin, "search", query,
            "-u", self.base_url,
            "-t", self.token,
            "-F", "id,url,title,domain,type",
            "-f", "json",
            "-L", str(limit),
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=40)
        if proc.returncode != 0:
            raise HisterSearchError(
                f"hister search rc={proc.returncode}: {proc.stderr.strip()[:300]}",
                url=query)
        try:
            return _tolerant_json(proc.stdout or "[]")
        except Exception as e:
            raise HisterSearchError(f"hister search returned non-JSON: {e}", url=query)

    # -- read: clean full-text of a stored doc --
    def get_document(self, url: str) -> Optional[Dict[str, Any]]:
        u = f"{self.base_url}/api/document?url={urllib.parse.quote(url, safe='')}"
        try:
            d = _http_get_json(u, self._auth())
        except Exception as e:
            raise DocumentReadError(f"GET /api/document failed: {e}", url=url)
        if not isinstance(d, dict):
            raise DocumentReadError("non-dict response from /api/document", url=url)
        return d

    # -- write: index raw html (server-side text extraction). CSRF handshake required. --
    def add(self, url: str, html: str, title: Optional[str] = None, label: str = "") -> Dict[str, Any]:
        jar = CookieJar()
        opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))

        # 1) CSRF token bound to a session cookie (GET /api/config through same jar)
        cfg_req = urllib.request.Request(
            f"{self.base_url}/api/config",
            headers=self._auth(),
            method="GET",
        )
        csrf = ""
        try:
            with opener.open(cfg_req, timeout=20) as r:
                csrf = r.headers.get("X-Csrf-Token", "")
        except Exception as e:
            raise IngestError(f"GET /api/config (CSRF) failed: {e}", url=url)
        if not csrf:
            raise IngestError("hister /api/config did not return X-Csrf-Token", url=url)

        # 2) POST /api/add with the same session + CSRF header
        payload = {"url": url, "html": html}
        if title:
            payload["title"] = title
        if label:
            payload["label"] = label
        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            f"{self.base_url}/api/add",
            data=body,
            headers={**self._auth(), "X-Csrf-Token": csrf, "Content-Type": "application/json"},
            method="POST",
        )
        try:
            with opener.open(req, timeout=60) as r:
                raw = r.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as e:
            raise IngestError(
                f"hister add HTTP {e.code}: {e.read().decode('utf-8','replace')[:300]}",
                url=url)
        except Exception as e:
            raise IngestError(f"hister add failed: {e}", url=url)
        try:
            return json.loads(raw)
        except Exception:
            return {"status": raw[:200]}

    # -- write: index a PDF's raw bytes (server-side text extraction via AddPDF).
    # POST /api/add_pdf with {"document": {...}, "pdf": "<std base64>"}; CSRF handshake
    # required, same as add().
    def add_pdf(self, url: str, pdf: bytes, title: Optional[str] = None,
                label: str = "") -> Dict[str, Any]:
        jar = CookieJar()
        opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))

        # 1) CSRF token bound to a session cookie (GET /api/config through same jar)
        cfg_req = urllib.request.Request(
            f"{self.base_url}/api/config",
            headers=self._auth(),
            method="GET",
        )
        csrf = ""
        try:
            with opener.open(cfg_req, timeout=20) as r:
                csrf = r.headers.get("X-Csrf-Token", "")
        except Exception as e:
            raise IngestError(f"GET /api/config (CSRF) failed: {e}", url=url)
        if not csrf:
            raise IngestError("hister /api/config did not return X-Csrf-Token", url=url)

        # 2) POST /api/add_pdf with the same session + CSRF header
        document = {"url": url, "title": title or url}
        if label:
            document["label"] = label
        payload = {"document": document, "pdf": base64.b64encode(pdf).decode("ascii")}
        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            f"{self.base_url}/api/add_pdf",
            data=body,
            headers={**self._auth(), "X-Csrf-Token": csrf, "Content-Type": "application/json"},
            method="POST",
        )
        try:
            with opener.open(req, timeout=60) as r:
                raw = r.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as e:
            raise IngestError(
                f"hister add_pdf HTTP {e.code}: {e.read().decode('utf-8','replace')[:300]}",
                url=url)
        except Exception as e:
            raise IngestError(f"hister add_pdf failed: {e}", url=url)
        try:
            return json.loads(raw)
        except Exception:
            return {"status": raw[:200]}


# ---------------------------------------------------------------------------
# SearXNG
# ---------------------------------------------------------------------------
class SearxNGClient:
    def __init__(self, base_url: str, log=None):
        self.base_url = base_url.rstrip("/")
        self.log = log

    def _msg(self, msg: str) -> None:
        if self.log:
            self.log(msg)

    def search(self, query: str, limit: int = 10, freshness=None) -> List[Dict[str, str]]:
        params = {"q": query, "format": "json"}
        tr, applied = map_freshness(freshness)
        if applied:
            params["time_range"] = tr
        q = urllib.parse.urlencode(params)
        try:
            d = _http_get_json(f"{self.base_url}/search?{q}", {}, timeout=30)
        except Exception as e:
            raise SearxNGSearchError(f"SearXNG search failed: {e}", url=query)
        results = d.get("results", []) if isinstance(d, dict) else []
        out = []
        for r in results:
            url = r.get("url", "")
            if url:
                out.append({"url": url, "title": r.get("title", ""), "content": r.get("content", "")})
            if len(out) >= limit:
                break
        return out


# ---------------------------------------------------------------------------
# Reranker (OpenAI-compatible /v1/rerank or /v1/rerank). Wire when endpoint provided.
# ---------------------------------------------------------------------------
class RerankClient:
    def __init__(self, url: str, model: str, token: str = "", log=None):
        self.url = url.rstrip("/")
        self.model = model
        self.token = token
        self.log = log

    def available(self) -> bool:
        return bool(self.url and self.model)

    def rerank(self, query: str, documents: List[str]) -> List[int]:
        """Return doc indices ordered best->worst by relevance_score."""
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        payload = {"model": self.model, "query": query, "documents": documents}
        req = urllib.request.Request(
            f"{self.url}/v1/rerank",
            data=json.dumps(payload).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                d = json.loads(r.read().decode("utf-8", "replace"))
        except urllib.error.HTTPError as e:
            raise RerankError(
                f"rerank HTTP {e.code}: {e.read().decode('utf-8','replace')[:300]}")
        except Exception as e:
            raise RerankError(f"rerank request failed: {e}")
        results = d.get("results", []) if isinstance(d, dict) else []
        ranked = sorted(results, key=lambda x: x.get("relevance_score", 0.0), reverse=True)
        return [int(r.get("index", -1)) for r in ranked if r.get("index", -1) >= 0]
