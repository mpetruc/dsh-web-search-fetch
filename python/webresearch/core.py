"""web-research core pipeline.

Deterministic, fault-tolerant orchestration with a per-stage contract:
every stage either returns a typed result or raises; the caller retries,
falls back, and always logs. A stage never silently returns empty.

Stage graph (web_research):
  1. SearXNG + Hister recall (parallel)          -> union, dedupe by URL
  2. split cached vs uncached
  3. fetch uncached via wreq (fingerprint rotation, camofox fallback hook)
  4. ingest fetched html -> POST /api/add (CSRF handled in client)
  5. return clean text via GET /api/document
  6. optional local rerank (never fails the tool)

Logging: structured lines to $WEBRESEARCH_LOGS_DIR or ~/.dsh/logs/
web-research.log and (via the result's "diagnostics") echoed to the caller.
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

try:
    from . import clients
    from .camofox import CamofoxClient, ManagedChallengeError
    from . import quality
    from .errors import ConfigError
except ImportError:  # standalone invocation
    import clients  # standalone invocation
    from camofox import CamofoxClient, ManagedChallengeError
    import quality
    from errors import ConfigError


def __log_path() -> str:
    d = os.getenv("WEBRESEARCH_LOGS_DIR") or os.path.join(
        os.environ.get("DSH_HOME", os.path.expanduser("~/.dsh")), "logs")
    os.makedirs(d, exist_ok=True)
    return os.path.join(d, "web-research.log")


def _get_logger(name: str = "web-research"):
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        fh = logging.FileHandler(__log_path(), encoding="utf-8")
        fh.setFormatter(logging.Formatter(
            "%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%dT%H:%M:%S"))
        logger.addHandler(fh)
    return logger


log = _get_logger()


def retry(fn, attempts: int = 3, base: float = 1.0, backoff: float = 2.0,
          step: str = "", exceptions=(Exception,)):
    last = None
    for i in range(attempts):
        try:
            return fn()
        except exceptions as e:
            last = e
            delay = base * (backoff ** i)
            log.warning("%s attempt %d/%d failed (%s); retrying in %.1fs",
                        step, i + 1, attempts, e, delay)
            time.sleep(delay)
    raise RuntimeError(f"{step} exhausted {attempts} attempts: {last}")


# ---------------------------------------------------------------------------
# Clients (lazy, from env on first use)
# ---------------------------------------------------------------------------
def _hister() -> clients.HisterClient:
    return clients.HisterClient(
        base_url=os.getenv("WEBRESEARCH_HISTER_URL", "").rstrip("/"),
        token=os.getenv("WEBRESEARCH_HISTER_TOKEN", ""),
        hister_bin=os.getenv("WEBRESEARCH_HISTER_BIN", "hister"),
        log=lambda m: log.info("hister | %s", m),
    )


def _searxng() -> clients.SearxNGClient:
    return clients.SearxNGClient(
        base_url=os.getenv("WEBRESEARCH_SEARXNG_URL", ""),
        log=lambda m: log.info("searxng | %s", m))


def _wreq_py() -> str:
    return os.getenv("WEBRESEARCH_WREQ_PY", "")


def _rerank() -> Optional[clients.RerankClient]:
    c = clients.RerankClient(
        url=os.getenv("WEBRESEARCH_RERANK_URL", ""),
        model=os.getenv("WEBRESEARCH_RERANK_MODEL", ""),
        token=os.getenv("WEBRESEARCH_RERANK_TOKEN", ""),
        log=lambda m: log.info("rerank | %s", m))
    return c if c.available() else None


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ---------------------------------------------------------------------------
# Cache freshness (stale-hit auto-refresh)
# ---------------------------------------------------------------------------
def _refresh_max_age_days() -> int:
    """Max age (days) a cached doc may be before it is auto re-fetched. Default 10;
    override with WEBRESEARCH_REFRESH_MAX_DAYS. 0 = never serve from cache."""
    try:
        return max(0, int(os.getenv("WEBRESEARCH_REFRESH_MAX_DAYS", "10")))
    except ValueError:
        return 10


def _parse_ts(value) -> Optional[datetime]:
    """Parse Hister doc timestamps (Go time.Time): RFC3339 with T or space,
    nanosecond precision, numeric offset or Z, or naive. Also handles unix
    seconds/millis. Returns a tz-aware UTC datetime, or None if unparseable."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        v = float(value)
        if v > 1e12:
            v /= 1000.0
        try:
            return datetime.fromtimestamp(v, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    s = str(value).strip()
    if not s:
        return None
    if s.replace(".", "", 1).isdigit():
        return _parse_ts(float(s))
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _is_fresh(doc: Optional[Dict[str, Any]], max_age_days: int) -> bool:
    """True when the cached doc is recent enough to serve without re-fetching.
    If the age cannot be determined, treat as fresh (log loudly) rather than churn."""
    if max_age_days <= 0:
        return False
    u = _parse_ts((doc or {}).get("updated"))
    if u is None:
        log.warning("web-research: cannot determine cache age for %s (updated=%r); "
                    "serving cache (set WEBRESEARCH_REFRESH_MAX_DAYS=0 to always refresh)",
                    (doc or {}).get("url"), (doc or {}).get("updated"))
        return True
    age_s = (datetime.now(timezone.utc) - u).total_seconds()
    return age_s < max_age_days * 86400


# Failure-shell detector: text that looks like a JS-required / cookie-wall / error
# page rather than real content. Guards against caching or serving such shells as
# "clean text" (e.g. nature.com returns an HTTP 200 shell, so no bot-challenge is
# detected and wreq "succeeds"). Empty or very short text is also treated as a failure.
_SHELL_MARKERS = (
    "couldn\x92t load", "could not load", "part of this site",
    "enable javascript", "ad blocker", "try using a different browser",
    "just a moment", "performing security verification", "verifying you are human",
    "verify you are human", "your connection was reset", "public access is not provided",
    "access is denied", "the site can\x92t be reached", "this site can\x92t be reached",
)


def _looks_like_failure(text: str) -> bool:
    """True when `text` is too thin to be real content, or matches a failure shell."""
    t = (text or "").strip()
    if not t:
        return True
    if len(t) < 40:
        return True
    low = t.lower()
    return any(m in low for m in _SHELL_MARKERS)


# ---------------------------------------------------------------------------
# Stage helpers
# ---------------------------------------------------------------------------
def _wreq_fetch(url: str, timeout: int = 90) -> Dict[str, Any]:
    """Fetch one URL via the wreq venv subprocess; rotate fingerprints inside."""
    py = _wreq_py()
    if not py:
        raise ConfigError("WEBRESEARCH_WREQ_PY not set (path to wreq venv python)", url=url)
    workdir = os.getenv("WEBRESEARCH_WORK_DIR", "/tmp")
    cmd = [py, os.path.join(os.path.dirname(__file__), "fetch_worker.py"),
           "--url", url, "--out", workdir]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    stdout = proc.stdout.strip()
    if proc.returncode != 0 or not stdout:
        raise RuntimeError(f"wreq fetch rc={proc.returncode}: {proc.stderr.strip()[:300]}")
    result = json.loads(stdout.splitlines()[-1])
    if not result.get("ok"):
        raise RuntimeError(f"wreq fetch failed: {result}")
    return result


def _camofox_enabled() -> bool:
    return os.getenv("WEBRESEARCH_CAMOFOX_FALLBACK", "false").lower() in (
        "1", "true", "yes", "on")


def _camofox_render(url: str) -> Dict[str, Any]:
    cam = CamofoxClient(
        base_url=os.getenv("CAMOFOX_URL", "http://localhost:9377"),
        api_key=os.getenv("CAMOFOX_API_KEY", ""),
        cookies_file=os.getenv("WEBRESEARCH_COOKIES_FILE", ""),
        log=lambda m: log.info("camofox | %s", m))
    return cam.render(url)


def fetch_html(url: str) -> Dict[str, Any]:
    """Fetch a page with fallback chaining:
    wreq (TLS fingerprint) -> camofox (anti-detect browser render).

    Returns {"kind": "html"|"pdf", "source": "wreq"|"camofox", "final_url": str}
    with either {"html": str} (HTML case) or {"pdf_bytes": bytes} (PDF case).
    Raises ManagedChallengeError or RuntimeError when all fetch layers fail.
    """
    try:
        f = _wreq_fetch(url)
        ct = (f.get("content_type") or "").lower()
        if "application/pdf" in ct or f.get("pdf_path"):
            # PDF fetched as raw bytes (fetch worker writes a .pdf temp file).
            with open(f["pdf_path"], "rb") as fh:
                pdf_bytes = fh.read()
            os.unlink(f["pdf_path"])
            log.info("fetch_html %s via wreq PDF (%s) %db", url, f.get("profile"), len(pdf_bytes))
            return {"kind": "pdf", "pdf_bytes": pdf_bytes, "source": "wreq",
                    "final_url": f.get("final_url", url)}
        with open(f["html_path"], "r", encoding="utf-8") as fh:
            html = fh.read()
        os.unlink(f["html_path"])
        log.info("fetch_html %s via wreq (%s)", url, f.get("profile"))
        return {"kind": "html", "html": html, "source": "wreq",
                "final_url": f.get("final_url", url)}
    except Exception as e:
        log.warning("fetch_html %s wreq failed: %s", url, e)
        if not _camofox_enabled():
            raise RuntimeError(f"wreq fetch failed and camofox fallback disabled: {e}")
        try:
            r = _camofox_render(url)
            log.info("fetch_html %s via camofox", url)
            return {"kind": "html", "html": r["html"], "source": "camofox",
                    "final_url": r.get("final_url", url)}
        except ManagedChallengeError as ce:
            log.error("fetch_html %s camofox blocked by managed challenge: %s", url, ce)
            raise
        except Exception as ce:
            log.error("fetch_html %s camofox failed too: %s", url, ce)
            raise RuntimeError(f"wreq and camofox both failed for {url}: {ce}") from ce



def _refresh_doc(url: str, title: str = "", label: str = "") -> Dict[str, Any]:
    """Single choke point for a re-fetch: fetch HTML -> ingest into Hister ->
    re-read clean text back from /api/document. Bounded retries per stage.

    If the wreq-cleaned text turns out to be a JS-required/cookie-wall failure shell
    (nature.com returns HTTP 200 + a shell, so no challenge is flagged upstream), and
    the camofox fallback is enabled, escalate to a real-browser render and re-ingest.
    Returns {"text", "source"} (source = wreq|camofox) or raises.
    """
    f = retry(lambda: fetch_html(url), attempts=2, step="refresh.fetch")

    # PDF path (content-type detected upstream): ingest the raw bytes via the
    # dedicated PDF endpoint; clean text is extracted server-side (Hister AddPDF).
    # Camofox escalation below is HTML-only — a PDF is not a JS-render shell.
    if f.get("kind") == "pdf":
        retry(lambda: _hister().add_pdf(url, f["pdf_bytes"], title=title, label=label),
              attempts=2, step="refresh.ingest_pdf")
        doc = retry(lambda: _hister().get_document(url), attempts=3, step="refresh.doc_pdf")
        text = (doc or {}).get("text") or ""
        return {"text": text, "source": f.get("source", "wreq"), "kind": "pdf"}

    retry(lambda: _hister().add(url, f["html"], title=title, label=label),
          attempts=2, step="refresh.ingest")
    doc = retry(lambda: _hister().get_document(url), attempts=3, step="refresh.doc")
    text = (doc or {}).get("text") or ""
    source = f["source"]

    # Escalate 200-shell (wreq) to a real-browser render when the clean text is garbage.
    if _looks_like_failure(text) and source == "wreq" and _camofox_enabled():
        log.warning("refresh %s wreq cleaned text looks like a failure shell (%d ch); "
                    "escalating to camofox", url, len(text))
        try:
            r = retry(lambda: _camofox_render(url), attempts=2, step="refresh.camofox")
        except ManagedChallengeError as ce:
            log.error("refresh %s camofox blocked by managed challenge: %s", url, ce)
        except Exception as ce:
            log.error("refresh %s camofox render failed: %s", url, ce)
        else:
            retry(lambda: _hister().add(url, r["html"], title=title, label=label),
                  attempts=2, step="refresh.ingest_camofox")
            doc = retry(lambda: _hister().get_document(url), attempts=3, step="refresh.doc_camofox")
            ctext = (doc or {}).get("text") or ""
            if not _looks_like_failure(ctext):
                text = ctext
                source = "camofox"
                log.info("refresh %s camofox render produced clean text (%d ch)",
                         url, len(text))
            else:
                log.warning("refresh %s camofox text also looks like a failure shell; "
                            "keeping best-effort wreq text", url)

    return {"text": text, "source": source, "kind": "html"}


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------
def web_recall(query: str, limit: int = 10) -> str:
    """Search ONLY the Hister cache; return clean text via /api/document."""
    t0 = time.time()
    diag = {"source": "hister", "searched": 0, "succeeded": 0, "failed": 0, "failures": []}
    results: List[Dict[str, Any]] = []
    try:
        hits = retry(lambda: _hister().search(query, limit), attempts=3,
                     step="recall.search")
    except Exception as e:
        log.error("recall.search failed: %s", e)
        return json.dumps({"error": f"web_recall: Hister search failed: {e}",
                           "diagnostics": diag})
    diag["searched"] = len(hits)
    for r in hits:
        url = r.get("url", "")
        if not url:
            continue
        try:
            doc = retry(lambda: _hister().get_document(url), attempts=2, step="recall.document")
            text = (doc or {}).get("text") or ""
            if not text.strip():
                raise RuntimeError("empty text in /api/document")
            results.append({"url": url, "title": r.get("title", ""), "text": text,
                            "retrieved_at": _now()})
            diag["succeeded"] += 1
        except Exception as e:
            diag["failed"] += 1
            diag["failures"].append({"url": url, "reason": str(e)})
            log.warning("recall.document failed %s: %s", url, e)
    diag["elapsed_s"] = round(time.time() - t0, 2)
    return json.dumps({"results": results, "diagnostics": diag})


def web_research(query: str, fetch_top: int = 3, limit: int = 10,
                 rerank: bool = False, freshness: Optional[str] = None) -> str:
    """SearXNG + Hister recall -> dedupe -> wreq-fetch uncached -> ingest -> clean text."""
    t0 = time.time()
    diag = {"searxng": 0, "hister": 0, "cached": 0, "fetched": 0, "ingested": 0,
            "failed": [], "reranked": False}
    tr, applied = clients.map_freshness(freshness)
    diag["freshness"] = {"requested": freshness, "time_range": tr, "applied": applied}

    # 1) parallel search: SearXNG + Hister recall
    searx = []
    try:
        searx = retry(lambda: _searxng().search(query, limit, freshness=freshness),
                      attempts=2, step="research.searxng")
        diag["searxng"] = len(searx)
    except Exception as e:
        diag["failed"].append({"stage": "searxng", "reason": str(e)})
        log.error("research.searxng: %s", e)
    hist = []
    try:
        hist = retry(lambda: _hister().search(query, limit), attempts=2, step="research.hister")
        diag["hister"] = len(hist)
    except Exception as e:
        diag["failed"].append({"stage": "hister(recall)", "reason": str(e)})
        log.error("research.hister: %s", e)

    if not searx and not hist:
        return json.dumps({"error": "web_research: both SearXNG and Hister returned nothing",
                           "diagnostics": diag})

    # 1b) drop spam/mirror + SEO-scraper results from the live search feed
    searx, spam_meta = quality.filter_spam(searx, query=query)
    diag["spam_filtered"] = spam_meta.get("spam_filtered", 0)
    diag["spam_bypassed"] = spam_meta.get("bypassed", False)

    cached_urls = {r["url"] for r in hist if r.get("url")}
    diag["cached"] = len(cached_urls)

    # 2) union/dedupe, cache-first: FRESH cached docs come straight from /api/document;
    #    STALE cached docs (updated older than the max age) are queued for auto re-fetch.
    results: Dict[str, Dict[str, Any]] = {}
    stale: Dict[str, Dict[str, Any]] = {}   # url -> {'doc','title'}
    label = os.getenv("WEBRESEARCH_LABEL", "web-research")
    max_age = _refresh_max_age_days()
    for r in hist:
        u = r["url"]
        if u in results or u in stale:
            continue
        try:
            doc = retry(lambda: _hister().get_document(u), attempts=2, step="research.doc_cached")
            text = (doc or {}).get("text") or ""
            if text.strip() and not _looks_like_failure(text):
                if _is_fresh(doc, max_age):
                    results[u] = {"url": u, "title": r.get("title", ""), "text": text,
                                  "source": "cache", "retrieved_at": _now()}
                else:
                    stale[u] = {"doc": doc, "title": r.get("title", "")}
            elif text.strip():
                # cached failure shell -> force a re-fetch (goes through _refresh_doc,
                # which escalates to a camofox render), never serve it as content.
                stale[u] = {"doc": doc, "title": r.get("title", "")}
        except Exception as e:
            diag["failed"].append({"stage": "cached_doc", "url": u, "reason": str(e)})

    # 3) re-fetch stale cache hits (priority) + uncached results, bounded by fetch_top
    pending = [{"url": u, "title": stale[u]["title"], "kind": "stale"} for u in stale]
    pending += [{"url": r["url"], "title": r.get("title", ""), "kind": "new"}
                for r in searx if r["url"] not in results and r["url"] not in stale]
    for item in pending[:fetch_top]:
        u = item["url"]
        kind = item["kind"]
        try:
            res = retry(lambda: _refresh_doc(u, title=item["title"], label=label),
                        attempts=2, step="research.fetch")
            diag["fetched"] += 1
            diag.setdefault("fetch_sources", []).append(res["source"])
            diag["ingested"] += 1
            text = res["text"]
            if text.strip() and not _looks_like_failure(text):
                results[u] = {"url": u, "title": item["title"], "text": text,
                              "source": "fresh" if kind == "new" else "updated",
                              "retrieved_at": _now()}
                if kind == "stale":
                    diag["stale_refreshed"] = diag.get("stale_refreshed", 0) + 1
            else:
                diag["failed"].append({"stage": "failed_shell_or_empty", "url": u})
                log.warning("research.doc_fetched failure shell or empty text after ingest: %s", u)
                if kind == "stale":
                    doc = stale[u].get("doc") or {}
                    sdoc = doc.get("text", "")
                    if sdoc and not _looks_like_failure(sdoc):
                        results[u] = {"url": u, "title": item["title"],
                                      "text": sdoc, "source": "cache-stale",
                                      "retrieved_at": _now()}
                    else:
                        results[u] = {"url": u, "title": item["title"],
                                      "text": "", "source": "cache-stale",
                                      "retrieved_at": _now()}
        except Exception as e:
            diag["failed"].append({"stage": "fetch_ingest", "url": u, "reason": str(e)})
            log.error("research fetch/ingest %s: %s", u, e)
            if kind == "stale":
                doc = stale[u]["doc"]
                results[u] = {"url": u, "title": item["title"],
                              "text": (doc or {}).get("text", ""), "source": "cache-stale",
                              "retrieved_at": _now()}

    # order results: start from cached-recall order then fetched
    ordered = list(results.values())

    # 6) optional rerank
    if rerank:
        rc = _rerank()
        if rc is None:
            diag["failed"].append({"stage": "rerank", "reason": "WEBRESEARCH_RERANK_URL/MODEL not configured"})
            log.warning("research.rerank requested but not configured")
        else:
            try:
                # llama.cpp rerank physical batch cap (now 16384 tokens ≈ 64K chars);
                # keep per-doc truncation bounded so N large docs can't overflow it.
                maxc = int(os.getenv("WEBRESEARCH_RERANK_MAX_CHARS", "4000"))
                texts = [d["text"][:maxc] for d in ordered]
                order = rc.rerank(query, texts)
                reordered = [ordered[i] for i in order if i < len(ordered)]
                if reordered:
                    ordered = reordered
                diag["reranked"] = True
            except Exception as e:
                diag["failed"].append({"stage": "rerank", "reason": str(e)})
                log.error("research.rerank: %s", e)

    # diversify: cap per-domain duplicates, move overflow behind the head
    ordered, div_meta = quality.diversify(ordered)
    diag["diversity_overflow"] = div_meta.get("overflow_count", 0)

    diag["results"] = len(ordered)
    diag["elapsed_s"] = round(time.time() - t0, 2)
    return json.dumps({"results": ordered, "diagnostics": diag})


def web_read(url: str, refresh: bool = False) -> str:
    """Single page: cache-first via /api/document; re-fetches when absent, when
    refresh=True, or when the cached copy is older than the max age (default 10 days)."""
    t0 = time.time()
    diag = {"url": url, "source": None}
    if not refresh:
        try:
            doc = retry(lambda: _hister().get_document(url), attempts=2, step="read.doc")
            text = (doc or {}).get("text") or ""
            if text.strip() and _is_fresh(doc, _refresh_max_age_days()):
                if _looks_like_failure(text):
                    diag["cached_looked_like_failure"] = True
                    log.warning("web_read %s cached text looks like a failure shell; "
                                "re-fetching instead of serving it", url)
                else:
                    diag["source"] = "cache"
                    return json.dumps({"url": url, "text": text,
                                       "retrieved_at": _now(), "diagnostics": diag})
        except Exception as e:
            diag["failed"] = {"stage": "read.doc", "reason": str(e)}

    try:
        res = _refresh_doc(url)
        diag["source"] = "fresh"
        diag["fetch_source"] = res["source"]
        return json.dumps({"url": url, "text": res["text"],
                           "retrieved_at": _now(), "diagnostics": diag})
    except Exception as e:
        log.error("web_read %s: %s", url, e)
        return json.dumps({"error": f"web_read failed: {e}", "diagnostics": diag})


def web_search_core(query: str, limit: int = 10, fetch_top: int = 3,
                    freshness: Optional[str] = None) -> str:
    """Handler backing the built-in `web_search` override.

    Drop-in shape (``{"data":{"web":[{title,url,description,position}]}}``) so existing
    web_search callers keep working, but content is cache-backed:
      - cached results            -> clean Hister text (via /api/document)
      - top `fetch_top` uncached  -> fetched + ingested -> clean text re-read from Hister
      - beyond that               -> discovery-only (title/url/description, NO text)
    Never returns search-engine snippets as citable content.
    """
    t0 = time.time()
    text_chars = int(os.getenv("WEBRESEARCH_SEARCH_TEXT_CHARS", "6000"))
    diag: Dict[str, Any] = {"searxng": 0, "hister": 0, "cached": 0, "fetched": 0,
                            "ingested": 0, "url_only": 0, "failed": [], "elapsed_s": 0}
    tr, applied = clients.map_freshness(freshness)
    diag["freshness"] = {"requested": freshness, "time_range": tr, "applied": applied}

    searx, hist = [], []
    try:
        searx = retry(lambda: _searxng().search(query, limit, freshness=freshness),
                      attempts=2, step="ws.searxng")
        diag["searxng"] = len(searx)
    except Exception as e:
        diag["failed"].append({"stage": "searxng", "reason": str(e)})
        log.error("ws.searxng: %s", e)
    try:
        hist = retry(lambda: _hister().search(query, limit), attempts=2, step="ws.hister")
        diag["hister"] = len(hist)
    except Exception as e:
        diag["failed"].append({"stage": "hister(recall)", "reason": str(e)})
        log.error("ws.hister: %s", e)

    if not searx and not hist:
        return json.dumps({"data": {"web": []},
                           "error": "web_search: no results from SearXNG or Hister",
                           "diagnostics": diag})

    # drop spam/mirror + SEO-scraper results from the live search feed
    searx, spam_meta = quality.filter_spam(searx, query=query)
    diag["spam_filtered"] = spam_meta.get("spam_filtered", 0)
    diag["spam_bypassed"] = spam_meta.get("bypassed", False)

    web: List[Dict[str, Any]] = []
    seen: set = set()

    def emit(title: str, url: str, desc: str = "", text: str = "") -> None:
        if not url or url in seen:
            return
        seen.add(url)
        e: Dict[str, Any] = {"title": title or "", "url": url, "position": len(web) + 1}
        if text.strip() and not _looks_like_failure(text):
            e["text"] = text[:text_chars]
            e["description"] = (desc or text[:200]).strip()
        else:
            e["description"] = desc or ""
            e["cached"] = False
        web.append(e)

    # 1) cache hits -> FRESH ones emit clean text; STALE ones wait for re-fetch below
    stale: Dict[str, Dict[str, Any]] = {}
    max_age = _refresh_max_age_days()
    for r in hist:
        u = r.get("url", "")
        if not u or u in seen or u in stale:
            continue
        try:
            doc = retry(lambda: _hister().get_document(u), attempts=2, step="ws.doc_cached")
            txt = (doc or {}).get("text") or ""
        except Exception:
            txt = ""
            doc = {}
        if txt.strip() and not _looks_like_failure(txt) and _is_fresh(doc, max_age):
            emit(r.get("title", ""), u, text=txt)
        elif txt.strip():
            stale[u] = {"title": r.get("title", ""), "doc": doc}
    diag["cached"] = sum(1 for e in web if e.get("text"))

    # 2) re-fetch stale cache hits (priority) + uncached SearXNG; fetch the top
    #    fetch_top, everything beyond is discovery-only (url_only)
    dlabel = os.getenv("WEBRESEARCH_LABEL", "web-research")
    cands = [{"url": u, "title": stale[u]["title"], "kind": "stale", "desc": ""} for u in stale]
    cands += [{"url": r.get("url"), "title": r.get("title", ""),
               "kind": "new", "desc": r.get("description", "")}
              for r in searx
              if r.get("url") and r.get("url") not in seen and r.get("url") not in stale]
    fetched = 0
    for rec in cands[:limit]:
        u = rec["url"]
        if fetched >= fetch_top:
            emit(rec["title"], u, desc=(rec.get("desc") or "")[:200])
            diag["url_only"] += 1
            continue
        try:
            res = retry(lambda: _refresh_doc(u, title=rec["title"], label=dlabel),
                        attempts=2, step="ws.fetch")
            diag["fetched"] += 1
            diag.setdefault("fetch_sources", []).append(res["source"])
            diag["ingested"] += 1
            txt = res["text"]
            if txt.strip() and not _looks_like_failure(txt):
                emit(rec["title"], u, text=txt)
            else:
                diag["failed"].append({"stage": "failed_shell_or_empty", "url": u})
                emit(rec["title"], u)
            fetched += 1
        except Exception as e:
            diag["failed"].append({"stage": "fetch_ingest", "url": u, "reason": str(e)})
            log.error("ws fetch/ingest %s: %s", u, e)
            if rec["kind"] == "stale":
                emit(rec["title"], u, text=(stale[u].get("doc") or {}).get("text", ""))
            else:
                emit(rec["title"], u)
            fetched += 1

    # diversify: cap per-domain duplicates, move overflow behind the head
    web, div_meta = quality.diversify(web)
    diag["diversity_overflow"] = div_meta.get("overflow_count", 0)

    diag["results"] = len(web)
    diag["elapsed_s"] = round(time.time() - t0, 2)
    return json.dumps({
        "data": {"web": web},
        "note": "Answer ONLY from entries that carry 'text' (cache-backed clean text). "
                "url-only entries are not yet cached — call web_read to fetch them.",
        "diagnostics": diag})
