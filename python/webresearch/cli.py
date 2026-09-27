"""Subprocess entry for the DSH web-research Host plugin.

The Node host plugin (index.js) spawns this script once per tool call and
web-seam request as::

    python3 <this file> <command>

with one JSON object on stdin and one JSON document on stdout. Exit code 0
means the pipeline ran — even when the JSON reports an ``error`` inside, which
is still a tool result; a nonzero exit means the invocation itself failed.

Commands
--------
recall         ``web_recall(query, limit)``           — Hister cache only.
research       ``web_research(query, fetch_top, limit, rerank, freshness)``
read           ``web_read(url)``                      — clean text of one URL.
search         ``web_search_core(query, limit, fetch_top, freshness)``
search_meta    Hister recall + SearXNG, metadata only (ctx.web search seam).
refresh_doc    fetch + ingest one URL, return clean text (ctx.web fetch seam).

All pipeline-heavy work happens inside the ported ``core`` module; this file
only translates CLI I/O and adds the two thin seam adapters.
"""
from __future__ import annotations

import json
import os
import sys

# Always run standalone (``python3 cli.py``), never as part of an installed
# package: import the ported pipeline modules by absolute name. Every module
# ships the Hermes "standalone invocation" import fallback for this.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core import (  # noqa: E402
    _refresh_doc,
    _searxng,
    log as core_log,
    retry,
    web_read,
    web_recall,
    web_research,
    web_search_core,
)
from quality import filter_spam  # noqa: E402

COMMANDS = {
    "recall": web_recall,
    "research": web_research,
    "read": web_read,
    "search": web_search_core,
}


def _call(command: str, args: dict) -> str:
    fn = COMMANDS[command]
    # Pass through only the keyword arguments the pipeline exposes, so tool
    # schema additions degrade gracefully instead of raising TypeError.
    import inspect

    params = inspect.signature(fn).parameters
    kwargs = {k: v for k, v in args.items() if k in params}
    if command == "search" and "refresh" in args and "fetch_top" not in args:
        # Compat: some callers spell the fetch budget "refresh".
        kwargs.setdefault("fetch_top", args["refresh"])
    return fn(**kwargs)


def _search_meta(args: dict) -> str:
    """Metadata-only discovery for the ctx.web search seam.

    Mirrors the Hermes WebResearchSearchProvider contract: Hister recall first
    (the cached, already-cleaned copies), then topped up with SearXNG results
    not already present. No page bodies are fetched here; results carry only
    url/title/snippet, truncated like the Hermes adapter.
    """
    limit = max(1, int(args.get("limit", 8)))
    query = str(args.get("query", ""))
    rows: list = []
    seen: set = set()
    try:
        recall = json.loads(web_recall(query, limit=limit))
        for r in recall.get("results", []):
            url = r.get("url", "")
            if not url or url in seen:
                continue
            seen.add(url)
            rows.append({"url": url, "title": r.get("title", ""),
                         "snippet": (r.get("text") or "")[:300]})
    except Exception as e:  # recall must never break discovery
        core_log.warning("search_meta recall failed: %s", e)
    if len(rows) < limit:
        try:
            searx = retry(lambda: _searxng().search(query, limit),
                          attempts=2, step="search_meta.searxng")
            searx, _ = filter_spam(searx, query=query)
            for r in searx:
                url = r.get("url", "")
                if not url or url in seen:
                    continue
                seen.add(url)
                rows.append({"url": url, "title": r.get("title", ""),
                             "snippet": (r.get("content") or "")[:300]})
                if len(rows) >= limit:
                    break
        except Exception as e:
            core_log.warning("search_meta searxng failed: %s", e)
    got = rows[:limit]
    return json.dumps({"sources": got, "count": len(got)})


def _refresh(args: dict) -> str:
    """Fetch + ingest one URL and return its clean text (ctx.web fetch seam)."""
    url = str(args.get("url", ""))
    if not url:
        raise ValueError("url is required")
    res = _refresh_doc(url)
    return json.dumps({
        "url": url,
        "source": res.get("source", ""),
        "kind": res.get("kind", ""),
        "text": res.get("text", ""),
    })


def main(argv: list) -> int:
    command = argv[1] if len(argv) > 1 else ""
    if command in ("search_meta", "refresh_doc"):
        gen = _search_meta if command == "search_meta" else _refresh
    elif command in COMMANDS:
        gen = lambda args: _call(command, args)  # noqa: E731
    else:
        print(json.dumps({"error": f"cli: unknown command {command!r}; expected one of "
                                   f"{sorted(COMMANDS)} | search_meta | refresh_doc"}))
        return 2

    raw = sys.stdin.read()
    try:
        args = json.loads(raw) if raw.strip() else {}
        if not isinstance(args, dict):
            raise ValueError("args must be a JSON object")
    except Exception as e:
        print(json.dumps({"error": f"cli: invalid stdin args: {e}"}))
        return 2

    try:
        out = gen(args)
        sys.stdout.write(out if isinstance(out, str) else json.dumps(out))
        return 0
    except Exception as e:  # invocation-level failure -> exit code 1
        print(json.dumps({"error": f"cli {command}: {type(e).__name__}: {e}"}))
        return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
