"""web-research typed error taxonomy.

Every pipeline stage raises a subclass of ``WebResearchError`` carrying
structured fields (``stage``, ``provider``, ``url``) so callers and the
``diagnostics``/receipt dicts get a stable, machine-readable failure instead of
a bare string. Each error exposes ``to_dict()`` for logging and JSON output.

Standalone-import safe (stdlib-only, no package deps) so it can be imported
from ``clients.py``, ``camofox.py`` and ``core.py`` under any interpreter and
under the plain ``import clients`` fallback path used by tests.
"""
from __future__ import annotations

from typing import Optional


class WebResearchError(RuntimeError):
    """Base class for all web-research pipeline failures."""

    stage: str = "web-research"
    provider: Optional[str] = None
    url: Optional[str] = None

    def __init__(self, message: str = "", *,
                 stage: Optional[str] = None,
                 provider: Optional[str] = None,
                 url: Optional[str] = None,
                 cause: Optional[BaseException] = None):
        self.stage = stage or self.stage
        self.provider = provider or self.provider
        self.url = url
        self.cause = cause
        msg = message or self.__class__.__name__
        if cause is not None and str(cause) and str(cause) not in str(msg):
            msg = f"{msg} (caused by {type(cause).__name__}: {cause})"
        super().__init__(msg)

    def to_dict(self) -> dict:
        d = {"stage": self.stage, "error": str(self)}
        if self.provider:
            d["provider"] = self.provider
        if self.url:
            d["url"] = self.url
        return d


class ConfigError(WebResearchError):
    """A required setting/path (env var, executable, endpoint) is missing."""
    stage = "config"


class SearchError(WebResearchError):
    """A search stage failed at the provider level."""
    stage = "search"


class SearxNGSearchError(SearchError):
    provider = "searxng"


class HisterSearchError(SearchError):
    provider = "hister"


class FetchError(WebResearchError):
    """Fetching a page's HTML failed across the available fetch layers."""
    stage = "fetch"


class WreqFetchError(FetchError):
    provider = "wreq"


class CamofoxRenderError(FetchError):
    provider = "camofox"


class IngestError(WebResearchError):
    """Writing raw HTML into the Hister cache failed."""
    stage = "ingest"
    provider = "hister"


class DocumentReadError(WebResearchError):
    """Reading clean full-text back from Hister /api/document failed."""
    stage = "document"
    provider = "hister"


class RerankError(WebResearchError):
    """Local rerank stage failed (does not fail the tool; reported only)."""
    stage = "rerank"


class RetryExhausted(WebResearchError):
    """All attempts of a bounded retry loop failed."""
    stage = "retry"
