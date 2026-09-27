"""Camofox anti-detect browser render client (REST, v2.4.7, port 9377).

render(url) -> {"final_url": str, "html": str} producing the FULL DOM html after
the page settles. Polls until any bot-challenge interstitial (Cloudflare "Just a
moment..." etc.) clears; if it persists it raises ManagedChallengeError so the
caller surfaces a manual-action message instead of returning an interstitial as
if it were content.
"""
from __future__ import annotations

import json
import os
import time as _time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict

try:
    from .errors import FetchError
except ImportError:  # standalone / plain `import camofox` fallback
    from errors import FetchError


class ManagedChallengeError(FetchError):
    """A bot-challenge interstitial the headless pipeline could not auto-solve.
    Inherits FetchError (stage='fetch') so callers can catch it as a fetch
    failure while still distinguishing an unsolved challenge from other errors."""
    stage = "fetch"


class CamofoxClient:
    CHALLENGE_MARKERS = ("just a moment", "__cf_", "cf-chl", "cf_chl",
                         "captcha-delivery", "attention required")

    def __init__(self, base_url: str, api_key: str,
                 challenge_timeout: int = 25, log=None, cookies_file: str = ""):
        self.base_url = base_url.rstrip("/")
        self.key = api_key
        self.challenge_timeout = challenge_timeout
        self.log = log
        self.cookies_file = cookies_file

    @staticmethod
    def _load_cookies(path: str, host: str) -> list:
        """Parse a Netscape cookies file, keep only rows for the target host.

        A cookie applies when host == cookie_domain, or when the cookie's
        include-subdomains flag is TRUE and host is a suffix of the domain.
        """
        out = []
        if not path or not os.path.exists(path):
            return out
        host = (host or "").split(":")[0].lower()
        for line in open(path, encoding="utf-8", errors="replace"):
            line = line.rstrip("\n")
            if not line or line.startswith("#"):
                continue
            p = line.split("\t")
            if len(p) < 7:
                continue
            dom, inc_sub, cpath, sec, exp, name, val = p[:7]
            d = dom.lstrip(".").lower()
            applies = host == d or (inc_sub == "TRUE" and host.endswith("." + d))
            if not applies:
                continue
            out.append({
                "name": name, "value": val, "domain": dom, "path": cpath or "/",
                "secure": sec == "TRUE",
                "httpOnly": False,
                "expires": int(float(exp)) if exp.strip().lstrip("-").isdigit() else -1,
            })
        return out

    def _inject_cookies(self, uid: str, url: str) -> int:
        cks = self._load_cookies(self.cookies_file, urllib.parse.urlparse(url).hostname or "")
        if not cks:
            return 0
        st, resp = self._req("POST", f"/sessions/{uid}/cookies", {"cookies": cks})
        if st != 200:
            self._msg(f"cookie inject failed ({st}): {resp}")
            return 0
        self._msg(f"injected {len(cks)} cookies for {url}")
        return len(cks)

    def _msg(self, m: str) -> None:
        if self.log:
            self.log(m)

    def _req(self, method: str, path: str, body=None, timeout: int = 60):
        h = {"Authorization": f"Bearer {self.key}"}
        data = None
        if body is not None:
            h["Content-Type"] = "application/json"
            data = json.dumps(body).encode()
        req = urllib.request.Request(self.base_url + path, data=data,
                                     headers=h, method=method)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.status, json.loads(r.read().decode("utf-8", "replace"))
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read().decode("utf-8", "replace"))

    def _evaluate(self, uid: str, tab_id: str, expr: str) -> str:
        _, r = self._req("POST", f"/tabs/{tab_id}/evaluate",
                         {"userId": uid, "expression": expr, "timeout": 20000})
        return r.get("result", "")

    # Camofox caps evaluate results at 1 MiB (MAX_RESULT_SIZE=1048576 in
    # tab.js) and returns a "[Truncated: result was N bytes, max 1048576]"
    # marker instead of the value. Any expression that can return a wall of
    # text (outerHTML of a heavy page) must be read in slices or it silently
    # comes back truncated. Stay well under the cap per call.
    _DOM_CHUNK = 768 * 1024  # chars; ~3/4 MiB of result, safe vs 1MiB cap

    def _read_dom(self, uid: str, tab_id: str, expr: str) -> str:
        """Evaluate `expr` (a string expression) in `_DOM_CHUNK`-sized slices
        and concatenate, so large results are never capped by camofox.

        Only valid when `expr` evaluates to a string with stable length
        across calls (e.g. ``document.documentElement.outerHTML``). The
        caller first probes ``<expr>.length`` to drive the loop bound.
        """
        try:
            total = int(self._evaluate(uid, tab_id, f"({expr}).length") or 0)
        except (TypeError, ValueError):
            total = 0
        parts = []
        off = 0
        while off < total:
            end = min(off + self._DOM_CHUNK, total)
            piece = self._evaluate(
                uid, tab_id, f"({expr}).substring({off}, {end})") or ""
            parts.append(piece)
            # Safety: if a slice comes back empty (or shorter than asked due
            # to a race/restart) don't loop forever on `off` not advancing.
            if len(piece) == 0:
                break
            off += self._DOM_CHUNK
        return "".join(parts)

    # The challenge sniff only needs a small sample of the page, not the whole
    # DOM — reading first 600 chars is cheap and identical to before.
    @staticmethod
    def _is_challenge(title: str, html: str) -> bool:
        s = (title + " " + " ".join(html[:600].split())).lower()
        return any(m in s for m in CamofoxClient.CHALLENGE_MARKERS)

    def render(self, url: str, timeout: int = 60) -> Dict[str, Any]:
        uid = f"wr_{int(_time.time() * 1000)}"
        try:
            st, tab = self._req("POST", "/tabs",
                                {"userId": uid, "sessionKey": f"sk_{uid}", "url": url},
                                timeout=timeout)
            tab_id = tab.get("tabId") if isinstance(tab, dict) else None
            if st != 200 or not tab_id:
                raise RuntimeError(f"camofox create tab failed ({st}): {tab}")

            # inject session cookies (e.g. for gated but non-CF-challenged pages)
            n = self._inject_cookies(uid, url)
            if n:
                self._msg(f"injected {n} cookies")

            self._req("POST", f"/tabs/{tab_id}/navigate",
                      {"userId": uid, "url": url}, timeout=timeout)
            self._req("POST", f"/tabs/{tab_id}/wait",
                      {"userId": uid, "waitForNetwork": True, "timeout": 20000}, timeout=timeout)

            deadline = _time.time() + self.challenge_timeout
            last_title = ""
            while _time.time() < deadline:
                title = self._evaluate(uid, tab_id, "document.title") or ""
                # Cheap sniff: only sample a bit of the DOM for challenge text
                # while polling; the full DOM is read (chunked) once it clears.
                sample = self._evaluate(
                    uid, tab_id, "document.documentElement.outerHTML.substring(0,700)") or ""
                last_title = title
                if not self._is_challenge(title, sample):
                    html = self._read_dom(
                        uid, tab_id, "document.documentElement.outerHTML")
                    final_url = self._evaluate(uid, tab_id, "location.href")
                    return {"final_url": final_url, "html": html, "rendered": True}
                _time.sleep(2.5)

            raise ManagedChallengeError(
                f"managed bot-challenge not auto-solved in {self.challenge_timeout}s "
                f"(title={last_title!r}); needs manual/headful action or a trusted IP")
        finally:
            try:
                self._req("DELETE", f"/sessions/{uid}")
            except Exception:
                pass
