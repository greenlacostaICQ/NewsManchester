"""HTTP fetching with browser-shaped headers and per-source overrides.

`_fetch_text` is the low-level call (single URL, default headers
overridable). `_fetch_source_body` adds the primary-then-fallback
strategy declared on `SourceDef.fallback_urls`.

Optional ETag / Last-Modified cache (module-level): when called with
``use_cache=True``, conditional headers (``If-None-Match`` /
``If-Modified-Since``) are sent based on the previous response and a
304 response raises ``NotModified`` instead of returning a body. The
cache file is loaded once via ``load_fetch_cache(state_dir)`` at the
start of a collector run and flushed via ``save_fetch_cache(state_dir)``
at the end. Used only for the per-source RSS / HTML fetch (core.py),
NOT for one-shot article enrichment in extract.py — those keep their
original "fetch every time" behaviour so a 304 never strands an
enrichment fetch with no body.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from http import cookiejar
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib import error, parse, request

from news_digest.pipeline.common import now_london

from .sources import SourceDef

logger = logging.getLogger(__name__)


class NotModified(Exception):
    """Server returned HTTP 304 — cached version is still current."""


# Cache layout: {url: {"etag": str, "last_modified": str,
#                       "fetched_at": ISO8601, "status": "200" | "304"}}.
# Module-level singleton because the collector is single-process; the
# lock makes the dict safe under ``ThreadPoolExecutor`` in core.py.
_FETCH_CACHE: dict[str, dict] = {}
_FETCH_CACHE_LOCK = threading.Lock()
_FETCH_CACHE_FILENAME = "fetch_cache.json"

# Daily pipeline means yesterday's cache entries are ~24h old. The TTL
# must exceed that to give any value at all. 7 days is a safe window:
# long enough for normal daily runs to benefit, short enough that an
# upstream ETag-scheme change heals naturally within a week.
_FETCH_CACHE_TTL_SECONDS = 7 * 24 * 3600


# Hosts that block urllib via Cloudflare bot challenge but pass through
# curl_cffi's Chrome TLS-fingerprint impersonation. Add a host here when
# urllib gets a clean 403/503 and curl_cffi returns 200 for the same URL.
_CLOUDFLARE_PROTECTED_HOSTS: tuple[str, ...] = (
    "gmp.police.uk",
    "mancity.com",
    "manchester2-search.funnelback.squiz.cloud",
    "news.salford.gov.uk",
    "salford.gov.uk",
    "trafford.gov.uk",
    "tameside.gov.uk",
    "wigan.gov.uk",
    "rncm.ac.uk",
    "eventbrite.co.uk",
    "eventbrite.com",
    "visitsalford.info",
)


def _host_is_cloudflare_protected(url: str) -> bool:
    try:
        host = parse.urlparse(url).hostname or ""
    except Exception:  # noqa: BLE001
        return False
    return any(host == h or host.endswith("." + h) for h in _CLOUDFLARE_PROTECTED_HOSTS)


def _resolve_url(url: str) -> str:
    """Expand {ENV_VAR} placeholders in URLs from environment."""
    import re

    now_utc = now_london().astimezone(UTC)
    dynamic_values = {
        "NOW_UTC": now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "NOW_MINUS_1D_UTC": (now_utc - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "PLUS_14D_UTC": (now_utc + timedelta(days=14)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "PLUS_30D_UTC": (now_utc + timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "PLUS_365D_UTC": (now_utc + timedelta(days=365)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "PLUS_540D_UTC": (now_utc + timedelta(days=540)).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    return re.sub(
        r"\{([A-Z0-9_]+)\}",
        lambda m: dynamic_values.get(m.group(1), os.environ.get(m.group(1), m.group(0))),
        url,
    )


# ── ETag / Last-Modified cache ──────────────────────────────────────────


def load_fetch_cache(state_dir: Path) -> None:
    """Load the on-disk cache into the module-level dict. Called once
    at the start of a collector run before any fetches."""
    path = state_dir / _FETCH_CACHE_FILENAME
    with _FETCH_CACHE_LOCK:
        _FETCH_CACHE.clear()
        if not path.exists():
            return
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("fetch_cache.json unreadable, ignoring (%s).", exc)
            return
        entries = raw.get("entries") if isinstance(raw, dict) else None
        if isinstance(entries, dict):
            _FETCH_CACHE.update({k: v for k, v in entries.items() if isinstance(v, dict)})


def save_fetch_cache(state_dir: Path) -> None:
    """Persist the module-level cache to disk. Called once at the end of
    a collector run after all fetches complete."""
    state_dir.mkdir(parents=True, exist_ok=True)
    path = state_dir / _FETCH_CACHE_FILENAME
    with _FETCH_CACHE_LOCK:
        payload = {
            "version": 1,
            "updated_at_london": now_london().isoformat(),
            "entries": dict(_FETCH_CACHE),
        }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _cache_entry_fresh(entry: dict) -> bool:
    """True if the entry has validators AND is younger than the TTL."""
    if not entry or not (entry.get("etag") or entry.get("last_modified")):
        return False
    fetched_at = entry.get("fetched_at") or ""
    if not fetched_at:
        return False
    try:
        parsed = datetime.fromisoformat(fetched_at)
    except ValueError:
        return False
    now = now_london()
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=now.tzinfo)
    age = (now - parsed).total_seconds()
    return 0 <= age <= _FETCH_CACHE_TTL_SECONDS


def _conditional_headers(url: str) -> dict[str, str]:
    """Return If-None-Match / If-Modified-Since headers for url, if we
    have fresh cached validators. Empty dict otherwise."""
    with _FETCH_CACHE_LOCK:
        entry = dict(_FETCH_CACHE.get(url) or {})
    if not _cache_entry_fresh(entry):
        return {}
    headers: dict[str, str] = {}
    if entry.get("etag"):
        headers["If-None-Match"] = entry["etag"]
    if entry.get("last_modified"):
        headers["If-Modified-Since"] = entry["last_modified"]
    return headers


def _store_cache_entry(url: str, *, etag: str = "", last_modified: str = "", status: str = "200") -> None:
    """Update the module cache for url with new validators."""
    with _FETCH_CACHE_LOCK:
        existing = dict(_FETCH_CACHE.get(url) or {})
        if etag:
            existing["etag"] = etag
        if last_modified:
            existing["last_modified"] = last_modified
        existing["fetched_at"] = now_london().isoformat()
        existing["status"] = status
        _FETCH_CACHE[url] = existing


# Transient network errors (timeout, DNS hiccup) will be retried once
# with a short backoff. ITV Granada in particular is intermittently
# slow and a 1-shot retry is enough to make most morning runs clean.
_FETCH_RETRY_BACKOFF_SECONDS = 1.5


# Some sources (notably gmp.police.uk) reject the previous bot-shaped UA
# string with HTTP 403. Use a plain browser-shaped User-Agent and add
# Accept-Language so WAF rules that gate on "looks like real traffic" are
# happier. Identification is preserved via the X-Source-Tag header so
# admins can still trace the bot in their logs if needed.
_DEFAULT_FETCH_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
    "X-Source-Tag": "MNewsDigestBot/1.1",
}


def _fetch_text_curl_cffi(url: str, headers: dict[str, str], *, use_cache: bool = False) -> str:
    """Fetch via curl_cffi using Chrome TLS fingerprint.

    Used for hosts that block urllib's default TLS handshake via
    Cloudflare bot challenge. Falls back to a single retry on transient
    network errors to match the urllib path's resilience.

    We strip our default browser-shaped headers (UA, Accept, Accept-Language)
    so curl_cffi can use its own impersonation-matched values. Source-specific
    headers like Referer/Origin are preserved.

    With ``use_cache=True``, conditional headers (If-None-Match /
    If-Modified-Since) are sent when we have fresh validators, and a 304
    response raises ``NotModified``. Sending validators also makes our
    traffic look more like a real cache-aware browser, which sometimes
    keeps a borderline Cloudflare rule from firing.

    HISTORY: 2026-05-15 added Sec-Fetch-* headers and a longer profile
    cascade (chrome131/safari18_0/firefox133/...) thinking it would help
    against newer Cloudflare rules. Result: Eventbrite Manchester
    + Markets + Trafford Council went from OK/stale to HTTP 405/403
    overnight. Reverted to the pre-2026-05-15 cascade and dropped the
    Sec-Fetch-* headers. 429 Retry-After handling is the ONLY new
    behaviour that survived the revert — it doesn't change the request
    shape, it just respects the server's throttle hint.
    """
    from curl_cffi import requests as cffi_requests  # noqa: PLC0415

    cffi_headers = {
        k: v for k, v in headers.items()
        if k not in {"User-Agent", "Accept", "Accept-Language", "X-Source-Tag"}
    }
    if use_cache:
        cffi_headers.update(_conditional_headers(url))

    # Some WAFs (notably gmp.police.uk on non-residential IP ranges like
    # GitHub Actions runners) block the default Chrome fingerprint and
    # accept a different one. Try a small cascade before giving up.
    profiles = ("chrome", "chrome120", "safari17_0")
    last_exc: Exception | None = None
    for profile in profiles:
        for attempt in range(2):
            try:
                response = cffi_requests.get(
                    url,
                    headers=cffi_headers,
                    impersonate=profile,
                    timeout=30,
                )
                if use_cache and response.status_code == 304:
                    _store_cache_entry(url, status="304")
                    raise NotModified(url)
                if response.status_code >= 400:
                    # 429 — honour Retry-After once, then surface the error.
                    if response.status_code == 429:
                        retry_after = response.headers.get("Retry-After", "")
                        try:
                            wait = min(int(retry_after), 30) if retry_after.isdigit() else 5
                        except (ValueError, AttributeError):
                            wait = 5
                        time.sleep(max(wait, 1))
                    raise RuntimeError(f"HTTP {response.status_code}")
                if use_cache:
                    _store_cache_entry(
                        url,
                        etag=str(response.headers.get("ETag", "") or ""),
                        last_modified=str(response.headers.get("Last-Modified", "") or ""),
                        status="200",
                    )
                return response.text
            except NotModified:
                raise
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                msg = str(exc)
                if attempt == 0 and not msg.startswith("HTTP "):
                    time.sleep(_FETCH_RETRY_BACKOFF_SECONDS)
                    continue
                break
    raise RuntimeError(str(last_exc) if last_exc else "curl_cffi fetch failed")


def _fetch_text(url: str, *, extra_headers: dict[str, str] | None = None, use_cache: bool = False) -> str:
    """Fetch a URL once, retry once on transient `URLError` (timeout, DNS).

    HTTPError (server-side 4xx/5xx) is treated as definitive and not
    retried — fallback URLs handle that case at the source level.

    For known Cloudflare-protected hosts we route through curl_cffi with
    a Chrome TLS-fingerprint impersonation — those hosts return 403/503
    to urllib's bare TLS handshake even with browser-shaped headers.

    With ``use_cache=True``, conditional headers are sent and 304
    responses raise ``NotModified``. Default is False so one-shot
    callers (article enrichment, weather, fallbacks) keep their existing
    semantics.
    """

    headers = dict(_DEFAULT_FETCH_HEADERS)
    if extra_headers:
        headers.update(extra_headers)
    if use_cache:
        headers.update(_conditional_headers(url))

    if _host_is_cloudflare_protected(url):
        return _fetch_text_curl_cffi(url, headers, use_cache=use_cache)

    current_url = url
    req = request.Request(current_url, headers=headers)
    cookies = cookiejar.CookieJar()
    opener = request.build_opener(request.HTTPCookieProcessor(cookies))
    last_url_error: Exception | None = None
    rate_limit_attempted = False
    redirect_count = 0
    for attempt in range(6):  # 1 initial + redirects/retry
        try:
            with opener.open(req, timeout=30) as response:
                if use_cache:
                    _store_cache_entry(
                        current_url,
                        etag=str(response.headers.get("ETag", "") or ""),
                        last_modified=str(response.headers.get("Last-Modified", "") or ""),
                        status="200",
                    )
                # 4MB cap: Stockport Events RSS alone is ~1.5MB; MEN front
                # page can exceed 900KB with images. 1.5MB cut mid-tag on
                # bigger RSS feeds and broke the XML parser silently.
                raw = response.read(4_000_000)
                charset = response.headers.get_content_charset() or "utf-8"
                return raw.decode(charset, errors="replace")
        except error.HTTPError as exc:
            # 304 Not Modified — cache is current. urllib surfaces it as
            # HTTPError; we re-raise as NotModified so the caller can
            # distinguish "fresh cache" from "real failure".
            if use_cache and exc.code == 304:
                _store_cache_entry(current_url, status="304")
                raise NotModified(current_url) from exc
            if exc.code in {301, 302, 303, 307, 308}:
                location = exc.headers.get("Location", "") if exc.headers else ""
                if location and redirect_count < 4:
                    redirect_count += 1
                    current_url = parse.urljoin(current_url, location)
                    req = request.Request(current_url, headers=headers)
                    continue
            # Honour Retry-After on 429 (rate limit) — Ticketmaster's API
            # uses it on every rate-limit response. One retry after the
            # advertised wait usually clears it. Cap the wait at 30s so
            # we don't stall the whole digest run on a hung throttle.
            if exc.code == 429 and not rate_limit_attempted:
                rate_limit_attempted = True
                retry_after = exc.headers.get("Retry-After", "") if exc.headers else ""
                try:
                    wait = min(int(retry_after), 30) if retry_after.isdigit() else 5
                except (ValueError, AttributeError):
                    wait = 5
                time.sleep(max(wait, 1))
                continue
            # Definitive HTTP error — don't retry, let _fetch_source_body
            # try the next fallback URL instead.
            raise RuntimeError(f"HTTP {exc.code}") from exc
        except error.URLError as exc:
            last_url_error = exc
            if attempt == 0:
                time.sleep(_FETCH_RETRY_BACKOFF_SECONDS)
                continue
            raise RuntimeError(str(exc.reason)) from exc
    # Defensive — loop above always returns or raises.
    if last_url_error is not None:
        raise RuntimeError(str(last_url_error))
    raise RuntimeError("Fetch failed without a captured exception")


def _source_fetch_headers(source: SourceDef) -> dict[str, str]:
    headers: dict[str, str] = {}
    if source.name == "Manchester Council":
        headers.update(
            {
                "Accept": "application/json, text/plain, */*",
                "Referer": "https://www.manchester.gov.uk/news-stories",
                "Origin": "https://www.manchester.gov.uk",
            }
        )
    return headers


def _fetch_source_body(source: SourceDef) -> tuple[str, str, list[str]]:
    """Fetch a source body, trying primary URL then any configured fallbacks.

    Returns a tuple of (body, fetched_url, attempt_log) where attempt_log
    is a list of human-readable failure notes for URLs we tried before
    succeeding (empty if the primary URL worked). Raises the last
    exception if everything fails.

    Uses the ETag / Last-Modified cache (``use_cache=True``). When the
    server returns 304, ``NotModified`` propagates to the caller —
    collector.core treats it as a real "no new content" signal, distinct
    from a fetch failure.
    """

    attempt_log: list[str] = []
    last_exception: Exception | None = None
    source_headers = _source_fetch_headers(source)
    for candidate_url in (_resolve_url(source.url), *[_resolve_url(u) for u in source.fallback_urls]):
        try:
            body = _fetch_text(candidate_url, extra_headers=source_headers, use_cache=True)
            return body, candidate_url, attempt_log
        except NotModified:
            # Propagate — caller distinguishes "not modified" from "failed".
            raise
        except Exception as exc:  # noqa: BLE001 - all failures are recorded.
            attempt_log.append(f"{candidate_url}: {exc}")
            last_exception = exc
            continue
    if last_exception is None:
        raise RuntimeError("Source has no URLs configured")
    raise last_exception
