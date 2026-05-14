"""HTTP fetching with browser-shaped headers and per-source overrides.

`_fetch_text` is the low-level call (single URL, default headers
overridable). `_fetch_source_body` adds the primary-then-fallback
strategy declared on `SourceDef.fallback_urls`.
"""

from __future__ import annotations

import os
import time
from datetime import UTC, timedelta
from urllib import error, parse, request

from news_digest.pipeline.common import now_london

from .sources import SourceDef


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
    "eventbrite.co.uk",
    "eventbrite.com",
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
    }

    return re.sub(
        r"\{([A-Z0-9_]+)\}",
        lambda m: dynamic_values.get(m.group(1), os.environ.get(m.group(1), m.group(0))),
        url,
    )


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


def _fetch_text_curl_cffi(url: str, headers: dict[str, str]) -> str:
    """Fetch via curl_cffi using Chrome TLS fingerprint.

    Used for hosts that block urllib's default TLS handshake via
    Cloudflare bot challenge. Falls back to a single retry on transient
    network errors to match the urllib path's resilience.

    We strip our default browser-shaped headers (UA, Accept, Accept-Language)
    so curl_cffi can use its own impersonation-matched values. Source-specific
    headers like Referer/Origin are preserved.
    """
    from curl_cffi import requests as cffi_requests  # noqa: PLC0415

    cffi_headers = {
        k: v for k, v in headers.items()
        if k not in {"User-Agent", "Accept", "Accept-Language", "X-Source-Tag"}
    }

    last_exc: Exception | None = None
    for attempt in range(2):
        try:
            response = cffi_requests.get(
                url,
                headers=cffi_headers,
                impersonate="chrome",
                timeout=20,
            )
            if response.status_code >= 400:
                raise RuntimeError(f"HTTP {response.status_code}")
            return response.text
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt == 0 and not str(exc).startswith("HTTP "):
                time.sleep(_FETCH_RETRY_BACKOFF_SECONDS)
                continue
            raise
    raise RuntimeError(str(last_exc) if last_exc else "curl_cffi fetch failed")


def _fetch_text(url: str, *, extra_headers: dict[str, str] | None = None) -> str:
    """Fetch a URL once, retry once on transient `URLError` (timeout, DNS).

    HTTPError (server-side 4xx/5xx) is treated as definitive and not
    retried — fallback URLs handle that case at the source level.

    For known Cloudflare-protected hosts we route through curl_cffi with
    a Chrome TLS-fingerprint impersonation — those hosts return 403/503
    to urllib's bare TLS handshake even with browser-shaped headers.
    """

    headers = dict(_DEFAULT_FETCH_HEADERS)
    if extra_headers:
        headers.update(extra_headers)

    if _host_is_cloudflare_protected(url):
        return _fetch_text_curl_cffi(url, headers)

    req = request.Request(url, headers=headers)
    last_url_error: Exception | None = None
    for attempt in range(2):  # 1 initial + 1 retry
        try:
            with request.urlopen(req, timeout=20) as response:
                # 4MB cap: Stockport Events RSS alone is ~1.5MB; MEN front
                # page can exceed 900KB with images. 1.5MB cut mid-tag on
                # bigger RSS feeds and broke the XML parser silently.
                raw = response.read(4_000_000)
                charset = response.headers.get_content_charset() or "utf-8"
                return raw.decode(charset, errors="replace")
        except error.HTTPError as exc:
            if exc.code in {403, 405, 429, 503}:
                try:
                    return _fetch_text_curl_cffi(url, headers)
                except Exception as cffi_exc:  # noqa: BLE001
                    raise RuntimeError(f"HTTP {exc.code}; curl_cffi fallback failed: {cffi_exc}") from cffi_exc
            # Other definitive HTTP errors are not retried; let
            # _fetch_source_body try the next fallback URL instead.
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
    elif source.name in {"GMP", "Salford Council", "Trafford Council", "Tameside Council", "Wigan Council"}:
        headers.update(
            {
                "Accept-Language": "en-GB,en;q=0.9",
                "Referer": source.url,
            }
        )
    elif "Eventbrite" in source.name:
        headers.update(
            {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-GB,en;q=0.9",
                "Referer": "https://www.eventbrite.co.uk/",
            }
        )
    return headers


def _fetch_source_body(source: SourceDef) -> tuple[str, str, list[str]]:
    """Fetch a source body, trying primary URL then any configured fallbacks.

    Returns a tuple of (body, fetched_url, attempt_log) where attempt_log
    is a list of human-readable failure notes for URLs we tried before
    succeeding (empty if the primary URL worked). Raises the last
    exception if everything fails.
    """

    attempt_log: list[str] = []
    last_exception: Exception | None = None
    source_headers = _source_fetch_headers(source)
    for candidate_url in (_resolve_url(source.url), *[_resolve_url(u) for u in source.fallback_urls]):
        try:
            body = _fetch_text(candidate_url, extra_headers=source_headers)
            return body, candidate_url, attempt_log
        except Exception as exc:  # noqa: BLE001 - all failures are recorded.
            attempt_log.append(f"{candidate_url}: {exc}")
            last_exception = exc
            continue
    if last_exception is None:
        raise RuntimeError("Source has no URLs configured")
    raise last_exception
