"""HTTP fetching with browser-shaped headers and per-source overrides.

`_fetch_text` is the low-level call (single URL, default headers
overridable). `_fetch_source_body` adds the primary-then-fallback
strategy declared on `SourceDef.fallback_urls`.
"""

from __future__ import annotations

import os
import time
from datetime import UTC, timedelta
from urllib import error, request

from news_digest.pipeline.common import now_london

from .sources import SourceDef


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


def _fetch_text(url: str, *, extra_headers: dict[str, str] | None = None) -> str:
    """Fetch a URL once, retry once on transient `URLError` (timeout, DNS).

    HTTPError (server-side 4xx/5xx) is treated as definitive and not
    retried — fallback URLs handle that case at the source level.
    """

    headers = dict(_DEFAULT_FETCH_HEADERS)
    if extra_headers:
        headers.update(extra_headers)
    req = request.Request(url, headers=headers)
    last_url_error: Exception | None = None
    for attempt in range(2):  # 1 initial + 1 retry
        try:
            with request.urlopen(req, timeout=20) as response:
                raw = response.read(1_500_000)
                charset = response.headers.get_content_charset() or "utf-8"
                return raw.decode(charset, errors="replace")
        except error.HTTPError as exc:
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
