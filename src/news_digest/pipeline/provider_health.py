"""In-run circuit breaker for LLM providers.

When a provider returns zero successful items twice in a row within a
single pipeline stage, mark it dead-for-this-run so subsequent calls
skip the provider and fall straight through to the next route tier
without paying another timeout. State is module-level: a fresh import
(e.g. between GH-Actions steps) starts with an empty breaker, which
is the desired behaviour because each pipeline stage is a separate
Python invocation.

Failure signal is the absence of any successful response from the
provider for a batch — covers timeouts, auth failures, 5xx, and other
exceptions caught inside the provider call. Prompt-level decode errors
that still yield SOME results do not count as failures.
"""
from __future__ import annotations

import logging
import threading

logger = logging.getLogger(__name__)

_FAILURE_THRESHOLD = 2

_LOCK = threading.Lock()
_DEAD_FOR_THIS_RUN: set[str] = set()
_FAILURE_COUNTS: dict[str, int] = {}


def is_dead(provider: str) -> bool:
    """Whether this provider has been marked dead for the current run."""
    with _LOCK:
        return provider in _DEAD_FOR_THIS_RUN


def record_failure(provider: str) -> bool:
    """Bump consecutive failure count; mark dead at threshold.

    Returns True iff the provider just transitioned to dead-for-this-run.
    """
    with _LOCK:
        if provider in _DEAD_FOR_THIS_RUN:
            return False
        count = _FAILURE_COUNTS.get(provider, 0) + 1
        _FAILURE_COUNTS[provider] = count
        if count >= _FAILURE_THRESHOLD:
            _DEAD_FOR_THIS_RUN.add(provider)
            logger.warning(
                "provider_health: %s reached %d consecutive failures — skipping for the rest of this run.",
                provider, count,
            )
            return True
        return False


def record_success(provider: str) -> None:
    """Reset the consecutive failure counter on a successful call."""
    with _LOCK:
        _FAILURE_COUNTS.pop(provider, None)


def reset() -> None:
    """Clear all state — used by tests and stage entry points."""
    with _LOCK:
        _DEAD_FOR_THIS_RUN.clear()
        _FAILURE_COUNTS.clear()


def dead_providers() -> list[str]:
    """Snapshot of currently-dead providers for reporting."""
    with _LOCK:
        return sorted(_DEAD_FOR_THIS_RUN)
