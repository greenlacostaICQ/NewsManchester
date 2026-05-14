"""Prompt versioning: semver bumped manually + content hash auto.

Every prompt in the pipeline is registered here with a human-readable
semver tag. The hash is derived from the prompt text at import time.
Release stage stores both per-run and warns if a hash changed but
semver didn't get bumped — that's the "silent drift" signal.

Add a new entry whenever a new prompt joins the pipeline.
"""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256

from news_digest.pipeline import llm_rewrite as _lr
from news_digest.pipeline import curator as _cur


def _h(text: str) -> str:
    return sha256(text.encode("utf-8")).hexdigest()[:8]


@dataclass(frozen=True, slots=True)
class PromptMeta:
    name: str
    version: str  # semver-ish, bumped manually when intent changes
    hash: str     # SHA-256[:8] of prompt text


# Bump the version field when the prompt's intent changes. Don't bump
# for trivial typo fixes — the hash will reflect those automatically,
# and the release gate will surface them as a silent-drift signal.
PROMPTS: tuple[PromptMeta, ...] = (
    PromptMeta(name="curator",         version="v3", hash=_h(_cur.CURATOR_PROMPT)),
    PromptMeta(name="city_news",       version="v3", hash=_h(_lr.PROMPT_CITY_NEWS)),
    PromptMeta(name="transport",       version="v2", hash=_h(_lr.PROMPT_TRANSPORT)),
    PromptMeta(name="events",          version="v2", hash=_h(_lr.PROMPT_EVENTS)),
    PromptMeta(name="diaspora_events", version="v1", hash=_h(_lr.PROMPT_DIASPORA_EVENTS)),
    PromptMeta(name="business",        version="v2", hash=_h(_lr.PROMPT_BUSINESS)),
    PromptMeta(name="football",        version="v2", hash=_h(_lr.PROMPT_FOOTBALL)),
    PromptMeta(name="fix_translate",   version="v1", hash=_h(_lr.FIX_TRANSLATE_SYSTEM)),
    PromptMeta(name="repair_draft",    version="v1", hash=_h(_lr.REPAIR_DRAFT_SYSTEM)),
)


def snapshot() -> list[dict[str, str]]:
    return [{"name": p.name, "version": p.version, "hash": p.hash} for p in PROMPTS]


def by_name() -> dict[str, PromptMeta]:
    return {p.name: p for p in PROMPTS}


def prompt_name_for(prompt_text: str) -> str:
    """Lookup the registered name for a prompt text. Used by llm_rewrite
    so the cost tracker can tag each call with a stable prompt name."""
    digest = _h(prompt_text)
    for p in PROMPTS:
        if p.hash == digest:
            return p.name
    return "unknown"
