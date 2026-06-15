"""Prompt versioning: semver bumped manually + content hash auto.

Every prompt in the pipeline is registered here with a human-readable
semver tag. The hash is derived from the prompt text at import time.
Release stage stores both per-run and warns if a hash changed but
semver didn't get bumped — that's the "silent drift" signal.

Add a new entry whenever a new prompt joins the pipeline.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from hashlib import sha256
import re

from news_digest.pipeline import llm_rewrite as _lr
from news_digest.pipeline import curator as _cur
from news_digest.pipeline import dedupe as _dd
from news_digest.pipeline import editor as _ed
from news_digest.pipeline import post_publish_judge as _ppj

PROMPT_REGISTRY_VERSION = 1


def _h(text: str) -> str:
    return sha256(text.encode("utf-8")).hexdigest()[:8]


@dataclass(frozen=True, slots=True)
class PromptMeta:
    name: str
    version: str  # semver-ish, bumped manually when intent changes
    hash: str     # SHA-256[:8] of prompt text

    @property
    def tag(self) -> str:
        return f"{self.name}@{self.version}+{self.hash}"


_PROMPT_VERSION_RE = re.compile(r"^v\d+(?:\.\d+){0,2}$")


# Bump the version field when the prompt's intent changes. Don't bump
# for trivial typo fixes — the hash will reflect those automatically,
# and the release gate will surface them as a silent-drift signal.
PROMPTS: tuple[PromptMeta, ...] = (
    PromptMeta(name="curator",         version="v4", hash=_h(_cur.CURATOR_PROMPT)),
    # v6: tightened examples/repair guidance for council deadlocks,
    # retail closures and property/planning stories after the 2026-05-27
    # live issue review.
    PromptMeta(name="city_news",       version="v7", hash=_h(_lr.PROMPT_CITY_NEWS)),
    PromptMeta(name="transport",       version="v5", hash=_h(_lr.PROMPT_TRANSPORT)),
    # v4: three explicit templates (one-off / festival / recurring) +
    # event.is_recurring guidance, S3 sprint.
    PromptMeta(name="events",          version="v5", hash=_h(_lr.PROMPT_EVENTS)),
    PromptMeta(name="diaspora_events", version="v4", hash=_h(_lr.PROMPT_DIASPORA_EVENTS)),
    PromptMeta(name="business",        version="v4", hash=_h(_lr.PROMPT_BUSINESS)),
    PromptMeta(name="football",        version="v5", hash=_h(_lr.PROMPT_FOOTBALL)),
    PromptMeta(name="fix_translate",   version="v2", hash=_h(_lr.FIX_TRANSLATE_SYSTEM)),
    PromptMeta(name="repair_draft",    version="v3", hash=_h(_lr.REPAIR_DRAFT_SYSTEM)),
    PromptMeta(name="dedupe_review",   version="v2", hash=_h(_dd._DEDUPE_REVIEW_PROMPT)),
    PromptMeta(name="pre_send_russian_editor", version="v2", hash=_h(_ed.PRE_SEND_RUSSIAN_EDITOR_PROMPT)),
    PromptMeta(name="post_publish_judge", version=_ppj.JUDGE_PROMPT_VERSION,
               hash=_h(_ppj.JUDGE_PROMPT)),
)


def validate_registry() -> list[str]:
    """Return registry problems; empty list means prompt metadata is usable."""
    errors: list[str] = []
    seen_names: set[str] = set()
    for prompt in PROMPTS:
        if prompt.name in seen_names:
            errors.append(f"Duplicate prompt name: {prompt.name}")
        seen_names.add(prompt.name)
        if not _PROMPT_VERSION_RE.match(prompt.version):
            errors.append(f"Prompt {prompt.name} has invalid version {prompt.version!r}.")
        if not re.fullmatch(r"[0-9a-f]{8}", prompt.hash):
            errors.append(f"Prompt {prompt.name} has invalid hash {prompt.hash!r}.")
    return errors


def snapshot() -> list[dict[str, str]]:
    return [asdict(p) | {"tag": p.tag} for p in PROMPTS]


def by_name() -> dict[str, PromptMeta]:
    return {p.name: p for p in PROMPTS}


def prompt_tag_for(name: str) -> str:
    prompt = by_name().get(name)
    return prompt.tag if prompt else f"{name}@unknown"


def prompt_name_for(prompt_text: str) -> str:
    """Lookup the registered name for a prompt text. Used by llm_rewrite
    so the cost tracker can tag each call with a stable prompt name."""
    if prompt_text.startswith("TODAY_DATE=") and "\n\n" in prompt_text:
        prompt_text = prompt_text.split("\n\n", 1)[1]
    digest = _h(prompt_text)
    for p in PROMPTS:
        if p.hash == digest:
            return p.name
    return "unknown"
