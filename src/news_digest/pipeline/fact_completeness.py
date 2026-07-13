"""Reverse fact-lock: catch critical source facts DROPPED from the RU line.

``fact_lock`` guards one direction only — the Russian rewrite must not *invent*
scalar facts or Latin entities that the evidence lacks. It is silent about the
opposite failure that shipped a neutered crime line: the English source said a
man admitted a "rape fantasy", the digest compressed it to a generic court
line, and the severity of the story vanished with no warning (fact_lock only
checks Russian→evidence, not evidence→Russian).

This module is the reverse gate for sensitive / hard-news lines. If a GRAVE
severity concept — a sexual offence, homicide, death, a weapon/violent act, or
the charged/convicted/acquitted stage — is present in the English source but no
Russian rendering of it survives in the shipped line, that is a critical
omission (MQM "omission" error class) and the line must be rewritten from the
candidate or pulled for rework. Non-grave scalar drops (a sentence length, a
price, a year) are reported as warnings only: a digest is *allowed* to compress.

Deterministic first, by design: a curated bilingual severity lexicon plus
number/date coverage resolve the clear cases here without a model call. The
pre-send LLM judge stays as the semantic fallback for everything this net
cannot decide on its own.
"""
from __future__ import annotations

import re
from typing import Any

from news_digest.pipeline.fact_lock import scalar_fact_tokens

FACT_COMPLETENESS_VERSION = "v1"

# (concept, obligation, english_source_pattern, russian_output_pattern)
# The English pattern decides whether the concept is *present in the source*;
# the Russian pattern decides whether *any* faithful rendering survived in the
# shipped line. Russian patterns are deliberately generous (many stems) so a
# legitimate synonym does not trip a false omission — a false positive here
# would rewrite or strip a correct line.
_SEVERITY_CONCEPTS: tuple[tuple[str, str, str, str], ...] = (
    (
        "sexual_offence",
        "charge/verdict/victim",
        r"\brape\b|\braped\b|sexual\s+(?:assault|offence|offense|abuse|activity|touching)|"
        r"indecent\s+(?:assault|exposure)|grooming|voyeuris|upskirt|child\s+sex",
        r"изнасил|сексуальн|растлен|педофил|домогат|непристойн|интимн|развратн",
    ),
    (
        "homicide",
        "charge/verdict/victim",
        r"\bmurder(?:ed|s)?\b|manslaughter|homicide",
        r"убийств|уби(?:л|т|й)|непредумышленн|лишени[ея]\s+жизни|расправ",
    ),
    (
        "death",
        "who/what",
        r"\bdied\b|\bdeath\b|\bdead\b|\bdies\b|\bfatal(?:ly)?\b|\bkilled\b",
        r"погиб|умер|смерт|скончал|гибел|летальн|жертв",
    ),
    (
        "weapon_violence",
        "who/what",
        r"stab(?:bed|bing)?|\bknife\b|\bknifed\b|shoot(?:ing)?|\bshot\b|\bgun\b|"
        r"\bgunman\b|firearm|acid\s+(?:attack|thrown)|strangl",
        r"ножев|\bнож\b|ножом|заколол|стрель|застрел|огнестрел|оружи|пистолет|"
        r"кислот|задуш|удуш",
    ),
    (
        "acquittal",
        "charge/verdict/victim",
        r"acquitt|cleared\s+of|found\s+not\s+guilty|not\s+guilty|no\s+charges?",
        r"оправд|невинов|не\s+виновн|снят[ыо]\s+обвинени|прекращ",
    ),
)

_CONCEPT_RU = {concept: ru for concept, _obl, _en, ru in _SEVERITY_CONCEPTS}


def critical_fact_obligations(source_text: str) -> list[str]:
    """Reader-critical obligation classes implied by the English source."""
    blob = source_text or ""
    obligations: list[str] = []
    for _concept, obligation, en, _ru in _SEVERITY_CONCEPTS:
        if re.search(en, blob, re.IGNORECASE) and obligation not in obligations:
            obligations.append(obligation)
    return obligations


def line_satisfies_concept(concept: str, line_text: str) -> bool:
    """True when the shipped line renders ``concept`` in Russian."""
    ru = _CONCEPT_RU.get(concept)
    if not ru:
        return True
    return bool(re.search(ru, line_text or "", re.IGNORECASE))


def translation_completeness_review(source_text: str, output_line: str) -> dict[str, Any]:
    """Compare an English source blob against the shipped Russian line.

    Returns ``applies`` (any grave concept was present in the source),
    ``obligations`` (the classes that had to be preserved), ``missing_critical``
    (grave concepts present in source but absent from the line → rewrite), and
    ``missing_noncritical`` (number/date tokens the line dropped → warning).
    """
    source = source_text or ""
    out = output_line or ""
    missing_critical: list[dict[str, str]] = []
    for concept, obligation, en, ru in _SEVERITY_CONCEPTS:
        hit = re.search(en, source, re.IGNORECASE)
        if not hit:
            continue
        if not re.search(ru, out, re.IGNORECASE):
            missing_critical.append(
                {"concept": concept, "obligation": obligation, "source_hit": hit.group(0).strip()[:60]}
            )
    obligations = critical_fact_obligations(source)
    # Non-critical: scalar facts (money, %, times, dates, big numbers) the source
    # carried but the line dropped. The digest is allowed to compress, so this is
    # warning-only — but a silently dropped sentence length or amount is worth a
    # flag for the report. Latin proper nouns are excluded on purpose: they are
    # routinely transliterated away and would flood the warning list.
    dropped_scalars = sorted(scalar_fact_tokens(source) - scalar_fact_tokens(out))
    return {
        "applies": bool(obligations),
        "obligations": obligations,
        "missing_critical": missing_critical,
        "missing_noncritical": dropped_scalars,
    }
