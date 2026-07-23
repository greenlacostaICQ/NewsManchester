"""Editorial board ranking — the real judge, one listwise call per block.

Why this exists as a separate stage from `english_cards`:

  * A fact card is about ONE story. The answer is the same today and tomorrow,
    so it is cached by content hash and reused (~80% hit rate on a normal day).
  * A ranking is about TODAY'S COMPETITION inside one section. The answer
    changes every morning, so it must never be cached.

Those two jobs used to ride on one call. The consequences were both visible in
production on 2026-07-23:

  1. 70 of 87 board members came from the card cache, and the cache does not
     store a verdict — so four fifths of the board had no rating at all.
  2. One absolute 0-100 scale covered every story type at once, so the model
     graded *record completeness* instead of *reader importance*: a bus stop
     closure scored 100 while a court report on a synagogue attacker scored 76.

Ranking inside a single block fixes (2) by construction — a roadworks notice and
a court report are never on the same scale because they are never in the same
call. Splitting the call fixes (1) because ranking no longer inherits the card's
cache.

Only blocks where the answer is NOT already in the record get a judge. Ticket
notability comes from Wikidata, event dates come from `event.date_start`, tram
closures come from the operator alert — a model adds nothing there and is not
called. See `JUDGED_BLOCKS`.
"""
from __future__ import annotations

import json
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from news_digest.pipeline.common import PRIMARY_BLOCKS, now_london, today_london, write_json
from news_digest.pipeline.model_routing import (
    chat_completion_options_for_route,
    resolve_model_route,
    sdk_retries_for_route,
)

logger = logging.getLogger(__name__)

BOARD_RANK_VERSION = 1

# Blocks whose ordering needs editorial judgement, with the criterion the judge
# is asked to apply. Everything absent from this map is ranked deterministically
# and never reaches the model:
#   transport / weather      — rule: show every real restriction; template render
#   ticket_radar             — Wikidata notability tier + sale lifecycle
#   outside_gm_tickets       — same, A-tier notability only
#   weekend_activities       — coverage of the weekend, not a quality contest
#   russian_events           — diaspora coverage, 19 candidates for 6 slots
#   professional_events      — already judged by the CV matcher; no second vote
#   next_7_days / openings / future_announcements / football — dates, venues and
#                              freshness already answer the question
JUDGED_BLOCKS: dict[str, str] = {
    "lead_story": (
        "The single main story of the day for Greater Manchester. Prefer city-wide "
        "consequence and a fresh, verifiable event over a strong headline."
    ),
    "today_focus": (
        "What a resident must act on or know TODAY. Prefer items with a concrete "
        "action, deadline or disruption landing today."
    ),
    "last_24h": (
        "Hard city news of the last 24 hours. Prefer public consequence — safety, "
        "courts, money, services, accountability — over profile and colour pieces."
    ),
    "city_watch": (
        "Slower civic and municipal signal. Prefer decisions, plans and money that "
        "change something for residents over announcements and self-promotion."
    ),
    "tech_business": (
        "Local business and tech. The main job is rejecting PR: prefer real "
        "investment, jobs, openings and closures over launches and award lists."
    ),
}

# Rank -> score conversion. Rank 1 in a block scores 100, the last rank scores 0,
# so the number is always relative to today's competition inside that block.
BOARD_RANK_TOP_SCORE = 100.0

# How hard the judge may push against the deterministic score. The deterministic
# publish-tier gap (must_include 80 vs strong 35) is 45, so +/-25 lets the judge
# reorder freely inside a tier and, at full stretch, promote an excellent
# "strong" item over a weak "must_include" — which is the point of having a
# judge. Deliberately tunable in one place: raise or lower it from the eval in
# tools/board_eval.py, not by editing call sites.
BOARD_RANK_WEIGHT = 0.5  # (score - 50) * 0.5 => -25 .. +25

# A reject only removes an item when the model is actually sure. Below this the
# verdict degrades to "backup" — the item stays recoverable instead of dying on
# a coin flip.
BOARD_REJECT_MIN_CONFIDENCE = 0.65

_MAX_ITEMS_PER_CALL = 60
_EVIDENCE_CHARS = 320


BOARD_RANK_SYSTEM = """You are the editorial board for the Greater Manchester AM Brief.

You are given EVERY candidate for ONE section of today's issue. Rank them against each other.

You are NOT grading how complete or well-formed a record is. A tidy roadworks notice with a
street name and a timestamp is not more important than a thin court report. Judge only what a
Greater Manchester resident would most want to know today, within this section.

Use only the supplied fields. Do not browse. Do not invent facts.

Return ONLY a JSON object: {"items": [...]}, ordered best first.
Each item:
{
  "fingerprint": "...",
  "rank": 1,
  "decision": "publish|backup|reject",
  "confidence": 0.0-1.0,
  "reason_codes": ["..."],
  "why": "one short sentence — REQUIRED for ranks 1-3, omit otherwise"
}

Hard rules:
- Every supplied fingerprint appears exactly once. Do not add or drop items.
- Ranks are 1..N, contiguous, no ties, best first.
- decision "reject" is for: not Greater Manchester, stale, pure PR, duplicate of a stronger
  item in this same list, or too thin to write a self-contained line from.
- decision "backup" is for genuinely useful items that lost to stronger competition today.
- confidence is your own certainty about THIS verdict, not about the story.
- Be honest about weak days: if the section is thin, say so with low ranks, do not inflate.
"""


def judged_block(candidate: dict) -> str:
    """Block name if this candidate belongs to a judged board, else ""."""
    if not isinstance(candidate, dict):
        return ""
    if candidate.get("is_lead"):
        return "lead_story"
    block = str(candidate.get("primary_block") or "")
    return block if block in JUDGED_BLOCKS else ""


def _clip(value: object, limit: int) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    return text[:limit]


def _rank_item_payload(candidate: dict) -> dict[str, object]:
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    return {
        "fingerprint": str(candidate.get("fingerprint") or ""),
        "title": _clip(candidate.get("title"), 200),
        "summary": _clip(candidate.get("summary") or candidate.get("lead"), 300),
        "evidence_excerpt": _clip(candidate.get("evidence_text"), _EVIDENCE_CHARS),
        "source_label": _clip(candidate.get("source_label"), 60),
        "category": _clip(candidate.get("category"), 40),
        "borough": _clip(candidate.get("borough"), 40),
        "published_at": _clip(candidate.get("published_at"), 32),
        "freshness_status": _clip(candidate.get("freshness_status"), 32),
        "event_date": _clip(event.get("date_start") or event.get("date"), 32),
        "venue": _clip(event.get("venue"), 80),
    }


def _parse_board_rank_results(
    raw: str,
    expected: dict[str, dict],
    block: str,
) -> tuple[dict[str, dict], dict[str, object]]:
    """Parse one block's ranking. Returns (fingerprint -> verdict, diagnostic)."""
    diagnostic: dict[str, object] = {
        "block": block,
        "sent": len(expected),
        "returned_items": 0,
        "accepted": 0,
        "rejected_counts": {},
        "parse_error": "",
    }

    def _reject(reason: str) -> None:
        counts = diagnostic["rejected_counts"]
        counts[reason] = int(counts.get(reason, 0)) + 1

    cleaned = str(raw or "").strip()
    cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
    cleaned = re.sub(r"\s*```$", "", cleaned).strip()
    try:
        payload = json.loads(cleaned)
    except json.JSONDecodeError as exc:
        diagnostic["parse_error"] = f"JSONDecodeError: {exc}"
        diagnostic["raw_excerpt"] = cleaned[:400]
        return {}, diagnostic

    items = payload.get("items") if isinstance(payload, dict) else payload
    if not isinstance(items, list):
        diagnostic["parse_error"] = f"items is {type(items).__name__}, not a list."
        diagnostic["raw_excerpt"] = cleaned[:400]
        return {}, diagnostic

    verdicts: dict[str, dict] = {}
    for position, item in enumerate(items, start=1):
        diagnostic["returned_items"] = int(diagnostic["returned_items"]) + 1
        if not isinstance(item, dict):
            _reject("bad_item_shape")
            continue
        fp = str(item.get("fingerprint") or "").strip()
        if not fp:
            _reject("missing_fingerprint")
            continue
        if fp not in expected:
            _reject("unknown_fingerprint")
            continue
        if fp in verdicts:
            _reject("duplicate_fingerprint")
            continue
        try:
            rank = int(float(item.get("rank")))
        except (TypeError, ValueError):
            # Model kept the order but dropped the number: list position is the
            # ranking it actually expressed, so use it rather than losing the item.
            rank = position
        try:
            confidence = float(item.get("confidence"))
        except (TypeError, ValueError):
            confidence = 0.0
        decision = str(item.get("decision") or "publish").strip().lower()
        if decision not in {"publish", "backup", "reject"}:
            decision = "publish"
        verdicts[fp] = {
            "rank": max(1, rank),
            "decision": decision,
            "confidence": max(0.0, min(1.0, confidence)),
            "reason_codes": [str(c)[:60] for c in (item.get("reason_codes") or [])][:8],
            "why": _clip(item.get("why"), 240),
        }

    # Ranks are re-derived from the model's own ordering so a block is always
    # 1..N contiguous even when the model skips or repeats a number.
    ordered = sorted(verdicts.items(), key=lambda pair: pair[1]["rank"])
    total = len(ordered)
    for position, (fp, verdict) in enumerate(ordered, start=1):
        verdict["rank"] = position
        verdict["rank_total"] = total
        verdict["score"] = (
            round(BOARD_RANK_TOP_SCORE * (total - position) / (total - 1), 2) if total > 1 else BOARD_RANK_TOP_SCORE
        )

    diagnostic["accepted"] = len(verdicts)
    missing = [fp for fp in expected if fp not in verdicts]
    diagnostic["missing_candidates"] = [
        {"fingerprint": fp, "title": _clip(expected[fp].get("title"), 120)} for fp in missing[:8]
    ]
    return verdicts, diagnostic


def _call_block(
    step,
    block: str,
    candidates: list[dict],
    diagnostics: list[dict],
) -> dict[str, dict]:
    """One listwise call for one block. Returns fingerprint -> verdict."""
    if not candidates or not step.api_key:
        return {}
    try:
        from openai import OpenAI  # noqa: PLC0415
    except ImportError:
        logger.error("openai package not installed. Run: pip install openai")
        return {}
    # Late import: the throttles are owned by llm_rewrite and shared across every
    # prompt group in the process. Importing them at module load would cycle.
    from news_digest.pipeline.llm_rewrite import (  # noqa: PLC0415
        _API_RATE_LIMITER,
        _API_SEMAPHORE,
        _API_TOKEN_LIMITER,
        _estimate_request_tokens,
    )

    expected = {str(c.get("fingerprint") or ""): c for c in candidates if str(c.get("fingerprint") or "")}
    user_payload = {
        "today_date": today_london(),
        "section": PRIMARY_BLOCKS.get(block, block),
        "ranking_criterion": JUDGED_BLOCKS.get(block, ""),
        "slots_available": len(expected),
        "candidates": [_rank_item_payload(c) for c in expected.values()],
    }
    messages = [
        {"role": "system", "content": BOARD_RANK_SYSTEM},
        {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
    ]
    client = OpenAI(
        api_key=step.api_key,
        base_url=step.base_url,
        timeout=step.timeout_seconds or 60,
        max_retries=0
        if step.provider_label.lower().startswith("openai")
        else sdk_retries_for_route(provider=step.provider_label, model=step.model, base_url=step.base_url),
    )
    # ~70 output tokens per ranked item plus headroom for the top-3 explanations.
    max_tokens = min(8192, 70 * len(expected) + 900)
    started_at = now_london().isoformat()
    t0 = time.monotonic()
    queue_wait_seconds = 0.0
    api_seconds = 0.0
    try:
        with _API_SEMAPHORE:
            queue_t0 = time.monotonic()
            _API_RATE_LIMITER.acquire()
            _API_TOKEN_LIMITER.acquire(_estimate_request_tokens(messages, max_tokens))
            queue_wait_seconds = time.monotonic() - queue_t0
            api_t0 = time.monotonic()
            response = client.chat.completions.create(
                model=step.model,
                messages=messages,
                temperature=0.1,
                max_tokens=max_tokens,
                **chat_completion_options_for_route(
                    provider=step.provider_label, model=step.model, base_url=step.base_url
                ),
            )
            api_seconds = time.monotonic() - api_t0
        from news_digest.pipeline.cost_tracker import record_call_from_response  # noqa: PLC0415

        record_call_from_response(
            response=response,
            stage="rank_digest",
            provider=step.provider_label.split("-", 1)[0],
            model=step.model,
            prompt_name="board_rank",
            messages=messages,
            max_tokens=max_tokens,
        )
        raw = response.choices[0].message.content.strip()
        verdicts, diagnostic = _parse_board_rank_results(raw, expected, block)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Board rank %s (%s) failed — %s", block, step.provider_label, exc)
        diagnostics.append(
            {
                "block": block,
                "provider": step.provider_label,
                "model": step.model,
                "sent": len(expected),
                "accepted": 0,
                "error": f"{exc.__class__.__name__}: {exc}",
                "started_at": started_at,
                "duration_seconds": round(time.monotonic() - t0, 3),
            }
        )
        return {}
    diagnostic.update(
        {
            "provider": step.provider_label,
            "model": step.model,
            "started_at": started_at,
            "finished_at": now_london().isoformat(),
            "duration_seconds": round(time.monotonic() - t0, 3),
            "queue_wait_seconds": round(queue_wait_seconds, 3),
            "api_seconds": round(api_seconds, 3),
        }
    )
    diagnostics.append(diagnostic)
    for verdict in verdicts.values():
        verdict["provider"] = step.provider_label
        verdict["model"] = step.model
    return verdicts


def rank_boards(
    candidates: list[dict],
    *,
    provider_override: str = "",
    base_url_override: str = "",
    model_override: str = "",
) -> tuple[dict[str, dict], dict[str, object]]:
    """Rank every judged block. Returns (fingerprint -> verdict, report).

    Blocks run concurrently; the shared semaphore and rate limiter still bound
    total API pressure. A block that fails leaves its candidates unranked — the
    deterministic order then stands, which is the same behaviour as a day when
    the model is unavailable.
    """
    report: dict[str, object] = {
        "schema_version": BOARD_RANK_VERSION,
        "enabled": False,
        "judged_blocks": sorted(JUDGED_BLOCKS),
        "blocks": {},
        "ranked_candidates": 0,
        "diagnostics": [],
        "policy": (
            "Listwise ranking, one call per block, never cached: the answer depends on today's "
            "competition. Blocks whose order is already determined by record fields "
            "(tickets, transport, weekend, events, professional) are not judged."
        ),
    }
    if provider_override == "none":
        report["skipped_reason"] = "LLM_PROVIDER=none"
        return {}, report

    by_block: dict[str, list[dict]] = {}
    for candidate in candidates:
        block = judged_block(candidate)
        if not block:
            continue
        by_block.setdefault(block, []).append(candidate)
    if not by_block:
        report["skipped_reason"] = "no_candidates_in_judged_blocks"
        return {}, report

    route = resolve_model_route(
        "board_rank",
        provider_override=provider_override,
        base_url_override=base_url_override,
        model_override=model_override,
    )
    if not route:
        report["skipped_reason"] = "no_route"
        return {}, report

    from news_digest.pipeline import provider_health  # noqa: PLC0415

    diagnostics: list[dict] = []
    verdicts: dict[str, dict] = {}

    def _rank_one(block: str, pool: list[dict]) -> dict[str, dict]:
        # Oversized blocks keep the strongest deterministic head: a listwise call
        # degrades once the list stops fitting the model's attention, and the tail
        # of a 100-item block is reserve material anyway.
        from news_digest.pipeline.story_intelligence import section_board_score  # noqa: PLC0415

        ordered = sorted(pool, key=lambda c: -float(section_board_score(c)))[:_MAX_ITEMS_PER_CALL]
        for step in route:
            if provider_health.is_dead(step.provider):
                continue
            result = _call_block(step, block, ordered, diagnostics)
            if result:
                provider_health.record_success(step.provider)
                return result
            provider_health.record_failure(step.provider)
        return {}

    with ThreadPoolExecutor(max_workers=min(len(by_block), 4)) as executor:
        futures = {
            block: executor.submit(_rank_one, block, pool)
            for block, pool in by_block.items()
        }
        for block, future in futures.items():
            block_verdicts = future.result()
            verdicts.update(block_verdicts)
            decisions: dict[str, int] = {}
            for verdict in block_verdicts.values():
                decisions[verdict["decision"]] = decisions.get(verdict["decision"], 0) + 1
            report["blocks"][block] = {
                "candidates": len(by_block[block]),
                "sent_to_model": min(len(by_block[block]), _MAX_ITEMS_PER_CALL),
                "ranked": len(block_verdicts),
                "decisions": decisions,
                "top3": [
                    {"rank": v["rank"], "why": v.get("why") or "", "fingerprint": fp}
                    for fp, v in sorted(block_verdicts.items(), key=lambda pair: pair[1]["rank"])[:3]
                ],
            }

    report["enabled"] = True
    report["ranked_candidates"] = len(verdicts)
    report["diagnostics"] = diagnostics
    return verdicts, report


def apply_board_rank(candidates: list[dict], verdicts: dict[str, dict]) -> int:
    """Write the judge's verdict onto candidates. Returns how many were ranked."""
    applied = 0
    for candidate in candidates:
        fp = str(candidate.get("fingerprint") or "").strip()
        verdict = verdicts.get(fp) if fp else None
        if not verdict:
            continue
        candidate["board_rank"] = verdict["rank"]
        candidate["board_rank_total"] = verdict.get("rank_total") or verdict["rank"]
        candidate["board_rank_score"] = verdict.get("score")
        candidate["board_decision"] = verdict["decision"]
        candidate["board_confidence"] = verdict["confidence"]
        candidate["board_reason_codes"] = verdict.get("reason_codes") or []
        candidate["board_rank_why"] = verdict.get("why") or ""
        candidate["judged_by"] = "model"
        candidate["board_judge_model"] = verdict.get("model") or ""
        applied += 1
    return applied


def board_rank_bonus(candidate: dict) -> float:
    """Ordering contribution of the judge, or 0.0 for anything it did not judge.

    Relative to the block: rank 1 gets +25, the last rank -25, everything in
    between is linear. An unjudged candidate is unaffected, so deterministic
    blocks keep exactly the order they had before the judge existed.
    """
    if not isinstance(candidate, dict):
        return 0.0
    score = candidate.get("board_rank_score")
    try:
        return (float(score) - 50.0) * BOARD_RANK_WEIGHT
    except (TypeError, ValueError):
        return 0.0


def board_reject_verdict(candidate: dict) -> tuple[bool, str]:
    """Should the judge's reject actually remove this candidate?

    Three guards, in order. A protected lane is never dropped on a model's word;
    a low-confidence reject degrades to reserve instead of a coin-flip removal;
    and the per-block floor is enforced by the caller, which is the only place
    that knows how many survivors are left.
    """
    if str(candidate.get("board_decision") or "") != "reject":
        return False, ""
    lane = candidate.get("protected_lane") if isinstance(candidate.get("protected_lane"), dict) else {}
    if lane.get("protected"):
        return False, "protected_lane_overrides_board_reject"
    try:
        confidence = float(candidate.get("board_confidence"))
    except (TypeError, ValueError):
        confidence = 0.0
    if confidence < BOARD_REJECT_MIN_CONFIDENCE:
        return False, f"board_reject_below_confidence_{BOARD_REJECT_MIN_CONFIDENCE}"
    return True, "board_reject"


def write_board_rank_report(project_root: Path, report: dict[str, object]) -> Path:
    path = project_root / "data" / "state" / "board_rank_report.json"
    write_json(path, report)
    return path
