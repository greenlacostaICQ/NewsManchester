"""Editorial curator pass — runs after validate-candidates, before llm-rewrite.

Sees all included candidates at once (~30-40), drops PR fluff / evergreen /
non-GM items, and marks one candidate as is_lead=True (main story of the day).
Uses gpt-4o-mini — decisions are binary, no deep reasoning required.
"""
from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
import re

from news_digest.pipeline.common import now_london, read_json, today_london, write_json

logger = logging.getLogger(__name__)

DEEPSEEK_BASE_URL = "https://api.deepseek.com/v1"
DEEPSEEK_MODEL = "deepseek-chat"
OPENAI_BASE_URL = "https://api.openai.com/v1"
OPENAI_MODEL = "gpt-4o-mini"
GROQ_BASE_URL = "https://api.groq.com/openai/v1"
GROQ_MODEL = "llama-3.3-70b-versatile"

CURATOR_PROMPT = """Ты редакторский куратор дайджеста «Greater Manchester AM Brief».

Тебе даётся список кандидатов. Для каждого прими решение: include true/false и is_lead true/false.

ПРИНЦИП: ВКЛЮЧАЙ ПО УМОЛЧАНИЮ. Если есть СОМНЕНИЕ — include: true. Дроп — только когда новость явно непригодна. Лучше пропустить чуть слабый пункт, чем потерять реальную городскую историю. Городские трагедии, происшествия, политические сюжеты, авторская журналистика — это всё нормальные новости даже если в title нет цифр и адресов: смотри evidence_text — там обычно вся конкретика.

ДРОПАЙ ТОЛЬКО (include: false):
- Чистый PR без новостного повода: «компания признана лучшей», «директор переизбран», «обновили партнёрство» — и в title, и в summary, и в evidence_text нет факта (даты, суммы £, имя, адрес, число пострадавших, исход дела).
- Awareness-кампании без события: «неделя осведомлённости о X», «месяц Y» — без конкретной даты/площадки/программы.
- Точные дубли темы: если 2+ кандидата про ОДНУ И ТУ ЖЕ историю (та же жертва / то же решение / то же событие) — оставь один с лучшим заголовком, остальные дропни.
- Evergreen-листинги без даты: «10 лучших баров», «куда пойти на выходных» — без конкретного события сегодня/завтра.
- Tech/бизнес-новость, где НИ в title, НИ в summary, НИ в evidence_text не упомянуты Manchester/Salford/Bolton/Bury/Oldham/Rochdale/Stockport/Tameside/Trafford/Wigan/GM/Greater Manchester и не указан конкретный GM-адрес.

ВКЛЮЧАЙ (include: true):
- Любое полиция/суды/преступление с конкретной деталью (имя, возраст, локация, исход) — даже если выглядит как мрачная история.
- Решение совета или политическое событие с локальным эффектом — даже если оно про долгие процессы (выборы, бюджет, кадры в местном самоуправлении).
- Транспортный сбой / закрытие дороги / работы с маршрутом ИЛИ временем.
- Городские происшествия и трагедии с конкретикой (пожар, ДТП, смерть с возрастом и районом).
- События с датой и площадкой; рынки/ярмарки с конкретным днём; концерты с датой и venue.
- Бизнес/еда — открытие/закрытие/инвестиция в конкретном районе GM.
- Футбол — любая существенная новость про Man Utd / Man City (матч-репорт, реакция тренера, трансфер, серьёзная травма, карьерная веха игрока). Дропни только чистый promo и youth/women's coverage.
- Авторская журналистика (The Mill, The Manc) — даже если у статьи нет «новостного триггера» в классическом смысле: если evidence_text показывает что это про конкретные имена, события или решения в GM — include: true.

ЛИДЕР (is_lead: true) — РОВНО ОДИН кандидат на весь батч:
Выбирай новость с наибольшим влиянием на жителей GM сегодня.
Предпочитай: крупное уголовное дело / громкое политическое событие / крупный транспортный коллапс / городская трагедия с масштабом.
Лидом не может быть футбол, еда, evergreen, событие, концерт.

Верни ТОЛЬКО JSON-массив без пояснений:
[{"fingerprint": "...", "include": true, "is_lead": false, "reason": "кратко почему"}]"""


_GM_BOROUGHS: tuple[str, ...] = (
    "Manchester", "Salford", "Trafford", "Stockport", "Tameside",
    "Oldham", "Rochdale", "Bury", "Bolton", "Wigan",
)
_CURATOR_PROTECTED_CATEGORIES = {"weather"}
_CURATOR_PROTECTED_BLOCKS = {"weather"}


def _is_curator_protected(candidate: dict) -> bool:
    return (
        str(candidate.get("category") or "") in _CURATOR_PROTECTED_CATEGORIES
        or str(candidate.get("primary_block") or "") in _CURATOR_PROTECTED_BLOCKS
    )


def _infer_borough(candidate: dict) -> str:
    text = " ".join(
        str(candidate.get(field) or "")
        for field in ("title", "summary", "lead", "evidence_text", "practical_angle")
    )
    for borough in _GM_BOROUGHS:
        if re.search(rf"\b{re.escape(borough)}\b", text, flags=re.IGNORECASE):
            return borough
    return ""


_CURATOR_BATCH_SIZE = 10
_CURATOR_GROQ_BATCH_SIZE = 6

# Tokens shorter than this don't carry enough signal (articles, fillers).
_DEDUP_MIN_TOKEN_LEN = 4
# Min |A ∩ B| / min(|A|, |B|) overlap ratio before treating as duplicate.
_DEDUP_OVERLAP_RATIO = 0.25
# Stopwords carry no story-identifying signal. Includes RSS-feed boilerplate
# and the prefix-5 forms of common event/market/transport template words
# that would otherwise create phantom overlap (e.g. "пройдёт", "товары",
# "независимые продавцы" appear in every market event regardless of which
# event it is).
_DEDUP_STOPWORDS = {
    # Geography (whole-region tokens recur in every borough story)
    "manchester", "greater", "salford", "stockport", "trafford", "oldham",
    "rochdale", "bolton", "tameside", "bury", "wigan",
    # Generic news fillers (RU + prefix-5 truncations)
    "новый", "новая", "новое", "новые", "после", "перед", "вчера",
    "сегодня", "завтра", "около", "более", "также", "может", "будет",
    "стал", "стала", "стало", "стали", "были", "было", "была",
    "котор", "город", "годы",
    "млн", "тысяч", "часть", "получ",
    # Council/formatting boilerplate
    "council", "councils", "совет", "район", "округ",
    # Transport template
    "tfgm", "задержки", "задер", "закрытие", "закрыт", "ремонт",
    "ожидайте", "ожида", "работы", "работ", "между", "сбой", "пробки",
    # Ticketmaster template
    "ticketmaster", "event", "public", "sale", "tour", "arena", "hall",
    "concert", "show", "london", "liverpool",
    "ноября", "октяб", "сентя", "авгус", "июля", "июня",
    "апрел", "марта", "февра", "января", "декаб",
    # Market/event template prefixes (Makers Market false-positive on 2026-05-12).
    # These appear in every event/market story regardless of identity.
    "maker", "marke", "пройд", "товар", "прода", "ремес", "незав",
    "состо", "пройде", "событ", "места",
    # Generic reaction/sentiment phrases ("местные жители выражают") that
    # triggered Oldham-elections ↔ Middleton-shop false-positive.
    "выраж", "жител", "местн",
}

_BLOCKS_TO_SKIP_DEDUP = {
    # Транспорт и tickets — все строки шаблонные, дедуп даёт ложные срабатывания.
    # Дубли тут ловятся другим механизмом (fingerprint на уровне TfGM/Ticketmaster).
    "transport", "ticket_radar", "outside_gm_tickets",
}


_DEDUP_PREFIX_LEN = 5


def _dedup_signature(candidate: dict) -> set[str]:
    """Token bag from draft_line only — title/lead carry RSS scrape noise
    ('reporter', 'updated', 'comments') that creates phantom overlap between
    unrelated MEN/Prolific North items. draft_line is the cleaned LLM
    output so noise is minimal, and proper nouns like 'Stockport County'
    or 'Adrian Brown' or 'Tour de France' survive into Russian text.

    We keyword-truncate to the first 5 chars so that Russian declensions
    collide ('Браун' / 'Брауна' both become 'браун', 'стадиона' / 'стадион'
    both 'стади'). Without this, dedup misses re-told stories like the
    Adrian-vs-Andrew Brown coverage on 2026-05-12."""
    text = str(candidate.get("draft_line") or "").lower()
    if not text:
        return set()
    # Keep alphanumerics + Cyrillic, split on everything else.
    tokens = re.findall(r"[a-zа-яё0-9]+", text, flags=re.IGNORECASE)
    out: set[str] = set()
    for t in tokens:
        if len(t) < _DEDUP_MIN_TOKEN_LEN:
            continue
        prefix = t[:_DEDUP_PREFIX_LEN]
        # Filter both forms — stopwords are stored as prefix-5 truncations
        # ("пройд"), so tokens like "пройдёт" need to match by prefix.
        if t in _DEDUP_STOPWORDS or prefix in _DEDUP_STOPWORDS:
            continue
        out.add(prefix)
    return out


def _semantic_dedup_pass(candidates: list[dict]) -> int:
    """Drop the second of any two included candidates that share enough
    meaningful tokens to look like the same story. Cross-block comparison
    (Adrian Brown was tagged today_focus by one source and last_24h by
    another on 2026-05-12 — per-block dedup missed the pair). Transport and
    ticket blocks are excluded because their lines share boilerplate. Keeps
    the earlier-listed candidate (curator order ~= editorial priority)."""
    seen: list[tuple[str, set[str]]] = []  # (fingerprint, tokens)
    dropped = 0
    for candidate in candidates:
        if not isinstance(candidate, dict) or not candidate.get("include"):
            continue
        if _is_curator_protected(candidate):
            continue
        block = str(candidate.get("primary_block") or "")
        if block in _BLOCKS_TO_SKIP_DEDUP:
            continue
        sig = _dedup_signature(candidate)
        if len(sig) < 4:
            # Too few signal tokens — can't reliably tell duplicate from coincidence.
            continue
        fp = str(candidate.get("fingerprint") or "")
        match_fp: str | None = None
        for earlier_fp, earlier_sig in seen:
            overlap = len(sig & earlier_sig)
            if overlap < 3:
                continue
            denom = min(len(sig), len(earlier_sig))
            if denom and overlap / denom >= _DEDUP_OVERLAP_RATIO:
                match_fp = earlier_fp
                break
        if match_fp is not None:
            candidate["include"] = False
            candidate["dedupe_decision"] = "drop"
            candidate["reason"] = f"Semantic dedup: near-duplicate of {match_fp[:8]}"
            dropped += 1
        else:
            seen.append((fp, sig))
    return dropped


def _call_curator_batch(batch: list[dict], client: object, model: str) -> list[dict]:
    payload = [
        {
            "fingerprint": c.get("fingerprint", ""),
            "title": c.get("title", ""),
            "summary": (c.get("summary") or "")[:320],
            "lead": (c.get("lead") or "")[:320],
            "evidence_text": (c.get("evidence_text") or "")[:700],
            "practical_angle": (c.get("practical_angle") or "")[:240],
            "category": c.get("category", ""),
            "primary_block": c.get("primary_block", ""),
            "source_label": c.get("source_label", ""),
            "source_url": c.get("source_url", ""),
            "published_at": c.get("published_at", ""),
            "freshness_status": c.get("freshness_status", ""),
            "event_page_type": c.get("event_page_type", ""),
            "borough": c.get("borough") or _infer_borough(c),
        }
        for c in batch
    ]
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": CURATOR_PROMPT},
            {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
        ],
        temperature=0.1,
        max_tokens=4000,
    )
    raw = response.choices[0].message.content.strip()
    if raw.startswith("```"):
        raw = raw.split("```", 2)[1]
        if raw.startswith("json"):
            raw = raw[4:]
        raw = raw.rsplit("```", 1)[0]
    return json.loads(raw.strip())


def _call_curator(candidates: list[dict], api_key: str, base_url: str, model: str, batch_size: int = _CURATOR_BATCH_SIZE) -> list[dict]:
    if not api_key or not candidates:
        return []
    try:
        from openai import OpenAI  # noqa: PLC0415
    except ImportError:
        logger.error("openai package not installed.")
        return []

    # max_retries=0: don't burn 3×60s on a dead endpoint — fail fast and let
    # run_curator_pass try the next provider in the chain.
    client = OpenAI(api_key=api_key, base_url=base_url, timeout=45, max_retries=0)
    results: list[dict] = []
    batches = [candidates[i:i + batch_size] for i in range(0, len(candidates), batch_size)]
    for i, batch in enumerate(batches):
        try:
            logger.info("Curator: batch %d/%d (%d candidates).", i + 1, len(batches), len(batch))
            results.extend(_call_curator_batch(batch, client, model))
        except Exception as exc:  # noqa: BLE001
            logger.warning("Curator call failed: %s", exc)
            return []
    return results


def run_curator_pass(project_root: Path) -> None:
    """Drop PR/evergreen candidates and mark lead story before LLM rewrite."""
    candidates_path = project_root / "data" / "state" / "candidates.json"
    report_path = project_root / "data" / "state" / "curator_report.json"

    if not candidates_path.exists():
        logger.warning("candidates.json not found, skipping curator pass.")
        write_json(report_path, {"status": "skipped", "reason": "missing candidates.json", "run_at": now_london().isoformat(), "run_date_london": today_london()})
        return

    payload = json.loads(candidates_path.read_text(encoding="utf-8"))
    candidates = payload.get("candidates", [])

    included = [
        c
        for c in candidates
        if isinstance(c, dict) and c.get("include") and not _is_curator_protected(c)
    ]
    if not included:
        logger.info("Curator: no included candidates.")
        write_json(report_path, {"status": "skipped", "reason": "no included candidates", "run_at": now_london().isoformat(), "run_date_london": today_london()})
        return

    logger.info("Curator: reviewing %d included candidates.", len(included))

    provider_override = os.environ.get("LLM_PROVIDER", "").lower().strip()
    if provider_override == "none":
        logger.info("LLM_PROVIDER=none — skipping curator pass.")
        write_json(report_path, {"status": "skipped", "reason": "LLM_PROVIDER=none", "run_at": now_london().isoformat(), "run_date_london": today_london()})
        return

    base_url = os.environ.get("LLM_BASE_URL") or DEEPSEEK_BASE_URL
    model = os.environ.get("LLM_MODEL") or DEEPSEEK_MODEL
    api_key = os.environ.get("LLM_API_KEY") or os.environ.get("DEEPSEEK_API_KEY", "")

    decisions = _call_curator(included, api_key, base_url, model)

    # OpenAI fallback
    if not decisions:
        logger.info("Curator: DeepSeek failed, trying OpenAI.")
        time.sleep(1)
        decisions = _call_curator(
            included,
            os.environ.get("OPENAI_API_KEY", ""),
            OPENAI_BASE_URL,
            OPENAI_MODEL,
        )

    # Groq fallback — free tier safety net so the curator never silently
    # skips and lets every candidate through unfiltered.
    if not decisions:
        logger.info("Curator: OpenAI failed, trying Groq.")
        time.sleep(1)
        decisions = _call_curator(
            included,
            os.environ.get("GROQ_API_KEY", ""),
            GROQ_BASE_URL,
            GROQ_MODEL,
            batch_size=_CURATOR_GROQ_BATCH_SIZE,
        )

    if not decisions:
        logger.warning("Curator: all providers failed — keeping existing include flags.")
        write_json(report_path, {"status": "skipped", "reason": "all providers failed", "run_at": now_london().isoformat(), "run_date_london": today_london()})
        return

    decision_map = {str(d.get("fingerprint") or ""): d for d in decisions if isinstance(d, dict)}
    dropped = 0
    lead_set = False

    for candidate in candidates:
        if _is_curator_protected(candidate):
            candidate["include"] = True
            candidate["is_lead"] = False
            continue
        fp = str(candidate.get("fingerprint") or "")
        decision = decision_map.get(fp)
        if not decision:
            continue
        if not decision.get("include", True):
            candidate["include"] = False
            candidate["dedupe_decision"] = "drop"
            candidate["reason"] = f"Curator drop: {decision.get('reason', '')}"
            dropped += 1
        if decision.get("is_lead") and not lead_set and candidate.get("include"):
            candidate["is_lead"] = True
            lead_set = True

    # Semantic dedup pass: catch near-duplicate stories the LLM curator missed
    # (same person / same event covered by multiple sources). On 2026-05-12 we
    # shipped: Adrian Brown × 2 (MEN + BBC), Tour de France × 2 (Manchester +
    # Oldham Council), Stockport County digitisation × 2 (Prolific North +
    # BusinessCloud) — fingerprint dedup misses all of these because the
    # surface text differs.
    semantic_dropped = _semantic_dedup_pass(candidates)
    dropped += semantic_dropped

    candidates_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("Curator: dropped %d candidates (semantic dedup: %d), lead=%s.", dropped, semantic_dropped, lead_set)

    write_json(report_path, {
        "run_at": now_london().isoformat(),
        "run_date_london": today_london(),
        "status": "complete",
        "reviewed": len(included),
        "dropped": dropped,
        "semantic_dropped": semantic_dropped,
        "lead_set": lead_set,
        "decisions": decisions,
    })
