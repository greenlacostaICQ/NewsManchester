"""Text cleaning and editorial defaults.

Pure functions: snippet/title cleaners, source-aware summary/lead
defaults, topic-aware practical_angle resolver. Anything that needs to
look at a SourceDef but never touches the network or filesystem.
"""

from __future__ import annotations

import html
import re

from .sources import SourceDef


_SUMMARY_THIN_THRESHOLD = 60


# Stripping a 'By <author>' byline is intentionally left out — the
# greedy regex variant ate real content. If a metadescription starts
# with the author's name we accept that mild ugliness.
_SNIPPET_NOISE_PREFIXES: tuple[re.Pattern[str], ...] = tuple(
    re.compile(rf"^{prefix}\s*[:\-—|]?\s*", re.IGNORECASE)
    for prefix in (
        r"updated",
        r"published\s+\d{1,2}\s+[a-z]+\s+\d{4}",
        r"\d{1,2}\s+[a-z]+\s+\d{4}",
    )
)
_SNIPPET_NOISE_SUFFIXES: tuple[re.Pattern[str], ...] = tuple(
    re.compile(rf"\s*{suffix}\s*$", re.IGNORECASE)
    for suffix in (
        r"\bread\s+more\b\.?",
        r"\bcontinue\s+reading\b\.?",
        r"\bread\s+the\s+full\s+article\b\.?",
        r"\bclick\s+here\b\.?",
        r"\.\.\.",
    )
)


def _clean_snippet(value: str) -> str:
    # Strip HTML tags first, then decode entities. Use html.unescape so we
    # cover numeric refs (&#160;, &#8217;) and named refs (&nbsp;, &mdash;)
    # in one shot — the previous .replace() chain only handled three of
    # them and left visible &#160; in GMP summaries.
    text = re.sub(r"<[^>]+>", " ", str(value or ""))
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    for pattern in _SNIPPET_NOISE_PREFIXES:
        text = pattern.sub("", text, count=1)
    for pattern in _SNIPPET_NOISE_SUFFIXES:
        text = pattern.sub("", text, count=1)
    text = text.strip(" |-—·•:.,")
    return text[:280].strip()


def _clean_title_text(title: str) -> str:
    cleaned = re.sub(r"\s+", " ", str(title or "")).strip()
    if not cleaned:
        return ""
    cleaned = cleaned.replace("’", "'").replace("–", "-")
    cleaned = re.sub(
        r"^\s*(News|Audio|Art\s*&\s*Culture|Community|Men'?s Team|Women'?s Team|EDS\s*&\s*Academy)\s*\|\s*",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"^\d{1,2}/\d{1,2}/\d{2}\s*", "", cleaned)
    cleaned = re.sub(r"^\s*(News|Audio|Art\s*&\s*Culture)\s*", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+\b\d+\s+days?\s+ago\b$", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(
        r"\s+\b(Men'?s Team|Women'?s Team|EDS\s*&\s*Academy|Community|Tickets)\b$",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"^\|\s*", "", cleaned).strip(" |-")
    return re.sub(r"\s+", " ", cleaned).strip()


def _is_thin_summary(summary: str, title: str) -> bool:
    """Return True if a summary is too thin to be useful editorially.

    RSS feeds (BBC, GMP) sometimes provide a one-line teaser as
    description that is identical to the title or only slightly longer.
    In those cases we want a richer og:description from the article HTML
    instead of treating the RSS field as authoritative.
    """

    cleaned = _clean_snippet(summary)
    if len(cleaned) < _SUMMARY_THIN_THRESHOLD:
        return True
    if cleaned.lower() == str(title or "").strip().lower():
        return True
    return False


def _source_specific_summary(source: SourceDef, title: str, summary: str) -> str:
    cleaned = _clean_snippet(summary)
    if not cleaned:
        return ""
    lowered = cleaned.lower()
    if source.name == "GMMH" and lowered.startswith("read the latest news from greater manchester mental health nhs foundation trust"):
        return ""
    if source.report_category == "media_layer" and lowered == title.strip().lower():
        return ""
    if source.report_category in {"media_layer", "gmp"} and len(cleaned) < 30:
        return ""
    return cleaned


def _derive_lead(source: SourceDef, title: str, summary: str) -> str:
    cleaned_summary = _clean_snippet(summary)
    if cleaned_summary:
        lowered = cleaned_summary.lower()
        if source.name == "GMMH" and lowered.startswith("read the latest news from greater manchester mental health nhs foundation trust"):
            return title
        if source.report_category in {"media_layer", "gmp"} and lowered.startswith(("read more about", "latest news", "breaking news")):
            return title
        first_sentence = re.split(r"(?<=[.!?])\s+", cleaned_summary, maxsplit=1)[0].strip()
        if first_sentence and len(first_sentence) >= 30 and first_sentence.lower() != title.strip().lower():
            return first_sentence
    return title


def _default_lead(source: SourceDef, title: str, summary: str = "") -> str:
    if source.report_category in {"media_layer", "gmp", "public_services"} or source.candidate_category == "council":
        cleaned_summary = _clean_snippet(summary)
        if cleaned_summary:
            first_sentence = re.split(r"(?<=[.!?])\s+", cleaned_summary, maxsplit=1)[0].strip()
            if first_sentence and first_sentence.lower() != title.strip().lower():
                return first_sentence
        return title
    return ""


def _default_summary(source: SourceDef, title: str) -> str:
    # Reserved for future per-source defaults. Currently empty for all
    # sources; collector callers fall through to RSS/og:description.
    return ""


def _looks_like_active_disruption(title: str) -> bool:
    lowered = str(title or "").lower()
    tokens = ("strike", "industrial action", "disruption", "cancel", "closure", "delay", "walkout")
    return any(token in lowered for token in tokens)


def _default_practical_angle(source: SourceDef, title: str = "", summary: str = "") -> str:
    lowered = title.lower()
    summary_lowered = summary.lower()
    if source.report_category == "transport":
        return "Есть возможное влияние на поездки сегодня."
    if source.report_category == "venues_tickets":
        if "onsale" in source.name.lower():
            return "Проверьте старт продаж и наличие билетов на официальной странице."
        return "Проверьте время, вход и наличие билетов на официальной странице."
    if source.report_category == "football":
        return "Проверьте время матча и официальные обновления клуба."
    if source.report_category == "public_services":
        if _looks_like_active_disruption(title):
            return "Если это касается вашего сервиса сегодня, уточнить статус у провайдера заранее."
        return "Это может повлиять на доступ к сервисам или работу учреждений."
    if source.report_category == "culture_weekly":
        return "Проверьте дату, время и бронирование перед планированием."
    if source.report_category == "food_openings":
        return "Новое место или запуск стоит проверить перед визитом."
    if source.report_category in {"media_layer", "gmp"} or source.candidate_category == "council":
        blob = f"{lowered} {summary_lowered}"
        if any(token in blob for token in ("weather warning", "amber warning", "yellow warning", "red warning", "flood")):
            return "Проверить, действует ли предупреждение в вашем районе и нужна ли смена планов на день."
        if any(token in summary_lowered for token in ("windows closed", "residents urged")) or "warning" in blob:
            return "Проверить, остаётся ли предупреждение активным сегодня и касается ли оно поездки или района."
        if any(token in blob for token in ("evacuat", "cordon", "lockdown")):
            return "Может сохраняться ограничение доступа или эвакуация в районе."
        if any(token in blob for token in ("fire", "blaze", "smoke")):
            return "Проверить, есть ли действующие ограничения по дыму или дорожные закрытия сегодня."
        if any(token in blob for token in ("closure", "closed", "shut", "cancel", "cancellation")):
            return "Проверить, есть ли действующие ограничения, предупреждения или влияние на район сегодня."
        if any(token in blob for token in ("charged", "arrest", "police appeal", "court", "sentence", "stab", "knife", "murder")):
            return "Полиция или суд сообщили новое развитие по делу."
        if any(token in blob for token in ("election", "polls", "by-election", "council", "mayor", "manifesto", "campaign", "vote", "ballot")):
            return "Учитывать как новую фазу городской политики или public-affairs повестки сегодня."
        if any(token in blob for token in ("metrolink", "bee network", "bus service", "tram", "rail", "train", "airport", "tfgm")):
            return "Проверить, влияет ли это на поездки по Greater Manchester сегодня или в ближайшие дни."
        if any(token in blob for token in ("hospital", "nhs", "gp", "a&e", "ambulance", "clinic", "doctor")):
            return "Если это касается доступа к сервисам или приёму, уточнить у провайдера до похода."
        if any(token in blob for token in ("school", "college", "university", "exam", "ofsted")):
            return "Если затрагивает ваше учебное заведение, проверить уведомления школы или вуза."
        if any(token in blob for token in ("housing", "rent", "evict", "homeless")):
            return "Учитывать как изменение жилищной политики или поддержки жителей."
        return "Есть новый городской контекст для Greater Manchester."
    return "Есть обновление, которое может быть полезно в сегодняшней сводке."
