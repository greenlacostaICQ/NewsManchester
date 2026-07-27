"""Single product policy registry for every digest block.

Planner, night inventory, rewrite and the pre-send judge must not maintain
parallel ideas of a block's size, geography or recovery contract.  This module
is deliberately dependency-free so lower-level modules (including ``common``)
can derive their compatibility constants from the same registry.
"""
from __future__ import annotations


BLOCK_POLICY_VERSION = "2026-07-27.p0"


BLOCK_POLICY_REGISTRY: dict[str, dict[str, object]] = {
    "weather": {
        "heading": "Погода", "min": 1, "max": 1, "geo_scope": "gm",
        "schedule": "daily", "required_fields": (), "repeat_policy": "operational",
        "backup_depth": 0, "min_sources": 1, "max_per_source": 0,
        "source_report_categories": frozenset({"synthetic"}), "candidate_categories": frozenset({"weather"}),
        "mode": "live_only", "serving_ttl_hours": 0.5, "retention_days": 2,
        "text_policy": "morning_live", "source_replacement_allowed": False,
        "optional": False, "intake_cap": 0,
    },
    "transport": {
        "heading": "Общественный транспорт сегодня", "min": 0, "max": 0, "geo_scope": "gm",
        "schedule": "daily", "required_fields": ("what_happened", "why_now"),
        "repeat_policy": "operational", "backup_depth": 0, "min_sources": 1, "max_per_source": 0,
        "source_report_categories": frozenset({"transport"}), "candidate_categories": frozenset({"transport"}),
        "mode": "hybrid", "serving_ttl_hours": 1.0, "retention_days": 14,
        "text_policy": "morning_live", "source_replacement_allowed": False,
        "optional": True, "intake_cap": 0,
    },
    "today_focus": {
        "heading": "Что важно сегодня", "min": 3, "max": 5, "geo_scope": "gm",
        "schedule": "daily", "required_fields": ("what_happened", "why_now"),
        "repeat_policy": "new_reader_action_today", "backup_depth": 3, "min_sources": 2, "max_per_source": 0,
        "source_report_categories": frozenset({"media_layer", "gmp", "public_services"}),
        "candidate_categories": frozenset({"media_layer", "gmp", "public_services", "council"}),
        "mode": "live_only", "serving_ttl_hours": 6.0, "retention_days": 7,
        "text_policy": "morning_live", "source_replacement_allowed": False,
        "optional": False, "intake_cap": 0,
    },
    "last_24h": {
        "heading": "Свежие новости", "min": 6, "max": 9, "geo_scope": "gm",
        "schedule": "daily", "required_fields": ("what_happened", "why_now"),
        "repeat_policy": "new_facts_only", "backup_depth": 3, "min_sources": 2, "max_per_source": 3,
        "source_report_categories": frozenset({"media_layer", "gmp"}),
        "candidate_categories": frozenset({"media_layer", "gmp", "council"}),
        "mode": "hybrid", "serving_ttl_hours": 6.0, "retention_days": 14,
        "text_policy": "morning_live", "source_replacement_allowed": False,
        "optional": False, "intake_cap": 0,
    },
    "lead_story": {
        "heading": "Главная история дня", "min": 1, "max": 1, "geo_scope": "gm",
        "schedule": "daily", "required_fields": ("what_happened", "why_now"),
        "repeat_policy": "new_facts_only", "backup_depth": 2, "min_sources": 1, "max_per_source": 0,
        "source_report_categories": frozenset({"media_layer", "gmp", "public_services"}),
        "candidate_categories": frozenset({"media_layer", "gmp", "public_services", "council"}),
        "mode": "live_only", "serving_ttl_hours": 6.0, "retention_days": 14,
        "text_policy": "morning_live", "source_replacement_allowed": False,
        "optional": False, "intake_cap": 0,
    },
    "city_watch": {
        "heading": "Городской радар", "min": 5, "max": 12, "geo_scope": "gm",
        "schedule": "daily", "required_fields": ("what_happened", "why_now"),
        "repeat_policy": "new_facts_only", "backup_depth": 3, "min_sources": 2, "max_per_source": 2,
        "source_report_categories": frozenset({"media_layer", "gmp", "public_services", "transport", "tech_business"}),
        "candidate_categories": frozenset({"media_layer", "gmp", "public_services", "council", "transport", "tech_business"}),
        "mode": "hybrid", "serving_ttl_hours": 24.0, "retention_days": 30,
        "text_policy": "morning_live", "source_replacement_allowed": False,
        "optional": False, "intake_cap": 0,
    },
    "weekend_activities": {
        "heading": "Выходные в GM", "min": 6, "max": 0, "geo_scope": "gm",
        "schedule": "thursday_to_sunday", "required_fields": ("event_name", "specific_event", "venue", "date_start", "action_url", "activity_type", "gm_fit"),
        "repeat_policy": "calendar_milestones", "backup_depth": 2, "min_sources": 2, "max_per_source": 0,
        "source_report_categories": frozenset({"culture_weekly"}), "candidate_categories": frozenset({"culture_weekly"}),
        "mode": "assist", "serving_ttl_hours": 96.0, "retention_days": 30,
        "text_policy": "deterministic_or_morning", "source_replacement_allowed": False,
        "optional": False, "intake_cap": 0,
    },
    "next_7_days": {
        "heading": "Что важно в ближайшие 7 дней", "min": 3, "max": 6, "geo_scope": "gm",
        "schedule": "daily", "required_fields": ("date_start", "non_leisure"),
        "repeat_policy": "calendar_milestones", "backup_depth": 2, "min_sources": 2, "max_per_source": 2,
        "source_report_categories": frozenset({"media_layer", "gmp", "public_services", "transport"}),
        "candidate_categories": frozenset({"media_layer", "gmp", "public_services", "council", "transport"}),
        "mode": "assist", "serving_ttl_hours": 96.0, "retention_days": 30,
        "text_policy": "deterministic_or_morning", "source_replacement_allowed": False,
        "optional": False, "intake_cap": 12,
    },
    "future_announcements": {
        "heading": "Дальние анонсы", "min": 0, "max": 0, "geo_scope": "gm",
        "schedule": "daily", "required_fields": ("event_name", "venue", "action_url"),
        "repeat_policy": "calendar_milestones", "backup_depth": 2, "min_sources": 1, "max_per_source": 0,
        "source_report_categories": frozenset({"culture_weekly", "venues_tickets"}),
        "candidate_categories": frozenset({"culture_weekly", "venues_tickets"}),
        "mode": "assist", "serving_ttl_hours": 336.0, "retention_days": 30,
        "text_policy": "deterministic_or_morning", "source_replacement_allowed": False,
        "optional": True, "intake_cap": 20,
    },
    "ticket_radar": {
        "heading": "Билеты / Ticket Radar", "min": 2, "max": 15, "geo_scope": "gm",
        "schedule": "daily", "required_fields": ("event_name", "date_start", "venue", "action_url", "ticket_type", "tier", "venue_scope", "ticket_why_now"),
        "repeat_policy": "ticket_calendar_milestones", "backup_depth": 2, "min_sources": 2, "max_per_source": 0,
        "source_report_categories": frozenset({"venues_tickets"}), "candidate_categories": frozenset({"venues_tickets"}),
        "mode": "assist", "serving_ttl_hours": 168.0, "retention_days": 30,
        "text_policy": "deterministic_or_morning", "source_replacement_allowed": False,
        "optional": False, "intake_cap": 20,
    },
    "outside_gm_tickets": {
        "heading": "Крупные концерты вне GM", "min": 0, "max": 6, "geo_scope": "uk_outside_gm",
        "schedule": "daily", "required_fields": ("event_name", "date_start", "venue", "action_url", "ticket_type", "outside_a_tier"),
        "repeat_policy": "ticket_calendar_milestones", "backup_depth": 2, "min_sources": 1, "max_per_source": 0,
        "source_report_categories": frozenset({"venues_tickets"}), "candidate_categories": frozenset({"venues_tickets"}),
        "mode": "assist", "serving_ttl_hours": 336.0, "retention_days": 30,
        "text_policy": "deterministic_or_morning", "source_replacement_allowed": False,
        "optional": True, "intake_cap": 6,
    },
    "russian_events": {
        "heading": "Русскоязычные концерты и стендап UK", "min": 1, "max": 6, "geo_scope": "uk",
        "schedule": "daily", "required_fields": ("event_name", "specific_event", "date_start", "venue", "russian_evidence", "russian_geography", "action_url"),
        "repeat_policy": "calendar_milestones", "backup_depth": 2, "min_sources": 1, "max_per_source": 0,
        "source_report_categories": frozenset({"diaspora_events"}),
        "candidate_categories": frozenset({"russian_speaking_events", "diaspora_events"}),
        "mode": "assist", "serving_ttl_hours": 168.0, "retention_days": 30,
        "text_policy": "deterministic_or_morning", "source_replacement_allowed": False,
        "optional": False, "intake_cap": 10,
    },
    "openings": {
        "heading": "Еда, открытия и рынки", "min": 3, "max": 3, "geo_scope": "gm",
        "schedule": "daily", "required_fields": ("event_name", "specific_event", "venue", "specific_venue", "opening_phase_or_date", "food_meaning", "action_url"),
        "repeat_policy": "new_phase_only", "backup_depth": 3, "min_sources": 2, "max_per_source": 0,
        "source_report_categories": frozenset({"food_openings"}), "candidate_categories": frozenset({"food_openings"}),
        "mode": "assist", "serving_ttl_hours": 168.0, "retention_days": 90,
        "text_policy": "morning_writer", "source_replacement_allowed": False,
        "source_not_modified_confirms_inventory": True, "optional": False, "intake_cap": 10,
    },
    "tech_business": {
        "heading": "IT и бизнес", "min": 0, "max": 5, "geo_scope": "gm",
        "schedule": "daily", "required_fields": ("what_happened", "why_now"),
        "repeat_policy": "new_facts_only", "backup_depth": 2, "min_sources": 1, "max_per_source": 0,
        "source_report_categories": frozenset({"tech_business"}), "candidate_categories": frozenset({"tech_business"}),
        "mode": "hybrid", "serving_ttl_hours": 24.0, "retention_days": 30,
        "text_policy": "morning_live", "source_replacement_allowed": False,
        "optional": True, "intake_cap": 0,
    },
    "professional_events": {
        "heading": "Business/tech события для тебя", "min": 1, "max": 4, "geo_scope": "gm",
        "schedule": "daily", "required_fields": ("event_name", "specific_event", "venue", "date_start", "professional_llm_cv", "professional_access", "action_url"),
        "repeat_policy": "calendar_milestones", "backup_depth": 2, "min_sources": 1, "max_per_source": 0,
        "source_report_categories": frozenset({"professional_events"}), "candidate_categories": frozenset({"professional_events"}),
        "mode": "assist", "serving_ttl_hours": 168.0, "retention_days": 30,
        "text_policy": "deterministic_or_morning", "source_replacement_allowed": False,
        "optional": False, "intake_cap": 10,
    },
    "football": {
        "heading": "Футбол", "min": 2, "max": 3, "geo_scope": "gm",
        "schedule": "daily", "required_fields": ("what_happened", "why_now"),
        "repeat_policy": "new_facts_only", "backup_depth": 2, "min_sources": 1, "max_per_source": 0,
        "source_report_categories": frozenset({"football"}), "candidate_categories": frozenset({"football"}),
        "mode": "hybrid", "serving_ttl_hours": 12.0, "retention_days": 14,
        "text_policy": "morning_live", "source_replacement_allowed": False,
        "optional": False, "intake_cap": 0,
    },
    "district_radar": {
        "heading": "Радар по районам", "min": 0, "max": 0, "geo_scope": "gm",
        "schedule": "retired", "required_fields": (), "repeat_policy": "never",
        "backup_depth": 0, "min_sources": 0, "max_per_source": 0,
        "source_report_categories": frozenset(), "candidate_categories": frozenset(),
        "mode": "retired", "serving_ttl_hours": 0.0, "retention_days": 0,
        "text_policy": "none", "source_replacement_allowed": False,
        "optional": True, "intake_cap": 0,
    },
}


PRIMARY_BLOCKS = {
    block: str(policy["heading"])
    for block, policy in BLOCK_POLICY_REGISTRY.items()
}
BLOCK_BY_HEADING = {heading: block for block, heading in PRIMARY_BLOCKS.items()}


def block_policy(block_or_heading: str) -> dict[str, object]:
    block = BLOCK_BY_HEADING.get(str(block_or_heading or ""), str(block_or_heading or ""))
    return BLOCK_POLICY_REGISTRY.get(block, {})


def block_active_on_weekday(block_or_heading: str, weekday: int) -> bool:
    schedule = str(block_policy(block_or_heading).get("schedule") or "daily")
    if schedule == "retired":
        return False
    if schedule == "thursday_to_sunday":
        return int(weekday) >= 3
    return True
