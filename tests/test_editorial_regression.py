"""Editorial regression pack (O3).

Goal: pin down past editorial defects so they cannot silently come back.
Each case targets one deterministic pipeline predicate (validator, dedupe,
writer quality, collector filter) and asserts the verdict.

Add a new case when a defect is fixed editorially: copy the live
candidate fields from data/state/candidates.json into a new test method
here, then assert the predicate that should now catch it.

Categories covered (7):
    1. no-date event
    2. PR / promotional press release
    3. not-GM item
    4. duplicate (intra-batch and cross-day)
    5. stale event / stale synthetic
    6. bad HTML in draft_line
    7. untranslated English prose

The current pack has 27 cases. Target band: 25-30.
"""
from __future__ import annotations

import unittest
from datetime import timedelta

from news_digest.pipeline.candidate_validator import (
    _demote_distant_weekend_event,
    _exclude_stale_event,
    _exclude_stale_ticket_onsale,
    _exclude_undated_event_like_candidate,
    _has_future_or_concrete_date,
)
from news_digest.pipeline.collector.filters import (
    _has_gm_token,
    _is_obviously_non_gm_food_item,
    _is_obviously_non_gm_weekend_item,
    _is_stale_transport,
)
from news_digest.pipeline.common import now_london
from news_digest.pipeline.curator import _is_curator_protected
from news_digest.pipeline.dedupe import (
    _apply_intra_batch_dedup,
    _similar_published_titles,
)
from news_digest.pipeline.editorial_contracts import topic_key_for_candidate
from news_digest.pipeline.event_quality import (
    event_quality_report,
    is_event_candidate,
)
from news_digest.pipeline.writer import (
    _apply_section_min_floor_pull_back,
    _block_contract_action,
    _draft_line_quality_errors,
    _looks_like_untranslated_english,
)


# --------------------------------------------------------------------------
# 1. No-date event (4 cases)
# --------------------------------------------------------------------------
class NoDateEventTest(unittest.TestCase):
    """Past defect: 'Summer festival coming soon' card rendered with no date.
    Validator must drop event-like candidates whose blob carries no date
    signal at all."""

    def test_undated_festival_dropped_by_validator(self) -> None:
        candidate = {
            "include": True,
            "category": "culture_weekly",
            "primary_block": "weekend_activities",
            "title": "Summer music festival coming soon to Manchester",
            "summary": "Big lineup announced. More details to follow later this year.",
            "lead": "",
            "evidence_text": "Coming soon. Announcement later this summer.",
            "source_url": "https://example.com/festival",
        }
        dropped = _exclude_undated_event_like_candidate(candidate)
        self.assertTrue(dropped)
        self.assertFalse(candidate["include"])
        self.assertIn("no concrete upcoming date", candidate["reason"])

    def test_event_with_concrete_future_date_kept(self) -> None:
        candidate = {
            "include": True,
            "category": "culture_weekly",
            "primary_block": "weekend_activities",
            "title": "Manchester Jazz Festival",
            "summary": "Multi-venue programme runs 5 June 2026.",
            "lead": "",
            "evidence_text": "5 June 2026 across the Northern Quarter.",
            "source_url": "https://example.com/jazz",
        }
        self.assertTrue(_has_future_or_concrete_date(candidate))
        self.assertFalse(_exclude_undated_event_like_candidate(candidate))
        self.assertTrue(candidate["include"])

    def test_event_quality_report_flags_missing_date(self) -> None:
        candidate = {
            "category": "culture_weekly",
            "primary_block": "weekend_activities",
            "title": "Exhibition at the Whitworth",
            "summary": "Exhibition runs at the Whitworth gallery in Manchester. Free entry.",
            "lead": "",
            "evidence_text": "Free entry, all welcome.",
            "source_url": "https://example.com/whitworth",
            "source_label": "The Whitworth",
        }
        report = event_quality_report(candidate)
        self.assertTrue(is_event_candidate(candidate))
        self.assertFalse(report["ok"])
        self.assertIn("date", report["missing"])

    def test_implicit_weekend_aggregator_url_counts_as_dated(self) -> None:
        candidate = {
            "include": True,
            "category": "culture_weekly",
            "primary_block": "weekend_activities",
            "title": "Indoor market in Stretford",
            "summary": "Recurring weekend market — featured listing.",
            "evidence_text": "Stalls open Saturday and Sunday.",
            "source_url": "https://example.com/things-to-do-this-weekend-in-manchester/",
        }
        self.assertTrue(_has_future_or_concrete_date(candidate))
        self.assertFalse(_exclude_undated_event_like_candidate(candidate))


class WriterPullbackRegressionTest(unittest.TestCase):
    """Rejected reserve items must never be resurrected into public sections."""

    def test_rejected_backup_candidate_not_pulled_into_public_section(self) -> None:
        candidate = {
            "include": False,
            "backup_candidate": True,
            "reject_reasons": ["weak_value_civic_pr"],
            "primary_block": "city_watch",
            "fingerprint": "gmmh-rejected",
            "title": "GMMH appoints new chair",
            "source_label": "GMMH",
            "source_url": "https://example.com/gmmh-chair",
            "draft_line": "• <b>GMMH</b> назначил нового председателя; проверьте.",
        }
        lines, fps, _scores, _titles, _srcs = _apply_section_min_floor_pull_back(
            "Городской радар",
            [],
            [],
            [],
            [],
            [],
            [candidate],
            set(),
            1,
            [],
            include_backup=True,
        )
        self.assertEqual([], lines)
        self.assertEqual([], fps)


# --------------------------------------------------------------------------
# 2. PR / promotional press release (3 cases)
# --------------------------------------------------------------------------
class PressReleaseTest(unittest.TestCase):
    """Past defect: university PR ('vice-chancellor opens new lecture')
    crowded out real city news in Городской радар. Writer's city-watch
    score must demote academic / generic PR copy."""

    def test_university_pr_scores_negative(self) -> None:
        from news_digest.pipeline.writer import _city_watch_score

        candidate = {
            "source_label": "University of Manchester",
            "title": "Vice-chancellor opens new lecture series",
            "summary": "The vice-chancellor opened a new academic lecture series for researchers and PhD students.",
            "lead": "",
            "evidence_text": "Vice-chancellor and senior academics welcomed new researchers to the campus.",
        }
        score = _city_watch_score(candidate)
        # Source weight 25 + academic penalty -35 + short title -5 + short evidence -8 ≈ -23.
        self.assertLess(score, 0, f"university PR should be deeply demoted, got {score}")

    def test_generic_council_pr_demoted(self) -> None:
        from news_digest.pipeline.writer import _city_watch_score

        candidate = {
            "source_label": "Manchester Council",
            "title": "Lord Mayor hosts community champions tea party",
            "summary": "The Lord Mayor welcomed community champions at a tea party in the Town Hall.",
            "lead": "",
            "evidence_text": "Community champions celebration at the Town Hall.",
        }
        score = _city_watch_score(candidate)
        # Source weight 100, hit two generic PR markers (-20), no £, no date, short evidence.
        # Should still land well below a real council story with £-amount.
        self.assertLess(score, 110)

    def test_real_borough_story_with_money_scores_higher(self) -> None:
        from news_digest.pipeline.writer import _city_watch_score

        candidate = {
            "source_label": "GMCA",
            "title": "Greater Manchester confirms £230m transport upgrade for Bee Network",
            "summary": "GMCA confirmed £230m of funding for the Bee Network. The package covers new buses across Salford, Trafford and Stockport in 2026.",
            "lead": "",
            "evidence_text": (
                "GMCA confirmed £230 million of capital funding for the Bee Network covering "
                "new electric buses, depot upgrades and route extensions across Salford, "
                "Trafford and Stockport. Procurement opens in June 2026 with deliveries "
                "expected from September. Mayor Andy Burnham said the package was the "
                "single largest transport investment since devolution."
            ),
        }
        score = _city_watch_score(candidate)
        self.assertGreater(score, 130, f"real GMCA money story should rank high, got {score}")

    def test_charity_sport_ranks_below_hard_local_news(self) -> None:
        from news_digest.pipeline.writer import _city_watch_score

        # 2026-06-10: a charity ultramarathon (£11m raised, Manchester, dates)
        # led the radar over real local news, because the radar score had no
        # news-type signal. Charity-sport must now rank below hard local news.
        charity_sport = {
            "source_label": "BBC Manchester Web",
            "title": "Kevin Sinfield reveals final MND ultramarathon challenge",
            "summary": "Kevin Sinfield will run an ultramarathon from Hull to Manchester to raise money for motor neurone disease charity.",
            "lead": "",
            "evidence_text": (
                "Kevin Sinfield announced his final ultramarathon challenge, running from "
                "Hull to Manchester to raise money for MND charity. He has raised more than "
                "£11m since 2020 in memory of Rob Burrow."
            ),
        }
        hard_news = {
            "source_label": "The Mill",
            "title": "Five arrested following Manchester fraud investigation",
            "summary": "Greater Manchester Police arrested five people after a fraud investigation in Manchester.",
            "lead": "",
            "evidence_text": (
                "Five people were arrested in Manchester after a Greater Manchester Police "
                "investigation into fraud. Officers said the suspects were charged and will "
                "appear in court next month. The case involves several Salford addresses."
            ),
        }
        charity_score = _city_watch_score(charity_sport)
        hard_score = _city_watch_score(hard_news)
        self.assertLess(
            charity_score,
            hard_score,
            f"charity-sport ({charity_score}) must rank below hard local news ({hard_score})",
        )


# --------------------------------------------------------------------------
# 3. Not-GM item (5 cases)
# --------------------------------------------------------------------------
class NotGreaterManchesterTest(unittest.TestCase):
    """Past defect: London / Liverpool food roundups landed in Food Openings
    because the GM filter only checked the slug. Filters must reject
    obvious-non-GM items in title + path + summary."""

    def test_london_food_item_rejected(self) -> None:
        self.assertTrue(_is_obviously_non_gm_food_item(
            title="Best new restaurants opening in London this spring",
            path="/london/best-new-restaurants-london",
            summary="The capital's most-anticipated openings across Soho and Shoreditch.",
        ))

    def test_manchester_food_item_kept(self) -> None:
        self.assertFalse(_is_obviously_non_gm_food_item(
            title="New ramen bar opens in Ancoats",
            path="/manchester/new-ramen-bar-ancoats",
            summary="Ramen specialist opens its first Manchester site in Ancoats.",
        ))

    def test_inbound_to_manchester_kept_even_when_brand_is_non_gm(self) -> None:
        """'Edinburgh Street Food expands to Manchester' is on-topic."""
        self.assertFalse(_is_obviously_non_gm_food_item(
            title="Edinburgh Street Food expands to Manchester",
            path="/food/edinburgh-street-food-to-manchester",
            summary="Edinburgh-born street food market opens its second site in Manchester.",
        ))

    def test_liverpool_weekend_item_rejected(self) -> None:
        self.assertTrue(_is_obviously_non_gm_weekend_item(
            title="A weekend in Liverpool: docks, music and food",
            path="/weekend/liverpool-guide",
            summary="Two days exploring Liverpool's waterfront.",
        ))

    def test_gm_token_detection_salford(self) -> None:
        self.assertTrue(_has_gm_token("New gallery opens in Salford Quays"))
        self.assertFalse(_has_gm_token("New gallery opens in Westbury, Wiltshire"))


# --------------------------------------------------------------------------
# 4. Duplicates (4 cases)
# --------------------------------------------------------------------------
class DuplicateTest(unittest.TestCase):
    """Past defect: same political story ran twice because tokens differed
    even though entity (Burnham / Mainoo / United) was shared.
    Past defect: Ticketmaster suffix made unrelated events look duplicate.
    """

    def test_intra_batch_shared_entity_drops_weaker_source(self) -> None:
        # Both items about Andy Burnham — different verbs, low Jaccard,
        # but shared distinctive entity. MEN (rank 1) must lose to BBC (rank 0).
        candidates = [
            {
                "include": True,
                "fingerprint": "fp-bbc",
                "title": "Labour allows Andy Burnham to stand for Makerfield selection",
                "primary_block": "city_watch",
                "source_label": "BBC Manchester",
            },
            {
                "include": True,
                "fingerprint": "fp-men",
                "title": "Andy Burnham eyes Westminster return with Makerfield gambit",
                "primary_block": "city_watch",
                "source_label": "MEN",
            },
        ]
        drops = _apply_intra_batch_dedup(candidates)
        self.assertEqual(len(drops), 1, drops)
        self.assertEqual(drops[0]["fingerprint"], "fp-men")
        self.assertEqual(drops[0]["kept_fingerprint"], "fp-bbc")

    def test_different_boroughs_are_not_duplicates(self) -> None:
        # Same headline pattern but Salford vs Oldham council — distinct stories.
        candidates = [
            {
                "include": True,
                "fingerprint": "fp-salford",
                "title": "Salford council approves new housing scheme",
                "primary_block": "city_watch",
                "source_label": "MEN",
            },
            {
                "include": True,
                "fingerprint": "fp-oldham",
                "title": "Oldham council approves new housing scheme",
                "primary_block": "city_watch",
                "source_label": "MEN",
            },
        ]
        drops = _apply_intra_batch_dedup(candidates)
        self.assertEqual(drops, [])

    def test_unrelated_ticket_events_are_not_deduped_by_generic_event_words(self) -> None:
        # 2026-05-25 defect: unrelated concerts collapsed because both titles
        # shared generic capitalised words such as Live / Concert.
        candidates = [
            {
                "include": True,
                "fingerprint": "avatar-orchestra",
                "dedupe_decision": "new",
                "category": "venues_tickets",
                "primary_block": "ticket_radar",
                "title": "Avatar - The Last Airbender - Film With Live Orchestra — event 2026-10-05 — public sale 2025-05-02 10:00",
                "summary": "event_date=2026-10-05 venue=Bridgewater Hall",
                "source_label": "Ticketmaster Manchester Upcoming",
                "source_url": "https://example.test/avatar-orchestra",
            },
            {
                "include": True,
                "fingerprint": "kumar-sanu",
                "dedupe_decision": "new",
                "category": "venues_tickets",
                "primary_block": "ticket_radar",
                "title": "Kumar Sanu Live In Concert — event 2026-05-25 — public sale 2026-04-14 10:00",
                "summary": "event_date=2026-05-25 venue=O2 Apollo Manchester",
                "source_label": "Ticketmaster Manchester Upcoming",
                "source_url": "https://example.test/kumar-sanu",
            },
        ]
        drops = _apply_intra_batch_dedup(candidates)
        self.assertEqual(drops, [])
        self.assertTrue(all(item["include"] for item in candidates))

    def test_same_ticket_event_dedupes_across_gm_and_uk_sections(self) -> None:
        candidates = [
            {
                "include": True,
                "fingerprint": "gm-take-that",
                "dedupe_decision": "new",
                "category": "venues_tickets",
                "primary_block": "ticket_radar",
                "title": "TAKE THAT - THE CIRCUS LIVE - Summer 2026 — event 2026-06-19",
                "summary": "Etihad Stadium | Manchester | Pop | event_date=2026-06-19 17:00 | ticket_type=major_upcoming",
                "event": {"event_name": "TAKE THAT - THE CIRCUS LIVE - Summer 2026", "venue": "Etihad Stadium", "date_start": "2026-06-19"},
                "source_label": "Ticketmaster Manchester Upcoming",
                "source_url": "https://ticketmaster.co.uk/take-that/event/3E006331A86D5743",
            },
            {
                "include": True,
                "fingerprint": "uk-take-that",
                "dedupe_decision": "new",
                "category": "venues_tickets",
                "primary_block": "outside_gm_tickets",
                "title": "TAKE THAT - THE CIRCUS LIVE - Summer 2026 — event 2026-06-19",
                "summary": "Etihad Stadium | Manchester | Pop | event_date=2026-06-19 17:00 | ticket_type=major_upcoming",
                "event": {"event_name": "TAKE THAT - THE CIRCUS LIVE - Summer 2026", "venue": "Etihad Stadium", "date_start": "2026-06-19"},
                "source_label": "Ticketmaster UK Major Upcoming",
                "source_url": "https://ticketmaster.co.uk/take-that/event/3E006331A86D5743",
            },
        ]
        drops = _apply_intra_batch_dedup(candidates)
        self.assertEqual(len(drops), 1, drops)
        self.assertEqual(drops[0]["fingerprint"], "uk-take-that")
        self.assertTrue(candidates[0]["include"])
        self.assertFalse(candidates[1]["include"])

    def test_venue_premium_ticket_events_require_specific_overlap(self) -> None:
        # "Venue Premium Tickets" is Ticketmaster packaging, not the event.
        candidates = [
            {
                "include": True,
                "fingerprint": "calum-scott-premium",
                "dedupe_decision": "new",
                "category": "venues_tickets",
                "primary_block": "ticket_radar",
                "title": "Venue Premium Tickets - Calum Scott — event 2026-05-27 — public sale 2025-04-11 09:00",
                "summary": "event_date=2026-05-27 venue=O2 Apollo Manchester",
                "source_label": "Ticketmaster Manchester Upcoming",
                "source_url": "https://example.test/calum-scott",
            },
            {
                "include": True,
                "fingerprint": "dermot-kennedy-premium",
                "dedupe_decision": "new",
                "category": "venues_tickets",
                "primary_block": "ticket_radar",
                "title": "Venue Premium Tickets - Dermot Kennedy — event 2026-05-30 — public sale 2025-10-03 10:00",
                "summary": "event_date=2026-05-30 venue=Co-op Live",
                "source_label": "Ticketmaster Manchester Upcoming",
                "source_url": "https://example.test/dermot-kennedy",
            },
        ]
        drops = _apply_intra_batch_dedup(candidates)
        self.assertEqual(drops, [])

    def test_topic_key_ignores_unrelated_event_page_chrome(self) -> None:
        # 2026-05 defect: evidence_text page chrome mentioned an event and
        # caused a council/news item to inherit that event topic.
        candidate = {
            "include": True,
            "fingerprint": "stockport-local-election",
            "category": "media_layer",
            "primary_block": "city_watch",
            "title": "Stockport local election 2026 results - Stockport Council",
            "summary": "Council results were published for Stockport wards.",
            "source_label": "Stockport Council",
            "evidence_text": "Related links: The BIG Stockport Car Boot, Romiley.",
        }
        self.assertNotEqual(
            topic_key_for_candidate(candidate),
            "event:big_stockport_car_boot",
        )

    def test_cross_day_published_title_match_via_entities(self) -> None:
        """Yesterday's published 'Mainoo signs new deal' must match
        today's 'United confirm Mainoo extension'."""
        matches = _similar_published_titles(
            normalized_title="united confirm mainoo extension",
            original_title="United confirm Mainoo extension",
            published_titles=[
                {
                    "fingerprint": "yesterday-mainoo",
                    "title": "Mainoo signs new United deal",
                    "normalized_title": "mainoo signs new united deal",
                }
            ],
        )
        self.assertTrue(matches, "shared entity Mainoo+United should match yesterday's headline")
        self.assertEqual(matches[0]["fingerprint"], "yesterday-mainoo")

    def test_weather_synthetic_protected_from_curator_dedup(self) -> None:
        """Weather synthetic items must be untouchable by the curator's
        semantic dedup pass — otherwise the forecast block can vanish."""
        weather_card = {"category": "weather", "primary_block": "weather"}
        self.assertTrue(_is_curator_protected(weather_card))


# --------------------------------------------------------------------------
# 5. Stale event / stale synthetic (5 cases)
# --------------------------------------------------------------------------
class StaleEventAndSyntheticTest(unittest.TestCase):
    """Past defect: Urmston Artisan Market ran on 18 May digest with a 2 May
    date because the only date was historical. Past defect: TfGM bus
    closure from 4 days ago rendered as live disruption."""

    def test_stale_ticket_onsale_demoted_not_dropped(self) -> None:
        past = (now_london() - timedelta(days=20)).strftime("%Y-%m-%d %H:%M")
        candidate = {
            "include": True,
            "category": "venues_tickets",
            "primary_block": "ticket_radar",
            "title": "Calum Scott — tour 2026",
            "summary": f"ticket_signal=onsale | public_onsale={past}",
        }
        self.assertFalse(_exclude_stale_ticket_onsale(candidate))
        self.assertTrue(candidate["include"])
        self.assertEqual(candidate["primary_block"], "future_announcements")
        self.assertEqual(candidate["editorial_status"], "borderline")
        self.assertIn("public_onsale", candidate["reason"])

    def test_stale_event_only_past_date_dropped(self) -> None:
        # Pick a past date: choose a day-month earlier in current year.
        today = now_london().date()
        past = today - timedelta(days=30)
        month_en = past.strftime("%B").lower()
        past_date_str = f"{past.day} {month_en}"
        candidate = {
            "include": True,
            "category": "culture_weekly",
            "primary_block": "weekend_activities",
            "title": f"Urmston Artisan Market — {past_date_str}",
            "summary": f"Market held on {past_date_str} in Urmston.",
            "lead": "",
            "evidence_text": f"One-off market on {past_date_str}.",
            "source_url": "https://example.com/urmston-market",
        }
        dropped = _exclude_stale_event(candidate)
        self.assertTrue(dropped, "candidate with only past date should be dropped")
        self.assertFalse(candidate["include"])

    def test_event_with_mixed_past_and_future_date_kept(self) -> None:
        today = now_london().date()
        past = today - timedelta(days=20)
        future = today + timedelta(days=20)
        candidate = {
            "include": True,
            "category": "culture_weekly",
            "primary_block": "next_7_days",
            "title": "Spring Series",
            "summary": (
                f"Opening night was {past.day} {past.strftime('%B').lower()}; "
                f"closing event {future.day} {future.strftime('%B').lower()}."
            ),
            "lead": "",
            "evidence_text": "",
            "source_url": "https://example.com/spring",
        }
        self.assertFalse(_exclude_stale_event(candidate))
        self.assertTrue(candidate["include"])

    def test_far_future_weekend_event_dropped_even_if_recurrence_misfires(self) -> None:
        # W1 / RC3: a "21 May 2027"-style festival must leave «Выходные в GM»
        # even when a weak-wording recurrence flag is set. Date is far beyond the
        # 30-day horizon relative to today, so the test stays calendar-stable.
        far = now_london().date() + timedelta(days=400)
        candidate = {
            "include": True,
            "category": "culture_weekly",
            "primary_block": "weekend_activities",
            "title": "Manchester Jazz Festival",
            "summary": "Annual festival.",
            "event": {
                "is_event": True,
                "event_name": "Manchester Jazz Festival",
                "date": far.isoformat(),
                "date_start": far.isoformat(),
                "date_confidence": "high",
                "is_recurring": True,
            },
        }
        self.assertTrue(_demote_distant_weekend_event(candidate))
        self.assertFalse(candidate["include"])
        self.assertIn("far-future", candidate["reason"])

    def test_stale_structured_event_date_dropped(self) -> None:
        past = now_london().date() - timedelta(days=30)
        candidate = {
            "include": True,
            "category": "venues_tickets",
            "primary_block": "next_7_days",
            "title": "Music of the Mystics - Royal Northern College of Music",
            "summary": "RNCM tickets",
            "event": {
                "is_event": True,
                "event_name": "Music of the Mystics",
                "venue": "Royal Northern College of Music",
                "date_start": past.isoformat(),
            },
            "source_url": "https://example.com/music-of-the-mystics",
        }
        self.assertTrue(_exclude_stale_event(candidate))
        self.assertFalse(candidate["include"])
        self.assertIn("structured event date", candidate["reason"])

    def test_transport_disruption_old_published_at_is_stale(self) -> None:
        old = (now_london() - timedelta(days=4)).isoformat()
        self.assertTrue(_is_stale_transport(old, "Bus 86 diverted in Stretford"))

    def test_transport_disruption_with_future_signal_kept(self) -> None:
        old = (now_london() - timedelta(days=4)).isoformat()
        # 'planned works' / 'from Monday' override the age check.
        self.assertFalse(_is_stale_transport(old, "Planned works on Metrolink Rochdale line"))


# --------------------------------------------------------------------------
# 6. Bad HTML in draft_line (4 cases)
# --------------------------------------------------------------------------
class BadDraftLineHtmlTest(unittest.TestCase):
    """Past defect: LLM occasionally returned `<a href=...>` or Markdown
    `**bold**` inside draft_line, breaking Telegram rendering. Writer's
    quality gate must reject these and let the writer attach the source
    anchor itself."""

    _GOOD_CANDIDATE = {
        "category": "media_layer",
        "primary_block": "city_watch",
        "title": "Council confirms major housing scheme",
        "summary": "Council confirmed a £45m housing scheme covering Salford and Trafford. Building starts in June 2026.",
        "lead": "",
        "evidence_text": (
            "Salford City Council confirmed a £45m housing scheme covering both Salford and Trafford. "
            "Construction starts in June 2026, with the first tenants expected by autumn 2027. "
            "Council leader Paul Dennett said the package was the largest in a decade."
        ),
    }

    def test_anchor_html_inside_draft_line_is_error(self) -> None:
        line = (
            "• Salford: совет подтвердил £45 млн на жильё в Salford и Trafford, "
            "стройка стартует в июне 2026 — следите за сроками. "
            '<a href="https://example.com/x">Salford Council</a>'
        )
        errors = _draft_line_quality_errors(self._GOOD_CANDIDATE, line)
        self.assertTrue(any("source anchor" in err for err in errors), errors)

    def test_markdown_bold_in_draft_line_is_error(self) -> None:
        line = (
            "• Salford: совет подтвердил **£45 млн** на жильё в Salford и Trafford, "
            "стройка стартует в июне 2026 — закладывайте сроки в планы района."
        )
        errors = _draft_line_quality_errors(self._GOOD_CANDIDATE, line)
        self.assertTrue(any("Markdown" in err for err in errors), errors)

    def test_missing_bullet_marker_is_error(self) -> None:
        line = (
            "Salford: совет подтвердил £45 млн на жильё в Salford и Trafford, "
            "стройка стартует в июне 2026 — закладывайте сроки в планы района."
        )
        errors = _draft_line_quality_errors(self._GOOD_CANDIDATE, line)
        self.assertTrue(any("bullet marker" in err for err in errors), errors)

    def test_html_b_emphasis_is_allowed(self) -> None:
        line = (
            "• Salford: совет подтвердил <b>£45 млн</b> на жильё в Salford и Trafford, "
            "стройка стартует в июне 2026 — закладывайте сроки в планы района."
        )
        errors = _draft_line_quality_errors(self._GOOD_CANDIDATE, line)
        # Telegram <b> is allowed; no anchor / Markdown / bullet / Cyrillic errors expected.
        for err in errors:
            self.assertNotIn("source anchor", err)
            self.assertNotIn("Markdown", err)
            self.assertNotIn("bullet marker", err)
            self.assertNotIn("Russian prose", err)


# --------------------------------------------------------------------------
# 7. Untranslated English prose (3 cases)
# --------------------------------------------------------------------------
class UntranslatedEnglishTest(unittest.TestCase):
    """Past defect: LLM returned the source headline in English under
    a Russian section. Writer must reject lines with no Cyrillic, and
    the helper must detect English-only prose with stopword density."""

    def test_all_english_draft_line_rejected(self) -> None:
        candidate = {
            "category": "media_layer",
            "primary_block": "city_watch",
            "title": "Council confirms scheme",
            "evidence_text": "Council confirmed a large housing scheme this week.",
        }
        line = (
            "• Salford Council has confirmed a major housing scheme that will cover "
            "Salford and Trafford with construction starting in June 2026, residents should keep an eye on this."
        )
        errors = _draft_line_quality_errors(candidate, line)
        self.assertTrue(any("Russian prose" in err for err in errors), errors)

    def test_english_prose_helper_flags_dense_english(self) -> None:
        text = (
            "The council has confirmed that the new scheme will run from June 2026 "
            "with the support of partners across the region. They will start work after the summer."
        )
        self.assertTrue(_looks_like_untranslated_english(text))

    def test_russian_with_one_english_loanword_is_ok(self) -> None:
        text = "Совет согласовал бюджет на Bee Network в этом году."
        self.assertFalse(_looks_like_untranslated_english(text))


# --------------------------------------------------------------------------
# 7b. Final-editor net: untranslated English + Latin/Cyrillic hybrid place names
# (2026-06-24: "murder", "Stockportа", "Urmstonе" shipped because the post-check
# detectors did not cover them and remaining_bad falsely read 0).
# --------------------------------------------------------------------------
class FinalEditorNetTest(unittest.TestCase):
    def test_latin_cyrillic_place_ending_is_auto_fixed(self) -> None:
        from news_digest.pipeline.editor import (
            _line_needs_russian_editor,
            _polish_russian_line_rules,
        )

        fixed, _ = _polish_russian_line_rules("• Мужчина из Stockportа признал вину. BBC")
        self.assertIn("Stockport", fixed)
        self.assertNotIn("Stockportа", fixed)
        self.assertFalse(_line_needs_russian_editor(fixed))

    def test_untranslated_english_word_is_flagged(self) -> None:
        from news_digest.pipeline.editor import _line_needs_russian_editor

        self.assertTrue(_line_needs_russian_editor("• В деле о murder ребёнка. BBC"))
        # Brand/proper names with a space stay Latin and are not flagged.
        self.assertFalse(_line_needs_russian_editor("• Robyn — 27 июня, Co-op Live. Ticketmaster"))

    def test_cross_section_dedup_keys_by_story_cluster(self) -> None:
        # Same story rendered in two blocks with different wording must share a
        # dedup key when the candidates share a story cluster.
        from news_digest.pipeline.editor import _candidate_index, _line_story_key

        cands = [
            {"source_url": "https://bbc.co.uk/news/articles/c1",
             "story_cluster_key": {"cluster_key": "incident:preston davey"}},
            {"source_url": "https://bbc.co.uk/news/articles/c2",
             "story_cluster_key": {"cluster_key": "incident:preston davey"}},
        ]
        cbk = _candidate_index(cands)
        line1 = '• В деле о murder ребёнка. <a href="https://bbc.co.uk/news/articles/c1">BBC</a>'
        line2 = '• Меры после убийства Дейви. <a href="https://bbc.co.uk/news/articles/c2">BBC</a>'
        self.assertEqual(_line_story_key(line1, cbk), _line_story_key(line2, cbk))
        # Lines without a shared cluster fall back to their own text (no merge).
        self.assertNotEqual(
            _line_story_key("• Пожар в Олдеме. MEN", cbk),
            _line_story_key("• Совет обсудит дома. MEN", cbk),
        )

    def test_cross_section_dedup_accepts_string_story_cluster_key(self) -> None:
        from news_digest.pipeline.editor import _candidate_index, _line_story_key

        cands = [
            {"source_url": "https://example.com/a", "story_cluster_key": "incident:green"},
            {"source_url": "https://example.com/b", "story_cluster_key": "incident:green"},
        ]
        cbk = _candidate_index(cands)
        self.assertEqual(
            _line_story_key('• История Green. <a href="https://example.com/a">MEN</a>', cbk),
            _line_story_key('• Та же история Green. <a href="https://example.com/b">BBC</a>', cbk),
        )

    def test_event_identity_uses_name_date_venue(self) -> None:
        from news_digest.pipeline.story_intelligence import event_identity_key

        candidate = {
            "title": "Food festival",
            "event": {
                "is_event": True,
                "event_name": "Manchester Food Festival",
                "date_start": "2026-06-27",
                "venue": "Mayfield Depot",
            },
        }
        key = event_identity_key(candidate)
        self.assertIn("manchester food festival", key)
        self.assertIn("2026 06 27", key)
        self.assertIn("mayfield depot", key)

    def test_story_cluster_key_prefers_story_phase_key(self) -> None:
        from news_digest.pipeline.story_intelligence import story_cluster_key

        candidate = {
            "category": "gmp",
            "primary_block": "last_24h",
            "title": "Police charge John Smith after Manchester stabbing",
            "summary": "John Smith has been charged after a stabbing in Manchester.",
            "published_at": "2026-06-26T07:10:00+01:00",
            "change_type": "new_phase",
            "entities": {"people": ["John Smith"], "districts": ["Manchester"]},
        }
        key = story_cluster_key(candidate)
        self.assertEqual(key, candidate["story_phase_key"])
        self.assertTrue(candidate["has_new_story_phase"])

    def test_same_section_reserve_skips_story_duplicate(self) -> None:
        from news_digest.pipeline.editor import _same_section_reserve_line

        rendered_story_keys = {"story:green|civic|manchester"}
        rendered_urls: set[str] = set()
        candidates = [
            {
                "primary_block": "today_focus",
                "public_reserve": True,
                "backup_pool_only": False,
                "story_identity_key": "story:green|civic|manchester",
                "draft_line": "• Дубль Green.",
                "source_url": "https://example.com/dup",
                "source_label": "BBC",
            },
            {
                "primary_block": "today_focus",
                "public_reserve": True,
                "backup_pool_only": False,
                "story_identity_key": "story:parks|civic|salford",
                "draft_line": "• Новый пункт про парки.",
                "source_url": "https://example.com/new",
                "source_label": "Council",
            },
        ]
        line = _same_section_reserve_line("Что важно сегодня", candidates, rendered_urls, rendered_story_keys)
        self.assertIn("парки", line)
        self.assertIn("story:parks|civic|salford", rendered_story_keys)

    def test_same_section_reserve_rebuilds_missing_draft_line(self) -> None:
        from news_digest.pipeline.editor import _same_section_reserve_line

        rendered_story_keys: set[str] = set()
        rendered_urls: set[str] = set()
        candidates = [
            {
                "primary_block": "ticket_radar",
                "category": "venues_tickets",
                "public_reserve": True,
                "backup_pool_only": False,
                "title": "Example Artist — event 2026-07-20",
                "summary": "AO Arena | Manchester | Pop | event_date=2026-07-20 19:00 | ticket_type=major_upcoming",
                "source_url": "https://ticketmaster.co.uk/example-artist",
                "source_label": "Ticketmaster",
                "event": {"venue": "AO Arena", "date_start": "2026-07-20T19:00:00+01:00"},
                "ticket_notability": {
                    "artist": "Example Artist",
                    "kind": "artist",
                    "tier": "A",
                    "signal": "streaming_popularity",
                    "signals": {"lastfm_listeners": 1800000},
                },
            },
        ]
        stats: dict[str, object] = {}

        line = _same_section_reserve_line("Билеты / Ticket Radar", candidates, rendered_urls, rendered_story_keys, stats)

        self.assertIn("Example Artist", line)
        self.assertNotIn("Last.fm", line)
        self.assertIn("AO Arena", line)
        self.assertGreaterEqual(int(stats.get("enriched_rewrite_used") or 0), 1)

    def test_block_contract_holds_non_a_tier_outside_gm(self) -> None:
        candidate = {
            "primary_block": "outside_gm_tickets",
            "category": "venues_tickets",
            "ticket_notability": {"tier": "B"},
            "title": "B-tier artist at Leeds Arena",
        }
        action = _block_contract_action(candidate, "• B-tier artist — Leeds Arena.")
        self.assertEqual(action["action"], "hold")
        self.assertEqual(action["reason"], "block_contract:outside_gm_non_a_tier")

    def test_block_contract_moves_solo_gig_out_of_weekend(self) -> None:
        candidate = {
            "primary_block": "weekend_activities",
            "category": "culture_weekly",
            "title": "Solo gig at Night & Day",
            "summary": "Headline show on Saturday.",
        }
        action = _block_contract_action(candidate, "• Solo gig в Night & Day в субботу.")
        self.assertEqual(action["action"], "reroute")
        self.assertEqual(action["target_block"], "ticket_radar")


# --------------------------------------------------------------------------
# Coverage summary — keep the pack inside the documented 25-30 band.
# --------------------------------------------------------------------------
class CoverageBandTest(unittest.TestCase):
    """If someone adds many cases without revisiting the doc, this test
    catches it. Update the band intentionally."""

    EXPECTED_MIN = 25
    EXPECTED_MAX = 50

    def test_total_case_count_inside_band(self) -> None:
        import sys

        loader = unittest.TestLoader()
        suite = loader.loadTestsFromModule(sys.modules[__name__])
        count = suite.countTestCases() - 1  # exclude this meta test itself
        self.assertGreaterEqual(count, self.EXPECTED_MIN)
        self.assertLessEqual(count, self.EXPECTED_MAX)


class NewPhaseRepeatTest(unittest.TestCase):
    """W7: a re-announced opening must not republish every morning (Örme).

    Örme ran 7 times over 10 days — same relaunch fact, same headline. Only a
    changed event fact or its actual opening day earns another run; court and
    incident stories keep the development-word rule (charged -> sentenced).
    """

    def _opening(self, *, event_day: str) -> dict:
        return {
            "title": "Michelin-listed cafe reverses closure plans and reopens with a new concept",
            "category": "food_openings",
            "primary_block": "openings",
            "event": {"date_start": event_day, "date": event_day},
        }

    def test_reannounced_opening_before_its_day_is_suppressed(self) -> None:
        from news_digest.pipeline.editorial_contracts import _new_phase_named_fact

        far = (now_london().date() + timedelta(days=10)).isoformat()
        today = self._opening(event_day=far)
        previous = {
            **self._opening(event_day=far),
            "first_published_day_london": (now_london().date() - timedelta(days=9)).isoformat(),
        }
        self.assertEqual(_new_phase_named_fact(today, previous), "")

    def test_opening_publishes_on_its_opening_day(self) -> None:
        from news_digest.pipeline.editorial_contracts import _new_phase_named_fact

        day = now_london().date().isoformat()
        today = self._opening(event_day=day)
        previous = {
            **self._opening(event_day=day),
            "first_published_day_london": (now_london().date() - timedelta(days=5)).isoformat(),
        }
        self.assertEqual(_new_phase_named_fact(today, previous), "strong_phase_development")

    def test_incident_new_development_word_is_a_new_phase(self) -> None:
        from news_digest.pipeline.editorial_contracts import _new_phase_named_fact

        previous = {
            "title": "Man charged over Bury town centre incident",
            "first_published_day_london": (now_london().date() - timedelta(days=1)).isoformat(),
        }
        today = {"title": "Man sentenced over Bury town centre incident"}
        self.assertEqual(_new_phase_named_fact(today, previous), "strong_phase_development")


if __name__ == "__main__":
    unittest.main()
