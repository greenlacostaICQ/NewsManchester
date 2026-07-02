from __future__ import annotations

import json
from types import SimpleNamespace
import sys
import types
import unittest
from unittest.mock import patch

from news_digest.pipeline.professional_events import (
    apply_professional_event_match,
    score_professional_event,
    _professional_event_has_minimum_facts,
)
from news_digest.pipeline.writer import _build_professional_event_fallback_line


class ProfessionalEventsTest(unittest.TestCase):
    def _candidate(self, title: str, summary: str, price: str = "free", venue: str = "Manchester Central") -> dict:
        return {
            "title": title,
            "summary": summary,
            "lead": "",
            "evidence_text": summary,
            "source_label": "Manchester Digital Events",
            "source_url": "https://www.manchesterdigital.com/events/example",
            "category": "professional_events",
            "primary_block": "professional_events",
            "event": {
                "is_event": True,
                "event_name": title,
                "venue": venue,
                "date": "2026-07-02",
                "date_start": "2026-07-02T09:30:00+01:00",
                "price": price,
                "booking_url": "https://www.manchesterdigital.com/events/example",
            },
        }

    def test_major_free_expo_is_published_as_major(self) -> None:
        c = self._candidate(
            "DTX Manchester AI and Digital Transformation Expo",
            "Free delegate pass for business leaders. Conference with AI, data, cloud, product and enterprise technology tracks.",
        )
        match = score_professional_event(c)
        self.assertTrue(match["publish"])
        self.assertEqual(match["event_level"], "major_conference_or_expo")
        self.assertTrue(match["major_conference_or_expo"])
        self.assertEqual(match["recommended_action"], "register")

    def test_basic_free_networking_can_pass_for_english_practice(self) -> None:
        c = self._candidate(
            "Manchester startup networking breakfast",
            "Free local business networking workshop for founders and product people in Manchester.",
            venue="Bonded Warehouse",
        )
        match = score_professional_event(c)
        self.assertTrue(match["publish"])
        self.assertEqual(match["event_level"], "english_practice_networking")
        self.assertTrue(match["english_practice_value"])

    def test_paid_event_without_free_path_is_rejected(self) -> None:
        c = self._candidate(
            "Fintech leadership dinner",
            "Tickets from £95 for a private dinner and vendor demo.",
            price="£95",
        )
        match = score_professional_event(c)
        self.assertFalse(match["publish"])
        self.assertEqual(match["recommended_action"], "skip")

    def test_paid_event_waits_for_cv_model_instead_of_deterministic_drop(self) -> None:
        c = self._candidate(
            "Fintech leadership dinner",
            "Tickets from £95. Roundtable for fintech product and banking leaders.",
            price="£95",
        )
        c["include"] = True

        apply_professional_event_match(c)

        self.assertTrue(c["include"])
        self.assertEqual(c["professional_match_status"], "needs_llm_cv_match")
        self.assertEqual(c["score_source"], "keyword")
        self.assertNotIn("english_editorial_score", c)

    def test_free_low_signal_event_waits_for_llm_cv_match_before_drop(self) -> None:
        c = self._candidate(
            "Generic student careers coffee morning",
            "Free student-only careers coffee morning with broad employer stalls.",
            venue="University building",
        )
        c["include"] = True

        apply_professional_event_match(c)

        self.assertTrue(c["include"])
        self.assertEqual(c["professional_match_status"], "needs_llm_cv_match")
        self.assertIn("professional_llm_cv_match_required", c["quality_warnings"])

    def test_keyword_score_does_not_self_publish_ungoverned_event_is_held(self) -> None:
        # W6: a strong-keyword professional event with no governing CV verdict
        # (no trustworthy date → never sent to the model, and no API key in CI)
        # is held for enrichment, not published off the keyword score alone.
        from news_digest.pipeline.professional_events import apply_professional_event_llm_matches

        c = self._candidate(
            "Manchester AI and product leadership conference",
            "Free delegate pass. AI, data, product and digital transformation tracks for business leaders.",
        )
        c["event"]["date"] = ""
        c["event"]["date_start"] = ""
        c["include"] = True

        apply_professional_event_match(c)
        self.assertEqual(c["professional_match_status"], "needs_llm_cv_match")

        report = apply_professional_event_llm_matches([c])
        self.assertFalse(c["include"])
        self.assertEqual(c["editorial_status"], "held_for_enrichment")
        self.assertIn("held 1", report["summary"])

    def test_eligible_event_unevaluated_by_model_is_held_not_dropped(self) -> None:
        # W6: a professional event WITH minimum facts is sent to the model, but
        # when no route can rule on it (cap / unavailable / no API key) it must be
        # held for enrichment — recoverable next run — not silently dropped, and
        # the held count must include it. The sibling test above covers the
        # no-facts candidate the post-model sweep holds; this covers the pending
        # path (_drop_pending_llm_candidates), which previously dropped without a
        # held status and was never counted in "held N".
        from unittest.mock import patch
        from news_digest.pipeline.professional_events import apply_professional_event_llm_matches

        c = self._candidate(
            "GM Chamber digital leadership briefing",
            "Free briefing on AI, data and digital transformation for business leaders.",
        )
        c["include"] = True
        apply_professional_event_match(c)
        self.assertEqual(c["professional_match_status"], "needs_llm_cv_match")
        self.assertTrue(_professional_event_has_minimum_facts(c))

        with patch("news_digest.pipeline.model_routing.resolve_model_route", return_value=[]):
            report = apply_professional_event_llm_matches([c])

        self.assertFalse(c["include"])
        self.assertEqual(c["editorial_status"], "held_for_enrichment")
        self.assertIn("held 1", report["summary"])

    def test_all_eligible_events_are_sent_without_default_cv_cap(self) -> None:
        from news_digest.pipeline.professional_events import apply_professional_event_llm_matches

        candidates = []
        for index in range(20):
            c = self._candidate(
                f"Manchester AI product briefing {index}",
                "Free briefing on AI, data and digital transformation for business leaders.",
            )
            c["include"] = True
            apply_professional_event_match(c)
            candidates.append(c)

        report = apply_professional_event_llm_matches(candidates)

        self.assertEqual(report["eligible"], 20)
        self.assertEqual(report["sent"], 20)
        self.assertEqual(report["not_sent"], 0)

    def test_llm_go_paid_strong_fit_can_publish_with_access_label(self) -> None:
        from news_digest.pipeline.professional_events import apply_professional_event_llm_matches

        c = self._candidate(
            "Fintech AI leadership roundtable",
            "Tickets from £95. Roundtable for fintech, banking, AI and product leaders.",
            price="£95",
            venue="Manchester Hall",
        )
        c["include"] = True
        apply_professional_event_match(c)
        rows = [{
            "id": c["source_url"],
            "fit": "go",
            "score": 88,
            "access_label": "paid",
            "reason": "Strong fintech and product-leadership fit.",
            "action": "register",
        }]

        with _fake_openai(rows), patch(
            "news_digest.pipeline.model_routing.resolve_model_route",
            return_value=[_fake_route()],
        ):
            report = apply_professional_event_llm_matches([c])

        self.assertTrue(c["include"])
        self.assertEqual(report["applied"], 1)
        self.assertEqual(c["professional_event_match"]["access_label"], "paid")
        self.assertEqual(c["score_source"], "model")
        self.assertEqual(c["score_verdict"], "go")
        self.assertNotIn("english_editorial_score", c)

    def test_llm_unknown_access_needs_strong_fit_and_full_place(self) -> None:
        from news_digest.pipeline.professional_events import apply_professional_event_llm_matches

        c = self._candidate(
            "Manchester product networking",
            "Networking for product and digital leaders; price not stated.",
            price="",
            venue="Manchester Hall",
        )
        c["include"] = True
        apply_professional_event_match(c)
        rows = [{
            "id": c["source_url"],
            "fit": "consider",
            "score": 70,
            "access_label": "unknown",
            "reason": "Some profile fit, but access is unclear.",
            "action": "consider",
        }]

        with _fake_openai(rows), patch(
            "news_digest.pipeline.model_routing.resolve_model_route",
            return_value=[_fake_route()],
        ):
            report = apply_professional_event_llm_matches([c])

        self.assertFalse(c["include"])
        self.assertEqual(c["editorial_status"], "held_for_enrichment")
        self.assertEqual(report["skipped"], 1)
        self.assertIn("unknown access needs CV go or strong consider", c["reason"])

    def test_writer_builds_self_contained_russian_card(self) -> None:
        c = self._candidate(
            "CreaTech Connect: Accelerating University-Industry Partnerships",
            "Free general admission. University-industry partnerships, innovation and business networking at SISTER.",
            venue="Renold Building (SISTER)",
        )
        c["professional_event_match"] = score_professional_event(c)
        line = _build_professional_event_fallback_line(c)
        self.assertIn("CreaTech Connect", line)
        self.assertIn("2 июля", line)
        self.assertIn("Уровень:", line)
        self.assertIn("бесплат", line.lower())
        self.assertIn("Почему тебе:", line)
        self.assertIn("Действие:", line)

    def test_professional_ranking_orders_go_above_consider_and_keyword(self) -> None:
        from news_digest.pipeline.llm_rewrite import _rewrite_shortlist_priority

        keyword_only = self._candidate(
            "AI keyword conference",
            "Free AI product data conference.",
        )
        keyword_only["include"] = True
        apply_professional_event_match(keyword_only)
        go = self._candidate("Weak-title useful roundtable", "Useful roundtable.")
        go.update({
            "professional_llm_match": {"fit": "go", "score": 60},
            "score_value": 60,
            "score_source": "model",
            "score_scope": "professional",
            "score_verdict": "go",
        })
        consider = self._candidate("Strong consider expo", "AI fintech product expo.")
        consider.update({
            "professional_llm_match": {"fit": "consider", "score": 99},
            "score_value": 99,
            "score_source": "model",
            "score_scope": "professional",
            "score_verdict": "consider",
        })

        self.assertGreater(_rewrite_shortlist_priority(go), _rewrite_shortlist_priority(consider))
        self.assertGreater(_rewrite_shortlist_priority(consider), _rewrite_shortlist_priority(keyword_only))


class ProfessionalMinimumFactsTest(unittest.TestCase):
    """W1 / RC3: the eligible=1/42 bottleneck was the gate requiring a parsed
    venue string. A dated GM event with a date + booking URL + GM source is
    eligible even without a venue token; a low-confidence far-future date is
    not."""

    def _prof(self, **event) -> dict:
        ev = {"event_name": "X", "date": "", "date_confidence": "none",
              "venue": "", "booking_url": ""}
        ev.update(event)
        return {
            "category": "professional_events",
            "primary_block": "professional_events",
            "title": "X",
            "source_label": "GM Chamber",
            "source_url": "https://www.gmchamber.co.uk/events/example",
            "event": ev,
        }

    def test_dated_gm_event_without_parsed_venue_is_eligible(self) -> None:
        c = self._prof(date="2026-07-03", date_confidence="medium", venue="")
        self.assertTrue(_professional_event_has_minimum_facts(c))

    def test_low_confidence_far_future_date_is_not_eligible(self) -> None:
        c = self._prof(date="2027-05-02", date_confidence="low", venue="Somewhere")
        self.assertFalse(_professional_event_has_minimum_facts(c))

    def test_no_date_is_not_eligible(self) -> None:
        self.assertFalse(_professional_event_has_minimum_facts(self._prof()))

    def test_programme_page_is_not_eligible(self) -> None:
        c = self._prof(date="2026-07-03", date_confidence="medium", booking_url="https://example.com/events")
        self.assertFalse(_professional_event_has_minimum_facts(c))


def _fake_route() -> SimpleNamespace:
    return SimpleNamespace(
        api_key="test-key",
        base_url="",
        timeout_seconds=1,
        provider="openai",
        provider_label="OpenAI",
        model="fake-model",
        role="professional_cv_match",
    )


class _FakeMessage:
    def __init__(self, content: str) -> None:
        self.content = content


class _FakeChoice:
    def __init__(self, content: str) -> None:
        self.message = _FakeMessage(content)


class _FakeResponse:
    def __init__(self, rows: list[dict]) -> None:
        self.choices = [_FakeChoice(json.dumps({"items": rows}))]


def _fake_openai(rows: list[dict]):
    module = types.ModuleType("openai")

    class _Completions:
        def create(self, **kwargs):
            return _FakeResponse(rows)

    class _Chat:
        def __init__(self) -> None:
            self.completions = _Completions()

    class _OpenAI:
        def __init__(self, **kwargs) -> None:
            self.chat = _Chat()

    module.OpenAI = _OpenAI
    return patch.dict(sys.modules, {"openai": module})


if __name__ == "__main__":
    unittest.main()
