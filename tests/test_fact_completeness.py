from __future__ import annotations

import unittest

from news_digest.pipeline.fact_completeness import (
    line_satisfies_concept,
    translation_completeness_review,
)
from news_digest.pipeline.pre_send_quality_judge import (
    _deterministic_completeness_scan,
    _recount_completeness_recovery,
    _rendered_candidates_by_url,
)


class TranslationCompletenessTests(unittest.TestCase):
    # The Tinder-class regression: the English source carries a grave severity
    # concept ("rape fantasy"), the shipped Russian line neutered it to a
    # generic court line, and fact_lock (Russian→evidence only) stayed silent.
    SRC = "Tinder date admitted rape fantasy, Manchester court hears. Man charged."
    NEUTERED = "• На суде в Манчестере рассматривается дело мужчины после свидания в Tinder."
    FAITHFUL = "• Мужчину обвиняют: он признался в фантазиях об изнасиловании (свидание в Tinder)."

    def test_neutered_line_flags_severity_omission(self) -> None:
        review = translation_completeness_review(self.SRC, self.NEUTERED)
        self.assertTrue(review["applies"])
        self.assertEqual(
            [m["concept"] for m in review["missing_critical"]], ["sexual_offence"]
        )
        self.assertIn("charge/verdict/victim", review["obligations"])

    def test_faithful_line_has_no_critical_omission(self) -> None:
        review = translation_completeness_review(self.SRC, self.FAITHFUL)
        self.assertTrue(review["applies"])
        self.assertEqual(review["missing_critical"], [])
        self.assertTrue(line_satisfies_concept("sexual_offence", self.FAITHFUL))

    def test_benign_event_line_does_not_apply(self) -> None:
        review = translation_completeness_review(
            "Jazz festival at Albert Hall, tickets £12",
            "• Джазовый фестиваль в Albert Hall, билеты £12.",
        )
        self.assertFalse(review["applies"])

    def test_dropped_scalar_is_warning_only(self) -> None:
        review = translation_completeness_review(
            "Man jailed for 12 years on 3 March", "• Мужчину отправили в тюрьму на срок."
        )
        # Sentence + weapon-free custody term is present in RU, so no critical
        # omission — but the dropped number/date facts surface as warnings.
        self.assertEqual(review["missing_critical"], [])
        self.assertIn("12", review["missing_noncritical"])

    def test_scan_emits_repair_worthy_error_for_neutered_line(self) -> None:
        url = "https://example.com/crime/tinder-case"
        candidate = {
            "source_url": url,
            "title": self.SRC,
            "compact_facts": "evidence_text=Man admitted a rape fantasy during the trial.",
        }
        slots = [
            {
                "line_index": 3,
                "section": "Свежие новости",
                "html": f'{self.NEUTERED} <a href="{url}">MEN</a>',
                "text": self.NEUTERED,
            }
        ]
        scan = _deterministic_completeness_scan(slots, _rendered_candidates_by_url([candidate]))
        self.assertEqual(scan["critical_omission_count"], 1)
        self.assertEqual(len(scan["critical_errors"]), 1)
        err = scan["critical_errors"][0]
        self.assertEqual(err["line_index"], 3)
        self.assertEqual(err["risk"], "translation")
        self.assertEqual(err["suggested_action"], "repair")

    def test_recount_distinguishes_recovered_from_pulled_line(self) -> None:
        url = "https://example.com/crime/tinder-case"
        completeness = {
            "critical_omissions": [
                {
                    "line_index": 1,
                    "source_url_key": "example.com/crime/tinder-case",
                    "concept": "sexual_offence",
                },
                {
                    "line_index": 2,
                    "source_url_key": "example.com/crime/removed-case",
                    "concept": "sexual_offence",
                },
            ],
            "recovered": 0,
            "pulled_for_rework": 0,
            "still_missing": 2,
        }
        html = (
            "<b>Свежие новости</b>\n"
            f'{self.FAITHFUL} <a href="{url}">MEN</a>\n'
            '• Reserve item from another source. <a href="https://example.com/city/other">MEN</a>\n'
        )

        _recount_completeness_recovery(completeness, html)

        self.assertEqual(completeness["recovered"], 1)
        self.assertEqual(completeness["pulled_for_rework"], 1)
        self.assertEqual(completeness["still_missing"], 0)


if __name__ == "__main__":
    unittest.main()
