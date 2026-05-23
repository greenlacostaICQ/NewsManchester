"""S6 tests: post-publish judge storage + drift detection.

LLM calls are NOT exercised here — the judge module is structured so
that `evaluate_today` returns gracefully on a missing API key, and
the pure-function tests below cover the parsing + history + drift
math that runs every day regardless.
"""
from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

from news_digest.pipeline.post_publish_judge import (
    AXES,
    EvalRow,
    JUDGE_PROMPT_VERSION,
    _parse_judge_reply,
    append_eval,
    build_candidate_summary,
    detect_drift,
    evaluate_today,
    load_history,
)


def _eval_row(date: str, **scores: float) -> EvalRow:
    """Make an EvalRow with default 4.0 on any axis the caller didn't set."""
    return EvalRow(
        date=date,
        factuality=float(scores.get("factuality", 4.0)),
        novelty=float(scores.get("novelty", 4.0)),
        source_diversity=float(scores.get("source_diversity", 4.0)),
        signal_density=float(scores.get("signal_density", 4.0)),
        coherence=float(scores.get("coherence", 4.0)),
        notes=str(scores.get("notes", "")),
    )


class JudgeReplyParsingTest(unittest.TestCase):
    def test_parse_bare_json(self) -> None:
        raw = '{"factuality": 4.5, "novelty": 3.0, "source_diversity": 4.0, "signal_density": 4.2, "coherence": 4.1, "notes": "повторы"}'
        out = _parse_judge_reply(raw)
        self.assertIsNotNone(out)
        self.assertEqual(out["novelty"], 3.0)
        self.assertEqual(out["notes"], "повторы")

    def test_parse_strips_markdown_fence(self) -> None:
        raw = "```json\n{\"factuality\": 4.0, \"novelty\": 4.0, \"source_diversity\": 4.0, \"signal_density\": 4.0, \"coherence\": 4.0, \"notes\": \"ok\"}\n```"
        out = _parse_judge_reply(raw)
        self.assertIsNotNone(out)
        self.assertEqual(out["factuality"], 4.0)

    def test_parse_handles_surrounding_prose(self) -> None:
        raw = "Here's the eval: {\"factuality\": 3.5, \"novelty\": 3.0, \"source_diversity\": 3.5, \"signal_density\": 3.5, \"coherence\": 3.5, \"notes\": \"x\"} — done."
        out = _parse_judge_reply(raw)
        self.assertIsNotNone(out)
        self.assertEqual(out["factuality"], 3.5)

    def test_parse_returns_none_on_garbage(self) -> None:
        self.assertIsNone(_parse_judge_reply("not json at all"))
        self.assertIsNone(_parse_judge_reply(""))
        self.assertIsNone(_parse_judge_reply("[1,2,3]"))


class AppendAndLoadTest(unittest.TestCase):
    def test_append_writes_jsonl(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            row = _eval_row("2026-05-20")
            path = append_eval(state_dir, row)
            self.assertTrue(path.exists())
            lines = path.read_text(encoding="utf-8").strip().splitlines()
            self.assertEqual(len(lines), 1)
            self.assertEqual(json.loads(lines[0])["date"], "2026-05-20")

    def test_append_is_idempotent_on_same_date(self) -> None:
        """Re-running on the same day overwrites today's row, not
        appends a duplicate. Important for re-sends that re-run the
        judge.
        """
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            append_eval(state_dir, _eval_row("2026-05-20", factuality=3.0))
            append_eval(state_dir, _eval_row("2026-05-20", factuality=4.7))
            path = state_dir / "digest_evals.jsonl"
            rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["factuality"], 4.7)

    def test_append_keeps_history(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            append_eval(state_dir, _eval_row("2026-05-18"))
            append_eval(state_dir, _eval_row("2026-05-19"))
            append_eval(state_dir, _eval_row("2026-05-20"))
            path = state_dir / "digest_evals.jsonl"
            rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
            self.assertEqual([r["date"] for r in rows], ["2026-05-18", "2026-05-19", "2026-05-20"])

    def test_load_history_filters_to_window(self) -> None:
        """Rows older than `since_days` (relative to today_london) drop
        out of load_history. We can't easily fake today_london in this
        test, so we just write a row dated "tomorrow + N years" to
        guarantee it's in-window, and an obviously-old 2020 row that
        must be excluded.
        """
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            append_eval(state_dir, _eval_row("2020-01-01"))
            append_eval(state_dir, _eval_row("2099-01-01"))
            rows = load_history(state_dir, since_days=30)
            dates = [r["date"] for r in rows]
            self.assertIn("2099-01-01", dates)
            self.assertNotIn("2020-01-01", dates)


class DetectDriftTest(unittest.TestCase):
    def test_no_data_returns_no_data_status(self) -> None:
        result = detect_drift([])
        self.assertEqual(result["status"], "no_data")
        self.assertEqual(result["signals"], [])

    def test_warming_up_when_baseline_too_short(self) -> None:
        """Three days isn't enough — drift detection holds off so we
        don't fire signals on noise.
        """
        history = [
            {"date": "2026-05-18", **dict.fromkeys(AXES, 4.0), "notes": ""},
            {"date": "2026-05-19", **dict.fromkeys(AXES, 4.0), "notes": ""},
            {"date": "2026-05-20", **dict.fromkeys(AXES, 2.0), "notes": ""},
        ]
        result = detect_drift(history)
        self.assertEqual(result["status"], "warming_up")
        # No signals even though today dropped — baseline too small.
        self.assertEqual(result["signals"], [])
        # But per-axis numbers are still computed.
        self.assertIn("novelty", result["axes"])

    def test_drop_below_one_sigma_flags_axis(self) -> None:
        """30 days of stable factuality at 4.0, then today drops to 2.0
        — must flag factuality.
        """
        history = [
            {"date": f"2026-05-{i:02d}",
             "factuality": 4.0 + (0.05 if i % 2 == 0 else -0.05),
             "novelty": 4.0,
             "source_diversity": 4.0,
             "signal_density": 4.0,
             "coherence": 4.0,
             "notes": ""}
            for i in range(1, 21)  # 20 days of baseline
        ]
        # Today: factuality crashes.
        history.append({
            "date": "2026-05-21",
            "factuality": 2.0,
            "novelty": 4.0,
            "source_diversity": 4.0,
            "signal_density": 4.0,
            "coherence": 4.0,
            "notes": "что-то рухнуло",
        })
        result = detect_drift(history)
        self.assertEqual(result["status"], "ok")
        signal_axes = {s["axis"] for s in result["signals"]}
        self.assertIn("factuality", signal_axes)
        # Other axes (constant) must not be flagged.
        self.assertNotIn("novelty", signal_axes)

    def test_above_baseline_axis_not_flagged(self) -> None:
        """Defensive: today UP from baseline must never be a signal."""
        history = [
            {"date": f"2026-05-{i:02d}",
             "factuality": 3.5,
             "novelty": 4.0,
             "source_diversity": 4.0,
             "signal_density": 4.0,
             "coherence": 4.0,
             "notes": ""}
            for i in range(1, 21)
        ]
        history.append({
            "date": "2026-05-21",
            "factuality": 4.9,
            "novelty": 4.0,
            "source_diversity": 4.0,
            "signal_density": 4.0,
            "coherence": 4.0,
            "notes": "",
        })
        result = detect_drift(history)
        self.assertEqual(result["signals"], [])

    def test_zero_variance_baseline_does_not_signal(self) -> None:
        """If baseline stddev is exactly 0 (impossible but defensive),
        any movement could otherwise look like infinite sigma. Must
        return zero signals.
        """
        history = [
            {"date": f"2026-05-{i:02d}",
             "factuality": 4.0,
             "novelty": 4.0,
             "source_diversity": 4.0,
             "signal_density": 4.0,
             "coherence": 4.0,
             "notes": ""}
            for i in range(1, 21)
        ]
        history.append({
            "date": "2026-05-21",
            "factuality": 2.0,
            "novelty": 4.0,
            "source_diversity": 4.0,
            "signal_density": 4.0,
            "coherence": 4.0,
            "notes": "",
        })
        result = detect_drift(history)
        # baseline_sigma = 0 → no signals even though delta is huge.
        # (This is the documented behaviour; production data always
        # has some noise so this guard is mostly insurance.)
        self.assertEqual(result["signals"], [])


class EvaluateTodayTest(unittest.TestCase):
    def test_skips_when_digest_missing(self) -> None:
        """Defensive: never raise on missing files; just report skipped."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "data" / "state").mkdir(parents=True)
            result = evaluate_today(root)
            self.assertEqual(result["status"], "skipped")
            self.assertIn("current_digest.html", str(result.get("reason") or ""))


class CandidateSummaryTest(unittest.TestCase):
    def test_filters_to_included_and_caps(self) -> None:
        cands = [
            {"include": True, "title": "A", "source_label": "BBC", "category": "media_layer", "primary_block": "city_watch"},
            {"include": False, "title": "B"},
            {"include": True, "title": "C", "source_label": "MEN", "category": "media_layer", "primary_block": "city_watch"},
        ]
        out = build_candidate_summary(cands)
        self.assertEqual(len(out), 2)
        self.assertEqual(out[0]["title"], "A")
        self.assertEqual(out[1]["source_label"], "MEN")


class PromptRegistryTest(unittest.TestCase):
    def test_judge_prompt_is_registered(self) -> None:
        from news_digest.pipeline.prompts_meta import by_name
        meta = by_name().get("post_publish_judge")
        self.assertIsNotNone(meta)
        self.assertEqual(meta.version, JUDGE_PROMPT_VERSION)


if __name__ == "__main__":
    unittest.main()
