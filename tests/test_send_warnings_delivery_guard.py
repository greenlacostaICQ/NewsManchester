import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import scripts.run_local_digest as cli
from news_digest.pipeline.common import today_london


class SendWarningsDeliveryGuardTests(unittest.TestCase):
    """Owner contract 2026-07-12: delivered issue → Telegram silent (report
    to stdout only); NOT delivered issue → short ⛔ alert with the gate error.
    Before, the logic was inverted (report on success, silence on failure —
    the 2026-07-11 blocked release went unnoticed)."""

    def _state(self, root: Path, *, decision: str, delivered_day: str, errors: list[str] | None = None) -> None:
        state = root / "data" / "state"
        state.mkdir(parents=True)
        (state / "release_report.json").write_text(
            json.dumps(
                {
                    "run_date_london": today_london(),
                    "release_decision": decision,
                    "errors": errors or [],
                    "warnings": ["Quality warning that would normally trigger support report."],
                }
            ),
            encoding="utf-8",
        )
        (state / "delivery_state.json").write_text(
            json.dumps({"last_delivery_day_london": delivered_day, "status": "delivered"}),
            encoding="utf-8",
        )

    def test_delivered_issue_sends_nothing_to_telegram(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._state(root, decision="pass", delivered_day=today_london())
            out = io.StringIO()
            with (
                mock.patch.object(cli, "PROJECT_ROOT", root),
                mock.patch.dict("os.environ", {"WARNINGS_TO_TELEGRAM": "1"}, clear=False),
                mock.patch.object(cli, "_load_store_and_client") as load_client,
                contextlib.redirect_stdout(out),
            ):
                self.assertEqual(cli.cmd_send_warnings(), 0)

            load_client.assert_not_called()
            self.assertIn("Telegram stays silent", out.getvalue())

    def test_blocked_issue_sends_short_alert_with_gate_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            # 2026-07-11 real case: gate fail, delivery_state stuck on the day before.
            self._state(
                root,
                decision="fail",
                delivered_day="2026-07-11",
                errors=["Rendered HTML violates public contract: ticket_radar_over_cap (17 > 15)."],
            )
            client = mock.Mock()
            settings = mock.Mock(telegram_target="42")
            store = mock.Mock(list_subscribers=lambda: [])
            out = io.StringIO()
            with (
                mock.patch.object(cli, "PROJECT_ROOT", root),
                mock.patch.dict("os.environ", {"WARNINGS_TO_TELEGRAM": "1"}, clear=False),
                mock.patch.object(cli, "_load_store_and_client", return_value=(settings, client, store)),
                contextlib.redirect_stdout(out),
            ):
                self.assertEqual(cli.cmd_send_warnings(), 0)

            client.send_text_in_chunks.assert_called_once()
            alert = client.send_text_in_chunks.call_args.args[1]
            self.assertIn("НЕ дошёл до читателя", alert)
            self.assertIn("ticket_radar_over_cap", alert)
            self.assertLess(alert.count("\n"), 5)


if __name__ == "__main__":
    unittest.main()
