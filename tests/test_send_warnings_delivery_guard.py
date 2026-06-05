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
    def test_admin_report_is_not_sent_when_today_digest_was_not_delivered(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state = root / "data" / "state"
            state.mkdir(parents=True)
            (state / "release_report.json").write_text(
                json.dumps(
                    {
                        "run_date_london": today_london(),
                        "release_decision": "pass",
                        "warnings": ["Quality warning that would normally trigger support report."],
                    }
                ),
                encoding="utf-8",
            )
            (state / "delivery_state.json").write_text(
                json.dumps({"last_delivery_day_london": "2026-06-04", "status": "delivered"}),
                encoding="utf-8",
            )

            out = io.StringIO()
            with (
                mock.patch.object(cli, "PROJECT_ROOT", root),
                mock.patch.dict("os.environ", {"WARNINGS_TO_TELEGRAM": "1"}, clear=False),
                mock.patch.object(cli, "_load_store_and_client") as load_client,
                contextlib.redirect_stdout(out),
            ):
                self.assertEqual(cli.cmd_send_warnings(), 0)

            load_client.assert_not_called()
            self.assertIn("today's digest was not delivered", out.getvalue())


if __name__ == "__main__":
    unittest.main()
