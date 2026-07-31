"""2026-07-31 incident: round 2 sends a filtered subset, the model answers with
0-based indices, and the fix landed on the global line 0 — the lead was
overwritten by a Городской радар line and the plan sverka blocked the send.
The batch must be addressed by local indices and mapped back.
"""
import json
import unittest
from unittest import mock

import news_digest.pipeline.editor as editor


class _Message:
    def __init__(self, content: str):
        self.content = content


class _Choice:
    def __init__(self, content: str):
        self.message = _Message(content)


class _Response:
    def __init__(self, content: str):
        self.choices = [_Choice(content)]


class EditorLineIndexMappingTest(unittest.TestCase):
    def test_renumbered_answer_maps_back_to_global_indices(self):
        items = [
            {"index": 40, "section": "Городской радар", "line": "• Атака собаки в Bolton. <a href=\"https://example.test/dog\">BBC</a>"},
            {"index": 42, "section": "Городской радар", "line": "• Комплекс достроен. <a href=\"https://example.test/build\">MEN</a>"},
        ]
        payload = json.dumps(
            {
                "items": [
                    {"index": 0, "action": "rewrite", "line": "• Атака собаки в Болтоне. <a href=\"https://example.test/dog\">BBC</a>"},
                    {"index": 1, "action": "ok", "line": "• Комплекс достроен. <a href=\"https://example.test/build\">MEN</a>"},
                ]
            },
            ensure_ascii=False,
        )
        with mock.patch.object(editor, "_editor_create_with_backoff", return_value=_Response(payload)):
            fixes, report = editor._call_pre_send_russian_editor_batch(object(), items, lambda **_kw: None)

        self.assertIn(40, fixes)
        self.assertNotIn(0, fixes, "local index leaked as a global line number")
        self.assertIn("Болтоне", fixes[40])
        self.assertTrue(report["coverage_complete"])


if __name__ == "__main__":
    unittest.main()
