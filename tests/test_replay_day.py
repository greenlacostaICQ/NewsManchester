import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

from scripts import replay_day


class ReplaySandboxLifecycleTest(unittest.TestCase):
    def test_default_temporary_sandbox_is_removed_after_success(self):
        with tempfile.TemporaryDirectory() as temp_root:
            with mock.patch.object(replay_day.tempfile, "tempdir", temp_root):
                with replay_day.replay_sandbox("2026-07-16", None, False) as sandbox:
                    created = sandbox
                    (sandbox / "data").mkdir()
                    self.assertTrue(sandbox.exists())
                self.assertFalse(created.exists())

    def test_default_temporary_sandbox_is_removed_after_failure(self):
        with tempfile.TemporaryDirectory() as temp_root:
            with mock.patch.object(replay_day.tempfile, "tempdir", temp_root):
                with self.assertRaisesRegex(RuntimeError, "replay failed"):
                    with replay_day.replay_sandbox("2026-07-16", None, False) as sandbox:
                        created = sandbox
                        raise RuntimeError("replay failed")
                self.assertFalse(created.exists())

    def test_keep_preserves_temporary_sandbox(self):
        with tempfile.TemporaryDirectory() as temp_root:
            with mock.patch.object(replay_day.tempfile, "tempdir", temp_root):
                with replay_day.replay_sandbox("2026-07-16", None, True) as sandbox:
                    created = sandbox
                self.assertTrue(created.exists())

    def test_explicit_sandbox_is_preserved(self):
        with tempfile.TemporaryDirectory() as temp_root:
            root = Path(temp_root) / "replays"
            with replay_day.replay_sandbox("2026-07-16", root, False) as sandbox:
                created = sandbox
            self.assertEqual(created, root / "2026-07-16")
            self.assertTrue(created.exists())

    def test_stale_cleanup_removes_only_owned_default_sandboxes(self):
        with tempfile.TemporaryDirectory() as temp_root:
            root = Path(temp_root)
            owned = root / "replay_2026-07-16_abcd1234"
            kept = root / "replay_2026-07-16_keep1234"
            unrelated = root / "replay_2026-07-16_other123"
            for path in (owned, kept, unrelated):
                path.mkdir()
            (owned / replay_day.REPLAY_SANDBOX_MARKER).write_text(
                str(replay_day.PROJECT_ROOT), encoding="utf-8"
            )
            (kept / replay_day.REPLAY_SANDBOX_MARKER).write_text(
                str(replay_day.PROJECT_ROOT), encoding="utf-8"
            )
            (unrelated / replay_day.REPLAY_SANDBOX_MARKER).write_text(
                "/different/project", encoding="utf-8"
            )
            old = time.time() - replay_day.STALE_REPLAY_SECONDS - 60
            for target in (owned, owned / replay_day.REPLAY_SANDBOX_MARKER, unrelated, unrelated / replay_day.REPLAY_SANDBOX_MARKER):
                os.utime(target, (old, old))
            with mock.patch.object(replay_day.tempfile, "tempdir", temp_root):
                removed = replay_day.cleanup_stale_replay_sandboxes()
            self.assertEqual(removed, 1)
            self.assertFalse(owned.exists())
            self.assertTrue(kept.exists())
            self.assertTrue(unrelated.exists())


if __name__ == "__main__":
    unittest.main()
