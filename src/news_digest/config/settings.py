from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os


def _load_dotenv(dotenv_path: Path) -> None:
    if not dotenv_path.exists():
        return

    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)

@dataclass(slots=True)
class Settings:
    telegram_bot_token: str
    telegram_target: str | None
    project_root: Path
    archive_dir: Path
    state_dir: Path


def load_settings(project_root: Path | None = None) -> Settings:
    root = (project_root or Path(__file__).resolve().parents[3]).resolve()
    _load_dotenv(root / ".env.local")

    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    target = os.getenv("TELEGRAM_TARGET", "").strip() or None

    if not token:
        raise RuntimeError(
            "TELEGRAM_BOT_TOKEN is not set. Put it into .env.local or export it in the shell."
        )

    archive_dir = root / "data" / "archive"
    state_dir = root / "data" / "state"
    archive_dir.mkdir(parents=True, exist_ok=True)
    state_dir.mkdir(parents=True, exist_ok=True)

    return Settings(
        telegram_bot_token=token,
        telegram_target=target,
        project_root=root,
        archive_dir=archive_dir,
        state_dir=state_dir,
    )
