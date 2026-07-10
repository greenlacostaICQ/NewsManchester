from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
from zoneinfo import ZoneInfo


LONDON_TZ = ZoneInfo("Europe/London")


def _read_json(path: Path, default: dict) -> dict:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


class StateStore:
    def __init__(self, state_dir: Path) -> None:
        self.state_dir = state_dir
        self.bot_state_path = state_dir / "bot_state.json"
        self.delivery_state_path = state_dir / "delivery_state.json"

    def _load_bot_state(self) -> dict:
        payload = _read_json(
            self.bot_state_path,
            {"last_update_id": None, "subscribers": []},
        )
        subscribers = payload.get("subscribers", [])
        if not isinstance(subscribers, list):
            subscribers = []
        payload["subscribers"] = [str(chat_id) for chat_id in subscribers]
        return payload

    def list_subscribers(self) -> list[str]:
        payload = self._load_bot_state()
        return list(payload["subscribers"])

    def get_last_delivery(self) -> dict:
        return _read_json(
            self.delivery_state_path,
            {"last_delivery_at": None, "last_delivery_day_london": None, "targets": [], "source_path": None},
        )

    def mark_delivery(
        self,
        targets: list[str],
        source_path: str,
        *,
        message_ids: list[int] | None = None,
        status: str = "delivered",
    ) -> None:
        now = datetime.now(LONDON_TZ)
        payload = {
            "last_delivery_at": now.isoformat(),
            "last_delivery_day_london": now.strftime("%Y-%m-%d"),
            "targets": sorted({str(target) for target in targets}),
            "source_path": source_path,
            "status": status,
            "message_ids": [int(m) for m in (message_ids or [])],
            "chunk_count": len(message_ids or []),
        }
        _write_json(self.delivery_state_path, payload)
