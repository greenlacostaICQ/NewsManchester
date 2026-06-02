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
    def __init__(self, state_dir: Path, archive_dir: Path) -> None:
        self.state_dir = state_dir
        self.archive_dir = archive_dir
        self.run_state_path = state_dir / "run_state.json"
        self.bot_state_path = state_dir / "bot_state.json"
        self.delivery_state_path = state_dir / "delivery_state.json"
        self.feedback_path = state_dir / "personalization_feedback.json"

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

    def _save_bot_state(self, payload: dict) -> None:
        subscribers = payload.get("subscribers", [])
        payload["subscribers"] = sorted({str(chat_id) for chat_id in subscribers})
        _write_json(self.bot_state_path, payload)

    def get_last_update_id(self) -> int | None:
        payload = self._load_bot_state()
        value = payload.get("last_update_id")
        if value is None:
            return None
        return int(value)

    def set_last_update_id(self, update_id: int) -> None:
        payload = self._load_bot_state()
        payload["last_update_id"] = int(update_id)
        self._save_bot_state(payload)

    def list_subscribers(self) -> list[str]:
        payload = self._load_bot_state()
        return list(payload["subscribers"])

    def add_subscriber(self, chat_id: str) -> bool:
        payload = self._load_bot_state()
        chat_id = str(chat_id)
        if chat_id in payload["subscribers"]:
            return False
        payload["subscribers"].append(chat_id)
        self._save_bot_state(payload)
        return True

    def remove_subscriber(self, chat_id: str) -> bool:
        payload = self._load_bot_state()
        chat_id = str(chat_id)
        if chat_id not in payload["subscribers"]:
            return False
        payload["subscribers"] = [item for item in payload["subscribers"] if item != chat_id]
        self._save_bot_state(payload)
        return True

    def record_item_feedback(self, *, fingerprint: str, reaction: str, chat_id: str) -> bool:
        fingerprint = str(fingerprint or "").strip()
        reaction = str(reaction or "").strip()
        if not fingerprint or reaction not in {"useful", "not_useful"}:
            return False
        payload = _read_json(self.feedback_path, {"schema_version": 1, "items": []})
        items = payload.get("items")
        if not isinstance(items, list):
            items = []
        now = datetime.now(LONDON_TZ).isoformat()
        matched = False
        for item in items:
            if not isinstance(item, dict):
                continue
            if str(item.get("fingerprint") or "") != fingerprint:
                continue
            item["reaction"] = reaction
            item["reaction_source"] = f"telegram:{chat_id}"
            item["reaction_at_london"] = now
            matched = True
            break
        if not matched:
            items.append(
                {
                    "date": datetime.now(LONDON_TZ).strftime("%Y-%m-%d"),
                    "fingerprint": fingerprint,
                    "reaction": reaction,
                    "reaction_source": f"telegram:{chat_id}",
                    "reaction_at_london": now,
                    "title": "",
                    "source_label": "",
                    "category": "",
                    "primary_block": "",
                    "scoring_trace": {},
                }
            )
        payload["schema_version"] = 1
        payload["items"] = items[-1000:]
        _write_json(self.feedback_path, payload)
        return True

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

    def mark_demo_run(self, issue_text: str) -> Path:
        now = datetime.now(LONDON_TZ)
        payload = _read_json(self.run_state_path, {"runs": []})
        payload["runs"].append(
            {
                "timestamp": now.isoformat(),
                "type": "demo_send",
            }
        )
        _write_json(self.run_state_path, payload)

        archive_path = self.archive_dir / f"{now.strftime('%Y-%m-%d')}-demo.md"
        archive_path.write_text(issue_text, encoding="utf-8")
        return archive_path
