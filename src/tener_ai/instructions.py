from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict


DEFAULT_INSTRUCTIONS: Dict[str, Any] = {
    "version": "default",
    "agents": {},
}


class AgentInstructions:
    def __init__(self, path: str | None = None) -> None:
        self.path = path
        self.payload = self._load(path)

    def reload(self) -> None:
        self.payload = self._load(self.path)

    def get(self, stage: str, fallback: str = "") -> str:
        agents = self.payload.get("agents")
        if not isinstance(agents, dict):
            return fallback
        value = agents.get(stage)
        return value if isinstance(value, str) else fallback

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.payload.get("version", "unknown"),
            "agents": dict(self.payload.get("agents") or {}),
            "source_path": self.path,
        }

    @staticmethod
    def _load(path: str | None) -> Dict[str, Any]:
        if not path:
            return dict(DEFAULT_INSTRUCTIONS)
        file_path = Path(path)
        if not file_path.exists():
            return dict(DEFAULT_INSTRUCTIONS)
        with file_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return dict(DEFAULT_INSTRUCTIONS)
        if not isinstance(data.get("agents"), dict):
            data["agents"] = {}
        return data
