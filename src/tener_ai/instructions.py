from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict


DEFAULT_INSTRUCTIONS: Dict[str, Any] = {
    "version": "default",
    "agents": {},
}

DEFAULT_EVALUATION_PLAYBOOK: Dict[str, Any] = {
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
        raw_agents = data.get("agents")
        if not isinstance(raw_agents, dict):
            data["agents"] = {}
            return data

        base_dir = file_path.parent
        normalized: Dict[str, str] = {}
        for key, value in raw_agents.items():
            if not isinstance(key, str):
                continue
            normalized[key] = AgentInstructions._resolve_agent_instruction(value=value, base_dir=base_dir)
        data["agents"] = normalized
        return data

    @staticmethod
    def _resolve_agent_instruction(value: Any, base_dir: Path) -> str:
        if isinstance(value, str):
            return value
        if not isinstance(value, dict):
            return ""

        inline = value.get("text")
        if isinstance(inline, str):
            return inline

        file_ref = value.get("file")
        if not isinstance(file_ref, str) or not file_ref.strip():
            return ""

        target = Path(file_ref)
        if not target.is_absolute():
            target = base_dir / target
        if not target.exists():
            return ""
        try:
            return target.read_text(encoding="utf-8")
        except OSError:
            return ""


class AgentEvaluationPlaybook:
    def __init__(self, path: str | None = None) -> None:
        self.path = path
        self.payload = self._load(path)

    def reload(self) -> None:
        self.payload = self._load(self.path)

    def get_agent_name(self, agent_key: str, fallback: str = "") -> str:
        agents = self.payload.get("agents")
        if not isinstance(agents, dict):
            return fallback
        entry = agents.get(agent_key)
        if not isinstance(entry, dict):
            return fallback
        name = entry.get("name")
        return name if isinstance(name, str) else fallback

    def get_instruction(self, agent_key: str, stage_key: str | None = None, fallback: str = "") -> str:
        agents = self.payload.get("agents")
        if not isinstance(agents, dict):
            return fallback
        entry = agents.get(agent_key)
        if not isinstance(entry, dict):
            return fallback
        stages = entry.get("stages")
        if not isinstance(stages, dict):
            return fallback
        stage_value = stages.get(stage_key) if stage_key else None
        if isinstance(stage_value, str):
            return stage_value
        wildcard = stages.get("*")
        if isinstance(wildcard, str):
            return wildcard
        return fallback

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.payload.get("version", "unknown"),
            "agents": dict(self.payload.get("agents") or {}),
            "source_path": self.path,
        }

    @staticmethod
    def _load(path: str | None) -> Dict[str, Any]:
        if not path:
            return dict(DEFAULT_EVALUATION_PLAYBOOK)
        file_path = Path(path)
        if not file_path.exists():
            return dict(DEFAULT_EVALUATION_PLAYBOOK)
        with file_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return dict(DEFAULT_EVALUATION_PLAYBOOK)
        raw_agents = data.get("agents")
        if not isinstance(raw_agents, dict):
            data["agents"] = {}
            return data

        base_dir = file_path.parent
        normalized: Dict[str, Dict[str, Any]] = {}
        for key, value in raw_agents.items():
            if not isinstance(key, str):
                continue
            if not isinstance(value, dict):
                continue
            name = value.get("name")
            raw_stages = value.get("stages")
            stages: Dict[str, str] = {}
            if isinstance(raw_stages, dict):
                for stage_key, stage_value in raw_stages.items():
                    if not isinstance(stage_key, str):
                        continue
                    resolved = AgentInstructions._resolve_agent_instruction(stage_value, base_dir=base_dir)
                    if resolved:
                        stages[stage_key] = resolved
            normalized[key] = {
                "name": name if isinstance(name, str) else "",
                "stages": stages,
            }
        data["agents"] = normalized
        return data
