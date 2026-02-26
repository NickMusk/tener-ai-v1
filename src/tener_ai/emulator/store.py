from __future__ import annotations

import copy
import json
import re
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional

from .contracts import normalize_emulator_project


class EmulatorProjectStore:
    def __init__(self, projects_dir: str | Path, company_profiles_path: str | Path) -> None:
        self.projects_dir = Path(projects_dir)
        self.company_profiles_path = Path(company_profiles_path)
        self._lock = threading.Lock()
        self._projects_by_id: Dict[str, Dict[str, Any]] = {}
        self._project_order: List[str] = []
        self._company_profiles: Dict[str, Dict[str, Any]] = {}
        self._load_error: Optional[str] = None
        self.reload()

    @property
    def load_error(self) -> Optional[str]:
        return self._load_error

    def reload(self) -> Dict[str, Any]:
        errors: List[str] = []
        projects_by_id: Dict[str, Dict[str, Any]] = {}
        project_order: List[str] = []

        if self.projects_dir.exists():
            for path in sorted(self.projects_dir.glob("*.json")):
                try:
                    payload = json.loads(path.read_text(encoding="utf-8"))
                    project = normalize_emulator_project(payload, source=str(path.name))
                    project_id = str(project["id"])
                    if project_id in projects_by_id:
                        raise ValueError(f"duplicate project id '{project_id}'")
                    projects_by_id[project_id] = project
                    project_order.append(project_id)
                except Exception as exc:
                    errors.append(f"{path.name}: {exc}")
        else:
            errors.append(f"projects dir not found: {self.projects_dir}")

        company_profiles: Dict[str, Dict[str, Any]] = {}
        if self.company_profiles_path.exists():
            try:
                payload = json.loads(self.company_profiles_path.read_text(encoding="utf-8"))
                company_profiles = self._normalize_company_profiles(payload)
            except Exception as exc:
                errors.append(f"{self.company_profiles_path.name}: {exc}")
        else:
            errors.append(f"company profiles file not found: {self.company_profiles_path}")

        with self._lock:
            self._projects_by_id = projects_by_id
            self._project_order = project_order
            self._company_profiles = company_profiles
            self._load_error = "; ".join(errors) if errors else None

        return {
            "status": "ok" if not errors else "degraded",
            "errors": errors,
            "project_count": len(project_order),
            "company_profile_count": len(company_profiles),
        }

    def health(self) -> Dict[str, Any]:
        with self._lock:
            project_count = len(self._project_order)
            company_profile_count = len(self._company_profiles)
            load_error = self._load_error
        return {
            "status": "ok" if not load_error else "degraded",
            "project_count": project_count,
            "company_profile_count": company_profile_count,
            "load_error": load_error,
        }

    def list_projects(self) -> List[Dict[str, Any]]:
        with self._lock:
            projects = [self._projects_by_id[project_id] for project_id in self._project_order]

        out: List[Dict[str, Any]] = []
        for item in projects:
            events = item.get("events") or []
            out.append(
                {
                    "id": item.get("id"),
                    "company": item.get("company"),
                    "role": item.get("role"),
                    "year": item.get("year"),
                    "candidateCount": len(item.get("candidates") or []),
                    "eventCount": len(events),
                    "lastEventTimestamp": events[-1].get("timestamp") if events else None,
                }
            )
        return out

    def get_project(self, project_id: str) -> Optional[Dict[str, Any]]:
        key = str(project_id or "").strip()
        if not key:
            return None
        with self._lock:
            project = self._projects_by_id.get(key)
        if project is None:
            return None
        return copy.deepcopy(project)

    def list_company_profiles(self) -> List[Dict[str, Any]]:
        with self._lock:
            items = list(self._company_profiles.values())
        out: List[Dict[str, Any]] = []
        for item in items:
            out.append(
                {
                    "id": item["id"],
                    "name": item["name"],
                    "domain": item["domain"],
                    "summary": item.get("summary"),
                }
            )
        out.sort(key=lambda x: str(x.get("name") or "").lower())
        return out

    def get_company_profile(self, company_key: str) -> Optional[Dict[str, Any]]:
        normalized = self._normalize_company_key(company_key)
        if not normalized:
            return None

        with self._lock:
            items = list(self._company_profiles.values())

        for item in items:
            if normalized in {
                item["id"].lower(),
                item["name"].lower(),
                item["domain"].lower(),
            }:
                return copy.deepcopy(item)
            domain = item["domain"].lower()
            if normalized == domain or normalized.endswith(f".{domain}"):
                return copy.deepcopy(item)

        return None

    @staticmethod
    def _normalize_company_profiles(payload: Any) -> Dict[str, Dict[str, Any]]:
        root: Dict[str, Any]
        if isinstance(payload, dict):
            root = payload
            raw_profiles = root.get("profiles")
        elif isinstance(payload, list):
            raw_profiles = payload
        else:
            raise ValueError("company profiles payload must be an object or array")

        if not isinstance(raw_profiles, list):
            raise ValueError("company profiles must include a 'profiles' array")

        profiles: Dict[str, Dict[str, Any]] = {}
        for index, raw in enumerate(raw_profiles):
            if not isinstance(raw, dict):
                raise ValueError(f"profiles[{index}] must be an object")
            name = str(raw.get("name") or "").strip()
            domain = str(raw.get("domain") or "").strip().lower()
            profile_id = str(raw.get("id") or "").strip().lower() or domain
            if not name:
                raise ValueError(f"profiles[{index}].name cannot be empty")
            if not domain:
                raise ValueError(f"profiles[{index}].domain cannot be empty")
            if not profile_id:
                raise ValueError(f"profiles[{index}].id cannot be empty")
            if profile_id in profiles:
                raise ValueError(f"duplicate company profile id '{profile_id}'")

            profile_payload = raw.get("profile")
            if profile_payload is not None and not isinstance(profile_payload, dict):
                raise ValueError(f"profiles[{index}].profile must be an object when provided")

            profiles[profile_id] = {
                "id": profile_id,
                "name": name,
                "domain": domain,
                "summary": str(raw.get("summary") or "").strip() or None,
                "profile": profile_payload or {},
            }
        return profiles

    @staticmethod
    def _normalize_company_key(value: str) -> str:
        raw = str(value or "").strip().lower()
        if not raw:
            return ""
        raw = re.sub(r"^https?://", "", raw)
        raw = raw.split("/", 1)[0]
        if raw.startswith("www."):
            raw = raw[4:]
        return raw
