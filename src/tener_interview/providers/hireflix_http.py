from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib import error, request
from uuid import uuid4


@dataclass(frozen=True)
class HireflixConfig:
    api_key: str
    base_url: str = "https://api.hireflix.com/me"
    position_id: str = ""
    timeout_seconds: int = 30
    public_app_base: str = "https://app.hireflix.com"
    allow_synthetic_email: bool = True
    synthetic_email_domain: str = "interview.local"
    allow_legacy_invite_fallback: bool = False


class HireflixHTTPAdapter:
    name = "hireflix"

    def __init__(self, config: HireflixConfig) -> None:
        if not config.api_key.strip():
            raise ValueError("TENER_HIREFLIX_API_KEY is required for Hireflix adapter")
        self.config = config

    def create_assessment(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        assessment_name = str(payload.get("assessment_name") or "").strip() or "Tener Interview"
        language = str(payload.get("language") or "").strip() or None
        questions_raw = payload.get("questions") if isinstance(payload.get("questions"), list) else []
        questions = [self._question_input(item, language=language) for item in questions_raw if isinstance(item, dict)]
        if not questions:
            raise ValueError("questions are required to create Hireflix assessment")

        create_queries: List[Tuple[str, Dict[str, Any]]] = [
            (
                """
                mutation SavePosition($position: PositionInputType!) {
                  Position {
                    save(position: $position) {
                      id
                      name
                    }
                  }
                }
                """,
                {"position": {"name": assessment_name, "questions": questions}},
            ),
            (
                """
                mutation CreatePosition($position: PositionInputType!) {
                  createPosition(input: $position) {
                    id
                    name
                  }
                }
                """,
                {"position": {"name": assessment_name, "questions": questions}},
            ),
            (
                """
                mutation SavePosition($input: PositionInputType!) {
                  savePosition(position: $input) {
                    id
                    name
                  }
                }
                """,
                {"input": {"name": assessment_name, "questions": questions}},
            ),
        ]

        last_error = ""
        for query, variables in create_queries:
            try:
                response = self._graphql(query=query, variables=variables)
            except Exception as exc:
                last_error = str(exc)
                continue
            created = self._extract_position_from_create_response(response)
            if created:
                position_id = str(created.get("id") or "").strip()
                if position_id:
                    return {
                        "assessment_id": position_id,
                        "assessment_name": str(created.get("name") or assessment_name).strip() or assessment_name,
                        "raw": created,
                    }

        raise ValueError(f"Failed to create Hireflix assessment. {last_error}".strip())

    def create_invitation(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        position_id = str(payload.get("position_id") or self.config.position_id).strip()
        if not position_id:
            raise ValueError("Hireflix position_id is required")

        candidate_name = str(payload.get("candidate_name") or "").strip() or "Candidate"
        first_name, last_name = self._split_name(candidate_name)
        candidate_email = str(payload.get("candidate_email") or "").strip().lower()
        if not candidate_email:
            if self.config.allow_synthetic_email:
                candidate_email = self._synthetic_email(payload)
            else:
                raise ValueError("candidate_email is required for Hireflix invite")

        interview = None
        new_mutation_error = ""
        try:
            interview = self._invite_with_new_mutation(
                position_id=position_id,
                first_name=first_name,
                last_name=last_name,
                candidate_email=candidate_email,
                external_id=str(payload.get("external_id") or "").strip() or None,
                phone=str(payload.get("candidate_phone") or "").strip() or None,
            )
        except Exception as exc:
            new_mutation_error = str(exc)

        if not interview or not str(interview.get("id") or "").strip():
            if self.config.allow_legacy_invite_fallback:
                interview = self._invite_with_legacy_mutation(
                    position_id=position_id,
                    candidate_name=f"{first_name} {last_name}".strip(),
                    candidate_email=candidate_email,
                )
            else:
                raise ValueError(
                    f"Hireflix inviteCandidateToInterview failed. {new_mutation_error or 'invite was rejected'}".strip()
                )

        interview_id = str(interview.get("id") or "").strip()
        if not interview_id:
            raise ValueError(f"Failed to invite candidate on Hireflix. {new_mutation_error}".strip())

        details = self._fetch_interview(interview_id)
        merged = details or interview
        interview_url = self._extract_public_url(merged)
        if not interview_url:
            interview_url = self._url_from_hash(str(merged.get("hash") or ""))
        if not interview_url:
            raise ValueError("Unable to resolve Hireflix public interview URL")

        candidate = merged.get("candidate") if isinstance(merged.get("candidate"), dict) else {}
        candidate_id = str(candidate.get("email") or candidate_email)

        return {
            "invitation_id": interview_id,
            "assessment_id": position_id,
            "candidate_id": candidate_id,
            "interview_url": interview_url,
            "raw": {
                "invite": interview,
                "interview": details,
            },
        }

    def get_interview_status(
        self,
        invitation_id: str,
        *,
        assessment_id: Optional[str] = None,
        candidate_id: Optional[str] = None,
        force: bool = False,
    ) -> Dict[str, Any]:
        _ = assessment_id, candidate_id, force
        try:
            interview = self._fetch_interview(invitation_id)
        except Exception as exc:
            return {
                "status": "failed",
                "error_code": "PROVIDER_STATUS_REQUEST_FAILED",
                "error_message": str(exc),
            }

        if not interview:
            return {
                "status": "failed",
                "error_code": "INTERVIEW_NOT_FOUND",
                "error_message": "Hireflix interview not found",
            }

        raw_status = str(interview.get("status") or "").strip().lower()
        answered = bool(interview.get("answered") is True)
        completed_ts = interview.get("completed")

        if raw_status in {"completed", "preselected", "discarded", "archived"} or completed_ts or answered:
            status = "completed"
        elif raw_status in {"pending", "new", "created"}:
            status = "invited"
        elif raw_status in {"started", "in_progress", "in-progress", "active"}:
            status = "in_progress"
        else:
            status = "in_progress"

        return {
            "status": status,
            "raw_status": raw_status,
            "interview": interview,
        }

    def get_interview_result(
        self,
        invitation_id: str,
        *,
        assessment_id: Optional[str] = None,
        candidate_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        _ = assessment_id, candidate_id
        try:
            interview = self._fetch_interview(invitation_id)
        except Exception as exc:
            return {
                "status": "failed",
                "error_code": "PROVIDER_RESULTS_REQUEST_FAILED",
                "error_message": str(exc),
            }

        if not interview:
            return {
                "status": "failed",
                "error_code": "INTERVIEW_NOT_FOUND",
                "error_message": "Hireflix interview not found",
            }

        global_score = self._normalize_score(self._dig(interview, "score", "value"))
        questions = interview.get("questions") if isinstance(interview.get("questions"), list) else []

        technical_values: List[float] = []
        soft_values: List[float] = []
        culture_values: List[float] = []

        answered_questions = 0
        for question in questions:
            if not isinstance(question, dict):
                continue
            category = self._category_for_question(question)
            answer = question.get("answer") if isinstance(question.get("answer"), dict) else {}
            has_answer = bool(answer.get("id") or answer.get("url") or self._dig(answer, "transcription", "text"))
            if has_answer:
                answered_questions += 1
            if not has_answer:
                continue

            if global_score is None:
                continue
            score = global_score
            if category == "culture_fit":
                culture_values.append(score)
            elif category == "soft_skills":
                soft_values.append(score)
            else:
                technical_values.append(score)

        technical = self._average(technical_values)
        soft = self._average(soft_values)
        culture = self._average(culture_values)

        if global_score is not None and technical is None and soft is None and culture is None:
            technical = global_score
            soft = global_score
            culture = global_score

        if technical is None and soft is None and culture is None:
            if questions:
                completion_ratio = float(answered_questions) / float(len(questions)) if len(questions) > 0 else 0.0
                technical = round(completion_ratio * 100.0, 2)
            else:
                return {
                    "status": "failed",
                    "error_code": "EMPTY_RESULTS",
                    "error_message": "No score or answered questions available",
                    "raw": interview,
                }

        return {
            "status": "ok",
            "result_id": str(invitation_id),
            "scores": {
                "technical": technical,
                "soft_skills": soft,
                "culture_fit": culture,
            },
            "raw": interview,
        }

    def _invite_with_new_mutation(
        self,
        *,
        position_id: str,
        first_name: str,
        last_name: str,
        candidate_email: str,
        external_id: Optional[str],
        phone: Optional[str],
    ) -> Optional[Dict[str, Any]]:
        query = """
        mutation InviteCandidateToInterview($input: InviteCandidateToInterviewInput!) {
          inviteCandidateToInterview(input: $input) {
            __typename
            ... on InterviewType {
              id
              hash
              status
              answered
              completed
              score { value }
              url { public short private }
            }
            ... on ExceededInvitesThisPeriodError {
              code
              name
              m1: message
            }
            ... on InterviewAlreadyExistsInPositionError {
              code
              name
              m2: message
            }
            ... on InterviewExternalIdAlreadyExistsInPositionError {
              code
              name
              m3: message
            }
            ... on PositionNotFoundError {
              code
              name
              m4: message
            }
            ... on PositionNotReadyToAcceptInvitesError {
              code
              name
              m5: message
            }
            ... on ValidationError {
              code
              name
              m6: message
              fieldErrors {
                flattenedPath
                message
              }
            }
          }
        }
        """
        candidate_input: Dict[str, Any] = {
            "email": candidate_email,
            "firstName": first_name,
            "lastName": last_name,
        }
        if phone:
            candidate_input["phone"] = phone

        input_payload: Dict[str, Any] = {
            "candidate": candidate_input,
            "positionId": position_id,
        }
        if external_id:
            input_payload["externalId"] = external_id

        resp = self._graphql(query=query, variables={"input": input_payload})
        data = resp.get("data") if isinstance(resp.get("data"), dict) else {}
        out = data.get("inviteCandidateToInterview") if isinstance(data.get("inviteCandidateToInterview"), dict) else {}
        if not out:
            return None
        typename = str(out.get("__typename") or "")
        if typename and typename != "InterviewType":
            error_message = self._invite_union_error_message(out=out, typename=typename)
            raise ValueError(error_message)
        if out.get("id"):
            return out
        if typename == "InterviewType":
            raise ValueError("inviteCandidateToInterview returned InterviewType without id")
        return None

    @staticmethod
    def _invite_union_error_message(*, out: Dict[str, Any], typename: str) -> str:
        code = str(out.get("code") or "").strip()
        message = str(
            out.get("message")
            or out.get("vmsg")
            or out.get("m1")
            or out.get("m2")
            or out.get("m3")
            or out.get("m4")
            or out.get("m5")
            or out.get("m6")
            or ""
        ).strip()
        name = str(out.get("name") or "").strip()
        field_errors = out.get("fieldErrors") if isinstance(out.get("fieldErrors"), list) else []
        field_summaries: List[str] = []
        for item in field_errors:
            if not isinstance(item, dict):
                continue
            path = str(item.get("flattenedPath") or "").strip()
            msg = str(item.get("message") or "").strip()
            if path and msg:
                field_summaries.append(f"{path}: {msg}")
            elif msg:
                field_summaries.append(msg)
        if field_summaries:
            joined = "; ".join(field_summaries)
            if message:
                message = f"{message} ({joined})"
            else:
                message = joined
        parts: List[str] = []
        if typename:
            parts.append(typename)
        if name and name != typename:
            parts.append(name)
        if code:
            parts.append(f"code={code}")
        if message:
            parts.append(message)
        if parts:
            return " ".join(parts)
        return "inviteCandidateToInterview was rejected"

    def _invite_with_legacy_mutation(self, *, position_id: str, candidate_name: str, candidate_email: str) -> Dict[str, Any]:
        query = """
        mutation LegacyInvite($positionId: String!, $candidateName: String!, $candidateEmail: String!) {
          Position(id: $positionId) {
            invite(candidate: { name: $candidateName, email: $candidateEmail }) {
              id
              hash
              status
              answered
              completed
              score { value }
              url { public short private }
            }
          }
        }
        """
        resp = self._graphql(
            query=query,
            variables={
                "positionId": position_id,
                "candidateName": candidate_name,
                "candidateEmail": candidate_email,
            },
        )
        data = resp.get("data") if isinstance(resp.get("data"), dict) else {}
        position_data = data.get("Position") if isinstance(data.get("Position"), dict) else {}
        interview = position_data.get("invite") if isinstance(position_data.get("invite"), dict) else {}
        if not interview or not str(interview.get("id") or "").strip():
            raise ValueError("Hireflix legacy invite mutation failed")
        return interview

    def _fetch_interview(self, interview_id: str) -> Optional[Dict[str, Any]]:
        if not interview_id:
            return None

        queries = [
            """
            query Interview($id: String!) {
              interview(id: $id) {
                id
                hash
                status
                answered
                completed
                score { value }
                candidate { name email }
                url { public short private }
                questions {
                  id
                  title
                  description
                  answer {
                    id
                    url
                    transcription { text languageCode }
                  }
                }
              }
            }
            """,
            """
            query Interview($id: String!) {
              interview(id: $id) {
                id
                hash
                status
                answered
                completed
                score { value }
                candidate { name email }
                url { public short private }
              }
            }
            """,
            """
            query Interview($id: String!) {
              interview(id: $id) {
                id
                hash
                status
              }
            }
            """,
        ]

        last_error = ""
        for query in queries:
            try:
                resp = self._graphql(query=query, variables={"id": interview_id})
            except Exception as exc:
                last_error = str(exc)
                continue
            data = resp.get("data") if isinstance(resp.get("data"), dict) else {}
            interview = data.get("interview") if isinstance(data.get("interview"), dict) else None
            if interview:
                return interview

        if last_error:
            raise ValueError(last_error)
        return None

    def _graphql(self, *, query: str, variables: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"query": query}
        if variables is not None:
            payload["variables"] = variables

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "x-api-key": self.config.api_key,
        }

        req = request.Request(
            url=self.config.base_url,
            method="POST",
            headers=headers,
            data=json.dumps(payload).encode("utf-8"),
        )

        try:
            with request.urlopen(req, timeout=self.config.timeout_seconds) as resp:
                raw = resp.read().decode("utf-8")
        except error.HTTPError as exc:
            body_raw = exc.read().decode("utf-8") if exc.fp else ""
            raise ValueError(f"Hireflix API {exc.code}: {body_raw or exc.reason or 'http error'}") from exc
        except error.URLError as exc:
            raise ValueError(f"Hireflix network error: {exc.reason}") from exc

        if not raw:
            raise ValueError("Hireflix API returned empty body")

        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError("Hireflix API returned non-JSON response") from exc

        if not isinstance(parsed, dict):
            raise ValueError("Hireflix API returned invalid JSON object")

        errors = parsed.get("errors")
        if isinstance(errors, list) and errors:
            message_parts = []
            for item in errors:
                if isinstance(item, dict):
                    message = str(item.get("message") or "").strip()
                    if message:
                        message_parts.append(message)
            if message_parts:
                raise ValueError("; ".join(message_parts))
            raise ValueError("Hireflix GraphQL request failed")

        return parsed

    def _extract_public_url(self, interview: Dict[str, Any]) -> str:
        url_data = interview.get("url") if isinstance(interview.get("url"), dict) else {}
        public_url = str(url_data.get("public") or "").strip()
        if public_url:
            return public_url
        short_url = str(url_data.get("short") or "").strip()
        if short_url:
            if short_url.startswith("http"):
                return short_url
            return f"https://{short_url}"
        return ""

    def _url_from_hash(self, interview_hash: str) -> str:
        clean_hash = interview_hash.strip()
        if not clean_hash:
            return ""
        base = self.config.public_app_base.rstrip("/")
        return f"{base}/{clean_hash}"

    def _synthetic_email(self, payload: Dict[str, Any]) -> str:
        candidate_id = str(payload.get("candidate_id") or "candidate").strip().lower()
        safe_local = "".join(ch if ch.isalnum() else "-" for ch in candidate_id).strip("-") or "candidate"
        nonce = uuid4().hex[:8]
        domain = self.config.synthetic_email_domain.strip().lower() or "interview.local"
        return f"hireflix-{safe_local}-{nonce}@{domain}"

    @staticmethod
    def _extract_position_from_create_response(resp: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        data = resp.get("data") if isinstance(resp.get("data"), dict) else {}

        position_root = data.get("Position") if isinstance(data.get("Position"), dict) else {}
        if isinstance(position_root.get("save"), dict):
            return position_root["save"]

        if isinstance(data.get("createPosition"), dict):
            return data["createPosition"]

        if isinstance(data.get("savePosition"), dict):
            return data["savePosition"]

        return None

    @staticmethod
    def _question_input(question: Dict[str, Any], *, language: Optional[str] = None) -> Dict[str, Any]:
        title = str(question.get("title") or "").strip()
        if not title:
            title = "Interview question"
        description = str(question.get("description") or "").strip()

        time_to_answer = HireflixHTTPAdapter._safe_int(question.get("timeToAnswer"), 120)
        time_to_think = HireflixHTTPAdapter._safe_int(question.get("timeToThink"), 12)
        retakes = HireflixHTTPAdapter._safe_int(question.get("retakes"), 1)

        out: Dict[str, Any] = {
            "title": title,
            "description": description,
            "timeToAnswer": max(30, time_to_answer),
            "timeToThink": max(5, time_to_think),
            "retakes": max(0, retakes),
        }
        if language:
            out["transcriptionLanguage"] = language
        return out

    @staticmethod
    def _safe_int(value: Any, fallback: int) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return int(fallback)

    @staticmethod
    def _normalize_score(raw: Any) -> Optional[float]:
        if raw is None:
            return None
        try:
            value = float(raw)
        except (TypeError, ValueError):
            return None
        if value < 0:
            value = 0.0
        if 0 <= value <= 1:
            value = value * 100.0
        if value > 100:
            value = 100.0
        return round(value, 2)

    @staticmethod
    def _category_for_question(question: Dict[str, Any]) -> str:
        explicit = str(question.get("category") or "").strip().lower()
        if explicit in {"culture_fit", "cultural_fit", "culture"}:
            return "culture_fit"
        if explicit in {"soft_skills", "soft skills", "soft"}:
            return "soft_skills"
        if explicit in {"hard_skills", "hard skills", "technical", "tech"}:
            return "technical"

        text = " ".join(
            [
                str(question.get("title") or ""),
                str(question.get("description") or ""),
            ]
        ).lower()

        culture_markers = {"culture", "values", "mission", "fit", "motivation"}
        soft_markers = {
            "communication",
            "team",
            "collaboration",
            "leadership",
            "stakeholder",
            "conflict",
            "behavioral",
            "behavioural",
            "soft",
        }
        technical_markers = {
            "technical",
            "coding",
            "architecture",
            "system design",
            "algorithm",
            "python",
            "java",
            "backend",
            "frontend",
            "sql",
            "devops",
            "cloud",
            "hard skills",
        }

        if any(marker in text for marker in culture_markers):
            return "culture_fit"
        if any(marker in text for marker in soft_markers):
            return "soft_skills"
        if any(marker in text for marker in technical_markers):
            return "technical"
        return "technical"

    @staticmethod
    def _average(values: Iterable[float]) -> Optional[float]:
        items = [float(v) for v in values]
        if not items:
            return None
        return round(sum(items) / float(len(items)), 2)

    @staticmethod
    def _dig(obj: Dict[str, Any], *keys: str) -> Any:
        cur: Any = obj
        for key in keys:
            if not isinstance(cur, dict):
                return None
            cur = cur.get(key)
        return cur

    @staticmethod
    def _split_name(full_name: str) -> Tuple[str, str]:
        cleaned = re.sub(r"\s+", " ", str(full_name or "")).strip()
        if not cleaned:
            return "Candidate", "Candidate"
        parts = [p for p in cleaned.split(" ") if p]
        first = HireflixHTTPAdapter._sanitize_name_part(parts[0])
        last_raw = " ".join(parts[1:]) if len(parts) > 1 else ""
        last = HireflixHTTPAdapter._sanitize_name_part(last_raw)
        if not first:
            first = "Candidate"
        if not last:
            last = "Candidate"
        return first[:64], last[:64]

    @staticmethod
    def _sanitize_name_part(value: str) -> str:
        if not value:
            return ""
        cleaned = re.sub(r"[^A-Za-z0-9 .,'-]+", " ", value)
        cleaned = re.sub(r"\s+", " ", cleaned).strip(" ,.-")
        return cleaned.strip()
