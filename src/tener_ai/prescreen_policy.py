from __future__ import annotations

from typing import Any, Dict, List, Optional


REMOTE_LOCATION_MARKERS = {"remote", "global", "worldwide", "anywhere", "hybrid", "distributed"}


def safe_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def collapse_salary_range_to_expectation(min_value: Any, max_value: Any) -> Optional[float]:
    salary_min = safe_float(min_value)
    salary_max = safe_float(max_value)
    if salary_min is None and salary_max is None:
        return None
    if salary_min is None:
        return salary_max
    if salary_max is None:
        return salary_min
    return round((salary_min + salary_max) / 2.0, 2)


class PrescreenPolicy:
    @staticmethod
    def location_confirmation_required(job_location: Any) -> bool:
        location = str(job_location or "").strip().lower()
        if not location:
            return False
        return not any(marker in location for marker in REMOTE_LOCATION_MARKERS)

    @staticmethod
    def auth_confirmation_required(work_authorization_required: Any) -> bool:
        return bool(work_authorization_required)

    @classmethod
    def written_answers_complete(
        cls,
        *,
        must_have_answer: Any,
        salary_expectation_gross_monthly: Any,
        job_location: Any,
        location_confirmed: Any,
        work_authorization_required: Any,
        work_authorization_confirmed: Any,
    ) -> bool:
        if not str(must_have_answer or "").strip():
            return False
        if safe_float(salary_expectation_gross_monthly) is None:
            return False
        if cls.location_confirmation_required(job_location) and location_confirmed is None:
            return False
        if cls.auth_confirmation_required(work_authorization_required) and work_authorization_confirmed is None:
            return False
        return True

    @classmethod
    def missing_question_keys(
        cls,
        *,
        must_have_answer: Any,
        salary_expectation_gross_monthly: Any,
        job_location: Any,
        location_confirmed: Any,
        work_authorization_required: Any,
        work_authorization_confirmed: Any,
    ) -> List[str]:
        missing: List[str] = []
        if not str(must_have_answer or "").strip():
            missing.append("must_have")
        if safe_float(salary_expectation_gross_monthly) is None:
            missing.append("salary")
        if cls.location_confirmation_required(job_location) and location_confirmed is None:
            missing.append("location_auth")
        elif cls.auth_confirmation_required(work_authorization_required) and work_authorization_confirmed is None:
            missing.append("location_auth")
        return missing

    @classmethod
    def prescreen_status(
        cls,
        *,
        cv_received: Any,
        must_have_answer: Any,
        salary_expectation_gross_monthly: Any,
        job_location: Any,
        location_confirmed: Any,
        work_authorization_required: Any,
        work_authorization_confirmed: Any,
    ) -> str:
        written_complete = cls.written_answers_complete(
            must_have_answer=must_have_answer,
            salary_expectation_gross_monthly=salary_expectation_gross_monthly,
            job_location=job_location,
            location_confirmed=location_confirmed,
            work_authorization_required=work_authorization_required,
            work_authorization_confirmed=work_authorization_confirmed,
        )
        if bool(cv_received) and written_complete:
            return "ready_for_interview"
        if bool(cv_received):
            return "cv_received_pending_answers"
        if written_complete:
            return "ready_for_cv"
        return "incomplete"

    @staticmethod
    def normalize_prescreen_status(value: Any) -> str:
        normalized = str(value or "").strip().lower()
        if normalized == "ready_for_screening_call":
            return "ready_for_interview"
        if normalized in {"cv_received_pending_answers", "ready_for_interview", "ready_for_cv", "incomplete"}:
            return normalized
        return normalized or "incomplete"

    @classmethod
    def match_status_from_prescreen_status(cls, prescreen_status: Any) -> str | None:
        normalized = cls.normalize_prescreen_status(prescreen_status)
        if normalized == "ready_for_cv":
            return "must_have_approved"
        if normalized == "cv_received_pending_answers":
            return "resume_received_pending_must_have"
        if normalized == "ready_for_interview":
            return "resume_received"
        return None

    @staticmethod
    def salary_alignment(
        *,
        job_salary_min: Any,
        job_salary_max: Any,
        candidate_salary_expectation_gross_monthly: Any,
    ) -> Dict[str, Any]:
        candidate_salary = safe_float(candidate_salary_expectation_gross_monthly)
        salary_min = safe_float(job_salary_min)
        salary_max = safe_float(job_salary_max)
        if candidate_salary is None or (salary_min is None and salary_max is None):
            return {"status": "unknown", "candidate_salary": candidate_salary}
        if salary_max is not None:
            overage = candidate_salary - salary_max
            if overage <= 0:
                return {"status": "within_budget", "candidate_salary": candidate_salary}
            if overage <= max(10000.0, float(salary_max) * 0.10):
                return {"status": "slightly_above", "candidate_salary": candidate_salary}
            return {"status": "far_above", "candidate_salary": candidate_salary}
        if salary_min is not None and candidate_salary < salary_min:
            return {"status": "below_budget", "candidate_salary": candidate_salary}
        return {"status": "unknown", "candidate_salary": candidate_salary}
