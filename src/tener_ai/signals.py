from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


UTC = timezone.utc


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _normalize_score_100(value: Any) -> float:
    score = _safe_float(value, 0.0)
    if score <= 1.0:
        score *= 100.0
    if score < 0.0:
        return 0.0
    if score > 100.0:
        return 100.0
    return score


def _clamp(value: float, min_value: float, max_value: float) -> float:
    if value < min_value:
        return min_value
    if value > max_value:
        return max_value
    return value


class SignalIngestionService:
    def __init__(self, db: Any) -> None:
        self.db = db

    def ingest_job(self, *, job_id: int, limit_candidates: int = 500, limit_per_candidate: int = 300) -> Dict[str, Any]:
        job = self.db.get_job(int(job_id))
        if not job:
            raise ValueError("job not found")

        candidates = self.db.list_candidates_for_job(int(job_id))
        safe_limit = max(1, min(int(limit_candidates or 500), 5000))
        selected = candidates[:safe_limit]

        written = 0
        candidates_processed = 0
        by_source = {"assessment": 0, "pre_resume_event": 0, "operation_log": 0, "match_snapshot": 0}

        for row in selected:
            candidate_id = int(row.get("candidate_id") or 0)
            if candidate_id <= 0:
                continue
            conversation_id = row.get("conversation_id")
            candidates_processed += 1

            written += self._ingest_assessment_signals(job_id=job_id, candidate_id=candidate_id, bucket=by_source)
            written += self._ingest_pre_resume_signals(
                job_id=job_id,
                candidate_id=candidate_id,
                conversation_id=conversation_id,
                limit=limit_per_candidate,
                bucket=by_source,
            )
            written += self._ingest_operation_signals(
                job_id=job_id,
                candidate_id=candidate_id,
                conversation_id=conversation_id,
                limit=limit_per_candidate,
                bucket=by_source,
            )
            written += self._ingest_match_snapshot_signal(
                job_id=job_id,
                candidate_id=candidate_id,
                conversation_id=conversation_id,
                bucket=by_source,
            )

        return {
            "status": "ok",
            "job_id": int(job_id),
            "job_title": str(job.get("title") or ""),
            "candidates_total": len(candidates),
            "candidates_processed": candidates_processed,
            "signals_upserted": written,
            "sources": by_source,
            "generated_at": utc_now_iso(),
        }

    def _ingest_assessment_signals(self, *, job_id: int, candidate_id: int, bucket: Dict[str, int]) -> int:
        rows = self.db.list_candidate_assessments(candidate_id=int(candidate_id), job_id=int(job_id))
        written = 0
        for item in rows:
            assessment_id = int(item.get("id") or 0)
            stage_key = str(item.get("stage_key") or "").strip().lower() or "assessment"
            agent_key = str(item.get("agent_key") or "").strip().lower() or "agent"
            score_raw = item.get("score")
            score_100 = _normalize_score_100(score_raw)
            impact = round((score_100 - 50.0) / 25.0, 3)
            status = str(item.get("status") or "").strip().lower()
            if score_raw is None:
                if status in {"qualified", "verified", "scored", "resume_received"}:
                    impact = 1.0
                elif status in {"rejected", "failed", "not_interested"}:
                    impact = -1.5
                else:
                    impact = 0.0

            title = str(item.get("reason") or "").strip() or f"{item.get('agent_name') or agent_key}: {stage_key}"
            detail = f"status={status or 'unknown'}; score={score_raw}" if score_raw is not None else f"status={status or 'unknown'}"
            source_id = str(assessment_id) if assessment_id > 0 else f"{agent_key}:{stage_key}"

            self.db.upsert_candidate_signal(
                job_id=int(job_id),
                candidate_id=int(candidate_id),
                source_type="assessment",
                source_id=source_id,
                signal_type=stage_key,
                signal_category=agent_key,
                title=title,
                detail=detail,
                impact_score=impact,
                confidence=0.8 if score_raw is not None else 0.6,
                observed_at=str(item.get("updated_at") or utc_now_iso()),
                signal_meta={
                    "agent_key": agent_key,
                    "agent_name": item.get("agent_name"),
                    "stage_key": stage_key,
                    "status": status,
                    "score": score_raw,
                    "details": item.get("details") if isinstance(item.get("details"), dict) else {},
                },
            )
            written += 1
            bucket["assessment"] = int(bucket.get("assessment") or 0) + 1
        return written

    def _ingest_pre_resume_signals(
        self,
        *,
        job_id: int,
        candidate_id: int,
        conversation_id: Any,
        limit: int,
        bucket: Dict[str, int],
    ) -> int:
        events = self.db.list_pre_resume_events_for_candidate(
            int(candidate_id),
            job_id=int(job_id),
            limit=max(1, min(int(limit or 300), 2000)),
        )
        written = 0
        for event in events:
            event_id = int(event.get("id") or 0)
            event_type = str(event.get("event_type") or "").strip().lower() or "event"
            intent = str(event.get("intent") or "").strip().lower()
            status = str(event.get("state_status") or "").strip().lower()
            impact = 0.0
            if intent == "resume_shared" or status == "resume_received":
                impact = 2.0
            elif intent == "not_interested" or status == "not_interested":
                impact = -2.0
            elif status == "unreachable":
                impact = -1.8
            elif event_type == "followup_sent":
                impact = -0.4
            elif event_type == "session_started":
                impact = 0.4

            source_id = str(event_id) if event_id > 0 else f"{event_type}:{event.get('created_at')}"
            title = f"Pre-resume: {event_type}"
            if intent:
                title = f"{title} ({intent})"
            detail = str(event.get("inbound_text") or event.get("outbound_text") or "").strip()[:240] or None

            self.db.upsert_candidate_signal(
                job_id=int(job_id),
                candidate_id=int(candidate_id),
                conversation_id=int(conversation_id) if conversation_id is not None else None,
                source_type="pre_resume_event",
                source_id=source_id,
                signal_type=event_type,
                signal_category="communication",
                title=title,
                detail=detail,
                impact_score=impact,
                confidence=0.75,
                observed_at=str(event.get("created_at") or utc_now_iso()),
                signal_meta={
                    "intent": intent or None,
                    "state_status": status or None,
                    "details": event.get("details") if isinstance(event.get("details"), dict) else {},
                },
            )
            written += 1
            bucket["pre_resume_event"] = int(bucket.get("pre_resume_event") or 0) + 1
        return written

    def _ingest_operation_signals(
        self,
        *,
        job_id: int,
        candidate_id: int,
        conversation_id: Any,
        limit: int,
        bucket: Dict[str, int],
    ) -> int:
        rows = self.db.list_logs_for_candidate(
            int(candidate_id),
            limit=max(1, min(int(limit or 300), 2000)),
        )
        written = 0
        for item in rows:
            details = item.get("details") if isinstance(item.get("details"), dict) else {}
            details_job_id = details.get("job_id")
            if details_job_id is not None:
                try:
                    if int(details_job_id) != int(job_id):
                        continue
                except (TypeError, ValueError):
                    pass

            operation = str(item.get("operation") or "").strip().lower()
            if not operation:
                continue
            if not (
                operation.startswith("agent.")
                or operation.startswith("scheduler.")
                or operation.startswith("poll.")
                or operation.startswith("interview.")
            ):
                continue

            log_id = int(item.get("id") or 0)
            status = str(item.get("status") or "").strip().lower()
            if status in {"error", "failed"}:
                impact = -1.2
            elif status in {"warning", "partial"}:
                impact = -0.5
            elif status in {"ok", "sent", "connected", "created"}:
                impact = 0.6
            elif status in {"skipped"}:
                impact = -0.2
            else:
                impact = 0.0

            category = operation.split(".", 1)[0]
            source_id = str(log_id) if log_id > 0 else f"{operation}:{item.get('created_at')}"
            title = f"{operation} [{status or 'unknown'}]"
            detail = str(details.get("reason") or details.get("error") or "").strip()[:260] or None

            self.db.upsert_candidate_signal(
                job_id=int(job_id),
                candidate_id=int(candidate_id),
                conversation_id=int(conversation_id) if conversation_id is not None else None,
                source_type="operation_log",
                source_id=source_id,
                signal_type=operation,
                signal_category=category,
                title=title,
                detail=detail,
                impact_score=impact,
                confidence=0.55,
                observed_at=str(item.get("created_at") or utc_now_iso()),
                signal_meta={
                    "status": status,
                    "entity_type": item.get("entity_type"),
                    "entity_id": item.get("entity_id"),
                    "details": details,
                },
            )
            written += 1
            bucket["operation_log"] = int(bucket.get("operation_log") or 0) + 1
        return written

    def _ingest_match_snapshot_signal(
        self,
        *,
        job_id: int,
        candidate_id: int,
        conversation_id: Any,
        bucket: Dict[str, int],
    ) -> int:
        match = self.db.get_candidate_match(int(job_id), int(candidate_id))
        if not match:
            return 0
        notes = match.get("verification_notes") if isinstance(match.get("verification_notes"), dict) else {}
        match_status = str(match.get("status") or "").strip().lower()
        score = _normalize_score_100(match.get("score"))
        interview_status = str(notes.get("interview_status") or "").strip().lower()
        interview_score = notes.get("interview_total_score")

        status_impact = {
            "verified": 1.4,
            "resume_received": 1.2,
            "awaiting_resume": 0.2,
            "needs_resume": -0.2,
            "rejected": -1.8,
            "outreached": 0.3,
            "responded": 0.8,
            "interviewed": 1.0,
            "hired": 2.2,
        }
        impact = status_impact.get(match_status, 0.0) + ((score - 50.0) / 35.0)
        if interview_status in {"scored", "completed"}:
            impact += 0.8
        elif interview_status in {"failed", "expired", "canceled"}:
            impact -= 0.8

        source_id = f"status={match_status}|score={round(score,2)}|iv={interview_status}|ivs={interview_score}"
        title = f"Match status: {match_status or 'unknown'}"
        detail = f"screening score={round(score,2)}; interview_status={interview_status or 'n/a'}"

        self.db.upsert_candidate_signal(
            job_id=int(job_id),
            candidate_id=int(candidate_id),
            conversation_id=int(conversation_id) if conversation_id is not None else None,
            source_type="match_snapshot",
            source_id=source_id,
            signal_type="match_status",
            signal_category="screening",
            title=title,
            detail=detail,
            impact_score=round(impact, 3),
            confidence=0.65,
            observed_at=str(match.get("created_at") or utc_now_iso()),
            signal_meta={
                "match_status": match_status,
                "score": match.get("score"),
                "verification_notes": notes,
            },
        )
        bucket["match_snapshot"] = int(bucket.get("match_snapshot") or 0) + 1
        return 1


class JobSignalsLiveViewService:
    def __init__(self, db: Any) -> None:
        self.db = db

    def build_job_view(self, *, job_id: int, limit_candidates: int = 200, limit_signals: int = 5000) -> Dict[str, Any]:
        job = self.db.get_job(int(job_id))
        if not job:
            raise ValueError("job not found")

        candidates = self.db.list_candidates_for_job(int(job_id))
        safe_candidate_limit = max(1, min(int(limit_candidates or 200), 2000))
        selected_candidates = candidates[:safe_candidate_limit]
        signals = self.db.list_job_signals(job_id=int(job_id), limit=max(1, min(int(limit_signals or 5000), 20000)))

        buckets: Dict[int, Dict[str, Any]] = {}
        for row in selected_candidates:
            candidate_id = int(row.get("candidate_id") or 0)
            if candidate_id <= 0:
                continue
            base_score = _normalize_score_100(row.get("score"))
            buckets[candidate_id] = {
                "candidate_id": candidate_id,
                "candidate_name": row.get("full_name") or row.get("candidate_name") or f"Candidate {candidate_id}",
                "status": row.get("current_status_label") or row.get("status") or "unknown",
                "base_score": round(base_score, 2),
                "impact_total": 0.0,
                "signal_count": 0,
                "top_signals": [],
            }

        for signal in signals:
            candidate_id = int(signal.get("candidate_id") or 0)
            bucket = buckets.get(candidate_id)
            if bucket is None:
                continue
            impact = _safe_float(signal.get("impact_score"), 0.0)
            bucket["impact_total"] = float(bucket.get("impact_total") or 0.0) + impact
            bucket["signal_count"] = int(bucket.get("signal_count") or 0) + 1

            top_signals = bucket.get("top_signals")
            if isinstance(top_signals, list) and len(top_signals) < 5:
                top_signals.append(
                    {
                        "title": signal.get("title"),
                        "detail": signal.get("detail"),
                        "impact_score": impact,
                        "signal_category": signal.get("signal_category"),
                        "observed_at": signal.get("observed_at"),
                    }
                )

        base_order = sorted(
            list(buckets.values()),
            key=lambda item: (float(item.get("base_score") or 0.0), int(item.get("candidate_id") or 0)),
            reverse=True,
        )
        previous_rank: Dict[int, int] = {}
        for index, row in enumerate(base_order, start=1):
            previous_rank[int(row["candidate_id"])] = index

        ranking: List[Dict[str, Any]] = []
        for row in buckets.values():
            impact_total = float(row.get("impact_total") or 0.0)
            impact_points = _clamp(impact_total * 4.0, -30.0, 30.0)
            live_score = _clamp(float(row.get("base_score") or 0.0) + impact_points, 0.0, 100.0)
            ranking.append(
                {
                    "candidate_id": row["candidate_id"],
                    "candidate_name": row["candidate_name"],
                    "status": row["status"],
                    "base_score": row["base_score"],
                    "live_score": round(live_score, 2),
                    "impact_points": round(impact_points, 2),
                    "signal_count": int(row.get("signal_count") or 0),
                    "top_signals": row.get("top_signals") if isinstance(row.get("top_signals"), list) else [],
                }
            )

        ranking.sort(
            key=lambda item: (
                float(item.get("live_score") or 0.0),
                float(item.get("base_score") or 0.0),
                int(item.get("candidate_id") or 0),
            ),
            reverse=True,
        )

        for index, item in enumerate(ranking, start=1):
            prev = previous_rank.get(int(item["candidate_id"]), index)
            item["rank"] = index
            item["previous_rank"] = prev
            item["rank_delta"] = prev - index

        timeline: List[Dict[str, Any]] = []
        category_counts: Dict[str, int] = {}
        for signal in signals[:1000]:
            category = str(signal.get("signal_category") or "other").strip().lower() or "other"
            category_counts[category] = int(category_counts.get(category) or 0) + 1
            timeline.append(
                {
                    "observed_at": signal.get("observed_at"),
                    "candidate_id": signal.get("candidate_id"),
                    "candidate_name": signal.get("candidate_name"),
                    "source_type": signal.get("source_type"),
                    "signal_type": signal.get("signal_type"),
                    "signal_category": category,
                    "title": signal.get("title"),
                    "detail": signal.get("detail"),
                    "impact_score": signal.get("impact_score"),
                }
            )

        return {
            "job_id": int(job_id),
            "job_title": str(job.get("title") or ""),
            "generated_at": utc_now_iso(),
            "candidates_total": len(selected_candidates),
            "signals_total": len(signals),
            "ranking": ranking,
            "timeline": timeline,
            "category_counts": category_counts,
        }


class MonitoringService:
    def __init__(self, db: Any) -> None:
        self.db = db

    def build_status(self, *, limit_jobs: int = 20) -> Dict[str, Any]:
        jobs = self.db.list_jobs(limit=max(1, min(int(limit_jobs or 20), 200)))
        items: List[Dict[str, Any]] = []
        alerts: List[Dict[str, Any]] = []

        for job in jobs:
            job_id = int(job.get("id") or 0)
            candidates = self.db.list_candidates_for_job(job_id)
            candidate_total = len(candidates)
            signals = self.db.list_job_signals(job_id=job_id, limit=5000)
            with_signals = len({int(item.get("candidate_id") or 0) for item in signals if int(item.get("candidate_id") or 0) > 0})
            coverage = (with_signals / candidate_total) if candidate_total > 0 else 1.0
            latest_signal_at = signals[0].get("observed_at") if signals else None
            items.append(
                {
                    "job_id": job_id,
                    "job_title": job.get("title"),
                    "candidate_total": candidate_total,
                    "candidate_with_signals": with_signals,
                    "signal_coverage": round(coverage, 3),
                    "signals_total": len(signals),
                    "latest_signal_at": latest_signal_at,
                }
            )
            if candidate_total >= 5 and coverage < 0.5:
                alerts.append(
                    {
                        "severity": "warning",
                        "job_id": job_id,
                        "reason": "low_signal_coverage",
                        "message": f"Only {with_signals}/{candidate_total} candidates have ingested signals",
                    }
                )
            if candidate_total >= 5 and len(signals) == 0:
                alerts.append(
                    {
                        "severity": "warning",
                        "job_id": job_id,
                        "reason": "signals_missing",
                        "message": "No candidate signals found for active job",
                    }
                )

        status = "ok"
        if alerts:
            status = "warning"

        return {
            "status": status,
            "generated_at": utc_now_iso(),
            "jobs_checked": len(items),
            "alerts": alerts,
            "items": items,
        }

