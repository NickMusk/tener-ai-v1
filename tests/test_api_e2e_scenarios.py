from __future__ import annotations

import json
import os
import threading
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory, gettempdir
from typing import Any, Dict, List, Optional, Tuple
from urllib import error, request

# Prevent import-time default service bootstrap from writing inside the repo.
os.environ.setdefault("TENER_DB_PATH", str(Path(gettempdir()) / "tener_api_bootstrap.sqlite3"))

from tener_ai.agents import FAQAgent, OutreachAgent, SourcingAgent, VerificationAgent
from tener_ai.db import Database
from tener_ai.instructions import AgentInstructions
from tener_ai.linkedin_provider import MockLinkedInProvider
from tener_ai.matching import MatchingEngine
from tener_ai.pre_resume_service import PreResumeCommunicationService
from tener_ai.workflow import WorkflowService
from tener_ai import main as api_main


class ApiE2EScenariosTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.root = Path(__file__).resolve().parents[1]

        scenarios_path = cls.root / "tests" / "scenarios" / "api_e2e_scenarios.json"
        with scenarios_path.open("r", encoding="utf-8") as f:
            cls.scenarios = json.load(f)

        contracts_path = cls.root / "tests" / "scenarios" / "api_response_contracts.json"
        with contracts_path.open("r", encoding="utf-8") as f:
            cls.contracts = json.load(f)["contracts"]

    def setUp(self) -> None:
        self._tmp = TemporaryDirectory()
        tmp_path = Path(self._tmp.name)

        self._previous_services = api_main.SERVICES
        api_main.SERVICES = self._build_test_services(tmp_path)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), api_main.TenerRequestHandler)
        self.base_url = f"http://127.0.0.1:{self.server.server_port}"
        self.server_thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.server_thread.start()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.server_thread.join(timeout=3)
        api_main.SERVICES = self._previous_services
        self._tmp.cleanup()

    def _build_test_services(self, tmp_path: Path) -> Dict[str, Any]:
        db = Database(str(tmp_path / "api_e2e.sqlite3"))
        db.init_schema()

        instructions = AgentInstructions(path=str(self.root / "config" / "agent_instructions.json"))
        matching = MatchingEngine(str(self.root / "config" / "matching_rules.json"))
        provider = MockLinkedInProvider(str(self.root / "data" / "mock_linkedin_profiles.json"))

        sourcing = SourcingAgent(provider, instruction=instructions.get("sourcing"))
        verification = VerificationAgent(matching, instruction=instructions.get("verification"))
        outreach = OutreachAgent(
            str(self.root / "config" / "outreach_templates.json"),
            matching,
            instruction=instructions.get("outreach"),
        )
        faq = FAQAgent(
            str(self.root / "config" / "outreach_templates.json"),
            matching,
            instruction=instructions.get("faq"),
        )
        pre_resume = PreResumeCommunicationService(
            templates_path=str(self.root / "config" / "outreach_templates.json"),
            instruction=instructions.get("pre_resume"),
        )

        workflow = WorkflowService(
            db=db,
            sourcing_agent=sourcing,
            verification_agent=verification,
            outreach_agent=outreach,
            faq_agent=faq,
            pre_resume_service=pre_resume,
            contact_all_mode=False,
            require_resume_before_final_verify=True,
            stage_instructions={
                "sourcing": instructions.get("sourcing"),
                "enrich": instructions.get("enrich"),
                "verification": instructions.get("verification"),
                "add": instructions.get("add"),
                "outreach": instructions.get("outreach"),
                "faq": instructions.get("faq"),
                "pre_resume": instructions.get("pre_resume"),
            },
            forced_test_ids_path=str(self.root / "config" / "forced_test_linkedin_ids.txt"),
            forced_test_score=0.99,
        )

        services = {
            "db": db,
            "instructions": instructions,
            "matching_engine": matching,
            "pre_resume": pre_resume,
            "workflow": workflow,
        }
        api_main.apply_agent_instructions(services)
        return services

    def _request(self, method: str, path: str, payload: Optional[Dict[str, Any]] = None) -> Tuple[int, Dict[str, Any]]:
        headers = {}
        data = None
        if payload is not None:
            headers["Content-Type"] = "application/json"
            data = json.dumps(payload).encode("utf-8")

        req = request.Request(url=f"{self.base_url}{path}", method=method, data=data, headers=headers)
        try:
            with request.urlopen(req, timeout=20) as resp:
                status = int(resp.status)
                body_raw = resp.read().decode("utf-8")
        except error.HTTPError as exc:
            status = int(exc.code)
            body_raw = exc.read().decode("utf-8")

        body = json.loads(body_raw) if body_raw else {}
        return status, body

    def _assert_contract(self, contract_key: str, status: int, payload: Dict[str, Any]) -> None:
        contract = self.contracts[contract_key]
        self.assertEqual(status, int(contract["status"]), f"Unexpected status for {contract_key}")

        required = contract.get("required") or {}
        for field_path, expected_type in required.items():
            exists, value = self._extract_path(payload, field_path)
            self.assertTrue(exists, f"Missing required field '{field_path}' for {contract_key}")
            self.assertTrue(
                self._matches_type(value, str(expected_type)),
                f"Field '{field_path}' must be {expected_type}; got {type(value).__name__}",
            )

    @staticmethod
    def _extract_path(payload: Dict[str, Any], field_path: str) -> Tuple[bool, Any]:
        current: Any = payload
        for part in field_path.split("."):
            if isinstance(current, dict):
                if part not in current:
                    return False, None
                current = current[part]
                continue
            if isinstance(current, list):
                try:
                    idx = int(part)
                except ValueError:
                    return False, None
                if idx < 0 or idx >= len(current):
                    return False, None
                current = current[idx]
                continue
            return False, None
        return True, current

    @staticmethod
    def _matches_type(value: Any, expected: str) -> bool:
        for type_name in expected.split("|"):
            name = type_name.strip().lower()
            if name == "int":
                if isinstance(value, int) and not isinstance(value, bool):
                    return True
            elif name == "number":
                if (isinstance(value, int) and not isinstance(value, bool)) or isinstance(value, float):
                    return True
            elif name == "string":
                if isinstance(value, str):
                    return True
            elif name == "array":
                if isinstance(value, list):
                    return True
            elif name == "object":
                if isinstance(value, dict):
                    return True
            elif name == "boolean":
                if isinstance(value, bool):
                    return True
            elif name == "null":
                if value is None:
                    return True
        return False

    def _assert_in_range(self, value: int, min_value: int, max_value: int, label: str) -> None:
        self.assertGreaterEqual(value, min_value, f"{label} below expected range")
        self.assertLessEqual(value, max_value, f"{label} above expected range")

    def _run_sourcing_enrichment_verify_part(
        self,
        scenario: Dict[str, Any],
        *,
        use_explicit_enrich_step: bool,
    ) -> Dict[str, Any]:
        expected = scenario["expected"]

        status, created = self._request("POST", "/api/jobs", scenario["job"])
        self._assert_contract("POST /api/jobs", status, created)
        job_id = int(created["job_id"])

        status, source = self._request(
            "POST",
            "/api/steps/source",
            {"job_id": job_id, "limit": scenario["source_limit"]},
        )
        self._assert_contract("POST /api/steps/source", status, source)
        self._assert_in_range(
            int(source["total"]),
            int(expected["source_min"]),
            int(expected["source_max"]),
            "source total",
        )
        profiles = source["profiles"]
        self.assertGreater(len(profiles), 0, "source step returned no profiles")

        enrich = None
        verify_input_profiles = profiles
        if use_explicit_enrich_step:
            status, enrich = self._request(
                "POST",
                "/api/steps/enrich",
                {"job_id": job_id, "profiles": profiles},
            )
            self._assert_contract("POST /api/steps/enrich", status, enrich)
            self.assertEqual(int(enrich["total"]) + int(enrich["failed"]), len(profiles))
            verify_input_profiles = enrich["profiles"]

        status, verify = self._request(
            "POST",
            "/api/steps/verify",
            {"job_id": job_id, "profiles": verify_input_profiles},
        )
        self._assert_contract("POST /api/steps/verify", status, verify)
        self._assert_in_range(
            int(verify["verified"]),
            int(expected["verified_min"]),
            int(expected["verified_max"]),
            "verified total",
        )
        self._assert_in_range(
            int(verify["rejected"]),
            int(expected["rejected_min"]),
            int(expected["rejected_max"]),
            "rejected total",
        )
        items = verify["items"]
        self.assertGreater(len(items), 0, "verify step returned no items")
        for item in items:
            notes = item.get("notes") or {}
            text = str(notes.get("human_explanation") or "").strip()
            self.assertTrue(text, "human_explanation must be present for each candidate")
            self.assertIn("score", text.lower())

        verified_names = {
            str(item.get("profile", {}).get("full_name"))
            for item in items
            if item.get("status") == "verified"
        }
        self.assertTrue(
            verified_names.intersection(set(expected["expected_verified_names"])),
            "expected verified candidate is missing",
        )
        return {"job_id": job_id, "source": source, "enrich": enrich, "verify": verify}

    def _run_communication_part(self, scenario: Dict[str, Any], *, job_id: int, verify: Dict[str, Any]) -> Dict[str, Any]:
        verified_items = [x for x in verify["items"] if x.get("status") == "verified"]
        self.assertGreater(len(verified_items), 0, "communication part requires at least one verified candidate")

        status, added = self._request(
            "POST",
            "/api/steps/add",
            {"job_id": job_id, "verified_items": verified_items},
        )
        self._assert_contract("POST /api/steps/add", status, added)
        self.assertEqual(int(added["total"]), len(verified_items))

        candidate_ids = [int(x["candidate_id"]) for x in added["added"]]
        status, outreach = self._request(
            "POST",
            "/api/steps/outreach",
            {"job_id": job_id, "candidate_ids": candidate_ids},
        )
        self._assert_contract("POST /api/steps/outreach", status, outreach)
        self.assertEqual(int(outreach["total"]), len(candidate_ids))

        conversation_ids = [int(x) for x in outreach.get("conversation_ids") or []]
        self.assertGreater(len(conversation_ids), 0, "outreach did not create conversation")
        conversation_id = conversation_ids[0]

        status, inbound = self._request(
            "POST",
            f"/api/conversations/{conversation_id}/inbound",
            {"message": scenario.get("inbound_message") or "What is the salary range?"},
        )
        self._assert_contract("POST /api/conversations/{conversation_id}/inbound", status, inbound)
        self.assertTrue(str(inbound["reply"]).strip())
        self.assertIn(str(inbound.get("mode") or "faq"), {"faq", "pre_resume"})

        return {
            "added": added,
            "outreach": outreach,
            "conversation_id": conversation_id,
            "inbound_reply": inbound,
        }

    def test_api_workflow_scenarios_match_contract_and_expected_ranges(self) -> None:
        status, payload = self._request("GET", "/health")
        self._assert_contract("GET /health", status, payload)

        for scenario in self.scenarios["workflow_scenarios"]:
            with self.subTest(scenario=scenario["id"]):
                expected = scenario["expected"]

                status, created = self._request("POST", "/api/jobs", scenario["job"])
                self._assert_contract("POST /api/jobs", status, created)
                job_id = int(created["job_id"])

                status, job = self._request("GET", f"/api/jobs/{job_id}")
                self._assert_contract("GET /api/jobs/{job_id}", status, job)
                self.assertEqual(job["id"], job_id)
                self.assertEqual(job["title"], scenario["job"]["title"])

                status, source = self._request(
                    "POST",
                    "/api/steps/source",
                    {"job_id": job_id, "limit": scenario["source_limit"]},
                )
                self._assert_contract("POST /api/steps/source", status, source)
                self._assert_in_range(
                    int(source["total"]),
                    int(expected["source_min"]),
                    int(expected["source_max"]),
                    "source total",
                )
                profiles = source["profiles"]
                self.assertGreater(len(profiles), 0, "source step returned no profiles")

                status, verify = self._request(
                    "POST",
                    "/api/steps/verify",
                    {"job_id": job_id, "profiles": profiles},
                )
                self._assert_contract("POST /api/steps/verify", status, verify)
                self._assert_in_range(
                    int(verify["verified"]),
                    int(expected["verified_min"]),
                    int(expected["verified_max"]),
                    "verified total",
                )
                self._assert_in_range(
                    int(verify["rejected"]),
                    int(expected["rejected_min"]),
                    int(expected["rejected_max"]),
                    "rejected total",
                )

                items = verify["items"]
                self.assertGreater(len(items), 0, "verify step returned no items")
                for item in items:
                    notes = item.get("notes") or {}
                    text = str(notes.get("human_explanation") or "").strip()
                    self.assertTrue(text, "human_explanation must be present for each candidate")
                    self.assertIn("score", text.lower())

                verified_names = {
                    str(item.get("profile", {}).get("full_name"))
                    for item in items
                    if item.get("status") == "verified"
                }
                self.assertTrue(
                    verified_names.intersection(set(expected["expected_verified_names"])),
                    "expected verified candidate is missing",
                )

                eligible_items = [x for x in items if x.get("status") == "verified"]
                status, added = self._request(
                    "POST",
                    "/api/steps/add",
                    {"job_id": job_id, "verified_items": eligible_items},
                )
                self._assert_contract("POST /api/steps/add", status, added)
                self.assertEqual(int(added["total"]), len(eligible_items))

                candidate_ids = [int(x["candidate_id"]) for x in added["added"]]
                status, outreach = self._request(
                    "POST",
                    "/api/steps/outreach",
                    {"job_id": job_id, "candidate_ids": candidate_ids},
                )
                self._assert_contract("POST /api/steps/outreach", status, outreach)
                self.assertEqual(int(outreach["total"]), len(candidate_ids))

                status, candidates = self._request("GET", f"/api/jobs/{job_id}/candidates")
                self._assert_contract("GET /api/jobs/{job_id}/candidates", status, candidates)
                self.assertEqual(len(candidates["items"]), len(candidate_ids))

                status, progress = self._request("GET", f"/api/jobs/{job_id}/progress")
                self._assert_contract("GET /api/jobs/{job_id}/progress", status, progress)
                step_names = {str(x.get("step")) for x in progress["items"]}
                for expected_step in ("source", "verify", "add", "outreach"):
                    self.assertIn(expected_step, step_names)

                status, chats = self._request("GET", f"/api/chats/overview?job_id={job_id}&limit=100")
                self._assert_contract("GET /api/chats/overview", status, chats)

                status, logs = self._request("GET", "/api/logs?limit=100")
                self._assert_contract("GET /api/logs", status, logs)
                self.assertGreater(len(logs["items"]), 0)

                conversation_ids = [int(x) for x in outreach.get("conversation_ids") or []]
                if conversation_ids:
                    status, inbound = self._request(
                        "POST",
                        f"/api/conversations/{conversation_ids[0]}/inbound",
                        {"message": scenario.get("inbound_message") or "What is the salary range?"},
                    )
                    self._assert_contract("POST /api/conversations/{conversation_id}/inbound", status, inbound)
                    self.assertTrue(str(inbound["reply"]).strip())

    def test_e2e_part_sourcing_enrichment_profile(self) -> None:
        scenario = self.scenarios["workflow_scenarios"][0]
        result = self._run_sourcing_enrichment_verify_part(
            scenario=scenario,
            use_explicit_enrich_step=True,
        )
        self.assertIsNotNone(result["enrich"])
        self.assertEqual(int(result["verify"]["job_id"]), int(result["job_id"]))

    def test_e2e_part_communication(self) -> None:
        scenario = self.scenarios["workflow_scenarios"][1]
        setup_result = self._run_sourcing_enrichment_verify_part(
            scenario=scenario,
            use_explicit_enrich_step=False,
        )
        comm = self._run_communication_part(
            scenario=scenario,
            job_id=int(setup_result["job_id"]),
            verify=setup_result["verify"],
        )
        self.assertTrue(str(comm["inbound_reply"]["reply"]).strip())

    def test_e2e_part_interviewing(self) -> None:
        scenario = self.scenarios["pre_resume_api_scenario"]
        session_id = str(scenario["session_id"])

        status, started = self._request(
            "POST",
            "/api/pre-resume/sessions/start",
            {
                "session_id": session_id,
                "candidate_name": scenario["candidate_name"],
                "job_title": scenario["job_title"],
                "scope_summary": scenario["scope_summary"],
                "core_profile_summary": scenario["core_profile_summary"],
                "language": scenario["language"],
                "conversation_id": 501,
                "job_id": 601,
                "candidate_id": 701,
            },
        )
        self._assert_contract("POST /api/pre-resume/sessions/start", status, started)
        self.assertEqual(started["state"]["status"], "awaiting_reply")

        status, inbound = self._request(
            "POST",
            f"/api/pre-resume/sessions/{session_id}/inbound",
            {"message": "Here is my resume https://example.com/alex-resume.pdf"},
        )
        self._assert_contract("POST /api/pre-resume/sessions/{session_id}/inbound", status, inbound)
        self.assertEqual(inbound["intent"], "resume_shared")
        self.assertEqual(inbound["state"]["status"], "resume_received")
        self.assertGreaterEqual(len(inbound["resume_links"]), 1)

        status, fetched = self._request("GET", f"/api/pre-resume/sessions/{session_id}")
        self._assert_contract("GET /api/pre-resume/sessions/{session_id}", status, fetched)
        self.assertEqual(fetched["status"], "resume_received")

    def test_e2e_full_flow_composed_from_parts(self) -> None:
        scenario = self.scenarios["workflow_scenarios"][0]
        setup_result = self._run_sourcing_enrichment_verify_part(
            scenario=scenario,
            use_explicit_enrich_step=True,
        )
        job_id = int(setup_result["job_id"])
        verify = setup_result["verify"]
        comm = self._run_communication_part(scenario=scenario, job_id=job_id, verify=verify)
        conversation_id = int(comm["conversation_id"])

        status, resumed = self._request(
            "POST",
            f"/api/conversations/{conversation_id}/inbound",
            {"message": "Here is my resume https://example.com/full-flow-cv.pdf"},
        )
        self._assert_contract("POST /api/conversations/{conversation_id}/inbound", status, resumed)
        self.assertEqual(str(resumed.get("mode") or ""), "pre_resume")
        self.assertEqual(((resumed.get("state") or {}).get("status")), "resume_received")

        status, candidates = self._request("GET", f"/api/jobs/{job_id}/candidates")
        self._assert_contract("GET /api/jobs/{job_id}/candidates", status, candidates)
        statuses = {str(x.get("status")) for x in candidates["items"]}
        self.assertIn("resume_received", statuses)

        status, progress = self._request("GET", f"/api/jobs/{job_id}/progress")
        self._assert_contract("GET /api/jobs/{job_id}/progress", status, progress)
        step_names = {str(x.get("step")) for x in progress["items"]}
        for expected_step in ("source", "enrich", "verify", "add", "outreach"):
            self.assertIn(expected_step, step_names)

    def test_pre_resume_api_endpoints_match_contract(self) -> None:
        scenario = self.scenarios["pre_resume_api_scenario"]
        session_id = str(scenario["session_id"])

        status, started = self._request(
            "POST",
            "/api/pre-resume/sessions/start",
            {
                "session_id": session_id,
                "candidate_name": scenario["candidate_name"],
                "job_title": scenario["job_title"],
                "scope_summary": scenario["scope_summary"],
                "core_profile_summary": scenario["core_profile_summary"],
                "language": scenario["language"],
                "conversation_id": 101,
                "job_id": 201,
                "candidate_id": 301,
            },
        )
        self._assert_contract("POST /api/pre-resume/sessions/start", status, started)
        self.assertEqual(started["state"]["status"], "awaiting_reply")

        status, inbound = self._request(
            "POST",
            f"/api/pre-resume/sessions/{session_id}/inbound",
            {"message": scenario["inbound_message"]},
        )
        self._assert_contract("POST /api/pre-resume/sessions/{session_id}/inbound", status, inbound)
        self.assertIn(inbound["state"]["status"], {"engaged_no_resume", "resume_promised", "awaiting_reply"})

        status, followup = self._request("POST", f"/api/pre-resume/sessions/{session_id}/followup")
        self._assert_contract("POST /api/pre-resume/sessions/{session_id}/followup", status, followup)
        if followup["sent"]:
            self.assertTrue(str(followup.get("outbound") or "").strip())
        else:
            self.assertTrue(str(followup.get("reason") or "").strip())

        status, unreachable = self._request(
            "POST",
            f"/api/pre-resume/sessions/{session_id}/unreachable",
            {"error": scenario["unreachable_error"]},
        )
        self._assert_contract("POST /api/pre-resume/sessions/{session_id}/unreachable", status, unreachable)
        self.assertEqual(unreachable["state"]["status"], "unreachable")

        status, fetched = self._request("GET", f"/api/pre-resume/sessions/{session_id}")
        self._assert_contract("GET /api/pre-resume/sessions/{session_id}", status, fetched)
        self.assertEqual(fetched["status"], "unreachable")

        status, sessions = self._request("GET", "/api/pre-resume/sessions?limit=50")
        self._assert_contract("GET /api/pre-resume/sessions", status, sessions)

        status, events = self._request("GET", f"/api/pre-resume/events?session_id={session_id}&limit=100")
        self._assert_contract("GET /api/pre-resume/events", status, events)
        event_types = {str(x.get("event_type")) for x in events["items"]}
        self.assertIn("session_started", event_types)
        self.assertIn("inbound_processed", event_types)


if __name__ == "__main__":
    unittest.main()
