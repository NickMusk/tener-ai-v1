import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from tener_ai.pre_resume_service import PreResumeCommunicationService


class PreResumeServiceTests(unittest.TestCase):
    def _start_default_session(self, service: PreResumeCommunicationService) -> None:
        out = service.start_session(
            session_id="s1",
            candidate_name="Alex",
            job_title="Senior Backend Engineer",
            scope_summary="python, aws, distributed systems",
            core_profile_summary="python, aws, distributed systems",
            language="en",
            job_location="Berlin",
            salary_min=120000,
            salary_max=150000,
            salary_currency="USD",
            work_authorization_required=True,
        )
        self.assertEqual(out["state"]["status"], "awaiting_reply")
        self.assertEqual(out["state"]["prescreen_status"], "incomplete")
        self.assertIn("written qualifying questions", out["outbound"])
        self.assertIn("async interview", out["outbound"])
        self.assertTrue(out["state"]["next_followup_at"])

    def test_salary_answer_can_arrive_before_other_answers(self) -> None:
        service = PreResumeCommunicationService()
        self._start_default_session(service)

        out = service.handle_inbound("s1", "I'm targeting 145k USD.")
        self.assertEqual(out["state"]["prescreen_status"], "incomplete")
        self.assertEqual(out["state"]["salary_expectation_min"], 145000.0)
        self.assertEqual(out["state"]["salary_expectation_max"], 145000.0)
        self.assertEqual(out["state"]["salary_expectation_currency"], "USD")
        self.assertIn("What hands-on experience", out["outbound"])
        self.assertNotIn("Please share your CV", out["outbound"])

    def test_resume_link_marks_session_as_cv_received_pending_answers(self) -> None:
        service = PreResumeCommunicationService()
        self._start_default_session(service)

        out = service.handle_inbound("s1", "Here is my resume https://example.com/alex_resume.pdf")
        self.assertEqual(out["intent"], "resume_shared")
        self.assertEqual(out["state"]["status"], "cv_received_pending_answers")
        self.assertEqual(out["state"]["prescreen_status"], "cv_received_pending_answers")
        self.assertTrue(bool(out["state"]["cv_received"]))
        self.assertTrue(out["resume_links"])
        self.assertIn("CV received", out["outbound"])

    def test_attachment_link_without_resume_keyword_is_detected(self) -> None:
        service = PreResumeCommunicationService()
        self._start_default_session(service)

        out = service.handle_inbound("s1", "attached file https://files.example.com/download/abc123")
        self.assertEqual(out["intent"], "resume_shared")
        self.assertEqual(out["state"]["prescreen_status"], "cv_received_pending_answers")
        self.assertTrue(out["resume_links"])

    def test_will_send_later_and_not_interested_transitions(self) -> None:
        service = PreResumeCommunicationService()
        self._start_default_session(service)

        promised = service.handle_inbound("s1", "I will send CV tomorrow")
        self.assertEqual(promised["intent"], "will_send_later")
        self.assertEqual(promised["state"]["status"], "resume_promised")
        self.assertIn("send your CV anytime", promised["outbound"])

        stop = service.handle_inbound("s1", "No thanks, not interested")
        self.assertEqual(stop["intent"], "not_interested")
        self.assertEqual(stop["state"]["status"], "not_interested")
        self.assertIsNone(stop["state"]["next_followup_at"])

    def test_multilingual_resume_signal_is_classified(self) -> None:
        service = PreResumeCommunicationService()
        self._start_default_session(service)

        es_resume = service.handle_inbound("s1", "Aqui esta mi CV")
        self.assertEqual(es_resume["intent"], "resume_shared")
        self.assertEqual(es_resume["state"]["prescreen_status"], "cv_received_pending_answers")
        self.assertTrue(bool(es_resume["state"]["cv_received"]))

    def test_inbound_message_switches_active_session_language(self) -> None:
        service = PreResumeCommunicationService()
        self._start_default_session(service)

        out = service.handle_inbound("s1", "Aqui esta mi CV")
        self.assertEqual(out["state"]["language"], "es")
        self.assertIn("Gracias", out["outbound"])

    def test_prescreen_stays_incomplete_until_required_answers_exist(self) -> None:
        service = PreResumeCommunicationService()
        self._start_default_session(service)

        first = service.handle_inbound("s1", "I have worked with Python and AWS for 6 years.")
        self.assertEqual(first["state"]["prescreen_status"], "incomplete")
        self.assertIsNone(first["state"]["salary_expectation_min"])

        second = service.handle_inbound("s1", "I'm targeting 140k USD and I'm based in Berlin with full work authorization.")
        self.assertEqual(second["state"]["status"], "ready_for_cv")
        self.assertEqual(second["state"]["prescreen_status"], "ready_for_cv")
        self.assertIn("Please share your CV", second["outbound"])

    def test_cv_can_arrive_before_ask_and_does_not_skip_prescreen(self) -> None:
        service = PreResumeCommunicationService()
        self._start_default_session(service)

        early_cv = service.handle_inbound("s1", "Attached CV https://example.com/alex.pdf")
        self.assertEqual(early_cv["state"]["prescreen_status"], "cv_received_pending_answers")

        answered = service.handle_inbound(
            "s1",
            "I have 7 years of Python and AWS experience, I am targeting 145k USD, I am based in Berlin, and I have full work authorization.",
        )
        self.assertEqual(answered["state"]["status"], "ready_for_interview")
        self.assertEqual(answered["state"]["prescreen_status"], "ready_for_interview")
        self.assertIn("async interview", answered["outbound"])

    def test_followup_sequence_and_stalled_state(self) -> None:
        service = PreResumeCommunicationService(max_followups=3)
        self._start_default_session(service)

        f1 = service.build_followup("s1")
        self.assertTrue(f1["sent"])
        self.assertEqual(f1["followup_number"], 1)

        f2 = service.build_followup("s1")
        self.assertTrue(f2["sent"])
        self.assertEqual(f2["followup_number"], 2)

        f3 = service.build_followup("s1")
        self.assertTrue(f3["sent"])
        self.assertEqual(f3["followup_number"], 3)

        f4 = service.build_followup("s1")
        self.assertFalse(f4["sent"])
        self.assertEqual(f4["reason"], "max_followups_reached")
        self.assertEqual(f4["state"]["status"], "stalled")

    def test_terminal_states_skip_followups(self) -> None:
        service = PreResumeCommunicationService()
        self._start_default_session(service)
        service.mark_unreachable("s1", "no_connection_with_recipient")

        out = service.build_followup("s1")
        self.assertFalse(out["sent"])
        self.assertEqual(out["reason"], "terminal_status")
        self.assertEqual(out["state"]["status"], "unreachable")

    def test_process_explanation_contains_new_process(self) -> None:
        service = PreResumeCommunicationService()
        out = service.start_session(
            session_id="s1",
            candidate_name="Alex",
            job_title="Senior Backend Engineer",
            scope_summary="python, aws, distributed systems",
            core_profile_summary="python, aws, distributed systems",
            language="en",
        )
        text = str(out["outbound"] or "")
        self.assertIn("written qualifying questions", text)
        self.assertIn("CV", text)
        self.assertIn("async interview", text)

    def test_language_template_fallback(self) -> None:
        with TemporaryDirectory() as td:
            templates = {
                "default_language": "en",
                "intro": {"en": "Hello {name}", "es": "Hola {name}"},
                "cv_request": {"en": "Send CV"},
                "screening_call_ready": {"en": "Ready for interview"},
                "not_interested_ack": {"en": "Ok"},
                "resume_promised_ack": {"en": "Noted"},
                "followups": {"1": {"en": "F1"}, "2": {"en": "F2"}, "3": {"en": "F3"}},
                "intent_answers": {"default": {"en": "Answer"}},
            }
            path = Path(td) / "templates.json"
            path.write_text(json.dumps(templates), encoding="utf-8")

            service = PreResumeCommunicationService(templates_path=str(path))
            start_es = service.start_session(
                session_id="es-session",
                candidate_name="Luis",
                job_title="Role",
                scope_summary="scope",
                language="es",
            )
            self.assertIn("Hola", start_es["outbound"])

            start_fallback = service.start_session(
                session_id="fr-session",
                candidate_name="Jean",
                job_title="Role",
                scope_summary="scope",
                language="fr",
            )
            self.assertIn("Hello", start_fallback["outbound"])


if __name__ == "__main__":
    unittest.main()
