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
        )
        self.assertEqual(out["state"]["status"], "awaiting_reply")
        self.assertIn("CV", out["outbound"])
        self.assertTrue(out["state"]["next_followup_at"])

    def test_start_and_salary_question_keeps_resume_cta(self) -> None:
        service = PreResumeCommunicationService()
        self._start_default_session(service)

        out = service.handle_inbound("s1", "What is the salary range?")
        self.assertEqual(out["intent"], "salary")
        self.assertEqual(out["state"]["status"], "engaged_no_resume")
        self.assertIn("Please share your CV", out["outbound"])

    def test_resume_link_marks_session_as_resume_received(self) -> None:
        service = PreResumeCommunicationService()
        self._start_default_session(service)

        out = service.handle_inbound("s1", "Here is my resume https://example.com/alex_resume.pdf")
        self.assertEqual(out["intent"], "resume_shared")
        self.assertEqual(out["state"]["status"], "resume_received")
        self.assertIsNone(out["state"]["next_followup_at"])
        self.assertTrue(out["resume_links"])
        self.assertIn("received", out["outbound"].lower())

    def test_attachment_link_without_resume_keyword_is_detected(self) -> None:
        service = PreResumeCommunicationService()
        self._start_default_session(service)

        out = service.handle_inbound("s1", "attached file https://files.example.com/download/abc123")
        self.assertEqual(out["intent"], "resume_shared")
        self.assertEqual(out["state"]["status"], "resume_received")
        self.assertTrue(out["resume_links"])

    def test_will_send_later_and_not_interested_transitions(self) -> None:
        service = PreResumeCommunicationService()
        self._start_default_session(service)

        promised = service.handle_inbound("s1", "I will send CV tomorrow")
        self.assertEqual(promised["intent"], "will_send_later")
        self.assertEqual(promised["state"]["status"], "resume_promised")
        self.assertIn("wait", promised["outbound"].lower())

        stop = service.handle_inbound("s1", "No thanks, not interested")
        self.assertEqual(stop["intent"], "not_interested")
        self.assertEqual(stop["state"]["status"], "not_interested")
        self.assertIsNone(stop["state"]["next_followup_at"])

    def test_multilingual_intents_are_classified(self) -> None:
        service = PreResumeCommunicationService()
        self._start_default_session(service)

        ru_later = service.handle_inbound("s1", "Пришлю резюме позже, завтра")
        self.assertEqual(ru_later["intent"], "will_send_later")
        self.assertEqual(ru_later["state"]["status"], "resume_promised")

        service = PreResumeCommunicationService()
        self._start_default_session(service)
        es_resume = service.handle_inbound("s1", "Aqui esta mi CV")
        self.assertEqual(es_resume["intent"], "resume_shared")
        self.assertEqual(es_resume["state"]["status"], "resume_received")

    def test_followup_sequence_and_stalled_state(self) -> None:
        service = PreResumeCommunicationService(max_followups=3)
        self._start_default_session(service)

        f1 = service.build_followup("s1")
        self.assertTrue(f1["sent"])
        self.assertEqual(f1["followup_number"], 1)
        self.assertEqual(f1["state"]["status"], "awaiting_reply")

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

    def test_contextual_yes_is_opt_in_after_pre_vetting_question(self) -> None:
        service = PreResumeCommunicationService()
        self._start_default_session(service)
        state = service.get_session("s1")
        assert state is not None
        state["awaiting_pre_vetting_opt_in"] = True
        service.seed_session(state)

        out = service.handle_inbound("s1", "yes, lets do it")
        self.assertEqual(out["intent"], "pre_vetting_opt_in")
        self.assertEqual(out["state"]["status"], "interview_opt_in")
        self.assertFalse(bool(out["state"].get("awaiting_pre_vetting_opt_in")))

    def test_sounds_interesting_triggers_pre_vetting_opt_in(self) -> None:
        service = PreResumeCommunicationService()
        self._start_default_session(service)

        out = service.handle_inbound("s1", "Sounds interesting")
        self.assertEqual(out["intent"], "pre_vetting_opt_in")
        self.assertEqual(out["state"]["status"], "interview_opt_in")

    def test_language_template_fallback(self) -> None:
        with TemporaryDirectory() as td:
            templates = {
                "default_language": "en",
                "intro": {"en": "Hello {name}", "es": "Hola {name}"},
                "resume_cta": {"en": "Send CV"},
                "resume_ack": {"en": "CV received"},
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
