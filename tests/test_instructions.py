import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from tener_ai.instructions import AgentEvaluationPlaybook, AgentInstructions


class AgentInstructionsTests(unittest.TestCase):
    def test_loads_agents_and_returns_values(self) -> None:
        with TemporaryDirectory() as td:
            path = Path(td) / "instructions.json"
            path.write_text(
                json.dumps(
                    {
                        "version": "x1",
                        "agents": {
                            "sourcing": "source instruction",
                            "verification": "verify instruction",
                        },
                    }
                ),
                encoding="utf-8",
            )

            book = AgentInstructions(str(path))
            self.assertEqual(book.get("sourcing"), "source instruction")
            self.assertEqual(book.get("verification"), "verify instruction")
            self.assertEqual(book.get("unknown", "fallback"), "fallback")
            self.assertEqual(book.to_dict()["version"], "x1")

    def test_reload_reads_updated_file(self) -> None:
        with TemporaryDirectory() as td:
            path = Path(td) / "instructions.json"
            path.write_text(json.dumps({"version": "v1", "agents": {"faq": "faq-1"}}), encoding="utf-8")
            book = AgentInstructions(str(path))
            self.assertEqual(book.get("faq"), "faq-1")

            path.write_text(json.dumps({"version": "v2", "agents": {"faq": "faq-2"}}), encoding="utf-8")
            book.reload()
            self.assertEqual(book.get("faq"), "faq-2")
            self.assertEqual(book.to_dict()["version"], "v2")

    def test_loads_instruction_from_relative_file_reference(self) -> None:
        with TemporaryDirectory() as td:
            root = Path(td)
            nested = root / "instructions"
            nested.mkdir(parents=True, exist_ok=True)
            guide = nested / "sourcing.md"
            guide.write_text("long sourcing instruction", encoding="utf-8")

            path = root / "instructions.json"
            path.write_text(
                json.dumps(
                    {
                        "version": "v-file",
                        "agents": {
                            "sourcing": {"file": "instructions/sourcing.md"},
                        },
                    }
                ),
                encoding="utf-8",
            )

            book = AgentInstructions(str(path))
            self.assertEqual(book.get("sourcing"), "long sourcing instruction")


class AgentEvaluationPlaybookTests(unittest.TestCase):
    def test_loads_agent_names_and_stage_instructions(self) -> None:
        with TemporaryDirectory() as td:
            path = Path(td) / "evaluation.json"
            path.write_text(
                json.dumps(
                    {
                        "version": "eval-v1",
                        "agents": {
                            "communication": {
                                "name": "Candidate Communication Agent",
                                "stages": {
                                    "outreach": "outreach eval instruction",
                                    "*": "fallback instruction",
                                },
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )

            book = AgentEvaluationPlaybook(str(path))
            self.assertEqual(book.get_agent_name("communication"), "Candidate Communication Agent")
            self.assertEqual(book.get_instruction("communication", "outreach"), "outreach eval instruction")
            self.assertEqual(book.get_instruction("communication", "dialogue"), "fallback instruction")
            self.assertEqual(book.to_dict()["version"], "eval-v1")

    def test_supports_file_reference_in_stage_instruction(self) -> None:
        with TemporaryDirectory() as td:
            root = Path(td)
            nested = root / "eval_docs"
            nested.mkdir(parents=True, exist_ok=True)
            guide = nested / "vetting.md"
            guide.write_text("score candidate for vetting", encoding="utf-8")

            path = root / "evaluation.json"
            path.write_text(
                json.dumps(
                    {
                        "version": "eval-v2",
                        "agents": {
                            "sourcing_vetting": {
                                "name": "Talent Scout Agent",
                                "stages": {
                                    "vetting": {"file": "eval_docs/vetting.md"},
                                },
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )

            book = AgentEvaluationPlaybook(str(path))
            self.assertEqual(book.get_instruction("sourcing_vetting", "vetting"), "score candidate for vetting")


if __name__ == "__main__":
    unittest.main()
