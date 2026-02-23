import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from tener_ai.instructions import AgentInstructions


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


if __name__ == "__main__":
    unittest.main()
