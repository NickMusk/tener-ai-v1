from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tener_ai.outreach_policy import LinkedInOutreachPolicy


class LinkedInOutreachPolicyTests(unittest.TestCase):
    def test_missing_file_falls_back_to_default(self) -> None:
        policy = LinkedInOutreachPolicy(path="/tmp/does-not-exist-policy.json")
        payload = policy.to_dict()
        self.assertEqual(payload["provider"], "unipile")
        self.assertEqual(payload["connect_invites"]["weekly_cap_per_account"], 100)
        self.assertEqual(payload["outbound_messages"]["daily_new_threads_per_account"]["min"], 10)
        self.assertEqual(payload["outbound_messages"]["daily_new_threads_per_account"]["max"], 15)

    def test_normalizes_invalid_values(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            file_path = Path(tmp) / "policy.json"
            file_path.write_text(
                json.dumps(
                    {
                        "provider": "UNIPILE",
                        "connect_invites": {"weekly_cap_per_account": -5},
                        "outbound_messages": {
                            "daily_new_threads_per_account": {"min": 19, "max": 9},
                            "replies_unlimited": True,
                        },
                        "multi_account_profile_pool": {
                            "enabled": True,
                            "target_accounts": {"min": 130, "max": 70},
                        },
                        "quiet_hours": {"enabled": False},
                    }
                ),
                encoding="utf-8",
            )
            policy = LinkedInOutreachPolicy(path=str(file_path))
            payload = policy.to_dict()
            self.assertEqual(payload["provider"], "unipile")
            self.assertEqual(payload["connect_invites"]["weekly_cap_per_account"], 1)
            self.assertEqual(payload["outbound_messages"]["daily_new_threads_per_account"]["min"], 9)
            self.assertEqual(payload["outbound_messages"]["daily_new_threads_per_account"]["max"], 19)
            self.assertEqual(payload["multi_account_profile_pool"]["target_accounts"]["min"], 70)
            self.assertEqual(payload["multi_account_profile_pool"]["target_accounts"]["max"], 130)
            self.assertFalse(payload["quiet_hours"]["enabled"])


if __name__ == "__main__":
    unittest.main()
