from __future__ import annotations

import hashlib
import re
import unittest
from pathlib import Path


class FirstDmIntegrityTests(unittest.TestCase):
    def test_first_message_module_matches_approved_baseline(self) -> None:
        path = Path(__file__).resolve().parents[1] / "services" / "first_message.py"
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        self.assertEqual(
            digest,
            "a34f2613c338b1f89284335d2462cc1ed1b1e43b3063dcdf4a511a3b7f0f32aa",
        )

    def test_first_dm_runtime_uses_safe_queue_and_original_selector(self) -> None:
        path = Path(__file__).resolve().parents[1] / "handlers" / "dm" / "dm_handlers.py"
        source = path.read_text(encoding="utf-8")
        self.assertIn("enqueue_pending(", source)
        self.assertIn("get_due_pending(account_user_id)", source)
        self.assertIn("claim_pending(row_id)", source)
        self.assertRegex(
            source,
            re.compile(r"outgoing_text\s*=\s*\(\s*choose_first_dm_text", re.S),
        )
        self.assertIn("if is_opted_out(target_id):", source)
        self.assertIn("if is_completed_contact(account_user_id, target_id):", source)
        self.assertIn("if is_contact_in_progress(account_user_id, target_id):", source)
        self.assertIn("first_claim = try_claim_first_dm(", source)
        self.assertIn("claim_token=first_claim", source)

    def test_contact_analytics_symbols_are_imported_before_runtime_use(self) -> None:
        path = Path(__file__).resolve().parents[1] / "handlers" / "dm" / "dm_handlers.py"
        source = path.read_text(encoding="utf-8")
        for symbol in (
            "is_completed_contact",
            "is_contact_in_progress",
            "record_first_dm as record_contact_first_dm",
            "record_source_seen",
            "try_claim_first_dm",
            "release_first_dm_claim",
        ):
            self.assertIn(symbol, source)


if __name__ == "__main__":
    unittest.main()
