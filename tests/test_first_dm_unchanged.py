from __future__ import annotations

import hashlib
import unittest
from pathlib import Path


class FirstDmIntegrityTests(unittest.TestCase):
    def test_first_message_module_matches_v1_0_13(self) -> None:
        path = Path(__file__).resolve().parents[1] / "services" / "first_message.py"
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        self.assertEqual(
            digest,
            "a34f2613c338b1f89284335d2462cc1ed1b1e43b3063dcdf4a511a3b7f0f32aa",
        )

    def test_first_dm_runtime_keeps_two_item_queue_and_original_selector(self) -> None:
        path = Path(__file__).resolve().parents[1] / "handlers" / "dm" / "dm_handlers.py"
        source = path.read_text(encoding="utf-8")
        self.assertIn("queue.append((target_id, sender))", source)
        self.assertIn("target_id, sender = queue.popleft()", source)
        self.assertIn("outgoing_text = choose_first_dm_text", source)
        self.assertIn("if is_opted_out(target_id):", source)


if __name__ == "__main__":
    unittest.main()
