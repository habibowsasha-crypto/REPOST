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

    def test_dm_runtime_matches_v1_0_13(self) -> None:
        path = Path(__file__).resolve().parents[1] / "handlers" / "dm" / "dm_handlers.py"
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        self.assertEqual(
            digest,
            "e3314ad1aeab97b399a11a3ce4aeb1d42b73e29dd73c265e390a3540613dffe6",
        )


if __name__ == "__main__":
    unittest.main()
