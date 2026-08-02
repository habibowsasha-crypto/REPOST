
from __future__ import annotations

import os
import unittest

os.environ.setdefault("API_ID", "1")
os.environ.setdefault("API_HASH", "h")
os.environ.setdefault("BOT_TOKEN", "1:t")
os.environ.setdefault("ADMIN_ID_LIST", "1")
os.environ.setdefault("DB_PATH", "/tmp/ai_first_dm_short.db")
os.environ.setdefault("BOT_SESSION_PATH", "/tmp/ai_first_dm_short_bot")
os.environ.setdefault("MEDIA_DIR", "/tmp/ai_first_dm_short_media")

from services.first_message_ai_generated import _build_prompt, _validate
from services.maxim_sales_funnel import PIRATE_VIP_LINK, build_ai_first_dm_short_plan


class AIFirstDMShortPathTests(unittest.TestCase):
    def test_prompt_is_engagement_focused(self) -> None:
        instructions, user = _build_prompt(
            source_chat_title="Crypto Chat",
            target_first_name="Ivan",
            recent=["привет как дела"],
        )
        self.assertTrue("обычный человек" in instructions or "ЗАХОТЕЛ" in instructions or "сво" in instructions.lower())
        self.assertTrue("чат" in user.lower() or "сво" in user.lower())
        self.assertIn("ссылок", instructions.lower().replace("ё", "е") or instructions)

    def test_first_reply_sends_link(self) -> None:
        plan = build_ai_first_dm_short_plan(
            stage="first_dm_sent",
            history=[("outgoing", "Привет"), ("incoming", "А?")],
            source_chat_title="Crypto",
            followup_count=0,
            max_followups=3,
        )
        self.assertEqual(plan.action, "concise_link")
        self.assertFalse(plan.close_after)
        joined = "\n".join(plan.messages)
        self.assertIn(PIRATE_VIP_LINK, joined)
        self.assertLessEqual(len(plan.messages), 2)

    def test_late_followup_closes(self) -> None:
        plan = build_ai_first_dm_short_plan(
            stage="link_sent_waiting_final",
            history=[("incoming", "ок")],
            source_chat_title=None,
            followup_count=2,
            max_followups=3,
        )
        self.assertTrue(plan.close_after)
        self.assertIn(PIRATE_VIP_LINK, plan.messages[0])

    def test_first_dm_validation_rejects_link(self) -> None:
        ok, reason = _validate("смотри https://t.me/x", [])
        self.assertFalse(ok)


if __name__ == "__main__":
    unittest.main()
