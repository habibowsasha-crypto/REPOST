# Channel DM Bot v1.0.48

Telegram user-account DM bot: monitors selected groups, builds one shared lead queue, sends First DM, continues active dialogs, handles refusals, sends only the administrator's exact channel link, and stores durable state in SQLite.

## Changes in v1.0.48

### New admin interface

- Unified visual style for the main menu and all sections.
- Main menu now shows:
  - First DM running/paused state;
  - pending and currently sending leads;
  - First DM sent today and all time;
  - colored status for connected accounts;
  - active dialogs and dialogs completed today;
  - channel, AI and monitor status.
- Added a dedicated `Диалоги` section. Dialog continuation is visible there and does not create push-notification spam.
- Account, queue, audience, opt-out, settings and retention screens use the same compact layout.

### Notification policy

Routine admin notifications are sent only for a successful First DM. Important system events such as PeerFlood, SpamBot restrictions and recovery still notify the admin. The bot does not send admin notifications for incoming replies, AI responses, links, follow-ups or ordinary dialog completion.

### Telegram cleanup after 30 days

- Each confirmed First DM stores its Telegram message ID and cleanup date.
- After 30 days the bot deletes private-chat messages starting with that First DM and all newer messages in the same dialog.
- Deletion uses `revoke=True`, so the selected messages are removed for both sides.
- Messages older than the tracked First DM are not touched.
- Temporary failures are retried with a durable retry schedule.

### Local text retention for 180 days

- Dialog texts remain in SQLite for up to 180 days from First DM.
- After that the bot clears message text from dialog history and delivery outboxes.
- IDs, account ownership, dates, status, statistics and opt-out information remain.

### Crash-safe automatic link and follow-up

- Automatic link and silence follow-up are persisted before Telegram send.
- If a crash or ambiguous network error happens, the bot checks the real Telegram history before deciding whether to retry.
- A message found in Telegram is committed without duplication.
- A message not found is safely rescheduled.

### Durable counters

- Added a `first_dm_events` journal.
- `Отправлено всего` survives restarts and account deletion.
- `Отправлено сегодня` is calculated in administrator time (UTC+3) from confirmed First-DM events.

## Confirmed project rules preserved

- Main-menu pause stops only new First DM from the queue.
- Existing dialogs continue during the pause.
- A disabled account finishes its own active dialogs and receives no new leads.
- Dialogs are not transferred between accounts.
- Only First DM counts toward First-DM limits and pacing.
- Explicit import/requeue may intentionally open a new First-DM attempt for a previously contacted user.
- Aggressive/blocking users are closed for all accounts.
- No new First-DM limits, filters, quotas, pauses or throttling were added.

## Railway variables

```text
API_ID=
API_HASH=
BOT_TOKEN=
ADMIN_ID_LIST=123456789
DB_PATH=/data/bot.db
BOT_SESSION_PATH=/data/bot
MEDIA_DIR=/data/media
CHANNEL_LINK=https://t.me/+...
CHANNEL_PITCH=Бесплатный канал: посты из закрытых VIP, платить не нужно
OPENAI_API_KEY=
AI_MODEL=gpt-4o-mini
AI_REQUEST_TIMEOUT_SECONDS=20
AI_DM_ENABLED=true
TELEGRAM_DIALOG_DELETE_DAYS=30
LOCAL_DIALOG_TEXT_RETENTION_DAYS=180
```

## Retention behavior

| Data | Retention |
|---|---:|
| Telegram messages from First DM onward | 30 days |
| Message texts in SQLite | 180 days |
| IDs, statuses, statistics and opt-out | retained |

The Telegram user-session must still exist when cleanup becomes due. The account deletion confirmation screen warns when dialogs still depend on that session for future Telegram cleanup.

## Quick live test

1. Deploy v1.0.48 over the existing volume/database.
2. Confirm the new dashboard and all-time First-DM counter.
3. Send a test First DM and confirm only one routine admin notification.
4. Continue the dialog and confirm no reply/link/follow-up push notifications appear.
5. Test direct refusal and opt-out.
6. Restart during a prepared automatic link/follow-up and verify no duplicate.
7. Temporarily set short retention values in a test environment only and confirm Telegram deletion starts at the tracked First DM, not before it.
8. Confirm local text purge keeps metadata and statistics.

## Tests

```bash
pip install -r requirements.txt
pytest -q
```

The release archive includes the full project plan, patch report and test report under `Аудиты/Мысли/`. Real Telegram delivery and revoke behavior must still be checked by the owner on test accounts.

## Security note

Do not commit `.env`, SQLite databases or Telegram session files. Restrict access to the Railway volume. Only IDs listed in `ADMIN_ID_LIST` can control the bot.
