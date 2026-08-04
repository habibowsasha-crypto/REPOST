# Channel DM Bot v1.0.64

Telegram user-account DM bot: monitors selected groups, builds one shared lead queue, sends First DM, continues active dialogs, handles refusals, sends only the administrator's exact channel link, and stores durable state in SQLite.


## Changes in v1.0.64 - simple First DM and varied four-step funnel

- First DM is now only a simple everyday opener such as `Привет, можно один вопрос?`, `Привет, ты занят?` or `Не отвлеку?`.
- First DM no longer asks about markets, positions, signals, formats, timeframes or trading.
- After any allowed reply, the bot immediately sends the complete promo with the exact administrator `CHANNEL_LINK`. No extra market question is inserted.
- After the promo, the bot sends a varied smoothing apology.
- After the apology, the bot automatically sends a separate instruction explaining how to close the `Заблокировать / Добавить` panel, press the link again and copy it manually if Telegram still blocks the transition.
- The standard path therefore uses four outgoing bot messages: First DM, promo, apology and link-opening instruction. The global maximum remains five outgoing messages, leaving one slot for a user-initiated follow-up when available.
- First DM, promo, apology and link-opening instruction each have a separate global anti-repeat window of the last 20 sent texts across all accounts.
- Exact or overly similar wording is rejected inside its own 20-message window. One initial AI generation and at most two repeat generations are allowed.
- Local fallback pools contain more than 20 variants and still forbid exact repetition inside the active window.
- A user reply between scheduled messages does not erase the remaining automatic sequence unless opt-out, a terminal refusal or the five-message maximum closes the dialog.
- Crash-safe outbox delivery and Telegram-history reconciliation now include the new link-help message.
- Existing v1.0.63 databases migrate without data loss. Historical phrase rows for all four new kinds are trimmed safely to 20. Existing completed or old apology rows are not retroactively messaged.
- Account interval 2-7 minutes, global spacing 90-180 seconds, daily limit 125, AI reply delay 20-60 seconds, automatic-message delay 60-60 seconds, PeerFlood 60-90 seconds and the 5-in-10 extra cooldown rule are unchanged.

This section supersedes older README statements about a promo-only 30-message uniqueness window and the older promo-to-apology-only automatic path.


## Changes in v1.0.63 - deep audit fixes

- Legacy prepared First DM recovery is finite. Entity-unavailable and PeerIdInvalid recovery is checked at most three times, then the prepared record is rolled back and the shared queue can try another available account.
- The v1.0.61 compatibility migration preserves administrator-edited PeerFlood ranges and removes only untouched automatic 600-600 values. Stale cooldown and next-send artifacts from that rejected release are capped safely.
- Ordinary PeerFlood pauses only the PeerFlood cooldown. It no longer creates a hidden extra 2-7 minute account interval. The fifth PeerFlood inside the rolling ten-minute window still adds the separately configurable extra cooldown.
- Deleting an account also deletes its stored PeerFlood hit history, so a newly added session cannot inherit a previous account lifecycle.
- The release gate reports unavailable ruff or mypy checks as SKIP, never PASS. Optional --require-dev-tools mode makes missing tools a release failure.
- New-install defaults, .env.example, runtime fallback values and release documents now agree with the approved production settings: account 2-7 minutes, global 90-180 seconds, daily limit 125, AI reply 20-60 seconds, apology 60-60 seconds and ordinary PeerFlood 60-90 seconds.
- Release text and source files use only the normal ASCII short hyphen.
- No approved queue priority, source-account priority, global opt-out, five-message limit, dialog ownership, uniqueness window or First DM selection order was changed.


## Changes in v1.0.62 - exact PeerFlood 5-in-10 escalation

- Built strictly from the confirmed v1.0.60 release. The rejected v1.0.61 behavior is not used.
- Ordinary PeerFlood behavior remains unchanged and continues to use the administrator's current random range.
- A separate rolling ten-minute event window is maintained for each Telegram account.
- The first four PeerFlood events inside that window do not change pacing and use only the ordinary configured pause.
- On the fifth PeerFlood inside the preceding ten minutes, a separate extra cooldown is added to the ordinary pause.
- The extra cooldown defaults to 600 seconds and is independently editable by the administrator.
- After the fifth event triggers the extra pause, that five-event group is consumed and the next PeerFlood begins a new group.
- No account interval, global spacing, daily quota, AI delay, apology delay, filter, source-account priority or account order changed.
- Migration restores any legacy temporary 20-40 minute per-account interval backup created by the old rapid-PeerFlood rule, without changing current runtime pacing settings.
- Added 7 dedicated v1.0.62 regression tests.


## Changes in v1.0.60 - dashboard First DM countdowns

- The main dashboard now shows the real persisted First DM wait under every participating account.
- An account waiting for its own interval shows `Следующий First DM через 8м`.
- An account ready by its own interval but blocked by global spacing shows `Готов · общая пауза ещё 40с`.
- A fully available account shows `Готов к First DM`.
- An account at its daily quota shows `Дневной лимит исчерпан`.
- PeerFlood, FloodWait, reauthorization and disabled-account states keep priority over the normal countdown.
- The countdown is display-only. No limit, interval, global spacing, pause, filter, quota or account-selection rule changed.
- Added 6 dedicated v1.0.60 regression tests.


## Changes in v1.0.59 - username-first shared queue delivery

- Every eligible account now attempts Telegram entity resolution by the lead's public username first.
- A username result is accepted only when its numeric Telegram user id matches the queued target id, preventing delivery to a changed or recycled username.
- If username resolution is unavailable, the account falls back to its own cached entity by numeric user id.
- The source account's stored access_hash is used only as the final source-only fallback and is never reused by another account.
- A non-source account can therefore take a lead from the shared queue and send First DM when that account can resolve the user's public username.
- AI First DM generation still starts only after a valid entity is resolved.
- Source-account priority, fallback account order, limits, intervals, global spacing, pause behavior, dialog ownership, global opt-out, five-message maximum and last-30 promo uniqueness remain unchanged.
- Added 7 dedicated v1.0.59 regression tests.


## Changes in v1.0.58 - safe First DM delivery and exact apology timing

- Production Loguru sinks disable backtrace and local-variable diagnostics, preventing Telegram session strings and account dictionaries from appearing in exception output.
- Telegram `ALLOW_PAYMENT_REQUIRED` errors are terminal `paid_message_required` failures. Prepared state is rolled back, provisional contact/dialog state is removed, and no history recovery or account fallback is scheduled.
- `PeerIdInvalidError` raised by the actual First DM send is treated as a definite failed send. Prepared state is rolled back and the same dispatch round continues through the next already eligible account.
- Telegram entity resolution now happens before First DM AI generation. One generated text is reused across account fallbacks in the same lead round.
- Legacy persisted apology ranges are migrated and clamped to 5-60 seconds. The admin editor rejects values outside this range.
- A dedicated lightweight one-second scheduler processes due apologies separately from the historical 20-second recovery and retention loop. Existing dialogs continue while First DM is paused.
- First DM style families and local fallbacks were expanded to reduce repeated openings without weakening validation or recent-similarity checks. AI attempts remain bounded at two before local fallback.
- All 125 existing tests remain, with 16 new v1.0.58 regression tests.
- No First DM limit, interval, global spacing, pause rule, source-account priority, account selection order, global opt-out rule, five-message limit, or last-30 promo uniqueness window was changed.


## Changes in v1.0.57 - account authorization recovery

- Telegram session loss is now stored as a durable `reauth_required` account state.
- The bot detects `session not authorized` and known authorization-loss errors during startup, periodic health checks, First DM and dialog sends.
- A lost account is immediately removed from active monitoring and First DM account selection.
- Existing dialogs remain assigned to the same account and wait for re-login. They are never transferred to another Telegram account.
- The administrator receives one alert per authorization-loss incident with buttons: `ПЕРЕЗАЙТИ`, `УДАЛИТЬ АККАУНТ`, and `ОТКРЫТЬ АККАУНТ`.
- Re-login uses the stored phone when available, requests Telegram code and 2FA, verifies the exact expected Telegram user ID, and preserves First DM settings, dialogs and statistics.
- Account deletion still requires confirmation, closes only that account's active dialogs and preserves historical statistics.
- Main menu, account list and account card show a red `Требуется повторный вход` state and a dedicated problem-account button.
- `ПРОВЕРИТЬ СНОВА` performs a manual authorization check without treating temporary network errors as logout.
- No First DM limit, interval, pause, quota, filter or account-selection priority was changed.


## Changes in v1.0.56 - production log stability hotfix

- First-DM ambiguity recovery now uses durable backoff instead of repeating invalid peer history checks every 15 seconds.
- `PeerIdInvalidError` is logged once per retry window without production traceback spam.
- AI generates a contextual opening while code assembles every mandatory channel fact, so validator failures no longer consume three full promo generations.
- Scheduled promo/apology processing checks durable outbox state before any AI request.
- Existing prepared actions wait for reconciliation and no longer regenerate text every background tick.
- Rejected scheduled prepares now log target, account, stage, action and outbox status, then receive a bounded retry deadline.
- No First-DM limits, intervals, pauses, filters, quotas or account-selection rules were changed.

## Changes in v1.0.55 - AI personality and completed dialog funnel

### One approved AI personality

- Every Telegram account uses the same AI character.
- The AI writes like an ordinary Telegram user interested in crypto.
- Replies are short, simple, contextual and informal without sounding like a manager or bot.
- The AI must not invent personal trades, deposits, profits, experience, VIP memberships or acquaintances.
- Profit guarantees and claims about profitable signals are rejected by validation.
- Every generated reply is normalized to the ordinary short hyphen `-`.

### Current five-message budget

1. First DM is a simple everyday opener.
2. Any allowed user reaction starts one complete promo message with the exact `CHANNEL_LINK`.
3. The promo explains that the channel is free and that software quickly copies new posts from closed VIP channels.
4. One smoothing apology follows after the configured 60-60 second delay.
5. One separate link-opening instruction follows after the same configured delay.
6. The standard path uses four outgoing messages. The absolute maximum remains five, so no sixth outgoing response is possible.

### Non-text reactions

- Voice notes are not transcribed.
- Every voice note, emoji-only message, sticker, GIF, photo, video or file is treated as a neutral or positive reaction.
- Non-text content always continues the normal promo branch.
- Aggression, refusal and stop requests are classified only from textual messages.

### Refusal behavior

- Soft refusal receives one calm promo with the link.
- A non-aggressive request to stop receives one final calm message with the link, then global opt-out.
- An aggressive textual refusal receives only a short apology without a link, then global opt-out.
- Global opt-out prevents every other Telegram account from contacting the same user.

### Uniqueness and fallback

- First DM, promo, apology and link-help texts are stored separately.
- Each type keeps exactly its last 20 sent texts across all accounts.
- A new text is compared only with the last 20 texts of the same type.
- One initial generation and at most two repeat generations are allowed.
- If AI generation is unavailable or invalid, a varied local fallback is used without exact repetition inside the active window.
- The exact channel link is appended by code, never trusted from model output.

### Reliability preserved

- Every incoming reaction is written to the durable dialog inbox before AI processing.
- Every outgoing promo, apology, Q&A answer and terminal reply uses the crash-safe outbox.
- Ambiguous Telegram delivery is reconciled against real chat history before retry.
- A pending user reply is processed first but does not erase the remaining scheduled apology or link-help step.
- Existing v1.0.54 `explained` dialogs are recovered and completed by the new funnel.

### First DM scope preserved

No First-DM limit, interval, pause, filter, quota or account-selection rule was changed in v1.0.55.

## Changes in v1.0.53 - Step 5 of the audit repair plan

### Finite and diagnosable queue failures

- `no_entity` is remembered separately for every participating Telegram account.
- A lead is closed with `no_entity_all_accounts` only after every participating account has actually proved that it cannot resolve the target.
- If an untried account is temporarily unavailable, the lead is deferred instead of repeatedly regenerating First DM text in a tight loop.
- Fresh group activity or explicit import clears the old entity-failure evidence and safely reopens the lead.
- Retryable pre-send errors are counted once per full dispatch round, not once per account. After the existing five rounds, the lead receives the terminal diagnostic `max_transient_attempts` without creating a false completed contact.
- Privacy/block/deactivated/invalid Telegram errors remain permanent and keep the previously approved global stop behavior.

### Large-queue performance

- `ORDER BY RANDOM()` was removed from the hot claim query.
- Queue claiming now uses an indexed rotating primary-key cursor with wrap-around.
- Added `idx_leads_status_target` for the hot claim path.
- A 100,000-row local SQLite benchmark claimed the next eligible lead in about 0.002 seconds and the query plan used the new index without a temporary random-sort tree.

### Cleaner diagnostics and logs

- Added durable `last_error`, `failure_reason`, `failure_at` fields on leads.
- Added `lead_account_failures` to preserve per-account resolution evidence.
- The queue screen shows recent terminal reasons without sending extra administrator notifications.
- Raw group messages, refreshes and skips moved from INFO to DEBUG, preventing high-volume group traffic from flooding normal logs.
- The dispatcher tick was split into focused selection, account-attempt and finalization helpers.

### Scope preserved

- No First-DM limit, interval, global spacing, pause, filter, quota or approved account-selection priority was changed.
- No dialog, notification, import or 30/180-day retention rule was changed.



## Changes in v1.0.52 - Step 4 of the audit repair plan

### Clear account lifecycle

- Only genuinely active funnel stages keep a disabled Telegram account connected.
- `link_sent`, `followup_sent` and `closed` are terminal stages and no longer keep an account alive for the 30-day retention wait.
- Each terminal dialog receives a stable `lifecycle_completed_at` timestamp. Later retention/status touches do not rewrite the real completion date.
- Main-menu and dialog counters now use the same active/terminal stage definitions.

### Efficient Telegram retention

- Due cleanup jobs are grouped by Telegram account. One existing or temporary client is reused for all jobs of that account in the batch.
- Long private histories are streamed and deleted in chunks of 100 message IDs instead of being fully accumulated in memory.
- Old repeated attempts remain bounded to their own message range and cannot delete newer attempts.
- Temporary failures receive exponential retry scheduling with a recorded reason.

### Terminal handling for removed accounts

- If an account/session is permanently removed, its impossible Telegram-retention jobs receive a final abandoned status instead of retrying forever.
- The dashboard reports such jobs separately. SQLite history/statistics remain available until their normal 180-day text cleanup.

### Clean shutdown

- The main periodic background loop is explicitly cancelled and awaited before Telegram clients and SQLite are shut down.
- Disabled accounts disconnect immediately after their last active dialog reaches a terminal state.

### Scope preserved

- No First-DM limit, interval, pacing, pause, filter, quota or account-selection rule was changed.
- Queue/no-entity/performance work remains Step 5.


## Changes in v1.0.51 - Step 3 of the audit repair plan

### Independent dialog attempts

- An explicit import/requeue no longer destroys the previous dialog.
- The previous attempt is copied atomically into `dialog_archives` before the operational tables are reset.
- Every archived attempt keeps its own account, stage, counters, history, First-DM evidence and 30/180-day retention state.
- The newly imported attempt starts as a separate current dialog and receives a separate `first_dm_events` record after delivery.

### Safe Telegram cleanup across repeated attempts

- When a new First DM is confirmed from the same Telegram account, the previous archived attempt receives an exact upper Telegram message-ID boundary.
- The old 30-day cleanup deletes only the messages belonging to the old attempt.
- If an exact ID is unavailable, the new First-DM timestamp is retained as a safe fallback boundary. Attempts from a different Telegram account do not need a boundary because they are different private chats.
- Cleanup of an archived attempt never closes, modifies or deletes the current attempt.

### Per-message 180-day text retention

- History entries now carry their own UTC timestamp.
- Incoming texts, prepared outgoing texts and sent replies each schedule retention from their own timestamp.
- When cleanup becomes due, only text older than 180 days is erased; newer text in the same dialog remains.
- Archived outbox/inbox snapshots are cleaned by the same rule.
- IDs, timestamps, stage, counters, statistics and opt-out data remain after text cleanup.

### Scope preserved

- Explicit import still intentionally opens a new First-DM attempt.
- No First-DM limit, interval, pacing, pause, filter, quota or account-selection rule was changed.
- Account lifecycle and queue performance remain Step 4 and Step 5 work.


## Changes in v1.0.50 - Step 2 of the audit repair plan

### Durable delivery for every dialog message

- One SQLite outbox protects every post-First-DM message, including promo, smoothing apology, Q&A reply and terminal refusal response.
- Each user-triggered delivery is tied to the durable `dialog_inbox` row that caused it.
- The message text and intended dialog transition are persisted before Telegram send.
- Telegram `message_id` and send time are committed atomically with history, stage, counters and contact state.
- A recovered delivery also marks its source inbox row complete, so the same user message cannot advance the funnel twice.

### Crash and network ambiguity recovery

- A lost network response no longer causes blind resend.
- The bot checks the real Telegram conversation from the prepare time onward.
- If the message exists, SQLite state is completed without sending again.
- If Telegram definitely has no matching message, the action becomes retryable.
- Temporary client/FloodWait/PeerFlood failures keep the incoming message pending instead of silently losing it.
- Hard-stop apology can be recovered even though opt-out already closed the dialog.

### No fixed 30/40/100-message scan limits

- First-DM recovery, dialog-message recovery and First-DM retention lookup use Telethon history iteration without a numeric message cap.
- Exact stored Telegram message IDs remain the primary evidence whenever available.

### Idempotent incoming history

- `dialog_inbox.history_appended` prevents the same user text from being inserted twice after an ambiguous send or restart.
- Startup now reconciles ambiguous outgoing messages before draining pending incoming messages.

### Scope preserved

- No First-DM interval, limit, pause, filter, quota or account-selection rule was changed.
- Import behavior and 30/180-day retention policy remain unchanged for Step 3 and Step 4.


## Changes in v1.0.49 - Step 1 of the audit repair plan

### Durable sequential incoming-message processing

- Every incoming private reply is persisted to the new `dialog_inbox` table before AI delay or Telegram send.
- Messages from the same account/user dialog are processed strictly one by one and in arrival order.
- A second quick message is no longer dropped while the first message is being processed.
- Telegram message IDs are stored and deduplicated, so a repeated update is not processed twice.
- Pending incoming messages recover automatically after a process restart.

### Direct refusal has highest priority

- A hard stop such as «не пиши» is saved as opt-out immediately.
- Scheduled link/follow-up work is cancelled before the waiting AI response can continue.
- Any normal queued messages behind the refusal are ignored.
- The bot sends one apology and permanently closes the dialog.
- If shutdown happens before the apology, the hard-stop message remains pending and resumes after restart.

### Dialog race protection

- Incoming replies, scheduled apology and follow-up now share one per-dialog lock.
- A closed dialog cannot be reopened by a late AI task or delayed delivery commit.
- Dialog history append is atomic under the SQLite lock.
- Local 180-day text purge also clears stored incoming-message text.

### Scope preserved

- No First-DM limit, interval, pacing, pause, filter or quota was changed.
- Menu, notification policy, import behavior and retention periods remain unchanged.

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

### Crash-safe promo, apology and follow-up

- Promo, smoothing apology and First-DM silence follow-up are persisted before Telegram send.
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
CHANNEL_PITCH=Бесплатный канал: софт почти сразу копирует новые посты из закрытых VIP, отдельные доступы покупать не нужно
OPENAI_API_KEY=
AI_MODEL=gpt-4o-mini
AI_REQUEST_TIMEOUT_SECONDS=20
AI_DM_ENABLED=true
AI_APOLOGY_DELAY_MIN=5
AI_APOLOGY_DELAY_MAX=60
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

1. Deploy v1.0.62 over the existing volume/database.
2. Confirm the new dashboard and all-time First-DM counter.
3. Send a test First DM and confirm only one routine admin notification.
4. Continue the dialog and confirm no reply/link/follow-up push notifications appear.
5. Test direct refusal and opt-out.
6. Restart during a prepared promo/apology/follow-up and verify no duplicate.
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
