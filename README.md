# Channel DM Bot v1.0.83


## v1.0.83 - entity-aware administrator dashboard

- The shared First DM queue now shows unique users only once.
- The dashboard separately shows users available to enabled accounts, users waiting for an account to be enabled and users with no available account.
- Every account block shows how many pending users are available to that exact account.
- The confusing `Только этот аккаунт видит` metric was not added.
- The dashboard explicitly explains that one user may be available to several accounts, but receives only one First DM.
- A claimed or sent user disappears from every account availability counter immediately.
- First DM routing, duplicate protection, dialog ownership, pacing, PeerFlood, Variant 3 and the database schema are unchanged.

## v1.0.82 - selectable dialog Variant 3

- Added `DIALOG_FLOW_VARIANT=1|2|3` without changing the default production flow.
- Variant 3 puts the approved short opening hint directly under the promo link.
- Variant 3 sends the smoothing apology as the only later automatic message.
- Detailed link-opening help is sent only after a real user reports a link problem.
- First DM style, pacing, refusals, opt-out, account ownership and the five-message cap are unchanged.
- Railway value for the approved mode: `DIALOG_FLOW_VARIANT=3`.

## Changes in v1.0.81 - First DM cooldown separated from real dialogs

- PeerFlood and local account cooldowns stop cold First DM work only.
- A real incoming reply is processed immediately by the same account that owns the dialog.
- Promo, Q&A, smoothing apology and link-opening help no longer wait for the First DM cooldown.
- Silence follow-up remains protected by the First DM cooldown because it is an autonomous pre-reply touch.
- A Telegram FloodWait or PeerFlood during a real dialog creates retry state only for that exact outbox action.
- Pending inbox, crash-safe outbox and no-cross-account ownership remain preserved.
- The database is upgraded in place without clearing or replacing /data/bot.db.

## Changes in v1.0.80 - global pause and PeerFlood loop fix

- Global First DM pause blocks new First DM and every autonomous pre-reply touch, including silence follow-up.
- Only dialogs with durable incoming user evidence continue while the global pause is active.
- The send boundary and ambiguous-outbox recovery repeat the same gate, preventing scheduler and restart races.
- A PeerFlood caused by silence follow-up creates a persistent 6-hour hold for that exact follow-up.
- Repeated PeerFlood inside one incident no longer creates repeated main administrator notifications or repeated SpamBot checks.
- SpamBot resume is transactionally claimed and idempotent. Duplicate or stale resume rows do not create another resume notification.
- Pending inbox rows, crash-safe outbox records, account ownership, opt-out and the approved dialog funnel remain intact.
- Startup migration repairs old prepared pre-reply follow-ups and stale FREE_PENDING or RESUMING rows without clearing the database.

This section supersedes the v1.0.79 statement that every existing dialog continues during global pause. A dialog continues only after a real incoming user message is durably recorded.

## Changes in v1.0.79 - active dialogs continue during First DM pause

- The main First DM pause now blocks only new First DM sends.
- After SpamBot confirms that an account is free and the approved PeerFlood cooldown has elapsed, the Telegram cooldown is cleared even when the global First DM worker remains paused.
- Existing dialogs can answer immediately, while next_send_at still protects future First DM pacing.
- A startup migration repairs accounts left in the old rolling global-pause hold.
- A real PeerFlood during an active-dialog reply still re-applies the normal safety pause.

## Changes in v1.0.78 - hashless local-cache owner and stable no-entity terminal

- Accounts that observed a target without a stored access hash now receive one safe local-cache lookup.
- Automatic dispatch still never performs a remote username search.
- Identical hashless activity no longer reopens a terminal no-entity lead.
- A lead reopens only when account-owned entity evidence genuinely improves.

- Automatic First DM dispatch now resolves Telegram entity access before generating any First DM text.
- Each sender account stores its own access hash for each target it actually saw in a monitored chat.
- Automatic dispatch tries only active authorized accounts that own entity evidence for that exact target.
- The source account that saw the user remains first priority.
- Automatic username search is disabled in the production First DM path, preventing repeated cross-account lookups and reducing Telegram FloodWait risk.
- A bounded recent-history sync scans at most 100 messages per selected chat and only backfills entity evidence for leads already in the queue.
- Each account and chat is synced at most once per 24 hours unless a recorded retry becomes due.
- New evidence may reopen only leads previously closed for missing entity access. Repeated identical activity cannot create a reopen and fail loop.
- `short_hook` now uses only the reviewed local pool and never calls AI.
- AI output in other modes is normalized before validation, and leading list markers such as `-`, `*`, `•` or `1.` are rejected as raw output and cannot be sent.
- Accounts marked for reauthorization are excluded from automatic entity ownership and sending attempts.
- Magnet, legacy, promo, apology, link help, dialog ownership, pacing, PeerFlood and SpamBot behavior remain unchanged.

## Changes in v1.0.76 - selectable short_hook First DM mode

- Added `FIRST_DM_STYLE=short_hook`.
- Added a closed reviewed pool of 40 short First DM hooks.
- Help wording is allowed only when it explicitly refers to help with a question.
- Vague requests such as `Можешь помочь?` and `Выручишь?` are rejected.
- `magnet` and `legacy` remain fully available.
- The active mode is visible in the dashboard and settings screen.
- Pacing, queue, dialogs, PeerFlood, SpamBot and the post-First-DM greeting guard are unchanged.

Telegram user-account DM bot: monitors selected groups, builds one shared lead queue, sends First DM, continues active dialogs, handles refusals, sends only the administrator's exact channel link, and stores durable state in SQLite.


## Changes in v1.0.75 - no repeated greeting after First DM

- Only the First DM may start with a greeting.
- Promo, smoothing apology, link-opening help, Q&A, terminal replies and silence follow-up must continue the existing conversation without saying hello again.
- AI prompts now explicitly forbid a new greeting after First DM.
- Promo, apology, link-help and Q&A validators reject texts starting with `Привет`, `Здравствуйте`, `Добрый день`, `Хай`, `Здарова`, `Салют`, `Hello`, `Hi` and related forms.
- The final delivery boundary removes an accidental leading greeting before Telegram send, including old PREPARED outbox rows created by an earlier version.
- When a legacy prepared text is repaired, the durable outbox, phrase uniqueness journal and dialog history are updated to the exact cleaned text.
- A post-First-DM message containing only a greeting is blocked instead of being sent empty.
- First DM magnet and legacy modes, queue behavior, pacing, PeerFlood, SpamBot, active dialogs and the five-message budget are unchanged.
- Release archives now require ASCII-only file and directory names. Historical documentation is stored under `AUDITS/NOTES/`.

## Changes in v1.0.69 - exact First DM text in admin notification

- The successful `FIRST DM ОТПРАВЛЕН` administrator notification now includes the exact First DM text actually accepted by Telegram.
- The displayed wording is taken from the same immutable delivery text passed to `client.send_message`, not regenerated from a template.
- The notification is still emitted only after Telegram delivery and the durable SQLite sent commit both succeed.
- Failed, ambiguous or rolled-back First DM attempts do not produce the successful notification.
- Promo, apology, link-help and all later dialog messages still do not create routine administrator notifications.
- Queue behavior, dialog funnel, five-message limit, phrase uniqueness, pacing and PeerFlood protections are unchanged.


## Changes in v1.0.68 - PeerFlood cooldown anti-stacking

- The configurable 5-in-10 extra cooldown can be applied only once during one active local PeerFlood pause.
- Further five-event groups in the same pause are consumed but do not add more time.
- Every local PeerFlood cooldown is capped at the configured ordinary maximum plus the configured 5-in-10 extra.
- Startup automatically repairs previously inflated local timers such as multi-hour values created by older releases.
- Telegram FloodWait and confirmed @SpamBot limited states are not shortened by this repair.
- Manual or automatic resume clears the one-shot burst marker for the next independent pause.


## Changes in v1.0.67 - deep audit privacy and cooldown hardening

- Reassuring phrases such as `не беспокойся, всё нормально` and `не отвлекайся, говори` are no longer mistaken for stop requests.
- Common direct aggressive refusals such as `отвали`, `проваливай`, `иди лесом`, `заткнись` and similar forms are terminal, block promo and create global opt-out.
- Calm refusal behavior from v1.0.66 is preserved: `нет, спасибо`, `не надо`, `неинтересно` and similar non-aggressive replies may continue to promo.
- All conversation history sent to OpenAI now redacts URLs, the exact administrator invite link and Telegram usernames. Local classification and similarity checks still use the complete locally stored text.
- Startup logs no longer print the exact private CHANNEL_LINK. They report only that the configured link was validated.
- Existing-dialog sends now respect active Telegram PeerFlood and FloodWait cooldowns. Promo, apology, link-help and AI replies wait for the account cooldown instead of retrying on every scheduler tick.
- Due automatic-message queries are bounded and processed oldest first, preventing an unbounded scheduler batch after a long outage.
- Incoming dialog inbox items remain pending during account cooldown and are retried after the real Telegram pause without duplicate AI generation.
- Existing shared queue, source-account priority, dialog ownership, last-20 uniqueness, five-message budget and all approved pacing values remain unchanged.

This section supersedes older statements that the exact configured link may be printed at startup or that active-dialog automatic sends ignore account-level Telegram cooldown.


## Changes in v1.0.66 - calm refusal routing correction

- A calm textual refusal such as `нет, спасибо`, `не надо`, `неинтересно` or `не хочу` is non-terminal and may continue to the approved promo branch.
- Only an explicit request not to write, aggressive refusal, insult or threat blocks the promo and creates global opt-out.
- Non-text reactions remain neutral or positive and continue the promo branch.
- A calm refusal received after promo does not close the dialog, does not create opt-out and does not cancel the scheduled apology or link-opening instruction.
- Any unsent v1.0.65 soft-close outbox action linked to a calm refusal is cancelled during migration so it cannot terminate the dialog after restart.
- All queue, pacing, PeerFlood, phrase uniqueness, five-message budget and delivery safety rules remain unchanged.

This section supersedes the v1.0.65 statement that a clear calm refusal is terminal before promo generation.


## Changes in v1.0.65 - deep audit and crash-safe funnel fixes

- A clear calm refusal such as `нет, спасибо` or `не надо` is now terminal before promo generation. AI classification cannot override an explicit local refusal into the advertising branch.
- A request not to write or an aggressive refusal receives only a short terminal reply without any channel link. Old prepared v1.0.64 stop replies containing a link are cancelled during migration.
- The five-message limit now reserves the mandatory apology and link-opening instruction. At most one user-initiated Q&A reply may consume the remaining slot after promo.
- Legacy v1.0.64 dialogs that already spent the apology slot on extra Q&A prioritize the link-opening instruction as their final allowed message.
- Phrase uniqueness is journaled atomically in the same SQLite transaction as durable prepare. A crash after Telegram delivery can no longer leave the sent wording outside the last-20 protection window.
- A nullable `delivery_key` and unique partial index make phrase journal recovery idempotent. Prepared outbox texts also participate in the active uniqueness window.
- First DM generation and durable prepare are serialized by phrase kind inside the process, preventing concurrent attempts from reading the same old window and selecting the same fresh wording. Promo generation already uses the same protection.
- Recent examples sent to OpenAI redact the administrator's invite URL and Telegram handles. Local similarity checks still use the complete locally stored text.
- Two fallback wording inconsistencies were corrected so every generated promo and link-opening instruction satisfies the same validator used by regression tests.
- Existing queue, source-account priority, dialog ownership, opt-out, pacing, PeerFlood rules and approved four-message automatic funnel are unchanged.

This section supersedes any older statement that a calm refusal may still receive the promotional link or that phrase history is recorded only after Telegram confirms delivery.


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

- Main-menu pause stops new First DM and all autonomous touches before the first incoming reply.
- Only dialogs with a durably recorded incoming user message continue during the pause.
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
AI_APOLOGY_DELAY_MIN=60
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

1. Deploy v1.0.81 over the existing Railway service and mounted volume.
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

The release archive includes the full project plan, patch report and test report under `AUDITS/NOTES/`. Real Telegram delivery and revoke behavior must still be checked by the owner on test accounts.

## Security note

Do not commit `.env`, SQLite databases or Telegram session files. Restrict access to the Railway volume. Only IDs listed in `ADMIN_ID_LIST` can control the bot.

## v1.0.70 - PeerFlood resume loop and bounded dialog recovery

- Automatic SpamBot resume schedules the next First DM through the existing per-account 2-7 minute interval.
- A PeerFlood on one lead stops that lead's current account round; the same target is not immediately probed by another account.
- Ambiguous dialog recovery uses username-first entity resolution, persisted retry timestamps and bounded attempts.
- Unrecoverable legacy follow-ups are closed safely instead of retrying every scheduler tick forever.
- Manual admin resume remains an explicit immediate override.


## v1.0.71 - Global First-DM pause and SpamBot resume guard

- The main `PAUSE FIRST DM` switch is authoritative for automatic SpamBot recovery.
- A SpamBot-free PeerFlood account is not auto-resumed while global First DM is paused.
- No misleading `FIRST DM ACCOUNT RESUMED` notification is emitted during the global pause.
- The paused account receives a rolling Telegram cooldown, so overdue active-dialog messages cannot recreate the resume/PeerFlood loop.
- Starting First DM later allows automatic recovery, followed by the existing 2-7 minute protective interval.
- The protective interval is now written to both `next_send_at` and `cooldown_until`, so it blocks First DM and dialog sends.
- A production dispatch round re-checks the global flag before the actual First-DM send and returns the claimed lead to the queue if the administrator pressed pause mid-round.
- Startup migration repairs v1.0.70 accounts that were auto-resumed by SpamBot while the global worker remained paused.
- Healthy active dialogs still continue during a normal global First-DM pause; only PeerFlood accounts waiting for safe recovery remain blocked.

## v1.0.72 - PeerFlood series reset and SpamBot check deduplication

- The first real PeerFlood in a rolling series still launches one SpamBot check.
- Later PeerFlood events inside the same 10-minute window do not send another `/start` to SpamBot.
- A repeated PeerFlood still receives the approved 60-90 second local pause and then the existing automatic 2-7 minute recovery interval.
- A real SpamBot `limited` result remains authoritative and is never overwritten by the deduplication path.
- One proven successful First DM clears all current PeerFlood hit rows, the visible series counter and the one-pause burst marker.
- The next PeerFlood after a successful First DM starts fresh at 1/5.
- Daily limits, account/global pacing, the five-in-ten extra pause, queue ownership and dialog behavior are unchanged.

## First DM modes v1.0.74

The default mode is `magnet`. It sends short trading questions about missed entries, late entries, signal monitoring, notifications, spot or futures, and manual or automated tracking.

The previous neutral opener system is preserved as `legacy`. To roll back without changing the ZIP or database, set:

```env
FIRST_DM_STYLE=legacy
```

To return to the new system, set:

```env
FIRST_DM_STYLE=magnet
```

All modes keep the global last-20 uniqueness check and allow only the short ASCII hyphen `-`.


## v1.0.74 First DM structural hardening

- Magnet mode accepts only reviewed, grammatically complete questions from the approved local pool.
- Free-form AI keyword combinations cannot be sent.
- Any form of the word help is forbidden in magnet First DM.
- Similarity fallback never weakens the last-20 uniqueness rule.
- Invalid FIRST_DM_STYLE values fail closed instead of silently enabling magnet.
- The dashboard shows the active First DM mode.
- Legacy rollback remains available with FIRST_DM_STYLE=legacy.


## v1.0.76 - Selectable short_hook First DM mode

A third First DM mode is available:

```env
FIRST_DM_STYLE=short_hook
```

The mode uses a closed reviewed pool of short answer-provoking hooks. Help wording is allowed only when it explicitly says that help is needed with a question, for example:

- `Привет, можешь помочь с вопросом?`
- `Слушай, поможешь с одним вопросом?`
- `Салам, можешь подсказать по одному вопросу?`

Vague requests such as `Можешь помочь?`, `Нужна помощь` or `Выручишь?` are rejected. AI cannot invent an unreviewed short hook.

Available values are now:

```env
FIRST_DM_STYLE=magnet
FIRST_DM_STYLE=short_hook
FIRST_DM_STYLE=legacy
```

Change the Railway variable and restart the deployment to activate the selected mode. The dashboard and settings screen show the active value.
