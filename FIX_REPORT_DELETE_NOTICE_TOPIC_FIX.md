# FIX REPORT - Delete notice topic routing

## Problem
Delete notifications sometimes appeared in General instead of the same target topic.

## Root cause
The message mapping database stored:
- source_chat_id
- source_message_id
- target_chat_id
- target_message_id

But it did not store:
- target_thread_id

When a source post was deleted, the bot replied to the copied message without explicitly pinning the reply to the target forum topic. In Telegram forum groups this can be routed to General.

## Fix
- Added `target_thread_id` to `message_map`.
- Added automatic SQLite migration for existing databases.
- Saved `target_thread_id` when copying single messages and albums.
- Delete notices now use `InputReplyToMessage(..., top_msg_id=target_thread_id)` when a topic is known.
- Edit media notices use the same topic-safe reply logic.

## Important
For old copied posts that were saved before this fix, `target_thread_id` may be empty in the existing SQLite database. The fix is guaranteed for new posts copied after redeploy.
