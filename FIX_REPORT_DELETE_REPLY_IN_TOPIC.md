# FIX: delete notice reply inside topic

Изменение:
- уведомление об удалении снова отправляется как reply на конкретный пост-копию;
- для forum topics используется InputReplyToMessage(reply_to_msg_id, top_msg_id), чтобы reply оставался в нужной теме;
- если Telegram/Telethon не даст ответить на конкретный пост, включается fallback: сообщение отправляется в нужную тему с ID поста-копии.

Важно:
- для старых постов, скопированных до фиксов, target_thread_id мог не сохраниться; тестировать нужно на новом посте после redeploy.
