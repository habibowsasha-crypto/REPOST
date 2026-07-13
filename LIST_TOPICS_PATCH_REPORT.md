# LIST_TOPICS_PATCH_REPORT

Добавлен helper `list_topics.py` для получения `message_thread_id` тем Telegram-групп.

## Зачем

Для переменных вида:

```env
EXTRA_TOPIC_ROUTE_MAP="SOURCE_CHAT_ID:SOURCE_TOPIC_ID>TARGET_CHAT_ID:TARGET_TOPIC_ID"
```

нужно знать ID темы источника и ID темы получателя. Скрипт показывает эти значения через Telegram/Telethon API.

## Как использовать

```bash
py list_topics.py -1002106424484
```

Вывод:

```text
chat_id: -1002106424484 | chat_title: PIXEL | message_thread_id: 12345 | topic: Название темы
```

## Проверка

Проверен синтаксис:

```bash
python3 -m py_compile main.py generate_session.py list_chats.py list_topics.py
```
