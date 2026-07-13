# Production clean patch

База: `vor158-main.zip`.

Цель патча: оставить простую логику проекта без реакций и лишних режимов:

```txt
Канал A -> Канал B
Канал A -> Канал C через ROUTE_MAP
Канал X -> Канал Y через ROUTE_MAP
```

## Что изменено

1. Добавлен `.env.example` с понятными Railway/GitHub переменными.
2. Добавлен `.gitignore`, чтобы не залить `.env`, session-файлы и SQLite-базу.
3. Добавлен `.dockerignore`, чтобы секреты и мусор не попадали в Docker build context.
4. Добавлен `Dockerfile` на Python 3.12 для более стабильного Railway deploy.
5. Исправлено копирование из собственных/admin каналов:
   - бот больше не отбрасывает `message.out=True` в `NewMessage`;
   - бот больше не отбрасывает альбомы, где сообщения пришли как `out=True`;
   - это нужно, когда Telegram считает посты из твоего канала исходящими.
6. Усилен `message_map` для multi-route:
   - новый primary key: `(source_chat_id, source_message_id, target_chat_id, target_message_id)`;
   - добавлена автоматическая миграция старой SQLite-таблицы;
   - это защищает edit/delete sync, когда один source-пост копируется в несколько target-чатов.
7. README дополнен коротким production-clean пояснением.

## Что специально не добавлялось

- reaction repost;
- фильтры ключевых слов;
- новые режимы копирования;
- управление из Telegram-меню;
- лишние служебные скрипты.

## Проверка

Выполнено:

```bash
python -m py_compile main.py generate_session.py list_chats.py
```

Синтаксических ошибок нет.
