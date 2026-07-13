# Extra routes additive patch

База: `telegram-userbot-reposter-production-clean-patch.zip`.

Цель патча: не трогать рабочие старые Railway Variables, но дать возможность добавлять новые каналы и маршруты отдельными переменными.

## Новые переменные

```env
EXTRA_SOURCE_CHATS=
EXTRA_ROUTE_MAP=
EXTRA_SOURCE_TOPIC_MAP=
EXTRA_TOPIC_ROUTE_MAP=
```

## Как бот объединяет конфиг

```txt
SOURCE_CHATS + EXTRA_SOURCE_CHATS
ROUTE_MAP + EXTRA_ROUTE_MAP
SOURCE_TOPIC_MAP + EXTRA_SOURCE_TOPIC_MAP
TOPIC_ROUTE_MAP + EXTRA_TOPIC_ROUTE_MAP
```

## Что это даёт

- Старые `SOURCE_CHATS` и `ROUTE_MAP` можно оставить как есть.
- Новые каналы добавляются в `EXTRA_SOURCE_CHATS`.
- Новые дубли/маршруты добавляются в `EXTRA_ROUTE_MAP`.
- Новые topic-to-topic маршруты добавляются в `EXTRA_TOPIC_ROUTE_MAP`.
- Дублирование остаётся additive: если один источник есть в `SOURCE_CHATS` и `ROUTE_MAP`, он продолжает идти в оба места.

## Что не менялось

- Не добавлялись реакции.
- Не менялась логика копирования сообщений, медиа, альбомов.
- Не менялась логика edit/delete sync.
- Не менялся формат старых переменных.

## Проверка

Выполнено:

```bash
python -m py_compile main.py generate_session.py list_chats.py
```

Синтаксических ошибок нет.
