# FIX REPORT — TRUE ADDITIVE ROUTE_MAP

Исправлена логика ROUTE_MAP.

## Цель

SOURCE_CHATS должен работать как раньше:

```env
SOURCE_CHATS=-100source:12
TARGET_CHAT=-100pirate_inside
```

ROUTE_MAP должен быть дополнительной отправкой:

```env
ROUTE_MAP=-100source>-100extra_chat
```

Если один источник есть одновременно в SOURCE_CHATS и ROUTE_MAP, бот обязан отправить пост в оба места:

1. В TARGET_CHAT / тему из SOURCE_CHATS
2. В дополнительный чат из ROUTE_MAP

## Что изменено

- ROUTE_MAP больше не является приоритетным маршрутом.
- ROUTE_MAP добавляется к маршрутам SOURCE_CHATS.
- Один источник может иметь несколько ROUTE_MAP-направлений.
- Дедупликация теперь учитывает target_chat и target_thread_id через route_suffix.
- Старый формат SOURCE_CHATS не менялся.

## Формат

```env
ROUTE_MAP=-100source1>-100target1,-100source2>-100target2
```

Допустимо несколько направлений для одного источника:

```env
ROUTE_MAP=-100source>-100target1,-100source>-100target2
```

## Важно

Тестировать только на новых постах после redeploy.
