# FIX REPORT - Topic-to-topic routing

Добавлена расширенная маршрутизация из тем источника в темы получателя.

## Новая переменная

```env
TOPIC_ROUTE_MAP=-100source:source_thread>-100target:target_thread
```

Пример:

```env
TOPIC_ROUTE_MAP=-1001111111111:4>-1002222222222:10,-1001111111111:8>-1002222222222:12
```

## Что изменено

- Добавлена поддержка маршрутов `source_chat:source_thread -> target_chat:target_thread`.
- Старый режим `SOURCE_CHATS` продолжает работать.
- Старый режим `SOURCE_CHATS=-100source:target_thread` продолжает работать.
- Если source chat используется в `TOPIC_ROUTE_MAP`, но входящее сообщение пришло из неуказанной темы - оно игнорируется.
- Добавлена защита от самоповтора: исходящие сообщения userbot не обрабатываются повторно.
- Связки оригинал -> копия сохраняют target chat, чтобы edit/delete работали даже при разных целевых группах.

## Рекомендуемый режим

Для тем используйте:

```env
COPY_MODE=copy
```

В режиме `forward` Telegram/Telethon может игнорировать placement в forum topic.
