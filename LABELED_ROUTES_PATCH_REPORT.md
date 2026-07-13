# LABELED ROUTES PATCH REPORT

## Цель

Добавить отдельный маршрутизатор для сценария:

```text
Канал 1 ┐
Канал 2 ├─> один общий канал
Канал 3 ┘
```

При этом в каждом скопированном посте должно быть видно, из какого канала пришёл пост.

## Новые переменные

```env
LABELED_ROUTE_MAP=
EXTRA_LABELED_ROUTE_MAP=
SOURCE_LABEL_MAP=
EXTRA_SOURCE_LABEL_MAP=
SOURCE_LABEL_TEMPLATE=📡 Источник: {source_title}\n\n{text}
```

## Логика

- `ROUTE_MAP` и `EXTRA_ROUTE_MAP` копируют посты как раньше, без изменения текста.
- `LABELED_ROUTE_MAP` и `EXTRA_LABELED_ROUTE_MAP` копируют посты с подписью источника.
- `SOURCE_LABEL_MAP` позволяет вручную задать имя источника.
- Если имя не задано, бот берёт название чата/канала из Telegram event.
- В labeled-маршрутах бот использует copy-логику даже если глобально стоит `COPY_MODE=forward`, потому что forward нельзя изменить и добавить подпись.

## Пример

```env
LABELED_ROUTE_MAP=-1001111111111>-1009999999999,-1002222222222>-1009999999999
SOURCE_LABEL_MAP=-1001111111111=Wolf Trading [PREMIUM],-1002222222222=CryptoGrad [VIP]
SOURCE_LABEL_TEMPLATE=📡 Источник: {source_title}\n\n{text}
```

Результат:

```text
📡 Источник: Wolf Trading [PREMIUM]

Текст сигнала...
```

## Что сохранено

- Старые `SOURCE_CHATS`, `TARGET_CHAT`, `ROUTE_MAP`, `EXTRA_ROUTE_MAP` не сломаны.
- Обычные маршруты продолжают копировать посты без подписи.
- Edit/delete sync сохранён.
- Для edit sync labeled-маршруты редактируются с сохранением подписи источника.

## Проверки

- `python -m py_compile main.py generate_session.py list_chats.py` - OK.
- Архив очищен от `.env`, `*.session`, `*.sqlite3`, `__pycache__`, `*.pyc`.
