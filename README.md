# Channel DM Bot v1.0.161.0.15

Telegram-бот для мягкой DM-рекламы канала.

**Стек:** Python 3.12 · Telethon · SQLite · OpenAI · Railway (worker + Volume)

## Что делает

1. Админ добавляет user-аккаунты, выбирает группы (вручную / все + исключения)
2. Аккаунты с тумблером **Участвует** слушают чаты
3. Активные люди → **одна общая очередь**
4. **Старт** воркера: random лид × random свободный аккаунт
5. First DM - короткий вопрос (AI или локальный пул), **без ссылки**
6. После ответа: explain → при тишине 60–120 сек авто-ссылка на канал
7. «Не пиши» / агрессия → извинение + **opt-out**
8. FloodWait cooldown · PeerFlood → @SpamBot → auto-resume

## Railway

1. Залей репозиторий / ZIP на GitHub → Deploy
2. **Volume** mount path: `/data`
3. Variables:

```text
API_ID=
API_HASH=
BOT_TOKEN=
ADMIN_ID_LIST=123456789
DB_PATH=/data/bot.db
BOT_SESSION_PATH=/data/bot
MEDIA_DIR=/data/media
CHANNEL_LINK=https://t.me/+...
CHANNEL_PITCH=Бесплатный канал: посты из закрытых VIP, платить не нужно
OPENAI_API_KEY=
AI_MODEL=gpt-4o-mini
AI_DM_ENABLED=true
```

4. CMD: `python main.py` (Dockerfile)
5. `/ping` → `pong` · `/start` → меню

## Темп (defaults)

| Параметр | Значение |
|----------|----------|
| Пауза на аккаунт | 10–15 мин |
| Глобально между first DM | 90–180 сек |
| Лимит / аккаунт / сутки | 45 |
| AI-ответ | 30–90 сек |
| Авто-ссылка при тишине | 60–120 сек |
| PeerFlood min cooldown | 30 мин |

## Быстрый старт сценарий

1. **Аккаунты** → добавить (phone → код → 2FA)
2. **Чаты** → Обновить группы → выбрать / режим
3. **Включить участие**
4. Написать в тестовой группе с другого аккаунта
5. **Рассылка → Очередь** - pending
6. **Старт** - first DM уходит
7. Ответить в ЛС - explain → ссылка

## Тесты

```bash
pip install -r requirements.txt pytest
pytest -q
```

## Структура

```text
main.py              entry
config.py            env
db/schema.py         SQLite
handlers/            admin UI
services/            queue, monitor, dispatcher, AI, SpamBot, dialog
texts/first_dm.py    fallback first DM
tests/               unit tests
```

## Безопасность

- Не коммить `.env`, `*.session`, `*.db`
- Сессии аккаунтов в Volume БД - ограничьте доступ к Railway
- Только `ADMIN_ID_LIST` управляет ботом

## Версии шагов

1–2 каркас/меню · 3 аккаунты · 4 чаты · 5 очередь · 6 dispatcher ·  
7 SpamBot · 8 AI first DM · 9 диалог · 10 opt-out · **11 релиз**


## Bugfix v1.0.2

- Fixed missing `_handle_private` (dialogs were broken)
- Participating accounts connect even with 0 selected chats (needed for send)
- Dialog replies scheduled via `create_task` (no blocked Telethon loop)
- Stale `claimed` leads auto-release after 15 min
- FloodWait / PeerFlood handling in dialog sends
- Status shows real first-DM count today
- Removed unused `apscheduler` dependency
