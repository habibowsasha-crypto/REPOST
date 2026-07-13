# Как поучаствовать в проекте

Буду рад PR-ам и issue — особенно по части устойчивости и безопасности.

## Запуск локально

```bash
git clone https://github.com/zerox9dev/TgBlaster.git
cd TgBlaster

python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate

pip install -r requirements.txt

cp .env.example .env
# открой .env и заполни API_ID, API_HASH, BOT_TOKEN, ADMIN_ID_LIST

python main.py
```

Для `API_ID` / `API_HASH` идёшь на [my.telegram.org](https://my.telegram.org/apps), для `BOT_TOKEN` — к [@BotFather](https://t.me/BotFather).

## Тесты

```bash
pytest
```

Тестов пока мало — если пишешь новую фичу, было бы здорово добавить хотя бы минимальную проверку к ней.

## Стиль кода

Линтер и форматтер — **ruff** (конфиг в `pyproject.toml`). Установи:

```bash
pip install ruff
```

Перед коммитом прогони:

```bash
ruff format .
ruff check .
```

Можно повесить это на pre-commit — конфиг уже лежит в `.pre-commit-config.yaml` (`pip install pre-commit && pre-commit install`).

Если ruff ругается на что-то неважное — можно подавить через `# noqa`, но лучше поправить.

## Как отправить изменение

1. Форкни репо, создай ветку от `main`:
   ```bash
   git checkout -b fix/something-broken
   ```
2. Внеси правки, убедись, что `ruff format .` и `ruff check .` не ругаются.
3. Открой PR на `main`. Опиши, что изменилось и почему.

Если делаешь большую фичу — лучше сначала открой issue и обсуди, чтобы не потратить время впустую.

## Чего не надо делать

- Не коммить `.env`, `sessions.db`, `.sessions/` — всё это уже в `.gitignore`, но на всякий случай проверь.
- Не добавлять в `requirements.txt` зависимости без необходимости.
