# LikeBot AI Comments - schema v3

Schema v3 относится к шагу 9 и добавляет неизменяемую историю индивидуальных
профилей аккаунтов. Она не подключает OpenAI, не создаёт комментарии и не
публикует сообщения в Telegram.

## Что меняется

- добавляется таблица `ai_account_profile_revisions`;
- `ai_account_profiles.telegram_user_id` становится уникальной стабильной
  границей личности через индекс `uq_ai_account_profiles_telegram_user`;
- существующий профиль с `account_id` получает Telegram ID из связанного
  `Account`, если поле раньше было пустым;
- отвязанный профиль автоматически связывается с вернувшимся аккаунтом того же
  Telegram ID;
- каждому существующему аккаунту без профиля создаётся один детерминированный
  профиль, выключенный по умолчанию;
- для текущей версии каждого профиля создаётся одна revision с точным
  каноническим JSON и SHA-256;
- `ai_settings.schema_version` переводится с `1` или `2` на `3` через
  optimistic guard.

После upgrade в базе 26 таблиц: 11 core и 15 изолированных `ai_`-таблиц.

## Перед upgrade

1. Остановить LikeBot/Railway replica, чтобы миграцию выполнял один процесс.
2. Оставить `AI_COMMENTS_ENABLED=false`, `AI_GENERATION_ENABLED=false`,
   `AI_DIALOGUES_ENABLED=false` и `AI_PUBLICATION_ENABLED=false`.
3. Создать и проверить backup:
   - PostgreSQL: `pg_dump` в custom format, затем `pg_restore --list`;
   - SQLite: при остановленном процессе выполнить `.backup` и проверить копию
     через `PRAGMA integrity_check`.
4. Не помещать `DATABASE_URL`, backup, runtime `.db` или Telegram sessions в
   release ZIP.

## Upgrade и проверка

```bash
AI_COMMENTS_ENABLED=false python -m tools.ai_comments_schema upgrade
python -m tools.ai_comments_schema verify
```

Upgrade выполняет fail-closed preflight, проверяет конфликтующие Telegram ID до
создания уникального индекса и работает в одной транзакции. При несовпадении
`Account.telegram_user_id` и сохранённой личности либо при двух профилях одного
Telegram ID миграция останавливается без включения AI-функций. Повторный запуск
идемпотентен и не создаёт повторные профили или revisions.

## Rollback

Rollback необратимо удаляет все 15 таблиц AI Comments, включая память каналов,
профили и их историю. Он допустим только после backup и с точным подтверждением:

```bash
python -m tools.ai_comments_schema rollback \
  --confirmation DROP_AI_COMMENTS_SCHEMA_V3
```

Rollback не удаляет и не меняет core-таблицы аккаунтов, каналов, реакций,
просмотров, подписок и настроек LikeBot. После rollback сначала разверните
подтверждённую совместимую версию.

## Delete policy и готовность аккаунта

- удаление core `Account` обнуляет только локальный `account_id`; профиль,
  Telegram ID и история остаются;
- revision защищена `RESTRICT` и не удаляется вместе с UI-архивированием;
- архивирование профиля является обратимым soft delete;
- профиль нового или восстановленного аккаунта не включается автоматически;
- readiness guard отклоняет профиль, если он выключен, архивирован, отвязан,
  quarantined, core-аккаунт выключен, сессия пуста, действует FloodWait,
  исчерпан дневной лимит или не завершён cooldown.

Live PostgreSQL smoke должен быть выполнен в CI/DEV до production deploy.
