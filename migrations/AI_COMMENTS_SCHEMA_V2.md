# LikeBot AI Comments - schema v2

> Историческая инструкция для LikeBot v1.0.39. Начиная с v1.0.40 используется
> schema v3 и актуальная процедура `migrations/AI_COMMENTS_SCHEMA_V3.md`;
> текущий CLI намеренно принимает только актуальный rollback token.

Schema v2 относится к шагу 8 и добавляет одну изолированную таблицу
`ai_channel_post_revisions` к 13 таблицам schema v1. Она сохраняет точный снимок
каждой принятой версии Telegram-поста. OpenAI API, генерация и публикация этой
миграцией не включаются.

## Перед upgrade

1. Остановить LikeBot/Railway replica, чтобы миграцию выполнял один процесс.
2. Убедиться, что `AI_COMMENTS_ENABLED=false`, `AI_GENERATION_ENABLED=false`,
   `AI_DIALOGUES_ENABLED=false` и `AI_PUBLICATION_ENABLED=false`.
3. Создать проверяемый backup:
   - PostgreSQL: `pg_dump` в custom format с `DATABASE_URL` только из environment,
     затем `pg_restore --list`;
   - SQLite: остановить процесс, выполнить `.backup` в отдельный файл и проверить
     копию через `PRAGMA integrity_check`.
4. Не помещать `DATABASE_URL`, backup, runtime `.db` или Telegram sessions в
   release ZIP.

## Upgrade и проверка

```bash
AI_COMMENTS_ENABLED=false python -m tools.ai_comments_schema upgrade
python -m tools.ai_comments_schema verify
```

Upgrade выполняет fail-closed preflight существующих таблиц, создаёт
`ai_channel_post_revisions`, переводит `ai_settings.schema_version` с `1` на `2`
и создаёт revision с причиной `backfill` для текущего снимка каждого ранее
сохранённого поста. Весь upgrade выполняется в одной транзакции. Повторный запуск
не меняет настройки и не создаёт повторные ревизии.

После проверки должны существовать 25 таблиц: 11 core и 14 с префиксом `ai_`.

## Rollback

Rollback необратимо удаляет все 14 таблиц AI Comments, включая память и историю
редакций. Он допустим только после backup и с точным подтверждением:

```bash
python -m tools.ai_comments_schema rollback \
  --confirmation DROP_AI_COMMENTS_SCHEMA_V2
```

Rollback не удаляет и не меняет основные таблицы аккаунтов, каналов, реакций,
просмотров, подписок и настроек LikeBot. После rollback сначала разверните
подтверждённую версию до AI Comments; запуск v1.0.37 или новее снова создаст свою
схему.

## Аудит редакций и delete policy

- одна revision уникальна по `(post_id, source_revision)`;
- revision хранит Telegram channel/message ID, текст, caption, media type, hash,
  topics, edit/delete time и причину фиксации;
- удаление канала выполняет `SET NULL` только для локального `channel_id`, а
  Telegram IDs и фактический снимок остаются;
- удаление поста при существующих revisions защищено `RESTRICT`; штатное
  удаление Telegram-поста оформляется tombstone и новой revision;
- существующие FK/delete policy schema v1 не изменяются.

Live PostgreSQL smoke должен быть выполнен в CI/DEV до production deploy.
