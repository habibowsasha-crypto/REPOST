# LikeBot AI Comments - schema v1

> Историческая инструкция для LikeBot v1.0.37-v1.0.38. Начиная с v1.0.39
> используется более новая schema; для v1.0.40 актуальна процедура
> `migrations/AI_COMMENTS_SCHEMA_V3.md`; текущий CLI намеренно не принимает
> старый rollback token v1.

Эта миграция относится только к шагу 6. Она создаёт 13 таблиц с префиксом
`ai_`, безопасные индексы и начальные настройки. OpenAI API, Telegram-публикация,
workers, handlers и меню не запускаются.

## Перед upgrade

1. Остановить LikeBot/Railway replica, чтобы отдельный процесс получил тот же
   PostgreSQL advisory lock.
2. Убедиться, что `AI_COMMENTS_ENABLED=false`,
   `AI_GENERATION_ENABLED=false`, `AI_DIALOGUES_ENABLED=false` и
   `AI_PUBLICATION_ENABLED=false`.
3. Создать проверяемый backup:
   - PostgreSQL: выполнить `pg_dump` в custom format, используя `DATABASE_URL`
     только из environment, затем проверить архив командой `pg_restore --list`;
   - SQLite: остановить процесс, выполнить `sqlite3 "$DB_PATH" ".backup '$BACKUP_PATH'"`,
     затем открыть копию через `PRAGMA integrity_check`.
4. Не помещать `DATABASE_URL`, backup БД или runtime `.db` в release ZIP.

## Upgrade и проверка

```bash
AI_COMMENTS_ENABLED=false python -m tools.ai_comments_schema upgrade
python -m tools.ai_comments_schema verify
```

`DATABASE_URL` читается только из environment и не печатается. Upgrade
идемпотентен: повторный запуск не перезаписывает значения `ai_settings` и не
трогает существующие 11 таблиц LikeBot.

## Rollback

Rollback нужен только для возврата на код до v1.0.37. Он необратимо удаляет все
13 таблиц AI Comments, поэтому запускается после backup и точного подтверждения:

```bash
python -m tools.ai_comments_schema rollback \
  --confirmation DROP_AI_COMMENTS_SCHEMA_V1
```

Rollback не удаляет и не изменяет `accounts`, `channels`, очереди реакций,
просмотров, подписок, `app_settings`, `configuration_events` и компактную
историю основного LikeBot. После rollback нельзя запускать v1.0.37 или новее:
обычный startup снова создаст schema v1. Сначала разверните предыдущий
подтверждённый ZIP.

## Delete policy

- ссылки на `channels`/`accounts` в профилях, постах, threads, drafts и jobs
  используют `ON DELETE SET NULL`, чтобы удаление основной записи не уничтожало
  минимальный аудит решения;
- `ai_comment_messages` защищены `RESTRICT` на удаление thread;
- `ai_knowledge_chunks` защищены `RESTRICT` на удаление source: источник сначала
  переводится в `retired`, затем очищается отдельной retention-задачей;
- сырой текст сделан nullable там, где retention должен оставить только IDs,
  hashes и timestamps.

Live PostgreSQL smoke должен быть выполнен в CI/DEV до production deploy.
