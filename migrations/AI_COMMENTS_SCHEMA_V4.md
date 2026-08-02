# LikeBot AI Comments schema v4

Версия приложения: `1.0.44`  
Этап: шаг 12 — конечные связанные диалоги и суточные квоты.

## Изменение схемы

Переход `v3 -> v4` добавляет только две изолированные таблицы:

- `ai_comment_thread_plans` — неизменяемая основа конечного плана, участники,
  exact reply graph, следующая позиция, интервалы, версия корневого поста;
- `ai_comment_quota_events` — идемпотентные события
  `reply_bonus_grant`/`reply_bonus_use` с локальным `day_key`.

Существующие core-таблицы, аккаунты, сессии, каналы, реакции, просмотры,
подписки, публикации памяти и single-comment drafts не преобразуются.
После upgrade ожидается 28 таблиц: 11 core и 17 `ai_`.

## Перед Railway deploy

1. Остановить дополнительные replica и оставить одну.
2. Сохранить PostgreSQL backup и Railway Variables.
3. Не менять `DATABASE_URL` и `SESSION_ENCRYPTION_KEY`.
4. Оставить все AI-флаги `false` на первом запуске.
5. Развернуть полный кандидат v1.0.44 и дождаться `Start polling`.
6. Проверить, что schema version равна `4`, а startup не содержит traceback.

Миграция идемпотентна: повторный startup не создаёт вторые таблицы и не
переписывает существующие данные.

## Суточная граница

Тimestamps в БД остаются UTC. Календарные сутки для лимитов вычисляются по
`AI_COMMENTS_TIMEZONE` (default `Europe/Moscow`). Новый `day_key` начинается
ровно в `00:00` этого часового пояса. Отдельный reset-job не используется:
запросы автоматически выбирают только события и опубликованные сообщения
текущего локального дня.

## Бонусные reply slots

- `reply_bonus_slots` хранится в versioned `style_json` профиля;
- допустимый диапазон `0-20`, default `3`;
- один bounded pool выдаётся профилю не более одного раза за локальные сутки;
- grant возможен только после наблюдения реального внешнего reply на уже
  опубликованный комментарий профиля;
- расход разрешён только в reply-context и только после исчерпания обычного
  `daily_limit`;
- repeated observer/publication acknowledgement идемпотентен;
- при смене локальной даты старые grant/use не участвуют в расчёте.

## Rollback

Автоматический destructive rollback запрещён. Поддерживаемый token:

```text
DROP_AI_COMMENTS_SCHEMA_V4
```

Он предназначен только для явно подтверждённого полного удаления всего
AI Comments schema в тестовой среде. Для production rollback безопаснее:

1. выключить `OPENAI_GATEWAY_ENABLED`, `AI_COMMENTS_ENABLED`,
   `AI_GENERATION_ENABLED`, `AI_DIALOGUES_ENABLED` и
   `AI_PUBLICATION_ENABLED`;
2. вернуть код v1.0.43;
3. не удалять новые таблицы — старая версия их не использует;
4. восстановить backup только при доказанном повреждении данных.

## Ограничение шага 12

Telegram-публикация и production observer входящих reply ещё отсутствуют.
Таблицы, бизнес-правила и repository hooks готовы, но их live-интеграция и
атомарное списание слота выполняются в шаге 15 вместе с publication queue.
