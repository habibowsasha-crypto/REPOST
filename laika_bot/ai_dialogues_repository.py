from __future__ import annotations

import hashlib
import json
import re
import uuid
from datetime import datetime, timedelta
from decimal import Decimal

from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError

from .ai_comment_generation import (
    AI_COMMENT_DRAFT_TTL_DAYS,
    AICommentGenerationError,
    AICommentValidationResult,
)
from .ai_comments_models import (
    AIAccountProfile,
    AIChannelPost,
    AICommentDraft,
    AICommentMessage,
    AICommentThread,
    AICommentThreadPlan,
    AIGenerationJob,
    AIUsageStat,
)
from .ai_dialogues import (
    AI_DIALOGUE_CONTEXT_POSTS,
    AI_DIALOGUE_PROMPT_VERSION,
    AI_DIALOGUE_SCHEMA_VERSION,
    AI_DIALOGUE_VALIDATOR_VERSION,
    AIDialogueError,
    AIDialogueMessageSnapshot,
    AIDialogueReplyContext,
    AIDialogueThreadSnapshot,
    build_dialogue_reply_context,
    build_linear_plan,
    decode_plan,
    encode_plan,
)
from .models import utcnow

MAX_DATABASE_ID = 9_223_372_036_854_775_807
_SAFE_ID_RE = re.compile(r"[A-Za-z0-9_.:-]{1,160}")


def _id(value: int, *, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or not 0 < value <= MAX_DATABASE_ID:
        raise ValueError(f"Некорректный {field}")
    return value


def _safe_model(value: str) -> str:
    text = str(value).strip()
    if not text or len(text) > 96 or _SAFE_ID_RE.fullmatch(text) is None:
        raise ValueError("Некорректное имя модели")
    return text


def _safe_request_id(value: str | None) -> str | None:
    text = str(value or "").strip()
    return text if text and _SAFE_ID_RE.fullmatch(text) else None


def _message_snapshot(
    message: AICommentMessage,
    *,
    position: int,
    profile_name: str | None,
) -> AIDialogueMessageSnapshot:
    return AIDialogueMessageSnapshot(
        id=int(message.id),
        thread_id=int(message.thread_id),
        position=position,
        account_profile_id=(
            int(message.account_profile_id)
            if message.account_profile_id is not None
            else None
        ),
        account_profile_name=profile_name,
        role=message.role,
        status=message.status,
        text=message.text,
        text_hash=message.text_hash,
        telegram_message_id=(
            int(message.telegram_message_id)
            if message.telegram_message_id is not None
            else None
        ),
        reply_to_local_message_id=(
            int(message.reply_to_local_message_id)
            if message.reply_to_local_message_id is not None
            else None
        ),
        reply_to_telegram_message_id=(
            int(message.reply_to_telegram_message_id)
            if message.reply_to_telegram_message_id is not None
            else None
        ),
        created_at=message.created_at,
        approved_at=message.approved_at,
        published_at=message.published_at,
    )


class AIDialoguesRepositoryMixin:
    async def create_ai_dialogue_thread(
        self,
        *,
        channel_id: int,
        post_id: int,
        profile_ids: tuple[int, ...],
        max_messages: int,
        min_interval_seconds: int,
        max_interval_seconds: int,
        expires_hours: int,
        created_by_admin_id: int,
        timezone_name: str = "UTC",
    ) -> AIDialogueThreadSnapshot:
        channel_id = _id(channel_id, field="ID канала")
        post_id = _id(post_id, field="ID публикации")
        created_by_admin_id = _id(created_by_admin_id, field="ID администратора")
        profile_ids = tuple(_id(item, field="ID профиля") for item in profile_ids)
        plan = build_linear_plan(profile_ids, int(max_messages))
        if not 0 <= int(min_interval_seconds) <= int(max_interval_seconds) <= 86_400:
            raise ValueError("Некорректный диапазон интервалов диалога")
        if not 1 <= int(expires_hours) <= 168:
            raise ValueError("Срок диалога должен быть от 1 до 168 часов")

        planned_by_profile: dict[int, int] = {}
        for item in plan:
            planned_by_profile[item.profile_id] = planned_by_profile.get(item.profile_id, 0) + 1
        for profile_id, planned_count in planned_by_profile.items():
            eligibility = await self.get_ai_account_profile_eligibility(
                profile_id,
                timezone_name=timezone_name,
                reply_context=False,
            )
            if not eligibility.allowed:
                raise AIDialogueError(
                    f"Профиль {profile_id} недоступен: {eligibility.reason}"
                )
            remaining = max(0, eligibility.daily_limit - eligibility.comments_today)
            if planned_count > remaining:
                raise AIDialogueError(
                    f"Профилю {profile_id} не хватает дневных слотов: "
                    f"нужно {planned_count}, доступно {remaining}"
                )

        now = utcnow()
        async with self.sessions() as session:
            post = await session.get(AIChannelPost, post_id)
            if post is None or post.channel_id != channel_id or post.deleted_at is not None:
                raise AIDialogueError("Публикация не найдена или удалена")
            existing = await session.scalar(
                select(AICommentThread).where(AICommentThread.root_post_id == post_id)
            )
            if existing is not None:
                raise AIDialogueError(
                    "Для этой публикации уже существует диалог. Откройте его или выберите другой пост."
                )
            profiles = list(
                (
                    await session.scalars(
                        select(AIAccountProfile).where(AIAccountProfile.id.in_(profile_ids))
                    )
                ).all()
            )
            if len(profiles) != len(profile_ids) or any(not item.enabled for item in profiles):
                raise AIDialogueError("Один из выбранных профилей выключен или удалён")
            thread = AICommentThread(
                channel_id=channel_id,
                root_post_id=post_id,
                status="planned",
                max_messages=max_messages,
                expires_at=now + timedelta(hours=expires_hours),
                created_by_admin_id=created_by_admin_id,
                version=1,
                created_at=now,
                updated_at=now,
            )
            session.add(thread)
            await session.flush()
            thread_plan = AICommentThreadPlan(
                thread_id=thread.id,
                root_post_revision=int(post.source_revision),
                root_post_hash=post.normalized_text_hash,
                participant_profile_ids_json=json.dumps(
                    list(profile_ids), ensure_ascii=False, separators=(",", ":")
                ),
                message_plan_json=encode_plan(plan),
                topic=None,
                next_position=1,
                accepted_messages=0,
                min_interval_seconds=int(min_interval_seconds),
                max_interval_seconds=int(max_interval_seconds),
                cancel_reason_safe=None,
                version=1,
                created_at=now,
                updated_at=now,
            )
            session.add(thread_plan)
            await session.commit()
            thread_id = int(thread.id)
        result = await self.get_ai_dialogue_thread(thread_id)
        if result is None:
            raise AIDialogueError("Созданный диалог не удалось прочитать")
        return result

    async def get_ai_dialogue_thread(self, thread_id: int) -> AIDialogueThreadSnapshot | None:
        thread_id = _id(thread_id, field="ID диалога")
        async with self.sessions() as session:
            row = (
                await session.execute(
                    select(AICommentThread, AICommentThreadPlan, AIChannelPost)
                    .join(AICommentThreadPlan, AICommentThreadPlan.thread_id == AICommentThread.id)
                    .outerjoin(AIChannelPost, AIChannelPost.id == AICommentThread.root_post_id)
                    .where(AICommentThread.id == thread_id)
                )
            ).one_or_none()
            if row is None:
                return None
            thread, plan_row, post = row
            message_rows = (
                await session.execute(
                    select(AICommentMessage, AIAccountProfile)
                    .outerjoin(
                        AIAccountProfile,
                        AIAccountProfile.id == AICommentMessage.account_profile_id,
                    )
                    .where(
                        AICommentMessage.thread_id == thread_id,
                        AICommentMessage.role == "draft",
                    )
                    .order_by(AICommentMessage.id.asc())
                )
            ).all()
            pending_draft_id = await session.scalar(
                select(AICommentDraft.id)
                .where(
                    AICommentDraft.thread_id == thread_id,
                    AICommentDraft.status == "pending_review",
                )
                .order_by(AICommentDraft.id.desc())
                .limit(1)
            )
        try:
            participant_ids_raw = json.loads(plan_row.participant_profile_ids_json)
        except (TypeError, json.JSONDecodeError) as exc:
            raise AIDialogueError("Участники диалога повреждены") from exc
        if not isinstance(participant_ids_raw, list) or not all(
            isinstance(item, int) and not isinstance(item, bool) and item > 0
            for item in participant_ids_raw
        ):
            raise AIDialogueError("Участники диалога повреждены")
        plan = decode_plan(plan_row.message_plan_json)
        messages = tuple(
            _message_snapshot(
                message,
                position=index,
                profile_name=(profile.name if profile is not None else None),
            )
            for index, (message, profile) in enumerate(message_rows, start=1)
        )
        return AIDialogueThreadSnapshot(
            id=int(thread.id),
            channel_id=(int(thread.channel_id) if thread.channel_id is not None else None),
            post_id=(int(thread.root_post_id) if thread.root_post_id is not None else None),
            telegram_message_id=(
                int(post.telegram_message_id) if post is not None else None
            ),
            status=thread.status,
            max_messages=int(thread.max_messages),
            expires_at=thread.expires_at,
            participant_profile_ids=tuple(participant_ids_raw),
            plan=plan,
            next_position=int(plan_row.next_position),
            accepted_messages=int(plan_row.accepted_messages),
            topic=plan_row.topic,
            min_interval_seconds=int(plan_row.min_interval_seconds),
            max_interval_seconds=int(plan_row.max_interval_seconds),
            root_post_revision=int(plan_row.root_post_revision),
            root_post_hash=plan_row.root_post_hash,
            version=int(thread.version),
            created_at=thread.created_at,
            updated_at=thread.updated_at,
            messages=messages,
            pending_draft_id=(int(pending_draft_id) if pending_draft_id is not None else None),
        )

    async def list_ai_dialogue_threads(
        self,
        *,
        channel_id: int | None = None,
        limit: int = 20,
    ) -> tuple[AIDialogueThreadSnapshot, ...]:
        if channel_id is not None:
            channel_id = _id(channel_id, field="ID канала")
        limit = max(1, min(int(limit), 50))
        statement = select(AICommentThread.id).order_by(
            AICommentThread.updated_at.desc(), AICommentThread.id.desc()
        ).limit(limit)
        if channel_id is not None:
            statement = statement.where(AICommentThread.channel_id == channel_id)
        async with self.sessions() as session:
            ids = tuple(int(item) for item in (await session.scalars(statement)).all())
        result = []
        for item in ids:
            snapshot = await self.get_ai_dialogue_thread(item)
            if snapshot is not None:
                result.append(snapshot)
        return tuple(result)

    async def build_ai_dialogue_reply_context(
        self,
        *,
        thread_id: int,
        recent_post_limit: int = 5,
        knowledge_limit: int = 4,
    ) -> AIDialogueReplyContext:
        thread = await self.get_ai_dialogue_thread(thread_id)
        if thread is None:
            raise AIDialogueError("Диалог не найден")
        now = utcnow()
        if thread.expires_at is not None and thread.expires_at <= now:
            await self.cancel_ai_dialogue_thread(
                thread.id, reason="Срок диалога истёк"
            )
            raise AIDialogueError("Срок диалога истёк")
        if thread.status != "planned" or thread.finished:
            raise AIDialogueError("Диалог не готов к следующей реплике")
        if thread.channel_id is None or thread.post_id is None:
            raise AIDialogueError("Канал или публикация диалога удалены")
        plan_item = thread.plan[thread.next_position - 1]
        base = await self.build_ai_single_comment_context(
            channel_id=thread.channel_id,
            post_id=thread.post_id,
            account_profile_id=plan_item.profile_id,
            recent_post_limit=max(AI_DIALOGUE_CONTEXT_POSTS - 1, int(recent_post_limit)),
            knowledge_limit=knowledge_limit,
        )
        if (
            base.post.source_revision != thread.root_post_revision
            or base.post.normalized_text_hash != thread.root_post_hash
        ):
            raise AIDialogueError("Корневая публикация изменилась; диалог остановлен")
        return build_dialogue_reply_context(
            thread=thread,
            position=thread.next_position,
            base=base,
            prior_messages=thread.messages,
        )

    async def create_ai_dialogue_generation_job(
        self,
        *,
        context: AIDialogueReplyContext,
        requested_by_admin_id: int,
        request_nonce: str | None = None,
    ) -> int:
        requested_by_admin_id = _id(requested_by_admin_id, field="ID администратора")
        nonce = request_nonce or uuid.uuid4().hex
        if re.fullmatch(r"[A-Za-z0-9_-]{8,64}", nonce) is None:
            raise ValueError("Некорректный nonce генерации")
        now = utcnow()
        key = (
            f"dialogue:{context.thread.id}:{context.position}:"
            f"{context.base.account_profile.id}:{context.base.post.source_revision}:{nonce}"
        )
        if len(key) > 160:
            key = "dialogue:" + hashlib.sha256(key.encode()).hexdigest()
        async with self.sessions() as session:
            thread = await session.get(AICommentThread, context.thread.id)
            plan = await session.scalar(
                select(AICommentThreadPlan).where(
                    AICommentThreadPlan.thread_id == context.thread.id
                )
            )
            post = await session.get(AIChannelPost, context.base.post.id)
            if (
                thread is None
                or plan is None
                or thread.status != "planned"
                or int(plan.next_position) != context.position
                or post is None
                or post.deleted_at is not None
                or int(post.source_revision) != context.base.post.source_revision
                or post.normalized_text_hash != context.base.post.normalized_text_hash
            ):
                raise AIDialogueError("Диалог или публикация изменились до запуска")
            thread.status = "generating"
            thread.updated_at = now
            row = AIGenerationJob(
                idempotency_key=key,
                channel_id=context.base.channel_id,
                post_id=context.base.post.id,
                thread_id=context.thread.id,
                requested_mode="dialogue",
                requested_count=1,
                source_post_revision=context.base.post.source_revision,
                requested_by_admin_id=requested_by_admin_id,
                status="running",
                attempts=1,
                max_attempts=1,
                due_at=now,
                locked_at=now,
                locked_by="admin-manual-step12",
                error_class=None,
                safe_error=None,
                created_at=now,
                updated_at=now,
                completed_at=None,
            )
            session.add(row)
            await session.commit()
            await session.refresh(row)
            return int(row.id)

    async def complete_ai_dialogue_generation(
        self,
        *,
        job_id: int,
        context: AIDialogueReplyContext,
        validation: AICommentValidationResult,
        model_name: str,
        request_id_safe: str | None,
        input_tokens: int,
        output_tokens: int,
        cached_tokens: int,
        latency_ms: int,
    ) -> int:
        job_id = _id(job_id, field="ID generation job")
        model_name = _safe_model(model_name)
        request_id_safe = _safe_request_id(request_id_safe)
        for name, value in {
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "cached_tokens": cached_tokens,
            "latency_ms": latency_ms,
        }.items():
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise ValueError(f"Некорректное значение {name}")
        now = utcnow()
        payload = validation.validation_payload()
        payload.update(
            {
                "dialogue_validator_version": AI_DIALOGUE_VALIDATOR_VERSION,
                "thread_id": context.thread.id,
                "position": context.position,
                "reply_to_ref": context.reply_to_ref,
            }
        )
        text = validation.normalized_text if validation.accepted else None
        status = "pending_review" if validation.accepted else "rejected"
        async with self.sessions() as session:
            job = await session.get(AIGenerationJob, job_id)
            thread = await session.get(AICommentThread, context.thread.id)
            plan = await session.scalar(
                select(AICommentThreadPlan).where(
                    AICommentThreadPlan.thread_id == context.thread.id
                )
            )
            post = await session.get(AIChannelPost, context.base.post.id)
            if (
                job is None
                or job.status != "running"
                or job.thread_id != context.thread.id
                or thread is None
                or thread.status != "generating"
                or plan is None
                or int(plan.next_position) != context.position
                or post is None
                or post.deleted_at is not None
                or int(post.source_revision) != context.base.post.source_revision
                or post.normalized_text_hash != context.base.post.normalized_text_hash
            ):
                raise AIDialogueError("Диалог изменился во время генерации")
            profile = await session.get(AIAccountProfile, context.base.account_profile.id)
            if profile is None or not profile.enabled:
                raise AIDialogueError("Профиль выключен во время генерации")
            draft = AICommentDraft(
                generation_job_id=job.id,
                thread_id=thread.id,
                post_id=post.id,
                account_profile_id=profile.id,
                supersedes_draft_id=None,
                alternative_index=context.position,
                revision=1,
                lock_version=1,
                reply_to_kind="post" if context.position == 1 else "message",
                reply_to_local_message_id=context.reply_to_local_message_id,
                reply_to_telegram_message_id=(
                    post.telegram_message_id if context.position == 1 else None
                ),
                text=text,
                text_hash=hashlib.sha256((text or "").encode()).hexdigest(),
                topic=validation.topic if validation.accepted else None,
                confidence=validation.confidence,
                knowledge_refs_json=json.dumps(
                    list(validation.knowledge_refs), ensure_ascii=False, separators=(",", ":")
                ),
                warnings_json=json.dumps(
                    list(validation.warnings), ensure_ascii=False, separators=(",", ":")
                ),
                validation_json=json.dumps(
                    payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
                ),
                status=status,
                model_name=model_name,
                prompt_version=AI_DIALOGUE_PROMPT_VERSION,
                schema_version=AI_DIALOGUE_SCHEMA_VERSION,
                source_post_revision=context.base.post.source_revision,
                source_post_hash=context.base.post.normalized_text_hash,
                estimated_cost_usd=None,
                decision_idempotency_key=None,
                created_at=now,
                updated_at=now,
                reviewed_at=None,
                reviewed_by=None,
                expires_at=now + timedelta(days=AI_COMMENT_DRAFT_TTL_DAYS),
            )
            usage = AIUsageStat(
                generation_job_id=job.id,
                model_name=model_name,
                request_id_safe=request_id_safe,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                cached_tokens=cached_tokens,
                tool_calls=0,
                latency_ms=latency_ms,
                cost_usd=None,
                success=True,
                error_class=None,
                created_at=now,
            )
            session.add_all([draft, usage])
            await session.flush()
            job.status = "succeeded"
            job.locked_at = None
            job.locked_by = None
            job.completed_at = now
            job.updated_at = now
            thread.status = "review" if validation.accepted else "planned"
            thread.updated_at = now
            await session.commit()
            return int(draft.id)

    async def fail_ai_dialogue_generation(
        self,
        *,
        job_id: int,
        thread_id: int,
        model_name: str,
        request_id_safe: str | None,
        input_tokens: int,
        output_tokens: int,
        cached_tokens: int,
        latency_ms: int,
        error_class: str,
        safe_error: str,
    ) -> None:
        await self.fail_ai_single_generation(
            job_id=job_id,
            model_name=model_name,
            request_id_safe=request_id_safe,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cached_tokens=cached_tokens,
            latency_ms=latency_ms,
            error_class=error_class,
            safe_error=safe_error,
        )
        thread_id = _id(thread_id, field="ID диалога")
        async with self.sessions() as session:
            await session.execute(
                update(AICommentThread)
                .where(
                    AICommentThread.id == thread_id,
                    AICommentThread.status == "generating",
                )
                .values(status="planned", updated_at=utcnow())
            )
            await session.commit()

    async def accept_ai_dialogue_draft(
        self,
        *,
        thread_id: int,
        draft_id: int,
        reviewed_by: int,
    ) -> AIDialogueThreadSnapshot:
        thread_id = _id(thread_id, field="ID диалога")
        draft_id = _id(draft_id, field="ID черновика")
        reviewed_by = _id(reviewed_by, field="ID администратора")
        now = utcnow()
        async with self.sessions() as session:
            thread = await session.get(AICommentThread, thread_id)
            plan = await session.scalar(
                select(AICommentThreadPlan).where(AICommentThreadPlan.thread_id == thread_id)
            )
            draft = await session.get(AICommentDraft, draft_id)
            if (
                thread is None
                or plan is None
                or draft is None
                or draft.thread_id != thread_id
                or thread.status != "review"
                or draft.status != "pending_review"
                or not draft.text
                or int(draft.alternative_index) != int(plan.next_position)
            ):
                raise AIDialogueError("Черновик устарел или уже обработан")
            post = await session.get(AIChannelPost, thread.root_post_id)
            if (
                post is None
                or post.deleted_at is not None
                or int(post.source_revision) != int(plan.root_post_revision)
                or post.normalized_text_hash != plan.root_post_hash
            ):
                raise AIDialogueError("Корневая публикация изменилась")
            if int(plan.next_position) > 1:
                target = await session.get(AICommentMessage, draft.reply_to_local_message_id)
                if (
                    target is None
                    or target.thread_id != thread_id
                    or target.status != "approved"
                ):
                    raise AIDialogueError("Целевая реплика удалена или устарела")
            message = AICommentMessage(
                thread_id=thread_id,
                account_profile_id=draft.account_profile_id,
                telegram_message_id=None,
                reply_to_local_message_id=draft.reply_to_local_message_id,
                reply_to_telegram_message_id=draft.reply_to_telegram_message_id,
                role="draft",
                text=draft.text,
                text_hash=draft.text_hash,
                status="approved",
                generated_at=draft.created_at,
                approved_at=now,
                published_at=None,
                failure_reason_safe=None,
                created_at=now,
                updated_at=now,
            )
            session.add(message)
            draft.status = "approved"
            draft.reviewed_at = now
            draft.reviewed_by = reviewed_by
            draft.updated_at = now
            plan.accepted_messages = int(plan.accepted_messages) + 1
            plan.next_position = int(plan.next_position) + 1
            plan.version = int(plan.version) + 1
            plan.updated_at = now
            thread.version = int(thread.version) + 1
            thread.status = (
                "completed"
                if int(plan.next_position) > int(thread.max_messages)
                else "planned"
            )
            thread.updated_at = now
            await session.commit()
        result = await self.get_ai_dialogue_thread(thread_id)
        if result is None:
            raise AIDialogueError("Диалог не удалось прочитать после подтверждения")
        return result

    async def reject_ai_dialogue_draft(
        self,
        *,
        thread_id: int,
        draft_id: int,
        reviewed_by: int,
    ) -> AIDialogueThreadSnapshot:
        thread_id = _id(thread_id, field="ID диалога")
        draft_id = _id(draft_id, field="ID черновика")
        reviewed_by = _id(reviewed_by, field="ID администратора")
        now = utcnow()
        async with self.sessions() as session:
            thread = await session.get(AICommentThread, thread_id)
            draft = await session.get(AICommentDraft, draft_id)
            if (
                thread is None
                or draft is None
                or draft.thread_id != thread_id
                or draft.status != "pending_review"
            ):
                raise AIDialogueError("Черновик устарел или уже обработан")
            draft.status = "rejected"
            draft.reviewed_at = now
            draft.reviewed_by = reviewed_by
            draft.updated_at = now
            if thread.status == "review":
                thread.status = "planned"
                thread.updated_at = now
            await session.commit()
        result = await self.get_ai_dialogue_thread(thread_id)
        if result is None:
            raise AIDialogueError("Диалог не удалось прочитать после отклонения")
        return result

    async def cancel_ai_dialogue_thread(
        self,
        thread_id: int,
        *,
        reason: str = "Отменено администратором",
    ) -> AIDialogueThreadSnapshot:
        thread_id = _id(thread_id, field="ID диалога")
        safe_reason = " ".join(str(reason).split())[:500] or "Отменено"
        now = utcnow()
        async with self.sessions() as session:
            thread = await session.get(AICommentThread, thread_id)
            plan = await session.scalar(
                select(AICommentThreadPlan).where(AICommentThreadPlan.thread_id == thread_id)
            )
            if thread is None or plan is None:
                raise AIDialogueError("Диалог не найден")
            if thread.status not in {"completed", "cancelled"}:
                thread.status = "cancelled"
                thread.version = int(thread.version) + 1
                thread.updated_at = now
                plan.cancel_reason_safe = safe_reason
                plan.version = int(plan.version) + 1
                plan.updated_at = now
                await session.execute(
                    update(AICommentDraft)
                    .where(
                        AICommentDraft.thread_id == thread_id,
                        AICommentDraft.status == "pending_review",
                    )
                    .values(
                        status="expired",
                        reviewed_at=now,
                        updated_at=now,
                    )
                )
            await session.commit()
        result = await self.get_ai_dialogue_thread(thread_id)
        if result is None:
            raise AIDialogueError("Диалог не удалось прочитать после отмены")
        return result

    async def record_ai_external_reply(
        self,
        *,
        thread_id: int,
        telegram_message_id: int,
        reply_to_local_message_id: int,
        text: str,
        timezone_name: str = "UTC",
        observed_at: datetime | None = None,
    ) -> int:
        """Persist a real external reply and unlock one bounded daily bonus pool."""

        thread_id = _id(thread_id, field="ID диалога")
        telegram_message_id = _id(telegram_message_id, field="Telegram ID сообщения")
        reply_to_local_message_id = _id(
            reply_to_local_message_id, field="ID исходного комментария"
        )
        normalized = " ".join(str(text).split()).strip()
        if not normalized or len(normalized) > 4_000:
            raise ValueError("Некорректный текст входящего ответа")
        now = observed_at or utcnow()
        target_profile_id: int | None = None
        async with self.sessions() as session:
            target = await session.get(AICommentMessage, reply_to_local_message_id)
            if (
                target is None
                or target.thread_id != thread_id
                or target.role != "published"
                or target.account_profile_id is None
            ):
                raise AIDialogueError(
                    "Бонус выдаётся только за реальный ответ на опубликованный комментарий профиля"
                )
            target_profile_id = int(target.account_profile_id)
            external = AICommentMessage(
                thread_id=thread_id,
                account_profile_id=None,
                telegram_message_id=telegram_message_id,
                reply_to_local_message_id=target.id,
                reply_to_telegram_message_id=target.telegram_message_id,
                role="external",
                text=normalized,
                text_hash=hashlib.sha256(normalized.encode()).hexdigest(),
                status="observed",
                generated_at=None,
                approved_at=None,
                published_at=now,
                failure_reason_safe=None,
                created_at=now,
                updated_at=now,
            )
            session.add(external)
            try:
                await session.commit()
            except IntegrityError:
                await session.rollback()
                existing = await session.scalar(
                    select(AICommentMessage.id).where(
                        AICommentMessage.thread_id == thread_id,
                        AICommentMessage.telegram_message_id == telegram_message_id,
                    )
                )
                if existing is None:
                    raise
                return int(existing)
            await session.refresh(external)
            external_id = int(external.id)
        await self.grant_ai_reply_bonus(
            target_profile_id,
            source_telegram_message_id=telegram_message_id,
            thread_id=thread_id,
            now=now,
            timezone_name=timezone_name,
        )
        return external_id
