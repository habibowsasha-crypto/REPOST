from __future__ import annotations

import hashlib
import json
import re
import uuid
from datetime import timedelta
from decimal import Decimal

from sqlalchemy import select, update

from .ai_comment_generation import (
    AI_COMMENT_DRAFT_TTL_DAYS,
    AI_COMMENT_PROMPT_VERSION,
    AI_COMMENT_SCHEMA_VERSION,
    AICommentDraftSnapshot,
    AICommentGenerationError,
    AICommentValidationResult,
    AIKnowledgeChunkSnapshot,
    AISingleCommentContext,
    build_single_comment_context,
    decode_object,
    decode_string_list,
    score_knowledge_chunk,
    tokenize_retrieval_text,
)
from .ai_comments_models import (
    AIAccountProfile,
    AIChannelPost,
    AICommentDraft,
    AIGenerationJob,
    AIKnowledgeChunk,
    AIKnowledgeSource,
    AIUsageStat,
)
from .models import utcnow

MAX_DATABASE_ID = 9_223_372_036_854_775_807
_SAFE_ID_RE = re.compile(r"[A-Za-z0-9_.:-]{1,160}")
_SAFE_ERROR_RE = re.compile(r"[a-z0-9_]{1,64}")


def _database_id(value: int, *, field: str) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 0 < value <= MAX_DATABASE_ID
    ):
        raise ValueError(f"Некорректный {field}")
    return value


def _safe_request_id(value: str | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if _SAFE_ID_RE.fullmatch(text) is None:
        return None
    return text


def _safe_model(value: str) -> str:
    text = str(value).strip()
    if re.fullmatch(r"[A-Za-z0-9_.:-]{1,96}", text) is None:
        raise ValueError("Некорректное название модели")
    return text


def _draft_snapshot(
    draft: AICommentDraft,
    post: AIChannelPost | None,
    profile: AIAccountProfile | None,
) -> AICommentDraftSnapshot:
    return AICommentDraftSnapshot(
        id=int(draft.id),
        generation_job_id=(
            int(draft.generation_job_id)
            if draft.generation_job_id is not None
            else None
        ),
        channel_id=int(post.channel_id) if post is not None and post.channel_id is not None else None,
        post_id=int(draft.post_id) if draft.post_id is not None else None,
        telegram_message_id=(
            int(post.telegram_message_id) if post is not None else None
        ),
        account_profile_id=(
            int(draft.account_profile_id)
            if draft.account_profile_id is not None
            else None
        ),
        account_profile_name=profile.name if profile is not None else None,
        text=draft.text,
        topic=draft.topic,
        confidence=(Decimal(draft.confidence) if draft.confidence is not None else None),
        knowledge_refs=decode_string_list(
            draft.knowledge_refs_json,
            field="ai_comment_drafts.knowledge_refs_json",
        ),
        warnings=decode_string_list(
            draft.warnings_json,
            field="ai_comment_drafts.warnings_json",
        ),
        validation=decode_object(
            draft.validation_json,
            field="ai_comment_drafts.validation_json",
        ),
        status=draft.status,
        model_name=draft.model_name,
        prompt_version=draft.prompt_version,
        schema_version=draft.schema_version,
        source_post_revision=int(draft.source_post_revision),
        source_post_hash=draft.source_post_hash,
        created_at=draft.created_at,
        expires_at=draft.expires_at,
    )


class AICommentGenerationRepositoryMixin:
    async def retrieve_ai_knowledge_chunks(
        self,
        query: str,
        *,
        limit: int = 4,
    ) -> tuple[AIKnowledgeChunkSnapshot, ...]:
        if not isinstance(query, str):
            raise TypeError("Поисковый запрос должен быть строкой")
        limit = max(0, min(int(limit), 8))
        if limit == 0:
            return ()
        query_tokens = tokenize_retrieval_text(query)
        if not query_tokens:
            return ()
        async with self.sessions() as session:
            rows = (
                await session.execute(
                    select(AIKnowledgeChunk, AIKnowledgeSource)
                    .join(
                        AIKnowledgeSource,
                        AIKnowledgeSource.id == AIKnowledgeChunk.source_id,
                    )
                    .where(
                        AIKnowledgeChunk.review_status == "ready",
                        AIKnowledgeChunk.index_eligible.is_(True),
                        AIKnowledgeChunk.retired_at.is_(None),
                        AIKnowledgeChunk.chunk_text.is_not(None),
                        AIKnowledgeSource.processing_status == "ready",
                        AIKnowledgeSource.review_status == "ready",
                        AIKnowledgeSource.retired_at.is_(None),
                    )
                    .order_by(AIKnowledgeChunk.id)
                    .limit(250)
                )
            ).all()
        scored: list[AIKnowledgeChunkSnapshot] = []
        for chunk, source in rows:
            text = str(chunk.chunk_text or "").strip()
            if not text:
                continue
            score = score_knowledge_chunk(
                query_tokens,
                topic=str(chunk.topic or ""),
                text=text,
            )
            if score <= 0:
                continue
            scored.append(
                AIKnowledgeChunkSnapshot(
                    id=int(chunk.id),
                    source_id=int(source.id),
                    source_title=source.title,
                    source_filename=source.filename,
                    page_from=int(chunk.page_from),
                    page_to=int(chunk.page_to),
                    topic=chunk.topic,
                    chunk_text=text,
                    chunk_hash=chunk.chunk_hash,
                    score=score,
                )
            )
        scored.sort(key=lambda item: (-item.score, item.id))
        return tuple(scored[:limit])

    async def build_ai_single_comment_context(
        self,
        *,
        channel_id: int,
        post_id: int,
        account_profile_id: int,
        recent_post_limit: int = 5,
        knowledge_limit: int = 4,
    ) -> AISingleCommentContext:
        channel_id = _database_id(channel_id, field="ID канала")
        post_id = _database_id(post_id, field="ID публикации")
        account_profile_id = _database_id(
            account_profile_id,
            field="ID профиля аккаунта",
        )
        post = await self.get_ai_channel_post(channel_id, post_id)
        if post is None:
            raise AICommentGenerationError("Публикация не найдена")
        memory = await self.get_ai_channel_memory(channel_id)
        profile = await self.get_ai_account_profile(account_profile_id)
        if profile is None:
            raise AICommentGenerationError("Профиль аккаунта не найден")
        query = "\n".join(
            value
            for value in (
                post.text,
                post.media_caption,
                memory.profile.topic,
                " ".join(post.detected_topics),
            )
            if value
        )
        knowledge = await self.retrieve_ai_knowledge_chunks(
            query,
            limit=knowledge_limit,
        )
        return build_single_comment_context(
            channel_id=channel_id,
            post=post,
            memory=memory,
            account_profile=profile,
            knowledge_chunks=knowledge,
            recent_post_limit=recent_post_limit,
        )

    async def create_ai_single_generation_job(
        self,
        *,
        context: AISingleCommentContext,
        requested_by_admin_id: int,
        request_nonce: str | None = None,
    ) -> int:
        requested_by_admin_id = _database_id(
            requested_by_admin_id,
            field="ID администратора",
        )
        nonce = request_nonce or uuid.uuid4().hex
        if re.fullmatch(r"[A-Za-z0-9_-]{8,64}", nonce) is None:
            raise ValueError("Некорректный nonce генерации")
        idempotency_key = (
            f"single:{context.channel_id}:{context.post.id}:"
            f"{context.post.source_revision}:{context.account_profile.id}:{nonce}"
        )
        if len(idempotency_key) > 160:
            idempotency_key = "single:" + hashlib.sha256(
                idempotency_key.encode("utf-8")
            ).hexdigest()
        now = utcnow()
        async with self.sessions() as session:
            post = await session.get(AIChannelPost, context.post.id)
            if (
                post is None
                or post.channel_id != context.channel_id
                or int(post.source_revision) != context.post.source_revision
                or post.normalized_text_hash != context.post.normalized_text_hash
                or post.deleted_at is not None
            ):
                raise AICommentGenerationError(
                    "Публикация изменилась до запуска генерации"
                )
            profile = await session.get(AIAccountProfile, context.account_profile.id)
            if profile is None or not profile.enabled:
                raise AICommentGenerationError("Профиль аккаунта больше недоступен")
            row = AIGenerationJob(
                idempotency_key=idempotency_key,
                channel_id=context.channel_id,
                post_id=context.post.id,
                thread_id=None,
                requested_mode="single",
                requested_count=1,
                source_post_revision=context.post.source_revision,
                requested_by_admin_id=requested_by_admin_id,
                status="running",
                attempts=1,
                max_attempts=1,
                due_at=now,
                locked_at=now,
                locked_by="admin-manual-step11",
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

    async def complete_ai_single_generation(
        self,
        *,
        job_id: int,
        context: AISingleCommentContext,
        validation: AICommentValidationResult,
        model_name: str,
        request_id_safe: str | None,
        input_tokens: int,
        output_tokens: int,
        cached_tokens: int,
        latency_ms: int,
    ) -> AICommentDraftSnapshot:
        job_id = _database_id(job_id, field="ID generation job")
        model_name = _safe_model(model_name)
        request_id_safe = _safe_request_id(request_id_safe)
        for field, value in {
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "cached_tokens": cached_tokens,
            "latency_ms": latency_ms,
        }.items():
            if (
                not isinstance(value, int)
                or isinstance(value, bool)
                or value < 0
                or value > MAX_DATABASE_ID
            ):
                raise ValueError(f"Некорректное значение {field}")

        now = utcnow()
        validation_json = json.dumps(
            validation.validation_payload(),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        knowledge_refs_json = json.dumps(
            list(validation.knowledge_refs),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        warnings_json = json.dumps(
            list(validation.warnings),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        draft_text = validation.normalized_text if validation.accepted else None
        text_hash = hashlib.sha256((draft_text or "").encode("utf-8")).hexdigest()
        status = "pending_review" if validation.accepted else "rejected"
        topic = validation.topic if validation.accepted else None

        async with self.sessions() as session:
            job = await session.get(AIGenerationJob, job_id)
            if job is None or job.status != "running":
                raise AICommentGenerationError("Generation job уже завершён или отсутствует")
            if (
                job.post_id != context.post.id
                or job.channel_id != context.channel_id
                or int(job.source_post_revision) != context.post.source_revision
            ):
                raise AICommentGenerationError("Generation job не совпадает с контекстом")
            post = await session.get(AIChannelPost, context.post.id)
            if (
                post is None
                or post.deleted_at is not None
                or int(post.source_revision) != context.post.source_revision
                or post.normalized_text_hash != context.post.normalized_text_hash
            ):
                raise AICommentGenerationError(
                    "Публикация изменилась во время генерации; результат отброшен"
                )
            profile = await session.get(AIAccountProfile, context.account_profile.id)
            if profile is None or not profile.enabled:
                raise AICommentGenerationError(
                    "Профиль аккаунта удалён или выключен во время генерации"
                )

            draft = AICommentDraft(
                generation_job_id=job.id,
                thread_id=None,
                post_id=post.id,
                account_profile_id=profile.id,
                supersedes_draft_id=None,
                alternative_index=1,
                revision=1,
                lock_version=1,
                reply_to_kind="post",
                reply_to_local_message_id=None,
                reply_to_telegram_message_id=post.telegram_message_id,
                text=draft_text,
                text_hash=text_hash,
                topic=topic,
                confidence=validation.confidence,
                knowledge_refs_json=knowledge_refs_json,
                warnings_json=warnings_json,
                validation_json=validation_json,
                status=status,
                model_name=model_name,
                prompt_version=AI_COMMENT_PROMPT_VERSION,
                schema_version=AI_COMMENT_SCHEMA_VERSION,
                source_post_revision=context.post.source_revision,
                source_post_hash=context.post.normalized_text_hash,
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
            result = await session.execute(
                update(AIGenerationJob)
                .where(
                    AIGenerationJob.id == job.id,
                    AIGenerationJob.status == "running",
                )
                .values(
                    status="succeeded",
                    locked_at=None,
                    locked_by=None,
                    error_class=None,
                    safe_error=None,
                    completed_at=now,
                    updated_at=now,
                )
            )
            if result.rowcount != 1:
                raise AICommentGenerationError("Generation job изменился конкурентно")
            await session.commit()
            await session.refresh(draft)
            return _draft_snapshot(draft, post, profile)

    async def fail_ai_single_generation(
        self,
        *,
        job_id: int,
        model_name: str,
        request_id_safe: str | None,
        input_tokens: int,
        output_tokens: int,
        cached_tokens: int,
        latency_ms: int,
        error_class: str,
        safe_error: str,
    ) -> None:
        job_id = _database_id(job_id, field="ID generation job")
        model_name = _safe_model(model_name)
        if _SAFE_ERROR_RE.fullmatch(error_class) is None:
            error_class = "unknown"
        safe_error = str(safe_error).strip()[:500] or "Безопасно скрытая ошибка"
        now = utcnow()
        async with self.sessions() as session:
            job = await session.get(AIGenerationJob, job_id)
            if job is None or job.status != "running":
                return
            session.add(
                AIUsageStat(
                    generation_job_id=job.id,
                    model_name=model_name,
                    request_id_safe=_safe_request_id(request_id_safe),
                    input_tokens=max(0, int(input_tokens)),
                    output_tokens=max(0, int(output_tokens)),
                    cached_tokens=max(0, int(cached_tokens)),
                    tool_calls=0,
                    latency_ms=max(0, int(latency_ms)),
                    cost_usd=None,
                    success=False,
                    error_class=error_class,
                    created_at=now,
                )
            )
            await session.execute(
                update(AIGenerationJob)
                .where(
                    AIGenerationJob.id == job.id,
                    AIGenerationJob.status == "running",
                )
                .values(
                    status="failed",
                    locked_at=None,
                    locked_by=None,
                    error_class=error_class,
                    safe_error=safe_error,
                    completed_at=now,
                    updated_at=now,
                )
            )
            await session.commit()

    async def list_ai_comment_drafts(
        self,
        *,
        channel_id: int | None = None,
        limit: int = 20,
    ) -> tuple[AICommentDraftSnapshot, ...]:
        if channel_id is not None:
            channel_id = _database_id(channel_id, field="ID канала")
        limit = max(1, min(int(limit), 50))
        statement = (
            select(AICommentDraft, AIChannelPost, AIAccountProfile)
            .outerjoin(AIChannelPost, AIChannelPost.id == AICommentDraft.post_id)
            .outerjoin(
                AIAccountProfile,
                AIAccountProfile.id == AICommentDraft.account_profile_id,
            )
            .order_by(AICommentDraft.created_at.desc(), AICommentDraft.id.desc())
            .limit(limit)
        )
        if channel_id is not None:
            statement = statement.where(AIChannelPost.channel_id == channel_id)
        async with self.sessions() as session:
            rows = (await session.execute(statement)).all()
        return tuple(
            _draft_snapshot(draft, post, profile)
            for draft, post, profile in rows
        )

    async def get_ai_comment_draft(
        self,
        draft_id: int,
    ) -> AICommentDraftSnapshot | None:
        draft_id = _database_id(draft_id, field="ID черновика")
        async with self.sessions() as session:
            row = (
                await session.execute(
                    select(AICommentDraft, AIChannelPost, AIAccountProfile)
                    .outerjoin(
                        AIChannelPost,
                        AIChannelPost.id == AICommentDraft.post_id,
                    )
                    .outerjoin(
                        AIAccountProfile,
                        AIAccountProfile.id == AICommentDraft.account_profile_id,
                    )
                    .where(AICommentDraft.id == draft_id)
                )
            ).one_or_none()
        if row is None:
            return None
        draft, post, profile = row
        return _draft_snapshot(draft, post, profile)
