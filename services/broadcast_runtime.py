from __future__ import annotations

import asyncio
import datetime as dt
from dataclasses import dataclass
from typing import Optional

from apscheduler.triggers.interval import IntervalTrigger
from loguru import logger
from telethon import TelegramClient
from telethon.errors import (
    ChatAdminRequiredError,
    ChatWriteForbiddenError,
    FloodWaitError,
    SlowModeWaitError,
)
from telethon.sessions import StringSession
from telethon.tl.types import Channel, Chat

from config import API_HASH, API_ID, conn, scheduler
from utils.telegram import create_broadcast_data, get_entity_by_id, gid_key


@dataclass(frozen=True)
class WorkingGroup:
    group_id: int
    identifier: str
    title: str


def _remove_job(job_id: str) -> None:
    job = scheduler.get_job(job_id)
    if job is not None:
        scheduler.remove_job(job_id)


def _mark_broadcast_error(user_id: int, group_id: int, reason: str, job_id: str) -> None:
    with conn:
        conn.execute(
            """
            UPDATE broadcasts
            SET is_active = 0, error_reason = ?
            WHERE user_id = ? AND group_id = ?
            """,
            (reason, user_id, gid_key(group_id)),
        )
    _remove_job(job_id)


def _current_broadcast(user_id: int, group_id: int):
    cursor = conn.cursor()
    try:
        return cursor.execute(
            """
            SELECT broadcast_text, photo_url, is_active
            FROM broadcasts
            WHERE user_id = ? AND group_id = ?
            ORDER BY rowid DESC LIMIT 1
            """,
            (user_id, gid_key(group_id)),
        ).fetchone()
    finally:
        cursor.close()


async def _send_content(client, entity, text: str, photo_url: Optional[str]) -> None:
    if photo_url:
        if text and len(text) <= 1024:
            await client.send_file(entity, photo_url, caption=text)
        else:
            await client.send_file(entity, photo_url)
            if text:
                await client.send_message(entity, text)
        return
    if text:
        await client.send_message(entity, text)
        return
    raise RuntimeError("Пустая рассылка: нет текста и изображения")


async def send_scheduled_broadcast(
    *,
    user_id: int,
    group_id: int,
    session_string: str,
    job_id: str,
    fallback_text: str,
    fallback_photo_url: Optional[str] = None,
    max_retries: int = 10,
) -> None:
    """Run one scheduled ordinary-broadcast delivery with durable state checks."""
    group_id = gid_key(group_id)
    retry_count = 0

    while retry_count < max_retries:
        current = _current_broadcast(user_id, group_id)
        if not current or not bool(current[2]):
            _remove_job(job_id)
            return

        text = current[0] if current[0] is not None else fallback_text
        photo_url = current[1] or fallback_photo_url

        try:
            async with TelegramClient(StringSession(session_string), API_ID, API_HASH) as client:
                if not await client.is_user_authorized():
                    _mark_broadcast_error(
                        user_id,
                        group_id,
                        "Сессия Telegram больше не авторизована",
                        job_id,
                    )
                    return

                entity = await get_entity_by_id(client, group_id, user_id=user_id)
                if entity is None:
                    _mark_broadcast_error(
                        user_id,
                        group_id,
                        "Не удалось восстановить Telegram entity группы",
                        job_id,
                    )
                    return

                await _send_content(client, entity, text or "", photo_url)
                with conn:
                    conn.execute(
                        """
                        INSERT INTO send_history
                            (user_id, group_id, group_name, sent_at, message_text)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            user_id,
                            group_id,
                            getattr(entity, "title", "") or "",
                            dt.datetime.now().isoformat(),
                            text or "",
                        ),
                    )
                    conn.execute(
                        """
                        UPDATE broadcasts SET error_reason = NULL
                        WHERE user_id = ? AND group_id = ?
                        """,
                        (user_id, group_id),
                    )
                logger.info(
                    f"Обычная рассылка отправлена: user={user_id}, group={group_id}"
                )
                return

        except (ChatWriteForbiddenError, ChatAdminRequiredError) as exc:
            _mark_broadcast_error(
                user_id,
                group_id,
                f"Нет права отправлять сообщения: {exc}",
                job_id,
            )
            return
        except (FloodWaitError, SlowModeWaitError) as exc:
            retry_count += 1
            wait_seconds = max(0, int(exc.seconds))
            logger.warning(
                f"{type(exc).__name__}: user={user_id}, group={group_id}, "
                f"ожидание {wait_seconds} сек"
            )
            await asyncio.sleep(wait_seconds)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            retry_count += 1
            logger.error(
                f"Ошибка обычной рассылки user={user_id}, group={group_id}, "
                f"попытка={retry_count}/{max_retries}: {type(exc).__name__}: {exc}"
            )
            if retry_count < max_retries:
                await asyncio.sleep(5)

    _mark_broadcast_error(
        user_id,
        group_id,
        f"Не удалось отправить после {max_retries} попыток",
        job_id,
    )


async def _working_groups(client: TelegramClient, user_id: int) -> list[WorkingGroup]:
    cursor = conn.cursor()
    try:
        rows = cursor.execute(
            """
            SELECT
                g.group_id,
                g.group_username,
                COALESCE(d.title, d.username, g.group_username, CAST(g.group_id AS TEXT)),
                COALESCE(d.is_available, 1)
            FROM groups AS g
            LEFT JOIN discovered_groups AS d
              ON d.user_id = g.user_id AND d.group_id = g.group_id
            WHERE g.user_id = ?
            ORDER BY lower(COALESCE(d.title, d.username, g.group_username, CAST(g.group_id AS TEXT)))
            """,
            (user_id,),
        ).fetchall()
    finally:
        cursor.close()

    result: list[WorkingGroup] = []
    seen: set[int] = set()
    for raw_group_id, identifier, title, available in rows:
        group_id = gid_key(raw_group_id)
        if group_id in seen or not bool(available):
            continue
        entity = await get_entity_by_id(
            client,
            group_id,
            user_id=user_id,
            identifier=identifier,
        )
        if not isinstance(entity, (Channel, Chat)):
            continue
        if isinstance(entity, Channel) and bool(getattr(entity, "broadcast", False)) and not bool(
            getattr(entity, "megagroup", False)
        ):
            continue
        seen.add(group_id)
        result.append(
            WorkingGroup(
                group_id=group_id,
                identifier=identifier or str(group_id),
                title=getattr(entity, "title", None) or title or str(group_id),
            )
        )
    return result


async def schedule_account_broadcast_jobs(
    *,
    user_id: int,
    text: str,
    min_minutes: int,
    max_minutes: Optional[int] = None,
    photo_url: Optional[str] = None,
    job_prefix: str = "broadcastALL",
) -> int:
    """Replace one account's scheduled ordinary-broadcast jobs."""
    min_minutes = int(min_minutes)
    if min_minutes <= 0:
        raise ValueError("Минимальный интервал должен быть больше нуля")
    if max_minutes is not None:
        max_minutes = int(max_minutes)
        if max_minutes <= min_minutes:
            raise ValueError("Максимальный интервал должен быть больше минимального")

    session_row = conn.execute(
        "SELECT session_string FROM sessions WHERE user_id = ?", (user_id,)
    ).fetchone()
    if not session_row:
        raise RuntimeError(f"Не найдена сессия аккаунта {user_id}")
    session_string = session_row[0]

    client = TelegramClient(StringSession(session_string), API_ID, API_HASH)
    await client.connect()
    try:
        if not await client.is_user_authorized():
            raise RuntimeError(f"Аккаунт {user_id} больше не авторизован")
        groups = await _working_groups(client, user_id)
    finally:
        await client.disconnect()

    old_group_ids: set[int] = set()
    for job in list(scheduler.get_jobs()):
        if job.id.startswith(f"{job_prefix}_{user_id}_"):
            try:
                old_group_ids.add(gid_key(job.id.rsplit("_", 1)[1]))
            except (ValueError, IndexError):
                pass
            scheduler.remove_job(job.id)

    # Rows belonging only to replaced mass jobs must no longer look active.
    # A separately scheduled one-group job, if present, keeps its active row.
    with conn:
        for old_group_id in old_group_ids:
            if scheduler.get_job(f"broadcast_{user_id}_{old_group_id}") is None:
                conn.execute(
                    "UPDATE broadcasts SET is_active = 0 WHERE user_id = ? AND group_id = ?",
                    (user_id, old_group_id),
                )

    if not groups:
        return 0

    average_minutes = (
        (min_minutes + max_minutes) / 2 if max_minutes is not None else float(min_minutes)
    )
    # APScheduler adds jitter on top of the base interval.  Using the minimum
    # as the base therefore produces the requested inclusive min..max range.
    trigger_minutes = float(min_minutes)
    jitter_seconds = (
        int((max_minutes - min_minutes) * 60) if max_minutes is not None else 0
    )
    spread_minutes = max(average_minutes / len(groups), 0.01)

    for index, group in enumerate(groups, start=1):
        # Replace any one-group schedule for the same destination to prevent
        # duplicate ordinary messages from two scheduler jobs.
        for prefix in ("broadcast", "broadcastALL"):
            conflict_id = f"{prefix}_{user_id}_{group.group_id}"
            if scheduler.get_job(conflict_id) is not None:
                scheduler.remove_job(conflict_id)

        job_id = f"{job_prefix}_{user_id}_{group.group_id}"
        create_broadcast_data(
            user_id,
            group.group_id,
            text,
            max(1, int(round(average_minutes))),
            photo_url,
        )
        trigger = IntervalTrigger(minutes=trigger_minutes, jitter=jitter_seconds)
        next_run = dt.datetime.now() + dt.timedelta(minutes=spread_minutes * index)
        scheduler.add_job(
            send_scheduled_broadcast,
            trigger,
            id=job_id,
            kwargs={
                "user_id": user_id,
                "group_id": group.group_id,
                "session_string": session_string,
                "job_id": job_id,
                "fallback_text": text,
                "fallback_photo_url": photo_url,
            },
            next_run_time=next_run,
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )
        logger.info(
            f"Создана задача {job_id} для {group.title}; первый запуск {next_run.isoformat()}"
        )

    if not scheduler.running:
        scheduler.start()
    return len(groups)


async def schedule_all_accounts_broadcast_jobs(
    *,
    text: str,
    min_minutes: int,
    max_minutes: Optional[int] = None,
    photo_url: Optional[str] = None,
) -> tuple[int, int]:
    """Schedule ordinary broadcasts for every connected account."""
    rows = conn.execute("SELECT user_id FROM sessions ORDER BY user_id").fetchall()
    accounts = groups = 0
    for (user_id,) in rows:
        try:
            count = await schedule_account_broadcast_jobs(
                user_id=int(user_id),
                text=text,
                min_minutes=min_minutes,
                max_minutes=max_minutes,
                photo_url=photo_url,
                job_prefix="broadcastALL",
            )
        except Exception as exc:
            logger.error(f"Не удалось запланировать рассылку аккаунта {user_id}: {exc}")
            continue
        accounts += 1
        groups += count
    return accounts, groups


def stop_group_broadcast_jobs(user_id: int, group_id: int) -> tuple[int, int]:
    """Stop every ordinary-broadcast job for one account/group pair."""
    group_id = gid_key(group_id)
    removed_jobs = 0
    for prefix in ("broadcast", "broadcastALL"):
        job_id = f"{prefix}_{user_id}_{group_id}"
        if scheduler.get_job(job_id) is not None:
            scheduler.remove_job(job_id)
            removed_jobs += 1
    with conn:
        cursor = conn.execute(
            """
            UPDATE broadcasts
            SET is_active = 0, error_reason = ?
            WHERE user_id = ? AND group_id = ? AND is_active = 1
            """,
            ("Остановлено администратором", user_id, group_id),
        )
        updated_rows = max(0, cursor.rowcount)
    return removed_jobs, updated_rows


def stop_account_broadcast_jobs(user_id: int) -> tuple[int, int]:
    """Stop every ordinary-broadcast job for one connected account."""
    removed_jobs = 0
    prefixes = (f"broadcast_{user_id}_", f"broadcastALL_{user_id}_")
    for job in list(scheduler.get_jobs()):
        if job.id.startswith(prefixes):
            scheduler.remove_job(job.id)
            removed_jobs += 1
    with conn:
        cursor = conn.execute(
            """
            UPDATE broadcasts
            SET is_active = 0, error_reason = ?
            WHERE user_id = ? AND is_active = 1
            """,
            ("Остановлено администратором", user_id),
        )
        updated_rows = max(0, cursor.rowcount)
    return removed_jobs, updated_rows


def stop_all_broadcast_jobs() -> tuple[int, int]:
    """Stop all ordinary-broadcast jobs across all accounts."""
    removed_jobs = 0
    for job in list(scheduler.get_jobs()):
        if job.id.startswith(("broadcast_", "broadcastALL_")):
            scheduler.remove_job(job.id)
            removed_jobs += 1
    with conn:
        cursor = conn.execute(
            """
            UPDATE broadcasts
            SET is_active = 0, error_reason = ?
            WHERE is_active = 1
            """,
            ("Остановлено администратором",),
        )
        updated_rows = max(0, cursor.rowcount)
    return removed_jobs, updated_rows
