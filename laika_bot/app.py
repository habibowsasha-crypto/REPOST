from __future__ import annotations

import asyncio
import logging
import os

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from . import __version__
from .alerts import CriticalAlertService
from .config import get_settings
from .db import Database
from .handlers import Handlers
from .openai_gateway import OpenAIGateway
from .recovery import AutomaticRecoveryService
from .services.jobs import JobService
from .services.telegram_accounts import ClientPool, LoginManager, SessionCipher


async def _handle_supervised_completion(
    done: set[asyncio.Task],
    polling: asyncio.Task,
    alerts: CriticalAlertService | None,
) -> None:
    """Fail closed when polling and a critical worker finish together."""

    background_done = sorted(
        (task for task in done if task is not polling),
        key=lambda task: task.get_name(),
    )
    for task in background_done:
        if task.cancelled():
            exc = RuntimeError("Critical background task was cancelled")
            if alerts is not None:
                await alerts.notify_critical_task_exit(task.get_name(), exc)
            raise RuntimeError(
                f"Critical background task cancelled unexpectedly: {task.get_name()}"
            ) from exc
        exc = task.exception()
        if exc is None:
            continue
        if alerts is not None:
            await alerts.notify_critical_task_exit(task.get_name(), exc)
        raise RuntimeError(
            f"Critical background task stopped: {task.get_name()}"
        ) from exc

    if background_done:
        stopped = background_done[0]
        if alerts is not None:
            await alerts.notify_critical_task_exit(stopped.get_name(), None)
        raise RuntimeError(
            f"Critical background task exited unexpectedly: {stopped.get_name()}"
        )

    if polling not in done:
        raise RuntimeError("Supervisor woke without a completed task")
    try:
        await polling
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # noqa: BLE001
        if alerts is not None:
            await alerts.notify_critical_task_exit("telegram-polling", exc)
        raise



async def main() -> None:
    settings = get_settings()
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    logger = logging.getLogger(__name__)
    logger.info("Laika Bot v%s starting", __version__)
    if settings.database_url.startswith("sqlite"):
        os.makedirs("data", exist_ok=True)

    db = Database(settings.database_url)
    pool: ClientPool | None = None
    bot: Bot | None = None
    login_manager: LoginManager | None = None
    handlers: Handlers | None = None
    openai_gateway: OpenAIGateway | None = None
    supervised: list[asyncio.Task] = []
    alerts: CriticalAlertService | None = None
    try:
        if not await db.acquire_instance_lock():
            raise RuntimeError(
                "Another LikeBot instance already owns the database queue lock. "
                "Run exactly one Railway replica."
            )
        # Hold the PostgreSQL singleton lock before create_all/compat migrations.
        # Otherwise two replicas starting simultaneously could race on ALTER TABLE
        # even though only one of them would later be allowed to run the workers.
        await db.init()

        cipher = SessionCipher(settings.session_encryption_key)
        login_manager = LoginManager(settings)
        pool = ClientPool(settings, db, cipher)
        jobs = JobService(settings, db, pool)
        openai_gateway = OpenAIGateway(settings)
        gateway_status = openai_gateway.status
        logger.info(
            "OpenAI gateway initialized enabled=%s key_configured=%s model=%s sdk=%s timeout_seconds=%s max_retries=%s",
            gateway_status.railway_enabled,
            gateway_status.key_configured,
            gateway_status.model,
            gateway_status.sdk_version,
            gateway_status.timeout_seconds,
            gateway_status.max_retries,
        )
        if gateway_status.railway_enabled and not gateway_status.key_configured:
            logger.warning(
                "OpenAI gateway is enabled but OPENAI_API_KEY is not configured; DEV probe remains blocked"
            )

        bot = Bot(
            token=settings.bot_token,
            default=DefaultBotProperties(parse_mode=ParseMode.HTML),
        )

        async def send_admin_alert(text: str) -> object:
            return await bot.send_message(settings.admin_id, text)

        alerts = CriticalAlertService(settings, db, jobs, send_admin_alert)
        recovery = AutomaticRecoveryService(settings, db, jobs, pool, alerts)
        pool.set_problem_notifier(alerts.notify_problem_account)
        handlers = Handlers(
            settings,
            db,
            cipher,
            login_manager,
            pool,
            jobs,
            recovery,
            openai_gateway=openai_gateway,
        )
        dp = Dispatcher()
        dp.include_router(handlers.router)

        worker_specs = (
            ("account-health", "account_health", jobs.account_health_loop),
            ("channel-monitor", "channel_monitor", jobs.monitor_loop),
            ("join-worker", "join_worker", jobs.join_worker_loop),
            ("reaction-worker", "reaction_worker", jobs.reaction_worker_loop),
            ("view-worker", "view_worker", jobs.view_worker_loop),
        )
        if recovery.enabled:
            background = [
                asyncio.create_task(
                    recovery.run_restartable_worker(public_name, health_name, factory),
                    name=public_name,
                )
                for public_name, health_name, factory in worker_specs
            ]
            background.append(
                asyncio.create_task(
                    recovery.monitor_loop(), name="automatic-recovery"
                )
            )
        else:
            background = [
                asyncio.create_task(factory(), name=public_name)
                for public_name, _health_name, factory in worker_specs
            ]

        if alerts.enabled:
            background.append(
                asyncio.create_task(
                    alerts.monitor_loop(), name="critical-alert-monitor"
                )
            )
        polling = asyncio.create_task(
            dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types()),
            name="telegram-polling",
        )
        supervised = [polling, *background]

        done, _ = await asyncio.wait(
            supervised,
            return_when=asyncio.FIRST_COMPLETED,
        )
        await _handle_supervised_completion(done, polling, alerts)
    finally:
        for task in supervised:
            if not task.done():
                task.cancel()
        if supervised:
            await asyncio.gather(*supervised, return_exceptions=True)
        if handlers is not None:
            await handlers.close()
        if openai_gateway is not None:
            await openai_gateway.close()
        if login_manager is not None:
            await login_manager.close()
        if pool is not None:
            await pool.close()
        if bot is not None:
            await bot.session.close()
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
