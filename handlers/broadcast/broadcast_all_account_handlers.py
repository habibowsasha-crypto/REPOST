from typing import Optional

from loguru import logger
from telethon import Button

from config import (
    MEDIA_DIR,
    New_Message,
    Query,
    bot,
    broadcast_all_state_account,
    callback_message,
    callback_query,
)
from services.admin_state import clear_admin_interaction_state, is_command_event
from services.menu_ui import render_menu
from utils.logging import log_message_event


@bot.on(Query(data=lambda d: d.decode().startswith("broadcast_All_account")))
async def broadcast_all_menu(event: callback_query) -> None:
    await clear_admin_interaction_state(event.sender_id)
    keyboard = [
        [Button.inline("⏲️ Интервал во все группы", "same_IntervalAll_account")],
        [Button.inline("🎲 Разный интервал (25-35)", "diff_IntervalAll_account")]
    ]
    await render_menu(event, "Выберите режим отправки:", buttons=keyboard)


# ---------- одинаковый интервал ----------
@bot.on(Query(data=lambda d: d.decode().startswith("same_IntervalAll_account")))
async def same_interval_start(event: callback_query) -> None:
    admin_id = event.sender_id
    await clear_admin_interaction_state(admin_id)
    broadcast_all_state_account[admin_id] = {"mode": "same", "step": "text"}
    await render_menu(event, "📝 Пришлите текст рассылки для **всех** групп этого аккаунта:")


# ---------- случайный интервал ----------
@bot.on(Query(data=lambda d: d.decode().startswith("diff_IntervalAll_account")))
async def diff_interval_start(event: callback_query) -> None:
    admin_id = event.sender_id
    await clear_admin_interaction_state(admin_id)
    broadcast_all_state_account[admin_id] = {"mode": "diff", "step": "text"}
    await render_menu(event, "📝 Пришлите текст рассылки, потом спрошу границы интервала:")


# ---------- мастер-диалог (текст → интервалы) ----------
@bot.on(New_Message(func=lambda e: e.sender_id in broadcast_all_state_account and not is_command_event(e)))
async def broadcast_all_dialog(event: callback_message) -> None:
    st = broadcast_all_state_account[event.sender_id]
    log_message_event(event, "обработка диалога рассылки по аккаунтам")
    # шаг 1 — получили текст
    if st["step"] == "text":
        st["text"] = event.text
        if st["mode"] == "same":
            st["step"] = "interval"
            await event.respond("⏲️ Введите интервал (минуты, одно число):")
        else:
            st["step"] = "min"
            await event.respond("🔢 Минимальный интервал (мин):")
        return

    # одинаковый интервал
    if st["mode"] == "same" and st["step"] == "interval":
        try:
            min_time = int(event.text)
        except ValueError:
            await event.respond("Некорректный формат числа. Попробуйте ещё раз или нажмите /start.")
            return
        if min_time <= 0:
            await event.respond("⚠ Должно быть положительное число.")
            return
        
        # Сохраняем интервал и переходим к выбору фото
        st["min_time"] = min_time
        st["step"] = "photo_choice"
        
        # Создаем кнопки для выбора
        buttons = [
            [Button.inline("✅ Да, прикрепить фото", b"photo_yes_all_account")],
            [Button.inline("📸 Только изображение", b"photo_only_all_account")],
            [Button.inline("❌ Нет, только текст", b"photo_no_all_account")]
        ]
        
        await event.respond("📸 Хотите прикрепить фото к сообщению?", buttons=buttons)
        return
        
    # шаг для получения фото (если выбрали "Да" или "Только изображение")
    if st["step"] == "photo" or st["step"] == "photo_only":
        if event.photo:
            try:
                # Скачиваем фото
                photo = await event.download_media(file=MEDIA_DIR)
                st["photo_url"] = photo
                
                # Запускаем рассылку с фото в зависимости от режима
                if st["mode"] == "same":
                    # Режим с одинаковым интервалом
                    accounts, groups = await schedule_all_accounts_broadcast(st["text"], st["min_time"], None, photo)
                    message_type = "только с фото" if st["step"] == "photo_only" else "с фото"
                    if groups:
                        await event.respond(f"✅ Запустил: аккаунтов {accounts}, групп {groups}, каждые {st['min_time']} мин {message_type}.")
                    else:
                        await event.respond("⚠ Ни у одного аккаунта нет доступных рабочих групп.")
                else:
                    # Режим с разными интервалами
                    accounts, groups = await schedule_all_accounts_broadcast(st["text"], st["min"], st["max_m"], photo)
                    message_type = "только с фото" if st["step"] == "photo_only" else "с фото"
                    if groups:
                        await event.respond(f"✅ Запустил: аккаунтов {accounts}, групп {groups}, случайно каждые {st['min']}-{st['max_m']} мин {message_type}.")
                    else:
                        await event.respond("⚠ Ни у одного аккаунта нет доступных рабочих групп.")
                
                broadcast_all_state_account.pop(event.sender_id, None)
            except Exception as e:
                logger.error(f"Ошибка при обработке фото: {e}")
                await event.respond("⚠ Произошла ошибка при обработке фото. Попробуйте еще раз или выберите рассылку без фото.")
        else:
            await event.respond("⚠ Пожалуйста, отправьте фото или выберите рассылку без фото (/start).")
        return

    # случайный интервал — шаг 2 (min)
    if st["mode"] == "diff" and st["step"] == "min":
        try:
            st["min"] = int(event.text)
        except ValueError:
            await event.respond("Некорректный формат числа. Попробуйте ещё раз или нажмите /start.")
            return
        if st["min"] <= 0:
            await event.respond("⚠ Минимальное число должно быть больше нуля.")
            return
        st["step"] = "max"
        await event.respond("🔢 Максимальный интервал (мин):")
        return

    # случайный интервал — шаг 3 (max)
    if st["mode"] == "diff" and st["step"] == "max":
        try:
            max_m = int(event.text)
        except ValueError:
            await event.respond("Некорректный формат числа. Попробуйте ещё раз или нажмите /start.")
            return
        if max_m <= st["min"]:
            await event.respond("⚠ Максимальное число должно быть больше минимального числа.")
            return
            
        # Сохраняем максимальный интервал и переходим к выбору фото
        st["max_m"] = max_m
        st["step"] = "photo_choice"
        
        # Создаем кнопки для выбора
        buttons = [
            [Button.inline("✅ Да, прикрепить фото", b"photo_yes_all_account")],
            [Button.inline("📸 Только изображение", b"photo_only_all_account")],
            [Button.inline("❌ Нет, только текст", b"photo_no_all_account")]
        ]
        
        await event.respond("📸 Хотите прикрепить фото к сообщению?", buttons=buttons)
        return


@bot.on(Query(data=lambda d: d.decode() == "photo_yes_all_account"))
async def photo_yes_all_handler(event: callback_query) -> None:
    user_id = event.sender_id
    
    if user_id not in broadcast_all_state_account:
        await render_menu(event, "⚠ Сессия истекла. Пожалуйста, начните заново с команды /start")
        return
        
    st = broadcast_all_state_account[user_id]
    st["step"] = "photo"
    
    await render_menu(event, "📤 Пожалуйста, отправьте фото, которое хотите прикрепить к сообщению:")


@bot.on(Query(data=lambda d: d.decode() == "photo_only_all_account"))
async def photo_only_all_account_handler(event: callback_query) -> None:
    user_id = event.sender_id
    
    if user_id not in broadcast_all_state_account:
        await render_menu(event, "⚠ Сессия истекла. Пожалуйста, начните заново с команды /start")
        return
        
    st = broadcast_all_state_account[user_id]
    st["step"] = "photo_only"
    st["text"] = ""  # Пустой текст для отправки только фото
    
    await render_menu(event, "📤 Пожалуйста, отправьте фото, которое хотите отправить без текста:")


@bot.on(Query(data=lambda d: d.decode() == "photo_no_all_account"))
async def photo_no_all_handler(event: callback_query) -> None:
    user_id = event.sender_id
    
    if user_id not in broadcast_all_state_account:
        await render_menu(event, "⚠ Сессия истекла. Пожалуйста, начните заново с команды /start")
        return
        
    st = broadcast_all_state_account[user_id]
    
    # Запускаем рассылку без фото
    try:
        if st["mode"] == "same":
            accounts, groups = await schedule_all_accounts_broadcast(st["text"], st["min_time"], None)
            text = (
                f"✅ Запустил: аккаунтов {accounts}, групп {groups}, каждые {st['min_time']} мин."
                if groups
                else "⚠ Ни у одного аккаунта нет доступных рабочих групп."
            )
        else:
            accounts, groups = await schedule_all_accounts_broadcast(st["text"], st["min"], st["max_m"])
            text = (
                f"✅ Запустил: аккаунтов {accounts}, групп {groups}, случайно каждые {st['min']}-{st['max_m']} мин."
                if groups
                else "⚠ Ни у одного аккаунта нет доступных рабочих групп."
            )
        await render_menu(event, text)
    except Exception as exc:
        logger.exception(f"Ошибка запуска рассылки по всем аккаунтам: {exc}")
        await render_menu(event, f"⚠ Не удалось запустить рассылку: {exc}")
    finally:
        broadcast_all_state_account.pop(user_id, None)


@bot.on(Query(data=lambda data: data.decode() == "Stop_Broadcast_All_account"))
async def stop_broadcast_all(event: callback_query) -> None:
    from services.broadcast_runtime import stop_all_broadcast_jobs

    jobs, rows = stop_all_broadcast_jobs()
    if jobs or rows:
        text = (
            "⛔ Все обычные рассылки остановлены.\n"
            f"Задач планировщика: {jobs}; активных записей: {rows}."
        )
    else:
        text = "ℹ️ Активных обычных рассылок нет."
    await render_menu(event, text, buttons=[[Button.inline("🏠 Главное меню", b"menu_home")]])


async def schedule_all_accounts_broadcast(
    text: str,
    min_m: int,
    max_m: Optional[int] = None,
    photo_url: Optional[str] = None,
) -> tuple[int, int]:
    """Compatibility wrapper around the hardened multi-account scheduler."""
    from services.broadcast_runtime import schedule_all_accounts_broadcast_jobs

    accounts, groups = await schedule_all_accounts_broadcast_jobs(
        text=text,
        min_minutes=min_m,
        max_minutes=max_m,
        photo_url=photo_url,
    )
    logger.info(f"Рассылка запланирована: аккаунтов {accounts}, групп {groups}")
    return accounts, groups
