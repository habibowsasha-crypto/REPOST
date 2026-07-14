from typing import Optional

from loguru import logger
from telethon import Button

from config import (
    MEDIA_DIR,
    New_Message,
    Query,
    bot,
    broadcast_all_state,
    callback_message,
    callback_query,
)
from services.admin_state import clear_admin_interaction_state, is_command_event
from services.menu_ui import render_menu
from utils.logging import log_message_event


@bot.on(Query(data=lambda d: d.decode().startswith("broadcastAll_")))
async def broadcast_all_menu(event: callback_query) -> None:
    admin_id = event.sender_id
    await clear_admin_interaction_state(admin_id)
    target_user_id = int(str(event.data.decode()).split("_")[1])
    # запоминаем аккаунт, с которого шлём
    broadcast_all_state[admin_id] = {"user_id": target_user_id}

    keyboard = [
        [Button.inline("⏲️ Интервал во все группы", f"sameIntervalAll_{target_user_id}")],
        [Button.inline("🎲 Разный интервал (25-35)", f"diffIntervalAll_{target_user_id}")]
    ]
    await render_menu(event, "Выберите режим отправки:", buttons=keyboard)


# ---------- одинаковый интервал ----------
@bot.on(Query(data=lambda d: d.decode().startswith("sameIntervalAll_")))
async def same_interval_start(event: callback_query) -> None:
    admin_id = event.sender_id
    await clear_admin_interaction_state(admin_id)
    uid = int(event.data.decode().split("_")[1])
    broadcast_all_state[admin_id] = {"user_id": uid, "mode": "same", "step": "text"}
    await render_menu(event, "📝 Пришлите текст рассылки для **всех** групп этого аккаунта:")


# ---------- случайный интервал ----------
@bot.on(Query(data=lambda d: d.decode().startswith("diffIntervalAll_")))
async def diff_interval_start(event: callback_query) -> None:
    admin_id = event.sender_id
    await clear_admin_interaction_state(admin_id)
    uid = int(event.data.decode().split("_")[1])
    broadcast_all_state[admin_id] = {"user_id": uid, "mode": "diff", "step": "text"}
    await render_menu(event, "📝 Пришлите текст рассылки, потом спрошу границы интервала:")


# ---------- мастер-диалог (текст → интервалы) ----------
@bot.on(New_Message(func=lambda e: e.sender_id in broadcast_all_state and not is_command_event(e)))
async def broadcast_all_dialog(event: callback_message) -> None:
    st = broadcast_all_state[event.sender_id]
    log_message_event(event, "обработка диалога рассылки")
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
            [Button.inline("✅ Да, прикрепить фото", b"photo_yes_all")],
            [Button.inline("📸 Только изображение", b"photo_only_all")],
            [Button.inline("❌ Нет, только текст", b"photo_no_all")]
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
                    count = await schedule_account_broadcast(int(st["user_id"]), st["text"], st["min_time"], None, photo)
                    message_type = "только с фото" if st["step"] == "photo_only" else "с фото"
                    if count:
                        await event.respond(f"✅ Запустил для {count} групп: каждые {st['min_time']} мин {message_type}.")
                    else:
                        await event.respond("⚠ У аккаунта нет доступных рабочих групп. Сначала синхронизируйте группы аккаунта.")
                else:
                    # Режим с разными интервалами
                    count = await schedule_account_broadcast(int(st["user_id"]), st["text"], st["min"], st["max_m"], photo)
                    message_type = "только с фото" if st["step"] == "photo_only" else "с фото"
                    if count:
                        await event.respond(f"✅ Запустил для {count} групп: случайно каждые {st['min']}-{st['max_m']} мин {message_type}.")
                    else:
                        await event.respond("⚠ У аккаунта нет доступных рабочих групп. Сначала синхронизируйте группы аккаунта.")
                
                broadcast_all_state.pop(event.sender_id, None)
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
            [Button.inline("✅ Да, прикрепить фото", b"photo_yes_all")],
            [Button.inline("📸 Только изображение", b"photo_only_all")],
            [Button.inline("❌ Нет, только текст", b"photo_no_all")]
        ]
        
        await event.respond("📸 Хотите прикрепить фото к сообщению?", buttons=buttons)
        return


@bot.on(Query(data=lambda d: d.decode() == "photo_yes_all"))
async def photo_yes_all_handler(event: callback_query) -> None:
    user_id = event.sender_id
    
    if user_id not in broadcast_all_state:
        await render_menu(event, "⚠ Сессия истекла. Пожалуйста, начните заново с команды /start")
        return
        
    st = broadcast_all_state[user_id]
    st["step"] = "photo"
    
    await render_menu(event, "📤 Пожалуйста, отправьте фото, которое хотите прикрепить к сообщению:")


@bot.on(Query(data=lambda d: d.decode() == "photo_only_all"))
async def photo_only_all_handler(event: callback_query) -> None:
    user_id = event.sender_id
    
    if user_id not in broadcast_all_state:
        await render_menu(event, "⚠ Сессия истекла. Пожалуйста, начните заново с команды /start")
        return
        
    st = broadcast_all_state[user_id]
    st["step"] = "photo_only"
    st["text"] = ""  # Пустой текст для отправки только фото
    
    await render_menu(event, "📤 Пожалуйста, отправьте фото, которое хотите отправить без текста:")


@bot.on(Query(data=lambda d: d.decode() == "photo_no_all"))
async def photo_no_all_handler(event: callback_query) -> None:
    user_id = event.sender_id
    
    if user_id not in broadcast_all_state:
        await render_menu(event, "⚠ Сессия истекла. Пожалуйста, начните заново с команды /start")
        return
        
    st = broadcast_all_state[user_id]
    
    # Запускаем рассылку без фото
    try:
        if st["mode"] == "same":
            count = await schedule_account_broadcast(int(st["user_id"]), st["text"], st["min_time"], None)
            text = (
                f"✅ Запустил для {count} групп: каждые {st['min_time']} мин."
                if count
                else "⚠ У аккаунта нет доступных рабочих групп. Сначала синхронизируйте группы аккаунта."
            )
        else:
            count = await schedule_account_broadcast(int(st["user_id"]), st["text"], st["min"], st["max_m"])
            text = (
                f"✅ Запустил для {count} групп: случайно каждые {st['min']}-{st['max_m']} мин."
                if count
                else "⚠ У аккаунта нет доступных рабочих групп. Сначала синхронизируйте группы аккаунта."
            )
        await render_menu(event, text)
    except Exception as exc:
        logger.exception(f"Ошибка запуска рассылки аккаунта {st.get('user_id')}: {exc}")
        await render_menu(event, f"⚠ Не удалось запустить рассылку: {exc}")
    finally:
        broadcast_all_state.pop(user_id, None)


@bot.on(Query(data=lambda data: data.decode().startswith("StopBroadcastAll_")))
async def stop_broadcast_all(event: callback_query) -> None:
    try:
        user_id = int(event.data.decode().split("_", 1)[1])
    except (ValueError, IndexError):
        await render_menu(event, "⚠ Некорректный ID аккаунта.")
        return

    from services.broadcast_runtime import stop_account_broadcast_jobs

    jobs, rows = stop_account_broadcast_jobs(user_id)
    if jobs or rows:
        text = (
            f"⛔ Обычная рассылка аккаунта {user_id} остановлена.\n"
            f"Задач планировщика: {jobs}; активных записей: {rows}."
        )
    else:
        text = "ℹ️ У этого аккаунта нет активной обычной рассылки."
    await render_menu(event, text, buttons=[[Button.inline("🏠 Главное меню", b"menu_home")]])


async def schedule_account_broadcast(
    user_id: int,
    text: str,
    min_m: int,
    max_m: Optional[int] = None,
    photo_url: Optional[str] = None,
) -> int:
    """Compatibility wrapper around the hardened broadcast scheduler."""
    from services.broadcast_runtime import schedule_account_broadcast_jobs

    count = await schedule_account_broadcast_jobs(
        user_id=user_id,
        text=text,
        min_minutes=min_m,
        max_minutes=max_m,
        photo_url=photo_url,
        job_prefix="broadcastALL",
    )
    logger.info(f"Для аккаунта {user_id} запланировано групп: {count}")
    return count
