from services.admin_state import clear_admin_interaction_state, is_command_event
from services.menu_ui import render_menu
import datetime
from loguru import logger
from typing import Optional

from apscheduler.triggers.interval import IntervalTrigger
from telethon.tl.custom import Button

from config import callback_query, scheduler, Query, bot, conn, New_Message, \
    broadcast_solo_state, callback_message, MEDIA_DIR
from utils.telegram import gid_key, create_broadcast_data
from utils.logging import log_message_event


async def send_broadcast_message(
    user_id: int,
    group_id: int,
    text: str,
    session_string: str,
    photo_url: Optional[str] = None,
    max_retries: int = 10,
) -> None:
    """Compatibility wrapper for one-group scheduled broadcasts."""
    from services.broadcast_runtime import send_scheduled_broadcast

    canonical_group_id = gid_key(group_id)
    job_id = f"broadcast_{user_id}_{canonical_group_id}"
    await send_scheduled_broadcast(
        user_id=user_id,
        group_id=canonical_group_id,
        session_string=session_string,
        job_id=job_id,
        fallback_text=text,
        fallback_photo_url=photo_url,
        max_retries=max_retries,
    )


@bot.on(Query(data=lambda d: d.decode().startswith("BroadcastTextInterval_")))
async def same_interval_start(event: callback_query) -> None:
    admin_id = event.sender_id
    await clear_admin_interaction_state(admin_id)
    data = event.data.decode()
    user_id, group_id = map(int, data.split("_")[1:])
    broadcast_solo_state[admin_id] = {"user_id": user_id, "mode": "same", "step": "text", "group_id": group_id}
    await render_menu(event, "📝 Пришлите текст рассылки для **одной** группы этого аккаунта:")


# ---------- мастер-диалог (текст → интервалы) ----------
@bot.on(New_Message(func=lambda e: e.sender_id in broadcast_solo_state and not is_command_event(e)))
async def broadcast_all_dialog(event: callback_message) -> None:
    st = broadcast_solo_state[event.sender_id]
    log_message_event(event, "обработка диалога индивидуальной рассылки")
    # шаг 1 — получили текст
    if st["step"] == "text":
        st["text"] = event.text
        st["step"] = "interval"
        await event.respond("⏲️ Введите интервал (минуты, одно число):")
        return

    # шаг 2 - получили интервал
    if st["step"] == "interval":
        try:
            min_time = int(event.text)
        except ValueError:
            await event.respond("Некорректный формат числа. Попробуйте ещё раз или нажмите /start.")
            return
        if min_time <= 0:
            await event.respond("⚠ Должно быть положительное число.")
            return
        
        # Добавляем шаг для выбора прикрепления фото
        st["interval"] = min_time
        st["step"] = "photo_choice"
        
        # Создаем кнопки для выбора
        buttons = [
            [Button.inline("✅ Да, прикрепить фото", b"photo_yes")],
            [Button.inline("📸 Только изображение", b"photo_only")],
            [Button.inline("❌ Нет, только текст", b"photo_no")]
        ]
        
        await event.respond("📸 Хотите прикрепить фото к сообщению?", buttons=buttons)
        return
        
    # шаг 3 - получили фото (если пользователь выбрал "Да" или "Только изображение")
    if st["step"] == "photo" or st["step"] == "photo_only":
        if event.photo:
            # Если пользователь отправил фото
            try:
                # Скачиваем фото
                photo = await event.download_media(file=MEDIA_DIR)
                st["photo_url"] = photo
                
                # Запускаем рассылку
                job_id = f"broadcast_{st['user_id']}_{gid_key(st['group_id'])}"
                
                # Обновляем данные рассылки в базе
                create_broadcast_data(st["user_id"], st["group_id"], st["text"], st["interval"], photo)
                
                # Для одной группы должна существовать только одна обычная задача.
                for prefix in ("broadcast", "broadcastALL"):
                    existing_id = f"{prefix}_{st['user_id']}_{gid_key(st['group_id'])}"
                    if scheduler.get_job(existing_id):
                        scheduler.remove_job(existing_id)
                
                # Получаем сессию
                cursor = conn.cursor()
                row = cursor.execute("SELECT session_string FROM sessions WHERE user_id = ?", (st["user_id"],)).fetchone()
                if not row:
                    await event.respond("⚠ Не удалось найти сессию для этого аккаунта.")
                    broadcast_solo_state.pop(event.sender_id, None)
                    cursor.close()
                    return
                session_string = row[0]
                cursor.close()
                
                # Устанавливаем триггер для планировщика
                trigger = IntervalTrigger(minutes=st["interval"])
                next_run = datetime.datetime.now() + datetime.timedelta(seconds=10)  # Запускаем через 10 секунд
                
                # Добавляем задачу в планировщик
                scheduler.add_job(
                    send_broadcast_message,
                    trigger,
                    args=[st["user_id"], st["group_id"], st["text"], session_string, photo],
                    id=job_id,
                    next_run_time=next_run,
                    replace_existing=True,
                    max_instances=1,
                    coalesce=True,
                )
                
                # Запускаем планировщик, если он еще не запущен
                if not scheduler.running:
                    scheduler.start()
                
                message_type = "только с фото" if st["step"] == "photo_only" else "с фото"
                await event.respond(f"✅ Запустил: каждые {st['interval']} мин {message_type}.")
                broadcast_solo_state.pop(event.sender_id, None)
            except Exception as e:
                logger.error(f"Ошибка при обработке фото: {e}")
                await event.respond("⚠ Произошла ошибка при обработке фото. Попробуйте еще раз или выберите рассылку без фото.")
        else:
            await event.respond("⚠ Пожалуйста, отправьте фото или выберите рассылку без фото (/start).")
        return


@bot.on(Query(data=lambda d: d.decode() == "photo_yes"))
async def photo_yes_handler(event: callback_query) -> None:
    user_id = event.sender_id
    
    if user_id not in broadcast_solo_state:
        await render_menu(event, "⚠ Сессия истекла. Пожалуйста, начните заново с команды /start")
        return
        
    st = broadcast_solo_state[user_id]
    st["step"] = "photo"
    
    await render_menu(event, "📤 Пожалуйста, отправьте фото, которое хотите прикрепить к сообщению:")


@bot.on(Query(data=lambda d: d.decode() == "photo_only"))
async def photo_only_handler(event: callback_query) -> None:
    user_id = event.sender_id
    
    if user_id not in broadcast_solo_state:
        await render_menu(event, "⚠ Сессия истекла. Пожалуйста, начните заново с команды /start")
        return
        
    st = broadcast_solo_state[user_id]
    st["step"] = "photo_only"
    st["text"] = ""  # Пустой текст для отправки только фото
    
    await render_menu(event, "📤 Пожалуйста, отправьте фото, которое хотите отправить без текста:")


@bot.on(Query(data=lambda d: d.decode() == "photo_no"))
async def photo_no_handler(event: callback_query) -> None:
    user_id = event.sender_id
    
    if user_id not in broadcast_solo_state:
        await render_menu(event, "⚠ Сессия истекла. Пожалуйста, начните заново с команды /start")
        return
        
    st = broadcast_solo_state[user_id]
    
    # Запускаем рассылку без фото
    job_id = f"broadcast_{st['user_id']}_{gid_key(st['group_id'])}"
    
    # Обновляем данные рассылки в базе
    create_broadcast_data(st["user_id"], st["group_id"], st["text"], st["interval"])
    
    # Для одной группы должна существовать только одна обычная задача.
    for prefix in ("broadcast", "broadcastALL"):
        existing_id = f"{prefix}_{st['user_id']}_{gid_key(st['group_id'])}"
        if scheduler.get_job(existing_id):
            scheduler.remove_job(existing_id)
    
    # Получаем сессию
    cursor = conn.cursor()
    row = cursor.execute("SELECT session_string FROM sessions WHERE user_id = ?", (st["user_id"],)).fetchone()
    if not row:
        await render_menu(event, "⚠ Не удалось найти сессию для этого аккаунта.")
        broadcast_solo_state.pop(user_id, None)
        cursor.close()
        return
    session_string = row[0]
    cursor.close()
    
    # Устанавливаем триггер для планировщика
    trigger = IntervalTrigger(minutes=st["interval"])
    next_run = datetime.datetime.now() + datetime.timedelta(seconds=10)  # Запускаем через 10 секунд
    
    # Добавляем задачу в планировщик
    scheduler.add_job(
        send_broadcast_message,
        trigger,
        args=[st["user_id"], st["group_id"], st["text"], session_string, None],
        id=job_id,
        next_run_time=next_run,
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    
    # Запускаем планировщик, если он еще не запущен
    if not scheduler.running:
        scheduler.start()
    
    await render_menu(event, f"✅ Запустил: каждые {st['interval']} мин.")
    broadcast_solo_state.pop(user_id, None)


@bot.on(Query(data=lambda data: data.decode().startswith("StartResumeBroadcast_")))
async def start_resume_broadcast(event: callback_query) -> None:
    data = event.data.decode()
    parts = data.split("_")

    if len(parts) < 3:
        await render_menu(event, "⚠ Произошла ошибка при обработке данных. Попробуйте еще раз.")
        return

    try:
        user_id = int(parts[1])
        group_id = int(parts[2])
    except ValueError as e:
        await render_menu(event, f"⚠ Ошибка при извлечении данных: {e}")
        return
    cursor = conn.cursor()
    job_id = f"broadcast_{user_id}_{gid_key(group_id)}"
    existing_job = scheduler.get_job(job_id) or scheduler.get_job(
        f"broadcastALL_{user_id}_{gid_key(group_id)}"
    )

    if existing_job:
        await render_menu(event, "⚠ Рассылка уже активна для этой группы.")
        cursor.close()
        return

    # Получаем данные рассылки из базы
    cursor.execute("""
                SELECT broadcast_text, interval_minutes, photo_url 
                FROM broadcasts 
                WHERE user_id = ? AND group_id = ?
            """, (user_id, gid_key(group_id)))
    row = cursor.fetchone()

    if not row:
        # Если данных нет, предлагаем настроить рассылку
        await render_menu(event, "⚠ Рассылка еще не настроена для этой группы. Пожалуйста, настройте текст и интервал рассылки.")
        cursor.close()
        return
    
    broadcast_text = row[0]
    interval_minutes = row[1]
    photo_url = row[2] if len(row) > 2 else None
    
    if (not broadcast_text and not photo_url) or not interval_minutes or interval_minutes <= 0:
        await render_menu(event, "⚠ Пожалуйста, убедитесь, что задан текст или изображение и установлен корректный интервал.")
        cursor.close()
        return
    
    # Получаем сессию пользователя
    session_string_row = cursor.execute("SELECT session_string FROM sessions WHERE user_id = ?",
                                        (user_id,)).fetchone()
    if not session_string_row:
        await render_menu(event, "⚠ Ошибка: не найден session_string для аккаунта.")
        cursor.close()
        return
    
    session_string = session_string_row[0]
    
    # Проверяем, существует ли запись о группе
    group_row = cursor.execute("SELECT group_username FROM groups WHERE user_id = ? AND group_id = ?", 
                              (user_id, gid_key(group_id))).fetchone()
    if not group_row:
        await render_menu(event, f"⚠ Группа не найдена в базе данных для user_id={user_id}, group_id={group_id}.")
        cursor.close()
        return

    # Активируем рассылку в базе данных
    cursor.execute("""
        UPDATE broadcasts 
        SET is_active = ?, error_reason = NULL
        WHERE user_id = ? AND group_id = ?
    """, (True, user_id, gid_key(group_id)))
    
    # Если запись не существует, создаем новую
    if cursor.rowcount == 0:
        cursor.execute("""
            INSERT INTO broadcasts (user_id, group_id, broadcast_text, interval_minutes, is_active, error_reason, photo_url)
            VALUES (?, ?, ?, ?, ?, NULL, ?)
        """, (user_id, gid_key(group_id), broadcast_text, interval_minutes, True, photo_url))
        
    conn.commit()
    
    # Создаем задачу в планировщике
    trigger = IntervalTrigger(minutes=interval_minutes)
    next_run = datetime.datetime.now() + datetime.timedelta(seconds=10)  # Запускаем через 10 секунд
    
    # Добавляем задачу в планировщик
    scheduler.add_job(
        send_broadcast_message,
        trigger,
        args=[user_id, group_id, broadcast_text, session_string, photo_url],
        id=job_id,
        next_run_time=next_run,
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    
    # Запускаем планировщик, если он еще не запущен
    if not scheduler.running:
        scheduler.start()
    
    await render_menu(event, f"✅ Рассылка успешно запущена! Первое сообщение будет отправлено через 10 секунд, затем каждые {interval_minutes} минут.")
    cursor.close()


@bot.on(Query(data=lambda data: data.decode().startswith("StopAccountBroadcast_")))
async def stop_broadcast(event: callback_query) -> None:
    try:
        user_id, group_id = map(int, event.data.decode().split("_")[1:])
    except (ValueError, IndexError):
        await render_menu(event, "⚠ Некорректные данные аккаунта или группы.")
        return

    from services.broadcast_runtime import stop_group_broadcast_jobs

    jobs, rows = stop_group_broadcast_jobs(user_id, group_id)
    if jobs or rows:
        text = f"⛔ Обычная рассылка в группу {gid_key(group_id)} остановлена."
    else:
        text = "ℹ️ Для этой группы нет активной обычной рассылки."
    await render_menu(event, text, buttons=[[Button.inline("🏠 Главное меню", b"menu_home")]])
