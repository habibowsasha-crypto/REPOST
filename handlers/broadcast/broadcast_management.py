import asyncio
import datetime
from loguru import logger
from typing import Union, Optional

from apscheduler.triggers.interval import IntervalTrigger
from telethon import Button, TelegramClient
from telethon.errors import ChatWriteForbiddenError, ChatAdminRequiredError, FloodWaitError, SlowModeWaitError
from telethon.sessions import StringSession
from telethon.tl.functions.messages import SendMessageRequest
from telethon.tl.types import Channel, Chat, PeerChannel, PeerChat

from config import callback_query, callback_message, broadcast_all_state, API_ID, API_HASH, scheduler, Query, bot, conn, \
    New_Message
from utils.telegram import gid_key, create_broadcast_data, get_active_broadcast_groups, get_entity_by_id
from utils.logging import log_message_event, log_user_action


@bot.on(Query(data=lambda d: d.decode().startswith("broadcastAll_")))
async def broadcast_all_menu(event: callback_query) -> None:
    admin_id = event.sender_id
    target_user_id = int(str(event.data.decode()).split("_")[1])
    # запоминаем аккаунт, с которого шлём
    broadcast_all_state[admin_id] = {"user_id": target_user_id}

    keyboard = [
        [Button.inline("⏲️ Интервал во все группы", f"sameIntervalAll_{target_user_id}")],
        [Button.inline("🎲 Разный интервал (25-35)", f"diffIntervalAll_{target_user_id}")]
    ]
    await event.respond("Выберите режим отправки:", buttons=keyboard)


# ---------- одинаковый интервал ----------
@bot.on(Query(data=lambda d: d.decode().startswith("sameIntervalAll_")))
async def same_interval_start(event: callback_query) -> None:
    admin_id = event.sender_id
    uid = int(event.data.decode().split("_")[1])
    broadcast_all_state[admin_id] = {"user_id": uid, "mode": "same", "step": "text"}
    await event.respond("📝 Пришлите текст рассылки для **всех** групп этого аккаунта:")


# ---------- случайный интервал ----------
@bot.on(Query(data=lambda d: d.decode().startswith("diffIntervalAll_")))
async def diff_interval_start(event: callback_query) -> None:
    admin_id = event.sender_id
    uid = int(event.data.decode().split("_")[1])
    broadcast_all_state[admin_id] = {"user_id": uid, "mode": "diff", "step": "text"}
    await event.respond("📝 Пришлите текст рассылки, потом спрошу границы интервала:")


# ---------- мастер-диалог (текст → интервалы) ----------
@bot.on(New_Message(func=lambda e: e.sender_id in broadcast_all_state and not (e.raw_text or "").lstrip().startswith("/")))
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
            await event.respond(f"Некорректный формат числа. попробуйте еще раз нажав /start")
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
                photo = await event.download_media()
                st["photo_url"] = photo
                
                # Запускаем рассылку с фото в зависимости от режима
                if st["mode"] == "same":
                    # Режим с одинаковым интервалом
                    await schedule_account_broadcast(int(st["user_id"]), st["text"], st["min_time"], None, photo)
                    message_type = "только с фото" if st["step"] == "photo_only" else "с фото"
                    await event.respond(f"✅ Запустил: каждые {st['min_time']} мин {message_type}.")
                else:
                    # Режим с разными интервалами
                    await schedule_account_broadcast(int(st["user_id"]), st["text"], st["min"], st["max_m"], photo)
                    message_type = "только с фото" if st["step"] == "photo_only" else "с фото"
                    await event.respond(f"✅ Запустил: случайно каждые {st['min']}-{st['max_m']} мин {message_type}.")
                
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
            await event.respond(f"Некорректный формат числа. попробуйте еще раз нажав /start")
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
            await event.respond(f"Некорректный формат числа. попробуйте еще раз нажав /start")
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
        await event.respond("⚠ Сессия истекла. Пожалуйста, начните заново с команды /start")
        return
        
    st = broadcast_all_state[user_id]
    st["step"] = "photo"
    
    await event.respond("📤 Пожалуйста, отправьте фото, которое хотите прикрепить к сообщению:")


@bot.on(Query(data=lambda d: d.decode() == "photo_only_all"))
async def photo_only_all_handler(event: callback_query) -> None:
    user_id = event.sender_id
    
    if user_id not in broadcast_all_state:
        await event.respond("⚠ Сессия истекла. Пожалуйста, начните заново с команды /start")
        return
        
    st = broadcast_all_state[user_id]
    st["step"] = "photo_only"
    st["text"] = ""  # Пустой текст для отправки только фото
    
    await event.respond("📤 Пожалуйста, отправьте фото, которое хотите отправить без текста:")


@bot.on(Query(data=lambda d: d.decode() == "photo_no_all"))
async def photo_no_all_handler(event: callback_query) -> None:
    user_id = event.sender_id
    
    if user_id not in broadcast_all_state:
        await event.respond("⚠ Сессия истекла. Пожалуйста, начните заново с команды /start")
        return
        
    st = broadcast_all_state[user_id]
    
    # Запускаем рассылку без фото
    if st["mode"] == "same":
        await schedule_account_broadcast(int(st["user_id"]), st["text"], st["min_time"], None)
        await event.respond(f"✅ Запустил: каждые {st['min_time']} мин.")
    else:
        await schedule_account_broadcast(int(st["user_id"]), st["text"], st["min"], st["max_m"])
        await event.respond(f"✅ Запустил: случайно каждые {st['min']}-{st['max_m']} мин.")
    
    broadcast_all_state.pop(user_id, None)


@bot.on(Query(data=lambda data: data.decode().startswith("StopBroadcastAll_")))
async def stop_broadcast_all(event: callback_query) -> None:
    data = event.data.decode()
    try:
        user_id = int(data.split("_")[1])
    except ValueError as e:
        await event.respond(f"⚠ Ошибка при извлечении user_id и group_id: {e}")
        return
    
    cursor = conn.cursor()
    msg = ["⛔ **Остановленные рассылки**:\n\n"]
    
    # Получаем информацию о группах для отображения названий вместо ID
    groups_data = cursor.execute("""
        SELECT g.group_id, g.group_username, b.is_active 
        FROM groups g 
        LEFT JOIN broadcasts b ON g.group_id = b.group_id AND b.user_id = g.user_id
        WHERE g.user_id = ?
    """, (user_id,)).fetchall()
    
    # Проверяем, есть ли активные рассылки
    has_stopped = False
    
    # Получаем клиента для получения названий групп
    session_string = cursor.execute("SELECT session_string FROM sessions WHERE user_id = ?", (user_id,)).fetchone()
    if not session_string:
        await event.respond("⚠ Не знайдено сесію для цього акаунта")
        cursor.close()
        return
    
    client = TelegramClient(StringSession(session_string[0]), API_ID, API_HASH)
    await client.connect()
    
    try:
        for group_id, group_username, is_active in groups_data:
            # Спробуємо отримати інформацію про групу
            try:
                # Спробуємо отримати entity групи
                try:
                    # Перевіряємо, чи це username чи ID
                    if group_username.startswith('@'):
                        # Це username групи
                        entity = await client.get_entity(group_username)
                    else:
                        # Спробуємо отримати entity за ID
                        try:
                            group_id_int = int(group_username)
                            entity = await get_entity_by_id(client, group_id_int)
                            if not entity:
                                # Якщо не вдалося отримати entity, використовуємо тільки ID для відображення
                                display_name = f"Група з ID {group_id}"
                                
                                # Проверяем наличие задания в планировщике
                                job_id = f"broadcastALL_{user_id}_{gid_key(group_id)}"
                                job = scheduler.get_job(job_id)
                                
                                # Проверяем также статус is_active в базе данных
                                is_active_in_db = cursor.execute(
                                    "SELECT is_active FROM broadcasts WHERE user_id = ? AND group_id = ?", 
                                    (user_id, gid_key(group_id))
                                ).fetchone()
                                
                                # Если есть задание в планировщике или активный статус в БД
                                if job or (is_active_in_db and is_active_in_db[0]):
                                    if job:
                                        scheduler.remove_job(job_id)
                                    
                                    # Обновляем статус в базе данных
                                    cursor.execute("UPDATE broadcasts SET is_active = ? WHERE user_id = ? AND group_id = ?",
                                                  (False, user_id, gid_key(group_id)))
                                    conn.commit()
                                    
                                    msg.append(f"⛔ Рассылка в {display_name} остановлена.")
                                    has_stopped = True
                                
                                # Пропускаем дальнейшую обработку этой группы
                                continue
                        except ValueError:
                            # Якщо не можемо перетворити в число, спробуємо використати як є
                            entity = await client.get_entity(group_username)
                except Exception as entity_error:
                    # Якщо не вдалося отримати entity, спробуємо альтернативний метод
                    if "Cannot find any entity corresponding to" in str(entity_error):
                        try:
                            # Спробуємо отримати entity за ID
                            try:
                                group_id_int = int(group_username) if group_username.isdigit() else group_id
                                entity = await get_entity_by_id(client, group_id_int)
                                if not entity:
                                    # Якщо не вдалося отримати entity, використовуємо тільки ID для відображення
                                    display_name = f"Група з ID {group_id}"
                                    
                                    # Проверяем наличие задания в планировщике
                                    job_id = f"broadcastALL_{user_id}_{gid_key(group_id)}"
                                    job = scheduler.get_job(job_id)
                                    
                                    # Проверяем также статус is_active в базе данных
                                    is_active_in_db = cursor.execute(
                                        "SELECT is_active FROM broadcasts WHERE user_id = ? AND group_id = ?", 
                                        (user_id, gid_key(group_id))
                                    ).fetchone()
                                    
                                    # Если есть задание в планировщике или активный статус в БД
                                    if job or (is_active_in_db and is_active_in_db[0]):
                                        if job:
                                            scheduler.remove_job(job_id)
                                        
                                        # Обновляем статус в базе данных
                                        cursor.execute("UPDATE broadcasts SET is_active = ? WHERE user_id = ? AND group_id = ?",
                                                      (False, user_id, gid_key(group_id)))
                                        conn.commit()
                                        
                                        msg.append(f"⛔ Рассылка в {display_name} остановлена.")
                                        has_stopped = True
                                    
                                    # Пропускаем дальнейшую обработку этой группы
                                    continue
                            except ValueError:
                                # Якщо не вдалося перетворити в число, просто зупиняємо задачі
                                display_name = f"Група з ID {group_id}"
                                
                                # Проверяем наличие задания в планировщике
                                job_id = f"broadcastALL_{user_id}_{gid_key(group_id)}"
                                job = scheduler.get_job(job_id)
                                
                                # Проверяем также статус is_active в базе данных
                                is_active_in_db = cursor.execute(
                                    "SELECT is_active FROM broadcasts WHERE user_id = ? AND group_id = ?", 
                                    (user_id, gid_key(group_id))
                                ).fetchone()
                                
                                # Если есть задание в планировщике или активный статус в БД
                                if job or (is_active_in_db and is_active_in_db[0]):
                                    if job:
                                        scheduler.remove_job(job_id)
                                    
                                    # Обновляем статус в базе данных
                                    cursor.execute("UPDATE broadcasts SET is_active = ? WHERE user_id = ? AND group_id = ?",
                                                  (False, user_id, gid_key(group_id)))
                                    conn.commit()
                                    
                                    msg.append(f"⛔ Рассылка в {display_name} остановлена.")
                                    has_stopped = True
                                
                                # Пропускаем дальнейшую обработку этой группы
                                continue
                        except Exception as alt_error:
                            logger.error(f"Ошибка при альтернативном получении Entity: {alt_error}")
                            
                            # Если все методы не сработали, останавливаем задачу без информации о группе
                            display_name = f"Група з ID {group_id}"
                            
                            # Проверяем наличие задания в планировщике
                            job_id = f"broadcastALL_{user_id}_{gid_key(group_id)}"
                            job = scheduler.get_job(job_id)
                            
                            # Проверяем также статус is_active в базе данных
                            is_active_in_db = cursor.execute(
                                "SELECT is_active FROM broadcasts WHERE user_id = ? AND group_id = ?", 
                                (user_id, gid_key(group_id))
                            ).fetchone()
                            
                            # Если есть задание в планировщике или активный статус в БД
                            if job or (is_active_in_db and is_active_in_db[0]):
                                if job:
                                    scheduler.remove_job(job_id)
                                
                                # Обновляем статус в базе данных
                                cursor.execute("UPDATE broadcasts SET is_active = ? WHERE user_id = ? AND group_id = ?",
                                              (False, user_id, gid_key(group_id)))
                                conn.commit()
                                
                                msg.append(f"⛔ Рассылка в {display_name} остановлена.")
                                has_stopped = True
                            
                            # Пропускаем дальнейшую обработку этой группы
                            continue
                    else:
                        logger.error(f"Ошибка при получении информации о группе: {str(entity_error)}")
                        continue
                
                # Пропускаємо канали-вітрини
                if isinstance(entity, Channel) and entity.broadcast and not entity.megagroup:
                    continue
                
                # Формуємо назву для відображення
                display_name = entity.title if hasattr(entity, 'title') else group_username
                
                # Проверяем наличие задания в планировщике
                job_id = f"broadcastALL_{user_id}_{gid_key(group_id)}"
                job = scheduler.get_job(job_id)
                
                # Проверяем также статус is_active в базе данных
                is_active_in_db = cursor.execute(
                    "SELECT is_active FROM broadcasts WHERE user_id = ? AND group_id = ?", 
                    (user_id, gid_key(group_id))
                ).fetchone()
                
                # Если есть задание в планировщике или активный статус в БД
                if job or (is_active_in_db and is_active_in_db[0]):
                    if job:
                        scheduler.remove_job(job_id)
                    
                    # Обновляем статус в базе данных
                    cursor.execute("UPDATE broadcasts SET is_active = ? WHERE user_id = ? AND group_id = ?",
                                  (False, user_id, gid_key(group_id)))
                    conn.commit()
                    
                    msg.append(f"⛔ Рассылка в группу **{display_name}** остановлена.")
                    has_stopped = True
                
            except Exception as e:
                logger.error(f"Ошибка при обработке группы {group_id}: {e}")
                
                # В случае ошибки все равно пытаемся остановить задачу
                try:
                    job_id = f"broadcastALL_{user_id}_{gid_key(group_id)}"
                    job = scheduler.get_job(job_id)
                    
                    # Проверяем также статус is_active в базе данных
                    is_active_in_db = cursor.execute(
                        "SELECT is_active FROM broadcasts WHERE user_id = ? AND group_id = ?", 
                        (user_id, gid_key(group_id))
                    ).fetchone()
                    
                    # Если есть задание в планировщике или активный статус в БД
                    if job or (is_active_in_db and is_active_in_db[0]):
                        if job:
                            scheduler.remove_job(job_id)
                        
                        # Обновляем статус в базе данных
                        cursor.execute("UPDATE broadcasts SET is_active = ? WHERE user_id = ? AND group_id = ?",
                                      (False, user_id, gid_key(group_id)))
                        conn.commit()
                        
                        msg.append(f"⛔ Рассылка в группу с ID {group_id} остановлена.")
                        has_stopped = True
                except Exception as stop_error:
                    logger.error(f"Критическая ошибка при остановке рассылки: {stop_error}")
                    continue
    
    finally:
        await client.disconnect()
    
    # Если нет остановленных рассылок
    if not has_stopped:
        msg.append("Нет активных рассылок для остановки.")
    
    await event.respond("\n".join(msg))
    cursor.close()


async def schedule_account_broadcast(user_id: int,
                                     text: str,
                                     min_m: int,
                                     max_m: Union[int] = None,
                                     photo_url: Optional[str] = None) -> None:
    """Ставит/обновляет jobs broadcastALL_<user>_<gid> только для чатов,
    куда аккаунт реально может писать."""
    # --- сессия ---
    cursor = conn.cursor()
    row = cursor.execute("SELECT session_string FROM sessions WHERE user_id = ?", (user_id,)).fetchone()
    cursor.execute("""UPDATE broadcasts SET broadcast_text = ? WHERE user_id = ?""", (text, user_id))
    if not row:
        return
    sess_str = row[0]

    client = TelegramClient(StringSession(sess_str), API_ID, API_HASH)
    await client.connect()
    
    # Сначала удаляем все существующие задания для этого пользователя
    # чтобы избежать дублирования сообщений
    for job in scheduler.get_jobs():
        if job.id.startswith(f"broadcastALL_{user_id}_"):
            scheduler.remove_job(job.id)
            logger.info(f"Удалено существующее задание {job.id}")

    # --- собираем «разрешённые» чаты/каналы ---
    groups = cursor.execute("""SELECT group_username, group_id FROM groups WHERE user_id = ?""", (user_id,)).fetchall()
    ok_entities: list[Channel | Chat] = []
    
    for group in groups:
        try:
            # Пробуем получить объект по username или ID
            group_username = group[0]
            group_id = group[1]
            
            # Проверяем, является ли group_username числом (ID группы) или именем пользователя
            if group_username.startswith('@'):
                # Это username группы
                try:
                    ent = await client.get_entity(group_username)
                except Exception as e:
                    logger.error(f"Не удалось получить entity по username {group_username}: {e}")
                    continue
            else:
                # Пробуем получить entity по ID
                try:
                    group_id_int = int(group_username)
                    ent = await get_entity_by_id(client, group_id_int)
                    if not ent:
                        logger.error(f"Не удалось получить entity для ID {group_username}")
                        continue
                except ValueError:
                    # Если не можем преобразовать в число, пробуем использовать как есть
                    try:
                        ent = await client.get_entity(group_username)
                    except Exception as e:
                        logger.error(f"Не удалось получить entity для {group_username}: {e}")
                        continue
            
            if not isinstance(ent, (Channel, Chat)):
                logger.info(f"пропускаем задачу {ent} так как данный чат Личный диалог или бот")
                continue
            if isinstance(ent, Channel) and ent.broadcast and not ent.megagroup:
                logger.info(f"пропускаем задачу {ent} так как данный чат витрина-канал")
                continue
            ok_entities.append(ent)
        except Exception as error:
            logger.warning(f"Не смог проверить: {error}")
            continue

    if not ok_entities:
        logger.info(f"Нету задач выходим")
        return

    sec_run = (((max_m - min_m) / len(ok_entities)) if max_m else min_m)
    current_time = sec_run
    for ent in ok_entities:
        logger.debug(ent)
        job_id = f"broadcastALL_{user_id}_{gid_key(ent.id)}"
        interval = (((max_m - min_m) / len(ok_entities)) if max_m else min_m)
        create_broadcast_data(user_id, gid_key(ent.id), text, interval, photo_url)
        if scheduler.get_job(job_id):
            logger.info(f"Удаляем задачу")
            scheduler.remove_job(job_id)

        async def send_message(
                ss: str = sess_str,
                entity: Union[Channel, Chat] = ent,
                jobs_id: str = job_id,
                start_text: str = text,
                start_photo_url: Optional[str] = photo_url,
                max_retries: int = 10
        ) -> None:
            """Отправляет сообщение с обработкой ошибок и повторными попытками."""
            retry_count = 0
            cursor = None

            while retry_count < max_retries:
                try:
                    async with TelegramClient(StringSession(ss), API_ID, API_HASH) as client:
                        cursor = conn.cursor()

                        # Получаем актуальный текст рассылки и фото из базы данных
                        cursor.execute("""SELECT broadcast_text, photo_url FROM broadcasts 
                                        WHERE group_id = ? AND user_id = ?""",
                                       (entity.id, user_id))
                        current_data = cursor.fetchone()
                        txt = current_data[0] if current_data and current_data[0] else start_text
                        photo_url_from_db = current_data[1] if current_data and len(current_data) > 1 else None
                        
                        # Определяем, использовать ли фото из базы данных или отправлять новое
                        photo_to_send = photo_url_from_db if photo_url_from_db else start_photo_url

                        # Попытка отправить сообщение
                        try:
                            if photo_to_send:
                                try:
                                    # Отправляем сообщение с фото
                                    await client.send_file(entity, photo_to_send, caption=txt)
                                    logger.info(f"Отправлено сообщение с фото в {entity.title}")
                                except Exception as photo_error:
                                    logger.error(f"Ошибка при отправке с фото: {photo_error}")
                                    # Если не удалось отправить с фото, пробуем отправить только текст
                                    await client.send_message(entity, txt)
                                    logger.info(f"Отправлено сообщение без фото в {entity.title}")
                            else:
                                # Отправляем обычное текстовое сообщение
                                await client.send_message(entity, txt)
                                logger.info(f"Отправлено: {txt} в {entity.title}")
                        except Exception as entity_error:
                            # Проверяем, не связана ли ошибка с невозможностью найти entity
                            if "Cannot find any entity corresponding to" in str(entity_error):
                                logger.info(f"Пробуем получить entity другим способом для {entity.id}")
                                # Пробуем получить entity другим способом
                                new_entity = await get_entity_by_id(client, entity.id)
                                if new_entity:
                                    if photo_to_send:
                                        try:
                                            # Отправляем сообщение с фото
                                            await client.send_file(new_entity, photo_to_send, caption=txt)
                                            logger.info(f"Отправлено сообщение с фото через альтернативный метод в {new_entity.title}")
                                        except Exception as alt_photo_error:
                                            logger.error(f"Ошибка при отправке с фото через альтернативный метод: {alt_photo_error}")
                                            # Если не удалось отправить с фото, пробуем отправить только текст
                                            await client.send_message(new_entity, txt)
                                            logger.info(f"Отправлено сообщение без фото через альтернативный метод в {new_entity.title}")
                                    else:
                                        await client.send_message(new_entity, txt)
                                        logger.info(f"Отправлено через альтернативный метод в {new_entity.title}")
                                    entity = new_entity  # Обновляем entity для дальнейшего использования
                                else:
                                    raise entity_error  # Если не удалось получить entity, пробрасываем исключение
                            else:
                                raise entity_error  # Другие ошибки пробрасываем дальше

                        cursor.execute("""INSERT INTO send_history 
                                        (user_id, group_id, group_name, sent_at, message_text) 
                                        VALUES (?, ?, ?, ?, ?)""",
                                       (user_id, entity.id, getattr(entity, 'title', ''),
                                        datetime.datetime.now().isoformat(), txt))
                except (ChatWriteForbiddenError, ChatAdminRequiredError) as e:
                    logger.error(f"Нет прав писать в {entity.title}: {e}")
                    break
                except (FloodWaitError, SlowModeWaitError) as e:
                    wait_time = e.seconds
                    logger.warning(f"{type(e).__name__}: ожидание {wait_time} сек.")
                    await asyncio.sleep(wait_time + 10)
                    retry_count += 1
                except Exception as e:
                    logger.error(f"Ошибка при отправке в {entity.title}: {type(e).__name__}: {e}")
                    retry_count += 1
                    await asyncio.sleep(5)
                else:
                    return

            logger.warning(f"Не удалось отправить в {entity.title} после {max_retries} попыток")
            with conn:
                cursor = conn.cursor()
                cursor.execute("""UPDATE broadcasts 
                                SET is_active = ? 
                                WHERE user_id = ? AND group_id = ?""",
                               (False, user_id, entity.id))
                if scheduler.get_job(jobs_id):
                    scheduler.remove_job(jobs_id)

        base = (min_m + max_m) // 2 if max_m else min_m
        jitter = (max_m - min_m) * 60 // 2 if max_m else 0
        trigger = IntervalTrigger(minutes=base, jitter=jitter)
        next_run = datetime.datetime.now() + datetime.timedelta(minutes=current_time)
        logger.info(f"Добавляем задачу отправить сообщения в {ent.title} в {next_run.isoformat()}")
        scheduler.print_jobs()
        scheduler.add_job(
            send_message,
            trigger,
            id=job_id,
            next_run_time=next_run,
            replace_existing=True,
        )
        logger.info(f"Создано новое задание {job_id} для группы {ent.title} (@{getattr(ent, 'username', 'без username')})")
        current_time += sec_run
    if not scheduler.running:
        logger.info("Запускаем все задачи")
        scheduler.start()

    await client.disconnect()
    cursor.close()
    
    if not ok_entities:
        return
