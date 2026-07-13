from services.admin_state import is_command_event
from services.menu_ui import render_menu
import asyncio
import datetime
from loguru import logger
from typing import Union, Optional

from apscheduler.triggers.interval import IntervalTrigger
from telethon import Button, TelegramClient
from telethon.errors import ChatWriteForbiddenError, ChatAdminRequiredError, FloodWaitError, SlowModeWaitError
from telethon.sessions import StringSession
from telethon.tl.types import Channel, Chat

from config import callback_query, callback_message, API_ID, API_HASH, scheduler, Query, bot, conn, \
    New_Message, broadcast_all_state_account
from utils.telegram import gid_key, create_broadcast_data, get_active_broadcast_groups, get_entity_by_id
from utils.logging import log_message_event, log_user_action


@bot.on(Query(data=lambda d: d.decode().startswith("broadcast_All_account")))
async def broadcast_all_menu(event: callback_query) -> None:
    keyboard = [
        [Button.inline("⏲️ Интервал во все группы", f"same_IntervalAll_account")],
        [Button.inline("🎲 Разный интервал (25-35)", f"diff_IntervalAll_account")]
    ]
    await render_menu(event, "Выберите режим отправки:", buttons=keyboard)


# ---------- одинаковый интервал ----------
@bot.on(Query(data=lambda d: d.decode().startswith("same_IntervalAll_account")))
async def same_interval_start(event: callback_query) -> None:
    admin_id = event.sender_id
    broadcast_all_state_account[admin_id] = {"mode": "same", "step": "text"}
    await render_menu(event, "📝 Пришлите текст рассылки для **всех** групп этого аккаунта:")


# ---------- случайный интервал ----------
@bot.on(Query(data=lambda d: d.decode().startswith("diff_IntervalAll_account")))
async def diff_interval_start(event: callback_query) -> None:
    admin_id = event.sender_id
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
                photo = await event.download_media()
                st["photo_url"] = photo
                
                # Запускаем рассылку с фото в зависимости от режима
                if st["mode"] == "same":
                    # Режим с одинаковым интервалом
                    await schedule_all_accounts_broadcast(st["text"], st["min_time"], None, photo)
                    message_type = "только с фото" if st["step"] == "photo_only" else "с фото"
                    await event.respond(f"✅ Запустил: каждые {st['min_time']} мин {message_type}.")
                else:
                    # Режим с разными интервалами
                    await schedule_all_accounts_broadcast(st["text"], st["min"], st["max_m"], photo)
                    message_type = "только с фото" if st["step"] == "photo_only" else "с фото"
                    await event.respond(f"✅ Запустил: случайно каждые {st['min']}-{st['max_m']} мин {message_type}.")
                
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
    if st["mode"] == "same":
        await schedule_all_accounts_broadcast(st["text"], st["min_time"], None)
        await render_menu(event, f"✅ Запустил: каждые {st['min_time']} мин.")
    else:
        await schedule_all_accounts_broadcast(st["text"], st["min"], st["max_m"])
        await render_menu(event, f"✅ Запустил: случайно каждые {st['min']}-{st['max_m']} мин.")
    
    broadcast_all_state_account.pop(user_id, None)


@bot.on(Query(data=lambda data: data.decode() == "Stop_Broadcast_All_account"))
async def stop_broadcast_all(event: callback_query) -> None:
    """Останавливает все активные рассылки для всех аккаунтов и групп"""
    msg_lines = ["⛔ **Остановленные рассылки**:\n\n"]
    processed_accounts = []

    with conn:
        cursor = conn.cursor()
        try:
            sessions = cursor.execute("SELECT user_id, session_string FROM sessions").fetchall()

            for user_id, session_string in sessions:
                # Проверяем, не обрабатывали ли мы уже этот аккаунт
                if user_id in processed_accounts:
                    continue
                processed_accounts.append(user_id)
                
                async with TelegramClient(StringSession(session_string), API_ID, API_HASH) as client:
                    try:
                        await client.connect()
                        account = await client.get_me()
                        username = getattr(account, 'username', 'без username')
                        account_name = account.first_name if hasattr(account, 'first_name') and account.first_name else username
                        
                        # Получаем активные группы для этого аккаунта
                        active_groups = get_active_broadcast_groups(user_id)
                        
                        if not active_groups:
                            continue
                        
                        # Добавляем информацию об аккаунте только если есть активные группы
                        account_msg = [f"**Аккаунт {account_name}**:\n"]
                        has_stopped_jobs = False
                        
                        # Получаем информацию о группах для отображения названий вместо ID
                        group_info = {}
                        groups_data = cursor.execute("""
                            SELECT group_id, group_username FROM groups WHERE user_id = ?
                        """, (user_id,)).fetchall()
                        
                        for g_id, g_username in groups_data:
                            group_info[g_id] = g_username
                        
                        # Обрабатываем каждую активную группу
                        for group_id in active_groups:
                            # Пробуем получить информацию о группе
                            try:
                                group_username = group_info.get(group_id, str(group_id))
                                
                                # Пробуем получить entity группы
                                try:
                                    # Проверяем, это username или ID
                                    if group_username.startswith('@'):
                                        # Это username группы
                                        group_entity = await client.get_entity(group_username)
                                    else:
                                        # Спробуємо отримати entity за ID
                                        try:
                                            group_id_int = int(group_username)
                                            group_entity = await get_entity_by_id(client, group_id_int)
                                            if not group_entity:
                                                # Если не удалось получить entity, используем только ID для отображения
                                                display_name = f"Група з ID {group_id}"
                                                
                                                # Проверяем и останавливаем задачи
                                                job_stopped = False
                                                job_id_all = f"broadcastALL_{user_id}_{gid_key(group_id)}"
                                                job_id_solo = f"broadcast_{user_id}_{gid_key(group_id)}"
                                                
                                                if scheduler.get_job(job_id_all):
                                                    scheduler.remove_job(job_id_all)
                                                    job_stopped = True
                                                
                                                if scheduler.get_job(job_id_solo):
                                                    scheduler.remove_job(job_id_solo)
                                                    job_stopped = True
                                                
                                                # Обновляем статус в базе данных
                                                cursor.execute(
                                                    "UPDATE broadcasts SET is_active = ? WHERE user_id = ? AND group_id = ?",
                                                    (False, user_id, gid_key(group_id)))
                                                
                                                # Добавляем сообщение о результате
                                                if job_stopped:
                                                    account_msg.append(f"⛔ Рассылка в {display_name} остановлена.\n")
                                                    has_stopped_jobs = True
                                                
                                                # Пропускаем дальнейшую обработку этой группы
                                                continue
                                        except ValueError:
                                            # Если не можем преобразовать в число, попробуем использовать как есть
                                            group_entity = await client.get_entity(group_username)
                                except Exception as entity_error:
                                    # Якщо не вдалося отримати entity, спробуємо альтернативний метод
                                    if "Cannot find any entity corresponding to" in str(entity_error):
                                        try:
                                            # Спробуємо отримати entity за ID
                                            try:
                                                group_id_int = int(group_username) if group_username.isdigit() else group_id
                                                group_entity = await get_entity_by_id(client, group_id_int)
                                                if not group_entity:
                                                    # Если не удалось получить entity, используем только ID для отображения
                                                    display_name = f"Група з ID {group_id}"
                                                    
                                                    # Проверяем и останавливаем задачи
                                                    job_stopped = False
                                                    job_id_all = f"broadcastALL_{user_id}_{gid_key(group_id)}"
                                                    job_id_solo = f"broadcast_{user_id}_{gid_key(group_id)}"
                                                    
                                                    if scheduler.get_job(job_id_all):
                                                        scheduler.remove_job(job_id_all)
                                                        job_stopped = True
                                                    
                                                    if scheduler.get_job(job_id_solo):
                                                        scheduler.remove_job(job_id_solo)
                                                        job_stopped = True
                                                    
                                                    # Обновляем статус в базе данных
                                                    cursor.execute(
                                                        "UPDATE broadcasts SET is_active = ? WHERE user_id = ? AND group_id = ?",
                                                        (False, user_id, gid_key(group_id)))
                                                    
                                                    # Добавляем сообщение о результате
                                                    if job_stopped:
                                                        account_msg.append(f"⛔ Рассылка в {display_name} остановлена.\n")
                                                        has_stopped_jobs = True
                                                    
                                                    # Пропускаем дальнейшую обработку этой группы
                                                    continue
                                            except ValueError:
                                                # Якщо не вдалося перетворити в число, просто зупиняємо задачі
                                                display_name = f"Група з ID {group_id}"
                                                
                                                # Проверяем и останавливаем задачи
                                                job_stopped = False
                                                job_id_all = f"broadcastALL_{user_id}_{gid_key(group_id)}"
                                                job_id_solo = f"broadcast_{user_id}_{gid_key(group_id)}"
                                                
                                                if scheduler.get_job(job_id_all):
                                                    scheduler.remove_job(job_id_all)
                                                    job_stopped = True
                                                
                                                if scheduler.get_job(job_id_solo):
                                                    scheduler.remove_job(job_id_solo)
                                                    job_stopped = True
                                                
                                                # Обновляем статус в базе данных
                                                cursor.execute(
                                                    "UPDATE broadcasts SET is_active = ? WHERE user_id = ? AND group_id = ?",
                                                    (False, user_id, gid_key(group_id)))
                                                
                                                # Добавляем сообщение о результате
                                                if job_stopped:
                                                    account_msg.append(f"⛔ Рассылка в {display_name} остановлена.\n")
                                                    has_stopped_jobs = True
                                                
                                                # Пропускаем дальнейшую обработку этой группы
                                                continue
                                        except Exception as alt_error:
                                            logger.error(f"Ошибка при альтернативном получении Entity: {alt_error}")
                                            
                                            # Если все методы не сработали, останавливаем задачу без информации о группе
                                            display_name = f"Група з ID {group_id}"
                                            
                                            # Проверяем и останавливаем задачи
                                            job_stopped = False
                                            job_id_all = f"broadcastALL_{user_id}_{gid_key(group_id)}"
                                            job_id_solo = f"broadcast_{user_id}_{gid_key(group_id)}"
                                            
                                            if scheduler.get_job(job_id_all):
                                                scheduler.remove_job(job_id_all)
                                                job_stopped = True
                                            
                                            if scheduler.get_job(job_id_solo):
                                                scheduler.remove_job(job_id_solo)
                                                job_stopped = True
                                            
                                            # Обновляем статус в базе данных
                                            cursor.execute(
                                                "UPDATE broadcasts SET is_active = ? WHERE user_id = ? AND group_id = ?",
                                                (False, user_id, gid_key(group_id)))
                                            
                                            # Добавляем сообщение о результате
                                            if job_stopped:
                                                account_msg.append(f"⛔ Рассылка в {display_name} остановлена.\n")
                                                has_stopped_jobs = True
                                            
                                            # Пропускаем дальнейшую обработку этой группы
                                            continue
                                    else:
                                        logger.error(f"Ошибка при получении информации о группе: {str(entity_error)}")
                                        continue
                                
                                # Пропускаем каналы-витрины
                                if isinstance(group_entity, Channel) and group_entity.broadcast and not group_entity.megagroup:
                                    logger.info(f"Пропускаємо канал {group_username}")
                                    continue
                                
                                # Формируем название для отображения
                                display_name = group_entity.title if hasattr(group_entity, 'title') else group_username
                                
                                # Проверяем и останавливаем задачи
                                job_stopped = False
                                job_id_all = f"broadcastALL_{user_id}_{gid_key(group_id)}"
                                job_id_solo = f"broadcast_{user_id}_{gid_key(group_id)}"
                                
                                if scheduler.get_job(job_id_all):
                                    scheduler.remove_job(job_id_all)
                                    job_stopped = True
                                
                                if scheduler.get_job(job_id_solo):
                                    scheduler.remove_job(job_id_solo)
                                    job_stopped = True
                                
                                # Обновляем статус в базе данных
                                cursor.execute(
                                    "UPDATE broadcasts SET is_active = ? WHERE user_id = ? AND group_id = ?",
                                    (False, user_id, gid_key(group_id)))
                                
                                # Добавляем сообщение о результате
                                if job_stopped:
                                    account_msg.append(f"⛔ Рассылка в группу **{display_name}** остановлена.\n")
                                    has_stopped_jobs = True
                                
                            except Exception as e:
                                logger.error(f"Ошибка при обработке группы {group_id}: {str(e)}")
                                
                                # В случае ошибки все равно пытаемся остановить задачу
                                try:
                                    job_id_all = f"broadcastALL_{user_id}_{gid_key(group_id)}"
                                    job_id_solo = f"broadcast_{user_id}_{gid_key(group_id)}"
                                    job_stopped = False
                                    
                                    if scheduler.get_job(job_id_all):
                                        scheduler.remove_job(job_id_all)
                                        job_stopped = True
                                    
                                    if scheduler.get_job(job_id_solo):
                                        scheduler.remove_job(job_id_solo)
                                        job_stopped = True
                                    
                                    # Обновляем статус в базе данных
                                    cursor.execute(
                                        "UPDATE broadcasts SET is_active = ? WHERE user_id = ? AND group_id = ?",
                                        (False, user_id, gid_key(group_id)))
                                    
                                    if job_stopped:
                                        account_msg.append(f"⛔ Рассылка в группу с ID {group_id} остановлена.\n")
                                        has_stopped_jobs = True
                                except Exception as stop_error:
                                    logger.error(f"Критическая ошибка при остановке рассылки: {stop_error}")
                                    continue
                        
                        # Добавляем сообщение об аккаунте только если были остановлены задачи
                        if has_stopped_jobs:
                            msg_lines.extend(account_msg)
                        
                    except Exception as e:
                        logger.error(f"Ошибка при обработке аккаунта {user_id}: {str(e)}")
                        msg_lines.append(f"⚠ Ошибка при обработке аккаунта {user_id}\n")
            
            # Если нет сообщений об остановленных рассылках, добавляем информационное сообщение
            if len(msg_lines) == 1:  # Только заголовок
                msg_lines.append("Нет активных рассылок для остановки.")
            
            await render_menu(event, "".join(msg_lines))

        finally:
            cursor.close()


async def schedule_all_accounts_broadcast(text: str,
                                          min_m: int,
                                          max_m: Optional[int] = None,
                                          photo_url: Optional[str] = None) -> None:
    """Планирует/обновляет задачи рассылки broadcastALL_<user>_<gid> только для чатов,
    куда пользователь действительно может писать."""

    with conn:
        cursor = conn.cursor()
        try:
            users = cursor.execute("SELECT user_id, session_string FROM sessions").fetchall()

            for user_id, session_string in users:
                cursor.execute("""UPDATE broadcasts SET broadcast_text = ? WHERE user_id = ?""",
                               (text, user_id))

                async with TelegramClient(StringSession(session_string), API_ID, API_HASH) as client:
                    await client.connect()

                    groups = cursor.execute("""SELECT group_username, group_id FROM groups 
                                            WHERE user_id = ?""", (user_id,)).fetchall()

                    ok_entities: list[Channel | Chat] = []
                    for group_username, group_id in groups:
                        try:
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

                            # Проверяем тип чата
                            if not isinstance(ent, (Channel, Chat)):
                                logger.info(f"Пропускаем {ent} - не чат/канал")
                                continue

                            # Пропускаем каналы-витрины
                            if isinstance(ent, Channel) and ent.broadcast and not ent.megagroup:
                                logger.info(f"Пропускаем {ent} - витрина-канал")
                                continue

                            ok_entities.append(ent)
                        except Exception as error:
                            logger.warning(f"Не смог проверить {group_username}: {error}")
                            continue

                    if not ok_entities:
                        continue

                    total_entities = len(ok_entities)
                    sec_run = ((max_m - min_m) / total_entities) if max_m else min_m
                    current_time = sec_run

                    for ent in ok_entities:
                        job_id = f"broadcastALL_{user_id}_{gid_key(ent.id)}"
                        interval = ((max_m - min_m) / total_entities) if max_m else min_m

                        create_broadcast_data(user_id, gid_key(ent.id), text, interval, photo_url)

                        if scheduler.get_job(job_id):
                            scheduler.remove_job(job_id)

                        async def send_message(
                                ss: str = session_string,
                                entity: Union[Channel, Chat] = ent,
                                jobs_id: str = job_id,
                                start_text: str = text,
                                start_photo_url: Optional[str] = photo_url,
                                max_retries: int = 10
                        ) -> None:
                            """Отправляет сообщение с обработкой ошибок и повторными попытками."""
                            retry_count = 0

                            while retry_count < max_retries:
                                try:
                                    async with TelegramClient(StringSession(ss), API_ID, API_HASH) as client:
                                        with conn:
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
                                                        logger.debug(f"Отправлено сообщение с фото в {entity.title}")
                                                    except Exception as photo_error:
                                                        logger.error(f"Ошибка при отправке с фото: {photo_error}")
                                                        # Если не удалось отправить с фото, пробуем отправить только текст
                                                        await client.send_message(entity, txt)
                                                        logger.debug(f"Отправлено сообщение без фото в {entity.title}")
                                                else:
                                                    # Отправляем обычное текстовое сообщение
                                                    await client.send_message(entity, txt)
                                                    logger.debug(f"Успешно отправлено в {entity.title}")
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
                                                                logger.debug(f"Отправлено сообщение с фото через альтернативный метод в {new_entity.title}")
                                                            except Exception as alt_photo_error:
                                                                logger.error(f"Ошибка при отправке с фото через альтернативный метод: {alt_photo_error}")
                                                                # Если не удалось отправить с фото, пробуем отправить только текст
                                                                await client.send_message(new_entity, txt)
                                                                logger.debug(f"Отправлено сообщение без фото через альтернативный метод в {new_entity.title}")
                                                        else:
                                                            await client.send_message(new_entity, txt)
                                                            logger.debug(f"Отправлено через альтернативный метод в {new_entity.title}")
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
                        jitter = (max_m - min_m) * 60 // 2 if max_m else min_m * 30
                        trigger = IntervalTrigger(minutes=base, jitter=jitter)
                        next_run = datetime.datetime.now() + datetime.timedelta(minutes=current_time)

                        logger.info(f"Добавляем задачу для {ent.title} на {next_run.isoformat()}")
                        scheduler.add_job(
                            send_message,
                            trigger,
                            id=job_id,
                            next_run_time=next_run,
                            replace_existing=True,
                        )
                        current_time += sec_run
        finally:
            cursor.close()

    if not scheduler.running:
        logger.info("Запускаем планировщик задач")
        scheduler.start()
