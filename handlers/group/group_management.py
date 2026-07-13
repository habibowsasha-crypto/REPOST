from services.menu_ui import render_menu
from loguru import logger
from typing import List, Optional, Union

from telethon import Button, TelegramClient
from telethon.sessions import StringSession
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.types import Channel, Chat, DialogFilter

from config import callback_query, API_ID, API_HASH, Query, bot, conn, processed_callbacks
from utils.telegram import broadcast_status_emoji, gid_key, get_entity_by_id


@bot.on(Query(data=lambda d: d.decode().startswith("account_") and d.decode().split("_", 1)[1].isdigit()))
async def account_menu(event: callback_query) -> None:
    """Обрабатывает нажатие кнопки "Назад" в списке групп и возвращает к меню аккаунта."""
    # Получаем уникальный идентификатор для этого callback
    callback_id = f"{event.sender_id}:{getattr(event.query, 'query_id', event.query.msg_id)}"
    
    # Проверяем, был ли уже обработан этот callback
    if callback_id in processed_callbacks:
        # Этот callback уже был обработан, просто возвращаемся без ответа
        return
        
    # Отмечаем callback как обработанный
    processed_callbacks[callback_id] = True
    
    data = event.data.decode()
    parts = data.split("_")
    
    # Проверяем формат данных
    if len(parts) < 2:
        await render_menu(event, "⚠ Ошибка: неверный формат данных")
        return
        
    # Проверяем, есть ли "info" в callback data
    if parts[1] == "info":
        # Формат account_info_user_id
        if len(parts) < 3:
            await render_menu(event, "⚠ Ошибка: неверный формат данных")
            return
        try:
            user_id = int(parts[2])
        except ValueError:
            await render_menu(event, "⚠ Ошибка: неверный ID пользователя")
            return
    else:
        # Формат account_user_id
        try:
            user_id = int(parts[1])
        except ValueError:
            await render_menu(event, "⚠ Ошибка: неверный ID пользователя")
            return
    
    # Формируем кнопки для меню аккаунта
    buttons = [
        [Button.inline("🔎 Найти группы аккаунта", f"sync_groups_{user_id}".encode())],
        [Button.inline("📋 Найденные группы", f"discovered_groups_{user_id}_0".encode())],
        [Button.inline("📋 Рабочий список групп", f"groups_{user_id}".encode())],
        [Button.inline("📢 Запустить рассылку во все группы", f"broadcastAll_{user_id}".encode())],
        [Button.inline("❌ Остановить общую рассылку", f"StopBroadcastAll_{user_id}".encode())],
        [Button.inline("◀️ Назад", b"my_accounts")],
        [Button.inline("🏠 Главное меню", b"menu_home")],
    ]
    
    # Отправляем меню аккаунта
    await render_menu(event, f"📱 **Меню аккаунта**\n\nВыберите действие:", buttons=buttons)


@bot.on(Query(data=b"my_groups"))
async def my_groups(event: callback_query) -> None:
    # Получаем уникальный идентификатор для этого callback
    callback_id = f"{event.sender_id}:{getattr(event.query, 'query_id', event.query.msg_id)}"
    
    # Проверяем, был ли уже обработан этот callback
    if callback_id in processed_callbacks:
        # Этот callback уже был обработан, просто возвращаемся без ответа
        return
        
    # Отмечаем callback как обработанный
    processed_callbacks[callback_id] = True
    
    cursor = conn.cursor()
    cursor.execute("SELECT group_id, group_username FROM groups")
    groups = cursor.fetchall()
    cursor.close()
    message = "❌ У вас нет добавленных групп."
    buttons = []
    if groups:
        message = "📑 **Список добавленных групп:**\n"
        buttons.append([Button.inline("➕ Добавить все аккаунты в эти группы", b"add_all_accounts_to_groups")])
        buttons.append([Button.inline("❌ Удалить группу", b"delete_group")])
        for group in groups:
            message += f"{group[1]}\n"
    buttons.append([Button.inline("🏠 Главное меню", b"menu_home")])
    await render_menu(event, message, buttons=buttons)


@bot.on(Query(data=b"add_all_accounts_to_groups"))
async def add_all_accounts_to_groups(event: callback_query) -> None:
    # Получаем уникальный идентификатор для этого callback
    callback_id = f"{event.sender_id}:{getattr(event.query, 'query_id', event.query.msg_id)}"
    
    # Проверяем, был ли уже обработан этот callback
    if callback_id in processed_callbacks:
        # Этот callback уже был обработан, просто возвращаемся без ответа
        return
        
    # Отмечаем callback как обработанный
    processed_callbacks[callback_id] = True
    
    cursor = conn.cursor()
    cursor.execute("SELECT user_id, session_string FROM sessions")
    accounts = cursor.fetchall()

    cursor.execute("SELECT group_id, group_username FROM groups")
    groups = cursor.fetchall()
    if not accounts:
        await render_menu(event, "❌ Нет добавленных аккаунтов.")
        return

    if not groups:
        await render_menu(event, "❌ Нет добавленных групп.")
        return

    for account in accounts:
        session = StringSession(account[1])
        client = TelegramClient(session, API_ID, API_HASH)
        await client.connect()
        try:
            for group in groups:
                try:
                    await client(JoinChannelRequest(group[1]))
                except Exception as e:
                    logger.error(f"Ошибка {e}")
                cursor.execute("""INSERT OR IGNORE INTO groups 
                                        (user_id, group_id, group_username) 
                                        VALUES (?, ?, ?)""", (account[0], group[0], group[1]))
                logger.info(f"Добавляем в базу данных группу ({account[0], group[0], group[1]})")
        except Exception as e:
            await render_menu(event, f"⚠ Ошибка при добавлении аккаунта: {e}")
        finally:
            await client.disconnect()
    group_list = "\n".join([f"📌 {group[1]}" for group in groups])
    await render_menu(event, f"✅ Аккаунты успешно добавлены в следующие группы:\n{group_list}")
    conn.commit()
    cursor.close()


@bot.on(Query(data=lambda event: event.decode().startswith("add_all_groups_")))
async def add_all_groups_to_account(event: callback_query) -> None:
    """Compatibility callback for buttons from older bot messages."""
    from handlers.group.group_discovery_handlers import sync_groups

    await sync_groups(event)


@bot.on(Query(data=lambda d: d.decode().startswith("groups_")))
async def groups_list(event: callback_query) -> None:
    """Отображает список групп пользователя."""
    # Получаем уникальный идентификатор для этого callback
    callback_id = f"{event.sender_id}:{getattr(event.query, 'query_id', event.query.msg_id)}"
    
    # Проверяем, был ли уже обработан этот callback
    if callback_id in processed_callbacks:
        # Этот callback уже был обработан, просто возвращаемся без ответа
        return
        
    # Отмечаем callback как обработанный
    processed_callbacks[callback_id] = True
    
    data = event.data.decode()
    user_id = int(data.split("_")[1])
    
    cursor = conn.cursor()
    session_row = cursor.execute("SELECT session_string FROM sessions WHERE user_id = ?", (user_id,)).fetchone()
    
    if not session_row:
        await render_menu(event, "⚠ Ошибка: не найдена сессия для этого аккаунта.")
        cursor.close()
        return
        
    session_string = session_row[0]
    client = TelegramClient(StringSession(session_string), API_ID, API_HASH)
    
    try:
        await client.connect()
        
        # Получаем список групп из БД
        cursor.execute("SELECT group_id, group_username FROM groups WHERE user_id = ?", (user_id,))
        groups = cursor.fetchall()
        
        if not groups:
            await render_menu(event, "📋 У вас нет добавленных групп. Добавьте группы через главное меню.")
            await client.disconnect()
            cursor.close()
            return
            
        # Формируем список групп с информацией о статусе рассылки
        group_list = []
        
        for group_id, group_username in groups:
            try:
                # Пытаемся получить entity группы
                try:
                    ent = await client.get_entity(group_username)
                except Exception as entity_error:
                    if "Cannot find any entity corresponding to" in str(entity_error):
                        try:
                            # Преобразуем username в ID, если это возможно
                            try:
                                group_id_int = int(group_username)
                                ent = await get_entity_by_id(client, group_id_int)
                                if not ent:
                                    logger.error(f"Не удалось получить entity для группы {group_username}")
                                    continue
                            except ValueError:
                                # Если username не является числом, пропускаем
                                logger.error(f"Не удалось получить entity для группы {group_username}")
                                continue
                        except Exception as alt_error:
                            logger.error(f"Ошибка при альтернативном получении Entity: {alt_error}")
                            continue
                    else:
                        logger.error(f"Ошибка при получении entity для группы {group_username}: {entity_error}")
                        continue
                
                # Получаем статус рассылки
                status = broadcast_status_emoji(user_id, group_id)
                
                # Формируем название группы для отображения
                group_name = getattr(ent, 'title', group_username)
                
                # Используем gid_key для правильной обработки ID группы
                group_list.append((gid_key(group_id), group_name, status))
            except Exception as e:
                logger.error(f"Ошибка при обработке группы {group_username}: {e}")
                continue
                
        # Формируем сообщение и кнопки
        if group_list:
            # Создаем кнопки для каждой группы
            buttons = []
            for group_id, group_name, status in group_list:
                # Используем правильный формат данных для кнопки
                data = f"groupInfo_{user_id}_{group_id}".encode()
                buttons.append([Button.inline(f"{status} {group_name}", data)])
                
            # Добавляем кнопку "Назад"
            buttons.append([Button.inline("◀️ Назад", f"account_info_{user_id}".encode())])
            
            await render_menu(event, "📋 **Список ваших групп:**\n\nВыберите группу для просмотра информации:", buttons=buttons)
        else:
            await render_menu(event, "⚠ Не удалось получить информацию о группах. Возможно, они были удалены или недоступны.")
            
    except Exception as e:
        logger.error(f"Ошибка при получении списка групп: {e}")
        await render_menu(event, f"⚠ Ошибка при получении списка групп: {str(e)}")
    finally:
        await client.disconnect()
        cursor.close()
