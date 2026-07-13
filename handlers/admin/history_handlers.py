from services.menu_ui import render_menu
from config import callback_query, Query, bot, conn
from telethon import events
import datetime
import os.path


@bot.on(Query(data=lambda data: data.decode().startswith("show_history")))
async def show_history(event: callback_query) -> None:
    cursor = conn.cursor()
    
    # Обновляем запрос, чтобы получить информацию о фото
    cursor.execute("""
            SELECT h.group_name, h.sent_at, h.message_text, b.photo_url
            FROM send_history h
            LEFT JOIN broadcasts b ON h.user_id = b.user_id AND h.group_id = b.group_id
            ORDER BY h.sent_at DESC
            LIMIT 10
        """)
    rows = cursor.fetchall()
    cursor.close()
    if not rows:
        await render_menu(event, "❌ История рассылки пуста.")
        return
    
    
    messages = ["🕗 **10 последних рассылок:**\n\n"]
    current_msg_index = 0
    current_length = len(messages[0])
    max_length = 4000  
    
    num = 1
    for row in rows:
        group_name, sent_at, message_text, photo_url = row
        
        if message_text and len(message_text) > 100:
            message_text = message_text[:97] + "..."
            
        entry = f"📌№{num}, Группа - **{group_name}**\n🕓 Время - **{sent_at}**\n💬 Сообщение - **{message_text}**"
        
        # Добавляем информацию о фото, если оно есть
        if photo_url:
            # Получаем только имя файла из пути
            photo_name = os.path.basename(photo_url) if photo_url else "неизвестно"
            entry += f"\n🖼 Фото: {photo_name}"
        
        entry += "\n\n"
        entry_length = len(entry)
        
        
        if current_length + entry_length > max_length:
            
            messages.append(entry)
            current_msg_index += 1
            current_length = entry_length
        else:
            
            messages[current_msg_index] += entry
            current_length += entry_length
        
        num += 1
    
    
    for msg in messages:
        await render_menu(event, msg)
