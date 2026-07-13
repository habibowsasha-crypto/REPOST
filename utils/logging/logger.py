"""
Утилиты для красивого логирования событий Telegram
"""
from loguru import logger
from telethon.events import NewMessage, CallbackQuery


def format_user_info(user) -> str:
    """Форматирует информацию о пользователе для логов"""
    if hasattr(user, 'username') and user.username:
        return f"@{user.username} (ID: {user.id})"
    elif hasattr(user, 'first_name'):
        full_name = user.first_name
        if hasattr(user, 'last_name') and user.last_name:
            full_name += f" {user.last_name}"
        return f"{full_name} (ID: {user.id})"
    else:
        return f"ID: {user.id}"


def format_chat_info(chat) -> str:
    """Форматирует информацию о чате/группе для логов"""
    if hasattr(chat, 'username') and chat.username:
        return f"@{chat.username} (ID: {chat.id})"
    elif hasattr(chat, 'title'):
        return f"{chat.title} (ID: {chat.id})"
    else:
        return f"Chat ID: {chat.id}"


def log_message_event(event: NewMessage.Event, action: str = "получено сообщение") -> None:
    """
    Красиво логирует событие нового сообщения
    
    Args:
        event: Событие нового сообщения
        action: Описание действия (например, "получено сообщение", "обработано команда")
    """
    try:
        # Получаем информацию об отправителе
        sender_info = "Неизвестный отправитель"
        if hasattr(event, 'sender_id') and event.sender_id:
            sender_info = f"ID: {event.sender_id}"
        
        # Получаем информацию о сообщении
        message_text = "Пустое сообщение"
        if hasattr(event.message, 'text') and event.message.text:
            # Обрезаем длинные сообщения
            text = event.message.text
            if len(text) > 100:
                text = text[:97] + "..."
            message_text = f'"{text}"'
        
        # Получаем информацию о чате
        chat_info = "Личные сообщения"
        if hasattr(event.message, 'chat') and event.message.chat:
            if hasattr(event.message.chat, 'title'):
                chat_info = f"Группа: {event.message.chat.title}"
        
        logger.info(f"📨 {action.capitalize()}: {message_text} от {sender_info} в {chat_info}")
        
    except Exception as e:
        logger.error(f"Ошибка при логировании события сообщения: {e}")


def log_callback_event(event: CallbackQuery.Event, action: str = "получен callback") -> None:
    """
    Красиво логирует событие callback query
    
    Args:
        event: Событие callback query
        action: Описание действия
    """
    try:
        # Получаем информацию об отправителе
        sender_info = "Неизвестный отправитель"
        if hasattr(event, 'sender_id') and event.sender_id:
            sender_info = f"ID: {event.sender_id}"
        
        # Получаем данные callback
        callback_data = "Нет данных"
        if hasattr(event, 'data') and event.data:
            callback_data = event.data.decode('utf-8', errors='ignore')
        
        logger.info(f"🔘 {action.capitalize()}: '{callback_data}' от {sender_info}")
        
    except Exception as e:
        logger.error(f"Ошибка при логировании callback события: {e}")


def log_user_action(user_id: int, action: str, details: str = "") -> None:
    """
    Логирует действие пользователя
    
    Args:
        user_id: ID пользователя
        action: Описание действия
        details: Дополнительные детали
    """
    message = f"👤 Пользователь {user_id}: {action}"
    if details:
        message += f" - {details}"
    
    logger.info(message)


def log_broadcast_action(action: str, details: str = "") -> None:
    """
    Логирует действия рассылки
    
    Args:
        action: Описание действия рассылки
        details: Дополнительные детали
    """
    message = f"📢 Рассылка: {action}"
    if details:
        message += f" - {details}"
    
    logger.info(message)


def log_error_with_context(error: Exception, context: str = "") -> None:
    """
    Логирует ошибку с контекстом
    
    Args:
        error: Объект ошибки
        context: Контекст где произошла ошибка
    """
    message = "❌ Ошибка"
    if context:
        message += f" в {context}"
    message += f": {str(error)}"
    
    logger.error(message)