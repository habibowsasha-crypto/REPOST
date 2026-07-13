import argparse
import asyncio
import os
from typing import Any, Iterable

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.functions.channels import GetForumTopicsRequest


load_dotenv()


def _format_username(entity: Any) -> str:
    username = getattr(entity, "username", None)
    return f"@{username}" if username else "@-"


def _topic_closed(topic: Any) -> str:
    if getattr(topic, "closed", False):
        return " | closed: true"
    return ""


def _topic_hidden(topic: Any) -> str:
    if getattr(topic, "hidden", False):
        return " | hidden: true"
    return ""


def _print_topic(chat_id: int, chat_title: str, topic: Any) -> None:
    topic_id = getattr(topic, "id", None)
    title = getattr(topic, "title", "") or "(без названия)"
    top_message = getattr(topic, "top_message", None)
    extra = ""
    if top_message is not None and top_message != topic_id:
        extra = f" | top_message: {top_message}"
    print(
        f"chat_id: {chat_id} | chat_title: {chat_title} | "
        f"message_thread_id: {topic_id} | topic: {title}{extra}"
        f"{_topic_closed(topic)}{_topic_hidden(topic)}"
    )


async def _get_all_topics(client: TelegramClient, entity: Any) -> list[Any]:
    topics: list[Any] = []
    offset_date = None
    offset_id = 0
    offset_topic = 0

    while True:
        result = await client(
            GetForumTopicsRequest(
                channel=entity,
                q="",
                offset_date=offset_date,
                offset_id=offset_id,
                offset_topic=offset_topic,
                limit=100,
            )
        )
        batch = list(getattr(result, "topics", []) or [])
        if not batch:
            break

        topics.extend(batch)
        if len(batch) < 100:
            break

        last = batch[-1]
        offset_topic = int(getattr(last, "id", 0) or 0)
        offset_id = int(getattr(last, "top_message", 0) or 0)
        offset_date = getattr(last, "date", None)

        if not offset_topic and not offset_id:
            break

    return topics


async def _list_for_chat(client: TelegramClient, chat_ref: str) -> None:
    entity = await client.get_entity(chat_ref)
    chat_id = getattr(entity, "id", None)
    # Telethon entity.id can be internal positive id; dialog.id has the -100... form.
    dialog_id = None
    async for dialog in client.iter_dialogs():
        if dialog.entity and getattr(dialog.entity, "id", None) == chat_id:
            dialog_id = dialog.id
            break

    final_chat_id = dialog_id if dialog_id is not None else chat_ref
    chat_title = getattr(entity, "title", None) or getattr(entity, "first_name", None) or str(chat_ref)

    print(f"\n=== Темы: {chat_title} | chat_id: {final_chat_id} | username: {_format_username(entity)} ===\n")

    try:
        topics = await _get_all_topics(client, entity)
    except Exception as exc:
        print(f"Не удалось получить темы для {chat_ref}: {type(exc).__name__}: {exc}")
        print("Проверь, что это группа с включенными темами и аккаунт имеет к ней доступ.")
        return

    if not topics:
        print("Темы не найдены. Возможно, это обычный канал/группа без Telegram Topics.")
        return

    for topic in topics:
        _print_topic(final_chat_id, chat_title, topic)

    print(
        "\nФормат для EXTRA_TOPIC_ROUTE_MAP:\n"
        "SOURCE_CHAT_ID:SOURCE_TOPIC_ID>TARGET_CHAT_ID:TARGET_TOPIC_ID\n"
        f"Пример: {final_chat_id}:{getattr(topics[0], 'id', 'SOURCE_TOPIC')}>-100TARGET:TARGET_TOPIC"
    )


async def _list_dialogs_with_topics_hint(client: TelegramClient) -> None:
    print("\nУкажи ID группы с темами:\n")
    print("    py list_topics.py -1002106424484\n")
    print("Доступные группы/каналы аккаунта:\n")
    async for dialog in client.iter_dialogs():
        entity = dialog.entity
        username = getattr(entity, "username", None)
        title = dialog.name
        print(f"ID: {dialog.id} | title: {title} | username: @{username if username else '-'}")


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Показать message_thread_id тем Telegram-группы для EXTRA_TOPIC_ROUTE_MAP."
    )
    parser.add_argument(
        "chat",
        nargs="?",
        help="ID/username группы с темами, например -1002106424484 или @username",
    )
    args = parser.parse_args()

    api_id = int(os.getenv("API_ID", "0"))
    api_hash = os.getenv("API_HASH", "").strip()
    session_string = os.getenv("SESSION_STRING", "").strip()

    if not api_id or not api_hash or not session_string:
        raise SystemExit("Заполни API_ID, API_HASH и SESSION_STRING в .env")

    client = TelegramClient(StringSession(session_string), api_id, api_hash)
    await client.start()

    if args.chat:
        await _list_for_chat(client, args.chat)
    else:
        await _list_dialogs_with_topics_hint(client)

    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
