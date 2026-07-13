import asyncio
import os

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.sessions import StringSession


load_dotenv()


async def main() -> None:
    api_id = int(os.getenv("API_ID", "0"))
    api_hash = os.getenv("API_HASH", "").strip()
    session_string = os.getenv("SESSION_STRING", "").strip()

    if not api_id or not api_hash or not session_string:
        raise SystemExit("Заполни API_ID, API_HASH и SESSION_STRING в .env")

    client = TelegramClient(StringSession(session_string), api_id, api_hash)
    await client.start()

    print("\n=== Список доступных чатов / каналов ===\n")
    async for dialog in client.iter_dialogs():
        entity = dialog.entity
        username = getattr(entity, "username", None)
        print(f"ID: {dialog.id} | title: {dialog.name} | username: @{username if username else '-'}")

    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
