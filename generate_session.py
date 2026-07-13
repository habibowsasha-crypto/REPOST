import asyncio
import getpass
import os

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.sessions import StringSession


load_dotenv()


def ask_value(name: str, secret: bool = False) -> str:
    current = os.getenv(name, "").strip()
    if current:
        return current
    if secret:
        return getpass.getpass(f"{name}: ").strip()
    return input(f"{name}: ").strip()


async def main() -> None:
    print("\n=== Telethon StringSession generator ===\n")
    print("Запускай этот файл только локально на своём компьютере.")
    print("SESSION_STRING нельзя отправлять никому и нельзя заливать в GitHub.\n")

    api_id_raw = ask_value("API_ID")
    api_hash = ask_value("API_HASH", secret=False)

    try:
        api_id = int(api_id_raw)
    except ValueError as exc:
        raise SystemExit("API_ID должен быть числом") from exc

    client = TelegramClient(StringSession(), api_id, api_hash)

    await client.start(
        phone=lambda: input("Телефон в международном формате, например +79990000000: ").strip(),
        code_callback=lambda: input("Код из Telegram: ").strip(),
        password=lambda: getpass.getpass("2FA пароль, если Telegram попросит: "),
    )

    session_string = client.session.save()

    print("\n=== ГОТОВО. СКОПИРУЙ SESSION_STRING НИЖЕ ===\n")
    print(session_string)
    print("\nВАЖНО:")
    print("1. Добавь эту строку в Railway Variables как SESSION_STRING.")
    print("2. Не сохраняй её в GitHub.")
    print("3. Если строка утекла - заверши активные сессии Telegram.\n")

    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
