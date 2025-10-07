# generate_session.py — Telethon v2 (async) version, prints ONLY to console
import asyncio
from getpass import getpass
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import SessionPasswordNeededError


async def main():
    api_id = "27190730" #int(input("API_ID: ").strip())
    api_hash = "b4185638e99213319a46b328a32ee908"  # input("API_HASH: ").strip()
    phone = input("Номер телефона (в формате +380...): ").strip()

    client = TelegramClient(StringSession(), api_id, api_hash)
    await client.connect()

    try:
        if not await client.is_user_authorized():
            await client.send_code_request(phone)
            code = input("Код из Telegram (из SMS/приложения): ").strip()
            try:
                await client.sign_in(phone=phone, code=code)
            except SessionPasswordNeededError:
                pwd = getpass("Пароль 2FA: ")
                await client.sign_in(password=pwd)

        session_str = client.session.save()
        print("\n===== STRING SESSION (Telethon) =====\n")
        print(session_str)
        print("\n(Строка сессии выведена в консоль. Скопируйте и храните в безопасном месте.)")
    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())