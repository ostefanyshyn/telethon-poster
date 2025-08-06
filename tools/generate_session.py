import os
from dotenv import load_dotenv
from telethon.sync import TelegramClient
from telethon.sessions import StringSession

# загружаем .env.gen, лежащий в той же папке, что и скрипт
ENV_PATH = os.path.join(os.path.dirname(__file__), ".env.gen")
load_dotenv(ENV_PATH)

TG_PASSWORD = os.getenv("TG_PASSWORD", "")

ACCOUNTS = []
for n in (1, 2, 3):
    api_id   = os.getenv(f"TG{n}_API_ID")
    api_hash = os.getenv(f"TG{n}_API_HASH")
    phone    = os.getenv(f"TG{n}_PHONE")
    if all((api_id, api_hash, phone)):
        ACCOUNTS.append((int(api_id), api_hash, phone))
if not ACCOUNTS:
    raise SystemExit("⛔  В .env.gen нет TG{n}_API_ID / TG{n}_API_HASH / TG{n}_PHONE.")

SESSIONS = []

for idx, (api_id, api_hash, phone) in enumerate(ACCOUNTS, start=1):
    print(f"\n── Аккаунт {idx}  ({phone}) ──")
    with TelegramClient(StringSession(), api_id, api_hash) as client:
        client.connect()
        if not client.is_user_authorized():
            # отправит код на указанный номер
            client.send_code_request(phone)
            code = input("Введите код из Telegram: ")
            client.sign_in(phone, code, password=TG_PASSWORD or None)
        session_str = client.session.save()
        SESSIONS.append(session_str)
        print("✔️  StringSession:", session_str)

print("\n=== Скопируйте в .env ===")
for i, s in enumerate(SESSIONS, start=1):
    print(f"TG{i}_SESSION={s}")