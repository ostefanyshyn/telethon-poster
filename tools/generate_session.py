import os
import argparse
from dotenv import load_dotenv
from telethon.sync import TelegramClient
from telethon.sessions import StringSession

# загружаем .env.gen, лежащий в той же папке, что и скрипт
ENV_PATH = os.path.join(os.path.dirname(__file__), ".env.gen")
load_dotenv(ENV_PATH)

TG_PASSWORD = os.getenv("TG_PASSWORD", "")

# --- Только одна сессия за запуск ---
parser = argparse.ArgumentParser(description="Сгенерировать StringSession для одного аккаунта.")
parser.add_argument(
    "-n", "--account", type=int, choices=[1, 2, 3], default=1,
    help="Какой набор переменных TG{n}_* использовать (по умолчанию: 1)."
)
args = parser.parse_args()
n = args.account

api_id = os.getenv(f"TG{n}_API_ID")
api_hash = os.getenv(f"TG{n}_API_HASH")
phone = os.getenv(f"TG{n}_PHONE")

if not all((api_id, api_hash, phone)):
    missing = [name for name in (f"TG{n}_API_ID", f"TG{n}_API_HASH", f"TG{n}_PHONE") if not os.getenv(name)]
    raise SystemExit("⛔  В .env.gen нет " + ", ".join(missing) + ".")

print(f"\n── Аккаунт {n}  ({phone}) ──")

with TelegramClient(StringSession(), int(api_id), api_hash) as client:
    client.connect()
    if not client.is_user_authorized():
        # отправит код на указанный номер
        client.send_code_request(phone)
        code = input("Введите код из Telegram: ")
        client.sign_in(phone, code, password=TG_PASSWORD or None)

    session_str = client.session.save()
    print("✔️  StringSession:", session_str)

print("\n=== Скопируйте в .env ===")
print(f"TG{n}_SESSION={session_str}")