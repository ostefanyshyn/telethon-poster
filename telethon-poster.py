# poster.py
# Telegram multi-account poster from Google Sheets
# -----------------------------------------------
# • трём аккаунтам нужны переменные:
#   TG{N}_API_ID, TG{N}_API_HASH, TG{N}_SESSION, TG{N}_CHANNEL
#   один общий GSHEET_ID (ID таблицы)
# • общий сервис-аккаунт Sheets → GOOGLE_CREDS_JSON (base-64 от creds.json)
# • рассчитывает время по Asia/Yerevan, постит альбом до 4 медиа,
#   собирает текст по шаблону с платными эмодзи

import os, json, base64, asyncio
from dotenv import load_dotenv      # ← добавили
load_dotenv()      
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import (
    MessageEntityCustomEmoji,
    InputMediaPhotoExternal,
)

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger

# ────────────────────────────────────────────────────────────────────────────
# Константы
TZ = ZoneInfo("Asia/Yerevan")

EMOJI = {   # ID кастом-эмодзи
    1: 5429293125518510398,
    2: 5814534640949530526,
    3: 5370853949358218655,
    4: 5370674552869232639,
    5: 5372943137415111238,
    6: 5373338978780979795,
    7: 5372991528811635071,
    8: 5364098734600762220,
    9: 5460811944883660881,
}
ZWSP = "\u2060"  # zero-width space, чтобы привязать entities

# соответствие "нормализованное имя" → "заголовок в Google Sheets"
FIELD_MAP = {
    "time_iso":       "Время",
    "status":         "Статус",
    "name":           "Имя",
    "skip":           "Пробелы перед короной",
    "services":       "Услуги",
    "extra_services": "Доп. услуги",
    "age":            "Возраст",
    "height":         "Рост",
    "weight":         "Вес",
    "bust":           "Грудь",
    "express":        "Express",
    "incall":         "Incall",
    "outcall":        "Outcall",
    "media1":         "Фото 1",
    "media2":         "Фото 2",
    "media3":         "Фото 3",
    "media4":         "Фото 4",
    "whatsapp":       "WhatsApp",
    "sent":           "Отправлено",
}

def canon(row: dict) -> dict:
    """Преобразует raw‑строку из Sheets в унифицированный dict."""
    return {k: row.get(v, "").strip() if isinstance(row.get(v), str) else row.get(v)
            for k, v in FIELD_MAP.items()}

# ────────────────────────────────────────────────────────────────────────────
# Google Sheets авторизация
gc_creds = json.loads(base64.b64decode(os.environ["GOOGLE_CREDS_JSON"]))
gc = gspread.authorize(
    ServiceAccountCredentials.from_json_keyfile_dict(
        gc_creds,
        ["https://spreadsheets.google.com/feeds",
         "https://www.googleapis.com/auth/drive"],
    )
)

# ────────────────────────────────────────────────────────────────────────────
# Читаем единый ID таблицы
SHEET_ID = os.getenv("GSHEET_ID")
if not SHEET_ID:
    raise SystemExit("⛔  Переменная GSHEET_ID не задана.")

# Собираем все аккаунты из переменных окружения
ACCOUNTS = []
for n in (1, 2, 3):
    api_id   = os.getenv(f"TG{n}_API_ID")
    api_hash = os.getenv(f"TG{n}_API_HASH")
    session  = os.getenv(f"TG{n}_SESSION")
    channel  = os.getenv(f"TG{n}_CHANNEL")
    if not all((api_id, api_hash, session, channel)):
        continue

    cli = TelegramClient(StringSession(session), int(api_id), api_hash)
    sheet = gc.open_by_key(SHEET_ID).sheet1  # первый таб
    ACCOUNTS.append({"client": cli, "channel": channel, "sheet": sheet})

# ────────────────────────────────────────────────────────────────────────────
def build_post(row: dict):
    """Возвращает (text, entities) согласно заданному шаблону."""
    entities, parts = [], []

    def add_emoji(num: int) -> str:
        offset = sum(len(p) for p in parts)
        entities.append(
            MessageEntityCustomEmoji(offset=offset, length=1, document_id=EMOJI[num])
        )
        return ZWSP

    # шапка
    parts += [add_emoji(1), f" {row['status']} ", add_emoji(1), "\n"]
    parts += [f"{row['skip']}{add_emoji(2)}\n"]
    parts += [f"{row['name']}\n\n"]

    # фото
    parts += ["Фото "]
    for num in (3, 4, 5, 6, 7):
        parts += [add_emoji(num), " "]
    parts += ["\n\n"]

    # услуги
    parts += ["Услуги:\n", f"{row['services']}\n", "Доп. услуги:\n",
              f"{row['extra_services']}\n\n"]

    # параметры
    parts += ["Параметры:\n",
              f"Возраст – {row['age']}\n",
              f"Рост   – {row['height']}\n",
              f"Вес    – {row['weight']}\n",
              f"Грудь  – {row['bust']}\n\n"]

    # цены
    parts += ["Цена:\n",
              f"Express – {row['express']}\n",
              f"Incall  – {row['incall']}\n",
              f"Outcall – {row['outcall']}\n\n"]

    # CTA
    parts += [add_emoji(8), " Назначь встречу уже сегодня! ", add_emoji(8), "\n"]
    parts += [f'<a href="{row["whatsapp"]}">Связь в WhatsApp</a> ', add_emoji(9)]

    return "".join(parts), entities

# ────────────────────────────────────────────────────────────────────────────
def record_from_sheet(row_raw: list[str]) -> dict:
    """Преобразование строки, **используется ТОЛЬКО когда в Google‑таблице нет шапки**.
    В вашем текущем листе заголовки есть, поэтому этот путь не задействуется."""
    return {
        "time_iso":       row_raw[1],
        "status":         row_raw[2],
        "name":           row_raw[3],
        "services":       row_raw[5],
        "extra_services": row_raw[6],
        "age":            row_raw[7],
        "height":         row_raw[8],
        "weight":         row_raw[9],
        "bust":           row_raw[10],
        "express":        row_raw[11],
        "incall":         row_raw[12],
        "outcall":        row_raw[13],
        "media1":         row_raw[16],
        "media2":         row_raw[17],
        "media3":         row_raw[18],
        "media4":         row_raw[19],
        "skip":           row_raw[20],
        "whatsapp":       row_raw[21],
    }

# ────────────────────────────────────────────────────────────────────────────
scheduler = AsyncIOScheduler(timezone=TZ)
# Интервал авто‑опроса таблицы (секунды). 0 = опрос выключен.
REFRESH_SEC = int(os.getenv("REFRESH_SECONDS", "0"))

def schedule_for_acc(acc: dict):
    """Ставит задачи из Google Sheet для конкретного аккаунта."""
    sheet = acc["sheet"]

    try:
        rows = sheet.get_all_records()          # если есть заголовки
        use_headers = True
    except gspread.exceptions.APIError:
        use_headers = False

    if use_headers:
        records = rows
    else:
        records = [record_from_sheet(sheet.row_values(i))
                   for i in range(2, sheet.row_count + 1)]

    now = datetime.now(TZ)
    for raw in records:
        r = canon(raw)

        # пропускаем уже отправленные или без времени
        if str(r.get("sent")).lower() in ("true", "yes", "1"):
            continue
        try:
            run_dt = datetime.fromisoformat(r["time_iso"]).replace(tzinfo=TZ)
        except (TypeError, ValueError):
            continue
        if run_dt < now:
            continue

        scheduler.add_job(
            send_post,
            trigger=DateTrigger(run_date=run_dt),
            args=[acc, r],
            name=f"{acc['channel']}_{run_dt.isoformat()}",
        )

async def send_post(acc: dict, row: dict):
    client, channel = acc["client"], acc["channel"]
    text, entities = build_post(row)

    media_urls = [row.get(k) for k in ("media1", "media2", "media3", "media4") if row.get(k)]
    if media_urls:
        album = [InputMediaPhotoExternal(u) for u in media_urls]
        await client.send_file(
            channel, album,
            caption=text,
            parse_mode="html",
            entities=entities,
        )
    else:
        await client.send_message(
            channel,
            text,
            parse_mode="html",
            entities=entities,
        )

# ────────────────────────────────────────────────────────────────────────────
async def main():
    # логиним все клиенты
    await asyncio.gather(*(acc["client"].start() for acc in ACCOUNTS))

    # планируем посты
    for acc in ACCOUNTS:
        schedule_for_acc(acc)

    scheduler.start()
    print("Scheduler started.")

    # Добавляем периодический опрос таблицы, если включено
    if REFRESH_SEC:
        for acc in ACCOUNTS:
            scheduler.add_job(
                schedule_for_acc,
                trigger="interval",
                seconds=REFRESH_SEC,
                args=[acc],
                max_instances=1,
                next_run_time=datetime.now(TZ) + timedelta(seconds=REFRESH_SEC),
            )
        print(f"Auto‑refresh every {REFRESH_SEC} s")

    # держим процесс, пока жив хотя бы один клиент
    await asyncio.gather(*(acc["client"].run_until_disconnected() for acc in ACCOUNTS))

if __name__ == "__main__":
    asyncio.run(main())