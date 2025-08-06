import os
import asyncio
import base64
import json
from datetime import datetime
import pytz
import requests
import io

import gspread
from telethon import TelegramClient
from telethon.sessions import StringSession
from dotenv import load_dotenv
load_dotenv()  # automatically pull variables from a .env file into os.environ

# Load configuration from environment variables
GSHEET_ID = os.environ.get("GSHEET_ID")
GOOGLE_CREDS_JSON = os.environ.get("GOOGLE_CREDS_JSON")
TG1_API_ID = int(os.environ.get("TG1_API_ID", 0))
TG1_API_HASH = os.environ.get("TG1_API_HASH")
TG1_SESSION = os.environ.get("TG1_SESSION")  # may be None or empty if not provided
TG1_CHANNEL = os.environ.get("TG1_CHANNEL")  # channel IDs as strings (including the '-' for channels)
TG2_API_ID = int(os.environ.get("TG2_API_ID", 0))
TG2_API_HASH = os.environ.get("TG2_API_HASH")
TG2_SESSION = os.environ.get("TG2_SESSION")
TG2_CHANNEL = os.environ.get("TG2_CHANNEL")
TG3_API_ID = int(os.environ.get("TG3_API_ID", 0))
TG3_API_HASH = os.environ.get("TG3_API_HASH")
TG3_SESSION = os.environ.get("TG3_SESSION")
TG3_CHANNEL = os.environ.get("TG3_CHANNEL")
REFRESH_SECONDS = int(os.environ.get("REFRESH_SECONDS", 30))

# Parse Google service account credentials from the base64 JSON string
credentials_json = json.loads(base64.b64decode(GOOGLE_CREDS_JSON)) if GOOGLE_CREDS_JSON else None

# Authorize gspread and open the sheet
gc = gspread.service_account_from_dict(credentials_json)  # use creds dict [oai_citation:12‡stackoverflow.com](https://stackoverflow.com/questions/71869394/python-make-gspread-service-account-take-a-python-dictionary-or-string-instead#:~:text=import%20gspread) [oai_citation:13‡stackoverflow.com](https://stackoverflow.com/questions/71869394/python-make-gspread-service-account-take-a-python-dictionary-or-string-instead#:~:text=gc%20%3D%20gspread)
sheet = gc.open_by_key(GSHEET_ID)
worksheet = sheet.sheet1  # assuming data is in the first sheet

# Timezone for parsing schedule times (Armenia)
tz = pytz.timezone("Asia/Yerevan")

# Telegram clients setup for three accounts
client1 = TelegramClient(StringSession(TG1_SESSION) if TG1_SESSION else 'tg1_session', TG1_API_ID, TG1_API_HASH)
client2 = TelegramClient(StringSession(TG2_SESSION) if TG2_SESSION else 'tg2_session', TG2_API_ID, TG2_API_HASH)
client3 = TelegramClient(StringSession(TG3_SESSION) if TG3_SESSION else 'tg3_session', TG3_API_ID, TG3_API_HASH)

# ---------------------------------------------------------------------------
# Custom HTML parser to convert <a href="emoji/{id}">X</a> into MessageEntityCustomEmoji
from telethon.extensions import html as tl_html
from telethon import types

class CustomHtml:
    @staticmethod
    def parse(text):
        text, entities = tl_html.parse(text)
        for i, e in enumerate(entities):
            if isinstance(e, types.MessageEntityTextUrl) and e.url.startswith("emoji/"):
                emoji_id = int(e.url.split("/", 1)[1])
                entities[i] = types.MessageEntityCustomEmoji(
                    e.offset, e.length, document_id=emoji_id
                )
        return text, entities

    @staticmethod
    def unparse(text, entities):
        # Needed if you ever download messages and turn entities back into HTML
        for i, e in enumerate(entities or []):
            if isinstance(e, types.MessageEntityCustomEmoji):
                entities[i] = types.MessageEntityTextUrl(
                    e.offset, e.length, url=f"emoji/{e.document_id}"
                )
        return tl_html.unparse(text, entities)

# Set the custom HTML parser as the parse_mode for all three Telegram clients
for _c in (client1, client2, client3):
    _c.parse_mode = CustomHtml()

# Custom emoji IDs as given
emoji_ids = {
    1: 5429293125518510398,
    2: 5814534640949530526,
    3: 5370853949358218655,
    4: 5370674552869232639,
    5: 5372943137415111238,
    6: 5373338978780979795,
    7: 5372991528811635071,
    8: 5364098734600762220,
    9: 5460811944883660881
}
# Placeholder symbols for each emoji (using generic or related Unicode emoji)
emoji_placeholders = {
    1: "☁️",  # crown (assumed for status)
    2: "👑",  # star or any symbol for second line
    3: "✅",  # camera for photo (example)
    4: "✅",
    5: "✅",
    6: "✅",
    7: "✅",
    8: "⚡️",  # fire or any highlight emoji for call-to-action
    9: "😜",  # playful face for contact link
}

async def send_post(record, row_idx):
    """Send a post to all three channels based on the data in record (a dict)."""
    status = record["Статус"]  # e.g., "Привет"
    name = record["Имя"]
    services = record["Услуги"]
    extra_services = record["Доп. услуги"]
    age = record["Возраст"]
    height = record["Рост"]
    weight = record["Вес"]
    bust = record["Грудь"]
    express_price = record["Express"]
    incall_price = record["Incall"]
    outcall_price = record["Outcall"]
    whatsapp_link = record["WhatsApp"]
    # Text that must appear *before* the crown sign on its own line
    skip_text = record.get("Пробелы перед короной", "")
    # ────────── compose parameter/price blocks ──────────
    # Build parameter lines dynamically (show only non‑empty fields)
    param_lines = []
    if age and str(age).strip():
        param_lines.append(f"Возраст - {age}")
    if height and str(height).strip():
        param_lines.append(f"Рост - {height}")
    if weight and str(weight).strip():
        param_lines.append(f"Вес - {weight}")
    if bust and str(bust).strip():
        param_lines.append(f"Грудь - {bust}")

    # Build the message HTML string following the desired layout
    message_html_lines = []

    # Optional blank lines before the first line
    if skip_text and skip_text.strip() != "":
        message_html_lines.append(skip_text)

    # ☁️  *status*  ☁️
    message_html_lines.append(
        f'<a href="emoji/{emoji_ids[1]}">{emoji_placeholders[1]}</a>'
        f'<i>{status}</i>'
        f'<a href="emoji/{emoji_ids[1]}">{emoji_placeholders[1]}</a>'
        
    )
    # Add a blank line after the status
    message_html_lines.append("")

    # строка с короной и префиксом из столбца U (если есть)
    prefix = f"{skip_text}" if skip_text else ""
    message_html_lines.append(
        f'{prefix}<a href="emoji/{emoji_ids[2]}">{emoji_placeholders[2]}</a>'
    )

    # Bold + italic name
    message_html_lines.append(f'<b><i>{name}</i></b>')

    # Bold “Фото …” line with five check‑mark emojis
    foto_checks = "".join(
        f'<a href="emoji/{emoji_ids[i]}">{emoji_placeholders[i]}</a>'
        for i in range(3, 8)
    )   
    
    # --- после строки «Фото …»
    message_html_lines.append("")
    message_html_lines.append(f'<b>Фото {foto_checks}</b>')
    message_html_lines.append("")          # ← пропуск

    # --- блок «Услуги» (показываем только если поле непустое) ---
    if services and str(services).strip():
        message_html_lines.append("Услуги:")
        message_html_lines.append(f'<b><i>{services}</i></b>')
        message_html_lines.append("")      # пустая строка‑разделитель

    # --- блок «Доп. услуги» (показываем только если поле непустое) ---
    if extra_services and str(extra_services).strip():
        message_html_lines.append("Доп. услуги:")
        message_html_lines.append(f'<b><i>{extra_services}</i></b>')
        message_html_lines.append("")      # пустая строка‑разделитель

    # --- блок «Параметры»
    if param_lines:                         # выводим блок только если есть хотя бы один параметр
        message_html_lines.append("Параметры:")
        message_html_lines.append(f'<b><i>{"\n".join(param_lines)}</i></b>')
        message_html_lines.append("")       # пустая строка‑разделитель

    # ─── формируем блок «Цена» динамически ───
    def _fmt_price(val):
        """
        Convert cell value to '<value>.000 AMD' style.
        If conversion fails, return the raw string with ' AMD' suffix.
        """
        try:
            num = float(str(val).replace(",", "."))
            # show always three decimals, e.g. 40.000
            return f"{num:.3f} AMD"
        except Exception:
            return f"{val} AMD"

    price_lines = []
    if express_price and str(express_price).strip():
        price_lines.append(f"Express - {_fmt_price(express_price)}")
    if incall_price and str(incall_price).strip():
        price_lines.append(f"Incall - {_fmt_price(incall_price)}")
    if outcall_price and str(outcall_price).strip():
        price_lines.append(f"Outcall - {_fmt_price(outcall_price)}")

    if price_lines:                         # выводим блок только если есть хотя бы одна цена
        message_html_lines.append("Цена:")
        message_html_lines.append(f'<b><i>{"\n".join(price_lines)}</i></b>')
        message_html_lines.append("")       # пустая строка‑разделитель

    # Call‑to‑action line with ⚡️ emojis (id 8)
    message_html_lines.append(
        f'<a href="emoji/{emoji_ids[8]}">{emoji_placeholders[8]}</a>'
        f'<b><i>Назначь встречу уже сегодня!</i></b>'
        f'<a href="emoji/{emoji_ids[8]}">{emoji_placeholders[8]}</a>'
    )

    # Contact line with 😌 (id 9)
    message_html_lines.append(
        f'<a href="{whatsapp_link}"><b>Связь в WhatsApp</b></a> '
        f'<a href="emoji/{emoji_ids[9]}">{emoji_placeholders[9]}</a>'
    )

    # Join all parts with newline separator
    message_html = "\n".join(message_html_lines)

    # Gather media files
    file_count = 0
    if "Фото в посте" in record:
        # This column might be an int or string number indicating how many photos to attach
        val = record["Фото в посте"]
        if isinstance(val, int):
            file_count = val
        elif isinstance(val, str) and val.isdigit():
            file_count = int(val)
    # Get the photo URLs from Q, R, S, T (we have them in record if get_all_records is used, likely as keys or we use indices)
    # The CSV header shows Photo URLs under keys 'Фото 1', 'Фото 2', etc., we need to match correctly.
    photo_keys = [k for k in record.keys() if k.startswith("Фото ") or k.startswith("Photo")]
    # Sort the keys to ensure order (Фото 1, Фото 2, ...)
    photo_keys.sort()
    photo_urls = []
    for pk in photo_keys:
        url = record[pk]
        if url and isinstance(url, str) and url.startswith("http"):
            photo_urls.append(url)
    # Download photos into raw-byte tuples so each Telegram client gets its own BytesIO copy
    photo_data = []  # list of (bytes, file_name)
    if file_count > 0 and photo_urls:
        for url in photo_urls[:file_count]:
            try:
                resp = requests.get(url)
                resp.raise_for_status()
                file_data = resp.content
                file_name = url.split("/")[-1] or "image.jpg"
                photo_data.append((file_data, file_name))
            except Exception as e:
                print(f"Warning: failed to download image {url} - {e}")
    # Send message (with media if available) via all three clients concurrently
    tasks = []
    # Determine target channels (convert to int if needed)
    channels = [TG1_CHANNEL, TG2_CHANNEL, TG3_CHANNEL]
    # Convert channel IDs to int for Telethon if they look like integers
    channels = [int(ch) if ch and ch.isdigit() or (ch and ch.startswith("-")) else ch for ch in channels]
    # Prepare send tasks
    for client, channel in zip([client1, client2, client3], channels):
        if photo_data:
            # Re‑instantiate fresh BytesIO objects so each client gets independent streams
            file_objs = []
            for data, fname in photo_data:
                bio = io.BytesIO(data)
                bio.name = fname
                file_objs.append(bio)
            tasks.append(client.send_file(channel, file_objs, caption=message_html))
        else:
            tasks.append(client.send_message(channel, message_html))
    # Run all send tasks concurrently
    await asyncio.gather(*tasks)
    # Update the Google Sheet to mark as sent (column A to TRUE)
    worksheet.update_cell(row_idx, 1, "TRUE")  # row_idx is actual sheet row index (1-based). Column 1 = A.
    print(f"Posted and marked row {row_idx} as sent.")

async def main():
    # Start all clients (connect them)
    await client1.start()
    await client2.start()
    await client3.start()
    print("Telegram clients connected. Starting schedule loop...")
    while True:
        # Fetch all records from sheet
        records = worksheet.get_all_records()  # this gives a list of dicts, excluding header
        now = datetime.now(tz)
        # Iterate with index to know which row to update (index 0 corresponds to sheet row 2, since row1 is header)
        for idx, record in enumerate(records, start=2):  # start=2 to account for header row
            sent_flag = record.get("Отправлено")  # could be boolean True/False or string "FALSE"/"TRUE"
            # Normalize the sent flag to boolean
            if sent_flag in [True, "TRUE", "True", "true"]:
                sent = True
            else:
                sent = False
            if sent:
                continue  # skip already sent
            time_str = record.get("Время")
            if not time_str:
                continue
            # Parse the scheduled time
            try:
                sched_time = datetime.strptime(time_str, "%d.%m.%Y %H:%M:%S")
            except Exception as e:
                # If parsing fails, skip this entry
                print(f"Failed to parse time for row {idx}: {time_str}")
                continue
            # Localize to Armenia timezone
            sched_time = tz.localize(sched_time)
            if sched_time <= now:
                # Time to send this post
                try:
                    await send_post(record, idx)  # pass the record and actual sheet row index
                except Exception as e:
                    print(f"Error while sending post for row {idx}: {e}")
        # Wait for the next cycle
        await asyncio.sleep(REFRESH_SECONDS)

# Run the main loop
asyncio.run(main())