import os
import asyncio
import base64
import json
from datetime import datetime, timedelta
import pytz
import requests
import io
import gspread
from telethon import TelegramClient, types
from telethon.sessions import StringSession
from telethon.extensions import html as tl_html
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
REFRESH_SECONDS = int(os.environ.get("REFRESH_SECONDS", 60))
# Parse Google service account credentials from the base64 JSON string
credentials_json = json.loads(base64.b64decode(GOOGLE_CREDS_JSON)) if GOOGLE_CREDS_JSON else None
# Authorize gspread and open the sheet
gc = gspread.service_account_from_dict(credentials_json)
sheet = gc.open_by_key(GSHEET_ID)
worksheet = sheet.sheet1  # assuming data is in the first sheet
# Timezone for parsing schedule times (Armenia)
tz = pytz.timezone("Asia/Yerevan")
# Avoid duplicate sends within a single runtime
processed_rows = set()
# ---------------------------------------------------------------------------
# SOCKS5 proxies for each account (username : password : host : port)
proxy1 = ('socks5', 'as.proxy.piaproxy.com', 5000, True,
'user-subaccount_O9xrM-region-bd-sessid-bddtfx89d4fs5n443-sesstime-90',
'Qefmegpajkitdotxo7')
proxy2 = ('socks5', 'as.proxy.piaproxy.com', 5000, True,
'user-subaccount_O9xrM-region-bd-sessid-bddtfx89d4fs5n444-sesstime-90',
'Qefmegpajkitdotxo7')
proxy3 = ('socks5', 'as.proxy.piaproxy.com', 5000, True,
'user-subaccount_O9xrM-region-bd-sessid-bddtfx89d4fs5n445-sesstime-90',
'Qefmegpajkitdotxo7')
# Telegram clients setup for three accounts
client1 = TelegramClient(StringSession(TG1_SESSION) if TG1_SESSION else 'tg1_session',
TG1_API_ID, TG1_API_HASH, proxy=proxy1)
client2 = TelegramClient(StringSession(TG2_SESSION) if TG2_SESSION else 'tg2_session',
TG2_API_ID, TG2_API_HASH, proxy=proxy2)
client3 = TelegramClient(StringSession(TG3_SESSION) if TG3_SESSION else 'tg3_session',
TG3_API_ID, TG3_API_HASH, proxy=proxy3)
# ---------------------------------------------------------------------------
# Custom HTML parser to convert <a href="emoji/{id}">X</a> into MessageEntityCustomEmoji
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
    1: 5429293125518510398, 2: 5814534640949530526, 3: 5370853949358218655,
    4: 5370674552869232639, 5: 5372943137415111238, 6: 5373338978780979795,
    7: 5372991528811635071, 8: 5364098734600762220, 9: 5460811944883660881
}
# Placeholder symbols for each emoji
emoji_placeholders = {
    1: "☁️", 2: "👑", 3: "✅", 4: "✅", 5: "✅", 6: "✅", 7: "✅", 8: "⚡️", 9: "😜",
}
async def send_post(record, row_idx):
    """Send a post to all three channels based on the data in record (a dict)."""
    status = record.get("Статус") or record.get("Cтатус") or ""
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
    skip_text = record.get("Пробелы перед короной", "")
    # Build parameter lines dynamically
    param_lines = []
    if age and str(age).strip(): param_lines.append(f"Возраст - {age}")
    if height and str(height).strip(): param_lines.append(f"Рост - {height}")
    if weight and str(weight).strip(): param_lines.append(f"Вес - {weight}")
    if bust and str(bust).strip(): param_lines.append(f"Грудь - {bust}")
    # Build the message HTML string
    message_html_lines = []
    if skip_text and skip_text.strip() != "": message_html_lines.append(skip_text)
    message_html_lines.append(f'<a href="emoji/{emoji_ids[1]}">{emoji_placeholders[1]}</a> <i>{status}</i> <a href="emoji/{emoji_ids[1]}">{emoji_placeholders[1]}</a>')
    message_html_lines.append("")
    prefix = f"{skip_text}" if skip_text else ""
    message_html_lines.append(f'{prefix}<a href="emoji/{emoji_ids[2]}">{emoji_placeholders[2]}</a>')
    message_html_lines.append(f'<b><i>{name}</i></b>')
    foto_checks = "".join(f'<a href="emoji/{emoji_ids[i]}">{emoji_placeholders[i]}</a>' for i in range(3, 8))
    message_html_lines.append("")
    message_html_lines.append(f'<b>Фото {foto_checks}</b>')
    message_html_lines.append("")
    if services and str(services).strip():
        message_html_lines.extend(["Услуги:", f'<b><i>{services}</i></b>', ""])
    if extra_services and str(extra_services).strip():
        message_html_lines.extend(["Доп. услуги:", f'<b><i>{extra_services}</i></b>', ""])
    if param_lines:
        message_html_lines.extend(["Параметры:", f'<b><i>{"\n".join(param_lines)}</i></b>', ""])
    def _fmt_price(val):
        try: return f"{float(str(val).replace(',', '.')):.3f} AMD"
        except Exception: return f"{val} AMD"
    price_lines = []
    if express_price and str(express_price).strip(): price_lines.append(f"Express - {_fmt_price(express_price)}")
    if incall_price and str(incall_price).strip(): price_lines.append(f"Incall - {_fmt_price(incall_price)}")
    if outcall_price and str(outcall_price).strip(): price_lines.append(f"Outcall - {_fmt_price(outcall_price)}")
    if price_lines:
        message_html_lines.extend(["Цена:", f'<b><i>{"\n".join(price_lines)}</i></b>', ""])
    message_html_lines.append(f'<a href="emoji/{emoji_ids[8]}">{emoji_placeholders[8]}</a><b><i>Назначь встречу уже сегодня!</i></b><a href="emoji/{emoji_ids[8]}">{emoji_placeholders[8]}</a>')
    message_html_lines.append(f'<a href="{whatsapp_link}"><b>Связь в WhatsApp</b></a> <a href="emoji/{emoji_ids[9]}">{emoji_placeholders[9]}</a>')
    message_html = "\n".join(message_html_lines)
    # Gather media URLs
    file_urls = []
    for n in range(1, 5):
        url = record.get(f"Ссылка {n}") or record.get(f"ссылка {n}")
        if isinstance(url, str) and url.strip().startswith("http"):
            file_urls.append(url.strip())
    # Download media and prepare BytesIO objects
    media_files = []
    if file_urls:
        for url in file_urls:
            try:
                resp = requests.get(url)
                resp.raise_for_status()
                ct = (resp.headers.get("Content-Type") or "application/octet-stream").lower().split(";", 1)[0]
                name = url.split("/")[-1].split("?")[0] or "file"
                if "." not in name:
                    if "video/" in ct: name = f"{name}.{ct.split('/', 1)[1]}"
                    elif "image/" in ct: name = f"{name}.{ct.split('/', 1)[1]}"
                bio = io.BytesIO(resp.content)
                bio.name = name
                media_files.append(bio)
            except Exception as e:
                print(f"Warning: failed to download media {url} - {e}")
    async def send_to_channel(client, channel):
        """Sends the message or media group to a single channel."""
        if not media_files:
            await client.send_message(channel, message_html)
            return
        await client.send_file(channel, media_files, caption=message_html)
    # Send message via all three clients concurrently
    tasks = []
    channels = [TG1_CHANNEL, TG2_CHANNEL, TG3_CHANNEL]
    channels_int = [int(ch) if ch and (ch.isdigit() or ch.startswith("-")) else ch for ch in channels]
    for client, channel in zip([client1, client2, client3], channels_int):
        if channel:
            tasks.append(send_to_channel(client, channel))
    if tasks:
        await asyncio.gather(*tasks)
    # Update the Google Sheet to mark as sent
    worksheet.update_cell(row_idx, 1, "TRUE")
    print(f"Posted and marked row {row_idx} as sent.")
async def main():
    """Main loop to connect clients and check the schedule."""
    await client1.start()
    await client2.start()
    await client3.start()
    print("Telegram clients connected. Starting schedule loop...")
    while True:
        try:
            records = worksheet.get_all_records()
            now = datetime.now(tz)
            for idx, record in enumerate(records, start=2):
                sent_flag = record.get("Отправлено")
                def _is_sent_value(v):
                    if isinstance(v, bool): return v
                    return str(v).strip().lower() in ("true", "1", "yes", "да", "y", "sent", "ок", "ok", "✔", "✅")
                if _is_sent_value(sent_flag) or idx in processed_rows or not str(record.get("Имя", "")).strip():
                    continue
                time_str = record.get("Время")
                if not time_str: continue
                try:
                    sched_time = tz.localize(datetime.strptime(time_str, "%d.%m.%Y %H:%M:%S"))
                except Exception:
                    print(f"Failed to parse time for row {idx}: {time_str}")
                    continue
                if sched_time <= now:
                    delta_sec = (now - sched_time).total_seconds()
                    if delta_sec <= 300:  # 5 minutes
                        processed_rows.add(idx)
                        await send_post(record, idx)
                    else:
                        processed_rows.add(idx)
        except Exception as e:
            print(f"An error occurred in the main loop: {e}")
            print("Restarting loop after a short delay...")
        await asyncio.sleep(REFRESH_SECONDS)
# Run the main loop
if __name__ == "__main__":
    asyncio.run(main())