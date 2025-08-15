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
from telethon.extensions import html as tl_html
from telethon import types
from dotenv import load_dotenv

# Загрузка переменных из .env файла
load_dotenv()

# --- 1. КОНФИГУРАЦИЯ ---

# Google Sheets
GSHEET_ID = os.environ.get("GSHEET_ID")
GOOGLE_CREDS_JSON = os.environ.get("GOOGLE_CREDS_JSON")

# Telegram аккаунты: читаем TG{n}_* циклом из окружения
accounts = []
for n in range(1, 21):
    api_id_str = os.environ.get(f"TG{n}_API_ID")
    api_hash = os.environ.get(f"TG{n}_API_HASH")
    session = os.environ.get(f"TG{n}_SESSION")
    channel = os.environ.get(f"TG{n}_CHANNEL")
    if not api_id_str or not api_hash:
        continue
    try:
        api_id = int(api_id_str)
    except Exception:
        api_id = 0
    accounts.append({"api_id": api_id, "api_hash": api_hash, "session": session, "channel": channel})

# Интервал обновления (в секундах)
REFRESH_SECONDS = int(os.environ.get("REFRESH_SECONDS", 30))

# --- 2. НАСТРОЙКА КЛИЕНТОВ ---

# Авторизация в Google Sheets
try:
    credentials_json = json.loads(base64.b64decode(GOOGLE_CREDS_JSON))
    gc = gspread.service_account_from_dict(credentials_json)
    sheet = gc.open_by_key(GSHEET_ID)
    worksheet = sheet.sheet1
except Exception as e:
    print(f"ОШИБКА: Не удалось подключиться к Google Sheets. Проверьте GOOGLE_CREDS_JSON и GSHEET_ID. {e}")
    exit()

# Часовой пояс для расписания (Армения)
tz = pytz.timezone("Asia/Yerevan")

# Прокси для каждого аккаунта (если нужны)
# Замените на ваши данные или оставьте None, если прокси не используются
proxy1 = ('socks5', 'as.proxy.piaproxy.com', 5000, True,
          'user-subaccount_O9xrM-region-bd-sessid-bddtfx89d4fs5n443-sesstime-90',
          'Qefmegpajkitdotxo7')
proxy2 = ('socks5', 'as.proxy.piaproxy.com', 5000, True,
          'user-subaccount_O9xrM-region-bd-sessid-bddtfx89d4fs5n444-sesstime-90',
          'Qefmegpajkitdotxo7')
proxy3 = ('socks5', 'as.proxy.piaproxy.com', 5000, True,
          'user-subaccount_O9xrM-region-bd-sessid-bddtfx89d4fs5n445-sesstime-90',
          'Qefmegpajkitdotxo7')

# Настройка клиентов Telegram (динамически)
clients = []
for i, acc in enumerate(accounts):
    prx = proxy1 if i == 0 else proxy2 if i == 1 else proxy3 if i == 2 else None
    session_or_name = StringSession(acc["session"]) if acc["session"] else f"tg{i+1}_session"
    clients.append(TelegramClient(session_or_name, acc["api_id"], acc["api_hash"], proxy=prx))

# --- 3. ПОЛЬЗОВАТЕЛЬСКИЕ EMOJI И ПАРСЕР ---

# Класс для обработки кастомных эмодзи в HTML
class CustomHtml:
    @staticmethod
    def parse(text):
        text, entities = tl_html.parse(text)
        for i, e in enumerate(entities):
            if isinstance(e, types.MessageEntityTextUrl) and e.url.startswith("emoji/"):
                emoji_id = int(e.url.split("/", 1)[1])
                entities[i] = types.MessageEntityCustomEmoji(e.offset, e.length, document_id=emoji_id)
        return text, entities

    @staticmethod
    def unparse(text, entities):
        for i, e in enumerate(entities or []):
            if isinstance(e, types.MessageEntityCustomEmoji):
                entities[i] = types.MessageEntityTextUrl(e.offset, e.length, url=f"emoji/{e.document_id}")
        return tl_html.unparse(text, entities)

# Установка парсера для всех клиентов
for _c in clients:
    _c.parse_mode = CustomHtml()

# ID кастомных эмодзи
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

# Unicode-заменители для отображения в коде
emoji_placeholders = {
    1: "☁️", 2: "👑", 3: "✅", 4: "✅", 5: "✅", 6: "✅", 7: "✅", 8: "⚡️", 9: "😜"
}

# --- 4. ФУНКЦИЯ ОТПРАВКИ ПОСТА ---

async def send_post(record, row_idx):
    """Собирает, форматирует и отправляет пост на основе строки из таблицы."""
    # Парсинг данных из записи
    status = record.get("Статус", "")
    name = record.get("Имя", "")
    services = record.get("Услуги", "")
    extra_services = record.get("Доп. услуги", "")
    age = record.get("Возраст", "")
    height = record.get("Рост", "")
    weight = record.get("Вес", "")
    bust = record.get("Грудь", "")
    express_price = record.get("Express", "")
    incall_price = record.get("Incall", "")
    outcall_price = record.get("Outcall", "")
    whatsapp_link = record.get("WhatsApp", "")
    skip_text = record.get("Пробелы перед короной", "")

    # Сборка HTML-сообщения
    param_lines = []
    if age and str(age).strip(): param_lines.append(f"Возраст - {age}")
    if height and str(height).strip(): param_lines.append(f"Рост - {height}")
    if weight and str(weight).strip(): param_lines.append(f"Вес - {weight}")
    if bust and str(bust).strip(): param_lines.append(f"Грудь - {bust}")
    
    message_html_lines = []
    if skip_text and skip_text.strip(): message_html_lines.append(skip_text)
    message_html_lines.append(f'<a href="emoji/{emoji_ids[1]}">{emoji_placeholders[1]}</a><i>{status}</i><a href="emoji/{emoji_ids[1]}">{emoji_placeholders[1]}</a>')
    message_html_lines.append("")
    prefix = f"{skip_text}" if skip_text else ""
    message_html_lines.append(f'{prefix}<a href="emoji/{emoji_ids[2]}">{emoji_placeholders[2]}</a>')
    message_html_lines.append(f'<b><i>{name}</i></b>')
    foto_checks = "".join(f'<a href="emoji/{emoji_ids[i]}">{emoji_placeholders[i]}</a>' for i in range(3, 8))
    message_html_lines.append("")
    message_html_lines.append(f'<b>Фото {foto_checks}</b>')
    message_html_lines.append("")
    if services and str(services).strip():
        message_html_lines.append("Услуги:")
        message_html_lines.append(f'<b><i>{services}</i></b>')
        message_html_lines.append("")
    if extra_services and str(extra_services).strip():
        message_html_lines.append("Доп. услуги:")
        message_html_lines.append(f'<b><i>{extra_services}</i></b>')
        message_html_lines.append("")
    if param_lines:
        message_html_lines.append("Параметры:")
        message_html_lines.append(f'<b><i>{"\n".join(param_lines)}</i></b>')
        message_html_lines.append("")
    def _fmt_price(val): return f"{val} AMD"
    price_lines = []
    if express_price and str(express_price).strip(): price_lines.append(f"Express - {_fmt_price(express_price)}")
    if incall_price and str(incall_price).strip(): price_lines.append(f"Incall - {_fmt_price(incall_price)}")
    if outcall_price and str(outcall_price).strip(): price_lines.append(f"Outcall - {_fmt_price(outcall_price)}")
    if price_lines:
        message_html_lines.append("Цена:")
        message_html_lines.append(f'<b><i>{"\n".join(price_lines)}</i></b>')
        message_html_lines.append("")
    message_html_lines.append(f'<a href="emoji/{emoji_ids[8]}">{emoji_placeholders[8]}</a><b><i>Назначь встречу уже сегодня!</i></b><a href="emoji/{emoji_ids[8]}">{emoji_placeholders[8]}</a>')
    message_html_lines.append(f'<a href="{whatsapp_link}"><b>Связь в WhatsApp</b></a> <a href="emoji/{emoji_ids[9]}">{emoji_placeholders[9]}</a>')
    message_html = "\n".join(message_html_lines)

    # Поиск и загрузка фотографий
    photo_column_headers = ["Ссылка 1", "Ссылка 2", "Ссылка 3", "Ссылка 4", "Ссылка 5", "Ссылка 6", "Ссылка 7", "Ссылка 8", "Ссылка 9", "Ссылка 10"]
    photo_urls = []
    for header in photo_column_headers:
        url = record.get(header)
        if url and isinstance(url, str) and url.startswith("http"):
            photo_urls.append(url)
            
    print(f"Найдено {len(photo_urls)} URL-адресов для строки {row_idx}.")

    photo_data = []
    if photo_urls:
        for url in photo_urls:
            try:
                resp = requests.get(url)
                resp.raise_for_status()
                file_data = resp.content
                file_name = url.split("/")[-1].split("?")[0] or "image.jpg"
                photo_data.append((file_data, file_name))
            except Exception as e:
                print(f"ПРЕДУПРЕЖДЕНИЕ: не удалось загрузить изображение {url} - {e}")
    
    # Отправка сообщений
    tasks = []
    clients_with_channels = [(c, acc.get("channel")) for c, acc in zip(clients, accounts)]

    for client, channel_str in clients_with_channels:
        if not (client.is_connected() and channel_str):
            continue
        
        try:
            channel = int(channel_str)
        except (ValueError, TypeError):
            channel = channel_str

        if photo_data:
            file_objs = [io.BytesIO(data) for data, fname in photo_data]
            for bio, (_, fname) in zip(file_objs, photo_data):
                bio.name = fname
            tasks.append(client.send_file(channel, file_objs, caption=message_html))
        else:
            tasks.append(client.send_message(channel, message_html))

    if tasks:
        await asyncio.gather(*tasks)
        worksheet.update_cell(row_idx, 1, "TRUE")
        print(f"Сообщение для строки {row_idx} отправлено и отмечено как отправленное.")
    else:
        print(f"Для строки {row_idx} не было найдено активных каналов для отправки.")

# --- 5. ГЛАВНЫЙ ЦИКЛ ПРОГРАММЫ ---

async def main():
    """Главная функция: подключается к клиентам и запускает бесконечный цикл проверки."""
    if not clients:
        print("ОШИБКА: Не настроен ни один Telegram клиент. Проверьте переменные TG{n}_API_ID и TG{n}_API_HASH.")
        return
        
    print("Подключение Telegram клиентов...")
    await asyncio.gather(*(c.start() for c in clients))
    print("Клиенты успешно подключены. Запуск основного цикла...")

    while True:
        try:
            print(f"Проверка таблицы... {datetime.now(tz).strftime('%H:%M:%S')}")
            records = worksheet.get_all_records()
            now = datetime.now(tz)

            for idx, record in enumerate(records, start=2):
                sent_flag = record.get("Отправлено")
                if str(sent_flag).upper() == "TRUE":
                    continue

                if not str(record.get("Имя", "")).strip():
                    continue

                time_str = record.get("Время")
                if not time_str:
                    continue

                try:
                    sched_time = datetime.strptime(time_str, "%d.%m.%Y %H:%M:%S")
                    sched_time = tz.localize(sched_time)

                    if sched_time <= now:
                        print(f"Найдена запись для отправки в строке {idx}.")
                        await send_post(record, idx)

                except ValueError:
                    print(f"ПРЕДУПРЕЖДЕНИЕ: Неверный формат времени в строке {idx}: '{time_str}'. Ожидается 'ДД.ММ.ГГГГ ЧЧ:ММ:СС'.")
                except Exception as e:
                    print(f"ОШИБКА при обработке строки {idx}: {e}")
            
            await asyncio.sleep(REFRESH_SECONDS)

        except gspread.exceptions.APIError as e:
            print(f"ОШИБКА API Google Sheets: {e}. Повторная попытка через {REFRESH_SECONDS} сек.")
            await asyncio.sleep(REFRESH_SECONDS)
        except Exception as e:
            print(f"КРИТИЧЕСКАЯ ОШИБКА в главном цикле: {e}")
            await asyncio.sleep(REFRESH_SECONDS)

# --- 6. ЗАПУСК СКРИПТА ---

if __name__ == "__main__":
    asyncio.run(main())