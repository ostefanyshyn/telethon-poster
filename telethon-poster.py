import os
import asyncio
import base64
import json
from datetime import datetime
import pytz
import requests
import io
import gspread
import sys
from telethon.network import connection as tl_connection
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.extensions import html as tl_html
from telethon import types
import telethon.errors as tl_errors
from dotenv import load_dotenv

# Загрузка переменных из .env файла
load_dotenv()

# Предупреждение о версии Python (Telethon может работать нестабильно на Python 3.13)
if sys.version_info >= (3, 13):
    print("ПРЕДУПРЕЖДЕНИЕ: Обнаружен Python 3.13+. Известны проблемы совместимости asyncio с Telethon. "
          "Рекомендуется использовать Python 3.11–3.12 или обновить Telethon до последней версии.")

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

    # Параметры прокси для этого аккаунта (опционально)
    p_type = os.environ.get(f"TG{n}_PROXY_TYPE")      # например: 'socks5' или 'http'
    host = os.environ.get(f"TG{n}_PROXY_HOST")
    port_str = os.environ.get(f"TG{n}_PROXY_PORT")
    rdns_str = os.environ.get(f"TG{n}_PROXY_RDNS", "true")
    user = os.environ.get(f"TG{n}_PROXY_USER")
    password = os.environ.get(f"TG{n}_PROXY_PASS")

    if not api_id_str or not api_hash:
        continue
    try:
        api_id = int(api_id_str)
    except Exception:
        api_id = 0

    # Сборка кортежа прокси, если задан
    proxy = None
    if p_type and host and port_str:
        try:
            port = int(port_str)
        except Exception:
            port = None
        if port:
            rdns = str(rdns_str).lower() in ("1", "true", "yes", "y", "on")
            proxy = (p_type, host, port, rdns, user, password)

    accounts.append({
        "index": n,
        "api_id": api_id,
        "api_hash": api_hash,
        "session": session,
        "channel": channel,
        "proxy": proxy,
    })

# Требуем наличие прокси для каждого аккаунта. Без прокси работать запрещено.
missing_proxy = [acc["index"] for acc in accounts if not acc.get("proxy")]
if missing_proxy:
    acc_list = ", ".join(f"TG{n}" for n in missing_proxy)
    print(
        f"ОШИБКА: Для {acc_list} не задан прокси. "
        f"Укажите TG{{n}}_PROXY_TYPE, TG{{n}}_PROXY_HOST, TG{{n}}_PROXY_PORT "
        f"(при необходимости TG{{n}}_PROXY_USER, TG{{n}}_PROXY_PASS, TG{{n}}_PROXY_RDNS)."
    )
    exit(1)

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

# Настройка клиентов Telegram (динамически)
clients = []
for i, acc in enumerate(accounts):
    prx = acc.get("proxy")
    session_or_name = StringSession(acc["session"]) if acc["session"] else f"tg{i+1}_session"
    clients.append(
        TelegramClient(
            session_or_name,
            acc["api_id"],
            acc["api_hash"],
            proxy=prx,
            connection=tl_connection.ConnectionTcpAbridged,  # избегаем tcpfull
            request_retries=3,
            connection_retries=1,
            retry_delay=2,
            timeout=30,
        )
    )

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
    9: 5460811944883660881,
    10: 5460675841665019102,
}

# Unicode-заменители для отображения в коде
emoji_placeholders = {
    1: "☁️", 2: "👑", 3: "✅", 4: "✅", 5: "✅", 6: "✅", 7: "✅", 8: "⚡️", 9: "😜",
    10: "✈️"
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
    telegram_link = record.get("Telegram", "")
    skip_text = record.get("Пробелы перед короной", "")

    # Сборка HTML-сообщения
    param_lines = []
    if age and str(age).strip(): param_lines.append(f"Возраст - {age}")
    if height and str(height).strip(): param_lines.append(f"Рост - {height}")
    if weight and str(weight).strip(): param_lines.append(f"Вес - {weight}")
    if bust and str(bust).strip(): param_lines.append(f"Грудь - {bust}")
    
    message_html_lines = []
    if skip_text and skip_text.strip(): message_html_lines.append(skip_text)
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
    def _fmt_price(val):
        try:
            # Normalize input like '40,5' to '40.5' and remove spaces
            num = float(str(val).replace(' ', '').replace(',', '.'))
            # Treat input as thousands of AMD (e.g., 40.0 -> 40000)
            amount = int(round(num * 1000))
            # Format with dot as thousands separator
            return f"{format(amount, ',d').replace(',', '.')} AMD"
        except Exception:
            # Fallback to raw value if parsing fails
            return f"{val} AMD"
    price_lines = []
    if express_price and str(express_price).strip(): price_lines.append(f"Express - {_fmt_price(express_price)}")
    if incall_price and str(incall_price).strip(): price_lines.append(f"Incall - {_fmt_price(incall_price)}")
    if outcall_price and str(outcall_price).strip(): price_lines.append(f"Outcall - {_fmt_price(outcall_price)}")
    if price_lines:
        message_html_lines.append("Цена:")
        message_html_lines.append(f'<b><i>{"\n".join(price_lines)}</i></b>')
        message_html_lines.append("")
    message_html_lines.append(f'<a href="emoji/{emoji_ids[8]}">{emoji_placeholders[8]}</a><b><i>Назначь встречу уже сегодня!</i></b><a href="emoji/{emoji_ids[8]}">{emoji_placeholders[8]}</a>')
    if telegram_link and str(telegram_link).strip():
        message_html_lines.append(f'<a href="emoji/{emoji_ids[10]}">{emoji_placeholders[10]}</a> <a href="{telegram_link}"><b>Связь в Telegram</b></a>')
    message_html_lines.append(f'<a href="emoji/{emoji_ids[9]}">{emoji_placeholders[9]}</a> <a href="{whatsapp_link}"><b>Связь в WhatsApp</b></a>')
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
                resp = requests.get(url, timeout=20)
                resp.raise_for_status()
                file_data = resp.content
                file_name = url.split("/")[-1].split("?")[0] or "image.jpg"
                photo_data.append((file_data, file_name))
            except Exception as e:
                print(f"ПРЕДУПРЕЖДЕНИЕ: не удалось загрузить изображение {url} - {e}")
    
    # Отправка сообщений — по одному клиенту, без совместного использования одних и тех же BytesIO
    clients_with_channels = [(c, acc.get("channel")) for c, acc in zip(clients, accounts)]

    sent_any = False
    for client, channel_str in clients_with_channels:
        if not channel_str:
            continue

        # Гарантируем подключение клиента (переподключаем при необходимости)
        try:
            if not client.is_connected():
                await client.connect()
        except Exception as e:
            print(f"ПРЕДУПРЕЖДЕНИЕ: не удалось подключить клиента перед отправкой: {e}")
            continue

        try:
            channel = int(channel_str)
        except (ValueError, TypeError):
            channel = channel_str

        try:
            if photo_data:
                # создаём свежие BytesIO на каждого клиента, чтобы не делить один и тот же буфер
                file_objs = []
                for data, fname in photo_data:
                    bio = io.BytesIO(data)
                    bio.name = fname
                    file_objs.append(bio)
                await client.send_file(channel, file_objs, caption=message_html)
            else:
                await client.send_message(channel, message_html)
            sent_any = True
            # Небольшая пауза, чтобы снизить одновременные запросы ко всем прокси
            await asyncio.sleep(0.5)
        except (tl_errors.FloodWaitError, tl_errors.SlowModeWaitError) as e:
            print(f"ОШИБКА: ограничение Telegram при отправке: {e}. Пропуск для этого клиента.")
        except Exception as e:
            # Попытка одноразового переподключения и повторной отправки
            print(f"ПРЕДУПРЕЖДЕНИЕ: ошибка при отправке через клиента: {e}. Пробуем переподключиться и повторить...")
            try:
                await client.disconnect()
            except Exception:
                pass
            try:
                await client.connect()
                if photo_data:
                    file_objs = []
                    for data, fname in photo_data:
                        bio = io.BytesIO(data)
                        bio.name = fname
                        file_objs.append(bio)
                    await client.send_file(channel, file_objs, caption=message_html)
                else:
                    await client.send_message(channel, message_html)
                sent_any = True
            except Exception as e2:
                print(f"ОШИБКА: повторная отправка не удалась: {e2}")

    if sent_any:
        worksheet.update_cell(row_idx, 1, "TRUE")
        print(f"Сообщение для строки {row_idx} отправлено и отмечено как отправленное.")
    else:
        print(f"Для строки {row_idx} не удалось отправить сообщение ни через один настроенный клиент.")

# --- 5. ГЛАВНЫЙ ЦИКЛ ПРОГРАММЫ ---

async def main():
    """Главная функция: подключается к клиентам и запускает бесконечный цикл проверки."""
    if not clients:
        print("ОШИБКА: Не настроен ни один Telegram клиент. Проверьте переменные TG{n}_API_ID и TG{n}_API_HASH.")
        return
        
    print("Подключение Telegram клиентов...")
    results = await asyncio.gather(*(c.start() for c in clients), return_exceptions=True)
    for idx, res in enumerate(results, start=1):
        if isinstance(res, Exception):
            print(f"ПРЕДУПРЕЖДЕНИЕ: клиент #{idx} не запустился: {res}")
    print("Клиенты успешно подключены. Запуск основного цикла...")

    while True:
        try:
            alive = sum(1 for c in clients if c.is_connected())
            print(f"Активных клиентов: {alive}/{len(clients)}")
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