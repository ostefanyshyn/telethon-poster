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

# Общие Telegram API ключи (одни для всех аккаунтов)
TG_API_ID_STR = os.environ.get("TG_API_ID") or os.environ.get("API_ID")
TG_API_HASH = os.environ.get("TG_API_HASH") or os.environ.get("API_HASH")
if not TG_API_ID_STR or not TG_API_HASH:
    print("ОШИБКА: Укажите TG_API_ID и TG_API_HASH (или API_ID/API_HASH) в окружении.")
    exit(1)
try:
    TG_API_ID = int(TG_API_ID_STR)
except Exception:
    TG_API_ID = 0

# Telegram аккаунты: читаем TG{n}_* циклом из окружения
accounts = []
for n in range(1, 21):
    session = os.environ.get(f"TG{n}_SESSION")
    channel = os.environ.get(f"TG{n}_CHANNEL")

    # Параметры прокси для этого аккаунта (опционально)
    p_type = os.environ.get(f"TG{n}_PROXY_TYPE")      # например: 'socks5' или 'http'
    host = os.environ.get(f"TG{n}_PROXY_HOST")
    port_str = os.environ.get(f"TG{n}_PROXY_PORT")
    rdns_str = os.environ.get(f"TG{n}_PROXY_RDNS", "true")
    user = os.environ.get(f"TG{n}_PROXY_USER")
    password = os.environ.get(f"TG{n}_PROXY_PASS")

    # Пропускаем аккаунт, если не задан ни session, ни channel
    if not session and not channel:
        continue

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
        "api_id": TG_API_ID,
        "api_hash": TG_API_HASH,
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

# Кол-во перс-столбцов-флагов в таблице: "Отправлено 1..N"
SENT_FLAGS_COUNT = int(os.environ.get("SENT_FLAGS_COUNT", 7))

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

# Кэш заголовков -> индексов столбцов
try:
    header_row = worksheet.row_values(1)
    HEADER_TO_COL = {name.strip(): idx for idx, name in enumerate(header_row, start=1)}
except Exception as e:
    print(f"ПРЕДУПРЕЖДЕНИЕ: не удалось прочитать заголовки листа: {e}")
    HEADER_TO_COL = {}

def get_col_index(name: str):
    idx = HEADER_TO_COL.get(name)
    if not idx:
        print(f"ПРЕДУПРЕЖДЕНИЕ: не найден столбец '{name}' в заголовке таблицы.")
    return idx

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

# Удобные словари доступа по индексу аккаунта
ACC_BY_INDEX = {acc["index"]: acc for acc in accounts}
CLIENT_BY_INDEX = {acc["index"]: c for c, acc in zip(clients, accounts)}

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

async def send_post(record, row_idx, pending_indices=None):
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
    if param_lines:
        message_html_lines.append("Параметры:")
        message_html_lines.append(f'<b><i>{"\n".join(param_lines)}</i></b>')
        message_html_lines.append("")
    if services and str(services).strip():
        message_html_lines.append("Услуги:")
        message_html_lines.append(f'<b><i>{services}</i></b>')
        message_html_lines.append("")
    if extra_services and str(extra_services).strip():
        message_html_lines.append("Доп. услуги:")
        message_html_lines.append(f'<b><i>{extra_services}</i></b>')
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

    # Поиск и загрузка медиа (фото и видео)
    media_column_headers = ["Ссылка 1", "Ссылка 2", "Ссылка 3", "Ссылка 4", "Ссылка 5", "Ссылка 6", "Ссылка 7", "Ссылка 8", "Ссылка 9", "Ссылка 10"]
    media_urls = []
    for header in media_column_headers:
        url = record.get(header)
        if url and isinstance(url, str) and url.startswith("http"):
            media_urls.append(url)
            
    print(f"Найдено {len(media_urls)} URL-адресов для строки {row_idx}.")

    media_data = []
    if media_urls:
        for url in media_urls:
            try:
                resp = requests.get(url, timeout=(5, 60))  # Увеличенный таймаут для видео
                resp.raise_for_status()
                file_data = resp.content
                file_base = url.split("/")[-1].split("?")[0]
                content_type = resp.headers.get('Content-Type', '').lower()
                
                # Определение имени файла с правильным расширением
                if file_base and '.' in file_base:
                    file_name = file_base
                else:
                    if 'video' in content_type:
                        file_name = "video.mp4"
                    elif 'image' in content_type:
                        file_name = "image.jpg"
                    else:
                        print(f"ПРЕДУПРЕЖДЕНИЕ: Неподдерживаемый тип {content_type} для {url}. Пропуск.")
                        continue
                
                media_data.append((file_data, file_name))
            except Exception as e:
                print(f"ПРЕДУПРЕЖДЕНИЕ: не удалось загрузить медиа {url} - {e}")
    
    # Отправка сообщений — по одному клиенту, без совместного использования одних и тех же BytesIO
    if pending_indices is None:
        target_indexes = [i for i, acc in ACC_BY_INDEX.items() if i <= SENT_FLAGS_COUNT and acc.get("channel") and str(record.get(f"Отправлено {i}", "")).upper() != "TRUE"]
    else:
        target_indexes = [i for i in pending_indices if i in ACC_BY_INDEX and ACC_BY_INDEX[i].get("channel")]

    clients_with_channels = [(CLIENT_BY_INDEX[i], ACC_BY_INDEX[i]) for i in sorted(target_indexes)]

    # Отправляем в каждый канал КОНКУРЕНТНО; учитываем успехи и провалы по аккаунтам
    targets_count = len(clients_with_channels)

    async def _send_to_one(client, acc):
        channel_str = acc.get("channel")
        acc_idx = acc.get("index")
        if not channel_str:
            return acc_idx, channel_str, False, "no_channel"

        # Гарантируем подключение клиента (переподключаем при необходимости)
        try:
            if not client.is_connected():
                await client.connect()
        except Exception as e:
            print(f"ПРЕДУПРЕЖДЕНИЕ: TG{acc_idx} не удалось подключить клиента перед отправкой: {e}")
            return acc_idx, channel_str, False, f"connect: {e}"

        try:
            channel = int(channel_str)
        except (ValueError, TypeError):
            channel = channel_str

        try:
            if media_data:
                # создаём свежие BytesIO на каждого клиента, чтобы не делить один и тот же буфер
                file_objs = []
                for data, fname in media_data:
                    bio = io.BytesIO(data)
                    bio.name = fname
                    file_objs.append(bio)
                await client.send_file(channel, file_objs, caption=message_html, supports_streaming=True)
            else:
                await client.send_message(channel, message_html)

            print(f"TG{acc_idx}: отправлено в {channel_str}")
            # Отмечаем флаг "Отправлено {n}" только для этого аккаунта
            flag_name = f"Отправлено {acc_idx}"
            col_idx = get_col_index(flag_name)
            if col_idx:
                try:
                    worksheet.update_cell(row_idx, col_idx, "TRUE")
                except Exception as e_upd:
                    print(f"ПРЕДУПРЕЖДЕНИЕ: не удалось обновить ячейку флага {flag_name} (строка {row_idx}): {e_upd}")

            return acc_idx, channel_str, True, None

        except (tl_errors.FloodWaitError, tl_errors.SlowModeWaitError) as e:
            print(f"ОШИБКА: TG{acc_idx} ограничение Telegram при отправке: {e}. Пропуск для этого клиента.")
            return acc_idx, channel_str, False, f"rate: {e}"

        except Exception as e:
            # Попытка одноразового переподключения и повторной отправки
            print(f"ПРЕДУПРЕЖДЕНИЕ: TG{acc_idx} ошибка при отправке: {e}. Пробуем переподключиться и повторить...")
            try:
                await client.disconnect()
            except Exception:
                pass
            try:
                await client.connect()
                if media_data:
                    file_objs = []
                    for data, fname in media_data:
                        bio = io.BytesIO(data)
                        bio.name = fname
                        file_objs.append(bio)
                    await client.send_file(channel, file_objs, caption=message_html, supports_streaming=True)
                else:
                    await client.send_message(channel, message_html)

                print(f"TG{acc_idx}: повторная отправка успешна в {channel_str}")
                flag_name = f"Отправлено {acc_idx}"
                col_idx = get_col_index(flag_name)
                if col_idx:
                    try:
                        worksheet.update_cell(row_idx, col_idx, "TRUE")
                    except Exception as e_upd:
                        print(f"ПРЕДУПРЕЖДЕНИЕ: не удалось обновить ячейку флага {flag_name} (строка {row_idx}): {e_upd}")

                return acc_idx, channel_str, True, None

            except Exception as e2:
                print(f"ОШИБКА: TG{acc_idx} повторная отправка не удалась: {e2}")
                return acc_idx, channel_str, False, f"retry: {e2}"

    # Запускаем все отправки одновременно
    results = await asyncio.gather(
        *[ _send_to_one(client, acc) for (client, acc) in clients_with_channels ],
        return_exceptions=False
    )

    ok = sum(1 for (_, _, success, _) in results if success)
    fail = [ (acc_idx, ch, err) for (acc_idx, ch, success, err) in results if not success ]
    print(f"Строка {row_idx}: перс-отправки завершены. Успешно {ok}/{targets_count}. Неудачи: {fail}")

# --- 5. ГЛАВНЫЙ ЦИКЛ ПРОГРАММЫ ---

async def main():
    """Главная функция: подключается к клиентам и запускает бесконечный цикл проверки."""
    if not clients:
        print("ОШИБКА: Не настроен ни один Telegram клиент. Проверьте TG_API_ID/TG_API_HASH и TG{n}_SESSION/TG{n}_CHANNEL.")
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
                if not str(record.get("Имя", "")).strip():
                    continue

                # Определяем какие аккаунты ещё не отправили (по флагам "Отправлено n")
                active_idx = [acc["index"] for acc in accounts if acc.get("channel") and acc["index"] <= SENT_FLAGS_COUNT]
                if not active_idx:
                    # Нет целевых аккаунтов с каналами
                    continue

                pending_idx = [i for i in active_idx if str(record.get(f"Отправлено {i}", "")).upper() != "TRUE"]
                if not pending_idx:
                    # Всё уже отправлено по всем требуемым аккаунтам
                    continue

                time_str = record.get("Время")
                if not time_str:
                    continue

                try:
                    sched_time = datetime.strptime(time_str, "%d.%m.%Y %H:%M:%S")
                    sched_time = tz.localize(sched_time)

                    if sched_time <= now:
                        print(f"Найдена запись для отправки в строке {idx}.")
                        await send_post(record, idx, pending_indices=pending_idx)

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