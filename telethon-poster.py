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
import time
import random
from urllib.parse import urlparse, unquote

# Загрузка переменных из .env файла
load_dotenv()

# --- 1. КОНФИГУРАЦИЯ ---

# Google Sheets
GSHEET_ID = os.environ.get("GSHEET_ID")
GOOGLE_CREDS_JSON = os.environ.get("GOOGLE_CREDS_JSON")

# Telegram Аккаунт 1
TG1_API_ID = int(os.environ.get("TG1_API_ID", 0))
TG1_API_HASH = os.environ.get("TG1_API_HASH")
TG1_SESSION = os.environ.get("TG1_SESSION")
TG1_CHANNEL = os.environ.get("TG1_CHANNEL")

# Telegram Аккаунт 2
TG2_API_ID = int(os.environ.get("TG2_API_ID", 0))
TG2_API_HASH = os.environ.get("TG2_API_HASH")
TG2_SESSION = os.environ.get("TG2_SESSION")
TG2_CHANNEL = os.environ.get("TG2_CHANNEL")

# Telegram Аккаунт 3
TG3_API_ID = int(os.environ.get("TG3_API_ID", 0))
TG3_API_HASH = os.environ.get("TG3_API_HASH")
TG3_SESSION = os.environ.get("TG3_SESSION")
TG3_CHANNEL = os.environ.get("TG3_CHANNEL")

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

# --- СЕТЕВЫЕ НАСТРОЙКИ ДЛЯ ЗАГРУЗКИ МЕДИА ---
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"
}
DOWNLOAD_TIMEOUT = 20  # сек
MAX_RETRIES = 3
CHUNK_SIZE = 8192

# Прокси для каждого аккаунта (если нужны)
# Укажите свои данные или оставьте None, если прокси не используются
proxy1 = ('socks5', 'as.proxy.piaproxy.com', 5000, True,
          'user-subaccount_O9xrM-region-bd-sessid-bddtfx89d4fs5n443-sesstime-90',
          'Qefmegpajkitdotxo7')
proxy2 = ('socks5', 'as.proxy.piaproxy.com', 5000, True,
          'user-subaccount_O9xrM-region-bd-sessid-bddtfx89d4fs5n444-sesstime-90',
          'Qefmegpajkitdotxo7')
proxy3 = ('socks5', 'as.proxy.piaproxy.com', 5000, True,
          'user-subaccount_O9xrM-region-bd-sessid-bddtfx89d4fs5n445-sesstime-90',
          'Qefmegpajkitdotxo7')

# Настройка клиентов Telegram
client1 = TelegramClient(StringSession(TG1_SESSION) if TG1_SESSION else 'tg1_session', TG1_API_ID, TG1_API_HASH, proxy=proxy1)
client2 = TelegramClient(StringSession(TG2_SESSION) if TG2_SESSION else 'tg2_session', TG2_API_ID, TG2_API_HASH, proxy=proxy2)
client3 = TelegramClient(StringSession(TG3_SESSION) if TG3_SESSION else 'tg3_session', TG3_API_ID, TG3_API_HASH, proxy=proxy3)

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
for _c in (client1, client2, client3):
    _c.parse_mode = CustomHtml()

# ID кастомных эмодзи (может использоваться для вставки специальных эмодзи в текст)
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

# Unicode-заменители для отображения в коде (если нужно показать эмодзи как символы)
emoji_placeholders = {
    1: "☁️", 2: "👑", 3: "✅", 4: "✅", 5: "✅", 6: "✅", 7: "✅", 8: "⚡️", 9: "😜"
}

def _choose_filename(url, resp, fallback_name):
    """Определяет имя файла по заголовку ответа или URL."""
    cd = resp.headers.get("Content-Disposition") or resp.headers.get("content-disposition")
    if cd and "filename=" in cd:
        fname = cd.split("filename=", 1)[1].strip().strip('";\'')
        if fname:
            return fname
    path = urlparse(url).path
    if path:
        last = os.path.basename(path)
        if last:
            return unquote(last.split("?")[0])
    return fallback_name

def download_file_with_retries(url, kind):
    """Скачивает файл с повторами. kind: 'photo' или 'video'. Возвращает (bytes, filename)."""
    default_name = "image.jpg" if kind == "photo" else "video.mp4"
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(
                url,
                headers=DEFAULT_HEADERS,
                stream=True,
                timeout=DOWNLOAD_TIMEOUT,
                allow_redirects=True,
            )
            resp.raise_for_status()
            fname = _choose_filename(url, resp, default_name)
            bio = io.BytesIO()
            for chunk in resp.iter_content(CHUNK_SIZE):
                if chunk:
                    bio.write(chunk)
            return bio.getvalue(), fname
        except Exception as e:
            last_err = e
            wait = min(2 ** (attempt - 1) + random.random(), 5)
            print(
                f"ПРЕДУПРЕЖДЕНИЕ: ошибка загрузки {kind} {url} (попытка {attempt}/{MAX_RETRIES}): {e}. Повтор через {wait:.1f}с"
            )
            time.sleep(wait)
    raise last_err

# --- 4. ФУНКЦИЯ ОТПРАВКИ ПОСТА ---

async def send_post(record, row_idx):
    """Собирает, форматирует и отправляет пост (фото+видео+текст), поддерживая фото и видео."""
    # Парсинг данных из записи (из столбцов таблицы Google Sheets)
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
    
    # Сборка HTML-сообщения (формируем текст поста из данных записи)
    message_html_lines = [
        # Здесь формируем список строк с информацией (имя, услуги, возраст и т.д.)
        # Например:
        f"<b>{name}</b>",
        f"{services}" + (f", {extra_services}" if extra_services else ""),
        f"Возраст: {age}, Рост: {height}, Вес: {weight}, Грудь: {bust}",
        f"Express: {express_price}, Incall: {incall_price}, Outcall: {outcall_price}",
    ]
    # Добавляем ссылку WhatsApp, если указана
    if whatsapp_link:
        message_html_lines.append(f'<a href="{whatsapp_link}">WhatsApp</a>')
    # Если нужно добавить пробелы перед эмодзи короны (skip_text содержит, например, символы пробела)
    if skip_text:
        message_html_lines.append(skip_text)
    # Добавляем эмодзи короны (id=2 соответствует "👑" из emoji_placeholders)
    crown_placeholder = emoji_placeholders.get(2, "")
    if crown_placeholder:
        message_html_lines.append(crown_placeholder)

    # Объединяем все строки в одно HTML сообщение с переводами строк
    message_html = "\n".join(message_html_lines)

    # --- Разделение фото и видео по ссылкам ---
    photo_column_headers = [f"Ссылка {i}" for i in range(1, 11)]  # Ссылка 1 ... Ссылка 10
    video_extensions = ['.mp4', '.mov', '.avi', '.mkv', '.webm', '.m4v']  # Расширения, считающиеся видео
    
    photo_urls = []
    video_urls = []
    for header in photo_column_headers:
        url = record.get(header)
        if url and isinstance(url, str) and url.startswith("http"):
            if any(url.lower().endswith(ext) for ext in video_extensions):
                video_urls.append(url)
            else:
                photo_urls.append(url)
    
    print(f"Найдено {len(photo_urls)} фото и {len(video_urls)} видео для строки {row_idx}.")

    # Загрузка данных фото
    photo_data = []
    for url in photo_urls:
        try:
            content, fname = download_file_with_retries(url, 'photo')
            photo_data.append((content, fname))
        except Exception as e:
            print(f"ПРЕДУПРЕЖДЕНИЕ: не удалось загрузить фото {url} - {e}")

    # Загрузка данных видео
    video_data = []
    for url in video_urls:
        try:
            content, fname = download_file_with_retries(url, 'video')
            video_data.append((content, fname))
        except Exception as e:
            print(f"ПРЕДУПРЕЖДЕНИЕ: не удалось загрузить видео {url} - {e}")

    # --- Отправка медиа (фото + видео) одним альбомом ---
    tasks = []
    clients_with_channels = [(client1, TG1_CHANNEL), (client2, TG2_CHANNEL), (client3, TG3_CHANNEL)]
    for client, channel_str in clients_with_channels:
        if not (client.is_connected() and channel_str):
            continue
        try:
            # Определяем идентификатор канала (int ID или строковый username)
            channel = int(channel_str)
        except (ValueError, TypeError):
            channel = channel_str
        # Создаём задачу отправки поста для каждого подключенного клиента
        tasks.append(send_media_for_client(client, channel, message_html, photo_data, video_data))
    # Выполняем отправку для всех клиентов параллельно
    if tasks:
        sent_messages = await asyncio.gather(*tasks, return_exceptions=True)
        # Проверяем, что хотя бы одна отправка была успешной (не None и без исключения)
        success = False
        for result in sent_messages:
            if isinstance(result, Exception):
                # Логируем исключения, если произошли
                print(f"Ошибка при отправке через одного из клиентов: {result}")
            elif result:  # результат не None
                success = True
        if success:
            # Отмечаем в таблице, что сообщение отправлено (колонка "Отправлено" установится в TRUE)
            worksheet.update_cell(row_idx, 1, "TRUE")
            print(f"Сообщение для строки {row_idx} отправлено и отмечено как отправленное.")
    else:
        print(f"Для строки {row_idx} не подключено ни одного Telegram-клиента для отправки.")

async def send_media_for_client(client, channel, caption, photo_data, video_data):
    """Отправляет фото+видео как один медиапост (альбом) с общей подписью.
       Если медиа больше 10, отправляет последующие файлы ответом на первое сообщение."""
    all_media = photo_data + video_data
    # Если нет медиа, но есть только текст, отправляем просто текстовое сообщение
    if not all_media:
        if caption:
            return await client.send_message(channel, caption)
        return None

    first_message = None
    prev_message = None

    # Отправляем медиа группами по 10 (лимит альбома Telegram)
    for i in range(0, len(all_media), 10):
        chunk = all_media[i:i + 10]
        file_objs = []
        for content, name in chunk:
            bio = io.BytesIO(content)
            bio.name = name  # Сохраняем имя файла для корректного определения типа (фото/видео)
            file_objs.append(bio)
        # Для первой группы указываем подпись только для первого файла
        if i == 0 and caption:
            # Формируем список подписей: первая с текстом, остальные пустые
            captions = [caption] + [""] * (len(file_objs) - 1)
        else:
            captions = None
        # Отправляем группу файлов (альбом). Если это не первая группа, отвечаем на предыдущее сообщение (reply_to).
        sent = await client.send_file(
            channel,
            file_objs,
            caption=captions,
            reply_to=prev_message.id if (i > 0 and prev_message) else None,
        )
        # send_file возвращает список сообщений (List[Message]) для альбома или единственный Message
        if isinstance(sent, list):
            msg = sent[0]  # Первый медиа-элемент несёт общую подпись
        else:
            msg = sent
        # Сохраняем первое сообщение альбома
        if first_message is None:
            first_message = msg
        prev_message = msg  # Для следующей группы задаём reply_to на последнее отправленное сообщение
    return first_message

# --- 5. ГЛАВНЫЙ ЦИКЛ ПРОГРАММЫ ---

async def main():
    """Главная функция: подключается к клиентам и запускает бесконечный цикл проверки таблицы."""
    clients = [c for c in [client1, client2, client3] if c.api_id and c.api_hash]
    if not clients:
        print("ОШИБКА: Не настроен ни один Telegram клиент. Проверьте переменные TG_API_ID и TG_API_HASH.")
        return
        
    print("Подключение Telegram клиентов...")
    await asyncio.gather(*(c.start() for c in clients))
    print("Клиенты успешно подключены. Запуск основного цикла...")

    while True:
        try:
            print(f"Проверка таблицы... {datetime.now(tz).strftime('%H:%M:%S')}")
            records = worksheet.get_all_records()
            now = datetime.now(tz)
            for idx, record in enumerate(records, start=2):  # начиная со 2-й строки (первая строка - заголовки)
                sent_flag = record.get("Отправлено")
                if str(sent_flag).upper() == "TRUE":
                    continue  # Пропускаем уже отправленные записи
                if not str(record.get("Имя", "")).strip():
                    continue  # Пропускаем пустые строки (без имени, считаем запись неполной)
                time_str = record.get("Время")
                if not time_str:
                    continue  # Если не указано время, пропускаем
                try:
                    sched_time = datetime.strptime(time_str, "%d.%m.%Y %H:%М:%S")
                    sched_time = tz.localize(sched_time)
                    if sched_time <= now:
                        print(f"Найдена запись для отправки (строка {idx}). Отправляем пост...")
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