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
    """Собирает, форматирует и отправляет пост, поддерживая фото и видео."""
    # --- Парсинг данных из записи (этот блок без изменений) ---
    status = record.get("Статус", "")
    name = record.get("Имя", "")
    # ... (весь остальной парсинг ваших полей остается здесь)
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
    
    # --- Сборка HTML-сообщения (этот блок без изменений) ---
    message_html = "\n".join([
        # ... (вся ваша логика сборки message_html_lines остается здесь)
    ])

    # --- 📸 ИЗМЕНЕНИЕ: Разделение фото и видео ---
    photo_column_headers = ["Ссылка 1", "Ссылка 2", "Ссылка 3", "Ссылка 4", "Ссылка 5", "Ссылка 6", "Ссылка 7", "Ссылка 8", "Ссылка 9", "Ссылка 10"]
    video_extensions = ['.mp4', '.mov', '.avi', '.mkv'] # Список расширений видео
    
    photo_urls = []
    video_urls = []

    for header in photo_column_headers:
        url = record.get(header)
        if url and isinstance(url, str) and url.startswith("http"):
            # Проверяем URL на расширения видео
            if any(url.lower().endswith(ext) for ext in video_extensions):
                video_urls.append(url)
            else:
                photo_urls.append(url)
    
    print(f"Найдено {len(photo_urls)} фото и {len(video_urls)} видео для строки {row_idx}.")

    # Загрузка данных
    photo_data = []
    for url in photo_urls:
        try:
            # ... (логика загрузки как раньше)
            resp = requests.get(url)
            resp.raise_for_status()
            photo_data.append((resp.content, url.split("/")[-1].split("?")[0] or "image.jpg"))
        except Exception as e:
            print(f"ПРЕДУПРЕЖДЕНИЕ: не удалось загрузить фото {url} - {e}")

    video_data = []
    for url in video_urls:
        try:
            # ... (логика загрузки как раньше)
            resp = requests.get(url)
            resp.raise_for_status()
            video_data.append((resp.content, url.split("/")[-1].split("?")[0] or "video.mp4"))
        except Exception as e:
            print(f"ПРЕДУПРЕЖДЕНИЕ: не удалось загрузить видео {url} - {e}")

    # --- 📤 ИЗМЕНЕНИЕ: Новая логика отправки ---
    tasks = []
    clients_with_channels = [(client1, TG1_CHANNEL), (client2, TG2_CHANNEL), (client3, TG3_CHANNEL)]

    for client, channel_str in clients_with_channels:
        if not (client.is_connected() and channel_str):
            continue
        
        try:
            channel = int(channel_str)
        except (ValueError, TypeError):
            channel = channel_str
        
        # Создаем асинхронную подзадачу для каждого клиента
        tasks.append(send_media_for_client(client, channel, message_html, photo_data, video_data))

    if tasks:
        sent_messages = await asyncio.gather(*tasks)
        # Проверяем, что хотя бы одна отправка была успешной (не None)
        if any(sent_messages):
            worksheet.update_cell(row_idx, 1, "TRUE")
            print(f"Сообщение для строки {row_idx} отправлено и отмечено как отправленное.")
    else:
        print(f"Для строки {row_idx} не было найдено активных каналов для отправки.")

async def send_media_for_client(client, channel, caption, photo_data, video_data):
    """Отправляет медиа для одного клиента, управляя ответами."""
    last_message = None
    
    # 1. Отправляем фотоальбом с подписью
    if photo_data:
        file_objs = [io.BytesIO(data) for data, fname in photo_data]
        for bio, (_, fname) in zip(file_objs, photo_data):
            bio.name = fname
        
        # Отправляем альбом и сохраняем сообщение
        sent_album = await client.send_file(channel, file_objs, caption=caption)
        last_message = sent_album[0] if isinstance(sent_album, list) else sent_album

    # 2. Если фото не было, отправляем текст, чтобы было на что отвечать
    elif caption and not video_data: # Отправляем текст только если нет и видео
        last_message = await client.send_message(channel, caption)
        
    # 3. Отправляем видео
    if video_data:
        # Если не было фото, но есть видео, отправляем подпись с первым видео
        if not photo_data:
            first_video_content, first_video_name = video_data.pop(0)
            bio = io.BytesIO(first_video_content)
            bio.name = first_video_name
            last_message = await client.send_file(channel, bio, caption=caption)

        # Отправляем остальные видео в ответ на предыдущее сообщение
        for content, name in video_data:
            bio = io.BytesIO(content)
            bio.name = name
            last_message = await client.send_file(channel, bio, reply_to=last_message.id)
            
    return last_message

# --- 5. ГЛАВНЫЙ ЦИКЛ ПРОГРАММЫ ---

async def main():
    """Главная функция: подключается к клиентам и запускает бесконечный цикл проверки."""
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