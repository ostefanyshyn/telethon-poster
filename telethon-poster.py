import asyncio
import base64
import json
import os
import re
from datetime import datetime
import pytz
import requests
import io
import gspread
import sys
import logging
from telethon.network import connection as tl_connection
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.extensions import html as tl_html
from telethon import types
import telethon.errors as tl_errors
from dotenv import load_dotenv

# Optional: pixel-accurate text width via Pillow (recommended on render.com)
try:
    from PIL import ImageFont
    _PIL_AVAILABLE = True
except Exception:
    _PIL_AVAILABLE = False


# Загрузка переменных из .env файла
load_dotenv()

# Mute noisy Telethon reconnect logs like "Server closed the connection" unless explicitly overridden
LOG_LEVEL = os.environ.get("TELETHON_LOG_LEVEL", "ERROR").upper()
try:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("telethon").setLevel(getattr(logging, LOG_LEVEL, logging.ERROR))
    logging.getLogger("telethon.network").setLevel(getattr(logging, LOG_LEVEL, logging.ERROR))
    logging.getLogger("telethon.network.mtprotosender").setLevel(getattr(logging, LOG_LEVEL, logging.ERROR))
except Exception:
    pass

# Предупреждение о версии Python (Telethon может работать нестабильно на Python 3.13)
if sys.version_info >= (3, 13):
    print("ПРЕДУПРЕЖДЕНИЕ: Обнаружен Python 3.13+. Известны проблемы совместимости asyncio с Telethon. "
          "Рекомендуется использовать Python 3.11–3.12 или обновить Telethon до последней версии.")

# --- 1. КОНФИГУРАЦИЯ ---

# Google Sheets
GSHEET_ID = os.environ.get("GSHEET_ID")
GOOGLE_CREDS_JSON = os.environ.get("GOOGLE_CREDS_JSON")

# Telegram аккаунты: общий TG_API_ID/TG_API_HASH и пер-аккаунтные TG{n}_SESSION, TG{n}_CHANNEL, прокси
accounts = []

# Общие Telegram API креды (единые для всех аккаунтов)
TG_API_ID_STR = os.environ.get("TG_API_ID")
TG_API_HASH = os.environ.get("TG_API_HASH")

if not TG_API_ID_STR or not TG_API_HASH:
    print("ОШИБКА: Укажите общие TG_API_ID и TG_API_HASH в переменных окружения.")
    exit(1)
try:
    TG_API_ID = int(TG_API_ID_STR)
except Exception:
    print("ОШИБКА: TG_API_ID должен быть числом.")
    exit(1)

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

    # Пропускаем пустые слоты без сессии/канала
    if not (session or channel):
        continue

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

# Авто-обнаружение флаговых столбцов (числовые имена колонок '1', '2', ...)
try:
    SENT_FLAG_INDICES = sorted(
        int(name) for name in HEADER_TO_COL.keys() if str(name).strip().isdigit()
    )
    if not SENT_FLAG_INDICES:
        print("ПРЕДУПРЕЖДЕНИЕ: Не найдено числовых флаговых столбцов в заголовке.")
except Exception as e:
    print(f"ПРЕДУПРЕЖДЕНИЕ: ошибка при определении флаговых столбцов: {e}")
    SENT_FLAG_INDICES = []

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
            request_retries=5,
            connection_retries=3,
            retry_delay=2,
            timeout=30,
            flood_sleep_threshold=60,
        )
    )

# Удобные словари доступа по индексу аккаунта
ACC_BY_INDEX = {acc["index"]: acc for acc in accounts}
CLIENT_BY_INDEX = {acc["index"]: c for c, acc in zip(clients, accounts)}

# Runtime de-duplication guard: prevent repeating sends if Google Sheets flag update lags
SENT_RUNTIME = set()  # stores tuples of (row_idx, acc_idx)

# --- 3. ПОЛЬЗОВАТЕЛЬСКИЕ EMOJI И ПАРСЕР + ТИПОГРАФИКА ---

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

# Установка парсера по умолчанию
for _c in clients:
    _c.parse_mode = CustomHtml()

# ID кастомных эмодзи
emoji_ids = {
    1: 5467538555158943525,   # 💭 (left)
    2: 5467538555158943525,   # 💭 (right)
    3: 5217822164362739968,   # 👑
    4: 5384225750213754731,   # ✅
    5: 5386698955591549040,   # ✅
    6: 5386434913887086602,   # ✅
    7: 5386675715523505193,   # ✅
    8: 5325547803936572038,   # ✨ (left)
    9: 5325547803936572038,   # ✨ (right)
    10: 5409048419211682843,  # 💵 (left)
    11: 5409048419211682843,  # 💵 (right)
    12: 5456140674028019486,  # ⚡️ (left)
    13: 5456140674028019486,  # ⚡️ (right)
    14: 5334998226636390258,  # 📱
}

# Unicode-заменители для отображения в коде
emoji_placeholders = {
    1: "💭",  2: "💭",
    3: "👑",
    4: "✅",  5: "✅",  6: "✅",  7: "✅",
    8: "✨",  9: "✨",
    10: "💵", 11: "💵",
    12: "⚡️", 13: "⚡️",
    14: "📱",
}

# Пробелы/переводы строк для стабильной типографики
NNBSP = "\u202F"   # узкий неразрывный пробел (для отступа короны)
THIN  = "\u2009"   # тонкий пробел (зазоры вокруг эмодзи)
CROWN_OFFSET_ADJUST = int(os.environ.get("CROWN_OFFSET_ADJUST", "0"))
CROWN_OFFSET_SCALE = float(os.environ.get("CROWN_OFFSET_SCALE", "1.25"))

# === Pixel-based width helpers (for precise crown centering) ===
# Configurable font and sizing (works on render.com). If font is missing, we fall back safely.
CROWN_FONT_PATH = os.environ.get("CROWN_FONT_PATH", "")  # e.g. ./fonts/DejaVuSans.ttf
CROWN_FONT_SIZE = int(os.environ.get("CROWN_FONT_SIZE", "18"))
# Scale factor for custom-emoji visual width relative to the font's 1em (tweak if crown looks off)
CROWN_EM_WIDTH_SCALE = float(os.environ.get("CROWN_EM_WIDTH_SCALE", "1.0"))
# Optional pixel adjustment (+/-) after centering math
CROWN_OFFSET_PX_ADJUST = int(os.environ.get("CROWN_OFFSET_PX_ADJUST", "0"))

# Device preset and fine-tune options for better mobile (iOS) rendering
CROWN_PRESET = os.environ.get("CROWN_PRESET", "").lower()  # e.g. "ios"
CROWN_FINE_TUNE = os.environ.get("CROWN_FINE_TUNE", "thin").lower()  # "thin" | "none"

# Apply iOS defaults only if user didn't explicitly override via env
if CROWN_PRESET == "ios":
    if "CROWN_EM_WIDTH_SCALE" not in os.environ:
        CROWN_EM_WIDTH_SCALE = 0.96  # iOS tends to render slightly wider vs 1em heuristic
    if "CROWN_OFFSET_PX_ADJUST" not in os.environ:
        CROWN_OFFSET_PX_ADJUST = 1   # nudge crown by ~1px to the right
_CROWN_FONT_SOURCE = None

# Lazy-load a font that supports Cyrillic on typical Linux containers
def _load_crown_font():
    global _CROWN_FONT_SOURCE
    if not _PIL_AVAILABLE:
        return None
    # Try env-provided font first
    paths_to_try = [p for p in [CROWN_FONT_PATH] if p]
    # Common Linux locations (present on many containers)
    paths_to_try += [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/dejavu/DejaVuSans.ttf",
        "/usr/local/share/fonts/DejaVuSans.ttf",
    ]
    for p in paths_to_try:
        try:
            if p and os.path.exists(p):
                _CROWN_FONT_SOURCE = f"env:{p}" if p == CROWN_FONT_PATH and p else f"system:{p}"
                return ImageFont.truetype(p, CROWN_FONT_SIZE)
        except Exception:
            pass
    # Final fallback – Pillow's default bitmap font (limited glyph coverage)
    try:
        _CROWN_FONT_SOURCE = "Pillow:load_default"
        return ImageFont.load_default()
    except Exception:
        return None

_CROWN_FONT = _load_crown_font()


if _PIL_AVAILABLE and _CROWN_FONT:
    if hasattr(_CROWN_FONT, "getlength"):
        _CROWN_MEASURE_MODE = "Pillow.getlength"
    else:
        _CROWN_MEASURE_MODE = "Pillow.getbbox"
    print(f"[CROWN] Pixel measurement ENABLED via Pillow ({_CROWN_MEASURE_MODE}); font={_CROWN_FONT_SOURCE}; size={CROWN_FONT_SIZE}")
else:
    print("[CROWN] Pixel measurement DISABLED; using heuristic width (~7 px/char).")

print(f"[CROWN] Preset={CROWN_PRESET or 'default'}, fine_tune={CROWN_FINE_TUNE}")

_tag_re = re.compile(r"<[^>]+>")

def _strip_tags(s: str) -> str:
    return _tag_re.sub("", str(s or "")).strip()

# Approximate text width in pixels using the selected font; safe fallback if PIL/font is unavailable
def _text_width_px(s: str) -> int:
    plain = str(s or "")
    if _PIL_AVAILABLE and _CROWN_FONT:
        try:
            if hasattr(_CROWN_FONT, "getlength"):
                return int(round(_CROWN_FONT.getlength(plain)))
            # Older Pillow fallback
            bbox = _CROWN_FONT.getbbox(plain)
            return max(0, int(bbox[2] - bbox[0]))
        except Exception:
            pass
    # Heuristic fallback (~7 px per character)
    return int(round(len(plain) * 7))

# Width of a space-like character (NNBSP by default) in pixels
def _space_width_px(ch: str = NNBSP) -> int:
    if _PIL_AVAILABLE and _CROWN_FONT:
        try:
            if hasattr(_CROWN_FONT, "getlength"):
                return max(1, int(round(_CROWN_FONT.getlength(ch))))
            bbox = _CROWN_FONT.getbbox(ch)
            return max(1, int(bbox[2] - bbox[0]))
        except Exception:
            pass
    return 7  # heuristic fallback

def _plain_len(s: str) -> int:
    """Приблизительная 'ширина' имени: убираем теги и считаем символы."""
    txt = re.sub(r"<[^>]+>", "", str(s or "")).strip()
    return len(txt)

def crown_over_name_lines(name: str, crown_html: str):
    """
    Возвращает две строки: (1) корона с автоотступом, (2) имя.
    Смещение вычисляется по ширине текста в пикселях (через Pillow, если доступно).
    На render.com это работает из коробки при наличии Pillow и шрифта; при их отсутствии
    используется безопасный эвристический фолбэк.
    Доступные настройки через ENV:
      - CROWN_FONT_PATH: путь к .ttf (например, DejaVuSans.ttf)
      - CROWN_FONT_SIZE: размер шрифта в px (по умолчанию 18)
      - CROWN_EM_WIDTH_SCALE: множитель ширины эмодзи относительно 1em
      - CROWN_OFFSET_PX_ADJUST: ручная пиксельная подстройка смещения
    """
    name_plain = _strip_tags(name)

    # Ширина имени в пикселях
    name_px = _text_width_px(name_plain)

    # Оценка ширины "эмодзи-короны" в пикселях: берём 1em (ширину символа "M") и масштабируем
    em_px = _space_width_px("M")  # 1em approximately
    crown_px = max(1, int(round(em_px * CROWN_EM_WIDTH_SCALE)))

    # Центрируем корону по центру имени
    offset_px = max(0, int(round(name_px / 2 - crown_px / 2)))

    # Ручная подстройка в пикселях (если нужно немного сдвинуть)
    offset_px += CROWN_OFFSET_PX_ADJUST

    # Конвертируем пиксели в количество узких неразрывных пробелов
    nnbsp_px = _space_width_px(NNBSP)
    # Coarse step by narrow NBSP
    n_spaces = max(0, int(offset_px // max(1, nnbsp_px)))
    leftover_px = max(0, int(offset_px - n_spaces * max(1, nnbsp_px)))

    # Fine-tune with THIN spaces (smaller width) to better match iOS rendering
    thin_count = 0
    if CROWN_FINE_TUNE == "thin":
        thin_px = _space_width_px(THIN)
        if thin_px > 0:
            thin_count = int(round(leftover_px / thin_px))
            thin_count = max(0, min(thin_count, 8))  # cap to avoid overshoot

    # Backward-compatible manual adjust in units of NNBSP
    n_spaces += max(0, CROWN_OFFSET_ADJUST)

    indent = (NNBSP * n_spaces) + (THIN * thin_count)
    line1 = f"{indent}{crown_html}"
    line2 = f"<b><i>{name}</i></b>"
    return line1, line2

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

    # --- сбор строк внутри блоков ---
    param_lines = []
    if age and str(age).strip():    param_lines.append(f"Возраст - {age}")
    if height and str(height).strip(): param_lines.append(f"Рост - {height}")
    if weight and str(weight).strip(): param_lines.append(f"Вес - {weight}")
    if bust and str(bust).strip():  param_lines.append(f"Грудь - {bust}")

    # Блоки сообщения (между блоками — ровно одна пустая строка)
    blocks = []

    # 1) Статус
    blocks.append(
        f'<a href="emoji/{emoji_ids[1]}">{emoji_placeholders[1]}</a>{THIN}'
        f'<i>{status}</i>{THIN}'
        f'<a href="emoji/{emoji_ids[2]}">{emoji_placeholders[2]}</a>'
    )

    # 2) Коронка над именем (2 строки)
    crown_html = f'<a href="emoji/{emoji_ids[3]}">{emoji_placeholders[3]}</a>'
    line1, line2 = crown_over_name_lines(name, crown_html)
    blocks.append("\n".join([line1, line2]))

    # 3) Фото
    foto_checks = "".join(f'<a href="emoji/{emoji_ids[i]}">{emoji_placeholders[i]}</a>' for i in range(4, 8))
    blocks.append(f'<b>Фото{THIN}{foto_checks}</b>')

    # 4) Услуги/Доп.услуги (если есть)
    services_lines = []
    if services and str(services).strip():
        services_lines += ["Услуги:", f'<b><i>{services}</i></b>']
    if extra_services and str(extra_services).strip():
        if services_lines:
            services_lines.append("")   # одна пустая строка внутри блока между разделами
        services_lines += ["Доп. услуги:", f'<b><i>{extra_services}</i></b>']
    if services_lines:
        inner = "\n".join(services_lines)
        blocks.append(f"<blockquote>{inner}</blockquote>")

    # 5) Параметры (если есть)
    if param_lines:
        params_header = (
            f'<a href="emoji/{emoji_ids[8]}">{emoji_placeholders[8]}</a>{THIN}'
            f'Параметры{THIN}'
            f'<a href="emoji/{emoji_ids[9]}">{emoji_placeholders[9]}</a>'
        )
        blocks.append(params_header + "\n" + '<b><i>' + "\n".join(param_lines) + '</i></b>')

    # 6) Цены (если есть)
    def _fmt_price(val):
        try:
            num = float(str(val).replace(' ', '').replace(',', '.'))
            amount = int(round(num * 1000))  # значения трактуем как тысячи AMD
            return f"{format(amount, ',d').replace(',', '.')} AMD"
        except Exception:
            return f"{val} AMD"

    price_lines = []
    if express_price and str(express_price).strip(): price_lines.append(f"Express - {_fmt_price(express_price)}")
    if incall_price and str(incall_price).strip():  price_lines.append(f"Incall - {_fmt_price(incall_price)}")
    if outcall_price and str(outcall_price).strip(): price_lines.append(f"Outcall - {_fmt_price(outcall_price)}")
    if price_lines:
        price_header = (
            f'<a href="emoji/{emoji_ids[10]}">{emoji_placeholders[10]}</a>{THIN}'
            f'Цена{THIN}'
            f'<a href="emoji/{emoji_ids[11]}">{emoji_placeholders[11]}</a>'
        )
        blocks.append(price_header + "\n" + '<b><i>' + "\n".join(price_lines) + '</i></b>')

    # 7) Призыв + контакты (БЕЗ пустой строки между ними)
    cta_and_contacts = [
        f'<a href="emoji/{emoji_ids[12]}">{emoji_placeholders[12]}</a>'
        f'{THIN}<b><i>Назначь встречу уже сегодня!</i></b>{THIN}'
        f'<a href="emoji/{emoji_ids[13]}">{emoji_placeholders[13]}</a>'
    ]
    if telegram_link and str(telegram_link).strip():
        cta_and_contacts.append(
            f'<a href="emoji/{emoji_ids[14]}">{emoji_placeholders[14]}</a> '
            f'<a href="{telegram_link}"><b>Связь в Telegram</b></a>'
        )
    if whatsapp_link and str(whatsapp_link).strip():
        cta_and_contacts.append(
            f'<a href="emoji/{emoji_ids[14]}">{emoji_placeholders[14]}</a> '
            f'<a href="{whatsapp_link}"><b>Связь в WhatsApp</b></a>'
        )
    blocks.append("\n".join(cta_and_contacts))

    # --- финальная склейка: ОДНА пустая строка между блоками ---
    message_html = "\n\n".join(blocks)

    # --- медиа ---
    media_column_headers = [f"Ссылка {i}" for i in range(1, 11)]
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
                resp = requests.get(url, timeout=(5, 60))
                resp.raise_for_status()
                file_data = resp.content
                file_base = url.split("/")[-1].split("?")[0]
                content_type = resp.headers.get('Content-Type', '').lower()
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

    # --- отправка ---
    if pending_indices is None:
        target_indexes = [
            i for i, acc in ACC_BY_INDEX.items()
            if i in SENT_FLAG_INDICES and acc.get("channel")
            and str(record.get(str(i), record.get(i, ""))).upper() != "TRUE"
        ]
    else:
        target_indexes = [i for i in pending_indices if i in ACC_BY_INDEX and ACC_BY_INDEX[i].get("channel")]

    clients_with_channels = [(CLIENT_BY_INDEX[i], ACC_BY_INDEX[i]) for i in sorted(target_indexes)]

    async def _send_to_one(client, acc):
        channel_str = acc.get("channel")
        acc_idx = acc.get("index")
        # Skip if we already sent this row to this account in this process
        rt_key = (row_idx, acc_idx)
        if rt_key in SENT_RUNTIME:
            return acc_idx, channel_str, True, "runtime-dup-skip"
        if not channel_str:
            return acc_idx, channel_str, False, "no_channel"

        try:
            if not client.is_connected():
                await client.connect()
        except Exception as e:
            print(f"ПРЕДУПРЕЖДЕНИЕ: TG{acc_idx} не удалось подключить клиента: {e}")
            return acc_idx, channel_str, False, f"connect: {e}"

        try:
            channel = int(channel_str)
        except (ValueError, TypeError):
            channel = channel_str

        try:
            if media_data:
                file_objs = []
                for data, fname in media_data:
                    bio = io.BytesIO(data); bio.name = fname; file_objs.append(bio)
                await client.send_file(
                    channel, file_objs, caption=message_html,
                    supports_streaming=True, parse_mode=CustomHtml()
                )
            else:
                await client.send_message(
                    channel, message_html, parse_mode=CustomHtml()
                )

            # отметить флаг
            flag_name = str(acc_idx)
            col_idx = get_col_index(flag_name)
            if col_idx:
                try:
                    worksheet.update_cell(row_idx, col_idx, "TRUE")
                except Exception as e_upd:
                    print(f"ПРЕДУПРЕЖДЕНИЕ: не удалось обновить флаг {flag_name} (строка {row_idx}): {e_upd}")
            SENT_RUNTIME.add(rt_key)
            return acc_idx, channel_str, True, None

        except (tl_errors.FloodWaitError, tl_errors.SlowModeWaitError) as e:
            print(f"ОШИБКА: TG{acc_idx} лимит Telegram: {e}")
            return acc_idx, channel_str, False, f"rate: {e}"
        except Exception as e:
            print(f"ПРЕДУПРЕЖДЕНИЕ: TG{acc_idx} ошибка отправки: {e}. Пробуем повтор...")
            try:
                await client.disconnect()
            except Exception:
                pass
            try:
                await client.connect()
                if media_data:
                    file_objs = []
                    for data, fname in media_data:
                        bio = io.BytesIO(data); bio.name = fname; file_objs.append(bio)
                    await client.send_file(
                        channel, file_objs, caption=message_html,
                        supports_streaming=True, parse_mode=CustomHtml()
                    )
                else:
                    await client.send_message(
                        channel, message_html, parse_mode=CustomHtml()
                    )
                # отметить флаг
                flag_name = str(acc_idx)
                col_idx = get_col_index(flag_name)
                if col_idx:
                    try:
                        worksheet.update_cell(row_idx, col_idx, "TRUE")
                    except Exception as e_upd:
                        print(f"ПРЕДУПРЕЖДЕНИЕ: не удалось обновить флаг {flag_name} (строка {row_idx}): {e_upd}")
                SENT_RUNTIME.add(rt_key)
                return acc_idx, channel_str, True, None
            except Exception as e2:
                print(f"ОШИБКА: TG{acc_idx} повторная отправка не удалась: {e2}")
                return acc_idx, channel_str, False, f"retry: {e2}"

    results = await asyncio.gather(
        *[_send_to_one(client, acc) for (client, acc) in clients_with_channels],
        return_exceptions=False
    )

    ok = sum(1 for (_, _, s, _) in results if s)
    fail = [(i, ch, err) for (i, ch, s, err) in results if not s]
    print(f"Строка {row_idx}: перс-отправки завершены. Успешно {ok}/{len(clients_with_channels)}. Неудачи: {fail}")

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
            # Proactive reconnect sweep to recover from "Server closed the connection" events
            for acc_idx, client in CLIENT_BY_INDEX.items():
                if not client.is_connected():
                    try:
                        await client.connect()
                        print(f"TG{acc_idx} переподключен.")
                    except Exception as e:
                        print(f"ПРЕДУПРЕЖДЕНИЕ: TG{acc_idx} не удалось переподключить: {e}")
            print(f"Активных клиентов: {alive}/{len(clients)}")
            print(f"Проверка таблицы... {datetime.now(tz).strftime('%H:%M:%S')}")
            records = worksheet.get_all_records()
            now = datetime.now(tz)

            for idx, record in enumerate(records, start=2):
                if not str(record.get("Имя", "")).strip():
                    continue

                active_idx = [acc["index"] for acc in accounts if acc.get("channel") and acc["index"] in SENT_FLAG_INDICES]
                if not active_idx:
                    continue

                pending_idx = [
                    i for i in active_idx
                    if str(record.get(str(i), record.get(i, ""))).upper() != "TRUE"
                ]
                if not pending_idx:
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