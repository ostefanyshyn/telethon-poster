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
import traceback
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
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s: %(message)s")
    logging.getLogger("telethon").setLevel(getattr(logging, LOG_LEVEL, logging.ERROR))
    logging.getLogger("telethon.network").setLevel(getattr(logging, LOG_LEVEL, logging.ERROR))
    logging.getLogger("telethon.network.mtprotosender").setLevel(getattr(logging, LOG_LEVEL, logging.ERROR))
except Exception:
    pass

# === Telegram DM notifications via Bot API ===================================
# Configure env: TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

def tg_notify(text: str):
    """
    Fire-and-forget notification to your personal chat via Bot API.
    Does nothing if TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID are not set.
    """
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        return
    try:
        msg = str(text)
        # Telegram max length ~4096
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            data={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": msg[:4000],
                "disable_web_page_preview": True,
            },
            timeout=(3, 10),
        )
    except Exception:
        # never break the app because of notifications
        pass

class TGBotLoggingHandler(logging.Handler):
    """Send ERROR/CRITICAL logs to Telegram DM."""
    def emit(self, record: logging.LogRecord):
        try:
            msg = self.format(record)
            if record.exc_info:
                msg += "\n\n" + "".join(traceback.format_exception(*record.exc_info))
            app = os.getenv("HEROKU_APP_NAME", "telethon-poster")
            tg_notify(f"🚨 {app}\n{msg}")
        except Exception:
            pass

_lvl = logging.ERROR
_tg_handler = TGBotLoggingHandler(level=_lvl)
_tg_handler.setFormatter(logging.Formatter("%(name)s: %(message)s"))
logging.getLogger().addHandler(_tg_handler)

# Helper to log and DM-notify skipped publications
def _notify_skip(row_idx, reason):
    logging.error(f"Строка {row_idx}: {reason}")
    tg_notify(f"⚠️ Строка {row_idx}: {reason}")

# Helper to warn (but not skip) about media issues and continue with remaining files
def _notify_media_issue(row_idx, reason):
    logging.warning(f"Строка {row_idx}: {reason}")
    tg_notify(f"ℹ️ Строка {row_idx}: {reason}")

# --- MEDIA DOWNLOAD (с подбором расширений) ----------------------------------

def _swap_media_extension(url: str):
    url_str = str(url or "")
    # Match an extension right before end or a query/hash (e.g. .jpg, .PNG)
    m = re.search(r'(\.[A-Za-z0-9]+)(?=$|[?#])', url_str)
    all_exts = (
        ".jpg", ".JPG",
        ".jpeg", ".JPEG",
        ".png", ".PNG",
        ".webp", ".WEBP",
        ".mp4", ".MP4",
        ".mov", ".MOV",
    )
    if m:
        prefix = url_str[:m.start(1)]
        suffix = url_str[m.end(1):]
        current_ext = url_str[m.start(1):m.end(1)]  # keep original case
        alts = [prefix + ext + suffix for ext in all_exts if ext != current_ext]
    else:
        # No extension: propose base + every extension
        alts = [url_str + ext for ext in all_exts]
    return alts or None

def _download_with_fallback(url: str, row_idx: int, timeout=(5, 60)):
    try:
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        return resp.content, url, resp.headers.get('Content-Type', '').lower()
    except Exception as first_e:
        alts = _swap_media_extension(url)
        if not alts:
            raise first_e
        last_err = first_e
        for alt in alts:
            try:
                resp = requests.get(alt, timeout=timeout)
                resp.raise_for_status()
                return resp.content, alt, resp.headers.get('Content-Type', '').lower()
            except Exception as e:
                last_err = e
                continue
        raise last_err

_DEF_EXTS = (
    ".jpg", ".JPG",
    ".jpeg", ".JPEG",
    ".png", ".PNG",
    ".webp", ".WEBP",
    ".mp4", ".MP4",
    ".mov", ".MOV",
)

def _has_known_ext(url: str) -> bool:
    u = str(url or "").lower()
    return any(u.endswith(ext) for ext in _DEF_EXTS)

def _download_with_ext_guess(url: str, row_idx: int, timeout=(5, 60)):
    u = str(url or "")
    return _download_with_fallback(u, row_idx, timeout=timeout)

def _global_excepthook(exc_type, exc, tb):
    logging.critical("Unhandled exception", exc_info=(exc_type, exc, tb))
sys.excepthook = _global_excepthook

# Предупреждение о версии Python (Telethon может работать нестабильно на Python 3.13)
if sys.version_info >= (3, 13):
    print("ПРЕДУПРЕЖДЕНИЕ: Обнаружен Python 3.13+. Известны проблемы совместимости asyncio с Telethon. "
          "Рекомендуется использовать Python 3.11–3.12 или обновить Telethon до последней версии.")

# --- 1. КОНФИГУРАЦИЯ ---

# Google Sheets
GSHEET_ID = os.environ.get("GSHEET_ID")
GOOGLE_CREDS_JSON = os.environ.get("GOOGLE_CREDS_JSON")

# Общие Telegram API креды
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

# Один общий аккаунт для всех каналов
TG_SESSION = os.environ.get("TG_SESSION") or os.environ.get("TG1_SESSION")
if not TG_SESSION:
    print("ОШИБКА: Укажите TG_SESSION (StringSession одного аккаунта) в переменных окружения.")
    exit(1)

# Глобальный (общий) прокси (опционально): TG_PROXY_* или TG16_PROXY_* как фолбэк
GLOBAL_PROXY = None
gp_type = os.environ.get("TG_PROXY_TYPE") or os.environ.get("PROXY_TYPE")
gp_host = os.environ.get("TG_PROXY_HOST") or os.environ.get("PROXY_HOST")
gp_port_str = os.environ.get("TG_PROXY_PORT") or os.environ.get("PROXY_PORT")
gp_rdns_str = os.environ.get("TG_PROXY_RDNS", os.environ.get("PROXY_RDNS", "true"))
gp_user = os.environ.get("TG_PROXY_USER") or os.environ.get("PROXY_USER")
gp_pass = os.environ.get("TG_PROXY_PASS") or os.environ.get("PROXY_PASS")

# Фолбэк к TG16_PROXY_*
if not (gp_type and gp_host and gp_port_str):
    gp_type = os.environ.get("TG16_PROXY_TYPE", gp_type)
    gp_host = os.environ.get("TG16_PROXY_HOST", gp_host)
    gp_port_str = os.environ.get("TG16_PROXY_PORT", gp_port_str)
    gp_rdns_str = os.environ.get("TG16_PROXY_RDNS", gp_rdns_str)
    gp_user = os.environ.get("TG16_PROXY_USER", gp_user)
    gp_pass = os.environ.get("TG16_PROXY_PASS", gp_pass)

if gp_type and gp_host and gp_port_str:
    try:
        gp_port = int(gp_port_str)
    except Exception:
        gp_port = None
    if gp_port:
        gp_rdns = str(gp_rdns_str).lower() in ("1", "true", "yes", "y", "on")
        GLOBAL_PROXY = (gp_type, gp_host, gp_port, gp_rdns, gp_user, gp_pass)

# Требование наличия прокси по переключателю (по умолчанию — включено)
REQUIRE_PROXY = str(os.environ.get("REQUIRE_PROXY", "true")).lower() in ("1", "true", "yes", "on")

# Собираем до 20 каналов TG{n}_CHANNEL
CHANNELS_BY_INDEX = {}
for n in range(1, 21):
    ch = os.environ.get(f"TG{n}_CHANNEL")
    if ch:
        CHANNELS_BY_INDEX[n] = ch

if not CHANNELS_BY_INDEX:
    print("ОШИБКА: Не задан ни один TG{n}_CHANNEL (например, TG1_CHANNEL).")
    exit(1)

# Совместимость: список с индексами и каналами для логики флагов в таблице
accounts = [{"index": i, "channel": ch} for i, ch in sorted(CHANNELS_BY_INDEX.items())]

# Проверка прокси — по переключателю REQUIRE_PROXY (по умолчанию обязателен)
if REQUIRE_PROXY and not GLOBAL_PROXY:
    logging.error(
        "Прокси не задан. Укажите TG_PROXY_TYPE/TG_PROXY_HOST/TG_PROXY_PORT "
        "(при необходимости TG_PROXY_USER/TG_PROXY_PASS/TG_PROXY_RDNS) — "
        "или установите REQUIRE_PROXY=0, чтобы разрешить работу без прокси."
    )
    exit(1)
elif not GLOBAL_PROXY:
    logging.warning("REQUIRE_PROXY=0 — продолжаем без прокси.")

# Интервал обновления (в секундах)
REFRESH_SECONDS = int(os.environ.get("REFRESH_SECONDS", 30))

# --- Validation tunables (speed up startup; override via env) ---
VALIDATION_CONNECT_TIMEOUT = int(os.environ.get("VALIDATION_CONNECT_TIMEOUT", "30"))
VALIDATION_AUTH_TIMEOUT = int(os.environ.get("VALIDATION_AUTH_TIMEOUT", "20"))
VALIDATION_DISCONNECT_TIMEOUT = int(os.environ.get("VALIDATION_DISCONNECT_TIMEOUT", "10"))
VALIDATION_CONCURRENCY = int(os.environ.get("VALIDATION_CONCURRENCY", "5"))

# --- Telethon network tunables (override via env) ---
TELETHON_REQUEST_RETRIES = int(os.environ.get("TELETHON_REQUEST_RETRIES", "7"))
TELETHON_CONNECTION_RETRIES = int(os.environ.get("TELETHON_CONNECTION_RETRIES", "6"))
TELETHON_RETRY_DELAY = int(os.environ.get("TELETHON_RETRY_DELAY", "3"))
TELETHON_TIMEOUT = int(os.environ.get("TELETHON_TIMEOUT", "45"))
TELETHON_FLOOD_SLEEP_THRESHOLD = int(os.environ.get("TELETHON_FLOOD_SLEEP_THRESHOLD", "60"))

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

# Fallback: если нет числовых флагов в таблице, шлём во ВСЕ каналы
if not SENT_FLAG_INDICES:
    SENT_FLAG_INDICES = [acc["index"] for acc in accounts]

def get_col_index(name: str):
    idx = HEADER_TO_COL.get(name)
    if not idx:
        print(f"ПРЕДУПРЕЖДЕНИЕ: не найден столбец '{name}' в заголовке таблицы.")
    return idx

# Часовой пояс для расписания (Армения)
tz = pytz.timezone("Asia/Yerevan")

# Настройка одного клиента Telegram (общий для всех каналов)
clients = []
common_proxy = GLOBAL_PROXY
client = TelegramClient(
    StringSession(TG_SESSION),
    TG_API_ID,
    TG_API_HASH,
    proxy=common_proxy,
    connection=tl_connection.ConnectionTcpAbridged,  # избегаем tcpfull
    request_retries=TELETHON_REQUEST_RETRIES,
    connection_retries=TELETHON_CONNECTION_RETRIES,
    retry_delay=TELETHON_RETRY_DELAY,
    timeout=TELETHON_TIMEOUT,
    flood_sleep_threshold=TELETHON_FLOOD_SLEEP_THRESHOLD,
)
clients.append(client)

# Удобные словари доступа по индексу (все индексы используют один и тот же клиент)
ACC_BY_INDEX = {acc["index"]: acc for acc in accounts}
CLIENT_BY_INDEX = {i: client for i in ACC_BY_INDEX.keys()}

# Runtime de-duplication guard: prevent repeating sends if Google Sheets flag update lags
SENT_RUNTIME = set()  # stores tuples of (row_idx, acc_idx)

# --- 3. ПОЛЬЗОВАТЕЛЬСКИЕ EMOJI И ПАРСЕР + ТИПОГРАФИКА ---

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

emoji_ids = {
    1: 5467538555158943525,   # 💭 (left)
    2: 5467538555158943525,   # 💭 (right)
    3: 5217822164362739968,   # 👑
    8: 5325547803936572038,   # ✨ (left)
    9: 5325547803936572038,   # ✨ (right)
    10: 5409048419211682843,  # 💵 (left)
    11: 5409048419211682843,  # 💵 (right)
    12: 5456140674028019486,  # ⚡️ (left)
    13: 5456140674028019486,  # ⚡️ (right)
    14: 5334998226636390258,  # 📱
    15: 5334544901428229844,  # ℹ️ (info)
}

emoji_placeholders = {
    1: "💭",  2: "💭",
    3: "👑",
    8: "✨",  9: "✨",
    10: "💵", 11: "💵",
    12: "⚡️", 13: "⚡️",
    14: "📱",
    15: "ℹ️",
}

# Custom set of emojis for the "Фото" checks line
FOTO_EMOJI_IDS = [
    5370853949358218655,
    5370674552869232639,
    5372943137415111238,
    5373338978780979795,
    5372991528811635071,
]

# Типографика короны
CROWN_OFFSET_ADJUST = int(os.environ.get("CROWN_OFFSET_ADJUST", "0"))
CROWN_OFFSET_SCALE = float(os.environ.get("CROWN_OFFSET_SCALE", "1.25"))
CROWN_THIN = "\u2009"  # тонкий пробел БЕЗ word-joiner — только для отступа короны
NNBSP = "\u202F"       # узкий неразрывный пробел
WORD_JOINER = "\u2060" # WORD JOINER
THIN  = "\u2009" + WORD_JOINER
CROWN_OFFSET_ADJUST = int(os.environ.get("CROWN_OFFSET_ADJUST", "0"))
CROWN_OFFSET_SCALE = float(os.environ.get("CROWN_OFFSET_SCALE", "1.25"))

# Pixel-based width helpers
CROWN_FONT_PATH = os.environ.get("CROWN_FONT_PATH", "")
CROWN_FONT_SIZE = int(os.environ.get("CROWN_FONT_SIZE", "18"))
CROWN_EM_WIDTH_SCALE = float(os.environ.get("CROWN_EM_WIDTH_SCALE", "1.0"))
CROWN_OFFSET_PX_ADJUST = int(os.environ.get("CROWN_OFFSET_PX_ADJUST", "0"))
CROWN_PRESET = os.environ.get("CROWN_PRESET", "").lower()  # e.g. "ios"
CROWN_FINE_TUNE = os.environ.get("CROWN_FINE_TUNE", "thin").lower()  # "thin" | "none"

if CROWN_PRESET == "ios":
    if "CROWN_EM_WIDTH_SCALE" not in os.environ:
        CROWN_EM_WIDTH_SCALE = 0.96
    if "CROWN_OFFSET_PX_ADJUST" not in os.environ:
        CROWN_OFFSET_PX_ADJUST = 1
_CROWN_FONT_SOURCE = None

def _load_crown_font():
    global _CROWN_FONT_SOURCE
    if not _PIL_AVAILABLE:
        return None
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
    except Exception:
        script_dir = os.getcwd()
    paths_to_try = [p for p in [CROWN_FONT_PATH] if p]
    paths_to_try += [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/dejavu/DejaVuSans.ttf",
        "/usr/local/share/fonts/DejaVuSans.ttf",
        os.path.join(script_dir, "fonts", "DejaVuSans.ttf"),
        os.path.join(script_dir, "fonts", "dejavu", "DejaVuSans.ttf"),
        "./fonts/DejaVuSans.ttf",
        "fonts/DejaVuSans.ttf",
    ]
    for d in [os.path.join(script_dir, "fonts"), "./fonts", "fonts"]:
        try:
            if os.path.isdir(d):
                for fname in os.listdir(d):
                    if fname.lower().endswith(".ttf"):
                        p = os.path.join(d, fname)
                        if p not in paths_to_try:
                            paths_to_try.append(p)
        except Exception:
            pass
    for p in paths_to_try:
        try:
            if p and os.path.exists(p):
                if CROWN_FONT_PATH and os.path.abspath(p) == os.path.abspath(CROWN_FONT_PATH):
                    _CROWN_FONT_SOURCE = f"env:{p}"
                elif os.path.abspath(p).startswith(os.path.abspath(script_dir)) or p.startswith("./") or p.startswith("fonts"):
                    _CROWN_FONT_SOURCE = f"local:{p}"
                else:
                    _CROWN_FONT_SOURCE = f"system:{p}"
                return ImageFont.truetype(p, CROWN_FONT_SIZE)
        except Exception:
            pass
    try:
        print("[CROWN] Не удалось найти валидный TTF. Проверенные пути:")
        for p in paths_to_try:
            print("   -", p)
    except Exception:
        pass
    return None

_CROWN_FONT = _load_crown_font()

if _PIL_AVAILABLE and _CROWN_FONT:
    if hasattr(_CROWN_FONT, "getlength"):
        _CROWN_MEASURE_MODE = "Pillow.getlength"
    else:
        _CROWN_MEASURE_MODE = "Pillow.getbbox"
    print(f"[CROWN] Pixel measurement ENABLED via Pillow ({_CROWN_MEASURE_MODE}); font={_CROWN_FONT_SOURCE}; size={CROWN_FONT_SIZE}")
else:
    print("ОШИБКА: пиксельное измерение для выравнивания короны недоступно (Pillow/шрифт не загружены). Требуется валидный TTF-шрифт (напр., DejaVuSans.ttf); загрузка ImageFont.load_default() не допускается. Установите шрифт или задайте CROWN_FONT_PATH. Без этого запуск запрещён.")
    sys.exit(1)

print(f"[CROWN] Preset={CROWN_PRESET or 'default'}, fine_tune={CROWN_FINE_TUNE}")

_tag_re = re.compile(r"<[^>]+>")
def _strip_tags(s: str) -> str:
    return _tag_re.sub("", str(s or "")).strip()

def _text_width_px(s: str) -> int:
    plain = str(s or "")
    if _PIL_AVAILABLE and _CROWN_FONT:
        try:
            if hasattr(_CROWN_FONT, "getlength"):
                return int(round(_CROWN_FONT.getlength(plain)))
            bbox = _CROWN_FONT.getbbox(plain)
            return max(0, int(bbox[2] - bbox[0]))
        except Exception:
            pass
    return int(round(len(plain) * 7))

def _space_width_px(ch: str = NNBSP) -> int:
    if _PIL_AVAILABLE and _CROWN_FONT:
        try:
            if hasattr(_CROWN_FONT, "getlength"):
                return max(1, int(round(_CROWN_FONT.getlength(ch))))
            bbox = _CROWN_FONT.getbbox(ch)
            return max(1, int(bbox[2] - bbox[0]))
        except Exception:
            pass
    return 7

def _plain_len(s: str) -> int:
    txt = re.sub(r"<[^>]+>", "", str(s or "")).strip()
    return len(txt)

def crown_over_name_lines(name: str, crown_html: str):
    name_plain = _strip_tags(name)
    name_px = _text_width_px(name_plain)
    em_px = _space_width_px("M")
    crown_px = max(1, int(round(em_px * CROWN_EM_WIDTH_SCALE)))
    offset_px = max(0, int(round(name_px / 2 - crown_px / 2)))
    offset_px += CROWN_OFFSET_PX_ADJUST
    nnbsp_px = _space_width_px(NNBSP)
    n_spaces = max(0, int(offset_px // max(1, nnbsp_px)))
    leftover_px = max(0, int(offset_px - n_spaces * max(1, nnbsp_px)))
    thin_count = 0
    if CROWN_FINE_TUNE == "thin":
        thin_px = _space_width_px(CROWN_THIN)
        if thin_px > 0:
            thin_count = int(round(leftover_px / thin_px))
            thin_count = max(0, min(thin_count, 8))
    n_spaces += max(0, CROWN_OFFSET_ADJUST)
    indent = (NNBSP * n_spaces) + (CROWN_THIN * thin_count)
    line1 = f"{indent}{crown_html}"
    line2 = f"<b><i>{name}</i></b>"
    return line1, line2

# --- 4. ФУНКЦИЯ ОТПРАВКИ ПОСТА ---

async def send_post(record, row_idx, pending_indices=None):
    """Собирает, форматирует и отправляет пост на основе строки из таблицы."""
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
    note_text = record.get("Примечание", "")
    nationality_raw = record.get("Национальность", "")
    nationality_flag = re.sub(r"\s+", "", str(nationality_raw or ""))

    # Требуемое количество рабочих медиа (из столбца "Количество"): пусто -> 4
    required_media_raw = record.get("Количество", "")
    try:
        required_media_count = int(str(required_media_raw).strip())
        if required_media_count < 1:
            required_media_count = 1
    except Exception:
        # Если значение пусто или некорректно — используем дефолт 4
        required_media_count = 4
    if str(required_media_raw).strip() == "":
        required_media_count = 4

    # Обязательное требование: без WhatsApp не публикуем
    if not (whatsapp_link and str(whatsapp_link).strip()):
        _notify_skip(row_idx, "Ячейка WhatsApp пуста. Публикация пропущена.")
        return

    # --- сбор строк внутри блоков ---
    param_lines = []
    if age and str(age).strip():    param_lines.append(f"Возраст: {age}")
    if height and str(height).strip(): param_lines.append(f"Рост: {height}")
    if weight and str(weight).strip(): param_lines.append(f"Вес: {weight}")
    if bust and str(bust).strip():  param_lines.append(f"Грудь: {bust}")

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
    if nationality_flag:
        line2 = f"{line2}{THIN}{nationality_flag}"
    blocks.append("\n".join([line1, line2]))
    # 3) Фото
    foto_checks = "".join(f'<a href="emoji/{eid}">✅</a>' for eid in FOTO_EMOJI_IDS)
    blocks.append(f'<b>Фото{THIN}{foto_checks}</b>')
    # 4) Услуги/Доп.услуги
    services_lines = []
    if services and str(services).strip():
        services_lines += ["Услуги:", f'<b><i>{services}</i></b>']
    if extra_services and str(extra_services).strip():
        if services_lines:
            services_lines.append("")
        services_lines += ["Доп. услуги:", f'<b><i>{extra_services}</i></b>']
    if services_lines:
        inner = "\n".join(services_lines)
        blocks.append(f"<blockquote>{inner}</blockquote>")
    # 5) Параметры
    if param_lines:
        params_header = (
            f'<a href="emoji/{emoji_ids[8]}">{emoji_placeholders[8]}</a>{THIN}'
            f'Параметры{THIN}'
            f'<a href="emoji/{emoji_ids[9]}">{emoji_placeholders[9]}</a>'
        )
        blocks.append(params_header + "\n" + '<b><i>' + "\n".join(param_lines) + '</i></b>')
    # 6) Цены
    def _fmt_price(val):
        try:
            num = float(str(val).replace(' ', '').replace(',', '.'))
            amount = int(round(num * 1000))  # значения трактуем как тысячи AMD
            return f"{format(amount, ',d').replace(',', '.')} AMD"
        except Exception:
            return f"{val} AMD"
    price_lines = []
    if express_price and str(express_price).strip(): price_lines.append(f"Express: {_fmt_price(express_price)}")
    if incall_price and str(incall_price).strip():  price_lines.append(f"Incall: {_fmt_price(incall_price)}")
    if outcall_price and str(outcall_price).strip(): price_lines.append(f"Outcall: {_fmt_price(outcall_price)}")
    if price_lines:
        price_header = (
            f'<a href="emoji/{emoji_ids[10]}">{emoji_placeholders[10]}</a>{THIN}'
            f'Цена{THIN}'
            f'<a href="emoji/{emoji_ids[11]}">{emoji_placeholders[11]}</a>'
        )
        blocks.append(price_header + "\n" + '<b><i>' + "\n".join(price_lines) + '</i></b>')
    # 6.5) Примечание
    if note_text and str(note_text).strip():
        note_header = (
            f'<a href="emoji/{emoji_ids[15]}">{emoji_placeholders[15]}</a>{THIN}'
            f'Примечание{THIN}'
            f'<a href="emoji/{emoji_ids[15]}">{emoji_placeholders[15]}</a>'
        )
        blocks.append(note_header + "\n" + '<b><i>' + str(note_text).strip() + '</i></b>')
    # 7) Призыв + контакты
    cta_and_contacts = [
        f'<a href="emoji/{emoji_ids[12]}">{emoji_placeholders[12]}</a>'
        f'{THIN}<b><i>Назначь встречу уже сегодня!</i></b>{THIN}'
        f'<a href="emoji/{emoji_ids[13]}">{emoji_placeholders[13]}</a>'
    ]
    if telegram_link and str(telegram_link).strip():
        cta_and_contacts.append(
            f'<a href="emoji/{emoji_ids[14]}">{emoji_placeholders[14]}</a>{THIN}'
            f'<a href="{telegram_link}"><b>Связь в Telegram</b></a>'
        )
    if whatsapp_link and str(whatsapp_link).strip():
        cta_and_contacts.append(
            f'<a href="emoji/{emoji_ids[14]}">{emoji_placeholders[14]}</a>{THIN}'
            f'<a href="{whatsapp_link}"><b>Связь в WhatsApp</b></a>'
        )
    blocks.append("\n".join(cta_and_contacts))

    message_html = "\n\n".join(blocks)

    # --- медиа ---
    media_column_headers = [f"Ссылка {i}" for i in range(1, 11)]
    media_urls = []
    for header in media_column_headers:
        url = record.get(header)
        if url and isinstance(url, str) and url.startswith("http"):
            media_urls.append(url)
    if not media_urls:
        _notify_skip(row_idx, "Нет ни одной ссылки на медиа. Публикация пропущена.")
        return
    if media_urls and len(set(media_urls)) != len(media_urls):
        _notify_skip(row_idx, "Обнаружены дубликаты ссылок на медиа. Публикация пропущена.")
        return

    print(f"Найдено {len(media_urls)} URL-адресов для строки {row_idx}.")
    media_data = []
    if media_urls:
        for url_idx, url in enumerate(media_urls, start=1):
            try:
                file_data, final_url, content_type = _download_with_ext_guess(url, row_idx, timeout=(5, 60))
                file_base = final_url.split("/")[-1].split("?")[0]
                if file_base and '.' in file_base:
                    file_name = file_base
                else:
                    if 'video' in content_type:
                        file_name = "video.mp4"
                    elif 'image' in content_type:
                        file_name = "image.jpg"
                    else:
                        _notify_media_issue(row_idx, f"Неподдерживаемый тип {content_type} для {final_url}. Пропускаю файл №{url_idx} и продолжаю.")
                        continue
                media_data.append((file_data, file_name))
            except Exception as e:
                _notify_media_issue(row_idx, f"Не удалось загрузить медиа {url} — {e}. Пропускаю файл №{url_idx} и продолжаю.")
                continue

    if not media_data:
        _notify_skip(row_idx, "Не удалось загрузить ни одно медиа. Публикация пропущена.")
        return
    # Требование по минимальному количеству рабочих медиа
    if len(media_data) < required_media_count:
        _notify_skip(row_idx, f"Загружено {len(media_data)} медиа, требуется минимум {required_media_count}. Публикация пропущена.")
        return
    if len(media_data) != len(media_urls):
        tg_notify(f"ℹ️ Строка {row_idx}: загрузил {len(media_data)}/{len(media_urls)} медиа, продолжаю с успешными.")

    # --- отправка ---
    if pending_indices is None:
        # Без числовых столбцов: отправляем во все каналы, кроме тех, где столбец есть и там TRUE
        target_indexes = [
            i for i, acc in ACC_BY_INDEX.items()
            if acc.get("channel") and str(record.get(str(i), record.get(i, ""))).upper() != "TRUE"
        ]
    else:
        target_indexes = [i for i in pending_indices if i in ACC_BY_INDEX and ACC_BY_INDEX[i].get("channel")]

    clients_with_channels = [(CLIENT_BY_INDEX[i], ACC_BY_INDEX[i]) for i in sorted(target_indexes)]

    async def _send_to_one(client, acc):
        channel_str = acc.get("channel")
        acc_idx = acc.get("index")
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
            try:
                channel = int(channel_str)
            except (ValueError, TypeError):
                channel = channel_str

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

            # отметить флаг, если столбец существует
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
            logging.error(f"TG{acc_idx} лимит Telegram: {e}", exc_info=True)
            return acc_idx, channel_str, False, f"rate: {e}"
        except Exception as e:
            logging.error(f"TG{acc_idx} ошибка отправки: {e}. Пробуем повтор...", exc_info=True)
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
                flag_name = str(acc_idx)
                col_idx = get_col_index(flag_name)
                if col_idx:
                    try:
                        worksheet.update_cell(row_idx, col_idx, "TRUE")
                    except Exception as e_upd:
                        print(f"ПРЕДУПРЕЖДЕНИЕ: не удалось обновить флаг {flag_name} (строка {row_idx}): {e_upд}")
                SENT_RUNTIME.add(rt_key)
                return acc_idx, channel_str, True, None
            except Exception as e2:
                logging.error(f"TG{acc_idx} повторная отправка не удалась: {e2}", exc_info=True)
                return acc_idx, channel_str, False, f"retry: {e2}"

    results = await asyncio.gather(
        *[_send_to_one(client, acc) for (client, acc) in clients_with_channels],
        return_exceptions=False
    )

    ok = sum(1 for (_, _, s, _) in results if s)
    fail = [(i, ch, err) for (i, ch, s, err) in results if not s]
    print(f"Строка {row_idx}: перс-отправки завершены. Успешно {ok}/{len(clients_with_channels)}. Неудачи: {fail}")

    if fail:
        tg_notify(f"❗️Строка {row_idx}: {ok}/{len(clients_with_channels)} успешно.\nПроблемы: {fail}")

    # Если была хотя бы одна успешная отправка — отмечаем глобальный флаг "Отправлено"
    if ok > 0:
        for fname in ("Отправлено", "отправлено"):
            col_idx = get_col_index(fname)
            if col_idx:
                try:
                    worksheet.update_cell(row_idx, col_idx, "TRUE")
                    break  # пометили любой из вариантов названия колонки
                except Exception as e_upd:
                    print(f"ПРЕДУПРЕЖДЕНИЕ: не удалось обновить глобальный флаг '{fname}' (строка {row_idx}): {e_upd}")

# --- 4.5. ПРЕДВАРИТЕЛЬНАЯ ПРОВЕРКА СЕССИЙ (без интерактива) ---

async def validate_sessions_before_start():
    """
    Проверяет, что общий StringSession авторизован. Короткие таймауты,
    чтобы не висеть на недоступной сети/прокси.
    """
    print("Проверка сессии Telegram... (один аккаунт)")
    client = clients[0]
    try:
        if not client.is_connected():
            await asyncio.wait_for(client.connect(), timeout=int(VALIDATION_CONNECT_TIMEOUT))
        authed = await asyncio.wait_for(client.is_user_authorized(), timeout=int(VALIDATION_AUTH_TIMEOUT))
        if not authed:
            logging.error("Сессия не авторизована. Проверь TG_SESSION.")
            exit(1)
        print("TG: OK")
    except asyncio.TimeoutError:
        logging.error(f"Timeout при проверке сессии ({VALIDATION_CONNECT_TIMEOUT + VALIDATION_AUTH_TIMEOUT}s)")
        exit(1)
    except Exception as e:
        logging.error(f"Ошибка проверки сессии: {e}")
        exit(1)
    finally:
        try:
            await asyncio.wait_for(client.disconnect(), timeout=int(VALIDATION_DISCONNECT_TIMEOUT))
        except Exception:
            pass

# --- 5. ГЛАВНЫЙ ЦИКЛ ПРОГРАММЫ ---

async def main():
    """Главная функция: подключается к клиентам и запускает бесконечный цикл проверки."""
    if not clients:
        print("ОШИБКА: Не настроен ни один Telegram клиент. Проверьте TG_API_ID/TG_API_HASH и TG_SESSION/TG{n}_CHANNEL.")
        return

    await validate_sessions_before_start()

    print("Подключение Telegram клиентов...")
    results = await asyncio.gather(*(c.start() for c in clients), return_exceptions=True)
    for idx, res in enumerate(results, start=1):
        if isinstance(res, Exception):
            print(f"ПРЕДУПРЕЖДЕНИЕ: клиент #{idx} не запустился: {res}")
    print("Клиенты успешно подключены. Запуск основного цикла...")
    tg_notify("🚀 telethon-постер запущен и следит за Google Sheets")

    while True:
        try:
            alive = sum(1 for c in clients if c.is_connected())
            # Proactive reconnect sweep
            seen = set()
            for acc_idx, client in CLIENT_BY_INDEX.items():
                key = id(client)
                if key in seen:
                    continue
                seen.add(key)
                if not client.is_connected():
                    try:
                        await client.connect()
                        print("TG переподключен.")
                    except Exception as e:
                        print(f"ПРЕДУПРЕЖДЕНИЕ: не удалось переподключить общий клиент: {e}")
            print(f"Активных клиентов: {alive}/{len(clients)}")
            print(f"Проверка таблицы... {datetime.now(tz).strftime('%H:%M:%S')}")
            records = worksheet.get_all_records()
            now = datetime.now(tz)

            for idx, record in enumerate(records, start=2):
                if not str(record.get("Имя", "")).strip():
                    continue

                # Глобальный флаг "Отправлено": если TRUE — пропускаем запись
                sent_flag = record.get("Отправлено", record.get("отправлено", ""))
                if str(sent_flag).strip().upper() == "TRUE":
                    continue

                # ВАЖНО: берём ВСЕ настроенные каналы
                active_idx = [acc["index"] for acc in accounts if acc.get("channel")]
                if not active_idx:
                    continue

                # Канал считается уже отправленным только если соответствующий столбец существует и там TRUE
                pending_idx = []
                for i in active_idx:
                    cell_val = record.get(str(i), record.get(i, ""))
                    if str(cell_val).upper() != "TRUE":
                        pending_idx.append(i)

                if not pending_idx:
                    continue
                print(f"Строка {idx}: каналы к отправке -> {pending_idx}")

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
            logging.error(f"ОШИБКА API Google Sheets: {e}. Повторная попытка через {REFRESH_SECONDS} сек.", exc_info=True)
            await asyncio.sleep(REFRESH_SECONDS)
        except Exception as e:
            logging.critical(f"КРИТИЧЕСКАЯ ОШИБКА в главном цикле: {e}", exc_info=True)
            await asyncio.sleep(REFRESH_SECONDS)

# --- 6. ЗАПУСК СКРИПТА ---

if __name__ == "__main__":
    asyncio.run(main())