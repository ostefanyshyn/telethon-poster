import csv
import asyncio
import time
from pathlib import Path
from datetime import datetime
from dotenv import dotenv_values
from telethon import TelegramClient
from telethon.errors import ChannelPrivateError, FloodWaitError
from telethon.sessions import StringSession
from telethon.tl.functions.channels import GetParticipantsRequest
from telethon.tl.types import (
    PeerChannel,
    UserStatusOnline,
    UserStatusOffline,
    UserStatusRecently,
    UserStatusLastWeek,
    UserStatusLastMonth,
    ChannelParticipantsSearch,
)

ENV_PATH = Path(__file__).with_name(".env.export")
env = dotenv_values(ENV_PATH)


def require_env(key: str) -> str:
    val = env.get(key)
    if not val:
        raise RuntimeError(f"Отсутствует переменная окружения {key} в {ENV_PATH}")
    return val

API_ID = int(require_env("TG_API_ID"))
API_HASH = require_env("TG_API_HASH")
CHANNEL_RAW = require_env("TG_CHANNEL")  # может быть username (@name) или id (-100...)
SESSION_STR = require_env("TG_SESSION")   # строковая сессия Telethon
OUTPUT_CSV = env.get("TG_OUTPUT_CSV", "channel_subscribers.csv")
SPLIT_DELETED = env.get("TG_SPLIT_DELETED", "false").lower() in {"1", "true", "yes"}

# Поведение при FloodWait
SLEEP_ON_FLOOD = env.get("SLEEP_ON_FLOOD", "true").lower() in {"1", "true", "yes"}

# Клиент с использованием строковой сессии
client = TelegramClient(StringSession(SESSION_STR), API_ID, API_HASH)


def status_to_str(st) -> str:
    """Приводим статус пользователя к удобной строке."""
    if not st:
        return "unknown"
    if isinstance(st, UserStatusOnline):
        return "online"
    if isinstance(st, UserStatusOffline):
        dt = getattr(st, "was_online", None)
        return f"offline (was {dt.isoformat() if isinstance(dt, datetime) else dt})"
    if isinstance(st, UserStatusRecently):
        return "recently"
    if isinstance(st, UserStatusLastWeek):
        return "last_week"
    if isinstance(st, UserStatusLastMonth):
        return "last_month"
    return type(st).__name__


async def resolve_channel(value: str):
    """Преобразуем строку из .env в entity для Telethon.
    Поддерживает username (@name) и числовой id формата -100XXXXXXXXXX.
    """
    v = (value or "").strip()
    # Если это привычный "чат id" с префиксом -100... — выделяем внутренний id канала
    if v.startswith("-100") and v[4:].isdigit():
        return PeerChannel(int(v[4:]))
    # Если просто число (на всякий случай), пробуем как PeerChannel
    if v.lstrip("-").isdigit():
        return PeerChannel(int(v.lstrip("-")))
    # Иначе это username/ссылка
    return v


def user_to_row(u) -> dict:
    """Максимально полный набор безопасных полей из User для CSV."""
    restriction_reason = getattr(u, "restriction_reason", None) or []
    try:
        restriction_text = "; ".join(getattr(r, "text", "") for r in restriction_reason) if restriction_reason else ""
    except Exception:
        restriction_text = str(restriction_reason)

    return {
        "user_id": getattr(u, "id", None),
        "access_hash": getattr(u, "access_hash", None),
        "username": getattr(u, "username", "") or "",
        "phone": getattr(u, "phone", "") or "",
        "first_name": getattr(u, "first_name", "") or "",
        "last_name": getattr(u, "last_name", "") or "",
        "is_self": bool(getattr(u, "self", False)),
        "mutual_contact": bool(getattr(u, "mutual_contact", False)),
        "deleted": bool(getattr(u, "deleted", False)),
        "bot": bool(getattr(u, "bot", False)),
        "scam": bool(getattr(u, "scam", False)),
        "fake": bool(getattr(u, "fake", False)),
        "restricted": bool(getattr(u, "restricted", False)),
        "verified": bool(getattr(u, "verified", False)),
        "premium": bool(getattr(u, "premium", False)),
        "lang_code": getattr(u, "lang_code", "") or getattr(u, "language_code", "") or "",
        "status": status_to_str(getattr(u, "status", None)),
        "restriction_reason": restriction_text,
    }


# Универсальный sweep по поиску участников
async def sweep_participants(channel, writer_active, writer_deleted, split_deleted=False):
    """Собираем участников серией поисковых запросов (до 200 на каждый запрос)
    с дедупликацией по user_id. Работает и для broadcast-каналов, и для супергрупп.
    Если split_deleted=True — пишем в разные CSV.
    """
    seen = set()
    count = 0
    active_count = 0
    deleted_count = 0
    # Набор коротких запросов: пустая строка + латиница + кириллица + цифры
    queries = [''] \
        + list('abcdefghijklmnopqrstuvwxyz') \
        + list('абвгдеёжзийклмнопрстуфхцчшщыьэюя') \
        + [str(d) for d in range(10)]

    for q in queries:
        offset = 0
        while True:
            try:
                res = await client(GetParticipantsRequest(
                    channel,
                    ChannelParticipantsSearch(q),
                    offset,
                    200,
                    0
                ))
            except FloodWaitError as e:
                seconds = int(getattr(e, 'seconds', 0)) or 0
                if SLEEP_ON_FLOOD and seconds > 0:
                    print(f"Flood wait на запросе '{q}': пауза {seconds} сек...")
                    time.sleep(seconds)
                    continue
                else:
                    raise

            if not res.users:
                break

            new = 0
            for u in res.users:
                if u.id in seen:
                    continue
                seen.add(u.id)
                row = user_to_row(u)
                if split_deleted and row.get("deleted"):
                    if writer_deleted:
                        writer_deleted.writerow(row)
                    deleted_count += 1
                else:
                    writer_active.writerow(row)
                    active_count += 1
                count += 1
                new += 1

            offset += len(res.users)
            # Небольшая пауза между страницами
            await asyncio.sleep(0.3)

            # Если по этому запросу больше новых не находим — переходим к следующему
            if new == 0 and offset > 0:
                break

    return count, active_count, deleted_count


async def export_participants():
    await client.start()
    me = await client.get_me()
    print("Signed in as:", getattr(me, "username", None) or f"{me.first_name} {me.last_name or ''}".strip())

    # Разрешаем entity канала
    entity_hint = await resolve_channel(CHANNEL_RAW)
    try:
        channel = await client.get_entity(entity_hint)
    except Exception as e:
        print(f"Не удалось найти канал по TG_CHANNEL='{CHANNEL_RAW}': {e}")
        await client.disconnect()
        return

    # Подготавливаем CSV
    fieldnames = list(user_to_row(type("U", (), {})()).keys())  # аккуратно формируем заголовки
    # Фикс: выше будет много None — поэтому лучше руками задать порядок полей
    fieldnames = [
        "user_id",
        "access_hash",
        "username",
        "phone",
        "first_name",
        "last_name",
        "is_self",
        "mutual_contact",
        "deleted",
        "bot",
        "scam",
        "fake",
        "restricted",
        "verified",
        "premium",
        "lang_code",
        "status",
        "restriction_reason",
    ]

    try:
        if SPLIT_DELETED:
            prefix = OUTPUT_CSV[:-4] if OUTPUT_CSV.lower().endswith(".csv") else OUTPUT_CSV
            active_csv = f"{prefix}_active.csv"
            deleted_csv = f"{prefix}_deleted.csv"
            with open(active_csv, "w", newline="", encoding="utf-8") as fa, \
                 open(deleted_csv, "w", newline="", encoding="utf-8") as fd:
                writer_a = csv.DictWriter(fa, fieldnames=fieldnames)
                writer_d = csv.DictWriter(fd, fieldnames=fieldnames)
                writer_a.writeheader()
                writer_d.writeheader()
                total, active_count, deleted_count = await sweep_participants(channel, writer_a, writer_d, split_deleted=True)
            print(f"Готово: всего {total} пользователей. Активные: {active_count} → {active_csv}; deleted: {deleted_count} → {deleted_csv}")
        else:
            with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                total, active_count, deleted_count = await sweep_participants(channel, writer, None, split_deleted=False)
            print(f"Готово: выгружено {total} пользователей в {OUTPUT_CSV} (из них deleted: {deleted_count})")

    except FloodWaitError as e:
        # Telegram просит подождать
        seconds = int(getattr(e, "seconds", 0)) or 0
        if SLEEP_ON_FLOOD and seconds > 0:
            print(f"Flood wait: пауза {seconds} сек...")
            time.sleep(seconds)
            print("Завершено после ожидания. Перезапустите скрипт, чтобы продолжить/повторить.")
        else:
            print(f"Flood wait: нужно подождать {getattr(e, 'seconds', '?')} сек. Завершаю.")
    except ChannelPrivateError:
        print("Ошибка: канал приватный или у вашего аккаунта нет прав читать участников.")
    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(export_participants())