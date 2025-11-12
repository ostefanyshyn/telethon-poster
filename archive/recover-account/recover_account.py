#!/usr/bin/env python3
"""
recover_account.py — выбор аккаунта по цифре и прослушивание входящих

Функции:
- Ищет env-файл (env.recover или .env.recover) в ряде локаций или по ENV_RECOVER_PATH
- Собирает пары (номер N) -> (TG{N}_SESSION + TG{N}_PROXY_*)
- Показывает интерактивное меню с найденными номерами N
- По выбранному номеру N подключается ТОЛЬКО через соответствующий прокси TG{N}_PROXY_*
- Слушает и печатает все входящие сообщения для выбранной сессии

Зависимости: telethon, python-dotenv, PySocks
pip install telethon python-dotenv PySocks

Обязательные переменные в env.recover/.env.recover:
  TG_API_ID=...
  TG_API_HASH=...
  # для каждого аккаунта N:
  TG{N}_SESSION=...
  TG{N}_PROXY_TYPE=socks5|socks4|http
  TG{N}_PROXY_HOST=...
  TG{N}_PROXY_PORT=...
  TG{N}_PROXY_USER=...
  TG{N}_PROXY_PASS=...
  TG{N}_PROXY_RDNS=true|false

(Поддерживаются также TG_SESSION/TG_SESSIONS/TG_SESSION_1 и TG_STRING_*; 
для них можно выбрать номер вручную через параметр --n.)
"""

import os
import re
import sys
import argparse
import asyncio
from datetime import datetime

from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.sessions import StringSession

try:
    import socks  # PySocks
except Exception:
    print("Требуется пакет PySocks: pip install PySocks", file=sys.stderr)
    raise

# ============= Вспомогательные утилиты =============

def now_iso() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def strtobool(s: str | None, default: bool = True) -> bool:
    if s is None:
        return default
    return s.strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def mask_middle(s: str, keep: int = 6) -> str:
    if not s:
        return ""
    if len(s) <= keep * 2:
        return s
    return f"{s[:keep]}…{s[-keep:]}"


# ============= Загрузка env =============
ENV_PATH: str | None = None


def _candidate_env_paths() -> list[str]:
    paths: list[str] = []
    explicit = os.getenv("ENV_RECOVER_PATH")
    if explicit:
        paths.append(os.path.abspath(explicit))
    for name in ("env.recover", ".env.recover"):
        paths.append(os.path.abspath(name))
    base = os.path.abspath(os.path.dirname(__file__))
    cur = base
    for _ in range(4):  # base + 3 родителя
        for name in ("env.recover", ".env.recover"):
            paths.append(os.path.join(cur, name))
        parent = os.path.dirname(cur)
        if parent == cur:
            break
        cur = parent
    # Уникализируем
    seen = set()
    uniq: list[str] = []
    for p in paths:
        n = os.path.normpath(p)
        if n not in seen:
            uniq.append(n)
            seen.add(n)
    return uniq


def load_env_or_fail() -> None:
    global ENV_PATH
    searched = _candidate_env_paths()
    for p in searched:
        if os.path.exists(p) and load_dotenv(p):
            ENV_PATH = p
            return
    for p in searched:  # fallback
        if load_dotenv(p):
            ENV_PATH = p
            return
    msg = [
        "Не найден файл переменных окружения (env.recover или .env.recover).",
        "Искомые пути:",
    ] + [f" - {p}" for p in searched]
    raise RuntimeError("\n".join(msg))


# ============= Чтение API и прокси/сессий =============

def get_api_credentials() -> tuple[int, str]:
    api_id = os.getenv("TG_API_ID")
    api_hash = os.getenv("TG_API_HASH")
    if not api_id or not api_hash:
        raise RuntimeError(
            f"Добавьте TG_API_ID и TG_API_HASH в {ENV_PATH or 'env.recover/.env.recover'} — они требуются Telethon."
        )
    try:
        return int(api_id), api_hash
    except ValueError:
        raise RuntimeError("TG_API_ID должен быть целым числом.")


class ProxySpec:
    def __init__(self, n: int, ptype: str, host: str, port: int, rdns: bool, user: str | None, pwd: str | None):
        self.n = n
        self.ptype = ptype
        self.host = host
        self.port = port
        self.rdns = rdns
        self.user = user
        self.pwd = pwd

    def to_pysocks(self):
        import socks
        pmap = {"socks5": socks.SOCKS5, "socks4": socks.SOCKS4, "http": socks.HTTP}
        if self.ptype.lower() not in pmap:
            raise RuntimeError(f"Неподдерживаемый TG{self.n}_PROXY_TYPE: {self.ptype}")
        base = (pmap[self.ptype.lower()], self.host, self.port, self.rdns)
        if self.user and self.pwd:
            return base + (self.user, self.pwd)
        return base

    def short(self) -> str:
        u = self.user or ""
        return f"{self.ptype}://{self.host}:{self.port} ({mask_middle(u, 8)})"


def read_numbered_sessions() -> dict[int, str]:
    out: dict[int, str] = {}
    for k, v in os.environ.items():
        m = re.fullmatch(r"TG(\d+)_SESSION", k)
        if m and v and v.strip():
            out[int(m.group(1))] = v.strip()
    return dict(sorted(out.items(), key=lambda kv: kv[0]))


def read_numbered_proxy(n: int) -> ProxySpec:
    def need(key: str) -> str:
        val = os.getenv(key)
        if not val:
            raise RuntimeError(
                f"Отсутствует {key}. Для выбранного номера {n} обязательно задайте TG{n}_PROXY_TYPE/HOST/PORT [+ USER/PASS при необходимости]."
            )
        return val

    ptype = need(f"TG{n}_PROXY_TYPE")
    host = need(f"TG{n}_PROXY_HOST")
    port_s = need(f"TG{n}_PROXY_PORT")
    try:
        port = int(port_s)
    except ValueError:
        raise RuntimeError(f"TG{n}_PROXY_PORT должен быть числом, а не '{port_s}'.")
    user = os.getenv(f"TG{n}_PROXY_USER")
    pwd = os.getenv(f"TG{n}_PROXY_PASS")
    rdns = strtobool(os.getenv(f"TG{n}_PROXY_RDNS"), True)
    return ProxySpec(n, ptype, host, port, rdns, user, pwd)


# Дополнительная поддержка не-нумерованных сессий (опция --n для ручного выбора)

def read_legacy_sessions() -> list[str]:
    sessions: list[str] = []
    single = os.getenv("TG_SESSION") or os.getenv("TG_STRING_SESSION")
    if single:
        sessions.append(single.strip())
    multi = os.getenv("TG_SESSIONS") or os.getenv("TG_STRING_SESSIONS")
    if multi:
        parts = re.split(r"[,\n\r]+", multi)
        sessions.extend([p.strip() for p in parts if p and p.strip()])
    # нумерованные альтернативной схемой
    alt = []
    for k, v in os.environ.items():
        m = re.fullmatch(r"TG_(?:STRING_)?SESSION_(\d+)", k)
        if m and v and v.strip():
            alt.append((int(m.group(1)), v.strip()))
    alt = [s for _, s in sorted(alt, key=lambda t: t[0])]
    sessions.extend(alt)
    # уникализируем
    seen = set(); uniq = []
    for s in sessions:
        if s and s not in seen:
            uniq.append(s); seen.add(s)
    return uniq


# ============= Логика приложения =============

def make_handler(account_label: str):
    async def on_new_message(event):
        origin = "Unknown"
        try:
            chat = await event.get_chat()
            sender = await event.get_sender()
            chat_title = getattr(chat, "title", None)
            first = getattr(sender, "first_name", None) or ""
            last = getattr(sender, "last_name", None) or ""
            username = getattr(sender, "username", None)
            sender_name = (first + (" " + last if last else "")).strip() or (f"@{username}" if username else None)
            origin = chat_title or sender_name or origin
        except Exception:
            pass
        text = event.raw_text.strip() if getattr(event, "raw_text", None) else ""
        if not text:
            text = "<сообщение без текста / медиа>"
        print(f"[{now_iso()}] [acct:{account_label}] [{origin}] {text}", flush=True)
    return on_new_message


async def listen_once(n: int, session: str, proxy: ProxySpec, api_id: int, api_hash: str):
    client = TelegramClient(
        StringSession(session),
        api_id,
        api_hash,
        proxy=proxy.to_pysocks(),
        device_model="Telethon Listener",
        system_version="Python",
        app_version="1.0",
        lang_code="ru",
        system_lang_code="ru-RU",
    )
    await client.connect()
    if not await client.is_user_authorized():
        raise RuntimeError("Эта StringSession не авторизована (или утратила авторизацию).")
    me = await client.get_me()
    label = (me.username or me.first_name or str(me.id))
    client.add_event_handler(make_handler(label), events.NewMessage(incoming=True))
    print(f"✅ Выбран аккаунт [{n}]: {label} (id={me.id}) через {proxy.short()}")
    print("▶ Слушаю входящие. Нажмите Ctrl+C для выхода.")
    try:
        await client.run_until_disconnected()
    finally:
        await client.disconnect()


def choose_number_interactive(candidates: dict[int, str], default: int | None = None) -> int:
    print("Доступные аккаунты (номер -> краткая информация):")
    for n, sess in candidates.items():
        print(f"  {n}: session {mask_middle(sess)}; proxy TG{n}_PROXY_*")
    if default is not None and default in candidates:
        prompt = f"Выберите номер [по умолчанию {default}]: "
    else:
        prompt = "Выберите номер: "

    while True:
        choice = input(prompt).strip()
        if not choice and default is not None and default in candidates:
            return default
        if choice.isdigit():
            val = int(choice)
            if val in candidates:
                return val
        print("Некорректный выбор. Введите одну из цифр из списка.")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Прослушивание входящих по StringSession через выбранный прокси")
    p.add_argument("--n", type=int, help="Номер аккаунта TG{N}_* для использования (обходит меню)")
    return p.parse_args()


async def run():
    load_env_or_fail()
    print(f"[{now_iso()}] Загружаю переменные окружения из: {ENV_PATH}")
    api_id, api_hash = get_api_credentials()

    numbered_sessions = read_numbered_sessions()
    args = parse_args()

    if not numbered_sessions:
        # Нет TG{N}_SESSION, но возможно есть TG_SESSION/TG_SESSIONS
        legacy = read_legacy_sessions()
        if not legacy:
            raise RuntimeError("Не найдено ни одной StringSession (ни TG{N}_SESSION, ни TG_SESSION/TG_SESSIONS)")
        if args.n is None:
            print("Найдены только не-нумерованные сессии. Используйте --n для фиксации номера прокси (например, 2), либо задайте TG{N}_SESSION.")
            n = int(input("Введите номер N для прокси TG{N}_PROXY_*: ").strip())
        else:
            n = args.n
        if not legacy:
            raise RuntimeError("Нет сессий для подключения.")
        # берём первую из legacy (или можно расширить до выбора)
        session = legacy[0]
        proxy = read_numbered_proxy(n)
        await listen_once(n, session, proxy, api_id, api_hash)
        return

    # Есть нумерованные сессии TG{N}_SESSION
    if args.n is not None:
        if args.n not in numbered_sessions:
            raise RuntimeError(f"Номер {args.n} не найден среди: {list(numbered_sessions.keys())}")
        n = args.n
    else:
        # интерактивный выбор
        # если только один — выбираем его
        if len(numbered_sessions) == 1:
            n = next(iter(numbered_sessions.keys()))
            print(f"Найден единственный аккаунт TG{n}_SESSION — выбираю его автоматически.")
        else:
            n = choose_number_interactive(numbered_sessions)

    session = numbered_sessions[n]
    proxy = read_numbered_proxy(n)
    await listen_once(n, session, proxy, api_id, api_hash)


def main():
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("\nЗавершено по Ctrl+C.")
    except Exception as e:
        print(f"Ошибка: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()