#!/usr/bin/env python3
"""
recover_account.py (перепрофилирован на прослушивание входящих)

Что делает:
- Берёт одну или несколько StringSession из env.recover/.env.recover
  Поддерживаемые переменные:
    - TG_SESSION (одна)
    - TG_SESSIONS (несколько, через запятую и/или перенос строки)
    - TG_SESSION_1, TG_SESSION_2, ... (нумерованные)
    - TG_STRING_SESSION / TG_STRING_SESSIONS / TG_STRING_SESSION_1 (синонимы)
- Подключается ТОЛЬКО через прокси из того же файла env.
- Печатает в консоль все ВХОДЯЩИЕ сообщения по всем валидным сессиям.

Зависимости: telethon, python-dotenv, PySocks
pip install telethon python-dotenv PySocks

В env.recover/.env.recover должны быть:
  TG_API_ID=123456
  TG_API_HASH=0123456789abcdef0123456789abcdef
  TG_PROXY_TYPE=socks5
  TG_PROXY_HOST=eu.proxy.piaproxy.com
  TG_PROXY_PORT=6000
  TG_PROXY_USER=...
  TG_PROXY_PASS=...
  TG_PROXY_RDNS=true
  # и одна из форм сессий:
  # TG_SESSION=1AA...  или TG_SESSIONS=1AA...,2BB...  или TG_SESSION_1=...
"""

import os
import re
import sys
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

#
# Поиск файла env: поддерживаем явный путь через ENV_RECOVER_PATH,
# текущую папку, папку со скриптом и её родителей до 3 уровней.
ENV_PATH: str | None = None


def _candidate_env_paths() -> list[str]:
    paths: list[str] = []
    # 0) явный путь из переменной окружения
    explicit = os.getenv("ENV_RECOVER_PATH")
    if explicit:
        paths.append(os.path.abspath(explicit))

    # 1) текущая рабочая директория
    for name in ("env.recover", ".env.recover"):
        paths.append(os.path.abspath(name))

    # 2) директория скрипта и до 3 родителей
    base = os.path.abspath(os.path.dirname(__file__))
    cur = base
    for _ in range(4):  # base + 3 родителя
        for name in ("env.recover", ".env.recover"):
            paths.append(os.path.join(cur, name))
        parent = os.path.dirname(cur)
        if parent == cur:
            break
        cur = parent

    # Удаляем дубли, нормализуем
    seen = set()
    uniq: list[str] = []
    for p in paths:
        n = os.path.normpath(p)
        if n not in seen:
            uniq.append(n)
            seen.add(n)
    return uniq


def load_env_or_fail() -> None:
    """Ищет env в нескольких местах и грузит первый найденный."""
    global ENV_PATH
    searched = _candidate_env_paths()
    for p in searched:
        if os.path.exists(p) and load_dotenv(p):
            ENV_PATH = p
            return
    # fallback: вдруг файл есть, но os.path.exists() не сработал
    for p in searched:
        if load_dotenv(p):
            ENV_PATH = p
            return
    msg = [
        "Не найден файл переменных окружения (env.recover или .env.recover).",
        "Искомые пути:",
    ] + [f" - {p}" for p in searched]
    raise RuntimeError("\n".join(msg))


def strtobool(s: str | None, default: bool = True) -> bool:
    if s is None:
        return default
    return s.strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def build_proxy_from_env():
    """Кортеж прокси для Telethon/PySocks: (ptype, host, port, rdns[, user, pass])"""
    ptype = os.getenv("TG_PROXY_TYPE")
    host = os.getenv("TG_PROXY_HOST")
    port = os.getenv("TG_PROXY_PORT")
    user = os.getenv("TG_PROXY_USER")
    pwd = os.getenv("TG_PROXY_PASS")
    rdns = strtobool(os.getenv("TG_PROXY_RDNS"), default=True)

    if not all([ptype, host, port]):
        raise RuntimeError(
            f"В {ENV_PATH or 'env.recover/.env.recover'} должны быть TG_PROXY_TYPE, TG_PROXY_HOST, TG_PROXY_PORT."
        )

    pmap = {"socks5": socks.SOCKS5, "socks4": socks.SOCKS4, "http": socks.HTTP}
    if ptype.lower() not in pmap:
        raise RuntimeError(f"Неподдерживаемый TG_PROXY_TYPE: {ptype}. Разрешены: socks5/socks4/http")

    try:
        port_i = int(port)
    except ValueError:
        raise RuntimeError("TG_PROXY_PORT должен быть числом.")

    base = (pmap[ptype.lower()], host, port_i, rdns)
    if user and pwd:
        return base + (user, pwd)
    return base


def get_api_credentials():
    api_id = os.getenv("TG_API_ID")
    api_hash = os.getenv("TG_API_HASH")
    if not api_id or not api_hash:
        raise RuntimeError(
            f"Добавьте TG_API_ID и TG_API_HASH в {ENV_PATH or 'env.recover/.env.recover'} — они требуются Telethon."
        )
    try:
        api_id = int(api_id)
    except ValueError:
        raise RuntimeError("TG_API_ID должен быть целым числом.")
    return api_id, api_hash


def now_iso() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def read_string_sessions_from_env() -> list[str]:
    """
    Читает StringSession из env.
    Поддерживает:
      - TG_SESSION
      - TG_SESSIONS (через запятую/переносы строк)
      - TG_SESSION_<N>
      - TG_STRING_SESSION / TG_STRING_SESSIONS / TG_STRING_SESSION_<N>
    """
    sessions: list[str] = []

    def _add_single(key: str):
        v = os.getenv(key)
        if v:
            v = v.strip()
            if v:
                sessions.append(v)

    # одиночные
    for key in ("TG_SESSION", "TG_STRING_SESSION"):
        _add_single(key)

    # мульти-блоки
    for key in ("TG_SESSIONS", "TG_STRING_SESSIONS"):
        block = os.getenv(key)
        if block:
            parts = re.split(r"[,\n\r]+", block)
            sessions.extend([p.strip() for p in parts if p and p.strip()])

    # нумерованные
    numbered = []
    for k, v in os.environ.items():
        m = re.fullmatch(r"TG_(?:STRING_)?SESSION_(\d+)", k)
        if m:
            idx = int(m.group(1))
            numbered.append((idx, (v or "").strip()))
    for _, v in sorted(numbered, key=lambda t: t[0]):
        if v:
            sessions.append(v)

    # уникализируем
    seen = set()
    uniq: list[str] = []
    for s in sessions:
        if s and s not in seen:
            uniq.append(s)
            seen.add(s)

    if not uniq:
        raise RuntimeError(
            "Не найдено ни одной StringSession. Добавьте TG_SESSION/TG_SESSIONS/TG_SESSION_1 (или TG_STRING_*) в env."
        )
    return uniq


def fmt_chat_sender(event) -> tuple[str, str]:
    """Возвращает (origin, text) для печати."""
    origin = "Unknown"
    text = event.raw_text.strip() if getattr(event, "raw_text", None) else ""
    return origin, (text or "<сообщение без текста / медиа>")


def make_handler(account_label: str):
    async def on_new_message(event):
        # источник (чат/пользователь)
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


async def run():
    load_env_or_fail()
    print(f"[{now_iso()}] Загружаю переменные окружения из: {ENV_PATH}")
    proxy = build_proxy_from_env()  # работаем только через прокси
    api_id, api_hash = get_api_credentials()
    sessions = read_string_sessions_from_env()

    clients: list[TelegramClient] = []
    for s in sessions:
        client = TelegramClient(
            StringSession(s),
            api_id,
            api_hash,
            proxy=proxy,
            device_model="Telethon Listener",
            system_version="Python",
            app_version="1.0",
            lang_code="ru",
            system_lang_code="ru-RU",
        )
        await client.connect()
        if not await client.is_user_authorized():
            print("❌ Одна из StringSession не авторизована. Пропускаю её.", file=sys.stderr)
            await client.disconnect()
            continue
        me = await client.get_me()
        label = (me.username or me.first_name or str(me.id))
        client.add_event_handler(make_handler(label), events.NewMessage(incoming=True))
        clients.append(client)
        print(f"✅ Аккаунт готов: {label} (id={me.id})", flush=True)

    if not clients:
        print("Нет ни одного валидного аккаунта для прослушивания. Завершение.", file=sys.stderr)
        sys.exit(1)

    print("▶ Слушаю входящие по всем активным аккаунтам. Нажмите Ctrl+C для выхода.")
    try:
        await asyncio.gather(*[c.run_until_disconnected() for c in clients])
    finally:
        await asyncio.gather(*[c.disconnect() for c in clients], return_exceptions=True)


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