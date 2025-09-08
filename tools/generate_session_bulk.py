#!/usr/bin/env python3
"""
Генератор Telegram StringSession для нескольких аккаунтов на базе переменных из .env.gen.

Особенности:
- Автоматически находит аккаунты TG{N}_* в .env.gen (N = 1..∞).
- Поддерживает прокси (SOCKS4/SOCKS5/HTTP) с RDNS и логином/паролем.
- Обрабатывает 2FA (two‑step) — берёт TG_PASSWORD из .env.gen, либо попросит ввести.
- Создаёт строковые сессии и сохраняет их в файл `sessions/sessions.txt`.
- Всегда перегенерирует сессии; существующие TG{N}_SESSION игнорируются.

Зависимости:
    pip install telethon pysocks
(скрипт не требует python-dotenv — .env.gen парсится самостоятельно)

Примеры запуска:
    python tools/generate_session_bulk --env .env.gen
    python tools/generate_session_bulk --only 1,3,7
"""
from __future__ import annotations

import argparse
import asyncio
import os
import re
import sys
from dataclasses import dataclass
from getpass import getpass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from telethon import TelegramClient, errors
from telethon.sessions import StringSession

try:
    import socks  # type: ignore
except Exception as e:
    print("[!] Требуется библиотека PySocks: pip install pysocks", file=sys.stderr)
    raise


# ----------------------------- утилиты парсинга -----------------------------
ENV_LINE_RE = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)\s*$")
PREFIX_RE = re.compile(r"^TG(\d+)_", re.IGNORECASE)


def parse_env_file(path: Path) -> Dict[str, str]:
    """Простой парсер .env без зависимостей. Поддерживает строки KEY=VALUE.
    Пробелы вокруг '=' игнорируются. Кавычки вокруг VALUE — опциональны.
    Комментарии (# ...) и пустые строки пропускаются.
    """
    env: Dict[str, str] = {}
    if not path.exists():
        raise FileNotFoundError(f"Не найден файл: {path}")

    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith('#'):
            continue
        m = ENV_LINE_RE.match(line)
        if not m:
            continue
        key, val = m.group(1), m.group(2)
        # срежем возможные кавычки
        if (val.startswith('"') and val.endswith('"')) or (val.startswith("'") and val.endswith("'")):
            val = val[1:-1]
        env[key] = val
    return env


def get_any(env: Dict[str, str], candidates: Iterable[str]) -> Optional[str]:
    """Получить значение по первому совпавшему ключу (без учёта регистра)."""
    lower_env = {k.lower(): v for k, v in env.items()}
    for key in candidates:
        v = lower_env.get(key.lower())
        if v is not None and v != "":
            return v
    return None


def to_bool(s: Optional[str], default: bool = False) -> bool:
    if s is None:
        return default
    return s.strip().lower() in {"1", "true", "yes", "y", "on"}


# ----------------------------- модель аккаунта ------------------------------
@dataclass
class Account:
    idx: int
    api_id: int
    api_hash: str
    phone: Optional[str]
    session_str: Optional[str]
    proxy: Optional[Tuple]


# --------------------------- сборка аккаунтов -------------------------------

def discover_accounts(env: Dict[str, str], only: Optional[List[int]] = None) -> List[Account]:
    # Собираем все индексы TG{N}_*
    indices: set[int] = set()
    for key in env.keys():
        m = PREFIX_RE.match(key)
        if m:
            indices.add(int(m.group(1)))

    if only:
        indices = {i for i in indices if i in set(only)}

    accounts: List[Account] = []
    for i in sorted(indices):
        # Ключи могут отличаться регистром (например, TG4_Phone)
        api_id_s = get_any(env, [f"TG{i}_API_ID"]) or ""
        api_hash = get_any(env, [f"TG{i}_API_HASH"]) or ""
        phone = get_any(env, [f"TG{i}_PHONE", f"TG{i}_Phone"])  # опционально
        session_str = get_any(env, [f"TG{i}_SESSION"])  # опционально

        if not api_id_s or not api_hash:
            print(f"[i] Пропуск TG{i}: отсутствует API_ID/API_HASH")
            continue

        # Прокси — все поля опциональны
        p_type = (get_any(env, [f"TG{i}_PROXY_TYPE"]) or "").lower()
        p_host = get_any(env, [f"TG{i}_PROXY_HOST"]) or ""
        p_port_s = get_any(env, [f"TG{i}_PROXY_PORT"]) or ""
        p_user = get_any(env, [f"TG{i}_PROXY_USER"]) or None
        p_pass = get_any(env, [f"TG{i}_PROXY_PASS"]) or None
        p_rdns = to_bool(get_any(env, [f"TG{i}_PROXY_RDNS"]), True)

        proxy = None
        if p_type and p_host and p_port_s.isdigit():
            port = int(p_port_s)
            if p_type in {"socks5", "socks5h"}:
                proxy = (socks.SOCKS5, p_host, port, p_rdns, p_user, p_pass)
            elif p_type in {"socks4", "socks4a"}:
                proxy = (socks.SOCKS4, p_host, port, p_rdns, p_user, p_pass)
            elif p_type in {"http", "https"}:
                # RDNS не используется для HTTP-прокси, но Telethon допускает кортеж из 5-6 элементов
                proxy = (socks.HTTP, p_host, port, p_rdns, p_user, p_pass)
            else:
                print(f"[!] TG{i}: неизвестный тип прокси '{p_type}', прокси отключён")

        # Требуем наличие прокси: без прокси скрипт работать не должен
        if proxy is None:
            print(f"[!] TG{i}: прокси не задан — прямое соединение запрещено. Укажите TG{i}_PROXY_* переменные.")
            continue

        try:
            api_id = int(api_id_s)
        except ValueError:
            print(f"[!] TG{i}: некорректный API_ID '{api_id_s}', пропуск")
            continue

        accounts.append(Account(
            idx=i,
            api_id=api_id,
            api_hash=api_hash,
            phone=phone,
            session_str=session_str,
            proxy=proxy,
        ))

    return accounts


# --------------------------- логин и сохранение -----------------------------
async def ensure_session(acc: Account, twofa_default: Optional[str], force: bool) -> Optional[str]:
    """Создать/вернуть StringSession для аккаунта. Возвращает строку сессии или None при ошибке.
    Если session_str уже задана и not force — возвращает её без логина.
    """
    tag = f"TG{acc.idx}"

    if acc.session_str and not force:
        return acc.session_str

    if not acc.phone:
        print(f"[!] {tag}: отсутствует {tag}_PHONE — без ранее выданной TG{acc.idx}_SESSION логин невозможен")
        return None

    # Если сессия отсутствует или принудительно обновляем — логинимся
    session = StringSession(acc.session_str) if acc.session_str and (not force) else StringSession()
    client = TelegramClient(session, acc.api_id, acc.api_hash, proxy=acc.proxy)

    try:
        await client.connect()
        if not await client.is_user_authorized():
            print(f"[→] {tag}: отправляю код на {acc.phone}")
            try:
                await client.send_code_request(acc.phone)
            except errors.FloodWaitError as e:
                print(f"[✗] {tag}: FloodWait {e.seconds}s — пропуск")
                await client.disconnect()
                return None
            except errors.PhoneNumberInvalidError:
                print(f"[✗] {tag}: неверный номер телефона {acc.phone}")
                await client.disconnect()
                return None

            code = input(f"Введите код для {acc.phone}: ")
            try:
                await client.sign_in(acc.phone, code)
            except errors.SessionPasswordNeededError:
                pwd = twofa_default if twofa_default else getpass(f"Пароль 2FA для {acc.phone} (не отображается): ")
                try:
                    await client.sign_in(password=pwd)
                except errors.PasswordHashInvalidError:
                    print(f"[✗] {tag}: неверный пароль 2FA")
                    await client.disconnect()
                    return None
            except errors.PhoneCodeInvalidError:
                print(f"[✗] {tag}: неверный код подтверждения")
                await client.disconnect()
                return None
        # Авторизовано — сохраняем строковую сессию
        sess_str = client.session.save()
        return sess_str
    except Exception as e:
        print(f"[✗] {tag}: ошибка {e}")
        return None
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass


# --------------------------------- CLI -------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description="Bulk‑генератор Telegram StringSession из .env.gen")
    parser.add_argument("--env", default=".env.gen", help="Путь к входному .env файлу (по умолчанию ./.env.gen)")
    parser.add_argument("--out", default="sessions/sessions.txt", help="Куда сохранить строки TG{N}_SESSION (по умолчанию ./sessions/sessions.txt)")
    parser.add_argument("--only", default=None, help="Список индексов через запятую (например, 1,3,7) — обработать только их")

    args = parser.parse_args()

    # Пути
    script_dir = Path(__file__).resolve().parent

    env_path = Path(args.env).expanduser()
    if not env_path.is_absolute():
        env_path = (Path.cwd() / env_path).resolve()

    # Если файл не найден по указанному пути — попробуем типичные варианты рядом со скриптом
    if not env_path.exists():
        candidates = [
            script_dir / ".env.gen",                 # tools/.env.gen (если запускаем из корня)
            script_dir / "tools" / ".env.gen",     # tools/tools/.env.gen на всякий случай
            script_dir.parent / ".env.gen",          # корень проекта
            script_dir.parent / "tools" / ".env.gen",
        ]
        found = None
        for cand in candidates:
            if cand.exists():
                found = cand
                break
        if found is not None:
            env_path = found.resolve()
        else:
            # покажем пользователю куда мы пробовали смотреть
            tried = [str(Path(args.env))] + [str(c) for c in candidates]
            raise FileNotFoundError("Не найден .env файл. Передай --env PATH.")

    out_path = Path(args.out).expanduser().resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    env = parse_env_file(env_path)
    twofa_default = env.get("TG_PASSWORD")  # общий пароль 2FA, если задан

    only_list: Optional[List[int]] = None
    if args.only:
        try:
            only_list = [int(x.strip()) for x in args.only.split(',') if x.strip()]
        except ValueError:
            print("[!] --only должен быть списком чисел через запятую, например: --only 1,2,5")
            return 2

    accounts = discover_accounts(env, only_list)
    if not accounts:
        print("[!] Не найдено ни одного аккаунта TG{N}_*")
        return 1

    async def runner() -> int:
        results: Dict[int, str] = {}
        for acc in accounts:
            sess = await ensure_session(acc, twofa_default=twofa_default, force=True)
            if sess:
                results[acc.idx] = sess
        if not results:
            print("[!] Не удалось получить ни одной сессии")
            return 1

        # Сформируем файл с сессиями
        lines = [f"TG{idx}_SESSION={sess}" for idx, sess in sorted(results.items())]
        out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        print("[✓] Все сессии сохранены.")
        return 0

    return asyncio.run(runner())


if __name__ == "__main__":
    sys.exit(main())
