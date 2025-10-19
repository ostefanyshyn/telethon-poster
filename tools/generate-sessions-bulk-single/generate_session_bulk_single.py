#!/usr/bin/env python3
"""
Генератор Telegram StringSession для нескольких аккаунтов на базе переменных из .env.gen.

Особенности:
- Автоматически находит аккаунты TG{N}_* в .env.gen (N = 1..∞).
- Поддерживает общий TG_API_ID/TG_API_HASH (или API_ID/API_HASH) — используется как дефолт для всех аккаунтов, если у TG{N}_* нет своих.
- Поддерживает прокси (SOCKS4/SOCKS5/HTTP) с RDNS и логином/паролем.
- Обрабатывает 2FA (two‑step) — берёт пароли из TG{N}_PASSWORDS/TG_PASSWORDS (через запятую) и/или TG{N}_PASSWORD/TG_PASSWORD; пробует по очереди, а затем запросит ввод.
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
import subprocess
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


def copy_to_clipboard(text: str) -> bool:
    """
    Copy `text` to the system clipboard.
    Tries pyperclip if available, then falls back to platform-specific utilities:
    - macOS: pbcopy
    - Windows: clip
    - Linux/Wayland/X11: wl-copy, xclip, xsel
    Returns True on success, False otherwise.
    """
    # Try pyperclip first if installed
    try:
        import pyperclip  # type: ignore
        pyperclip.copy(text)
        return True
    except Exception:
        pass

    # macOS
    try:
        proc = subprocess.Popen(["pbcopy"], stdin=subprocess.PIPE)
        proc.communicate(input=text.encode("utf-8"))
        if proc.returncode == 0:
            return True
    except Exception:
        pass

    # Windows
    try:
        # clip expects UTF-16LE
        proc = subprocess.Popen(["clip"], stdin=subprocess.PIPE, shell=True)
        proc.communicate(input=text.encode("utf-16le"))
        if proc.returncode == 0:
            return True
    except Exception:
        pass

    # Linux / BSD / Wayland
    for cmd in (["wl-copy"], ["xclip", "-selection", "clipboard"], ["xsel", "--clipboard", "--input"]):
        try:
            proc = subprocess.Popen(cmd, stdin=subprocess.PIPE)
            proc.communicate(input=text.encode("utf-8"))
            if proc.returncode == 0:
                return True
        except Exception:
            continue

    return False


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
        key, raw_val = m.group(1), m.group(2).strip()
        # поддержка inline‑комментариев и лишних пробелов
        val = raw_val
        quoted = (val.startswith('"') and val.endswith('"')) or (val.startswith("'") and val.endswith("'"))
        if quoted:
            val = val[1:-1]
        else:
            # удаляем комментарий после значения: KEY=VALUE # comment
            hash_pos = val.find('#')
            if hash_pos != -1:
                val = val[:hash_pos]
        val = val.strip()
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


def split_passwords(val: Optional[str]) -> List[str]:
    """Разбивает строку паролей 2FA по запятым/точкам с запятой и удаляет дубликаты, сохраняя порядок."""
    if not val:
        return []
    parts = re.split(r"[;,]", val)
    out: List[str] = []
    for p in parts:
        p = p.strip()
        if p and p not in out:
            out.append(p)
    return out


def get_passwords_for(env: Dict[str, str], idx: int) -> List[str]:
    """Возвращает список паролей 2FA для аккаунта по приоритету:
    TG{N}_PASSWORDS → TG{N}_PASSWORD → TG_PASSWORDS → TG_PASSWORD
    """
    acc_list = get_any(env, [f"TG{idx}_PASSWORDS"]) or None
    acc_single = get_any(env, [f"TG{idx}_PASSWORD"]) or None
    glob_list = get_any(env, ["TG_PASSWORDS"]) or None
    glob_single = get_any(env, ["TG_PASSWORD"]) or None

    passwords: List[str] = []
    passwords += split_passwords(acc_list)
    if acc_single and acc_single not in passwords:
        passwords.append(acc_single)
    for p in split_passwords(glob_list):
        if p not in passwords:
            passwords.append(p)
    if glob_single and glob_single not in passwords:
        passwords.append(glob_single)
    return passwords


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
        api_id_s = get_any(env, [f"TG{i}_API_ID", "TG_API_ID", "API_ID"]) or ""
        api_hash = get_any(env, [f"TG{i}_API_HASH", "TG_API_HASH", "API_HASH"]) or ""
        phone = get_any(env, [f"TG{i}_PHONE", f"TG{i}_Phone"])  # опционально
        session_str = get_any(env, [f"TG{i}_SESSION"])  # опционально

        if not api_id_s or not api_hash:
            print(f"[i] Пропуск TG{i}: отсутствует API_ID/API_HASH")
            continue

        # Прокси — пробуем сначала конкретные TG{i}_*, затем TG_PROXY_*, затем глобальные PROXY_*
        p_type = (get_any(env, [f"TG{i}_PROXY_TYPE", "TG_PROXY_TYPE", "PROXY_TYPE"]) or "").lower()
        p_host = get_any(env, [f"TG{i}_PROXY_HOST", "TG_PROXY_HOST", "PROXY_HOST"]) or ""
        p_port_s = get_any(env, [f"TG{i}_PROXY_PORT", "TG_PROXY_PORT", "PROXY_PORT"]) or ""
        p_user = get_any(env, [f"TG{i}_PROXY_USER", "TG_PROXY_USER", "PROXY_USER"]) or None
        p_pass = get_any(env, [f"TG{i}_PROXY_PASS", "TG_PROXY_PASS", "PROXY_PASS"]) or None
        # RDNS флаг: по-умолчанию True если не указан
        p_rdns = to_bool(get_any(env, [f"TG{i}_PROXY_RDNS", "TG_PROXY_RDNS", "PROXY_RDNS"]), True)

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
            print(f"[!] TG{i}: прокси не задан — прямое соединение запрещено. Укажите TG{i}_PROXY_* или глобальные PROXY_* переменные.")
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
async def ensure_session(acc: Account, twofa_passwords: Optional[List[str]], force: bool) -> Optional[str]:
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

            code = input(f"Введите код для {acc.phone} (или 'skip' для пропуска): ").strip()
            if code.lower() == "skip":
                print(f"[i] {tag}: пропуск аккаунта по команде 'skip'")
                await client.disconnect()
                return None
            try:
                await client.sign_in(acc.phone, code)
            except errors.SessionPasswordNeededError:
                success = False
                candidates = twofa_passwords or []
                print(f"[→] {tag}: требуется 2FA, пробую {len(candidates)} паролей из .env")
                for idx, pwd in enumerate(candidates, start=1):
                    try:
                        await client.sign_in(password=pwd)
                        print(f"[✓] {tag}: 2FA принят (кандидат #{idx} из .env)")
                        success = True
                        break
                    except errors.PasswordHashInvalidError:
                        print(f"[✗] {tag}: 2FA кандидат #{idx} не подошёл")
                        continue
                if not success:
                    pwd = getpass(f"Пароль 2FA для {acc.phone} (не отображается, введите 'skip' для пропуска): ").strip()
                    if pwd.lower() == "skip":
                        print(f"[i] {tag}: пропуск аккаунта по команде 'skip'")
                        await client.disconnect()
                        return None
                    try:
                        await client.sign_in(password=pwd)
                        success = True
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
    # twofa_default = env.get("TG_PASSWORD")  # общий пароль 2FA, если задан

    only_list: Optional[List[int]] = None
    if args.only:
        try:
            only_list = [int(x.strip()) for x in args.only.split(',') if x.strip()]
        except ValueError:
            print("[!] --only должен быть списком чисел через запятую, например: --only 1,2,5")
            return 2
    else:
        # Соберём доступные индексы из .env и попросим пользователя выбрать
        available = sorted({int(m.group(1)) for k in env.keys() if (m := PREFIX_RE.match(k))})
        if not available:
            print("[!] Не найдено ни одного аккаунта TG{N}_*")
            return 1
        print("Найденные аккаунты:")
        print(", ".join(str(x) for x in available))
        choice = input("Введите номер аккаунта (например '3'), список через запятую (например '1,3') или 'all' для всех: ").strip()
        if choice.lower() == 'all' or choice == '':
            only_list = None
        else:
            try:
                only_list = [int(x.strip()) for x in choice.split(',') if x.strip()]
            except ValueError:
                print("[!] Неверный ввод. Ожидался номер или список номеров через запятую.")
                return 2

    accounts = discover_accounts(env, only_list)
    if not accounts:
        print("[!] Не найдено ни одного подходящего аккаунта после фильтрации (проверьте переменные API/proxy)")
        return 1

    async def runner() -> int:
        results: Dict[int, str] = {}
        for acc in accounts:
            sess = await ensure_session(acc, twofa_passwords=get_passwords_for(env, acc.idx), force=True)
            if sess:
                results[acc.idx] = sess
                line = f"TG{acc.idx}_SESSION={sess}"
                # Print exactly in the requested KEY=VALUE format
                print(line)
                # Try to copy to the system clipboard
                if copy_to_clipboard(line):
                    print(f"[✓] TG{acc.idx}: строка сессии скопирована в буфер обмена")
                else:
                    print(f"[!] TG{acc.idx}: не удалось скопировать в буфер обмена — установите pyperclip или используйте pbcopy/xclip/clip")
        if not results:
            print("[!] Не удалось получить ни одной сессии")
            return 1

        # Уже напечатали строки и попытались скопировать их по мере генерации
        return 0

    return asyncio.run(runner())


if __name__ == "__main__":
    sys.exit(main())
