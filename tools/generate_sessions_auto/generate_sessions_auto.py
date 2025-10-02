# -*- coding: utf-8 -*-
"""
Автоподъём Telethon-клиентов из Session String с обязательным SOCKS5-прокси.
Берёт конфиг из .env.genauto (или .envauto/.genauto). Каждый TG<N>_* должен иметь прокси.
Ключи сессий в .env: TG<N>_SESSION (TG<N>_SESSIONS поддерживается для совместимости).
В конце печатает актуальные Session String для всех успешно стартовавших аккаунтов.
"""
import asyncio
import os
import re
from pathlib import Path
from typing import Dict, Any, Tuple, Optional

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import RPCError
import socks  # из пакета PySocks


# ---------- утилиты ----------

TRUE_SET = {"1", "true", "yes", "y", "on"}

def load_kv_file(path: Path) -> Dict[str, str]:
    """Простой .env-парсер: KEY=VALUE, игнор пустых/комментов."""
    data: Dict[str, str] = {}
    if not path.exists():
        return data
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        data[k.strip()] = v.strip()
    return data

def find_config_file() -> Path:
    """Ищем .env.genauto (приоритет), затем .envauto/.genauto.
    Порядок:
    1) Переменная окружения GEN_AUTO_FILE (если задана)
    2) Текущая директория и все родительские директории
    3) Директория скрипта и все её родительские директории
    """
    # 1) Явный путь через env-переменную
    env_path = os.getenv("GEN_AUTO_FILE")
    if env_path:
        p = Path(env_path).expanduser().resolve()
        if p.exists():
            return p
        raise FileNotFoundError(f"GEN_AUTO_FILE указывает на несуществующий файл: {p}")

    # 2) Набор имён по приоритету
    names = (".env.genauto", ".envauto", ".genauto")

    searched = []

    # 2.1) Проход вверх от текущей директории
    cwd = Path.cwd().resolve()
    for base in [cwd, *cwd.parents]:
        for name in names:
            cand = base / name
            searched.append(str(cand))
            if cand.exists():
                return cand

    # 2.2) Проход вверх от директории скрипта
    here = Path(__file__).resolve().parent
    for base in [here, *here.parents]:
        for name in names:
            cand = base / name
            searched.append(str(cand))
            if cand.exists():
                return cand

    # 3) Не нашли — формируем понятную ошибку
    searched_hint = "\n".join(searched[:30])  # ограничим вывод
    raise FileNotFoundError(
        "Не найден конфиг .env.genauto (или .envauto/.genauto).\n"
        "Где искали (первые 30 путей):\n" + searched_hint + "\n\n"
        "Подсказка: задайте полный путь через переменную окружения GEN_AUTO_FILE, "
        "например: GEN_AUTO_FILE=/abs/path/.env.genauto"
    )

def collect_accounts(env: Dict[str, str]) -> Dict[int, Dict[str, str]]:
    """
    Собираем TG<N>_* группы. Возвращаем словарь {N: {KEY: VAL}}.
    Обязательно требуем PHONE, SESSION и все поля прокси.
    """
    groups: Dict[int, Dict[str, str]] = {}
    rx = re.compile(r"^TG(\d+)_(PHONE|SESSION|SESSIONS|PROXY_TYPE|PROXY_HOST|PROXY_PORT|PROXY_USER|PROXY_PASS|PROXY_RDNS)$")
    for key, val in env.items():
        m = rx.match(key)
        if not m:
            continue
        idx = int(m.group(1))
        subkey = m.group(2)
        groups.setdefault(idx, {})[subkey] = val

    # Совместимость: TG<N>_SESSIONS → TG<N>_SESSION, если SESSION не задан
    for idx, cfg in groups.items():
        if "SESSION" not in cfg and cfg.get("SESSIONS"):
            cfg["SESSION"] = cfg["SESSIONS"]

    filtered: Dict[int, Dict[str, str]] = {}
    for idx, cfg in sorted(groups.items()):
        missing = []
        for req in ("PHONE", "SESSION", "PROXY_TYPE", "PROXY_HOST", "PROXY_PORT"):
            if req not in cfg or not cfg[req]:
                missing.append(req)
        if missing:
            print(f"[TG{idx}] Пропущены поля: {', '.join(missing)} — аккаунт будет пропущен.")
            continue
        if cfg["PROXY_TYPE"].strip().lower() != "socks5":
            print(f"[TG{idx}] PROXY_TYPE={cfg['PROXY_TYPE']} не поддерживается (нужен socks5). Пропуск.")
            continue
        filtered[idx] = cfg
    return filtered

def build_proxy(cfg: Dict[str, str]):
    """
    Возвращаем tuple для Telethon/PySocks:
    (proxy_type, host, port, rdns, username, password)
    """
    host = cfg["PROXY_HOST"]
    port = int(cfg["PROXY_PORT"])
    user = cfg.get("PROXY_USER") or None
    pwd = cfg.get("PROXY_PASS") or None
    rdns = str(cfg.get("PROXY_RDNS", "")).strip().lower() in TRUE_SET
    return (socks.SOCKS5, host, port, rdns, user, pwd)

def sanitize_name(phone_or_idx: str) -> str:
    """Аккуратное имя для лога (и т.п.)."""
    s = re.sub(r"[^\d+]+", "_", phone_or_idx).strip("_")
    return s or "account"


# ---------- основная логика ----------

async def start_account(idx: int, api_id: int, api_hash: str, cfg: Dict[str, str]) -> Tuple[int, Optional[str], Optional[str]]:
    """
    Стартуем аккаунт TG<idx>. Возвращаем (idx, phone, new_session_string) или (idx, phone, None) при ошибке.
    """
    phone = cfg.get("PHONE", f"TG{idx}")
    name = sanitize_name(phone) or f"TG{idx}"
    session_string = cfg["SESSION"]
    proxy = build_proxy(cfg)

    # ВАЖНО: Telethon-строка — это уже готовая сессия; просто подключаемся через прокси.
    client = TelegramClient(
        session=StringSession(session_string),
        api_id=api_id,
        api_hash=api_hash,
        proxy=proxy,
        # можно добавить connection/retry/timeout при желании
    )

    try:
        async with client:
            me = await client.get_me()
            print(f"[TG{idx} | {phone}] ✅ Запущен: {me.id} @{me.username or me.first_name}")
            # Обновляем строковую сессию (на случай изменений внутри, например DC/лимиты)
            refreshed = client.session.save()
            return idx, phone, refreshed
    except RPCError as e:
        print(f"[TG{idx} | {phone}] ❌ RPCError: {e}")
        return idx, phone, None
    except Exception as e:
        print(f"[TG{idx} | {phone}] ❌ Ошибка: {e}")
        return idx, phone, None

async def main():
    # 1) конфиг
    cfg_path = find_config_file()
    print(f"Использую конфиг: {cfg_path}")
    env = load_kv_file(cfg_path)

    # 2) общие API_ID/API_HASH
    try:
        api_id = int(env["TG_API_ID"])
        api_hash = env["TG_API_HASH"]
    except KeyError as e:
        raise SystemExit(f"В {cfg_path.name} не найдены TG_API_ID / TG_API_HASH") from e

    # 3) собираем аккаунты
    accounts = collect_accounts(env)
    if not accounts:
        raise SystemExit("Не найдено ни одного валидного TG<N>_* аккаунта с SOCKS5-прокси.")

    # 4) ограничим параллелизм, чтобы не душить прокси
    sem = asyncio.Semaphore(5)

    async def guarded(idx: int, cfg: Dict[str, str]):
        async with sem:
            return await start_account(idx, api_id, api_hash, cfg)

    results = await asyncio.gather(*(guarded(idx, cfg) for idx, cfg in accounts.items()))

    # 5) сводка + печать всех актуальных строк
    ok, fail = 0, 0
    print("\n=== Итоги запуска ===")
    for idx, phone, s in results:
        if s:
            ok += 1
        else:
            fail += 1
    print(f"Успешно: {ok} | Ошибок: {fail}")

    # Вывод в формате: TG{n}_SESSION=<string>
    for idx, phone, s in results:
        if s:
            print(f"TG{idx}_SESSION={s}")
        else:
            # Комментируем ошибочные строки, чтобы не ломать .env при копипасте
            print(f"# TG{idx}_SESSION=<ошибка для {phone}>")

if __name__ == "__main__":
    asyncio.run(main())