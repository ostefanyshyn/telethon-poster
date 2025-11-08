

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Конвертация Telethon .session (SQLite) в String Session и вывод в терминал.

Использование:
  python3 convert-session-to-string-session /путь/к/файлу.session

Если путь не указан, используется DEFAULT_PATH ниже.
"""
import sys
import os

try:
    from telethon.sessions import SQLiteSession, StringSession
except Exception as e:
    print("Не удалось импортировать telethon. Установите его командой: pip install telethon")
    raise

DEFAULT_PATH = "/Users/ostap/Downloads/37477747994 3/37477747994.session"

def main():
    path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_PATH

    if not os.path.exists(path):
        print(f"Файл не найден: {path}")
        sys.exit(1)

    # Загружаем SQLite-сессию с диска
    sess = SQLiteSession(path)
    # На всякий случай пытаемся явно загрузить данные
    try:
        sess.load()
    except Exception:
        pass

    # Конвертация в String Session и вывод
    try:
        string_session = StringSession.save(sess)
    except Exception as e:
        print(
            "Не удалось конвертировать .session в String Session.\n"
            "Обновите telethon до последней версии: pip install -U telethon\n"
            f"Техническая ошибка: {e}"
        )
        sys.exit(2)

    print(string_session)

if __name__ == "__main__":
    main()