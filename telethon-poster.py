from telethon.tl.types import MessageEntityCustomEmoji

row_raw = sheet.row_values(i)      # список ячеек
row = {
    "time_iso":          row_raw[1],
    "status":            row_raw[2],
    "name":              row_raw[3],
    "services":          row_raw[5],
    "extra_services":    row_raw[6],
    "age":               row_raw[7],
    "height":            row_raw[8],
    "weight":            row_raw[9],
    "bust":              row_raw[10],
    "express":           row_raw[11],
    "incall":            row_raw[12],
    "outcall":           row_raw[13],
    "media1":            row_raw[16],
    "media2":            row_raw[17],
    "media3":            row_raw[18],
    "media4":            row_raw[19],
    "skip":              row_raw[20],
    "whatsapp":          row_raw[21],
}

from zoneinfo import ZoneInfo
TZ_ARM = ZoneInfo("Asia/Yerevan")   # официальное название

# при разборе времени из колонки B:
post_time = datetime.fromisoformat(row["time_iso"]).replace(tzinfo=TZ_ARM)

EMOJI = {
    1: 5429293125518510398,
    2: 5814534640949530526,
    3: 5370853949358218655,
    4: 5370674552869232639,
    5: 5372943137415111238,
    6: 5373338978780979795,
    7: 5372991528811635071,
    8: 5364098734600762220,
    9: 5460811944883660881,
}
ZWSP = "\u2060"   # невидимый символ-заглушка (длина = 1)
def build_post(row):
    """
    Сборка текста + MessageEntityCustomEmoji-список.
    Колонки берём согласно вашему описанию.
    """
    entities = []
    text_parts = []

    def add_emoji(n):
        offset = sum(len(p) for p in text_parts)
        entities.append(
            MessageEntityCustomEmoji(offset=offset, length=1, document_id=EMOJI[n])
        )
        return ZWSP  # вставляем один «пустой» символ, который заменят entities

    # ─── шапка ───
    text_parts.append(add_emoji(1))
    text_parts.append(f" {row['status']} ")
    text_parts.append(add_emoji(1) + "\n")
    # пропуск
    text_parts.append(f"{row['skip']}{add_emoji(2)}\n")
    # имя
    text_parts.append(f"{row['name']}\n\n")

    # ─── фото/галерея │️ эмодзи 3-7 ───
    text_parts.append("Фото ")
    for n in (3, 4, 5, 6, 7):
        text_parts.append(add_emoji(n))
        text_parts.append(" ")
    text_parts.append("\n\n")

    # ─── услуги ───
    text_parts.append("Услуги:\n")
    text_parts.append(f"{row['services']}\n")
    text_parts.append("Доп. услуги:\n")
    text_parts.append(f"{row['extra_services']}\n\n")

    # ─── параметры ───
    text_parts.append("Параметры:\n")
    text_parts.append(f"Возраст – {row['age']}\n")
    text_parts.append(f"Рост   – {row['height']}\n")
    text_parts.append(f"Вес    – {row['weight']}\n")
    text_parts.append(f"Грудь  – {row['bust']}\n\n")

    # ─── цены ───
    text_parts.append("Цена:\n")
    text_parts.append(f"Express – {row['express']}\n")
    text_parts.append(f"Incall  – {row['incall']}\n")
    text_parts.append(f"Outcall – {row['outcall']}\n\n")

    # ─── CTA ───
    text_parts.append(add_emoji(8))
    text_parts.append(" Назначь встречу уже сегодня! ")
    text_parts.append(add_emoji(8) + "\n")
    # гиперссылка WhatsApp
    text_parts.append(f'<a href="{row["whatsapp"]}">Связь в WhatsApp</a> ')
    text_parts.append(add_emoji(9))

    return "".join(text_parts), entities
async def send_post(acc, row):
    client  = acc["client"]
    channel = acc["channel"]

    text, entities = build_post(row)

    # --- собираем медиа (URL или пустая строка) ---
    media_urls = [row.get(col) for col in ("media1", "media2", "media3", "media4")]
    media_urls = [u for u in media_urls if u]

    if media_urls:
        from telethon.tl.types import InputMediaPhotoExternal
        album = [InputMediaPhotoExternal(u) for u in media_urls]
        await client.send_file(
            channel,
            album,
            caption=text,
            parse_mode="html",
            entities=entities,
        )
    else:
        await client.send_message(
            channel,
            text,
            parse_mode="html",
            entities=entities,
        )

