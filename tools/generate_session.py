from telethon.sync import TelegramClient
from telethon.sessions import StringSession

api_id = 26739235 
api_hash = "568bd4c85caaf0c7683a07585d7d06b3"

with TelegramClient(StringSession(), api_id, api_hash) as client:
    print(client.session.save())