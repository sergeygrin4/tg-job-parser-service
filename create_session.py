# create_session.py
import os
from telethon import TelegramClient

TG_API_ID = int(os.getenv("TG_API_ID") or input("TG_API_ID: "))
TG_API_HASH = os.getenv("TG_API_HASH") or input("TG_API_HASH: ")
TG_SESSION = os.getenv("TG_SESSION", "tg_session")

client = TelegramClient(TG_SESSION, TG_API_ID, TG_API_HASH)

async def main():
    await client.start()  # тут ты один раз введёшь номер / код локально
    print("✅ Session saved:", TG_SESSION + ".session")

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
