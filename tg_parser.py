import asyncio
import json
import logging
import os
import random
from datetime import timezone
from urllib import request as urllib_request
from urllib.error import HTTPError, URLError

import aiohttp
from telethon import TelegramClient
from telethon.errors import FloodWaitError, RPCError
from telethon.sessions import StringSession

# ---------- ЛОГГЕР ----------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - tg_parser - %(levelname)s - %(message)s",
)
logger = logging.getLogger("tg_parser")

# ---------- КОНФИГ И ОКРУЖЕНИЕ ----------

def _env_first(*names: str, default: str = "") -> str:
    for n in names:
        v = os.getenv(n)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return default

API_ID = int(_env_first("TG_API_ID", "API_ID", default="0") or "0")
API_HASH = _env_first("TG_API_HASH", "API_HASH", default="")
SESSION_STRING = _env_first("TG_SESSION", "TELEGRAM_SESSION", "SESSION", default="")

# ---------- ФУНКЦИИ ПАРСИНГА ----------

async def fetch_sources(session: aiohttp.ClientSession) -> list[str]:
    # Здесь можно добавить логику для получения источников
    # Например, считывание из файла или API
    # Для примера, возвращаем пустой список
    try:
        with open('sources.txt', 'r') as f:
            return [line.strip() for line in f.readlines()]
    except FileNotFoundError:
        logger.error("Файл sources.txt не найден!")
        return []

async def parse_source(client: TelegramClient, session: aiohttp.ClientSession, source: str) -> None:
    try:
        # Пример: Получаем сообщения из канала или чата
        messages = await client.get_messages(source, limit=10)
        for message in messages:
            logger.info(f"Сообщение от {source}: {message.text}")
            # Ваш код для обработки сообщений
    except Exception as e:
        logger.error(f"Ошибка при парсинге источника {source}: {e}")

async def send_alert(message: str) -> None:
    # Пример функции для отправки уведомлений
    logger.info(message)

async def main() -> None:
    if not SESSION_STRING:
        logger.error("Сессия Telegram не предоставлена!")
        return

    # Инициализация клиента Telegram
    client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

    try:
        await client.start()
        send_alert("✅ tg_parser started.")

        while True:
            sources = await fetch_sources(None)  # Передаем None для session, так как это не используется в fetch_sources
            if not sources:
                send_alert("⚠️ tg_parser: sources пустые — парсить нечего")
            else:
                for s in sources:
                    await parse_source(client, None, s)  # Передаем None для session, так как это не используется

            sleep_s = random.randint(60, 300)  # Пример случайного времени ожидания
            logger.info(f"⏲️ sleep {sleep_s}s")
            await asyncio.sleep(sleep_s)

    finally:
        await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
