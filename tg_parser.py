# tg_parser.py
import os
import time
import logging
import hashlib
from datetime import datetime

from telethon import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - tg_parser - %(levelname)s - %(message)s",
)
log = logging.getLogger(__name__)

API_BASE_URL = os.getenv("API_BASE_URL")  # тот же, что и для fb_parser
API_SECRET = os.getenv("API_SECRET", "mvp-secret-key-2024-xyz")

TG_API_ID = int(os.getenv("TG_API_ID", "0"))
TG_API_HASH = os.getenv("TG_API_HASH")
TG_SESSION = os.getenv("TG_SESSION", "tg_session")

# Каналы/чаты, через запятую: @channel1,@channel2,-1001234567890
TG_SOURCES = [
    s.strip()
    for s in os.getenv("TG_SOURCES", "").split(",")
    if s.strip()
]

JOB_KEYWORDS = [
    kw.strip().lower()
    for kw in os.getenv("JOB_KEYWORDS", "вакансия,работа,job,hiring,remote,developer").split(",")
    if kw.strip()
]

CHECK_INTERVAL_MINUTES = int(os.getenv("CHECK_INTERVAL_MINUTES", "5"))
MESSAGES_PER_SOURCE = int(os.getenv("MESSAGES_PER_SOURCE", "50"))


def text_matches_keywords(text: str) -> bool:
    t = (text or "").lower()
    return any(kw in t for kw in JOB_KEYWORDS)


def build_external_id(chat_id: int, message_id: int) -> str:
    raw = f"tg:{chat_id}:{message_id}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def send_job(source_name: str, url: str | None, message_text: str, chat_id: int, message_id: int, date: datetime | None):
    external_id = build_external_id(chat_id, message_id)

    payload = {
        "source": "telegram",
        "source_name": source_name,
        "external_id": external_id,
        "url": url,
        "text": message_text,
        "created_at": date.isoformat() if date else None,
    }

    headers = {
        "Content-Type": "application/json",
        "X-API-KEY": API_SECRET,
    }

    resp = requests.post(f"{API_BASE_URL}/post", json=payload, headers=headers)
    if resp.status_code == 200:
        data = resp.json()
        if data.get("status") == "duplicate":
            log.info(f"🔁 Уже есть такое сообщение: {external_id}")
        else:
            log.info(f"✅ Новая вакансия отправлена: {external_id}")
    else:
        log.error(f"❌ Ошибка отправки вакансии: {resp.status_code} {resp.text}")


async def parse_source(client: TelegramClient, source: str):
    """
    source может быть @channel, username или numeric id
    """
    log.info(f"🔍 Парсим Telegram источник: {source}")

    entity = await client.get_entity(source)

    history = await client(
        GetHistoryRequest(
            peer=entity,
            limit=MESSAGES_PER_SOURCE,
            offset_date=None,
            offset_id=0,
            max_id=0,
            min_id=0,
            add_offset=0,
            hash=0,
        )
    )

    for message in history.messages:
        if not getattr(message, "message", None):
            continue

        text = message.message
        if not text_matches_keywords(text):
            continue

        # Формируем t.me ссылку если возможно
        url = None
        if getattr(entity, "username", None):
            url = f"https://t.me/{entity.username}/{message.id}"

        send_job(
            source_name=str(source),
            url=url,
            message_text=text,
            chat_id=entity.id,
            message_id=message.id,
            date=message.date,
        )


async def run_loop_async():
    if not TG_SOURCES:
        log.error("TG_SOURCES не задан — нечего парсить")
        return

    client = TelegramClient(TG_SESSION, TG_API_ID, TG_API_HASH)
    await client.start()
    log.info("🚀 Запуск Telegram Job Parser")

    while True:
        try:
            for source in TG_SOURCES:
                await parse_source(client, source)
                time.sleep(1)
        except Exception as e:
            log.exception(f"❌ Ошибка в основном цикле: {e}")

        log.info(f"⏳ Ожидание {CHECK_INTERVAL_MINUTES} минут...")
        time.sleep(CHECK_INTERVAL_MINUTES * 60)


def main():
    import asyncio

    asyncio.run(run_loop_async())


if __name__ == "__main__":
    main()
