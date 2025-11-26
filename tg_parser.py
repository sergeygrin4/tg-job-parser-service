# tg_parser.py
import os
import time
import logging
import hashlib
from datetime import datetime
from typing import List, Optional

from telethon import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest
import requests
import psycopg2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - tg_parser - %(levelname)s - %(message)s",
)
log = logging.getLogger(__name__)

API_BASE_URL = (os.getenv("API_BASE_URL") or "").rstrip("/")  # тот же, что и для miniapp
API_SECRET = os.getenv("API_SECRET", "mvp-secret-key-2024-xyz")

TG_API_ID = int(os.getenv("TG_API_ID", "0"))
TG_API_HASH = os.getenv("TG_API_HASH")
TG_SESSION = os.getenv("TG_SESSION", "tg_session")

# Подключение к той же БД, что и miniapp (таблица fb_groups)
DATABASE_URL = os.getenv("DATABASE_URL")

# Фоллбек-источники из переменной окружения (если с БД что-то не так)
RAW_TG_SOURCES = os.getenv("TG_SOURCES", "")

JOB_KEYWORDS: List[str] = [
    kw.strip().lower()
    for kw in os.getenv(
        "JOB_KEYWORDS",
        "вакансия,работа,job,hiring,remote,developer,программист,engineer",
    ).split(",")
    if kw.strip()
]

CHECK_INTERVAL_MINUTES = int(os.getenv("CHECK_INTERVAL_MINUTES", "5"))
MESSAGES_PER_SOURCE = int(os.getenv("MESSAGES_PER_SOURCE", "50"))


def get_tg_sources_from_db() -> List[str]:
    """Возвращает список Telegram-источников из БД.

    Используем таблицу fb_groups из miniapp:

        CREATE TABLE IF NOT EXISTS fb_groups (
            id SERIAL PRIMARY KEY,
            group_id TEXT NOT NULL,
            group_name TEXT,
            enabled BOOLEAN DEFAULT TRUE,
            added_at TIMESTAMPTZ DEFAULT NOW()
        );

    Логика отбора:
      * enabled = TRUE
      * group_id НЕ начинается с 'http' — считаем, что это Telegram username,
        а не ссылка на FB-группу.
    """
    sources: List[str] = []

    if DATABASE_URL:
        try:
            conn = psycopg2.connect(DATABASE_URL)
            cur = conn.cursor()
            cur.execute(
                """
                SELECT group_id
                FROM fb_groups
                WHERE enabled = TRUE
                  AND group_id NOT LIKE 'http%%'
                ORDER BY id ASC
                """
            )
            rows = cur.fetchall()
            conn.close()

            sources = [row[0] for row in rows if row[0]]
            if sources:
                log.info(f"📥 Получено {len(sources)} Telegram-источников из БД: {sources}")
                return sources
            else:
                log.warning(
                    "В БД (fb_groups) нет Telegram-каналов (enabled=TRUE, group_id NOT LIKE 'http%%')"
                )
        except Exception as e:
            log.exception(
                f"❌ Не удалось получить Telegram-источники из БД, fallback на TG_SOURCES: {e}"
            )
    else:
        log.warning("DATABASE_URL не задан — использую TG_SOURCES из env")

    # Фоллбек: читаем TG_SOURCES из env
    sources = [s.strip() for s in RAW_TG_SOURCES.split(",") if s.strip()]
    if sources:
        log.info(f"📥 Источники из переменной TG_SOURCES: {sources}")
    else:
        log.error("Не найдено ни одного источника ни в БД, ни в TG_SOURCES")
    return sources


def text_matches_keywords(text: str) -> bool:
    t = (text or "").lower()
    return any(kw in t for kw in JOB_KEYWORDS)


def build_external_id(chat_id: int, message_id: int) -> str:
    raw = f"tg:{chat_id}:{message_id}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def send_job(
    source_name: str,
    url: Optional[str],
    message_text: str,
    chat_id: int,
    message_id: int,
    date: Optional[datetime],
) -> None:
    if not API_BASE_URL:
        log.error("API_BASE_URL не задан — не могу отправить вакансию на backend")
        return

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

    try:
        resp = requests.post(f"{API_BASE_URL}/post", json=payload, headers=headers)
    except Exception as e:
        log.exception(f"❌ Ошибка HTTP при отправке вакансии: {e}")
        return

    if resp.status_code == 200:
        try:
            data = resp.json()
        except Exception:
            data = {}
        if data.get("status") == "duplicate":
            log.info(f"🔁 Уже есть такое сообщение: {external_id}")
        else:
            log.info(f"✅ Новая вакансия отправлена: {external_id}")
    else:
        log.error(f"❌ Ошибка отправки вакансии: {resp.status_code} {resp.text}")


async def parse_source(client: TelegramClient, source: str) -> None:
    # source может быть @channel, username или numeric id
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


async def run_loop_async() -> None:
    if not TG_API_ID or not TG_API_HASH:
        log.error("TG_API_ID/TG_API_HASH не заданы")
        return

    client = TelegramClient(TG_SESSION, TG_API_ID, TG_API_HASH)
    await client.start()
    log.info("🚀 Запуск Telegram Job Parser")

    while True:
        tg_sources = get_tg_sources_from_db()

        if not tg_sources:
            log.warning("Нет ни одного Telegram-источника для парсинга — жду и попробую снова позже")
        else:
            for source in tg_sources:
                try:
                    await parse_source(client, source)
                    time.sleep(1)
                except Exception as e:
                    log.exception(f"❌ Ошибка при парсинге источника {source}: {e}")

        log.info(f"⏳ Ожидание {CHECK_INTERVAL_MINUTES} минут...")
        time.sleep(CHECK_INTERVAL_MINUTES * 60)


def main() -> None:
    import asyncio

    asyncio.run(run_loop_async())


if __name__ == "__main__":
    main()
