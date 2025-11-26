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


# ----------------- ЛОГИ -----------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - tg_parser - %(levelname)s - %(message)s",
)
log = logging.getLogger("tg_parser")


# ----------------- КОНФИГ -----------------

# URL миниаппа, без /post на конце, например:
# https://web-production-ad84.up.railway.app
API_BASE_URL = (os.getenv("API_BASE_URL") or "").rstrip("/")

# Должен совпадать с API_SECRET в миниаппе
API_SECRET = os.getenv("API_SECRET", "mvp-secret-key-2024-xyz")

TG_API_ID = int(os.getenv("TG_API_ID", "0"))
TG_API_HASH = os.getenv("TG_API_HASH")
TG_SESSION = os.getenv("TG_SESSION", "tg_session")

# Строка подключения к Postgres (public URL от Railway)
DATABASE_URL = os.getenv("DATABASE_URL")

# Фоллбек-источники (если БД вдруг не работает)
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


# ----------------- РАБОТА С БД -----------------


def get_tg_sources_from_db() -> List[str]:
    """
    Возвращает список Telegram-источников из БД.

    В таблице fb_groups лежат и FB, и TG:

        id | group_id                         | group_name | enabled
        ---+----------------------------------+-----------+--------
         1 | https://www.facebook.com/groups/...
         6 | https://t.me/proamazon1
         7 | https://t.me/AmazonSvoboda/1
        ...

    Логика:
      - берём только enabled = TRUE
      - считаем Telegram всё, где:
            group_id ILIKE '%t.me/%'
         ИЛИ group_id LIKE '@...'
      - facebook-ссылки автоматически отваливаются, т.к. без t.me
    """
    sources: List[str] = []

    # если БД не настроена — уходим в TG_SOURCES
    if not DATABASE_URL:
        log.warning("DATABASE_URL не задан — читаю TG_SOURCES из env")
        raw_sources = RAW_TG_SOURCES
        return [s.strip() for s in raw_sources.split(",") if s.strip()]

    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT group_id
            FROM fb_groups
            WHERE enabled = TRUE
              AND (
                    group_id ILIKE '%%t.me/%%'
                 OR group_id LIKE '@%%'
              )
            ORDER BY id ASC
            """
        )
        rows = cur.fetchall()
        conn.close()

        sources = [row[0] for row in rows if row[0]]

        if sources:
            log.info(f"📥 Получено {len(sources)} Telegram-источников из БД: {sources}")
        else:
            log.warning(
                "В БД (fb_groups) нет Telegram-каналов "
                "(enabled=TRUE, group_id ILIKE '%t.me/%' или LIKE '@%')"
            )

    except Exception as e:
        log.exception(
            f"❌ Не удалось получить Telegram-источники из БД, fallback на TG_SOURCES: {e}"
        )
        raw_sources = RAW_TG_SOURCES
        sources = [s.strip() for s in raw_sources.split(",") if s.strip()]

    if not sources:
        log.error("Не найдено ни одного источника ни в БД, ни в TG_SOURCES")

    return sources


# ----------------- УТИЛИТЫ -----------------


def text_matches_keywords(text: str) -> bool:
    t = (text or "").lower()
    return any(kw in t for kw in JOB_KEYWORDS)


def build_external_id(chat_id: int, message_id: int) -> str:
    raw = f"tg:{chat_id}:{message_id}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


# ----------------- ОТПРАВКА ВАКАНСИИ НА API -----------------


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


# ----------------- ПАРСИНГ ОДНОГО ИСТОЧНИКА -----------------


async def parse_source(client: TelegramClient, source: str) -> None:
    log.info(f"🔍 Парсим Telegram источник: {source}")

    # source может быть https://t.me/... или @username
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


# ----------------- ОСНОВНОЙ ЦИКЛ -----------------


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
            log.warning(
                "Нет ни одного Telegram-источника для парсинга — жду и попробую снова позже"
            )
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
