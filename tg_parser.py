import asyncio
import logging
import os
import json
from datetime import datetime, timezone
from urllib import request as urllib_request
from urllib.error import URLError, HTTPError

import aiohttp
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import RPCError, FloodWaitError

# ---------- ЛОГГЕР ----------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - tg_parser - %(levelname)s - %(message)s",
)
logger = logging.getLogger("tg_parser")

# ---------- КОНФИГ ----------

TG_API_ID = int(os.getenv("TG_API_ID") or os.getenv("API_ID") or "0")
TG_API_HASH = os.getenv("TG_API_HASH") or os.getenv("API_HASH") or ""
TG_SESSION = (
    os.getenv("TG_SESSION")
    or os.getenv("TELEGRAM_SESSION")
    or os.getenv("SESSION")
    or ""
)

API_BASE_URL = (os.getenv("API_BASE_URL") or "").rstrip("/")
if not API_BASE_URL:
    # ВАЖНО: лучше всегда задавать API_BASE_URL в env.
    API_BASE_URL = "https://telegram-job-parser-production.up.railway.app"

API_SECRET = os.getenv("API_SECRET", "")

POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "300"))
MESSAGES_LIMIT_PER_SOURCE = int(os.getenv("MESSAGES_LIMIT_PER_SOURCE", "50"))
MAX_TEXT_LEN = int(os.getenv("MAX_TEXT_LEN", "3500"))

# Фильтрация по ключевым словам (миниапп сам фильтрует тоже, но пусть будет дубль)
ENABLE_KEYWORDS_FILTER = (os.getenv("ENABLE_KEYWORDS_FILTER", "1").strip() != "0")


def _auth_headers() -> dict:
    headers = {"Content-Type": "application/json"}
    if API_SECRET:
        headers["X-API-KEY"] = API_SECRET
    return headers


def send_alert(text: str):
    """
    Системный алерт в миниапп: POST /api/alert
    Без requests, только urllib (stdlib).
    """
    try:
        url = f"{API_BASE_URL}/api/alert"
        payload = json.dumps({"source": "tg_parser", "message": text}).encode("utf-8")

        req = urllib_request.Request(url, data=payload, method="POST")
        req.add_header("Content-Type", "application/json")

        # auth (если включён API_SECRET в miniapp)
        if API_SECRET:
            req.add_header("X-API-KEY", API_SECRET)
            req.add_header("Authorization", f"Bearer {API_SECRET}")

        with urllib_request.urlopen(req, timeout=10) as resp:
            _ = resp.read()

    except (HTTPError, URLError, TimeoutError):
        pass
    except Exception:
        pass


async def fetch_secret(session: aiohttp.ClientSession, key: str) -> str | None:
    """Берём секрет из miniapp (/api/parser_secrets/<key>)."""
    url = f"{API_BASE_URL}/api/parser_secrets/{key}"
    try:
        async with session.get(url, headers=_auth_headers(), timeout=10) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
            return data.get("value")
    except Exception:
        return None


async def fetch_sources(session: aiohttp.ClientSession) -> list[dict]:
    """Берём список источников из miniapp (/api/sources)."""
    url = f"{API_BASE_URL}/api/sources"
    # sources защищены админкой, поэтому для парсера используем /api/groups (в этом проекте так исторически)
    # если у вас реально другой эндпоинт — тут поправьте.
    url2 = f"{API_BASE_URL}/api/groups"

    for u in (url2, url):
        try:
            async with session.get(u, headers=_auth_headers(), timeout=15) as resp:
                txt = await resp.text()
                if resp.status != 200:
                    logger.warning("Не удалось получить источники %s: %s %s", u, resp.status, txt[:300])
                    continue
                data = await resp.json()
                items = data.get("items") or data.get("groups") or data.get("sources") or []
                if isinstance(items, list) and items:
                    return items
        except Exception:
            continue

    send_alert("⚠️ tg_parser: список источников в /api/groups не найдено")
    return []


async def send_post(session: aiohttp.ClientSession, payload: dict):
    """
    POST /post
    """
    url = f"{API_BASE_URL}/post"
    try:
        async with session.post(url, json=payload, headers=_auth_headers(), timeout=15) as resp:
            text = await resp.text()
            if resp.status != 200:
                logger.error("❌ Ошибка /post: %s %s", resp.status, text[:500])
            else:
                logger.info("✅ Пост отправлен в миниапп: %s", text[:200])
    except Exception as e:
        logger.error("❌ Не удалось отправить пост в %s: %s", url, e)


# ---------- ФИЛЬТР ПО КЛЮЧЕВЫМ СЛОВАМ ----------

def is_relevant_by_keywords(text: str) -> bool:
    if not text:
        return False
    # миниапп хранит keywords в БД, но тут — локальный быстрый фильтр (опционально)
    # если нужно — можно подтянуть keywords аналогично sources.
    return True


# ---------- ОСНОВНОЙ ЦИКЛ ----------

async def process_source(session: aiohttp.ClientSession, client: TelegramClient, source: dict):
    """
    Читает последние сообщения из канала/чата и отправляет релевантные в miniapp.
    source ожидается в формате: {id?, title?, link?}
    """
    link = (source.get("link") or "").strip()
    title = (source.get("title") or "").strip() or link

    if not link:
        return

    try:
        entity = await client.get_entity(link)
    except Exception as e:
        logger.error("Не удалось получить entity %s: %s", link, e)
        return

    try:
        messages = await client.get_messages(entity, limit=MESSAGES_LIMIT_PER_SOURCE)
    except FloodWaitError as e:
        logger.warning("FloodWait %s sec for %s", e.seconds, link)
        await asyncio.sleep(e.seconds + 1)
        return
    except RPCError as e:
        logger.error("RPCError get_messages %s: %s", link, e)
        return
    except Exception as e:
        logger.error("Ошибка get_messages %s: %s", link, e)
        return

    for msg in reversed(messages):
        try:
            if not msg or not getattr(msg, "id", None):
                continue

            text = (getattr(msg, "message", None) or "").strip()
            if not text:
                continue

            if len(text) > MAX_TEXT_LEN:
                text = text[:MAX_TEXT_LEN].rstrip() + "…"

            if ENABLE_KEYWORDS_FILTER and not is_relevant_by_keywords(text):
                continue

            # url на сообщение (для публичных каналов/чатов будет кликабельно)
            url = ""
            try:
                if getattr(entity, "username", None):
                    url = f"https://t.me/{entity.username}/{msg.id}"
            except Exception:
                url = ""

            payload = {
                "source": "telegram",
                "source_name": title or "telegram",
                "external_id": f"{link}:{msg.id}",
                "url": url,
                "text": text,
                "sender_username": "",
                "created_at": msg.date.astimezone(timezone.utc).isoformat() if getattr(msg, "date", None) else None,
            }

            await send_post(session, payload)

        except Exception as e:
            logger.error("Ошибка обработки сообщения: %s", e)


async def main():
    if not TG_API_ID or not TG_API_HASH or not TG_SESSION:
        send_alert("❌ tg_parser: не хватает TG_API_ID/TG_API_HASH/TG_SESSION")
        raise SystemExit(1)

    async with aiohttp.ClientSession() as session:
        # если хотите скрывать токены в env — можно вытягивать их из miniapp:
        # example: secret = await fetch_secret(session, "SOME_KEY")
        sources = await fetch_sources(session)

        if not sources:
            send_alert("⚠️ tg_parser: sources пустые — парсить нечего")
            return

        client = TelegramClient(StringSession(TG_SESSION), TG_API_ID, TG_API_HASH)

        await client.connect()
        if not await client.is_user_authorized():
            send_alert("❌ tg_parser: Telegram session не авторизована")
            await client.disconnect()
            return

        send_alert(f"✅ tg_parser started. sources={len(sources)} poll={POLL_INTERVAL_SECONDS}s")

        try:
            while True:
                for s in sources:
                    await process_source(session, client, s)
                await asyncio.sleep(POLL_INTERVAL_SECONDS)
        finally:
            await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
