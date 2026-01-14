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

# ---------- КОНФИГ И ОКРУЖЕНИЕ ----------

API_ID = int(os.getenv("TG_API_ID") or os.getenv("API_ID") or "0")
API_HASH = os.getenv("TG_API_HASH") or os.getenv("API_HASH") or ""

SESSION_STRING = (
    os.getenv("TG_SESSION")
    or os.getenv("TELEGRAM_SESSION")
    or os.getenv("SESSION")
    or ""
)

API_BASE_URL = (os.getenv("API_BASE_URL") or "").rstrip("/")
if not API_BASE_URL:
    API_BASE_URL = "https://telegram-job-parser-production.up.railway.app"

API_SECRET = (os.getenv("API_SECRET") or "").strip()

POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "300"))
MESSAGES_LIMIT_PER_SOURCE = int(os.getenv("MESSAGES_LIMIT_PER_SOURCE", "50"))
MAX_TEXT_LEN = int(os.getenv("MAX_TEXT_LEN", "3500"))

if not API_ID or not API_HASH:
    logger.error("❌ TG_API_ID/API_ID или TG_API_HASH/API_HASH не заданы в переменных окружения")

if not SESSION_STRING:
    logger.warning(
        "⚠️ TG_SESSION/TELEGRAM_SESSION/SESSION не задана. "
        "Попробуем взять StringSession из miniapp (/api/parser_secrets/tg_session)."
    )

# ---------- КЛЮЧЕВЫЕ СЛОВА ----------

KEYWORDS = [
    # RU
    "вакансия", "вакансии", "ищем", "требуется", "нужен сотрудник", "нужна помощь", "нужен человек",
    "нужен помощник", "нужна помощница", "нужен ассистент", "нужен менеджер", "ищу исполнителя",
    "ищу помощника", "ищу сотрудника", "ищу ассистента", "в команду", "в нашу команду", "к нам в команду",
    "открыта вакансия", "открыт набор", "открыта позиция", "работа удалённо", "удалённая работа",
    "удаленка", "фриланс", "ищу на фриланс", "ищу специалиста", "ищу человека", "ищем специалиста",
    "ищем в команду", "хочу нанять", "возьму на проект", "нужен человек в проект", "ищем на проект",
    "набор сотрудников", "расширяем команду",
    # EN
    "we are hiring", "hiring", "looking for", "we’re looking for", "need help with", "need a person",
    "need an assistant", "looking for a team member", "freelancer needed", "remote position",
    "job offer", "job opening", "open position", "apply now", "join our team", "recruiting",
    "team expansion", "full-time", "part-time", "contractor", "long-term collaboration",
    "replacement guarantee", "if you have an account", "account needed", "account required",
    "contact me on telegram", "please contact me",
]
KEYWORDS_LOWER = [k.lower() for k in KEYWORDS]

# ---------- HTTP-УТИЛИТЫ ----------

def _auth_headers() -> dict:
    headers = {"Content-Type": "application/json"}
    if API_SECRET:
        headers["X-API-KEY"] = API_SECRET
        headers["Authorization"] = f"Bearer {API_SECRET}"
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
                logger.error("❌ Ошибка /api/parser_secrets/%s: %s %s", key, resp.status, await resp.text())
                return None
            data = await resp.json()
            value = data.get("value")
            return value if value else None
    except Exception as e:
        logger.error("❌ Не удалось получить секрет %s из %s: %s", key, url, e)
        return None


def _is_telegram_source(group_id: str) -> bool:
    if not group_id:
        return False
    s = group_id.strip()
    if not s:
        return False
    lower = s.lower()
    if "facebook.com" in lower or "fb.com" in lower:
        return False
    if s.startswith("@"):
        return True
    if "t.me/" in lower or "telegram.me/" in lower:
        return True
    return False


async def fetch_sources(session: aiohttp.ClientSession) -> list[str]:
    """
    GET /api/groups
    Ожидаем {"groups":[{"group_id":"..."}, ...]}
    Берём только Telegram-источники, Facebook и прочие выкидываем.
    """
    url = f"{API_BASE_URL}/api/groups"
    try:
        async with session.get(url, timeout=10) as resp:
            if resp.status != 200:
                logger.error("❌ Ошибка /api/groups: %s %s", resp.status, await resp.text())
                return []
            data = await resp.json()
    except Exception as e:
        logger.error("❌ Не удалось получить источники из %s: %s", url, e)
        return []

    groups = data.get("groups") or []
    sources: list[str] = []
    skipped: list[str] = []

    for g in groups:
        gid = (g.get("group_id") or "").strip()
        if not gid:
            continue
        if _is_telegram_source(gid):
            sources.append(gid)
        else:
            skipped.append(gid)

    if skipped:
        logger.info("ℹ️ Пропущены не-Telegram источники (например FB): %s", skipped)

    if sources:
        logger.info("📥 Получено %d Telegram-источников: %s", len(sources), sources)
    else:
        logger.info("📥 Telegram-источников в /api/groups не найдено")

    return sources


async def send_post(session: aiohttp.ClientSession, payload: dict):
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


def is_relevant_by_keywords(text: str) -> bool:
    if not text:
        return False
    t = text.lower()
    return any(kw in t for kw in KEYWORDS_LOWER)


async def parse_source(client: TelegramClient, session: aiohttp.ClientSession, source: str):
    logger.info("🔍 Парсим Telegram источник: %s", source)

    if not client.is_connected():
        logger.warning("⚠️ Клиент Telegram отключён, переподключаем...")
        await client.connect()

    try:
        entity = await client.get_entity(source)
    except Exception as e:
        logger.error("❌ Не удалось получить entity %s: %s", source, e)
        return

    try:
        messages = await client.get_messages(entity, limit=MESSAGES_LIMIT_PER_SOURCE)
    except FloodWaitError as e:
        logger.warning("⏳ FloodWait %s sec for %s", e.seconds, source)
        await asyncio.sleep(e.seconds + 1)
        return
    except RPCError as e:
        logger.error("❌ RPCError get_messages %s: %s", source, e)
        return
    except Exception as e:
        logger.error("❌ Ошибка get_messages %s: %s", source, e)
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

            if not is_relevant_by_keywords(text):
                continue

            url = ""
            try:
                if getattr(entity, "username", None):
                    url = f"https://t.me/{entity.username}/{msg.id}"
            except Exception:
                url = ""

            payload = {
                "source": "telegram",
                "source_name": source,
                "external_id": f"{source}:{msg.id}",
                "url": url,
                "text": text,
                "sender_username": "",
                "created_at": msg.date.astimezone(timezone.utc).isoformat() if getattr(msg, "date", None) else None,
            }

            await send_post(session, payload)

        except Exception as e:
            logger.error("❌ Ошибка обработки сообщения: %s", e)


async def main():
    if not API_ID or not API_HASH:
        send_alert("❌ tg_parser: не хватает TG_API_ID/TG_API_HASH")
        raise SystemExit(1)

    async with aiohttp.ClientSession() as session:
        global SESSION_STRING
        if not SESSION_STRING:
            SESSION_STRING = await fetch_secret(session, "tg_session") or ""

        if not SESSION_STRING:
            send_alert("❌ tg_parser: TG_SESSION пустая и tg_session не найден в miniapp")
            raise SystemExit(1)

        sources = await fetch_sources(session)
        if not sources:
            send_alert("⚠️ tg_parser: sources пустые — парсить нечего")
            return

        client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

        await client.connect()
        if not await client.is_user_authorized():
            send_alert("❌ tg_parser: Telegram session не авторизована")
            await client.disconnect()
            return

        send_alert(f"✅ tg_parser started. sources={len(sources)} poll={POLL_INTERVAL_SECONDS}s")

        try:
            while True:
                for s in sources:
                    await parse_source(client, session, s)
                await asyncio.sleep(POLL_INTERVAL_SECONDS)
        finally:
            await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
