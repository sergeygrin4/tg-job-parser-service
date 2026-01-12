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
    # ВАЖНО: лучше всегда задавать API_BASE_URL в env.
    API_BASE_URL = "https://miniapptg-production-caaa.up.railway.app"

API_SECRET = os.getenv("API_SECRET", "")

POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "300"))
MESSAGES_LIMIT_PER_SOURCE = int(os.getenv("MESSAGES_LIMIT_PER_SOURCE", "50"))

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
            value = data.get("value")
            return value if value else None
    except Exception:
        return None


async def fetch_sources(session: aiohttp.ClientSession) -> list[str]:
    """
    GET /api/groups
    Ожидаем {"groups":[{"group_id":"..."}, ...]}
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
    sources = []
    for g in groups:
        gid = (g.get("group_id") or "").strip()
        if gid:
            sources.append(gid)

    if sources:
        logger.info("📥 Получено %d Telegram-источников: %s", len(sources), sources)
    else:
        logger.info("📥 Источников в /api/groups не найдено")

    return sources


async def send_post(session: aiohttp.ClientSession, payload: dict):
    """
    POST /post
    """
    url = f"{API_BASE_URL}/post"
    try:
        async with session.post(url, json=payload, headers=_auth_headers(), timeout=15) as resp:
            text = await resp.text()
            if resp.status != 200:
                logger.error("❌ Ошибка /post: %s %s", resp.status, text)
                return
            logger.info("✅ Пост отправлен в миниапп: %s", text)
    except Exception as e:
        logger.error("❌ Ошибка HTTP при отправке поста: %s", e)


def is_relevant_by_keywords(text: str | None) -> bool:
    if not text:
        return False
    t = text.lower()
    return any(kw in t for kw in KEYWORDS_LOWER)


async def parse_source(client: TelegramClient, session: aiohttp.ClientSession, source: str):
    logger.info("🔍 Парсим Telegram источник: %s", source)

    if not client.is_connected():
        logger.warning("⚠️ Клиент Telegram отключён, переподключаем...")
        await client.connect()

    # entity
    try:
        normalized = source.strip()
        if normalized.startswith("https://t.me/"):
            normalized = normalized.replace("https://t.me/", "")
        elif normalized.startswith("http://t.me/"):
            normalized = normalized.replace("http://t.me/", "")
        normalized = normalized.rstrip("/")

        entity = await client.get_entity(normalized)

    except FloodWaitError as e:
        logger.error("⏳ FloodWait при get_entity %s: %s sec", source, e.seconds)
        send_alert(f"Telegram FloodWait (get_entity). Ждать {e.seconds} сек.\n\nИсточник: {source}")
        await asyncio.sleep(e.seconds)
        return

    except RPCError as e:
        logger.error("❌ RPCError при get_entity %s: %s", source, e)
        if "authorization has been invalidated" in str(e).lower():
            send_alert(
                "Telegram парсер потерял авторизацию (authorization invalidated).\n"
                "Нужно пересоздать StringSession и обновить TG_SESSION.\n\n"
                f"Источник: {source}"
            )
        else:
            send_alert(f"Ошибка Telegram парсера при получении entity.\nИсточник: {source}\nОшибка: {e}")
        return

    except Exception as e:
        logger.error("❌ Ошибка при get_entity %s: %s", source, e)
        send_alert(f"Неожиданная ошибка при получении entity.\nИсточник: {source}\nОшибка: {e}")
        return

    # info
    try:
        channel_username = getattr(entity, "username", None)
    except Exception:
        channel_username = None

    try:
        channel_title = getattr(entity, "title", None) or getattr(entity, "first_name", None)
    except Exception:
        channel_title = None

    # messages
    try:
        async for message in client.iter_messages(entity, limit=MESSAGES_LIMIT_PER_SOURCE):
            text = message.message or ""
            if not text:
                continue

            if not is_relevant_by_keywords(text):
                continue

            created_at: datetime = message.date
            if created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=timezone.utc)

            external_id = str(message.id)

            if channel_username:
                msg_link = f"https://t.me/{channel_username}/{message.id}"
            else:
                msg_link = f"https://t.me/{normalized}"

            sender_username = None
            try:
                if message.sender and getattr(message.sender, "username", None):
                    sender_username = message.sender.username
            except Exception:
                sender_username = None

            payload = {
                "source": source,
                "source_name": channel_title,
                "external_id": external_id,
                "url": msg_link,
                "text": text,
                "sender_username": sender_username,
                "created_at": created_at.isoformat(),
            }

            logger.info("📨 Релевантный пост в %s (id=%s) → отправляем", source, external_id)
            await send_post(session, payload)

    except FloodWaitError as e:
        logger.error("⏳ FloodWait при iter_messages %s: %s sec", source, e.seconds)
        send_alert(f"Telegram FloodWait (iter_messages). Ждать {e.seconds} сек.\n\nИсточник: {source}")
        await asyncio.sleep(e.seconds)

    except RPCError as e:
        logger.error("❌ RPCError при iter_messages %s: %s", source, e)
        if "authorization has been invalidated" in str(e).lower():
            send_alert(
                "Telegram парсер потерял авторизацию (authorization invalidated) во время чтения.\n"
                "Нужно пересоздать StringSession.\n\n"
                f"Источник: {source}"
            )
        else:
            send_alert(f"Ошибка Telegram парсера при чтении истории.\nИсточник: {source}\nОшибка: {e}")

    except Exception as e:
        logger.error("❌ Ошибка парсинга %s: %s", source, e)
        send_alert(f"Критическая ошибка Telegram парсера при парсинге.\nИсточник: {source}\nОшибка: {e}")


async def run_loop_async():
    if not API_ID or not API_HASH:
        logger.error("❌ Нет Telegram конфигурации (API_ID/API_HASH), выходим.")
        return

    async def _build_client(session_str: str) -> TelegramClient | None:
        try:
            session_obj = StringSession(session_str)
        except ValueError:
            return None
        return TelegramClient(session_obj, API_ID, API_HASH)

    async def _post_status(http: aiohttp.ClientSession, key: str, value: str):
        url = f"{API_BASE_URL}/api/parser_status/{key}"
        try:
            async with http.post(url, json={"value": value}, headers=_auth_headers(), timeout=10):
                return
        except Exception:
            return

    async with aiohttp.ClientSession() as http:
        # СНАЧАЛА пытаемся взять сессию из миниаппа
        current_session = await fetch_secret(http, "tg_session")
        if current_session:
            logger.info("🔑 Используем TG StringSession из miniapp (длина %d символов)", len(current_session))
        elif SESSION_STRING:
            # Фоллбек: env, если в miniapp ещё ничего нет
            current_session = SESSION_STRING
            logger.warning(
                "⚠️ В miniapp пока нет tg_session, используем TG_SESSION/TELEGRAM_SESSION/SESSION из env (длина %d символов)",
                len(current_session),
            )
        else:
            logger.error("❌ Не удалось получить TG StringSession ни из miniapp, ни из env")
            send_alert(
                "Telegram парсер не стартовал: нет StringSession.\n"
                "Открой миниапп → ⚙️ Настройки → Аккаунты → Telegram сессия и создай/вставь сессию."
            )
            return

        while True:
            client = await _build_client(current_session)
            if not client:
                send_alert(
                    "Telegram парсер: некорректная StringSession.\n"
                    "Пересоздай сессию в миниаппе (⚙️ Настройки → Аккаунты)."
                )
                await asyncio.sleep(60)
                # ещё раз пробуем взять из miniapp
                new_session = await fetch_secret(http, "tg_session")
                if new_session:
                    current_session = new_session
                continue

            try:
                await client.connect()
                logger.info("✅ Подключились к Telegram (connect)")

                if not await client.is_user_authorized():
                    logger.error("❌ Telegram клиент НЕ авторизован (StringSession слетела/не подходит)")
                    send_alert(
                        "Telegram парсер: сессия не авторизована.\n"
                        "Открой миниапп → ⚙️ Настройки → Аккаунты → Telegram сессия и пересоздай её."
                    )
                    await _post_status(http, "tg_auth_required", "true")
                    await asyncio.sleep(60)
                    # пробуем получить новую сессию из miniapp
                    new_session = await fetch_secret(http, "tg_session")
                    if new_session and new_session != current_session:
                        logger.warning("🔄 Получена новая TG StringSession из miniapp после ошибки авторизации")
                        current_session = new_session
                    continue

                await _post_status(http, "tg_auth_required", "false")
                logger.info("✅ Telegram клиент авторизован")

                while True:
                    # проверяем, не обновилась ли сессия в miniapp
                    new_session = await fetch_secret(http, "tg_session")
                    if new_session and new_session != current_session:
                        logger.warning("🔄 TG session обновилась в miniapp — переподключаемся")
                        current_session = new_session
                        break

                    sources = await fetch_sources(http)
                    if not sources:
                        logger.info("ℹ️ Источников нет, спим %s секунд", POLL_INTERVAL_SECONDS)
                        await asyncio.sleep(POLL_INTERVAL_SECONDS)
                        continue

                    for source in sources:
                        try:
                            await parse_source(client, http, source)
                        except Exception as e:
                            logger.error("❌ Ошибка парсинга источника %s: %s", source, e)

                    await _post_status(http, "tg_last_ok", datetime.now(timezone.utc).isoformat())
                    logger.info("⏳ Ждём %s секунд до следующего цикла", POLL_INTERVAL_SECONDS)
                    await asyncio.sleep(POLL_INTERVAL_SECONDS)

            finally:
                try:
                    await client.disconnect()
                except Exception:
                    pass


def main():
    logger.info("🚀 Запуск Telegram Job Parser (без интерактивного логина)")
    asyncio.run(run_loop_async())



if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        send_alert(f"🚨 Критическая ошибка запуска Telegram парсера.\n\nОшибка: {e}")
        raise
