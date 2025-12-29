import asyncio
import logging
import os
from datetime import datetime, timezone

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

# Пытаемся взять API_ID / API_HASH из разных вариантов переменных
API_ID = int(os.getenv("TG_API_ID") or os.getenv("API_ID") or "0")
API_HASH = os.getenv("TG_API_HASH") or os.getenv("API_HASH") or ""

# Пытаемся взять строку сессии из нескольких имён:
# TG_SESSION / TELEGRAM_SESSION / SESSION
SESSION_STRING = (
    os.getenv("TG_SESSION")
    or os.getenv("TELEGRAM_SESSION")
    or os.getenv("SESSION")
    or ""
)

API_BASE_URL = (os.getenv("API_BASE_URL") or "").rstrip("/")
if not API_BASE_URL:
    # пример: https://telegram-job-parser-production.up.railway.app
    API_BASE_URL = "https://telegram-job-parser-production.up.railway.app"

API_SECRET = os.getenv("API_SECRET", "")

POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "300"))  # дефолт 5 минут
MESSAGES_LIMIT_PER_SOURCE = int(os.getenv("MESSAGES_LIMIT_PER_SOURCE", "50"))

if not API_ID or not API_HASH:
    logger.error("❌ TG_API_ID/API_ID или TG_API_HASH/API_HASH не заданы в переменных окружения")

if not SESSION_STRING:
    logger.error(
        "❌ Не найдена строка сессии Telegram. "
        "Установи одну из переменных: TG_SESSION, TELEGRAM_SESSION или SESSION"
    )

# ---------- КЛЮЧЕВЫЕ СЛОВА ----------

KEYWORDS = [
    # Русские
    "вакансия", "вакансии", "ищем", "требуется", "нужен сотрудник", "нужна помощь", "нужен человек",
    "нужен помощник", "нужна помощница", "нужен ассистент", "нужен менеджер", "ищу исполнителя",
    "ищу помощника", "ищу сотрудника", "ищу ассистента", "в команду", "в нашу команду", "к нам в команду",
    "открыта вакансия", "открыт набор", "открыта позиция", "работа удалённо", "удалённая работа",
    "удаленка", "фриланс", "ищу на фриланс", "ищу специалиста", "ищу человека", "ищем специалиста",
    "ищем в команду", "хочу нанять", "возьму на проект", "нужен человек в проект", "ищем на проект",
    "набор сотрудников", "расширяем команду",
    # Английские
    "we are hiring", "hiring", "looking for", "we’re looking for", "need help with", "need a person",
    "need an assistant", "looking for a team member", "freelancer needed", "remote position",
    "job offer", "job opening", "open position", "apply now", "join our team", "recruiting",
    "team expansion", "full-time", "part-time", "contractor", "long-term collaboration",
    "replacement guarantee", "if you have an account", "account needed", "account required",
    "contact me on telegram", "please contact me",
]
KEYWORDS_LOWER = [k.lower() for k in KEYWORDS]


# ---------- HTTP-УТИЛИТЫ ----------

def _headers():
    headers = {"Content-Type": "application/json"}
    if API_SECRET:
        headers["X-API-KEY"] = API_SECRET
    return headers


async def fetch_sources(session: aiohttp.ClientSession) -> list[str]:
    """
    Берём источники из миниаппа: GET /api/groups
    Ожидаем ответ вида: {"groups": [{"group_id": "...", ...}, ...]}
    """
    url = f"{API_BASE_URL}/api/groups"
    try:
        async with session.get(url, timeout=10) as resp:
            if resp.status != 200:
                logger.error("❌ Ошибка запроса /api/groups: %s %s", resp.status, await resp.text())
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
        logger.info("📥 Получено %d Telegram-источников из БД: %s", len(sources), sources)
    else:
        logger.info("📥 Источников из /api/groups не найдено")

    return sources


async def send_post(session: aiohttp.ClientSession, payload: dict):
    """
    Отправка вакансии в миниапп: POST /post
    Миниапп уже сам делает AI-фильтр и дубликаты.
    """
    url = f"{API_BASE_URL}/post"
    try:
        async with session.post(url, json=payload, headers=_headers(), timeout=15) as resp:
            text = await resp.text()
            if resp.status != 200:
                logger.error("❌ Ошибка отправки поста (%s): %s %s", url, resp.status, text)
                return
            logger.info("✅ Пост отправлен в миниапп: %s", text)
    except Exception as e:
        logger.error("❌ Ошибка HTTP при отправке поста: %s", e)


# ---------- ЛОГИКА ФИЛЬТРАЦИИ ----------

def is_relevant_by_keywords(text: str | None) -> bool:
    if not text:
        return False
    t = text.lower()
    return any(kw in t for kw in KEYWORDS_LOWER)


# ---------- АЛЕРТЫ ДЛЯ ТГ ----------

def send_alert(text: str):
    try:
        requests.post(
            f"{API_BASE_URL}/api/alert",
            headers={"X-API-SECRET": API_SECRET},
            json={
                "source": "tg_parser",
                "message": text,
            },
            timeout=10,
        )
    except Exception:
        pass



# ---------- ПАРСИНГ ОДНОГО ИСТОЧНИКА ----------

async def parse_source(client: TelegramClient, session: aiohttp.ClientSession, source: str):
    logger.info("🔍 Парсим Telegram источник: %s", source)

    # Если клиент по какой-то причине отвалился — переподключаем
    if not client.is_connected():
        logger.warning("⚠️ Клиент Telegram отключён, пробуем подключиться заново...")
        await client.connect()
        if not await client.is_user_authorized():
            logger.error("❌ Клиент Telegram не авторизован после переподключения")

            send_alert(
                "Telegram парсер потерял авторизацию.\n"
                "Клиент не авторизован после переподключения.\n"
                "Нужно перелогиниться и пересоздать session.\n\n"
                f"Источник: {source}"
            )
            return

    # ---------- Получение entity ----------
    try:
        normalized = source.strip()
        if normalized.startswith("https://t.me/"):
            normalized = normalized.replace("https://t.me/", "")
        elif normalized.startswith("http://t.me/"):
            normalized = normalized.replace("http://t.me/", "")
        normalized = normalized.rstrip("/")

        if normalized and not normalized.startswith("@"):
            normalized_for_entity = normalized
        else:
            normalized_for_entity = normalized

        entity = await client.get_entity(normalized_for_entity)

    except FloodWaitError as e:
        logger.error(
            "⏳ FloodWaitError при получении entity %s: нужно подождать %s секунд",
            source,
            e.seconds,
        )

        send_alert(
            "Telegram временно ограничил запросы (FloodWait).\n"
            f"Нужно подождать {e.seconds} секунд.\n\n"
            f"Источник: {source}"
        )

        await asyncio.sleep(e.seconds)
        return

    except RPCError as e:
        logger.error(
            "❌ RPCError при получении entity %s: %s",
            source,
            e,
        )

        error_text = str(e).lower()

        if "authorization has been invalidated" in error_text:
            send_alert(
                "Telegram парсер потерял авторизацию.\n"
                "Аккаунт выбило из сессии.\n"
                "Нужно перелогиниться и пересоздать session.\n\n"
                f"Источник: {source}"
            )
        else:
            send_alert(
                "Ошибка Telegram парсера при получении entity.\n\n"
                f"Источник: {source}\n"
                f"Ошибка: {e}"
            )

        return

    except Exception as e:
        logger.error(
            "❌ Ошибка при получении entity для %s: %s",
            source,
            e,
        )

        send_alert(
            "Неожиданная ошибка Telegram парсера при получении entity.\n\n"
            f"Источник: {source}\n"
            f"Ошибка: {e}"
        )

        return

    # ---------- Данные источника ----------
    try:
        channel_username = getattr(entity, "username", None)
    except Exception:
        channel_username = None

    try:
        channel_title = getattr(entity, "title", None) or getattr(entity, "first_name", None)
    except Exception:
        channel_title = None

    # ---------- Чтение сообщений ----------
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
                if source.startswith("http://") or source.startswith("https://"):
                    msg_link = source
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

            logger.info(
                "📨 Найден релевантный пост в %s (id=%s), отправляем в миниапп",
                source,
                external_id,
            )

            await send_post(session, payload)

    except FloodWaitError as e:
        logger.error(
            "⏳ FloodWaitError при чтении истории %s: нужно подождать %s секунд",
            source,
            e.seconds,
        )

        send_alert(
            "Telegram временно ограничил чтение истории (FloodWait).\n"
            f"Нужно подождать {e.seconds} секунд.\n\n"
            f"Источник: {source}"
        )

        await asyncio.sleep(e.seconds)

    except RPCError as e:
        logger.error(
            "❌ RPCError при чтении истории %s: %s",
            source,
            e,
        )

        send_alert(
            "Ошибка Telegram парсера при чтении истории.\n\n"
            f"Источник: {source}\n"
            f"Ошибка: {e}"
        )

    except Exception as e:
        logger.error(
            "❌ Неожиданная ошибка при парсинге источника %s: %s",
            source,
            e,
        )

        send_alert(
            "Критическая ошибка Telegram парсера при парсинге источника.\n\n"
            f"Источник: {source}\n"
            f"Ошибка: {e}"
        )


# ---------- ОСНОВНОЙ ЦИКЛ ----------

async def run_loop_async():
    if not API_ID or not API_HASH:
        logger.error("❌ Нет конфигурации Telegram клиента (API_ID/API_HASH), выходим.")
        return

    if not SESSION_STRING:
        logger.error(
            "❌ Нет строки сессии Telegram. "
            "Установи TG_SESSION / TELEGRAM_SESSION / SESSION в Railway (StringSession)."
        )
        return

    # Пытаемся создать StringSession, если строка кривая — не падаем в traceback, а логируем
    try:
        session_obj = StringSession(SESSION_STRING)
    except ValueError:
        logger.error(
            "❌ Некорректная строка сессии Telegram. "
            "Переменная TG_SESSION/TELEGRAM_SESSION/SESSION не является валидным StringSession. "
            "Нужно заново сгенерировать StringSession локально через Telethon."
        )
        return

    client = TelegramClient(session_obj, API_ID, API_HASH)

    async with client:
        await client.connect()

        if not await client.is_user_authorized():
            logger.error("❌ Telegram клиент не авторизован. Проверь StringSession / API_ID / API_HASH")
            return

        logger.info("✅ Telegram клиент подключён и авторизован")

        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    sources = await fetch_sources(session)
                    if not sources:
                        logger.info("ℹ️ Источников нет, спим %s секунд", POLL_INTERVAL_SECONDS)
                        await asyncio.sleep(POLL_INTERVAL_SECONDS)
                        continue

                    for source in sources:
                        try:
                            await parse_source(client, session, source)
                        except Exception as e:
                            logger.error("❌ Ошибка при парсинге источника %s: %s", source, e)

                    logger.info("⏳ Ждём %s секунд до следующего цикла", POLL_INTERVAL_SECONDS)
                    await asyncio.sleep(POLL_INTERVAL_SECONDS)

                except Exception as e:
                    logger.error("❌ Неожиданная ошибка в основном цикле: %s", e)
                    await asyncio.sleep(10)


def main():
    logger.info("🚀 Запуск Telegram Job Parser")
    asyncio.run(run_loop_async())


if __name__ == "__main__":
    try:
        main()
    except EOFError as e:
        send_alert(
            "🚨 Telegram парсер потерял сессию.\n\n"
            "Telegram выбил аккаунт из всех сессий.\n"
            "Telethon попытался запросить телефон,\n"
            "но это headless-среда (Railway).\n\n"
            "❗ Требуется действие:\n"
            "- пересоздать Telegram StringSession\n"
            "- обновить TG_STRING_SESSION в Railway\n"
        )
        raise
    except Exception as e:
        send_alert(
            "🚨 Критическая ошибка запуска Telegram парсера.\n\n"
            f"Ошибка: {e}"
        )
        raise

