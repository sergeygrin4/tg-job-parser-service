import asyncio
import hashlib
import logging
import os
import random
import re
from datetime import timezone
from typing import Any, Optional

import requests
from telethon import TelegramClient
from telethon.errors import FloodWaitError
from telethon.sessions import StringSession

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - tg_parser - %(levelname)s - %(message)s",
)
logger = logging.getLogger("tg_parser")


# -----------------------------
# ENV helpers
# -----------------------------
def _env_first(*names: str, default: str = "") -> str:
    for n in names:
        v = os.getenv(n)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return default


# -----------------------------
# Miniapp config
# -----------------------------
API_BASE_URL = _env_first(
    "MINIAPP_URL",
    "miniapp_url",
    "API_BASE_URL",
    "API_URL",
    default="",
).rstrip("/")
if not API_BASE_URL:
    raise RuntimeError("MINIAPP_URL/miniapp_url is not set (URL of miniapp service)")


def _get_api_secret() -> str:
    return (
        os.getenv("API_SECRET")
        or os.getenv("MINIAPP_API_SECRET")
        or os.getenv("X_API_KEY")
        or os.getenv("PARSER_API_SECRET")
        or ""
    ).strip()


API_SECRET = _get_api_secret()


def _auth_headers() -> dict:
    headers: dict[str, str] = {}
    if API_SECRET:
        headers["X-API-KEY"] = API_SECRET
        headers["Authorization"] = f"Bearer {API_SECRET}"
    return headers


TG_GROUPS_API_URL = (os.getenv("TG_GROUPS_API_URL") or f"{API_BASE_URL}/api/groups").strip()

TG_API_ID = int(_env_first("TG_API_ID", "TG_API_ID_DEFAULT", "API_ID", default="0") or "0")
TG_API_HASH = _env_first("TG_API_HASH", "TG_API_HASH_DEFAULT", "API_HASH", default="")

# сколько новых сообщений максимум обрабатывать за один цикл на один источник
TG_NEW_MESSAGES_LIMIT = int(_env_first("TG_NEW_MESSAGES_LIMIT", default="50") or "50")

# warm start: чтобы при первом запуске НЕ тащил историю
WARM_START = (_env_first("TG_WARM_START", default="true").lower() in ("1", "true", "yes", "y"))

# polling
POLL_INTERVAL_SECONDS_RAW = _env_first("POLL_INTERVAL_SECONDS", default="").strip()
POLL_INTERVAL_MIN_SECONDS = int(_env_first("POLL_INTERVAL_MIN_SECONDS", default="60") or "60")
POLL_INTERVAL_MAX_SECONDS = int(_env_first("POLL_INTERVAL_MAX_SECONDS", default="180") or "180")

# keywords
JOB_KEYWORDS_RAW = _env_first("JOB_KEYWORDS", default="").strip()


def _parse_keywords(raw: str) -> list[str]:
    # поддержим и запятые, и точки с запятой, и переносы строк
    raw = (raw or "").strip()
    if not raw:
        return []
    parts = re.split(r"[,\n;]+", raw)
    out = []
    for p in parts:
        p = p.strip().lower()
        if not p:
            continue
        out.append(p)
    # uniq preserving order
    return list(dict.fromkeys(out))


JOB_KEYWORDS = _parse_keywords(JOB_KEYWORDS_RAW)


def _next_sleep_seconds() -> int:
    if POLL_INTERVAL_SECONDS_RAW:
        try:
            return max(1, int(POLL_INTERVAL_SECONDS_RAW))
        except Exception:
            return 60
    lo = max(1, int(POLL_INTERVAL_MIN_SECONDS))
    hi = max(lo, int(POLL_INTERVAL_MAX_SECONDS))
    return random.randint(lo, hi)


# -----------------------------
# Optional GPT filter (OpenAI)
# -----------------------------
OPENAI_API_KEY = (os.getenv("OPENAI_API_KEY") or "").strip()
OPENAI_BASE_URL = (os.getenv("OPENAI_BASE_URL") or "https://api.openai.com/v1").rstrip("/")
OPENAI_MODEL = (os.getenv("OPENAI_MODEL") or "gpt-4o-mini").strip()
GPT_ENABLED = bool(OPENAI_API_KEY) and (_env_first("GPT_ENABLED", default="true").lower() in ("1", "true", "yes", "y"))

# ограничим длину текста в GPT, чтобы не жечь токены
GPT_TEXT_MAX = int(_env_first("GPT_TEXT_MAX", default="2500") or "2500")


def gpt_is_relevant(text: str) -> tuple[bool, str]:
    """
    Возвращает (relevant, reason).
    Если GPT выключен — считаем, что релевантно (после keyword-фильтра).
    """
    if not GPT_ENABLED:
        return True, "gpt_disabled"

    t = (text or "").strip()
    if len(t) > GPT_TEXT_MAX:
        t = t[:GPT_TEXT_MAX].rstrip() + "…"

    prompt = (
        "Ты фильтр вакансий.\n"
        "Определи: это СООБЩЕНИЕ — реальная вакансия/поиск сотрудника/заказ на работу?\n"
        "Важное:\n"
        "- Если это просто обсуждение, новости, мемы, болтовня, ссылки без описания — НЕ релевантно.\n"
        "- Если явно ищут сотрудника/исполнителя/ассистента/менеджера и т.п. — релевантно.\n"
        "Ответь СТРОГО JSON без пояснений:\n"
        '{"relevant": true/false, "reason": "коротко почему"}\n\n'
        "Текст:\n"
        f"{t}"
    )

    url = f"{OPENAI_BASE_URL}/chat/completions"
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": OPENAI_MODEL,
        "temperature": 0,
        "messages": [
            {"role": "system", "content": "Ты аккуратно отвечаешь строго валидным JSON."},
            {"role": "user", "content": prompt},
        ],
    }

    try:
        r = requests.post(url, headers=headers, json=payload, timeout=30)
        if r.status_code >= 400:
            return True, f"gpt_http_{r.status_code}"  # чтобы не дропать всё из-за gpt
        data = r.json() or {}
        content = (
            (data.get("choices") or [{}])[0]
            .get("message", {})
            .get("content", "")
            .strip()
        )
        # иногда модель оборачивает в ```json
        content = re.sub(r"^```json\s*", "", content)
        content = re.sub(r"\s*```$", "", content)

        import json as _json

        obj = _json.loads(content)
        rel = bool(obj.get("relevant"))
        reason = str(obj.get("reason") or "").strip()[:200]
        return rel, reason or "ok"
    except Exception as e:
        # если GPT упал — лучше пропустить, чем спамить “всё не релевантно”
        return True, f"gpt_error:{type(e).__name__}"


# -----------------------------
# Miniapp helpers
# -----------------------------
def send_alert(text: str) -> None:
    try:
        r = requests.post(
            f"{API_BASE_URL}/api/alert",
            headers=_auth_headers(),
            json={"text": text, "message": text, "source": "tg_parser"},
            timeout=10,
        )
        if r.status_code >= 400:
            logger.error("❌ /api/alert failed http=%s body=%s", r.status_code, r.text[:800])
    except Exception:
        logger.exception("❌ /api/alert exception")


def post_status(key: str, value: str) -> None:
    try:
        r = requests.post(
            f"{API_BASE_URL}/api/parser_status/{key}",
            json={"value": value},
            headers=_auth_headers(),
            timeout=10,
        )
        if r.status_code >= 400:
            logger.error("❌ /api/parser_status/%s failed http=%s body=%s", key, r.status_code, r.text[:800])
    except Exception:
        logger.exception("❌ /api/parser_status exception")


def fetch_tg_session_from_miniapp() -> str:
    """miniapp endpoint: GET /api/parser_secrets/tg_session -> {value: "..."}"""
    if not API_SECRET:
        return ""
    url = f"{API_BASE_URL}/api/parser_secrets/tg_session"
    try:
        r = requests.get(url, headers=_auth_headers(), timeout=10)
        if r.status_code >= 400:
            return ""
        data = r.json() or {}
        return (data.get("value") or "").strip()
    except Exception:
        return ""


def _looks_like_telegram(raw: str) -> bool:
    s = (raw or "").strip().lower()
    if not s:
        return False
    if s.startswith("@"):
        return True
    if "t.me/" in s or "telegram.me/" in s:
        return True
    if re.fullmatch(r"[a-zA-Z0-9_]{4,}", s):
        return True
    if re.fullmatch(r"-?\d+", s):
        return True
    return False


def fetch_telegram_sources() -> list[str]:
    try:
        r = requests.get(TG_GROUPS_API_URL, headers=_auth_headers(), timeout=10)
        if r.status_code >= 400:
            logger.error("❌ groups fetch failed http=%s body=%s", r.status_code, r.text[:300])
            return []
        data = r.json() or {}
        groups = data.get("groups") or []
    except Exception as e:
        logger.error("❌ groups fetch exception: %s", e)
        return []

    out: list[str] = []
    for g in groups:
        if not isinstance(g, dict):
            continue
        if not g.get("enabled", True):
            continue
        t = (g.get("type") or "").lower().strip()
        if t and t != "telegram":
            continue
        raw = (g.get("group_id") or "").strip()
        if not raw:
            continue
        if not _looks_like_telegram(raw):
            continue
        out.append(raw)

    return list(dict.fromkeys(out))


def _normalize_tg_source(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return ""

    m = re.search(r"(?:t\.me|telegram\.me)/([a-zA-Z0-9_+\-]+)/?", s)
    if m:
        tail = m.group(1)
        if tail.startswith("+"):
            # invite link: без join обычно не вытащим. Оставляем как есть.
            return s
        return "@" + tail.lstrip("@")

    if re.fullmatch(r"[a-zA-Z0-9_]{4,}", s) and not s.startswith("@"):
        return "@" + s

    return s


def _external_id_from_message(msg: Any) -> str:
    pid = None
    try:
        peer = getattr(msg, "peer_id", None)
        if peer is not None:
            pid = getattr(peer, "channel_id", None) or getattr(peer, "chat_id", None) or getattr(peer, "user_id", None)
    except Exception:
        pid = None
    return f"{pid or 'unknown'}:{getattr(msg, 'id', None)}"


def _hash_fallback(text: str, created_at: Optional[str]) -> str:
    base = (text or "").strip() + "|" + (created_at or "")
    return hashlib.sha256(base.encode("utf-8", "ignore")).hexdigest()


def send_job_to_miniapp(
    text: str,
    external_id: str,
    url: Optional[str],
    created_at: Optional[str],
    source_name: str,
    sender_username: Optional[str],
) -> None:
    payload = {
        "source": "telegram",
        "source_name": source_name,
        "external_id": external_id or _hash_fallback(text, created_at),
        "url": url,
        "text": text,
        "sender_username": sender_username,
        "created_at": created_at,
    }

    r = requests.post(f"{API_BASE_URL}/post", json=payload, headers=_auth_headers(), timeout=30)
    if r.status_code != 200:
        logger.error("❌ /post failed: http=%s body=%s", r.status_code, r.text[:800])
        send_alert(f"TG parser: /post failed\nHTTP {r.status_code}\n{r.text[:800]}")
        r.raise_for_status()


# -----------------------------
# Keyword match
# -----------------------------
def _keyword_match(text: str) -> bool:
    if not JOB_KEYWORDS:
        return False  # важно: если keywords не заданы — НИЧЕГО не шлём (иначе будет “весь чат”)
    t = (text or "").lower()
    return any(k in t for k in JOB_KEYWORDS)


# -----------------------------
# Parsing
# -----------------------------
async def _parse_one_source(
    client: TelegramClient,
    source_raw: str,
    last_ids: dict[str, int],
) -> tuple[int, int, int]:
    """
    Возвращает (new_msgs_seen, matched_by_keywords, sent_after_gpt)
    """
    source = _normalize_tg_source(source_raw)
    if not source:
        return 0, 0, 0

    try:
        entity = await client.get_entity(source)
    except Exception as e:
        logger.warning("⚠️ cannot resolve source=%r: %s", source_raw, e)
        return 0, 0, 0

    username = getattr(entity, "username", None)
    title = getattr(entity, "title", None) or getattr(entity, "first_name", None) or source_raw
    source_name = str(title)

    # ключ для словаря last_ids должен быть стабильный
    entity_key = source_name + "|" + (username or source)

    # warm start: при первом запуске — запоминаем текущий top id и НЕ шлём историю
    if entity_key not in last_ids and WARM_START:
        try:
            latest = await client.get_messages(entity, limit=1)
            if latest and latest[0]:
                last_ids[entity_key] = int(latest[0].id)
                logger.info("🔥 warm_start: %s last_id=%s", source_name, last_ids[entity_key])
                return 0, 0, 0
        except Exception:
            last_ids[entity_key] = 0

    min_id = int(last_ids.get(entity_key, 0) or 0)

    new_seen = 0
    kw_matched = 0
    sent = 0
    max_id_seen = min_id

    try:
        # reverse=True => от старых к новым (удобно обновлять last_id)
        async for msg in client.iter_messages(entity, min_id=min_id, limit=TG_NEW_MESSAGES_LIMIT, reverse=True):
            new_seen += 1
            if msg.id and int(msg.id) > max_id_seen:
                max_id_seen = int(msg.id)

            text = (getattr(msg, "message", None) or getattr(msg, "text", None) or "").strip()
            if not text:
                continue

            if not _keyword_match(text):
                continue
            kw_matched += 1

            # GPT refine
            ok, reason = gpt_is_relevant(text)
            if not ok:
                logger.info("🧹 gpt_drop (%s): %s", source_name, reason)
                continue

            dt = getattr(msg, "date", None)
            created_at = None
            if dt:
                try:
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    created_at = dt.astimezone(timezone.utc).isoformat()
                except Exception:
                    created_at = None

            url = None
            if username:
                url = f"https://t.me/{username}/{msg.id}"

            sender_username = None
            try:
                sender = await msg.get_sender()
                sender_username = getattr(sender, "username", None)
                if sender_username:
                    sender_username = "@" + sender_username.lstrip("@")
            except Exception:
                pass

            ext_id = _external_id_from_message(msg)
            send_job_to_miniapp(
                text=text,
                external_id=ext_id,
                url=url,
                created_at=created_at,
                source_name=source_name,
                sender_username=sender_username,
            )
            sent += 1

    except FloodWaitError as e:
        wait_s = int(getattr(e, "seconds", 0) or 0)
        logger.warning("⏳ FloodWait on %s: %ss", source_name, wait_s)
        await asyncio.sleep(wait_s + 1)
    except Exception as e:
        logger.warning("⚠️ parse failed (%s): %s", source_name, e)

    # обновляем last_id даже если ничего не отправили — иначе снова будем гонять одно и то же
    last_ids[entity_key] = max(max_id_seen, min_id)
    return new_seen, kw_matched, sent


async def main() -> None:
    if not TG_API_ID or not TG_API_HASH:
        raise RuntimeError("TG_API_ID/TG_API_HASH is not set")

    if not JOB_KEYWORDS:
        msg = (
            "TG parser: JOB_KEYWORDS пустой — чтобы не спамить, парсер НИЧЕГО не будет отправлять.\n"
            "Заполни JOB_KEYWORDS в Railway (через запятую)."
        )
        logger.error(msg)
        post_status("tg_parser", "no_keywords")
        send_alert(msg)

    last_ids: dict[str, int] = {}

    last_session = ""
    client: TelegramClient | None = None

    logger.info("✅ tg_parser started (warm_start=%s, keywords=%s, gpt=%s)", WARM_START, len(JOB_KEYWORDS), GPT_ENABLED)
    send_alert("✅ tg_parser started.")

    while True:
        session_str = fetch_tg_session_from_miniapp() or _env_first("TG_SESSION", "TELEGRAM_SESSION", "SESSION", default="")
        session_str = (session_str or "").strip()

        if not session_str:
            msg = (
                "TG parser: StringSession пустая.\n"
                "Открой миниапп → ⚙️ Настройки → Telegram и создай/вставь StringSession."
            )
            logger.error(msg)
            post_status("tg_parser", "no_session")
            send_alert(msg)
            await asyncio.sleep(60)
            continue

        if client is None or session_str != last_session:
            if client is not None:
                try:
                    await client.disconnect()
                except Exception:
                    pass
            client = TelegramClient(StringSession(session_str), TG_API_ID, TG_API_HASH)
            await client.connect()
            last_session = session_str

        assert client is not None

        try:
            if not await client.is_user_authorized():
                msg = "TG parser: текущая StringSession не авторизована (not_authorized)."
                logger.error(msg)
                post_status("tg_parser", "not_authorized")
                send_alert(msg)
                await asyncio.sleep(120)
                continue

            sources = fetch_telegram_sources()
            if not sources:
                logger.warning("⚠️ sources пустые — парсить нечего")
                post_status("tg_parser", "no_sources")
            else:
                total_new = 0
                total_kw = 0
                total_sent = 0

                # если keywords пустые — просто читаем новые ids, но не шлём
                if not JOB_KEYWORDS:
                    for s in sources:
                        await _parse_one_source(client, s, last_ids)
                    post_status("tg_parser", f"no_keywords sources={len(sources)}")
                else:
                    for s in sources:
                        new_seen, kw_matched, sent = await _parse_one_source(client, s, last_ids)
                        total_new += new_seen
                        total_kw += kw_matched
                        total_sent += sent

                    post_status("tg_parser", f"ok new={total_new} kw={total_kw} sent={total_sent} sources={len(sources)}")
                    logger.info("✅ cycle: new=%s kw=%s sent=%s sources=%s", total_new, total_kw, total_sent, len(sources))

        except FloodWaitError as e:
            wait_s = int(getattr(e, "seconds", 0) or 0)
            logger.warning("⏳ FloodWait (global): %ss", wait_s)
            post_status("tg_parser", f"flood_wait {wait_s}s")
            await asyncio.sleep(wait_s + 1)
        except Exception as e:
            logger.exception("❌ unexpected error")
            post_status("tg_parser", "error")
            send_alert(f"TG parser error: {e}")

        sleep_s = _next_sleep_seconds()
        logger.info("⏲️ sleep %ss", sleep_s)
        await asyncio.sleep(sleep_s)


if __name__ == "__main__":
    asyncio.run(main())
