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
# Config / ENV (same style as fb_parser)
# -----------------------------
def _env_first(*names: str, default: str = "") -> str:
    for n in names:
        v = os.getenv(n)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return default


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


TG_API_ID = int(_env_first("TG_API_ID", "TG_API_ID_DEFAULT", "API_ID", default="0") or "0")
TG_API_HASH = _env_first("TG_API_HASH", "TG_API_HASH_DEFAULT", "API_HASH", default="")

TG_GROUPS_API_URL = (os.getenv("TG_GROUPS_API_URL") or f"{API_BASE_URL}/api/groups").strip()

POLL_INTERVAL_SECONDS_RAW = _env_first("POLL_INTERVAL_SECONDS", default="").strip()
POLL_INTERVAL_MIN_SECONDS = int(_env_first("POLL_INTERVAL_MIN_SECONDS", default="60") or "60")
POLL_INTERVAL_MAX_SECONDS = int(_env_first("POLL_INTERVAL_MAX_SECONDS", default="180") or "180")

TG_MESSAGE_LIMIT = int(_env_first("TG_MESSAGE_LIMIT", default="30") or "30")


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
    # allow bare channel/group usernames without @
    if re.fullmatch(r"[a-zA-Z0-9_]{4,}", s):
        return True
    # numeric ids can exist, but in miniapp we rarely store them
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

    # unique preserving order
    return list(dict.fromkeys(out))


def _normalize_tg_source(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return ""

    # full url -> keep only username part when possible
    m = re.search(r"(?:t\.me|telegram\.me)/([a-zA-Z0-9_+\-]+)/?", s)
    if m:
        tail = m.group(1)
        # t.me/+xxxxx is an invite (cannot fetch without join)
        if tail.startswith("+"):
            return s
        return "@" + tail.lstrip("@")

    # username without @
    if re.fullmatch(r"[a-zA-Z0-9_]{4,}", s) and not s.startswith("@"):
        return "@" + s

    return s


def _external_id_from_message(msg: Any) -> str:
    # stable across runs: peer + message id
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
# Telegram parsing
# -----------------------------
async def _parse_one_source(client: TelegramClient, source_raw: str, seen: set[str]) -> int:
    source = _normalize_tg_source(source_raw)
    if not source:
        return 0

    try:
        entity = await client.get_entity(source)
    except Exception as e:
        logger.warning("⚠️ cannot resolve source=%r: %s", source_raw, e)
        return 0

    username = getattr(entity, "username", None)
    title = getattr(entity, "title", None) or getattr(entity, "first_name", None) or source_raw
    source_name = title

    try:
        msgs = await client.get_messages(entity, limit=TG_MESSAGE_LIMIT)
    except FloodWaitError as e:
        wait_s = int(getattr(e, "seconds", 0) or 0)
        logger.warning("⏳ FloodWait on get_messages (%s): %ss", source_raw, wait_s)
        await asyncio.sleep(wait_s + 1)
        return 0
    except Exception as e:
        logger.warning("⚠️ get_messages failed (%s): %s", source_raw, e)
        return 0

    new_count = 0
    for msg in msgs:
        text = (getattr(msg, "message", None) or getattr(msg, "text", None) or "").strip()
        if not text:
            continue

        ext_id = _external_id_from_message(msg)
        if ext_id in seen:
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

        send_job_to_miniapp(
            text=text,
            external_id=ext_id,
            url=url,
            created_at=created_at,
            source_name=str(source_name),
            sender_username=sender_username,
        )
        seen.add(ext_id)
        new_count += 1

    return new_count


async def main() -> None:
    if not TG_API_ID or not TG_API_HASH:
        raise RuntimeError("TG_API_ID/TG_API_HASH is not set")

    seen: set[str] = set()
    seen_max = int(_env_first("SEEN_MAX", default="8000") or "8000")

    last_session = ""
    client: TelegramClient | None = None

    logger.info(
        "✅ tg_parser started (poll=%ss, limit=%s)",
        POLL_INTERVAL_SECONDS_RAW or f"{POLL_INTERVAL_MIN_SECONDS}-{POLL_INTERVAL_MAX_SECONDS}",
        TG_MESSAGE_LIMIT,
    )
    send_alert("✅ tg_parser started.")

    while True:
        # refresh session from miniapp (so admin can rotate it without redeploy)
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

        # (re)connect client if session changed
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
                for s in sources:
                    total_new += await _parse_one_source(client, s, seen)

                post_status("tg_parser", f"ok new={total_new} sources={len(sources)}")
                logger.info("✅ parsed: new=%s sources=%s", total_new, len(sources))

                # basic seen set cap
                if len(seen) > seen_max:
                    drop_n = max(1, len(seen) // 4)
                    for x in list(seen)[:drop_n]:
                        seen.discard(x)

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
