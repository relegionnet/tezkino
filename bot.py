import asyncio
import html
import logging
import os
import re
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import aiosqlite
from aiogram import BaseMiddleware, Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ChatMemberStatus, ChatType, ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter
from aiogram.filters import BaseFilter, Command, CommandObject, CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup, default_state
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    BotCommand,
    BotCommandScopeAllGroupChats,
    BotCommandScopeChat,
    CallbackQuery,
    ErrorEvent,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    LabeledPrice,
    Message,
    PreCheckoutQuery,
    ReplyKeyboardMarkup,
)
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)

# ============================ SOZLAMALAR ============================
BOT_TOKEN = os.getenv("BOT_TOKEN", "")


def _parse_ids(raw: str) -> list:
    out = []
    for x in raw.replace(";", ",").split(","):
        x = x.strip()
        if x.lstrip("-").isdigit():
            out.append(int(x))
        elif x:
            logging.warning("ADMINS ichida noto'g'ri qiymat o'tkazib yuborildi: %r", x)
    return out


ADMINS = _parse_ids(os.getenv("ADMINS", ""))
# Baza doim bot.py yonida turadi (qaysi papkadan ishga tushirilmasin)
DB_PATH = str(BASE_DIR / "kino.db")
SUB_CACHE_TTL = 60  # soniya: obuna bo'lgan foydalanuvchi qayta tekshirilmaydi
CAPTION_LIMIT = 1024  # Telegram caption chegarasi

# ---- Premium (pullik kinolar) ----
# Narxlar so'mda. To'lov Click orqali (Humo karta). Xohlaganingizcha o'zgartiring / tarif qo'shing.
# "stars" — faqat ENABLE_STARS=1 bo'lsa ishlatiladi (ixtiyoriy Telegram Stars to'lovi).
PREMIUM_PLANS = [
    {"days": 1, "price": 5000, "stars": 30, "title": "1 kunlik"},
    {"days": 10, "price": 9000, "stars": 55, "title": "10 kunlik"},
    {"days": 365, "price": 26000, "stars": 160, "title": "365 kunlik"},
]
# @BotFather → Bot Settings → Payments → Click orqali olingan provider token
CLICK_PROVIDER_TOKEN = os.getenv("CLICK_PROVIDER_TOKEN", "").strip()
ENABLE_STARS = os.getenv("ENABLE_STARS", "0").strip().lower() in ("1", "true", "yes")
# Admin bilan bog'lanish: @username yoki https:// havola (ixtiyoriy)
PREMIUM_CONTACT = os.getenv("PREMIUM_CONTACT", "").strip()
UZ_TZ = timezone(timedelta(hours=5))

# Boshlang'ich majburiy kanallar; mavjud bazaga ham ishga tushganda qo'shiladi.
# Keyin qo'shimcha kanallarni bot ichidan: Admin panel -> 📢 Kanallar orqali boshqarasiz.
# Yopiq kanal uchun id = "-1001234567890", url = invite havola.
# MUHIM: bot hamma kanallarda ADMIN bo'lishi shart!
CHANNELS = [
    {"id": "@tarix_siyosat_falsafa", "url": "https://t.me/tarix_siyosat_falsafa", "title": "Tarix, siyosat, falsafa"},
    {"id": "@cyber_meros", "url": "https://t.me/cyber_meros", "title": "Cyber meros"},
    {"id": "@YovuzDaho", "url": "https://t.me/YovuzDaho", "title": "YovuzDaho"},
    {"id": "@eskirgan_xotira", "url": "https://t.me/eskirgan_xotira", "title": "Eskirgan xotira"},
    {"id": "@ace_create", "url": "https://t.me/ace_create", "title": "Ace create"},
    {"id": "@Gapirma_iltmos", "url": "https://t.me/Gapirma_iltmos", "title": "Gapirma iltmos"},
    {"id": "@csrage", "url": "https://t.me/csrage", "title": "csrage"},
]
PAGE_SIZE = 8
# ====================================================================


def esc(x) -> str:
    return html.escape(str(x or ""))


_bot_info = None


async def bot_info(bot: Bot):
    """Bot ma'lumotini bir marta olib eslab qoladi (har safar get_me chaqirilmaydi)."""
    global _bot_info
    if _bot_info is None:
        _bot_info = await bot.get_me()
    return _bot_info


async def bot_username(bot: Bot) -> str:
    return (await bot_info(bot)).username or ""


# ------------------------------ DB ------------------------------
_db = None  # bitta umumiy ulanish (har so'rovda qayta ochilmaydi)
_write_lock = asyncio.Lock()


async def fetch(sql, args=(), one=False):
    async with _db.execute(sql, args) as cur:
        rows = await cur.fetchall()
    if one:
        return dict(rows[0]) if rows else None
    return [dict(r) for r in rows]


async def execute(sql, args=()):
    async with _write_lock:
        try:
            cur = await _db.execute(sql, args)
            await _db.commit()
            return cur.rowcount
        except Exception:
            await _db.rollback()
            raise


async def close_db():
    global _db
    if _db is not None:
        await _db.close()
        _db = None


async def init_db():
    global _db
    _db = await aiosqlite.connect(DB_PATH, timeout=30)
    _db.row_factory = aiosqlite.Row
    await _db.execute("PRAGMA journal_mode=WAL")
    await _db.execute("PRAGMA synchronous=NORMAL")
    db = _db
    await db.executescript(
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            full_name TEXT,
            joined TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS movies (
            code TEXT PRIMARY KEY,
            file_id TEXT NOT NULL,
            file_type TEXT NOT NULL DEFAULT 'video',
            title TEXT,
            views INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS channels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id TEXT NOT NULL UNIQUE,
            url TEXT NOT NULL,
            title TEXT
        );
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT
        );
        CREATE TABLE IF NOT EXISTS payments (
            charge_id TEXT PRIMARY KEY,
            user_id INTEGER,
            days INTEGER,
            stars INTEGER,
            created INTEGER,
            currency TEXT,
            amount INTEGER
        );
        CREATE TABLE IF NOT EXISTS card_payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            days INTEGER,
            amount INTEGER,
            created INTEGER,
            status TEXT DEFAULT 'pending'
        );
        """
    )
    # eski bazadan yangilash (yangi ustunlar)
    async with db.execute("PRAGMA table_info(movies)") as cur:
        cols = {r[1] for r in await cur.fetchall()}
    for col in ("year", "genre", "language", "quality", "description"):
        if col not in cols:
            await db.execute(f"ALTER TABLE movies ADD COLUMN {col} TEXT")
    if "is_premium" not in cols:
        await db.execute("ALTER TABLE movies ADD COLUMN is_premium INTEGER DEFAULT 0")
    async with db.execute("PRAGMA table_info(users)") as cur:
        ucols = {r[1] for r in await cur.fetchall()}
    async with db.execute("PRAGMA table_info(payments)") as cur:
        pcols = {r[1] for r in await cur.fetchall()}
    for col, typ in (("currency", "TEXT"), ("amount", "INTEGER")):
        if col not in pcols:
            await db.execute(f"ALTER TABLE payments ADD COLUMN {col} {typ}")
    if "premium_until" not in ucols:
        await db.execute("ALTER TABLE users ADD COLUMN premium_until INTEGER DEFAULT 0")
    # Avvalgi namunaviy kanallarni olib tashlash
    await db.execute(
        "DELETE FROM channels WHERE chat_id IN (?, ?, ?, ?, ?, ?, ?)",
        tuple(f"@kanal{i}" for i in range(1, 8)),
    )
    # Boshlang'ich kanallar FAQAT BIR MARTA qo'shiladi. (Avval har ishga tushishda qayta
    # qo'shilib, admin o'chirgan kanal bot qayta yonganda qaytib kelardi.)
    async with db.execute("SELECT value FROM meta WHERE key = 'channels_seeded'") as cur:
        seeded = await cur.fetchone()
    if not seeded:
        async with db.execute("SELECT COUNT(*) FROM channels") as cur:
            count = (await cur.fetchone())[0]
        if count == 0:
            for ch in CHANNELS:
                await db.execute(
                    "INSERT OR IGNORE INTO channels (chat_id, url, title) VALUES (?, ?, ?)",
                    (str(ch["id"]), ch["url"], ch["title"]),
                )
        await db.execute("INSERT OR REPLACE INTO meta (key, value) VALUES ('channels_seeded', '1')")
    await db.commit()
    await load_plan_prices()  # admin o'zgartirgan narxlar (bo'lsa) PREMIUM_PLANS ga qo'llanadi


_known_users = set()


async def add_user(user_id: int, full_name: str):
    if user_id in _known_users:
        return
    await execute(
        "INSERT OR IGNORE INTO users (user_id, full_name) VALUES (?, ?)", (user_id, full_name)
    )
    _known_users.add(user_id)


async def get_movie(code: str):
    return await fetch("SELECT * FROM movies WHERE code = ?", (code,), one=True)


async def save_movie(d: dict):
    await execute(
        "INSERT INTO movies (code, file_id, file_type, title, year, genre, language, quality, description, is_premium) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            d["code"], d["file_id"], d["file_type"], d.get("title"), d.get("year"),
            d.get("genre"), d.get("language"), d.get("quality"), d.get("description"),
            1 if d.get("is_premium") else 0,
        ),
    )


async def next_free_code() -> str:
    rows = await fetch("SELECT code FROM movies")
    nums = [int(r["code"]) for r in rows if str(r["code"]).isdigit()]
    return str(max(nums) + 1 if nums else 1)


async def all_user_ids():
    return [r["user_id"] for r in await fetch("SELECT user_id FROM users")]


# ------------------------------ Premium ------------------------------
def fmt_dt(ts: int) -> str:
    return datetime.fromtimestamp(ts, UZ_TZ).strftime("%d.%m.%Y %H:%M")


async def get_premium_until(user_id: int) -> int:
    r = await fetch("SELECT premium_until FROM users WHERE user_id = ?", (user_id,), one=True)
    return int(r["premium_until"] or 0) if r else 0


async def is_premium(user_id: int) -> bool:
    return user_id in ADMINS or await get_premium_until(user_id) > time.time()


async def grant_premium(user_id: int, days: int, name: str = "") -> int:
    """Premium muddatini uzaytiradi (faol bo'lsa — tugash sanasiga qo'shadi). Yangi tugash vaqtini qaytaradi."""
    await execute("INSERT OR IGNORE INTO users (user_id, full_name) VALUES (?, ?)", (user_id, name))
    await execute(
        "UPDATE users SET premium_until = MAX(COALESCE(premium_until, 0), ?) + ? WHERE user_id = ?",
        (int(time.time()), days * 86400, user_id),
    )
    return await get_premium_until(user_id)


async def revoke_premium(user_id: int) -> int:
    return await execute("UPDATE users SET premium_until = 0 WHERE user_id = ?", (user_id,))


def plan_by_days(days: int):
    for p in PREMIUM_PLANS:
        if p["days"] == days:
            return p
    return None


def plan_from_payload(payload: str):
    payload = payload or ""
    if payload.startswith("premium:") and payload[8:].isdigit():
        return plan_by_days(int(payload[8:]))
    return None


async def notify_admins(bot: Bot, text: str) -> None:
    for admin_id in ADMINS:
        try:
            await bot.send_message(admin_id, text)
        except Exception:
            pass


# ------------------- To'lov kartasi (admin kiritadi) -------------------
async def get_meta(key: str):
    r = await fetch("SELECT value FROM meta WHERE key = ?", (key,), one=True)
    return r["value"] if r else None


async def set_meta(key: str, value: str) -> None:
    await execute("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)", (key, value))


async def get_pay_card():
    number = await get_meta("pay_card_number")
    if not number:
        return None
    return {"number": number, "owner": (await get_meta("pay_card_owner")) or ""}


def fmt_card(number: str) -> str:
    return " ".join(number[i:i + 4] for i in range(0, len(number), 4))


async def load_plan_prices() -> None:
    """Admin bot ichidan o'zgartirgan narxlarni bazadan olib, PREMIUM_PLANS ga qo'llaydi."""
    for p in PREMIUM_PLANS:
        v = await get_meta(f"plan_price:{p['days']}")
        if v and v.isdigit() and int(v) > 0:
            p["price"] = int(v)


# --------------------------- Klaviaturalar ---------------------------
def _style_supported() -> bool:
    """Rangli tugmalar (Bot API 9.4 / aiogram>=3.25). Eski versiyada xatosiz o'chib qoladi."""
    try:
        return "style" in InlineKeyboardButton.model_fields and "style" in KeyboardButton.model_fields
    except Exception:
        return False


HAS_STYLE = _style_supported()
# ---- Tugma ranglari: HAMMA joyda faqat shu qoidalar bo'yicha ----
GREEN = "success"  # 🟢 ijobiy asosiy amal: saqlash, yuborish, to'lash, tarif tanlash, qo'shish
BLUE = "primary"   # 🔵 bo'lim ochish, havola, tanlov, sozlash
RED = "danger"     # 🔴 faqat bekor qilish va o'chirish
# rangsiz (None) — Orqaga, sahifalash, ikkinchi darajali tugmalar


def btn(text, data, style=None):
    kw = {"style": style} if (style and HAS_STYLE) else {}
    return InlineKeyboardButton(text=text, callback_data=data, **kw)


def url_btn(text, url, style=None):
    kw = {"style": style} if (style and HAS_STYLE) else {}
    return InlineKeyboardButton(text=text, url=url, **kw)


def kbtn(text, style=None):
    kw = {"style": style} if (style and HAS_STYLE) else {}
    return KeyboardButton(text=text, **kw)


def main_menu(is_admin: bool = False) -> ReplyKeyboardMarkup:
    rows = [
        [kbtn("🔍 Kino qidirish", BLUE), kbtn("🔥 Top kinolar", BLUE)],
        [kbtn("🎞 Yangi kinolar", BLUE), kbtn("💎 Premium", GREEN)],
        [kbtn("ℹ️ Yordam")],
    ]
    if is_admin:  # faqat adminlarga ko'rinadi
        rows.append([kbtn("🛠 Admin panel", BLUE)])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def admin_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [btn("➕ Kino yuklash", "adm_add", GREEN), btn("🎞 Kinolar", "mv_page:0", BLUE)],
            [btn("💎 Premium boshqaruvi", "adm_prem", BLUE), btn("📢 Kanallar", "adm_ch", BLUE)],
            [btn("📊 Statistika", "adm_stats", BLUE), btn("📨 Xabar yuborish", "adm_bc", BLUE)],
        ]
    )


def cancel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[btn("❌ Bekor qilish", "adm_cancel", RED)]])


def step_kb(required: bool, idx: int = 0) -> InlineKeyboardMarkup:
    rows = []
    if not required:
        rows.append([btn("⏭ O'tkazib yuborish", f"skip:{idx}")])
    rows.append([btn("❌ Bekor qilish", "adm_cancel", RED)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def code_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [btn("🎲 Avtomatik kod", "autocode", GREEN)],
            [btn("❌ Bekor qilish", "adm_cancel", RED)],
        ]
    )


def access_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [btn("🆓 Bepul", "acc:0", BLUE), btn("💎 Pullik (Premium)", "acc:1", BLUE)],
            [btn("❌ Bekor qilish", "adm_cancel", RED)],
        ]
    )


def confirm_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [btn("✅ Saqlash", "save_movie", GREEN), btn("❌ Bekor qilish", "adm_cancel", RED)]
        ]
    )


def sub_kb(not_sub: list) -> InlineKeyboardMarkup:
    rows = [[url_btn(f"📢 {ch['title']}", ch["url"], BLUE)] for ch in not_sub]
    rows.append([btn("✅ Obunani tekshirish", "check_sub", GREEN)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def premium_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[btn("💎 Premium olish", "prem_open", GREEN)]])


def mv_kb(m: dict) -> InlineKeyboardMarkup:
    code = m["code"]
    if m.get("is_premium"):
        tog = btn("🆓 Bepul qilish", f"mv_tog:{code}", BLUE)
    else:
        tog = btn("💎 Pullik qilish", f"mv_tog:{code}", BLUE)
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [btn("🔢 Kodni o'zgartirish", f"mv_code:{code}", BLUE), btn("🗑 O'chirish", f"mv_del:{code}", RED)],
            [tog],
            [btn("⬅️ Ro'yxat", "mv_page:0")],
        ]
    )


# ------------------------ Majburiy obuna ------------------------
def to_chat_id(chat_id: str):
    return int(chat_id) if chat_id.lstrip("-").isdigit() else chat_id


_sub_ok = {}      # user_id -> monotonic vaqt (shu vaqtgacha obuna tekshirilmaydi)
_alert_ts = {}    # chat_id -> oxirgi ogohlantirish vaqti


async def _alert_admins(bot: Bot, ch: dict, err) -> None:
    """Kanalni tekshirib bo'lmasa (odatda bot admin emas) adminlarga 6 soatda bir marta xabar."""
    now = time.monotonic()
    last = _alert_ts.get(ch["chat_id"])
    if last is not None and now - last < 6 * 3600:
        return
    _alert_ts[ch["chat_id"]] = now
    text = (
        "⚠️ <b>Majburiy obuna tekshirilmadi</b>\n\n"
        f"Kanal: {esc(ch['title'])} (<code>{esc(ch['chat_id'])}</code>)\n"
        f"Sabab: <code>{esc(err)}</code>\n\n"
        "Bot shu kanalda <b>admin</b> ekanini va ID to'g'riligini tekshiring. "
        "Tuzatilguncha bu kanal uchun obuna talab qilinmaydi."
    )
    for admin_id in ADMINS:
        try:
            await bot.send_message(admin_id, text)
        except Exception:
            pass


async def _is_not_member(bot: Bot, ch: dict, user_id: int) -> bool:
    for attempt in range(2):
        try:
            m = await bot.get_chat_member(to_chat_id(ch["chat_id"]), user_id)
            break
        except TelegramRetryAfter as e:
            if attempt == 0:
                await asyncio.sleep(min(e.retry_after, 5))
                continue
            return False
        except Exception as e:
            if "user not found" in str(e).lower():
                return True
            # Kanal/bot muammosi: foydalanuvchini bloklab qo'ymaymiz, adminni ogohlantiramiz
            logging.warning("Kanal tekshirishda xato (%s): %s", ch["chat_id"], e)
            await _alert_admins(bot, ch, e)
            return False
    else:
        return False
    return m.status in (ChatMemberStatus.LEFT, ChatMemberStatus.KICKED) or (
        m.status == ChatMemberStatus.RESTRICTED and not getattr(m, "is_member", True)
    )


async def get_unsubscribed(bot: Bot, user_id: int, use_cache: bool = True) -> list:
    if use_cache and _sub_ok.get(user_id, 0) > time.monotonic():
        return []
    channels = await fetch("SELECT * FROM channels ORDER BY id")
    flags = await asyncio.gather(*(_is_not_member(bot, ch, user_id) for ch in channels))
    result = [ch for ch, bad in zip(channels, flags) if bad]
    if result:
        _sub_ok.pop(user_id, None)
    else:
        _sub_ok[user_id] = time.monotonic() + SUB_CACHE_TTL
    return result


SUB_TEXT = (
    "👋 Assalomu alaykum!\n\n"
    "Botdan foydalanish uchun quyidagi <b>kanallarga obuna bo'ling</b>, "
    "so'ng <b>«✅ Obunani tekshirish»</b> tugmasini bosing 👇"
)


class SubMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        user = data.get("event_from_user")
        if user is None or user.is_bot:
            return await handler(event, data)

        msg = event if isinstance(event, Message) else getattr(event, "message", None)
        if msg is None or msg.chat.type != ChatType.PRIVATE:
            return await handler(event, data)

        # Foydalanuvchini bazaga yozib qo'yamiz (broadcast hammaga yetib borishi uchun)
        try:
            await add_user(user.id, user.full_name)
        except Exception as e:
            logging.warning("add_user xatosi: %s", e)

        if user.id in ADMINS:
            return await handler(event, data)
        try:
            if await is_premium(user.id):  # Premium foydalanuvchilar majburiy obunadan ozod
                return await handler(event, data)
        except Exception as e:
            logging.warning("is_premium xatosi: %s", e)
        if isinstance(event, Message) and event.successful_payment:
            return await handler(event, data)  # to'lov hech qachon obuna tufayli yo'qolmasin
        if isinstance(event, CallbackQuery) and event.data == "check_sub":
            return await handler(event, data)

        not_sub = await get_unsubscribed(data["bot"], user.id)
        if not_sub:
            if isinstance(event, CallbackQuery):
                await event.answer("❗ Avval kanallarga obuna bo'ling", show_alert=True)
            await msg.answer(SUB_TEXT, reply_markup=sub_kb(not_sub))
            return
        return await handler(event, data)


# --------------------------- Kino chiqarish ---------------------------
def _caption(m: dict, bot_username_: str, description) -> str:
    lines = [f"🎬 <b>{esc(m.get('title') or 'Kino')}</b>", ""]
    if m.get("year"):
        lines.append(f"📅 Yil: {esc(m['year'])}")
    if m.get("genre"):
        lines.append(f"🎭 Janr: {esc(m['genre'])}")
    if m.get("language"):
        lines.append(f"🌐 Til: {esc(m['language'])}")
    if m.get("quality"):
        lines.append(f"📀 Sifat: {esc(m['quality'])}")
    if m.get("is_premium"):
        lines.append("💎 Premium kino")
    if description:
        lines += ["", f"📝 {esc(description)}"]
    lines += ["", f"🔢 Kod: <code>{esc(m['code'])}</code>", f"🤖 @{bot_username_}"]
    return "\n".join(lines)


def build_caption(m: dict, bot_username_: str) -> str:
    """Telegram caption 1024 belgidan oshsa xato beradi — izohni kerak bo'lsa qisqartiramiz."""
    desc = (m.get("description") or "").strip()
    text = _caption(m, bot_username_, desc)
    for limit in (300, 150, 60, 0):
        if len(text) <= CAPTION_LIMIT:
            break
        short = (desc[:limit].rstrip() + "…") if limit else ""
        text = _caption(m, bot_username_, short)
    return text[:CAPTION_LIMIT] if len(text) > CAPTION_LIMIT else text


async def send_media(bot: Bot, chat_id: int, m: dict, caption: str, kb=None):
    if m["file_type"] == "video":
        await bot.send_video(chat_id, m["file_id"], caption=caption, reply_markup=kb)
    else:
        await bot.send_document(chat_id, m["file_id"], caption=caption, reply_markup=kb)


async def send_movie(message: Message, code: str, user_id=None):
    movie = await get_movie(code)
    if not movie:
        await message.answer("❌ Bunday kodli kino topilmadi.\nKodni tekshirib qaytadan yuboring.")
        return
    uid = user_id or (message.from_user.id if message.from_user else 0)
    if movie.get("is_premium") and not await is_premium(uid):
        await message.answer(
            "💎 <b>Bu pullik kino.</b>\n\nKinoni ko'rish uchun <b>Premium obuna</b> kerak.",
            reply_markup=premium_kb(),
        )
        return
    username = await bot_username(message.bot)
    try:
        await send_media(message.bot, message.chat.id, movie, build_caption(movie, username))
    except TelegramForbiddenError:
        return  # foydalanuvchi botni bloklagan
    except TelegramBadRequest as e:
        logging.error("Kino yuborilmadi (kod %s): %s", code, e)
        await message.answer("⚠️ Kinoni yuborib bo'lmadi. Iltimos, keyinroq urinib ko'ring.")
        return
    await execute("UPDATE movies SET views = views + 1 WHERE code = ?", (code,))


async def safe_edit(msg: Message, text: str, kb=None):
    try:
        await msg.edit_text(text, reply_markup=kb)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            return
        await msg.answer(text, reply_markup=kb)  # masalan: xabar video bo'lsa matn tahrirlanmaydi


# =========================== ROUTERLAR ===========================
group_router = Router()   # GURUHLAR: kino ko'rsatilmaydi, /stop
start_router = Router()   # /start — hamma uchun (holatni ham tozalaydi)
admin_router = Router()   # FAQAT adminlar uchun
user_router = Router()    # oddiy foydalanuvchilar


class IsAdmin(BaseFilter):
    async def __call__(self, event) -> bool:
        u = getattr(event, "from_user", None)
        return bool(u and u.id in ADMINS)


group_router.message.filter(F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}))
start_router.message.filter(F.chat.type == ChatType.PRIVATE)
admin_router.message.filter(IsAdmin())
admin_router.callback_query.filter(IsAdmin())


# ============================== /start ==============================
@start_router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext, command: CommandObject):
    await state.clear()
    await add_user(message.from_user.id, message.from_user.full_name)
    await message.answer(
        f"🎬 Xush kelibsiz, <b>{esc(message.from_user.first_name)}</b>!\n\n"
        "Kino <b>kodini</b> (raqamini) yuboring — men kinoni topib beraman.",
        reply_markup=main_menu(message.from_user.id in ADMINS),
    )
    # t.me/bot?start=125 ko'rinishidagi havola orqali to'g'ridan-to'g'ri kino
    arg = (command.args or "").strip()
    if arg.isdigit() and len(arg) <= 18:
        await send_movie(message, str(int(arg)))


@start_router.message(Command("help"))
async def cmd_help(message: Message):
    await message.answer(
        "ℹ️ <b>Yordam</b>\n\n"
        "1️⃣ Kino kodini (masalan: <code>125</code>) yuboring\n"
        "2️⃣ Bot kinoni sizga jo'natadi\n\n"
        "Kodlarni kanallarimizdan topishingiz mumkin.\n\n"
        "💎 Pullik kinolar uchun Premium obuna kerak — /premium"
    )


# ============================ FOYDALANUVCHI ============================
@user_router.callback_query(F.data == "check_sub")
async def cb_check_sub(call: CallbackQuery):
    not_sub = await get_unsubscribed(call.bot, call.from_user.id, use_cache=False)
    if not_sub:
        await call.answer(f"❌ Siz hali {len(not_sub)} ta kanalga obuna bo'lmadingiz!", show_alert=True)
        try:
            await call.message.edit_reply_markup(reply_markup=sub_kb(not_sub))
        except TelegramBadRequest:
            pass  # tugmalar o'zgarmagan bo'lsa Telegram xato qaytaradi — bu normal
        return
    await add_user(call.from_user.id, call.from_user.full_name)
    await call.answer("✅ Rahmat! Obuna tasdiqlandi.")
    try:
        await call.message.delete()
    except Exception:
        pass
    await call.message.answer(
        "✅ Obuna tasdiqlandi!\n\nEndi kino kodini yuboring 👇",
        reply_markup=main_menu(call.from_user.id in ADMINS),
    )


@user_router.message(StateFilter(default_state), F.text == "🔍 Kino qidirish")
async def btn_search(message: Message):
    await message.answer("🔢 Kino <b>kodini</b> (raqamini) yuboring:")


@user_router.message(StateFilter(default_state), F.text == "🔥 Top kinolar")
async def btn_top(message: Message):
    rows = await fetch("SELECT code, title, views, is_premium FROM movies ORDER BY views DESC, rowid DESC LIMIT 10")
    if not rows:
        await message.answer("Hozircha kinolar yo'q.")
        return
    text = "🔥 <b>Eng ko'p ko'rilgan kinolar:</b>\n\n"
    for i, r in enumerate(rows, 1):
        text += f"{i}. {'💎 ' if r['is_premium'] else ''}{esc(r['title'])} — kod: <code>{esc(r['code'])}</code> (👁 {r['views']})\n"
    await message.answer(text)


@user_router.message(StateFilter(default_state), F.text == "🎞 Yangi kinolar")
async def btn_new(message: Message):
    rows = await fetch("SELECT code, title, is_premium FROM movies ORDER BY rowid DESC LIMIT 30")
    if not rows:
        await message.answer("Hozircha kinolar yo'q.")
        return
    text = "🎞 <b>Oxirgi qo'shilgan kinolar:</b>\n\n"
    for r in rows:
        line = f"🔢 <code>{esc(r['code'])}</code> — {'💎 ' if r['is_premium'] else ''}{esc(r['title'])}\n"
        if len(text) + len(line) > 3900:  # teg o'rtasidan kesilib ketmasligi uchun qatorlab
            break
        text += line
    await message.answer(text)


@user_router.message(StateFilter(default_state), F.text == "ℹ️ Yordam")
async def btn_help(message: Message):
    await message.answer(
        "ℹ️ <b>Yordam</b>\n\n"
        "1️⃣ Kino kodini (masalan: <code>125</code>) yuboring\n"
        "2️⃣ Bot kinoni sizga jo'natadi\n\n"
        "Kodlarni kanallarimizdan topishingiz mumkin.\n\n"
        "💎 Pullik kinolar uchun Premium obuna kerak — /premium"
    )


# ================================ ADMIN ================================
class AddMovie(StatesGroup):
    video = State()
    field = State()
    code = State()
    access = State()
    confirm = State()


class ChangeCode(StatesGroup):
    new = State()


class AddChannel(StatesGroup):
    chat = State()
    link = State()


class Broadcast(StatesGroup):
    content = State()
    confirm = State()


# (kalit, so'rov matni, majburiymi)
STEPS = [
    ("title", "✏️ <b>Kino nomini</b> yozing:", True),
    ("year", "📅 Chiqqan <b>yilini</b> yozing (masalan: 2023):", False),
    ("genre", "🎭 <b>Janrini</b> yozing (masalan: Jangari, Komediya):", False),
    ("language", "🌐 <b>Tilini</b> yozing (masalan: O'zbek tilida):", False),
    ("quality", "📀 <b>Sifatini</b> yozing (masalan: 1080p):", False),
    ("description", "📝 Kino tagiga <b>izoh</b> (qisqacha tavsif) yozing:", False),
]
LIMITS = {"title": 150, "description": 500}


@admin_router.message(Command("admin"))
@admin_router.message(StateFilter(default_state), F.text == "🛠 Admin panel")
async def admin_panel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("🛠 <b>Admin panel</b>\n\nKerakli bo'limni tanlang 👇", reply_markup=admin_kb())


@admin_router.callback_query(F.data == "adm_cancel")
async def adm_cancel(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await call.answer("Bekor qilindi")
    await safe_edit(call.message, "🛠 <b>Admin panel</b>", admin_kb())


@admin_router.callback_query(F.data == "adm_home")
async def adm_home(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await call.answer()
    await safe_edit(call.message, "🛠 <b>Admin panel</b>", admin_kb())


@admin_router.callback_query(F.data == "adm_stats")
async def adm_stats(call: CallbackQuery):
    u = (await fetch("SELECT COUNT(*) c FROM users", one=True))["c"]
    m = (await fetch("SELECT COUNT(*) c FROM movies", one=True))["c"]
    v = (await fetch("SELECT COALESCE(SUM(views),0) c FROM movies", one=True))["c"]
    ch = (await fetch("SELECT COUNT(*) c FROM channels", one=True))["c"]
    pr = (await fetch("SELECT COUNT(*) c FROM users WHERE premium_until > ?", (int(time.time()),), one=True))["c"]
    pm = (await fetch("SELECT COUNT(*) c FROM movies WHERE is_premium = 1", one=True))["c"]
    await call.answer()
    await safe_edit(
        call.message,
        f"📊 <b>Statistika</b>\n\n👥 Foydalanuvchilar: <b>{u}</b>\n🎬 Kinolar: <b>{m}</b>\n"
        f"👁 Jami ko'rishlar: <b>{v}</b>\n📢 Majburiy kanallar: <b>{ch}</b>\n"
        f"💎 Premium foydalanuvchilar: <b>{pr}</b>\n💰 Pullik kinolar: <b>{pm}</b>",
        InlineKeyboardMarkup(inline_keyboard=[[btn("⬅️ Orqaga", "adm_home")]]),
    )


# ---------------------- Kino yuklash (bosqichma-bosqich) ----------------------
@admin_router.callback_query(F.data == "adm_add")
async def adm_add(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await state.set_state(AddMovie.video)
    await call.answer()
    await safe_edit(call.message, "🎬 <b>1-qadam.</b> Kinoni yuboring (video yoki fayl):", cancel_kb())


@admin_router.message(AddMovie.video, F.video | F.document)
async def add_video(message: Message, state: FSMContext):
    if message.video:
        file_id, ftype = message.video.file_id, "video"
    else:
        file_id, ftype = message.document.file_id, "document"
    await state.update_data(file_id=file_id, file_type=ftype, step=-1)
    await next_step(message, state)


@admin_router.message(AddMovie.video)
async def add_video_bad(message: Message):
    await message.answer("❗ Iltimos, kinoni video yoki fayl ko'rinishida yuboring.", reply_markup=cancel_kb())


async def next_step(msg: Message, state: FSMContext):
    d = await state.get_data()
    idx = d.get("step", -1) + 1
    if idx < len(STEPS):
        await state.update_data(step=idx)
        await state.set_state(AddMovie.field)
        _, prompt, required = STEPS[idx]
        await msg.answer(f"<b>{idx + 2}-qadam.</b> {prompt}", reply_markup=step_kb(required, idx))
    else:
        await state.set_state(AddMovie.code)
        await msg.answer(
            "🔢 <b>Oxirgi qadam.</b> Kinoga <b>kod (raqam)</b> bering.\n"
            "Foydalanuvchi shu kodni yozsa, kino chiqadi.\n\n"
            "Kodni o'zingiz yozing yoki avtomatik tugmasini bosing 👇",
            reply_markup=code_kb(),
        )


@admin_router.message(AddMovie.field, F.text)
async def field_text(message: Message, state: FSMContext):
    d = await state.get_data()
    key = STEPS[d["step"]][0]
    val = message.text.strip()
    limit = LIMITS.get(key, 60)
    if len(val) > limit:
        await message.answer(f"❗ Juda uzun. Eng ko'pi bilan {limit} ta belgi yozing.")
        return
    await state.update_data(**{key: val})
    await next_step(message, state)


@admin_router.message(AddMovie.field)
async def field_bad(message: Message):
    await message.answer("❗ Iltimos, matn yuboring.")


@admin_router.callback_query(AddMovie.field, F.data.startswith("skip:"))
async def field_skip(call: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    idx = d.get("step", -1)
    # Eski (allaqachon o'tilgan) bosqichning tugmasi yoki ikki marta bosish — e'tiborsiz
    if call.data != f"skip:{idx}":
        await call.answer()
        return
    if STEPS[idx][2]:
        await call.answer("Bu maydon majburiy!", show_alert=True)
        return
    await call.answer()
    try:
        await call.message.edit_reply_markup(reply_markup=None)
    except TelegramBadRequest:
        pass
    await next_step(call.message, state)


async def show_preview(msg: Message, state: FSMContext, bot: Bot):
    d = await state.get_data()
    username = await bot_username(bot)
    await state.set_state(AddMovie.confirm)
    await msg.answer("👀 <b>Ko'rib chiqing</b> — foydalanuvchiga shunday ko'rinadi:")
    await send_media(bot, msg.chat.id, d, build_caption(d, username), confirm_kb())


async def ask_access(msg: Message, state: FSMContext):
    await state.set_state(AddMovie.access)
    await msg.answer("💎 Bu kino <b>bepulmi</b> yoki <b>pullik (Premium)</b>mi?", reply_markup=access_kb())


@admin_router.callback_query(AddMovie.access, F.data.in_({"acc:0", "acc:1"}))
async def set_access(call: CallbackQuery, state: FSMContext):
    await state.update_data(is_premium=1 if call.data == "acc:1" else 0)
    await call.answer()
    try:
        await call.message.edit_reply_markup(reply_markup=None)
    except TelegramBadRequest:
        pass
    await show_preview(call.message, state, call.bot)


@admin_router.callback_query(AddMovie.code, F.data == "autocode")
async def code_auto(call: CallbackQuery, state: FSMContext):
    await call.answer()
    await state.update_data(code=await next_free_code())
    await ask_access(call.message, state)


@admin_router.message(AddMovie.code, F.text.regexp(r"^\d{1,10}$"))
async def code_manual(message: Message, state: FSMContext):
    code = str(int(message.text))
    if await get_movie(code):
        await message.answer(f"❗ <code>{code}</code> kodi band. Boshqa kod yozing:", reply_markup=code_kb())
        return
    await state.update_data(code=code)
    await ask_access(message, state)


@admin_router.message(AddMovie.code)
async def code_bad(message: Message):
    await message.answer("❗ Kod faqat raqamlardan iborat bo'lishi kerak.", reply_markup=code_kb())


@admin_router.callback_query(AddMovie.confirm, F.data == "save_movie")
async def do_save(call: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    try:
        await save_movie(d)
    except sqlite3.IntegrityError:
        await call.answer("Bu kod band bo'lib qoldi!", show_alert=True)
        return
    await state.clear()
    await call.answer("✅ Saqlandi")
    try:
        await call.message.edit_reply_markup(reply_markup=None)
    except TelegramBadRequest:
        pass
    await call.message.answer(
        f"✅ <b>Kino saqlandi!</b>\n\n🎬 {esc(d.get('title'))}\n🔢 Kod: <code>{d['code']}</code>\n\n"
        "Endi foydalanuvchilar shu kodni yozsa, kino chiqadi.",
        reply_markup=admin_kb(),
    )


# -------------------------- Kinolar ro'yxati --------------------------
@admin_router.callback_query(F.data.startswith("mv_page:"))
async def mv_page(call: CallbackQuery, state: FSMContext):
    await state.clear()
    page = int(call.data.split(":")[1])
    total = (await fetch("SELECT COUNT(*) c FROM movies", one=True))["c"]
    rows = await fetch(
        "SELECT code, title, is_premium FROM movies ORDER BY rowid DESC LIMIT ? OFFSET ?",
        (PAGE_SIZE, page * PAGE_SIZE),
    )
    await call.answer()
    if not rows and page == 0:
        await safe_edit(
            call.message,
            "Hozircha kinolar yo'q.",
            InlineKeyboardMarkup(inline_keyboard=[[btn("⬅️ Orqaga", "adm_home")]]),
        )
        return
    kb = [[btn(f"{'💎 ' if r['is_premium'] else ''}{r['code']} • {(r['title'] or 'Kino')[:35]}", f"mv:{r['code']}")] for r in rows]
    nav = []
    if page > 0:
        nav.append(btn("⬅️", f"mv_page:{page - 1}"))
    if (page + 1) * PAGE_SIZE < total:
        nav.append(btn("➡️", f"mv_page:{page + 1}"))
    if nav:
        kb.append(nav)
    kb.append([btn("🏠 Admin panel", "adm_home")])
    await safe_edit(
        call.message,
        f"🎞 <b>Kinolar</b> (jami: {total})\nKinoni tanlang 👇",
        InlineKeyboardMarkup(inline_keyboard=kb),
    )


@admin_router.callback_query(F.data.startswith("mv:"))
async def mv_open(call: CallbackQuery):
    code = call.data.split(":", 1)[1]
    m = await get_movie(code)
    if not m:
        await call.answer("Kino topilmadi", show_alert=True)
        return
    username = await bot_username(call.bot)
    kb = mv_kb(m)
    await call.answer()
    await send_media(call.bot, call.message.chat.id, m, build_caption(m, username), kb)


@admin_router.callback_query(F.data.startswith("mv_tog:"))
async def mv_toggle(call: CallbackQuery):
    code = call.data.split(":", 1)[1]
    m = await get_movie(code)
    if not m:
        await call.answer("Kino topilmadi", show_alert=True)
        return
    new = 0 if m.get("is_premium") else 1
    await execute("UPDATE movies SET is_premium = ? WHERE code = ?", (new, code))
    m["is_premium"] = new
    await call.answer("💎 Endi pullik" if new else "🆓 Endi bepul")
    username = await bot_username(call.bot)
    try:
        await call.message.edit_caption(caption=build_caption(m, username), reply_markup=mv_kb(m))
    except TelegramBadRequest:
        pass


@admin_router.callback_query(F.data.startswith("mv_del:"))
async def mv_del(call: CallbackQuery):
    code = call.data.split(":", 1)[1]
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[btn("✅ Ha, o'chirish", f"mv_delok:{code}", RED), btn("❌ Yo'q", "adm_home")]]
    )
    await call.answer()
    await call.message.answer(f"🗑 <code>{esc(code)}</code> kodli kinoni o'chirasizmi?", reply_markup=kb)


@admin_router.callback_query(F.data.startswith("mv_delok:"))
async def mv_delok(call: CallbackQuery):
    code = call.data.split(":", 1)[1]
    ok = await execute("DELETE FROM movies WHERE code = ?", (code,))
    await call.answer("O'chirildi" if ok else "Topilmadi")
    await safe_edit(
        call.message,
        "✅ Kino o'chirildi." if ok else "❌ Kino topilmadi.",
        InlineKeyboardMarkup(inline_keyboard=[[btn("🏠 Admin panel", "adm_home")]]),
    )


@admin_router.callback_query(F.data.startswith("mv_code:"))
async def mv_code(call: CallbackQuery, state: FSMContext):
    old = call.data.split(":", 1)[1]
    await state.set_state(ChangeCode.new)
    await state.update_data(old=old)
    await call.answer()
    await call.message.answer(
        f"🔢 <code>{esc(old)}</code> kodi o'rniga <b>yangi kod</b> (raqam) yozing:", reply_markup=cancel_kb()
    )


@admin_router.message(ChangeCode.new, F.text.regexp(r"^\d{1,10}$"))
async def mv_code_new(message: Message, state: FSMContext):
    d = await state.get_data()
    new = str(int(message.text))
    if await get_movie(new):
        await message.answer(f"❗ <code>{new}</code> kodi band. Boshqa kod yozing:", reply_markup=cancel_kb())
        return
    try:
        changed = await execute("UPDATE movies SET code = ? WHERE code = ?", (new, d["old"]))
    except sqlite3.IntegrityError:
        await message.answer(f"❗ <code>{new}</code> kodi band. Boshqa kod yozing:", reply_markup=cancel_kb())
        return
    await state.clear()
    if not changed:
        await message.answer("❌ Kino topilmadi (ehtimol o'chirilgan).", reply_markup=admin_kb())
        return
    await message.answer(
        f"✅ Kod o'zgartirildi: <code>{esc(d['old'])}</code> → <code>{new}</code>", reply_markup=admin_kb()
    )


@admin_router.message(ChangeCode.new)
async def mv_code_bad(message: Message):
    await message.answer("❗ Kod faqat raqamlardan iborat bo'lishi kerak.", reply_markup=cancel_kb())


# ------------------------ Majburiy kanallar boshqaruvi ------------------------
async def show_channels(call_or_msg, edit=True):
    rows = await fetch("SELECT * FROM channels ORDER BY id")
    text = "📢 <b>Majburiy kanallar</b>\n\n"
    if rows:
        for i, r in enumerate(rows, 1):
            text += f"{i}. {esc(r['title'])} — <code>{esc(r['chat_id'])}</code>\n"
        text += "\nO'chirish uchun kanal tugmasini bosing."
    else:
        text += "Hozircha kanal yo'q (obuna talab qilinmaydi)."
    kb = [[btn(f"❌ {r['title'] or r['chat_id']}", f"ch_del:{r['id']}", RED)] for r in rows]
    kb.append([btn("➕ Kanal qo'shish", "ch_add", GREEN)])
    kb.append([btn("⬅️ Orqaga", "adm_home")])
    markup = InlineKeyboardMarkup(inline_keyboard=kb)
    if edit:
        await safe_edit(call_or_msg, text, markup)
    else:
        await call_or_msg.answer(text, reply_markup=markup)


@admin_router.callback_query(F.data == "adm_ch")
async def adm_ch(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await call.answer()
    await show_channels(call.message)


@admin_router.callback_query(F.data.startswith("ch_del:"))
async def ch_del(call: CallbackQuery):
    await execute("DELETE FROM channels WHERE id = ?", (int(call.data.split(":")[1]),))
    _sub_ok.clear()
    await call.answer("O'chirildi")
    await show_channels(call.message)


@admin_router.callback_query(F.data == "ch_add")
async def ch_add(call: CallbackQuery, state: FSMContext):
    await state.set_state(AddChannel.chat)
    await call.answer()
    await call.message.answer(
        "➕ Kanalni yuboring:\n• ochiq kanal: <code>@username</code> yoki havola\n"
        "• yopiq kanal: raqamli ID (<code>-100...</code>)\n\n"
        "⚠️ Bot avval o'sha kanalda <b>admin</b> bo'lishi kerak.",
        reply_markup=cancel_kb(),
    )


def parse_chat(text: str):
    t = text.strip()
    for p in (
        "https://t.me/",
        "http://t.me/",
        "t.me/",
        "https://telegram.me/",
        "http://telegram.me/",
        "telegram.me/",
    ):
        if t.startswith(p):
            t = t[len(p):]
            break
    if not t:
        return None
    if t.startswith("joinchat/"):
        t = "+" + t[len("joinchat/"):]
    if t.startswith("@") or t.startswith("+") or t.lstrip("-").isdigit():
        return t
    if "/" not in t:
        return "@" + t
    return None


async def finish_channel(message: Message, state: FSMContext, chat_id: str, url: str, title: str):
    await execute(
        "INSERT OR REPLACE INTO channels (chat_id, url, title) VALUES (?, ?, ?)", (chat_id, url, title)
    )
    _sub_ok.clear()
    await state.clear()
    await message.answer(f"✅ Kanal qo'shildi: <b>{esc(title)}</b>")
    await show_channels(message, edit=False)


@admin_router.message(AddChannel.chat, F.text)
async def ch_chat(message: Message, state: FSMContext):
    chat_id = parse_chat(message.text)
    if not chat_id:
        await message.answer("❗ Noto'g'ri format. @username yoki raqamli ID yuboring.", reply_markup=cancel_kb())
        return
    try:
        chat = await message.bot.get_chat(to_chat_id(chat_id))
        me = await message.bot.get_me()
        member = await message.bot.get_chat_member(chat.id, me.id)
        if member.status not in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR):
            raise ValueError("bot admin emas")
    except Exception as e:
        logging.warning("Kanal qo'shishda xato: %s", e)
        await message.answer(
            "❌ Kanal topilmadi yoki bot u yerda <b>admin emas</b>.\n"
            "Botni kanalga admin qilib, qayta urinib ko'ring.",
            reply_markup=cancel_kb(),
        )
        return
    stored_id = chat_id if chat_id.startswith("@") else str(chat.id)
    title = chat.title or stored_id
    if chat.username:
        await finish_channel(message, state, stored_id, f"https://t.me/{chat.username}", title)
    else:
        await state.set_state(AddChannel.link)
        await state.update_data(chat_id=stored_id, title=title)
        await message.answer(
            "🔗 Bu yopiq kanal. Foydalanuvchilar uchun <b>invite havolani</b> yuboring (https://t.me/+...):",
            reply_markup=cancel_kb(),
        )


@admin_router.message(AddChannel.link, F.text.startswith("http"))
async def ch_link(message: Message, state: FSMContext):
    d = await state.get_data()
    await finish_channel(message, state, d["chat_id"], message.text.strip(), d["title"])


@admin_router.message(AddChannel.link)
async def ch_link_bad(message: Message):
    await message.answer("❗ Havola https:// bilan boshlanishi kerak.", reply_markup=cancel_kb())


# ------------------------- Premium boshqaruvi (admin) -------------------------
class PremAdmin(StatesGroup):
    grant = State()
    revoke = State()


def prem_admin_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [btn("➕ Premium berish", "prem_grant", GREEN), btn("➖ Olib tashlash", "prem_revoke", RED)],
            [btn("📋 Premium foydalanuvchilar", "prem_list", BLUE)],
            [btn("💳 To'lov kartasi", "prem_card", BLUE)],
            [btn("💰 Narxlarni o'zgartirish", "prem_prices", BLUE)],
            [btn("⬅️ Orqaga", "adm_home")],
        ]
    )


@admin_router.callback_query(F.data == "adm_prem")
async def adm_prem(call: CallbackQuery, state: FSMContext):
    await state.clear()
    now = int(time.time())
    act = (await fetch("SELECT COUNT(*) c FROM users WHERE premium_until > ?", (now,), one=True))["c"]
    paid = (await fetch("SELECT COUNT(*) c FROM movies WHERE is_premium = 1", one=True))["c"]
    card = await get_pay_card()
    card_txt = fmt_card(card["number"]) if card else "kiritilmagan"
    await call.answer()
    await safe_edit(
        call.message,
        "💎 <b>Premium boshqaruvi</b>\n\n"
        f"👤 Faol Premium foydalanuvchilar: <b>{act}</b>\n"
        f"🎬 Pullik kinolar: <b>{paid}</b>\n"
        f"💳 To'lov kartasi: <b>{card_txt}</b>\n\n"
        "Kinoni pullik qilish: <b>🎞 Kinolar</b> → kinoni tanlang → <b>💎 Pullik qilish</b>.",
        prem_admin_kb(),
    )


@admin_router.callback_query(F.data == "prem_list")
async def prem_list(call: CallbackQuery):
    rows = await fetch(
        "SELECT user_id, full_name, premium_until FROM users WHERE premium_until > ? "
        "ORDER BY premium_until DESC LIMIT 30",
        (int(time.time()),),
    )
    await call.answer()
    text = "📋 <b>Premium foydalanuvchilar</b>\n\n"
    if not rows:
        text += "Hozircha yo'q."
    for r in rows:
        name = (r["full_name"] or "—")[:30]
        text += f"• <code>{r['user_id']}</code> {esc(name)} — {fmt_dt(r['premium_until'])} gacha\n"
    await safe_edit(
        call.message, text, InlineKeyboardMarkup(inline_keyboard=[[btn("⬅️ Orqaga", "adm_prem")]])
    )


@admin_router.callback_query(F.data == "prem_grant")
async def prem_grant(call: CallbackQuery, state: FSMContext):
    await state.set_state(PremAdmin.grant)
    await call.answer()
    await call.message.answer(
        "➕ Foydalanuvchi <b>ID</b> va <b>kun</b> sonini bo'sh joy bilan yuboring.\n"
        "Masalan: <code>123456789 30</code>",
        reply_markup=cancel_kb(),
    )


@admin_router.message(PremAdmin.grant, F.text)
async def prem_grant_do(message: Message, state: FSMContext):
    m = re.match(r"^\s*(\d{1,15})\s+(\d{1,4})\s*$", message.text)
    if not m or not (1 <= int(m.group(2)) <= 3650):
        await message.answer(
            "❗ Format noto'g'ri. Masalan: <code>123456789 30</code> (kun: 1–3650)", reply_markup=cancel_kb()
        )
        return
    uid, days = int(m.group(1)), int(m.group(2))
    until = await grant_premium(uid, days)
    await state.clear()
    notified = True
    try:
        await message.bot.send_message(uid, f"🎉 Sizga 💎 <b>Premium</b> berildi!\nMuddati: <b>{fmt_dt(until)}</b> gacha.")
    except Exception:
        notified = False
    await message.answer(
        f"✅ <code>{uid}</code> ga {days} kun Premium berildi.\nTugash: <b>{fmt_dt(until)}</b>"
        + ("" if notified else "\n⚠️ Foydalanuvchiga xabar yuborib bo'lmadi (bot bilan chat boshlamagan bo'lishi mumkin)."),
        reply_markup=admin_kb(),
    )


@admin_router.message(PremAdmin.grant)
async def prem_grant_bad(message: Message):
    await message.answer("❗ ID va kunni matn ko'rinishida yuboring.", reply_markup=cancel_kb())


@admin_router.callback_query(F.data == "prem_revoke")
async def prem_revoke(call: CallbackQuery, state: FSMContext):
    await state.set_state(PremAdmin.revoke)
    await call.answer()
    await call.message.answer("➖ Premium olib tashlanadigan foydalanuvchi <b>ID</b> sini yuboring:", reply_markup=cancel_kb())


@admin_router.message(PremAdmin.revoke, F.text.regexp(r"^\s*\d{1,15}\s*$"))
async def prem_revoke_do(message: Message, state: FSMContext):
    uid = int(message.text.strip())
    changed = await revoke_premium(uid)
    await state.clear()
    await message.answer(
        f"✅ <code>{uid}</code> dan Premium olib tashlandi." if changed else "❌ Bunday foydalanuvchi bazada yo'q.",
        reply_markup=admin_kb(),
    )


@admin_router.message(PremAdmin.revoke)
async def prem_revoke_bad(message: Message):
    await message.answer("❗ Faqat raqamli ID yuboring.", reply_markup=cancel_kb())


# ------------------------- Premium narxlarini o'zgartirish (admin) -------------------------
class PremPrice(StatesGroup):
    value = State()


PRICE_MIN = 1000
PRICE_MAX = 50_000_000


def prices_text() -> str:
    lines = ["💰 <b>Premium narxlari</b>", ""]
    for p in PREMIUM_PLANS:
        lines.append(f"• {esc(p['title'])} ({p['days']} kun) — <b>{money(p['price'])} so'm</b>")
    lines += ["", "O'zgartirmoqchi bo'lgan tarifni tanlang 👇"]
    return "\n".join(lines)


def prices_admin_kb() -> InlineKeyboardMarkup:
    rows = [
        [btn(f"✏️ {p['title']} — {money(p['price'])} so'm", f"pp_edit:{p['days']}", BLUE)]
        for p in PREMIUM_PLANS
    ]
    rows.append([btn("⬅️ Orqaga", "adm_prem")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


@admin_router.callback_query(F.data == "prem_prices")
async def prem_prices(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await call.answer()
    await safe_edit(call.message, prices_text(), prices_admin_kb())


@admin_router.callback_query(F.data.regexp(r"^pp_edit:\d+$"))
async def pp_edit(call: CallbackQuery, state: FSMContext):
    plan = plan_by_days(int(call.data.split(":", 1)[1]))
    if not plan:
        await call.answer("Tarif topilmadi", show_alert=True)
        return
    await state.set_state(PremPrice.value)
    await state.update_data(days=plan["days"])
    await call.answer()
    await call.message.answer(
        f"💰 <b>{esc(plan['title'])}</b> tarifi uchun yangi narxni <b>so'mda</b> yuboring.\n"
        f"Hozirgi narx: <b>{money(plan['price'])} so'm</b>\n\n"
        f"Masalan: <code>12000</code> (oraliq: {money(PRICE_MIN)} – {money(PRICE_MAX)})",
        reply_markup=cancel_kb(),
    )


@admin_router.message(PremPrice.value, F.text)
async def pp_value(message: Message, state: FSMContext):
    d = await state.get_data()
    days = d.get("days")
    plan = plan_by_days(int(days)) if str(days).isdigit() else None
    if not plan:
        await state.clear()
        await message.answer("❗ Jarayon uzildi. Qaytadan urinib ko'ring.", reply_markup=admin_kb())
        return
    raw = re.sub(r"\s", "", message.text or "")
    if not re.fullmatch(r"\d{1,9}", raw) or not (PRICE_MIN <= int(raw) <= PRICE_MAX):
        await message.answer(
            f"❗ Narx faqat raqam bo'lishi va {money(PRICE_MIN)} – {money(PRICE_MAX)} so'm oralig'ida bo'lishi kerak.",
            reply_markup=cancel_kb(),
        )
        return
    new_price = int(raw)
    old_price = plan["price"]
    await set_meta(f"plan_price:{plan['days']}", str(new_price))  # avval bazaga, keyin xotiraga
    plan["price"] = new_price
    await state.clear()
    await message.answer(
        f"✅ <b>{esc(plan['title'])}</b> narxi o'zgartirildi:\n"
        f"{money(old_price)} so'm → <b>{money(new_price)} so'm</b>\n\n"
        "Yangi narx foydalanuvchilar uchun darhol amal qiladi.",
        reply_markup=prices_admin_kb(),
    )


@admin_router.message(PremPrice.value)
async def pp_value_bad(message: Message):
    await message.answer("❗ Narxni raqam ko'rinishida yuboring.", reply_markup=cancel_kb())


# ---------------------- To'lov kartasi va chek tasdiqlash (admin) ----------------------
class PremCard(StatesGroup):
    number = State()
    owner = State()


def card_admin_kb(has_card: bool) -> InlineKeyboardMarkup:
    rows = [[btn("✏️ Kartani o'zgartirish" if has_card else "➕ Karta kiritish", "prem_card_set", GREEN)]]
    if has_card:
        rows.append([btn("🗑 Kartani o'chirish", "prem_card_del", RED)])
    rows.append([btn("⬅️ Orqaga", "adm_prem")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


@admin_router.callback_query(F.data == "prem_card")
async def prem_card(call: CallbackQuery, state: FSMContext):
    await state.clear()
    card = await get_pay_card()
    await call.answer()
    if card:
        text = (
            "💳 <b>To'lov kartasi</b>\n\n"
            f"Karta: <code>{fmt_card(card['number'])}</code>\n"
            f"Egasi: <b>{esc(card['owner'] or '—')}</b>\n\n"
            "Foydalanuvchilar Premium uchun shu kartaga to'lov qiladi va chek yuboradi. "
            "Chek sizga keladi — tasdiqlasangiz Premium avtomatik beriladi."
        )
    else:
        text = (
            "💳 <b>To'lov kartasi</b>\n\n"
            "Karta hali kiritilmagan. Karta kiritsangiz, foydalanuvchilar Premium olishda "
            "«💳 Kartaga to'lash» tugmasini ko'radi."
        )
    await safe_edit(call.message, text, card_admin_kb(bool(card)))


@admin_router.callback_query(F.data == "prem_card_set")
async def prem_card_set(call: CallbackQuery, state: FSMContext):
    await state.set_state(PremCard.number)
    await call.answer()
    await call.message.answer(
        "💳 <b>Karta raqamini</b> yuboring (16 ta raqam).\nMasalan: <code>8600 1234 5678 9012</code>",
        reply_markup=cancel_kb(),
    )


@admin_router.message(PremCard.number, F.text)
async def prem_card_number(message: Message, state: FSMContext):
    digits = re.sub(r"[\s-]", "", message.text or "")
    if not re.fullmatch(r"\d{16}", digits):
        await message.answer("❗ Karta raqami 16 ta raqamdan iborat bo'lishi kerak.", reply_markup=cancel_kb())
        return
    await state.update_data(number=digits)
    await state.set_state(PremCard.owner)
    await message.answer(
        "👤 Karta <b>egasining ismini</b> yuboring (masalan: <code>Ali Valiyev</code>).\n"
        "Ko'rsatmaslik uchun <code>-</code> yuboring.",
        reply_markup=cancel_kb(),
    )


@admin_router.message(PremCard.number)
async def prem_card_number_bad(message: Message):
    await message.answer("❗ Karta raqamini matn ko'rinishida yuboring.", reply_markup=cancel_kb())


@admin_router.message(PremCard.owner, F.text)
async def prem_card_owner(message: Message, state: FSMContext):
    d = await state.get_data()
    number = d.get("number")
    if not number:
        await state.clear()
        await message.answer("❗ Jarayon uzildi. Qaytadan urinib ko'ring.", reply_markup=admin_kb())
        return
    owner = (message.text or "").strip()
    owner = "" if owner == "-" else owner[:60]
    await set_meta("pay_card_number", number)
    await set_meta("pay_card_owner", owner)
    await state.clear()
    await message.answer(
        "✅ To'lov kartasi saqlandi.\n\n"
        f"Karta: <code>{fmt_card(number)}</code>\n"
        f"Egasi: <b>{esc(owner or '—')}</b>",
        reply_markup=prem_admin_kb(),
    )


@admin_router.message(PremCard.owner)
async def prem_card_owner_bad(message: Message):
    await message.answer("❗ Ismni matn ko'rinishida yuboring (yoki <code>-</code>).", reply_markup=cancel_kb())


@admin_router.callback_query(F.data == "prem_card_del")
async def prem_card_del(call: CallbackQuery):
    await execute("DELETE FROM meta WHERE key IN ('pay_card_number', 'pay_card_owner')")
    await call.answer("Karta o'chirildi")
    await safe_edit(
        call.message,
        "💳 <b>To'lov kartasi</b>\n\nKarta o'chirildi.",
        card_admin_kb(False),
    )


@admin_router.callback_query(F.data.regexp(r"^pc_(ok|no):\d+$"))
async def pc_decide(call: CallbackQuery):
    action, _, raw = call.data.partition(":")
    pid = int(raw)
    row = await fetch("SELECT * FROM card_payments WHERE id = ?", (pid,), one=True)
    if not row:
        await call.answer("So'rov topilmadi", show_alert=True)
        return
    approve = action == "pc_ok"
    # faqat bitta admin hal qila oladi (ikki marta tasdiqlanib ketmasligi uchun)
    changed = await execute(
        "UPDATE card_payments SET status = ? WHERE id = ? AND status = 'pending'",
        ("approved" if approve else "rejected", pid),
    )
    if not changed:
        try:
            await call.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        await call.answer("Bu so'rov allaqachon ko'rib chiqilgan", show_alert=True)
        return
    uid, days = row["user_id"], row["days"]
    if approve:
        try:
            until = await grant_premium(uid, days)
        except Exception as e:
            logging.error("Kartaga to'lov: Premium berilmadi (user %s): %s", uid, e)
            await execute("UPDATE card_payments SET status = 'pending' WHERE id = ?", (pid,))
            await call.answer("⚠️ Premium berishda xato. Qayta urinib ko'ring.", show_alert=True)
            return
        try:
            await execute(
                "INSERT OR IGNORE INTO payments (charge_id, user_id, days, created, currency, amount) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (f"card:{pid}", uid, days, int(time.time()), "UZS", int(row["amount"] or 0) * 100),
            )
        except Exception as e:
            logging.warning("Kartaga to'lov yozilmadi: %s", e)
        try:
            await call.bot.send_message(
                uid,
                f"🎉 <b>To'lovingiz tasdiqlandi!</b> Premium faollashtirildi.\n"
                f"Muddati: <b>{fmt_dt(until)}</b> gacha.\n\nEndi pullik kinolarni ko'rishingiz mumkin 🍿",
            )
        except Exception:
            pass
        result = f"✅ Tasdiqlandi — <code>{uid}</code> ga {days} kun Premium berildi (tugash: {fmt_dt(until)})."
    else:
        try:
            extra = f"\n\nSavol bo'lsa admin bilan bog'laning: {esc(PREMIUM_CONTACT)}" if PREMIUM_CONTACT else ""
            await call.bot.send_message(
                uid,
                "❌ <b>To'lovingiz tasdiqlanmadi.</b>\nChek noto'g'ri yoki to'lov tushmagan bo'lishi mumkin." + extra,
            )
        except Exception:
            pass
        result = f"❌ Rad etildi — <code>{uid}</code> ning to'lovi."
    try:
        await call.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await call.answer("Bajarildi")
    await call.message.reply(result)


# ---------------------------- Xabar yuborish ----------------------------
def bc_confirm_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[btn("✅ Yuborish", "bc_send", GREEN), btn("❌ Bekor qilish", "adm_cancel", RED)]]
    )


@admin_router.callback_query(F.data == "adm_bc")
async def adm_bc(call: CallbackQuery, state: FSMContext):
    await state.set_state(Broadcast.content)
    await call.answer()
    await safe_edit(
        call.message,
        "📨 Hammaga yuboriladigan xabarni yuboring (matn, rasm, video — istalgani):",
        cancel_kb(),
    )


@admin_router.message(Broadcast.content)
async def bc_preview(message: Message, state: FSMContext):
    """Xabar darrov yuborilmaydi — avval ko'rsatib, tasdiq so'raladi (tasodifan yuborib qo'ymaslik uchun)."""
    await state.update_data(src_chat=message.chat.id, src_msg=message.message_id)
    await state.set_state(Broadcast.confirm)
    total = len(await all_user_ids())
    try:
        await message.copy_to(message.chat.id)
    except Exception as e:
        logging.warning("Broadcast preview xatosi: %s", e)
    await message.answer(
        f"👆 Shu xabar <b>{total}</b> ta foydalanuvchiga yuboriladi. Tasdiqlaysizmi?",
        reply_markup=bc_confirm_kb(),
    )


async def copy_with_retry(bot: Bot, uid: int, src_chat: int, src_msg: int) -> str:
    for _ in range(3):
        try:
            await bot.copy_message(uid, src_chat, src_msg)
            return "ok"
        except TelegramRetryAfter as e:
            await asyncio.sleep(e.retry_after + 1)
        except TelegramForbiddenError:
            return "blocked"
        except Exception:
            return "fail"
    return "fail"


@admin_router.callback_query(Broadcast.confirm, F.data == "bc_send")
async def do_broadcast(call: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    await state.clear()  # ikki marta bosilsa ikki marta yuborilmasligi uchun darrov tozalaymiz
    await call.answer()
    try:
        await call.message.edit_reply_markup(reply_markup=None)
    except TelegramBadRequest:
        pass
    ids = await all_user_ids()
    status = await call.message.answer(f"⏳ Yuborilmoqda... (0/{len(ids)})")
    ok = blocked = fail = 0
    for i, uid in enumerate(ids, 1):
        res = await copy_with_retry(call.bot, uid, d["src_chat"], d["src_msg"])
        if res == "ok":
            ok += 1
        elif res == "blocked":
            blocked += 1
        else:
            fail += 1
        await asyncio.sleep(0.05)
        if i % 100 == 0:
            try:
                await status.edit_text(f"⏳ Yuborilmoqda... ({i}/{len(ids)})")
            except Exception:
                pass
    text = (
        "✅ Yuborish tugadi!\n\n"
        f"📨 Yetkazildi: <b>{ok}</b>\n"
        f"🚫 Botni bloklaganlar: <b>{blocked}</b>\n"
        f"⚠️ Boshqa xato: <b>{fail}</b>"
    )
    try:
        await status.edit_text(text, reply_markup=admin_kb())
    except Exception:
        await call.message.answer(text, reply_markup=admin_kb())


# ============================ PREMIUM (foydalanuvchi) ============================
def money(n: int) -> str:
    return f"{n:,}".replace(",", " ")


def contact_url() -> str:
    c = PREMIUM_CONTACT
    return c if c.startswith("http") else f"https://t.me/{c.lstrip('@')}"


def click_enabled() -> bool:
    return bool(CLICK_PROVIDER_TOKEN)


async def premium_screen(uid: int):
    until = await get_premium_until(uid)
    card = await get_pay_card()
    lines = [
        "💎 <b>PREMIUM — MAXSUS IMKONIYATLAR</b>",
        "",
        "• 🚫 Kanallarga obuna bo'lmasdan foydalanish",
        "",
        "• 💎 Premium foydalanuvchilar uchun maxsus (pullik) kinolar",
        "",
    ]
    if uid in ADMINS:
        lines += ["👑 Siz adminsiz — barcha kinolar siz uchun ochiq.", ""]
    elif until > time.time():
        lines += [f"✅ Premium faol: <b>{fmt_dt(until)}</b> gacha", ""]
    if not (click_enabled() or ENABLE_STARS or card):
        lines += ["⚠️ Onlayn to'lov hozircha sozlanmagan. Admin bilan bog'laning.", ""]
    lines.append("🤖 <b>Kino Premium — kino sevuvchilar uchun yanada ko'proq imkoniyat!</b>")
    rows = [
        [btn(f"👑 {p['title']} - {money(p['price'])} so'm", f"plan:{p['days']}", GREEN)]
        for p in PREMIUM_PLANS
    ]
    if PREMIUM_CONTACT:
        rows.append([url_btn("💬 Admin bilan bog'lanish", contact_url(), BLUE)])
    return "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=rows)


def plan_screen(plan: dict, card=None):
    text = (
        "<blockquote>💳 <b>To'lov tizimini tanlang</b>\n\n"
        f"💎 <b>Tarif:</b> {esc(plan['title'])}\n"
        f"🗓 <b>Muddat:</b> {plan['days']} kun\n"
        f"💵 <b>Narx:</b> {money(plan['price'])} so'm</blockquote>"
    )
    rows = []
    if click_enabled():
        rows.append([btn("💳 Humo (Click)", f"pay:click:{plan['days']}", GREEN)])
    if ENABLE_STARS:
        rows.append([btn(f"⭐ Telegram Stars — {plan['stars']}", f"pay:stars:{plan['days']}", GREEN)])
    if card:
        rows.append([btn("💳 Kartaga to'lash", f"paycard:{plan['days']}", GREEN)])
    if PREMIUM_CONTACT:
        rows.append([url_btn("💬 Boshqa usul (admin)", contact_url(), BLUE)])
    if not rows:
        text += "\n\n⚠️ Onlayn to'lov hozircha sozlanmagan. Admin bilan bog'laning."
    rows.append([btn("⏪ Orqaga", "prem_back")])
    return text, InlineKeyboardMarkup(inline_keyboard=rows)


async def send_premium_invoice(bot: Bot, uid: int, plan: dict, method: str) -> None:
    common = dict(
        chat_id=uid,
        title=f"💎 Premium — {plan['title']}",
        description=f"{plan['days']} kunlik Premium obuna: pullik kinolar va majburiy obunasiz foydalanish.",
        payload=f"premium:{plan['days']}",
    )
    if method == "click":
        await bot.send_invoice(
            **common,
            provider_token=CLICK_PROVIDER_TOKEN,
            currency="UZS",
            prices=[LabeledPrice(label=f"Premium {plan['title']}", amount=plan["price"] * 100)],  # tiyinda
        )
    else:
        await bot.send_invoice(
            **common,
            provider_token="",
            currency="XTR",
            prices=[LabeledPrice(label=f"Premium {plan['title']}", amount=plan["stars"])],
        )


def expected_amount(plan: dict, currency: str):
    if currency == "UZS" and click_enabled():
        return plan["price"] * 100
    if currency == "XTR" and ENABLE_STARS:
        return plan["stars"]
    return None


@user_router.message(StateFilter(default_state), F.text == "💎 Premium")
@user_router.message(StateFilter(default_state), Command("premium"))
async def btn_premium(message: Message):
    text, kb = await premium_screen(message.from_user.id)
    await message.answer(text, reply_markup=kb)


@user_router.callback_query(F.data == "prem_open")
async def cb_prem_open(call: CallbackQuery):
    await call.answer()
    text, kb = await premium_screen(call.from_user.id)
    await call.bot.send_message(call.from_user.id, text, reply_markup=kb)


@user_router.callback_query(F.data == "prem_back")
async def cb_prem_back(call: CallbackQuery):
    await call.answer()
    text, kb = await premium_screen(call.from_user.id)
    await safe_edit(call.message, text, kb)


@user_router.callback_query(F.data.startswith("plan:"))
async def cb_plan(call: CallbackQuery):
    raw = call.data.split(":", 1)[1]
    plan = plan_by_days(int(raw)) if raw.isdigit() else None
    if not plan:
        await call.answer("Tarif topilmadi", show_alert=True)
        return
    await call.answer()
    text, kb = plan_screen(plan, await get_pay_card())
    await safe_edit(call.message, text, kb)


@user_router.callback_query(F.data.startswith("pay:"))
async def cb_pay(call: CallbackQuery):
    parts = call.data.split(":")
    plan = plan_by_days(int(parts[2])) if len(parts) == 3 and parts[2].isdigit() else None
    method = parts[1] if len(parts) == 3 else ""
    ok_method = (method == "click" and click_enabled()) or (method == "stars" and ENABLE_STARS)
    if not plan or not ok_method:
        await call.answer("Bu to'lov usuli mavjud emas", show_alert=True)
        return
    await call.answer()
    try:
        await send_premium_invoice(call.bot, call.from_user.id, plan, method)
    except Exception as e:
        logging.error("Invoice yuborilmadi (%s): %s", method, e)
        await call.bot.send_message(call.from_user.id, "⚠️ To'lovni boshlab bo'lmadi. Keyinroq urinib ko'ring yoki admin bilan bog'laning.")


@user_router.pre_checkout_query()
async def pre_checkout(query: PreCheckoutQuery):
    plan = plan_from_payload(query.invoice_payload)
    exp = expected_amount(plan, query.currency) if plan else None
    if exp is None or query.total_amount != exp:
        await query.answer(ok=False, error_message="Tarif topilmadi yoki narx o'zgargan. Qaytadan urinib ko'ring.")
        return
    await query.answer(ok=True)


def fmt_paid(currency: str, total: int) -> str:
    return f"{money(total // 100)} so'm" if currency == "UZS" else f"{total} ⭐"


@user_router.message(F.successful_payment)
async def paid(message: Message):
    pay = message.successful_payment
    uid = message.from_user.id
    plan = plan_from_payload(pay.invoice_payload)
    charge = pay.telegram_payment_charge_id
    try:
        await execute(
            "INSERT INTO payments (charge_id, user_id, days, created, currency, amount) VALUES (?, ?, ?, ?, ?, ?)",
            (charge, uid, plan["days"] if plan else 0, int(time.time()), pay.currency, pay.total_amount),
        )
    except sqlite3.IntegrityError:
        return  # bu to'lov allaqachon qayta ishlangan
    amount = fmt_paid(pay.currency, pay.total_amount)
    if not plan:
        logging.error("Noma'lum tarifli to'lov: %s (user %s, charge %s)", pay.invoice_payload, uid, charge)
        await notify_admins(
            message.bot,
            f"⚠️ To'lov keldi ({amount}), tarif aniqlanmadi.\nUser: <code>{uid}</code>\nCharge: <code>{esc(charge)}</code>",
        )
        await message.answer("⚠️ To'lov qabul qilindi, lekin tarif aniqlanmadi. Iltimos, admin bilan bog'laning.")
        return
    try:
        until = await grant_premium(uid, plan["days"], message.from_user.full_name)
    except Exception as e:
        logging.error("Premium berilmadi (user %s, charge %s): %s", uid, charge, e)
        await notify_admins(
            message.bot,
            f"🚨 To'lov qabul qilindi ({amount}), lekin Premium berilmadi!\nUser: <code>{uid}</code>\nCharge: <code>{esc(charge)}</code>",
        )
        await message.answer("⚠️ To'lov qabul qilindi, Premium faollashtirilmoqda. Agar bir necha daqiqada chiqmasa, admin bilan bog'laning.")
        return
    await message.answer(
        f"🎉 <b>Rahmat!</b> Premium faollashtirildi.\nMuddati: <b>{fmt_dt(until)}</b> gacha.\n\nEndi pullik kinolarni ko'rishingiz mumkin 🍿"
    )
    await notify_admins(
        message.bot,
        f"💰 Yangi Premium: <code>{uid}</code> {esc(message.from_user.full_name)} — {esc(plan['title'])} ({amount})",
    )


# ------------------- Kartaga to'lash (chek yuborish, admin tasdiqlaydi) -------------------
class PayCard(StatesGroup):
    receipt = State()


def paycard_cancel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[btn("❌ Bekor qilish", "paycard_cancel", RED)]])


@user_router.callback_query(F.data.startswith("paycard:"))
async def cb_paycard(call: CallbackQuery, state: FSMContext):
    raw = call.data.split(":", 1)[1]
    plan = plan_by_days(int(raw)) if raw.isdigit() else None
    card = await get_pay_card()
    if not plan or not card:
        await call.answer("Bu to'lov usuli mavjud emas", show_alert=True)
        return
    pend = await fetch(
        "SELECT COUNT(*) c FROM card_payments WHERE user_id = ? AND status = 'pending'",
        (call.from_user.id,),
        one=True,
    )
    if pend and pend["c"] >= 1:
        await call.answer("⏳ Oldingi chekingiz hali ko'rib chiqilmoqda. Iltimos, kuting.", show_alert=True)
        return
    await call.answer()
    await state.set_state(PayCard.receipt)
    await state.update_data(days=plan["days"])
    owner = f"👤 <b>Karta egasi:</b> {esc(card['owner'])}\n" if card["owner"] else ""
    await call.message.answer(
        "<blockquote>💳 <b>Kartaga to'lov</b>\n\n"
        f"💎 <b>Tarif:</b> {esc(plan['title'])} ({plan['days']} kun)\n"
        f"💵 <b>To'lov summasi:</b> {money(plan['price'])} so'm\n\n"
        f"💳 <b>Karta raqami:</b>\n<code>{fmt_card(card['number'])}</code>\n"
        f"{owner}</blockquote>\n"
        "1️⃣ Yuqoridagi kartaga <b>aynan shu summani</b> o'tkazing.\n"
        "2️⃣ To'lov <b>chekini (skrinshot)</b> shu yerga rasm qilib yuboring.\n"
        "3️⃣ Admin tekshirib tasdiqlagach, Premium avtomatik yoqiladi ✅",
        reply_markup=paycard_cancel_kb(),
    )


@user_router.callback_query(F.data == "paycard_cancel")
async def cb_paycard_cancel(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await call.answer("Bekor qilindi")
    try:
        await call.message.delete()
    except Exception:
        pass


@user_router.message(PayCard.receipt, F.photo | F.document)
async def paycard_receipt(message: Message, state: FSMContext):
    d = await state.get_data()
    days = d.get("days")
    plan = plan_by_days(int(days)) if str(days).isdigit() else None
    if not plan:
        await state.clear()
        await message.answer("⚠️ Tarif topilmadi. Qaytadan /premium ni bosing.")
        return
    uid = message.from_user.id
    try:
        async with _write_lock:
            try:
                cur = await _db.execute(
                    "INSERT INTO card_payments (user_id, days, amount, created, status) VALUES (?, ?, ?, ?, 'pending')",
                    (uid, plan["days"], plan["price"], int(time.time())),
                )
                await _db.commit()
                pid = cur.lastrowid
            except Exception:
                await _db.rollback()
                raise
    except Exception as e:
        logging.error("Kartaga to'lov so'rovi yozilmadi (user %s): %s", uid, e)
        await message.answer("⚠️ Xatolik yuz berdi. Iltimos, keyinroq urinib ko'ring.")
        return
    u = message.from_user
    uname = f"@{u.username}" if u.username else "—"
    caption = (
        "💳 <b>Kartaga to'lov cheki</b>\n\n"
        f"👤 {esc(u.full_name)} ({esc(uname)})\n"
        f"🆔 <code>{uid}</code>\n"
        f"💎 Tarif: {esc(plan['title'])} ({plan['days']} kun)\n"
        f"💵 Summa: <b>{money(plan['price'])} so'm</b>"
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[btn("✅ Tasdiqlash", f"pc_ok:{pid}", GREEN), btn("❌ Rad etish", f"pc_no:{pid}", RED)]]
    )
    sent = 0
    for admin_id in ADMINS:
        try:
            if message.photo:
                await message.bot.send_photo(admin_id, message.photo[-1].file_id, caption=caption, reply_markup=kb)
            else:
                await message.bot.send_document(admin_id, message.document.file_id, caption=caption, reply_markup=kb)
            sent += 1
        except Exception as e:
            logging.warning("Chek adminga (%s) yuborilmadi: %s", admin_id, e)
    await state.clear()
    if not sent:
        await execute("DELETE FROM card_payments WHERE id = ?", (pid,))
        await message.answer("⚠️ Chekni adminga yuborib bo'lmadi. Keyinroq urinib ko'ring yoki admin bilan bog'laning.")
        return
    await message.answer(
        "✅ <b>Chekingiz adminga yuborildi.</b>\nTekshirilgach Premium faollashtiriladi va sizga xabar keladi."
    )


@user_router.message(PayCard.receipt)
async def paycard_receipt_bad(message: Message):
    await message.answer(
        "❗ Iltimos, to'lov <b>chekini rasm (skrinshot)</b> ko'rinishida yuboring.",
        reply_markup=paycard_cancel_kb(),
    )


# ========================= GURUH (kino ko'rsatilmaydi) =========================
GROUP_NO_MOVIE = "🚫 Guruhda kino ko'rib bo'lmaydi.\n\n🎬 Kino ko'rish uchun botning o'ziga o'ting 👇"
GROUP_JOIN_TEXT = (
    "👋 Salom! Men kino botman.\n\n"
    "🚫 Guruhda kino ko'rib bo'lmaydi — kino ko'rish uchun botning o'ziga o'ting 👇\n\n"
    "🛑 Meni guruhdan chiqarish uchun: /stop (faqat guruh adminlari)"
)
_group_ts = {}
GROUP_COOLDOWN = 15  # soniya: guruhda spam bo'lmasligi uchun


async def go_bot_kb(bot: Bot) -> InlineKeyboardMarkup:
    username = await bot_username(bot)
    return InlineKeyboardMarkup(inline_keyboard=[[url_btn("🤖 Botga o'tish", f"https://t.me/{username}", BLUE)]])


def is_group_trigger(text: str, username: str) -> bool:
    """Guruhda faqat kino kodi, botga yozilgan buyruq yoki bot eslatilganda javob beramiz."""
    t = (text or "").strip().lower()
    u = f"@{username.lower()}"
    if not t:
        return False
    if u in t:
        return True
    if re.fullmatch(r"[0-9]{1,18}", t):
        return True
    if t.startswith("/"):
        cmd = t.split()[0]
        return "@" not in cmd or cmd.endswith(u)
    return False


async def can_stop(message: Message) -> bool:
    if message.sender_chat and message.sender_chat.id == message.chat.id:
        return True  # anonim guruh admini
    u = message.from_user
    if u is None:
        return False
    if u.id in ADMINS:
        return True
    try:
        m = await message.bot.get_chat_member(message.chat.id, u.id)
        return m.status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR)
    except Exception:
        return False


@group_router.message(Command("stop"))
async def group_stop(message: Message):
    if not await can_stop(message):
        await message.reply("❌ Bu buyruq faqat guruh adminlari uchun.")
        return
    try:
        await message.answer("👋 Xayr! Men guruhdan chiqyapman.\nKino ko'rish uchun botning o'ziga o'ting 👇",
                             reply_markup=await go_bot_kb(message.bot))
    except Exception:
        pass
    try:
        await message.bot.leave_chat(message.chat.id)
    except Exception as e:
        logging.warning("Guruhdan chiqib bo'lmadi (%s): %s", message.chat.id, e)


@group_router.message(F.new_chat_members)
async def group_joined(message: Message):
    info = await bot_info(message.bot)
    if not any(u.id == info.id for u in (message.new_chat_members or [])):
        return
    await message.answer(GROUP_JOIN_TEXT, reply_markup=await go_bot_kb(message.bot))


@group_router.message(F.text)
async def group_text(message: Message):
    username = await bot_username(message.bot)
    if not username or not is_group_trigger(message.text, username):
        return
    now = time.monotonic()
    last = _group_ts.get(message.chat.id)
    if last is not None and now - last < GROUP_COOLDOWN:
        return
    _group_ts[message.chat.id] = now
    await message.reply(GROUP_NO_MOVIE, reply_markup=await go_bot_kb(message.bot))


@group_router.message()
async def group_ignore(message: Message):
    return  # guruhdagi boshqa hamma narsa e'tiborsiz (boshqa routerlarga o'tmasin)


@start_router.message(Command("stop"))
async def private_stop(message: Message):
    await message.answer("ℹ️ /stop buyrug'i guruhlarda botni guruhdan chiqarish uchun ishlatiladi.")


# ========================= KINO KODI =========================
@user_router.message(StateFilter(default_state), F.text.regexp(r"^[0-9]{1,18}$"))
async def by_code(message: Message):
    await send_movie(message, str(int(message.text.strip())))


@user_router.message(StateFilter(default_state), F.text)
async def fallback(message: Message):
    await message.answer("🔢 Iltimos, kino <b>kodini (faqat raqam)</b> yuboring.")


# ===================== Oxirgi tutqich (eskirgan tugmalar) =====================
last_router = Router()


@last_router.callback_query()
async def stale_callback(call: CallbackQuery):
    # Bot qayta ishga tushgach holat yo'qoladi — tugma "aylanib" qolmasligi uchun javob beramiz
    await call.answer("⌛ Bu tugma eskirgan. /start yoki /admin ni bosing.", show_alert=True)


@last_router.message(~StateFilter(default_state))
async def stale_state_message(message: Message):
    await message.answer(
        "❗ Hozir boshqa amal bajarilmoqda. Davom eting yoki bekor qilish uchun /admin ni bosing."
    )


# ================================ XATOLAR ================================
async def on_error(event: ErrorEvent):
    logging.error("Handlerda kutilmagan xato: %r", event.exception, exc_info=event.exception)
    upd = event.update
    try:
        if upd.callback_query:
            await upd.callback_query.answer("⚠️ Xatolik yuz berdi. Qayta urinib ko'ring.", show_alert=True)
        elif upd.message:
            await upd.message.answer("⚠️ Xatolik yuz berdi. Iltimos, keyinroq urinib ko'ring.")
    except Exception:
        pass
    return True


# ================================ MAIN ================================
async def set_commands(bot: Bot):
    base = [
        BotCommand(command="start", description="Botni ishga tushirish"),
        BotCommand(command="premium", description="Premium obuna"),
        BotCommand(command="help", description="Yordam"),
    ]
    try:
        await bot.set_my_commands(base)
        await bot.set_my_commands(
            [BotCommand(command="stop", description="Botni guruhdan chiqarish (adminlar)")],
            scope=BotCommandScopeAllGroupChats(),
        )
    except Exception as e:
        logging.warning("Buyruqlarni o'rnatishda xato: %s", e)
    for admin_id in ADMINS:  # /admin buyrug'i faqat adminlarga ko'rinadi
        try:
            await bot.set_my_commands(
                base + [BotCommand(command="admin", description="Admin panel")],
                scope=BotCommandScopeChat(chat_id=admin_id),
            )
        except Exception as e:  # admin botga hali /start bosmagan bo'lishi mumkin
            logging.warning("Admin %s uchun buyruq o'rnatilmadi: %s", admin_id, e)


async def main():
    if not BOT_TOKEN:
        raise SystemExit("BOT_TOKEN topilmadi! .env faylini tekshiring.")
    if not ADMINS:
        logging.warning("ADMINS bo'sh! .env faylga admin ID yozing.")
    try:
        bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    except Exception as e:
        raise SystemExit(f"BOT_TOKEN noto'g'ri: {e}")

    await init_db()
    try:
        dp = Dispatcher(storage=MemoryStorage())
        dp.message.outer_middleware(SubMiddleware())
        dp.callback_query.outer_middleware(SubMiddleware())
        dp.include_router(group_router)  # guruhlar birinchi: boshqa routerlarga o'tmaydi
        dp.include_router(start_router)
        dp.include_router(admin_router)
        dp.include_router(user_router)
        dp.include_router(last_router)
        dp.errors.register(on_error)

        await bot.delete_webhook(drop_pending_updates=True)
        await bot_username(bot)  # token to'g'riligini ham shu yerda tekshiradi
        await set_commands(bot)
        logging.info("Bot ishga tushdi: @%s", await bot_username(bot))
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        await close_db()
        await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Bot to'xtatildi.")
