import asyncio
import html
import logging
import os

import aiosqlite
from aiogram import BaseMiddleware, Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ChatMemberStatus, ChatType, ParseMode
from aiogram.filters import BaseFilter, Command, CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup, default_state
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    BotCommand,
    BotCommandScopeChat,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)

# ============================ SOZLAMALAR ============================
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMINS = [int(x) for x in os.getenv("ADMINS", "").split(",") if x.strip()]
DB_PATH = "kino.db"

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


# ------------------------------ DB ------------------------------
async def fetch(sql, args=(), one=False):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(sql, args)
        rows = await cur.fetchall()
    if one:
        return dict(rows[0]) if rows else None
    return [dict(r) for r in rows]


async def execute(sql, args=()):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(sql, args)
        await db.commit()
        return cur.rowcount


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
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
            """
        )
        # eski bazadan yangilash (yangi ustunlar)
        cols = {r[1] for r in await (await db.execute("PRAGMA table_info(movies)")).fetchall()}
        for col in ("year", "genre", "language", "quality", "description"):
            if col not in cols:
                await db.execute(f"ALTER TABLE movies ADD COLUMN {col} TEXT")
        # Avvalgi namunaviy kanallarni olib tashlab, belgilangan kanallarni har ishga tushishda qo'shish.
        await db.execute(
            "DELETE FROM channels WHERE chat_id IN (?, ?, ?, ?, ?, ?, ?)",
            tuple(f"@kanal{i}" for i in range(1, 8)),
        )
        for ch in CHANNELS:
            await db.execute(
                "INSERT OR IGNORE INTO channels (chat_id, url, title) VALUES (?, ?, ?)",
                (str(ch["id"]), ch["url"], ch["title"]),
            )
        await db.commit()


async def add_user(user_id: int, full_name: str):
    await execute(
        "INSERT OR IGNORE INTO users (user_id, full_name) VALUES (?, ?)", (user_id, full_name)
    )


async def get_movie(code: str):
    return await fetch("SELECT * FROM movies WHERE code = ?", (code,), one=True)


async def save_movie(d: dict):
    await execute(
        "INSERT INTO movies (code, file_id, file_type, title, year, genre, language, quality, description) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            d["code"], d["file_id"], d["file_type"], d.get("title"), d.get("year"),
            d.get("genre"), d.get("language"), d.get("quality"), d.get("description"),
        ),
    )


async def next_free_code() -> str:
    rows = await fetch("SELECT code FROM movies")
    nums = [int(r["code"]) for r in rows if str(r["code"]).isdigit()]
    return str(max(nums) + 1 if nums else 1)


async def all_user_ids():
    return [r["user_id"] for r in await fetch("SELECT user_id FROM users")]


# --------------------------- Klaviaturalar ---------------------------
def main_menu(is_admin: bool = False) -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text="🔍 Kino qidirish"), KeyboardButton(text="🔥 Top kinolar")],
        [KeyboardButton(text="🎞 Yangi kinolar"), KeyboardButton(text="ℹ️ Yordam")],
    ]
    if is_admin:  # faqat adminlarga ko'rinadi
        rows.append([KeyboardButton(text="🛠 Admin panel")])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def btn(text, data):
    return InlineKeyboardButton(text=text, callback_data=data)


def admin_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [btn("➕ Kino yuklash", "adm_add"), btn("🎞 Kinolar", "mv_page:0")],
            [btn("📢 Kanallar", "adm_ch"), btn("📊 Statistika", "adm_stats")],
            [btn("📨 Xabar yuborish", "adm_bc")],
        ]
    )


def cancel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[btn("❌ Bekor qilish", "adm_cancel")]])


def step_kb(required: bool) -> InlineKeyboardMarkup:
    rows = []
    if not required:
        rows.append([btn("⏭ O'tkazib yuborish", "skip")])
    rows.append([btn("❌ Bekor qilish", "adm_cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def code_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [btn("🎲 Avtomatik kod", "autocode")],
            [btn("❌ Bekor qilish", "adm_cancel")],
        ]
    )


def confirm_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[btn("✅ Saqlash", "save_movie"), btn("❌ Bekor qilish", "adm_cancel")]]
    )


def sub_kb(not_sub: list) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text=f"📢 {ch['title']}", url=ch["url"])] for ch in not_sub]
    rows.append([btn("✅ Obunani tekshirish", "check_sub")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ------------------------ Majburiy obuna ------------------------
def to_chat_id(chat_id: str):
    return int(chat_id) if chat_id.lstrip("-").isdigit() else chat_id


async def get_unsubscribed(bot: Bot, user_id: int) -> list:
    result = []
    for ch in await fetch("SELECT * FROM channels ORDER BY id"):
        try:
            m = await bot.get_chat_member(to_chat_id(ch["chat_id"]), user_id)
            if m.status in (ChatMemberStatus.LEFT, ChatMemberStatus.KICKED) or (
                m.status == ChatMemberStatus.RESTRICTED and not getattr(m, "is_member", True)
            ):
                result.append(ch)
        except Exception as e:
            # Odatda: bot kanalda admin emas yoki kanal ID noto'g'ri
            logging.warning("Kanal tekshirishda xato (%s): %s", ch["chat_id"], e)
            result.append(ch)
    return result


SUB_TEXT = (
    "👋 Assalomu alaykum!\n\n"
    "Botdan foydalanish uchun quyidagi <b>kanallarga obuna bo'ling</b>, "
    "so'ng <b>«✅ Obunani tekshirish»</b> tugmasini bosing 👇"
)


class SubMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        user = data.get("event_from_user")
        if user is None or user.id in ADMINS:
            return await handler(event, data)
        if isinstance(event, CallbackQuery) and event.data == "check_sub":
            return await handler(event, data)

        msg = event if isinstance(event, Message) else event.message
        if msg is None or msg.chat.type != ChatType.PRIVATE:
            return await handler(event, data)

        not_sub = await get_unsubscribed(data["bot"], user.id)
        if not_sub:
            if isinstance(event, CallbackQuery):
                await event.answer("❗ Avval kanallarga obuna bo'ling", show_alert=True)
            await msg.answer(SUB_TEXT, reply_markup=sub_kb(not_sub))
            return
        return await handler(event, data)


# --------------------------- Kino chiqarish ---------------------------
def build_caption(m: dict, bot_username: str) -> str:
    lines = [f"🎬 <b>{esc(m.get('title') or 'Kino')}</b>", ""]
    if m.get("year"):
        lines.append(f"📅 Yil: {esc(m['year'])}")
    if m.get("genre"):
        lines.append(f"🎭 Janr: {esc(m['genre'])}")
    if m.get("language"):
        lines.append(f"🌐 Til: {esc(m['language'])}")
    if m.get("quality"):
        lines.append(f"📀 Sifat: {esc(m['quality'])}")
    if m.get("description"):
        lines += ["", f"📝 {esc(m['description'])}"]
    lines += ["", f"🔢 Kod: <code>{esc(m['code'])}</code>", f"🤖 @{bot_username}"]
    return "\n".join(lines)


async def send_media(bot: Bot, chat_id: int, m: dict, caption: str, kb=None):
    if m["file_type"] == "video":
        await bot.send_video(chat_id, m["file_id"], caption=caption, reply_markup=kb)
    else:
        await bot.send_document(chat_id, m["file_id"], caption=caption, reply_markup=kb)


async def send_movie(message: Message, code: str):
    movie = await get_movie(code)
    if not movie:
        await message.answer("❌ Bunday kodli kino topilmadi.\nKodni tekshirib qaytadan yuboring.")
        return
    me = await message.bot.get_me()
    await send_media(message.bot, message.chat.id, movie, build_caption(movie, me.username))
    await execute("UPDATE movies SET views = views + 1 WHERE code = ?", (code,))


async def safe_edit(msg: Message, text: str, kb=None):
    try:
        await msg.edit_text(text, reply_markup=kb)
    except Exception:
        await msg.answer(text, reply_markup=kb)


# =========================== ROUTERLAR ===========================
start_router = Router()   # /start — hamma uchun (holatni ham tozalaydi)
admin_router = Router()   # FAQAT adminlar uchun
user_router = Router()    # oddiy foydalanuvchilar


class IsAdmin(BaseFilter):
    async def __call__(self, event) -> bool:
        u = getattr(event, "from_user", None)
        return bool(u and u.id in ADMINS)


admin_router.message.filter(IsAdmin())
admin_router.callback_query.filter(IsAdmin())


# ============================== /start ==============================
@start_router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    await add_user(message.from_user.id, message.from_user.full_name)
    await message.answer(
        f"🎬 Xush kelibsiz, <b>{esc(message.from_user.first_name)}</b>!\n\n"
        "Kino kodini (raqamini) yuboring — men kinoni topib beraman.",
        reply_markup=main_menu(message.from_user.id in ADMINS),
    )


# ============================ FOYDALANUVCHI ============================
@user_router.callback_query(F.data == "check_sub")
async def cb_check_sub(call: CallbackQuery):
    not_sub = await get_unsubscribed(call.bot, call.from_user.id)
    if not_sub:
        await call.answer(f"❌ Siz hali {len(not_sub)} ta kanalga obuna bo'lmadingiz!", show_alert=True)
        try:
            await call.message.edit_reply_markup(reply_markup=sub_kb(not_sub))
        except Exception:
            pass
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
    await message.answer("🔢 Kino kodini (raqamini) yuboring:")


@user_router.message(StateFilter(default_state), F.text == "🔥 Top kinolar")
async def btn_top(message: Message):
    rows = await fetch("SELECT code, title, views FROM movies ORDER BY views DESC, rowid DESC LIMIT 10")
    if not rows:
        await message.answer("Hozircha kinolar yo'q.")
        return
    text = "🔥 <b>Eng ko'p ko'rilgan kinolar:</b>\n\n"
    for i, r in enumerate(rows, 1):
        text += f"{i}. {esc(r['title'])} — kod: <code>{esc(r['code'])}</code> (👁 {r['views']})\n"
    await message.answer(text)


@user_router.message(StateFilter(default_state), F.text == "🎞 Yangi kinolar")
async def btn_new(message: Message):
    rows = await fetch("SELECT code, title FROM movies ORDER BY rowid DESC LIMIT 30")
    if not rows:
        await message.answer("Hozircha kinolar yo'q.")
        return
    text = "🎞 <b>Oxirgi qo'shilgan kinolar:</b>\n\n"
    for r in rows:
        text += f"🔢 <code>{esc(r['code'])}</code> — {esc(r['title'])}\n"
    await message.answer(text[:4000])


@user_router.message(StateFilter(default_state), F.text == "ℹ️ Yordam")
async def btn_help(message: Message):
    await message.answer(
        "ℹ️ <b>Yordam</b>\n\n"
        "1️⃣ Kino kodini (masalan: <code>125</code>) yuboring\n"
        "2️⃣ Bot kinoni sizga jo'natadi\n\n"
        "Kodlarni kanallarimizdan topishingiz mumkin."
    )


# ================================ ADMIN ================================
class AddMovie(StatesGroup):
    video = State()
    field = State()
    code = State()
    confirm = State()


class ChangeCode(StatesGroup):
    new = State()


class AddChannel(StatesGroup):
    chat = State()
    link = State()


class Broadcast(StatesGroup):
    content = State()


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
    await call.answer()
    await safe_edit(
        call.message,
        f"📊 <b>Statistika</b>\n\n👥 Foydalanuvchilar: <b>{u}</b>\n🎬 Kinolar: <b>{m}</b>\n"
        f"👁 Jami ko'rishlar: <b>{v}</b>\n📢 Majburiy kanallar: <b>{ch}</b>",
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
        await msg.answer(f"<b>{idx + 2}-qadam.</b> {prompt}", reply_markup=step_kb(required))
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


@admin_router.callback_query(AddMovie.field, F.data == "skip")
async def field_skip(call: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    if STEPS[d["step"]][2]:
        await call.answer("Bu maydon majburiy!", show_alert=True)
        return
    await call.answer()
    await call.message.edit_reply_markup(reply_markup=None)
    await next_step(call.message, state)


async def show_preview(msg: Message, state: FSMContext, bot: Bot):
    d = await state.get_data()
    me = await bot.get_me()
    await state.set_state(AddMovie.confirm)
    await msg.answer("👀 <b>Ko'rib chiqing</b> — foydalanuvchiga shunday ko'rinadi:")
    await send_media(bot, msg.chat.id, d, build_caption(d, me.username), confirm_kb())


@admin_router.callback_query(AddMovie.code, F.data == "autocode")
async def code_auto(call: CallbackQuery, state: FSMContext):
    await call.answer()
    await state.update_data(code=await next_free_code())
    await show_preview(call.message, state, call.bot)


@admin_router.message(AddMovie.code, F.text.regexp(r"^\d{1,10}$"))
async def code_manual(message: Message, state: FSMContext):
    code = str(int(message.text))
    if await get_movie(code):
        await message.answer(f"❗ <code>{code}</code> kodi band. Boshqa kod yozing:", reply_markup=code_kb())
        return
    await state.update_data(code=code)
    await show_preview(message, state, message.bot)


@admin_router.message(AddMovie.code)
async def code_bad(message: Message):
    await message.answer("❗ Kod faqat raqamlardan iborat bo'lishi kerak.", reply_markup=code_kb())


@admin_router.callback_query(AddMovie.confirm, F.data == "save_movie")
async def do_save(call: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    if await get_movie(d["code"]):
        await call.answer("Bu kod band bo'lib qoldi!", show_alert=True)
        return
    await save_movie(d)
    await state.clear()
    await call.answer("✅ Saqlandi")
    await call.message.edit_reply_markup(reply_markup=None)
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
        "SELECT code, title FROM movies ORDER BY rowid DESC LIMIT ? OFFSET ?",
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
    kb = [[btn(f"{r['code']} • {(r['title'] or 'Kino')[:35]}", f"mv:{r['code']}")] for r in rows]
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
    me = await call.bot.get_me()
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [btn("🔢 Kodni o'zgartirish", f"mv_code:{code}"), btn("🗑 O'chirish", f"mv_del:{code}")],
            [btn("⬅️ Ro'yxat", "mv_page:0")],
        ]
    )
    await call.answer()
    await send_media(call.bot, call.message.chat.id, m, build_caption(m, me.username), kb)


@admin_router.callback_query(F.data.startswith("mv_del:"))
async def mv_del(call: CallbackQuery):
    code = call.data.split(":", 1)[1]
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[btn("✅ Ha, o'chirish", f"mv_delok:{code}"), btn("❌ Yo'q", "adm_home")]]
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
    await execute("UPDATE movies SET code = ? WHERE code = ?", (new, d["old"]))
    await state.clear()
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
    kb = [[btn(f"❌ {r['title'] or r['chat_id']}", f"ch_del:{r['id']}")] for r in rows]
    kb.append([btn("➕ Kanal qo'shish", "ch_add")])
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
    for p in ("https://t.me/", "http://t.me/", "t.me/"):
        if t.startswith(p):
            t = t[len(p):]
    if t.startswith("@") or t.lstrip("-").isdigit():
        return t
    if t and not t.startswith("+") and "/" not in t:
        return "@" + t
    return None


async def finish_channel(message: Message, state: FSMContext, chat_id: str, url: str, title: str):
    await execute(
        "INSERT OR REPLACE INTO channels (chat_id, url, title) VALUES (?, ?, ?)", (chat_id, url, title)
    )
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


# ---------------------------- Xabar yuborish ----------------------------
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
async def do_broadcast(message: Message, state: FSMContext):
    await state.clear()
    ids = await all_user_ids()
    status = await message.answer(f"⏳ Yuborilmoqda... (0/{len(ids)})")
    ok = fail = 0
    for i, uid in enumerate(ids, 1):
        try:
            await message.copy_to(uid)
            ok += 1
        except Exception:
            fail += 1
        await asyncio.sleep(0.05)
        if i % 100 == 0:
            try:
                await status.edit_text(f"⏳ Yuborilmoqda... ({i}/{len(ids)})")
            except Exception:
                pass
    await status.edit_text(
        f"✅ Yuborish tugadi!\n\n📨 Yetkazildi: <b>{ok}</b>\n🚫 Yetkazilmadi: <b>{fail}</b>",
        reply_markup=admin_kb(),
    )


# ========================= KINO KODI (raqam) =========================
@user_router.message(StateFilter(default_state), F.text.regexp(r"^\d+$"))
async def by_code(message: Message):
    await add_user(message.from_user.id, message.from_user.full_name)
    await send_movie(message, str(int(message.text.strip())))


@user_router.message(StateFilter(default_state), F.text)
async def fallback(message: Message):
    await message.answer("🔢 Iltimos, kino <b>kodini (faqat raqam)</b> yuboring.")


# ================================ MAIN ================================
async def set_commands(bot: Bot):
    try:
        await bot.set_my_commands([BotCommand(command="start", description="Botni ishga tushirish")])
        for admin_id in ADMINS:  # /admin buyrug'i faqat adminlarga ko'rinadi
            await bot.set_my_commands(
                [
                    BotCommand(command="start", description="Botni ishga tushirish"),
                    BotCommand(command="admin", description="Admin panel"),
                ],
                scope=BotCommandScopeChat(chat_id=admin_id),
            )
    except Exception as e:
        logging.warning("Buyruqlarni o'rnatishda xato: %s", e)


async def main():
    if not BOT_TOKEN:
        raise SystemExit("BOT_TOKEN topilmadi! .env faylini tekshiring.")
    if not ADMINS:
        logging.warning("ADMINS bo'sh! .env faylga admin ID yozing.")
    await init_db()
    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=MemoryStorage())
    dp.message.outer_middleware(SubMiddleware())
    dp.callback_query.outer_middleware(SubMiddleware())
    dp.include_router(start_router)
    dp.include_router(admin_router)
    dp.include_router(user_router)
    await bot.delete_webhook(drop_pending_updates=True)
    await set_commands(bot)
    logging.info("Bot ishga tushdi!")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
