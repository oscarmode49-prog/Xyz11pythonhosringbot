import asyncio
import hashlib
import html
import logging
import os
import re
import signal
import shutil
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import psutil
from aiohttp import web, ClientSession, ClientTimeout
from aiogram import Bot, Dispatcher, F, types
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart, Command
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup,
    InlineKeyboardButton, FSInputFile, CallbackQuery
)

# ========================= CONFIG =========================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
OWNER_ID = int(os.getenv("OWNER_ID", str(ADMIN_ID or 0)))
MAX_BOT_FILE_MB = int(os.getenv("MAX_BOT_FILE_MB", "10"))

BASE = Path(__file__).resolve().parent
DATA_DIR = BASE / "data"
BOTS_DIR = BASE / "hosted_bots"
LOG_DIR = BASE / "logs"
VENV_DIR = BASE / "venvs"
CACHE_DIR = BASE / "cache"
for folder in (DATA_DIR, BOTS_DIR, LOG_DIR, VENV_DIR, CACHE_DIR):
    folder.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "hosting.sqlite3"
LOG_FILE = LOG_DIR / "hosting.log"

os.environ.setdefault("PIP_CACHE_DIR", str(CACHE_DIR / "pip"))
os.environ.setdefault("PIP_DISABLE_PIP_VERSION_CHECK", "1")
os.environ.setdefault("PIP_NO_INPUT", "1")
os.environ.setdefault("PIP_NO_CACHE_DIR", "0")
os.environ.setdefault("PIP_PREFER_BINARY", "1")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing. Add it in Render Environment Variables.")
if not ADMIN_ID or not OWNER_ID:
    raise RuntimeError("ADMIN_ID and OWNER_ID are required in Render Environment Variables.")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler(LOG_FILE, encoding="utf-8")],
)
log = logging.getLogger("hosting")
bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
STARTED_AT = time.time()
PROCESSES: dict[tuple[int, int], subprocess.Popen] = {}
STARTING: set[tuple[int, int]] = set()
CRASH_RESTARTS: dict[tuple[int, int], list[float]] = {}
USER_STATES: dict[int, str] = {}
PENDING_TOKENS: dict[int, str] = {}
MAINTENANCE = False
SPINNER = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"

# ========================= DATABASE =========================
def db(query, params=(), fetch="none"):
    with sqlite3.connect(DB_PATH, timeout=30) as con:
        con.row_factory = sqlite3.Row
        cur = con.execute(query, params)
        if fetch == "one":
            return cur.fetchone()
        if fetch == "all":
            return cur.fetchall()
        con.commit()
        return cur.lastrowid

def init_db():
    db("""CREATE TABLE IF NOT EXISTS users(
        user_id INTEGER PRIMARY KEY, username TEXT, full_name TEXT,
        joined_at TEXT NOT NULL, last_seen TEXT NOT NULL,
        language TEXT DEFAULT 'en', banned INTEGER DEFAULT 0)""")
    db("""CREATE TABLE IF NOT EXISTS hosted_bots(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL, file_path TEXT NOT NULL, file_name TEXT NOT NULL,
        file_size INTEGER DEFAULT 0, bot_username TEXT DEFAULT '', status TEXT DEFAULT 'PENDING',
        uploaded_at TEXT NOT NULL, started_at TEXT, pid INTEGER, last_error TEXT DEFAULT '',
        slot INTEGER DEFAULT 1, req_hash TEXT DEFAULT '',
        admin_disabled INTEGER DEFAULT 0, off_reason TEXT DEFAULT '')""")
    db("""CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT NOT NULL)""")
    db("""CREATE TABLE IF NOT EXISTS activity_logs(
        id INTEGER PRIMARY KEY AUTOINCREMENT, actor_id INTEGER NOT NULL,
        action TEXT NOT NULL, target_user_id INTEGER, details TEXT DEFAULT '',
        created_at TEXT NOT NULL)""")
    db("""CREATE TABLE IF NOT EXISTS admins(
        user_id INTEGER PRIMARY KEY, role TEXT NOT NULL DEFAULT 'admin',
        permissions TEXT NOT NULL DEFAULT '[]', added_at TEXT NOT NULL)""")
    db("""CREATE TABLE IF NOT EXISTS build_events(
        id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
        slot INTEGER, status TEXT NOT NULL, details TEXT DEFAULT '',
        created_at TEXT NOT NULL)""")
    # Safe migrations for databases created by older versions.
    columns = {r['name'] for r in db("PRAGMA table_info(hosted_bots)", fetch="all")}
    if "admin_disabled" not in columns:
        db("ALTER TABLE hosted_bots ADD COLUMN admin_disabled INTEGER DEFAULT 0")
    if "off_reason" not in columns:
        db("ALTER TABLE hosted_bots ADD COLUMN off_reason TEXT DEFAULT ''")
    defaults = {
        "welcome_en": (
            "🎉🚀 <b>WELCOME TO XYZ BOT HOSTING</b> 🚀🎉\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "✨ <b>Welcome to our Premium Bot Hosting Platform!</b>\n\n"
            "🔥 <b>What you can do here:</b>\n"
            "📤 Upload your own <code>bot.py</code>\n"
            "⚡ After admin approval, the bot will run automatically\n"
            "🤖 Use <b>MY BOT</b> to On/Off/Delete your bot\n"
            "📊 View live stats and speed\n"
            "🌐 Supports English/Bengali language\n"
            "👑 Owner and Admin support 24/7\n\n"
            "💎 <b>Premium Features:</b>\n"
            "• Auto dependency installation\n"
            "• Real-time logs\n"
            "• Unlimited uptime\n"
            "• Free hosting\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "👉 <b>Start by clicking the button below!</b> 👇"
        ),
        "welcome_bn": (
            "🎉🚀 <b>XYZ BOT HOSTING</b> -এ স্বাগতম 🚀🎉\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "✨ <b>আমাদের প্রিমিয়াম বট হোস্টিং প্ল্যাটফর্মে স্বাগতম!</b>\n\n"
            "🔥 <b>কী কী করতে পারবেন এখানে:</b>\n"
            "📤 নিজের <code>bot.py</code> আপলোড করুন\n"
            "⚡ অ্যাডমিন অ্যাপ্রুভালের পর বট চালু হবে\n"
            "🤖 <b>MY BOT</b> থেকে বট অন/অফ/ডিলিট করুন\n"
            "📊 লাইভ স্ট্যাটস ও স্পিড দেখুন\n"
            "🌐 বাংলা/ইংরেজি ভাষা সাপোর্ট\n"
            "👑 ওনার ও অ্যাডমিন সাপোর্ট ২৪/৭\n\n"
            "💎 <b>প্রিমিয়াম ফিচার:</b>\n"
            "• অটো ডিপেন্ডেন্সি ইনস্টল\n"
            "• রিয়েল-টাইম লগ\n"
            "• আনলিমিটেড আপটাইম\n"
            "• ফ্রি হোস্টিং\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "👉 <b>নিচের বাটন থেকে শুরু করুন!</b> 👇"
        ),
        "update_channel": "https://t.me/",
        "owner_link": "https://t.me/",
        "maintenance": "0",
        "maintenance_message_en": "🛠️ Hosting is temporarily under maintenance. Please try again later.",
        "maintenance_message_bn": "🛠️ হোস্টিং সিস্টেম সাময়িক রক্ষণাবেক্ষণে আছে। অনুগ্রহ করে পরে চেষ্টা করুন।",
        "auto_approve": "0",
        "auto_restart": "0",
    }
    for k, v in defaults.items():
        db("INSERT OR IGNORE INTO settings(key,value) VALUES(?,?)", (k, v))

def setting(key, default=""):
    row = db("SELECT value FROM settings WHERE key=?", (key,), "one")
    return row["value"] if row else default

def set_setting(key, value):
    db("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))

def touch_user(user: types.User):
    now = datetime.now(timezone.utc).isoformat()
    db("""INSERT INTO users(user_id,username,full_name,joined_at,last_seen)
       VALUES(?,?,?,?,?) ON CONFLICT(user_id) DO UPDATE SET
       username=excluded.username, full_name=excluded.full_name, last_seen=excluded.last_seen""",
       (user.id, user.username or "", user.full_name or "", now, now))

def log_activity(actor_id: int, action: str, target_user_id=None, details=""):
    db("INSERT INTO activity_logs(actor_id,action,target_user_id,details,created_at) VALUES(?,?,?,?,?)",
       (actor_id, action, target_user_id, str(details)[:1000], datetime.now(timezone.utc).isoformat()))

def is_admin(uid: int) -> bool:
    if uid in {ADMIN_ID, OWNER_ID}:
        return True
    row = db("SELECT user_id FROM admins WHERE user_id=?", (uid,), "one")
    return bool(row)

def user_lang(uid: int) -> str:
    row = db("SELECT language FROM users WHERE user_id=?", (uid,), "one")
    return row["language"] if row and row["language"] in {"en", "bn"} else "en"

TEXT = {
    "en": {
        "upload": "📤 𝐔𝐏𝐋𝐎𝐀𝐃 𝐅𝐈𝐋𝐄", "profile": "👤 𝐏𝐑𝐎𝐅𝐈𝐋𝐄", "mybot": "🤖 𝐌𝐘 𝐁𝐎𝐓",
        "speed": "⚡ 𝐁𝐎𝐓 𝐒𝐏𝐄𝐄𝐃", "stats": "📊 𝐁𝐎𝐓 𝐒𝐓𝐀𝐓𝐒", "channel": "📢 𝐔𝐏𝐃𝐀𝐓𝐄 𝐂𝐇𝐀𝐍𝐍𝐄𝐋",
        "owner": "👑 𝐎𝐖𝐍𝐄𝐑", "language": "🌐 𝐋𝐀𝐍𝐆𝐔𝐀𝐆𝐄", "admin": "🛠️ 𝐀𝐃𝐌𝐈𝐍 𝐏𝐀𝐍𝐄𝐋",
        "back": "⬅️ 𝐁𝐀𝐂𝐊", "token_prompt": "🔐 <b>Send your Bot Token first.</b>\n\nYour token will be verified immediately. After verification, send your <code>bot.py</code> file.", "token_invalid": "❌ Invalid token. Please send a valid BotFather token.", "token_check": "🔎 <b>Verifying token...</b>", "token_ok": "✅ <b>Token verified!</b>\n🤖 Bot: @{username}\n\nNow send your <code>bot.py</code> file.", "need_file": "📨 Please attach your <code>bot.py</code> as a document.", "back": "⬅️ 𝐁𝐀𝐂𝐊", "send_file": "📨 Send your <code>bot.py</code> file as a Telegram document.",
        "only_py": "❌ Only a file named exactly <code>bot.py</code> is accepted.",
        "pending": "✅ Your file is prepared and sent to the admin for approval.",
        "limit": "⚠️ You already have a bot. Delete it from MY BOT before uploading another one.",
        "no_bot": "You have not uploaded a bot yet.", "on": "🟢 BOT ON", "off": "🔴 BOT OFF", "delete": "🗑️ DELETE BOT",
        "lang_pick": "Choose your language:", "saved": "✅ Language updated.", "banned": "⛔ Your access is restricted. Contact the owner.",
        "not_admin": "⛔ Admin access required.", "error": "❌ Something went wrong. Please try again.",
    },
    "bn": {
        "upload": "📤 ফাইল আপলোড", "profile": "👤 প্রোফাইল", "mybot": "🤖 আমার বট",
        "speed": "⚡ বট স্পিড", "stats": "📊 বট পরিসংখ্যান", "channel": "📢 আপডেট চ্যানেল",
        "owner": "👑 ওনার", "language": "🌐 ভাষা", "admin": "🛠️ অ্যাডমিন প্যানেল",
        "back": "⬅️ 𝐁𝐀𝐂𝐊", "send_file": "📨 Token যাচাই হয়েছে। এখন তোমার <code>bot.py</code> ফাইলটি Telegram document হিসেবে পাঠাও।",
        "only_py": "❌ শুধু <code>bot.py</code> নামের ফাইল গ্রহণ করা হবে।",
        "pending": "✅ ফাইল প্রস্তুত। Admin approval-এর জন্য পাঠানো হয়েছে।",
        "limit": "⚠️ তোমার আগে থেকেই একটি বট আছে। নতুন বট আপলোডের আগে MY BOT থেকে সেটি ডিলিট করো।",
        "no_bot": "তুমি এখনো কোনো বট আপলোড করোনি।", "on": "🟢 BOT ON", "off": "🔴 BOT OFF", "delete": "🗑️ DELETE BOT",
        "lang_pick": "ভাষা নির্বাচন করো:", "saved": "✅ ভাষা পরিবর্তন হয়েছে।", "banned": "⛔ তোমার অ্যাক্সেস বন্ধ। Owner-এর সাথে যোগাযোগ করো।",
        "not_admin": "⛔ শুধু Admin এই কাজটি করতে পারবে।", "error": "❌ সমস্যা হয়েছে। আবার চেষ্টা করো।",
    },
}

def tr(uid, key):
    lang = user_lang(uid)
    custom_message = setting(f"message_{key}_{lang}", "")
    if custom_message:
        return custom_message
    custom_button = setting(f"button_{key}_{lang}", "")
    if custom_button:
        return custom_button
    return TEXT[lang].get(key, TEXT["en"].get(key, key))

def user_message(uid, key, fallback=""):
    lang = user_lang(uid)
    custom = setting(f"message_{key}_{lang}", "")
    if custom:
        return custom
    return fallback

def disabled_bot_for_user(uid):
    return db("SELECT slot,off_reason FROM hosted_bots WHERE user_id=? AND admin_disabled=1 ORDER BY slot LIMIT 1",
              (uid,), "one")

def disabled_notice_text(uid, reason):
    if user_lang(uid) == "bn":
        return ("🚫 <b>বট সাময়িকভাবে বন্ধ</b>\\n\\nঅ্যাডমিনিস্ট্রেটর আপনার হোস্ট করা বটটি সাময়িকভাবে বন্ধ করেছেন।"
                f"\\n\\n<b>কারণ:</b> {html.escape(reason or 'কারণ উল্লেখ করা হয়নি।')}"
                "\\n\\nবিস্তারিত জানতে অ্যাডমিনের সাথে যোগাযোগ করুন।")
    return ("🚫 <b>BOT TEMPORARILY DISABLED</b>\\n\\nYour hosted bot has been temporarily disabled by the administrator."
            f"\\n\\n<b>Reason:</b> {html.escape(reason or 'No reason provided.')}"
            "\\n\\nPlease contact the administrator for further information.")

def main_keyboard(uid):
    t = TEXT[user_lang(uid)]
    rows = [
        [KeyboardButton(text=t["upload"], style="success")],
        [KeyboardButton(text=t["profile"], style="primary"), KeyboardButton(text=t["mybot"], style="success")],
        [KeyboardButton(text=t["speed"], style="primary"), KeyboardButton(text=t["stats"], style="danger")],
        [KeyboardButton(text=t["channel"], style="primary")],
        [KeyboardButton(text=t["owner"], style="success"), KeyboardButton(text=t["language"], style="danger")],
    ]
    if is_admin(uid):
        rows.append([KeyboardButton(text=t["admin"], style="danger")])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True, is_persistent=True, input_field_placeholder="Choose an option…")

def back_kb(uid):
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text=tr(uid, "back"), style="primary")]], resize_keyboard=True)

def admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👥 All Users", callback_data="adm:users:0", style="primary"),
         InlineKeyboardButton(text="📊 Overview", callback_data="adm:overview", style="success")],
        [InlineKeyboardButton(text="🤖 All Hosted Bots", callback_data="adm:bots", style="primary"),
         InlineKeyboardButton(text="🟡 Pending Bots", callback_data="adm:pending", style="success")],
        [InlineKeyboardButton(text="📨 Broadcast", callback_data="adm:broadcast", style="danger")],
        [InlineKeyboardButton(text="📢 Set Channel", callback_data="adm:set:channel", style="primary"),
         InlineKeyboardButton(text="👑 Set Owner", callback_data="adm:set:owner", style="success")],
        [InlineKeyboardButton(text="✍️ Welcome Msg", callback_data="adm:set:welcome", style="primary"),
         InlineKeyboardButton(text="🔒 Maintenance", callback_data="adm:maintenance", style="danger")],
        [InlineKeyboardButton(text="⚡ Auto-Approve", callback_data="adm:toggleauto", style="success"),
         InlineKeyboardButton(text="♻️ Auto-Restart", callback_data="adm:togglerestart", style="success")],
        [InlineKeyboardButton(text="📄 Logs", callback_data="adm:logs", style="primary")],
        [InlineKeyboardButton(text="📝 Messages", callback_data="adm:messages", style="primary"),
         InlineKeyboardButton(text="🔤 Button Names", callback_data="adm:buttons", style="primary")],
        [InlineKeyboardButton(text="💾 Storage", callback_data="adm:storage", style="primary"),
         InlineKeyboardButton(text="🛠️ System Health", callback_data="adm:health", style="success")],
        [InlineKeyboardButton(text="🔄 Refresh Panel", callback_data="adm:home", style="primary")],
    ])

# ========================= HELPERS: PID / LOCK =========================
def pid_file(uid, slot=1):
    return VENV_DIR / f"{uid}_{slot}" / "bot.pid"

def save_pid(uid, slot, pid):
    try:
        pf = pid_file(uid, slot)
        pf.parent.mkdir(parents=True, exist_ok=True)
        pf.write_text(str(pid))
    except Exception:
        log.exception("Could not save pid file")

def read_pid(uid, slot=1):
    try:
        pf = pid_file(uid, slot)
        if pf.exists():
            return int(pf.read_text().strip())
    except Exception:
        pass
    return None

def clear_pid(uid, slot=1):
    try:
        pid_file(uid, slot).unlink(missing_ok=True)
    except Exception:
        pass

def kill_pid(pid: int):
    if not pid:
        return
    try:
        if not psutil.pid_exists(pid):
            return
        proc = psutil.Process(pid)
        if os.name == "posix":
            try:
                os.killpg(os.getpgid(pid), signal.SIGTERM)
            except Exception:
                proc.terminate()
        else:
            proc.terminate()
        try:
            proc.wait(timeout=5)
        except Exception:
            proc.kill()
    except Exception:
        log.exception("kill_pid failed for %s", pid)

def kill_stale_process(uid, slot=1):
    key = (uid, slot)
    proc = PROCESSES.pop(key, None)
    if proc and proc.poll() is None:
        try:
            if os.name == "posix":
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            else:
                proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
        except Exception:
            log.exception("Could not stop child process for %s/%s", uid, slot)
    pid = read_pid(uid, slot)
    if pid:
        kill_pid(pid)
    clear_pid(uid, slot)
    row = db("SELECT pid FROM hosted_bots WHERE user_id=? AND slot=?", (uid, slot), "one")
    if row and row["pid"]:
        kill_pid(int(row["pid"]))

# ========================= PROCESS MANAGEMENT =========================
def extract_embedded_token(path: Path) -> str:
    try:
        source = path.read_text(encoding="utf-8", errors="ignore")
        patterns = [
            r"(?m)^\s*(?:BOT_TOKEN|TOKEN|API_TOKEN)\s*=\s*['\"]([0-9]{5,}:[A-Za-z0-9_-]{15,})['\"]",
        ]
        for pattern in patterns:
            match = re.search(pattern, source)
            if match:
                return match.group(1)
    except Exception:
        log.exception("Could not inspect uploaded bot token")
    return ""

async def refresh_bot_username(uid: int, slot: int = 1):
    path = bot_file(uid, slot)
    token = read_bot_token(uid, slot)
    if not token:
        return
    try:
        timeout = ClientTimeout(total=8)
        async with ClientSession(timeout=timeout) as session:
            async with session.get(f"https://api.telegram.org/bot{token}/getMe") as response:
                payload = await response.json(content_type=None)
        if payload.get("ok"):
            username = payload["result"].get("username", "")
            db("UPDATE hosted_bots SET bot_username=? WHERE user_id=? AND slot=?", (username, uid, slot))
    except Exception:
        log.exception("Could not resolve hosted bot username for %s", uid)

def bot_dir(uid, slot=1):
    return BOTS_DIR / f"{uid}_{slot}"

def bot_file(uid, slot=1):
    return bot_dir(uid, slot) / "bot.py"

def bot_token_file(uid, slot=1):
    return bot_dir(uid, slot) / ".bot_token"

def save_bot_token(uid, slot, token):
    token_path = bot_token_file(uid, slot)
    token_path.parent.mkdir(parents=True, exist_ok=True)
    token_path.write_text(token.strip(), encoding="utf-8")
    try:
        token_path.chmod(0o600)
    except OSError:
        pass

def read_bot_token(uid, slot=1):
    token_path = bot_token_file(uid, slot)
    return token_path.read_text(encoding="utf-8").strip() if token_path.exists() else ""

def remove_bot_token(uid, slot=1):
    bot_token_file(uid, slot).unlink(missing_ok=True)

def remove_bot_storage(uid, slot=1):
    """Remove a bot only after its process has been stopped."""
    kill_stale_process(uid, slot)
    shutil.rmtree(bot_dir(uid, slot), ignore_errors=True)
    shutil.rmtree(venv_dir(uid, slot), ignore_errors=True)
    (LOG_DIR / f"bot_{uid}_{slot}.log").unlink(missing_ok=True)
    clear_pid(uid, slot)

def inject_user_token(path: Path):
    """Make common hard-coded TOKEN assignments use the private BOT_TOKEN env value."""
    source = path.read_text(encoding="utf-8", errors="ignore")
    assignment = re.compile(r"(?m)^(\s*)(BOT_TOKEN|TOKEN|API_TOKEN)\s*=\s*([\"'])([0-9]{5,}:[A-Za-z0-9_-]{15,})\3\s*$")
    source, count = assignment.subn(lambda m: f"{m.group(1)}{m.group(2)} = os.getenv('BOT_TOKEN', '')", source)
    if count and not re.search(r"(?m)^\s*import\s+os\b|^\s*from\s+os\s+import\b", source):
        source = "import os\n" + source
    path.write_text(source, encoding="utf-8")

def venv_dir(uid, slot=1):
    return VENV_DIR / f"{uid}_{slot}"

def venv_python(uid, slot=1):
    vdir = venv_dir(uid, slot)
    if os.name == "posix":
        return vdir / "bin" / "python"
    return vdir / "Scripts" / "python.exe"

IMPORT_MAP = {
    "telegram": "python-telegram-bot", "telebot": "pyTelegramBotAPI", "aiogram": "aiogram",
    "pyrogram": "pyrogram", "telethon": "telethon", "discord": "discord.py",
    "requests": "requests", "aiohttp": "aiohttp", "flask": "flask",
    "fastapi": "fastapi", "uvicorn": "uvicorn", "django": "django",
    "bs4": "beautifulsoup4", "PIL": "Pillow", "cv2": "opencv-python",
    "numpy": "numpy", "pandas": "pandas", "dotenv": "python-dotenv",
    "psutil": "psutil", "pytz": "pytz", "dateutil": "python-dateutil",
    "sqlalchemy": "SQLAlchemy", "pymongo": "pymongo", "redis": "redis",
    "httpx": "httpx", "yaml": "PyYAML", "jinja2": "Jinja2",
    "tgcrypto": "TgCrypto", "cryptography": "cryptography", "motor": "motor",
    "pydantic": "pydantic", "asyncpg": "asyncpg", "psycopg2": "psycopg2-binary",
    "mysql": "mysql-connector-python", "gtts": "gTTS",
    "googletrans": "googletrans==4.0.0-rc1", "speech_recognition": "SpeechRecognition",
    "moviepy": "moviepy", "gspread": "gspread", "oauth2client": "oauth2client",
    "tornado": "tornado", "openai": "openai", "google": "google-generativeai",
    "groq": "groq", "aiofiles": "aiofiles", "pydub": "pydub",
    "yt_dlp": "yt-dlp", "instaloader": "instaloader", "wikipedia": "wikipedia",
    "translate": "translate", "langdetect": "langdetect", "emoji": "emoji",
    "colorama": "colorama", "rich": "rich", "tabulate": "tabulate",
    "prettytable": "prettytable", "pdf2image": "pdf2image", "PyPDF2": "PyPDF2",
    "docx": "python-docx", "openpyxl": "openpyxl", "matplotlib": "matplotlib",
    "seaborn": "seaborn", "scipy": "scipy", "sklearn": "scikit-learn",
    "tensorflow": "tensorflow", "torch": "torch", "transformers": "transformers",
    "nltk": "nltk", "spacy": "spacy", "qrcode": "qrcode",
    "barcode": "python-barcode", "fpdf": "fpdf", "reportlab": "reportlab",
    "docx2pdf": "docx2pdf", "pypdf": "pypdf", "xlsxwriter": "XlsxWriter",
    "lxml": "lxml", "httplib2": "httplib2",
    "googleapiclient": "google-api-python-client",
    "google_auth_oauthlib": "google-auth-oauthlib",
}

BASE_PACKAGES = ["aiogram", "aiohttp", "psutil", "python-dotenv", "requests"]

def detect_requirements(source: str) -> list[str]:
    try:
        found = set(BASE_PACKAGES)
        for line in source.splitlines():
            m = re.match(r"\s*(?:from|import)\s+([a-zA-Z0-9_]+)", line)
            if m:
                mod = m.group(1)
                if mod in IMPORT_MAP:
                    found.add(IMPORT_MAP[mod])
        return sorted(found)
    except Exception:
        return BASE_PACKAGES

def requirements_for(uid, slot=1) -> tuple[list[str], str]:
    d = bot_dir(uid, slot)
    req_file = d / "requirements.txt"
    if req_file.exists():
        try:
            pkgs = []
            for line in req_file.read_text(encoding="utf-8", errors="ignore").splitlines():
                line = line.strip()
                if not line or line.startswith("#") or line.startswith("-"):
                    continue
                pkgs.append(line)
            if pkgs:
                return pkgs, "requirements.txt"
        except Exception:
            log.exception("requirements.txt parse failed")
    source = bot_file(uid, slot).read_text(encoding="utf-8", errors="ignore")
    return detect_requirements(source), "auto"

def req_hash(packages: list[str]) -> str:
    h = hashlib.sha256()
    for p in packages:
        h.update(p.encode())
    return h.hexdigest()[:16]

def installed_hash_file(uid, slot=1):
    return venv_dir(uid, slot) / ".deps_hash"

def read_installed_hash(uid, slot=1) -> str:
    f = installed_hash_file(uid, slot)
    return f.read_text().strip() if f.exists() else ""

def write_installed_hash(uid, slot, val):
    installed_hash_file(uid, slot).parent.mkdir(parents=True, exist_ok=True)
    installed_hash_file(uid, slot).write_text(val)

# ========================= ANIMATION & INSTALL =========================
async def _progress_animation(msg, base_text, done_event):
    bars = [
        "▱▱▱▱▱▱▱▱▱▱", "▰▱▱▱▱▱▱▱▱▱", "▰▰▱▱▱▱▱▱▱▱", "▰▰▰▱▱▱▱▱▱▱",
        "▰▰▰▰▱▱▱▱▱▱", "▰▰▰▰▰▱▱▱▱▱", "▰▰▰▰▰▰▱▱▱▱", "▰▰▰▰▰▰▰▱▱▱",
        "▰▰▰▰▰▰▰▰▱▱", "▰▰▰▰▰▰▰▰▰▱", "▰▰▰▰▰▰▰▰▰▰"
    ]
    step = 0
    while not done_event.is_set():
        bar = bars[min(step, len(bars)-1)]
        try:
            await msg.edit_text(f"{bar} <b>{base_text}…</b>")
        except Exception:
            pass
        step += 1
        try:
            await asyncio.wait_for(done_event.wait(), timeout=1.5)
        except asyncio.TimeoutError:
            continue

async def ensure_venv_and_install(uid, slot=1, notify=True):
    vdir = venv_dir(uid, slot)
    py = venv_python(uid, slot)
    packages, _src = requirements_for(uid, slot)
    want_hash = req_hash(packages)

    if not py.exists():
        log.info("Creating venv for %s/%s", uid, slot)
        proc = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "venv", str(vdir),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
        )
        await proc.communicate()
        py = venv_python(uid, slot)
        if not py.exists():
            raise RuntimeError("venv creation failed")

    if read_installed_hash(uid, slot) == want_hash:
        return py

    status_msg = None
    done_event = asyncio.Event()
    anim_task = None

    if notify:
        try:
            status_msg = await bot.send_message(uid, "▱▱▱▱▱▱▱▱▱▱ <b>Installing dependencies…</b>")
            anim_task = asyncio.create_task(_progress_animation(status_msg, "Installing dependencies", done_event))
        except Exception:
            status_msg = None

    try:
        proc = await asyncio.create_subprocess_exec(
            str(py), "-m", "pip", "install", "--upgrade", "--quiet",
            "pip", "setuptools", "wheel",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
        )
        await proc.communicate()

        log.info("Installing for %s/%s: %s", uid, slot, packages)
        proc = await asyncio.create_subprocess_exec(
            str(py), "-m", "pip", "install", "--quiet",
            "--prefer-binary", "--no-input", *packages,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
        )
        out, _ = await proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(out.decode(errors="replace")[-2000:])
        
        write_installed_hash(uid, slot, want_hash)
        return py
    finally:
        done_event.set()
        if anim_task:
            try:
                await anim_task
            except Exception:
                pass
        if status_msg:
            try:
                await status_msg.edit_text("▰▰▰▰▰▰▰▰▰▰ 100%\n✅ <b>Dependencies installed successfully!</b>")
                await asyncio.sleep(1)
            except Exception:
                pass

def stop_process(uid, slot=1):
    kill_stale_process(uid, slot)
    db("UPDATE hosted_bots SET status='OFF',pid=NULL,started_at=NULL WHERE user_id=? AND slot=?", (uid, slot))

async def start_process(uid, slot=1):
    path = bot_file(uid, slot)
    if not path.exists():
        raise FileNotFoundError("bot.py not found")

    # start_and_report() owns STARTING; checking it here deadlocks the launcher.
    kill_stale_process(uid, slot)

    # Dependencies are prepared before the admin approval request.
    # This hash check should be quick and must not show an install animation.
    key = (uid, slot)
    py = await ensure_venv_and_install(uid, slot, notify=False)

    # ফ্রেশ লগ ফাইল
    log_path = LOG_DIR / f"bot_{uid}_{slot}.log"
    with open(log_path, "w") as f:
        f.write("") 
        
    out = open(log_path, "ab", buffering=0)
    env = os.environ.copy()
    # Never expose the Hosting Manager credentials to uploaded scripts.
    env.pop("BOT_TOKEN", None)
    env.pop("ADMIN_ID", None)
    env.pop("OWNER_ID", None)
    uploaded_token = read_bot_token(uid, slot)
    if not uploaded_token:
        out.close()
        raise RuntimeError("Bot token is missing. Upload again and provide a valid token first.")
    env["BOT_TOKEN"] = uploaded_token
    env["PYTHONUNBUFFERED"] = "1"
    kwargs = {"cwd": str(path.parent), "stdin": subprocess.DEVNULL, "stdout": out,
              "stderr": subprocess.STDOUT, "close_fds": True, "env": env}
    if os.name == "posix":
        kwargs["start_new_session"] = True
    
    proc = subprocess.Popen([str(py), "-u", "bot.py"], **kwargs)
    PROCESSES[key] = proc
    save_pid(uid, slot, proc.pid)
    db("UPDATE hosted_bots SET status='STARTING',pid=?,started_at=? WHERE user_id=? AND slot=?",
       (proc.pid, datetime.now(timezone.utc).isoformat(), uid, slot))
    return proc

async def _run_bot_bg(uid, slot):
    """ব্যাকগ্রাউন্ডে বট রান করবে। হোস্টিং বট আটকে যাবে না।"""
    try:
        db("UPDATE hosted_bots SET status='STARTING' WHERE user_id=? AND slot=?", (uid, slot))
        proc = await start_process(uid, slot)
        
        # প্রসেস রান হচ্ছে কিনা তা ভেরিফাই করতে অপেক্ষা
        await asyncio.sleep(2.5)
        
        if proc.poll() is not None:
            log_text = read_bot_log(uid, slot)
            db("UPDATE hosted_bots SET status='ERROR',pid=NULL,last_error=? WHERE user_id=? AND slot=?",
               (log_text[-3500:], uid, slot))
            await bot.send_message(uid, "❌ <b>BOT CRASHED ON STARTUP</b>\n\nYour bot.py has an error:\n<pre>" + html.escape(log_text[-3200:] or "Process exited without output") + "</pre>", reply_markup=main_keyboard(uid))
            return
            
        db("UPDATE hosted_bots SET status='ON',pid=? WHERE user_id=? AND slot=?", (proc.pid, uid, slot))
        await refresh_bot_username(uid, slot)
        await bot.send_message(uid, "✅ <b>YOUR BOT IS NOW ONLINE</b>\n\n🚀 Your <code>bot.py</code> process started successfully!\n\n👉 Use MY BOT to manage it.", reply_markup=main_keyboard(uid))
        
    except Exception as exc:
        log.exception("Bot start failed for %s/%s", uid, slot)
        db("UPDATE hosted_bots SET status='ERROR',pid=NULL,last_error=? WHERE user_id=? AND slot=?", (str(exc)[:3500], uid, slot))
        try:
            await bot.send_message(uid, "❌ <b>BOT START ERROR</b>\\n\\n<pre>" + html.escape(str(exc)[:3000]) + "</pre>", reply_markup=main_keyboard(uid))
        except Exception:
            log.exception("Could not notify user about bot start failure")
    finally:
        STARTING.discard((uid, slot))

async def start_and_report(uid, slot=1, notify=True):
    """প্রসেস শুরু করার জন্য ব্যাকগ্রাউন্ড টাস্ক তৈরি করে"""
    row = db("SELECT admin_disabled,off_reason FROM hosted_bots WHERE user_id=? AND slot=?", (uid, slot), "one")
    if not row:
        return False
    if row["admin_disabled"]:
        if notify:
            reason = row["off_reason"] or "No reason provided."
            try:
                await bot.send_message(uid, disabled_notice_text(uid, reason))
            except Exception:
                pass
        return False
    key = (uid, slot)
    if key in STARTING:
        return False
    STARTING.add(key)
    
    # এখানে সরাসরি await না করে ব্যাকগ্রাউন্ড টাস্ক পাঠানো হলো
    asyncio.create_task(_run_bot_bg(uid, slot))
    return True

def read_bot_log(uid, slot=1):
    p = LOG_DIR / f"bot_{uid}_{slot}.log"
    if not p.exists():
        return "No log output available."
    try:
        return p.read_text(encoding="utf-8", errors="replace")[-6000:]
    except Exception as exc:
        return str(exc)

async def monitor_processes():
    while True:
        await asyncio.sleep(5)
        for key, proc in list(PROCESSES.items()):
            uid, slot = key
            if proc.poll() is not None:
                PROCESSES.pop(key, None)
                STARTING.discard(key)
                clear_pid(uid, slot)
                row = db("SELECT status FROM hosted_bots WHERE user_id=? AND slot=?", (uid, slot), "one")
                if row and row["status"] not in {"OFF", "DELETED"}:
                    log_text = read_bot_log(uid, slot)
                    db("UPDATE hosted_bots SET status='ERROR',pid=NULL,last_error=? WHERE user_id=? AND slot=?",
                       (log_text[-3500:], uid, slot))
                    try:
                        await bot.send_message(uid, "⚠️ <b>YOUR BOT STOPPED</b>\n\nLatest error/output:\n<pre>" + html.escape(log_text[-3000:]) + "</pre>")
                    except Exception:
                        pass
                    disabled = db("SELECT admin_disabled FROM hosted_bots WHERE user_id=? AND slot=?", (uid, slot), "one")
                    if setting("auto_restart", "0") == "1" and disabled and not disabled["admin_disabled"]:
                        now_ts = time.time()
                        attempts = [t for t in CRASH_RESTARTS.get(key, []) if now_ts - t < 600]
                        if len(attempts) < 3:
                            attempts.append(now_ts)
                            CRASH_RESTARTS[key] = attempts
                            await asyncio.sleep(2)
                            await start_and_report(uid, slot, notify=True)

async def safe_edit_or_send(call: CallbackQuery, text: str, reply_markup=None, parse_mode="HTML"):
    try:
        await call.message.edit_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
    except Exception:
        try:
            await call.message.answer(text, reply_markup=reply_markup, parse_mode=parse_mode)
        except Exception:
            pass



@dp.message(F.photo | F.video)
async def broadcast_media(message: types.Message):
    uid = message.from_user.id
    if not is_admin(uid) or USER_STATES.get(uid) != "admin_broadcast":
        return
    USER_STATES.pop(uid, None)
    users = db("SELECT user_id FROM users WHERE banned=0", fetch="all")
    delivered = 0
    for item in users:
        try:
            await bot.copy_message(item["user_id"], message.chat.id, message.message_id)
            delivered += 1
            await asyncio.sleep(0.04)
        except Exception:
            pass
    await message.answer(f"✅ Media broadcast delivered to {delivered}/{len(users)} users.", reply_markup=main_keyboard(uid))

# ========================= USER SCREENS =========================
@dp.message(CommandStart())
async def start(message: types.Message):
    uid = message.from_user.id
    touch_user(message.from_user)
    row = db("SELECT banned FROM users WHERE user_id=?", (uid,), "one")
    if row and row["banned"]:
        await message.answer(tr(uid, "banned")); return
    disabled = disabled_bot_for_user(uid)
    if disabled:
        await message.answer(disabled_notice_text(uid, disabled["off_reason"]))
        return
    key = "welcome_bn" if user_lang(uid) == "bn" else "welcome_en"
    await message.answer(setting(key), reply_markup=main_keyboard(uid))

@dp.message(F.text)
async def text_router(message: types.Message):
    uid = message.from_user.id
    touch_user(message.from_user)
    text = (message.text or "").strip()
    row = db("SELECT banned FROM users WHERE user_id=?", (uid,), "one")
    if row and row["banned"]:
        await message.answer(tr(uid, "banned")); return
    disabled = disabled_bot_for_user(uid)
    if disabled and not is_admin(uid):
        await message.answer(disabled_notice_text(uid, disabled["off_reason"]))
        return
    if MAINTENANCE and not is_admin(uid):
        key = "maintenance_message_bn" if user_lang(uid) == "bn" else "maintenance_message_en"
        await message.answer(setting(key), reply_markup=None)
        return

    state = USER_STATES.get(uid)
    # Navigation and menu buttons must always work, even while waiting for a token/file.
    menu_labels = set(TEXT[user_lang(uid)].values())
    navigation_texts = menu_labels | {"🇧🇩 বাংলা", "🇬🇧 ENGLISH", "/start"}
    if state in {"await_token", "await_upload"} and text in navigation_texts:
        if text != tr(uid, "back") and text not in {"🇧🇩 বাংলা", "🇬🇧 ENGLISH"}:
            USER_STATES.pop(uid, None)
            PENDING_TOKENS.pop(uid, None)
        elif text == tr(uid, "back"):
            USER_STATES.pop(uid, None)
            PENDING_TOKENS.pop(uid, None)
            await message.answer("🏠 <b>MAIN MENU</b>", reply_markup=main_keyboard(uid)); return
    state = USER_STATES.get(uid)
    if state:
        if state == "await_token":
            if text == TEXT[user_lang(uid)]["back"] or text.lower() in {"/cancel", "cancel"}:
                USER_STATES.pop(uid, None)
                PENDING_TOKENS.pop(uid, None)
                await message.answer("🏠 <b>MAIN MENU</b>", reply_markup=main_keyboard(uid))
                return
            token = text.strip()
            try:
                await message.delete()  # keep the submitted token out of the visible chat history
            except Exception:
                pass
            if not re.fullmatch(r"[0-9]{5,}:[A-Za-z0-9_-]{20,}", token):
                await message.answer(tr(uid, "token_invalid"))
                return
            status = await message.answer(tr(uid, "token_check"))
            try:
                timeout = ClientTimeout(total=12)
                async with ClientSession(timeout=timeout) as session:
                    async with session.get(f"https://api.telegram.org/bot{token}/getMe") as response:
                        payload = await response.json(content_type=None)
                if not payload.get("ok"):
                    await status.edit_text(tr(uid, "token_invalid"))
                    return
                PENDING_TOKENS[uid] = token
                USER_STATES[uid] = "await_upload"
                bot_info = payload.get("result", {})
                await status.edit_text(tr(uid, "token_ok").format(username=html.escape(bot_info.get('username') or 'unknown')))
            except Exception:
                log.exception("Token validation request failed for user %s", uid)
                await status.edit_text("⚠️ Token যাচাই করা যায়নি। কিছুক্ষণ পরে আবার চেষ্টা করো।" if user_lang(uid) == "bn" else "⚠️ Could not verify the token. Please try again later.")
            return
        if state == "await_upload":
            if text == TEXT[user_lang(uid)]["back"]:
                USER_STATES.pop(uid, None)
                await message.answer("🏠 <b>MAIN MENU</b>", reply_markup=main_keyboard(uid))
            else:
                await message.answer(tr(uid, "need_file"))
            return
        if state == "admin_broadcast":
            if not is_admin(uid):
                USER_STATES.pop(uid, None); return
            USER_STATES.pop(uid, None)
            users = db("SELECT user_id FROM users WHERE banned=0", fetch="all")
            ok = 0
            for item in users:
                try:
                    await bot.send_message(item["user_id"], text)
                    ok += 1
                    await asyncio.sleep(0.04)
                except Exception:
                    pass
            await message.answer(f"✅ Broadcast delivered to {ok}/{len(users)} users.", reply_markup=main_keyboard(uid))
            return
        if state.startswith("admin_set_"):
            if not is_admin(uid):
                USER_STATES.pop(uid, None); return
            kind = state.removeprefix("admin_set_")
            USER_STATES.pop(uid, None)
            if kind == "channel":
                set_setting("update_channel", text)
            elif kind == "owner":
                set_setting("owner_link", text)
            elif kind == "welcome":
                set_setting("welcome_en", text)
            await message.answer("✅ Setting saved.", reply_markup=main_keyboard(uid))
            return
        if state.startswith("admin_bot_off_reason:"):
            if not is_admin(uid):
                USER_STATES.pop(uid, None); return
            _, target_uid, target_slot = state.split(":")
            USER_STATES.pop(uid, None)
            reason = text.strip()
            if not reason:
                await message.answer("Reason cannot be empty. Please try again.")
                USER_STATES[uid] = state
                return
            target_uid, target_slot = int(target_uid), int(target_slot)
            stop_process(target_uid, target_slot)
            db("UPDATE hosted_bots SET admin_disabled=1,off_reason=?,status='OFF',pid=NULL,started_at=NULL WHERE user_id=? AND slot=?",
               (reason[:1000], target_uid, target_slot))
            log_activity(uid, "bot_disabled", target_uid, f"slot={target_slot}; reason={reason[:500]}")
            await message.answer(f"🚫 Bot disabled for user <code>{target_uid}</code> · slot {target_slot}.")
            try:
                await bot.send_message(target_uid, "🚫 <b>BOT TEMPORARILY DISABLED</b>\\n\\n"
                    "Your hosted bot has been temporarily disabled by the administrator.\\n\\n"
                    f"<b>Reason:</b> {html.escape(reason)}\\n\\n"
                    "Please contact the administrator for further information.")
            except Exception:
                pass
            return
        if state.startswith("admin_edit_setting:"):
            if not is_admin(uid):
                USER_STATES.pop(uid, None); return
            key = state.split(":", 1)[1]
            USER_STATES.pop(uid, None)
            set_setting(key, text[:4000])
            log_activity(uid, "setting_updated", None, key)
            await message.answer("✅ Message saved.", reply_markup=main_keyboard(uid))
            return
        if state.startswith("admin_edit_button:"):
            if not is_admin(uid):
                USER_STATES.pop(uid, None); return
            _, _, key, lang = state.split(":")
            USER_STATES.pop(uid, None)
            set_setting(f"button_{key}_{lang}", text[:64])
            log_activity(uid, "button_renamed", None, f"{key}:{lang}")
            await message.answer("✅ Button label saved.", reply_markup=main_keyboard(uid))
            return
        if state == "admin_user_lookup":
            USER_STATES.pop(uid, None)
            if text.isdigit():
                await show_user_detail(message, int(text))
            else:
                await message.answer("Send a numeric Telegram user ID.", reply_markup=main_keyboard(uid))
            return

    t = TEXT[user_lang(uid)]
    if text == t["upload"]:
        if MAINTENANCE and not is_admin(uid):
            key = "maintenance_message_bn" if user_lang(uid) == "bn" else "maintenance_message_en"
            await message.answer(setting(key), reply_markup=None); return
        if not is_admin(uid):
            existing = db("SELECT id FROM hosted_bots WHERE user_id=?", (uid,), "one")
            if existing:
                await message.answer(tr(uid, "limit"), reply_markup=main_keyboard(uid)); return
        PENDING_TOKENS.pop(uid, None)
        USER_STATES[uid] = "await_token"
        await message.answer(tr(uid, "token_prompt"), reply_markup=back_kb(uid))
        return
    if text == t["profile"]: await show_profile(message); return
    if text == t["mybot"]:   await show_my_bot(message); return
    if text == t["speed"]:   await show_speed(message); return
    if text == t["stats"]:   await show_stats(message); return
    if text == t["channel"]:
        url = setting("update_channel")
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📢 Open Update Channel", url=url, style="primary")]])
        await message.answer("📢 <b>UPDATE CHANNEL</b>\n\nClick the button below to open our update channel.", reply_markup=kb)
        return
    if text == t["owner"]:
        url = setting("owner_link")
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="👑 Contact Owner", url=url, style="success")]])
        await message.answer("👑 <b>OWNER CONTACT</b>\n\nClick the button below to contact the owner.", reply_markup=kb)
        return
    if text == t["language"]:
        await message.answer(tr(uid, "lang_pick"), reply_markup=ReplyKeyboardMarkup(keyboard=[
            [KeyboardButton(text="🇧🇩 বাংলা", style="success"), KeyboardButton(text="🇬🇧 ENGLISH", style="primary")],
            [KeyboardButton(text=tr(uid, "back"), style="danger")]], resize_keyboard=True))
        return
    if text in {"🇧🇩 বাংলা", "🇬🇧 ENGLISH"}:
        lang = "bn" if text == "🇧🇩 বাংলা" else "en"
        db("UPDATE users SET language=? WHERE user_id=?", (lang, uid))
        await message.answer(tr(uid, "saved"), reply_markup=main_keyboard(uid)); return
    if text == tr(uid, "back"):
        USER_STATES.pop(uid, None)
        await message.answer("🏠 <b>MAIN MENU</b>", reply_markup=main_keyboard(uid)); return
    if text == t["admin"] and is_admin(uid):
        await message.answer("🛠️ <b>ADMIN CONTROL CENTER</b>\nSelect a control below.", reply_markup=admin_kb()); return

@dp.message(F.document)
async def receive_bot_file(message: types.Message):
    uid = message.from_user.id
    touch_user(message.from_user)
    doc = message.document
    disabled = disabled_bot_for_user(uid)
    if disabled and not is_admin(uid):
        await message.answer(disabled_notice_text(uid, disabled["off_reason"]))
        return
    if USER_STATES.get(uid) != "await_upload" or not PENDING_TOKENS.get(uid):
        await message.answer("প্রথমে UPLOAD FILE চাপো, তারপর Bot Token যাচাই করিয়ে ফাইল পাঠাও।", reply_markup=main_keyboard(uid)); return
    if doc.file_name != "bot.py":
        await message.answer(tr(uid, "only_py")); return
    if (doc.file_size or 0) > MAX_BOT_FILE_MB * 1024 * 1024:
        await message.answer((f"❌ ফাইলটি অনেক বড়। সর্বোচ্চ {MAX_BOT_FILE_MB} MB হতে পারবে." if user_lang(uid) == "bn" else f"❌ File is too large. Maximum size: {MAX_BOT_FILE_MB} MB.")); return
    if not is_admin(uid):
        existing = db("SELECT id FROM hosted_bots WHERE user_id=?", (uid,), "one")
        if existing:
            USER_STATES.pop(uid, None)
            await message.answer(tr(uid, "limit"), reply_markup=main_keyboard(uid)); return

    rows = db("SELECT slot FROM hosted_bots WHERE user_id=? ORDER BY slot", (uid,), "all")
    used = {r["slot"] for r in rows}
    slot = 1
    while slot in used:
        slot += 1
    user_dir = bot_dir(uid, slot)
    user_dir.mkdir(parents=True, exist_ok=True)
    target = user_dir / "bot.py"
    try:
        tg_file = await bot.get_file(doc.file_id)
        await bot.download_file(tg_file.file_path, destination=target)
        save_bot_token(uid, slot, PENDING_TOKENS[uid])
        inject_user_token(target)
        me = message.from_user
        now = datetime.now(timezone.utc).isoformat()
        db("INSERT INTO hosted_bots(user_id,file_path,file_name,file_size,status,uploaded_at,slot) VALUES(?,?,?,?,?,?,?)",
           (uid, str(target), "bot.py", doc.file_size or 0, "PENDING", now, slot))
        USER_STATES.pop(uid, None)
        PENDING_TOKENS.pop(uid, None)

        build_msg = await message.answer(
            "<b>BUILD YOUR BOT PLEASE WAIT....</b>\n\n"
            "<code>▱▱▱▱▱▱▱▱▱▱</code>\n"
            "⚙️ Preparing your bot and installing requirements..."
        )
        done = asyncio.Event()
        build_task = asyncio.create_task(_progress_animation(build_msg, "BUILD YOUR BOT PLEASE WAIT", done))
        try:
            await ensure_venv_and_install(uid, slot, notify=False)
            db("INSERT INTO build_events(user_id,slot,status,details,created_at) VALUES(?,?,?,?,?)",
               (uid, slot, "SUCCESS", "dependencies prepared", datetime.now(timezone.utc).isoformat()))
        except Exception as build_exc:
            db("INSERT INTO build_events(user_id,slot,status,details,created_at) VALUES(?,?,?,?,?)",
               (uid, slot, "FAILED", str(build_exc)[:1000], datetime.now(timezone.utc).isoformat()))
            raise
        finally:
            done.set()
            try:
                await build_task
            except Exception:
                pass
        try:
            await build_msg.edit_text("<b>BUILD YOUR BOT PLEASE WAIT....</b>\n\n<code>▰▰▰▰▰▰▰▰▰▰</code> 100%\n✅ Build preparation complete.")
        except Exception:
            pass

        username = f"@{me.username}" if me.username else "Not set"
        caption = (
            "📥 <b>NEW BOT APPROVAL REQUEST</b>\n\n"
            f"👤 User: {html.escape(me.full_name)}\n"
            f"🔗 Username: {html.escape(username)}\n"
            f"🆔 User ID: <code>{uid}</code>\n"
            f"📄 File: <code>bot.py</code>\n"
            f"💾 Size: {(doc.file_size or 0)/1024:.1f} KB\n"
            f"🕒 Uploaded: {html.escape(now)}\n\nReview and choose an action."
        )
        markup = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="✅ ACCEPT & RUN", callback_data=f"approve:{uid}:{slot}", style="success"),
            InlineKeyboardButton(text="❌ REJECT", callback_data=f"reject:{uid}:{slot}", style="danger")
        ]])
        await bot.send_message(ADMIN_ID, caption + "\\n\\n<i>Source file remains in runtime storage; no admin-side file backup is created.</i>", reply_markup=markup)
        await message.answer(tr(uid, "pending"), reply_markup=main_keyboard(uid))

        auto = setting("auto_approve", "0") == "1"
        if auto:
            await start_and_report(uid, slot, notify=True)

    except Exception as exc:
        log.exception("Upload failed")
        target.unlink(missing_ok=True)
        remove_bot_token(uid, slot)
        db("DELETE FROM hosted_bots WHERE user_id=? AND slot=?", (uid, slot))
        await message.answer(("❌ আপলোড ব্যর্থ হয়েছে:\n<code>" if user_lang(uid) == "bn" else "❌ Upload failed:\n<code>") + html.escape(str(exc)) + "</code>", reply_markup=main_keyboard(uid))

async def show_profile(message):
    u = message.from_user
    uid = u.id
    row = db("SELECT joined_at,language FROM users WHERE user_id=?", (uid,), "one")
    count = db("SELECT COUNT(*) n FROM hosted_bots WHERE user_id=?", (uid,), "one")["n"]
    joined = row["joined_at"][:10] if row else "Unknown"
    text = ("👤 <b>USER PROFILE</b>\n\n"
            f"🧑 Name: {html.escape(u.full_name)}\n"
            f"🔗 Username: @{html.escape(u.username) if u.username else 'Not set'}\n"
            f"🆔 ID: <code>{uid}</code>\n"
            f"📅 Joined: <code>{joined}</code>\n"
            f"🤖 Hosted bots: <b>{count}</b>\n"
            f"🌐 Language: <b>{user_lang(uid).upper()}</b>")
    photos = await bot.get_user_profile_photos(uid, limit=1)
    if photos.total_count:
        await message.answer_photo(photos.photos[0][-1].file_id, caption=text, reply_markup=main_keyboard(uid))
    else:
        await message.answer(text, reply_markup=main_keyboard(uid))

async def show_my_bot(message):
    uid = message.from_user.id
    rows = db("SELECT * FROM hosted_bots WHERE user_id=? ORDER BY slot", (uid,), "all")
    if not rows:
        await message.answer(tr(uid, "no_bot"), reply_markup=main_keyboard(uid)); return
    if len(rows) == 1:
        await send_bot_card(message, rows[0])
    else:
        buttons = [[InlineKeyboardButton(text=f"{'🟢' if r['status']=='ON' else '🔴'} Slot {r['slot']} · {r['status']}",
                                         callback_data=f"viewbot:{uid}:{r['slot']}")] for r in rows]
        await message.answer("🤖 <b>YOUR BOTS</b>\n\nSelect a bot to manage:", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

async def send_bot_card(message, row):
    uid = row["user_id"]; slot = row["slot"]
    started = row["started_at"] or "—"
    runtime = "—"
    if row["started_at"] and row["status"] in {"ON", "STARTING"}:
        try:
            started_dt = datetime.fromisoformat(row["started_at"])
            seconds = max(0, int((datetime.now(timezone.utc) - started_dt).total_seconds()))
            runtime = f"{seconds // 3600}h {(seconds % 3600) // 60}m"
        except Exception:
            pass
    status_emoji = {"ON": "🟢", "OFF": "🔴", "ERROR": "⚠️", "PENDING": "🟡", "STARTING": "⏳"}.get(row["status"], "⚪")
    text = ("🤖 <b>MY BOT</b>\n\n"
            f"📄 File: <code>{html.escape(row['file_name'])}</code>\n"
            f"👤 Bot owner: <code>{uid}</code>\n"
            f"🔗 Bot username: <b>@{html.escape(row['bot_username']) if row['bot_username'] else 'Not detected'}</b>\n"
            f"📦 Size: {row['file_size']/1024:.1f} KB\n"
            f"📌 Status: <b>{status_emoji} {row['status']}</b>\n"
            f"🕒 Started: <code>{html.escape(started)}</code>\n"
            f"⏱️ Running time: <b>{runtime}</b>\n"
            f"🧾 Uploaded: <code>{html.escape(row['uploaded_at'][:19])}</code>\n"
            f"🔢 Slot: <b>{slot}</b>")
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🟢 BOT ON", callback_data=f"self:on:{uid}:{slot}", style="success"),
         InlineKeyboardButton(text="🔴 BOT OFF", callback_data=f"self:off:{uid}:{slot}", style="danger")],
        [InlineKeyboardButton(text="🗑️ DELETE BOT", callback_data=f"self:delete:{uid}:{slot}", style="danger")],
        [InlineKeyboardButton(text="📜 VIEW ERROR LOG", callback_data=f"self:log:{uid}:{slot}", style="primary")]
    ]))

async def show_speed(message):
    uid = message.from_user.id
    t0 = time.perf_counter()
    await bot.get_me()
    latency = (time.perf_counter() - t0) * 1000
    vm = psutil.virtual_memory()
    online_count = db("SELECT COUNT(*) n FROM hosted_bots WHERE user_id=? AND status='ON'", (uid,), "one")["n"]
    text = ("⚡ <b>BOT SPEED</b>\n\n"
            f"🚀 Response: <b>{latency:.2f} ms</b>\n"
            f"🟢 Hosting service: <b>ONLINE</b>\n"
            f"⚙️ System: <b>PYTHON</b>\n"
            f"💻 CPU: <b>{psutil.cpu_percent(interval=0.1):.1f}%</b>\n"
            f"💾 RAM: <b>{vm.percent:.1f}%</b>\n"
            f"🖥️ Your online bots: <b>{online_count}</b>")
    await message.answer(text, reply_markup=main_keyboard(uid))

async def show_stats(message):
    uid = message.from_user.id
    total_users = db("SELECT COUNT(*) n FROM users", fetch="one")["n"]
    total_bots = db("SELECT COUNT(*) n FROM hosted_bots", fetch="one")["n"]
    online = db("SELECT COUNT(*) n FROM hosted_bots WHERE status='ON'", fetch="one")["n"]
    my = db("SELECT COUNT(*) n FROM hosted_bots WHERE user_id=?", (uid,), "one")["n"]
    text = ("📊 <b>HOSTING STATISTICS</b>\n\n"
            f"👥 Total users: <b>{total_users}</b>\n"
            f"🤖 Hosted bots: <b>{total_bots}</b>\n"
            f"🟢 Bots online: <b>{online}</b>\n"
            f"🔴 Bots offline: <b>{total_bots-online}</b>\n"
            f"📌 Your bots: <b>{my}</b>\n"
            f"⏱️ Service uptime: <b>{int((time.time()-STARTED_AT)//60)} minutes</b>")
    await message.answer(text, reply_markup=main_keyboard(uid))

# ========================= ADMIN CALLBACKS =========================
@dp.callback_query(F.data.startswith("viewbot:"))
async def view_bot(call: types.CallbackQuery):
    _, uid, slot = call.data.split(":")
    uid = int(uid); slot = int(slot)
    if uid != call.from_user.id and not is_admin(call.from_user.id):
        await call.answer("Not allowed", show_alert=True); return
    row = db("SELECT * FROM hosted_bots WHERE user_id=? AND slot=?", (uid, slot), "one")
    if not row:
        await call.answer("Bot not found", show_alert=True); return
    await call.answer()
    await send_bot_card(call.message, row)

@dp.callback_query(F.data.startswith("approve:"))
async def approve_bot(call: types.CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Admin only", show_alert=True); return
    parts = call.data.split(":")
    uid = int(parts[1]); slot = int(parts[2]) if len(parts) > 2 else 1
    row = db("SELECT status FROM hosted_bots WHERE user_id=? AND slot=?", (uid, slot), "one")
    if not row or row["status"] != "PENDING":
        await call.answer("This request is no longer pending.", show_alert=True); return
    await call.answer("Approved. Starting…")
    await call.message.edit_reply_markup(reply_markup=None)
    await call.message.reply("✅ <b>APPROVED</b> — installing deps & launching…")
    await start_and_report(uid, slot, notify=True)

@dp.callback_query(F.data.startswith("reject:"))
async def reject_bot(call: types.CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Admin only", show_alert=True); return
    parts = call.data.split(":")
    uid = int(parts[1]); slot = int(parts[2]) if len(parts) > 2 else 1
    row = db("SELECT status FROM hosted_bots WHERE user_id=? AND slot=?", (uid, slot), "one")
    if not row or row["status"] != "PENDING":
        await call.answer("Request is no longer pending.", show_alert=True); return
    remove_bot_storage(uid, slot)
    db("DELETE FROM hosted_bots WHERE user_id=? AND slot=?", (uid, slot))
    await call.answer("Rejected")
    await call.message.edit_reply_markup(reply_markup=None)
    try:
        await bot.send_message(uid, "❌ <b>BOT UPLOAD REJECTED</b>\nYour upload was not approved. You can upload again.", reply_markup=main_keyboard(uid))
    except Exception:
        pass

async def show_user_detail(message, uid):
    row = db("SELECT * FROM users WHERE user_id=?", (uid,), "one")
    if not row:
        await message.answer("User not found."); return
    bots = db("SELECT * FROM hosted_bots WHERE user_id=?", (uid,), "all")
    txt = (
        "👤 <b>USER DETAILS</b>\n\n"
        f"Name: {html.escape(row['full_name'])}\n"
        f"Username: @{html.escape(row['username']) if row['username'] else 'None'}\n"
        f"ID: <code>{uid}</code>\n"
        f"Joined: <code>{html.escape(row['joined_at'])}</code>\n"
        f"Last active: <code>{html.escape(row['last_seen'])}</code>\n"
        f"Language: <b>{row['language'].upper()}</b>\n"
        f"Banned: <b>{'YES' if row['banned'] else 'NO'}</b>\n"
        f"Hosted bots: <b>{len(bots)}</b>\n"
    )
    buttons = [[InlineKeyboardButton(text="🚫 Ban" if not row["banned"] else "✅ Unban",
                                     callback_data=f"user:{'ban' if not row['banned'] else 'unban'}:{uid}",
                                     style="danger" if not row["banned"] else "success")]]
    for b in bots:
        txt += f"\n• Slot {b['slot']}: {b['status']} @{html.escape(b['bot_username']) if b['bot_username'] else '—'}"
        buttons.append([InlineKeyboardButton(text=f"▶️ START Slot {b['slot']}", callback_data=f"bot:on:{uid}:{b['slot']}", style="success"),
                        InlineKeyboardButton(text="🚫 OFF + REASON", callback_data=f"adm:offreason:{uid}:{b['slot']}", style="danger")])
        if b["admin_disabled"]:
            buttons.append([InlineKeyboardButton(text="🟢 ENABLE BOT", callback_data=f"bot:enable:{uid}:{b['slot']}", style="success")])
        buttons.append([InlineKeyboardButton(text=f"🗑️ Delete Slot {b['slot']}", callback_data=f"bot:delete:{uid}:{b['slot']}", style="danger")])
    await message.answer(txt, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

@dp.callback_query(F.data.startswith("adm:"))
async def admin_actions(call: types.CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Admin only", show_alert=True); return
    parts = call.data.split(":")
    action = parts[1]
    await call.answer()

    if action == "home":
        await safe_edit_or_send(call, "🛠️ <b>ADMIN CONTROL CENTER</b>\n\nSelect a control below 👇", reply_markup=admin_kb())
        return

    if action == "overview":
        users = db("SELECT COUNT(*) n FROM users", fetch="one")["n"]
        bots = db("SELECT COUNT(*) n FROM hosted_bots", fetch="one")["n"]
        online = db("SELECT COUNT(*) n FROM hosted_bots WHERE status='ON'", fetch="one")["n"]
        pending = db("SELECT COUNT(*) n FROM hosted_bots WHERE status='PENDING'", fetch="one")["n"]
        stopped = db("SELECT COUNT(*) n FROM hosted_bots WHERE status='OFF'", fetch="one")["n"]
        disabled = db("SELECT COUNT(*) n FROM hosted_bots WHERE admin_disabled=1", fetch="one")["n"]
        failed = db("SELECT COUNT(*) n FROM hosted_bots WHERE status='ERROR'", fetch="one")["n"]
        total_builds = db("SELECT COUNT(*) n FROM build_events", fetch="one")["n"]
        failed_builds = db("SELECT COUNT(*) n FROM build_events WHERE status='FAILED'", fetch="one")["n"]
        banned = db("SELECT COUNT(*) n FROM users WHERE banned=1", fetch="one")["n"]
        active_since = (datetime.now(timezone.utc) - __import__('datetime').timedelta(days=7)).isoformat()
        active = db("SELECT COUNT(*) n FROM users WHERE last_seen>=?", (active_since,), "one")["n"]
        vm = psutil.virtual_memory()
        disk = psutil.disk_usage(str(BASE))
        await safe_edit_or_send(
            call,
            f"📊 <b>ADMIN OVERVIEW</b>\\n\\n"
            f"👥 Total users: <b>{users}</b> · Active (7d): <b>{active}</b>\\n"
            f"🚫 Banned users: <b>{banned}</b>\\n"
            f"🤖 Total bots: <b>{bots}</b>\\n"
            f"🟢 Running: <b>{online}</b> · 🔴 Stopped: <b>{stopped}</b>\\n"
            f"🚫 Disabled: <b>{disabled}</b> · 🟡 Pending: <b>{pending}</b>\\n"
            f"⚠️ Failed bots: <b>{failed}</b>\\n"
            f"🏗 Builds: <b>{total_builds}</b> · Failed builds: <b>{failed_builds}</b>\\n\\n"
            f"🖥 CPU: <b>{psutil.cpu_percent(interval=0.1):.1f}%</b>\\n"
            f"💾 RAM: <b>{vm.percent:.1f}%</b> ({vm.used//(1024**2)} MB used)\\n"
            f"📦 Storage: <b>{disk.percent:.1f}%</b> ({disk.used//(1024**3)} / {disk.total//(1024**3)} GB)",
            reply_markup=admin_kb()
        )
        return

    if action == "users":
        page = int(parts[2]) if len(parts) > 2 else 0
        rows = db("SELECT user_id,full_name,username,banned FROM users ORDER BY joined_at DESC LIMIT 10 OFFSET ?", (page*10,), "all")
        buttons = []
        for u in rows:
            title = (u["full_name"] or str(u["user_id"]))[:22]
            buttons.append([InlineKeyboardButton(text=f"{'🚫' if u['banned'] else '👤'} {title} · {u['user_id']}", callback_data=f"adm:user:{u['user_id']}", style="primary")])
        buttons.append([InlineKeyboardButton(text="🔎 Find user by ID", callback_data="adm:find", style="success")])
        buttons.append([InlineKeyboardButton(text="⬅️ Prev", callback_data=f"adm:users:{max(0,page-1)}", style="primary"),
                        InlineKeyboardButton(text="Next ➡️", callback_data=f"adm:users:{page+1}", style="primary")])
        buttons.append([InlineKeyboardButton(text="🏠 Back to Panel", callback_data="adm:home", style="danger")])
        await safe_edit_or_send(call, f"👥 <b>ALL USERS</b> · Page {page+1}", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
        return

    if action == "user":
        await show_user_detail(call.message, int(parts[2])); return
    if action == "offreason":
        target_uid, target_slot = int(parts[2]), int(parts[3])
        USER_STATES[call.from_user.id] = f"admin_bot_off_reason:{target_uid}:{target_slot}"
        await call.message.answer("✍️ Enter the reason for disabling this user's bot:")
        return
    if action == "messages":
        rows = [
            ("welcome_en", "Start message · English"),
            ("welcome_bn", "Start message · বাংলা"),
            ("maintenance_message_en", "Maintenance message · English"),
            ("maintenance_message_bn", "Maintenance message · বাংলা"),
        ]
        for key in sorted(TEXT["en"]):
            rows.append((f"message_{key}_en", f"{key} · English"))
            rows.append((f"message_{key}_bn", f"{key} · বাংলা"))
        kb = [[InlineKeyboardButton(text=f"✏️ {label}", callback_data=f"adm:editsetting:{key}", style="primary")] for key,label in rows]
        kb.append([InlineKeyboardButton(text="⬅️ Back", callback_data="adm:home", style="danger")])
        await safe_edit_or_send(call, "📝 <b>MESSAGE MANAGEMENT</b>\\nSelect a saved message to edit:", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
        return
    if action == "buttons":
        keys = ["upload","profile","mybot","speed","stats","channel","owner","language","admin"]
        kb = [[InlineKeyboardButton(text=f"🔤 {key.upper()} · EN", callback_data=f"adm:editbutton:{key}:en"),
               InlineKeyboardButton(text=f"🔤 {key.upper()} · BN", callback_data=f"adm:editbutton:{key}:bn")] for key in keys]
        kb.append([InlineKeyboardButton(text="⬅️ Back", callback_data="adm:home", style="danger")])
        await safe_edit_or_send(call, "🔤 <b>BUTTON LABEL MANAGEMENT</b>\\nChoose a button and language:", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
        return
    if action == "editsetting":
        key = parts[2]
        USER_STATES[call.from_user.id] = f"admin_edit_setting:{key}"
        current = setting(key, "")
        await call.message.answer("Current saved text:\\n\\n" + html.escape(current) + "\\n\\nSend the replacement text:")
        return
    if action == "editbutton":
        key, lang = parts[2], parts[3]
        USER_STATES[call.from_user.id] = f"admin_edit_button:{key}:{lang}"
        current = setting(f"button_{key}_{lang}", "") or TEXT[lang].get(key, key)
        await call.message.answer(f"Current {lang.upper()} label:\\n{html.escape(current)}\\n\\nSend the new button label:")
        return
    if action == "storage":
        total = 0
        for folder in (DATA_DIR, BOTS_DIR, LOG_DIR, VENV_DIR, CACHE_DIR):
            for item in folder.rglob("*"):
                if item.is_file():
                    try: total += item.stat().st_size
                    except OSError: pass
        await safe_edit_or_send(call, f"💾 <b>STORAGE USAGE</b>\\n\\nRuntime + database + logs + cache: <b>{total/1024/1024:.2f} MB</b>\\nBots: <code>{BOTS_DIR}</code>\\nVirtual environments: <code>{VENV_DIR}</code>", reply_markup=admin_kb())
        return
    if action == "health":
        vm = psutil.virtual_memory()
        disk = psutil.disk_usage(str(BASE))
        await safe_edit_or_send(call, f"🛠️ <b>SERVER HEALTH</b>\\n\\nCPU: <b>{psutil.cpu_percent(interval=0.1):.1f}%</b>\\nRAM: <b>{vm.percent:.1f}%</b>\\nDisk: <b>{disk.percent:.1f}%</b>\\nProcesses managed: <b>{len(PROCESSES)}</b>", reply_markup=admin_kb())
    if action == "find":
        USER_STATES[call.from_user.id] = "admin_user_lookup"
        await call.message.answer("Send the Telegram user ID."); return
    if action == "pending":
        rows = db("SELECT user_id,slot,status,file_size,uploaded_at FROM hosted_bots WHERE status='PENDING' ORDER BY uploaded_at DESC LIMIT 50", fetch="all")
        buttons = []
        for r in rows:
            buttons.append([InlineKeyboardButton(text=f"🟡 User {r['user_id']} · Slot {r['slot']}", callback_data=f"adm:user:{r['user_id']}", style="primary")])
            buttons.append([InlineKeyboardButton(text="✅ ACCEPT", callback_data=f"approve:{r['user_id']}:{r['slot']}", style="success"), InlineKeyboardButton(text="❌ REJECT", callback_data=f"reject:{r['user_id']}:{r['slot']}", style="danger")])
        buttons.append([InlineKeyboardButton(text="🏠 Back to Panel", callback_data="adm:home", style="primary")])
        await safe_edit_or_send(call, f"🟡 <b>PENDING BOT REQUESTS</b>\nTotal: {len(rows)}", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
        return

    if action == "bots":
        rows = db("SELECT user_id,slot,status,file_size FROM hosted_bots ORDER BY uploaded_at DESC LIMIT 30", fetch="all")
        buttons = [[InlineKeyboardButton(text=f"{r['status']} · User {r['user_id']} · Slot {r['slot']}", callback_data=f"adm:user:{r['user_id']}", style="primary")] for r in rows]
        buttons.append([InlineKeyboardButton(text="🏠 Back", callback_data="adm:home", style="danger")])
        await safe_edit_or_send(call, "🤖 <b>HOSTED BOTS</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons or [[InlineKeyboardButton(text="No bots", callback_data="adm:home")]]))
        return
    if action == "broadcast":
        USER_STATES[call.from_user.id] = "admin_broadcast"
        await call.message.answer("📨 Send the broadcast message now."); return
    if action == "set":
        kind = parts[2]
        USER_STATES[call.from_user.id] = f"admin_set_{kind}"
        await call.message.answer(f"Send the new {kind} value:"); return
    if action == "maintenance":
        global MAINTENANCE
        MAINTENANCE = not MAINTENANCE
        set_setting("maintenance", "1" if MAINTENANCE else "0")
        await safe_edit_or_send(call, f"🔒 Maintenance: <b>{'ON' if MAINTENANCE else 'OFF'}</b>", reply_markup=admin_kb())
        return
    if action == "toggleauto":
        cur = setting("auto_approve", "0") == "1"
        set_setting("auto_approve", "0" if cur else "1")
        await safe_edit_or_send(call, f"⚡ Auto-Approve: <b>{'OFF' if cur else 'ON'}</b>", reply_markup=admin_kb())
        return
    if action == "togglerestart":
        cur = setting("auto_restart", "0") == "1"
        set_setting("auto_restart", "0" if cur else "1")
        await safe_edit_or_send(call, f"♻️ Auto-Restart: <b>{'OFF' if cur else 'ON'}</b> (max 3 attempts / 10 min)", reply_markup=admin_kb())
        return
    if action == "logs":
        content = LOG_FILE.read_text(encoding="utf-8", errors="replace")[-3500:] if LOG_FILE.exists() else "No logs yet."
        await call.message.answer("📄 <b>SYSTEM LOGS</b>\n<pre>" + html.escape(content) + "</pre>")
        return

@dp.callback_query(F.data.startswith("user:"))
async def user_actions(call: types.CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Admin only", show_alert=True); return
    _, action, raw_uid = call.data.split(":")
    uid = int(raw_uid)
    if action == "ban":
        db("UPDATE users SET banned=1 WHERE user_id=?", (uid,))
        try: await bot.send_message(uid, "⛔ Your account has been restricted by the admin.")
        except Exception: pass
    elif action == "unban":
        db("UPDATE users SET banned=0 WHERE user_id=?", (uid,))
    await call.answer("User updated")
    await show_user_detail(call.message, uid)

@dp.callback_query(F.data.startswith("bot:"))
async def admin_bot_actions(call: types.CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Admin only", show_alert=True); return
    parts = call.data.split(":")
    action = parts[1]; uid = int(parts[2]); slot = int(parts[3]) if len(parts) > 3 else 1
    row = db("SELECT user_id,admin_disabled,off_reason FROM hosted_bots WHERE user_id=? AND slot=?", (uid, slot), "one")
    if not row:
        await call.answer("Bot not found", show_alert=True); return
    if row["admin_disabled"] and uid == call.from_user.id and not is_admin(call.from_user.id):
        await call.answer("Disabled by administrator", show_alert=True)
        await call.message.answer(disabled_notice_text(uid, row["off_reason"]))
        return
    if action == "on":
        disabled = db("SELECT admin_disabled,off_reason FROM hosted_bots WHERE user_id=? AND slot=?", (uid, slot), "one")
        if disabled and disabled["admin_disabled"]:
            reason = disabled["off_reason"] or "No reason provided."
            await call.answer("Disabled by admin: " + reason[:180], show_alert=True)
            await call.message.answer("🚫 <b>BOT TEMPORARILY DISABLED</b>\\n\\nReason: " + html.escape(reason))
            return
        await call.answer("Starting…")
        await start_and_report(uid, slot, notify=True)
    elif action == "off":
        USER_STATES[call.from_user.id] = f"admin_bot_off_reason:{uid}:{slot}"
        await call.answer("A reason is required")
        await call.message.answer("✍️ Enter the reason for disabling this user's bot:")
        return
    elif action == "enable":
        db("UPDATE hosted_bots SET admin_disabled=0,off_reason='',status='OFF' WHERE user_id=? AND slot=?", (uid, slot))
        log_activity(call.from_user.id, "bot_enabled", uid, f"slot={slot}")
        await call.answer("Bot enabled. User can start it again.")
        try: await bot.send_message(uid, "✅ <b>YOUR HOSTED BOT HAS BEEN RE-ENABLED</b>\\nYou can start it from MY BOT.")
        except Exception: pass
    elif action == "delete":
        remove_bot_storage(uid, slot)
        db("DELETE FROM hosted_bots WHERE user_id=? AND slot=?", (uid, slot))
        log_activity(call.from_user.id, "bot_deleted", uid, f"slot={slot}")
        await call.answer("Bot deleted")
        try: await bot.send_message(uid, "🗑️ <b>YOUR HOSTED BOT WAS DELETED BY ADMIN</b>", reply_markup=main_keyboard(uid))
        except Exception: pass
    await call.message.answer(f"✅ Bot action done for user <code>{uid}</code> slot {slot}.")

@dp.callback_query(F.data.startswith("self:"))
async def self_bot_actions(call: types.CallbackQuery):
    parts = call.data.split(":")
    action = parts[1]; uid = int(parts[2]); slot = int(parts[3]) if len(parts) > 3 else 1
    if uid != call.from_user.id and not is_admin(call.from_user.id):
        await call.answer("Not allowed", show_alert=True); return
    row = db("SELECT user_id,admin_disabled,off_reason FROM hosted_bots WHERE user_id=? AND slot=?", (uid, slot), "one")
    if not row:
        await call.answer("Bot not found", show_alert=True); return
    if row["admin_disabled"] and uid == call.from_user.id and not is_admin(call.from_user.id):
        await call.answer("Disabled by administrator", show_alert=True)
        await call.message.answer(disabled_notice_text(uid, row["off_reason"]))
        return
    if action == "on":
        disabled = db("SELECT admin_disabled,off_reason FROM hosted_bots WHERE user_id=? AND slot=?", (uid, slot), "one")
        if disabled and disabled["admin_disabled"]:
            reason = disabled["off_reason"] or "No reason provided."
            await call.answer("Disabled by admin: " + reason[:180], show_alert=True)
            await call.message.answer("🚫 <b>BOT TEMPORARILY DISABLED</b>\\n\\nReason: " + html.escape(reason))
            return
        await call.answer("Starting…")
        await start_and_report(uid, slot, notify=True)
    elif action == "off":
        stop_process(uid, slot)
        await call.answer("Bot stopped")
        await call.message.answer("🔴 <b>BOT STOPPED</b>")
    elif action == "delete":
        remove_bot_storage(uid, slot)
        db("DELETE FROM hosted_bots WHERE user_id=? AND slot=?", (uid, slot))
        await call.answer("Deleted")
        await call.message.answer("🗑️ Bot deleted. You can upload a new bot.py now.", reply_markup=main_keyboard(uid))
    elif action == "log":
        log_text = read_bot_log(uid, slot)
        await call.answer()
        await call.message.answer("📜 <b>BOT LOG</b>\n<pre>" + html.escape(log_text[-3500:]) + "</pre>")

# ========================= RENDER HEALTH SERVER & SHUTDOWN =========================
async def health(request):
    return web.Response(text="ok")

async def start_web_server():
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", int(os.getenv("PORT", "10000")))
    await site.start()
    log.info("Health server started on port %s", os.getenv("PORT", "10000"))

async def restore_approved_bots():
    rows = db("SELECT user_id,slot,status FROM hosted_bots WHERE status IN ('ON','STARTING')", fetch="all")
    for row in rows:
        uid = row["user_id"]; slot = row["slot"]
        if bot_file(uid, slot).exists():
            await start_and_report(uid, slot, notify=False)

def cleanup_on_exit(*args):
    log.info("Shutting down, stopping all child processes...")
    for key, proc in list(PROCESSES.items()):
        uid, slot = key
        kill_stale_process(uid, slot)
    db("UPDATE hosted_bots SET status='OFF',pid=NULL,started_at=NULL WHERE status IN ('ON','STARTING')")
    log.info("All child processes stopped.")

def cleanup_orphaned_storage():
    """Remove bot/venv directories only when no database record references them."""
    rows = db("SELECT user_id,slot FROM hosted_bots", fetch="all")
    valid = {f"{r['user_id']}_{r['slot']}" for r in rows}
    for root in (BOTS_DIR, VENV_DIR):
        for child in root.iterdir():
            if child.is_dir() and child.name not in valid:
                shutil.rmtree(child, ignore_errors=True)
    # Trim oversized manager and child logs without touching active runtime files.
    for log_path in LOG_DIR.glob("*.log"):
        try:
            if log_path.stat().st_size > 2 * 1024 * 1024:
                tail = log_path.read_bytes()[-1024 * 1024:]
                log_path.write_bytes(tail)
        except OSError:
            log.exception("Could not rotate log %s", log_path)

def create_database_backup():
    backup_dir = DATA_DIR / "backups"
    backup_dir.mkdir(parents=True, exist_ok=True)
    target = backup_dir / f"hosting_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.sqlite3"
    try:
        with sqlite3.connect(DB_PATH) as source, sqlite3.connect(target) as dest:
            source.backup(dest)
        backups = sorted(backup_dir.glob("hosting_*.sqlite3"), key=lambda p: p.stat().st_mtime, reverse=True)
        for old in backups[3:]:
            old.unlink(missing_ok=True)
    except Exception:
        log.exception("Database backup failed")
        target.unlink(missing_ok=True)

async def maintenance_tasks():
    while True:
        await asyncio.sleep(6 * 60 * 60)
        try:
            cleanup_orphaned_storage()
            create_database_backup()
        except Exception:
            log.exception("Scheduled maintenance task failed")

async def main():
    init_db()
    cleanup_orphaned_storage()
    create_database_backup()
    global MAINTENANCE
    MAINTENANCE = setting("maintenance", "0") == "1"
    await start_web_server()
    asyncio.create_task(monitor_processes())
    
    # ⬅️ হোস্ট বট রিস্টার্ট হলে আগের চালু থাকা বটগুলো আবার চালু হবে
    asyncio.create_task(restore_approved_bots())
    
    signal.signal(signal.SIGTERM, lambda *a: cleanup_on_exit())
    signal.signal(signal.SIGINT, lambda *a: cleanup_on_exit())
    
    log.info("Hosting manager is running")
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

if __name__ == "__main__":
    asyncio.run(main())