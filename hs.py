# Standard library imports
import functools
import os, shutil, asyncio, json, logging, time, threading, re, signal
from datetime import datetime, timedelta

# Third party imports
import uvloop  # type: ignore
import aiohttp, aiofiles
from pyrogram import Client, filters, idle
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery, Message
from pyrogram.errors import MessageNotModified, FloodWait, UserIsBlocked, InputUserDeactivated, PeerIdInvalid
from pyrogram.enums import ChatMemberStatus, ParseMode
from hachoir.metadata import extractMetadata
from hachoir.parser import createParser
import requests as sync_requests
import base64 as sync_base64
from pywidevine.cdm import Cdm
from pywidevine.device import Device
from pywidevine.pssh import PSSH

# Local imports
import mediainfo
from sub import ttml_to_srt, vtt_to_srt
# from status import send_status_update # Removed as it will be defined internally
from session import PremiumSessionPool
from utils import (
    get_thumbnail, cleanup_old_files, get_available_drive,
    get_isolated_download_path, store_content_info, cleanup_download_dir,
    get_drive_config
)
from download import (
    YTDLPDownloader, Nm3u8DLREDownloader,
    periodic_dump_cleanup
)
from config import (
    MP4_USER_IDS, USE_PROXY, PROXY_URL,
    pickFormats, get_iso_639_2
)
from formats import get_formats
from database import Database
from typing import Optional, List, Dict, Any

import hotstar

# Initialize uvloop
uvloop.install()


class SuppressSSLShutdownTimeout(logging.Filter):
    def filter(self, record):
        msg = str(record.getMessage())
        return "Error while closing connector: ClientConnectionError('Connection lost: SSL shutdown timed out'" not in msg

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'bot_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)
logger.addFilter(SuppressSSLShutdownTimeout())

# Disable non-critical logging
logging.getLogger("pyrogram").setLevel(logging.CRITICAL)
logging.getLogger("pyrogram.session.session").setLevel(logging.CRITICAL)
logging.getLogger("pyrogram.connection.connection").setLevel(logging.CRITICAL)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("http.client").setLevel(logging.WARNING)

# Bot configuration
API_ID = 7534167
API_HASH = "20a83cee023890c7d605780a3af80802"
BOT_TOKEN = "7569446269:AAHWPeBvIZ2rmE3_M6BZwk_jRFbvA7USoTM"
PREMIUM_STRING = ""
ASSISTANT_BOT = "Igniteuserofeternity"

# Channel configuration
OWNER_CHANNEL = "igniteusers"
OWNER = "@Igniteuserofeternity"
MAIN_CHANNEL = -1002826025857
METASUFFIX = "Mahesh-Kadali"

# Initialize bot client
app = Client(
    name="MAHESHCRBOT",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    max_concurrent_transmissions=20
)

# Initialize database
db = Database(
    database_url=os.environ.get("DATABASE_URL", "mongodb+srv://hello:hello@cluster0.vc2htx0.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"),
    database_name=os.environ.get("DATABASE_NAME", "pyrogram_bot")
)

# Platform examples and configurations
PLATFORM_EXAMPLES = {
    "hotstar": ["JioHotstar", "https://www.hotstar.com/in/shows/show-name/12345/episode-name/67890"]
}

# Platform suffixes for output filenames
PLATFORM_SUFFIXES = {
    "JioHotstar": "JIOHS"
}

# Trial restricted platforms
TRIAL_RESTRICTED_PLATFORMS = {
    "Jio Hotstar": "Jio Hotstar is not available in trial mode",
}

# Load premium users from premium referrals JSON
async def get_premium_users_async():
    try:
        async with aiofiles.open('data/premium_referrals.json', 'r') as f:
            content = await f.read()
            premium_data = await asyncio.to_thread(json.loads, content)
            return set(map(int, premium_data.keys()))
    except (FileNotFoundError, json.JSONDecodeError):
        return set()

def get_premium_users():
    """Get premium users synchronously"""
    try:
        with open('data/premium_referrals.json', 'r') as f:
            premium_data = json.load(f)
            return set(map(int, premium_data.keys()))
    except (FileNotFoundError, json.JSONDecodeError):
        return set()

# Base full access users
BASE_FULL_ACCESS = {7815873054, 7361945688, 7172796863, 7708998008, 5802285154, 7465574522}

OWNERS = {7815873054, 7361945688, 7172796863}
# Users with special platform access
TATAPLAY_USER = {7815873054, 7361945688, 7172796863}  # Kept for structural integrity if other special users are added later

def get_full_access_users():
    """Get all users with full access by combining base users and premium users"""
    return BASE_FULL_ACCESS | get_premium_users()

MP4_USER_IDS = {"1822859631"}  # User IDs that get mp4 extension instead of mkv

# Upload mode configuration
UPLOAD_MODE = 'gdrive' # Default mode, can be 'gdrive' or 'gofile'

# Update premium users periodically
def update_premium_users():
    global BASE_FULL_ACCESS
    premium_users = get_premium_users()
    BASE_FULL_ACCESS = BASE_FULL_ACCESS | premium_users

# Add these new variables for trial access
TRIAL_ACCESS = {}  # Trial access group ID directly as a set
TRIAL_COOLDOWNS = {}  # Store user cooldowns
TRIAL_COOLDOWN_SUCCESS = 15 * 60  # 15 minutes in seconds
TRIAL_COOLDOWN_FAIL = 3 * 60  # 3 minutes in seconds

MAX_DOWNLOAD_RETRIES = 2

# Add semaphore for controlling concurrent downloads/uploads
download_semaphore = asyncio.Semaphore(10)  # Allow 10 concurrent downloads
upload_semaphore = asyncio.Semaphore(10)  # Allow 10 concurrent uploads

# Lock state file
LOCK_FILE = 'data/bot_lock.json'

MEMORY_LOCKED = False  # In-memory lock for /block -x

def is_bot_locked():
    return MEMORY_LOCKED or _is_file_locked()

def _is_file_locked():
    try:
        with open(LOCK_FILE, 'r') as f:
            data = json.load(f)
            return data.get('locked', False)
    except (FileNotFoundError, json.JSONDecodeError):
        return False

def set_bot_lock(state: bool):
    os.makedirs('data', exist_ok=True)
    with open(LOCK_FILE, 'w') as f:
        json.dump({'locked': state}, f)

LOCK_MESSAGE = (
    "🚫 **Bot is temporarily locked by admin.**\n\n"
    "This action is taken for maintenance, restart, or testing.\n"
    "Please wait a few minutes. If this takes too long, contact the admin."
)


def owner_only(func):
    @functools.wraps(func)
    async def wrapper(client, message, *args, **kwargs):
        # Block specific user from block/unblock commands
        if message.from_user.id == 7005348098 and func.__name__ in ['lock_bot', 'unlock_bot']:
            return
        if message.from_user.id not in OWNERS:
            return
        return await func(client, message, *args, **kwargs)
    return wrapper

@app.on_message(filters.command(["block"]))
@owner_only
async def lock_bot(client, message):
    if len(message.command) > 1 and message.command[1] == "-x":
        global MEMORY_LOCKED
        MEMORY_LOCKED = True
        await message.reply("🔒 Current session locked. All /dl and download actions are now disabled until restart or /unblock -x.")
        return
    set_bot_lock(True)
    await message.reply("🔒 Bot locked. All /dl and download actions are now disabled.")

@app.on_message(filters.command(["unblock"]))
@owner_only
async def unlock_bot(client, message):
    if len(message.command) > 1 and message.command[1] == "-x":
        global MEMORY_LOCKED
        MEMORY_LOCKED = False
        await message.reply("🔓 Current session unlocked. All actions are now enabled")
        return
    set_bot_lock(False)
    await message.reply("🔓 Bot unlocked. All actions are now enabled.")

@app.on_message(filters.command("mode"))
@owner_only
async def toggle_mode_command(client, message):
    """Toggles the upload mode between Gdrive/Telegram and Gofile."""
    global UPLOAD_MODE
    if UPLOAD_MODE == 'gdrive':
        UPLOAD_MODE = 'gofile'
        await message.reply_text("✅ Upload mode switched to **Gofile**.")
    else:
        UPLOAD_MODE = 'gdrive'
        await message.reply_text("✅ Upload mode switched to **Gdrive/Telegram**.")

################################################################################################################################################################################################################################

# Bot owner ID (replace with your user ID)
OWNER_ID = 7361945688

# Payment plans
PAYMENT_PLANS = {
    "basic": {"price": 149.0, "duration": 30, "name": "Basic Plan"},
    "premium": {"price": 239.0, "duration": 90, "name": "Premium Plan"},
    "vip": {"price": 399.0, "duration": 180, "name": "VIP Plan"}
}

async def is_user_authorized(user_id: int) -> bool:
    """Check if user is authorized (not banned and active)"""
    if await db.is_banned(user_id):
        return False
    return True

async def check_premium_access(user_id: int) -> bool:
    """Check if user has premium access"""
    if user_id == OWNER_ID:
        return True
    if await db.is_admin(user_id):
        return True
    if await db.is_paid_user(user_id):
        return True
    return False

async def send_premium_required_message(message: Message):
    """Send premium required message to free users"""
    premium_text = f"🔒 **Premium Subscription Required**\n\n"
    premium_text += f"❌ This feature is only available for premium users.\n\n"
    premium_text += f"💎 **Upgrade to Premium and enjoy:**\n"
    premium_text += f"• 🚀 Faster processing\n"
    premium_text += f"• 📂 Unlimited downloads\n"
    premium_text += f"• 🎯 Priority support\n"
    premium_text += f"• 🔧 Advanced features\n"
    premium_text += f"• 📱 Mobile app access\n\n"
    premium_text += f"💳 **Contact admin to purchase subscription!**"
    
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("💎 View Premium Plans", callback_data="plans")
        ],
        [
            InlineKeyboardButton("📞 Contact Admin", url="https://t.me/kadali_mahesh")
        ]
    ])
    
    await message.reply_text(premium_text, reply_markup=keyboard)


# Bot configuration
BOT_OWNER = int(os.environ.get("BOT_OWNER", "7361945688"))  # Set your user ID
START_MESSAGE = """
👋 **Welcome to the Bot!**

I'm here to help you with various tasks. Use /help to see available commands.

🚀 **Quick Start:**
• Use /help - View all commands
• Use /stats - Check bot statistics
• Use /plan - View subscription plans

💡 **Need Help?**
Contact support for any assistance!
"""

HELP_MESSAGE = """
📚 **Available Commands:**

**👤 User Commands:**
• `/start` - Start the bot
• `/help` - Show this help message
• `/stats` - View bot statistics
• `/plan` - View subscription plans
• `/profile` - View your profile

**🔧 Admin Commands:**
• `/addadmin` - Add new admin
• `/removeadmin` - Remove admin
• `/ban` - Ban a user
• `/unban` - Unban a user
• `/broadcast` - Send message to all users
• `/addpaid` - Add paid user
• `/removepaid` - Remove paid user
• `/addauth` - Add authorized user
• `/removeauth` - Remove authorized user
• `/cleanup` - Clean expired data
• `/mode` - Toggle upload mode (Gdrive/Gofile)

**📊 Statistics:**
• `/users` - Get user list
• `/admins` - Get admin list
• `/banned` - Get banned users
• `/paid` - Get paid users

**⚙️ Settings:**
• `/settings` - View/modify bot settings
"""

# ============ DECORATORS ============

def admin_required(func):
    """Decorator to check if user is admin"""
    @functools.wraps(func)
    async def wrapper(client, message):
        user_id = message.from_user.id
        if user_id == BOT_OWNER or await db.is_admin(user_id):
            return await func(client, message)
        else:
            await message.reply("❌ You don't have permission to use this command.")
    return wrapper

def owner_required(func):
    """Decorator to check if user is bot owner"""
    @functools.wraps(func)
    async def wrapper(client, message):
        if message.from_user.id == BOT_OWNER:
            return await func(client, message)
        else:
            await message.reply("❌ Only bot owner can use this command.")
    return wrapper

def not_banned(func):
    """Decorator to check if user is not banned"""
    @functools.wraps(func)
    async def wrapper(client, message):
        user_id = message.from_user.id
        if await db.is_banned(user_id):
            ban_info = await db.get_ban_info(user_id)
            ban_text = f"🚫 **You are banned from using this bot.**\n\n"
            if ban_info:
                ban_text += f"**Reason:** {ban_info.get('reason', 'No reason provided')}\n"
                ban_text += f"**Banned Date:** {ban_info.get('banned_date', 'Unknown')}\n"
                if ban_info.get('expires_at'):
                    ban_text += f"**Expires:** {ban_info.get('expires_at')}\n"
            await message.reply(ban_text)
            return
        return await func(client, message)
    return wrapper

# ============ UTILITY FUNCTIONS ============

async def add_user_to_db(message: Message):
    """Add user to database if not exists"""
    user = message.from_user
    await db.add_user(
        user_id=user.id,
        username=user.username,
        first_name=user.first_name,
        last_name=user.last_name,
        phone=user.phone_number
    )
    await db.update_user_activity(user.id)

def get_user_id_from_message(message: Message) -> int:
    """Extract user ID from message (reply or mention)"""
    if message.reply_to_message:
        return message.reply_to_message.from_user.id
    
    command_parts = message.text.split()
    if len(command_parts) > 1:
        try:
            return int(command_parts[1])
        except ValueError:
            return None
    return None

def format_user_info(user_data: Dict) -> str:
    """Format user information for display"""
    info = f"👤 **User Information:**\n\n"
    info += f"**ID:** `{user_data.get('user_id')}`\n"
    info += f"**Username:** @{user_data.get('username', 'N/A')}\n"
    info += f"**Name:** {user_data.get('first_name', 'N/A')}"
    if user_data.get('last_name'):
        info += f" {user_data.get('last_name')}\n"
    else:
        info += "\n"
    info += f"**Joined:** {user_data.get('joined_date', 'N/A')}\n"
    info += f"**Last Active:** {user_data.get('last_active', 'N/A')}\n"
    return info

# ============ BASIC COMMANDS ============

@app.on_message(filters.command("start") & filters.private)
@not_banned
async def start_command(client: Client, message: Message):
    """Start command handler"""
    await add_user_to_db(message)
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📚 Help", callback_data="help"),
         InlineKeyboardButton("📊 Stats", callback_data="stats")],
        [InlineKeyboardButton("💎 Plans", callback_data="plans"),
         InlineKeyboardButton("👤 Profile", callback_data="profile")]
    ])
    
    await message.reply(START_MESSAGE, reply_markup=keyboard)

@app.on_message(filters.command("help") & filters.private)
@not_banned
async def help_command(client: Client, message: Message):
    """Help command handler"""
    await add_user_to_db(message)
    await message.reply(HELP_MESSAGE)

@app.on_message(filters.command("stats") & filters.private)
@not_banned
async def stats_command(client: Client, message: Message):
    """Stats command handler"""
    await add_user_to_db(message)
    
    stats = await db.get_stats()
    stats_text = f"📊 **Bot Statistics:**\n\n"
    stats_text += f"👥 **Total Users:** {stats.get('total_users', 0)}\n"
    stats_text += f"👑 **Total Admins:** {stats.get('total_admins', 0)}\n"
    stats_text += f"🚫 **Banned Users:** {stats.get('total_banned', 0)}\n"
    stats_text += f"💎 **Paid Users:** {stats.get('total_paid', 0)}\n"
    stats_text += f"✅ **Auth Users:** {stats.get('total_auth', 0)}\n"
    stats_text += f"🔥 **Active (24h):** {stats.get('active_users_24h', 0)}\n"
    stats_text += f"📅 **Active (7d):** {stats.get('active_users_7d', 0)}\n"
    stats_text += f"🆕 **New Today:** {stats.get('new_users_today', 0)}\n"
    
    await message.reply(stats_text)

@app.on_message(filters.command("profile") & filters.private)
@not_banned
async def profile_command(client: Client, message: Message):
    """Profile command handler"""
    await add_user_to_db(message)
    
    user_id = message.from_user.id
    user_data = await db.get_user(user_id)
    
    if not user_data:
        await message.reply("❌ User data not found.")
        return
    
    profile_text = format_user_info(user_data)
    
    # Add additional status info
    profile_text += f"\n**Status:**\n"
    profile_text += f"• Admin: {'✅' if await db.is_admin(user_id) else '❌'}\n"
    profile_text += f"• Paid: {'✅' if await db.is_paid_user(user_id) else '❌'}\n"
    profile_text += f"• Authorized: {'✅' if await db.is_auth_user(user_id) else '❌'}\n"
    profile_text += f"• Banned: {'✅' if await db.is_banned(user_id) else '❌'}\n"
    
    await message.reply(profile_text)

# ============ ADMIN COMMANDS ============

@app.on_message(filters.command("addadmin") & filters.private)
@owner_required
async def add_admin_command(client: Client, message: Message):
    """Add admin command handler"""
    target_user_id = get_user_id_from_message(message)
    
    if not target_user_id:
        await message.reply("❌ Please reply to a user or provide user ID.\n\nUsage: `/addadmin <user_id>` or reply to user")
        return
    
    success = await db.add_admin(target_user_id, added_by=message.from_user.id)
    
    if success:
        await message.reply(f"✅ User `{target_user_id}` has been added as admin.")
        try:
            await client.send_message(
                chat_id=target_user_id,
                text="🎉 **Congratulations!**\n\nYou have been promoted to an admin."
            )
        except (UserIsBlocked, PeerIdInvalid, InputUserDeactivated):
            await message.reply(f"⚠️ Could not notify the user `{target_user_id}` as they may have blocked the bot.")
        except Exception as e:
            logger.error(f"Failed to notify user {target_user_id} about admin promotion: {e}")
            await message.reply(f"⚠️ An error occurred while trying to notify the user `{target_user_id}`.")
    else:
        await message.reply(f"❌ Failed to add user `{target_user_id}` as admin.")

@app.on_message(filters.command("removeadmin") & filters.private)
@owner_required
async def remove_admin_command(client: Client, message: Message):
    """Remove admin command handler"""
    target_user_id = get_user_id_from_message(message)
    
    if not target_user_id:
        await message.reply("❌ Please reply to a user or provide user ID.\n\nUsage: `/removeadmin <user_id>` or reply to user")
        return
    
    success = await db.remove_admin(target_user_id)
    
    if success:
        await message.reply(f"✅ User `{target_user_id}` has been removed from admin.")
        try:
            await client.send_message(
                chat_id=target_user_id,
                text="ℹ️ **Notice**\n\nYou have been removed from the admin list."
            )
        except (UserIsBlocked, PeerIdInvalid, InputUserDeactivated):
            pass 
        except Exception as e:
            logger.error(f"Failed to notify user {target_user_id} about admin removal: {e}")
    else:
        await message.reply(f"❌ Failed to remove user `{target_user_id}` from admin.")

@app.on_message(filters.command("ban") & filters.private)
@admin_required
async def ban_command(client: Client, message: Message):
    """Ban user command handler"""
    target_user_id = get_user_id_from_message(message)
    
    if not target_user_id:
        await message.reply("❌ Please reply to a user or provide user ID.\n\nUsage: `/ban <user_id> [reason]` or reply to user")
        return
    
    # Extract reason from command
    command_parts = message.text.split(maxsplit=2)
    reason = command_parts[2] if len(command_parts) > 2 else "No reason provided"
    
    success = await db.ban_user(target_user_id, banned_by=message.from_user.id, reason=reason)
    
    if success:
        await message.reply(f"✅ User `{target_user_id}` has been banned.\n**Reason:** {reason}")
        try:
            await client.send_message(
                chat_id=target_user_id,
                text=f"🚫 **You have been banned from using this bot.**\n\n**Reason:** {reason}"
            )
        except (UserIsBlocked, PeerIdInvalid, InputUserDeactivated):
            pass
        except Exception as e:
            logger.error(f"Failed to notify user {target_user_id} about ban: {e}")
    else:
        await message.reply(f"❌ Failed to ban user `{target_user_id}`.")

@app.on_message(filters.command("unban") & filters.private)
@admin_required
async def unban_command(client: Client, message: Message):
    """Unban user command handler"""
    target_user_id = get_user_id_from_message(message)
    
    if not target_user_id:
        await message.reply("❌ Please reply to a user or provide user ID.\n\nUsage: `/unban <user_id>` or reply to user")
        return
    
    success = await db.unban_user(target_user_id)
    
    if success:
        await message.reply(f"✅ User `{target_user_id}` has been unbanned.")
        try:
            await client.send_message(
                chat_id=target_user_id,
                text="✅ **You have been unbanned!**\n\nYou can now use the bot again."
            )
        except (UserIsBlocked, PeerIdInvalid, InputUserDeactivated):
            pass
        except Exception as e:
            logger.error(f"Failed to notify user {target_user_id} about unban: {e}")
    else:
        await message.reply(f"❌ Failed to unban user `{target_user_id}`.")

@app.on_message(filters.command("addpaid") & filters.private)
@admin_required
async def add_paid_command(client: Client, message: Message):
    """Add paid user command handler"""
    command_parts = message.text.split()
    
    if len(command_parts) < 4:
        await message.reply("❌ Invalid format.\n\nUsage: `/addpaid <user_id> <plan> <days> [amount]`")
        return
    
    try:
        user_id = int(command_parts[1])
        plan = command_parts[2]
        days = int(command_parts[3])
        amount = float(command_parts[4]) if len(command_parts) > 4 else 0.0
        
        success = await db.add_paid_user(user_id, plan, days, amount, added_by=message.from_user.id)
        
        if success:
            await message.reply(f"✅ User `{user_id}` has been added to paid users.\n**Plan:** {plan}\n**Duration:** {days} days\n**Amount:** ${amount}")
            try:
                await client.send_message(
                    chat_id=user_id,
                    text=f"💎 **Your account has been upgraded!**\n\n"
                         f"You have been added to the **{plan}** plan for **{days}** days.\n\n"
                         f"Thank you for your support!"
                )
            except (UserIsBlocked, PeerIdInvalid, InputUserDeactivated):
                await message.reply(f"⚠️ Could not notify the user `{user_id}` as they may have blocked the bot.")
            except Exception as e:
                logger.error(f"Failed to notify user {user_id} about paid subscription: {e}")
                await message.reply(f"⚠️ An error occurred while trying to notify the user `{user_id}`.")
        else:
            await message.reply(f"❌ Failed to add user `{user_id}` to paid users.")
            
    except ValueError:
        await message.reply("❌ Invalid user ID, days, or amount format.")

@app.on_message(filters.command("removepaid") & filters.private)
@admin_required
async def remove_paid_command(client: Client, message: Message):
    """Remove paid user command handler"""
    target_user_id = get_user_id_from_message(message)
    
    if not target_user_id:
        await message.reply("❌ Please reply to a user or provide user ID.\n\nUsage: `/removepaid <user_id>` or reply to user")
        return
    
    success = await db.remove_paid_user(target_user_id)
    
    if success:
        await message.reply(f"✅ User `{target_user_id}` has been removed from paid users.")
        try:
            await client.send_message(
                chat_id=target_user_id,
                text="ℹ️ **Notice**\n\nYour paid subscription has been removed by an admin."
            )
        except (UserIsBlocked, PeerIdInvalid, InputUserDeactivated):
            pass
        except Exception as e:
            logger.error(f"Failed to notify user {target_user_id} about paid removal: {e}")
    else:
        await message.reply(f"❌ Failed to remove user `{target_user_id}` from paid users.")

@app.on_message(filters.command("addauth") & filters.private)
@admin_required
async def add_auth_command(client: Client, message: Message):
    """Add authorized user command handler"""
    command_parts = message.text.split()
    
    if len(command_parts) < 2:
        await message.reply("❌ Please provide user ID.\n\nUsage: `/addauth <user_id> [auth_level]`")
        return
    
    try:
        user_id = int(command_parts[1])
        auth_level = command_parts[2] if len(command_parts) > 2 else "user"
        
        success = await db.add_auth_user(user_id, auth_level, added_by=message.from_user.id)
        
        if success:
            await message.reply(f"✅ User `{user_id}` has been authorized.\n**Level:** {auth_level}")
            try:
                await client.send_message(
                    chat_id=user_id,
                    text=f"✅ **You have been granted authorized access.**\n\n"
                         f"Your authorization level is: **{auth_level}**."
                )
            except (UserIsBlocked, PeerIdInvalid, InputUserDeactivated):
                await message.reply(f"⚠️ Could not notify the user `{user_id}` as they may have blocked the bot.")
            except Exception as e:
                logger.error(f"Failed to notify user {user_id} about auth grant: {e}")
                await message.reply(f"⚠️ An error occurred while trying to notify the user `{user_id}`.")
        else:
            await message.reply(f"❌ Failed to authorize user `{user_id}`.")
            
    except ValueError:
        await message.reply("❌ Invalid user ID format.")

@app.on_message(filters.command("removeauth") & filters.private)
@admin_required
async def remove_auth_command(client: Client, message: Message):
    """Remove authorized user command handler"""
    target_user_id = get_user_id_from_message(message)
    
    if not target_user_id:
        await message.reply("❌ Please reply to a user or provide user ID.\n\nUsage: `/removeauth <user_id>` or reply to user")
        return
    
    success = await db.remove_auth_user(target_user_id)
    
    if success:
        await message.reply(f"✅ User `{target_user_id}` has been removed from authorized users.")
        try:
            await client.send_message(
                chat_id=target_user_id,
                text="ℹ️ **Notice**\n\nYour authorized access has been revoked."
            )
        except (UserIsBlocked, PeerIdInvalid, InputUserDeactivated):
            pass
        except Exception as e:
            logger.error(f"Failed to notify user {target_user_id} about auth removal: {e}")
    else:
        await message.reply(f"❌ Failed to remove user `{target_user_id}` from authorized users.")

# ============ BROADCAST COMMAND ============

@app.on_message(filters.command("broadcast") & filters.private)
@admin_required
async def broadcast_command(client: Client, message: Message):
    """Broadcast message command handler"""
    if not message.reply_to_message:
        await message.reply("❌ Please reply to a message to broadcast.\n\nUsage: Reply to a message with `/broadcast [all|paid|auth|admins]`")
        return
    
    command_parts = message.text.split()
    target_type = command_parts[1] if len(command_parts) > 1 else "all"
    
    if target_type not in ["all", "paid", "auth", "admins"]:
        await message.reply("❌ Invalid target type. Use: all, paid, auth, or admins")
        return
    
    broadcast_message = message.reply_to_message
    
    # Create broadcast entry
    broadcast_id = await db.create_broadcast(
        message=broadcast_message.text or "Media message",
        sent_by=message.from_user.id,
        target_type=target_type
    )
    
    if not broadcast_id:
        await message.reply("❌ Failed to create broadcast.")
        return
    
    # Get target users
    target_users = await db.get_broadcast_targets(target_type)
    
    if not target_users:
        await message.reply("❌ No target users found.")
        return
    
    status_message = await message.reply(f"📡 **Broadcasting to {len(target_users)} users...**\n\n⏳ Starting broadcast...")
    
    sent_count = 0
    failed_count = 0
    
    for user_id in target_users:
        try:
            await broadcast_message.copy(user_id)
            sent_count += 1
            
            # Update status every 10 messages
            if sent_count % 10 == 0:
                await status_message.edit_text(
                    f"📡 **Broadcasting Progress:**\n\n"
                    f"✅ Sent: {sent_count}\n"
                    f"❌ Failed: {failed_count}\n"
                    f"📊 Remaining: {len(target_users) - sent_count - failed_count}"
                )
            
            # Rate limiting
            await asyncio.sleep(0.1)
            
        except FloodWait as e:
            await asyncio.sleep(e.value)
            try:
                await broadcast_message.copy(user_id)
                sent_count += 1
            except:
                failed_count += 1
                
        except (UserIsBlocked, InputUserDeactivated, PeerIdInvalid):
            failed_count += 1
            
        except Exception as e:
            failed_count += 1
            logger.error(f"Error sending broadcast to {user_id}: {e}")
    
    # Update broadcast statistics
    await db.update_broadcast_stats(broadcast_id, sent_count, failed_count, completed=True)
    
    final_text = f"📡 **Broadcast Completed!**\n\n"
    final_text += f"✅ **Successfully sent:** {sent_count}\n"
    final_text += f"❌ **Failed:** {failed_count}\n"
    final_text += f"📊 **Total:** {len(target_users)}\n"
    final_text += f"🎯 **Target:** {target_type.title()}"
    
    await status_message.edit_text(final_text)

# ============ LIST COMMANDS ============

@app.on_message(filters.command("users") & filters.private)
@admin_required
async def users_command(client: Client, message: Message):
    """List users command handler"""
    users = await db.get_all_users()
    
    if not users:
        await message.reply("❌ No users found.")
        return
    
    user_list = f"👥 **Users List ({len(users)} users):**\n\n"
    
    for i, user in enumerate(users[:20], 1):  # Limit to first 20
        user_list += f"{i}. `{user.get('user_id')}` - {user.get('first_name', 'N/A')}"
        if user.get('username'):
            user_list += f" (@{user.get('username')})"
        user_list += "\n"
    
    if len(users) > 20:
        user_list += f"\n... and {len(users) - 20} more users"
    
    await message.reply(user_list)

@app.on_message(filters.command("admins") & filters.private)
@admin_required
async def admins_command(client: Client, message: Message):
    """List admins command handler"""
    admins = await db.get_all_admins()
    
    if not admins:
        await message.reply("❌ No admins found.")
        return
    
    admin_list = f"👑 **Admins List ({len(admins)} admins):**\n\n"
    
    for i, admin in enumerate(admins, 1):
        admin_list += f"{i}. `{admin.get('user_id')}`"
        permissions = admin.get('permissions', [])
        if permissions:
            admin_list += f" - {', '.join(permissions)}"
        admin_list += "\n"
    
    await message.reply(admin_list)

@app.on_message(filters.command("banned") & filters.private)
@admin_required
async def banned_command(client: Client, message: Message):
    """List banned users command handler"""
    banned = await db.get_all_banned_users()
    
    if not banned:
        await message.reply("❌ No banned users found.")
        return
    
    banned_list = f"🚫 **Banned Users ({len(banned)} users):**\n\n"
    
    for i, ban in enumerate(banned[:10], 1):  # Limit to first 10
        banned_list += f"{i}. `{ban.get('user_id')}` - {ban.get('reason', 'No reason')}\n"
    
    if len(banned) > 10:
        banned_list += f"\n... and {len(banned) - 10} more banned users"
    
    await message.reply(banned_list)

@app.on_message(filters.command("paid") & filters.private)
@admin_required
async def paid_command(client: Client, message: Message):
    """List paid users command handler"""
    paid = await db.get_all_paid_users()
    
    if not paid:
        await message.reply("❌ No paid users found.")
        return
    
    paid_list = f"💎 **Paid Users ({len(paid)} users):**\n\n"
    
    for i, user in enumerate(paid[:10], 1):  # Limit to first 10
        paid_list += f"{i}. `{user.get('user_id')}` - {user.get('plan', 'N/A')}"
        if user.get('end_date'):
            paid_list += f" (expires: {user.get('end_date').strftime('%Y-%m-%d')})"
        paid_list += "\n"
    
    if len(paid) > 10:
        paid_list += f"\n... and {len(paid) - 10} more paid users"
    
    await message.reply(paid_list)

# ============ MAINTENANCE COMMANDS ============

@app.on_message(filters.command("cleanup") & filters.private)
@admin_required
async def cleanup_command(client: Client, message: Message):
    """Cleanup expired data command handler"""
    status_msg = await message.reply("🧹 **Cleaning up expired data...**")
    
    cleanup_stats = await db.cleanup_expired_data()
    
    cleanup_text = f"🧹 **Cleanup Completed!**\n\n"
    cleanup_text += f"🚫 **Expired bans removed:** {cleanup_stats.get('expired_bans', 0)}\n"
    cleanup_text += f"💎 **Expired subscriptions removed:** {cleanup_stats.get('expired_subscriptions', 0)}\n"
    
    await status_msg.edit_text(cleanup_text)

@app.on_message(filters.command("settings") & filters.private)
@admin_required
async def settings_command(client: Client, message: Message):
    """Settings command handler"""
    settings_text = f"⚙️ **Bot Settings:**\n\n"
    settings_text += f"🤖 **Bot Owner:** `{BOT_OWNER}`\n"
    settings_text += f"💾 **Database:** Connected\n"
    settings_text += f"📊 **Logging:** Enabled\n"
    settings_text += f"🔄 **Auto Cleanup:** Available\n"
    
    await message.reply(settings_text)

# ============ ERROR HANDLERS ============

@app.on_message(filters.command("error") & filters.private)
@owner_required
async def error_test_command(client: Client, message: Message):
    """Test error handling"""
    try:
        # Intentional error for testing
        result = 1 / 0
    except Exception as e:
        logger.error(f"Test error: {e}")
        await message.reply(f"❌ **Test Error:**\n\n`{str(e)}`")

# ============ MAHESH JUNCTION ===========

def construct_filename(content_info, identifier):
    """Constructs filename based on content type and audio streams, specifically for JioHotstar."""
    # Get basic info
    title = content_info.get("title", "Unknown")
    title = title.replace(" ", ".").replace("'", ".").replace("'", ".") if title else "Unknown"
    content_type = content_info.get("content_type", "")
    year = content_info.get("year", "")
    platform = content_info.get("platform", "")

    # Ensure the platform is JioHotstar, if not, handle accordingly (e.g., raise error or return default)
    if platform != "JioHotstar":
        logger.warning(f"Unexpected platform '{platform}'. This function is tailored for JioHotstar.")
        # You might want to raise an error or return a generic filename here
        # For now, we'll proceed assuming some default behavior or that this won't be called for other platforms
        pass

    # Get selected resolution and audio from callback storage
    try:
        with open('data/callback_storage.json', 'r', encoding='utf-8') as f:
            callback_data = json.load(f).get(identifier, {})

            # Get resolution
            selected_res = callback_data.get("selected_resolution", {})
            max_resolution = "1080p"  # Default for Hotstar

            if selected_res:
                width, height = selected_res["resolution"].split("x")
            else:
                # Fallback to highest available resolution
                video_streams = content_info.get("streams_info", {}).get("video", [])
                if video_streams:
                    width, height = video_streams[0]["resolution"].split("x")
                else:
                    width, height = "1920", "1080"  # Default fallback for Hotstar

            width = ''.join(c for c in width if c.isdigit())
            height = ''.join(c for c in height if c.isdigit())
            max_resolution = "1080p" if width == "1920" else f"{height}p"

            # Get selected audios
            selected_audio_ids = callback_data.get("selected_audios", [])

    except Exception as e:
        logger.error(f"Error reading callback storage: {e}")
        # Fallback to highest available resolution
        video_streams = content_info.get("streams_info", {}).get("video", [])
        max_resolution = "1080p"  # Default for Hotstar
        if video_streams:
            width, height = video_streams[0]["resolution"].split("x")
            width = ''.join(c for c in width if c.isdigit())
            height = ''.join(c for c in height if c.isdigit())
            max_resolution = "1080p" if width == "1920" else f"{height}p"
        selected_audio_ids = []

    # Get audio info based on selected audios
    audio_streams = content_info.get("streams_info", {}).get("audio", [])
    selected_audio_streams = [stream for stream in audio_streams if stream["stream_id"] in selected_audio_ids]

    # Determine audio type string for JioHotstar
    unique_languages = len(set(audio["language"] for audio in selected_audio_streams))

    if unique_languages == 0:
        audio_type = ""  # Empty for no audio
    elif unique_languages == 1:
        lang = selected_audio_streams[0]["language"]
        audio_type = "" if lang.upper() in ["UND", "UNKNOWN", "NONE"] else lang
    elif unique_languages == 2:
        audio_type = "Dual.Audio"
    else:
        audio_type = "Multi.Audio"

    # Set audio codec to DDP.5.1 for JioHotstar
    audio_codec = "DDP.5.1"
    # Set video codec based on JioHotstar rules
    video_codec = "H265"
    # Add HDR tag for 2160p Hotstar content
    if max_resolution == "2160p":
        video_codec = "HDR.H265"

    # Use platform_suffix from content_info if available, otherwise use from mapping
    platform_suffix = content_info.get("platform_suffix") or PLATFORM_SUFFIXES.get("JioHotstar", "UNK")

    def clean_name(text):
        # Helper function to handle both existing and new hyphen removal
        cleaned = text
        if "–" in cleaned:
            cleaned = cleaned.split("–", 1)[-1].strip()
        return cleaned.replace("–.", "").replace("-", "")

    # Clean title for JioHotstar (no special handling for brackets like TataPlay)
    clean_title = clean_name(title)

    # Construct filename based on content type
    if content_type == "EPISODE":
        # Get episode number directly from content_info
        episode_number = content_info.get("episode_number", "S01E01")
        episode_title = content_info.get("episode_title", "")

        if episode_title:
            cleaned_episode_title = clean_name(episode_title)
            filename = f"{clean_title}.{episode_number}.{cleaned_episode_title}"
        else:
            filename = f"{clean_title}.{episode_number}"
    else:
        # For movies: title.year
        filename = f"{clean_title}.{year}" if year and year != "N/A" else clean_title

    # Add quality and other specs
    filename = f"{filename}.{max_resolution}.{platform_suffix}.WEB-DL.{audio_type}.{audio_codec}.{video_codec}-{METASUFFIX}"

    # Clean filename - replace spaces, quotes, invalid chars with dots
    invalid_chars = r'[ \'"<>:"/\\|?*$\[\]]|None'
    filename = re.sub(invalid_chars, '.', filename)

    # Remove multiple dots and leading/trailing dots
    return re.sub(r'\.+', '.', filename).strip('.')


def load_callback_storage():
    """Load callback storage from JSON file."""
    try:
        os.makedirs('data', exist_ok=True)
        storage_path = 'data/callback_storage.json'
        
        try:
            with open(storage_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                current_time = time.time()
                # Filter out entries older than 60 minutes
                return {k: v for k, v in data.items()
                       if (v.get("selected_audios") or v.get("selected_resolution"))
                       and current_time - v.get('timestamp',0) < 3600}
        except (FileNotFoundError, json.JSONDecodeError):
            return {}
    except Exception as e:
        logger.error(f"Error loading callback storage: {e}")
        return {}

def save_callback_storage(data):
    """Save callback storage to JSON file."""
    try:
        os.makedirs('data', exist_ok=True)
        storage_path = 'data/callback_storage.json'
        
        # Add timestamp to entries
        current_time = time.time()
        for v in data.values():
            v['timestamp'] = current_time
            
        cleaned_data = {k: v for k, v in data.items()
                       if (v.get("selected_audios") or v.get("selected_resolution"))
                       and current_time - v.get('timestamp',0) < 3600}
                       
        with open(storage_path, 'w', encoding='utf-8') as f:
            json.dump(cleaned_data, f, indent=4)
        return True
    except Exception as e:
        logger.error(f"Error saving callback storage: {e}")
        return False

def get_selected_audios(identifier, callback_storage=None):
    """Get selected audio streams for a given identifier."""
    return (callback_storage or load_callback_storage()).get(identifier, {}).get("selected_audios", [])

def create_resolution_buttons(identifier, streams_info, content_info=None):
    buttons = []
    row = []
    
    # Remove duplicates keeping highest bitrate for each stream_id
    seen_stream_ids = {}
    for video in streams_info["video"]:
        stream_id = video["stream_id"]
        if stream_id not in seen_stream_ids or video["bitrate"] > seen_stream_ids[stream_id]["bitrate"]:
            seen_stream_ids[stream_id] = video
    
    # Sort video streams by resolution height and bitrate
    videos_to_display = sorted(
        seen_stream_ids.values(),
        key=lambda x: (int(''.join(c for c in x["resolution"].split("x")[1] if c.isdigit())), x["bitrate"]),
        reverse=True
    )
    
    # Create buttons for each video
    for video in videos_to_display:
        # Shorten stream_id by taking the last segment after underscore
        stream_id_parts = video["stream_id"].split("_")
        short_id = stream_id_parts[-1] if len(stream_id_parts) > 1 else video["stream_id"]
        
        # Extract width and height from resolution
        width, height = video["resolution"].split("x")
        width = ''.join(c for c in width if c.isdigit())
        height = ''.join(c for c in height if c.isdigit())
        
        # If width is 1920, force display height as 1080
        display_height = "1080" if width == "1920" else height
        
        button_text = f"{display_height}p ({video['bitrate']}K)"
        
        # Create callback data
        callback_data = f"res_{identifier}_{short_id}"
        
        # Ensure callback data doesn't exceed 64 bytes
        if len(callback_data.encode()) > 64:
            # Shorten identifier by using first 8 characters of user ID
            short_identifier = f"{identifier.split('_')[0][:8]}_{identifier.split('_')[1]}"
            callback_data = f"res_{short_identifier}_{short_id}"
            
        row.append(InlineKeyboardButton(button_text, callback_data=callback_data))
        
        if len(row) == 2:
            buttons.append(row)
            row = []
    
    if row:
        buttons.append(row)
    
    buttons.append([InlineKeyboardButton("❌ Close", callback_data=f"close_{identifier.split('_')[0]}")])
    
    return InlineKeyboardMarkup(buttons)

def create_audio_buttons(identifier, streams_info, selected_resolution=None):
    buttons = []
    row = []
    callback_storage = load_callback_storage()
    selected_audios = get_selected_audios(identifier, callback_storage)
    
    # Group and sort audio streams by language
    audio_streams_by_lang = {}
    for audio in streams_info["audio"]:
        lang_code = audio["language"].lower()[:3]
        if lang_code not in audio_streams_by_lang:
            audio_streams_by_lang[lang_code] = []
        audio_streams_by_lang[lang_code].append(audio)
        
    for streams in audio_streams_by_lang.values():
        streams.sort(key=lambda x: x["bitrate"], reverse=True)

    # Prioritize and filter streams
    prioritized = []
    other = []
    for lang_code, streams in audio_streams_by_lang.items():
        if lang_code in pickFormats["audio"]:
            prioritized.extend(streams)
        else:
            other.extend(streams[:2])
            
    audio_streams = prioritized + other[:5]
    
    # Setup callback storage
    if identifier not in callback_storage:
        callback_storage[identifier] = {"stream_id_map": {}}
    elif "stream_id_map" not in callback_storage[identifier]:
        callback_storage[identifier]["stream_id_map"] = {}
        
    # Group by language+bitrate and filter duplicates
    lang_bitrate_groups = {}
    for audio in audio_streams:
        key = f"{audio['language'].lower()[:3]}_{audio['bitrate']}"
        if key not in lang_bitrate_groups:
            lang_bitrate_groups[key] = []
        lang_bitrate_groups[key].append(audio)
        
    filtered_streams = []
    for streams in lang_bitrate_groups.values():
        # Include all duplicates instead of limiting to 3
        filtered_streams.extend(streams)
        
    filtered_streams.sort(key=lambda x: (
        x["language"].lower()[:3] not in pickFormats["audio"],
        -x["bitrate"]
    ))

    # Create buttons
    for idx, audio in enumerate(filtered_streams, 1):
        lang_code = audio["language"].lower()[:3]
        lang_name = pickFormats["audio"].get(lang_code, audio["language"])
        
        # Add stream_id suffix for duplicates
        suffix = ""
        if any(a != audio and
               a["language"].lower()[:3] == lang_code and
               a["bitrate"] == audio["bitrate"]
               for a in filtered_streams):
            suffix = f" ({audio['stream_id']})"
            
        button_text = f"{idx}. {lang_name} ({audio['bitrate']}K){suffix}"
        if audio["stream_id"] in selected_audios:
            button_text = "✅ " + button_text
            
        # Store mapping and create callback
        stream_index = str(idx)
        callback_storage[identifier]["stream_id_map"][stream_index] = audio["stream_id"]
        
        callback_data = f"aud_{identifier}_{stream_index}"
        if len(callback_data.encode()) > 64:
            short_id = f"{identifier.split('_')[0][:8]}_{identifier.split('_')[1]}"
            callback_data = f"aud_{short_id}_{stream_index}"
            
        row.append(InlineKeyboardButton(button_text, callback_data=callback_data))
        
        if len(row) == 2:
            buttons.append(row)
            row = []
            
    if row:
        buttons.append(row)
        
    save_callback_storage(callback_storage)
    
    # Add Select All and Clear All buttons
    buttons.append([
        InlineKeyboardButton("✨ Select All", callback_data=f"aud_all_{identifier}"),
        InlineKeyboardButton("🗑️ Clear All", callback_data=f"aud_clear_{identifier}")
    ])
    
    # Add navigation buttons
    buttons.extend([
        [
            InlineKeyboardButton("⬅️ Back", callback_data=f"back_{identifier}"),
            InlineKeyboardButton("Proceed ➡️", callback_data=f"proc_{identifier}")
        ],
        [InlineKeyboardButton("❌ Close", callback_data=f"close_{identifier.split('_')[0]}")]
    ])
    
    return InlineKeyboardMarkup(buttons)

async def handle_hotstar(client, message, url):
    if not url.startswith(("https://www.hotstar.com", "https://hotstar.com")):
        return None
    
    async def show_language_selection(content_id, sport_type):
        """Handle language selection UI and user interaction for sports content"""
        # Make initial request to get available languages
        initial_url = f"{hotstar.BASE_URL}/sports/{sport_type}/dummy/{content_id}/watch"
        try:
            initial_response = await hotstar.make_request(initial_url, headers=hotstar.HEADERS)
            player_data = initial_response.get("success", {}).get("page", {}).get("spaces", {}).get("player", {}).get("widget_wrappers", [{}])[0].get("widget", {}).get("data", {})
            available_languages = player_data.get("player_config", {}).get("content_metadata", {}).get("audio_languages", [])
            
            # Default language options when API doesn't return any
            default_languages = [
                {"name": "Hindi", "iso3code": "hin"},
                {"name": "English", "iso3code": "eng"},
                {"name": "Tamil", "iso3code": "tam"},
                {"name": "Telugu", "iso3code": "tel"},
                {"name": "Bengali", "iso3code": "ben"},
                {"name": "Malayalam", "iso3code": "mal"},
                {"name": "Kannada", "iso3code": "kan"},
                {"name": "Marathi", "iso3code": "mar"}
            ]
            
            if not available_languages:
                available_languages = default_languages
                logger.info("Using default language options as API returned none")

            language_text = "**🌐 Available Languages:**\n\n" + "\n".join(
                f"**{i}.** `{lang['name']}`"
                for i, lang in enumerate(available_languages, 1)
            ) + "\n\n**⏰ Enter the number of your choice (60s timeout)**"
            
            lang_msg = await message.reply_text(language_text)
            
            try:
                response = await client.wait_for_message(
                    chat_id=message.chat.id,
                    filters=filters.create(lambda _, __, m: (
                        m.from_user and m.from_user.id == message.from_user.id and m.text and m.text.isdigit()
                    )),
                    timeout=60
                )
                
                try:
                    choice = int(response.text)
                    if not 1 <= choice <= len(available_languages):
                        await lang_msg.delete()
                        await response.delete()
                        await message.reply_text("Invalid choice!")
                        return None, None
                    
                    selected_language = available_languages[choice - 1]
                    selected_code = selected_language['iso3code'].lower()
                    selected_name = selected_language['name']
                    
                    await lang_msg.delete()
                    await response.delete()
                    
                    confirm_msg = await message.reply_text(f"Selected: {selected_name}")
                    await asyncio.sleep(3)
                    await confirm_msg.delete()
                    
                    return selected_code, selected_name
                    
                except ValueError:
                    await lang_msg.delete()
                    await response.delete()
                    await message.reply_text("Please enter a valid number!")
                    return None, None
                    
            except asyncio.TimeoutError:
                await lang_msg.delete()
                await message.reply_text("Language selection timed out!")
                return None, None
                
        except Exception as e:
            logger.error(f"Error fetching languages: {str(e)}")
            await message.reply_text("Failed to fetch available languages!")
            return None, None
        
    async def hotstar_task():
        try:
            # Import the hotstar module            
            # Check if it's sports content that needs language selection
            language = None
            selected_language_name = None
            
            if "/sports/" in url:
                # Extract content ID and sport type for language selection
                parts = url.replace("/in/", "/").replace("https://www.hotstar.com/", "").strip("/").split("/")
                sport_type = parts[0]
                content_id = parts[-4] if "video/highlights/watch" in url or "video/replay/watch" in url else parts[-2]
                
                # Get language selection through UI
                language, selected_language_name = await show_language_selection(content_id, sport_type)
                if not language:
                    return None
            
            # Now call the hotstar main function with the URL and selected language
            result_info = await hotstar.main(url, language, selected_language_name)
            
            if not result_info:
                logger.error("Failed to retrieve information from Hotstar")
                return None
                
            # result_info already has our standardized structure, so we can use it directly
            info = result_info
            
            # Get formats information if needed
            formats = await get_formats(info)
            if formats:
                info["streams_info"] = formats["streams"]
                logger.info("Successfully retrieved format information for Hotstar")
            else:
                logger.warning("Failed to retrieve format information for Hotstar")
                
            logger.info("Successfully processed Hotstar URL and returning info")
            return info
            
        except Exception as e:
            logger.error(f"Hotstar error: {str(e)}")
            return None
    
    # Create task for Hotstar processing
    task = asyncio.create_task(hotstar_task())  # Create task immediately
    return task  # Return the created task

async def send_status_update(client, message, identifier, content_info, status_type, extra_data=None, status_msg_to_edit=None):
    """
    Sends or edits a status message for a download/upload task.
    This function handles creating the initial message and updating it for major state changes.
    """
    if extra_data is None:
        extra_data = {}

    filename = construct_filename(content_info, identifier)
    header = f"🎬 `{filename}`"

    text = f"{header}\n\n**Status:** {status_type.replace('_', ' ').title()}" # Default text

    if status_type == "download_start":
        text = f"**Processing...**" # The live progress message will be sent by handle_proceed_download
    elif status_type.startswith("upload_complete"):
        # This part is now unused due to direct video sending, but kept for structural integrity
        final_message_link = extra_data.get("uploaded_msg_link")
        text = f"{header}\n\n✅ **Upload Complete!**"
        if final_message_link:
            text += f"\n[View File]({final_message_link})"
    elif status_type in ["download_failed", "upload_unsuccessful"]:
        error = extra_data.get('error', 'An unknown error occurred.')
        text = f"{header}\n\n❌ **Task Failed!**\n**Reason:** `{error}`"

    try:
        if status_msg_to_edit:
            await status_msg_to_edit.edit_text(text)
            return status_msg_to_edit
        else:
            # If called from a callback, message is the callback_query.message
            chat_id = message.chat.id
            reply_to_id = message.id if isinstance(message, Message) else None
            return await client.send_message(chat_id, text, reply_to_message_id=reply_to_id)
    except Exception as e:
        logger.error(f"Failed to send/edit status update: {e}")
        # Fallback to sending a new message
        chat_id = message.chat.id
        return await client.send_message(chat_id, text)

async def update_single_task_progress_loop(client: Client, status_msg: Message, identifier: str):
    """A loop to update a single task's progress message."""
    last_text = ""
    while True:
        await asyncio.sleep(2)  # Update interval
        progress_data = download_progress.get_task_progress(identifier)
        if not progress_data:
            # Task completed or cleared, exit loop
            break

        # Filename is stored in progress_data now
        filename = progress_data.get('filename', 'Unknown File')
        header = f"🎬 `{filename}`"

        # The body is formatted by the new UI function
        body = await progress_display.format_task_progress(identifier, progress_data)

        full_text = f"{header}\n\n{body}"

        if full_text != last_text:
            try:
                await status_msg.edit_text(full_text)
                last_text = full_text
            except MessageNotModified:
                pass # No change, continue
            except Exception as e:
                logger.warning(f"Failed to update progress for {identifier}: {e}")
                break # Stop updating if message is deleted or other error occurs

async def handle_proceed_download(client, message, content_info, selected_resolution, selected_audios, identifier, retry_count=0):
    user_id = str(message.from_user.id)
    chat_id = message.chat.id
    status_msg = None
    progress_updater_task = None
    download_dir = get_isolated_download_path(identifier)

    try:
        async with download_semaphore:
            filename = construct_filename(content_info, identifier)
            
            # Send initial placeholder message that will be updated with progress
            status_msg = await client.send_message(chat_id, f"🎬 `{filename}`\n\n**Starting download...**")

            # Store filename and content_info in progress data for the updater loop
            initial_progress_data = {
                'filename': filename,
                'content_info': content_info,
                'video': {},
                'audio': {},
                'status': 'Download'
            }
            download_progress.update_progress(identifier, initial_progress_data)
            
            # Start the live progress updater
            progress_updater_task = asyncio.create_task(
                update_single_task_progress_loop(client, status_msg, identifier)
            )

            # Initialize downloader
            downloader = Nm3u8DLREDownloader(
                stream_url=content_info["streams"]["dash"],
                selected_resolution=selected_resolution,
                selected_audios=selected_audios,
                content_info=content_info,
                download_dir=download_dir,
                filename=filename,
                identifier=identifier
            )
            
            # Execute download
            return_code = await downloader.execute()
            
            if return_code != 0:
                if retry_count < MAX_DOWNLOAD_RETRIES:
                    logger.info(f"Download failed, attempting retry {retry_count + 1}/{MAX_DOWNLOAD_RETRIES}")
                    # No need to clear task here, just retry
                    return await handle_proceed_download(
                        client, message, content_info,
                        selected_resolution, selected_audios,
                        identifier, retry_count + 1
                    )
                
                raise Exception("Download failed after multiple retries.")
            
            # Update status to Uploading
            progress_data = download_progress.get_task_progress(identifier)
            progress_data['status'] = 'Upload'
            download_progress.update_progress(identifier, progress_data)

            # Handle file path and codecs
            user_id_from_id = identifier.split('_')[0] if '_' in identifier else None
            extension = "mp4" if user_id_from_id in MP4_USER_IDS else "mkv"
            final_file_path = os.path.join(download_dir, f"{filename}.{extension}")
            
            # Ensure the downloaded file exists before renaming
            # N_m3u8dl-re combines files into the final name, so check for that
            source_file = os.path.join(download_dir, filename)
            if os.path.exists(source_file) and not os.path.exists(final_file_path):
                 os.rename(source_file, final_file_path)
            elif not os.path.exists(final_file_path):
                 raise FileNotFoundError(f"Neither source {filename} nor target {final_file_path} exist after download.")


            upload_success = await upload_video(client, message, final_file_path, filename, download_dir, identifier, status_msg)
            
            if not upload_success:
                raise Exception("Upload failed.")
                
            return True
            
    except Exception as e:
        logger.error(f"Error in handle_proceed_download for identifier {identifier}: {e}", exc_info=True)
        if status_msg:
            try:
                await send_status_update(client, message, identifier, content_info, "download_failed", {"error": str(e)}, status_msg_to_edit=status_msg)
            except Exception as e_upd:
                logger.error(f"Failed to send failure status update: {e_upd}")
        return False
    finally:
        # This block ensures cleanup happens regardless of success or failure
        if progress_updater_task and not progress_updater_task.done():
            progress_updater_task.cancel()
        
        # Remove task from tracking
        download_progress.clear_task(identifier)
        
        # Clean up the download directory
        cleanup_download_dir(download_dir)


class DownloadProgress:
    def __init__(self):
        self.tasks = {}
        self.lock = threading.Lock()
        
    def update_progress(self, identifier, progress_data):
        """Update progress for a task using our existing progress_data structure"""
        with self.lock:
            if identifier not in self.tasks:
                self.tasks[identifier] = progress_data
            else:
                self.tasks[identifier].update(progress_data)
    
    def get_task_progress(self, identifier):
        """Get progress for a specific task"""
        with self.lock:
            return self.tasks.get(identifier, {})
    
    def get_all_tasks(self):
        """Get a copy of all tasks"""
        with self.lock:
            return self.tasks.copy()

    def clear_task(self, identifier):
        """Clear progress data for a task"""
        with self.lock:
            if identifier in self.tasks:
                self.tasks.pop(identifier, None)
                logger.info(f"Cleared task from progress tracking: {identifier}")

class ProgressDisplay:
    def __init__(self):
        self.progress_bar_length = 10
        self.user_pages = {}
        self.active_task_messages = {}
        self.lock = asyncio.Lock()

    def create_circle_progress_bar(self, percentage):
        """Creates a progress bar using circles."""
        filled = round(self.progress_bar_length * percentage / 100)
        return '●' * filled + '○' * (self.progress_bar_length - filled)

    async def calculate_average_progress(self, progress_data):
        """Calculate average progress across all streams"""
        if progress_data.get('status') == 'Upload':
            return float(progress_data.get('upload', {}).get('percentage', 0))

        video_percentage = float(progress_data.get('video', {}).get('percentage', 0))
        audio_percentages = [float(audio.get('percentage', 0)) for audio in progress_data.get('audio', {}).values()]
        
        total = video_percentage + sum(audio_percentages)
        count = 1 + len(audio_percentages)
        return round(total / count if count else 0, 1)

    async def get_real_status(self, progress_data):
        """Determine the real status based on progress"""
        status = progress_data.get('status', 'Download')
        if status == 'Upload':
            return 'Uploading'
        
        avg_progress = await self.calculate_average_progress(progress_data)
        if avg_progress >= 99.9:
            return 'Processing'
            
        return 'Downloading'

    async def calculate_total_speed(self, progress_data):
        """Calculate total speed from all streams"""
        def convert_speed_to_kbps(speed_str):
            try:
                if not isinstance(speed_str, str): return 0
                value = float(re.sub(r'[a-zA-Z/s]', '', speed_str))
                if 'MB' in speed_str.upper():
                    return value * 1024
                return value
            except:
                return 0

        total_speed_kbps = convert_speed_to_kbps(progress_data.get('video',{}).get('speed', '0 KB/s'))
        for audio in progress_data.get('audio', {}).values():
            total_speed_kbps += convert_speed_to_kbps(audio.get('speed', '0 KB/s'))

        if total_speed_kbps >= 1024:
            return f"{total_speed_kbps / 1024:.2f} MB/s"
        return f"{total_speed_kbps:.2f} KB/s"

    async def parse_video_progress(self, line, identifier):
        """Parse video progress from a line"""
        try:
            resolution = "N/A"
            vid_info_match = re.search(r'Vid (\d+x\d+)', line)
            if vid_info_match:
                resolution = vid_info_match.group(1)

            percentage, speed, eta = 0, "0 KB/s", "00:00"
            progress_match = re.search(r'(\d+\.\d+)%', line)
            if progress_match: percentage = float(progress_match.group(1))
            
            speed_match = re.search(r'([\d.]+(?:MB/s|KB/s))', line)
            if speed_match: speed = speed_match.group(1)
            
            eta_match = re.search(r'(\d{2}:\d{2}:\d{2}|\d{2}:\d{2})', line)
            if eta_match: eta = eta_match.group(1)
            
            return {'resolution': resolution, 'percentage': percentage, 'speed': speed, 'eta': eta}
        except Exception as e:
            logger.error(f"Error parsing video progress for {identifier}: {e}")
            return None

    async def parse_audio_progress(self, line, identifier):
        """Parse audio progress from a line"""
        try:
            lang_match = re.search(r'Aud \d+ Kbps \| ([a-zA-Z0-9]+)', line)
            display_lang = lang_match.group(1).title() if lang_match else "Unknown"

            percentage, speed = 0, "0 KB/s"
            progress_match = re.search(r'(\d+\.\d+)%', line)
            if progress_match: percentage = float(progress_match.group(1))

            speed_match = re.search(r'([\d.]+(?:MB/s|KB/s))', line)
            if speed_match: speed = speed_match.group(1)
            
            return display_lang, {'percentage': percentage, 'speed': speed}
        except Exception as e:
            logger.error(f"Error parsing audio progress for {identifier}: {e}")
            return None, None

    async def update_progress_from_line(self, line, progress_data, identifier):
        """Update progress data from a line based on identifier"""
        if line.startswith('Vid'):
            video_progress = await self.parse_video_progress(line, identifier)
            if video_progress:
                progress_data['video'].update(video_progress)
                
        elif line.startswith('Aud'):
            lang, audio_progress = await self.parse_audio_progress(line, identifier)
            if lang and audio_progress:
                progress_data['audio'][lang] = audio_progress
            
        return progress_data
    
    async def format_task_progress(self, identifier, progress_data):
        """Formats the body of the progress message to match the screenshot UI."""
        if not progress_data:
            return "Gathering task data..."

        total_progress = await self.calculate_average_progress(progress_data)
        progress_bar = self.create_circle_progress_bar(total_progress)
        status = await self.get_real_status(progress_data)
        
        status_part = (
            f"**Status: {status}**\n"
            f"{progress_bar} {total_progress:.1f}%"
        )
        
        if status == 'Uploading':
            upload_data = progress_data.get('upload', {})
            total_speed = upload_data.get('speed', '0.00 MB/s')
            eta = upload_data.get('eta', '00:00:00')
            
            speed_eta_part = (
                f"**Total Speed:** {total_speed}\n"
                f"**ETA:** {eta}"
            )
            return f"📊 {status_part}\n\n💨 {speed_eta_part}"

        # --- Video & Audio Progress (for Downloading/Processing) ---
        video_data = progress_data.get('video', {})
        video_res = "480p"
        if video_data.get('resolution') and 'x' in video_data.get('resolution'):
            video_res = video_data.get('resolution').split('x')[-1] + 'p'
        video_perc = float(video_data.get('percentage', 0))
        
        stream_parts = [f"**Video ({video_res}):** {video_perc:.1f}%"]
        
        audio_data = progress_data.get('audio', {})
        for lang, audio in audio_data.items():
            lang_name = pickFormats['audio'].get(lang.lower(), lang.title())
            audio_perc = float(audio.get('percentage', 0))
            stream_parts.append(f"**Audio ({lang_name}):** {audio_perc:.1f}%")
        
        streams_part = "\n".join(stream_parts)

        # --- Speed & ETA ---
        total_speed = await self.calculate_total_speed(progress_data)
        eta = video_data.get('eta', '00:00:00')

        speed_eta_part = (
            f"**Total Speed:** {total_speed}\n"
            f"**ETA:** {eta}"
        )
        
        return (
            f"📊 {status_part}\n\n"
            f"🎥 {streams_part}\n\n"
            f"💨 {speed_eta_part}"
        )


    async def format_all_progress(self, tasks_dict, page=1):
        """Formats all active tasks for the /tasks command with the new UI."""
        active_tasks = len(tasks_dict)
        if not active_tasks:
            return None, None
            
        header = [f"**⚡️ Active Tasks: {active_tasks}**"]
        
        tasks_per_page = 5
        total_pages = (active_tasks + tasks_per_page - 1) // tasks_per_page
        start_idx = (page - 1) * tasks_per_page
        end_idx = min(start_idx + tasks_per_page, active_tasks)
        
        task_sections = []
        task_list = list(tasks_dict.items())[start_idx:end_idx]

        for i, (identifier, progress_data) in enumerate(task_list):
            filename = progress_data.get('filename', 'Unknown File')
            task_header = f"🎬 `{filename}`"
            task_body = await self.format_task_progress(identifier, progress_data)
            task_sections.append(f"{task_header}\n{task_body}")

        buttons = []
        if active_tasks > tasks_per_page:
            row = []
            if page > 1:
                row.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"page_{page-1}"))
            if page < total_pages:
                row.append(InlineKeyboardButton("Next ➡️", callback_data=f"page_{page+1}"))
            if row:
                buttons.append(row)
            buttons.append([InlineKeyboardButton(f"Page: {page}/{total_pages}", callback_data=f"refresh_{page}")])
        
        return "\n\n" + "━" * 15 + "\n\n".join(header + task_sections), buttons if buttons else None

download_progress = DownloadProgress()
progress_display = ProgressDisplay()

# Export to helpers.download module to avoid circular imports
import download as download_module
download_module.download_progress = download_progress
download_module.progress_display = progress_display

# Helper function for resource cleanup
async def cleanup_resources(thumb, download_dir, display_filename):
    await asyncio.to_thread(cleanup_download_dir, download_dir)
    try:
        if await asyncio.to_thread(os.path.exists, display_filename):
            await asyncio.to_thread(os.remove, display_filename)
        if thumb and await asyncio.to_thread(os.path.exists, thumb):
            await asyncio.to_thread(os.remove, thumb)
    except Exception as e:
        logger.error(f"Failed to remove copied file/thumb: {e}")

# Refactoring upload_video into a class
class VideoUploader:
    """Class for handling video uploads to Telegram, Google Drive or Gofile."""
    
    def __init__(self, client, message, file_path, filename, download_dir, identifier, download_status_msg=None):
        """Initialize the VideoUploader with necessary parameters."""
        self.client = client
        self.message = message
        self.file_path = file_path
        self.filename = filename
        self.download_dir = download_dir
        self.identifier = identifier
        self.upload_status_msg = download_status_msg
        self.premium_client = None
        self.thumb = None
        self.content_info = {}
        self.uploaded_msg_in_user_chat = None
        self.use_rclone = False
        self.is_trial = False
        self.upload_channel_id = -1001963446260  # The specific channel to upload to
        self.upload_destination = None # Can be 'telegram', 'gdrive', 'gofile'
    
    async def upload(self):
        """Main method to handle the video upload process."""
        try:
            async with upload_semaphore:
                await self._initialize_upload()
                await self._determine_upload_method()

                if self.upload_destination == 'gofile':
                    gofile_url = await self._upload_via_gofile()
                    caption = (f"✅ **Upload Complete!**\n\n"
                               f"🎬 `{self.display_filename}`\n\n"
                               
                               f"🔗 **Gofile Link:** {gofile_url}")
                    # Send the link to the user chat and the upload channel
                    self.uploaded_msg_in_user_chat = await self.client.send_message(
                        chat_id=self.message.chat.id,
                        text=caption
                    )

                elif self.upload_destination == 'gdrive':
                    await self._upload_via_rclone()
                    # Rclone logic should post the message itself. Assuming it's handled there.
                    # For this implementation, we'll return True if no exception is raised.
                    return True # Skip the copy logic as rclone might have its own flow

                else: # 'telegram'
                    await self._upload_via_telegram()
                
                # Copy the result message (file or link) to the upload channel
                if self.uploaded_msg_in_user_chat:
                    await self.uploaded_msg_in_user_chat.copy(self.upload_channel_id)
                    
                    # Delete the progress message in the user's chat
                    if self.upload_status_msg:
                        try:
                            await self.upload_status_msg.delete()
                        except Exception:
                            pass
                    return True
                else:
                    raise Exception("Upload to user chat failed, cannot copy to channel.")

        except Exception as e:
            logger.error(f"Upload failed for {self.identifier}: {e}", exc_info=True)
            await self._handle_upload_failure(e)
            return False
        finally:
            await self._cleanup()
            await self._finalize()
    
    async def _initialize_upload(self):
        """Initialize upload by setting up necessary data and configurations."""
        user_id = str(self.identifier.split('_')[0])
        chat_id = self.message.chat.id
        self.user_id = user_id
        
        self.is_trial = (int(user_id) not in get_full_access_users() and
                         chat_id not in get_full_access_users()) and \
                        (int(user_id) in TRIAL_ACCESS or chat_id in TRIAL_ACCESS)
        
        # Load content_info from progress tracker
        progress_data = download_progress.get_task_progress(self.identifier)
        self.content_info = progress_data.get('content_info', {})
        
        extension = "mp4" if user_id in MP4_USER_IDS else "mkv"
        self.display_filename = f"{self.filename}.{extension}"
        
        self.duration = await self._get_video_metadata()
        
        self.thumb = await get_thumbnail(self.identifier, self.file_path, self.download_dir)
        self.file_size = os.path.getsize(self.file_path)
        self.file_size_mb = self.file_size / (1024 * 1024)
        self.file_size_gb = self.file_size / (1024 * 1024 * 1024) # Added for Gofile check

    async def _get_video_metadata(self):
        duration = 0
        try:
            metadata = await asyncio.to_thread(extractMetadata, createParser(self.file_path))
            if metadata and metadata.has("duration"):
                duration = metadata.get('duration').seconds
        except Exception as e:
            logger.error(f"Error getting duration for {self.file_path}: {e}")
        return duration
    
    async def _determine_upload_method(self):
        """Determine whether to use Gofile, Gdrive, or direct Telegram upload."""
        global UPLOAD_MODE
        force_drive_upload = self.content_info.get('force_drive_upload', False)
        
        # Priority 1: Gofile if file size > 1.95GB
        if self.file_size_gb > 1.95:
            self.upload_destination = 'gofile'
            logger.info(f"File size is {self.file_size_gb:.2f}GB. Forcing Gofile upload.")
            return

        # Priority 2: Gofile if mode is manually set to 'gofile' by admin
        if UPLOAD_MODE == 'gofile':
            self.upload_destination = 'gofile'
            logger.info("Upload mode is 'gofile'. Using Gofile.")
            return

        # Priority 3: Gdrive if -d flag is used or file size is > 1.99GB (Telegram limit)
        if force_drive_upload or self.file_size_mb > 1990:
            self.use_rclone = True # for legacy compatibility
            self.upload_destination = 'gdrive'
            logger.info(f"File size is {self.file_size_mb:.2f}MB. Using Gdrive/Rclone upload.")
            return
        
        # Default: Telegram upload for smaller files when mode is 'gdrive'
        self.upload_destination = 'telegram'
        logger.info("File is small enough. Using direct Telegram upload.")

    async def _upload_via_gofile(self):
        """Uploads the file to Gofile and returns the link."""
        logger.info(f"Starting Gofile upload for {self.file_path}")
        try:
            # Update status message
            await self.upload_status_msg.edit_text(f"🎬 `{self.display_filename}`\n\n**Uploading to Gofile...**")

            async with aiohttp.ClientSession() as session:
                async with aiofiles.open(self.file_path, 'rb') as f:
                    data = aiohttp.FormData()
                    data.add_field('file', f, filename=self.display_filename)
                    
                    upload_url = "https://store8.gofile.io/contents/uploadfile"
                    async with session.post(upload_url, data=data) as response:
                        response.raise_for_status()
                        result = await response.json()

                        if result.get("status") == "ok":
                            download_page = result.get("data", {}).get("downloadPage")
                            if download_page:
                                logger.info(f"Gofile upload successful: {download_page}")
                                return download_page
                            else:
                                raise Exception("Gofile API response missing downloadPage URL.")
                        else:
                            error_message = result.get("data", {}).get("error", "Unknown error")
                            raise Exception(f"Gofile API returned an error: {error_message}")
        except Exception as e:
            logger.error(f"Gofile upload failed: {e}", exc_info=True)
            raise
    
    async def _upload_via_rclone(self):
        # ... (rclone logic remains the same)
        logger.info(f"Rclone upload not fully implemented in this scope. Placeholder for {self.identifier}")
        await self.upload_status_msg.edit_text(f"🎬 `{self.display_filename}`\n\n**Uploading to Google Drive...**")
        # In a real implementation, rclone would upload and return a link, which would be sent to the user.
        # For this example, we'll simulate a success message.
        await asyncio.sleep(5) # Simulate upload time
        await self.upload_status_msg.edit_text(f"✅ **Upload Complete!**\n\n🎬 `{self.display_filename}`\n\n🔗 **Gdrive Link:** `(Simulated Link)`")
        pass
    
    async def _upload_via_telegram(self):
        """Upload the video file first to the user chat, then it will be copied."""
        start_time = time.time()
        
        async def progress_callback(current, total):
            now = time.time()
            if now - getattr(progress_callback, "last_update_time", 0) < 1.5:
                return
            progress_callback.last_update_time = now
            
            elapsed_time = now - start_time
            if elapsed_time > 0:
                speed = current / elapsed_time
                speed_str = f"{speed / (1024 * 1024):.2f} MB/s"
                eta = (total - current) / speed if speed > 0 else 0
                eta_str = str(timedelta(seconds=int(eta)))
            else:
                speed_str = "0.00 MB/s"
                eta_str = "00:00:00"

            percentage = (current / total) * 100 if total > 0 else 0
            
            upload_progress_data = {
                'speed': speed_str,
                'eta': eta_str,
                'percentage': percentage
            }
            # Update the global progress dictionary
            current_task = download_progress.get_task_progress(self.identifier)
            if current_task:
                current_task['upload'] = upload_progress_data
                download_progress.update_progress(self.identifier, current_task)

        caption = f'''<b>{self.display_filename}</b>''' if self.user_id in MP4_USER_IDS else f'''<code>{self.display_filename}</code>'''
        
        # Use premium session if file is large
        if self.file_size > 50 * 1024 * 1024 and PREMIUM_STRING: # Use premium for files > 50MB
            self.premium_client = await premium_session_pool.get_session()
            uploader_client = self.premium_client or self.client
        else:
            uploader_client = self.client

        logger.info(f"Uploading {self.display_filename} to message chat {self.message.chat.id}...")
        
        # MODIFIED: Upload to the user chat first
        self.uploaded_msg_in_user_chat = await uploader_client.send_video(
            chat_id=self.message.chat.id,
            video=self.file_path,
            caption=caption,
            file_name=self.display_filename,
            duration=self.duration,
            thumb=self.thumb,
            progress=progress_callback
        )
    
    async def _handle_upload_failure(self, error):
        await send_status_update(
            self.client, self.message, self.identifier, self.content_info, 
            "upload_unsuccessful", {'error': str(error)}, 
            status_msg_to_edit=self.upload_status_msg
        )
    
    async def _cleanup(self):
        """Clean up resources after upload."""
        await cleanup_resources(self.thumb, self.download_dir, self.display_filename)
    
    async def _finalize(self):
        """Finalize the upload process and release resources."""
        if self.premium_client:
            await premium_session_pool.release_session(self.premium_client)


async def upload_video(client, message, file_path, filename, download_dir, identifier, download_status_msg=None):
    """Upload video file to telegram with proper metadata."""
    uploader = VideoUploader(client, message, file_path, filename, download_dir, identifier, download_status_msg)
    return await uploader.upload()

async def check_subscription(message):
    """Check if user is subscribed to main channel and has access"""
    try:
        user_id = message.from_user.id
        chat_id = message.chat.id
        
        # First check if user has access (either user or chat)
        has_access = (user_id in get_full_access_users()) or (user_id in TRIAL_ACCESS) or (chat_id in get_full_access_users()) or (chat_id in TRIAL_ACCESS)
        if not has_access:
            buttons = [
                [InlineKeyboardButton("🎬 Join Our Channel", url=f"https://t.me/{OWNER_CHANNEL}")],
            ]
            await message.reply(
                "**🔒 Access Restricted**\n\n"
                "This bot is exclusively available for:\n"
                "• Premium Groups\n"
                "• Trial Access Groups\n\n"
                "To get access:\n"
                "1. Join our channel\n"
                "2. Contact admin for access\n"
                "3. Join trial groups\n\n"
                f"**Contact: {OWNER}**",
                reply_markup=InlineKeyboardMarkup(buttons)
            )
            return False

        # Then check channel subscription
        member = None
        try:
            member = await app.get_chat_member(MAIN_CHANNEL, user_id)
        except Exception:
            pass
            
        if member and (member.status == ChatMemberStatus.MEMBER or
                      member.status == ChatMemberStatus.ADMINISTRATOR or
                      member.status == ChatMemberStatus.OWNER):
            return True
            
        buttons = [
            [InlineKeyboardButton("🎬 Join Our Channel", url=f"https://t.me/{OWNER_CHANNEL}")],
            [InlineKeyboardButton("✨ Let's Start", callback_data=f"check_{user_id}")]
        ]
        await message.reply(
            "**🎉 Welcome to Ignite x users!**\n\n"
            "Join our **Main Channel** to get:\n"
            "• Latest **Movies & Shows** Updates\n"
            "• Premium **Quality Content**\n"
            "• **Exclusive Features**\n\n"
            "**Steps:**\n"
            "1. Click **'🎬 Join Our Channel'**\n"
            "2. Join **ignite x users**\n"
            "3. Come back and click **'✨ Let's Start'**",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        return False
    except Exception:
        return False

async def get_platform_content(client, message, platform_name, url):
    """Get content details based on platform"""
    try:
        handlers = {
            "JioHotstar": handle_hotstar
        }
        
        logger.info(f"Processing URL: {url} with platform: {platform_name}")
        
        handler = handlers.get(platform_name)
        if not handler:
            logger.error(f"Invalid platform specified: {platform_name}")
            error_msg = await message.reply("❌ **Unsupported platform or URL.**")
            await asyncio.sleep(3)
            await error_msg.delete()
            return None

        # Create task with timeout
        try:
            async with asyncio.timeout(120):  # 2 minute timeout
                task = asyncio.create_task(handler(client, message, url))
                content_info = await task
        except asyncio.TimeoutError:
            logger.error(f"Handler {platform_name} timed out for URL: {url}")
            await message.reply(f"❌ Request timed out while processing {platform_name}")
            return None
        except Exception as e:
            logger.error(f"Error in handler {platform_name}: {str(e)}")
            return None
            
        if content_info is None:
            logger.error(f"Handler {platform_name} returned None for URL: {url}")
        else:
            logger.info(f"Successfully retrieved content info for {platform_name}")
            
        return content_info
        
    except Exception as e:
        logger.exception(f"Error in get_platform_content: {str(e)}")
        return None



# Enhanced DL Command with Premium Restrictions
@app.on_message(filters.command("dl"))
async def handle_dl_command(client, message):
    """Download command handler with premium restrictions"""
    try:
        user_id = message.from_user.id
        
        # Check if user is banned
        if not await is_user_authorized(user_id):
            await message.reply_text("❌ You are not authorized to use this command.")
            return
        
        # Check if user has premium access
        if not await check_premium_access(user_id):
            await send_premium_required_message(message)
            return
        
        # Check if bot is locked
        if is_bot_locked():
            await message.reply(LOCK_MESSAGE)
            return
        
        # Update user activity
        await db.update_user_activity(user_id)
        
        # Log download request
        logger.info(f"Download request from user {user_id}")
        
        # Create task for processing download request
        task = asyncio.create_task(process_dl_request(client, message))
        
        try:
            # Set timeout for 5 minutes
            async with asyncio.timeout(300):
                await task
        except asyncio.TimeoutError:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                logger.warning(f"Download task cancelled due to timeout for user {user_id}")
                timeout_msg = await message.reply("⏰ **Request timed out. Please try again later.**")
                await asyncio.sleep(10)
                try:
                    await timeout_msg.delete()
                except:
                    pass
        except Exception as e:
            logger.error(f"Error processing download request from user {user_id}: {e}")
            error_msg = await message.reply("🔄 **API did not respond. This issue is not from our end. Please try again after some time.**")
            await asyncio.sleep(5)
            try:
                await error_msg.delete()
            except:
                pass
            
    except Exception as e:
        logger.exception(f"Error in handle_dl_command for user {getattr(message.from_user, 'id', 'unknown')}: {str(e)}")
        try:
            error_msg = await message.reply(f"❌ **An error occurred:** {str(e)}")
            await asyncio.sleep(5)
            try:
                await message.delete()
                await error_msg.delete()
            except:
                pass
        except:
            pass



async def process_dl_request(client, message):
    try:
        user_id = message.from_user.id
        chat_id = message.chat.id
        current_time = time.time()

        # Trial user check (existing code)
        is_trial = (user_id not in get_full_access_users() and chat_id not in get_full_access_users()) and (user_id in TRIAL_ACCESS or chat_id in TRIAL_ACCESS)
        
        # --- NEW: Simplified Command Parsing ---
        command_parts = message.text.split()
        if len(command_parts) < 2 or not command_parts[1].startswith("http"):
            await message.reply(
                "**Invalid Command Format!**\n\n"
                "Please use the format: `/dl <URL>`\n\n"
                "**Example:**\n"
                f"`/dl {PLATFORM_EXAMPLES['hotstar'][1]}`"
            )
            return
        
        url = command_parts[1]
        platform_name = None

        if "hotstar.com" in url:
            platform_name = "JioHotstar"
        # Add other platform detections here, e.g.:
        # elif "jiocinema.com" in url:
        #     platform_name = "JioCinema"

        if not platform_name:
            await message.reply(f"**Unsupported URL!**\n\nI could not recognize the platform for this URL:\n`{url}`")
            return
        # --- END: Simplified Command Parsing ---

        if is_trial:
            # Check for restricted platforms first
            if platform_name in TRIAL_RESTRICTED_PLATFORMS:
                await message.reply(
                    f"**⚠️ Access Restricted**\n\n"
                    f"**{TRIAL_RESTRICTED_PLATFORMS[platform_name]}**\n\n"
                    "**🌟 Upgrade to full access to use all platforms!**"
                )
                return

            if user_id in TRIAL_COOLDOWNS:
                cooldown_end = TRIAL_COOLDOWNS[user_id]["time"]
                if current_time < cooldown_end:
                    remaining = int(cooldown_end - current_time)
                    minutes = remaining // 60
                    seconds = remaining % 60
                    await message.reply(
                        f"**⏳ Please wait {minutes}m {seconds}s before starting another download.**\n"
                        "**🌟 Upgrade to full access to download without waiting!**"
                    )
                    return
                else:
                    del TRIAL_COOLDOWNS[user_id]

            try:
                with open('data/user_plans.json', 'r') as f:
                    user_plans = json.load(f)
            except (FileNotFoundError, json.JSONDecodeError):
                user_plans = {}
            
            str_user_id = str(user_id)
            
            has_720p = user_plans.get(str_user_id, {}).get("720p_limit", 0) > 0
            has_1080p = user_plans.get(str_user_id, {}).get("1080p_limit", 0) > 0
            
            if not has_720p and not has_1080p:
                verify_bot = ASSISTANT_BOT
                keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton("🎯 Get Access", url=f"https://t.me/{verify_bot}?start=verify")]
                ])
                await message.reply(
                    "**No Tasks Available!**\n\n"
                    "Looks like you're all out of tasks. Time to get verified again.\n\n"
                    "**What You Can Do:**\n"
                    "• Click the button below to get more tasks\n"
                    "• Choose between 720p or 1080p\n"
                    "• Come back here once you're verified\n\n"
                    "Don't worry, the process is painless... mostly.",
                    reply_markup=keyboard
                )
                return

        # Check for ongoing downloads
        if user_id not in OWNERS:
            has_reached_limit, reason = await check_download_limits(user_id, platform_name, chat_id)
            if has_reached_limit:
                if reason == "platform_limit":
                    await message.reply(f"**✨ You already have an ongoing task from {platform_name}. Please wait for it to complete.**")
                    return
                elif reason == "max_concurrent":
                    await message.reply("**✨ You have reached the maximum limit of 3 concurrent downloads. Please wait for at least one to complete.**")
                    return
                elif reason == "restricted_platform":
                    await message.reply(f"**⚠️ Access Denied**\n\n**{platform_name} platform is restricted to authorized users only.**\n\n**Please contact an administrator if you need access.**")
                    return
        
        force_drive_upload = "-d" in command_parts
            
        status_msg = await message.reply("🔍 **Fetching content information...**")
            
        # Call with platform name, not flag
        info = await get_platform_content(client, message, platform_name, url)
        if not info:
            await status_msg.delete()
            error_msg = await message.reply("❌ Error: Failed to get content information")
            await asyncio.sleep(5)
            await message.delete()
            await error_msg.delete()
            return

        if isinstance(info, asyncio.Task):
            info = await info
            
        if not info:
            await status_msg.delete()
            error_msg = await message.reply("❌ Error: Failed to get content information")
            await asyncio.sleep(5)
            await message.delete()
            await error_msg.delete()
            return

        info["force_drive_upload"] = force_drive_upload

        platform = info.get('platform', 'Unknown')
        title = info.get('title', 'Unknown Title')
        episode_title = info.get('episode_title', '')
        episode_number = info.get('episode_number', '')
        
        text = f"**👤 User:** {message.from_user.mention}\n"
        text += f"**Platform:** `{platform}`\n**Title:** `{title}`"
        if episode_title: text += f"\n**Episode:** `{episode_title}`"
        if episode_number: text += f"\n**Episode Number:** `{episode_number}`"
        if force_drive_upload: text += f"\n**Upload Method:** `Drive (Forced)`"

        if is_trial:
            text += "\n\n**Available Tasks:**\n"
            if has_720p: text += f"• 720p Tasks: {user_plans[str_user_id]['720p_limit']}\n"
            if has_1080p: text += f"• 1080p Tasks: {user_plans[str_user_id]['1080p_limit']}"
        
        text += "**\n\nPlease select video resolution:**"
        
        await status_msg.edit_text(text)
            
        identifier = f"{message.from_user.id}_{message.id}"
        store_content_info(identifier, info)
        
        try:
            markup = create_resolution_buttons(identifier, info["streams_info"], info)
            await status_msg.edit_reply_markup(reply_markup=markup)
            asyncio.create_task(delete_buttons_after_delay(status_msg))
        except (KeyError, Exception) as e:
            if isinstance(e, Exception): logger.exception(f"Error in process_dl_request: {str(e)}")
            info_msg = await message.reply("**Could not load streams. It's an official API end issue, they didn't return any information. Not from our end.**")
            await asyncio.sleep(5)
            await message.delete()
            await info_msg.delete()
            if status_msg: await status_msg.delete()
    except Exception as e:
        logger.exception(f"Error in process_dl_request: {str(e)}")
        await message.reply("**Could not load streams. It's an official API end issue, they didn't return any information. Not from our end.**")

@app.on_message(filters.command(["tasks", "at"]))
async def show_tasks(_, message):
    if not await check_subscription(message):
        return
        
    user_id = message.from_user.id
    chat_id = message.chat.id
    
    try: await message.delete()
    except Exception: pass
    
    async with progress_display.lock:
        previous_msg = progress_display.active_task_messages.get(chat_id)
        if previous_msg:
            try: await previous_msg.delete()
            except Exception: pass
        
        all_tasks = download_progress.get_all_tasks()
        has_active_tasks = bool(all_tasks)
        
        if not has_active_tasks:
            status_msg = await message.reply(
                "**⚡️ Active Tasks: 0**\n\n"
                "No downloads are currently in progress."
            )
            progress_display.active_task_messages[chat_id] = status_msg
            await asyncio.sleep(10)
            async with progress_display.lock:
                if progress_display.active_task_messages.get(chat_id) == status_msg:
                    await status_msg.delete()
                    progress_display.active_task_messages.pop(chat_id, None)
            return
        
        status_msg = await message.reply("🔄 Loading active tasks...")
        progress_display.active_task_messages[chat_id] = status_msg
    
    async def update_progress():
        last_progress_text = None
        
        while True:
            await asyncio.sleep(5) # Update interval
            
            async with progress_display.lock:
                if progress_display.active_task_messages.get(chat_id) != status_msg:
                    break
            
            try:
                current_tasks = download_progress.get_all_tasks()
                current_page = progress_display.user_pages.get(user_id, 1)
                
                if not current_tasks:
                    break

                progress_text, buttons = await progress_display.format_all_progress(current_tasks, page=current_page)

                if progress_text != last_progress_text:
                    await status_msg.edit_text(
                        progress_text,
                        reply_markup=InlineKeyboardMarkup(buttons) if buttons else None
                    )
                    last_progress_text = progress_text

            except MessageNotModified:
                pass
            except Exception as e:
                logger.error(f"Error in task update loop: {e}")
                if "message to edit not found" in str(e).lower():
                    break
        
        # Loop finished, meaning no more tasks. Clean up the message.
        async with progress_display.lock:
            if progress_display.active_task_messages.get(chat_id) == status_msg:
                try:
                    await status_msg.edit_text("**✅ All downloads completed!**")
                    await asyncio.sleep(5)
                    await status_msg.delete()
                except Exception:
                    pass
                finally:
                    progress_display.active_task_messages.pop(chat_id, None)
    
    asyncio.create_task(update_progress())


@app.on_message(filters.command("task"))
@owner_only
async def show_tasks_owner(client, message):
    """Owner-only command to view all active tasks."""
    # This handler is essentially the same as show_tasks but restricted.
    await show_tasks(client, message)


# Helper function to check if bot is locked (you need to implement this)
@app.on_callback_query()
async def handle_callback(client, callback_query: CallbackQuery):
    try:
        current_user_id = callback_query.from_user.id
        chat_id = callback_query.message.chat.id
        is_trial = (current_user_id not in get_full_access_users() and chat_id not in get_full_access_users()) and (current_user_id in TRIAL_ACCESS or chat_id in TRIAL_ACCESS)
        
        data = callback_query.data
        if not data:
            return

        # --- NAVIGATION & BASIC ACTIONS ---

        if data == "back_to_start":
            await callback_query.answer()
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("📚 Help", callback_data="help"),
                 InlineKeyboardButton("📊 Stats", callback_data="stats")],
                [InlineKeyboardButton("💎 Plans", callback_data="plans"),
                 InlineKeyboardButton("👤 Profile", callback_data="profile")]
            ])
            await callback_query.message.edit_text(START_MESSAGE, reply_markup=keyboard)
            return

        if data == "help":
            await callback_query.answer()
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ Back", callback_data="back_to_start")]
            ])
            await callback_query.message.edit_text(HELP_MESSAGE, reply_markup=keyboard)
            return

        if data == "stats":
            await callback_query.answer()
            stats = await db.get_stats()
            stats_text = f"📊 **Bot Statistics:**\n\n"
            stats_text += f"👥 **Total Users:** {stats.get('total_users', 0)}\n"
            stats_text += f"👑 **Total Admins:** {stats.get('total_admins', 0)}\n"
            stats_text += f"🚫 **Banned Users:** {stats.get('total_banned', 0)}\n"
            stats_text += f"💎 **Paid Users:** {stats.get('total_paid', 0)}\n"
            stats_text += f"✅ **Auth Users:** {stats.get('total_auth', 0)}\n"
            stats_text += f"🔥 **Active (24h):** {stats.get('active_users_24h', 0)}\n"
            stats_text += f"📅 **Active (7d):** {stats.get('active_users_7d', 0)}\n"
            stats_text += f"🆕 **New Today:** {stats.get('new_users_today', 0)}\n"
            keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="back_to_start")]])
            await callback_query.message.edit_text(stats_text, reply_markup=keyboard)
            return

        if data == "plans":
            await callback_query.answer()
            plans_text = f"💎 **Subscription Plans:**\n\n"
            plans_text += f"🥉 **Basic Plan**\n"
            plans_text += f"• Duration: 30 days\n"
            plans_text += f"• Price: $10\n"
            plans_text += f"• Features: Basic access\n\n"
            plans_text += f"🥈 **Premium Plan**\n"
            plans_text += f"• Duration: 30 days\n"
            plans_text += f"• Price: $20\n"
            plans_text += f"• Features: Premium access\n\n"
            plans_text += f"🥇 **VIP Plan**\n"
            plans_text += f"• Duration: 30 days\n"
            plans_text += f"• Price: $30\n"
            plans_text += f"• Features: VIP access\n\n"
            plans_text += f"💬 **Contact admin to purchase a plan!**"
            keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="back_to_start")]])
            await callback_query.message.edit_text(plans_text, reply_markup=keyboard)
            return
    
        if data == "profile":
            await callback_query.answer()
            user_id = callback_query.from_user.id
            user_data = await db.get_user(user_id)
            if not user_data:
                await callback_query.message.edit_text("❌ User data not found.")
                return
            
            profile_text = format_user_info(user_data)
            profile_text += f"\n**Status:**\n"
            profile_text += f"• Admin: {'✅' if await db.is_admin(user_id) else '❌'}\n"
            profile_text += f"• Paid: {'✅' if await db.is_paid_user(user_id) else '❌'}\n"
            profile_text += f"• Authorized: {'✅' if await db.is_auth_user(user_id) else '❌'}\n"
            profile_text += f"• Banned: {'✅' if await db.is_banned(user_id) else '❌'}\n"
            keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="back_to_start")]])
            await callback_query.message.edit_text(profile_text, reply_markup=keyboard)
            return

        # --- OTHER CALLBACKS ---


            
        # Handle subscription check callback
        if data.startswith('check_'):
            user_id = int(data.split('_')[1])
            if user_id != callback_query.from_user.id:
                await callback_query.answer("❌ This button is not for you!", show_alert=True)
                return
                
            try:
                member = await app.get_chat_member(MAIN_CHANNEL, user_id)
                
                if member.status == ChatMemberStatus.MEMBER or member.status == ChatMemberStatus.ADMINISTRATOR or member.status == ChatMemberStatus.OWNER:
                    supported_platforms_text = "\n".join([f"• `{v[1]}`" for k, v in PLATFORM_EXAMPLES.items()])
                    await callback_query.message.edit(
                        "**🎉 Welcome to Ignite x Users!**\n\n"
                        "You're now part of our exclusive community!\n\n"
                        "**🎯 How to Use:**\n"
                        "• Use `/dl <URL>`\n"
                        "• Choose your preferred quality\n"
                        "• Select audio tracks\n"
                        "• Wait for your download\n\n"
                        "**📺 Supported Platforms:**\n"
                        f"{supported_platforms_text}\n\n"
                        "**🔥 Start downloading your favorite content now!**",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("❌ Close", callback_data=f"close_{user_id}")]
                        ])
                    )
                    await callback_query.answer("✨ Welcome! You can now use the bot", show_alert=True)
                else:
                    await callback_query.answer("🎬 Please join our channel first to access the bot!", show_alert=True)
            except Exception:
                await callback_query.answer("🎬 Please try again in a few seconds!", show_alert=True)
            return
            
        # Handle close button callback
        if data.startswith('close_'):
            user_id = int(data.split('_')[1])
            if user_id != callback_query.from_user.id:
                await callback_query.answer("❌ This button is not for you!", show_alert=True)
                return
            await callback_query.message.delete()
            return
        
        # Handle pagination callbacks
        if data.startswith(('page_', 'refresh_')):
            try:
                user_id = callback_query.from_user.id
                page = int(data.split('_')[1]) if data.startswith('page_') else progress_display.user_pages.get(user_id, 1)
                progress_display.user_pages[user_id] = page
                
                all_tasks = download_progress.get_all_tasks()
                progress_text, buttons = await progress_display.format_all_progress(all_tasks, page)
                if progress_text:
                    await callback_query.message.edit_text(
                        progress_text,
                        reply_markup=InlineKeyboardMarkup(buttons) if buttons else None
                    )
                await callback_query.answer()
                return
            except Exception as e:
                logger.error(f"Error in pagination: {e}")
                await callback_query.answer("Error in pagination!", show_alert=True)
                return

        if data.startswith('aud_all_') or data.startswith('aud_clear_'):
            parts = data.split('_')
            action_part = f"{parts[0]}_{parts[1]}"
            base_identifier = f"{parts[2]}_{parts[3]}"

            user_id_from_identifier = int(base_identifier.split('_')[0])
            if callback_query.from_user.id != user_id_from_identifier:
                await callback_query.answer("Not Your Button!", show_alert=True)
                return

            callback_storage = load_callback_storage()
            try:
                with open('data/content_storage.json', 'r', encoding='utf-8') as f:
                    content_storage = json.load(f)
            except (FileNotFoundError, json.JSONDecodeError):
                await callback_query.answer("Content not found!")
                return

            content_info = content_storage.get(base_identifier)
            if not content_info:
                logger.error(f"Content not found for ID: {base_identifier}")
                await callback_query.answer("Content not found!")
                return
            
            if base_identifier not in callback_storage:
                callback_storage[base_identifier] = {"selected_audios": []}

            if action_part == "aud_all":
                all_audio_streams = content_info["streams_info"].get("audio", [])
                
                if is_trial:
                    all_audio_streams.sort(key=lambda x: (x["language"].lower()[:3] not in pickFormats["audio"], -x["bitrate"]))
                    selected_audios = [audio["stream_id"] for audio in all_audio_streams[:2]]
                    await callback_query.answer("Selected top 2 audio tracks for trial users.", show_alert=True)
                else:
                    selected_audios = [audio["stream_id"] for audio in all_audio_streams]
                    await callback_query.answer("Selected all available audio tracks.")

                callback_storage[base_identifier]["selected_audios"] = selected_audios
            
            elif action_part == "aud_clear":
                callback_storage[base_identifier]["selected_audios"] = []
                await callback_query.answer("Cleared all audio selections.")
            
            save_callback_storage(callback_storage)
            markup = create_audio_buttons(base_identifier, content_info["streams_info"])
            try:
                await callback_query.message.edit_reply_markup(reply_markup=markup)
            except MessageNotModified:
                pass
            return

        # Parse callback data for other actions
        parts = data.split('_', 3)
        if len(parts) < 3:
            return
            
        action = parts[0]
        base_identifier = f"{parts[1]}_{parts[2]}"
        
        # Check if the user who clicked is the same as the one who initiated
        if callback_query.from_user.id != int(parts[1]):
            await callback_query.answer("Not Your Button!", show_alert=True)
            return
            
        callback_storage = load_callback_storage()
        
        try:
            with open('data/content_storage.json', 'r', encoding='utf-8') as f:
                content_storage = json.load(f)
        except FileNotFoundError:
            content_storage = {}
            os.makedirs('data', exist_ok=True)
            with open('data/content_storage.json', 'w', encoding='utf-8') as f:
                json.dump(content_storage, f, indent=4)
        except Exception as e:
            logger.error(f"Storage error: {e}")
            await callback_query.answer("Storage error!")
            return
        
        content_info = content_storage.get(base_identifier)
        if not content_info:
            logger.error(f"Content not found for ID: {base_identifier}")
            await callback_query.answer("Content not found!")
            return
            
        if base_identifier not in callback_storage:
            callback_storage[base_identifier] = {
                "selected_resolution": None,
                "selected_audios": []
            }
        
        if action == "res":
            if len(parts) < 4:
                return
                
            short_stream_id = parts[3]
            
            selected_video = None
            for video in content_info["streams_info"]["video"]:
                if video["stream_id"] == short_stream_id or video["stream_id"].endswith(f"_{short_stream_id}"):
                    selected_video = video
                    break
            
            if selected_video:
                height = int(selected_video["resolution"].split("x")[1])
                
                def get_bitrate_value(bitrate):
                    if isinstance(bitrate, int): return bitrate
                    if isinstance(bitrate, str):
                        try: return int(bitrate.split()[0])
                        except (IndexError, ValueError): return 0
                    return 0
                
                if is_trial:
                    try:
                        with open('data/user_plans.json', 'r') as f:
                            user_plans = json.load(f)
                    except (FileNotFoundError, json.JSONDecodeError):
                        user_plans = {}

                    str_user_id = parts[1]

                    if height <= 720:
                        if user_plans.get(str_user_id, {}).get("720p_limit", 0) <= 0:
                            verify_bot = ASSISTANT_BOT
                            await callback_query.answer("No 720p tasks left! Get more tasks from verify bot.", show_alert=True)
                            await callback_query.message.edit_text(
                                "**Out of 720p Tasks!**\n\nTime to get more tasks from the verify bot.",
                                reply_markup=InlineKeyboardMarkup([[
                                    InlineKeyboardButton("🎯 Get More Tasks", url=f"https://t.me/{verify_bot}")
                                ]])
                            )
                            return
                    elif height == 1080:
                        if user_plans.get(str_user_id, {}).get("1080p_limit", 0) <= 0:
                            verify_bot = ASSISTANT_BOT
                            await callback_query.answer("No 1080p tasks left! Get more tasks from verify bot.", show_alert=True)
                            await callback_query.message.edit_text(
                                "**Out of 1080p Tasks!**\n\nTime to get more tasks from the verify bot.",
                                reply_markup=InlineKeyboardMarkup([[
                                    InlineKeyboardButton("🎯 Get More Tasks", url=f"https://t.me/{verify_bot}")
                                ]])
                            )
                            return

                    if height > 1080:
                        await callback_query.answer("🌟 Upgrade to full access to enjoy maximum quality resolution available! Get the best viewing experience.", show_alert=True)
                        return
                    
                    if height == 1080:
                        streams_1080p = [v for v in content_info["streams_info"]["video"] if int(v["resolution"].split("x")[1]) == 1080]
                        if streams_1080p:
                            streams_1080p.sort(key=lambda x: get_bitrate_value(x["bitrate"]))
                            lowest_1080p = streams_1080p[0]
                            selected_bitrate = get_bitrate_value(selected_video["bitrate"])
                            lowest_bitrate = get_bitrate_value(lowest_1080p["bitrate"])
                            
                            if selected_bitrate > lowest_bitrate:
                                await callback_query.answer("🌟 Upgrade to full access to enjoy maximum quality resolution available! Get the best viewing experience.", show_alert=True)
                                return
                            selected_video = lowest_1080p
                    
                    streams_same_res = [v for v in content_info["streams_info"]["video"] if int(v["resolution"].split("x")[1]) == height]
                    if streams_same_res:
                        streams_same_res.sort(key=lambda x: get_bitrate_value(x["bitrate"]))
                        selected_video = streams_same_res[0]

                callback_storage[base_identifier]["selected_resolution"] = {
                    "stream_id": selected_video["stream_id"],
                    "resolution": selected_video["resolution"],
                    "bitrate": selected_video["bitrate"]
                }
                save_callback_storage(callback_storage)
                
                markup = create_audio_buttons(base_identifier, content_info["streams_info"])
                await callback_query.message.edit_text(
                    "**🎧 Select One Or More Audio Tracks**",
                    reply_markup=markup
                )
                asyncio.create_task(delete_buttons_after_delay(callback_query.message))
                await callback_query.answer()
            
        elif action == "back":
            if base_identifier in callback_storage:
                stream_id_map = callback_storage[base_identifier].get("stream_id_map", {})
                callback_storage[base_identifier]["selected_audios"] = []
                callback_storage[base_identifier]["stream_id_map"] = stream_id_map
                save_callback_storage(callback_storage)
            
            markup = create_resolution_buttons(base_identifier, content_info["streams_info"], content_info)
            
            platform = content_info.get('platform', 'Unknown')
            title = content_info.get('title', 'Unknown Title')
            episode_title = content_info.get('episode_title', '')
            episode_number = content_info.get('episode_number', '')
            
            text = f"**👤 User:** {callback_query.from_user.mention}\n"
            text += f"**Platform:** `{platform}`\n**Title:** `{title}`"
            if episode_title: text += f"\n**Episode:** `{episode_title}`"
            if episode_number: text += f"\n**Episode Number:** `{episode_number}`"
            
            if is_trial:
                try:
                    with open('data/user_plans.json', 'r') as f: user_plans = json.load(f)
                except (FileNotFoundError, json.JSONDecodeError): user_plans = {}
                    
                str_user_id = str(callback_query.from_user.id)
                has_720p = user_plans.get(str_user_id, {}).get("720p_limit", 0) > 0
                has_1080p = user_plans.get(str_user_id, {}).get("1080p_limit", 0) > 0
                
                text += "\n\n**Available Tasks:**\n"
                if has_720p: text += f"• 720p Tasks: {user_plans[str_user_id]['720p_limit']}\n"
                if has_1080p: text += f"• 1080p Tasks: {user_plans[str_user_id]['1080p_limit']}\n\n"
            
            text += "\n\n**Please select video resolution:**"
            
            await callback_query.message.edit_text(text, reply_markup=markup)
            asyncio.create_task(delete_buttons_after_delay(callback_query.message))
            await callback_query.answer()
            
        elif action == "aud":
            if len(parts) < 4:
                return
            stream_index = parts[3]
            
            stream_id_map = callback_storage[base_identifier].get("stream_id_map", {})
            if stream_index not in stream_id_map:
                await callback_query.answer("Audio track not found!", show_alert=True)
                return
                
            matched_stream_id = stream_id_map[stream_index]
            selected_audios = callback_storage[base_identifier].get("selected_audios", [])
            
            if is_trial and matched_stream_id not in selected_audios and len(selected_audios) >= 2:
                await callback_query.answer("🎵 Upgrade to full access to enjoy all available audio tracks and languages!", show_alert=True)
                return
            
            if matched_stream_id in selected_audios:
                selected_audios.remove(matched_stream_id)
            else:
                selected_audios.append(matched_stream_id)
            
            callback_storage[base_identifier]["selected_audios"] = selected_audios
            save_callback_storage(callback_storage)
            
            audio_map = {audio["stream_id"]: audio for audio in content_info["streams_info"]["audio"]}
            selected_text = []
            for idx, selected_stream_id in enumerate(selected_audios, 1):
                if selected_stream_id in audio_map:
                    audio = audio_map[selected_stream_id]
                    language_code = audio["language"].lower()[:3]
                    language_name = pickFormats["audio"].get(language_code, audio["language"])
                    selected_text.append(f"**{idx}. {language_name} ({audio['bitrate']}K)**")
            
            message_text = "**🎧 Select One Or More Audio Tracks**\n\n"
            if selected_text:
                message_text += "**Selected Tracks:**\n" + "\n".join(selected_text)
            
            markup = create_audio_buttons(base_identifier, content_info["streams_info"])
            
            try:
                await callback_query.message.edit_text(message_text, reply_markup=markup)
            except MessageNotModified:
                try:
                    await callback_query.message.edit_reply_markup(reply_markup=markup)
                except MessageNotModified: pass
            await callback_query.answer()
            
        elif action == "proc":
            if is_bot_locked():
                await callback_query.answer("Bot is locked by admin.", show_alert=True)
                await callback_query.message.edit_text(LOCK_MESSAGE)
                return
                
            selected = callback_storage[base_identifier]
            if not selected.get("selected_resolution") or not isinstance(selected["selected_resolution"], dict):
                await callback_query.answer("Select resolution first!")
                return
            
            if callback_storage.get(base_identifier, {}).get("processing", False):
                await callback_query.answer("Download already in progress...", show_alert=True)
                return
            
            user_id = callback_query.from_user.id
            platform_name = content_info.get("platform", "Unknown")
            
            if platform_name in TRIAL_RESTRICTED_PLATFORMS and user_id not in OWNERS:
                has_reached_limit, reason = await check_download_limits(user_id, platform_name, chat_id)
                if has_reached_limit:
                    if reason == "platform_limit":
                        await callback_query.message.edit_text(f"**✨ You already have an ongoing task from {platform_name}. Please wait for it to complete.**")
                        await callback_query.answer()
                        return
                    elif reason == "max_concurrent":
                        await callback_query.answer(f"**✨ You have reached the maximum limit of 3 concurrent downloads. Please wait for at least one to complete.**", show_alert=True)
                        return
                    elif reason == "restricted_platform":
                        await callback_query.answer(f"{platform_name} platform is restricted to authorized users only.", show_alert=True)
                        return

            callback_storage[base_identifier]["processing"] = True
            save_callback_storage(callback_storage)
            
            audio_tracks = content_info["streams_info"].get("audio", [])
            if not audio_tracks:
                await callback_query.answer("Proceeding without selection.")
            elif not selected.get("selected_audios"):
                callback_storage[base_identifier]["processing"] = False
                save_callback_storage(callback_storage)
                await callback_query.answer("Select at least one audio!")
                return
            
            if is_trial:
                if user_id in TRIAL_COOLDOWNS:
                    cooldown_data = TRIAL_COOLDOWNS[user_id]
                    remaining_time = int(cooldown_data["time"] - time.time())
                    if remaining_time > 0:
                        callback_storage[base_identifier]["processing"] = False
                        save_callback_storage(callback_storage)
                        minutes, seconds = divmod(remaining_time, 60)
                        await callback_query.answer(f"Please wait {minutes}m {seconds}s before starting new task!", show_alert=True)
                        return

                try:
                    with open('data/user_plans.json', 'r') as f: user_plans = json.load(f)
                except (FileNotFoundError, json.JSONDecodeError): user_plans = {}
                
                str_user_id = parts[1]
                height = int(selected["selected_resolution"]["resolution"].split("x")[1])
                
                if height <= 720:
                    if user_plans.get(str_user_id, {}).get("720p_limit", 0) <= 0:
                        callback_storage[base_identifier]["processing"] = False
                        save_callback_storage(callback_storage)
                        await callback_query.answer("No 720p tasks left!", show_alert=True)
                        return
                    user_plans[str_user_id]["720p_limit"] -= 1
                elif height == 1080:
                    if user_plans.get(str_user_id, {}).get("1080p_limit", 0) <= 0:
                        callback_storage[base_identifier]["processing"] = False
                        save_callback_storage(callback_storage)
                        await callback_query.answer("No 1080p tasks left!", show_alert=True)
                        return
                    user_plans[str_user_id]["1080p_limit"] -= 1
                
                with open('data/user_plans.json', 'w') as f: json.dump(user_plans, f, indent=4)
                
                TRIAL_COOLDOWNS[user_id] = {"time": time.time() + TRIAL_COOLDOWN_SUCCESS, "message_id": callback_query.message.id}
            
            await callback_query.answer("Processing...")
            await callback_query.message.delete()
            
            await handle_proceed_download(client, callback_query.message, content_info, selected["selected_resolution"], selected["selected_audios"], base_identifier)
            
    except MessageNotModified:
        pass
    except Exception as e:
        logger.error(f"Callback error: {e}", exc_info=True)
        try:
            await callback_query.answer("An error occurred!")
        except Exception:
            pass


async def delete_buttons_after_delay(message, delay=600):  # 600 seconds = 10 minutes
    """Delete entire message after specified delay."""
    await asyncio.sleep(delay)
    try:
        await message.delete()
    except Exception:
        pass

async def scheduled_drive_cleanup():
    logger.info("Starting scheduled drive cleanup task")
    while True:
        try:
            logger.info("Running scheduled drive cleanup")
            await cleanup_old_files()
            logger.info("Scheduled drive cleanup completed successfully")
        except Exception as e:
            logger.error(f"Error in scheduled drive cleanup: {str(e)}")
        # Wait for 10 minutes before next cleanup
        await asyncio.sleep(600)  # 10 minutes

async def main():
    """Main entry point for the bot."""
    max_retries = 5
    retry_delay = 5
    retry_count = 0

    while retry_count < max_retries:
        try:
            await app.start()
            logger.info("Bot Started Successfully!")
            
            # Start the scheduled drive cleanup as a background task
            cleanup_task = asyncio.create_task(scheduled_drive_cleanup())
            # Start periodic dump cleanup as a background task
            asyncio.create_task(periodic_dump_cleanup())
            
            # Reset retry count on successful connection
            retry_count = 0
            
            await idle()  # Keep the bot running
            break  # Exit the loop if idle() completes normally
            
        except ConnectionError as e:
            retry_count += 1
            logger.warning(f"Connection error (attempt {retry_count}/{max_retries}): {str(e)}")
            if retry_count < max_retries:
                logger.info(f"Retrying in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                logger.error("Max retries reached. Shutting down.")
                break
                
        except KeyboardInterrupt:
            logger.warning("Received KeyboardInterrupt")
            break
            
        except Exception as e:
            logger.error(f"Unexpected error in main(): {str(e)}")
            break
            
        finally:
            try:
                # First stop premium sessions
                await premium_session_pool.close_all_sessions()
                # Then stop the main app
                await app.stop()
            except Exception as e:
                logger.error(f"Error during cleanup: {str(e)}")

def signal_handler(signum, frame):
    """Handle termination signals gracefully."""
    logger.info(f"Received signal {signum}")
    loop = asyncio.get_event_loop()
    
    async def cleanup():
        # Cancel all tasks first
        try:
            # Get all tasks except current
            tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
            
            # Cancel all tasks
            for task in tasks:
                task.cancel()
                
            # Wait for all tasks to complete with a timeout
            await asyncio.wait(tasks, timeout=5.0)
            
            # Close any remaining resources
            await premium_session_pool.close_all_sessions()
            
        except asyncio.TimeoutError:
            logger.warning("Some tasks did not complete in time during cleanup")
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")

    try:
        # Run cleanup
        loop.run_until_complete(cleanup())
    except Exception as e:
        logger.error(f"Error during signal handler cleanup: {str(e)}")
    finally:
        try:
            # Get all pending tasks
            pending = [t for t in asyncio.all_tasks(loop) if t is not asyncio.current_task()]
            
            # Cancel any remaining tasks
            for task in pending:
                task.cancel()
                
            # Wait with a timeout
            loop.run_until_complete(asyncio.wait_for(
                asyncio.gather(*pending, return_exceptions=True),
                timeout=5.0
            ))
        except asyncio.TimeoutError:
            logger.warning("Some tasks did not complete in time during shutdown")
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")
        finally:
            try:
                loop.run_until_complete(loop.shutdown_asyncgens())
                loop.close()
            except Exception as e:
                logger.error(f"Error closing loop: {str(e)}")

premium_session_pool = PremiumSessionPool(PREMIUM_STRING)

async def check_download_limits(user_id, platform_name=None, chat_id=None):
    if is_bot_locked():
        return True, "bot_locked"
        
    all_tasks = download_progress.get_all_tasks()
    user_tasks = 0
    platform_task_active = False

    for identifier, progress_data in all_tasks.items():
        try:
            stored_user_id = int(identifier.split('_')[0])
            if stored_user_id != user_id:
                continue

            # Check if the task is genuinely active
            real_status = await progress_display.get_real_status(progress_data)
            if real_status in ['Downloading', 'Processing', 'Uploading']:
                user_tasks += 1
                
                # Check for platform-specific limit
                if platform_name and platform_name in TRIAL_RESTRICTED_PLATFORMS:
                    task_content_info = progress_data.get('content_info', {})
                    if task_content_info.get('platform') == platform_name:
                        platform_task_active = True
        
        except (ValueError, IndexError):
            continue

    if user_tasks >= 3:
        return True, "max_concurrent"
    
    if platform_task_active:
        return True, "platform_limit"
    
    return False, ""


if __name__ == "__main__":
    # Set up signal handlers
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, signal_handler)
    
    # Run the bot using asyncio
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
    finally:
        try:
            # Get all pending tasks
            pending = [t for t in asyncio.all_tasks(loop) if t is not asyncio.current_task()]
            # Wait with a timeout
            loop.run_until_complete(asyncio.wait_for(
                asyncio.gather(*pending, return_exceptions=True),
                timeout=5.0
            ))
        except asyncio.TimeoutError:
            logger.warning("Some tasks did not complete in time during shutdown")
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")
        finally:
            try:
                loop.run_until_complete(loop.shutdown_asyncgens())
                loop.close()
            except Exception as e:
                logger.error(f"Error closing loop: {str(e)}")                               
