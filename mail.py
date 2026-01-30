import os
import logging
import requests
import random
import string
import asyncio
import sqlite3
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, MessageHandler, filters

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Bot configuration
BOT_TOKEN = "8410734690:AAEc2XpXsNlGuXoZYUoaMWpCDz-4MXBjBh4"
API_BASE = 'https://api.mail.tm'
ADMIN_USER_ID = 8128648817  # Admin ID
SUPPORT_USERNAME = "@tneh_owner"
REQUIRED_GROUP = "@TNEH_FREE_FILE"  # Your group username
GROUP_LINK = "https://t.me/+QkMGTxBpqftkNDU1"

# Store user data
user_sessions = {}
otp_messages = {}
group_members = set()  # Track users who have joined group
pending_verification = {}  # Users waiting for group verification

# Database setup
def init_db():
    conn = sqlite3.connect('bot_admin.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            created_at TEXT,
            email_count INTEGER DEFAULT 0,
            last_active TEXT,
            is_group_member INTEGER DEFAULT 0,
            verified_at TEXT
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS otp_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            email TEXT,
            otp_code TEXT,
            source TEXT,
            received_at TEXT,
            FOREIGN KEY (user_id) REFERENCES users (user_id)
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS broadcasts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            admin_id INTEGER,
            message TEXT,
            sent_count INTEGER DEFAULT 0,
            failed_count INTEGER DEFAULT 0,
            created_at TEXT
        )
    ''')
    conn.commit()
    conn.close()

def update_user_stats(user_id, username, first_name, last_name, is_member=False):
    conn = sqlite3.connect('bot_admin.db')
    cursor = conn.cursor()
    now = datetime.now().isoformat()

    cursor.execute('''
        INSERT OR REPLACE INTO users
        (user_id, username, first_name, last_name, created_at, last_active, is_group_member, verified_at)
        VALUES (?, ?, ?, ?, COALESCE((SELECT created_at FROM users WHERE user_id = ?), ?), ?, ?, COALESCE((SELECT verified_at FROM users WHERE user_id = ?), ?))
    ''', (user_id, username, first_name, last_name, user_id, now, now, 1 if is_member else 0, user_id, now if is_member else None))

    conn.commit()
    conn.close()

def log_otp(user_id, email, otp_code, source):
    conn = sqlite3.connect('bot_admin.db')
    cursor = conn.cursor()

    cursor.execute('''
        INSERT INTO otp_logs (user_id, email, otp_code, source, received_at)
        VALUES (?, ?, ?, ?, ?)
    ''', (user_id, email, otp_code, source, datetime.now().isoformat()))

    cursor.execute('''
        UPDATE users SET email_count = email_count + 1
        WHERE user_id = ?
    ''', (user_id,))

    conn.commit()
    conn.close()

def log_broadcast(admin_id, message, sent_count, failed_count):
    conn = sqlite3.connect('bot_admin.db')
    cursor = conn.cursor()
    
    cursor.execute('''
        INSERT INTO broadcasts (admin_id, message, sent_count, failed_count, created_at)
        VALUES (?, ?, ?, ?, ?)
    ''', (admin_id, message, sent_count, failed_count, datetime.now().isoformat()))
    
    conn.commit()
    conn.close()

async def check_group_membership(user_id, context):
    """Check if user has joined the required group"""
    try:
        chat_member = await context.bot.get_chat_member(
            chat_id=REQUIRED_GROUP,
            user_id=user_id
        )
        return chat_member.status in ['member', 'administrator', 'creator']
    except Exception as e:
        logger.error(f"Error checking group membership: {e}")
        return False

class EmailAccount:
    def __init__(self):
        self.id = None
        self.address = None
        self.password = None
        self.token = None
        self.created_at = None
        self.expires_at = None
        self.messages = []
        self.last_check = None
        self.message_count = 0
        self.otp_codes = []

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send welcome message and check group membership"""
    user_id = update.effective_user.id
    username = update.effective_user.username
    first_name = update.effective_user.first_name
    last_name = update.effective_user.last_name

    # Check if user is in group
    is_member = await check_group_membership(user_id, context)
    
    if not is_member:
        # Show join group message
        keyboard = [
            [InlineKeyboardButton("📢 Join Our Group", url=GROUP_LINK)],
            [InlineKeyboardButton("✅ I've Joined", callback_data="verify_membership")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        welcome_text = f"""
👋 *Welcome to TNEH Temporary Mail Bot!*

📢 *Important Notice:*
To use this bot, you must join our Telegram group first.

🔗 *Group Link:* {GROUP_LINK}
👥 *Group:* TNEH FREE FILE

*Why join our group?*
• Get updates on new features
• Receive support from community
• Stay informed about maintenance

*After joining, click "✅ I've Joined" below.*
        """
        
        await update.message.reply_text(
            welcome_text,
            parse_mode='Markdown',
            reply_markup=reply_markup,
            disable_web_page_preview=True
        )
        
        # Store user for verification
        pending_verification[user_id] = {
            'username': username,
            'first_name': first_name,
            'last_name': last_name
        }
        return
    
    # User is already member, proceed with email creation
    update_user_stats(user_id, username, first_name, last_name, True)
    
    # Create new session
    if user_id not in user_sessions:
        user_sessions[user_id] = EmailAccount()
    
    welcome_text = f"""
📧 *Welcome to TNEH Temporary Mail Bot!*

🤝 *Thank you for joining our group!*

I'll create a temporary email address for you that expires in 10 minutes.
You can extend the timer or create a new email anytime.

*Special Features:*
• 🔍 Auto-detects OTP codes from emails
• 📱 Quick OTP access buttons
• 📊 OTP history tracking
• ⚡ Fast email generation

*Getting your email ready...* 🚀

👨‍💻 *Support:* {SUPPORT_USERNAME}
        """
    
    await update.message.reply_text(welcome_text, parse_mode='Markdown')
    
    # Generate new email
    await create_new_email(update, context)

async def verify_membership(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Verify user has joined the group"""
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    
    if user_id not in pending_verification:
        await query.edit_message_text("❌ Session expired. Please use /start again.")
        return
    
    # Check membership
    is_member = await check_group_membership(user_id, context)
    
    if not is_member:
        keyboard = [
            [InlineKeyboardButton("📢 Join Our Group", url=GROUP_LINK)],
            [InlineKeyboardButton("✅ Check Again", callback_data="verify_membership")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            "❌ *You haven't joined our group yet!*\n\n"
            "Please join the group first, then click 'Check Again'.",
            parse_mode='Markdown',
            reply_markup=reply_markup,
            disable_web_page_preview=True
        )
        return
    
    # User has joined, proceed
    user_data = pending_verification.pop(user_id)
    update_user_stats(user_id, user_data['username'], user_data['first_name'], 
                      user_data['last_name'], True)
    
    if user_id not in user_sessions:
        user_sessions[user_id] = EmailAccount()
    
    # Update the message
    await query.edit_message_text(
        "✅ *Membership Verified!*\n\n"
        "Thank you for joining our group! Creating your temporary email... 🚀",
        parse_mode='Markdown'
    )
    
    # Generate new email
    await create_new_email(update, context)

async def create_new_email(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Create a new temporary email account."""
    user_id = update.effective_user.id

    try:
        # Show creating message
        if update.callback_query:
            await update.callback_query.edit_message_text("🔄 Creating your temporary email...")
        else:
            await update.message.reply_text("🔄 Creating your temporary email...")

        # Get available domain
        try:
            domain_response = requests.get(f"{API_BASE}/domains", timeout=15)
            if domain_response.status_code != 200:
                await send_message(update, "❌ Failed to get email domains. Please try again.")
                return

            domains_data = domain_response.json()
            if not domains_data.get('hydra:member'):
                await send_message(update, "❌ No email domains available. Please try again.")
                return

            domain = domains_data['hydra:member'][0]['domain']
        except requests.exceptions.RequestException:
            await send_message(update, "❌ Network error. Please check your connection and try again.")
            return

        # Generate random email and password
        username = ''.join(random.choices(string.ascii_lowercase + string.digits, k=10))
        address = f"{username}@{domain}"
        password = ''.join(random.choices(string.ascii_lowercase + string.digits, k=16))

        # Create account
        try:
            account_response = requests.post(
                f"{API_BASE}/accounts",
                json={"address": address, "password": password},
                timeout=15
            )

            if account_response.status_code != 201:
                if len(domains_data['hydra:member']) > 1:
                    domain = domains_data['hydra:member'][1]['domain']
                    address = f"{username}@{domain}"
                    account_response = requests.post(
                        f"{API_BASE}/accounts",
                        json={"address": address, "password": password},
                        timeout=15
                    )

                if account_response.status_code != 201:
                    await send_message(update, "❌ Failed to create email account. Please try again.")
                    return
        except requests.exceptions.RequestException:
            await send_message(update, "❌ Network error during account creation. Please try again.")
            return

        account_data = account_response.json()

        # Get token
        try:
            token_response = requests.post(
                f"{API_BASE}/token",
                json={"address": address, "password": password},
                timeout=15
            )

            if token_response.status_code != 200:
                await send_message(update, "❌ Failed to authenticate email account. Please try /start again.")
                return
        except requests.exceptions.RequestException:
            await send_message(update, "❌ Network error during authentication. Please try again.")
            return

        token_data = token_response.json()

        # Update user session
        user_sessions[user_id].id = account_data['id']
        user_sessions[user_id].address = address
        user_sessions[user_id].password = password
        user_sessions[user_id].token = token_data['token']
        user_sessions[user_id].created_at = datetime.now()
        user_sessions[user_id].expires_at = datetime.now() + timedelta(minutes=10)
        user_sessions[user_id].messages = []
        user_sessions[user_id].last_check = datetime.now()
        user_sessions[user_id].message_count = 0
        user_sessions[user_id].otp_codes = []

        # Start inbox checking
        asyncio.create_task(check_inbox_periodically(user_id, context))

        # Create keyboard
        keyboard = [
            [InlineKeyboardButton("📬 Check Inbox", callback_data="check_inbox")],
            [InlineKeyboardButton("🔄 Refresh", callback_data="refresh_inbox")],
            [InlineKeyboardButton("🔍 Check OTPs", callback_data="check_otps")],
            [
                InlineKeyboardButton("📧 New Email", callback_data="new_email"),
                InlineKeyboardButton("⏰ Extend Timer", callback_data="extend_timer")
            ],
            [
                InlineKeyboardButton("🗑️ Delete Email", callback_data="delete_email"),
                InlineKeyboardButton("ℹ️ Help", callback_data="show_help")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        # Send success message
        await send_message(
            update,
            f"✅ *TNEH Email Created Successfully!*\n\n"
            f"📧 *Your Email Address:*\n`{address}`\n\n"
            f"⏰ *Expires at:* {user_sessions[user_id].expires_at.strftime('%H:%M:%S')}\n"
            f"📬 *Messages:* 0\n"
            f"🔢 *OTPs Received:* 0\n\n"
            f"*Use this email for sign-ups and verification!* 📝\n\n"
            f"Click *🔍 Check OTPs* to quickly view verification codes.\n\n"
            f"👥 *Group:* {GROUP_LINK}\n"
            f"👨‍💻 *Support:* {SUPPORT_USERNAME}",
            reply_markup=reply_markup
        )

    except Exception as e:
        logger.error(f"Error creating email: {e}")
        await send_message(update, "❌ An error occurred. Please try /start again.")

def extract_otp_from_text(text):
    """Extract OTP codes from email text"""
    if not text:
        return None

    import re
    patterns = [
        r'\b\d{4,8}\b',
        r'code[\s:]*(\d{4,8})',
        r'OTP[\s:]*(\d{4,8})',
        r'verification[\s:]*(\d{4,8})',
        r'password[\s:]*(\d{4,8})',
        r'code[\s:]*is[\s:]*(\d{4,8})',
        r'[\s:]+(\d{4,8})[\s:]+',
    ]

    for pattern in patterns:
        matches = re.findall(pattern, text, re.IGNORECASE)
        if matches:
            return max(matches, key=len)

    standalone_codes = re.findall(r'\b\d{4,8}\b', text)
    if standalone_codes:
        for code in standalone_codes:
            if len(code) in [4, 5, 6, 8]:
                if not (len(code) == 4 and '202' in code):
                    return code

    return None

def extract_otp_source(subject, from_address):
    """Extract the source/service from email metadata."""
    source = "Unknown"
    service_patterns = {
        'google': 'Google',
        'facebook': 'Facebook',
        'twitter': 'Twitter',
        'instagram': 'Instagram',
        'whatsapp': 'WhatsApp',
        'telegram': 'Telegram',
        'amazon': 'Amazon',
        'paypal': 'PayPal',
        'microsoft': 'Microsoft',
        'apple': 'Apple',
        'github': 'GitHub',
        'linkedin': 'LinkedIn',
        'netflix': 'Netflix',
        'spotify': 'Spotify',
        'discord': 'Discord',
        'binance': 'Binance',
        'coinbase': 'Coinbase'
    }

    combined_text = (subject + ' ' + from_address).lower()

    for pattern, service in service_patterns.items():
        if pattern in combined_text:
            source = service
            break

    return source

async def check_inbox(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int = None) -> None:
    """Check for new emails in inbox."""
    if user_id is None:
        user_id = update.effective_user.id

    if user_id not in user_sessions or not user_sessions[user_id].token:
        await send_message(update, "❌ No active email session. Use /start to create one.")
        return

    try:
        session = user_sessions[user_id]

        if datetime.now() > session.expires_at:
            await send_message(update, "❌ Email session expired. Creating a new one...")
            await create_new_email(update, context)
            return

        if update.callback_query:
            await update.callback_query.edit_message_text("🔄 Checking your inbox...")

        try:
            headers = {"Authorization": f"Bearer {session.token}"}
            response = requests.get(f"{API_BASE}/messages", headers=headers, timeout=15)

            if response.status_code == 401:
                await send_message(update, "❌ Session expired. Creating new email...")
                await create_new_email(update, context)
                return

            if response.status_code != 200:
                await send_message(update, "❌ Failed to fetch inbox. Please try again.")
                return

            messages_data = response.json()
            messages = messages_data['hydra:member']
        except requests.exceptions.RequestException:
            await send_message(update, "❌ Network error while checking inbox. Please try again.")
            return

        await process_otp_messages(user_id, messages, session.messages)

        session.messages = messages
        session.last_check = datetime.now()

        if not messages:
            keyboard = [
                [InlineKeyboardButton("🔄 Refresh", callback_data="refresh_inbox")],
                [InlineKeyboardButton("🔍 Check OTPs", callback_data="check_otps")],
                [InlineKeyboardButton("📧 New Email", callback_data="new_email")],
                [InlineKeyboardButton("⏰ Extend Timer", callback_data="extend_timer")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)

            await send_message(
                update,
                "📭 *Inbox Empty*\n\nNo emails received yet. Waiting for incoming messages...\n\n"
                "Share your email address with services to receive emails here!\n\n"
                f"👥 *Don't forget to join our group:* {GROUP_LINK}",
                reply_markup=reply_markup
            )
        else:
            await display_inbox(update, messages, user_id)

    except Exception as e:
        logger.error(f"Error checking inbox: {e}")
        await send_message(update, "❌ Failed to check inbox. Please try again.")

async def process_otp_messages(user_id, current_messages, previous_messages):
    """Process new messages to extract OTP codes."""
    if not previous_messages:
        previous_messages = []

    previous_ids = {msg['id'] for msg in previous_messages}
    new_messages = [msg for msg in current_messages if msg['id'] not in previous_ids]

    for message in new_messages:
        try:
            session = user_sessions[user_id]
            headers = {"Authorization": f"Bearer {session.token}"}
            full_message_response = requests.get(
                f"{API_BASE}/messages/{message['id']}",
                headers=headers,
                timeout=15
            )

            if full_message_response.status_code == 200:
                full_message = full_message_response.json()
                message_text = full_message.get('text', '')

                otp_code = extract_otp_from_text(message_text)
                if otp_code:
                    source = extract_otp_source(
                        message.get('subject', ''),
                        message.get('from', {}).get('address', '')
                    )

                    otp_data = {
                        'code': otp_code,
                        'source': source,
                        'timestamp': datetime.now().isoformat(),
                        'email': session.address,
                        'message_id': message['id']
                    }

                    session.otp_codes.append(otp_data)
                    log_otp(user_id, session.address, otp_code, source)

                    if user_id not in otp_messages:
                        otp_messages[user_id] = []
                    otp_messages[user_id].append(otp_data)

                    logger.info(f"OTP detected for user {user_id}: {otp_code} from {source}")

        except Exception as e:
            logger.error(f"Error processing OTP message: {e}")

async def display_inbox(update: Update, messages: list, user_id: int) -> None:
    """Display inbox with messages."""
    session = user_sessions[user_id]
    new_count = len(messages) - session.message_count
    session.message_count = len(messages)

    message_text = f"📬 *Inbox* • {len(messages)} message(s)"
    if new_count > 0:
        message_text += f" • 🆕 {new_count} new"

    otp_count = len(session.otp_codes)
    if otp_count > 0:
        message_text += f" • 🔢 {otp_count} OTPs"

    message_text += "\n\n"

    for i, msg in enumerate(messages[:8]):
        from_addr = msg['from']['address']
        from_name = msg['from']['name'] or from_addr
        subject = msg['subject'] or "No Subject"
        received_time = datetime.fromisoformat(msg['createdAt'].replace('Z', '+00:00'))
        time_str = received_time.strftime('%H:%M')

        has_otp = any(otp['message_id'] == msg['id'] for otp in session.otp_codes)
        otp_indicator = " 🔢" if has_otp else ""

        message_text += f"*{i+1}. {subject}*{otp_indicator}\n"
        message_text += f"   👤 *From:* `{from_addr}`\n"
        message_text += f"   🕒 *Time:* {time_str}\n"

        if msg['intro']:
            preview = msg['intro'][:60] + "..." if len(msg['intro']) > 60 else msg['intro']
            message_text += f"   📝 *Preview:* {preview}\n"

        message_text += "\n"

    if len(messages) > 8:
        message_text += f"📋 *... and {len(messages) - 8} more messages*\n\n"

    message_text += f"⏰ *Session expires:* {session.expires_at.strftime('%H:%M:%S')}\n"
    message_text += f"👥 *Join our group:* {GROUP_LINK}"

    keyboard = []
    for i, msg in enumerate(messages[:5]):
        subject_preview = msg['subject'][:15] + "..." if len(msg['subject']) > 15 else msg['subject']
        has_otp = any(otp['message_id'] == msg['id'] for otp in session.otp_codes)
        otp_indicator = " 🔢" if has_otp else ""
        keyboard.append([InlineKeyboardButton(
            f"📧 {i+1}. {subject_preview}{otp_indicator}",
            callback_data=f"view_message_{msg['id']}"
        )])

    keyboard.extend([
        [InlineKeyboardButton("🔍 Check OTPs", callback_data="check_otps")],
        [InlineKeyboardButton("🔄 Refresh Inbox", callback_data="refresh_inbox")],
        [
            InlineKeyboardButton("📧 New Email", callback_data="new_email"),
            InlineKeyboardButton("⏰ Extend", callback_data="extend_timer")
        ],
        [InlineKeyboardButton("🗑️ Delete Email", callback_data="delete_email")]
    ])

    reply_markup = InlineKeyboardMarkup(keyboard)
    await send_message(update, message_text, reply_markup=reply_markup)

async def check_otps(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Display all received OTP codes."""
    user_id = update.effective_user.id

    if user_id not in user_sessions:
        await send_message(update, "❌ No active email session.")
        return

    session = user_sessions[user_id]

    if not session.otp_codes:
        keyboard = [
            [InlineKeyboardButton("📬 Check Inbox", callback_data="check_inbox")],
            [InlineKeyboardButton("🔄 Refresh", callback_data="refresh_inbox")],
            [InlineKeyboardButton("📧 New Email", callback_data="new_email")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await send_message(
            update,
            "🔍 *No OTPs Found*\n\n"
            "No verification codes have been detected yet.\n\n"
            "Make sure to:\n"
            "• Use your email for sign-ups\n"
            "• Request verification codes\n"
            "• Wait for emails to arrive\n\n"
            "OTPs will appear here automatically when detected!\n\n"
            f"👥 *Join our group for updates:* {GROUP_LINK}",
            reply_markup=reply_markup
        )
        return

    message_text = f"🔢 *OTP Codes Received* • {len(session.otp_codes)} total\n\n"

    for i, otp in enumerate(reversed(session.otp_codes[-10:])):
        timestamp = datetime.fromisoformat(otp['timestamp']).strftime('%H:%M:%S')
        message_text += f"*{i+1}. {otp['source']}*\n"
        message_text += f"   🎯 *Code:* `{otp['code']}`\n"
        message_text += f"   📧 *Email:* `{otp['email']}`\n"
        message_text += f"   🕒 *Time:* {timestamp}\n\n"

    if len(session.otp_codes) > 10:
        message_text += f"📋 *... and {len(session.otp_codes) - 10} more OTPs*\n\n"

    keyboard = []
    for i, otp in enumerate(reversed(session.otp_codes[-3:])):
        keyboard.append([InlineKeyboardButton(
            f"📋 {otp['source']}: {otp['code']}",
            callback_data=f"copy_otp_{otp['code']}"
        )])

    keyboard.extend([
        [InlineKeyboardButton("📬 Back to Inbox", callback_data="check_inbox")],
        [InlineKeyboardButton("🔄 Refresh OTPs", callback_data="refresh_inbox")],
        [InlineKeyboardButton("🗑️ Clear OTPs", callback_data="clear_otps")]
    ])

    reply_markup = InlineKeyboardMarkup(keyboard)
    await send_message(update, message_text, reply_markup=reply_markup)

async def view_message(update: Update, context: ContextTypes.DEFAULT_TYPE, message_id: str) -> None:
    """View a specific email message."""
    user_id = update.effective_user.id

    if user_id not in user_sessions:
        await send_message(update, "❌ No active email session.")
        return

    try:
        session = user_sessions[user_id]
        headers = {"Authorization": f"Bearer {session.token}"}

        await update.callback_query.edit_message_text("📧 Loading message content...")

        response = requests.get(f"{API_BASE}/messages/{message_id}", headers=headers, timeout=15)
        if response.status_code != 200:
            await send_message(update, "❌ Failed to fetch message.")
            return

        message_data = response.json()

        from_addr = message_data['from']['address']
        from_name = message_data['from']['name'] or from_addr
        subject = message_data['subject'] or "No Subject"
        received_time = datetime.fromisoformat(message_data['createdAt'].replace('Z', '+00:00'))

        has_otp = any(otp['message_id'] == message_id for otp in session.otp_codes)
        otp_info = ""
        if has_otp:
            otp_data = next(otp for otp in session.otp_codes if otp['message_id'] == message_id)
            otp_info = f"\n🎯 *OTP Detected:* `{otp_data['code']}` (*{otp_data['source']}*)\n"

        message_text = f"*{subject}* {'🔢' if has_otp else ''}\n\n"
        message_text += f"*From:* `{from_addr}`\n"
        message_text += f"*To:* `{session.address}`\n"
        message_text += f"*Date:* {received_time.strftime('%Y-%m-%d %H:%M')}\n"
        message_text += otp_info
        message_text += "\n--- Content ---\n\n"

        if message_data['text']:
            content = message_data['text']
            content = content.replace('\\n', '\n').replace('\\t', '    ')
            if len(content) > 1800:
                content = content[:1800] + "\n\n... (message truncated - too long)"
            message_text += content
        elif message_data['html']:
            message_text += "*HTML content available* (view in web version)\n\n"
            import re
            clean_text = re.sub('<[^<]+?>', '', message_data['html'][0])
            if clean_text.strip():
                preview = clean_text[:800] + "..." if len(clean_text) > 800 else clean_text
                message_text += f"*Text preview:*\n{preview}"
        else:
            message_text += "*No readable content available*"

        keyboard = [
            [InlineKeyboardButton("📬 Back to Inbox", callback_data="check_inbox")],
        ]

        if has_otp:
            keyboard[0].append(InlineKeyboardButton("📋 Copy OTP", callback_data=f"copy_otp_{otp_data['code']}"))

        keyboard.extend([
            [InlineKeyboardButton("🔍 Check OTPs", callback_data="check_otps")],
            [InlineKeyboardButton("🗑️ Delete This Email", callback_data=f"delete_message_{message_id}")],
        ])

        reply_markup = InlineKeyboardMarkup(keyboard)

        await send_message(update, message_text, reply_markup=reply_markup)

    except Exception as e:
        logger.error(f"Error viewing message: {e}")
        await send_message(update, "❌ Failed to load message content.")

async def extend_timer(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Extend the email session timer."""
    user_id = update.effective_user.id

    if user_id not in user_sessions:
        await send_message(update, "❌ No active email session.")
        return

    session = user_sessions[user_id]
    session.expires_at = datetime.now() + timedelta(minutes=10)

    keyboard = [
        [InlineKeyboardButton("📬 Check Inbox", callback_data="check_inbox")],
        [InlineKeyboardButton("🔍 Check OTPs", callback_data="check_otps")],
        [InlineKeyboardButton("🔄 Refresh", callback_data="refresh_inbox")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await send_message(
        update,
        f"✅ *Timer Extended!*\n\n"
        f"Your email `{session.address}`\n"
        f"Will now expire at: {session.expires_at.strftime('%H:%M:%S')}\n\n"
        f"You have 10 more minutes to receive emails!\n\n"
        f"👥 *Join our group:* {GROUP_LINK}",
        reply_markup=reply_markup
    )

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle inline keyboard button presses."""
    query = update.callback_query
    await query.answer()

    user_id = query.from_user.id

    if query.data == "verify_membership":
        await verify_membership(update, context)
    
    elif query.data == "refresh_inbox" or query.data == "check_inbox":
        await check_inbox(update, context, user_id)

    elif query.data == "new_email":
        await create_new_email(update, context)

    elif query.data == "extend_timer":
        await extend_timer(update, context)

    elif query.data == "delete_email":
        await delete_email(update, context)

    elif query.data == "show_help":
        await show_help(update, context)

    elif query.data == "check_otps":
        await check_otps(update, context)

    elif query.data == "clear_otps":
        await clear_otps(update, context)

    elif query.data.startswith("view_message_"):
        message_id = query.data.replace("view_message_", "")
        await view_message(update, context, message_id)

    elif query.data.startswith("copy_otp_"):
        otp_code = query.data.replace("copy_otp_", "")
        await context.bot.send_message(
            chat_id=user_id,
            text=f"📋 *OTP Code Ready to Copy:*\n\n`{otp_code}`\n\nClick and hold to copy the code.\n\n👥 *Join our group:* {GROUP_LINK}",
            parse_mode='Markdown'
        )

    elif query.data.startswith("delete_message_"):
        message_id = query.data.replace("delete_message_", "")
        await delete_message(update, context, message_id)

    elif query.data == "admin_panel":
        await show_admin_panel(update, context)

    elif query.data == "admin_refresh":
        await show_admin_panel(update, context)

    elif query.data == "admin_users":
        await admin_users(update, context)

    elif query.data == "admin_otp_logs":
        await admin_otp_logs(update, context)

    elif query.data == "admin_broadcast":
        await admin_broadcast_start(update, context)

async def clear_otps(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Clear OTP history for current session."""
    user_id = update.effective_user.id

    if user_id in user_sessions:
        user_sessions[user_id].otp_codes = []
        if user_id in otp_messages:
            otp_messages[user_id] = []

    keyboard = [
        [InlineKeyboardButton("📬 Check Inbox", callback_data="check_inbox")],
        [InlineKeyboardButton("🔍 Check OTPs", callback_data="check_otps")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await send_message(
        update,
        "✅ *OTP History Cleared*\n\nAll OTP codes have been cleared from this session.",
        reply_markup=reply_markup
    )

async def delete_email(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Delete current email and create new one."""
    user_id = update.effective_user.id

    if user_id in user_sessions:
        user_sessions[user_id].otp_codes = []
        if user_id in otp_messages:
            otp_messages[user_id] = []

        try:
            session = user_sessions[user_id]
            if session.id and session.token:
                requests.delete(
                    f"{API_BASE}/accounts/{session.id}",
                    headers={"Authorization": f"Bearer {session.token}"},
                    timeout=5
                )
        except:
            pass

    await send_message(update, "🗑️ Email deleted. Creating new one...")
    await create_new_email(update, context)

async def delete_message(update: Update, context: ContextTypes.DEFAULT_TYPE, message_id: str) -> None:
    """Delete a specific message."""
    user_id = update.effective_user.id

    if user_id not in user_sessions:
        await send_message(update, "❌ No active email session.")
        return

    try:
        session = user_sessions[user_id]
        headers = {"Authorization": f"Bearer {session.token}"}

        response = requests.delete(f"{API_BASE}/messages/{message_id}", headers=headers, timeout=5)

        if response.status_code in [200, 204]:
            await send_message(update, "✅ Message deleted successfully!")
            await check_inbox(update, context, user_id)
        else:
            await send_message(update, "❌ Failed to delete message.")

    except Exception as e:
        logger.error(f"Error deleting message: {e}")
        await send_message(update, "❌ Failed to delete message.")

async def check_inbox_periodically(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Periodically check inbox for new messages."""
    while user_id in user_sessions:
        try:
            session = user_sessions[user_id]

            if datetime.now() > session.expires_at:
                try:
                    await context.bot.send_message(
                        chat_id=user_id,
                        text="⏰ *Email Session Expired*\n\nYour temporary email has expired. Use /start to create a new one.\n\n👥 *Join our group:* " + GROUP_LINK,
                        parse_mode='Markdown'
                    )
                except:
                    pass
                break

            headers = {"Authorization": f"Bearer {session.token}"}
            response = requests.get(f"{API_BASE}/messages", headers=headers, timeout=10)

            if response.status_code == 200:
                messages_data = response.json()
                new_messages = messages_data['hydra:member']

                await process_otp_messages(user_id, new_messages, session.messages)

                if new_messages and len(new_messages) > session.message_count:
                    new_count = len(new_messages) - session.message_count
                    session.message_count = len(new_messages)

                    new_otp_count = len([otp for otp in session.otp_codes
                                       if otp['timestamp'] > (datetime.now() - timedelta(minutes=1)).isoformat()])

                    try:
                        keyboard = [[InlineKeyboardButton("📬 View Inbox", callback_data="check_inbox")]]
                        if new_otp_count > 0:
                            keyboard[0].append(InlineKeyboardButton("🔍 View OTPs", callback_data="check_otps"))

                        reply_markup = InlineKeyboardMarkup(keyboard)

                        notification_text = f"🎉 *You have {new_count} new message(s)!*"
                        if new_otp_count > 0:
                            notification_text += f"\n🔢 *{new_otp_count} new OTP code(s) detected!*"

                        notification_text += f"\n\n👥 *Join our group:* {GROUP_LINK}"

                        await context.bot.send_message(
                            chat_id=user_id,
                            text=notification_text,
                            parse_mode='Markdown',
                            reply_markup=reply_markup
                        )
                    except Exception as e:
                        logger.error(f"Error sending notification: {e}")

                session.messages = new_messages

        except Exception as e:
            logger.error(f"Error in periodic inbox check: {e}")

        await asyncio.sleep(20)

async def send_message(update: Update, text: str, reply_markup=None) -> None:
    """Helper function to send messages."""
    try:
        if update.callback_query:
            await update.callback_query.edit_message_text(
                text=text,
                parse_mode='Markdown',
                reply_markup=reply_markup,
                disable_web_page_preview=True
            )
        else:
            await update.message.reply_text(
                text=text,
                parse_mode='Markdown',
                reply_markup=reply_markup,
                disable_web_page_preview=True
            )
    except Exception as e:
        logger.error(f"Error sending message: {e}")

async def show_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show help message."""
    help_text = f"""
📧 *TNEH Temporary Mail Bot Help*

*Commands:*
/start - Create a new temporary email
/help - Show this help message
/admin - Admin panel (Admin only)

*How to Use:*
1. Click /start to create a temporary email
2. Use the generated email for sign-ups/verifications
3. Receive emails directly in this chat
4. OTP codes are automatically detected!

*OTP Features:*
• 🔍 Auto-detection of verification codes
• 📱 Quick OTP copy buttons
• 📊 OTP history tracking
• 🎯 Source identification (Google, Facebook, etc.)

*Important:*
• You must join our group to use this bot
• Emails expire after 10 minutes (can be extended)
• Support available through {SUPPORT_USERNAME}

*Buttons:*
• 📬 Check Inbox - View received emails
• 🔍 Check OTPs - View verification codes
• 🔄 Refresh - Manual inbox refresh
• 📧 New Email - Create new address
• ⏰ Extend Timer - Add 10 more minutes
• 🗑️ Delete Email - Remove current email

*Note:* Emails are automatically deleted when session expires.
Developed by **TNEH Team**

👥 *Group:* {GROUP_LINK}
👨‍💻 *Support:* {SUPPORT_USERNAME}
    """

    keyboard = [
        [InlineKeyboardButton("🚀 Create New Email", callback_data="new_email")],
        [InlineKeyboardButton("📬 Check Inbox", callback_data="check_inbox")],
        [InlineKeyboardButton("🔍 Check OTPs", callback_data="check_otps")],
        [InlineKeyboardButton("👥 Join Group", url=GROUP_LINK)]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await send_message(update, help_text, reply_markup=reply_markup)

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /help command."""
    await show_help(update, context)

# ==================== ADMIN PANEL FUNCTIONS ====================

async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show admin panel to authorized users."""
    user_id = update.effective_user.id

    if user_id != ADMIN_USER_ID:
        await update.message.reply_text("❌ Access denied. Admin only.")
        return

    await show_admin_panel(update, context)

async def show_admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Display admin panel with statistics."""
    user_id = update.effective_user.id

    if user_id != ADMIN_USER_ID:
        await send_message(update, "❌ Access denied. Admin only.")
        return

    # Get statistics
    conn = sqlite3.connect('bot_admin.db')
    cursor = conn.cursor()
    
    cursor.execute('SELECT COUNT(*) FROM users')
    total_users = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM users WHERE is_group_member = 1')
    group_members = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM users WHERE last_active > ?',
                  ((datetime.now() - timedelta(hours=1)).isoformat(),))
    active_sessions = cursor.fetchone()[0]
    
    cursor.execute('SELECT SUM(email_count) FROM users')
    total_emails = cursor.fetchone()[0] or 0
    
    cursor.execute('SELECT COUNT(*) FROM otp_logs')
    total_otps = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM users WHERE verified_at IS NOT NULL')
    verified_users = cursor.fetchone()[0]
    
    conn.close()

    admin_text = f"""
👑 *TNEH Admin Panel*

📊 *Bot Statistics:*
• 👥 Total Users: `{total_users}`
• ✅ Group Members: `{group_members}`
• 🔥 Active Sessions: `{active_sessions}`
• 📧 Emails Created: `{total_emails}`
• 🔢 OTPs Received: `{total_otps}`
• ✅ Verified Users: `{verified_users}`
• 💾 Memory Usage: `{len(user_sessions)} sessions`

📈 *User Status:*
• 🟢 Active in last hour: `{active_sessions}`
• 🔴 Not in group: `{total_users - group_members}`
• ⚪ Unverified: `{total_users - verified_users}`

🕒 *Last Updated:* {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

*Admin Commands:*
/broadcast - Send message to all users
/stats - Detailed statistics
/users - View user list
/otplogs - View OTP logs
    """

    keyboard = [
        [InlineKeyboardButton("📊 Refresh Stats", callback_data="admin_refresh")],
        [InlineKeyboardButton("👥 User List", callback_data="admin_users")],
        [InlineKeyboardButton("📋 OTP Logs", callback_data="admin_otp_logs")],
        [InlineKeyboardButton("📢 Broadcast", callback_data="admin_broadcast")],
        [InlineKeyboardButton("📈 Statistics", callback_data="admin_stats")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await send_message(update, admin_text, reply_markup=reply_markup)

async def admin_users(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show user list."""
    user_id = update.effective_user.id

    if user_id != ADMIN_USER_ID:
        await send_message(update, "❌ Access denied. Admin only.")
        return

    conn = sqlite3.connect('bot_admin.db')
    cursor = conn.cursor()
    cursor.execute('''
        SELECT user_id, username, first_name, last_name, email_count, last_active, is_group_member
        FROM users
        ORDER BY last_active DESC
        LIMIT 50
    ''')
    users = cursor.fetchall()
    conn.close()

    if not users:
        await send_message(update, "📝 No users found in database.")
        return

    user_text = f"👥 *Recent Users* ({len(users)} users)\n\n"

    for i, (user_id, username, first_name, last_name, email_count, last_active, is_member) in enumerate(users[:20]):
        user_text += f"*{i+1}. {first_name} {last_name}*\n"
        user_text += f"   👤 ID: `{user_id}`\n"
        if username:
            user_text += f"   📱 @{username}\n"
        user_text += f"   📧 Emails: {email_count}\n"
        user_text += f"   👥 In Group: {'✅' if is_member else '❌'}\n"
        user_text += f"   🕒 Last: {datetime.fromisoformat(last_active).strftime('%m/%d %H:%M')}\n\n"

    if len(users) > 20:
        user_text += f"📋 *... and {len(users) - 20} more users*"

    keyboard = [[InlineKeyboardButton("🔙 Back to Admin", callback_data="admin_panel")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await send_message(update, user_text, reply_markup=reply_markup)

async def admin_otp_logs(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show OTP logs."""
    user_id = update.effective_user.id

    if user_id != ADMIN_USER_ID:
        await send_message(update, "❌ Access denied. Admin only.")
        return

    conn = sqlite3.connect('bot_admin.db')
    cursor = conn.cursor()
    cursor.execute('''
        SELECT o.otp_code, o.source, o.email, o.received_at, u.first_name, u.last_name, u.is_group_member
        FROM otp_logs o
        LEFT JOIN users u ON o.user_id = u.user_id
        ORDER BY o.received_at DESC
        LIMIT 30
    ''')
    logs = cursor.fetchall()
    conn.close()

    if not logs:
        await send_message(update, "📝 No OTP logs found.")
        return

    log_text = f"📋 *Recent OTP Logs* ({len(logs)} entries)\n\n"

    for i, (otp_code, source, email, received_at, first_name, last_name, is_member) in enumerate(logs[:15]):
        user_info = f"{first_name} {last_name}" if first_name else "Unknown"
        time_str = datetime.fromisoformat(received_at).strftime('%m/%d %H:%M')
        group_status = "✅" if is_member else "❌"
        
        log_text += f"*{i+1}. {source}*\n"
        log_text += f"   🔢 Code: `{otp_code}`\n"
        log_text += f"   📧 Email: `{email}`\n"
        log_text += f"   👤 User: {user_info}\n"
        log_text += f"   👥 In Group: {group_status}\n"
        log_text += f"   🕒 Time: {time_str}\n\n"

    if len(logs) > 15:
        log_text += f"📋 *... and {len(logs) - 15} more logs*"

    keyboard = [[InlineKeyboardButton("🔙 Back to Admin", callback_data="admin_panel")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await send_message(update, log_text, reply_markup=reply_markup)

async def admin_broadcast_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Start broadcast process."""
    user_id = update.effective_user.id

    if user_id != ADMIN_USER_ID:
        await send_message(update, "❌ Access denied. Admin only.")
        return

    await send_message(update, 
        "📢 *Broadcast Message*\n\n"
        "Please use the command:\n"
        "`/broadcast your message here`\n\n"
        "Example:\n"
        "`/broadcast Hello users! New update available.`\n\n"
        "The message will be sent to all users in the database."
    )

async def admin_broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle broadcast message to all users."""
    user_id = update.effective_user.id

    if user_id != ADMIN_USER_ID:
        await send_message(update, "❌ Access denied. Admin only.")
        return

    if not context.args:
        await send_message(update, "❌ Usage: /broadcast <message>")
        return

    message = ' '.join(context.args)

    # Get all users from database
    conn = sqlite3.connect('bot_admin.db')
    cursor = conn.cursor()
    cursor.execute('SELECT user_id FROM users')
    users = cursor.fetchall()
    conn.close()

    sent_count = 0
    failed_count = 0

    broadcast_msg = await update.message.reply_text(f"📤 Broadcasting to {len(users)} users...")

    for (user_id,) in users:
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=f"📢 *Broadcast from TNEH Team*\n\n{message}\n\n👥 *Join our group:* {GROUP_LINK}\n👨‍💻 *Support:* {SUPPORT_USERNAME}",
                parse_mode='Markdown',
                disable_web_page_preview=True
            )
            sent_count += 1
            await asyncio.sleep(0.1)  # Rate limiting
        except Exception as e:
            failed_count += 1
            logger.error(f"Failed to send broadcast to {user_id}: {e}")

    # Log broadcast
    log_broadcast(ADMIN_USER_ID, message, sent_count, failed_count)

    await broadcast_msg.edit_text(
        f"📊 *Broadcast Results*\n\n"
        f"✅ Successfully sent: `{sent_count}` users\n"
        f"❌ Failed: `{failed_count}` users\n"
        f"📝 Total: `{len(users)}` users\n\n"
        f"📢 *Message sent:*\n{message[:100]}..." if len(message) > 100 else message
    )

async def admin_stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show detailed statistics."""
    user_id = update.effective_user.id

    if user_id != ADMIN_USER_ID:
        await send_message(update, "❌ Access denied. Admin only.")
        return

    conn = sqlite3.connect('bot_admin.db')
    cursor = conn.cursor()
    
    # General stats
    cursor.execute('SELECT COUNT(*) FROM users')
    total_users = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM users WHERE is_group_member = 1')
    group_members = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM otp_logs')
    total_otps = cursor.fetchone()[0]
    
    cursor.execute('SELECT SUM(email_count) FROM users')
    total_emails = cursor.fetchone()[0] or 0
    
    # Top OTP sources
    cursor.execute('''
        SELECT source, COUNT(*) as count
        FROM otp_logs
        GROUP BY source
        ORDER BY count DESC
        LIMIT 10
    ''')
    top_sources = cursor.fetchall()

    # Daily stats
    cursor.execute('''
        SELECT strftime('%Y-%m-%d', created_at) as date, COUNT(*) as count
        FROM users
        WHERE created_at > date('now', '-7 days')
        GROUP BY date
        ORDER BY date DESC
    ''')
    daily_users = cursor.fetchall()

    cursor.execute('''
        SELECT strftime('%Y-%m-%d', received_at) as date, COUNT(*) as count
        FROM otp_logs
        WHERE received_at > date('now', '-7 days')
        GROUP BY date
        ORDER BY date DESC
    ''')
    daily_otps = cursor.fetchall()

    # User activity
    cursor.execute('''
        SELECT 
            SUM(CASE WHEN last_active > datetime('now', '-1 hour') THEN 1 ELSE 0 END) as active_1h,
            SUM(CASE WHEN last_active > datetime('now', '-24 hours') THEN 1 ELSE 0 END) as active_24h,
            SUM(CASE WHEN last_active > datetime('now', '-7 days') THEN 1 ELSE 0 END) as active_7d
        FROM users
    ''')
    activity = cursor.fetchone()
    
    conn.close()

    stats_text = f"""
📈 *TNEH Detailed Statistics*

*General Stats:*
• 👥 Total Users: `{total_users}`
• ✅ Group Members: `{group_members}` ({group_members/total_users*100:.1f}%)
• 📧 Emails Created: `{total_emails}`
• 🔢 OTPs Received: `{total_otps}`

*User Activity:*
• 🟢 Active (1 hour): `{activity[0] or 0}`
• 🟡 Active (24 hours): `{activity[1] or 0}`
• 🟠 Active (7 days): `{activity[2] or 0}`

*Top OTP Sources:*
"""

    for source, count in top_sources:
        stats_text += f"• {source}: `{count}`\n"

    stats_text += "\n*Last 7 Days - New Users:*\n"
    for date, count in daily_users:
        stats_text += f"• {date}: `{count}` users\n"

    stats_text += "\n*Last 7 Days - OTP Activity:*\n"
    for date, count in daily_otps:
        stats_text += f"• {date}: `{count}` OTPs\n"

    stats_text += f"\n👥 *Required Group:* {GROUP_LINK}"
    stats_text += f"\n👨‍💻 *Support:* {SUPPORT_USERNAME}"

    keyboard = [[InlineKeyboardButton("🔙 Back to Admin", callback_data="admin_panel")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await send_message(update, stats_text, reply_markup=reply_markup)

def main() -> None:
    """Start the bot."""
    # Initialize database
    init_db()

    # Create application
    application = Application.builder().token(BOT_TOKEN).build()

    # Add handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("admin", admin_command))
    application.add_handler(CommandHandler("broadcast", admin_broadcast_command))
    application.add_handler(CommandHandler("stats", admin_stats_command))
    application.add_handler(CommandHandler("users", admin_users))
    application.add_handler(CommandHandler("otplogs", admin_otp_logs))
    application.add_handler(CallbackQueryHandler(handle_callback))

    # Start the bot
    print("🤖 TNEH Temporary Mail Bot is starting...")
    print("📧 Bot Token:", BOT_TOKEN[:10] + "..." + BOT_TOKEN[-5:])
    print("👑 Admin User:", ADMIN_USER_ID)
    print("👥 Required Group:", GROUP_LINK)
    print("👨‍💻 Support:", SUPPORT_USERNAME)
    print("🚀 Bot is now running. Press Ctrl+C to stop.")

    try:
        application.run_polling()
    except Exception as e:
        logger.error(f"Bot error: {e}")

if __name__ == '__main__':
    main()