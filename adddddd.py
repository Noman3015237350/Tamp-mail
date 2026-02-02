"""
TNEH EARNING BOT - COMPLETE FIXED VERSION
Version: 3.5.1
Date: 2024
Author: TNEH Team
Description: Complete earning bot with fixed ad watching system, real-time countdown,
             automatic payments, and comprehensive admin features.
"""

import logging
import json
import sqlite3
import time
import random
import string
import hashlib
import asyncio
import threading
import secrets
import re
from datetime import datetime, timedelta, date
from typing import Dict, List, Tuple, Optional, Any, Union
from decimal import Decimal, ROUND_HALF_UP
import pytz
from telegram import (
    Update, 
    InlineKeyboardButton, 
    InlineKeyboardMarkup, 
    ReplyKeyboardMarkup, 
    KeyboardButton,
    ReplyKeyboardRemove,
    ChatPermissions
)
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
    ConversationHandler,
    PicklePersistence
)
from telegram.constants import ParseMode

# ====================== CONFIGURATION ======================
BOT_TOKEN = "8570267673:AAEymwAZICr68eMO9KFK21Da2SIJmQwopLA"
ADMIN_ID = 6401269793

# Database file
DB_FILE = "tneh_bot_v3.db"
BACKUP_DIR = "backups/"

# Earning settings
EARN_PER_AD = Decimal('0.01')  # $0.01 per valid ad view
REFERRAL_BONUS = Decimal('0.05')  # $0.05 per referral
MIN_WITHDRAWAL = Decimal('5.00')  # Minimum $5 to withdraw
MIN_REFERRAL_WITHDRAWAL = Decimal('1.00')  # Minimum $1 from referrals

# Ad verification settings
AD_LINKS = [
    "https://otieu.com/4/10552104",
    "https://otieu.com/4/10552103",
    "https://otieu.com/4/10552102",
    "https://otieu.com/4/10552098",
    "https://otieu.com/4/10552096",
    "https://otieu.com/4/10552095",
    "https://otieu.com/4/10524009",
    "https://otieu.com/4/10521019",
    "https://otieu.com/4/10520164"
]

AD_WAIT_TIME = 30  # 30 seconds to wait on ad page
MAX_ADS_PER_DAY = 100  # Maximum ads per day per user
AD_COOLDOWN = 30  # 30 seconds between ads

# Gift code settings
GIFT_CODE_EXPIRY_DAYS = 30  # Gift codes expire after 30 days
GIFT_CODE_LENGTH = 12  # Length of gift codes
MAX_REDEEM_PER_USER = 5  # Maximum redeem codes per user

# Withdrawal methods
WITHDRAWAL_METHODS = {
    'bkash': {
        'name': 'bKash',
        'min_amount': Decimal('5.00'),
        'fee_percent': Decimal('1.5'),
        'processing_time': '24-48 hours'
    },
    'nagad': {
        'name': 'Nagad',
        'min_amount': Decimal('5.00'),
        'fee_percent': Decimal('1.5'),
        'processing_time': '24-48 hours'
    },
    'crypto': {
        'name': 'Crypto (USDT)',
        'min_amount': Decimal('10.00'),
        'fee_percent': Decimal('2.0'),
        'processing_time': '1-2 hours'
    }
}

# Security settings
MAX_LOGIN_ATTEMPTS = 5
SESSION_TIMEOUT = 3600  # 1 hour
IP_BLOCK_TIME = 3600  # 1 hour for IP blocking

# Conversation states
(
    AWAITING_WITHDRAWAL_METHOD,
    AWAITING_WITHDRAWAL_AMOUNT,
    AWAITING_WITHDRAWAL_DETAILS,
    AWAITING_ADMIN_ACTION,
    AWAITING_ADMIN_USER_ID,
    AWAITING_ADMIN_AMOUNT,
    AWAITING_ADMIN_REASON,
    AWAITING_GIFT_CODE_AMOUNT,
    AWAITING_BROADCAST_MESSAGE,
    AWAITING_MASS_PAYMENT_FILE,
    AWAITING_SUPPORT_MESSAGE
) = range(11)

# ====================== LOGGING SETUP ======================
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler('tneh_bot.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ====================== UTILITY FUNCTIONS ======================
def format_currency(amount: Decimal) -> str:
    """Format decimal as currency string"""
    return f"${amount.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP):.2f}"

def format_number(number: Union[int, float, Decimal]) -> str:
    """Format number with commas"""
    return f"{number:,}"

def generate_session_id() -> str:
    """Generate unique session ID"""
    return f"sess_{secrets.token_hex(16)}"

def validate_amount(amount_str: str) -> Tuple[bool, Decimal, str]:
    """Validate and parse amount string"""
    try:
        amount = Decimal(amount_str)
        if amount <= 0:
            return False, Decimal('0.00'), "Amount must be positive"
        if amount > Decimal('10000.00'):
            return False, Decimal('0.00'), "Amount too large (max $10,000)"
        return True, amount, "Valid"
    except:
        return False, Decimal('0.00'), "Invalid amount format"

def validate_phone_number(phone: str) -> bool:
    """Validate phone number format"""
    pattern = r'^(?:\+88|88)?(01[3-9]\d{8})$'
    return bool(re.match(pattern, phone))

def validate_email(email: str) -> bool:
    """Validate email format"""
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, email))

def get_current_time() -> str:
    """Get current time string"""
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def get_today_date() -> str:
    """Get today's date string"""
    return date.today().isoformat()

def calculate_withdrawal_fee(amount: Decimal, method: str) -> Decimal:
    """Calculate withdrawal fee"""
    fee_percent = WITHDRAWAL_METHODS[method]['fee_percent']
    fee = (amount * fee_percent) / Decimal('100')
    return fee.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

def get_net_withdrawal_amount(amount: Decimal, method: str) -> Decimal:
    """Calculate net amount after fee"""
    fee = calculate_withdrawal_fee(amount, method)
    return (amount - fee).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

# ====================== DATABASE SETUP ======================
def init_database():
    """Initialize the SQLite database with required tables"""
    try:
        # Create backup directory if not exists
        import os
        if not os.path.exists(BACKUP_DIR):
            os.makedirs(BACKUP_DIR)
        
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        # Enable foreign keys
        cursor.execute('PRAGMA foreign_keys = ON')
        
        # Users table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                referrer_id INTEGER,
                balance DECIMAL(10,2) DEFAULT 0.00,
                referral_balance DECIMAL(10,2) DEFAULT 0.00,
                total_earned DECIMAL(10,2) DEFAULT 0.00,
                total_withdrawn DECIMAL(10,2) DEFAULT 0.00,
                referrals_count INTEGER DEFAULT 0,
                join_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_ad_time TIMESTAMP,
                ads_today INTEGER DEFAULT 0,
                last_reset_date DATE DEFAULT CURRENT_DATE,
                account_status TEXT DEFAULT 'active',
                is_admin INTEGER DEFAULT 0,
                is_premium INTEGER DEFAULT 0,
                premium_expiry TIMESTAMP,
                total_ads_watched INTEGER DEFAULT 0,
                total_referral_earnings DECIMAL(10,2) DEFAULT 0.00,
                total_ad_earnings DECIMAL(10,2) DEFAULT 0.00,
                last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                language TEXT DEFAULT 'en',
                country TEXT,
                email TEXT,
                phone TEXT,
                two_factor_enabled INTEGER DEFAULT 0,
                security_question TEXT,
                security_answer_hash TEXT,
                failed_login_attempts INTEGER DEFAULT 0,
                last_failed_login TIMESTAMP,
                ip_address TEXT,
                user_agent TEXT,
                device_id TEXT,
                is_verified INTEGER DEFAULT 0,
                verification_date TIMESTAMP,
                notes TEXT,
                custom_fields TEXT DEFAULT '{}',
                INDEX idx_referrer (referrer_id),
                INDEX idx_status (account_status),
                INDEX idx_join_date (join_date)
            )
        ''')
        
        # Transactions table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                type TEXT CHECK(type IN (
                    'ad_earn', 'referral', 'withdrawal', 'admin_add', 
                    'admin_subtract', 'gift_code', 'premium_purchase',
                    'bonus', 'refund', 'correction', 'fee', 'reward',
                    'contest_prize', 'survey_earnings', 'task_completion'
                )),
                amount DECIMAL(10,2),
                description TEXT,
                status TEXT DEFAULT 'completed',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                reference_id TEXT,
                metadata TEXT DEFAULT '{}',
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
                INDEX idx_user_transactions (user_id, created_at),
                INDEX idx_transaction_type (type, status)
            )
        ''')
        
        # Withdrawals table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS withdrawals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                amount DECIMAL(10,2),
                net_amount DECIMAL(10,2),
                fee DECIMAL(10,2),
                method TEXT CHECK(method IN ('bkash', 'nagad', 'crypto')),
                details TEXT,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                processed_at TIMESTAMP,
                admin_notes TEXT,
                withdrawal_type TEXT DEFAULT 'balance',
                transaction_hash TEXT,
                receipt_url TEXT,
                rejected_reason TEXT,
                admin_id INTEGER,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
                FOREIGN KEY (admin_id) REFERENCES users(user_id) ON DELETE SET NULL,
                INDEX idx_withdrawal_status (status, created_at),
                INDEX idx_user_withdrawals (user_id, created_at)
            )
        ''')
        
        # Ad clicks tracking
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ad_clicks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                ad_url TEXT,
                ad_session_id TEXT UNIQUE,
                click_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                verified INTEGER DEFAULT 0,
                verified_at TIMESTAMP,
                earnings DECIMAL(10,2) DEFAULT 0.00,
                ip_hash TEXT,
                user_agent TEXT,
                device_info TEXT,
                duration INTEGER,
                is_fraud INTEGER DEFAULT 0,
                fraud_score DECIMAL(5,2) DEFAULT 0.00,
                country_code TEXT,
                browser TEXT,
                os TEXT,
                referrer_url TEXT,
                metadata TEXT DEFAULT '{}',
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
                INDEX idx_ad_verification (verified, verified_at),
                INDEX idx_user_ads (user_id, click_time),
                INDEX idx_fraud_detection (is_fraud, fraud_score)
            )
        ''')
        
        # Active ad sessions
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS active_ad_sessions (
                session_id TEXT PRIMARY KEY,
                user_id INTEGER,
                ad_url TEXT,
                start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                valid_until TIMESTAMP,
                completed INTEGER DEFAULT 0,
                reward_given INTEGER DEFAULT 0,
                earnings DECIMAL(10,2) DEFAULT 0.00,
                timer_started INTEGER DEFAULT 0,
                timer_start_time TIMESTAMP,
                last_ping TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                ping_count INTEGER DEFAULT 0,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
                INDEX idx_active_sessions (user_id, valid_until),
                INDEX idx_session_expiry (valid_until)
            )
        ''')
        
        # Gift codes table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS gift_codes (
                code TEXT PRIMARY KEY,
                amount DECIMAL(10,2),
                created_by INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expiry_date TIMESTAMP,
                max_uses INTEGER DEFAULT 1,
                current_uses INTEGER DEFAULT 0,
                is_active INTEGER DEFAULT 1,
                description TEXT,
                code_type TEXT DEFAULT 'standard',
                minimum_balance DECIMAL(10,2) DEFAULT 0.00,
                minimum_ads INTEGER DEFAULT 0,
                valid_for_users TEXT DEFAULT 'all',
                FOREIGN KEY (created_by) REFERENCES users(user_id) ON DELETE SET NULL,
                INDEX idx_gift_code_active (is_active, expiry_date),
                INDEX idx_gift_code_type (code_type)
            )
        ''')
        
        # Gift code redemptions
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS gift_code_redemptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                gift_code TEXT,
                redeemed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                amount DECIMAL(10,2),
                ip_address TEXT,
                device_id TEXT,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
                FOREIGN KEY (gift_code) REFERENCES gift_codes(code) ON DELETE CASCADE,
                INDEX idx_user_redemptions (user_id, redeemed_at),
                UNIQUE(user_id, gift_code)
            )
        ''')
        
        # Referral tracking
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS referrals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                referrer_id INTEGER,
                referred_id INTEGER UNIQUE,
                joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active INTEGER DEFAULT 1,
                earnings_generated DECIMAL(10,2) DEFAULT 0.00,
                level INTEGER DEFAULT 1,
                last_earnings_date TIMESTAMP,
                total_earnings DECIMAL(10,2) DEFAULT 0.00,
                FOREIGN KEY (referrer_id) REFERENCES users(user_id) ON DELETE CASCADE,
                FOREIGN KEY (referred_id) REFERENCES users(user_id) ON DELETE CASCADE,
                INDEX idx_referrer_stats (referrer_id, is_active),
                INDEX idx_referral_earnings (referrer_id, earnings_generated)
            )
        ''')
        
        # Admin actions log
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS admin_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                admin_id INTEGER,
                action TEXT,
                target_user_id INTEGER,
                details TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                ip_address TEXT,
                user_agent TEXT,
                severity TEXT DEFAULT 'info',
                metadata TEXT DEFAULT '{}',
                FOREIGN KEY (admin_id) REFERENCES users(user_id) ON DELETE SET NULL,
                FOREIGN KEY (target_user_id) REFERENCES users(user_id) ON DELETE SET NULL,
                INDEX idx_admin_actions (admin_id, created_at),
                INDEX idx_action_type (action, severity)
            )
        ''')
        
        # User statistics daily
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS daily_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                date DATE DEFAULT CURRENT_DATE,
                ads_watched INTEGER DEFAULT 0,
                earnings_today DECIMAL(10,2) DEFAULT 0.00,
                referrals_today INTEGER DEFAULT 0,
                login_count INTEGER DEFAULT 0,
                active_minutes INTEGER DEFAULT 0,
                tasks_completed INTEGER DEFAULT 0,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
                UNIQUE(user_id, date),
                INDEX idx_daily_stats (date, user_id)
            )
        ''')
        
        # User settings
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_settings (
                user_id INTEGER PRIMARY KEY,
                notifications_enabled INTEGER DEFAULT 1,
                email_notifications INTEGER DEFAULT 0,
                push_notifications INTEGER DEFAULT 0,
                language TEXT DEFAULT 'en',
                timezone TEXT DEFAULT 'UTC',
                currency TEXT DEFAULT 'USD',
                theme TEXT DEFAULT 'light',
                auto_start_ad INTEGER DEFAULT 0,
                ad_reminder INTEGER DEFAULT 1,
                referral_notifications INTEGER DEFAULT 1,
                withdrawal_notifications INTEGER DEFAULT 1,
                two_factor_auth INTEGER DEFAULT 0,
                security_level TEXT DEFAULT 'medium',
                data_sharing INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            )
        ''')
        
        # Support tickets
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS support_tickets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                ticket_id TEXT UNIQUE,
                subject TEXT,
                message TEXT,
                status TEXT DEFAULT 'open',
                priority TEXT DEFAULT 'medium',
                category TEXT,
                assigned_to INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                closed_at TIMESTAMP,
                resolution TEXT,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
                FOREIGN KEY (assigned_to) REFERENCES users(user_id) ON DELETE SET NULL,
                INDEX idx_ticket_status (status, priority),
                INDEX idx_user_tickets (user_id, created_at)
            )
        ''')
        
        # Ticket messages
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ticket_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticket_id INTEGER,
                user_id INTEGER,
                message TEXT,
                is_admin INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                attachments TEXT DEFAULT '[]',
                read_status INTEGER DEFAULT 0,
                FOREIGN KEY (ticket_id) REFERENCES support_tickets(id) ON DELETE CASCADE,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
                INDEX idx_ticket_conversation (ticket_id, created_at)
            )
        ''')
        
        # Contests and promotions
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS contests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                description TEXT,
                start_date TIMESTAMP,
                end_date TIMESTAMP,
                prize_pool DECIMAL(10,2),
                status TEXT DEFAULT 'upcoming',
                rules TEXT,
                winners_count INTEGER DEFAULT 1,
                entry_requirements TEXT DEFAULT '{}',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_contest_dates (start_date, end_date)
            )
        ''')
        
        # Contest participants
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS contest_participants (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                contest_id INTEGER,
                user_id INTEGER,
                joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                score DECIMAL(10,2) DEFAULT 0.00,
                rank INTEGER,
                prize DECIMAL(10,2),
                FOREIGN KEY (contest_id) REFERENCES contests(id) ON DELETE CASCADE,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
                UNIQUE(contest_id, user_id),
                INDEX idx_contest_ranking (contest_id, score DESC)
            )
        ''')
        
        # System logs
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS system_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                level TEXT,
                module TEXT,
                message TEXT,
                details TEXT,
                ip_address TEXT,
                user_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_system_logs (level, created_at),
                INDEX idx_module_logs (module, created_at)
            )
        ''')
        
        # Backup logs
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS backup_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                backup_type TEXT,
                filename TEXT,
                size_bytes INTEGER,
                status TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP,
                notes TEXT
            )
        ''')
        
        conn.commit()
        
        # Create admin user if not exists
        cursor.execute('SELECT * FROM users WHERE user_id = ?', (ADMIN_ID,))
        admin = cursor.fetchone()
        if not admin:
            cursor.execute('''
                INSERT INTO users (
                    user_id, username, first_name, last_name, 
                    is_admin, is_premium, is_verified, balance
                ) VALUES (?, 'TNEH', 'Admin', 'User', 1, 1, 1, 1000.00)
            ''', (ADMIN_ID,))
            
            # Create admin settings
            cursor.execute('''
                INSERT INTO user_settings (user_id) VALUES (?)
            ''', (ADMIN_ID,))
            
            logger.info(f"✅ Admin user created: {ADMIN_ID}")
        
        # Create default settings for existing users without settings
        cursor.execute('''
            INSERT OR IGNORE INTO user_settings (user_id)
            SELECT user_id FROM users WHERE user_id NOT IN (
                SELECT user_id FROM user_settings
            )
        ''')
        
        conn.commit()
        conn.close()
        
        logger.info("✅ Database initialized successfully with all tables")
        
        # Create database indexes for performance
        create_database_indexes()
        
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        raise

def create_database_indexes():
    """Create additional indexes for better performance"""
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        # Create additional indexes
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_users_balance ON users(balance DESC)",
            "CREATE INDEX IF NOT EXISTS idx_users_total_earned ON users(total_earned DESC)",
            "CREATE INDEX IF NOT EXISTS idx_users_join_date_desc ON users(join_date DESC)",
            "CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_withdrawals_date ON withdrawals(created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_ad_clicks_date ON ad_clicks(click_time DESC)",
            "CREATE INDEX IF NOT EXISTS idx_daily_stats_comprehensive ON daily_stats(date DESC, earnings_today DESC)",
            "CREATE INDEX IF NOT EXISTS idx_referrals_earnings ON referrals(earnings_generated DESC)",
            "CREATE INDEX IF NOT EXISTS idx_gift_codes_expiry ON gift_codes(expiry_date DESC)",
            "CREATE INDEX IF NOT EXISTS idx_support_tickets_date ON support_tickets(created_at DESC)",
        ]
        
        for index_sql in indexes:
            cursor.execute(index_sql)
        
        conn.commit()
        conn.close()
        logger.info("✅ Database indexes created successfully")
        
    except Exception as e:
        logger.error(f"Error creating database indexes: {e}")

def backup_database():
    """Create database backup"""
    try:
        import os
        import shutil
        from datetime import datetime
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_file = f"{BACKUP_DIR}tneh_bot_backup_{timestamp}.db"
        
        # Create backup
        shutil.copy2(DB_FILE, backup_file)
        
        # Log backup
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO backup_logs (backup_type, filename, size_bytes, status, notes)
            VALUES (?, ?, ?, ?, ?)
        ''', ('full', backup_file, os.path.getsize(backup_file), 'success', 'Automatic daily backup'))
        
        conn.commit()
        conn.close()
        
        logger.info(f"✅ Database backup created: {backup_file}")
        
        # Clean old backups (keep last 7 days)
        clean_old_backups()
        
        return True, backup_file
    except Exception as e:
        logger.error(f"Error backing up database: {e}")
        return False, str(e)

def clean_old_backups(days_to_keep: int = 7):
    """Clean old backup files"""
    try:
        import os
        import glob
        from datetime import datetime, timedelta
        
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        
        backup_files = glob.glob(f"{BACKUP_DIR}tneh_bot_backup_*.db")
        
        deleted_count = 0
        for backup_file in backup_files:
            try:
                # Extract date from filename
                filename = os.path.basename(backup_file)
                date_str = filename.replace('tneh_bot_backup_', '').replace('.db', '')
                file_date = datetime.strptime(date_str[:15], '%Y%m%d_%H%M%S')
                
                if file_date < cutoff_date:
                    os.remove(backup_file)
                    deleted_count += 1
            except:
                continue
        
        logger.info(f"✅ Cleaned {deleted_count} old backup files")
        return deleted_count
    except Exception as e:
        logger.error(f"Error cleaning old backups: {e}")
        return 0

# ====================== DATABASE FUNCTIONS ======================
def get_db_connection():
    """Get a database connection with row factory"""
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

def get_user(user_id: int) -> Optional[Dict]:
    """Get user information from database"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
        user = cursor.fetchone()
        
        if user:
            user_dict = dict(user)
            
            # Update last active time
            cursor.execute('''
                UPDATE users SET last_active = CURRENT_TIMESTAMP WHERE user_id = ?
            ''', (user_id,))
            
            # Reset daily ads counter if it's a new day
            last_reset = user_dict.get('last_reset_date')
            today = date.today().isoformat()
            
            if last_reset and last_reset != today:
                cursor.execute('''
                    UPDATE users 
                    SET ads_today = 0, last_reset_date = ?
                    WHERE user_id = ?
                ''', (today, user_id))
                user_dict['ads_today'] = 0
            
            conn.commit()
            
            # Get user settings
            cursor.execute('SELECT * FROM user_settings WHERE user_id = ?', (user_id,))
            settings = cursor.fetchone()
            if settings:
                user_dict['settings'] = dict(settings)
            
            conn.close()
            return user_dict
        conn.close()
        return None
    except Exception as e:
        logger.error(f"Error getting user {user_id}: {e}")
        return None

def get_user_with_stats(user_id: int) -> Optional[Dict]:
    """Get user information with detailed statistics"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get user info
        cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
        user = cursor.fetchone()
        
        if not user:
            conn.close()
            return None
        
        user_dict = dict(user)
        
        # Get today's stats
        cursor.execute('''
            SELECT * FROM daily_stats 
            WHERE user_id = ? AND date = DATE('now')
        ''', (user_id,))
        today_stats = cursor.fetchone()
        
        # Get weekly stats
        cursor.execute('''
            SELECT 
                SUM(ads_watched) as weekly_ads,
                SUM(earnings_today) as weekly_earnings,
                SUM(referrals_today) as weekly_referrals
            FROM daily_stats 
            WHERE user_id = ? AND date >= DATE('now', '-7 days')
        ''', (user_id,))
        weekly_stats = cursor.fetchone()
        
        # Get monthly stats
        cursor.execute('''
            SELECT 
                SUM(ads_watched) as monthly_ads,
                SUM(earnings_today) as monthly_earnings,
                SUM(referrals_today) as monthly_referrals
            FROM daily_stats 
            WHERE user_id = ? AND date >= DATE('now', '-30 days')
        ''', (user_id,))
        monthly_stats = cursor.fetchone()
        
        # Get referral stats
        cursor.execute('''
            SELECT 
                COUNT(*) as total_referrals,
                SUM(earnings_generated) as total_referral_earnings,
                SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) as active_referrals
            FROM referrals 
            WHERE referrer_id = ?
        ''', (user_id,))
        referral_stats = cursor.fetchone()
        
        # Get withdrawal stats
        cursor.execute('''
            SELECT 
                COUNT(*) as total_withdrawals,
                SUM(amount) as total_withdrawn_amount,
                SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END) as completed_withdrawals
            FROM withdrawals 
            WHERE user_id = ?
        ''', (user_id,))
        withdrawal_stats = cursor.fetchone()
        
        conn.close()
        
        # Combine all stats
        user_dict['stats'] = {
            'today': dict(today_stats) if today_stats else {
                'ads_watched': 0,
                'earnings_today': Decimal('0.00'),
                'referrals_today': 0
            },
            'weekly': dict(weekly_stats) if weekly_stats else {
                'weekly_ads': 0,
                'weekly_earnings': Decimal('0.00'),
                'weekly_referrals': 0
            },
            'monthly': dict(monthly_stats) if monthly_stats else {
                'monthly_ads': 0,
                'monthly_earnings': Decimal('0.00'),
                'monthly_referrals': 0
            },
            'referrals': dict(referral_stats) if referral_stats else {
                'total_referrals': 0,
                'total_referral_earnings': Decimal('0.00'),
                'active_referrals': 0
            },
            'withdrawals': dict(withdrawal_stats) if withdrawal_stats else {
                'total_withdrawals': 0,
                'total_withdrawn_amount': Decimal('0.00'),
                'completed_withdrawals': Decimal('0.00')
            }
        }
        
        return user_dict
    except Exception as e:
        logger.error(f"Error getting user stats {user_id}: {e}")
        return None

def create_user(user_id: int, username: str, first_name: str, last_name: str, referrer_id: int = None) -> bool:
    """Create a new user in database"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if user already exists
        cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
        if cursor.fetchone():
            conn.close()
            return True
        
        # Get IP address and user agent if available
        ip_address = "unknown"
        user_agent = "unknown"
        
        # Create new user
        cursor.execute('''
            INSERT INTO users (
                user_id, username, first_name, last_name, 
                referrer_id, is_admin, last_reset_date,
                ip_address, user_agent, join_date
            ) VALUES (?, ?, ?, ?, ?, ?, DATE('now'), ?, ?, CURRENT_TIMESTAMP)
        ''', (
            user_id, username, first_name, last_name, 
            referrer_id, 1 if user_id == ADMIN_ID else 0,
            ip_address, user_agent
        ))
        
        # Create user settings
        cursor.execute('''
            INSERT INTO user_settings (user_id) VALUES (?)
        ''', (user_id,))
        
        # Create today's stats entry
        cursor.execute('''
            INSERT OR IGNORE INTO daily_stats (user_id, date) VALUES (?, DATE('now'))
        ''', (user_id,))
        
        # If referred by someone, give referral bonus
        if referrer_id and referrer_id != user_id:
            # Update referrer's referrals count
            cursor.execute('''
                UPDATE users 
                SET referrals_count = referrals_count + 1,
                    referral_balance = referral_balance + ?,
                    total_referral_earnings = total_referral_earnings + ?,
                    total_earned = total_earned + ?
                WHERE user_id = ?
            ''', (REFERRAL_BONUS, REFERRAL_BONUS, REFERRAL_BONUS, referrer_id))
            
            # Add transaction record
            cursor.execute('''
                INSERT INTO transactions (
                    user_id, type, amount, description, reference_id
                ) VALUES (?, 'referral', ?, ?, ?)
            ''', (referrer_id, REFERRAL_BONUS, f'Referral bonus for user {user_id}', f'REF_{user_id}_{int(time.time())}'))
            
            # Add referral tracking
            cursor.execute('''
                INSERT INTO referrals (referrer_id, referred_id, earnings_generated)
                VALUES (?, ?, ?)
            ''', (referrer_id, user_id, REFERRAL_BONUS))
            
            # Update referrer's daily stats
            cursor.execute('''
                INSERT INTO daily_stats (user_id, referrals_today)
                VALUES (?, 1)
                ON CONFLICT(user_id, date) DO UPDATE SET
                referrals_today = referrals_today + 1
            ''', (referrer_id,))
            
            # Log referral
            logger.info(f"👥 User {user_id} referred by {referrer_id}")
        
        # Log user creation
        cursor.execute('''
            INSERT INTO system_logs (level, module, message, user_id)
            VALUES ('info', 'user', 'New user created', ?)
        ''', (user_id,))
        
        conn.commit()
        conn.close()
        
        logger.info(f"✅ User created: {user_id} (Name: {first_name} {last_name}, Referred by: {referrer_id})")
        return True
    except Exception as e:
        logger.error(f"Error creating user {user_id}: {e}")
        return False

def update_balance(
    user_id: int, 
    amount: Decimal, 
    transaction_type: str, 
    description: str = "", 
    reference_id: str = None,
    metadata: Dict = None
) -> bool:
    """Update user balance and add transaction record"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        logger.info(f"💰 Updating balance for user {user_id}: ${amount} ({transaction_type})")
        
        # Generate reference ID if not provided
        if not reference_id:
            reference_id = f"TX_{user_id}_{int(time.time())}_{secrets.token_hex(4)}"
        
        # Convert metadata to JSON string
        metadata_json = json.dumps(metadata) if metadata else '{}'
        
        # Check if it's a withdrawal (negative amount)
        if amount < 0:
            # Verify sufficient balance
            cursor.execute('SELECT balance FROM users WHERE user_id = ?', (user_id,))
            current_balance = cursor.fetchone()
            
            if not current_balance or Decimal(str(current_balance['balance'])) < abs(amount):
                conn.close()
                logger.error(f"Insufficient balance for user {user_id}")
                return False
            
            # Update balance (subtract)
            cursor.execute('''
                UPDATE users 
                SET balance = balance + ?
                WHERE user_id = ?
            ''', (amount, user_id))
            
        else:
            # Positive amount (deposit)
            if transaction_type == 'referral':
                # Update referral balance
                cursor.execute('''
                    UPDATE users 
                    SET referral_balance = referral_balance + ?,
                        total_referral_earnings = total_referral_earnings + ?,
                        total_earned = total_earned + ?
                    WHERE user_id = ?
                ''', (amount, amount, amount, user_id))
            else:
                # Update main balance
                cursor.execute('''
                    UPDATE users 
                    SET balance = balance + ?,
                        total_earned = total_earned + ?,
                        total_ad_earnings = total_ad_earnings + ?
                    WHERE user_id = ?
                ''', (amount, amount, amount, user_id))
        
        # Add transaction record
        cursor.execute('''
            INSERT INTO transactions (
                user_id, type, amount, description, 
                reference_id, metadata, status
            ) VALUES (?, ?, ?, ?, ?, ?, 'completed')
        ''', (user_id, transaction_type, amount, description, reference_id, metadata_json))
        
        # Update daily earnings for positive transactions
        if amount > 0 and transaction_type not in ['referral', 'withdrawal']:
            cursor.execute('''
                INSERT INTO daily_stats (user_id, earnings_today)
                VALUES (?, ?)
                ON CONFLICT(user_id, date) DO UPDATE SET
                earnings_today = earnings_today + ?
            ''', (user_id, amount, amount))
        
        # Log transaction
        cursor.execute('''
            INSERT INTO system_logs (level, module, message, user_id, details)
            VALUES ('info', 'transaction', 'Balance updated', ?, ?)
        ''', (user_id, f'Type: {transaction_type}, Amount: {amount}, Ref: {reference_id}'))
        
        conn.commit()
        conn.close()
        
        logger.info(f"✅ Balance updated for user {user_id}: {amount} ({transaction_type})")
        return True
    except Exception as e:
        logger.error(f"Error updating balance for user {user_id}: {e}")
        return False

def create_ad_session(user_id: int) -> Tuple[str, str]:
    """Create a new ad watching session with random ad URL"""
    try:
        # Select random ad URL
        ad_url = random.choice(AD_LINKS)
        session_id = f"ad_{user_id}_{int(time.time())}_{secrets.token_hex(8)}"
        valid_until = datetime.now() + timedelta(seconds=AD_WAIT_TIME + 300)  # 5 minutes extra grace
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Create active session
        cursor.execute('''
            INSERT INTO active_ad_sessions (
                session_id, user_id, ad_url, valid_until, earnings
            ) VALUES (?, ?, ?, ?, ?)
        ''', (session_id, user_id, ad_url, valid_until, EARN_PER_AD))
        
        # Record the ad click
        cursor.execute('''
            INSERT INTO ad_clicks (
                user_id, ad_url, ad_session_id, earnings, 
                click_time, country_code, browser
            ) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, 'BD', 'Telegram')
        ''', (user_id, ad_url, session_id, EARN_PER_AD))
        
        # Update user's ads_today counter
        cursor.execute('''
            UPDATE users 
            SET ads_today = ads_today + 1,
                total_ads_watched = total_ads_watched + 1,
                last_ad_time = CURRENT_TIMESTAMP
            WHERE user_id = ?
        ''', (user_id,))
        
        # Update daily stats
        cursor.execute('''
            INSERT INTO daily_stats (user_id, ads_watched)
            VALUES (?, 1)
            ON CONFLICT(user_id, date) DO UPDATE SET
            ads_watched = ads_watched + 1
        ''', (user_id,))
        
        # Log ad session creation
        cursor.execute('''
            INSERT INTO system_logs (level, module, message, user_id, details)
            VALUES ('info', 'ad', 'Ad session created', ?, ?)
        ''', (user_id, f'Session: {session_id}, URL: {ad_url}'))
        
        conn.commit()
        conn.close()
        
        logger.info(f"✅ Ad session created: {session_id} for user {user_id} (URL: {ad_url})")
        return session_id, ad_url
    except Exception as e:
        logger.error(f"Error creating ad session: {e}")
        return f"error_{int(time.time())}", AD_LINKS[0]

def verify_ad_session(session_id: str, user_id: int) -> Tuple[bool, Decimal, str]:
    """Verify if an ad session was completed successfully"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get session with lock
        cursor.execute('''
            SELECT * FROM active_ad_sessions 
            WHERE session_id = ? AND user_id = ?
        ''', (session_id, user_id))
        session = cursor.fetchone()
        
        if not session:
            conn.close()
            return False, Decimal('0.00'), "Session not found or expired"
        
        # Check if already rewarded
        if session['reward_given']:
            conn.close()
            return False, Decimal('0.00'), "Already rewarded for this session"
        
        # Check if timer was started
        if not session['timer_started']:
            conn.close()
            return False, Decimal('0.00'), "Timer not started. Please start the timer first."
        
        # Check if session is still valid
        if session['valid_until']:
            valid_until = datetime.fromisoformat(session['valid_until'].replace('Z', '+00:00'))
            if datetime.now() > valid_until:
                cursor.execute('DELETE FROM active_ad_sessions WHERE session_id = ?', (session_id,))
                conn.commit()
                conn.close()
                return False, Decimal('0.00'), "Session expired. Please start a new ad."
        
        # Check timer start time
        timer_start_time_str = session['timer_start_time']
        if not timer_start_time_str:
            conn.close()
            return False, Decimal('0.00'), "Timer not properly started"
        
        try:
            if '.' in timer_start_time_str:
                timer_start_time = datetime.strptime(timer_start_time_str, '%Y-%m-%d %H:%M:%S.%f')
            else:
                timer_start_time = datetime.strptime(timer_start_time_str, '%Y-%m-%d %H:%M:%S')
        except Exception as e:
            logger.error(f"Error parsing timer time: {e}")
            conn.close()
            return False, Decimal('0.00'), "Timer error. Please try again."
        
        now = datetime.now()
        time_elapsed = (now - timer_start_time).total_seconds()
        
        logger.info(f"⏰ Time check for session {session_id}:")
        logger.info(f"   Timer start: {timer_start_time}")
        logger.info(f"   Current time: {now}")
        logger.info(f"   Time elapsed: {time_elapsed:.1f} seconds")
        logger.info(f"   Required wait: {AD_WAIT_TIME} seconds")
        
        # Check if enough time has passed (with 2-second grace period)
        if time_elapsed >= (AD_WAIT_TIME - 2):
            # Mark as completed and give reward
            cursor.execute('''
                UPDATE active_ad_sessions 
                SET completed = 1, reward_given = 1,
                    last_ping = CURRENT_TIMESTAMP
                WHERE session_id = ?
            ''', (session_id,))
            
            # Update ad_clicks table
            cursor.execute('''
                UPDATE ad_clicks 
                SET verified = 1, verified_at = CURRENT_TIMESTAMP,
                    duration = ?
                WHERE ad_session_id = ? AND user_id = ?
            ''', (int(time_elapsed), session_id, user_id))
            
            # Give reward to user
            earnings = Decimal(str(session['earnings']))
            reference_id = f"AD_{session_id}"
            
            success = update_balance(
                user_id, 
                earnings, 
                'ad_earn', 
                f'Ad watch completed: {session_id[:8]}...',
                reference_id,
                {'session_id': session_id, 'ad_url': session['ad_url']}
            )
            
            if not success:
                conn.rollback()
                conn.close()
                return False, Decimal('0.00'), "Failed to update balance"
            
            # Log successful verification
            cursor.execute('''
                INSERT INTO system_logs (level, module, message, user_id, details)
                VALUES ('info', 'ad', 'Ad verified successfully', ?, ?)
            ''', (user_id, f'Session: {session_id}, Earnings: {earnings}'))
            
            conn.commit()
            conn.close()
            
            logger.info(f"✅ Ad session verified and rewarded: {session_id} - ${earnings}")
            return True, earnings, "Success! Ad verified and rewarded."
        else:
            remaining = max(0, AD_WAIT_TIME - time_elapsed)
            conn.close()
            logger.info(f"❌ Not enough time waited: {time_elapsed:.1f}s (need {AD_WAIT_TIME}s)")
            return False, Decimal('0.00'), f"Please wait {int(remaining)} more seconds"
            
    except Exception as e:
        logger.error(f"Error verifying ad session: {e}")
        return False, Decimal('0.00'), f"Technical error: {str(e)}"

def start_ad_timer(session_id: str, user_id: int) -> Tuple[bool, str]:
    """Start the timer for an ad session"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if session exists and belongs to user
        cursor.execute('''
            SELECT * FROM active_ad_sessions 
            WHERE session_id = ? AND user_id = ? AND timer_started = 0
        ''', (session_id, user_id))
        
        session = cursor.fetchone()
        if not session:
            conn.close()
            return False, "Session not found or timer already started"
        
        # Update session to start timer
        cursor.execute('''
            UPDATE active_ad_sessions 
            SET timer_started = 1, timer_start_time = CURRENT_TIMESTAMP,
                last_ping = CURRENT_TIMESTAMP
            WHERE session_id = ?
        ''', (session_id,))
        
        # Log timer start
        cursor.execute('''
            INSERT INTO system_logs (level, module, message, user_id, details)
            VALUES ('info', 'ad', 'Ad timer started', ?, ?)
        ''', (user_id, f'Session: {session_id}'))
        
        conn.commit()
        conn.close()
        
        logger.info(f"⏰ Timer started for session {session_id}")
        return True, "Timer started successfully"
    except Exception as e:
        logger.error(f"Error starting ad timer: {e}")
        return False, f"Error: {str(e)}"

def can_watch_ad(user_id: int) -> Tuple[bool, str]:
    """Check if user can watch an ad"""
    user_data = get_user(user_id)
    
    if not user_data:
        return False, "❌ Please use /start first to create your account."
    
    # Check if account is active
    if user_data.get('account_status', 'active') != 'active':
        return False, "❌ Your account is not active. Please contact admin."
    
    # Check daily ad limit
    ads_today = user_data.get('ads_today', 0)
    if ads_today >= MAX_ADS_PER_DAY:
        return False, f"❌ Daily ad limit reached ({MAX_ADS_PER_DAY} ads). Try again tomorrow!"
    
    # Check cooldown
    last_ad_time = user_data.get('last_ad_time')
    if last_ad_time:
        try:
            if '.' in last_ad_time:
                last_ad = datetime.strptime(last_ad_time, '%Y-%m-%d %H:%M:%S.%f')
            else:
                last_ad = datetime.strptime(last_ad_time, '%Y-%m-%d %H:%M:%S')
            
            time_since_last_ad = (datetime.now() - last_ad).total_seconds()
            
            # Check cooldown
            if time_since_last_ad < AD_COOLDOWN:
                remaining = int(AD_COOLDOWN - time_since_last_ad)
                return False, f"⏳ Please wait {remaining} seconds before watching another ad."
        except Exception as e:
            logger.error(f"Error parsing last_ad_time: {e}")
    
    return True, ""

# ====================== AD WATCHING LOGIC ======================
async def update_countdown(
    query, 
    context, 
    session_id: str, 
    start_time: datetime,
    message_id: int
):
    """Update countdown timer every second"""
    try:
        for i in range(AD_WAIT_TIME + 1):  # +1 to include 0
            # Check if session still exists
            if 'ad_sessions' not in context.chat_data:
                break
                
            session_data = context.chat_data['ad_sessions'].get(session_id)
            if not session_data:
                break
            
            # Check if verification already happened
            if session_data.get('verified', False):
                break
                
            elapsed = i
            remaining = AD_WAIT_TIME - elapsed
            
            # Calculate progress percentage
            progress = min(100, int((elapsed / AD_WAIT_TIME) * 100))
            progress_bars = int(progress / 10)
            progress_bar = "▰" * progress_bars + "▱" * (10 - progress_bars)
            
            # Prepare updated text
            current_time = datetime.now()
            completion_time = start_time + timedelta(seconds=AD_WAIT_TIME)
            
            text = f"""
*⏳ Ad Timer Running* 🎬

✅ *Timer started:* {start_time.strftime('%H:%M:%S')}
⏰ *Required wait:* {AD_WAIT_TIME} seconds
🎯 *Complete at:* {completion_time.strftime('%H:%M:%S')}

*📊 Progress:*
{progress_bar} {progress}%

*⏱️ Time remaining:* {remaining} seconds

*💡 Instructions:*
1. Keep the ad page open
2. Wait for timer to complete
3. Click *✅ Verify & Claim* after {AD_WAIT_TIME} seconds

*⚠️ Don't close this chat!*
"""
            
            # Update button text based on remaining time
            if remaining > 0:
                button_text = f"⏳ Please wait ({remaining}s)"
                callback_data = "waiting"
            else:
                button_text = "🎉 Time's up! Click to claim"
                callback_data = "waiting"
            
            keyboard = [
                [InlineKeyboardButton(button_text, callback_data=callback_data)],
                [InlineKeyboardButton("✅ Verify & Claim", callback_data=f"verify_{session_id}")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            # Edit message with updated countdown
            try:
                await context.bot.edit_message_text(
                    chat_id=query.message.chat_id,
                    message_id=message_id,
                    text=text,
                    reply_markup=reply_markup,
                    parse_mode=ParseMode.MARKDOWN
                )
            except Exception as e:
                # If message editing fails, break the loop
                if "Message is not modified" not in str(e):
                    logger.error(f"Error updating countdown message: {e}")
                break
            
            # Wait 1 second between updates
            await asyncio.sleep(1)
                
    except asyncio.CancelledError:
        logger.info(f"Countdown cancelled for session {session_id}")
    except Exception as e:
        logger.error(f"Error in countdown task: {e}")

async def handle_ad_timer_start(
    update: Update, 
    context: ContextTypes.DEFAULT_TYPE, 
    session_id: str
):
    """Handle timer start for ad watching"""
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    
    # Check if session exists
    if 'ad_sessions' not in context.chat_data or session_id not in context.chat_data['ad_sessions']:
        await query.edit_message_text(
            "❌ *Session expired*\n\nPlease start a new ad session.",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    session_data = context.chat_data['ad_sessions'][session_id]
    
    # Check if user owns this session
    if session_data['user_id'] != user_id:
        await query.answer("❌ This is not your ad session!", show_alert=True)
        return
    
    # Start timer in database
    success, message = start_ad_timer(session_id, user_id)
    
    if not success:
        await query.edit_message_text(
            f"❌ *Error starting timer*\n\n{message}",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    # Update message to show countdown
    start_time = datetime.now()
    session_data['timer_start'] = start_time
    session_data['verified'] = False
    session_data['timer_started'] = True
    
    # Edit message to show countdown
    completion_time = start_time + timedelta(seconds=AD_WAIT_TIME)
    
    text = f"""
*⏳ Ad Timer Started* ✅

✅ *Timer started at:* {start_time.strftime('%H:%M:%S')}
⏰ *Wait time required:* {AD_WAIT_TIME} seconds
🎯 *Complete at:* {completion_time.strftime('%H:%M:%S')}

*📊 Progress:*
▰▰▰▰▰▰▰▰▰▰ 0%

*⏱️ Time remaining:* {AD_WAIT_TIME} seconds

*🔗 Ad URL:* {session_data['ad_url']}

*💡 Instructions:*
1. Keep the ad page open
2. Wait for the timer to complete
3. Click *✅ Verify & Claim* after {AD_WAIT_TIME} seconds

*⚠️ Important:*
• Don't close the ad page
• Don't refresh the page
• Stay on this chat for timer updates
"""
    
    keyboard = [
        [InlineKeyboardButton(f"⏳ Waiting... ({AD_WAIT_TIME}s)", callback_data="waiting")],
        [InlineKeyboardButton("✅ Verify & Claim", callback_data=f"verify_{session_id}")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        text,
        reply_markup=reply_markup,
        parse_mode=ParseMode.MARKDOWN
    )
    
    # Start countdown updates
    countdown_task = asyncio.create_task(
        update_countdown(query, context, session_id, start_time, query.message.message_id)
    )
    
    # Store the task for cancellation if needed
    session_data['countdown_task'] = countdown_task

async def verify_ad_session_complete(
    update: Update, 
    context: ContextTypes.DEFAULT_TYPE, 
    session_id: str
):
    """Verify and complete ad session with automatic payment"""
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    
    # Cancel any running countdown task
    if 'ad_sessions' in context.chat_data and session_id in context.chat_data['ad_sessions']:
        session_data = context.chat_data['ad_sessions'][session_id]
        
        # Check if user owns this session
        if session_data['user_id'] != user_id:
            await query.answer("❌ This is not your ad session!", show_alert=True)
            return
        
        # Check if timer was started
        if not session_data.get('timer_started'):
            await query.edit_message_text(
                "❌ *Please start the timer first!*\n\n"
                "Click '⏰ Start Timer & Wait' button to begin countdown.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        # Cancel countdown task if it exists
        if 'countdown_task' in session_data and session_data['countdown_task']:
            session_data['countdown_task'].cancel()
    
    # Verify with database
    success, earnings, error_msg = verify_ad_session(session_id, user_id)
    
    if success:
        user_data = get_user_with_stats(user_id)
        
        if user_data:
            # Get today's earnings
            today_total = user_data['stats']['today'].get('earnings_today', Decimal('0.00'))
            ads_today = user_data.get('ads_today', 0)
            remaining_ads = MAX_ADS_PER_DAY - ads_today
            
            # Clean up session from memory
            if 'ad_sessions' in context.chat_data and session_id in context.chat_data['ad_sessions']:
                del context.chat_data['ad_sessions'][session_id]
            
            # Format earnings nicely
            earnings_formatted = format_currency(earnings)
            balance_formatted = format_currency(Decimal(str(user_data['balance'])))
            total_earned_formatted = format_currency(Decimal(str(user_data['total_earned'])))
            today_total_formatted = format_currency(today_total)
            
            success_text = f"""
✅ *Congratulations!* 🎉

🎬 *Ad successfully verified!*
💰 *Earned:* {earnings_formatted}

*📊 Account Summary:*
💵 *Balance:* {balance_formatted}
📈 *Total Earned:* {total_earned_formatted}
📅 *Today's Earnings:* {today_total_formatted}
🎯 *Ads Today:* {ads_today}/{MAX_ADS_PER_DAY}
🎮 *Remaining Today:* {remaining_ads} ads

*💡 Quick Tips:*
• Watch more ads to earn faster!
• Invite friends for bonus earnings!
• Check gift codes for free money!

*🔥 Keep going! You're doing great!*
"""
            
            await query.edit_message_text(
                success_text,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("▶️ Watch Another Ad", callback_data="watch_another_ad")],
                    [InlineKeyboardButton("💰 My Wallet", callback_data="my_wallet")],
                    [InlineKeyboardButton("👥 Invite Friends", callback_data="invite_friends")]
                ])
            )
        else:
            await query.edit_message_text(
                "❌ *Error retrieving user data*\n\n"
                "Please check your balance in the wallet section.",
                parse_mode=ParseMode.MARKDOWN
            )
    else:
        # Show error message
        if "Wait" in error_msg:
            error_text = f"""
❌ *Not Enough Time!* ⏰

{error_msg}

*⏳ Required wait time:* {AD_WAIT_TIME} seconds
*💡 Tip:* Keep the ad page open and wait patiently.
*🔄 Please try again after waiting.*
"""
        elif "Already rewarded" in error_msg:
            error_text = """
❌ *Already Claimed!* 🎯

This ad session has already been completed and rewarded.

*💡 What to do next:*
• Start a new ad session
• Check your balance
• Continue earning!
"""
        elif "Session not found" in error_msg or "Session expired" in error_msg:
            error_text = """
❌ *Session Expired!* ⏰

This ad session has expired or doesn't exist.

*💡 What to do next:*
1. Start a new ad session
2. Complete the wait time
3. Claim your reward

*🔥 Don't worry! Just start over.*
"""
        elif "Timer not started" in error_msg:
            error_text = """
❌ *Timer Not Started!* ⏰

Please click '⏰ Start Timer & Wait' button first.

*📋 Steps:*
1. Click ad link
2. Click 'Start Timer'
3. Wait {AD_WAIT_TIME} seconds
4. Click 'Verify & Claim'

*💡 The timer must be started to earn rewards.*
""".format(AD_WAIT_TIME=AD_WAIT_TIME)
        else:
            error_text = f"""
❌ *Verification Failed!* ⚠️

*Reason:* {error_msg}

*⏳ Required wait time:* {AD_WAIT_TIME} seconds
*💡 Please try again with a new ad.*

*🔧 If problem persists, contact support.*
"""
        
        await query.edit_message_text(
            error_text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Try Again", callback_data="try_again_ad")],
                [InlineKeyboardButton("📞 Contact Support", callback_data="contact_support")]
            ])
        )

# ====================== TELEGRAM BOT HANDLERS ======================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command"""
    user = update.effective_user
    user_id = user.id
    username = user.username or user.first_name or "User"
    first_name = user.first_name or ""
    last_name = user.last_name or ""
    
    # Log start command
    logger.info(f"🚀 /start command from {user_id} (@{username})")
    
    # Check for referral parameter
    referrer_id = None
    if context.args:
        try:
            referrer_id = int(context.args[0])
            if referrer_id == user_id:  # Prevent self-referral
                referrer_id = None
                logger.info(f"🔄 Self-referral prevented for {user_id}")
        except ValueError:
            logger.warning(f"Invalid referral parameter from {user_id}: {context.args[0]}")
        except Exception as e:
            logger.error(f"Error processing referral: {e}")
    
    # Create user if not exists
    user_data = get_user(user_id)
    if not user_data:
        create_user(user_id, username, first_name, last_name, referrer_id)
        welcome_msg = "🎉 *Welcome to TNEH EARNING BOT!*\nYour account has been created successfully!"
        
        # Send welcome bonus for new users
        try:
            update_balance(
                user_id,
                Decimal('0.10'),
                'bonus',
                'Welcome bonus for new user',
                f'WELCOME_{user_id}',
                {'type': 'welcome_bonus'}
            )
            welcome_msg += "\n\n🎁 *Bonus Alert:* You received $0.10 welcome bonus!"
        except Exception as e:
            logger.error(f"Error giving welcome bonus: {e}")
    else:
        welcome_msg = "👋 *Welcome back to TNEH EARNING BOT!*"
    
    # Update user's last active time
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE users SET last_active = CURRENT_TIMESTAMP WHERE user_id = ?
        ''', (user_id,))
        
        # Update daily login count
        cursor.execute('''
            INSERT INTO daily_stats (user_id, login_count)
            VALUES (?, 1)
            ON CONFLICT(user_id, date) DO UPDATE SET
            login_count = login_count + 1
        ''', (user_id,))
        
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Error updating user activity: {e}")
    
    # Send welcome message
    welcome_text = f"""
{welcome_msg}

🌟 *Earn Real Money by Watching Ads & Inviting Friends*
*Simple Tasks • Real Rewards • Secure Withdrawals*

💰 *Instant Earnings:*
• ${EARN_PER_AD} per ad view
• ${REFERRAL_BONUS} per referral
• Daily bonuses & gift codes

💳 *Withdrawal Methods:*
• bKash (Min: ${MIN_WITHDRAWAL})
• Nagad (Min: ${MIN_WITHDRAWAL})
• Crypto/USDT (Min: ${MIN_WITHDRAWAL})

⚡ *Features:*
• {MAX_ADS_PER_DAY} ads daily
• Real-time countdown timer
• Instant payments
• 24/7 support

👇 *Tap a button below to get started*
"""
    
    # Create main keyboard
    keyboard = [
        [KeyboardButton("▶️ Watch Ad & Earn"), KeyboardButton("💰 My Wallet")],
        [KeyboardButton("👥 Referral Program"), KeyboardButton("🎁 Gift Codes")],
        [KeyboardButton("📊 My Statistics"), KeyboardButton("🏆 Leaderboard")],
        [KeyboardButton("⚙️ Settings"), KeyboardButton("📞 Support")]
    ]
    
    # Add admin panel for admin
    user_data = get_user(user_id)
    if user_data and user_data.get('is_admin'):
        keyboard.append([KeyboardButton("🛠 Admin Panel")])
    
    reply_markup = ReplyKeyboardMarkup(
        keyboard, 
        resize_keyboard=True, 
        one_time_keyboard=False,
        input_field_placeholder="Choose an option..."
    )
    
    await update.message.reply_text(
        welcome_text,
        reply_markup=reply_markup,
        parse_mode=ParseMode.MARKDOWN,
        disable_web_page_preview=True
    )
    
    # Send tips message after 2 seconds
    await asyncio.sleep(2)
    
    tips_text = """
💡 *Quick Start Tips:*

1️⃣ *Watch Ads Daily:* Complete {MAX_ADS_PER_DAY} ads every day for maximum earnings
2️⃣ *Invite Friends:* Earn ${REFERRAL_BONUS} for every active referral
3️⃣ *Use Gift Codes:* Check for free money codes regularly
4️⃣ *Reach Minimum:* Need ${MIN_WITHDRAWAL} to withdraw to bKash/Nagad
5️⃣ *Stay Active:* Login daily for potential bonuses

🔥 *Start earning now by tapping "Watch Ad & Earn"!*
""".format(
    MAX_ADS_PER_DAY=MAX_ADS_PER_DAY,
    REFERRAL_BONUS=REFERRAL_BONUS,
    MIN_WITHDRAWAL=MIN_WITHDRAWAL
)
    
    await update.message.reply_text(
        tips_text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=reply_markup
    )

async def start_watching_ad(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start watching ad process with countdown"""
    user_id = update.effective_user.id
    username = update.effective_user.username or update.effective_user.first_name
    
    logger.info(f"🎬 User {user_id} (@{username}) starting ad watching")
    
    # Check if user can watch ad
    can_watch, message = can_watch_ad(user_id)
    if not can_watch:
        await update.message.reply_text(
            message,
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    # Create ad session with random ad URL
    session_id, ad_url = create_ad_session(user_id)
    
    # Store session data in context for later verification
    if 'ad_sessions' not in context.chat_data:
        context.chat_data['ad_sessions'] = {}
    
    context.chat_data['ad_sessions'][session_id] = {
        'user_id': user_id,
        'ad_url': ad_url,
        'timer_started': False,
        'verified': False,
        'countdown_task': None
    }
    
    # Format the ad message
    text = f"""
*📺 Watch Ad to Earn ${EARN_PER_AD}* 💰

*📋 Step-by-Step Guide:*

1️⃣ *Click the link below* to open the ad
2️⃣ *Click "⏰ Start Timer & Wait"* to begin countdown
3️⃣ *Wait for {AD_WAIT_TIME} seconds* on the ad page
4️⃣ *Click "✅ Verify & Claim"* after timer completes

🔗 *Ad Link:* {ad_url}

*⏳ Timer Duration:* {AD_WAIT_TIME} seconds
*🆔 Session ID:* `{session_id}`
*⏰ Start Time:* {datetime.now().strftime('%H:%M:%S')}

*⚠️ Important Rules:*
• Keep the ad page open for {AD_WAIT_TIME} seconds
• Don't refresh or close the page
• Timer starts when you click "Start Timer"
• You can only claim after timer completes
• One ad every {AD_COOLDOWN} seconds

*💡 Pro Tip:* Use a timer on your phone to track {AD_WAIT_TIME} seconds!
"""
    
    # Create inline keyboard
    keyboard = [
        [InlineKeyboardButton("🔗 Open Ad Link", url=ad_url)],
        [InlineKeyboardButton("⏰ Start Timer & Wait", callback_data=f"start_timer_{session_id}")],
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel_ad")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    try:
        message = await update.message.reply_text(
            text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.MARKDOWN,
            disable_web_page_preview=True
        )
        
        # Store message ID for later editing
        context.chat_data['ad_sessions'][session_id]['message_id'] = message.message_id
        
        logger.info(f"✅ Ad session started for user {user_id}: {session_id}")
        
    except Exception as e:
        logger.error(f"Error sending ad message: {e}")
        await update.message.reply_text(
            "❌ *Error starting ad session*\n\nPlease try again in a moment.",
            parse_mode=ParseMode.MARKDOWN
        )

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle all callback queries"""
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    data = query.data
    
    logger.info(f"📞 Callback from {user_id}: {data}")
    
    try:
        if data.startswith("start_timer_"):
            session_id = data.split("start_timer_", 1)[1]
            await handle_ad_timer_start(update, context, session_id)
            
        elif data.startswith("verify_"):
            session_id = data.split("_", 1)[1]
            await verify_ad_session_complete(update, context, session_id)
            
        elif data == "cancel_ad":
            # Clean up any existing sessions for this user
            if 'ad_sessions' in context.chat_data:
                user_sessions = [
                    sid for sid, sess in context.chat_data['ad_sessions'].items() 
                    if sess['user_id'] == user_id
                ]
                for sid in user_sessions:
                    session_data = context.chat_data['ad_sessions'][sid]
                    # Cancel countdown task if exists
                    if 'countdown_task' in session_data and session_data['countdown_task']:
                        session_data['countdown_task'].cancel()
                    del context.chat_data['ad_sessions'][sid]
            
            await query.edit_message_text(
                "❌ *Ad watching cancelled.*\n\n"
                "💡 You can start a new ad anytime by tapping 'Watch Ad & Earn'.",
                parse_mode=ParseMode.MARKDOWN
            )
            
        elif data == "watch_another_ad":
            # Send user back to start watching ad
            await query.edit_message_text(
                "🔄 *Starting new ad session...*",
                parse_mode=ParseMode.MARKDOWN
            )
            
            # Simulate start_watching_ad
            can_watch, message = can_watch_ad(user_id)
            if not can_watch:
                await query.edit_message_text(message, parse_mode=ParseMode.MARKDOWN)
                return
            
            session_id, ad_url = create_ad_session(user_id)
            
            if 'ad_sessions' not in context.chat_data:
                context.chat_data['ad_sessions'] = {}
            
            context.chat_data['ad_sessions'][session_id] = {
                'user_id': user_id,
                'ad_url': ad_url,
                'timer_started': False,
                'verified': False,
                'countdown_task': None
            }
            
            text = f"""
*📺 New Ad Session Started*

🔗 *Ad Link:* {ad_url}

Click *"⏰ Start Timer & Wait"* to begin!
"""
            
            keyboard = [
                [InlineKeyboardButton("🔗 Open Ad Link", url=ad_url)],
                [InlineKeyboardButton("⏰ Start Timer & Wait", callback_data=f"start_timer_{session_id}")],
                [InlineKeyboardButton("❌ Cancel", callback_data="cancel_ad")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await query.edit_message_text(
                text,
                reply_markup=reply_markup,
                parse_mode=ParseMode.MARKDOWN
            )
            
        elif data == "my_wallet":
            user_data = get_user_with_stats(user_id)
            if user_data:
                balance = format_currency(Decimal(str(user_data['balance'])))
                ref_balance = format_currency(Decimal(str(user_data.get('referral_balance', 0))))
                total_earned = format_currency(Decimal(str(user_data['total_earned'])))
                
                text = f"""
💰 *My Wallet*

*Main Balance:* {balance}
*Referral Balance:* {ref_balance}
*Total Earned:* {total_earned}

💳 *Withdrawal Methods Available:*
• bKash (Min: ${MIN_WITHDRAWAL})
• Nagad (Min: ${MIN_WITHDRAWAL})
• Crypto/USDT (Min: ${MIN_WITHDRAWAL})

💡 *Need help with withdrawal?* Contact support!
"""
                
                keyboard = [
                    [InlineKeyboardButton("💸 Withdraw Money", callback_data="withdraw_money")],
                    [InlineKeyboardButton("📊 View Statistics", callback_data="view_stats")],
                    [InlineKeyboardButton("🔙 Back to Menu", callback_data="back_to_menu")]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                
                await query.edit_message_text(text, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN)
                
        elif data == "invite_friends":
            user_data = get_user(user_id)
            referral_link = f"https://t.me/TNEH_EARNING_BOT?start={user_id}"
            
            text = f"""
👥 *Invite Friends & Earn*

💰 *Earn ${REFERRAL_BONUS} for every friend who joins and becomes active!*

🔗 *Your Referral Link:*
`{referral_link}`

📊 *Your Referral Stats:*
• Total Referrals: {user_data.get('referrals_count', 0)}
• Referral Balance: {format_currency(Decimal(str(user_data.get('referral_balance', 0))))}

📋 *How it works:*
1. Share your link with friends
2. They join using your link
3. When they become active, you get ${REFERRAL_BONUS}!
4. Unlimited earnings - no limit on referrals!

💡 *Tip:* Share on social media for more referrals!
"""
            
            keyboard = [
                [InlineKeyboardButton("📤 Share Link", switch_inline_query=f"Join TNEH Earning Bot and earn money! {referral_link}")],
                [InlineKeyboardButton("🔙 Back", callback_data="back_to_menu")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await query.edit_message_text(text, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN)
            
        elif data == "try_again_ad":
            await query.edit_message_text(
                "🔄 *Starting new ad session...*",
                parse_mode=ParseMode.MARKDOWN
            )
            
            can_watch, message = can_watch_ad(user_id)
            if not can_watch:
                await query.edit_message_text(message, parse_mode=ParseMode.MARKDOWN)
                return
            
            session_id, ad_url = create_ad_session(user_id)
            
            if 'ad_sessions' not in context.chat_data:
                context.chat_data['ad_sessions'] = {}
            
            context.chat_data['ad_sessions'][session_id] = {
                'user_id': user_id,
                'ad_url': ad_url,
                'timer_started': False,
                'verified': False,
                'countdown_task': None
            }
            
            text = f"""
*🔄 New Ad Session*

🔗 *Ad Link:* {ad_url}

Click *"⏰ Start Timer & Wait"* to begin!
"""
            
            keyboard = [
                [InlineKeyboardButton("🔗 Open Ad Link", url=ad_url)],
                [InlineKeyboardButton("⏰ Start Timer & Wait", callback_data=f"start_timer_{session_id}")],
                [InlineKeyboardButton("❌ Cancel", callback_data="cancel_ad")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await query.edit_message_text(
                text,
                reply_markup=reply_markup,
                parse_mode=ParseMode.MARKDOWN
            )
            
        elif data == "contact_support":
            text = """
📞 *Contact Support*

Need help? Our support team is here for you!

📋 *Before contacting support:*
1. Check the /help command
2. Read the instructions carefully
3. Make sure you followed all steps

💬 *To contact support:*
1. Use /support command
2. Describe your issue clearly
3. Include relevant details

⏰ *Response Time:* 24-48 hours
🎯 *Best time to contact:* 10 AM - 6 PM (GMT+6)

🔧 *Common issues solved quickly:*
• Ad verification problems
• Withdrawal questions
• Account issues
• Payment delays
"""
            
            keyboard = [
                [InlineKeyboardButton("📝 Open Support Ticket", callback_data="open_ticket")],
                [InlineKeyboardButton("📖 View FAQ", callback_data="view_faq")],
                [InlineKeyboardButton("🔙 Back", callback_data="back_to_menu")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await query.edit_message_text(text, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN)
            
        elif data == "back_to_menu":
            # Return to main menu
            user_data = get_user(user_id)
            
            text = """
🏠 *Main Menu*

Choose an option below:
"""
            
            keyboard = [
                [InlineKeyboardButton("▶️ Watch Ad & Earn", callback_data="watch_another_ad")],
                [InlineKeyboardButton("💰 My Wallet", callback_data="my_wallet")],
                [InlineKeyboardButton("👥 Invite Friends", callback_data="invite_friends")],
                [InlineKeyboardButton("📊 Statistics", callback_data="view_stats")],
                [InlineKeyboardButton("🎁 Gift Codes", callback_data="gift_codes")],
                [InlineKeyboardButton("⚙️ Settings", callback_data="settings_menu")]
            ]
            
            if user_data and user_data.get('is_admin'):
                keyboard.append([InlineKeyboardButton("🛠 Admin Panel", callback_data="admin_panel")])
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await query.edit_message_text(text, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN)
            
        elif data == "waiting":
            await query.answer("⏳ Please wait for the timer to complete!", show_alert=True)
            
        elif data == "view_stats":
            user_data = get_user_with_stats(user_id)
            if user_data:
                today = user_data['stats']['today']
                weekly = user_data['stats']['weekly']
                monthly = user_data['stats']['monthly']
                referrals = user_data['stats']['referrals']
                
                text = f"""
📊 *Your Statistics*

*📅 Today:*
• Ads Watched: {today.get('ads_watched', 0)}
• Earnings: {format_currency(today.get('earnings_today', Decimal('0.00')))}
• Referrals: {today.get('referrals_today', 0)}

*📆 Last 7 Days:*
• Ads Watched: {weekly.get('weekly_ads', 0)}
• Earnings: {format_currency(weekly.get('weekly_earnings', Decimal('0.00')))}
• Referrals: {weekly.get('weekly_referrals', 0)}

*📈 Last 30 Days:*
• Ads Watched: {monthly.get('monthly_ads', 0)}
• Earnings: {format_currency(monthly.get('monthly_earnings', Decimal('0.00')))}
• Referrals: {monthly.get('monthly_referrals', 0)}

*👥 Referral Stats:*
• Total Referrals: {referrals.get('total_referrals', 0)}
• Active Referrals: {referrals.get('active_referrals', 0)}
• Referral Earnings: {format_currency(referrals.get('total_referral_earnings', Decimal('0.00')))}

*🎯 Lifetime:*
• Total Ads: {user_data.get('total_ads_watched', 0)}
• Total Earned: {format_currency(Decimal(str(user_data.get('total_earned', 0))))}
• Account Age: {(datetime.now() - datetime.fromisoformat(user_data.get('join_date').replace('Z', '+00:00'))).days} days
"""
                
                keyboard = [
                    [InlineKeyboardButton("📈 Detailed Report", callback_data="detailed_report")],
                    [InlineKeyboardButton("🏆 Leaderboard", callback_data="show_leaderboard")],
                    [InlineKeyboardButton("🔙 Back", callback_data="back_to_menu")]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                
                await query.edit_message_text(text, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN)
                
        elif data == "gift_codes":
            text = """
🎁 *Gift Codes*

Redeem gift codes for free money!

*📋 How to use:*
1. Get a gift code from admin or promotions
2. Use command: `/redeem CODE12345678`
3. Get instant money in your balance!

*💡 Where to find codes:*
• Admin announcements
• Special promotions
• Contest prizes
• Giveaway events

*⚠️ Rules:*
• Each code can be used once
• Codes have expiry dates
• Minimum balance may be required for some codes

*🎯 Current Active Codes:* None available
Check announcements for new codes!
"""
            
            keyboard = [
                [InlineKeyboardButton("🔑 Enter Gift Code", callback_data="enter_gift_code")],
                [InlineKeyboardButton("📢 Check Announcements", callback_data="check_announcements")],
                [InlineKeyboardButton("🔙 Back", callback_data="back_to_menu")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await query.edit_message_text(text, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN)
            
        elif data == "settings_menu":
            text = """
⚙️ *Settings*

Configure your account preferences:

*🔔 Notifications:*
• Ad reminders
• Withdrawal updates
• Referral notifications

*🌐 Language:* English
*💵 Currency:* USD
*🎨 Theme:* Light

*🔒 Security:*
• Two-factor authentication
• Login alerts
• Session management

*📊 Privacy:*
• Data sharing preferences
• Activity visibility
"""
            
            keyboard = [
                [InlineKeyboardButton("🔔 Notification Settings", callback_data="notification_settings")],
                [InlineKeyboardButton("🔒 Security Settings", callback_data="security_settings")],
                [InlineKeyboardButton("🌐 Language Settings", callback_data="language_settings")],
                [InlineKeyboardButton("🔙 Back", callback_data="back_to_menu")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await query.edit_message_text(text, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN)
            
        elif data == "admin_panel":
            user_data = get_user(user_id)
            if not user_data or not user_data.get('is_admin'):
                await query.answer("❌ Access denied!", show_alert=True)
                return
            
            text = """
🛠 *Admin Control Panel*

*📊 System Overview:*
• Total Users: Loading...
• Active Today: Loading...
• Total Earnings: Loading...

*🔧 Quick Actions:*
• View user details
• Send money to user
• Create gift codes
• Broadcast messages

*📈 Statistics:*
• System performance
• User activity
• Financial reports

*⚙️ Maintenance:*
• Database backup
• System logs
• Cleanup tasks
"""
            
            keyboard = [
                [InlineKeyboardButton("👤 User Management", callback_data="admin_users")],
                [InlineKeyboardButton("💰 Financial Tools", callback_data="admin_finance")],
                [InlineKeyboardButton("📢 Broadcast", callback_data="admin_broadcast")],
                [InlineKeyboardButton("📊 System Stats", callback_data="admin_stats")],
                [InlineKeyboardButton("🔙 Back", callback_data="back_to_menu")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await query.edit_message_text(text, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN)
            
        else:
            await query.edit_message_text(
                "❌ Unknown command.\n\nUse the buttons provided.",
                parse_mode=ParseMode.MARKDOWN
            )
            
    except Exception as e:
        logger.error(f"Error in handle_callback: {e}")
        await query.edit_message_text(
            "❌ An error occurred. Please try again.",
            parse_mode=ParseMode.MARKDOWN
        )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle button-based menu navigation"""
    message_text = update.message.text
    user_id = update.effective_user.id
    username = update.effective_user.username or update.effective_user.first_name
    
    logger.info(f"📨 Message from {user_id} (@{username}): {message_text}")
    
    try:
        if message_text == "▶️ Watch Ad & Earn":
            await start_watching_ad(update, context)
            
        elif message_text == "💰 My Wallet":
            user_data = get_user_with_stats(user_id)
            if not user_data:
                await update.message.reply_text("❌ Please use /start first to create your account.")
                return
            
            balance = format_currency(Decimal(str(user_data['balance'])))
            ref_balance = format_currency(Decimal(str(user_data.get('referral_balance', 0))))
            total_earned = format_currency(Decimal(str(user_data['total_earned'])))
            
            text = f"""
💰 *My Wallet*

*💵 Main Balance:* {balance}
*👥 Referral Balance:* {ref_balance}
*📊 Total Earned:* {total_earned}

*💳 Withdrawal Methods:*
• bKash (Minimum: ${MIN_WITHDRAWAL})
• Nagad (Minimum: ${MIN_WITHDRAWAL})
• Crypto/USDT (Minimum: ${MIN_WITHDRAWAL})

*👥 Referral Withdrawal:* Minimum ${MIN_REFERRAL_WITHDRAWAL}

💡 *Watch ads daily to increase your balance!*
"""
            
            keyboard = [
                [KeyboardButton("💸 Withdraw Money"), KeyboardButton("📊 View Statistics")],
                [KeyboardButton("▶️ Watch Ad & Earn"), KeyboardButton("👥 Referral Program")],
                [KeyboardButton("🔙 Main Menu")]
            ]
            reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            
            await update.message.reply_text(
                text,
                reply_markup=reply_markup,
                parse_mode=ParseMode.MARKDOWN
            )
            
        elif message_text == "👥 Referral Program":
            user_data = get_user(user_id)
            if not user_data:
                await update.message.reply_text("❌ Please use /start first to create your account.")
                return
            
            referral_link = f"https://t.me/TNEH_EARNING_BOT?start={user_id}"
            
            text = f"""
👥 *Referral Program*

*💰 Referral Reward:* *${REFERRAL_BONUS}* per active referral
*🔥 Unlimited earnings!* No limit on referrals

*🔗 Your Referral Link:*
`{referral_link}`

*📊 Your Referral Stats:*
• *Total Referrals:* {user_data['referrals_count']}
• *Available Referral Balance:* *{format_currency(Decimal(str(user_data.get('referral_balance', 0))))}*

*✅ How it works:*
1. Share your link with friends
2. They join using your link
3. When they become active, you get *${REFERRAL_BONUS}*

*💡 Tips for more referrals:*
• Share on social media
• Send to friends and family
• Post in relevant groups
• Use the short link above
"""
            
            keyboard = [
                [KeyboardButton("📤 Share Link"), KeyboardButton("📊 Referral Stats")],
                [KeyboardButton("▶️ Watch Ad & Earn"), KeyboardButton("💰 My Wallet")],
                [KeyboardButton("🔙 Main Menu")]
            ]
            reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            
            await update.message.reply_text(
                text,
                reply_markup=reply_markup,
                parse_mode=ParseMode.MARKDOWN,
                disable_web_page_preview=True
            )
            
        elif message_text == "🎁 Gift Codes":
            text = """
🎁 *Gift Codes System*

*💰 Free money from gift codes!*
*🎫 Redeem codes to boost your earnings*

*📋 How to use:*
1. Get a gift code from admin
2. Use command: `/redeem CODE12345678`
3. Get instant money in your balance!

*💡 Tip:* Follow our channel for gift code giveaways!

*🎯 Current Active Codes:*
• No active codes at the moment
• Check back later or contact admin

*⚠️ Rules:*
• One redemption per code per user
• Codes expire after 30 days
• Minimum balance may apply
"""
            
            keyboard = [
                [KeyboardButton("🔑 Redeem Code"), KeyboardButton("📢 Check Announcements")],
                [KeyboardButton("▶️ Watch Ad & Earn"), KeyboardButton("💰 My Wallet")],
                [KeyboardButton("🔙 Main Menu")]
            ]
            reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            
            await update.message.reply_text(
                text,
                reply_markup=reply_markup,
                parse_mode=ParseMode.MARKDOWN
            )
            
        elif message_text == "📊 My Statistics":
            user_data = get_user_with_stats(user_id)
            if not user_data:
                await update.message.reply_text("❌ Please use /start first to create your account.")
                return
            
            today = user_data['stats']['today']
            weekly = user_data['stats']['weekly']
            monthly = user_data['stats']['monthly']
            referrals = user_data['stats']['referrals']
            withdrawals = user_data['stats']['withdrawals']
            
            # Calculate account age
            join_date = datetime.fromisoformat(user_data.get('join_date').replace('Z', '+00:00'))
            account_age = (datetime.now() - join_date).days
            
            text = f"""
📊 *Detailed Statistics*

*👤 Account Info:*
• User ID: `{user_id}`
• Account Age: {account_age} days
• Status: {user_data.get('account_status', 'active').upper()}
• Joined: {join_date.strftime('%Y-%m-%d')}

*💰 Financial Overview:*
• Balance: {format_currency(Decimal(str(user_data['balance'])))}
• Total Earned: {format_currency(Decimal(str(user_data['total_earned'])))}
• Total Withdrawn: {format_currency(Decimal(str(user_data.get('total_withdrawn', 0))))}

*📅 Today's Performance:*
• Ads Watched: {today.get('ads_watched', 0)}/{MAX_ADS_PER_DAY}
• Earnings: {format_currency(today.get('earnings_today', Decimal('0.00')))}
• Referrals: {today.get('referrals_today', 0)}

*📆 Weekly Summary (Last 7 Days):*
• Total Ads: {weekly.get('weekly_ads', 0)}
• Total Earnings: {format_currency(weekly.get('weekly_earnings', Decimal('0.00')))}
• New Referrals: {weekly.get('weekly_referrals', 0)}

*📈 Monthly Summary (Last 30 Days):*
• Total Ads: {monthly.get('monthly_ads', 0)}
• Total Earnings: {format_currency(monthly.get('monthly_earnings', Decimal('0.00')))}
• New Referrals: {monthly.get('monthly_referrals', 0)}

*👥 Referral Network:*
• Total Referrals: {referrals.get('total_referrals', 0)}
• Active Referrals: {referrals.get('active_referrals', 0)}
• Referral Earnings: {format_currency(referrals.get('total_referral_earnings', Decimal('0.00')))}

*💸 Withdrawal History:*
• Total Withdrawals: {withdrawals.get('total_withdrawals', 0)}
• Total Amount: {format_currency(Decimal(str(withdrawals.get('total_withdrawn_amount', 0))))}
• Completed: {format_currency(Decimal(str(withdrawals.get('completed_withdrawals', 0))))}

*🎯 Lifetime Totals:*
• Total Ads Watched: {user_data.get('total_ads_watched', 0)}
• Total Ad Earnings: {format_currency(Decimal(str(user_data.get('total_ad_earnings', 0))))}
• Total Referral Earnings: {format_currency(Decimal(str(user_data.get('total_referral_earnings', 0))))}
"""
            
            keyboard = [
                [KeyboardButton("📈 Export Data"), KeyboardButton("🏆 Leaderboard")],
                [KeyboardButton("▶️ Watch Ad & Earn"), KeyboardButton("💰 My Wallet")],
                [KeyboardButton("🔙 Main Menu")]
            ]
            reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            
            await update.message.reply_text(
                text,
                reply_markup=reply_markup,
                parse_mode=ParseMode.MARKDOWN
            )
            
        elif message_text == "🏆 Leaderboard":
            await show_leaderboard(update, context)
            
        elif message_text == "⚙️ Settings":
            text = """
⚙️ *Account Settings*

Configure your preferences and security settings:

*🔔 Notification Settings:*
• Ad watching reminders
• Withdrawal notifications
• Referral updates
• System announcements

*🌐 Language & Region:*
• Interface language
• Timezone settings
• Currency display

*🔒 Security & Privacy:*
• Two-factor authentication
• Login alerts
• Session management
• Data privacy settings

*🎨 Appearance:*
• Theme (Light/Dark)
• Font size
• Display preferences

*📱 Device Management:*
• Active sessions
• Device authorization
• Login history
"""
            
            keyboard = [
                [KeyboardButton("🔔 Notifications"), KeyboardButton("🌐 Language")],
                [KeyboardButton("🔒 Security"), KeyboardButton("🎨 Appearance")],
                [KeyboardButton("🔙 Main Menu")]
            ]
            reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            
            await update.message.reply_text(
                text,
                reply_markup=reply_markup,
                parse_mode=ParseMode.MARKDOWN
            )
            
        elif message_text == "📞 Support":
            text = """
📞 *Support Center*

Need help? We're here for you!

*📋 Common Issues & Solutions:*

1. *Ad not verifying?*
   - Make sure you waited {AD_WAIT_TIME} seconds
   - Don't close the ad page
   - Click "Verify" after timer completes

2. *Withdrawal issues?*
   - Minimum amount: ${MIN_WITHDRAWAL}
   - Processing time: 24-48 hours
   - Check your payment details

3. *Account problems?*
   - Use /start to refresh
   - Clear chat and restart
   - Contact admin for help

*💬 How to get support:*
1. Use /support command
2. Describe your issue clearly
3. Include your user ID: `{user_id}`
4. Be patient for response

*⏰ Support Hours:* 24/7
*🎯 Average Response:* 24-48 hours

*🔗 Useful Links:*
• FAQ: /help
• Contact: /support
• Updates: Check announcements
""".format(AD_WAIT_TIME=AD_WAIT_TIME, MIN_WITHDRAWAL=MIN_WITHDRAWAL, user_id=user_id)
            
            keyboard = [
                [KeyboardButton("📝 Open Ticket"), KeyboardButton("📖 FAQ")],
                [KeyboardButton("🔙 Main Menu")]
            ]
            reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            
            await update.message.reply_text(
                text,
                reply_markup=reply_markup,
                parse_mode=ParseMode.MARKDOWN
            )
            
        elif message_text == "🛠 Admin Panel":
            user_data = get_user(user_id)
            if not user_data or not user_data.get('is_admin'):
                await update.message.reply_text("❌ *Access Denied*\n\nThis panel is for administrators only.")
                return
            
            text = """
🛠 *Admin Control Panel*

*🔧 Available Commands:*
• /user <id> - View user details
• /send <id> <amount> - Send money to user
• /code <amount> - Create gift code
• /stats - Show system statistics
• /broadcast <message> - Broadcast to all users
• /withdrawals - View pending withdrawals
• /backup - Create database backup
• /logs - View system logs

*📊 Quick Stats:*
• Total Users: (Use /stats)
• Active Today: (Use /stats)
• Total Earnings: (Use /stats)

💡 *Use commands in chat for admin actions.*
"""
            
            await update.message.reply_text(
                text,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=ReplyKeyboardMarkup([["🔙 Main Menu"]], resize_keyboard=True)
            )
            
        elif message_text == "🔙 Main Menu":
            await start(update, context)
            
        elif message_text == "💸 Withdraw Money":
            await handle_withdrawal_start(update, context)
            
        elif message_text == "📤 Share Link":
            user_data = get_user(user_id)
            referral_link = f"https://t.me/TNEH_EARNING_BOT?start={user_id}"
            
            text = f"""
📤 *Share Your Referral Link*

🔗 *Your Link:* `{referral_link}`

*💡 Sharing Tips:*
1. *Copy the link above*
2. *Share with friends & family*
3. *Post in relevant groups*
4. *Share on social media*

*📝 Sample Message:*
"Join TNEH Earning Bot and earn money by watching ads! Use my referral link: {referral_link}"

💰 *Remember:* You earn ${REFERRAL_BONUS} for every active referral!
"""
            
            await update.message.reply_text(
                text,
                parse_mode=ParseMode.MARKDOWN,
                disable_web_page_preview=True
            )
            
        elif message_text == "🔑 Redeem Code":
            await update.message.reply_text(
                "🔑 *Redeem Gift Code*\n\n"
                "To redeem a gift code, use the command:\n"
                "`/redeem CODE12345678`\n\n"
                "Replace `CODE12345678` with your actual gift code.\n\n"
                "💡 *Example:* `/redeem TNEH2024GIFT`",
                parse_mode=ParseMode.MARKDOWN
            )
            
        elif message_text == "📢 Check Announcements":
            await update.message.reply_text(
                "📢 *Latest Announcements*\n\n"
                "No announcements at the moment.\n\n"
                "💡 *Check back regularly for:*\n"
                "• Gift code giveaways\n"
                "• Bonus promotions\n"
                "• System updates\n"
                "• Contest announcements\n\n"
                "🔥 *Stay tuned for exciting offers!*",
                parse_mode=ParseMode.MARKDOWN
            )
            
        elif message_text == "📖 FAQ":
            await help_command(update, context)
            
        elif message_text == "📝 Open Ticket":
            await update.message.reply_text(
                "📝 *Open Support Ticket*\n\n"
                "To open a support ticket, use the command:\n"
                "`/support <your message>`\n\n"
                "💡 *Example:* `/support My withdrawal is pending for 3 days`\n\n"
                "🔧 *Please include:*\n"
                "• Clear description of issue\n"
                "• Relevant details\n"
                "• Screenshots if needed\n\n"
                "⏰ *Response time:* 24-48 hours",
                parse_mode=ParseMode.MARKDOWN
            )
            
        else:
            # Default response for unknown messages
            await update.message.reply_text(
                "🤖 *TNEH Earning Bot*\n\n"
                "Please use the buttons below to navigate.\n"
                "Or use /help to see available commands.",
                reply_markup=ReplyKeyboardMarkup([
                    ["▶️ Watch Ad & Earn", "💰 My Wallet"],
                    ["👥 Referral Program", "🎁 Gift Codes"],
                    ["📊 My Statistics", "🔙 Main Menu"]
                ], resize_keyboard=True),
                parse_mode=ParseMode.MARKDOWN
            )
            
    except Exception as e:
        logger.error(f"Error in handle_message: {e}")
        await update.message.reply_text(
            "❌ An error occurred. Please try again.",
            reply_markup=ReplyKeyboardMarkup([["🔙 Main Menu"]], resize_keyboard=True),
            parse_mode=ParseMode.MARKDOWN
        )

# ====================== COMMAND HANDLERS ======================
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send help message"""
    text = f"""
*🤖 TNEH EARNING BOT - Help Guide*

*💰 How to Earn Money:*
1. *Watch Ads* - ${EARN_PER_AD} per ad (Max: {MAX_ADS_PER_DAY}/day)
2. *Invite Friends* - ${REFERRAL_BONUS} per referral
3. *Redeem Codes* - Free money from gift codes
4. *Complete Tasks* - Bonus tasks (coming soon)

*🎯 Tips for Maximum Earnings:*
• Watch all {MAX_ADS_PER_DAY} ads daily
• Invite as many friends as possible
• Check for gift code giveaways
• Login daily for potential bonuses

*📋 Available Commands:*
• /start - Start the bot
• /help - Show this help message
• /wallet - Check your balance
• /referral - Get your referral link
• /stats - View your statistics
• /leaderboard - Top earners list
• /support <message> - Contact support
• /redeem <code> - Redeem gift code
• /withdraw - Start withdrawal process

*💳 Withdrawal Information:*
• Minimum: ${MIN_WITHDRAWAL} (bKash/Nagad)
• Minimum: ${MIN_REFERRAL_WITHDRAWAL} (Referral)
• Processing: 24-48 hours
• Fees: 1.5% (bKash/Nagad), 2% (Crypto)

*⚠️ Important Rules:*
• One account per person
• No cheating or fraud
• Follow ad watching rules
• Be patient with withdrawals

*📞 Support & Contact:*
• Support Group: https://t.me/+QkMGTxBpqftkNDU1
• Email: support@tneh.com
• Response Time: 24-48 hours

*🙏 Thank you for using TNEH EARNING BOT*
*Earn smart. Earn secure.* 💰
"""
    
    await update.message.reply_text(
        text,
        parse_mode=ParseMode.MARKDOWN,
        disable_web_page_preview=True
    )

async def wallet_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Check wallet balance"""
    user_id = update.effective_user.id
    user_data = get_user_with_stats(user_id)
    
    if not user_data:
        await update.message.reply_text("❌ Please use /start first to create your account.")
        return
    
    balance = format_currency(Decimal(str(user_data['balance'])))
    ref_balance = format_currency(Decimal(str(user_data.get('referral_balance', 0))))
    total_earned = format_currency(Decimal(str(user_data['total_earned'])))
    total_withdrawn = format_currency(Decimal(str(user_data.get('total_withdrawn', 0))))
    
    text = f"""
💰 *Wallet Summary*

*💵 Main Balance:* {balance}
*👥 Referral Balance:* {ref_balance}
*📊 Total Earned:* {total_earned}
*💸 Total Withdrawn:* {total_withdrawn}

*📈 Today's Earnings:* {format_currency(user_data['stats']['today'].get('earnings_today', Decimal('0.00')))}

*💳 Withdrawal Options:*
1. *bKash* - Min: ${MIN_WITHDRAWAL} (1.5% fee)
2. *Nagad* - Min: ${MIN_WITHDRAWAL} (1.5% fee)
3. *Crypto/USDT* - Min: ${MIN_WITHDRAWAL} (2% fee)

*👥 Referral Withdrawal:* Min ${MIN_REFERRAL_WITHDRAWAL}

💡 *Need ${MIN_WITHDRAWAL} to withdraw? Watch more ads!*
"""
    
    keyboard = [
        [InlineKeyboardButton("💸 Withdraw Money", callback_data="withdraw_money")],
        [InlineKeyboardButton("📊 View Statistics", callback_data="view_stats")],
        [InlineKeyboardButton("▶️ Watch Ad", callback_data="watch_another_ad")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        text,
        reply_markup=reply_markup,
        parse_mode=ParseMode.MARKDOWN
    )

async def referral_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Get referral information"""
    user_id = update.effective_user.id
    user_data = get_user(user_id)
    
    if not user_data:
        await update.message.reply_text("❌ Please use /start first to create your account.")
        return
    
    referral_link = f"https://t.me/TNEH_EARNING_BOT?start={user_id}"
    
    text = f"""
👥 *Referral Program*

*💰 Earn ${REFERRAL_BONUS} for every friend who joins!*

*🔗 Your Referral Link:*
`{referral_link}`

*📊 Your Stats:*
• Total Referrals: {user_data['referrals_count']}
• Referral Balance: {format_currency(Decimal(str(user_data.get('referral_balance', 0))))}
• Total Referral Earnings: {format_currency(Decimal(str(user_data.get('total_referral_earnings', 0))))}

*🎯 How it works:*
1. Share your link with friends
2. They join using your link
3. They watch their first ad
4. You get ${REFERRAL_BONUS} instantly!

*💡 Tips for success:*
• Share on WhatsApp, Facebook, Twitter
• Join Telegram groups and share
• Tell friends about easy earnings
• No limit on referrals!

*📝 Sample message to share:*
"Join TNEH Earning Bot and earn money by watching ads! Use my referral link to get started: {referral_link}"
"""
    
    keyboard = [
        [InlineKeyboardButton("📤 Share Link", switch_inline_query=f"Join and earn! {referral_link}")],
        [InlineKeyboardButton("📊 Referral Stats", callback_data="referral_stats")],
        [InlineKeyboardButton("🔙 Main Menu", callback_data="back_to_menu")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        text,
        reply_markup=reply_markup,
        parse_mode=ParseMode.MARKDOWN,
        disable_web_page_preview=True
    )

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Command to show system statistics"""
    user_id = update.effective_user.id
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get system statistics
        cursor.execute('SELECT COUNT(*) as total_users FROM users')
        total_users = cursor.fetchone()['total_users']
        
        cursor.execute('SELECT COUNT(*) as active_users FROM users WHERE account_status = "active"')
        active_users = cursor.fetchone()['active_users']
        
        cursor.execute('SELECT COUNT(*) as today_users FROM users WHERE DATE(last_active) = DATE("now")')
        today_users = cursor.fetchone()['today_users']
        
        cursor.execute('SELECT SUM(balance) as total_balance FROM users')
        total_balance = cursor.fetchone()['total_balance'] or Decimal('0.00')
        
        cursor.execute('SELECT SUM(total_earned) as total_earned FROM users')
        total_earned = cursor.fetchone()['total_earned'] or Decimal('0.00')
        
        cursor.execute('SELECT SUM(total_withdrawn) as total_withdrawn FROM users')
        total_withdrawn = cursor.fetchone()['total_withdrawn'] or Decimal('0.00')
        
        cursor.execute('SELECT COUNT(*) as total_ads FROM ad_clicks')
        total_ads = cursor.fetchone()['total_ads']
        
        cursor.execute('SELECT COUNT(*) as verified_ads FROM ad_clicks WHERE verified = 1')
        verified_ads = cursor.fetchone()['verified_ads']
        
        cursor.execute('SELECT COUNT(*) as today_ads FROM ad_clicks WHERE DATE(click_time) = DATE("now")')
        today_ads = cursor.fetchone()['today_ads']
        
        cursor.execute('SELECT COUNT(*) as pending_withdrawals FROM withdrawals WHERE status = "pending"')
        pending_withdrawals = cursor.fetchone()['pending_withdrawals']
        
        cursor.execute('SELECT SUM(amount) as pending_amount FROM withdrawals WHERE status = "pending"')
        pending_amount = cursor.fetchone()['pending_amount'] or Decimal('0.00')
        
        cursor.execute('SELECT COUNT(*) as total_referrals FROM referrals')
        total_referrals = cursor.fetchone()['total_referrals']
        
        cursor.execute('SELECT SUM(earnings_generated) as total_referral_earnings FROM referrals')
        total_referral_earnings = cursor.fetchone()['total_referral_earnings'] or Decimal('0.00')
        
        conn.close()
        
        # Calculate success rate
        success_rate = (verified_ads / total_ads * 100) if total_ads > 0 else 0
        
        # Format large numbers
        total_users_f = format_number(total_users)
        total_earned_f = format_currency(total_earned)
        total_withdrawn_f = format_currency(total_withdrawn)
        total_balance_f = format_currency(total_balance)
        pending_amount_f = format_currency(pending_amount)
        total_referral_earnings_f = format_currency(total_referral_earnings)
        
        text = f"""
*📊 System Statistics Dashboard*

*👥 User Statistics:*
• Total Users: {total_users_f}
• Active Users: {active_users}
• Active Today: {today_users}

*💰 Financial Statistics:*
• Total Earned: {total_earned_f}
• Total Withdrawn: {total_withdrawn_f}
• Total Balance: {total_balance_f}
• Pending Withdrawals: {pending_withdrawals} ({pending_amount_f})

*📈 Ad Performance:*
• Total Ad Clicks: {format_number(total_ads)}
• Verified Ads: {format_number(verified_ads)}
• Today's Ads: {format_number(today_ads)}
• Success Rate: {success_rate:.1f}%

*👥 Referral Network:*
• Total Referrals: {format_number(total_referrals)}
• Referral Earnings: {total_referral_earnings_f}

*⚙️ System Settings:*
• Per Ad: ${EARN_PER_AD}
• Per Referral: ${REFERRAL_BONUS}
• Min Withdrawal: ${MIN_WITHDRAWAL}
• Max Ads/Day: {MAX_ADS_PER_DAY}
• Ad Wait Time: {AD_WAIT_TIME} seconds

*📅 Today's Date:* {date.today().strftime('%Y-%m-%d')}
*⏰ Last Updated:* {datetime.now().strftime('%H:%M:%S')}
"""
        
        await update.message.reply_text(
            text,
            parse_mode=ParseMode.MARKDOWN
        )
        
    except Exception as e:
        logger.error(f"Error in stats_command: {e}")
        await update.message.reply_text(
            "❌ Error loading statistics. Please try again later.",
            parse_mode=ParseMode.MARKDOWN
        )

async def user_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command: View user details"""
    user_id = update.effective_user.id
    user_data = get_user(user_id)
    
    if not user_data or not user_data.get('is_admin'):
        await update.message.reply_text("❌ Admin access required.")
        return
    
    if not context.args:
        await update.message.reply_text("Usage: /user <user_id>")
        return
    
    try:
        target_user_id = int(context.args[0])
        target_user = get_user_with_stats(target_user_id)
        
        if not target_user:
            await update.message.reply_text("❌ User not found.")
            return
        
        # Format user data
        balance = format_currency(Decimal(str(target_user['balance'])))
        ref_balance = format_currency(Decimal(str(target_user.get('referral_balance', 0))))
        total_earned = format_currency(Decimal(str(target_user['total_earned'])))
        total_withdrawn = format_currency(Decimal(str(target_user.get('total_withdrawn', 0))))
        
        # Calculate account age
        join_date = datetime.fromisoformat(target_user.get('join_date').replace('Z', '+00:00'))
        account_age = (datetime.now() - join_date).days
        
        # Get today's stats
        today = target_user['stats']['today']
        weekly = target_user['stats']['weekly']
        monthly = target_user['stats']['monthly']
        
        text = f"""
*👤 User Details - ID: {target_user_id}*

*📄 Account Information:*
• *Username:* @{target_user.get('username', 'N/A')}
• *Name:* {target_user.get('first_name', '')} {target_user.get('last_name', '')}
• *Joined:* {join_date.strftime('%Y-%m-%d %H:%M:%S')}
• *Account Age:* {account_age} days
• *Status:* {target_user.get('account_status', 'active').upper()}
• *Admin:* {'✅ Yes' if target_user.get('is_admin') else '❌ No'}
• *Premium:* {'✅ Yes' if target_user.get('is_premium') else '❌ No'}
• *Verified:* {'✅ Yes' if target_user.get('is_verified') else '❌ No'}

*💰 Financial Information:*
• *Balance:* {balance}
• *Referral Balance:* {ref_balance}
• *Total Earned:* {total_earned}
• *Total Withdrawn:* {total_withdrawn}
• *Referrer ID:* {target_user.get('referrer_id', 'None')}

*📊 Activity Statistics:*
• *Total Ads Watched:* {target_user.get('total_ads_watched', 0)}
• *Today's Ads:* {today.get('ads_watched', 0)}/{MAX_ADS_PER_DAY}
• *Total Referrals:* {target_user.get('referrals_count', 0)}
• *Active Referrals:* {target_user['stats']['referrals'].get('active_referrals', 0)}

*📈 Performance Metrics:*
• *Today's Earnings:* {format_currency(today.get('earnings_today', Decimal('0.00')))}
• *Weekly Earnings:* {format_currency(weekly.get('weekly_earnings', Decimal('0.00')))}
• *Monthly Earnings:* {format_currency(monthly.get('monthly_earnings', Decimal('0.00')))}

*🕒 Last Active:* {target_user.get('last_active', 'Never')}
"""
        
        keyboard = [
            [InlineKeyboardButton("💰 Send Money", callback_data=f"admin_send_{target_user_id}")],
            [InlineKeyboardButton("📊 More Stats", callback_data=f"admin_stats_{target_user_id}")],
            [InlineKeyboardButton("🔙 Admin Panel", callback_data="admin_panel")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=reply_markup
        )
        
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID. Please enter a number.")
    except Exception as e:
        logger.error(f"Error in user_command: {e}")
        await update.message.reply_text("❌ An error occurred.")

async def code_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command: Create gift code"""
    user_id = update.effective_user.id
    user_data = get_user(user_id)
    
    if not user_data or not user_data.get('is_admin'):
        await update.message.reply_text("❌ Admin access required.")
        return
    
    if len(context.args) < 1:
        await update.message.reply_text("Usage: /code <amount> [description]")
        return
    
    try:
        amount = Decimal(context.args[0])
        
        if amount <= 0:
            await update.message.reply_text("❌ Amount must be positive.")
            return
        
        description = " ".join(context.args[1:]) if len(context.args) > 1 else f"Gift code ${amount}"
        
        # Generate gift code
        code = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(GIFT_CODE_LENGTH))
        expiry_date = datetime.now() + timedelta(days=GIFT_CODE_EXPIRY_DAYS)
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO gift_codes (code, amount, created_by, expiry_date, description)
            VALUES (?, ?, ?, ?, ?)
        ''', (code, amount, user_id, expiry_date, description))
        
        conn.commit()
        conn.close()
        
        await update.message.reply_text(
            f"✅ *Gift Code Created!*\n\n"
            f"*Code:* `{code}`\n"
            f"*Amount:* ${amount:.2f}\n"
            f"*Description:* {description}\n"
            f"*Expiry Date:* {expiry_date.strftime('%Y-%m-%d')}\n"
            f"*Max Uses:* 1\n\n"
            f"💡 Share with users: `/redeem {code}`",
            parse_mode=ParseMode.MARKDOWN
        )
            
    except ValueError:
        await update.message.reply_text("❌ Invalid amount. Usage: /code <amount> [description]")
    except Exception as e:
        logger.error(f"Error in code_command: {e}")
        await update.message.reply_text("❌ An error occurred.")

async def redeem_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Command to redeem gift code"""
    user_id = update.effective_user.id
    
    if not context.args:
        await update.message.reply_text("Usage: /redeem <code>")
        return
    
    code = context.args[0].upper().strip()
    
    # Validate code format
    if len(code) != GIFT_CODE_LENGTH or not code.isalnum():
        await update.message.reply_text("❌ Invalid gift code format!")
        return
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if code exists and is valid
        cursor.execute('''
            SELECT * FROM gift_codes 
            WHERE code = ? AND is_active = 1 
            AND (expiry_date IS NULL OR expiry_date > CURRENT_TIMESTAMP)
        ''', (code,))
        gift_code = cursor.fetchone()
        
        if not gift_code:
            conn.close()
            await update.message.reply_text("❌ Invalid or expired gift code!")
            return
        
        # Check if user has already redeemed this code
        cursor.execute('''
            SELECT * FROM gift_code_redemptions 
            WHERE user_id = ? AND gift_code = ?
        ''', (user_id, code))
        
        if cursor.fetchone():
            conn.close()
            await update.message.reply_text("❌ You have already redeemed this code!")
            return
        
        # Check max uses
        if gift_code['current_uses'] >= gift_code['max_uses']:
            cursor.execute('UPDATE gift_codes SET is_active = 0 WHERE code = ?', (code,))
            conn.commit()
            conn.close()
            await update.message.reply_text("❌ This code has reached its usage limit!")
            return
        
        amount = Decimal(str(gift_code['amount']))
        
        # Update gift code usage
        cursor.execute('''
            UPDATE gift_codes 
            SET current_uses = current_uses + 1
            WHERE code = ?
        ''', (code,))
        
        # Record redemption
        cursor.execute('''
            INSERT INTO gift_code_redemptions (user_id, gift_code, amount)
            VALUES (?, ?, ?)
        ''', (user_id, code, amount))
        
        # Add money to user's balance
        update_balance(
            user_id, 
            amount, 
            'gift_code', 
            f'Gift code redemption: {code}',
            f'GIFT_{code}',
            {'code': code, 'amount': str(amount)}
        )
        
        conn.commit()
        conn.close()
        
        user_data = get_user(user_id)
        balance = format_currency(Decimal(str(user_data['balance'])))
        
        await update.message.reply_text(
            f"✅ *Congratulations!*\n\n"
            f"You redeemed *${amount:.2f}* from gift code `{code}`!\n\n"
            f"💰 *New Balance:* {balance}\n"
            f"🎉 *Enjoy your earnings!*\n\n"
            f"💡 *Want more?* Watch ads or invite friends!",
            parse_mode=ParseMode.MARKDOWN
        )
            
    except Exception as e:
        logger.error(f"Error redeeming gift code: {e}")
        await update.message.reply_text("❌ An error occurred. Please try again.")

async def send_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command: Send money to user"""
    user_id = update.effective_user.id
    user_data = get_user(user_id)
    
    if not user_data or not user_data.get('is_admin'):
        await update.message.reply_text("❌ Admin access required.")
        return
    
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /send <user_id> <amount> [description]")
        return
    
    try:
        target_user_id = int(context.args[0])
        amount = Decimal(context.args[1])
        description = " ".join(context.args[2:]) if len(context.args) > 2 else f"Admin gift from {user_id}"
        
        if amount <= 0:
            await update.message.reply_text("❌ Amount must be positive.")
            return
        
        if amount > Decimal('10000.00'):
            await update.message.reply_text("❌ Amount too large (max $10,000).")
            return
        
        # Check if target user exists
        target_user = get_user(target_user_id)
        if not target_user:
            await update.message.reply_text("❌ Target user not found.")
            return
        
        # Send money
        reference_id = f"ADMIN_SEND_{user_id}_{int(time.time())}"
        success = update_balance(
            target_user_id,
            amount,
            'admin_add',
            description,
            reference_id,
            {'admin_id': user_id, 'reason': description}
        )
        
        if success:
            # Log admin action
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO admin_logs (admin_id, action, target_user_id, details, severity)
                VALUES (?, 'send_money', ?, ?, 'info')
            ''', (user_id, target_user_id, f'Sent ${amount} to user {target_user_id}: {description}'))
            conn.commit()
            conn.close()
            
            new_balance = format_currency(Decimal(str(target_user['balance'])) + amount)
            
            await update.message.reply_text(
                f"✅ *Money Sent Successfully!*\n\n"
                f"*To:* @{target_user.get('username', 'N/A')} (ID: {target_user_id})\n"
                f"*Amount:* ${amount:.2f}\n"
                f"*Description:* {description}\n"
                f"*New Balance:* {new_balance}\n"
                f"*Reference:* `{reference_id}`",
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await update.message.reply_text("❌ Failed to send money. Please try again.")
            
    except ValueError:
        await update.message.reply_text("❌ Invalid parameters. Usage: /send <user_id> <amount> [description]")
    except Exception as e:
        logger.error(f"Error in send_command: {e}")
        await update.message.reply_text("❌ An error occurred.")

async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command: Broadcast message to all users"""
    user_id = update.effective_user.id
    user_data = get_user(user_id)
    
    if not user_data or not user_data.get('is_admin'):
        await update.message.reply_text("❌ Admin access required.")
        return
    
    if not context.args:
        await update.message.reply_text("Usage: /broadcast <message>")
        return
    
    message = " ".join(context.args)
    broadcast_text = f"""
*📢 Announcement from Admin*

{message}

---
*TNEH EARNING BOT*
*Date:* {date.today().strftime('%Y-%m-%d')}
"""
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT user_id FROM users WHERE account_status = "active"')
        users = cursor.fetchall()
        conn.close()
        
        total_users = len(users)
        success_count = 0
        fail_count = 0
        
        # Send initial status
        status_msg = await update.message.reply_text(
            f"📤 *Starting broadcast...*\n\n"
            f"Total users: {total_users}\n"
            f"Message length: {len(message)} characters\n\n"
            f"⏳ Please wait...",
            parse_mode=ParseMode.MARKDOWN
        )
        
        # Send to users
        for idx, user in enumerate(users):
            try:
                await context.bot.send_message(
                    chat_id=user['user_id'],
                    text=broadcast_text,
                    parse_mode=ParseMode.MARKDOWN
                )
                success_count += 1
                
                # Update status every 50 users
                if idx % 50 == 0:
                    try:
                        await status_msg.edit_text(
                            f"📤 *Broadcasting...*\n\n"
                            f"Progress: {idx+1}/{total_users}\n"
                            f"Successful: {success_count}\n"
                            f"Failed: {fail_count}\n"
                            f"Completion: {((idx+1)/total_users*100):.1f}%",
                            parse_mode=ParseMode.MARKDOWN
                        )
                    except:
                        pass
                
                # Avoid rate limiting
                await asyncio.sleep(0.1)
                
            except Exception as e:
                fail_count += 1
                logger.error(f"Failed to send to user {user['user_id']}: {e}")
                continue
        
        # Final status
        await status_msg.edit_text(
            f"✅ *Broadcast Completed!*\n\n"
            f"📊 *Results:*\n"
            f"• Total Users: {total_users}\n"
            f"• Successfully Sent: {success_count}\n"
            f"• Failed: {fail_count}\n"
            f"• Success Rate: {(success_count/total_users*100):.1f}%\n\n"
            f"📝 *Message Preview:*\n"
            f"{(message[:200] + '...' if len(message) > 200 else message)}",
            parse_mode=ParseMode.MARKDOWN
        )
        
        # Log broadcast
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO admin_logs (admin_id, action, details, severity)
            VALUES (?, 'broadcast', ?, 'info')
        ''', (user_id, f'Broadcast sent to {success_count} users: {message[:100]}...'))
        conn.commit()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error in broadcast_command: {e}")
        await update.message.reply_text(f"❌ Error during broadcast: {str(e)}")

async def withdrawals_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command: View pending withdrawals"""
    user_id = update.effective_user.id
    user_data = get_user(user_id)
    
    if not user_data or not user_data.get('is_admin'):
        await update.message.reply_text("❌ Admin access required.")
        return
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get pending withdrawals
        cursor.execute('''
            SELECT w.*, u.username, u.first_name 
            FROM withdrawals w
            LEFT JOIN users u ON w.user_id = u.user_id
            WHERE w.status = 'pending'
            ORDER BY w.created_at DESC
            LIMIT 50
        ''')
        withdrawals = cursor.fetchall()
        
        # Get stats
        cursor.execute('''
            SELECT 
                COUNT(*) as total_pending,
                SUM(amount) as total_amount,
                COUNT(CASE WHEN DATE(created_at) = DATE('now') THEN 1 END) as today_count
            FROM withdrawals 
            WHERE status = 'pending'
        ''')
        stats = cursor.fetchone()
        
        conn.close()
        
        if not withdrawals:
            await update.message.reply_text(
                "📭 *No Pending Withdrawals*\n\n"
                "There are currently no pending withdrawal requests.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        text = f"""
📋 *Pending Withdrawals*

*📊 Summary:*
• Total Pending: {stats['total_pending']}
• Total Amount: {format_currency(Decimal(str(stats['total_amount'] or 0)))}
• Today's Requests: {stats['today_count']}

*📝 Recent Requests (Latest 10):*
"""
        
        for idx, withdrawal in enumerate(withdrawals[:10]):
            username = withdrawal['username'] or withdrawal['first_name'] or f"User {withdrawal['user_id']}"
            amount = format_currency(Decimal(str(withdrawal['amount'])))
            method = withdrawal['method'].upper()
            created = withdrawal['created_at'][:16] if withdrawal['created_at'] else "N/A"
            
            text += f"\n{idx+1}. *{username}* - {amount} via {method}\n"
            text += f"   ID: `{withdrawal['id']}` | Created: {created}\n"
            text += f"   Details: {withdrawal['details'][:50]}..."
        
        if len(withdrawals) > 10:
            text += f"\n\n...and {len(withdrawals) - 10} more requests."
        
        text += "\n\n💡 *Use /process <withdrawal_id> to process a withdrawal.*"
        
        await update.message.reply_text(
            text,
            parse_mode=ParseMode.MARKDOWN
        )
        
    except Exception as e:
        logger.error(f"Error in withdrawals_command: {e}")
        await update.message.reply_text("❌ Error loading withdrawals.")

async def process_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command: Process a withdrawal"""
    user_id = update.effective_user.id
    user_data = get_user(user_id)
    
    if not user_data or not user_data.get('is_admin'):
        await update.message.reply_text("❌ Admin access required.")
        return
    
    if not context.args:
        await update.message.reply_text("Usage: /process <withdrawal_id> [approved/rejected] [notes]")
        return
    
    try:
        withdrawal_id = int(context.args[0])
        action = context.args[1].lower() if len(context.args) > 1 else "approved"
        notes = " ".join(context.args[2:]) if len(context.args) > 2 else "Processed by admin"
        
        if action not in ['approved', 'rejected', 'processing']:
            await update.message.reply_text("❌ Invalid action. Use: approved, rejected, or processing")
            return
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get withdrawal details
        cursor.execute('''
            SELECT w.*, u.username, u.balance 
            FROM withdrawals w
            LEFT JOIN users u ON w.user_id = u.user_id
            WHERE w.id = ?
        ''', (withdrawal_id,))
        
        withdrawal = cursor.fetchone()
        
        if not withdrawal:
            conn.close()
            await update.message.reply_text("❌ Withdrawal not found.")
            return
        
        if withdrawal['status'] != 'pending':
            conn.close()
            await update.message.reply_text(f"❌ Withdrawal already {withdrawal['status']}.")
            return
        
        # Update withdrawal status
        cursor.execute('''
            UPDATE withdrawals 
            SET status = ?, 
                processed_at = CURRENT_TIMESTAMP,
                admin_notes = ?,
                admin_id = ?
            WHERE id = ?
        ''', (action, notes, user_id, withdrawal_id))
        
        # If rejected, refund the amount
        if action == 'rejected':
            amount = Decimal(str(withdrawal['amount']))
            cursor.execute('''
                UPDATE users 
                SET balance = balance + ?
                WHERE user_id = ?
            ''', (amount, withdrawal['user_id']))
            
            # Add transaction record
            cursor.execute('''
                INSERT INTO transactions (user_id, type, amount, description)
                VALUES (?, 'refund', ?, ?)
            ''', (withdrawal['user_id'], amount, f'Withdrawal #{withdrawal_id} rejected and refunded'))
        
        # Log admin action
        cursor.execute('''
            INSERT INTO admin_logs (admin_id, action, target_user_id, details, severity)
            VALUES (?, 'process_withdrawal', ?, ?, ?)
        ''', (user_id, withdrawal['user_id'], f'Withdrawal #{withdrawal_id} {action}: {notes}', 'info' if action == 'approved' else 'warning'))
        
        conn.commit()
        conn.close()
        
        # Notify user
        try:
            status_text = "approved and processed" if action == 'approved' else "rejected and refunded"
            notification = f"""
📋 *Withdrawal Update*

Your withdrawal request #{withdrawal_id} has been {status_text}.

*Details:*
• Amount: {format_currency(Decimal(str(withdrawal['amount'])))}
• Method: {withdrawal['method'].upper()}
• Status: {action.upper()}
• Notes: {notes}

💡 {'Payment should arrive soon!' if action == 'approved' else 'Amount has been refunded to your balance.'}
"""
            
            await context.bot.send_message(
                chat_id=withdrawal['user_id'],
                text=notification,
                parse_mode=ParseMode.MARKDOWN
            )
        except Exception as e:
            logger.error(f"Failed to notify user {withdrawal['user_id']}: {e}")
        
        await update.message.reply_text(
            f"✅ *Withdrawal Processed*\n\n"
            f"Withdrawal #{withdrawal_id} has been {action}.\n"
            f"User notified: {'✅' if action == 'approved' else '🔄 Refunded'}\n"
            f"Notes: {notes}",
            parse_mode=ParseMode.MARKDOWN
        )
        
    except ValueError:
        await update.message.reply_text("❌ Invalid withdrawal ID.")
    except Exception as e:
        logger.error(f"Error in process_command: {e}")
        await update.message.reply_text(f"❌ Error processing withdrawal: {str(e)}")

async def backup_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command: Create database backup"""
    user_id = update.effective_user.id
    user_data = get_user(user_id)
    
    if not user_data or not user_data.get('is_admin'):
        await update.message.reply_text("❌ Admin access required.")
        return
    
    # Send initial message
    msg = await update.message.reply_text("🔄 *Creating database backup...*", parse_mode=ParseMode.MARKDOWN)
    
    try:
        # Create backup
        success, result = backup_database()
        
        if success:
            backup_file = result
            file_size = os.path.getsize(backup_file) / (1024 * 1024)  # Convert to MB
            
            await msg.edit_text(
                f"✅ *Backup Created Successfully!*\n\n"
                f"*File:* `{os.path.basename(backup_file)}`\n"
                f"*Size:* {file_size:.2f} MB\n"
                f"*Time:* {datetime.now().strftime('%H:%M:%S')}\n\n"
                f"💡 Backup stored in: `{BACKUP_DIR}`",
                parse_mode=ParseMode.MARKDOWN
            )
            
            # Log backup creation
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO admin_logs (admin_id, action, details, severity)
                VALUES (?, 'backup', ?, 'info')
            ''', (user_id, f'Database backup created: {os.path.basename(backup_file)} ({file_size:.2f} MB)'))
            conn.commit()
            conn.close()
            
        else:
            await msg.edit_text(
                f"❌ *Backup Failed*\n\n"
                f"Error: {result}",
                parse_mode=ParseMode.MARKDOWN
            )
            
    except Exception as e:
        logger.error(f"Error in backup_command: {e}")
        await msg.edit_text(
            f"❌ *Backup Failed*\n\n"
            f"Error: {str(e)}",
            parse_mode=ParseMode.MARKDOWN
        )

async def logs_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command: View system logs"""
    user_id = update.effective_user.id
    user_data = get_user(user_id)
    
    if not user_data or not user_data.get('is_admin'):
        await update.message.reply_text("❌ Admin access required.")
        return
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get recent logs
        cursor.execute('''
            SELECT * FROM system_logs 
            ORDER BY created_at DESC 
            LIMIT 20
        ''')
        logs = cursor.fetchall()
        
        # Get log statistics
        cursor.execute('''
            SELECT 
                COUNT(*) as total_logs,
                COUNT(CASE WHEN level = 'error' THEN 1 END) as error_logs,
                COUNT(CASE WHEN level = 'warning' THEN 1 END) as warning_logs,
                COUNT(CASE WHEN DATE(created_at) = DATE('now') THEN 1 END) as today_logs
            FROM system_logs
        ''')
        stats = cursor.fetchone()
        
        conn.close()
        
        text = f"""
📋 *System Logs*

*📊 Statistics:*
• Total Logs: {stats['total_logs']}
• Error Logs: {stats['error_logs']}
• Warning Logs: {stats['warning_logs']}
• Today's Logs: {stats['today_logs']}

*📝 Recent Logs (Latest 10):*
"""
        
        for idx, log in enumerate(logs[:10]):
            level_emoji = {
                'error': '❌',
                'warning': '⚠️',
                'info': 'ℹ️',
                'debug': '🐛'
            }.get(log['level'], '📝')
            
            time_str = log['created_at'][11:19] if log['created_at'] else "N/A"
            module = log['module'][:15] + '...' if len(log['module']) > 15 else log['module']
            message = log['message'][:30] + '...' if len(log['message']) > 30 else log['message']
            
            text += f"\n{idx+1}. {level_emoji} *[{log['level'].upper()}]* {time_str}\n"
            text += f"   Module: {module}\n"
            text += f"   Message: {message}\n"
            if log['user_id']:
                text += f"   User: {log['user_id']}"
        
        text += "\n\n💡 *Use /log <module> to filter logs by module.*"
        
        await update.message.reply_text(
            text,
            parse_mode=ParseMode.MARKDOWN
        )
        
    except Exception as e:
        logger.error(f"Error in logs_command: {e}")
        await update.message.reply_text("❌ Error loading logs.")

async def handle_withdrawal_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start withdrawal process"""
    user_id = update.effective_user.id
    user_data = get_user(user_id)
    
    if not user_data:
        await update.message.reply_text("❌ Please use /start first to create your account.")
        return
    
    # Check minimum balance
    balance = Decimal(str(user_data['balance']))
    if balance < MIN_WITHDRAWAL:
        await update.message.reply_text(
            f"❌ *Insufficient Balance*\n\n"
            f"You need at least ${MIN_WITHDRAWAL} to withdraw.\n"
            f"Current balance: ${balance:.2f}\n\n"
            f"💡 *Watch more ads to reach the minimum!*",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    # Show withdrawal methods
    text = f"""
💸 *Withdraw Money*

*💰 Available Balance:* ${balance:.2f}
*💰 Minimum Withdrawal:* ${MIN_WITHDRAWAL}

*💳 Choose a withdrawal method:*

1. *bKash* (Bangladesh)
   • Minimum: ${MIN_WITHDRAWAL}
   • Fee: 1.5%
   • Processing: 24-48 hours

2. *Nagad* (Bangladesh)
   • Minimum: ${MIN_WITHDRAWAL}
   • Fee: 1.5%
   • Processing: 24-48 hours

3. *Crypto (USDT)* (TRC20)
   • Minimum: ${MIN_WITHDRAWAL}
   • Fee: 2%
   • Processing: 1-2 hours

💡 *Note:* Fees are deducted from the withdrawal amount.
"""
    
    keyboard = [
        [InlineKeyboardButton("📱 bKash", callback_data="withdraw_bkash")],
        [InlineKeyboardButton("📱 Nagad", callback_data="withdraw_nagad")],
        [InlineKeyboardButton("₿ Crypto (USDT)", callback_data="withdraw_crypto")],
        [InlineKeyboardButton("🔙 Back to Wallet", callback_data="my_wallet")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        text,
        reply_markup=reply_markup,
        parse_mode=ParseMode.MARKDOWN
    )

async def show_leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show leaderboard with top earners"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get top 10 earners
        cursor.execute('''
            SELECT user_id, username, first_name, total_earned, balance
            FROM users 
            WHERE account_status = 'active'
            ORDER BY total_earned DESC 
            LIMIT 10
        ''')
        top_users = cursor.fetchall()
        
        # Get user's rank
        cursor.execute('''
            SELECT COUNT(*) + 1 as rank
            FROM users 
            WHERE total_earned > (SELECT total_earned FROM users WHERE user_id = ?)
            AND account_status = 'active'
        ''', (update.effective_user.id,))
        user_rank = cursor.fetchone()
        
        # Get user's stats
        cursor.execute('''
            SELECT total_earned, balance FROM users WHERE user_id = ?
        ''', (update.effective_user.id,))
        user_stats = cursor.fetchone()
        
        conn.close()
        
        if not top_users:
            await update.message.reply_text(
                "🏆 *Leaderboard*\n\n"
                "No users yet. Be the first to earn!",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        text = "*🏆 TNEH Leaderboard*\n*Top Earners* 🔥\n\n"
        
        medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]
        for idx, user in enumerate(top_users[:10]):
            medal = medals[idx] if idx < 10 else f"#{idx+1}"
            name = user['username'] or user['first_name'] or f"User {user['user_id']}"
            earnings = Decimal(str(user['total_earned'])) or Decimal('0.00')
            
            # Truncate long names
            if len(name) > 15:
                name = name[:12] + "..."
            
            text += f"{medal} *{name}* - *${earnings:.2f}*\n"
        
        # Add user's rank if they're not in top 10
        if user_rank and user_stats:
            user_rank_num = user_rank['rank']
            user_earnings = format_currency(Decimal(str(user_stats['total_earned'])))
            
            text += f"\n📊 *Your Position:* #{user_rank_num}\n"
            text += f"💰 *Your Earnings:* {user_earnings}\n"
            
            if user_rank_num > 10:
                text += f"🎯 *You need ${(Decimal(str(top_users[9]['total_earned'])) - Decimal(str(user_stats['total_earned']))):.2f} more to reach top 10!*\n"
        
        text += "\n💡 *Earn more to reach the top!*\n"
        text += "🔥 *Watch ads daily and invite friends!*"
        
        keyboard = [
            [InlineKeyboardButton("▶️ Watch Ad & Earn", callback_data="watch_another_ad")],
            [InlineKeyboardButton("👥 Invite Friends", callback_data="invite_friends")],
            [InlineKeyboardButton("🔙 Main Menu", callback_data="back_to_menu")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=reply_markup
        )
    except Exception as e:
        logger.error(f"Error showing leaderboard: {e}")
        await update.message.reply_text(
            "❌ Error loading leaderboard. Please try again.",
            parse_mode=ParseMode.MARKDOWN
        )

# ====================== SUPPORT HANDLERS ======================
async def support_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle support command"""
    user_id = update.effective_user.id
    
    if not context.args:
        await update.message.reply_text(
            "📞 *Support*\n\n"
            "Usage: /support <your message>\n\n"
            "💡 *Example:* `/support My withdrawal is pending for 3 days`\n\n"
            "🔧 *Please include:*\n"
            "• Clear description of issue\n"
            "• Relevant details\n"
            "• Screenshots if needed\n\n"
            "⏰ *Response time:* 24-48 hours",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    message = " ".join(context.args)
    user_data = get_user(user_id)
    
    if not user_data:
        await update.message.reply_text("❌ Please use /start first to create your account.")
        return
    
    try:
        # Generate ticket ID
        ticket_id = f"TKT{user_id}{int(time.time()) % 1000000:06d}"
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Create support ticket
        cursor.execute('''
            INSERT INTO support_tickets (
                user_id, ticket_id, subject, message, status, priority, category
            ) VALUES (?, ?, ?, ?, 'open', 'medium', 'general')
        ''', (user_id, ticket_id, 'User Support Request', message))
        
        # Get ticket ID
        ticket_db_id = cursor.lastrowid
        
        # Add initial message
        cursor.execute('''
            INSERT INTO ticket_messages (ticket_id, user_id, message, is_admin)
            VALUES (?, ?, ?, 0)
        ''', (ticket_db_id, user_id, message))
        
        conn.commit()
        conn.close()
        
        # Send confirmation to user
        await update.message.reply_text(
            f"✅ *Support Ticket Created!*\n\n"
            f"*Ticket ID:* `{ticket_id}`\n"
            f"*Status:* Open\n"
            f"*Priority:* Medium\n"
            f"*Created:* {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
            f"📋 *Your Message:*\n"
            f"{message[:200]}{'...' if len(message) > 200 else ''}\n\n"
            f"💡 *What happens next:*\n"
            "1. Our team will review your ticket\n"
            "2. You'll receive a response here\n"
            "3. Response time: 24-48 hours\n\n"
            f"🔧 *Need to add more info?* Reply to this thread.",
            parse_mode=ParseMode.MARKDOWN
        )
        
        # Notify admin
        admin_notification = f"""
📞 *New Support Ticket*

*Ticket ID:* `{ticket_id}`
*User:* @{user_data.get('username', 'N/A')} (ID: {user_id})
*Time:* {datetime.now().strftime('%H:%M:%S')}

*Message:*
{message[:500]}{'...' if len(message) > 500 else ''}

💡 *Use /ticket {ticket_id} to view and respond.*
"""
        
        try:
            await context.bot.send_message(
                chat_id=ADMIN_ID,
                text=admin_notification,
                parse_mode=ParseMode.MARKDOWN
            )
        except:
            pass
        
    except Exception as e:
        logger.error(f"Error creating support ticket: {e}")
        await update.message.reply_text(
            "❌ Error creating support ticket. Please try again later.",
            parse_mode=ParseMode.MARKDOWN
        )

# ====================== ERROR HANDLER ======================
async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle errors in the bot"""
    logger.error(f"Update {update} caused error {context.error}")
    
    try:
        # Notify admin about error
        error_msg = str(context.error)[:1000]
        
        admin_notification = f"""
❌ *Bot Error Occurred*

*Error:* `{error_msg}`
*Time:* {datetime.now().strftime('%H:%M:%S')}

"""
        
        if update and update.effective_user:
            admin_notification += f"*User:* {update.effective_user.id}"
            if update.effective_user.username:
                admin_notification += f" (@{update.effective_user.username})"
        
        try:
            await context.bot.send_message(
                chat_id=ADMIN_ID,
                text=admin_notification,
                parse_mode=ParseMode.MARKDOWN
            )
        except:
            pass
        
        # Log error to database
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO system_logs (level, module, message, details, user_id)
                VALUES ('error', 'bot', 'Bot error occurred', ?, ?)
            ''', (error_msg, update.effective_user.id if update and update.effective_user else None))
            conn.commit()
            conn.close()
        except Exception as db_error:
            logger.error(f"Error logging to database: {db_error}")
        
        # Send user-friendly error message
        if update and update.message:
            await update.message.reply_text(
                "❌ An error occurred. Our team has been notified.\n"
                "Please try again in a few moments.",
                parse_mode=ParseMode.MARKDOWN
            )
            
    except Exception as e:
        logger.error(f"Error in error handler: {e}")

# ====================== SCHEDULED TASKS ======================
async def scheduled_backup(context: ContextTypes.DEFAULT_TYPE):
    """Scheduled daily backup"""
    try:
        logger.info("🔄 Running scheduled daily backup...")
        success, result = backup_database()
        
        if success:
            logger.info(f"✅ Scheduled backup completed: {result}")
            
            # Notify admin
            try:
                await context.bot.send_message(
                    chat_id=ADMIN_ID,
                    text=f"✅ *Scheduled Backup Completed*\n\nFile: {os.path.basename(result)}",
                    parse_mode=ParseMode.MARKDOWN
                )
            except:
                pass
        else:
            logger.error(f"❌ Scheduled backup failed: {result}")
            
    except Exception as e:
        logger.error(f"Error in scheduled backup: {e}")

async def cleanup_old_sessions(context: ContextTypes.DEFAULT_TYPE):
    """Clean up old ad sessions"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Delete sessions older than 1 hour
        cursor.execute('''
            DELETE FROM active_ad_sessions 
            WHERE valid_until < DATETIME('now', '-1 hour')
        ''')
        
        deleted_count = cursor.rowcount
        
        # Delete old ad clicks (older than 30 days)
        cursor.execute('''
            DELETE FROM ad_clicks 
            WHERE click_time < DATETIME('now', '-30 days')
        ''')
        
        old_clicks_deleted = cursor.rowcount
        
        conn.commit()
        conn.close()
        
        if deleted_count > 0 or old_clicks_deleted > 0:
            logger.info(f"🧹 Cleaned up {deleted_count} old sessions and {old_clicks_deleted} old ad clicks")
            
    except Exception as e:
        logger.error(f"Error cleaning up old sessions: {e}")

async def reset_daily_counts(context: ContextTypes.DEFAULT_TYPE):
    """Reset daily counts for all users"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Reset ads_today for all users
        cursor.execute('''
            UPDATE users 
            SET ads_today = 0, last_reset_date = DATE('now')
            WHERE last_reset_date < DATE('now')
        ''')
        
        reset_count = cursor.rowcount
        
        # Archive today's daily_stats
        cursor.execute('''
            INSERT INTO daily_stats_archive 
            SELECT * FROM daily_stats WHERE date = DATE('now', '-1 day')
        ''')
        
        # Clear old daily_stats (keep last 30 days)
        cursor.execute('''
            DELETE FROM daily_stats 
            WHERE date < DATE('now', '-30 days')
        ''')
        
        conn.commit()
        conn.close()
        
        if reset_count > 0:
            logger.info(f"🔄 Reset daily counts for {reset_count} users")
            
    except Exception as e:
        logger.error(f"Error resetting daily counts: {e}")

# ====================== MAIN FUNCTION ======================
def main():
    """Start the bot"""
    try:
        # Import os for backup directory
        import os
        
        # Initialize database
        init_database()
        
        # Create Application with persistence
        persistence = PicklePersistence(filepath="tneh_bot_persistence")
        
        application = ApplicationBuilder() \
            .token(BOT_TOKEN) \
            .persistence(persistence) \
            .build()
        
        # Add command handlers
        application.add_handler(CommandHandler("start", start))
        application.add_handler(CommandHandler("help", help_command))
        application.add_handler(CommandHandler("wallet", wallet_command))
        application.add_handler(CommandHandler("referral", referral_command))
        application.add_handler(CommandHandler("stats", stats_command))
        application.add_handler(CommandHandler("leaderboard", show_leaderboard))
        application.add_handler(CommandHandler("support", support_command))
        application.add_handler(CommandHandler("redeem", redeem_command))
        
        # Admin commands
        application.add_handler(CommandHandler("user", user_command))
        application.add_handler(CommandHandler("code", code_command))
        application.add_handler(CommandHandler("send", send_command))
        application.add_handler(CommandHandler("broadcast", broadcast_command))
        application.add_handler(CommandHandler("withdrawals", withdrawals_command))
        application.add_handler(CommandHandler("process", process_command))
        application.add_handler(CommandHandler("backup", backup_command))
        application.add_handler(CommandHandler("logs", logs_command))
        
        # Add callback query handlers
        application.add_handler(CallbackQueryHandler(handle_callback))
        
        # Add message handlers
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
        
        # Add error handler
        application.add_error_handler(error_handler)
        
        # Schedule jobs
        job_queue = application.job_queue
        
        if job_queue:
            # Daily backup at 3 AM
            job_queue.run_daily(scheduled_backup, time=time(hour=3, minute=0, second=0))
            
            # Cleanup old sessions every hour
            job_queue.run_repeating(cleanup_old_sessions, interval=3600, first=10)
            
            # Reset daily counts at midnight
            job_queue.run_daily(reset_daily_counts, time=time(hour=0, minute=0, second=0))
        
        # Start the bot
        print("=" * 70)
        print("🤖 TNEH EARNING BOT - ENHANCED VERSION 3.5.1")
        print("=" * 70)
        print(f"🔑 Bot Token: {BOT_TOKEN[:15]}...")
        print(f"👑 Admin ID: {ADMIN_ID}")
        print(f"💰 Per Ad: ${EARN_PER_AD}")
        print(f"👥 Per Referral: ${REFERRAL_BONUS}")
        print(f"📊 Ad URLs: {len(AD_LINKS)} links")
        print(f"⏰ Ad Wait Time: {AD_WAIT_TIME} seconds")
        print(f"📈 Max Ads/Day: {MAX_ADS_PER_DAY}")
        print(f"💳 Min Withdrawal: ${MIN_WITHDRAWAL}")
        print("=" * 70)
        print("✅ Bot is ready and running!")
        print("✅ Database initialized successfully!")
        print("✅ Scheduled tasks configured!")
        print("=" * 70)
        
        # Create initial backup
        try:
            success, backup_file = backup_database()
            if success:
                print(f"✅ Initial backup created: {backup_file}")
        except:
            print("⚠️ Could not create initial backup")
        
        # Run the bot
        application.run_polling(
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=True
        )
        
    except Exception as e:
        logger.error(f"Fatal error in main: {e}")
        print(f"❌ Fatal error: {e}")
        raise

if __name__ == '__main__':
    # Import os here to avoid circular import
    import os
    
    # Run the bot

    main()

