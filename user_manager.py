import sqlite3
import os
import secrets
from cryptography.fernet import Fernet
import logging
from dotenv import load_dotenv
import time
import uuid
from datetime import datetime
from typing import Dict, Optional, List

load_dotenv()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Константы
MAX_REQUESTS_PER_ACCOUNT = 1000
MAX_ACTIVE_ACCOUNTS = 5

# Настройка шифрования
ENCRYPTION_KEY = os.getenv('ENCRYPTION_KEY', 'default_encryption_key_must_be_32_bytes_').encode()
cipher = Fernet(ENCRYPTION_KEY)

def get_db_connection():
    """Получает соединение с базой данных."""
    conn = sqlite3.connect('users.db')
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """Инициализирует базу данных, создает таблицы, если они не существуют."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Создаем таблицу пользователей
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
        api_key TEXT PRIMARY KEY,
        username TEXT,
        password TEXT,
        created_at TEXT,
        last_used TEXT,
        vk_token TEXT
    )
    ''')
    
    # Создаем таблицу аккаунтов Telegram
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS telegram_accounts (
        id TEXT PRIMARY KEY,
        user_api_key TEXT,
        api_id TEXT,
        api_hash TEXT,
        phone TEXT,
        proxy TEXT,
        status TEXT,
        session_file TEXT,
        requests_count INTEGER,
        last_request_time REAL,
        added_at TEXT,
        session_string TEXT,
        phone_code_hash TEXT,
        is_active INTEGER DEFAULT 1,
        request_limit INTEGER DEFAULT 1000,
        FOREIGN KEY (user_api_key) REFERENCES users (api_key)
    )
    ''')
    
    # Добавляем недостающие столбцы к таблице telegram_accounts
    check_and_add_column(cursor, "telegram_accounts", "session_string", "TEXT")
    check_and_add_column(cursor, "telegram_accounts", "phone_code_hash", "TEXT")
    check_and_add_column(cursor, "telegram_accounts", "is_active", "INTEGER DEFAULT 1")
    check_and_add_column(cursor, "telegram_accounts", "request_limit", "INTEGER DEFAULT 1000")
    
    # Создаем таблицу аккаунтов VK
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS vk_accounts (
        id TEXT PRIMARY KEY,
        user_api_key TEXT,
        token TEXT,
        proxy TEXT,
        status TEXT,
        requests_count INTEGER,
        last_request_time REAL,
        added_at TEXT,
        user_id INTEGER,
        user_name TEXT,
        error_message TEXT,
        error_code INTEGER,
        last_checked_at TEXT,
        is_active INTEGER DEFAULT 1,
        request_limit INTEGER DEFAULT 1000,
        FOREIGN KEY (user_api_key) REFERENCES users (api_key)
    )
    ''')
    
    # Добавляем недостающие столбцы к таблице vk_accounts
    check_and_add_column(cursor, "vk_accounts", "user_id", "INTEGER")
    check_and_add_column(cursor, "vk_accounts", "user_name", "TEXT")
    check_and_add_column(cursor, "vk_accounts", "error_message", "TEXT")
    check_and_add_column(cursor, "vk_accounts", "error_code", "INTEGER")
    check_and_add_column(cursor, "vk_accounts", "last_checked_at", "TEXT")
    check_and_add_column(cursor, "vk_accounts", "is_active", "INTEGER DEFAULT 1")
    check_and_add_column(cursor, "vk_accounts", "request_limit", "INTEGER DEFAULT 1000")
    
    conn.commit()
    conn.close()

def check_and_add_column(cursor, table_name, column_name, column_type):
    """Проверяет наличие столбца в таблице и добавляет его, если он отсутствует."""
    try:
        # Получаем информацию о столбцах таблицы
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()
        
        # Проверяем, есть ли уже такой столбец
        column_exists = any(column[1] == column_name for column in columns)
        
        if not column_exists:
            # Добавляем столбец, если он не существует
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")
            logger.info(f"Столбец {column_name} успешно добавлен в таблицу {table_name}")
        else:
            logger.info(f"Столбец {column_name} уже существует в таблице {table_name}")
    except Exception as e:
        logger.error(f"Ошибка при добавлении столбца {column_name} в таблицу {table_name}: {e}")

async def register_user(username: str = None, password: str = None):
    """Регистрирует нового пользователя и возвращает его API ключ."""
    api_key = str(uuid.uuid4())
    created_at = datetime.now().isoformat()
    
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('INSERT INTO users (api_key, username, password, created_at) VALUES (?, ?, ?, ?)',
                  (api_key, username, password, created_at))
    conn.commit()
    conn.close()
    
    logger.info(f"Зарегистрирован новый пользователь с API ключом: {api_key}")
    return api_key

def get_user(api_key: str) -> Optional[Dict]:
    """Получает информацию о пользователе."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('SELECT * FROM users WHERE api_key = ?', (api_key,))
    user = cursor.fetchone()
    
    if user:
        user_dict = dict(user)
        # Получаем Telegram аккаунты
        cursor.execute('SELECT * FROM telegram_accounts WHERE user_api_key = ?', (api_key,))
        user_dict['telegram_accounts'] = [dict(acc) for acc in cursor.fetchall()]
        
        # Получаем VK аккаунты
        cursor.execute('SELECT * FROM vk_accounts WHERE user_api_key = ?', (api_key,))
        user_dict['vk_accounts'] = [dict(acc) for acc in cursor.fetchall()]
        
        conn.close()
        return user_dict
    
    conn.close()
    return None

def update_user_last_used(api_key: str) -> None:
    """Обновляет время последнего использования пользователя."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('UPDATE users SET last_used = ? WHERE api_key = ?',
                  (datetime.now().isoformat(), api_key))
    conn.commit()
    conn.close()

def add_telegram_account(api_key: str, account_data: Dict) -> bool:
    """Добавляет аккаунт Telegram для пользователя."""
    if not get_user(api_key):
        return False
    
    # Используем ID из account_data, если он есть, иначе генерируем новый
    account_id = account_data.get('id') or str(uuid.uuid4())
    added_at = datetime.now().isoformat()
    
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
    INSERT INTO telegram_accounts 
    (id, user_api_key, api_id, api_hash, phone, proxy, status, session_file,
     requests_count, last_request_time, added_at, session_string, phone_code_hash)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        account_id, api_key, account_data.get('api_id'), account_data.get('api_hash'),
        account_data.get('phone'), account_data.get('proxy'), account_data.get('status', 'active'),
        account_data.get('session_file'), 0, None, added_at,
        account_data.get('session_string', ''), account_data.get('phone_code_hash', '')
    ))
    conn.commit()
    conn.close()
    return True

def update_telegram_account(api_key: str, account_id: str, account_data: Dict) -> bool:
    """Обновляет данные аккаунта Telegram."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Проверяем, принадлежит ли аккаунт пользователю
    cursor.execute('SELECT * FROM telegram_accounts WHERE id = ? AND user_api_key = ?',
                  (account_id, api_key))
    if not cursor.fetchone():
        conn.close()
        return False
    
    # Обновляем данные
    cursor.execute('''
    UPDATE telegram_accounts 
    SET api_id = ?, api_hash = ?, phone = ?, proxy = ?, status = ?, session_file = ?,
        session_string = ?, phone_code_hash = ?
    WHERE id = ?
    ''', (
        account_data.get('api_id'), account_data.get('api_hash'),
        account_data.get('phone'), account_data.get('proxy'),
        account_data.get('status'), account_data.get('session_file'),
        account_data.get('session_string'), account_data.get('phone_code_hash'),
        account_id
    ))
    
    conn.commit()
    conn.close()
    return True

def delete_telegram_account(api_key: str, account_id: str) -> bool:
    """Удаляет аккаунт Telegram."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('DELETE FROM telegram_accounts WHERE id = ? AND user_api_key = ?',
                  (account_id, api_key))
    deleted = cursor.rowcount > 0
    
    conn.commit()
    conn.close()
    return deleted

def add_vk_account(api_key: str, account_data: Dict) -> bool:
    """Добавляет аккаунт VK для пользователя."""
    if not get_user(api_key):
        return False
    
    # Используем ID из account_data, если он есть, иначе генерируем новый
    account_id = account_data.get('id') or str(uuid.uuid4())
    added_at = datetime.now().isoformat()
    encrypted_token = cipher.encrypt(account_data['token'].encode()).decode()
    
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
    INSERT INTO vk_accounts 
    (id, user_api_key, token, proxy, status, requests_count, last_request_time, added_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        account_id, api_key, encrypted_token, account_data.get('proxy'),
        'active', 0, None, added_at
    ))
    conn.commit()
    conn.close()
    return True

def update_vk_account(api_key: str, account_id: str, account_data: Dict) -> bool:
    """Обновляет данные аккаунта VK."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Проверяем, принадлежит ли аккаунт пользователю
    cursor.execute('SELECT * FROM vk_accounts WHERE id = ? AND user_api_key = ?',
                  (account_id, api_key))
    if not cursor.fetchone():
        conn.close()
        return False
    
    # Шифруем токен, если он предоставлен
    token = account_data.get('token')
    if token:
        token = cipher.encrypt(token.encode()).decode()
    
    # Обновляем данные
    cursor.execute('''
    UPDATE vk_accounts 
    SET token = COALESCE(?, token), proxy = ?, status = ?
    WHERE id = ?
    ''', (token, account_data.get('proxy'), account_data.get('status'), account_id))
    
    conn.commit()
    conn.close()
    return True

def delete_vk_account(api_key: str, account_id: str) -> bool:
    """Удаляет аккаунт VK."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('DELETE FROM vk_accounts WHERE id = ? AND user_api_key = ?',
                  (account_id, api_key))
    deleted = cursor.rowcount > 0
    
    conn.commit()
    conn.close()
    return deleted

def get_active_accounts(api_key: str, platform: str) -> List[Dict]:
    """Получает список активных аккаунтов для платформы."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    if platform == 'telegram':
        cursor.execute('''
        SELECT * FROM telegram_accounts 
        WHERE user_api_key = ? AND requests_count < ? AND status = 'active'
        ORDER BY requests_count ASC
        LIMIT ?
        ''', (api_key, MAX_REQUESTS_PER_ACCOUNT, MAX_ACTIVE_ACCOUNTS))
    else:  # vk
        cursor.execute('''
        SELECT * FROM vk_accounts 
        WHERE user_api_key = ? AND requests_count < ? AND status = 'active'
        ORDER BY requests_count ASC
        LIMIT ?
        ''', (api_key, MAX_REQUESTS_PER_ACCOUNT, MAX_ACTIVE_ACCOUNTS))
    
    accounts = [dict(acc) for acc in cursor.fetchall()]
    
    # Расшифровываем VK токены
    if platform == 'vk':
        for acc in accounts:
            acc['token'] = cipher.decrypt(acc['token'].encode()).decode()
    
    conn.close()
    return accounts

def get_next_available_account(api_key: str, platform: str) -> Optional[Dict]:
    """Получает следующий доступный аккаунт для использования."""
    active_accounts = get_active_accounts(api_key, platform)
    if not active_accounts:
        return None
    
    if len(active_accounts) == 1:
        account = active_accounts[0]
        account["degraded_mode"] = True
        return account
    
    return active_accounts[0]

def update_account_usage(api_key: str, account_id: str, platform: str, token_expired: bool = False) -> bool:
    """Обновляет статистику использования аккаунта."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    table = 'telegram_accounts' if platform == 'telegram' else 'vk_accounts'
    status_update = ", status = 'inactive'" if token_expired else ""
    
    cursor.execute(f'''
    UPDATE {table} 
    SET requests_count = requests_count + 1, 
        last_request_time = ?{status_update}
    WHERE id = ? AND user_api_key = ?
    ''', (time.time(), account_id, api_key))
    
    updated = cursor.rowcount > 0
    conn.commit()
    conn.close()
    return updated

def set_vk_token(api_key: str, vk_token: str) -> bool:
    """Устанавливает VK токен для пользователя."""
    if not get_user(api_key):
        return False
    
    encrypted_token = cipher.encrypt(vk_token.encode()).decode()
    
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
    UPDATE users 
    SET vk_token = ?
    WHERE api_key = ?
    ''', (encrypted_token, api_key))
    
    updated = cursor.rowcount > 0
    conn.commit()
    conn.close()
    return updated

def get_vk_token(api_key: str) -> Optional[str]:
    """Получает VK токен пользователя."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('SELECT vk_token FROM users WHERE api_key = ?', (api_key,))
    result = cursor.fetchone()
    conn.close()
    
    if result and result['vk_token']:
        return cipher.decrypt(result['vk_token'].encode()).decode()
    return None

def get_users_dict() -> Dict:
    """Получает всех пользователей в формате словаря {api_key: user_data}.
    Для совместимости с прежней структурой данных."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('SELECT * FROM users')
    users_rows = cursor.fetchall()
    
    users = {}
    
    for user in users_rows:
        user_dict = dict(user)
        api_key = user_dict['api_key']
        
        # Получаем Telegram аккаунты
        cursor.execute('SELECT * FROM telegram_accounts WHERE user_api_key = ?', (api_key,))
        user_dict['telegram_accounts'] = [dict(acc) for acc in cursor.fetchall()]
        
        # Получаем VK аккаунты
        cursor.execute('SELECT * FROM vk_accounts WHERE user_api_key = ?', (api_key,))
        user_dict['vk_accounts'] = [dict(acc) for acc in cursor.fetchall()]
        
        # Расшифровываем VK токен, если он есть
        if user_dict.get('vk_token'):
            user_dict['vk_token'] = cipher.decrypt(user_dict['vk_token'].encode()).decode()
        
        users[api_key] = user_dict
    
    conn.close()
    return users

def verify_api_key(api_key: str) -> bool:
    """Проверяет, существует ли пользователь с указанным API ключом."""
    user = get_user(api_key)
    if not user:
        return False
    
    # Обновляем время последнего использования
    update_user_last_used(api_key)
    return True

def save_users(users_dict: Dict) -> None:
    """Сохраняет словарь пользователей в базу данных.
    
    Это функция обратной совместимости для кода, который еще не был полностью обновлен.
    
    Args:
        users_dict: Словарь данных пользователей в формате {api_key: user_data}
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    for api_key, user_data in users_dict.items():
        # Проверяем, существует ли пользователь
        cursor.execute('SELECT api_key FROM users WHERE api_key = ?', (api_key,))
        user_exists = cursor.fetchone() is not None
        
        # Основные данные пользователя
        if user_exists:
            # Обновляем существующего пользователя
            cursor.execute('''
            UPDATE users SET
                username = ?,
                password = ?,
                last_used = ?,
                vk_token = ?
            WHERE api_key = ?
            ''', (
                user_data.get('username'),
                user_data.get('password'),
                user_data.get('last_used'),
                user_data.get('vk_token', ''),
                api_key
            ))
        else:
            # Создаем нового пользователя
            cursor.execute('''
            INSERT INTO users (api_key, username, password, created_at, last_used, vk_token)
            VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                api_key,
                user_data.get('username'),
                user_data.get('password'),
                user_data.get('created_at', datetime.now().isoformat()),
                user_data.get('last_used'),
                user_data.get('vk_token', '')
            ))
        
        # Удаляем все аккаунты пользователя (мы их пересоздадим)
        cursor.execute('DELETE FROM telegram_accounts WHERE user_api_key = ?', (api_key,))
        cursor.execute('DELETE FROM vk_accounts WHERE user_api_key = ?', (api_key,))
        
        # Добавляем Telegram аккаунты
        for account in user_data.get('telegram_accounts', []):
            account_id = account.get('id', str(uuid.uuid4()))
            cursor.execute('''
            INSERT INTO telegram_accounts (
                id, user_api_key, api_id, api_hash, phone, proxy, status,
                session_file, requests_count, last_request_time, added_at,
                session_string, phone_code_hash
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                account_id,
                api_key,
                account.get('api_id'),
                account.get('api_hash'),
                account.get('phone'),
                account.get('proxy'),
                account.get('status', 'active'),
                account.get('session_file'),
                account.get('requests_count', 0),
                account.get('last_request_time'),
                account.get('added_at', datetime.now().isoformat()),
                account.get('session_string', ''),
                account.get('phone_code_hash', '')
            ))
        
        # Добавляем VK аккаунты
        for account in user_data.get('vk_accounts', []):
            account_id = account.get('id', str(uuid.uuid4()))
            token = account.get('token', '')
            if token:
                token = cipher.encrypt(token.encode()).decode()
            
            cursor.execute('''
            INSERT INTO vk_accounts (
                id, user_api_key, token, proxy, status,
                requests_count, last_request_time, added_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                account_id,
                api_key,
                token,
                account.get('proxy'),
                account.get('status', 'active'),
                account.get('requests_count', 0),
                account.get('last_request_time'),
                account.get('added_at', datetime.now().isoformat())
            ))
    
    conn.commit()
    conn.close()

if __name__ == "__main__":
    # Обновление структуры базы данных
    print("Обновление структуры базы данных...")
    init_db()
    print("Структура базы данных успешно обновлена!") 