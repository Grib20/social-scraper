import sqlite3
import logging
import os
import sys
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_FILE = 'users.db'

def get_db_connection():
    """Создает и возвращает соединение с базой данных."""
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

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
            return True
        else:
            logger.info(f"Столбец {column_name} уже существует в таблице {table_name}")
            return False
    except Exception as e:
        logger.error(f"Ошибка при добавлении столбца {column_name} в таблицу {table_name}: {e}")
        return False

def check_table_exists(cursor, table_name):
    """Проверяет существование таблицы в базе данных."""
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
    return cursor.fetchone() is not None

def update_database_structure():
    """Обновляет структуру базы данных, добавляя недостающие столбцы."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Проверяем существование таблицы пользователей
    if not check_table_exists(cursor, "users"):
        logger.info("Таблица users не существует, создаем...")
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id TEXT PRIMARY KEY,
            username TEXT,
            password TEXT,
            api_key TEXT UNIQUE,
            vk_token TEXT,
            created_at TEXT,
            last_login TEXT
        )
        ''')
    
    # Добавляем недостающие столбцы к таблице users
    check_and_add_column(cursor, "users", "username", "TEXT")
    check_and_add_column(cursor, "users", "password", "TEXT")
    check_and_add_column(cursor, "users", "id", "TEXT")
    
    # Проверяем существование таблицы telegram_accounts
    if not check_table_exists(cursor, "telegram_accounts"):
        logger.info("Таблица telegram_accounts не существует, создаем...")
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS telegram_accounts (
            id TEXT PRIMARY KEY,
            user_api_key TEXT,
            api_id INTEGER,
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
            FOREIGN KEY (user_api_key) REFERENCES users (api_key)
        )
        ''')
    
    # Добавляем недостающие столбцы к таблице telegram_accounts
    check_and_add_column(cursor, "telegram_accounts", "session_string", "TEXT")
    check_and_add_column(cursor, "telegram_accounts", "phone_code_hash", "TEXT")
    
    # Проверяем существование таблицы vk_accounts
    if not check_table_exists(cursor, "vk_accounts"):
        logger.info("Таблица vk_accounts не существует, создаем...")
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
            FOREIGN KEY (user_api_key) REFERENCES users (api_key)
        )
        ''')
    
    # Добавляем недостающие столбцы к таблице vk_accounts
    changed = False
    changed |= check_and_add_column(cursor, "vk_accounts", "user_id", "INTEGER")
    changed |= check_and_add_column(cursor, "vk_accounts", "user_name", "TEXT")
    changed |= check_and_add_column(cursor, "vk_accounts", "error_message", "TEXT")
    changed |= check_and_add_column(cursor, "vk_accounts", "error_code", "INTEGER")
    changed |= check_and_add_column(cursor, "vk_accounts", "last_checked_at", "TEXT")
    
    conn.commit()
    conn.close()
    
    return changed

if __name__ == "__main__":
    logger.info("Начало обновления структуры базы данных...")
    
    # Создаем бэкап базы данных
    if os.path.exists(DB_FILE):
        import shutil
        import time
        backup_file = f"{DB_FILE}.backup_{int(time.time())}"
        try:
            shutil.copy2(DB_FILE, backup_file)
            logger.info(f"Создан бэкап базы данных: {backup_file}")
        except Exception as e:
            logger.error(f"Не удалось создать бэкап базы данных: {e}")
    
    # Обновляем структуру базы данных
    changed = update_database_structure()
    
    if changed:
        logger.info("Структура базы данных успешно обновлена!")
    else:
        logger.info("Структура базы данных не требует обновления.")
    
    logger.info("Проверка структуры таблицы vk_accounts:")
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(vk_accounts)")
    for column in cursor.fetchall():
        logger.info(f"Столбец: {column[1]}, Тип: {column[2]}")
    conn.close() 