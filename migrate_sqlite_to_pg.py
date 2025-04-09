#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio
import sqlite3
import asyncpg
import os
import logging
from datetime import datetime, timezone
from typing import Optional
from cryptography.fernet import Fernet
from dotenv import load_dotenv
import sys

# --- Настройка --- 
load_dotenv()

SQLITE_DB_PATH = os.getenv('SQLITE_DB_PATH', 'users.db') # Путь к исходной SQLite БД
# Возвращаем DATABASE_URL
DATABASE_URL = os.getenv('DATABASE_URL') 
ENCRYPTION_KEY = os.getenv('ENCRYPTION_KEY')

# Настройка логирования
log_format = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_format)
logger = logging.getLogger("migration")

# Проверки
if not os.path.exists(SQLITE_DB_PATH):
    logger.error(f"Ошибка: Файл SQLite не найден по пути: {SQLITE_DB_PATH}")
    sys.exit(1)

# Проверяем DATABASE_URL
if not DATABASE_URL:
    logger.error("Ошибка: DATABASE_URL не установлен в .env")
    sys.exit(1)

if not ENCRYPTION_KEY:
    logger.error("Ошибка: ENCRYPTION_KEY не установлен в .env")
    sys.exit(1)

try:
    cipher = Fernet(ENCRYPTION_KEY.encode())
    logger.info("Ключ шифрования успешно загружен.")
except Exception as e:
    logger.error(f"Ошибка инициализации шифрования: {e}")
    sys.exit(1)

# --- Функции преобразования данных --- 

def parse_datetime(value: Optional[str]) -> Optional[datetime]:
    """Преобразует строку ISO или timestamp в datetime объект с UTC.
       Возвращает None, если входное значение None или пустое.
    """
    if not value:
        return None
    try:
        # Попробуем как ISO формат (стандартный для datetime.isoformat())
        dt = datetime.fromisoformat(value)
        # Если нет таймзоны, считаем UTC
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        # Если есть таймзона, конвертируем в UTC
        return dt.astimezone(timezone.utc)
    except ValueError:
        # Попробуем как timestamp (float/int)
        try:
            timestamp = float(value)
            return datetime.fromtimestamp(timestamp, tz=timezone.utc)
        except (ValueError, TypeError):
            logger.warning(f"Не удалось распознать формат даты/времени: {value}")
            return None
    except Exception as e:
         logger.error(f"Неожиданная ошибка при парсинге даты '{value}': {e}")
         return None

def sqlite_bool_to_pg(value: Optional[int]) -> Optional[bool]:
    """Преобразует SQLite INTEGER (0/1) в PostgreSQL BOOLEAN."""
    if value is None:
        return None
    return bool(value)

# --- Основная функция миграции --- 
async def migrate_data():
    sqlite_conn = None
    pg_pool = None
    total_users = 0
    total_tg = 0
    total_vk = 0

    try:
        # 1. Подключение к SQLite
        logger.info(f"Подключение к SQLite: {SQLITE_DB_PATH}")
        sqlite_conn = sqlite3.connect(SQLITE_DB_PATH, timeout=30.0) # Увеличим таймаут на всякий случай
        sqlite_conn.row_factory = sqlite3.Row # Возвращать строки как словари
        cursor = sqlite_conn.cursor()
        logger.info("Подключение к SQLite успешно.")

        # 2. Подключение к PostgreSQL
        logger.info("Подключение к PostgreSQL...")
        try:
            # Используем DATABASE_URL для создания пула
            pg_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)

            # Проверка соединения
            async with pg_pool.acquire() as conn:
                 await conn.fetchval("SELECT 1")
            logger.info("Подключение к PostgreSQL и пулу успешно.")
        except Exception as e:
            logger.error(f"Не удалось подключиться к PostgreSQL: {e}")
            raise # Перевыбрасываем, чтобы попасть в общий except блок

        # 3. Чтение данных из SQLite
        logger.info("Чтение данных из SQLite...")
        cursor.execute("SELECT * FROM users")
        sqlite_users = cursor.fetchall()
        total_users = len(sqlite_users)
        logger.info(f"Прочитано {total_users} пользователей.")

        cursor.execute("SELECT * FROM telegram_accounts")
        sqlite_tg_accounts = cursor.fetchall()
        total_tg = len(sqlite_tg_accounts)
        logger.info(f"Прочитано {total_tg} Telegram аккаунтов.")

        cursor.execute("SELECT * FROM vk_accounts")
        sqlite_vk_accounts = cursor.fetchall()
        total_vk = len(sqlite_vk_accounts)
        logger.info(f"Прочитано {total_vk} VK аккаунтов.")

        # Закрываем соединение SQLite после чтения
        sqlite_conn.close()
        logger.info("Соединение с SQLite закрыто.")
        sqlite_conn = None # Помечаем, что соединение закрыто

        # 4. Подготовка данных для PostgreSQL
        logger.info("Подготовка данных для PostgreSQL...")
        
        pg_users_data = []
        for u in sqlite_users:
            pg_users_data.append((
                u['api_key'],
                u['username'],
                u['password'],
                parse_datetime(u['created_at']),
                parse_datetime(u['last_used']),
                u['vk_token'] # Переносим как есть (зашифровано)
            ))

        pg_tg_accounts_data = []
        for tg in sqlite_tg_accounts:
            pg_tg_accounts_data.append((
                tg['id'],
                tg['user_api_key'],
                tg['api_id'],
                tg['api_hash'],
                tg['phone'],
                tg['proxy'],
                tg['status'],
                tg['session_file'],
                tg['requests_count'],
                float(tg['last_request_time']) if tg['last_request_time'] is not None else None,
                parse_datetime(tg['added_at']),
                tg['session_string'],
                tg['phone_code_hash'],
                sqlite_bool_to_pg(tg['is_active']),
                tg['request_limit'],
                parse_datetime(tg['last_used'])
            ))

        pg_vk_accounts_data = []
        for vk in sqlite_vk_accounts:
            pg_vk_accounts_data.append((
                vk['id'],
                vk['user_api_key'],
                vk['token'], # Переносим как есть (зашифровано)
                vk['proxy'],
                vk['status'],
                vk['requests_count'],
                float(vk['last_request_time']) if vk['last_request_time'] is not None else None,
                parse_datetime(vk['added_at']),
                vk['user_id'],
                vk['user_name'],
                vk['error_message'],
                vk['error_code'],
                parse_datetime(vk['last_checked_at']),
                sqlite_bool_to_pg(vk['is_active']),
                vk['request_limit'],
                parse_datetime(vk['last_used'])
            ))
        
        logger.info("Данные подготовлены.")

        # 5. Запись данных в PostgreSQL
        async with pg_pool.acquire() as conn:
            async with conn.transaction():
                logger.info("Начало транзакции в PostgreSQL. Очистка существующих таблиц...")
                # Очищаем таблицы перед вставкой (ВАЖНО: убедитесь, что это желаемое поведение)
                # Порядок важен из-за внешних ключей (сначала дочерние)
                await conn.execute("DELETE FROM telegram_accounts")
                await conn.execute("DELETE FROM vk_accounts")
                await conn.execute("DELETE FROM users")
                logger.info("Таблицы PostgreSQL очищены.")

                logger.info(f"Вставка {len(pg_users_data)} пользователей...")
                if pg_users_data:
                     await conn.copy_records_to_table(
                         'users', 
                         records=pg_users_data,
                         columns=['api_key', 'username', 'password', 'created_at', 'last_used', 'vk_token']
                     )
                logger.info("Пользователи вставлены.")
                
                logger.info(f"Вставка {len(pg_tg_accounts_data)} Telegram аккаунтов...")
                if pg_tg_accounts_data:
                    await conn.copy_records_to_table(
                        'telegram_accounts', 
                        records=pg_tg_accounts_data,
                        columns=['id', 'user_api_key', 'api_id', 'api_hash', 'phone', 'proxy', 'status', 'session_file',
                                 'requests_count', 'last_request_time', 'added_at', 'session_string', 'phone_code_hash',
                                 'is_active', 'request_limit', 'last_used']
                    )
                logger.info("Telegram аккаунты вставлены.")

                logger.info(f"Вставка {len(pg_vk_accounts_data)} VK аккаунтов...")
                if pg_vk_accounts_data:
                    await conn.copy_records_to_table(
                        'vk_accounts', 
                        records=pg_vk_accounts_data,
                        columns=['id', 'user_api_key', 'token', 'proxy', 'status', 'requests_count',
                                 'last_request_time', 'added_at', 'user_id', 'user_name', 'error_message',
                                 'error_code', 'last_checked_at', 'is_active', 'request_limit', 'last_used']
                    )
                logger.info("VK аккаунты вставлены.")
                
                logger.info("Коммит транзакции в PostgreSQL...")
            # Транзакция завершена (commit или rollback при ошибке)
            logger.info("Транзакция успешно завершена.")

        logger.info("--- Миграция данных успешно завершена! ---")
        logger.info(f"Перенесено: Пользователей={total_users}, TG аккаунтов={total_tg}, VK аккаунтов={total_vk}")

    except sqlite3.Error as e:
        logger.error(f"Ошибка SQLite: {e}")
        import traceback
        logger.error(traceback.format_exc())
    except asyncpg.PostgresError as e:
        logger.error(f"Ошибка PostgreSQL: {e}")
        import traceback
        logger.error(traceback.format_exc())
    except ConnectionError as e:
         logger.error(f"Ошибка соединения: {e}")
    except Exception as e:
        logger.error(f"Неожиданная ошибка во время миграции: {e}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        if sqlite_conn:
            try:
                sqlite_conn.close()
                logger.info("Соединение SQLite закрыто (в блоке finally).")
            except Exception as e:
                 logger.error(f"Ошибка при закрытии SQLite соединения: {e}")
        if pg_pool:
            try:
                await pg_pool.close()
                logger.info("Пул соединений PostgreSQL закрыт.")
            except Exception as e:
                 logger.error(f"Ошибка при закрытии пула PostgreSQL: {e}")

# --- Запуск миграции --- 
if __name__ == "__main__":
    logger.info("Запуск скрипта миграции данных из SQLite в PostgreSQL...")
    asyncio.run(migrate_data())
    logger.info("Скрипт миграции завершил работу.") 