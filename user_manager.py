# Стандартные библиотеки
import asyncio
import logging
import os
import secrets
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional

# Сторонние библиотеки (требуют установки)
from cryptography.fernet import Fernet 
from dotenv import load_dotenv 
import asyncpg 

load_dotenv()

# Настройка логирования
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(levelname)s - %(name)s - %(message)s'
# )
logger = logging.getLogger(__name__)

# Константы
MAX_REQUESTS_PER_ACCOUNT = 1000
MAX_ACTIVE_ACCOUNTS = 5
# Возвращаем чтение DATABASE_URL
DATABASE_URL = os.getenv('DATABASE_URL')
print(f"Loaded DATABASE_URL: '{DATABASE_URL}'")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL не установлен в переменных окружения.")

# Настройка шифрования
ENCRYPTION_KEY = os.getenv('ENCRYPTION_KEY', 'default_encryption_key_must_be_32_bytes_').encode()
cipher = Fernet(ENCRYPTION_KEY)

# --- Глобальный пул соединений ---
# Используем пул для управления соединениями
db_pool = None

async def get_db_pool():
    """Инициализирует и возвращает пул соединений asyncpg."""
    global db_pool
    if db_pool is None:
        try:
            # Убираем отладку
            # logger.info(f"[DEBUG] Попытка создать пул с параметрами:")
            # ...
            
            # Снова используем DATABASE_URL для создания пула
            db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)

            logger.info("Пул соединений с PostgreSQL успешно создан.")
            # Можно добавить тестовый запрос для проверки
            async with db_pool.acquire() as conn:
                 await conn.fetchval("SELECT 1")
            logger.info("Тестовый запрос к PostgreSQL выполнен успешно.")
        except Exception as e:
            logger.error(f"Ошибка создания пула соединений PostgreSQL: {e}")
            db_pool = None # Сбрасываем пул при ошибке
            raise # Перевыбрасываем исключение
    return db_pool

async def get_db_connection():
    """Асинхронно получает соединение из пула."""
    pool = await get_db_pool()
    if pool is None:
         raise ConnectionError("Не удалось получить пул соединений с БД.")
    # Получаем соединение из пула
    # conn = await pool.acquire() 
    # return conn
    # Возвращаем сам пул, чтобы использовать 'async with pool.acquire() as conn:'
    return pool 

async def initialize_database():
    """Асинхронно инициализирует базу данных PostgreSQL, если таблицы не существуют."""
    try:
        pool = await get_db_pool() # Убедимся, что пул инициализирован
        if pool:
             async with pool.acquire() as conn: # Получаем соединение
                 async with conn.transaction(): # Используем транзакцию для инициализации
                    await init_db(conn) # Передаем соединение в init_db
             logger.info("Инициализация схемы базы данных PostgreSQL завершена.")
        else:
             logger.error("Не удалось инициализировать БД: пул соединений недоступен.")
             
    except Exception as e:
         logger.error(f"Критическая ошибка при инициализации базы данных PostgreSQL: {e}")
         import traceback
         logger.error(traceback.format_exc())
         # Возможно, стоит завершить приложение, если БД недоступна при старте

async def init_db(conn: asyncpg.Connection):
    """Асинхронно инициализирует базу данных, создает таблицы PostgreSQL, если они не существуют."""
    # Используем переданное соединение conn
    logger.info(f"Проверка и инициализация таблиц в PostgreSQL...")

    # --- Создание таблиц ---
    # Используем синтаксис PostgreSQL
    # VARCHAR(36) или UUID для ключей
    # TIMESTAMPTZ для дат/времени
    # BOOLEAN для флагов
    # DOUBLE PRECISION (или FLOAT) для REAL
    # TEXT для длинных строк
    await conn.execute('''
    CREATE TABLE IF NOT EXISTS users (
        api_key VARCHAR(36) PRIMARY KEY, 
        username TEXT, 
        password TEXT,
        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, 
        last_used TIMESTAMPTZ, 
        vk_token TEXT 
    )''')
    logger.info("Таблица 'users' проверена/создана.")
    
    await conn.execute('''
    CREATE TABLE IF NOT EXISTS telegram_accounts (
        id VARCHAR(36) PRIMARY KEY, 
        user_api_key VARCHAR(36) REFERENCES users(api_key) ON DELETE CASCADE, 
        api_id INTEGER, 
        api_hash TEXT,
        phone TEXT, 
        proxy TEXT, 
        status TEXT, 
        session_file TEXT,
        requests_count INTEGER DEFAULT 0, 
        last_request_time DOUBLE PRECISION, -- или TIMESTAMPTZ? time.time() возвращает float
        added_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, 
        session_string TEXT, 
        phone_code_hash TEXT,
        is_active BOOLEAN DEFAULT TRUE, 
        request_limit INTEGER DEFAULT 1000,
        last_used TIMESTAMPTZ
    )''')
    logger.info("Таблица 'telegram_accounts' проверена/создана.")

    await conn.execute('''
    CREATE TABLE IF NOT EXISTS vk_accounts (
        id VARCHAR(36) PRIMARY KEY, 
        user_api_key VARCHAR(36) REFERENCES users(api_key) ON DELETE CASCADE, 
        token TEXT, 
        proxy TEXT,
        status TEXT, 
        requests_count INTEGER DEFAULT 0, 
        last_request_time DOUBLE PRECISION, -- или TIMESTAMPTZ?
        added_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, 
        user_id BIGINT, -- Используем BIGINT для ID VK
        user_name TEXT, 
        error_message TEXT,
        error_code INTEGER, 
        last_checked_at TIMESTAMPTZ, 
        is_active BOOLEAN DEFAULT TRUE,
        request_limit INTEGER DEFAULT 1000, 
        last_used TIMESTAMPTZ
    )''')
    logger.info("Таблица 'vk_accounts' проверена/создана.")
    # --- Конец создания таблиц ---

    # --- Добавление столбцов (с использованием PostgreSQL) ---
    logger.debug("Проверка и добавление недостающих столбцов...")
    await check_and_add_column(conn, "telegram_accounts", "session_string", "TEXT")
    await check_and_add_column(conn, "telegram_accounts", "phone_code_hash", "TEXT")
    await check_and_add_column(conn, "telegram_accounts", "is_active", "BOOLEAN DEFAULT TRUE")
    await check_and_add_column(conn, "telegram_accounts", "request_limit", "INTEGER DEFAULT 1000")
    await check_and_add_column(conn, "telegram_accounts", "last_used", "TIMESTAMPTZ")

    await check_and_add_column(conn, "vk_accounts", "user_id", "BIGINT")
    await check_and_add_column(conn, "vk_accounts", "user_name", "TEXT")
    await check_and_add_column(conn, "vk_accounts", "error_message", "TEXT")
    await check_and_add_column(conn, "vk_accounts", "error_code", "INTEGER")
    await check_and_add_column(conn, "vk_accounts", "last_checked_at", "TIMESTAMPTZ")
    await check_and_add_column(conn, "vk_accounts", "is_active", "BOOLEAN DEFAULT TRUE")
    await check_and_add_column(conn, "vk_accounts", "request_limit", "INTEGER DEFAULT 1000")
    await check_and_add_column(conn, "vk_accounts", "last_used", "TIMESTAMPTZ")
    logger.debug("Проверка столбцов завершена.")
    # --- Конец добавления столбцов ---

    # Коммит не нужен явно, так как используется 'async with conn.transaction()'

    # --- Проверка структуры таблиц (опционально, для логов) ---
    logger.debug("Проверка структуры таблиц...")
    for table in ['users', 'telegram_accounts', 'vk_accounts']:
        try:
            # Запрос к information_schema для PostgreSQL
            columns = await conn.fetch(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = 'public' AND table_name = $1
                ORDER BY ordinal_position;
            """, table)
            if columns:
                column_names = [col['column_name'] for col in columns]
                logger.info(f"Структура таблицы {table}: {', '.join(column_names)}")
            else:
                # Это не должно произойти, если CREATE TABLE IF NOT EXISTS сработал
                logger.warning(f"Не удалось получить структуру таблицы {table} (возможно, она не создана).")
        except Exception as e:
            logger.error(f"Ошибка при проверке структуры таблицы {table}: {e}")
    logger.info("Инициализация таблиц в PostgreSQL завершена.")
    # --- Конец init_db ---


# Функция проверки и добавления столбца для PostgreSQL
async def check_and_add_column(conn: asyncpg.Connection, table_name, column_name, column_type):
    """Асинхронно проверяет наличие столбца в таблице PostgreSQL и добавляет его, если он отсутствует."""
    try:
        # Проверяем существование столбца в information_schema
        exists = await conn.fetchval(f"""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.columns 
                WHERE table_schema = 'public' 
                  AND table_name = $1 
                  AND column_name = $2
            );
        """, table_name, column_name)

        if not exists:
            # Используем безопасное форматирование для имен таблиц/столбцов
            # Хотя table_name и column_name здесь контролируются кодом, это хорошая практика
            await conn.execute(f'ALTER TABLE public.{table_name} ADD COLUMN "{column_name}" {column_type}')
            logger.info(f"Столбец '{column_name}' успешно добавлен в таблицу '{table_name}'")
        # else:
             # logger.debug(f"Столбец '{column_name}' уже существует в таблице '{table_name}'.")

    except asyncpg.PostgresError as e: # Ловим ошибки PostgreSQL
        # Особо обрабатываем ошибку, если таблица не существует (хотя CREATE TABLE IF NOT EXISTS должен это предотвратить)
        if isinstance(e, asyncpg.exceptions.UndefinedTableError):
             logger.warning(f"Таблица {table_name} не найдена при попытке добавить столбец {column_name}. Ошибка: {e}")
        else:
             logger.error(f"Ошибка PostgreSQL при добавлении столбца {column_name} в таблицу {table_name}: {e}")
             # Можно перевыбросить или обработать иначе
             # raise
    except Exception as e:
        logger.error(f"Неожиданная ошибка при добавлении столбца {column_name} в таблицу {table_name}: {e}")
        # raise

async def register_user(username: str | None = None, password: str | None = None) -> Optional[str]:
    """Асинхронно регистрирует нового пользователя и возвращает его API ключ."""
    api_key = str(uuid.uuid4())
    # Используем UTC для created_at
    created_at = datetime.now(timezone.utc) 
    pool = None
    try:
        pool = await get_db_connection() 
        if not pool:
            logger.error("Не удалось получить пул соединений в register_user")
            return None 

        async with pool.acquire() as conn: # Получаем соединение из пула
             async with conn.transaction(): # Используем транзакцию
                # Используем плейсхолдеры $1, $2, ...
                await conn.execute(
                    'INSERT INTO users (api_key, username, password, created_at) VALUES ($1, $2, $3, $4)',
                    api_key, username, password, created_at
                )
                # commit() не нужен явно при использовании 'async with conn.transaction()'
    
        logger.info(f"Зарегистрирован новый пользователь с API ключом: {api_key}")
        return api_key
    # Ловим ошибки asyncpg
    except asyncpg.PostgresError as e: 
        logger.error(f"Ошибка PostgreSQL в register_user: {e}")
        return None 
    except ConnectionError as e: # Ошибка получения пула
         logger.error(f"Ошибка соединения в register_user: {e}")
         return None
    except Exception as e:
        logger.error(f"Неожиданная ошибка в register_user: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None
    # finally: 
        # Закрытие соединения не нужно, пул управляет этим
        # if conn:
        #    await pool.release(conn) # Возвращаем соединение в пул

async def get_user(api_key: str) -> Optional[Dict]:
    """Асинхронно получает информацию о пользователе и его аккаунтах."""
    pool = None
    user_dict = None
    try:
        pool = await get_db_connection()  
        if not pool:
            return None

        async with pool.acquire() as conn:  
            # Получаем пользователя
            user_record = await conn.fetchrow('SELECT * FROM users WHERE api_key = $1', api_key)  
            
            if user_record:
                user_dict = dict(user_record) # Преобразуем Record в dict
                
                # Получаем Telegram аккаунты
                tg_records = await conn.fetch('SELECT * FROM telegram_accounts WHERE user_api_key = $1', api_key)  
                user_dict['telegram_accounts'] = [dict(acc) for acc in tg_records]

                # Получаем VK аккаунты
                vk_records = await conn.fetch('SELECT * FROM vk_accounts WHERE user_api_key = $1', api_key)
                
                processed_vk_accounts = []
                for acc_row in vk_records:
                    acc = dict(acc_row)
                    encrypted_token_str = acc.get('token')
                    if encrypted_token_str:
                        try:
                            # Пытаемся расшифровать
                            acc['token'] = cipher.decrypt(encrypted_token_str.encode()).decode()
                            # logger.debug(f"VK Token decrypted successfully for account {acc.get('id')}")
                        except Exception as e:
                            # Если не удалось, проверяем, не сохранен ли он уже в незашифрованном виде
                            if isinstance(encrypted_token_str, str) and encrypted_token_str.startswith('vk1.a.'):
                                 logger.warning(f"VK Token for account {acc.get('id')} seems unencrypted. Using as is.")
                                 acc['token'] = encrypted_token_str # Используем как есть
                            else:
                                 logger.error(f"Failed to decrypt VK token for account {acc.get('id')}, and it's not a valid unencrypted token. Error: {e}")
                                 acc['token'] = None # Ошибка расшифровки и формат неверный
                    else:
                         acc['token'] = None # Токена нет
                    processed_vk_accounts.append(acc)
                
                user_dict['vk_accounts'] = processed_vk_accounts
                return user_dict

            return None  # Пользователь не найден
            
    # Ловим ошибки asyncpg
    except asyncpg.PostgresError as e: 
        logger.error(f"Ошибка PostgreSQL в get_user для {api_key}: {e}")
        return None
    except ConnectionError as e:
         logger.error(f"Ошибка соединения в get_user для {api_key}: {e}")
         return None
    except Exception as e:
        logger.error(f"Неожиданная ошибка в get_user для {api_key}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None
    # finally:
        # Соединение возвращается в пул автоматически через 'async with'
        # if conn:
        #    await pool.release(conn)

async def update_user_last_used(api_key: str) -> None:
    """Асинхронно обновляет время последнего использования пользователя."""
    pool = None
    try:
        pool = await get_db_connection()  
        if not pool: return

        # Обновление выполняется в одной команде, транзакция не обязательна,
        # но и не повредит для консистентности.
        async with pool.acquire() as conn:  
            await conn.execute('UPDATE users SET last_used = $1 WHERE api_key = $2', 
                             datetime.now(timezone.utc), api_key)
    except asyncpg.PostgresError as e:
        logger.error(f"Ошибка PostgreSQL в update_user_last_used для {api_key}: {e}")
    except ConnectionError as e:
         logger.error(f"Ошибка соединения в update_user_last_used для {api_key}: {e}")
    except Exception as e:
        logger.error(f"Неожиданная ошибка в update_user_last_used для {api_key}: {e}")
        import traceback
        logger.error(traceback.format_exc())
    # finally:
        # Соединение возвращается в пул автоматически

async def add_telegram_account(api_key: str, account_data: Dict) -> Optional[str]:
    """Асинхронно добавляет аккаунт Telegram и возвращает его ID."""
    # Проверка пользователя выполняется внутри, get_user использует пул
    # user = await get_user(api_key) 
    # if not user:
    #     logger.error(f"Пользователь с api_key={api_key} не найден для добавления TG аккаунта.")
    #     return None

    account_id = account_data.get('id') or str(uuid.uuid4())
    # Используем UTC
    added_at = datetime.now(timezone.utc)
    pool = None
    try:
        pool = await get_db_connection()  
        if not pool: return None

        async with pool.acquire() as conn:  
            async with conn.transaction(): # Используем транзакцию для INSERT
                await conn.execute('''
    INSERT INTO telegram_accounts 
    (id, user_api_key, api_id, api_hash, phone, proxy, status, session_file,
                 requests_count, last_request_time, added_at, session_string, phone_code_hash, 
                 is_active, request_limit, last_used)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
    ''', 
        account_id, 
        api_key, 
        account_data.get('api_id'), 
        account_data.get('api_hash'),
        account_data.get('phone'), 
        account_data.get('proxy'), 
        account_data.get('status', 'new'), 
        account_data.get('session_file'), 
        0, # requests_count
        account_data.get('last_request_time'), # last_request_time (Double precision)
        added_at, # added_at (TIMESTAMPTZ)
        account_data.get('session_string', ''), 
        account_data.get('phone_code_hash', ''),
        account_data.get('is_active', True), # is_active (Boolean)
        account_data.get('request_limit', 1000), # request_limit (INTEGER)
        account_data.get('last_used') # last_used (TIMESTAMPTZ or None)
      )
            logger.info(f"Добавлен Telegram аккаунт {account_id} для пользователя {api_key}")
            return account_id 
    except asyncpg.PostgresError as e:
        # Проверяем на нарушение внешнего ключа (если пользователь удален)
        if isinstance(e, asyncpg.exceptions.ForeignKeyViolationError):
             logger.error(f"Не удалось добавить TG аккаунт: пользователь с api_key={api_key} не найден. Ошибка: {e}")
        else:
             logger.error(f"Ошибка PostgreSQL в add_telegram_account для {api_key}: {e}")
        return None
    except ConnectionError as e:
         logger.error(f"Ошибка соединения в add_telegram_account для {api_key}: {e}")
         return None
    except Exception as e:
        logger.error(f"Неожиданная ошибка в add_telegram_account для {api_key}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None
    # finally:
        # Пул управляет соединением

async def update_telegram_account(api_key: str, account_id: str, account_data: Dict) -> bool:
    """Обновляет данные аккаунта Telegram."""
    pool = None
    try:
        pool = await get_db_connection()
        if not pool:
            return False

        async with pool.acquire() as conn:
            # Проверка существования записи не обязательна, UPDATE просто ничего не сделает,
            # но можно оставить для ясности или возврата False, если запись не найдена.
            # result = await conn.fetchval('SELECT 1 FROM telegram_accounts WHERE id = $1 AND user_api_key = $2', 
            #                             account_id, api_key)
            # if not result:
            #     logger.warning(f"Попытка обновить несуществующий TG аккаунт: id={account_id}, api_key={api_key}")
            #     return False

            # Формируем SET часть запроса динамически, чтобы обновлять только переданные поля?
            # Пока обновляем все поля, как было.
            rows_affected_str = await conn.execute(''' 
                UPDATE telegram_accounts 
                SET api_id = $1, api_hash = $2, phone = $3, proxy = $4, status = $5, 
                    session_file = $6, session_string = $7, phone_code_hash = $8,
                    is_active = $9, request_limit = $10, last_used = $11 
                WHERE id = $12 AND user_api_key = $13
            ''', 
                account_data.get('api_id'), 
                account_data.get('api_hash'),
                account_data.get('phone'), 
                account_data.get('proxy'),
                account_data.get('status'), 
                account_data.get('session_file'),
                account_data.get('session_string'), 
                account_data.get('phone_code_hash'),
                account_data.get('is_active'), # Boolean or None
                account_data.get('request_limit'), # Integer or None
                account_data.get('last_used'), # TIMESTAMPTZ or None
                account_id,
                api_key
            )

            # execute возвращает строку вида 'UPDATE N', извлекаем N
            rows_affected = int(rows_affected_str.split()[1])
            if rows_affected > 0:
                 logger.info(f"Обновлен TG аккаунт id={account_id} для api_key={api_key}")
                 return True
            else:
                 # Либо аккаунт не найден, либо данные не изменились (но execute все равно вернет 'UPDATE 0')
                 # Проверим, существует ли он вообще
                 exists = await conn.fetchval('SELECT 1 FROM telegram_accounts WHERE id = $1 AND user_api_key = $2', 
                                          account_id, api_key)
                 if not exists:
                     logger.warning(f"Попытка обновить несуществующий TG аккаунт: id={account_id}, api_key={api_key}")
                 else:
                     logger.info(f"Обновление TG аккаунта id={account_id} не привело к изменениям.")
                 return False

    except asyncpg.PostgresError as e:
        logger.error(f"Ошибка PostgreSQL в update_telegram_account (id={account_id}, api_key={api_key}): {e}")
        return False
    except ConnectionError as e:
         logger.error(f"Ошибка соединения в update_telegram_account (id={account_id}, api_key={api_key}): {e}")
         return False
    except Exception as e:
        logger.error(f"Неожиданная ошибка в update_telegram_account (id={account_id}, api_key={api_key}): {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False
    # finally:
        # Пул управляет соединением

async def delete_telegram_account(api_key: str, account_id: str) -> bool:
    """Удаляет аккаунт Telegram."""
    pool = None
    try:
        pool = await get_db_connection()
        if not pool: return False
        
        async with pool.acquire() as conn:
             # Используем транзакцию, хотя здесь одна команда DELETE
             async with conn.transaction(): 
                result_str = await conn.execute(
                    'DELETE FROM telegram_accounts WHERE id = $1 AND user_api_key = $2',
                    account_id, api_key
                )
                # execute возвращает 'DELETE N'
                deleted_count = int(result_str.split()[1])
                if deleted_count > 0:
                    logger.info(f"Удален TG аккаунт id={account_id} для api_key={api_key}")
                    return True
                else:
                    logger.warning(f"Попытка удалить несуществующий TG аккаунт: id={account_id}, api_key={api_key}")
                    return False
    except asyncpg.PostgresError as e:
        logger.error(f"Ошибка PostgreSQL в delete_telegram_account (id={account_id}, api_key={api_key}): {e}")
        return False
    except ConnectionError as e:
         logger.error(f"Ошибка соединения в delete_telegram_account (id={account_id}, api_key={api_key}): {e}")
         return False
    except Exception as e:
        logger.error(f"Неожиданная ошибка в delete_telegram_account (id={account_id}, api_key={api_key}): {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False
    # finally:
        # Пул управляет соединением

async def add_vk_account(api_key: str, account_data: Dict) -> bool:
    """Добавляет аккаунт VK для пользователя."""
    # Проверка пользователя через get_user не нужна, внешний ключ проверит
    # if not await get_user(api_key):
    #     return False
    
    token = account_data.get('token')
    if not token or not isinstance(token, str):
        logger.error("Невалидный токен VK: токен отсутствует или не является строкой")
        return False
    
    token_to_save = None
    if token.startswith('vk1.a.'):
        logger.info("Токен VK имеет правильный формат (vk1.a.). Шифруем перед сохранением.")
        try:
            token_to_save = cipher.encrypt(token.encode()).decode()
        except Exception as e:
            logger.error(f"Ошибка шифрования токена VK для пользователя {api_key}: {e}")
            return False  
    else:
        logger.error("Токен VK имеет неверный формат, должен начинаться с vk1.a.")
        return False

    account_id = account_data.get('id') or str(uuid.uuid4())
    added_at = datetime.now(timezone.utc)
    
    pool = None
    try:
        pool = await get_db_connection()
        if not pool: return False
        
        async with pool.acquire() as conn:
            async with conn.transaction(): # Транзакция для INSERT
                await conn.execute('''
                    INSERT INTO vk_accounts 
                    (id, user_api_key, token, proxy, status, requests_count, 
                     last_request_time, added_at, user_id, user_name, error_message, 
                     error_code, last_checked_at, is_active, request_limit, last_used)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
                ''', 
                    account_id, 
                    api_key, 
                    token_to_save, # Зашифрованный токен
                    account_data.get('proxy'),
                    account_data.get('status', 'active'), 
                    0, # requests_count
                    account_data.get('last_request_time'), # last_request_time
                    added_at, # added_at
                    account_data.get('user_id'), # user_id (BIGINT)
                    account_data.get('user_name'), # user_name (TEXT)
                    account_data.get('error_message'), # error_message (TEXT)
                    account_data.get('error_code'), # error_code (INTEGER)
                    account_data.get('last_checked_at'), # last_checked_at (TIMESTAMPTZ)
                    account_data.get('is_active', True), # is_active (BOOLEAN)
                    account_data.get('request_limit', 1000), # request_limit (INTEGER)
                    account_data.get('last_used') # last_used (TIMESTAMPTZ or None)
                )
            logger.info(f"Добавлен VK аккаунт id={account_id} для api_key={api_key}")
            return True
    except asyncpg.PostgresError as e:
        if isinstance(e, asyncpg.exceptions.ForeignKeyViolationError):
             logger.error(f"Не удалось добавить VK аккаунт: пользователь с api_key={api_key} не найден. Ошибка: {e}")
        else:
             logger.error(f"Ошибка PostgreSQL при добавлении аккаунта VK (api_key={api_key}): {e}")
        return False
    except ConnectionError as e:
         logger.error(f"Ошибка соединения при добавлении аккаунта VK (api_key={api_key}): {e}")
         return False
    except Exception as e:
        logger.error(f"Неожиданная ошибка при добавлении аккаунта VK (api_key={api_key}): {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False
    # finally:
        # Пул управляет соединением

async def update_vk_account(api_key: str, account_id: str, account_data: Dict) -> bool:
    """Обновляет данные аккаунта VK."""
    pool = None
    try:
        pool = await get_db_connection()
        if not pool: return False
        
        async with pool.acquire() as conn:
            
            token = account_data.get('token')
            token_to_update_encrypted = None 
            update_token = False # Флаг, нужно ли обновлять токен

            if token:
                update_token = True # Помечаем, что токен нужно обновить
                if token.startswith('vk1.a.'):
                    logger.info(f"Токен VK для обновления ({account_id}) имеет правильный формат. Шифруем.")
                    try:
                        token_to_update_encrypted = cipher.encrypt(token.encode()).decode()
                    except Exception as e:
                        logger.error(f"Ошибка шифрования токена VK при обновлении аккаунта {account_id}: {e}")
                        return False 
                else:
                    logger.error(f"Токен VK для обновления ({account_id}) имеет неверный формат.")
                    return False

            # --- Формируем запрос UPDATE --- 
            set_clauses = []
            params = []
            param_index = 1

            # Добавляем поля для обновления, кроме токена
            fields_to_update = {
                'proxy': account_data.get('proxy'), 
                'status': account_data.get('status'),
                'user_id': account_data.get('user_id'),
                'user_name': account_data.get('user_name'),
                'error_message': account_data.get('error_message'),
                'error_code': account_data.get('error_code'),
                'last_checked_at': account_data.get('last_checked_at'),
                'is_active': account_data.get('is_active'),
                'request_limit': account_data.get('request_limit'),
                'last_used': account_data.get('last_used')
            }

            for field, value in fields_to_update.items():
                 if value is not None: # Обновляем, только если значение передано
                     # Правильный синтаксис f-строки для экранирования кавычек
                     set_clauses.append(f'"{field}" = ${param_index}') 
                     params.append(value)
                     param_index += 1
            
            # Добавляем токен, если он обновляется
            if update_token:
                 set_clauses.append(f'token = ${param_index}')
                 params.append(token_to_update_encrypted)
                 param_index += 1
                 
            if not set_clauses: # Нечего обновлять
                 logger.info(f"Нет данных для обновления VK аккаунта id={account_id}")
                 return False # Или True, если считать это успехом?
                 
            # Добавляем условия WHERE
            params.append(account_id)
            where_clause = f"WHERE id = ${param_index}"
            param_index += 1
            params.append(api_key)
            where_clause += f" AND user_api_key = ${param_index}"

            update_query = f"UPDATE vk_accounts SET {', '.join(set_clauses)} {where_clause}"
            
            # logger.debug(f"Выполнение UPDATE VK: {update_query} с параметрами: {params}")
            result_str = await conn.execute(update_query, *params)
            
            rows_affected = int(result_str.split()[1])
            
            if rows_affected > 0:
                logger.info(f"Обновлен VK аккаунт id={account_id} для api_key={api_key}")
                return True
            else:
                 # Проверяем, существует ли аккаунт
                 exists = await conn.fetchval('SELECT 1 FROM vk_accounts WHERE id = $1 AND user_api_key = $2', 
                                          account_id, api_key)
                 if not exists:
                     logger.warning(f"Попытка обновить несуществующий VK аккаунт: id={account_id}, api_key={api_key}")
                 else:
                     logger.info(f"Обновление VK аккаунта id={account_id} не привело к изменениям.")
                 return False

    except asyncpg.PostgresError as e:
        logger.error(f"Ошибка PostgreSQL при обновлении аккаунта VK (id={account_id}, api_key={api_key}): {e}")
        return False
    except ConnectionError as e:
         logger.error(f"Ошибка соединения при обновлении аккаунта VK (id={account_id}, api_key={api_key}): {e}")
         return False
    except Exception as e:
        logger.error(f"Неожиданная ошибка при обновлении аккаунта VK (id={account_id}, api_key={api_key}): {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False
    # finally:
        # Пул управляет соединением

async def delete_vk_account(api_key: str, account_id: str) -> bool:
    """Удаляет аккаунт VK."""
    pool = None
    try:
        pool = await get_db_connection()
        if not pool: return False
        
        async with pool.acquire() as conn:
             async with conn.transaction(): 
                result_str = await conn.execute(
                    'DELETE FROM vk_accounts WHERE id = $1 AND user_api_key = $2',
                    account_id, api_key
                )
                deleted_count = int(result_str.split()[1])
                if deleted_count > 0:
                    logger.info(f"Удален VK аккаунт id={account_id} для api_key={api_key}")
                    return True
                else:
                    logger.warning(f"Попытка удалить несуществующий VK аккаунт: id={account_id}, api_key={api_key}")
                    return False
    except asyncpg.PostgresError as e:
        logger.error(f"Ошибка PostgreSQL при удалении аккаунта VK (id={account_id}, api_key={api_key}): {e}")
        return False
    except ConnectionError as e:
         logger.error(f"Ошибка соединения при удалении аккаунта VK (id={account_id}, api_key={api_key}): {e}")
         return False
    except Exception as e:
        logger.error(f"Неожиданная ошибка при удалении аккаунта VK (id={account_id}, api_key={api_key}): {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False
    # finally:
        # Пул управляет соединением

async def get_active_accounts(api_key: str, platform: str) -> List[Dict]:
    """Асинхронно получает список активных аккаунтов для пользователя."""
    if platform not in ['telegram', 'vk']:
        logger.error(f"Неверная платформа '{platform}' запрошена для api_key={api_key}")
        return []

    table = 'telegram_accounts' if platform == 'telegram' else 'vk_accounts'
    accounts = []
    pool = None 

    try: 
        pool = await get_db_connection() 
        if not pool:
            logger.error(f"Не удалось получить пул соединений для get_active_accounts ({api_key}, {platform})")
            return [] 

        async with pool.acquire() as conn:
            # Используем $1 для api_key и $2 для is_active=True
            records = await conn.fetch(
                f"SELECT * FROM {table} WHERE user_api_key = $1 AND is_active = $2",
                api_key, True 
            )
            # logger.debug(f"Получено {len(records)} строк из БД.")
            for record in records:
                accounts.append(dict(record)) # Преобразуем asyncpg.Record в dict

        # Расшифровываем токены VK, если нужно
        if platform == 'vk':
            # logger.debug(f"Расшифровка токенов для {len(accounts)} VK аккаунтов...")
            for acc in accounts:
                encrypted_token_str = acc.get('token')
                if encrypted_token_str:
                    try:
                        acc['token'] = cipher.decrypt(encrypted_token_str.encode()).decode()
                    except Exception as e:
                        if isinstance(encrypted_token_str, str) and encrypted_token_str.startswith('vk1.a.'):
                             logger.warning(f"VK Token for account {acc.get('id')} in get_active_accounts seems unencrypted.")
                             acc['token'] = encrypted_token_str 
                        else:
                             logger.error(f"Failed to decrypt VK token for account {acc.get('id')} in get_active_accounts. Error: {e}")
                             acc['token'] = None 
                else:
                    acc['token'] = None

        logger.debug(f"Получено {len(accounts)} активных {platform} аккаунтов для {api_key}")
        return accounts 

    except asyncpg.PostgresError as e:
        logger.error(f"Ошибка PostgreSQL при получении активных {platform} аккаунтов для {api_key}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return [] 
    except ConnectionError as e:
         logger.error(f"Ошибка соединения при получении активных {platform} аккаунтов для {api_key}: {e}")
         return []
    except Exception as e:
        logger.error(f"Неожиданная ошибка при получении активных {platform} аккаунтов для {api_key}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return [] 
    # finally:
        # Пул управляет соединением

async def get_next_available_account(api_key: str, platform: str) -> Optional[Dict]:
    """Получает следующий доступный аккаунт для использования."""
    active_accounts = await get_active_accounts(api_key, platform)
    if not active_accounts:
        return None
    
    if len(active_accounts) == 1:
        account = active_accounts[0]
        account["degraded_mode"] = True
        return account
    
    return active_accounts[0]

async def get_next_available_account_async(api_key: str, platform: str) -> Optional[Dict]:
    """Асинхронная версия получения следующего доступного аккаунта."""
    active_accounts = await get_active_accounts(api_key, platform)
    if not active_accounts:
        return None
    
    if len(active_accounts) == 1:
        account = active_accounts[0]
        account["degraded_mode"] = True
        return account
    
    return active_accounts[0]

async def update_account_usage(api_key: str, account_id: str, platform: str, token_expired: bool = False) -> bool:
    """Асинхронно обновляет статистику использования аккаунта."""
    pool = None
    try:
        pool = await get_db_connection()
        if not pool: return False
        
        async with pool.acquire() as conn:
            table = 'telegram_accounts' if platform == 'telegram' else 'vk_accounts'
            # Формируем SET часть запроса
            set_clauses = [
                 "requests_count = requests_count + 1",
                 "last_request_time = $1",
                 "last_used = $2"
            ]
            params = [time.time(), datetime.now(timezone.utc)]
            param_idx = 3 # Начинаем с $3
            
            if token_expired:
                 set_clauses.append(f"status = ${param_idx}")
                 params.append('inactive')
                 param_idx += 1
                 set_clauses.append(f"is_active = ${param_idx}")
                 params.append(False) # Деактивируем
                 param_idx += 1
            
            # Добавляем условия WHERE
            where_clause = f"WHERE id = ${param_idx} AND user_api_key = ${param_idx + 1}"
            params.extend([account_id, api_key])
            
            query = f"UPDATE {table} SET {', '.join(set_clauses)} {where_clause}"
            
            # logger.debug(f"Выполнение UPDATE USAGE: {query} с параметрами: {params}")
            result_str = await conn.execute(query, *params)
            
            rows_affected = int(result_str.split()[1])
            if rows_affected > 0:
                 # logger.info(f"Обновлено использование аккаунта {platform}:{account_id} для {api_key}")
                 return True
            else:
                 logger.warning(f"Не удалось обновить использование (аккаунт не найден?): {platform}:{account_id} для {api_key}")
                 return False

    except asyncpg.PostgresError as e:
        logger.error(f"Ошибка PostgreSQL при обновлении использования аккаунта {platform}:{account_id}: {e}")
        return False
    except ConnectionError as e:
         logger.error(f"Ошибка соединения при обновлении использования аккаунта {platform}:{account_id}: {e}")
         return False
    except Exception as e:
        logger.error(f"Неожиданная ошибка при обновлении использования аккаунта {platform}:{account_id}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False
    # finally:
        # Пул управляет соединением

async def set_vk_token(api_key: str, vk_token: str) -> bool:
    """Устанавливает VK токен для пользователя (в таблице users)."""
    if not isinstance(vk_token, str) or not vk_token.startswith('vk1.a.'):
        logger.error(f"Неверный формат токена VK для set_vk_token: {vk_token}")
        return False

    encrypted_token = None
    try:
        encrypted_token = cipher.encrypt(vk_token.encode()).decode()
    except Exception as e:
        logger.error(f"Ошибка шифрования токена VK (users) для {api_key}: {e}")
        return False

    pool = None
    try:
        pool = await get_db_connection()
        if not pool: return False
        
        async with pool.acquire() as conn:
            result_str = await conn.execute('''
            UPDATE users
            SET vk_token = $1
            WHERE api_key = $2
            ''', encrypted_token, api_key)  

            rows_affected = int(result_str.split()[1])
            if rows_affected > 0:
                logger.info(f"Успешно установлен зашифрованный VK токен (users) для {api_key}")
                return True
            else:
                logger.warning(f"Не удалось обновить VK токен (users) для {api_key}, пользователь не найден при UPDATE?")
                return False

    except asyncpg.PostgresError as e:
        logger.error(f"Ошибка PostgreSQL при обновлении токена VK (users) для {api_key}: {e}")
        return False
    except ConnectionError as e:
         logger.error(f"Ошибка соединения при обновлении токена VK (users) для {api_key}: {e}")
         return False
    except Exception as e:
        logger.error(f"Неожиданная ошибка при обновлении токена VK (users) для {api_key}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False
    # finally:
        # Пул управляет соединением

async def get_vk_token(api_key: str) -> Optional[str]:
    """Получает VK токен пользователя (из таблицы users)."""
    pool = None
    try:
        pool = await get_db_connection()
        if not pool: return None
        
        async with pool.acquire() as conn:
            # Используем fetchval для получения одного значения
            encrypted_token_str = await conn.fetchval('SELECT vk_token FROM users WHERE api_key = $1', api_key)
        
        if encrypted_token_str:
            try:
                decrypted_token = cipher.decrypt(encrypted_token_str.encode()).decode()
                if decrypted_token.startswith('vk1.a.'):
                    return decrypted_token
                else:
                    logger.error(f"Расшифрованный токен VK (users) имеет неверный формат: {decrypted_token}")
                    return None
            except Exception as e:
                import traceback
                error_details = str(e)
                tb = traceback.format_exc()
                logger.error(f"Ошибка при расшифровке токена VK (users) для пользователя {api_key}: {error_details}")
                logger.error(f"Трассировка: {tb}")
                
                # Пробуем вернуть как есть, если похоже на токен
                if isinstance(encrypted_token_str, str) and encrypted_token_str.startswith('vk1.a.'):
                    logger.warning(f"Возвращаем необработанный токен VK (users) для пользователя {api_key}")
                    return encrypted_token_str
                return None
        else:
            # Токен не найден или равен NULL
            return None
            
    except asyncpg.PostgresError as e:
        logger.error(f"Ошибка PostgreSQL при получении токена VK (users) для {api_key}: {e}")
        return None
    except ConnectionError as e:
         logger.error(f"Ошибка соединения при получении токена VK (users) для {api_key}: {e}")
         return None
    except Exception as e:
        logger.error(f"Неожиданная ошибка при получении токена VK (users) для {api_key}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None
    # finally:
        # Пул управляет соединением

async def get_users_dict() -> Dict:
    """Получает всех пользователей и их аккаунты в формате словаря {api_key: user_data}."""
    pool = None
    users_result = {}
    try:
        pool = await get_db_connection()
        if not pool:
             logger.error("Не удалось получить пул соединений в get_users_dict")
             return {}

        async with pool.acquire() as conn:
            # 1. Получить всех пользователей
            logger.debug("get_users_dict: Получение пользователей...")
            users_records = await conn.fetch('SELECT * FROM users')
            if not users_records:
                logger.info("get_users_dict: Пользователи не найдены.")
                return {}

            # Подготавливаем словарь пользователей
            for record in users_records:
                user_data = dict(record)
                api_key = user_data.get('api_key')
                if not api_key:
                    logger.warning(f"Найден пользователь без api_key: {user_data}")
                    continue 

                user_data['telegram_accounts'] = []
                user_data['vk_accounts'] = []

                # Расшифровываем VK токен пользователя (если он есть в таблице users)
                encrypted_token_str = user_data.get('vk_token')
                if encrypted_token_str:
                    try:
                        user_data['vk_token'] = cipher.decrypt(encrypted_token_str.encode()).decode()
                    except Exception as e:
                        logger.error(f"Ошибка расшифровки vk_token для {api_key} в get_users_dict: {e}")
                        user_data['vk_token'] = None 

                users_result[api_key] = user_data

            # 2. Получить все аккаунты Telegram
            logger.debug("get_users_dict: Получение аккаунтов Telegram...")
            tg_accounts_records = await conn.fetch('SELECT * FROM telegram_accounts')
            for record in tg_accounts_records:
                acc = dict(record)
                user_api_key = acc.get('user_api_key')
                if user_api_key in users_result:
                    users_result[user_api_key]['telegram_accounts'].append(acc)
                else:
                    logger.warning(f"Найден Telegram аккаунт ({acc.get('id')}) для несуществующего пользователя {user_api_key}")

            # 3. Получить все аккаунты VK
            logger.debug("get_users_dict: Получение аккаунтов VK...")
            vk_accounts_records = await conn.fetch('SELECT * FROM vk_accounts')
            for record in vk_accounts_records:
                acc = dict(record)
                user_api_key = acc.get('user_api_key')
                if user_api_key in users_result:
                    # Расшифровываем токен VK аккаунта
                    encrypted_token_str_vk = acc.get('token')
                    if encrypted_token_str_vk:
                         try:
                             acc['token'] = cipher.decrypt(encrypted_token_str_vk.encode()).decode()
                         except Exception as e:
                             logger.error(f"Ошибка расшифровки токена VK аккаунта {acc.get('id')} в get_users_dict: {e}")
                             acc['token'] = None 
                    users_result[user_api_key]['vk_accounts'].append(acc)
                else:
                     logger.warning(f"Найден VK аккаунт ({acc.get('id')}) для несуществующего пользователя {user_api_key}")

        logger.info(f"get_users_dict: Успешно получены данные для {len(users_result)} пользователей.")
        return users_result

    except asyncpg.PostgresError as e:
        logger.error(f"Ошибка PostgreSQL в get_users_dict: {e}")
        import traceback
        logger.error(traceback.format_exc()) 
        return {} 
    except ConnectionError as e:
         logger.error(f"Ошибка соединения в get_users_dict: {e}")
         return {}
    except Exception as e:
        logger.error(f"Неожиданная ошибка в get_users_dict: {e}")
        import traceback
        logger.error(traceback.format_exc()) 
        return {} 
    # finally:
        # Пул управляет соединением

async def verify_api_key(api_key: str) -> bool:
    """Проверяет, существует ли пользователь с указанным API ключом."""
    user = await get_user(api_key)
    if not user:
        return False
    
    await update_user_last_used(api_key)
    return True

async def save_users(users_dict: Dict) -> None:
    """Сохраняет словарь пользователей в базу данных PostgreSQL.
    
    ВНИМАНИЕ: Эта функция полностью перезаписывает данные для пользователей из словаря,
    удаляя существующие аккаунты перед вставкой новых. 
    Используйте с осторожностью или перепишите для более гранулярного обновления.
    
    Args:
        users_dict: Словарь данных пользователей в формате {api_key: user_data}
    """
    pool = None
    try:
        pool = await get_db_connection()
        if not pool:
             logger.error("Не удалось получить пул соединений в save_users")
             return

        async with pool.acquire() as conn:
             # Начинаем одну большую транзакцию для всех пользователей
             async with conn.transaction(): 
                for api_key, user_data in users_dict.items():
                    # Проверяем существование пользователя (UPSERT)
                    # Можно использовать INSERT ... ON CONFLICT DO UPDATE для атомарности
                    user_exists = await conn.fetchval('SELECT 1 FROM users WHERE api_key = $1', api_key)
            
                    # Шифруем vk_token пользователя перед сохранением (если есть)
                    vk_token_user = user_data.get('vk_token')
                    vk_token_user_encrypted = None
                    if vk_token_user and isinstance(vk_token_user, str) and vk_token_user.startswith('vk1.a.'):
                        try:
                            vk_token_user_encrypted = cipher.encrypt(vk_token_user.encode()).decode()
                        except Exception as e:
                             logger.error(f"Ошибка шифрования vk_token пользователя {api_key} в save_users: {e}")
                             # Пропускаем обновление токена?
                    
                    if user_exists:
                        await conn.execute('''
                            UPDATE users SET
                                username = $1,
                                password = $2,
                                last_used = $3,
                                vk_token = $4
                            WHERE api_key = $5
                        ''', 
                            user_data.get('username'),
                            user_data.get('password'),
                            user_data.get('last_used'),
                            vk_token_user_encrypted, # Зашифрованный или None
                            api_key
                        )
                    else:
                        await conn.execute('''
                            INSERT INTO users (api_key, username, password, created_at, last_used, vk_token)
                            VALUES ($1, $2, $3, $4, $5, $6)
                        ''', 
                            api_key,
                            user_data.get('username'),
                            user_data.get('password'),
                            user_data.get('created_at', datetime.now(timezone.utc)),
                            user_data.get('last_used'),
                            vk_token_user_encrypted # Зашифрованный или None
                        )
            
                    # Удаляем старые аккаунты ПЕРЕД вставкой новых
                    await conn.execute('DELETE FROM telegram_accounts WHERE user_api_key = $1', api_key)
                    await conn.execute('DELETE FROM vk_accounts WHERE user_api_key = $1', api_key)
            
                    # Вставляем аккаунты Telegram
                    tg_accounts_to_insert = []
                    for account in user_data.get('telegram_accounts', []):
                        account_id = account.get('id', str(uuid.uuid4()))
                        tg_accounts_to_insert.append((
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
                            account.get('added_at', datetime.now(timezone.utc)),
                            account.get('session_string', ''),
                            account.get('phone_code_hash', ''),
                            account.get('is_active', True),
                            account.get('request_limit', 1000),
                            account.get('last_used')
                        ))
                    
                    if tg_accounts_to_insert:
                        await conn.copy_records_to_table(
                             'telegram_accounts', 
                             records=tg_accounts_to_insert,
                             columns=['id', 'user_api_key', 'api_id', 'api_hash', 'phone', 'proxy', 'status', 'session_file',
                                      'requests_count', 'last_request_time', 'added_at', 'session_string', 'phone_code_hash',
                                      'is_active', 'request_limit', 'last_used']
                         ) 
            
                    # Вставляем аккаунты VK
                    vk_accounts_to_insert = []
                    for account in user_data.get('vk_accounts', []):
                        account_id = account.get('id', str(uuid.uuid4()))
                        token = account.get('token')
                        encrypted_token = None
                        if token and isinstance(token, str) and token.startswith('vk1.a.'):
                            try:
                                encrypted_token = cipher.encrypt(token.encode()).decode()
                            except Exception as e:
                                logger.error(f"Ошибка шифрования токена VK аккаунта {account_id} в save_users: {e}")
                                # Пропускаем этот токен?
                        
                        vk_accounts_to_insert.append((
                            account_id,
                            api_key,
                            encrypted_token, # Зашифрованный или None
                            account.get('proxy'),
                            account.get('status', 'active'),
                            account.get('requests_count', 0),
                            account.get('last_request_time'),
                            account.get('added_at', datetime.now(timezone.utc)),
                            account.get('user_id'),
                            account.get('user_name'),
                            account.get('error_message'),
                            account.get('error_code'),
                            account.get('last_checked_at'),
                            account.get('is_active', True),
                            account.get('request_limit', 1000),
                            account.get('last_used')
                        ))
                    
                    if vk_accounts_to_insert:
                         await conn.copy_records_to_table(
                             'vk_accounts', 
                             records=vk_accounts_to_insert,
                             columns=['id', 'user_api_key', 'token', 'proxy', 'status', 'requests_count',
                                      'last_request_time', 'added_at', 'user_id', 'user_name', 'error_message',
                                      'error_code', 'last_checked_at', 'is_active', 'request_limit', 'last_used']
                         )
    
                # Коммит происходит автоматически при выходе из 'async with conn.transaction()'
                logger.info(f"save_users: Успешно сохранены/обновлены данные для {len(users_dict)} пользователей.")

    except asyncpg.PostgresError as e:
        logger.error(f"Ошибка PostgreSQL в save_users: {e}")
        import traceback
        logger.error(traceback.format_exc()) 
    except ConnectionError as e:
         logger.error(f"Ошибка соединения в save_users: {e}")
    except Exception as e:
        logger.error(f"Неожиданная ошибка в save_users: {e}")
        import traceback
        logger.error(traceback.format_exc()) 
    # finally:
        # Пул управляет соединением

async def fix_vk_tokens():
    """Асинхронно исправляет дважды зашифрованные токены VK в PostgreSQL."""
    logger.info("Проверка и исправление токенов VK в PostgreSQL...")
    count = 0
    pool = None  
    try:
        pool = await get_db_connection()
        if not pool:
            logger.error("Не удалось получить пул соединений в fix_vk_tokens")
            return 0  

        async with pool.acquire() as conn:
            # Получаем все ID и токены
            rows = await conn.fetch("SELECT id, token FROM vk_accounts WHERE token IS NOT NULL")

            updates = []
            for record in rows:
                acc_id, encrypted_token_str = record['id'], record['token']
                if not encrypted_token_str: continue
                
                try:
                    decrypted_once_bytes = cipher.decrypt(encrypted_token_str.encode())
                    # Попытка второй расшифровки
                    cipher.decrypt(decrypted_once_bytes)
                    # Если вторая удалась, сохраняем результат первой
                    updates.append((decrypted_once_bytes.decode(), acc_id))
                    count += 1
                except Exception:
                    # Ошибка -> токен нормальный или поврежден, пропускаем
                    continue

            # Выполняем обновление в транзакции, если есть что обновлять
            if updates:
                async with conn.transaction():
                     # Используем executemany для массового обновления
                    await conn.executemany("UPDATE vk_accounts SET token = $1 WHERE id = $2", updates)
                logger.info(f"Успешно исправлено {len(updates)} дважды зашифрованных токенов VK.")
            else:
                logger.info("Не найдено дважды зашифрованных токенов VK для исправления.")

    # Обрабатываем специфичные ошибки PostgreSQL
    except asyncpg.exceptions.UndefinedTableError:
        logger.warning(f"Не удалось проверить токены VK: таблица 'vk_accounts' не найдена.")
    except asyncpg.PostgresError as e:
        logger.error(f"Ошибка PostgreSQL при исправлении токенов VK: {e}")
        import traceback
        logger.error(traceback.format_exc())
    except ConnectionError as e:
         logger.error(f"Ошибка соединения в fix_vk_tokens: {e}")
    except Exception as e:
        logger.error(f"Неожиданная ошибка при исправлении токенов VK: {e}")
        import traceback
        logger.error(traceback.format_exc())
    # finally:
        # Пул управляет соединением

    return count

if __name__ == "__main__":
    print("Инициализация структуры базы данных PostgreSQL...")
    # Убедимся, что пул создается перед вызовом initialize_database
    async def main():
        pool = await get_db_pool() # Инициализация пула
        if not pool:
             logger.error("Не удалось создать пул соединений. Выход.")
             return 
             
        await initialize_database() # Инициализация схемы
        
        # Вызываем fix_vk_tokens после инициализации
        logger.info("Запуск исправления токенов VK...")
        fixed_count = await fix_vk_tokens()
        logger.info(f"Исправлено {fixed_count} токенов VK.")
        
        # Закрытие пула при завершении (если скрипт должен завершаться)
        if db_pool:
             await db_pool.close()
             logger.info("Пул соединений PostgreSQL закрыт.")
             
    asyncio.run(main())
    print("Инициализация базы данных PostgreSQL завершена!") 