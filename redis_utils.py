import redis
import logging
import time
import random
import sqlite3
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Получаем параметры подключения из .env
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_DB = int(os.getenv('REDIS_DB', 0))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', None)

# Инициализация клиента Redis
redis_client = None

def init_redis():
    """Инициализирует подключение к Redis."""
    global redis_client
    try:
        redis_client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            password=REDIS_PASSWORD,
            decode_responses=True,  # Автоматически декодируем ответы
            socket_timeout=5,
            socket_connect_timeout=5,
            retry_on_timeout=True
        )
        # Проверяем соединение
        redis_client.ping()
        logger.info(f"Подключение к Redis на {REDIS_HOST}:{REDIS_PORT} успешно")
        return True
    except Exception as e:
        logger.error(f"Ошибка подключения к Redis: {e}")
        redis_client = None
        return False

def get_redis():
    """Возвращает клиент Redis, при необходимости инициализирует подключение."""
    global redis_client
    if redis_client is None:
        init_redis()
    return redis_client

def update_account_usage_redis(api_key, account_id, platform):
    """Обновляет статистику использования аккаунта в Redis."""
    redis = get_redis()
    if not redis:
        logger.warning("Redis недоступен, пропускаем обновление статистики")
        return False

    try:
        # Ключи для хранения данных
        count_key = f"account:{platform}:{account_id}:requests_count"
        last_used_key = f"account:{platform}:{account_id}:last_used"
        
        # Текущее время
        current_time = datetime.now().isoformat()
        
        # Используем транзакцию Redis для атомарного обновления
        pipeline = redis.pipeline()
        pipeline.incr(count_key)
        pipeline.set(last_used_key, current_time)
        # Устанавливаем время жизни ключей, например, 30 дней
        pipeline.expire(count_key, 60*60*24*30)
        pipeline.expire(last_used_key, 60*60*24*30)
        pipeline.execute()
        
        # С некоторой вероятностью синхронизируем с базой данных
        if random.random() < 0.1:  # 10% вероятность синхронизации
            sync_account_stats_to_db(account_id, platform)
        
        return True
    except Exception as e:
        logger.error(f"Ошибка обновления статистики в Redis: {e}")
        return False

def sync_account_stats_to_db(account_id, platform, force=False):
    """Синхронизирует статистику из Redis в SQLite."""
    redis = get_redis()
    if not redis:
        logger.warning("Redis недоступен, синхронизация статистики невозможна")
        return False

    try:
        # Ключи Redis
        count_key = f"account:{platform}:{account_id}:requests_count"
        last_used_key = f"account:{platform}:{account_id}:last_used"
        
        # Получаем данные из Redis
        count = redis.get(count_key)
        last_used = redis.get(last_used_key)
        
        if count is None and not force:
            logger.debug(f"Нет данных в Redis для аккаунта {platform}:{account_id}")
            return False
        
        # Определяем таблицу в зависимости от платформы
        table = 'telegram_accounts' if platform == 'telegram' else 'vk_accounts'
        
        # Обновляем данные в SQLite
        from user_manager import get_db_connection
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Если count или last_used не найдены, но force=True, получаем текущие значения из базы
        if count is None and force:
            cursor.execute(f"SELECT requests_count FROM {table} WHERE id = ?", (account_id,))
            row = cursor.fetchone()
            if row:
                count = row['requests_count']
            else:
                count = 0
        
        if last_used is None and force:
            last_used = datetime.now().isoformat()
        
        # Выполняем обновление
        cursor.execute(f'''
            UPDATE {table} 
            SET requests_count = ?, 
                last_used = ?
            WHERE id = ?
        ''', (int(count) if count else 0, last_used if last_used else None, account_id))
        
        conn.commit()
        conn.close()
        
        logger.debug(f"Синхронизирована статистика для аккаунта {platform}:{account_id}")
        return cursor.rowcount > 0
    except Exception as e:
        logger.error(f"Ошибка синхронизации с БД: {e}")
        return False

def sync_all_accounts_stats():
    """Синхронизирует статистику всех аккаунтов из Redis в SQLite."""
    redis = get_redis()
    if not redis:
        logger.warning("Redis недоступен, полная синхронизация статистики невозможна")
        return False
    
    try:
        # Получаем все ключи счетчиков
        count_keys = redis.keys("account:*:*:requests_count")
        
        success_count = 0
        error_count = 0
        
        for key in count_keys:
            # Разбиваем ключ на части для получения платформы и ID аккаунта
            parts = key.split(":")
            if len(parts) != 4:
                continue
                
            platform = parts[1]
            account_id = parts[2]
            
            # Синхронизируем данные аккаунта
            if sync_account_stats_to_db(account_id, platform):
                success_count += 1
            else:
                error_count += 1
        
        logger.info(f"Синхронизировано {success_count} аккаунтов, ошибок: {error_count}")
        return True
    except Exception as e:
        logger.error(f"Ошибка при массовой синхронизации статистики: {e}")
        return False

def get_account_stats_redis(account_id, platform):
    """Получает статистику использования аккаунта из Redis."""
    redis = get_redis()
    if not redis:
        return None
    
    try:
        # Ключи Redis
        count_key = f"account:{platform}:{account_id}:requests_count"
        last_used_key = f"account:{platform}:{account_id}:last_used"
        
        # Получаем данные из Redis
        count = redis.get(count_key)
        last_used = redis.get(last_used_key)
        
        return {
            "requests_count": int(count) if count else 0,
            "last_used": last_used
        }
    except Exception as e:
        logger.error(f"Ошибка получения статистики из Redis: {e}")
        return None

def reset_account_stats_redis(account_id, platform):
    """Сбрасывает статистику использования аккаунта в Redis."""
    redis = get_redis()
    if not redis:
        return False
    
    try:
        # Ключи Redis
        count_key = f"account:{platform}:{account_id}:requests_count"
        last_used_key = f"account:{platform}:{account_id}:last_used"
        
        # Сбрасываем значения
        redis.set(count_key, 0)
        redis.delete(last_used_key)
        
        # Синхронизируем с базой данных
        sync_account_stats_to_db(account_id, platform, force=True)
        
        return True
    except Exception as e:
        logger.error(f"Ошибка сброса статистики в Redis: {e}")
        return False

def reset_all_account_stats():
    """Сбрасывает статистику использования всех аккаунтов."""
    redis = get_redis()
    if not redis:
        return False
    
    try:
        # Получаем все ключи счетчиков
        count_keys = redis.keys("account:*:*:requests_count")
        last_used_keys = redis.keys("account:*:*:last_used")
        
        # Используем транзакцию Redis
        pipeline = redis.pipeline()
        
        # Сбрасываем счетчики
        for key in count_keys:
            pipeline.set(key, 0)
        
        # Удаляем метки времени последнего использования
        for key in last_used_keys:
            pipeline.delete(key)
        
        pipeline.execute()
        
        # Синхронизируем с базой данных
        # Получаем список всех аккаунтов
        from user_manager import get_db_connection
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Обновляем VK аккаунты
        cursor.execute("UPDATE vk_accounts SET requests_count = 0, last_used = NULL")
        vk_updated = cursor.rowcount
        
        # Обновляем Telegram аккаунты
        cursor.execute("UPDATE telegram_accounts SET requests_count = 0, last_used = NULL")
        tg_updated = cursor.rowcount
        
        conn.commit()
        conn.close()
        
        logger.info(f"Сброшена статистика для {vk_updated} VK аккаунтов и {tg_updated} Telegram аккаунтов")
        return True
    except Exception as e:
        logger.error(f"Ошибка сброса статистики: {e}")
        return False

# Запускаем инициализацию при импорте модуля
init_redis() 