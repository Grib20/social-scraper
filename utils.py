import os
import logging
import asyncio
from datetime import datetime, timedelta
import pytz
from redis_utils import get_redis, reset_account_stats_redis
import asyncpg
from user_manager import get_db_connection

logger = logging.getLogger(__name__)

def read_docker_secret(secret_name: str) -> str:
    """
    Читает секрет из Docker secrets или переменных окружения.
    
    Args:
        secret_name (str): Имя секрета
        
    Returns:
        str: Значение секрета или пустая строка, если секрет не найден
    """
    try:
        with open(f"/run/secrets/{secret_name}", "r") as f:
            return f.read().strip()
    except Exception as e:
        logger.error(f"Ошибка при чтении Docker secret {secret_name}: {e}")
        return os.getenv(secret_name, "")

async def auto_reset_unused_accounts(interval_seconds=600, inactive_minutes=60):
    """
    Периодически сбрасывает статистику для аккаунтов, которые не использовались более inactive_minutes.
    """
    while True:
        await asyncio.sleep(interval_seconds)
        try:
            redis_client = await get_redis()
            if not redis_client:
                continue

            # Ищем все ключи last_used
            cursor = 0
            now = datetime.now(pytz.timezone("Europe/Moscow"))
            threshold = now - timedelta(minutes=inactive_minutes)
            while True:
                cursor, keys = await redis_client.scan(cursor, match='account:*:*:last_used', count=500)
                for key in keys:
                    last_used_str = await redis_client.get(key)
                    if not last_used_str:
                        continue
                    try:
                        last_used = datetime.fromisoformat(last_used_str)
                    except Exception:
                        continue
                    if last_used < threshold:
                        # Извлекаем platform и account_id из ключа
                        parts = key.split(':')
                        if len(parts) >= 4:
                            platform = parts[1]
                            account_id = parts[2]
                            await reset_account_stats_redis(account_id, platform)
                if cursor == 0:
                    break
        except Exception as e:
            print(f"[auto_reset_unused_accounts] Ошибка: {e}") 

async def clean_orphan_redis_keys():
    """
    Удаляет висячие ключи статистики аккаунтов из Redis, если аккаунта нет в БД.
    """
    redis_client = await get_redis()
    if not redis_client:
        print("[clean_orphan_redis_keys] Redis недоступен!")
        return

    # Получаем все ключи статистики аккаунтов
    cursor = 0
    keys_to_check = []
    while True:
        cursor, keys = await redis_client.scan(cursor, match='account:*:*:requests_count', count=500)
        keys_to_check.extend(keys)
        if cursor == 0:
            break

    # Собираем account_id и platform
    orphan_keys = []
    orphan_accounts = set()
    if not keys_to_check:
        print("[clean_orphan_redis_keys] Нет ключей для проверки.")
        return

    # Получаем все id аккаунтов из БД
    pool = await get_db_connection()
    async with pool.acquire() as conn:
        tg_ids = set(row['id'] for row in await conn.fetch('SELECT id FROM telegram_accounts'))
        vk_ids = set(row['id'] for row in await conn.fetch('SELECT id FROM vk_accounts'))

    for key in keys_to_check:
        parts = key.split(':')
        if len(parts) >= 4:
            platform = parts[1]
            account_id = parts[2]
            if (platform == 'telegram' and account_id not in tg_ids) or (platform == 'vk' and account_id not in vk_ids):
                # Добавляем оба ключа (requests_count и last_used)
                orphan_keys.append(f"account:{platform}:{account_id}:requests_count")
                orphan_keys.append(f"account:{platform}:{account_id}:last_used")
                orphan_accounts.add((platform, account_id))

    # Удаляем найденные висячие ключи
    deleted = 0
    if orphan_keys:
        chunk_size = 500
        for i in range(0, len(orphan_keys), chunk_size):
            chunk = orphan_keys[i:i+chunk_size]
            res = await redis_client.delete(*chunk)
            deleted += res
    print(f"[clean_orphan_redis_keys] Удалено {deleted} висячих ключей для {len(orphan_accounts)} аккаунтов.") 

async def auto_clean_orphan_redis_keys(interval_seconds=86400):
    """
    Автоматически запускает очистку висячих ключей раз в сутки (по умолчанию).
    """
    while True:
        await asyncio.sleep(interval_seconds)
        try:
            await clean_orphan_redis_keys()
        except Exception as e:
            print(f"[auto_clean_orphan_redis_keys] Ошибка: {e}") 