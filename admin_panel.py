import os
import logging
from typing import Dict, List, Optional
from fastapi import HTTPException, Security
from fastapi.security import APIKeyHeader
from dotenv import load_dotenv
from datetime import datetime
import uuid
import user_manager 
import asyncpg
from asyncpg import Record
from asyncpg.exceptions import PostgresError

# Загружаем переменные окружения
load_dotenv()

# Настройка логирования
logger = logging.getLogger(__name__)

# Функция для чтения Docker secrets
def read_docker_secret(secret_name):
    try:
        with open(f'/run/secrets/{secret_name}', 'r', encoding='utf-8') as f:
            return f.read().strip()
    except (FileNotFoundError, PermissionError):
        return None

# Проверка админ-ключа
admin_key_from_secret = read_docker_secret('admin_key')
ADMIN_KEY = admin_key_from_secret or os.getenv('ADMIN_KEY', 'your-secret-admin-key')
api_key_header = APIKeyHeader(name="X-Admin-Key")

async def verify_admin_key(api_key: str) -> bool:
    """Проверяет, является ли ключ админ-ключом."""
    return api_key == ADMIN_KEY

# Функции для работы с аккаунтами Telegram
async def add_telegram_account(user_id: str, account_data: Dict) -> bool:
    """Добавляет аккаунт Telegram для пользователя."""
    user = await user_manager.get_user(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="Пользователь не найден")
    for account in user.get("telegram_accounts", []):
        if account.get("phone") == account_data.get("phone"):
            raise HTTPException(status_code=400, detail="Аккаунт с таким номером телефона уже существует")
    account_id = await user_manager.add_telegram_account(user_id, account_data)
    if not account_id:
        raise HTTPException(status_code=500, detail="Ошибка при добавлении аккаунта")
    return True

async def update_telegram_account(user_id: str, account_id: str, account_data: Dict) -> bool:
    """Обновляет данные аккаунта Telegram."""
    user = await user_manager.get_user(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="Пользователь не найден")
    account_exists = any(acc.get("id") == account_id for acc in user.get("telegram_accounts", []))
    if not account_exists:
        raise HTTPException(status_code=404, detail="Аккаунт не найден")
    success = await user_manager.update_telegram_account(user_id, account_id, account_data)
    if not success:
        raise HTTPException(status_code=500, detail="Ошибка при обновлении аккаунта")
    return True

async def delete_telegram_account(user_id: str, account_id: str) -> bool:
    """Удаляет аккаунт Telegram."""
    user = await user_manager.get_user(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="Пользователь не найден")
    account_exists = any(acc.get("id") == account_id for acc in user.get("telegram_accounts", []))
    if not account_exists:
        raise HTTPException(status_code=404, detail="Аккаунт не найден")
    success = await user_manager.delete_telegram_account(user_id, account_id)
    if not success:
        raise HTTPException(status_code=500, detail="Ошибка при удалении аккаунта")
    return True

# Функции для работы с аккаунтами VK
async def add_vk_account(api_key: str, account_data: Dict) -> bool:
    """Добавляет аккаунт VK для пользователя."""
    user = await user_manager.get_user(api_key)
    if not user:
        raise HTTPException(status_code=404, detail="Пользователь не найден")
    token = account_data.get('token')
    if not token or not isinstance(token, str) or not token.startswith('vk1.a.'):
        raise HTTPException(status_code=400, detail="Токен VK не указан или имеет неверный формат (должен начинаться с vk1.a.)")
    success = await user_manager.add_vk_account(api_key, account_data)
    if not success:
        raise HTTPException(status_code=500, detail="Ошибка при добавлении аккаунта VK")
    return True

async def update_vk_account(user_id: str, account_id: str, account_data: Dict) -> bool:
    """Обновляет данные аккаунта VK."""
    user = await user_manager.get_user(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="Пользователь не найден")
    account_exists = any(acc.get("id") == account_id for acc in user.get("vk_accounts", []))
    if not account_exists:
        raise HTTPException(status_code=404, detail="Аккаунт VK не найден")
    token = account_data.get('token')
    if token and (not isinstance(token, str) or not token.startswith('vk1.a.')):
        raise HTTPException(status_code=400, detail="Токен VK имеет неверный формат (должен начинаться с vk1.a.)")
    success = await user_manager.update_vk_account(user_id, account_id, account_data)
    if not success:
        raise HTTPException(status_code=500, detail="Ошибка при обновлении аккаунта VK")
    return True

async def delete_vk_account(user_id: str, account_id: str) -> bool:
    """Удаляет аккаунт VK."""
    user = await user_manager.get_user(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="Пользователь не найден")
    account_exists = any(acc.get("id") == account_id for acc in user.get("vk_accounts", []))
    if not account_exists:
        raise HTTPException(status_code=404, detail="Аккаунт VK не найден")
    success = await user_manager.delete_vk_account(user_id, account_id)
    if not success:
        raise HTTPException(status_code=500, detail="Ошибка при удалении аккаунта VK")
    return True

# Функции для работы с пользователями
async def register_user(username: str, password: str) -> str:
    """Регистрирует нового пользователя."""
    api_key = await user_manager.register_user(username, password)
    if api_key is None:
        raise HTTPException(status_code=500, detail="Ошибка при регистрации пользователя")
    return api_key

async def get_user(api_key: str) -> Optional[Dict]:
    """Получает информацию о пользователе."""
    user = await user_manager.get_user(api_key)
    if not user:
        raise HTTPException(404, "Пользователь не найден")
    return user

async def delete_user_by_id(api_key: str) -> bool:
    """Удаляет пользователя и его аккаунты по API ключу."""
    pool = None
    try:
        pool = await user_manager.get_db_connection()
        if not pool:
             raise HTTPException(status_code=503, detail="Не удалось получить пул БД")
             
        async with pool.acquire() as conn:
            # Используем транзакцию для атомарного удаления
            async with conn.transaction():
                # Удаляем связанные аккаунты (ON DELETE CASCADE должен сработать, но для надежности можно оставить)
                # await conn.execute('DELETE FROM telegram_accounts WHERE user_api_key = $1', api_key)
                # await conn.execute('DELETE FROM vk_accounts WHERE user_api_key = $1', api_key)
                
                # Удаляем самого пользователя
                result_str = await conn.execute('DELETE FROM users WHERE api_key = $1', api_key)
                
                deleted_count = int(result_str.split()[1])
                if deleted_count == 0:
                    # Если пользователь не найден, возвращаем ошибку
                    raise HTTPException(status_code=404, detail="Пользователь не найден для удаления")
        
        # Удаляем файлы сессий Telegram (это остается)        
        # user = await user_manager.get_user(api_key) # Больше не нужно, удаляем ниже
        # if user: # Пользователь уже удален из БД
        #     for account in user.get("telegram_accounts", []):
        #         if account.get("session_file") and os.path.exists(account["session_file"]):
        #             try:
        #                 os.remove(account["session_file"])
        #             except Exception as e:
        #                 logger.error(f"Ошибка при удалении файла сессии {account['session_file']}: {e}")
        
        logger.info(f"Пользователь с api_key={api_key} успешно удален.")
        return True
    except PostgresError as e: # Используем PostgresError
        logger.error(f"Ошибка PostgreSQL при удалении пользователя {api_key}: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка БД при удалении пользователя: {e}")
    except Exception as e:
        logger.error(f"Неожиданная ошибка при удалении пользователя {api_key}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Внутренняя ошибка сервера: {e}")

async def get_all_users() -> List[Dict]:
    """Получает список всех пользователей и их аккаунты."""
    pool = None
    try:
        pool = await user_manager.get_db_connection()
        if not pool:
             raise HTTPException(status_code=503, detail="Не удалось получить пул БД")

        async with pool.acquire() as conn:
            users_records = await conn.fetch('SELECT * FROM users ORDER BY created_at DESC')
        users = []
            for user_record in users_records:
                user_dict = dict(user_record)
            api_key = user_dict['api_key']
            
            # Получаем Telegram аккаунты
                tg_records = await conn.fetch('SELECT * FROM telegram_accounts WHERE user_api_key = $1', api_key)
                user_dict['telegram_accounts'] = [dict(acc) for acc in tg_records]
            
            # Получаем VK аккаунты
                vk_records = await conn.fetch('SELECT * FROM vk_accounts WHERE user_api_key = $1', api_key)
                vk_accounts_processed = []
                for vk_record in vk_records:
                     acc_dict = dict(vk_record)
                     # Расшифровываем токен VK аккаунта
                     encrypted_token_str_vk = acc_dict.get('token')
                     if encrypted_token_str_vk:
                          try:
                              acc_dict['token'] = user_manager.cipher.decrypt(encrypted_token_str_vk.encode()).decode()
                          except Exception as e:
                              logger.error(f"Ошибка расшифровки токена VK аккаунта {acc_dict.get('id')} в get_all_users: {e}")
                              acc_dict['token'] = None # Устанавливаем None при ошибке
                     vk_accounts_processed.append(acc_dict)
                user_dict['vk_accounts'] = vk_accounts_processed
                
                # Расшифровываем VK токен пользователя (если он есть)
                user_vk_token_encrypted = user_dict.get('vk_token')
                if user_vk_token_encrypted:
                    try:
                        user_dict['vk_token'] = user_manager.cipher.decrypt(user_vk_token_encrypted.encode()).decode()
                    except Exception as e:
                        logger.error(f"Ошибка расшифровки vk_token пользователя {api_key} в get_all_users: {e}")
                        user_dict['vk_token'] = None # Устанавливаем None при ошибке
            
            users.append(user_dict)
    return users
    except PostgresError as e: # Используем PostgresError
        logger.error(f"Ошибка PostgreSQL в get_all_users: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка БД при получении пользователей: {e}")
    except Exception as e:
        logger.error(f"Неожиданная ошибка в get_all_users: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Внутренняя ошибка сервера: {e}")

async def get_system_stats():
    """Получает системную статистику."""
    stats = {
        "total_users": 0,
        "total_telegram_accounts": 0,
        "total_vk_accounts": 0,
        "last_created_user": None
    }
    pool = None
    try:
        pool = await user_manager.get_db_connection()
        if not pool:
             logger.error("Не удалось получить пул соединений в get_system_stats")
             raise HTTPException(status_code=503, detail="Не удалось получить пул БД")
             
        async with pool.acquire() as conn:
            users_count = await conn.fetchval('SELECT COUNT(*) FROM users')
            stats["total_users"] = users_count if users_count is not None else 0

            tg_count = await conn.fetchval('SELECT COUNT(*) FROM telegram_accounts')
            stats["total_telegram_accounts"] = tg_count if tg_count is not None else 0

            vk_count = await conn.fetchval('SELECT COUNT(*) FROM vk_accounts')
            stats["total_vk_accounts"] = vk_count if vk_count is not None else 0

            last_created = await conn.fetchval('SELECT MAX(created_at) FROM users')
            stats["last_created_user"] = last_created.isoformat() if last_created else None
        
        return stats
    except PostgresError as e: # Используем PostgresError
        logger.error(f"Ошибка PostgreSQL в get_system_stats: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка БД при получении статистики: {e}")
    except Exception as e:
        logger.error(f"Неожиданная ошибка в get_system_stats: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Внутренняя ошибка сервера: {e}")

# Константы для ротации аккаунтов
MAX_REQUESTS_PER_ACCOUNT = user_manager.MAX_REQUESTS_PER_ACCOUNT

# Функции для ротации аккаунтов
def get_next_available_account(accounts: List[Dict], platform: str) -> Optional[Dict]:
    """Получает следующий доступный аккаунт для использования."""
    if not accounts:
        return None
    try:
        sorted_accounts = sorted(accounts, key=lambda x: x.get("requests_count", 0))
        available_accounts = [
            acc for acc in sorted_accounts
            if acc.get("requests_count", 0) < MAX_REQUESTS_PER_ACCOUNT
        ]
        if not available_accounts:
            return None
        return available_accounts[0]
    except Exception as e:
        logger.error(f"Ошибка при выборе следующего доступного аккаунта: {e}")
        return None

async def update_account_usage(api_key: str, account_id: str, platform: str) -> bool:
    """Обновляет статистику использования аккаунта."""
    # Используем функцию из user_manager
    return await user_manager.update_account_usage(api_key, account_id, platform)

async def update_user_vk_token(api_key: str, vk_token: str) -> bool:
    """Обновляет VK токен пользователя."""
    if not vk_token or not isinstance(vk_token, str) or not vk_token.startswith('vk1.a.'):
        raise HTTPException(status_code=400, detail="Токен VK не указан или имеет неверный формат (должен начинаться с vk1.a.)")
    success = await user_manager.set_vk_token(api_key, vk_token)
    if not success:
        raise HTTPException(status_code=404, detail="Пользователь не найден")
    return True

async def verify_api_key(api_key: str) -> bool:
    """Проверяет, существует ли пользователь с указанным API ключом."""
    user = user_manager.get_user(api_key)
    if not user:
        return False
    
    # Обновляем время последнего использования
    await user_manager.update_user_last_used(api_key)
    return True

async def get_account_status(api_key: str, platform: str) -> Dict:
    """Получает статус аккаунтов пользователя."""
    user = await user_manager.get_user(api_key)
    if not user:
        raise HTTPException(status_code=404, detail="Пользователь не найден")
    
    try:
        accounts_key = f"{platform}_accounts"
        accounts = user.get(accounts_key, [])
            return {
                "total": len(accounts),
            "active": sum(1 for acc in accounts if acc.get('is_active', False)),
                "accounts": accounts
            }
    except Exception as e:
        logger.error(f"Ошибка при получении статуса аккаунтов ({platform}): {e}")
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")

async def get_telegram_account(user_id: str, account_id: str) -> Optional[Dict]:
    """Получает данные аккаунта Telegram."""
    user = await user_manager.get_user(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="Пользователь не найден")
    
    for account in user.get('telegram_accounts', []):
        if account.get('id') == account_id:
            return account
    
    raise HTTPException(status_code=404, detail="Аккаунт не найден")

async def get_vk_account(api_key: str, account_id: str) -> Optional[Dict]:
    """Получает данные аккаунта VK."""
    user = await user_manager.get_user(api_key)
    if not user:
        raise HTTPException(status_code=404, detail="Пользователь не найден")
    
    for account in user.get('vk_accounts', []):
        if account.get('id') == account_id:
            # Расшифровываем токен перед возвратом
            encrypted_token = account.get('token')
            if encrypted_token:
                 try:
                     account['token'] = user_manager.cipher.decrypt(encrypted_token.encode()).decode()
                 except Exception as e:
                     logger.error(f"Ошибка расшифровки токена VK аккаунта {account_id} в get_vk_account: {e}")
                     account['token'] = None
            return account
    
    raise HTTPException(status_code=404, detail="Аккаунт VK не найден") 