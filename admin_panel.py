import os
import logging
from typing import Dict, List, Optional
from fastapi import HTTPException, Security
from fastapi.security import APIKeyHeader
from dotenv import load_dotenv
from datetime import datetime
import uuid
import user_manager  # Импортируем модуль для работы с базой данных

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
    # Проверяем, существует ли пользователь
    user = user_manager.get_user(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="Пользователь не найден")
    
    # Проверяем наличие аккаунта с таким же номером телефона
    for account in user.get("telegram_accounts", []):
        if account["phone"] == account_data["phone"]:
            raise HTTPException(status_code=400, detail="Аккаунт с таким номером телефона уже существует")
    
    # Добавляем аккаунт через user_manager
    success = user_manager.add_telegram_account(user_id, account_data)
    if not success:
        raise HTTPException(status_code=500, detail="Ошибка при добавлении аккаунта")
    return True

async def update_telegram_account(user_id: str, account_id: str, account_data: Dict) -> bool:
    """Обновляет данные аккаунта Telegram."""
    # Проверяем, существует ли пользователь и аккаунт
    user = user_manager.get_user(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="Пользователь не найден")
    
    # Ищем аккаунт
    account_exists = False
    for account in user.get("telegram_accounts", []):
        if account.get("id") == account_id:
            account_exists = True
            break
    
    if not account_exists:
        raise HTTPException(status_code=404, detail="Аккаунт не найден")
    
    # Обновляем аккаунт через user_manager
    success = user_manager.update_telegram_account(user_id, account_id, account_data)
    if not success:
        raise HTTPException(status_code=500, detail="Ошибка при обновлении аккаунта")
    return True

async def delete_telegram_account(user_id: str, account_id: str) -> bool:
    """Удаляет аккаунт Telegram."""
    # Проверяем, существует ли пользователь
    user = user_manager.get_user(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="Пользователь не найден")
    
    # Проверяем существование аккаунта
    account_exists = False
    for account in user.get("telegram_accounts", []):
        if account.get("id") == account_id:
            account_exists = True
            break
    
    if not account_exists:
        raise HTTPException(status_code=404, detail="Аккаунт не найден")
    
    # Удаляем аккаунт через user_manager
    success = user_manager.delete_telegram_account(user_id, account_id)
    if not success:
        raise HTTPException(status_code=500, detail="Ошибка при удалении аккаунта")
    return True

# Функции для работы с аккаунтами VK
async def add_vk_account(api_key: str, account_data: Dict) -> bool:
    """Добавляет аккаунт VK для пользователя."""
    # Проверяем, существует ли пользователь
    user = user_manager.get_user(api_key)
    if not user:
        raise HTTPException(status_code=404, detail="Пользователь не найден")
    
    # Добавляем аккаунт через user_manager
    success = user_manager.add_vk_account(api_key, account_data)
    if not success:
        raise HTTPException(status_code=500, detail="Ошибка при добавлении аккаунта")
    return True

async def update_vk_account(api_key: str, account_id: str, account_data: Dict) -> bool:
    """Обновляет данные аккаунта VK."""
    # Проверяем, существует ли пользователь
    user = user_manager.get_user(api_key)
    if not user:
        raise HTTPException(status_code=404, detail="Пользователь не найден")
    
    # Ищем аккаунт
    account_exists = False
    for account in user.get("vk_accounts", []):
        if account.get("id") == account_id:
            account_exists = True
            break
    
    if not account_exists:
        raise HTTPException(status_code=404, detail="Аккаунт не найден")
    
    # Обновляем аккаунт через user_manager
    success = user_manager.update_vk_account(api_key, account_id, account_data)
    if not success:
        raise HTTPException(status_code=500, detail="Ошибка при обновлении аккаунта")
    return True

async def delete_vk_account(api_key: str, account_id: str) -> bool:
    """Удаляет аккаунт VK."""
    # Проверяем, существует ли пользователь
    user = user_manager.get_user(api_key)
    if not user:
        raise HTTPException(status_code=404, detail="Пользователь не найден")
    
    # Проверяем существование аккаунта
    account_exists = False
    for account in user.get("vk_accounts", []):
        if account.get("id") == account_id:
            account_exists = True
            break
    
    if not account_exists:
        raise HTTPException(status_code=404, detail="Аккаунт не найден")
    
    # Удаляем аккаунт через user_manager
    success = user_manager.delete_vk_account(api_key, account_id)
    if not success:
        raise HTTPException(status_code=500, detail="Ошибка при удалении аккаунта")
    return True

# Функции для работы с пользователями
async def register_user(username: str, password: str) -> str:
    """Регистрирует нового пользователя."""
    # Регистрируем пользователя через user_manager
    api_key = await user_manager.register_user(username, password)
    return api_key

async def get_user(api_key: str) -> Optional[Dict]:
    """Получает информацию о пользователе."""
    user = user_manager.get_user(api_key)
    if not user:
        raise HTTPException(404, "Пользователь не найден")
    return user

async def delete_user(api_key: str) -> bool:
    """Удаляет пользователя."""
    # Получаем данные пользователя, чтобы удалить файлы сессий
    user = user_manager.get_user(api_key)
    if not user:
        raise HTTPException(404, "Пользователь не найден")
    
    # Удаляем файлы сессий Telegram
    for account in user.get("telegram_accounts", []):
        if account.get("session_file") and os.path.exists(account["session_file"]):
            try:
                os.remove(account["session_file"])
            except Exception as e:
                logger.error(f"Ошибка при удалении файла сессии {account['session_file']}: {e}")
    
    # Удаляем пользователя
    from user_manager import get_db_connection
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Сначала удаляем все аккаунты пользователя
    cursor.execute('DELETE FROM telegram_accounts WHERE user_api_key = ?', (api_key,))
    cursor.execute('DELETE FROM vk_accounts WHERE user_api_key = ?', (api_key,))
    
    # Затем удаляем самого пользователя
    cursor.execute('DELETE FROM users WHERE api_key = ?', (api_key,))
    
    conn.commit()
    conn.close()
    return True

async def get_all_users() -> List[Dict]:
    """Получает список всех пользователей."""
    from user_manager import get_db_connection
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('SELECT * FROM users')
    users_rows = cursor.fetchall()
    
    users = []
    
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
            user_dict['vk_token'] = user_manager.cipher.decrypt(user_dict['vk_token'].encode()).decode()
        
        users.append(user_dict)
    
    conn.close()
    return users

async def get_system_stats() -> Dict:
    """Получает статистику системы."""
    from user_manager import get_db_connection
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Получаем количество пользователей
    cursor.execute('SELECT COUNT(*) as count FROM users')
    total_users = cursor.fetchone()['count']
    
    # Получаем количество аккаунтов Telegram
    cursor.execute('SELECT COUNT(*) as count FROM telegram_accounts')
    total_telegram_accounts = cursor.fetchone()['count']
    
    # Получаем количество аккаунтов VK
    cursor.execute('SELECT COUNT(*) as count FROM vk_accounts')
    total_vk_accounts = cursor.fetchone()['count']
    
    # Получаем последнего созданного пользователя
    cursor.execute('SELECT MAX(created_at) as last_created FROM users')
    last_created = cursor.fetchone()['last_created']
    
    conn.close()
    
    return {
        "total_users": total_users,
        "total_telegram_accounts": total_telegram_accounts,
        "total_vk_accounts": total_vk_accounts,
        "last_created_user": last_created
    }

# Константы для ротации аккаунтов
MAX_REQUESTS_PER_ACCOUNT = user_manager.MAX_REQUESTS_PER_ACCOUNT

# Функции для ротации аккаунтов
def get_next_available_account(accounts: List[Dict], platform: str) -> Optional[Dict]:
    """Получает следующий доступный аккаунт для использования."""
    if not accounts:
        return None
    
    # Сортируем аккаунты по количеству запросов
    sorted_accounts = sorted(accounts, key=lambda x: x.get("requests_count", 0))
    
    # Проверяем, есть ли аккаунты, не достигшие лимита
    available_accounts = [
        acc for acc in sorted_accounts
        if acc.get("requests_count", 0) < MAX_REQUESTS_PER_ACCOUNT
    ]
    
    if not available_accounts:
        return None
    
    # Возвращаем аккаунт с наименьшим количеством запросов
    return available_accounts[0]

def update_account_usage(api_key: str, account_id: str, platform: str) -> bool:
    """Обновляет статистику использования аккаунта."""
    # Используем функцию из user_manager
    return user_manager.update_account_usage(api_key, account_id, platform)

async def update_user_vk_token(api_key: str, vk_token: str) -> bool:
    """Обновляет VK токен пользователя."""
    success = user_manager.set_vk_token(api_key, vk_token)
    if not success:
        raise HTTPException(status_code=404, detail="Пользователь не найден")
    return True

async def verify_api_key(api_key: str) -> bool:
    """Проверяет, существует ли пользователь с указанным API ключом."""
    user = user_manager.get_user(api_key)
    if not user:
        return False
    
    # Обновляем время последнего использования
    user_manager.update_user_last_used(api_key)
    return True

async def get_account_status(api_key: str) -> Dict:
    """Получает статус аккаунтов пользователя."""
    user = user_manager.get_user(api_key)
    if not user:
        raise HTTPException(status_code=404, detail="Пользователь не найден")
    
    # Получаем статус аккаунтов Telegram
    telegram_accounts = user.get('telegram_accounts', [])
    telegram_status = {
        "total": len(telegram_accounts),
        "active": sum(1 for acc in telegram_accounts if acc.get('status') == 'active'),
        "accounts": telegram_accounts
    }
    
    # Получаем статус аккаунтов VK
    vk_accounts = user.get('vk_accounts', [])
    vk_status = {
        "total": len(vk_accounts),
        "active": sum(1 for acc in vk_accounts if acc.get('status') == 'active'),
        "accounts": vk_accounts
    }
    
    return {
        "telegram": telegram_status,
        "vk": vk_status
    }

async def get_telegram_account(user_id: str, account_id: str) -> Optional[Dict]:
    """Получает данные аккаунта Telegram."""
    user = user_manager.get_user(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="Пользователь не найден")
    
    for account in user.get('telegram_accounts', []):
        if account.get('id') == account_id:
            return account
    
    raise HTTPException(status_code=404, detail="Аккаунт не найден")

async def get_vk_account(api_key: str, account_id: str) -> Optional[Dict]:
    """Получает данные аккаунта VK."""
    user = user_manager.get_user(api_key)
    if not user:
        raise HTTPException(status_code=404, detail="Пользователь не найден")
    
    for account in user.get('vk_accounts', []):
        if account.get('id') == account_id:
            return account
    
    raise HTTPException(status_code=404, detail="Аккаунт не найден") 