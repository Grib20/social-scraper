import json
import os
import secrets
from cryptography.fernet import Fernet
import logging
from dotenv import load_dotenv
import time
import uuid
from datetime import datetime
from typing import Dict, Optional

load_dotenv()
logger = logging.getLogger(__name__)

USERS_FILE = 'users.json'
ENCRYPTION_KEY = os.getenv('ENCRYPTION_KEY')
cipher = Fernet(ENCRYPTION_KEY.encode())

async def init_users_file():
    if not os.path.exists(USERS_FILE):
        await save_users({})

async def get_users():
    await init_users_file()
    with open(USERS_FILE, 'r') as f:
        users = json.load(f)
    for api_key, data in users.items():
        if data.get('vk_token'):
            data['vk_token'] = cipher.decrypt(data['vk_token'].encode()).decode()
    return users

async def save_users(users):
    users_to_save = users.copy()
    for api_key, data in users_to_save.items():
        if data.get('vk_token'):
            data['vk_token'] = cipher.encrypt(data['vk_token'].encode()).decode()
    with open(USERS_FILE, 'w') as f:
        json.dump(users_to_save, f, indent=2)

async def register_user():
    users = await get_users()
    api_key = str(uuid.uuid4())
    users[api_key] = {
        "created_at": datetime.now().isoformat(),
        "last_used": None,
        "telegram_accounts": [],
        "vk_accounts": []
    }
    await save_users(users)
    logger.info(f"Зарегистрирован новый пользователь с API ключом: {api_key}")
    return api_key

async def set_vk_token(api_key, vk_token):
    users = await get_users()
    if api_key not in users:
        raise ValueError("Пользователь с таким API ключом не найден")
    users[api_key]['vk_token'] = vk_token
    await save_users(users)
    return True

async def get_vk_token(api_key):
    users = await get_users()
    return users.get(api_key, {}).get('vk_token')

def load_users() -> Dict:
    """Загружает пользователей из JSON файла."""
    if os.path.exists(USERS_FILE):
        try:
            with open(USERS_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Ошибка при загрузке пользователей: {e}")
            return {}
    return {}

def get_user(api_key: str) -> Optional[Dict]:
    """Получает информацию о пользователе."""
    users = load_users()
    return users.get(api_key)

def update_user_last_used(api_key: str) -> None:
    """Обновляет время последнего использования пользователя."""
    users = load_users()
    if api_key in users:
        users[api_key]["last_used"] = datetime.now().isoformat()
        save_users(users)

def add_telegram_account(api_key: str, account_data: Dict) -> bool:
    """Добавляет аккаунт Telegram для пользователя."""
    users = load_users()
    if api_key not in users:
        return False
    
    if "telegram_accounts" not in users[api_key]:
        users[api_key]["telegram_accounts"] = []
    
    account_data["id"] = str(uuid.uuid4())
    account_data["requests_count"] = 0
    account_data["last_request_time"] = None
    account_data["added_at"] = datetime.now().isoformat()
    
    users[api_key]["telegram_accounts"].append(account_data)
    save_users(users)
    return True

def update_telegram_account(api_key: str, account_id: str, account_data: Dict) -> bool:
    """Обновляет данные аккаунта Telegram."""
    users = load_users()
    if api_key not in users or "telegram_accounts" not in users[api_key]:
        return False
    
    for account in users[api_key]["telegram_accounts"]:
        if account.get("id") == account_id:
            account.update(account_data)
            save_users(users)
            return True
    
    return False

def delete_telegram_account(api_key: str, account_id: str) -> bool:
    """Удаляет аккаунт Telegram."""
    users = load_users()
    if api_key not in users or "telegram_accounts" not in users[api_key]:
        return False
    
    users[api_key]["telegram_accounts"] = [
        acc for acc in users[api_key]["telegram_accounts"]
        if acc.get("id") != account_id
    ]
    save_users(users)
    return True

def add_vk_account(api_key: str, account_data: Dict) -> bool:
    """Добавляет аккаунт VK для пользователя."""
    users = load_users()
    if api_key not in users:
        return False
    
    if "vk_accounts" not in users[api_key]:
        users[api_key]["vk_accounts"] = []
    
    account_data["id"] = str(uuid.uuid4())
    account_data["requests_count"] = 0
    account_data["last_request_time"] = None
    account_data["added_at"] = datetime.now().isoformat()
    
    users[api_key]["vk_accounts"].append(account_data)
    save_users(users)
    return True

def update_vk_account(api_key: str, account_id: str, account_data: Dict) -> bool:
    """Обновляет данные аккаунта VK."""
    users = load_users()
    if api_key not in users or "vk_accounts" not in users[api_key]:
        return False
    
    for account in users[api_key]["vk_accounts"]:
        if account.get("id") == account_id:
            account.update(account_data)
            save_users(users)
            return True
    
    return False

def delete_vk_account(api_key: str, account_id: str) -> bool:
    """Удаляет аккаунт VK."""
    users = load_users()
    if api_key not in users or "vk_accounts" not in users[api_key]:
        return False
    
    users[api_key]["vk_accounts"] = [
        acc for acc in users[api_key]["vk_accounts"]
        if acc.get("id") != account_id
    ]
    save_users(users)
    return True

def get_next_available_account(api_key: str, platform: str) -> Optional[Dict]:
    """Получает следующий доступный аккаунт для использования."""
    users = load_users()
    if api_key not in users:
        return None
    
    accounts_key = f"{platform}_accounts"
    if accounts_key not in users[api_key]:
        return None
    
    accounts = users[api_key][accounts_key]
    if not accounts:
        return None
    
    # Сортируем аккаунты по количеству запросов
    sorted_accounts = sorted(accounts, key=lambda x: x.get("requests_count", 0))
    
    # Проверяем, есть ли аккаунты, не достигшие лимита
    available_accounts = [
        acc for acc in sorted_accounts
        if acc.get("requests_count", 0) < 1000  # Максимальное количество запросов на аккаунт
    ]
    
    if not available_accounts:
        return None
    
    # Возвращаем аккаунт с наименьшим количеством запросов
    return available_accounts[0]

def update_account_usage(api_key: str, account_id: str, platform: str) -> bool:
    """Обновляет статистику использования аккаунта."""
    users = load_users()
    if api_key not in users:
        return False
    
    accounts_key = f"{platform}_accounts"
    if accounts_key not in users[api_key]:
        return False
    
    for account in users[api_key][accounts_key]:
        if account.get("id") == account_id:
            account["requests_count"] = account.get("requests_count", 0) + 1
            account["last_request_time"] = datetime.now().isoformat()
            save_users(users)
            return True
    
    return False