import json
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
logger = logging.getLogger(__name__)

USERS_FILE = 'users.json'
ENCRYPTION_KEY = os.getenv('ENCRYPTION_KEY')
cipher = Fernet(ENCRYPTION_KEY.encode())

# Константы для ротации аккаунтов
MAX_REQUESTS_PER_ACCOUNT = 1000  # Максимальное количество запросов на аккаунт
ACCOUNT_COOLDOWN = 3600  # Время отдыха аккаунта в секундах (1 час)
MAX_ACTIVE_ACCOUNTS = 3  # Максимальное количество одновременно активных аккаунтов
DEGRADED_MODE_DELAY = 0.5  # Задержка в режиме пониженной производительности (500мс)
DEGRADED_MODE_SEMAPHORE = 1  # Максимум 1 одновременный запрос в режиме пониженной производительности

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

def get_active_accounts(api_key: str, platform: str) -> List[Dict]:
    """Получает список активных аккаунтов для платформы."""
    users = load_users()
    if api_key not in users:
        return []
    
    accounts_key = f"{platform}_accounts"
    if accounts_key not in users[api_key]:
        return []
    
    accounts = users[api_key][accounts_key]
    current_time = time.time()
    
    # Получаем все аккаунты, которые не достигли лимита запросов
    active_accounts = [
        acc for acc in accounts
        if acc.get("requests_count", 0) < MAX_REQUESTS_PER_ACCOUNT and
        (not acc.get("token_expired", False))  # Проверяем, не истек ли токен
    ]
    
    # Если есть аккаунты в кулдауне, но нет других активных аккаунтов,
    # возвращаем аккаунт с наименьшим количеством запросов
    if not active_accounts:
        accounts.sort(key=lambda x: x.get("requests_count", 0))
        return [accounts[0]]
    
    # Сортируем по количеству запросов (меньше запросов = выше приоритет)
    active_accounts.sort(key=lambda x: x.get("requests_count", 0))
    
    # Возвращаем только MAX_ACTIVE_ACCOUNTS аккаунтов
    return active_accounts[:MAX_ACTIVE_ACCOUNTS]

def get_next_available_account(api_key: str, platform: str) -> Optional[Dict]:
    """Получает следующий доступный аккаунт для использования."""
    active_accounts = get_active_accounts(api_key, platform)
    if not active_accounts:
        return None
    
    # Если есть только один аккаунт или все аккаунты в кулдауне,
    # используем режим пониженной производительности
    if len(active_accounts) == 1:
        account = active_accounts[0]
        account["degraded_mode"] = True
        return account
    
    # Выбираем аккаунт с наименьшим количеством запросов
    return active_accounts[0]

def update_account_usage(api_key: str, account_id: str, platform: str, token_expired: bool = False) -> bool:
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
            account["last_request_time"] = time.time()
            
            # Если токен истек, помечаем аккаунт
            if token_expired:
                account["token_expired"] = True
            
            # Если аккаунт достиг лимита запросов, но это единственный аккаунт,
            # продолжаем использовать его в режиме пониженной производительности
            if account["requests_count"] >= MAX_REQUESTS_PER_ACCOUNT:
                account["degraded_mode"] = True
            
            save_users(users)
            return True
    
    return False

def get_account_status(api_key: str, platform: str) -> Dict:
    """Получает статус всех аккаунтов для платформы."""
    users = load_users()
    if api_key not in users:
        return {"total": 0, "active": 0, "in_cooldown": 0, "token_expired": 0, "accounts": []}
    
    accounts_key = f"{platform}_accounts"
    if accounts_key not in users[api_key]:
        return {"total": 0, "active": 0, "in_cooldown": 0, "token_expired": 0, "accounts": []}
    
    accounts = users[api_key][accounts_key]
    current_time = time.time()
    
    status = {
        "total": len(accounts),
        "active": 0,
        "in_cooldown": 0,
        "token_expired": 0,
        "accounts": []
    }
    
    for account in accounts:
        account_status = {
            "id": account.get("id"),
            "requests_count": account.get("requests_count", 0),
            "last_request_time": account.get("last_request_time"),
            "degraded_mode": account.get("degraded_mode", False),
            "token_expired": account.get("token_expired", False),
            "status": "active"
        }
        
        if account.get("token_expired", False):
            status["token_expired"] += 1
            account_status["status"] = "token_expired"
        elif account.get("requests_count", 0) >= MAX_REQUESTS_PER_ACCOUNT:
            if account.get("degraded_mode", False):
                status["active"] += 1
                account_status["status"] = "degraded"
            else:
                status["in_cooldown"] += 1
                account_status["status"] = "cooldown"
        else:
            status["active"] += 1
        
        status["accounts"].append(account_status)
    
    return status