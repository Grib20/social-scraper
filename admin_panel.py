import os
import json
import logging
from typing import Dict, List, Optional
from fastapi import HTTPException, Security
from fastapi.security import APIKeyHeader
from dotenv import load_dotenv
from datetime import datetime
import uuid

# Загружаем переменные окружения
load_dotenv()

# Настройка логирования
logger = logging.getLogger(__name__)

# Путь к файлу с пользователями
USERS_FILE = "users.json"

# Максимальное количество запросов на аккаунт
MAX_REQUESTS_PER_ACCOUNT = 1000

# Загрузка пользователей из файла
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

# Сохранение пользователей в файл
def save_users(users: Dict) -> None:
    """Сохраняет пользователей в JSON файл."""
    try:
        with open(USERS_FILE, 'w') as f:
            json.dump(users, f, indent=4)
    except Exception as e:
        logger.error(f"Ошибка при сохранении пользователей: {e}")
        raise HTTPException(status_code=500, detail="Ошибка при сохранении данных")

# Проверка админ-ключа
ADMIN_KEY = os.getenv('ADMIN_KEY', 'your-secret-admin-key')
api_key_header = APIKeyHeader(name="X-Admin-Key")

async def verify_admin_key(api_key: str = Security(api_key_header)) -> bool:
    """Проверяет, является ли ключ админ-ключом."""
    if api_key != ADMIN_KEY:
        raise HTTPException(
            status_code=403,
            detail="Неверный админ-ключ"
        )
    return True

# Функции для работы с аккаунтами Telegram
async def add_telegram_account(api_key: str, account_data: Dict) -> bool:
    """Добавляет аккаунт Telegram для пользователя."""
    users = load_users()
    if api_key not in users:
        raise HTTPException(status_code=404, detail="Пользователь не найден")
    
    if "telegram_accounts" not in users[api_key]:
        users[api_key]["telegram_accounts"] = []
    
    # Добавляем информацию о запросах
    account_data["requests_count"] = 0
    account_data["last_request_time"] = None
    account_data["added_at"] = datetime.now().isoformat()
    
    users[api_key]["telegram_accounts"].append(account_data)
    save_users(users)
    return True

async def update_telegram_account(api_key: str, account_id: str, account_data: Dict) -> bool:
    """Обновляет данные аккаунта Telegram."""
    users = load_users()
    if api_key not in users or "telegram_accounts" not in users[api_key]:
        raise HTTPException(status_code=404, detail="Аккаунт не найден")
    
    for account in users[api_key]["telegram_accounts"]:
        if account.get("id") == account_id:
            account.update(account_data)
            save_users(users)
            return True
    
    raise HTTPException(status_code=404, detail="Аккаунт не найден")

async def delete_telegram_account(api_key: str, account_id: str) -> bool:
    """Удаляет аккаунт Telegram."""
    users = load_users()
    if api_key not in users or "telegram_accounts" not in users[api_key]:
        raise HTTPException(status_code=404, detail="Аккаунт не найден")
    
    users[api_key]["telegram_accounts"] = [
        acc for acc in users[api_key]["telegram_accounts"]
        if acc.get("id") != account_id
    ]
    save_users(users)
    return True

# Функции для работы с аккаунтами VK
async def add_vk_account(api_key: str, account_data: Dict) -> bool:
    """Добавляет аккаунт VK для пользователя."""
    users = load_users()
    if api_key not in users:
        raise HTTPException(status_code=404, detail="Пользователь не найден")
    
    if "vk_accounts" not in users[api_key]:
        users[api_key]["vk_accounts"] = []
    
    # Добавляем информацию о запросах
    account_data["requests_count"] = 0
    account_data["last_request_time"] = None
    account_data["added_at"] = datetime.now().isoformat()
    
    users[api_key]["vk_accounts"].append(account_data)
    save_users(users)
    return True

async def update_vk_account(api_key: str, account_id: str, account_data: Dict) -> bool:
    """Обновляет данные аккаунта VK."""
    users = load_users()
    if api_key not in users or "vk_accounts" not in users[api_key]:
        raise HTTPException(status_code=404, detail="Аккаунт не найден")
    
    for account in users[api_key]["vk_accounts"]:
        if account.get("id") == account_id:
            account.update(account_data)
            save_users(users)
            return True
    
    raise HTTPException(status_code=404, detail="Аккаунт не найден")

async def delete_vk_account(api_key: str, account_id: str) -> bool:
    """Удаляет аккаунт VK."""
    users = load_users()
    if api_key not in users or "vk_accounts" not in users[api_key]:
        raise HTTPException(status_code=404, detail="Аккаунт не найден")
    
    users[api_key]["vk_accounts"] = [
        acc for acc in users[api_key]["vk_accounts"]
        if acc.get("id") != account_id
    ]
    save_users(users)
    return True

# Функции для работы с пользователями
async def get_all_users() -> List[Dict]:
    """Получает список всех пользователей."""
    users = load_users()
    return [
        {
            "api_key": api_key,
            "telegram_accounts": user_data.get("telegram_accounts", []),
            "vk_accounts": user_data.get("vk_accounts", []),
            "created_at": user_data.get("created_at"),
            "last_used": user_data.get("last_used")
        }
        for api_key, user_data in users.items()
    ]

async def get_user(api_key: str) -> Optional[Dict]:
    """Получает информацию о конкретном пользователе."""
    users = load_users()
    if api_key not in users:
        raise HTTPException(status_code=404, detail="Пользователь не найден")
    return {
        "api_key": api_key,
        "telegram_accounts": users[api_key].get("telegram_accounts", []),
        "vk_accounts": users[api_key].get("vk_accounts", []),
        "created_at": users[api_key].get("created_at"),
        "last_used": users[api_key].get("last_used")
    }

async def delete_user(api_key: str) -> bool:
    """Удаляет пользователя."""
    users = load_users()
    if api_key not in users:
        raise HTTPException(status_code=404, detail="Пользователь не найден")
    del users[api_key]
    save_users(users)
    return True

async def get_system_stats() -> Dict:
    """Получает статистику системы."""
    users = load_users()
    total_telegram_accounts = sum(len(u.get("telegram_accounts", [])) for u in users.values())
    total_vk_accounts = sum(len(u.get("vk_accounts", [])) for u in users.values())
    
    return {
        "total_users": len(users),
        "total_telegram_accounts": total_telegram_accounts,
        "total_vk_accounts": total_vk_accounts,
        "last_created_user": max((u.get("created_at") for u in users.values()), default=None)
    }

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

async def update_user_vk_token(api_key: str, vk_token: str) -> bool:
    """Обновляет VK токен пользователя."""
    users = load_users()
    if api_key not in users:
        raise HTTPException(status_code=404, detail="Пользователь не найден")
    
    if "vk_accounts" not in users[api_key]:
        users[api_key]["vk_accounts"] = []
    
    # Создаем новый аккаунт с токеном
    account_data = {
        "id": str(uuid.uuid4()),
        "token": vk_token,
        "requests_count": 0,
        "last_request_time": None,
        "added_at": datetime.now().isoformat()
    }
    
    users[api_key]["vk_accounts"].append(account_data)
    save_users(users)
    return True

async def verify_api_key(api_key: str) -> bool:
    """Проверяет валидность API ключа пользователя."""
    users = load_users()
    if api_key not in users:
        raise HTTPException(
            status_code=401,
            detail="Неверный API ключ"
        )
    
    # Обновляем время последнего использования
    users[api_key]["last_used"] = datetime.now().isoformat()
    save_users(users)
    return True 