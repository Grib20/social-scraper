import asyncio
from fastapi import FastAPI, HTTPException, Request, Security, Body, Header
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.security import APIKeyHeader
from contextlib import asynccontextmanager
import uvicorn
import logging
from dotenv import load_dotenv  # Импортируем раньше всех
import os
import uuid
from typing import List, Union
import sys
from datetime import datetime
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError
import time

load_dotenv()  # Загружаем .env до импорта модулей

# Определяем api_key_header
api_key_header = APIKeyHeader(name="X-Admin-Key")

# Проверяем наличие обязательных переменных окружения
if not os.getenv("BASE_URL"):
    print("Ошибка: Переменная окружения BASE_URL не установлена")
    sys.exit(1)

# Импорты модулей после load_dotenv
from telegram_utils import (
    start_client, find_channels, 
    get_trending_posts, get_posts_in_channels, 
    get_posts_by_keywords, get_posts_by_period, 
    get_album_messages
)
from vk_utils import VKClient, find_vk_groups, get_vk_posts, get_vk_posts_in_groups
from user_manager import (
    register_user, set_vk_token, get_vk_token, get_user, 
    get_next_available_account, update_account_usage, update_user_last_used,
    get_users_dict, verify_api_key
)
from media_utils import init_scheduler, close_scheduler
from admin_panel import (
    verify_admin_key, get_all_users, get_user as admin_get_user, delete_user as admin_delete_user,
    update_user_vk_token, get_system_stats,
    add_telegram_account as admin_add_telegram_account, update_telegram_account as admin_update_telegram_account, 
    delete_telegram_account as admin_delete_telegram_account,
    add_vk_account as admin_add_vk_account, update_vk_account as admin_update_vk_account, 
    delete_vk_account as admin_delete_vk_account,
    get_account_status,
    get_telegram_account as admin_get_telegram_account, get_vk_account as admin_get_vk_account
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Telegram клиенты для каждого аккаунта
telegram_clients = {}
vk_clients = {}

# Классы для управления пулами клиентов
class ClientPool:
    """Базовый класс для пула клиентов."""
    
    def __init__(self):
        self.clients = {}  # account_id -> client
        self.usage_counts = {}  # account_id -> count
        self.last_used = {}  # account_id -> timestamp
        
    def reset_stats(self):
        """Сбрасывает статистику использования клиентов."""
        self.usage_counts = {}
        self.last_used = {}
    
    def get_client(self, account_id):
        """Получает клиента по ID аккаунта."""
        return self.clients.get(account_id)
    
    def add_client(self, account_id, client):
        """Добавляет клиента в пул."""
        self.clients[account_id] = client
        self.usage_counts[account_id] = 0
        self.last_used[account_id] = 0
        
    def remove_client(self, account_id):
        """Удаляет клиента из пула."""
        if account_id in self.clients:
            del self.clients[account_id]
        if account_id in self.usage_counts:
            del self.usage_counts[account_id]
        if account_id in self.last_used:
            del self.last_used[account_id]
    
    def select_next_client(self, active_accounts):
        """Выбирает следующего клиента для использования на основе стратегии ротации."""
        if not active_accounts:
            return None, None
        
        # Сортируем аккаунты по количеству использований и времени последнего использования
        sorted_accounts = sorted(
            active_accounts, 
            key=lambda acc: (self.usage_counts.get(acc['id'], 0), self.last_used.get(acc['id'], 0))
        )
        
        selected_account = sorted_accounts[0]
        account_id = selected_account['id']
        
        # Увеличиваем счетчик использования и обновляем время
        self.usage_counts[account_id] = self.usage_counts.get(account_id, 0) + 1
        self.last_used[account_id] = time.time()
        
        # Получаем или создаем клиента
        client = self.get_client(account_id)
        if not client:
            client = self.create_client(selected_account)
            self.add_client(account_id, client)
        
        return client, account_id
    
    def create_client(self, account):
        """Создает нового клиента для аккаунта."""
        raise NotImplementedError("Метод должен быть реализован в подклассе")


class VKClientPool(ClientPool):
    """Пул клиентов VK."""
    
    def create_client(self, account):
        """Создает нового клиента VK."""
        from vk_utils import VKClient
        token = account.get('token')
        # Проверяем, не является ли токен зашифрованным
        if token and len(token) > 100:  # Примерный признак зашифрованного токена
            from user_manager import cipher
            try:
                token = cipher.decrypt(token.encode()).decode()
            except Exception as e:
                logger.error(f"Ошибка при расшифровке токена VK: {e}")
                
        return VKClient(token, account.get('proxy'), account['id'])
    
    def get_active_clients(self, api_key):
        """Получает активные клиенты VK на основе активных аккаунтов."""
        from user_manager import get_active_accounts
        active_accounts = get_active_accounts(api_key, "vk")
        
        # Проверяем, все ли активные аккаунты имеют клиентов
        for account in active_accounts:
            if account['id'] not in self.clients:
                self.add_client(account['id'], self.create_client(account))
        
        return active_accounts


class TelegramClientPool(ClientPool):
    """Пул клиентов Telegram."""
    
    def __init__(self):
        super().__init__()
        self.connected_clients = set()  # Множество подключенных клиентов
        
    async def create_client(self, account):
        """Создает нового клиента Telegram."""
        from telegram_utils import create_telegram_client
        api_id = int(account['api_id'])
        api_hash = account['api_hash']
        session_file = account['session_file']
        proxy = account.get('proxy')
        
        client = await create_telegram_client(session_file, api_id, api_hash, proxy)
        return client
    
    async def connect_client(self, account_id):
        """Подключает клиента Telegram."""
        client = self.get_client(account_id)
        if client and account_id not in self.connected_clients:
            await client.connect()
            self.connected_clients.add(account_id)
        return client
    
    async def disconnect_client(self, account_id):
        """Отключает клиента Telegram."""
        client = self.get_client(account_id)
        if client and account_id in self.connected_clients:
            await client.disconnect()
            self.connected_clients.remove(account_id)
    
    async def disconnect_all(self):
        """Отключает всех клиентов Telegram."""
        for account_id in list(self.connected_clients):
            await self.disconnect_client(account_id)
    
    async def get_active_clients(self, api_key):
        """Получает активные клиенты Telegram на основе активных аккаунтов."""
        from user_manager import get_active_accounts
        active_accounts = get_active_accounts(api_key, "telegram")
        
        # Проверяем, все ли активные аккаунты имеют клиентов
        for account in active_accounts:
            if account['id'] not in self.clients:
                client = await self.create_client(account)
                self.add_client(account['id'], client)
        
        return active_accounts
    
    async def select_next_client(self, api_key):
        """Выбирает следующего клиента Telegram для использования."""
        active_accounts = await self.get_active_clients(api_key)
        client, account_id = super().select_next_client(active_accounts)
        
        if client and account_id:
            # Подключаем клиента, если он не подключен
            await self.connect_client(account_id)
        
        return client, account_id


# Инициализация пулов клиентов
vk_pool = VKClientPool()
telegram_pool = TelegramClientPool()


# Обновляем функцию auth_middleware
async def auth_middleware(request: Request, platform: str):
    """Middleware для авторизации запросов к API."""
    api_key = request.headers.get('api-key') or request.headers.get('x-api-key')
    
    if not api_key:
        # Пробуем получить из авторизации Bearer
        auth_header = request.headers.get('authorization')
        if auth_header and auth_header.startswith('Bearer '):
            api_key = auth_header.split(' ')[1]
        else:
            raise HTTPException(401, "API ключ обязателен")
    
    # Проверяем существование пользователя (убираем await)
    if not verify_api_key(api_key):
        raise HTTPException(401, "Неверный API ключ")
    
    # Обновляем время последнего использования
    update_user_last_used(api_key)
    
    # В зависимости от платформы, получаем соответствующий клиент
    if platform == 'vk':
        active_accounts = vk_pool.get_active_clients(api_key)
        client, account_id = vk_pool.select_next_client(active_accounts)
        if not client:
            raise HTTPException(429, "Все аккаунты VK достигли лимита запросов")
        
        update_account_usage(api_key, account_id, "vk")
        return client
    
    elif platform == 'telegram':
        client, account_id = await telegram_pool.select_next_client(api_key)
        if not client:
            raise HTTPException(429, "Все аккаунты Telegram достигли лимита запросов")
        
        update_account_usage(api_key, account_id, "telegram")
        return client
    
    else:
        raise HTTPException(400, "Неизвестная платформа")

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_scheduler()
    yield
    await close_scheduler()

app = FastAPI(lifespan=lifespan)

# Инициализируем шаблоны
templates = Jinja2Templates(directory="templates")

# Добавляем базовый контекст для всех шаблонов
def get_base_context(request: Request):
    return {
        "request": request,
        "base_url": os.getenv("BASE_URL")
    }

# Монтируем статические файлы
app.mount("/static", StaticFiles(directory="static"), name="static")

# Маршрут для главной страницы
@app.get("/")
async def index(request: Request):
    """Отображает главную страницу."""
    return templates.TemplateResponse(
        "index.html",
        get_base_context(request)
    )

# Маршрут для страницы входа
@app.get("/login")
async def login(request: Request):
    """Отображает страницу входа."""
    return templates.TemplateResponse(
        "login.html",
        get_base_context(request)
    )

# Маршрут для админ-панели
@app.get("/admin")
async def admin_panel(request: Request):
    # Пытаемся получить ключ из заголовка 
    admin_key = request.headers.get("X-Admin-Key")
    
    # Если ключа нет в заголовке, пытаемся получить его из query параметра
    if not admin_key:
        admin_key = request.query_params.get("admin_key")
    
    # Если ключа нет и в query параметрах, пытаемся получить из cookie
    if not admin_key:
        admin_key = request.cookies.get("admin_key")
        
    if not admin_key or not await verify_admin_key(admin_key):
        return RedirectResponse(url="/login")
    
    return templates.TemplateResponse(
        "admin_panel.html",
        {"request": request}
    )

@app.post("/admin/validate")
async def validate_admin_key(request: Request):
    # Пытаемся получить ключ из заголовка
    admin_key = request.headers.get("X-Admin-Key")
    
    # Если ключа нет в заголовке, пытаемся получить из cookie
    if not admin_key:
        admin_key = request.cookies.get("admin_key")
    
    if not admin_key or not await verify_admin_key(admin_key):
        raise HTTPException(status_code=401, detail="Invalid admin key")
    
    return JSONResponse(content={"status": "success"})

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_scheduler()
    yield
    await close_scheduler()

app = FastAPI(lifespan=lifespan)

# Инициализируем шаблоны
templates = Jinja2Templates(directory="templates")

# Добавляем базовый контекст для всех шаблонов
def get_base_context(request: Request):
    return {
        "request": request,
        "base_url": os.getenv("BASE_URL")
    }

# Монтируем статические файлы
app.mount("/static", StaticFiles(directory="static"), name="static")

# Маршрут для главной страницы
@app.get("/")
async def index(request: Request):
    """Отображает главную страницу."""
    return templates.TemplateResponse(
        "index.html",
        get_base_context(request)
    )

# Маршрут для страницы входа
@app.get("/login")
async def login(request: Request):
    """Отображает страницу входа."""
    return templates.TemplateResponse(
        "login.html",
        get_base_context(request)
    )

# Маршрут для админ-панели
@app.get("/admin")
async def admin_panel(request: Request):
    # Пытаемся получить ключ из заголовка 
    admin_key = request.headers.get("X-Admin-Key")
    
    # Если ключа нет в заголовке, пытаемся получить его из query параметра
    if not admin_key:
        admin_key = request.query_params.get("admin_key")
    
    # Если ключа нет и в query параметрах, пытаемся получить из cookie
    if not admin_key:
        admin_key = request.cookies.get("admin_key")
        
    if not admin_key or not await verify_admin_key(admin_key):
        return RedirectResponse(url="/login")
    
    return templates.TemplateResponse(
        "admin_panel.html",
        {"request": request}
    )

@app.post("/admin/validate")
async def validate_admin_key(request: Request):
    # Пытаемся получить ключ из заголовка
    admin_key = request.headers.get("X-Admin-Key")
    
    # Если ключа нет в заголовке, пытаемся получить из cookie
    if not admin_key:
        admin_key = request.cookies.get("admin_key")
    
    if not admin_key or not await verify_admin_key(admin_key):
        raise HTTPException(status_code=401, detail="Invalid admin key")
    
    return JSONResponse(content={"status": "success"})

# Админ-эндпоинты для управления аккаунтами
@app.post("/admin/users/{user_id}/telegram")
async def add_telegram_account(user_id: str, request: Request):
    admin_key = request.headers.get("X-Admin-Key")
    if not admin_key or not await verify_admin_key(admin_key):
        raise HTTPException(status_code=401, detail="Invalid admin key")
    
    data = await request.json()
    
    # Добавляем аккаунт через admin_panel
    await admin_add_telegram_account(user_id, data)
    return {"status": "success"}

@app.delete("/admin/users/{user_id}/telegram/{phone}")
async def delete_telegram_account(user_id: str, phone: str, request: Request):
    admin_key = request.headers.get("X-Admin-Key")
    if not admin_key or not await verify_admin_key(admin_key):
        raise HTTPException(status_code=401, detail="Invalid admin key")
    
    # Находим ID аккаунта по номеру телефона
    user = await admin_get_user(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    account_id = None
    for account in user.get("telegram_accounts", []):
        if account.get("phone") == phone:
            account_id = account.get("id")
            break
    
    if not account_id:
        raise HTTPException(status_code=404, detail="Telegram account not found")
    
    # Удаляем аккаунт через admin_panel
    await admin_delete_telegram_account(user_id, account_id)
    return {"status": "success"}

@app.post("/admin/users/{user_id}/vk")
async def add_vk_account(user_id: str, request: Request):
    admin_key = request.headers.get("X-Admin-Key")
    if not admin_key or not await verify_admin_key(admin_key):
        raise HTTPException(status_code=401, detail="Invalid admin key")
    
    data = await request.json()
    
    # Добавляем аккаунт через admin_panel
    await admin_add_vk_account(user_id, data)
    return {"status": "success"}

@app.delete("/api/vk/accounts/{account_id}")
async def delete_vk_account_endpoint(request: Request, account_id: str):
    """Удаляет VK аккаунт."""
    auth_header = request.headers.get('authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        raise HTTPException(401, "API ключ обязателен")
    admin_key = auth_header.split(' ')[1]
    
    # Проверяем админ-ключ
    if not await verify_admin_key(admin_key):
        raise HTTPException(401, "Неверный админ-ключ")
    
    # Получаем ID пользователя, для которого удаляется аккаунт
    user_id = request.headers.get('X-User-Id')
    if not user_id:
        raise HTTPException(400, "ID пользователя не указан")
    
    # Удаляем аккаунт через admin_panel
    await admin_delete_vk_account(user_id, account_id)
    
    return {"status": "success"}

# Админ-эндпоинты
@app.get("/admin/stats")
async def get_admin_stats(request: Request):
    """Получает статистику системы."""
    admin_key = request.headers.get("X-Admin-Key")
    if not admin_key or admin_key != os.getenv("ADMIN_KEY"):
        raise HTTPException(401, "Неверный админ-ключ")
    
    stats = await get_system_stats()
    return stats

@app.get("/admin/users")
async def get_users(request: Request):
    # Пытаемся получить ключ из заголовка
    admin_key = request.headers.get("X-Admin-Key")
    
    # Если ключа нет в заголовке, пытаемся получить из cookie
    if not admin_key:
        admin_key = request.cookies.get("admin_key")
    
    if not admin_key or not await verify_admin_key(admin_key):
        raise HTTPException(status_code=401, detail="Invalid admin key")
    
    users_data = get_users_dict()
    users_list = []
    
    # Сначала проверим, нужно ли обновить структуру данных с API ключами
    users_updated = False
    
    for user_id, user_info in users_data.items():
        # Добавляем ID к пользователю
        user_data = {**user_info, "id": user_id}
        
        # Убедимся, что у каждого пользователя есть API ключ
        if "api_key" not in user_info:
            api_key = str(uuid.uuid4())
            users_data[user_id]["api_key"] = api_key
            user_data["api_key"] = api_key
            users_updated = True
        
        users_list.append(user_data)
    
    # Сохраняем обновленные данные с API ключами, если были изменения
    if users_updated:
        save_users(users_data)
    
    return users_list

@app.get("/admin/users/{api_key}", dependencies=[Security(verify_admin_key)])
async def admin_user(api_key: str):
    """Получает информацию о конкретном пользователе."""
    return await admin_get_user(api_key)

@app.delete("/admin/users/{user_id}")
async def delete_user_by_id(user_id: str, request: Request):
    # Проверяем админ-ключ
    admin_key = request.headers.get("X-Admin-Key")
    if not admin_key:
        admin_key = request.cookies.get("admin_key")
    
    if not admin_key or not await verify_admin_key(admin_key):
        raise HTTPException(status_code=401, detail="Invalid admin key")
    
    logger.info(f"Попытка удаления пользователя с ID: {user_id}")
    
    # Вместо удаления из словаря используем функцию admin_delete_user
    try:
        # Проверяем существование пользователя напрямую через базу данных
        from user_manager import get_db_connection
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('SELECT api_key FROM users WHERE api_key = ?', (user_id,))
        user = cursor.fetchone()
        
        if not user:
            logger.error(f"Пользователь с ID {user_id} не найден в базе данных")
            conn.close()
            raise HTTPException(status_code=404, detail="Пользователь не найден")
        
        logger.info(f"Пользователь найден, удаляем аккаунты пользователя")
        
        # Получаем ID всех аккаунтов перед удалением для логирования
        cursor.execute('SELECT id FROM telegram_accounts WHERE user_api_key = ?', (user_id,))
        telegram_accounts = [row['id'] for row in cursor.fetchall()]
        logger.info(f"Telegram аккаунты для удаления: {telegram_accounts}")
        
        cursor.execute('SELECT id FROM vk_accounts WHERE user_api_key = ?', (user_id,))
        vk_accounts = [row['id'] for row in cursor.fetchall()]
        logger.info(f"VK аккаунты для удаления: {vk_accounts}")
        
        # Сначала удаляем все аккаунты пользователя
        cursor.execute('DELETE FROM telegram_accounts WHERE user_api_key = ?', (user_id,))
        telegram_deleted = cursor.rowcount
        logger.info(f"Удалено telegram аккаунтов: {telegram_deleted}")
        
        cursor.execute('DELETE FROM vk_accounts WHERE user_api_key = ?', (user_id,))
        vk_deleted = cursor.rowcount
        logger.info(f"Удалено vk аккаунтов: {vk_deleted}")
        
        # Затем удаляем самого пользователя
        cursor.execute('DELETE FROM users WHERE api_key = ?', (user_id,))
        user_deleted = cursor.rowcount
        logger.info(f"Удалено пользователей: {user_deleted}")
        
        conn.commit()
        conn.close()
        
        if user_deleted > 0:
            logger.info(f"Пользователь {user_id} успешно удален")
            return {"status": "success", "message": "User deleted successfully"}
        else:
            logger.error(f"Не удалось удалить пользователя {user_id}")
            raise HTTPException(status_code=500, detail="Не удалось удалить пользователя")
        
    except HTTPException as e:
        # Перенаправляем ошибку, если пользователь не найден
        logger.error(f"HTTP ошибка при удалении пользователя: {str(e)}")
        raise e
    except Exception as e:
        # Логируем другие ошибки
        logger.error(f"Ошибка при удалении пользователя: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Ошибка при удалении пользователя: {str(e)}")

@app.put("/admin/users/{api_key}/vk-token", dependencies=[Security(verify_admin_key)])
async def admin_update_vk_token(api_key: str, vk_token: str):
    """Обновляет VK токен пользователя."""
    return await update_user_vk_token(api_key, vk_token)

# Существующие эндпоинты
@app.post("/register")
async def register_user_endpoint(request: Request):
    admin_key = request.headers.get("X-Admin-Key")
    if not admin_key:
        admin_key = request.cookies.get("admin_key")
    
    if not admin_key or not await verify_admin_key(admin_key):
        raise HTTPException(status_code=401, detail="Invalid admin key")
    
    data = await request.json()
    username = data.get("username")
    password = data.get("password")
    
    if not username or not password:
        raise HTTPException(status_code=400, detail="Username and password are required")
    
    # Проверяем, не занято ли имя пользователя
    users = get_users_dict()
    for user_id, user_data in users.items():
        if user_data.get("username") == username:
            raise HTTPException(status_code=400, detail="Username already exists")
    
    # Создаем нового пользователя с помощью функции из user_manager
    api_key = await register_user(username, password)
    
    return {"id": api_key, "username": username, "api_key": api_key}

@app.post("/set-vk-token")
async def set_token(request: Request, data: dict):
    auth_header = request.headers.get('authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        raise HTTPException(401, "API ключ обязателен")
    api_key = auth_header.split(' ')[1]
    token = data.get('token')
    if not token:
        raise HTTPException(400, "Токен VK не указан")
    await set_vk_token(api_key, token)
    vk_clients[api_key] = VKClient(token)
    return {"message": "Токен VK установлен"}

@app.post("/find-groups")
async def find_groups(request: Request, data: dict):
    platform = data.get('platform', 'telegram')
    keywords = data.get('keywords', [])
    if not keywords:
        raise HTTPException(400, "Ключевые слова обязательны")
    
    min_members = data.get('min_members', 1000 if platform == 'vk' else 100000)
    max_groups = data.get('max_groups', 10 if platform == 'vk' else 20)

    if platform == 'telegram':
        client = await auth_middleware(request, 'telegram')
        return await find_channels(client, keywords, min_members, max_groups)
    elif platform == 'vk':
        vk = await auth_middleware(request, 'vk')
        return await find_vk_groups(vk, keywords, min_members, max_groups)
    raise HTTPException(400, "Платформа не поддерживается")

@app.post("/trending-posts")
async def trending_posts(request: Request, data: dict):
    platform = data.get('platform', 'telegram')
    group_ids = data.get('group_ids', [])
    if not group_ids:
        raise HTTPException(400, "ID групп обязательны")
    
    days_back = data.get('days_back', 7)
    posts_per_group = data.get('posts_per_group', 10)
    min_views = data.get('min_views')

    if platform == 'telegram':
        client = await auth_middleware(request, 'telegram')
        return await get_trending_posts(client, group_ids, days_back, posts_per_group, min_views)
    elif platform == 'vk':
        vk = await auth_middleware(request, 'vk')
        return await get_vk_posts_in_groups(vk, group_ids, count=posts_per_group * len(group_ids), min_views=min_views, days_back=days_back)
    raise HTTPException(400, "Платформа не поддерживается")

@app.post("/posts")
async def get_posts(request: Request, data: dict):
    platform = data.get('platform', 'telegram')
    group_ids = data.get('group_ids', [])
    if not group_ids:
        raise HTTPException(400, "ID групп обязательны")
    
    keywords = data.get('keywords')
    count = data.get('count', 10)
    min_views = data.get('min_views', 1000)
    days_back = data.get('days_back', 3 if platform == 'telegram' else 7)

    if platform == 'telegram':
        client = await auth_middleware(request, 'telegram')
        return await get_posts_in_channels(client, group_ids, keywords, count, min_views, days_back)
    elif platform == 'vk':
        vk = await auth_middleware(request, 'vk')
        return await get_vk_posts_in_groups(vk, group_ids, keywords, count, min_views, days_back)
    raise HTTPException(400, "Платформа не поддерживается")

@app.post("/posts-by-keywords")
async def posts_by_keywords(request: Request, data: dict):
    platform = data.get('platform', 'telegram')
    group_keywords = data.get('group_keywords', [])
    if not group_keywords:
        raise HTTPException(400, "Ключевые слова для групп обязательны")
    
    post_keywords = data.get('post_keywords', [])
    count = data.get('count', 10)
    min_views = data.get('min_views', 1000)
    days_back = data.get('days_back', 3 if platform == 'telegram' else 7)
    max_groups = data.get('max_groups', 10)

    if platform == 'telegram':
        client = await auth_middleware(request, 'telegram')
        return await get_posts_by_keywords(client, group_keywords, post_keywords, count, min_views, days_back)
    elif platform == 'vk':
        vk = await auth_middleware(request, 'vk')
        return await get_vk_posts(vk, group_keywords, post_keywords, count, min_views, days_back, max_groups)
    raise HTTPException(400, "Платформа не поддерживается")

@app.post("/posts-by-period")
async def get_posts_by_period(request: Request, data: dict):
    """Получение постов из групп за указанный период."""
    platform = data.get('platform', 'telegram')
    group_ids = data.get('group_ids', [])
    if not group_ids:
        raise HTTPException(400, "ID групп обязательны")
    
    max_posts = data.get('max_posts', 100)
    days_back = data.get('days_back', 7)
    min_views = data.get('min_views', 0)

    if platform == 'telegram':
        client = await auth_middleware(request, 'telegram')
        from telegram_utils import get_posts_by_period as get_telegram_posts_by_period
        return await get_telegram_posts_by_period(client, group_ids, max_posts, days_back, min_views)
    elif platform == 'vk':
        vk = await auth_middleware(request, 'vk')
        from vk_utils import get_posts_by_period as get_vk_posts_by_period
        return await get_vk_posts_by_period(vk, group_ids, max_posts, days_back, min_views)
    raise HTTPException(400, "Платформа не поддерживается")

@app.get("/api/accounts/status")
async def get_accounts_status(api_key: str = Header(...)):
    """Получает статус всех аккаунтов."""
    if not verify_api_key(api_key):
        raise HTTPException(status_code=401, detail="Неверный API ключ")
    
    telegram_status = get_account_status(api_key, "telegram")
    vk_status = get_account_status(api_key, "vk")
    
    return {
        "telegram": telegram_status,
        "vk": vk_status
    }

@app.post("/api/vk/posts-by-period")
async def get_vk_posts_by_period(
    group_ids: List[int],
    max_posts: int = 100,
    days_back: int = 7,
    min_views: int = 0,
    api_key: str = Header(...)
):
    """Получение постов из групп за указанный период."""
    if not verify_api_key(api_key):
        raise HTTPException(status_code=401, detail="Неверный API ключ")
    
    account = get_next_available_account(api_key, "vk")
    if not account:
        raise HTTPException(status_code=429, detail="Достигнут лимит запросов")
    
    async with VKClient(account["access_token"], account.get("proxy"), account["id"]) as vk:
        posts = await vk.get_posts_by_period(group_ids, max_posts, days_back, min_views)
        return {"posts": posts}

# Маршрут для страницы регистрации
@app.get("/register")
async def register_page(request: Request):
    """Отображает страницу регистрации."""
    return templates.TemplateResponse(
        "register.html",
        get_base_context(request)
    )

# Маршрут для страницы управления аккаунтами
@app.get("/accounts")
async def accounts_page(request: Request):
    """Отображает страницу управления аккаунтами."""
    auth_header = request.headers.get('authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return templates.TemplateResponse(
            "register.html",
            get_base_context(request)
        )
    api_key = auth_header.split(' ')[1]
    
    # Проверяем валидность API ключа
    if not verify_api_key(api_key):
        return templates.TemplateResponse(
            "register.html",
            get_base_context(request)
        )
    
    return templates.TemplateResponse(
        "accounts.html",
        get_base_context(request)
    )

# Эндпоинты для работы с Telegram аккаунтами
@app.post("/api/telegram/accounts")
async def add_telegram_account_endpoint(request: Request):
    """Добавляет новый Telegram аккаунт."""
    logger.info("Начало обработки запроса на добавление Telegram аккаунта")
    auth_header = request.headers.get('authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        logger.error("API ключ не предоставлен или в неверном формате")
        raise HTTPException(401, "API ключ обязателен")
    admin_key = auth_header.split(' ')[1]
    
    # Получаем ID пользователя, для которого добавляется аккаунт
    user_id = request.headers.get('X-User-Id')
    if not user_id:
        logger.error("ID пользователя не указан")
        raise HTTPException(400, "ID пользователя не указан")
    
    # Проверяем админ-ключ
    if not await verify_admin_key(admin_key):
        logger.error(f"Неверный админ-ключ: {admin_key}")
        raise HTTPException(401, "Неверный админ-ключ")
    
    logger.info(f"Админ-ключ верифицирован, добавляем аккаунт для пользователя {user_id}")
    
    form_data = await request.form()
    api_id = form_data.get('api_id')
    api_hash = form_data.get('api_hash')
    phone = form_data.get('phone')
    proxy = form_data.get('proxy')
    
    logger.info(f"Получены данные формы: api_id={api_id}, phone={phone}, proxy={'указан' if proxy else 'не указан'}")
    
    if not api_id or not api_hash or not phone:
        logger.error("Обязательные поля не заполнены")
        raise HTTPException(400, "Обязательные поля не заполнены")
    
    # Создаем новый аккаунт
    account_data = {
        "api_id": api_id,
        "api_hash": api_hash,
        "phone": phone,
        "proxy": proxy,
        "status": "pending"
    }
    
    # Создаем директорию для сессий, если её нет
    os.makedirs("sessions", exist_ok=True)
    logger.info("Создана директория для сессий")

    # Создаем директорию для текущего пользователя
    user_sessions_dir = f"sessions/{user_id}"
    os.makedirs(user_sessions_dir, exist_ok=True)
    logger.info(f"Создана директория для сессий пользователя: {user_sessions_dir}")

    # Генерируем ID аккаунта
    account_id = str(uuid.uuid4())
    account_data["id"] = account_id

    # Создаем стандартное имя сессии (Telethon добавит .session)
    session_name = f"{user_sessions_dir}/{phone}"
    account_data["session_file"] = session_name
    logger.info(f"Назначено имя сессии: {session_name}")
    
    # Создаем Telegram клиент и отправляем код
    logger.info(f"Создаем Telegram клиент с сессией {session_name}")
    try:
        # Убедимся, что api_id передается как число
        api_id_int = int(api_id)
        logger.info(f"Преобразован api_id в число: {api_id_int}")
        
        client = await create_telegram_client(session_name, api_id_int, api_hash, proxy)
        
        logger.info("Устанавливаем соединение с Telegram")
        await client.connect()
        logger.info("Соединение с Telegram установлено")
        
        # Проверяем, авторизован ли уже клиент
        logger.info("Проверка статуса авторизации")
        is_authorized = await client.is_user_authorized()
        logger.info(f"Клиент {'авторизован' if is_authorized else 'не авторизован'}")
        
        if is_authorized:
            logger.info("Клиент уже авторизован, обновляем статус аккаунта")
            account_data["status"] = "active"
            await admin_add_telegram_account(user_id, account_data)
            await client.disconnect()
            logger.info("Соединение с Telegram закрыто")
            logger.info("Клиент уже авторизован, сессия сохранена в файл")
            
            return {
                "account_id": account_id,
                "requires_auth": False
            }
        
        # Отправляем запрос на код авторизации
        logger.info(f"Отправка запроса на код авторизации для номера {phone}")
        result = await client.send_code_request(phone)
        logger.info(f"Запрос на код авторизации успешно отправлен, результат: {result}")
        
        # Сохраняем phone_code_hash в данных аккаунта
        account_data["phone_code_hash"] = result.phone_code_hash
        
        await client.disconnect()
        logger.info("Соединение с Telegram закрыто")
        
        # Добавляем аккаунт в базу данных
        logger.info(f"Добавление аккаунта в базу данных, ID: {account_id}")
        await admin_add_telegram_account(user_id, account_data)
        
        logger.info("Аккаунт добавлен, требуется авторизация")
        return {
            "account_id": account_id,
            "requires_auth": True
        }
    except Exception as e:
        logger.error(f"Ошибка при создании аккаунта: {str(e)}")
        # Если произошла ошибка, удаляем файл сессии, если он был создан
        session_name = account_data.get("session_file")
        if session_name:
            session_path = f"{session_name}.session"
            if os.path.exists(session_path):
                os.remove(session_path)
            # Проверяем, пуста ли директория пользователя, и если да, удаляем её
            user_dir = os.path.dirname(session_name)
            if os.path.exists(user_dir) and not os.listdir(user_dir):
                os.rmdir(user_dir)
                logger.info(f"Удалена пустая директория: {user_dir}")
        
        raise HTTPException(400, f"Ошибка при создании аккаунта: {str(e)}")

@app.post("/api/telegram/verify-code")
async def verify_telegram_code(request: Request):
    """Проверяет код авторизации Telegram."""
    logger.info("Начало проверки кода авторизации Telegram")
    auth_header = request.headers.get('authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        logger.error("API ключ не предоставлен или в неверном формате")
        raise HTTPException(401, "API ключ обязателен")
    admin_key = auth_header.split(' ')[1]
    
    # Проверяем админ-ключ
    if not await verify_admin_key(admin_key):
        logger.error("Неверный админ-ключ")
        raise HTTPException(401, "Неверный админ-ключ")
    
    data = await request.json()
    account_id = data.get('account_id')
    code = data.get('code')
    
    logger.info(f"Полученные данные: account_id={account_id}, code={code}")
    
    if not account_id or not code:
        logger.error("Не указаны необходимые параметры")
        raise HTTPException(400, "Не указаны необходимые параметры")
    
    # Находим аккаунт по ID
    logger.info(f"Поиск аккаунта с ID {account_id}")
    found = False
    
    # Ищем пользователя, которому принадлежит аккаунт
    from user_manager import get_db_connection
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT * FROM telegram_accounts 
        WHERE id = ?
    ''', (account_id,))
    
    account = cursor.fetchone()
    
    if not account:
        logger.error(f"Аккаунт с ID {account_id} не найден")
        conn.close()
        raise HTTPException(404, "Аккаунт не найден")
    
    user_api_key = account['user_api_key']
    
    try:
        phone = account['phone']
        api_id = int(account['api_id'])
        api_hash = account['api_hash']
        proxy = account['proxy']
        session_file = account['session_file']
        
        logger.info(f"Создание клиента Telegram, сессия: {session_file}")
        client = await create_telegram_client(session_file, api_id, api_hash, proxy)
        
        logger.info("Подключение к Telegram")
        await client.connect()
        
        # Если уже авторизован, просто возвращаем успех
        logger.info("Проверка статуса авторизации")
        if await client.is_user_authorized():
            logger.info("Клиент уже авторизован")
            await client.disconnect()
            
            # Обновляем статус аккаунта
            cursor.execute('''
                UPDATE telegram_accounts
                SET status = ?
                WHERE id = ?
            ''', ('active', account_id))
            conn.commit()
            
            # При использовании файловой сессии нет необходимости сохранять session_string
            logger.info("Авторизация выполнена успешно, сессия сохранена в файл")
            
            conn.close()
            return {"account_id": account_id, "requires_2fa": False}
        
        # Пытаемся авторизоваться с кодом
        try:
            logger.info(f"Вход с кодом авторизации: {code}")
            # Преобразуем объект sqlite3.Row в словарь
            account_dict = dict(account)
            result = await client.sign_in(
                phone, 
                code,
                phone_code_hash=account_dict.get("phone_code_hash")
            )
            
            # Успешная авторизация
            logger.info("Успешная авторизация!")
            await client.disconnect()
            
            # Обновляем статус аккаунта
            cursor.execute('''
                UPDATE telegram_accounts
                SET status = ?
                WHERE id = ?
            ''', ('active', account_id))
            conn.commit()
            
            # При использовании файловой сессии нет необходимости сохранять session_string
            logger.info("Авторизация выполнена успешно, сессия сохранена в файл")
            
            conn.close()
            return {"account_id": account_id, "requires_2fa": False}
        except PhoneCodeInvalidError:
            logger.error("Неверный код")
            await client.disconnect()
            conn.close()
            raise HTTPException(400, "Неверный код")
        except SessionPasswordNeededError:
            # Требуется 2FA
            logger.info("Требуется 2FA")
            await client.disconnect()
            conn.close()
            return {"account_id": account_id, "requires_2fa": True}
    except Exception as e:
        logger.error(f"Ошибка авторизации: {str(e)}")
        conn.close()
        raise HTTPException(400, f"Ошибка авторизации: {str(e)}")

@app.post("/api/telegram/verify-2fa")
async def verify_telegram_2fa(request: Request):
    """Проверяет пароль 2FA для Telegram."""
    auth_header = request.headers.get('authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        raise HTTPException(401, "API ключ обязателен")
    admin_key = auth_header.split(' ')[1]
    
    # Проверяем админ-ключ
    if not await verify_admin_key(admin_key):
        raise HTTPException(401, "Неверный админ-ключ")
    
    data = await request.json()
    account_id = data.get('account_id')
    password = data.get('password')
    
    if not account_id or not password:
        raise HTTPException(400, "Не указаны необходимые параметры")
    
    # Находим аккаунт по ID
    from user_manager import get_db_connection
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT * FROM telegram_accounts 
        WHERE id = ?
    ''', (account_id,))
    
    account = cursor.fetchone()
    
    if not account:
        conn.close()
        raise HTTPException(404, "Аккаунт не найден")
    
    try:
        # Преобразуем объект sqlite3.Row в словарь
        account_dict = dict(account)
        session_file = account_dict["session_file"]
        client = await create_telegram_client(session_file, int(account_dict["api_id"]), account_dict["api_hash"], account_dict.get("proxy"))
        
        if account_dict.get("proxy"):
            client.set_proxy(account_dict["proxy"])
        
        await client.connect()
        await client.sign_in(password=password)
        await client.disconnect()
        
        # Обновляем статус аккаунта
        cursor.execute('''
            UPDATE telegram_accounts
            SET status = ?
            WHERE id = ?
        ''', ('active', account_id))
        conn.commit()
        
        # При использовании файловой сессии нет необходимости сохранять session_string
        logger.info("2FA авторизация выполнена успешно, сессия сохранена в файл")
        
        conn.close()
        
        return {"status": "success"}
    except Exception as e:
        conn.close()
        raise HTTPException(400, f"Ошибка авторизации: {str(e)}")

@app.delete("/api/telegram/accounts/{phone}")
async def delete_telegram_account_endpoint(request: Request, phone: str):
    """Удаляет Telegram аккаунт."""
    auth_header = request.headers.get('authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        raise HTTPException(401, "API ключ обязателен")
    admin_key = auth_header.split(' ')[1]
    
    # Проверяем админ-ключ
    if not await verify_admin_key(admin_key):
        raise HTTPException(401, "Неверный админ-ключ")
    
    # Получаем ID пользователя, для которого удаляется аккаунт
    user_id = request.headers.get('X-User-Id')
    if not user_id:
        raise HTTPException(400, "ID пользователя не указан")
    
    # Находим аккаунт по номеру телефона
    from user_manager import get_db_connection
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT * FROM telegram_accounts 
        WHERE user_api_key = ? AND phone = ?
    ''', (user_id, phone))
    
    account = cursor.fetchone()
    
    if not account:
        conn.close()
        raise HTTPException(404, "Аккаунт с указанным номером телефона не найден")
    
    # Преобразуем объект sqlite3.Row в словарь
    account_dict = dict(account)
    account_id = account_dict['id']
    session_file = account_dict.get('session_file')
    
    # Удаляем файл сессии, если он есть
    if session_file:
        session_path = f"{session_file}.session"
        if os.path.exists(session_path):
            os.remove(session_path)
        # Проверяем, пуста ли директория пользователя, и если да, удаляем её
        user_dir = os.path.dirname(session_file)
        if os.path.exists(user_dir) and not os.listdir(user_dir):
            os.rmdir(user_dir)
            logger.info(f"Удалена пустая директория: {user_dir}")
    
    # Удаляем аккаунт из базы данных
    cursor.execute('''
        DELETE FROM telegram_accounts 
        WHERE id = ?
    ''', (account_id,))
    
    conn.commit()
    conn.close()
    
    return {"status": "success"}

# Эндпоинты для работы с VK аккаунтами
@app.post("/api/vk/accounts")
async def add_vk_account_endpoint(request: Request):
    """Добавляет новый VK аккаунт."""
    auth_header = request.headers.get('authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        raise HTTPException(401, "API ключ обязателен")
    admin_key = auth_header.split(' ')[1]
    
    # Получаем ID пользователя, для которого добавляется аккаунт
    user_id = request.headers.get('X-User-Id')
    if not user_id:
        raise HTTPException(400, "ID пользователя не указан")
    
    # Проверяем админ-ключ
    if not await verify_admin_key(admin_key):
        raise HTTPException(401, "Неверный админ-ключ")
    
    data = await request.json()
    token = data.get('token')
    proxy = data.get('proxy')
    
    if not token:
        raise HTTPException(400, "Токен VK обязателен")
    
    # Создаем новый аккаунт
    account_data = {
        "token": token,
        "proxy": proxy,
        "status": "active"
    }
    
    account_id = str(uuid.uuid4())
    account_data["id"] = account_id
    
    # Добавляем аккаунт в базу данных
    await admin_add_vk_account(user_id, account_data)
    
    return {
        "account_id": account_id,
        "status": "success"
    }

@app.get("/api/accounts/status")
async def get_accounts_status(request: Request):
    """Получает статус всех аккаунтов пользователя."""
    auth_header = request.headers.get('authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        raise HTTPException(401, "API ключ обязателен")
    api_key = auth_header.split(' ')[1]
    
    # Получаем данные пользователя
    user_data = await admin_get_user(api_key)
    if not user_data:
        raise HTTPException(404, "Пользователь не найден")
    
    return {
        "telegram_accounts": user_data.get("telegram_accounts", []),
        "vk_accounts": user_data.get("vk_accounts", [])
    }

@app.get("/admin/users/{user_id}/api-key")
async def get_user_api_key(user_id: str, request: Request):
    # Проверяем админ-ключ
    admin_key = request.headers.get("X-Admin-Key")
    if not admin_key:
        admin_key = request.cookies.get("admin_key")
    
    if not admin_key or not await verify_admin_key(admin_key):
        raise HTTPException(status_code=401, detail="Invalid admin key")
    
    # Получаем данные пользователя
    user = await admin_get_user(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    # API ключ теперь хранится как primary key пользователя
    api_key = user_id
    
    return {"api_key": api_key}

@app.post("/admin/users/{user_id}/regenerate-api-key")
async def regenerate_api_key(user_id: str, request: Request):
    # Проверяем админ-ключ
    admin_key = request.headers.get("X-Admin-Key")
    if not admin_key:
        admin_key = request.cookies.get("admin_key")
    
    if not admin_key or not await verify_admin_key(admin_key):
        raise HTTPException(status_code=401, detail="Invalid admin key")
    
    # Это действие невозможно с новой структурой данных,
    # так как api_key теперь является primary key пользователя
    raise HTTPException(status_code=400, detail="API key regeneration is not supported with the new database structure")

@app.get("/api/telegram/accounts/{account_id}/status")
async def check_telegram_account_status(request: Request, account_id: str):
    """Проверяет статус авторизации аккаунта Telegram."""
    logger.info(f"Запрос на проверку статуса аккаунта с ID {account_id}")
    auth_header = request.headers.get('authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        logger.error("API ключ не предоставлен или в неверном формате")
        raise HTTPException(401, "API ключ обязателен")
    admin_key = auth_header.split(' ')[1]
    
    # Проверяем админ-ключ
    if not await verify_admin_key(admin_key):
        logger.error("Неверный админ-ключ")
        raise HTTPException(401, "Неверный админ-ключ")
    
    # Находим аккаунт по ID
    from user_manager import get_db_connection
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT * FROM telegram_accounts 
        WHERE id = ?
    ''', (account_id,))
    
    account = cursor.fetchone()
    
    if not account:
        logger.error(f"Аккаунт с ID {account_id} не найден")
        conn.close()
        raise HTTPException(404, "Аккаунт не найден")
    
    # Преобразуем объект sqlite3.Row в словарь
    account_dict = dict(account)
    session_file = account_dict["session_file"]
    api_id = int(account_dict["api_id"])
    api_hash = account_dict["api_hash"]
    proxy = account_dict.get("proxy")
    current_status = account_dict.get("status", "unknown")
    
    try:
        logger.info(f"Создание клиента Telegram, сессия: {session_file}")
        client = await create_telegram_client(session_file, api_id, api_hash, proxy)
        
        logger.info("Устанавливаем соединение с Telegram")
        await client.connect()
        
        # Проверяем, авторизован ли клиент
        is_authorized = await client.is_user_authorized()
        await client.disconnect()
        
        # Обновляем статус в базе данных, если есть изменения
        if is_authorized and current_status != "active":
            cursor.execute('''
                UPDATE telegram_accounts
                SET status = ?
                WHERE id = ?
            ''', ('active', account_id))
            conn.commit()
            logger.info(f"Статус аккаунта обновлен на 'active'")
        elif not is_authorized and current_status != "pending":
            cursor.execute('''
                UPDATE telegram_accounts
                SET status = ?
                WHERE id = ?
            ''', ('pending', account_id))
            conn.commit()
            logger.info(f"Статус аккаунта обновлен на 'pending'")
        
        conn.close()
        return {
            "account_id": account_id,
            "is_authorized": is_authorized,
            "status": "active" if is_authorized else "pending"
        }
    except Exception as e:
        logger.error(f"Ошибка при проверке статуса аккаунта: {str(e)}")
        conn.close()
        return {
            "account_id": account_id,
            "is_authorized": False,
            "status": "error",
            "error": str(e)
        }

@app.post("/api/telegram/accounts/{account_id}/resend-code")
async def resend_telegram_code(request: Request, account_id: str):
    """Повторно отправляет код авторизации для существующего аккаунта Telegram."""
    logger.info(f"Запрос на повторную отправку кода для аккаунта с ID {account_id}")
    auth_header = request.headers.get('authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        logger.error("API ключ не предоставлен или в неверном формате")
        raise HTTPException(401, "API ключ обязателен")
    admin_key = auth_header.split(' ')[1]
    
    # Проверяем админ-ключ
    if not await verify_admin_key(admin_key):
        logger.error("Неверный админ-ключ")
        raise HTTPException(401, "Неверный админ-ключ")
    
    # Находим аккаунт по ID
    from user_manager import get_db_connection
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT * FROM telegram_accounts 
        WHERE id = ?
    ''', (account_id,))
    
    account = cursor.fetchone()
    
    if not account:
        logger.error(f"Аккаунт с ID {account_id} не найден")
        conn.close()
        raise HTTPException(404, "Аккаунт не найден")
    
    # Преобразуем объект sqlite3.Row в словарь
    account_dict = dict(account)
    session_file = account_dict["session_file"]
    api_id = int(account_dict["api_id"])
    api_hash = account_dict["api_hash"]
    phone = account_dict["phone"]
    proxy = account_dict.get("proxy")
    
    try:
        logger.info(f"Создание клиента Telegram, сессия: {session_file}")
        client = await create_telegram_client(session_file, api_id, api_hash, proxy)
        
        logger.info("Устанавливаем соединение с Telegram")
        await client.connect()
        
        # Проверяем, не авторизован ли уже клиент
        if await client.is_user_authorized():
            logger.info("Клиент уже авторизован")
            await client.disconnect()
            
            # Обновляем статус аккаунта
            cursor.execute('''
                UPDATE telegram_accounts
                SET status = ?
                WHERE id = ?
            ''', ('active', account_id))
            conn.commit()
            conn.close()
            
            return {
                "account_id": account_id,
                "requires_auth": False,
                "message": "Аккаунт уже авторизован"
            }
        
        # Отправляем запрос на код авторизации
        logger.info(f"Отправка запроса на код авторизации для номера {phone}")
        result = await client.send_code_request(phone)
        logger.info(f"Запрос на код авторизации успешно отправлен, результат: {result}")
        
        # Сохраняем phone_code_hash
        cursor.execute('''
            UPDATE telegram_accounts
            SET phone_code_hash = ?
            WHERE id = ?
        ''', (result.phone_code_hash, account_id))
        conn.commit()
        
        await client.disconnect()
        logger.info("Соединение с Telegram закрыто")
        
        conn.close()
        return {
            "account_id": account_id,
            "requires_auth": True,
            "message": "Код авторизации отправлен"
        }
    except Exception as e:
        logger.error(f"Ошибка при отправке кода авторизации: {str(e)}")
        conn.close()
        raise HTTPException(400, f"Ошибка при отправке кода авторизации: {str(e)}")

@app.post("/api/telegram/upload-session")
async def upload_telegram_session(request: Request):
    """Загружает файл сессии Telegram и добавляет аккаунт."""
    logger.info("Начало обработки запроса на загрузку файла сессии Telegram")
    auth_header = request.headers.get('authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        logger.error("API ключ не предоставлен или в неверном формате")
        raise HTTPException(401, "API ключ обязателен")
    admin_key = auth_header.split(' ')[1]
    
    # Получаем ID пользователя
    user_id = request.headers.get('X-User-Id')
    if not user_id:
        logger.error("ID пользователя не указан")
        raise HTTPException(400, "ID пользователя не указан")
    
    # Проверяем админ-ключ
    if not await verify_admin_key(admin_key):
        logger.error(f"Неверный админ-ключ: {admin_key}")
        raise HTTPException(401, "Неверный админ-ключ")
    
    logger.info(f"Админ-ключ верифицирован, обрабатываем файл сессии для пользователя {user_id}")
    
    # Получаем данные из формы
    form_data = await request.form()
    api_id = form_data.get('api_id')
    api_hash = form_data.get('api_hash')
    phone = form_data.get('phone')
    proxy = form_data.get('proxy')
    session_file = form_data.get('session_file')
    
    if not api_id or not api_hash or not phone or not session_file:
        logger.error("Обязательные поля не заполнены")
        raise HTTPException(400, "Обязательные поля не заполнены")
    
    # Создаем директорию для сессий, если её нет
    os.makedirs("sessions", exist_ok=True)
    user_sessions_dir = f"sessions/{user_id}"
    os.makedirs(user_sessions_dir, exist_ok=True)
    
    # Путь к файлу сессии
    session_path = f"{user_sessions_dir}/{phone}"
    full_session_path = f"{session_path}.session"
    
    # Сохраняем файл сессии
    session_content = await session_file.read()
    with open(full_session_path, "wb") as f:
        f.write(session_content)
    
    logger.info(f"Файл сессии сохранен: {full_session_path}")
    
    # Создаем новый аккаунт
    account_data = {
        "api_id": api_id,
        "api_hash": api_hash,
        "phone": phone,
        "proxy": proxy,
        "session_file": session_path,
        "status": "pending"  # Изначально устанавливаем статус pending
    }
    
    # Генерируем ID аккаунта
    account_id = str(uuid.uuid4())
    account_data["id"] = account_id
    
    try:
        # Создаем клиент Telegram и проверяем авторизацию
        client = await create_telegram_client(session_path, int(api_id), api_hash, proxy)
        
        logger.info("Устанавливаем соединение с Telegram для проверки сессии")
        await client.connect()
        
        # Проверяем, авторизован ли клиент
        is_authorized = await client.is_user_authorized()
        logger.info(f"Сессия {'авторизована' if is_authorized else 'не авторизована'}")
        
        if is_authorized:
            account_data["status"] = "active"
            
            # Получаем информацию о пользователе, чтобы убедиться, что сессия действительно работает
            me = await client.get_me()
            logger.info(f"Успешно получена информация о пользователе: {me.id}")
        
        await client.disconnect()
        
        # Добавляем аккаунт в базу данных
        await admin_add_telegram_account(user_id, account_data)
        
        return {
            "account_id": account_id,
            "is_authorized": is_authorized,
            "status": account_data["status"],
            "message": "Файл сессии загружен и проверен"
        }
    except Exception as e:
        logger.error(f"Ошибка при проверке файла сессии: {str(e)}")
        
        # Удаляем файл сессии, если произошла ошибка
        if os.path.exists(full_session_path):
            os.remove(full_session_path)
            logger.info(f"Удален файл сессии после ошибки: {full_session_path}")
        
        # Проверяем, пуста ли директория пользователя
        if os.path.exists(user_sessions_dir) and not os.listdir(user_sessions_dir):
            os.rmdir(user_sessions_dir)
            logger.info(f"Удалена пустая директория: {user_sessions_dir}")
        
        raise HTTPException(400, f"Ошибка при проверке файла сессии: {str(e)}")

@app.get("/api/vk/accounts/{account_id}/status")
async def check_vk_account_status(request: Request, account_id: str):
    """Проверяет статус аккаунта VK."""
    logger.info(f"Проверка статуса VK аккаунта с ID {account_id}")
    auth_header = request.headers.get('authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        logger.error("API ключ не предоставлен или в неверном формате")
        raise HTTPException(401, "API ключ обязателен")
    admin_key = auth_header.split(' ')[1]
    
    # Проверяем админ-ключ
    if not await verify_admin_key(admin_key):
        logger.error("Неверный админ-ключ")
        raise HTTPException(401, "Неверный админ-ключ")
    
    # Находим аккаунт по ID
    from user_manager import get_db_connection, cipher
    import vk_api
    from vk_api.exceptions import ApiError
    from datetime import datetime
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT * FROM vk_accounts 
        WHERE id = ?
    ''', (account_id,))
    
    account = cursor.fetchone()
    
    if not account:
        logger.error(f"Аккаунт с ID {account_id} не найден")
        conn.close()
        raise HTTPException(404, "Аккаунт не найден")
    
    # Преобразуем объект sqlite3.Row в словарь
    account_dict = dict(account)
    encrypted_token = account_dict["token"]
    current_status = account_dict.get("status", "unknown")
    last_checked_at = datetime.now().isoformat()
    
    try:
        # Расшифровываем токен
        token = cipher.decrypt(encrypted_token.encode()).decode()
        logger.info(f"Токен VK успешно расшифрован")
        
        # Создаем сессию VK API
        session = vk_api.VkApi(token=token)
        vk = session.get_api()
        
        # Проверяем валидность токена, запрашивая информацию о пользователе
        user_info = vk.users.get(fields='photo_50,screen_name')[0]
        
        # Если запрос выполнен успешно, токен действителен
        status = "active"
        logger.info(f"Токен VK действителен, пользователь: {user_info}")
        
        # Обновляем статус и информацию о пользователе в базе данных
        cursor.execute('''
            UPDATE vk_accounts
            SET status = ?, 
                user_id = ?, 
                user_name = ?,
                error_message = NULL,
                error_code = NULL,
                last_checked_at = ?
            WHERE id = ?
        ''', (
            status, 
            user_info.get('id'), 
            f"{user_info.get('first_name', '')} {user_info.get('last_name', '')}".strip(),
            last_checked_at,
            account_id
        ))
        conn.commit()
        logger.info(f"Статус аккаунта обновлен на '{status}'")
        
        conn.close()
        return {
            "account_id": account_id,
            "status": status,
            "user_info": user_info
        }
    except ApiError as e:
        logger.error(f"Ошибка VK API при проверке токена: {str(e)}")
        
        # Определяем тип ошибки
        error_message = str(e)
        error_code = getattr(e, 'code', 0)
        status = "invalid"
        
        if error_code == 5:  # Токен недействителен
            status = "invalid"
            error_message = "Недействительный токен или истек срок действия"
        elif error_code == 27:  # Ключ доступа сообщества недействителен
            status = "invalid"
            error_message = "Ключ доступа сообщества недействителен"
        elif error_code == 38:  # Пользователь заблокирован
            status = "banned"
            error_message = "Пользователь заблокирован"
        elif error_code == 29:  # Превышен лимит запросов
            status = "rate_limited"
            error_message = "Превышен лимит запросов к API"
        elif error_code == 17:  # Требуется валидация аккаунта
            status = "validation_required"
            error_message = "Требуется валидация аккаунта"
        
        # Обновляем статус в базе данных с информацией об ошибке
        cursor.execute('''
            UPDATE vk_accounts
            SET status = ?,
                error_message = ?,
                error_code = ?,
                last_checked_at = ?
            WHERE id = ?
        ''', (status, error_message, error_code, last_checked_at, account_id))
        conn.commit()
        logger.info(f"Статус аккаунта обновлен на '{status}' из-за ошибки: {error_message}")
        
        conn.close()
        return {
            "account_id": account_id,
            "status": status,
            "error": error_message,
            "error_code": error_code
        }
    except Exception as e:
        logger.error(f"Ошибка при проверке статуса VK токена: {str(e)}")
        
        # Обновляем статус в базе данных с информацией об ошибке
        error_message = str(e)
        cursor.execute('''
            UPDATE vk_accounts
            SET status = 'error',
                error_message = ?,
                last_checked_at = ?
            WHERE id = ?
        ''', (error_message, last_checked_at, account_id))
        conn.commit()
        
        conn.close()
        return {
            "account_id": account_id,
            "status": "error",
            "error": error_message
        }

@app.on_event("shutdown")
async def shutdown_event():
    """Обработчик закрытия приложения."""
    logger.info("Приложение завершает работу, отключаем все клиенты Telegram")
    await telegram_pool.disconnect_all()
    logger.info("Все клиенты Telegram отключены")

# Добавим эндпоинт для получения расширенной статистики аккаунтов
@app.get("/api/admin/accounts/stats")
async def get_accounts_stats(request: Request):
    """Получение статистики аккаунтов для отображения в интерфейсе."""
    # Получаем API ключ из заголовка X-API-KEY
    api_key = request.headers.get('x-api-key')
    
    # Если ключа нет, проверяем авторизацию
    if not api_key:
        auth_header = request.headers.get('authorization')
        if not auth_header or not auth_header.startswith('Bearer '):
            raise HTTPException(status_code=401, detail="API ключ обязателен")
        api_key = auth_header.split(' ')[1]
    
    # Проверяем админ-ключ
    if not await verify_admin_key(api_key):
        raise HTTPException(status_code=401, detail="Неверный админ-ключ")
    
    from user_manager import get_db_connection
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Получаем данные по VK аккаунтам
    cursor.execute('''
        SELECT 
            id, 
            user_api_key, 
            token,
            status, 
            proxy,
            requests_count,
            last_request_time,
            added_at,
            user_name
        FROM vk_accounts
    ''')
    vk_accounts_data = cursor.fetchall()
    
    # Получаем данные по Telegram аккаунтам
    cursor.execute('''
        SELECT 
            id, 
            user_api_key, 
            phone, 
            status, 
            api_id,
            api_hash,
            proxy,
            requests_count,
            last_request_time,
            added_at
        FROM telegram_accounts
    ''')
    telegram_accounts_data = cursor.fetchall()
    
    conn.close()
    
    # Форматируем данные для VK
    vk_accounts = []
    for account in vk_accounts_data:
        vk_accounts.append({
            "id": account[0],
            "user_api_key": account[1],
            "login": account[8] or "Неизвестно",  # Используем user_name как логин
            "status": account[3],
            "proxy": account[4],
            "requests_made": account[5] or 0,
            "request_limit": 1000,  # Устанавливаем стандартный лимит
            "last_used": account[6],
            "active": account[3] == 'active'  # Используем статус для определения активности
        })
    
    # Форматируем данные для Telegram
    telegram_accounts = []
    for account in telegram_accounts_data:
        telegram_accounts.append({
            "id": account[0],
            "user_api_key": account[1],
            "phone": account[2],
            "status": account[3],
            "api_id": account[4],
            "api_hash": account[5],
            "proxy": account[6],
            "requests_made": account[7] or 0,
            "request_limit": 1000,  # Устанавливаем стандартный лимит
            "last_used": account[8],
            "active": account[3] == 'active'  # Используем статус для определения активности
        })
    
    return {
        "vk": vk_accounts,
        "telegram": telegram_accounts
    }

# Эндпоинт для изменения статуса аккаунта (активен/неактивен)
@app.post("/api/admin/accounts/toggle_status")
async def toggle_account_status(request: Request, data: dict):
    """Изменение статуса аккаунта (активен/неактивен)."""
    # Получаем API ключ из заголовка X-API-KEY
    api_key = request.headers.get('x-api-key')
    
    # Если ключа нет, проверяем авторизацию
    if not api_key:
        auth_header = request.headers.get('authorization')
        if not auth_header or not auth_header.startswith('Bearer '):
            raise HTTPException(status_code=401, detail="API ключ обязателен")
        api_key = auth_header.split(' ')[1]
    
    # Проверяем админ-ключ
    if not await verify_admin_key(api_key):
        raise HTTPException(status_code=401, detail="Неверный админ-ключ")
    
    # Проверяем обязательные параметры
    account_id = data.get('account_id')
    platform = data.get('platform')
    active = data.get('active')
    
    if not account_id or not platform or active is None:
        raise HTTPException(status_code=400, detail="Необходимо указать account_id, platform и active")
    
    from user_manager import get_db_connection
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        if platform.lower() == 'vk':
            cursor.execute(
                "UPDATE vk_accounts SET is_active = ? WHERE id = ?", 
                (1 if active else 0, account_id)
            )
            # Также обновляем статус, чтобы он соответствовал активности
            cursor.execute(
                "UPDATE vk_accounts SET status = ? WHERE id = ?", 
                ('active' if active else 'inactive', account_id)
            )
        elif platform.lower() == 'telegram':
            cursor.execute(
                "UPDATE telegram_accounts SET is_active = ? WHERE id = ?", 
                (1 if active else 0, account_id)
            )
            # Также обновляем статус, чтобы он соответствовал активности
            cursor.execute(
                "UPDATE telegram_accounts SET status = ? WHERE id = ?", 
                ('active' if active else 'inactive', account_id)
            )
        else:
            conn.close()
            raise HTTPException(status_code=400, detail="Неизвестная платформа")
        
        conn.commit()
        conn.close()
        
        # Обновляем статус в пуле клиентов
        if platform.lower() == 'vk':
            # Найдем клиент в пуле и обновим его
            for user_key, clients in vk_pool.clients.items():
                for i, client in enumerate(clients):
                    if str(client.id) == str(account_id):
                        client.is_active = active
                        break
        elif platform.lower() == 'telegram':
            # Для Telegram также обновим статус в пуле
            for user_key, clients in telegram_pool.clients.items():
                for i, client in enumerate(clients):
                    if str(client.id) == str(account_id):
                        client.is_active = active
                        # Если деактивировали, возможно нужно отключить клиент
                        if not active and account_id in telegram_pool.connected_clients:
                            asyncio.create_task(telegram_pool.disconnect_client(account_id))
                        break
        
        return {"success": True, "message": f"Статус аккаунта {account_id} изменен на {'активен' if active else 'неактивен'}"}
    
    except Exception as e:
        conn.close()
        logger.error(f"Ошибка при изменении статуса аккаунта: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Ошибка при изменении статуса: {str(e)}")

# Добавим эндпоинт для получения расширенной статистики аккаунтов
@app.get("/api/admin/accounts/stats/detailed")
async def get_accounts_statistics_detailed(request: Request):
    """Получает расширенную статистику всех аккаунтов для админ-панели."""
    auth_header = request.headers.get('authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        raise HTTPException(401, "API ключ обязателен")
    admin_key = auth_header.split(' ')[1]
    
    # Проверяем админ-ключ
    if not await verify_admin_key(admin_key):
        raise HTTPException(401, "Неверный админ-ключ")
    
    from user_manager import get_db_connection
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Статистика по Telegram аккаунтам
    cursor.execute('''
        SELECT 
            status, 
            COUNT(*) as count, 
            AVG(requests_count) as avg_requests,
            MAX(requests_count) as max_requests,
            MIN(requests_count) as min_requests
        FROM telegram_accounts 
        GROUP BY status
    ''')
    telegram_stats = [dict(row) for row in cursor.fetchall()]
    
    # Статистика по VK аккаунтам
    cursor.execute('''
        SELECT 
            status, 
            COUNT(*) as count,
            AVG(requests_count) as avg_requests,
            MAX(requests_count) as max_requests,
            MIN(requests_count) as min_requests
        FROM vk_accounts 
        GROUP BY status
    ''')
    vk_stats = [dict(row) for row in cursor.fetchall()]
    
    # Статистика по использованию аккаунтов в разрезе пользователей
    cursor.execute('''
        SELECT 
            u.username,
            u.api_key,
            COUNT(t.id) as telegram_count,
            COUNT(v.id) as vk_count,
            SUM(t.requests_count) as telegram_requests,
            SUM(v.requests_count) as vk_requests
        FROM users u
        LEFT JOIN telegram_accounts t ON u.api_key = t.user_api_key
        LEFT JOIN vk_accounts v ON u.api_key = v.user_api_key
        GROUP BY u.api_key
    ''')
    user_stats = [dict(row) for row in cursor.fetchall()]
    
    # Статистика по использованию клиентов из пула
    vk_usage = {
        account_id: {
            "usage_count": count,
            "last_used": vk_pool.last_used.get(account_id, 0)
        } for account_id, count in vk_pool.usage_counts.items()
    }
    
    telegram_usage = {
        account_id: {
            "usage_count": count,
            "last_used": telegram_pool.last_used.get(account_id, 0),
            "connected": account_id in telegram_pool.connected_clients
        } for account_id, count in telegram_pool.usage_counts.items()
    }
    
    conn.close()
    
    return {
        "telegram": {
            "stats_by_status": telegram_stats,
            "usage": telegram_usage,
            "connected_count": len(telegram_pool.connected_clients)
        },
        "vk": {
            "stats_by_status": vk_stats,
            "usage": vk_usage
        },
        "users": user_stats,
        "timestamp": time.time()
    }

# Новый эндпоинт для расширенного получения трендовых постов
@app.post("/api/trending-posts-extended")
async def api_trending_posts_extended(request: Request, data: dict):
    """
    Получение трендовых постов с расширенными параметрами фильтрации 
    и поддержкой медиа-альбомов.
    """
    platform = data.get('platform', 'telegram')
    channel_ids = data.get('channel_ids', [])
    if not channel_ids:
        raise HTTPException(status_code=400, detail="ID каналов обязательны")
    
    # Параметры фильтрации
    days_back = data.get('days_back', 7)
    posts_per_channel = data.get('posts_per_channel', 10)
    min_views = data.get('min_views')
    min_reactions = data.get('min_reactions')
    min_comments = data.get('min_comments')
    min_forwards = data.get('min_forwards')
    
    # Получение API ключа из заголовка
    api_key = request.headers.get("Authorization", "").replace("Bearer ", "")
    if not api_key:
        raise HTTPException(status_code=401, detail="API ключ не указан")
    
    if platform == 'telegram':
        # Получаем следующий доступный аккаунт
        from user_manager import get_next_available_account
        
        account = get_next_available_account(api_key, "telegram")
        if not account:
            raise HTTPException(status_code=400, detail="Нет доступных аккаунтов Telegram")
        
        # Получаем клиент
        client = await auth_middleware(request, 'telegram')
        
        # Получаем трендовые посты с расширенными параметрами
        posts = await get_trending_posts(
            client, 
            channel_ids, 
            days_back=days_back, 
            posts_per_channel=posts_per_channel,
            min_views=min_views,
            min_reactions=min_reactions,
            min_comments=min_comments,
            min_forwards=min_forwards
        )
        
        # Обновляем статистику использования аккаунта
        await update_account_usage(api_key, account["id"], "telegram")
        
        return posts
    elif platform == 'vk':
        # Здесь можно добавить аналогичную логику для VK
        raise HTTPException(status_code=501, detail="Расширенные параметры пока не поддерживаются для VK")
    else:
        raise HTTPException(status_code=400, detail="Платформа не поддерживается")

# Новый эндпоинт для загрузки медиафайлов
@app.post("/api/media/upload")
async def api_media_upload(request: Request):
    """
    Загрузка медиафайлов в хранилище S3.
    
    Поддерживает загрузку изображений и видео,
    создаёт превью для больших файлов и оптимизирует изображения.
    """
    # Получение API ключа из заголовка
    api_key = request.headers.get("Authorization", "").replace("Bearer ", "")
    if not api_key:
        raise HTTPException(status_code=401, detail="API ключ не указан")
    
    # Проверяем наличие файла
    form = await request.form()
    if "file" not in form:
        raise HTTPException(status_code=400, detail="Файл не найден в запросе")
    
    file = form["file"]
    
    # Генерируем уникальное имя файла
    import uuid
    import os
    file_id = str(uuid.uuid4())
    file_ext = os.path.splitext(file.filename)[1]
    s3_filename = f"media/{file_id}{file_ext}"
    local_path = f"temp_{file_id}{file_ext}"
    
    # Сохраняем файл на диск
    try:
        with open(local_path, "wb") as buffer:
            content = await file.read()
            buffer.write(content)
        
        # Импортируем и используем функции из media_utils
        from media_utils import upload_to_s3, S3_LINK_TEMPLATE
        
        # Определяем, нужно ли оптимизировать файл
        optimize = file_ext.lower() in ['.jpg', '.jpeg', '.png']
        
        # Загружаем файл в S3
        success, info = await upload_to_s3(local_path, s3_filename, optimize=optimize)
        
        # Удаляем локальный файл
        if os.path.exists(local_path):
            os.remove(local_path)
        
        # Возвращаем результат
        if success:
            # Если был создан превью для большого файла
            if info and info.get('is_preview', False):
                return {
                    "success": True,
                    "message": "Превью создано для большого файла",
                    "thumbnail_url": S3_LINK_TEMPLATE.format(filename=info.get('thumbnail')),
                    "preview_url": S3_LINK_TEMPLATE.format(filename=info.get('preview')),
                    "original_size": info.get('size')
                }
            # Обычная загрузка
            return {
                "success": True,
                "message": "Файл успешно загружен",
                "url": S3_LINK_TEMPLATE.format(filename=s3_filename)
            }
        else:
            # Если загрузка не удалась из-за превышения размера
            if info and info.get('reason') == 'size_limit_exceeded':
                return {
                    "success": False,
                    "message": f"Файл превышает максимально допустимый размер ({info.get('size')} байт)",
                    "size": info.get('size'),
                    "error": "size_limit_exceeded"
                }
            # Другие ошибки
            return {
                "success": False,
                "message": "Не удалось загрузить файл",
                "error": "upload_failed"
            }
    except Exception as e:
        # Удаляем локальный файл в случае ошибки
        if os.path.exists(local_path):
            os.remove(local_path)
        
        logger.error(f"Ошибка при загрузке файла: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка при загрузке файла: {str(e)}")

if __name__ == "__main__":
    port = int(os.getenv("PORT", 3030))
    uvicorn.run(app, host="0.0.0.0", port=port)