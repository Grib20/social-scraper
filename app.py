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
from typing import List
import sys
from datetime import datetime
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError
from telethon.sessions import StringSession

load_dotenv()  # Загружаем .env до импорта модулей

# Определяем api_key_header
api_key_header = APIKeyHeader(name="X-Admin-Key")

# Проверяем наличие обязательных переменных окружения
if not os.getenv("BASE_URL"):
    print("Ошибка: Переменная окружения BASE_URL не установлена")
    sys.exit(1)

# Импорты модулей после load_dotenv
from telegram_utils import start_client, find_channels, get_trending_posts as get_telegram_trending, get_posts_in_channels, get_posts_by_keywords, get_posts_by_period
from vk_utils import VKClient, find_vk_groups, get_vk_posts, get_vk_posts_in_groups
from user_manager import register_user, set_vk_token, get_vk_token
from media_utils import init_scheduler, close_scheduler
from admin_panel import (
    verify_admin_key, get_all_users, get_user as admin_get_user, delete_user as admin_delete_user,
    update_user_vk_token, get_system_stats,
    add_telegram_account as admin_add_telegram_account, update_telegram_account as admin_update_telegram_account, 
    delete_telegram_account as admin_delete_telegram_account,
    add_vk_account as admin_add_vk_account, update_vk_account as admin_update_vk_account, 
    delete_vk_account as admin_delete_vk_account,
    get_next_available_account, update_account_usage, verify_api_key, get_account_status,
    load_users, save_users, get_telegram_account as admin_get_telegram_account, get_vk_account as admin_get_vk_account
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Telegram клиенты для каждого аккаунта
telegram_clients = {}
vk_clients = {}

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

async def auth_middleware(request: Request, platform: str):
    auth_header = request.headers.get('authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        raise HTTPException(401, "Неверный или отсутствующий API ключ")
    api_key = auth_header.split(' ')[1]
    
    if platform == 'vk':
        users = load_users()
        if api_key not in users or not users[api_key].get("vk_accounts"):
            raise HTTPException(401, "Для этого API ключа не установлены аккаунты VK")
        
        account = get_next_available_account(users[api_key]["vk_accounts"], "vk")
        if not account:
            raise HTTPException(429, "Все аккаунты VK достигли лимита запросов")
        
        if account["id"] not in vk_clients:
            vk_clients[account["id"]] = VKClient(account["token"])
        
        update_account_usage(api_key, account["id"], "vk")
        return vk_clients[account["id"]]
    
    elif platform == 'telegram':
        users = load_users()
        if api_key not in users or not users[api_key].get("telegram_accounts"):
            raise HTTPException(401, "Для этого API ключа не установлены аккаунты Telegram")
        
        account = get_next_available_account(users[api_key]["telegram_accounts"], "telegram")
        if not account:
            raise HTTPException(429, "Все аккаунты Telegram достигли лимита запросов")
        
        if account["id"] not in telegram_clients:
            client = TelegramClient(
                StringSession(account.get("session_string", "")),
                account["api_id"],
                account["api_hash"]
            )
            if account.get("proxy"):
                client.set_proxy(account["proxy"])
            await start_client(client)
            telegram_clients[account["id"]] = client
        
        update_account_usage(api_key, account["id"], "telegram")
        return telegram_clients[account["id"]]
    
    return api_key

# Админ-эндпоинты для управления аккаунтами
@app.post("/admin/users/{user_id}/telegram")
async def add_telegram_account(user_id: str, request: Request):
    admin_key = request.headers.get("X-Admin-Key")
    if not admin_key or not await verify_admin_key(admin_key):
        raise HTTPException(status_code=401, detail="Invalid admin key")
    
    data = await request.json()
    users = load_users()
    if user_id not in users:
        raise HTTPException(status_code=404, detail="User not found")
    
    if "telegram_accounts" not in users[user_id]:
        users[user_id]["telegram_accounts"] = []
    
    users[user_id]["telegram_accounts"].append(data)
    save_users(users)
    return {"status": "success"}

@app.delete("/admin/users/{user_id}/telegram/{phone}")
async def delete_telegram_account(user_id: str, phone: str, request: Request):
    admin_key = request.headers.get("X-Admin-Key")
    if not admin_key or not await verify_admin_key(admin_key):
        raise HTTPException(status_code=401, detail="Invalid admin key")
    
    users = load_users()
    if user_id not in users:
        raise HTTPException(status_code=404, detail="User not found")
    
    if "telegram_accounts" not in users[user_id]:
        raise HTTPException(status_code=404, detail="No telegram accounts found")
    
    users[user_id]["telegram_accounts"] = [
        acc for acc in users[user_id]["telegram_accounts"]
        if acc["phone"] != phone
    ]
    save_users(users)
    return {"status": "success"}

@app.post("/admin/users/{user_id}/vk")
async def add_vk_account(user_id: str, request: Request):
    admin_key = request.headers.get("X-Admin-Key")
    if not admin_key or not await verify_admin_key(admin_key):
        raise HTTPException(status_code=401, detail="Invalid admin key")
    
    data = await request.json()
    users = load_users()
    if user_id not in users:
        raise HTTPException(status_code=404, detail="User not found")
    
    if "vk_accounts" not in users[user_id]:
        users[user_id]["vk_accounts"] = []
    
    users[user_id]["vk_accounts"].append(data)
    save_users(users)
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
    
    # Загружаем пользователей
    users = load_users()
    if user_id not in users:
        raise HTTPException(404, "Пользователь не найден")
    
    # Ищем аккаунт по ID
    account_found = False
    
    if "vk_accounts" not in users[user_id]:
        raise HTTPException(404, "У пользователя нет аккаунтов VK")
    
    # Удаляем аккаунт из списка
    updated_accounts = []
    for account in users[user_id]["vk_accounts"]:
        if account.get("id") != account_id:
            updated_accounts.append(account)
        else:
            account_found = True
    
    if not account_found:
        raise HTTPException(404, "Аккаунт с указанным ID не найден")
    
    users[user_id]["vk_accounts"] = updated_accounts
    
    # Сохраняем изменения
    save_users(users)
    
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
    
    users_data = load_users()
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

@app.delete("/admin/users/{user_id}", dependencies=[Security(verify_admin_key)])
async def delete_user_by_id(user_id: str, request: Request):
    # Проверяем админ-ключ
    admin_key = request.headers.get("X-Admin-Key")
    if not admin_key:
        admin_key = request.cookies.get("admin_key")
    
    if not admin_key or not await verify_admin_key(admin_key):
        raise HTTPException(status_code=401, detail="Invalid admin key")
    
    # Получаем данные пользователей
    users = load_users()
    
    # Проверяем, существует ли пользователь
    if user_id not in users:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Удаляем пользователя
    del users[user_id]
    
    # Сохраняем обновленные данные
    save_users(users)
    
    return {"status": "success", "message": "User deleted successfully"}

@app.put("/admin/users/{api_key}/vk-token", dependencies=[Security(verify_admin_key)])
async def admin_update_vk_token(api_key: str, vk_token: str):
    """Обновляет VK токен пользователя."""
    return await update_user_vk_token(api_key, vk_token)

# Существующие эндпоинты
@app.post("/register")
async def register_user(request: Request):
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
    
    users = load_users()
    
    # Проверяем, не занято ли имя пользователя
    for user_id, user_data in users.items():
        if user_data.get("username") == username:
            raise HTTPException(status_code=400, detail="Username already exists")
    
    # Создаем нового пользователя
    user_id = str(uuid.uuid4())
    api_key = str(uuid.uuid4())  # Генерируем API ключ
    
    users[user_id] = {
        "username": username,
        "password": password,  # В реальном приложении пароль нужно хешировать
        "api_key": api_key,
        "telegram_accounts": [],
        "vk_accounts": []
    }
    
    save_users(users)
    
    return {"id": user_id, "username": username, "api_key": api_key}

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
        return await get_telegram_trending(client, group_ids, days_back, posts_per_group, min_views)
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
async def get_posts_by_period(
    request: Request,
    platform: str = Body(...),
    group_ids: List[int] = Body(...),
    max_posts: int = Body(100),
    days_back: int = Body(7),
    min_views: int = Body(0)
):
    """Получение постов из групп за указанный период."""
    api_key = request.headers.get("Authorization", "").replace("Bearer ", "")
    if not api_key:
        raise HTTPException(status_code=401, detail="API ключ не указан")
    
    if platform == "telegram":
        account = await get_next_available_account(api_key, "telegram")
        if not account:
            raise HTTPException(status_code=400, detail="Нет доступных аккаунтов Telegram")
        
        client = await auth_middleware(request, 'telegram')
        posts = await get_posts_by_period(client, group_ids, max_posts, days_back, min_views)
        await update_account_usage(api_key, account["id"], "telegram")
        return posts
    elif platform == "vk":
        account = await get_next_available_account(api_key, "vk")
        if not account:
            raise HTTPException(status_code=400, detail="Нет доступных аккаунтов VK")
        
        vk = await auth_middleware(request, 'vk')
        posts = await get_posts_by_period(vk, group_ids, max_posts, days_back, min_views)
        await update_account_usage(api_key, account["id"], "vk")
        return posts
    else:
        raise HTTPException(status_code=400, detail="Неподдерживаемая платформа")

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
    if not await verify_api_key(api_key):
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
    session_string = ""  # Изначально пустая строка для новой сессии
    account_data["session_file"] = session_name
    account_data["session_string"] = session_string  # Добавляем для хранения строки сессии
    logger.info(f"Назначено имя сессии: {session_name}")
    
    # Создаем Telegram клиент и отправляем код
    logger.info(f"Создаем Telegram клиент с сессией {session_name}")
    try:
        # Убедимся, что api_id передается как число
        api_id_int = int(api_id)
        logger.info(f"Преобразован api_id в число: {api_id_int}")
        
        client = await create_telegram_client(session_string, api_id_int, api_hash, proxy)
        
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
            
            # Сохраняем строку сессии после успешного подключения
            session_string = client.session.save()
            account_data["session_string"] = session_string
            logger.info("Сохранена строка сессии")
            
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
    
    # Загружаем пользователей
    users_data = load_users()
    
    # Ищем аккаунт по ID
    logger.info(f"Поиск аккаунта с ID {account_id}")
    found = False
    
    for user_id, user_info in users_data.items():
        if "telegram_accounts" in user_info:
            for account in user_info["telegram_accounts"]:
                if account.get("id") == account_id:
                    # Нашли аккаунт
                    found = True
                    logger.info(f"Аккаунт найден у пользователя {user_id}, телефон: {account.get('phone')}")
                    try:
                        session_name = account["session_file"]
                        session_string = account.get("session_string", "")
                        logger.info(f"Создание клиента Telegram, сессия: {session_name}")
                        client = await create_telegram_client(session_string, int(account["api_id"]), account["api_hash"], account.get("proxy"))
                        
                        if account.get("proxy"):
                            logger.info(f"Устанавливаем прокси: {account['proxy']}")
                            client.set_proxy(account["proxy"])
                        
                        logger.info("Подключение к Telegram")
                        await client.connect()
                        
                        # Если уже авторизован, просто возвращаем успех
                        logger.info("Проверка статуса авторизации")
                        if await client.is_user_authorized():
                            logger.info("Клиент уже авторизован")
                            await client.disconnect()
                            account["status"] = "active"
                            save_users(users_data)
                            # Сохраняем строку сессии после успешного подключения
                            session_string = client.session.save()
                            account["session_string"] = session_string
                            logger.info("Обновлена строка сессии после авторизации")
                            return {"account_id": account_id, "requires_2fa": False}
                        
                        # Пытаемся авторизоваться с кодом
                        try:
                            logger.info(f"Вход с кодом авторизации: {code}")
                            result = await client.sign_in(
                                account["phone"], 
                                code,
                                phone_code_hash=account.get("phone_code_hash")
                            )
                            
                            # Успешная авторизация
                            logger.info("Успешная авторизация!")
                            await client.disconnect()
                            account["status"] = "active"
                            save_users(users_data)
                            # Сохраняем строку сессии после успешной авторизации
                            session_string = client.session.save()
                            account["session_string"] = session_string
                            logger.info("Обновлена строка сессии после авторизации")
                            return {"account_id": account_id, "requires_2fa": False}
                        except PhoneCodeInvalidError:
                            logger.error("Неверный код")
                            await client.disconnect()
                            raise HTTPException(400, "Неверный код")
                        except SessionPasswordNeededError:
                            # Требуется 2FA
                            logger.info("Требуется 2FA")
                            await client.disconnect()
                            return {"account_id": account_id, "requires_2fa": True}
                    except Exception as e:
                        logger.error(f"Ошибка авторизации: {str(e)}")
                        raise HTTPException(400, f"Ошибка авторизации: {str(e)}")
    
    if not found:
        logger.error(f"Аккаунт с ID {account_id} не найден")
    
    raise HTTPException(404, "Аккаунт не найден")

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
    
    # Загружаем пользователей
    users_data = load_users()
    
    # Ищем аккаунт по ID
    for user_id, user_info in users_data.items():
        if "telegram_accounts" in user_info:
            for account in user_info["telegram_accounts"]:
                if account.get("id") == account_id:
                    # Нашли аккаунт
                    try:
                        session_name = account["session_file"]
                        session_string = account.get("session_string", "")
                        client = await create_telegram_client(session_string, int(account["api_id"]), account["api_hash"], account.get("proxy"))
                        
                        if account.get("proxy"):
                            client.set_proxy(account["proxy"])
                        
                        await client.connect()
                        await client.sign_in(password=password)
                        await client.disconnect()
                        
                        # Обновляем статус аккаунта
                        account["status"] = "active"
                        save_users(users_data)
                        
                        # Сохраняем строку сессии после успешной 2FA авторизации
                        session_string = client.session.save()
                        account["session_string"] = session_string
                        logger.info("Обновлена строка сессии после 2FA авторизации")
                        
                        return {"status": "success"}
                    except Exception as e:
                        raise HTTPException(400, f"Ошибка авторизации: {str(e)}")
    
    raise HTTPException(404, "Аккаунт не найден")

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
    
    # Загружаем пользователей
    users = load_users()
    if user_id not in users:
        raise HTTPException(404, "Пользователь не найден")
    
    # Ищем аккаунт по номеру телефона
    account_id = None
    account_data = None
    
    if "telegram_accounts" not in users[user_id]:
        raise HTTPException(404, "У пользователя нет аккаунтов Telegram")
    
    for account in users[user_id]["telegram_accounts"]:
        if account["phone"] == phone:
            account_id = account.get("id")
            account_data = account
            break
    
    if not account_id or not account_data:
        raise HTTPException(404, "Аккаунт с указанным номером телефона не найден")
    
    # Удаляем файл сессии, если он есть
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
    
    # Удаляем аккаунт из списка
    users[user_id]["telegram_accounts"] = [
        acc for acc in users[user_id]["telegram_accounts"]
        if acc["phone"] != phone
    ]
    
    # Сохраняем изменения
    save_users(users)
    
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
    
    # Получаем данные пользователей
    users = load_users()
    
    # Проверяем, существует ли пользователь
    if user_id not in users:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Проверяем, есть ли у пользователя API ключ
    if "api_key" not in users[user_id]:
        # Генерируем новый API ключ
        api_key = str(uuid.uuid4())
        users[user_id]["api_key"] = api_key
        save_users(users)
    else:
        api_key = users[user_id]["api_key"]
    
    return {"api_key": api_key}

@app.post("/admin/users/{user_id}/regenerate-api-key")
async def regenerate_api_key(user_id: str, request: Request):
    # Проверяем админ-ключ
    admin_key = request.headers.get("X-Admin-Key")
    if not admin_key:
        admin_key = request.cookies.get("admin_key")
    
    if not admin_key or not await verify_admin_key(admin_key):
        raise HTTPException(status_code=401, detail="Invalid admin key")
    
    # Получаем данные пользователей
    users = load_users()
    
    # Проверяем, существует ли пользователь
    if user_id not in users:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Генерируем новый API ключ
    new_api_key = str(uuid.uuid4())
    users[user_id]["api_key"] = new_api_key
    
    # Сохраняем обновленные данные
    save_users(users)
    
    return {"api_key": new_api_key, "status": "success"}

# Функция для создания Telegram клиента
async def create_telegram_client(session_name, api_id, api_hash, proxy=None):
    """Создает и настраивает клиент Telegram с StringSession"""
    # Используем StringSession вместо файловой сессии
    client = TelegramClient(
        StringSession(session_name if session_name else ""),
        api_id,
        api_hash
    )
    
    if proxy:
        client.set_proxy(proxy)
        
    return client

if __name__ == "__main__":
    port = int(os.getenv("PORT", 3030))
    uvicorn.run(app, host="0.0.0.0", port=port)