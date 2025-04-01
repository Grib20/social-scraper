import asyncio
from fastapi import FastAPI, HTTPException, Request, Security, Body, Header
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from fastapi.templating import Jinja2Templates
from contextlib import asynccontextmanager
import uvicorn
import logging
from dotenv import load_dotenv  # Импортируем раньше всех
import os
import uuid
from typing import List
import sys

load_dotenv()  # Загружаем .env до импорта модулей

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
    verify_admin_key, get_all_users, get_user, delete_user,
    update_user_vk_token, get_system_stats,
    add_telegram_account, update_telegram_account, delete_telegram_account,
    add_vk_account, update_vk_account, delete_vk_account,
    get_next_available_account, update_account_usage, verify_api_key, get_account_status
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

# Монтируем статические файлы
app.mount("/static", StaticFiles(directory="static"), name="static")

# Маршрут для главной страницы
@app.get("/")
async def index(request: Request):
    """Отображает главную страницу."""
    return templates.TemplateResponse("index.html", {
        "request": request,
        "base_url": os.getenv("BASE_URL")
    })

# Маршрут для страницы входа
@app.get("/login")
async def login(request: Request):
    """Отображает страницу входа."""
    return templates.TemplateResponse("login.html", {
        "request": request,
        "base_url": os.getenv("BASE_URL")
    })

# Маршрут для админ-панели
@app.get("/admin")
async def admin_panel(request: Request):
    """Отображает админ-панель."""
    return templates.TemplateResponse("admin_panel.html", {
        "request": request,
        "base_url": os.getenv("BASE_URL")
    })

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
                f"telegram_session_{account['id']}",
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
@app.post("/admin/users/{api_key}/telegram-accounts", dependencies=[Security(verify_admin_key)])
async def admin_add_telegram_account(api_key: str, account_data: dict = Body(...)):
    """Добавляет аккаунт Telegram для пользователя."""
    account_data["id"] = str(uuid.uuid4())
    return await add_telegram_account(api_key, account_data)

@app.put("/admin/users/{api_key}/telegram-accounts/{account_id}", dependencies=[Security(verify_admin_key)])
async def admin_update_telegram_account(api_key: str, account_id: str, account_data: dict = Body(...)):
    """Обновляет данные аккаунта Telegram."""
    return await update_telegram_account(api_key, account_id, account_data)

@app.delete("/admin/users/{api_key}/telegram-accounts/{account_id}", dependencies=[Security(verify_admin_key)])
async def admin_delete_telegram_account(api_key: str, account_id: str):
    """Удаляет аккаунт Telegram."""
    return await delete_telegram_account(api_key, account_id)

@app.post("/admin/users/{api_key}/vk-accounts", dependencies=[Security(verify_admin_key)])
async def admin_add_vk_account(api_key: str, account_data: dict = Body(...)):
    """Добавляет аккаунт VK для пользователя."""
    account_data["id"] = str(uuid.uuid4())
    return await add_vk_account(api_key, account_data)

@app.put("/admin/users/{api_key}/vk-accounts/{account_id}", dependencies=[Security(verify_admin_key)])
async def admin_update_vk_account(api_key: str, account_id: str, account_data: dict = Body(...)):
    """Обновляет данные аккаунта VK."""
    return await update_vk_account(api_key, account_id, account_data)

@app.delete("/admin/users/{api_key}/vk-accounts/{account_id}", dependencies=[Security(verify_admin_key)])
async def admin_delete_vk_account(api_key: str, account_id: str):
    """Удаляет аккаунт VK."""
    return await delete_vk_account(api_key, account_id)

# Админ-эндпоинты
@app.get("/admin/stats", dependencies=[Security(verify_admin_key)])
async def admin_stats():
    """Получает статистику системы."""
    return await get_system_stats()

@app.get("/admin/users", dependencies=[Security(verify_admin_key)])
async def admin_users():
    """Получает список всех пользователей."""
    return await get_all_users()

@app.get("/admin/users/{api_key}", dependencies=[Security(verify_admin_key)])
async def admin_user(api_key: str):
    """Получает информацию о конкретном пользователе."""
    return await get_user(api_key)

@app.delete("/admin/users/{api_key}", dependencies=[Security(verify_admin_key)])
async def admin_delete_user(api_key: str):
    """Удаляет пользователя."""
    return await delete_user(api_key)

@app.put("/admin/users/{api_key}/vk-token", dependencies=[Security(verify_admin_key)])
async def admin_update_vk_token(api_key: str, vk_token: str):
    """Обновляет VK токен пользователя."""
    return await update_user_vk_token(api_key, vk_token)

# Существующие эндпоинты
@app.post("/register")
async def register():
    api_key = await register_user()
    return {"api_key": api_key, "message": "Пользователь зарегистрирован"}

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

if __name__ == "__main__":
    port = int(os.getenv("PORT", 3030))
    uvicorn.run(app, host="0.0.0.0", port=port)