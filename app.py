import asyncio
from fastapi import FastAPI, HTTPException, Request, Security, Body, Header
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from contextlib import asynccontextmanager
import uvicorn
import logging
from dotenv import load_dotenv  # Импортируем раньше всех
import os
import uuid
from typing import List
import sys
from datetime import datetime

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
async def admin_panel():
    """Отображает админ-панель."""
    return templates.TemplateResponse("admin_panel.html", {"request": {}, "BASE_URL": os.getenv("BASE_URL")})

@app.post("/admin/validate")
async def validate_admin_key(request: Request):
    """Проверяет валидность админ-ключа."""
    admin_key = request.headers.get("X-Admin-Key")
    if not admin_key or admin_key != os.getenv("ADMIN_KEY"):
        raise HTTPException(401, "Неверный админ-ключ")
    return {"status": "ok"}

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
    """Получает список всех пользователей."""
    admin_key = request.headers.get("X-Admin-Key")
    if not admin_key or admin_key != os.getenv("ADMIN_KEY"):
        raise HTTPException(401, "Неверный админ-ключ")
    
    users = await get_all_users()
    return users

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
async def register(request: Request):
    """Регистрирует нового пользователя."""
    try:
        admin_key = request.headers.get("X-Admin-Key")
        if not admin_key or admin_key != os.getenv("ADMIN_KEY"):
            raise HTTPException(status_code=401, detail="Неверный админ-ключ")
        
        data = await request.json()
        username = data.get("username")
        password = data.get("password")
        
        if not username or not password:
            raise HTTPException(status_code=400, detail="Имя пользователя и пароль обязательны")
        
        api_key = str(uuid.uuid4())
        users = load_users()
        
        if api_key in users:
            raise HTTPException(status_code=400, detail="Ошибка генерации API ключа")
        
        users[api_key] = {
            "username": username,
            "password": password,
            "created_at": datetime.now().isoformat(),
            "last_used": None,
            "telegram_accounts": [],
            "vk_accounts": []
        }
        
        save_users(users)
        return {"api_key": api_key, "message": "Пользователь успешно зарегистрирован"}
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

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
async def add_telegram_account(request: Request):
    """Добавляет новый Telegram аккаунт."""
    auth_header = request.headers.get('authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        raise HTTPException(401, "API ключ обязателен")
    api_key = auth_header.split(' ')[1]
    
    form_data = await request.form()
    api_id = form_data.get('api_id')
    api_hash = form_data.get('api_hash')
    phone = form_data.get('phone')
    proxy = form_data.get('proxy')
    session_file = form_data.get('session_file')
    
    if not api_id or not api_hash or not phone:
        raise HTTPException(400, "Обязательные поля не заполнены")
    
    # Создаем новый аккаунт
    account_data = {
        "api_id": api_id,
        "api_hash": api_hash,
        "phone": phone,
        "proxy": proxy,
        "status": "pending"
    }
    
    if session_file:
        # Сохраняем файл сессии
        session_path = f"sessions/{api_key}_{phone}.session"
        with open(session_path, "wb") as f:
            f.write(await session_file.read())
        account_data["session_file"] = session_path
    
    account_id = str(uuid.uuid4())
    account_data["id"] = account_id
    
    # Добавляем аккаунт в базу данных
    await add_telegram_account(api_key, account_data)
    
    # Проверяем авторизацию
    if not session_file:
        return {
            "account_id": account_id,
            "requires_auth": True
        }
    
    return {
        "account_id": account_id,
        "requires_auth": False
    }

@app.post("/api/telegram/verify-code")
async def verify_telegram_code(request: Request):
    """Проверяет код авторизации Telegram."""
    auth_header = request.headers.get('authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        raise HTTPException(401, "API ключ обязателен")
    api_key = auth_header.split(' ')[1]
    
    data = await request.json()
    account_id = data.get('account_id')
    code = data.get('code')
    
    if not account_id or not code:
        raise HTTPException(400, "Не указаны необходимые параметры")
    
    # Получаем данные аккаунта
    account = await get_telegram_account(api_key, account_id)
    if not account:
        raise HTTPException(404, "Аккаунт не найден")
    
    # Проверяем код
    try:
        client = TelegramClient(
            account["session_file"],
            account["api_id"],
            account["api_hash"]
        )
        if account.get("proxy"):
            client.set_proxy(account["proxy"])
        
        await client.connect()
        if not await client.is_user_authorized():
            await client.send_code_request(account["phone"])
            await client.sign_in(account["phone"], code)
            
            # Проверяем, требуется ли 2FA
            if await client.is_user_authorized():
                await client.disconnect()
                return {
                    "account_id": account_id,
                    "requires_2fa": False
                }
            else:
                await client.disconnect()
                return {
                    "account_id": account_id,
                    "requires_2fa": True
                }
    except Exception as e:
        raise HTTPException(400, f"Ошибка авторизации: {str(e)}")

@app.post("/api/telegram/verify-2fa")
async def verify_telegram_2fa(request: Request):
    """Проверяет пароль 2FA для Telegram."""
    auth_header = request.headers.get('authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        raise HTTPException(401, "API ключ обязателен")
    api_key = auth_header.split(' ')[1]
    
    data = await request.json()
    account_id = data.get('account_id')
    password = data.get('password')
    
    if not account_id or not password:
        raise HTTPException(400, "Не указаны необходимые параметры")
    
    # Получаем данные аккаунта
    account = await get_telegram_account(api_key, account_id)
    if not account:
        raise HTTPException(404, "Аккаунт не найден")
    
    # Проверяем пароль 2FA
    try:
        client = TelegramClient(
            account["session_file"],
            account["api_id"],
            account["api_hash"]
        )
        if account.get("proxy"):
            client.set_proxy(account["proxy"])
        
        await client.connect()
        await client.sign_in(password=password)
        await client.disconnect()
        
        # Обновляем статус аккаунта
        await update_telegram_account(api_key, account_id, {"status": "active"})
        
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(400, f"Ошибка авторизации: {str(e)}")

@app.delete("/api/telegram/accounts/{account_id}")
async def delete_telegram_account_endpoint(request: Request, account_id: str):
    """Удаляет Telegram аккаунт."""
    auth_header = request.headers.get('authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        raise HTTPException(401, "API ключ обязателен")
    api_key = auth_header.split(' ')[1]
    
    # Получаем данные аккаунта
    account = await get_telegram_account(api_key, account_id)
    if not account:
        raise HTTPException(404, "Аккаунт не найден")
    
    # Удаляем файл сессии, если он есть
    if account.get("session_file") and os.path.exists(account["session_file"]):
        os.remove(account["session_file"])
    
    # Удаляем аккаунт из базы данных
    await delete_telegram_account(api_key, account_id)
    
    return {"status": "success"}

# Эндпоинты для работы с VK аккаунтами
@app.post("/api/vk/accounts")
async def add_vk_account_endpoint(request: Request):
    """Добавляет новый VK аккаунт."""
    auth_header = request.headers.get('authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        raise HTTPException(401, "API ключ обязателен")
    api_key = auth_header.split(' ')[1]
    
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
    await add_vk_account(api_key, account_data)
    
    return {
        "account_id": account_id,
        "status": "success"
    }

@app.delete("/api/vk/accounts/{account_id}")
async def delete_vk_account_endpoint(request: Request, account_id: str):
    """Удаляет VK аккаунт."""
    auth_header = request.headers.get('authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        raise HTTPException(401, "API ключ обязателен")
    api_key = auth_header.split(' ')[1]
    
    # Удаляем аккаунт из базы данных
    await delete_vk_account(api_key, account_id)
    
    return {"status": "success"}

# Эндпоинт для получения статуса аккаунтов
@app.get("/api/accounts/status")
async def get_accounts_status(request: Request):
    """Получает статус всех аккаунтов пользователя."""
    auth_header = request.headers.get('authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        raise HTTPException(401, "API ключ обязателен")
    api_key = auth_header.split(' ')[1]
    
    # Получаем данные пользователя
    user_data = await get_user(api_key)
    if not user_data:
        raise HTTPException(404, "Пользователь не найден")
    
    return {
        "telegram_accounts": user_data.get("telegram_accounts", []),
        "vk_accounts": user_data.get("vk_accounts", [])
    }

if __name__ == "__main__":
    port = int(os.getenv("PORT", 3030))
    uvicorn.run(app, host="0.0.0.0", port=port)