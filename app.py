import asyncio
from fastapi import FastAPI, HTTPException, Request, Security, Body, Header, responses, Depends, File, UploadFile, Form
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
from typing import List, Union, Dict, Optional, Any
import sys
from datetime import datetime, timedelta, timezone
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError, PasswordHashInvalidError, PhoneCodeExpiredError, FloodWaitError
import time
import redis
import sqlite3
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from fastapi.middleware.cors import CORSMiddleware
import re
import csv
import json
import traceback
from typing import List, Dict, Any, Optional, Union, Tuple
from fastapi import FastAPI, Request, HTTPException, status
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from telethon.errors import SessionPasswordNeededError
import telegram_utils # <-- Добавляем этот импорт
import inspect
from redis_utils import update_account_usage_redis
import user_manager
import media_utils
import asyncpg
from pydantic import BaseModel, Field 
from user_manager import get_db_pool

load_dotenv()  # Загружаем .env до импорта модулей

# Функция для чтения Docker secrets
def read_docker_secret(secret_name):
    try:
        with open(f'/run/secrets/{secret_name}', 'r', encoding='utf-8') as f:
            return f.read().strip()
    except (FileNotFoundError, PermissionError):
        # Если не удалось прочитать из Docker secrets, пробуем из переменной окружения
        env_name = secret_name.upper()
        if secret_name == "aws_access_key":
            env_name = "AWS_ACCESS_KEY_ID"
        elif secret_name == "aws_secret_key":
            env_name = "AWS_SECRET_ACCESS_KEY"
        elif secret_name == "encryption_key":
            env_name = "ENCRYPTION_KEY"
        return os.getenv(env_name)

# Настройка логирования
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('scraper.log')
    ]
)
logger = logging.getLogger(__name__)

# Инициализация секретов из Docker secrets или переменных окружения
AWS_ACCESS_KEY_ID = read_docker_secret('aws_access_key') or os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = read_docker_secret('aws_secret_key') or os.getenv('AWS_SECRET_ACCESS_KEY')
ENCRYPTION_KEY = read_docker_secret('encryption_key') or os.getenv('ENCRYPTION_KEY')

if AWS_ACCESS_KEY_ID:
    os.environ['AWS_ACCESS_KEY_ID'] = AWS_ACCESS_KEY_ID
if AWS_SECRET_ACCESS_KEY:
    os.environ['AWS_SECRET_ACCESS_KEY'] = AWS_SECRET_ACCESS_KEY
if ENCRYPTION_KEY:
    os.environ['ENCRYPTION_KEY'] = ENCRYPTION_KEY

# Настройка подключения к Redis, если доступно
try:
    redis_url = os.getenv("REDIS_URL")
    import redis.asyncio as aredis
    redis_client = aredis.from_url(redis_url) if redis_url else None
    if redis_client:
        # Для асинхронного клиента ping() нужно вызывать через await, 
        # но здесь мы в синхронном контексте, поэтому просто инициализируем
        logger.info("Асинхронное подключение к Redis сконфигурировано")
    else:
        logger.warning("Redis не настроен, кэширование будет работать только локально")
except Exception as e:
    redis_client = None
    logger.warning(f"Ошибка подключения к Redis: {e}. Кэширование будет работать только локально")

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
    get_posts_by_keywords, get_album_messages
)
from vk_utils import VKClient, find_vk_groups, get_vk_posts, get_vk_posts_in_groups
from user_manager import (
    get_db_pool, register_user, set_vk_token, get_db_connection, get_vk_token, get_user, 
    get_next_available_account, update_account_usage, update_user_last_used,
    get_users_dict, verify_api_key, get_active_accounts
)
from media_utils import init_scheduler, close_scheduler
from admin_panel import (
    verify_admin_key, get_all_users, get_user as admin_get_user, delete_user_by_id as admin_delete_user,
    update_user_vk_token, get_system_stats,
    add_telegram_account as admin_add_telegram_account, update_telegram_account as admin_update_telegram_account, 
    delete_telegram_account as admin_delete_telegram_account,
    add_vk_account as admin_add_vk_account, update_vk_account as admin_update_vk_account, 
    delete_vk_account as admin_delete_vk_account,
    get_account_status,
    get_telegram_account as admin_get_telegram_account, get_vk_account as admin_get_vk_account
)
from pools import vk_pool, telegram_pool
from account_manager import initialize_stats_manager, stats_manager


# Telegram клиенты для каждого аккаунта
telegram_clients = {}
vk_clients = {}


# Импорты для работы с Redis
from redis_utils import (
    update_account_usage_redis,
    get_account_stats_redis,
    reset_all_account_stats,
    sync_all_accounts_stats
)

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
    
    # Проверяем существование пользователя с помощью асинхронной функции
    if not await verify_api_key(api_key):
        raise HTTPException(401, "Неверный API ключ")
    
    if platform == 'vk':
        logger.info(f"Получение активных VK аккаунтов для пользователя с API ключом {api_key}")
        # Получаем клиент через select_next_client
        client, account_id = await vk_pool.select_next_client(api_key)
        
        if not client:
            logger.error(f"Не удалось создать клиент VK для пользователя с API ключом {api_key}")
            raise HTTPException(429, "Не удалось инициализировать клиент VK. Проверьте валидность токена в личном кабинете.")
        
        if isinstance(client, bool):
            logger.error(f"Получен некорректный клиент VK (тип bool) для пользователя с API ключом {api_key}")
            raise HTTPException(500, "Внутренняя ошибка сервера при инициализации клиента VK")

        logger.info(f"Используется VK аккаунт {account_id}")
        
        # Используем Redis для обновления статистики
        try:
            await update_account_usage_redis(api_key, account_id, "vk") # <-- Добавляем await ЗДЕСЬ
        except Exception as e:
            logger.error(f"Ошибка при вызове update_account_usage_redis в auth_middleware (VK): {e}")
            # Не прерываем работу, если статистика не обновилась
        return client
        
    
    elif platform == 'telegram':
        client, account_id = await telegram_pool.select_next_client(api_key)
        if not client:
            logger.error(f"Не удалось создать клиент Telegram для пользователя с API ключом {api_key}")
            raise HTTPException(429, "Не удалось инициализировать клиент Telegram. Добавьте аккаунт Telegram в личном кабинете.")
        
        logger.info(f"Используется Telegram аккаунт {account_id}")
        # Используем Redis для обновления статистики
        try:
            await update_account_usage_redis(api_key, account_id, "telegram") # <-- Добавляем await ЗДЕСЬ
        except Exception as e:
             logger.error(f"Ошибка при вызове update_account_usage_redis в auth_middleware (Telegram): {e}")
             # Не прерываем работу, если статистика не обновилась
        return client
    
    else:
        logger.error(f"Запрос к неизвестной платформе: {platform}")
        raise HTTPException(400, f"Неизвестная платформа: {platform}")

# Настраиваем приложение FastAPI и его жизненный цикл
# --- Lifespan для инициализации и очистки ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- Инициализация ---
    logger.info("Запуск инициализации приложения...")
    try:
        await media_utils.init_scheduler()
        logger.info("Планировщик медиа инициализирован.")
    except Exception as e:
        logger.error(f"Ошибка инициализации планировщика: {e}", exc_info=True)
        # Решите, нужно ли прерывать запуск приложения здесь

    try:
        await user_manager.initialize_database()
        logger.info("База данных успешно инициализирована.")
    except Exception as e:
        logger.error(f"Критическая ошибка при инициализации базы данных: {e}", exc_info=True)
        # Скорее всего, стоит прервать запуск, если БД не инициализирована
        raise RuntimeError(f"Не удалось инициализировать базу данных: {e}") from e

    # Инициализация Redis (обычно это просто создание клиента, сам коннект по запросу)
    if redis_client:
         logger.info("Асинхронный клиент Redis доступен.")
    else:
         logger.warning("Асинхронный клиент Redis НЕ доступен.")

    logger.info("Приложение готово к работе.")
    
    # --- Работа приложения ---
    yield
    
    # --- Завершение работы ---
    logger.info("Начало завершения работы приложения...")

    # 1. Остановка планировщика
    try:
        await media_utils.close_scheduler()
        logger.info("Планировщик медиа остановлен.")
    except Exception as e:
        logger.error(f"Ошибка при остановке планировщика: {e}", exc_info=True)

    # 2. Закрытие соединения с Redis
    if redis_client:
        try:
            logger.info("Закрытие асинхронного соединения с Redis...")
            # --- ИСПРАВЛЕНО ЗДЕСЬ ---
            # Просто вызываем await close() для асинхронного клиента
            await redis_client.close()
            # Опционально: ожидание закрытия (для некоторых библиотек/версий)
            # if hasattr(redis_client, 'wait_closed'):
            #     await redis_client.wait_closed()
            logger.info("Асинхронное соединение с Redis успешно закрыто.")
        except Exception as e:
            logger.error(f"Ошибка при закрытии соединения с Redis: {e}", exc_info=True)
    else:
         logger.info("Клиент Redis не был инициализирован, закрытие не требуется.")


    # 3. Закрытие пулов клиентов (если они будут реализованы с методом close)
    # logger.info("Закрытие пулов клиентов (если реализовано)...")
    # try:
    #     if telegram_pool and hasattr(telegram_pool, 'close_all_clients'):
    #         await telegram_pool.close_all_clients()
    #     if vk_pool and hasattr(vk_pool, 'close_all_clients'):
    #         await vk_pool.close_all_clients()
    #     logger.info("Пулы клиентов закрыты (если были методы).")
    # except Exception as e:
    #     logger.error(f"Ошибка при закрытии пулов клиентов: {e}", exc_info=True)


    logger.info("Приложение успешно остановлено.")

app = FastAPI(lifespan=lifespan)

# Настройка CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Разрешаем доступ со всех источников
    allow_credentials=True,
    allow_methods=["*"],  # Разрешаем все методы
    allow_headers=["*"],  # Разрешаем все заголовки
)

# Инициализируем шаблоны
templates = Jinja2Templates(directory="templates")

# Добавляем базовый контекст для всех шаблонов
def get_base_context(request: Request):
    base_url = str(request.base_url)
    if base_url.endswith('/'):
        base_url = base_url[:-1]
    return {
        "request": request,
        "base_url": base_url
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
async def login_page(request: Request):
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
        get_base_context(request)
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

@app.post("/admin/users/{user_id}/telegram")
async def add_telegram_account(user_id: str, request: Request):
    admin_key = request.headers.get("X-Admin-Key")
    if not admin_key or not await verify_admin_key(admin_key):
        raise HTTPException(status_code=401, detail="Invalid admin key")
    
    data = await request.json()
    
    # Добавляем аккаунт через admin_panel
    await admin_add_telegram_account(user_id, data)
    return {"status": "success"}

@app.delete("/admin/users/{user_id}/telegram/{account_id}")
async def delete_telegram_account(user_id: str, account_id: str, request: Request):
    admin_key = request.headers.get("X-Admin-Key")
    if not admin_key or not await verify_admin_key(admin_key):
        raise HTTPException(status_code=401, detail="Invalid admin key")
    
    # Находим ID аккаунта по номеру телефона
    user = await admin_get_user(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Проверяем существование аккаунта
    account_exists = False
    for account in user.get("telegram_accounts", []):
        if account.get("id") == account_id:
            account_exists = True
            break
    
    if not account_exists:
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

@app.put("/api/vk/accounts/{account_id}")
async def update_vk_account_endpoint(request: Request, account_id: str):
    """Обновляет VK аккаунт."""
    logger.info(f"Начало обработки запроса на обновление VK аккаунта {account_id}")
    auth_header = request.headers.get('authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        logger.error("API ключ не предоставлен или в неверном формате")
        raise HTTPException(401, "API ключ обязателен")
    admin_key = auth_header.split(' ')[1]
    
    # Получаем ID пользователя, для которого обновляется аккаунт
    user_id = request.headers.get('X-User-Id')
    if not user_id:
        logger.error("ID пользователя не указан")
        raise HTTPException(400, "ID пользователя не указан")
    
    # Проверяем админ-ключ
    if not await verify_admin_key(admin_key):
        logger.error(f"Неверный админ-ключ: {admin_key}")
        raise HTTPException(401, "Неверный админ-ключ")
    
    logger.info(f"Админ-ключ верифицирован, обрабатываем данные аккаунта для пользователя {user_id}")
    
    # Получаем данные из формы или JSON
    try:
        content_type = request.headers.get("content-type", "").lower()
        if "application/json" in content_type:
            data = await request.json()
            account_data = data
        else:
            form_data = await request.form()
            # Преобразуем FormData в словарь
            account_data = {
                "token": form_data.get('token'),
                "proxy": form_data.get('proxy'),
                "status": form_data.get('status', 'active')
            }
            # Удаляем None значения
            account_data = {k: v for k, v in account_data.items() if v is not None}
        
        # Проверяем формат токена, если он есть
        token = account_data.get('token')
        if token and not token.startswith('vk1.a.'):
            logger.error("Неверный формат токена VK, должен начинаться с vk1.a.")
            raise HTTPException(400, "Неверный формат токена VK, должен начинаться с vk1.a.")
        
        # Обновляем аккаунт через admin_panel
        await admin_update_vk_account(user_id, account_id, account_data)
        logger.info(f"VK аккаунт {account_id} успешно обновлен")
        
        return {
            "account_id": account_id,
            "status": "success",
            "message": "Аккаунт успешно обновлен"
        }
    except HTTPException as e:
        # Переадресуем HTTP исключения
        logger.error(f"HTTP ошибка при обновлении VK аккаунта: {e.detail}")
        raise
    except Exception as e:
        logger.error(f"Ошибка при обновлении VK аккаунта: {str(e)}")
        raise HTTPException(500, f"Ошибка при обновлении VK аккаунта: {str(e)}")

# Админ-эндпоинты
@app.get("/admin/stats")
async def get_admin_stats(request: Request):
    """Получает статистику системы."""
    admin_key = request.headers.get("X-Admin-Key")
    if not admin_key or admin_key != os.getenv("ADMIN_KEY"):
        raise HTTPException(401, "Неверный админ-ключ")
    
    stats = await get_system_stats()
    return stats

# Определим модели Pydantic для четкой структуры ответа
class VKAccountInfo(BaseModel):
    id: str
    user_api_key: str # Добавим для ясности, хотя frontend может не использовать
    token: str | None = None 
    user_name: str | None = None
    proxy: str | None = None
    status: str = 'unknown'
    is_active: bool = False
    added_at: Any | None = None 

class TelegramAccountInfo(BaseModel):
    id: str
    user_api_key: str # Добавим для ясности
    phone: str | None = None
    api_id: str | None = None 
    api_hash: str | None = None 
    proxy: str | None = None
    status: str = 'unknown'
    is_active: bool = False
    added_at: Any | None = None

class UserInfo(BaseModel):
    id: str # Это будет api_key пользователя
    username: str
    api_key: str | None = None # Можно дублировать или убрать, если id достаточно
    vk_accounts: List[VKAccountInfo] = []
    telegram_accounts: List[TelegramAccountInfo] = []

# Эндпоинт для получения списка пользователей с их аккаунтами
# Эндпоинт для получения списка пользователей с их аккаунтами
@app.get("/admin/users", response_model=List[UserInfo])
async def get_users_with_accounts(request: Request):
    """Получает список всех пользователей с их VK и Telegram аккаунтами."""
    admin_key_header = request.headers.get("Authorization")
    if not admin_key_header or not admin_key_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Отсутствует или неверный формат Bearer токена")
    
    token = admin_key_header.split(" ")[1]
    if not await verify_admin_key(token):
        raise HTTPException(status_code=401, detail="Неверный админ-ключ")

    pool = await get_db_pool()
    # Используем api_key как ключ словаря
    users_data: Dict[str, Dict[str, Any]] = {} 

    try:
        async with pool.acquire() as conn:
            # 1. Получаем всех пользователей, используя api_key
            users_query = "SELECT api_key, username FROM users ORDER BY username" # Запрашиваем api_key и username
            user_records = await conn.fetch(users_query)

            if not user_records:
                logger.info("Пользователи не найдены.")
                return []

            # Инициализируем словарь пользователей
            api_keys_list = [] # Список API ключей для запросов аккаунтов
            for record in user_records:
                api_key = record['api_key'] # Получаем api_key
                api_keys_list.append(api_key) 
                users_data[api_key] = {
                    # Заполняем UserInfo.id значением api_key
                    "id": api_key, 
                    "username": record['username'],
                    "api_key": api_key, # Дублируем для поля api_key в UserInfo
                    "vk_accounts": [],
                    "telegram_accounts": []
                }
                
            if not api_keys_list: 
                return []

            # 2. Получаем все VK аккаунты для этих пользователей
            # Фильтруем по user_api_key, используя список api_keys_list
            # ANY($1::text[]) или ANY($1::varchar[]) в зависимости от точного типа в БД
            vk_query = """
                SELECT id, user_api_key, token, user_name, proxy, status, is_active, added_at 
                FROM vk_accounts 
                WHERE user_api_key = ANY($1::text[]) 
                ORDER BY added_at DESC
            """
            # Передаем список API ключей
            vk_records = await conn.fetch(vk_query, api_keys_list) 
            
            for record in vk_records:
                user_api_key = record['user_api_key'] 
                if user_api_key in users_data:
                    masked_token = maskToken(record['token']) if record['token'] else None
                    users_data[user_api_key]["vk_accounts"].append({
                        "id": str(record['id']), # ID аккаунта VK
                        "user_api_key": user_api_key, # Связь с пользователем
                        "token": masked_token,
                        "user_name": record['user_name'],
                        "proxy": record['proxy'],
                        "status": record['status'],
                        "is_active": record['is_active'],
                        "added_at": record['added_at'] 
                    })

            # 3. Получаем все Telegram аккаунты для этих пользователей
            # Фильтруем по user_api_key
            tg_query = """
                SELECT id, user_api_key, phone, proxy, status, is_active, added_at 
                FROM telegram_accounts 
                WHERE user_api_key = ANY($1::text[])
                ORDER BY added_at DESC
            """
             # Передаем список API ключей
            tg_records = await conn.fetch(tg_query, api_keys_list)
            
            for record in tg_records:
                user_api_key = record['user_api_key'] 
                if user_api_key in users_data:
                    users_data[user_api_key]["telegram_accounts"].append({
                        "id": str(record['id']), # ID аккаунта Telegram
                        "user_api_key": user_api_key, # Связь с пользователем
                        "phone": record['phone'],
                        "proxy": record['proxy'],
                        "status": record['status'],
                        "is_active": record['is_active'],
                        "added_at": record['added_at'],
                        "api_id": None, # Не передаем на фронт
                        "api_hash": None # Не передаем на фронт
                    })

        # Преобразуем словарь users_data в список объектов UserInfo
        result_list = [UserInfo(**user_info) for user_info in users_data.values()]
        
        logger.info(f"Успешно получены данные для {len(result_list)} пользователей.")
        return result_list

    except asyncpg.PostgresError as db_err:
        logger.error(f"Ошибка PostgreSQL при получении пользователей и аккаунтов: {db_err}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка базы данных: {str(db_err)}") 
    except Exception as e:
        logger.error(f"Неизвестная ошибка при получении пользователей: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера при получении пользователей")



# Вспомогательная функция маскировки токена (если ее нет)
def maskToken(token: str | None) -> str | None:
    """Маскирует VK токен, оставляя видимыми только часть символов."""
    if not token:
        return None
    if token.startswith("vk1.a.") and len(token) > 15:
         return token[:8] + "..." + token[-4:]
    elif len(token) > 8:
        return token[:4] + "..." + token[-4:]
    else:
        return token

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
    
    pool = await user_manager.get_db_pool()
    if not pool:
        logger.error(f"Не удалось получить пул соединений для удаления пользователя {user_id}")
        raise HTTPException(status_code=500, detail="Ошибка сервера: База данных недоступна")

    user_deleted = 0
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                # Проверяем существование пользователя
                user_exists = await conn.fetchval('SELECT EXISTS(SELECT 1 FROM users WHERE api_key = $1)', user_id)
                
                if not user_exists:
                    logger.error(f"Пользователь с ID {user_id} не найден в базе данных для удаления")
                    raise HTTPException(status_code=404, detail="Пользователь не найден")
                
                logger.info(f"Пользователь {user_id} найден, удаляем связанные аккаунты и пользователя...")
                
                # Удаляем связанные аккаунты (внешние ключи с ON DELETE CASCADE должны сработать, но для явности можно оставить)
                tg_deleted = await conn.execute('DELETE FROM telegram_accounts WHERE user_api_key = $1', user_id)
                vk_deleted = await conn.execute('DELETE FROM vk_accounts WHERE user_api_key = $1', user_id)
                logger.info(f"Удалено telegram аккаунтов: {tg_deleted.split()[-1]}, vk аккаунтов: {vk_deleted.split()[-1]}") # Извлекаем количество из строки 'DELETE N'
        
                # Затем удаляем самого пользователя
                delete_result = await conn.execute('DELETE FROM users WHERE api_key = $1', user_id)
                user_deleted = int(delete_result.split()[-1]) # Извлекаем количество из строки 'DELETE N'
        
        if user_deleted > 0:
            logger.info(f"Пользователь {user_id} успешно удален")
            return {"status": "success", "message": "User deleted successfully"}
        else:
            # Эта ветка маловероятна, если user_exists был true, но на всякий случай
            logger.error(f"Не удалось удалить пользователя {user_id} после проверки существования")
            raise HTTPException(status_code=500, detail="Не удалось удалить пользователя")
        
    except HTTPException as e:
        # Перенаправляем ошибку 404
        if e.status_code == 404:
            raise e
        # Логируем другие HTTP ошибки
        logger.error(f"HTTP ошибка при удалении пользователя {user_id}: {e.detail}", exc_info=True)
        raise HTTPException(status_code=e.status_code, detail=f"Ошибка при удалении пользователя: {e.detail}")
    except Exception as e:
        # Логируем другие ошибки базы данных или логики
        logger.error(f"Ошибка при удалении пользователя {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Внутренняя ошибка сервера при удалении пользователя")

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
    
    # Проверяем, не занято ли имя пользователя напрямую в БД
    pool = await user_manager.get_db_pool()
    if not pool:
        logger.error("Не удалось получить пул соединений к БД для регистрации")
        raise HTTPException(status_code=500, detail="Ошибка сервера: База данных недоступна")
        
    try:
        async with pool.acquire() as conn:
            username_exists = await conn.fetchval("SELECT EXISTS(SELECT 1 FROM users WHERE username = $1)", username)
        
        if username_exists:
            logger.warning(f"Попытка регистрации с уже существующим именем пользователя: {username}")
            raise HTTPException(status_code=400, detail="Username already exists")
            
    except Exception as e:
        logger.error(f"Ошибка при проверке существования имени пользователя {username} в БД: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Ошибка проверки имени пользователя")
    
    # Создаем нового пользователя с помощью функции из user_manager
    try:
        # Предполагаем, что register_user теперь асинхронная и работает с БД
        api_key = await register_user(username, password)
        if not api_key:
            logger.error(f"Функция register_user не вернула api_key для пользователя {username}")
            raise HTTPException(status_code=500, detail="Не удалось создать пользователя")
    
        logger.info(f"Успешно зарегистрирован пользователь {username} с api_key {api_key}")
        return {"id": api_key, "username": username, "api_key": api_key}
        
    except Exception as e:
        logger.error(f"Ошибка при вызове register_user для пользователя {username}: {e}", exc_info=True)
        # Избегаем раскрытия деталей внутренней ошибки пользователю
        raise HTTPException(status_code=500, detail="Ошибка при регистрации пользователя")

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
async def find_groups(request: Request):
    try:
        data = await request.json()
        keywords = data.get("keywords")
        api_key = data.get("api_key", "")
        platform = data.get("platform", "vk")
        min_members = data.get("min_members", 10000)
        max_count = data.get("max_groups", 20)
        
        # Получаем API ключ из заголовка запроса или из тела запроса
        if not api_key:
            api_key = request.headers.get('api-key') or request.headers.get('x-api-key')
            if not api_key:
                auth_header = request.headers.get('authorization')
                if auth_header and auth_header.startswith('Bearer '):
                    api_key = auth_header.split(' ')[1]

        if not keywords:
            return JSONResponse(status_code=400, content={"error": "No keywords provided"})
            
        if not api_key:
            return JSONResponse(status_code=401, content={"error": "API key is required"})
        
        # Проверяем API ключ с помощью verify_api_key вместо get_user
        from user_manager import verify_api_key
        if not await verify_api_key(api_key):
            return JSONResponse(status_code=403, content={"error": "Invalid API key"})
            
        # Создаем новый запрос с API ключом в заголовке
        # Вместо прямого изменения _headers, создаем новый объект scope
        new_scope = dict(request.scope)
        new_headers = [(k.lower().encode(), v.encode()) for k, v in request.headers.items()]
        new_headers.append((b'api-key', api_key.encode()))
        new_scope['headers'] = new_headers
        request_for_auth = Request(new_scope)
        
        # Используем параллельную обработку для обоих платформ
        if platform.lower() == "vk":
            # Получаем клиент VK через auth_middleware
            try:
                from vk_utils import find_groups_by_keywords
                import inspect
                
                # Получаем клиент VK используя auth_middleware
                vk_client = await auth_middleware(request_for_auth, 'vk')
                if not vk_client:
                    return JSONResponse(
                        status_code=400, 
                        content={"error": "No VK account available"}
                    )

                # Проверяем, что vk_client не является bool
                if isinstance(vk_client, bool):
                     logger.error("auth_middleware вернул bool вместо клиента VK")
                     return JSONResponse(
                        status_code=500,
                        content={"error": "Failed to initialize VK client (internal error)"}
                    )

                # --- Добавляем детальное логирование ---
                logger.info(f"Тип полученного vk_client: {type(vk_client)}")
                logger.info(f"Является ли vk_client экземпляром VKClient: {isinstance(vk_client, VKClient)}")
                logger.info(f"Тип функции find_groups_by_keywords: {type(find_groups_by_keywords)}")
                
                # Создаем объект корутины перед await
                coro_to_await = find_groups_by_keywords(vk_client, keywords, min_members, max_count, api_key)
                
                logger.info(f"Тип объекта для await: {type(coro_to_await)}")
                logger.info(f"Является ли объект awaitable: {inspect.isawaitable(coro_to_await)}")
                # --- Конец детального логирования ---
                
                # Ищем группы
                groups = await find_groups_by_keywords(vk_client, keywords, min_members, max_count, api_key)
                return {"groups": groups, "count": len(groups)}
            except Exception as e:
                logger.error(f"Error in find_groups for VK: {e}")
                return JSONResponse(
                    status_code=500,
                    content={"error": f"Failed to find VK groups: {str(e)}"}
                )
        else:  # telegram
            # Получаем клиент Telegram через auth_middleware
            try:
                from telegram_utils import find_channels
                
                # Получаем Telegram клиент используя запрос с правильным заголовком
                client = await auth_middleware(request_for_auth, 'telegram')
                if not client:
                    return JSONResponse(
                        status_code=400,
                        content={"error": "No Telegram account available"}
                    )
                
                # Ищем каналы
                channels = await find_channels(client, keywords, min_members, max_count, api_key)
                return {"groups": channels, "count": len(channels)}
            except Exception as e:
                logger.error(f"Error in find_groups for Telegram: {e}")
                return JSONResponse(
                    status_code=500,
                    content={"error": f"Failed to find Telegram channels: {str(e)}"}
                )
    except Exception as e:
        logger.error(f"Error in find_groups: {e}")
        return JSONResponse(
            status_code=500,
            content={"error": f"Internal server error: {str(e)}"}
        )

@app.post("/trending-posts")
async def trending_posts(request: Request, data: dict):
    # Инициализируем планировщик медиа
    from media_utils import init_scheduler
    await init_scheduler()
    
    platform = data.get('platform', 'telegram')
    group_ids = data.get('group_ids', [])
    if not group_ids:
        raise HTTPException(400, "ID групп обязательны")
    
    days_back = data.get('days_back', 7)
    posts_per_group = data.get('posts_per_group', 10)
    min_views = data.get('min_views', 0)  # Устанавливаем значение по умолчанию 0

    # Получаем API ключ из заголовка для передачи в функцию get_trending_posts
    api_key = request.headers.get('api-key') or request.headers.get('x-api-key')
    if not api_key:
        auth_header = request.headers.get('authorization')
        if auth_header and auth_header.startswith('Bearer '):
            api_key = auth_header.split(' ')[1]

    if platform == 'telegram':
        client = await auth_middleware(request, 'telegram')
        # Устанавливаем non_blocking=True для более быстрого ответа
        return await get_trending_posts(client, group_ids, days_back, posts_per_group, min_views, api_key=api_key, non_blocking=True)
    elif platform == 'vk':
        vk = await auth_middleware(request, 'vk')
        return await get_vk_posts_in_groups(vk, group_ids, count=posts_per_group * len(group_ids), min_views=min_views, days_back=days_back)
    raise HTTPException(400, "Платформа не поддерживается")

@app.post("/posts")
async def get_posts(request: Request, data: dict):
    """Получение постов из групп по ключевым словам."""
    api_key = request.headers.get('api-key') or request.headers.get('x-api-key')
    if not api_key:
        auth_header = request.headers.get('authorization')
        if auth_header and auth_header.startswith('Bearer '):
            api_key = auth_header.split(' ')[1]
        else:
            raise HTTPException(401, "API ключ обязателен")
    
    if not await verify_api_key(api_key):
        raise HTTPException(401, "Неверный API ключ")
        
    platform = data.get('platform', 'vk')
    
    # Поддержка обоих форматов (JS и Python)
    group_keywords = data.get('group_keywords', data.get('groupKeywords', []))
    search_keywords = data.get('search_keywords', data.get('searchKeywords', None))
    count = data.get('count', 10)
    min_views = data.get('min_views', data.get('minViews', 1000))
    days_back = data.get('days_back', data.get('daysBack', 7))
    max_groups = data.get('max_groups', data.get('maxGroups', 10))
    max_posts_per_group = data.get('max_posts_per_group', data.get('maxPostsPerGroup', 300))
    group_ids = data.get('group_ids', data.get('groupIds', None))
    
    logger.info(f"Получение постов с параметрами: platform={platform}, group_keywords={group_keywords}, search_keywords={search_keywords}")
    
    if platform == 'vk':
        vk = await auth_middleware(request, 'vk')
        if not vk:
            raise HTTPException(500, "Не удалось получить VK клиент")
            
        from vk_utils import find_vk_groups, get_vk_posts, get_vk_posts_in_groups
        
        try:
            # Если переданы group_ids, используем их, иначе ищем группы по ключевым словам
            if group_ids:
                # Форматируем group_ids в нужный формат
                formatted_group_ids = []
                for gid in group_ids:
                    gid_str = str(gid)
                    if not gid_str.startswith('-'):
                        gid_str = f"-{gid_str}"
                    formatted_group_ids.append(gid_str)
                
                # Получаем посты напрямую из групп
                posts = await get_vk_posts_in_groups(
                    vk, 
                    formatted_group_ids, 
                    search_keywords, 
                    count, 
                    min_views, 
                    days_back, 
                    max_posts_per_group
                )
            else:
                # Получаем посты через поиск групп
                posts = await get_vk_posts(
                    vk, 
                    group_keywords, 
                    search_keywords, 
                    count, 
                    min_views, 
                    days_back, 
                    max_groups, 
                    max_posts_per_group
                )
            
            # Если запрос был в формате JS-версии, форматируем ответ соответствующим образом
            if 'groupKeywords' in data and group_keywords:
                # Возвращаем результат в формате JS-версии {keyword: posts[]}
                return {group_keywords[0] if isinstance(group_keywords, list) else group_keywords: posts}
            
            return posts
            
        except Exception as e:
            import traceback
            logger.error(f"Ошибка при получении постов: {str(e)}")
            logger.error(traceback.format_exc())
            raise HTTPException(500, f"Ошибка при получении постов: {str(e)}")
            
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
    # Инициализируем планировщик медиа
    from media_utils import init_scheduler
    await init_scheduler()
    
    platform = data.get('platform', 'telegram')
    group_ids = data.get('group_ids', [])
    if not group_ids:
        raise HTTPException(400, "ID групп обязательны")
    
    max_posts = data.get('max_posts', 100)
    days_back = data.get('days_back', 7)
    min_views = data.get('min_views', 0)

    api_key = request.headers.get('api-key') or request.headers.get('x-api-key')
    if not api_key:
        # Пробуем получить из авторизации Bearer
        auth_header = request.headers.get('authorization')
        if auth_header and auth_header.startswith('Bearer '):
            api_key = auth_header.split(' ')[1]
        else:
            raise HTTPException(401, "API ключ обязателен")

    if platform == 'telegram':
        client = await auth_middleware(request, 'telegram')
        from telegram_utils import get_posts_by_period as get_telegram_posts_by_period
        # Устанавливаем non_blocking=True для более быстрого ответа
        return await get_telegram_posts_by_period(client, group_ids, max_posts, days_back, min_views, api_key=api_key, non_blocking=True)
    elif platform == 'vk':
        vk = await auth_middleware(request, 'vk')
        # Устанавливаем API ключ для клиента VK
        vk.api_key = api_key
        # Вызываем метод get_posts_by_period у объекта VKClient
        return await vk.get_posts_by_period(group_ids, max_posts, days_back, min_views)
    raise HTTPException(400, "Платформа не поддерживается")

@app.get("/api/accounts/status")
async def get_accounts_status(api_key: str = Header(...)):
    """Получает статус всех аккаунтов."""
    if not await verify_api_key(api_key):
        raise HTTPException(status_code=401, detail="Неверный API ключ")
    
    # Исправляем вызовы функции get_account_status, добавляя await
    telegram_status = await get_account_status(api_key, "telegram")
    vk_status = await get_account_status(api_key, "vk")
    
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
    """Получение постов из групп VK за указанный период."""
    if not await verify_api_key(api_key):
        raise HTTPException(status_code=401, detail="Неверный API ключ")
    
    account = await get_next_available_account(api_key, "vk")
    if not account:
        raise HTTPException(status_code=429, detail="Достигнут лимит запросов")
    
    async with VKClient(account["access_token"], account.get("proxy"), account["id"]) as vk:
        posts = await vk.get_posts_by_period(group_ids, max_posts, days_back, min_views)
        return {"posts": posts}

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
    if not await verify_admin_key(api_key):
        return templates.TemplateResponse(
            "register.html",
            get_base_context(request)
        )
    
    return templates.TemplateResponse(
        "accounts.html",
        get_base_context(request)
    )

@app.post("/api/telegram/accounts")
async def add_telegram_account_endpoint(request: Request):
    """Добавляет новый аккаунт Telegram для пользователя."""
    logger.info("Начало обработки запроса на добавление Telegram аккаунта")
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
    
    logger.info(f"Админ-ключ верифицирован, обрабатываем данные аккаунта для пользователя {user_id}")
    
    # Получаем данные из формы
    form_data = await request.form()
    api_id_str = form_data.get('api_id')
    api_hash = form_data.get('api_hash')
    phone = form_data.get('phone')
    proxy = form_data.get('proxy')
    session_file = form_data.get('session_file')
    
    # Проверка и преобразование api_id
    api_id_int = None
    if isinstance(api_id_str, str):
        try:
            api_id_int = int(api_id_str)
        except ValueError:
            logger.error(f"Неверное значение для api_id: '{api_id_str}'")
            raise HTTPException(status_code=400, detail="Неверное значение для api_id: должно быть числом")
    elif api_id_str is None:
        logger.error("api_id обязателен")
        raise HTTPException(status_code=400, detail="api_id обязателен")
    else:
        logger.error(f"Неожиданный тип для api_id: {type(api_id_str)}")
        raise HTTPException(status_code=400, detail="Неверный тип данных для api_id")

    # Проверка остальных обязательных полей (убедимся, что они строки или None)
    if not isinstance(api_hash, str) or not api_hash:
        logger.error("api_hash обязателен и должен быть строкой")
        raise HTTPException(status_code=400, detail="api_hash обязателен")
    if not isinstance(phone, str) or not phone:
        logger.error("phone обязателен и должен быть строкой")
        raise HTTPException(status_code=400, detail="phone обязателен")
    if proxy and not isinstance(proxy, str):
         logger.error(f"Неожиданный тип для proxy: {type(proxy)}")
         raise HTTPException(status_code=400, detail="Неверный тип данных для proxy")
        
    # Генерируем ID аккаунта
    account_id = str(uuid.uuid4())
    
    # Создаем директорию для сессий, если её нет
    # Эта директория нужна всегда, независимо от того, загружаем ли мы сессию или создаем новую
    os.makedirs("sessions", exist_ok=True)
    logger.info("Создана директория для сессий")

    # Создаем директорию для текущего пользователя
    # Также нужна всегда для хранения сессий конкретного пользователя
    user_sessions_dir = f"sessions/{user_id}"
    os.makedirs(user_sessions_dir, exist_ok=True)
    logger.info(f"Создана директория для сессий пользователя: {user_sessions_dir}")
        
    # Проверяем прокси, если он указан
    if proxy:
        logger.info(f"Проверка прокси для Telegram: {proxy}")
        from telegram_utils import validate_proxy
        is_valid, proxy_type = validate_proxy(proxy)
        
        if not is_valid:
            logger.error(f"Неверный формат прокси: {proxy}")
            raise HTTPException(400, "Неверный формат прокси")
    
    # Обрабатываем загрузку файла сессии, если он предоставлен
    if isinstance(session_file, UploadFile) and await session_file.read(1):  # Проверяем, что файл не пустой
        await session_file.seek(0)  # Возвращаем указатель в начало файла
        
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
            "id": account_id,
            "api_id": api_id_int, # Используем проверенный int
            "api_hash": api_hash,
            "phone": phone,
            "proxy": proxy,
            "session_file": session_path,
            "status": "pending"  # Изначально устанавливаем статус pending
        }
        
        try:
            # Создаем клиент Telegram и проверяем авторизацию
            from telegram_utils import create_telegram_client
            client = await create_telegram_client(session_path, api_id_int, api_hash, proxy)
            
            logger.info("Устанавливаем соединение с Telegram для проверки сессии")
            await client.connect()
            
            # Проверяем, авторизован ли клиент
            is_authorized = await client.is_user_authorized()
            logger.info(f"Сессия {'авторизована' if is_authorized else 'не авторизована'}")
            
            if is_authorized:
                account_data["status"] = "active"
                
                # Получаем информацию о пользователе, чтобы убедиться, что сессия действительно работает
                me = await client.get_me()
                logger.info(f"Успешно получена информация о пользователе: {getattr(me, 'id', getattr(me, 'user_id', str(me)))}")
            
            # Исправляем ошибку: client.disconnect() может не быть корутиной
            if asyncio.iscoroutinefunction(client.disconnect):
                await client.disconnect()
            else:
                client.disconnect()
            
            # Добавляем аккаунт в базу данных
            await admin_add_telegram_account(user_id, account_data)
            
            return {
                "account_id": account_id,
                "is_authorized": is_authorized,
                "status": account_data["status"],
                "message": "Файл сессии загружен и аккаунт добавлен"
            }
        except Exception as e:
            logger.error(f"Ошибка при проверке файла сессии: {str(e)}")
            
            # Удаляем файл сессии, если произошла ошибка
            if os.path.exists(full_session_path):
                os.remove(full_session_path)
                logger.info(f"Удален файл сессии после ошибки: {full_session_path}")
            
            raise HTTPException(400, f"Ошибка при проверке файла сессии: {str(e)}")
    
    # Если файл сессии не предоставлен, создаем стандартное имя сессии (Telethon добавит .session)
    session_name = f"{user_sessions_dir}/{phone}"
    logger.info(f"Назначено имя сессии: {session_name}")
    
    # Создаем аккаунт
    account_data = {
        "id": account_id,
        "api_id": api_id_int, # Используем проверенный int
        "api_hash": api_hash,
        "phone": phone,
        "proxy": proxy,
        "session_file": session_name,
        "status": "pending"
    }
    
    # Создаем Telegram клиент и отправляем код
    logger.info(f"Создаем Telegram клиент с сессией {session_name}")
    from telegram_utils import create_telegram_client, start_client
    
    try:
        # Явное преобразование api_hash в строку (на случай, если неявно придет другой тип)
        api_hash_str = str(api_hash) if api_hash is not None else ""
        # Используем проверенный api_id_int
        client = await create_telegram_client(session_name, api_id_int, api_hash_str, proxy if isinstance(proxy, str) or proxy is None else None)
        
        # Подключаемся к Telegram
        await start_client(client)
        
        # Проверяем, авторизован ли аккаунт
        is_authorized = await client.is_user_authorized()
        if is_authorized:
            logger.info(f"Клиент уже авторизован")
            account_data["status"] = "active"
            
            # Добавляем аккаунт в базу данных
            await admin_add_telegram_account(user_id, account_data)
            
            # Проверяем, является ли метод disconnect корутиной
            if asyncio.iscoroutinefunction(client.disconnect):
                await client.disconnect()
            else:
                client.disconnect()
                
            return {
                "account_id": account_id,
                "is_authorized": True,
                "status": "active"
            }
        
        # Если не авторизован, отправляем код
        from telethon.errors import SessionPasswordNeededError, FloodWaitError
        
        # Отправляем код на номер телефона
        logger.info(f"Отправляем код на номер {phone}")
        try:
            # Явно преобразуем phone в строку, чтобы избежать проблем с типами
            phone_str = str(phone) if phone is not None else ""
            result = await client.send_code_request(phone_str)
            phone_code_hash = result.phone_code_hash
            
            # Добавляем hash в данные аккаунта
            account_data["phone_code_hash"] = phone_code_hash
        except FloodWaitError as e:
            logger.error(f"FloodWaitError при отправке кода: {e}")
            # Проверяем, является ли метод disconnect корутиной
            if asyncio.iscoroutinefunction(client.disconnect):
                await client.disconnect()
            else:
                client.disconnect()
            raise HTTPException(400, f"Telegram требует подождать {e.seconds} секунд перед повторной попыткой")
        
        # Добавляем аккаунт в базу данных
        await admin_add_telegram_account(user_id, account_data)
        
        # Проверяем, является ли метод disconnect корутиной
        if asyncio.iscoroutinefunction(client.disconnect):
            await client.disconnect()
        else:
            client.disconnect()
            
        return {
            "account_id": account_id,
            "requires_auth": True,
            "status": "pending"
        }
    except Exception as e:
        logger.error(f"Ошибка при создании клиента Telegram: {str(e)}")
        raise HTTPException(400, f"Ошибка при создании клиента Telegram: {str(e)}")

@app.post("/api/telegram/verify-code")
async def verify_telegram_code(request: Request):
    data = await request.json()
    account_id = data.get('account_id')
    code = data.get('code')
    password = data.get('password') # Для 2FA
    logger.info(f"Запрос на верификацию кода для аккаунта {account_id}. Наличие пароля: {'Да' if password else 'Нет'}")

    auth_header = request.headers.get('authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        logger.error("API ключ не предоставлен для верификации кода")
        raise HTTPException(401, "API ключ обязателен")
    admin_key = auth_header.split(' ')[1]

    if not await verify_admin_key(admin_key):
        logger.error("Неверный админ-ключ для верификации кода")
        raise HTTPException(401, "Неверный админ-ключ")

    if not account_id or not code:
        logger.error("account_id или code отсутствуют в запросе на верификацию")
        raise HTTPException(400, "Необходимы account_id и code")

    # Используем пул соединений для PostgreSQL
    pool = await get_db_connection()
    
    # Получаем все необходимые данные, включая phone_code_hash
    query = 'SELECT phone, api_id, api_hash, session_file, proxy, phone_code_hash FROM telegram_accounts WHERE id = $1'
    account_record = await pool.fetchrow(query, account_id)
    
    if not account_record:
        logger.error(f"Аккаунт {account_id} не найден для верификации кода")
        raise HTTPException(404, "Аккаунт не найден")

    phone = account_record['phone']
    api_id_str = account_record['api_id']
    api_hash = account_record['api_hash']
    session_file = account_record['session_file']
    proxy = account_record['proxy']
    phone_code_hash = account_record['phone_code_hash'] # Получаем сохраненный хэш

    if not all([phone, api_id_str, api_hash, session_file]):
        logger.error(f"Неполные данные для верификации кода аккаунта {account_id}")
        raise HTTPException(400, "Неполные данные аккаунта для верификации")

    if not phone_code_hash:
         logger.error(f"Отсутствует phone_code_hash для аккаунта {account_id}. Невозможно верифицировать код.")
         raise HTTPException(400, "Сначала нужно запросить код авторизации (phone_code_hash отсутствует)")
    try:
        api_id = int(api_id_str) if api_id_str is not None else None
        if api_id is None:
            raise ValueError("api_id не может быть None")
    except (ValueError, TypeError):
        logger.error(f"Неверный формат api_id {api_id_str} для верификации кода аккаунта {account_id}")
        raise HTTPException(400, f"Неверный формат api_id: {api_id_str}")

    client = None
    try:
        logger.info(f"Создание клиента для верификации кода, сессия: {session_file}")
        client = await telegram_utils.create_telegram_client(str(session_file), api_id, str(api_hash), proxy)

        logger.info(f"Подключение клиента для верификации кода аккаунта {account_id}")
        await client.connect()

        user = None
        if not await client.is_user_authorized():
            try:
                logger.info(f"Попытка входа с кодом для аккаунта {account_id}")
                # Используем phone_code_hash из базы данных
                user = await client.sign_in(str(phone), code, phone_code_hash=phone_code_hash)
                logger.info(f"Вход с кодом для {account_id} успешен.")
            except SessionPasswordNeededError:
                logger.info(f"Для аккаунта {account_id} требуется пароль 2FA")
                if not password:
                    # Если пароль нужен, но не предоставлен, возвращаем специальный статус
                    logger.warning(f"Пароль 2FA не предоставлен для {account_id}, но он требуется.")
                    # Обновляем статус, чтобы UI знал, что нужен пароль
                    try:
                        async with pool.acquire() as conn:
                            update_query = 'UPDATE telegram_accounts SET status = $1 WHERE id = $2'
                            await conn.execute(update_query, 'pending_2fa', account_id)
                    except Exception as db_err:
                        logger.error(f"Не удалось обновить статус на 'pending_2fa' для {account_id}: {db_err}")

                    return JSONResponse(status_code=401, content={"message": "Требуется пароль 2FA", "account_id": account_id, "status": "pending_2fa"})
                try:
                    logger.info(f"Попытка входа с паролем 2FA для аккаунта {account_id}")
                    user = await client.sign_in(password=password)
                    logger.info(f"Вход с паролем 2FA для {account_id} успешен.")
                except PasswordHashInvalidError:
                    logger.error(f"Неверный пароль 2FA для аккаунта {account_id}")
                    # Не меняем статус, чтобы можно было попробовать снова ввести пароль
                    raise HTTPException(status_code=400, detail="Неверный пароль 2FA")
                except Exception as e_pwd:
                    logger.error(f"Ошибка при входе с паролем 2FA для {account_id}: {str(e_pwd)}", exc_info=True)
                    # Статус 'error' при других ошибках пароля
                    try:
                        async with pool.acquire() as conn:
                            update_query = 'UPDATE telegram_accounts SET status = $1 WHERE id = $2'
                            await conn.execute(update_query, 'error', account_id)
                    except Exception as db_err:
                        logger.error(f"Не удалось обновить статус на 'error' после ошибки 2FA для {account_id}: {db_err}")
                    raise HTTPException(status_code=500, detail=f"Ошибка при входе с паролем 2FA: {str(e_pwd)}")

            except PhoneCodeInvalidError as e_code:
                 logger.error(f"Ошибка кода (PhoneCodeInvalidError) для аккаунта {account_id}: {str(e_code)}")
                 # Статус 'pending_code' - код неверный или истек, нужно запросить новый
                 try:
                     async with pool.acquire() as conn:
                         update_query = 'UPDATE telegram_accounts SET status = $1, phone_code_hash = NULL WHERE id = $2'
                         await conn.execute(update_query, 'pending_code', account_id)
                 except Exception as db_err:
                     logger.error(f"Не удалось обновить статус на 'pending_code' после ошибки кода для {account_id}: {db_err}")
                 raise HTTPException(status_code=400, detail=f"Ошибка кода: {str(e_code)}")
            except PhoneCodeExpiredError as e_code:
                 logger.error(f"Ошибка кода (PhoneCodeExpiredError) для аккаунта {account_id}: {str(e_code)}")
                 # Статус 'pending_code' - код неверный или истек, нужно запросить новый
                 try:
                     async with pool.acquire() as conn:
                         update_query = 'UPDATE telegram_accounts SET status = $1, phone_code_hash = NULL WHERE id = $2'
                         await conn.execute(update_query, 'pending_code', account_id)
                 except Exception as db_err:
                     logger.error(f"Не удалось обновить статус на 'pending_code' после ошибки кода для {account_id}: {db_err}")
                 raise HTTPException(status_code=400, detail=f"Ошибка кода: {str(e_code)}")
            except FloodWaitError as e_flood:
                 logger.error(f"Ошибка FloodWait при верификации кода для {account_id}: ждите {e_flood.seconds} секунд")
                 raise HTTPException(status_code=429, detail=f"Слишком много попыток. Попробуйте через {e_flood.seconds} секунд.")
            except Exception as e_signin:
                 logger.error(f"Непредвиденная ошибка при входе для аккаунта {account_id}: {str(e_signin)}", exc_info=True)
                 # Статус 'error' при других ошибках входа
                 try:
                     async with pool.acquire() as conn:
                         update_query = 'UPDATE telegram_accounts SET status = $1 WHERE id = $2'
                         await conn.execute(update_query, 'error', account_id)
                 except Exception as db_err:
                     logger.error(f"Не удалось обновить статус на 'error' после ошибки входа для {account_id}: {db_err}")
                 raise HTTPException(status_code=500, detail=f"Внутренняя ошибка сервера при входе: {str(e_signin)}")
        else:
             logger.info(f"Аккаунт {account_id} уже авторизован при попытке верификации кода.")
             user = await client.get_me()

        # Если мы дошли сюда, значит авторизация прошла успешно
        logger.info(f"Аккаунт {account_id} успешно авторизован/верифицирован.")
        # Обновляем статус на 'active' и очищаем phone_code_hash
        try:
            async with pool.acquire() as conn:
                update_query = "UPDATE telegram_accounts SET status = $1, phone_code_hash = NULL WHERE id = $2"
                await conn.execute(update_query, 'active', account_id)
        except Exception as db_err:
            logger.error(f"Не удалось обновить статус на 'active' для {account_id}: {db_err}")

        user_info = {}
        if user:
            try:
                user_info = {
                    "id": getattr(user, "id", None),
                    "username": getattr(user, "username", None),
                    "first_name": getattr(user, "first_name", None),
                    "last_name": getattr(user, "last_name", None),
                    "phone": getattr(user, "phone", None)
                }
            except Exception as e:
                logger.error(f"Ошибка при получении информации о пользователе: {str(e)}")

        return {"message": "Аккаунт успешно авторизован", "account_id": account_id, "status": "active", "user_info": user_info}

    except Exception as e:
        logger.error(f"Непредвиденная ошибка в процессе верификации для {account_id}: {str(e)}", exc_info=True)
        # Статус 'error' при глобальных ошибках
        try:
            async with pool.acquire() as conn:
                update_query = 'UPDATE telegram_accounts SET status = $1 WHERE id = $2'
                await conn.execute(update_query, 'error', account_id)
        except Exception as db_err:
             logger.error(f"Не удалось обновить статус на 'error' после глобальной ошибки верификации для {account_id}: {db_err}")

        raise HTTPException(status_code=500, detail=f"Внутренняя ошибка сервера при верификации: {str(e)}")
    finally:
        if client:
            if hasattr(client, 'is_connected') and callable(client.is_connected):
                is_connected = client.is_connected()
                if is_connected:
                    client.disconnect()
                    logger.info(f"Клиент для верификации кода аккаунта {account_id} отключен.")

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
        raise HTTPException(status_code=400, detail="Не указаны account_id и password")
        
    logger.info(f"Попытка верификации 2FA для аккаунта {account_id}")

    # Используем пул соединений asyncpg
    pool = await user_manager.get_db_pool()
    if not pool:
        logger.error(f"Не удалось получить пул соединений для верификации 2FA аккаунта {account_id}")
        raise HTTPException(status_code=500, detail="Ошибка сервера: База данных недоступна")

    client = None # Инициализируем для блока finally
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                # Находим аккаунт по ID, используя asyncpg синтаксис
                query = '''
                    SELECT api_id, api_hash, phone, proxy, session_file 
                    FROM telegram_accounts 
                    WHERE id = $1
                '''
                account_record = await conn.fetchrow(query, account_id)
                
                if not account_record:
                    logger.warning(f"Аккаунт {account_id} не найден для верификации 2FA")
                    # Транзакция откатится автоматически при выходе из блока или исключении
                    raise HTTPException(status_code=404, detail="Аккаунт не найден")
                
                logger.info(f"Аккаунт {account_id} найден, попытка авторизации через Telethon...")
                
                # Получаем данные из записи
                api_id = int(account_record['api_id']) # Преобразуем к int
                api_hash = account_record['api_hash']
                phone = account_record['phone'] # Может понадобиться для sign_in, если нет пароля
                proxy = account_record['proxy']
                session_file = account_record['session_file']
                
                # Создаем клиент Telethon (прокси уже передается сюда)
                client = await telegram_utils.create_telegram_client(session_file, api_id, api_hash, proxy)
                
                # Избыточный вызов set_proxy убран
                # if proxy:
                #     client.set_proxy(proxy)
        
                await client.connect()
                # Пытаемся войти с паролем (Telethon сам обработает, если пароль не нужен)
                await client.sign_in(phone=phone, password=password) # Передаем и телефон на всякий случай
                logger.info(f"Telethon sign_in выполнен успешно для аккаунта {account_id}")
        
                # Обновляем статус аккаунта в той же транзакции
                update_query = '''
                    UPDATE telegram_accounts
                    SET status = $1
                    WHERE id = $2
                '''
                update_result = await conn.execute(update_query, 'active', account_id)
                
                if 'UPDATE 1' not in update_result:
                    # Это маловероятно, так как мы только что нашли запись
                    logger.error(f"Не удалось обновить статус для аккаунта {account_id} после 2FA")
                    # Транзакция откатится
                    raise HTTPException(status_code=500, detail="Не удалось обновить статус аккаунта")
                
                logger.info(f"Статус аккаунта {account_id} обновлен на 'active' в БД.")
                # Транзакция закоммитится автоматически при выходе из блока 'async with conn.transaction()'
        
        # Успешное завершение
        logger.info(f"2FA авторизация для аккаунта {account_id} выполнена успешно.")
        return {"status": "success"}
        
    except HTTPException as http_exc:
        # Просто перевыбрасываем HTTP исключения (например, 404)
        raise http_exc
    except SessionPasswordNeededError:
        # Эта ошибка здесь не должна возникать, т.к. мы передаем пароль
        logger.error(f"Неожиданная ошибка SessionPasswordNeededError для аккаунта {account_id} при верификации 2FA")
        raise HTTPException(status_code=400, detail="Ошибка верификации: Похоже, требуется код, а не пароль")
    except (PhoneCodeInvalidError, PasswordHashInvalidError) as auth_err:
        logger.warning(f"Ошибка авторизации Telethon (неверный пароль?) для аккаунта {account_id}: {auth_err}")
        raise HTTPException(status_code=400, detail=f"Ошибка авторизации: Неверный пароль.")
    except FloodWaitError as flood_err:
        logger.warning(f"FloodWaitError при верификации 2FA для аккаунта {account_id}: {flood_err}")
        raise HTTPException(status_code=429, detail=f"Слишком много попыток. Подождите {flood_err.seconds} секунд.")
    except Exception as e:
        # Логируем другие ошибки (Telethon, DB и т.д.)
        logger.error(f"Непредвиденная ошибка при верификации 2FA для аккаунта {account_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Внутренняя ошибка сервера при верификации: {str(e)}")
    finally:
        # Закрываем клиент Telethon, если он был создан
        if client and client.is_connected():
            try:
                # Используем проверку на корутину для disconnect
                if asyncio.iscoroutinefunction(client.disconnect):
                    await client.disconnect()
                else:
                    client.disconnect()
                logger.info(f"Клиент Telethon для аккаунта {account_id} отключен после верификации 2FA.")
            except Exception as disc_err:
                logger.error(f"Ошибка при отключении клиента Telethon для аккаунта {account_id}: {disc_err}", exc_info=True)
        # Соединение с БД закрывается автоматически блоком 'async with pool.acquire()'

@app.delete("/api/telegram/accounts/{account_id}")
async def delete_telegram_account_endpoint(request: Request, account_id: str):
    """Удаляет Telegram аккаунт."""
    auth_header = request.headers.get('authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        raise HTTPException(401, "API ключ обязателен")
    admin_key = auth_header.split(' ')[1]
    
    # Проверяем админ-ключ
    if not await verify_admin_key(admin_key):
        raise HTTPException(401, "Неверный админ-ключ")
    
    # Получаем ID пользователя (он не нужен для удаления по ID аккаунта, но оставим проверку заголовка)
    user_id = request.headers.get('X-User-Id')
    if not user_id:
        raise HTTPException(400, "ID пользователя не указан")
    
    # Находим аккаунт по ID
    from user_manager import get_db_pool
    pool = await get_db_pool()
    
    try:
        async with pool.acquire() as conn:
            # Находим аккаунт по ID
            query = '''
                SELECT * FROM telegram_accounts 
                WHERE id = $1 
            '''
            account = await conn.fetchrow(query, account_id)
            
            if not account:
                raise HTTPException(404, "Аккаунт с указанным ID не найден")
            
            # Преобразуем объект Record в словарь
            account_dict = dict(account)
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
            delete_query = '''
                DELETE FROM telegram_accounts 
                WHERE id = $1
            '''
            await conn.execute(delete_query, account_id)
            
            return {"status": "success"}
    except Exception as e:
        logger.error(f"Ошибка при удалении Telegram аккаунта {account_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка при удалении аккаунта: {str(e)}")

@app.put("/api/telegram/accounts/{account_id}")
async def update_telegram_account(account_id: str, request: Request):
    """Обновляет прокси для указанного Telegram аккаунта."""
    auth_header = request.headers.get('authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        raise HTTPException(401, "API ключ обязателен")
    admin_key = auth_header.split(' ')[1]
    
    # Проверяем админ-ключ
    if not await verify_admin_key(admin_key):
        raise HTTPException(401, "Неверный админ-ключ")
   
    # 2. Получить данные из тела запроса
    try:
        data = await request.json()
        new_proxy = data.get('proxy')  # Может быть None или пустой строкой
        # Валидация прокси, если нужно
        if new_proxy:
            from telegram_utils import validate_proxy
            is_valid, _ = validate_proxy(new_proxy)
            if not is_valid:
                raise HTTPException(status_code=400, detail="Неверный формат прокси")
                 
    except json.JSONDecodeError:
        logger.error(f"Ошибка декодирования JSON при обновлении прокси TG {account_id}")
        raise HTTPException(status_code=400, detail="Некорректный формат JSON")
    except HTTPException as http_exc:  # Перебрасываем свои HTTP ошибки (например, от валидации прокси)
        raise http_exc
    except Exception as e:
        logger.error(f"Ошибка при получении/валидации данных для обновления прокси TG {account_id}: {e}")
        raise HTTPException(status_code=400, detail="Ошибка в данных запроса")
        
    logger.info(f"Попытка обновления прокси для TG аккаунта {account_id} на '{new_proxy}'")
        
    # 3. Обновить запись в БД
    pool = await user_manager.get_db_pool()
    if not pool:
        logger.error(f"Не удалось получить пул соединений для обновления прокси TG {account_id}")
        raise HTTPException(status_code=500, detail="Ошибка сервера: База данных недоступна")
        
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                # Обновляем запись в БД с использованием asyncpg
                update_query = "UPDATE telegram_accounts SET proxy = $1 WHERE id = $2"
                # Передаем None, если new_proxy пустая строка или None
                proxy_to_set = new_proxy if new_proxy else None 
                update_result = await conn.execute(update_query, proxy_to_set, account_id)
                
                # Проверяем, была ли запись обновлена (asyncpg возвращает статус, например 'UPDATE 1')
                if 'UPDATE 1' not in update_result:
                    # Запись не найдена, транзакция откатится
                    logger.warning(f"Telegram аккаунт с ID {account_id} не найден для обновления прокси.")
                    raise HTTPException(status_code=404, detail=f"Telegram аккаунт с ID {account_id} не найден")
                
                logger.info(f"Прокси для Telegram аккаунта {account_id} успешно обновлен в БД.")
                # Транзакция коммитится автоматически
                
        # Успешное завершение
        return {"message": "Прокси успешно обновлен"}
        
    except HTTPException as http_exc:
        # Перебрасываем HTTP ошибки (например, 404)
        raise http_exc
    except asyncpg.PostgresError as db_err:
        logger.error(f"Ошибка PostgreSQL при обновлении прокси для TG {account_id}: {db_err}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка базы данных при обновлении прокси")
    except Exception as e:
        logger.error(f"Непредвиденная ошибка при обновлении прокси для TG {account_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Внутренняя ошибка сервера при обновлении прокси")
    # Блоки finally и ручное закрытие conn не нужны благодаря 'async with'

# Эндпоинты для работы с VK аккаунтами
@app.post("/api/vk/accounts")
async def add_vk_account_endpoint(request: Request):
    """Добавляет новый VK аккаунт."""
    logger.info("Начало обработки запроса на добавление VK аккаунта")
    from fastapi import UploadFile
    try:
        # Обрабатываем как form-data
        form_data = await request.form()
        token = form_data.get("token")
        proxy = form_data.get("proxy", "")
        user_id = form_data.get("userId")
        
        # Получаем admin_key из заголовков
        auth_header = request.headers.get('authorization')
        if auth_header and auth_header.startswith('Bearer '):
            admin_key = auth_header.split(' ')[1]
        else:
            admin_key = request.cookies.get("admin_key")
        
        if not admin_key:
            logger.error("Admin ключ не предоставлен")
            raise HTTPException(status_code=401, detail="Admin ключ обязателен")
            
        if not user_id:
            logger.error("ID пользователя не указан")
            raise HTTPException(status_code=400, detail="ID пользователя не указан")
            
        if not token:
            logger.error("Токен VK не указан")
            raise HTTPException(status_code=400, detail="Токен VK обязателен")
        
        # Проверяем админ-ключ
        if not await verify_admin_key(admin_key):
            logger.error(f"Неверный админ-ключ: {admin_key}")
            raise HTTPException(status_code=401, detail="Неверный админ-ключ")
        
        if isinstance(token, UploadFile):
            token_data = await token.read()
            token_str = token_data.decode('utf-8') if isinstance(token_data, bytes) else str(token_data)
        else:
            token_str = str(token) if token is not None else ""

        if proxy and isinstance(proxy, UploadFile):
            proxy_data = await proxy.read()
            proxy_str = proxy_data.decode('utf-8') if isinstance(proxy_data, bytes) else str(proxy_data)
        else:
            proxy_str = str(proxy) if proxy is None else ""
        
        logger.info(f"Админ-ключ верифицирован, обрабатываем данные аккаунта для пользователя {user_id}")
        
        # Проверяем формат токена


        if not isinstance(token_str, str) or not token_str.startswith('vk1.a.'):
            logger.error("Неверный формат токена VK")
            raise HTTPException(status_code=400, detail="Неверный формат токена VK")
        
        # Валидируем прокси, если он указан
        if proxy:
            logger.info(f"Проверка прокси для VK: {proxy}")
            from vk_utils import validate_proxy
            is_valid = validate_proxy(proxy_str)
            
            if not is_valid:
                logger.error(f"Неверный формат прокси: {proxy}")
                raise HTTPException(status_code=400, detail="Неверный формат прокси")

        # Генерируем ID аккаунта
        account_id = str(uuid.uuid4())
        
        # Проверяем токен через VK API
        vk_user_id = None
        vk_user_name = None
        status = "pending"
        error_message = None
        error_code = None
        
        try:
            # Используем прокси при создании клиента, если он указан и валиден
            from vk_utils import VKClient
            
            # Передаем исходный, нешифрованный токен и проверенный proxy
            token_str_safe = str(token_str) if token_str is not None else None
            proxy_str_safe = str(proxy_str) if proxy_str is not None else None
            if token_str_safe is None:
                raise HTTPException(status_code=400, detail="Не указан токен VK")

            async with VKClient(token_str_safe, proxy_str_safe, account_id, admin_key) as vk:
                result = await vk._make_request("users.get", {"fields": "photo_50,screen_name"})
                if "response" in result and result["response"]:
                    vk_user_info = result["response"][0]
                    vk_user_id = vk_user_info.get('id')
                    vk_user_name = vk_user_info.get('first_name', '') + ' ' + vk_user_info.get('last_name', '')
                    status = "active"
                    logger.info(f"Токен VK успешно проверен для пользователя {vk_user_id} ({vk_user_name})")
                else:
                    error_info = result.get('error', {})
                    error_message = error_info.get('error_msg', 'Неизвестная ошибка VK API')
                    error_code = error_info.get('error_code')
                    status = "error"
                    logger.error(f"Ошибка проверки токена VK: {error_message} (Код: {error_code})")
                    
        except Exception as e:
            logger.error(f"Исключение при проверке токена VK через API: {e}", exc_info=True)
            status = "error"
            error_message = f"Ошибка подключения или API: {str(e)[:200]}"
        
        # Подготавливаем данные для сохранения
        account_data = {
            "token": token,
            "proxy": proxy,
            "status": status,
            "vk_user_id": vk_user_id,
            "vk_user_name": vk_user_name,
            "error_message": error_message,
            "error_code": error_code
        }

        # Используем правильную функцию add_vk_account с двумя аргументами
        try:
            from user_manager import add_vk_account
            # Функция возвращает bool, а не словарь
            user_id_str = str(user_id) if user_id is not None else ""
            success = await add_vk_account(user_id_str, account_data)
            
            if not success:
                logger.error(f"Ошибка при добавлении VK аккаунта")
                raise HTTPException(status_code=500, detail="Не удалось добавить VK аккаунт")
                
            logger.info(f"VK аккаунт {account_id} добавлен для пользователя {user_id} со статусом {status}")
            
            return {
                "success": True,
                "account_id": account_id,
                "status": status,
                "vk_user_id": vk_user_id,
                "vk_user_name": vk_user_name,
                "message": "VK аккаунт добавлен" + (" (требует проверки)." if status != "active" else ".")
            }
            
        except asyncpg.PostgresError as db_err:
            logger.error(f"Ошибка PostgreSQL при добавлении VK аккаунта: {db_err}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Ошибка базы данных: {str(db_err)}")
        except Exception as db_e:
            logger.error(f"Ошибка при добавлении VK аккаунта {account_id}: {db_e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Ошибка при добавлении VK аккаунта: {str(db_e)}")
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Непредвиденная ошибка при добавлении VK аккаунта: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Внутренняя ошибка сервера: {str(e)}")

@app.get("/api/vk/accounts")
async def get_vk_accounts(request: Request):
    """Получает VK аккаунты пользователя."""
    auth_header = request.headers.get('authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        raise HTTPException(401, "API ключ обязателен")
    api_key = auth_header.split(' ')[1]
    
    # Получаем данные пользователя
    user_data = await admin_get_user(api_key)
    if not user_data:
        raise HTTPException(404, "Пользователь не найден")
    
    return user_data.get("vk_accounts", [])

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
        raise HTTPException(status_code=401, detail="API ключ обязателен")
    admin_key = auth_header.split(' ')[1]
    
    # Проверяем админ-ключ
    if not await verify_admin_key(admin_key):
        logger.error("Неверный админ-ключ")
        raise HTTPException(status_code=401, detail="Неверный админ-ключ")

    pool = await user_manager.get_db_pool()
    if not pool:
        logger.error(f"Не удалось получить пул соединений для проверки статуса TG {account_id}")
        raise HTTPException(status_code=500, detail="Ошибка сервера: База данных недоступна")

    client = None
    new_status = 'unknown'  # Изначальный статус
    session_file = None
    api_id_int = None
    api_hash = None
    proxy = None

    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                # 1. Получаем данные аккаунта
                query = '''
                    SELECT api_id, api_hash, session_file, proxy, status
                    FROM telegram_accounts
                    WHERE id = $1
                '''
                account_record = await conn.fetchrow(query, account_id)

                if not account_record:
                    logger.warning(f"Аккаунт {account_id} не найден для проверки статуса")
                    raise HTTPException(status_code=404, detail="Аккаунт не найден")

                # Сохраняем текущий статус на случай ошибки
                current_status_db = account_record['status']
                new_status = current_status_db  # По умолчанию оставляем старый статус

                # Извлекаем данные
                api_id_str = account_record['api_id']
                api_hash = account_record['api_hash']
                session_file = account_record['session_file']
                proxy = account_record['proxy']

                # 2. Проверяем наличие необходимых данных
                if not api_id_str or not api_hash or not session_file:
                    logger.warning(f"Неполные данные для аккаунта {account_id} (api_id, api_hash или session_file отсутствуют)")
                    new_status = 'error'
                    # Обновляем статус в рамках транзакции
                    if new_status != current_status_db:
                        update_query = 'UPDATE telegram_accounts SET status = $1 WHERE id = $2'
                        await conn.execute(update_query, new_status, account_id)
                    # Транзакция закоммитится
                    raise HTTPException(status_code=400, detail="Неполные данные для проверки аккаунта")

                # 3. Преобразуем api_id
                try:
                    api_id_int = int(api_id_str)
                except (ValueError, TypeError):
                    logger.warning(f"Неверный формат api_id ('{api_id_str}') для аккаунта {account_id}")
                    new_status = 'error'
                    # Обновляем статус в рамках транзакции
                    if new_status != current_status_db:
                        update_query = 'UPDATE telegram_accounts SET status = $1 WHERE id = $2'
                        await conn.execute(update_query, new_status, account_id)
                    # Транзакция закоммитится
                    raise HTTPException(status_code=400, detail="Неверный формат api_id для аккаунта")

                # 4. Проверяем статус через Telethon
                logger.info(f"Создание клиента Telethon для проверки статуса аккаунта {account_id}")
                from telegram_utils import create_telegram_client  # Импорт внутри для ясности
                client = await create_telegram_client(session_file, api_id_int, api_hash, proxy)

        await client.connect()
        is_authorized = await client.is_user_authorized()
        logger.info(f"Статус авторизации Telethon для {account_id}: {is_authorized}")

        new_status = 'active' if is_authorized else 'pending'

        # 5. Обновляем статус в БД, если он изменился
        if new_status != current_status_db:
            logger.info(f"Обновление статуса для аккаунта {account_id} с '{current_status_db}' на '{new_status}'")
            update_query = '''
                UPDATE telegram_accounts
                SET status = $1
                WHERE id = $2
            '''
            await conn.execute(update_query, new_status, account_id)
            # Транзакция закоммитится
        else:
            logger.info(f"Статус для аккаунта {account_id} не изменился ('{current_status_db}')")

        # Если все прошло успешно внутри транзакции
        return {"account_id": account_id, "status": new_status, "is_authorized": new_status == 'active'}

    except HTTPException as http_exc:
        # Просто перебрасываем HTTP ошибки (404, 400)
        raise http_exc
    except Exception as e:
        logger.error(f"Ошибка при проверке статуса аккаунта {account_id}: {e}", exc_info=True)
        # Попытка обновить статус на 'error' в отдельной транзакции, если основная не удалась
        try:
            async with pool.acquire() as conn_err:
                async with conn_err.transaction():
                    update_query = 'UPDATE telegram_accounts SET status = $1 WHERE id = $2 AND status != $1'
                    await conn_err.execute(update_query, 'error', account_id)
                    logger.info(f"Установлен статус 'error' для аккаунта {account_id} после исключения.")
                    new_status = 'error'  # Обновляем для ответа
        except Exception as db_err:
            logger.error(f"Не удалось обновить статус на 'error' для аккаунта {account_id} после основной ошибки: {db_err}")
            # new_status останется тем, что было до ошибки (или 'unknown')

        # Возвращаем ошибку 500
        raise HTTPException(status_code=500, detail=f"Внутренняя ошибка при проверке статуса: {str(e)}")

    finally:
        # Гарантированно отключаем клиент, если он был создан и подключен
        if client:
            try:
                # Проверяем, нужно ли использовать await
                if client.is_connected():  # Сначала проверка, что он вообще подключен
                    if asyncio.iscoroutinefunction(client.disconnect):
                        await client.disconnect()
                    else:
                        client.disconnect()
                    logger.info(f"Клиент Telethon для проверки статуса {account_id} отключен.")
            except Exception as disc_err:
                logger.error(f"Ошибка при отключении клиента Telethon для {account_id}: {disc_err}", exc_info=True)


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

    # Получаем пул соединений PostgreSQL
    pool = await user_manager.get_db_pool()
    if not pool:
        logger.error(f"Не удалось получить пул соединений для повторной отправки кода TG {account_id}")
        raise HTTPException(500, "Ошибка сервера: База данных недоступна")
    
    client = None
    
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                # 1. Получаем данные аккаунта
                query = '''
                    SELECT api_id, api_hash, phone, proxy, status
                    FROM telegram_accounts 
                    WHERE id = $1
                '''
                account_record = await conn.fetchrow(query, account_id)
                
                if not account_record:
                    logger.error(f"Аккаунт с ID {account_id} не найден")
                    raise HTTPException(404, "Аккаунт не найден")
                
                # Преобразуем record в словарь для удобства
                api_id_str = account_record['api_id']
                api_hash = account_record['api_hash']
                phone = account_record['phone']
                proxy = account_record['proxy']
                current_status = account_record['status']
                
                # Проверяем наличие необходимых данных
                if not api_id_str or not api_hash or not phone:
                    logger.error(f"Неполные данные для аккаунта {account_id}")
                    # Обновляем статус на 'error' если еще не установлен
                    if current_status != 'error':
                        await conn.execute(
                            'UPDATE telegram_accounts SET status = $1 WHERE id = $2',
                            'error', account_id
                        )
                    raise HTTPException(400, "Неполные данные аккаунта")
                
                # Преобразуем api_id в int
                try:
                    api_id = int(api_id_str)
                except (ValueError, TypeError):
                    logger.error(f"Неверный формат api_id для аккаунта {account_id}: {api_id_str}")
                    if current_status != 'error':
                        await conn.execute(
                            'UPDATE telegram_accounts SET status = $1 WHERE id = $2',
                            'error', account_id
                        )
                    raise HTTPException(400, "Неверный формат api_id")
                
                # Создаем клиент Telegram
                from telegram_utils import create_telegram_client
                logger.info(f"Создание клиента Telegram, сессия: {account_id}")
                # Используем временную сессию для операции отправки кода
                client = await create_telegram_client(f"resend_{account_id}", api_id, api_hash, proxy)
                
                logger.info("Устанавливаем соединение с Telegram")
                await client.connect()
                
                # Проверяем, не авторизован ли уже клиент
                if await client.is_user_authorized():
                    logger.info("Клиент уже авторизован")
                    # Отключаем временный клиент
                    if asyncio.iscoroutinefunction(client.disconnect):
                        await client.disconnect()
                    else:
                        client.disconnect()
                    client = None  # Сбрасываем, чтобы finally его не трогал
                    
                    # Обновляем статус аккаунта на 'active'
                    if current_status != 'active':
                        await conn.execute(
                            'UPDATE telegram_accounts SET status = $1, phone_code_hash = NULL WHERE id = $2',
                            'active', account_id
                        )
                        logger.info(f"Статус аккаунта {account_id} обновлен на 'active' (т.к. уже авторизован)")
                    
                    return {
                        "account_id": account_id,
                        "requires_auth": False,
                        "message": "Аккаунт уже авторизован"
                    }
                
                # Отправляем запрос на код авторизации
                logger.info(f"Отправка запроса на код авторизации для номера {phone}")
                try:
                    result = await client.send_code_request(phone)
                    logger.info(f"Запрос на код авторизации успешно отправлен, hash: {result.phone_code_hash}")
                    
                    # Сохраняем phone_code_hash и обновляем статус
                    await conn.execute(
                        'UPDATE telegram_accounts SET phone_code_hash = $1, status = $2 WHERE id = $3',
                        result.phone_code_hash, 'pending_code', account_id
                    )
                    
                    # Транзакция закоммитится автоматически при выходе из блока
                    return {
                        "account_id": account_id,
                        "requires_auth": True,
                        "message": "Код авторизации отправлен"
                    }
                    
                except FloodWaitError as e:
                    logger.error(f"FloodWaitError при отправке кода: {e}")
                    raise HTTPException(429, f"Слишком много попыток. Подождите {e.seconds} секунд.")
                
    except HTTPException:
        # Просто перебрасываем специфические HTTP ошибки
        raise
    except asyncpg.PostgresError as db_e:
        logger.error(f"Ошибка PostgreSQL при работе с БД: {db_e}", exc_info=True)
        raise HTTPException(500, "Ошибка базы данных")
    except Exception as e:
        logger.error(f"Ошибка при отправке кода авторизации: {str(e)}", exc_info=True)
        # Попытка обновить статус на 'error'
        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    await conn.execute(
                        'UPDATE telegram_accounts SET status = $1 WHERE id = $2 AND status != $1',
                        'error', account_id
                    )
        except Exception as update_err:
            logger.error(f"Не удалось обновить статус на 'error': {update_err}")
        
        raise HTTPException(400, f"Ошибка при отправке кода авторизации: {str(e)}")
    finally:
        # Отключаем клиент, если он был создан
        if client:
            try:
                if hasattr(client, 'is_connected') and client.is_connected():
                    if asyncio.iscoroutinefunction(client.disconnect):
                        await client.disconnect()
                    else:
                        client.disconnect()
                    logger.info("Соединение с Telegram закрыто")
            except Exception as disc_err:
                logger.error(f"Ошибка при отключении клиента: {disc_err}")

@app.get("/api/vk/accounts/{account_id}/status")
async def check_vk_account_status(request: Request, account_id: str):
    """Проверяет статус аккаунта VK."""
    logger.info(f"Проверка статуса VK аккаунта с ID {account_id}")
    auth_header = request.headers.get('authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        logger.error("API ключ не предоставлен или в неверном формате")
        raise HTTPException(401, "API ключ обязателен")
    admin_key = auth_header.split(' ')[1]

    if not await verify_admin_key(admin_key):
        logger.error("Неверный админ-ключ")
        raise HTTPException(401, "Неверный админ-ключ")

    from user_manager import get_db_pool, cipher # Эти импорты можно вынести наверх файла
    from vk_utils import VKClient # Этот импорт тоже

    token: Optional[str] = None
    proxy: Optional[str] = None
    user_info: Optional[dict] = None
    status: str = "unknown"
    error_message: Optional[str] = None
    error_code: int = 0
    # Определяем текущее время как объект datetime один раз
    last_checked_at_dt = datetime.now(timezone.utc) # <--- ИЗМЕНЕНО: теперь объект datetime

    pool = await get_db_pool()
    if not pool:
        logger.error(f"Не удалось получить пул соединений для проверки статуса VK {account_id}")
        raise HTTPException(500, "Ошибка сервера: База данных недоступна")

    decrypted_token_for_update: Optional[str] = None # Переменная для хранения расшифрованного токена для обновления

    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                # --- Шаг 1: Чтение данных и расшифровка токена ---
                query = 'SELECT token, proxy, status FROM vk_accounts WHERE id = $1'
                account_record = await conn.fetchrow(query, account_id)

                if not account_record:
                    logger.error(f"Аккаунт с ID {account_id} не найден в БД")
                    raise HTTPException(404, "Аккаунт не найден")

                token_value = account_record['token']
                proxy = account_record['proxy']
                current_status = account_record['status']

                if not token_value:
                    logger.warning(f"Токен отсутствует в БД для аккаунта {account_id}")
                    status = "error"
                    error_message = "Токен отсутствует в БД"
                elif token_value.startswith('vk1.a.'):
                    logger.info(f"Токен для {account_id} не зашифрован (или уже обновлен).")
                    token = token_value
                    decrypted_token_for_update = token # Сохраняем для обновления
                else:
                    try:
                        logger.info(f"Пытаемся расшифровать токен для {account_id}")
                        decrypted_token = cipher.decrypt(token_value.encode()).decode()

                        if decrypted_token.startswith('vk1.a.'):
                            logger.info(f"Токен для {account_id} успешно расшифрован.")
                            token = decrypted_token
                            decrypted_token_for_update = token # Сохраняем расшифрованный токен для обновления
                        else:
                            try:
                                logger.info(f"Проверка двойного шифрования для {account_id}")
                                decrypted_twice = cipher.decrypt(decrypted_token.encode()).decode()
                                if decrypted_twice.startswith('vk1.a.'):
                                    logger.info(f"Токен для {account_id} был зашифрован дважды.")
                                    token = decrypted_twice
                                    decrypted_token_for_update = token # Сохраняем правильно расшифрованный токен
                                else:
                                    logger.error(f"Невалидный формат токена после двойной расшифровки для {account_id}")
                                    status = "error"
                                    error_message = "Невалидный формат расшифрованного токена (двойное шифрование?)"
                            except Exception as e:
                                logger.error(f"Ошибка при попытке двойной расшифровки для {account_id}: {e}")
                                status = "error"
                                error_message = "Невалидный формат расшифрованного токена"

                    except Exception as decrypt_error:
                        logger.error(f"Ошибка расшифровки токена для {account_id}: {decrypt_error}")
                        status = "error"
                        error_message = f"Ошибка расшифровки токена: {str(decrypt_error)}"

                # --- Шаг 2: Проверка через API VK (если есть токен и нет ошибки) ---
                if token and status == "unknown":
                    try:
                        logger.info(f"Проверка токена через VK API для {account_id}")
                        async with VKClient(token, proxy) as vk:
                            result = await vk._make_request("users.get", {"fields": "photo_50,screen_name"})

                            if not result or "response" not in result or not result["response"]:
                                status = "error"
                                error_message = "Ошибка API VK: не получен ответ или нет данных пользователя"
                                logger.error(f"{error_message} для {account_id}")
                            else:
                                user_info = result["response"][0]
                                status = "active"
                                logger.info(f"Токен VK для {account_id} действителен: {user_info}")

                    except Exception as api_e:
                        logger.error(f"Ошибка при проверке токена VK API для {account_id}: {api_e}")
                        error_message = str(api_e)
                        # Определяем статус по ошибке API (логика остается)
                        # ... (код определения error_code и status по api_e) ...
                        if "error_code" in error_message:
                            try:
                                error_code = int(error_message.split("error_code")[1].split(":")[1].strip().split(",")[0])
                            except: pass
                        if "Токен недействителен" in error_message or "access_token has expired" in error_message or error_code == 5:
                             status = "invalid"
                        elif "Ключ доступа сообщества недействителен" in error_message or error_code == 27:
                             status = "invalid"
                        elif "Пользователь заблокирован" in error_message or error_code == 38:
                             status = "banned"
                        elif "Превышен лимит запросов" in error_message or error_code == 29:
                             status = "rate_limited"
                        elif "Требуется валидация" in error_message or error_code == 17:
                             status = "validation_required"
                        else: status = "error"

                elif status == "unknown":
                    status = "error"
                    if not error_message:
                        error_message = "Токен отсутствует или не удалось расшифровать"

                # --- Шаг 3: Финальное обновление статуса в БД ---
                user_id_to_save = user_info.get('id') if user_info else None
                user_name_to_save = f"{user_info.get('first_name', '')} {user_info.get('last_name', '')}".strip() if user_info else None
                
                # Обновляем is_active на основе статуса
                is_active_bool = status == "active"

                # Обновляем и токен, если он был успешно расшифрован или изначально был верным
                update_query = '''
                    UPDATE vk_accounts SET
                        status = $1, user_id = $2, user_name = $3,
                        error_message = $4, error_code = $5, last_checked_at = $6,
                        is_active = $7, token = $8 
                    WHERE id = $9
                '''
                await conn.execute(
                    update_query,
                    status, user_id_to_save, user_name_to_save,
                    error_message, error_code,
                    last_checked_at_dt, # <--- ИЗМЕНЕНО: передаем объект datetime
                    is_active_bool, # Обновляем is_active
                    decrypted_token_for_update, # Обновляем токен
                    account_id
                )
                logger.info(f"Статус аккаунта {account_id} обновлен на '{status}' в БД (токен также обновлен при необходимости).")
                # Транзакция закоммитится автоматически

        # --- Возвращаем результат ---
        if status == "active":
            return {"account_id": account_id, "status": status, "user_info": user_info}
        else:
            response_data = {"account_id": account_id, "status": status, "error": error_message, "error_code": error_code}
            if user_info:
                response_data["user_info"] = user_info
            return response_data

    except HTTPException as http_exc:
        raise http_exc
    except asyncpg.PostgresError as db_err:
        logger.error(f"Ошибка PostgreSQL при проверке статуса VK {account_id}: {db_err}", exc_info=True)
        raise HTTPException(500, f"Ошибка базы данных: {str(db_err)}")
    except Exception as final_e:
        logger.error(f"Непредвиденная ошибка в check_vk_account_status для {account_id}: {final_e}\n{traceback.format_exc()}")
        
        try:
            async with pool.acquire() as conn_err:
                async with conn_err.transaction():
                    error_update_query = '''
                        UPDATE vk_accounts 
                        SET status = $1, error_message = $2, last_checked_at = $3, is_active = FALSE 
                        WHERE id = $4 AND status != $1
                    '''
                    error_msg = f"Внутренняя ошибка: {str(final_e)[:150]}"
                    # Используем новый объект datetime для блока ошибки
                    error_time_dt = datetime.now(timezone.utc) # <--- ИЗМЕНЕНО: объект datetime
                    await conn_err.execute(error_update_query, 'error', error_msg, error_time_dt, account_id)
                    logger.info(f"Установлен статус 'error' для VK аккаунта {account_id} после исключения.")
        except Exception as update_err:
            logger.error(f"Не удалось обновить статус на 'error' для VK {account_id}: {update_err}")
            
        raise HTTPException(500, f"Внутренняя ошибка сервера: {str(final_e)}")


async def shutdown_event():
    """Обработчик закрытия приложения."""
    logger.info("Приложение завершает работу, отключаем все клиенты Telegram")
    
    # Отключаем все клиенты
    if hasattr(telegram_pool, 'clients'):
        for client_id, client in telegram_pool.clients.items():
            try:
                if hasattr(client, 'disconnect'):
                    if asyncio.iscoroutinefunction(client.disconnect):
                        await client.disconnect()
                    else:
                        client.disconnect()
                    logger.debug(f"Клиент {client_id} отключен")
            except Exception as e:
                logger.warning(f"Ошибка при отключении клиента {client_id}: {e}")
    
    logger.info("Все клиенты Telegram отключены")

# Добавим эндпоинт для получения расширенной статистики аккаунтов
@app.get("/api/admin/accounts/stats")
async def admin_accounts_stats(request: Request):
    """Получает статистику использования аккаунтов."""
    # Проверяем админ-ключ
    admin_key = request.headers.get("X-Admin-Key") or request.headers.get("X-API-KEY")
    if not admin_key:
        admin_key = request.cookies.get("admin_key")
    
    if not admin_key or not await verify_admin_key(admin_key):
        raise HTTPException(status_code=401, detail="Invalid admin key")
    
    # Используем пул соединений PostgreSQL
    from user_manager import get_db_pool
    import asyncpg
    
    pool = await get_db_pool()
    if not pool:
        logger.error("Не удалось получить пул соединений для статистики аккаунтов")
        raise HTTPException(500, "Ошибка сервера: База данных недоступна")
    
    vk_accounts = []
    telegram_accounts = []
    
    try:
        async with pool.acquire() as conn:
            # Проверяем существование таблиц (для PostgreSQL)
            tables_query = """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name IN ('vk_accounts', 'telegram_accounts')
            """
            tables_rows = await conn.fetch(tables_query)
            tables = [row['table_name'] for row in tables_rows]
            logger.info(f"Доступные таблицы в базе данных: {', '.join(tables)}")
            
            # Получаем все аккаунты VK, если таблица существует
            if 'vk_accounts' in tables:
                try:
                    vk_query = '''
                        SELECT 
                            id, 
                            user_api_key, 
                            token, 
                            proxy, 
                            status, 
                            is_active, 
                            request_limit,
                            user_id,
                            user_name,
                            error_message
                        FROM vk_accounts
                    '''
                    vk_rows = await conn.fetch(vk_query)
                    vk_accounts = [dict(row) for row in vk_rows]
                    logger.info(f"Получено {len(vk_accounts)} VK аккаунтов")
                except Exception as e:
                    logger.error(f"Ошибка при запросе VK аккаунтов: {e}", exc_info=True)
            else:
                logger.warning("Таблица vk_accounts не найдена в базе данных")
            
            # Получаем все аккаунты Telegram, если таблица существует
            if 'telegram_accounts' in tables:
                try:
                    tg_query = '''
                        SELECT 
                            id, 
                            user_api_key, 
                            phone, 
                            proxy, 
                            status, 
                            is_active, 
                            request_limit,
                            api_id,
                            api_hash
                        FROM telegram_accounts
                    '''
                    tg_rows = await conn.fetch(tg_query)
                    telegram_accounts = [dict(row) for row in tg_rows]
                    logger.info(f"Получено {len(telegram_accounts)} Telegram аккаунтов")
                except Exception as e:
                    logger.error(f"Ошибка при запросе Telegram аккаунтов: {e}", exc_info=True)
            else:
                logger.warning("Таблица telegram_accounts не найдена в базе данных")
    
    except asyncpg.PostgresError as db_err:
        logger.error(f"Ошибка PostgreSQL при получении статистики аккаунтов: {db_err}", exc_info=True)
        raise HTTPException(500, f"Ошибка базы данных: {str(db_err)}")
    except Exception as e:
        logger.error(f"Непредвиденная ошибка при получении статистики аккаунтов: {e}", exc_info=True)
        raise HTTPException(500, f"Внутренняя ошибка сервера: {str(e)}")
    
    # Преобразуем Record объекты в словари и обрабатываем типы данных
    vk_accounts_processed = []
    for account in vk_accounts:
        account_dict = dict(account)
        # Преобразуем bool в int для совместимости с фронтендом (если нужно)
        if isinstance(account_dict.get('is_active'), bool):
            account_dict['is_active'] = 1 if account_dict['is_active'] else 0
        vk_accounts_processed.append(account_dict)
    
    telegram_accounts_processed = []
    for account in telegram_accounts:
        account_dict = dict(account)
        # Преобразуем bool в int для совместимости с фронтендом (если нужно)
        if isinstance(account_dict.get('is_active'), bool):
            account_dict['is_active'] = 1 if account_dict['is_active'] else 0
        telegram_accounts_processed.append(account_dict)
    
    # Дополняем данные статистикой из Redis, если доступен
    if redis_client:
        # Обработка VK аккаунтов
        for account in vk_accounts_processed:
            account['login'] = account.get('user_name', 'Нет данных')
            account['active'] = account.get('is_active', 1) == 1
            
            # Получаем актуальную статистику из Redis
            try:
                redis_stats = await get_account_stats_redis(account['id'], 'vk')
                if redis_stats:
                    account['requests_count'] = redis_stats.get('requests_count', 0)
                    account['last_used'] = redis_stats.get('last_used')
                else:
                    account['requests_count'] = 0
                    account['last_used'] = None
            except Exception as e:
                logger.error(f"Ошибка при получении статистики из Redis для VK аккаунта {account['id']}: {e}")
                account['requests_count'] = 0
                account['last_used'] = None
        
        # Обработка Telegram аккаунтов
        for account in telegram_accounts_processed:
            account['login'] = account.get('phone', 'Нет данных')
            account['active'] = account.get('is_active', 1) == 1
            
            # Получаем актуальную статистику из Redis
            try:
                redis_stats = await get_account_stats_redis(account['id'], 'telegram')
                if redis_stats:
                    account['requests_count'] = redis_stats.get('requests_count', 0)
                    account['last_used'] = redis_stats.get('last_used')
                else:
                    account['requests_count'] = 0
                    account['last_used'] = None
            except Exception as e:
                logger.error(f"Ошибка при получении статистики из Redis для Telegram аккаунта {account['id']}: {e}")
                account['requests_count'] = 0
                account['last_used'] = None
    else:
        # Если Redis недоступен, инициализируем значения по умолчанию
        for account in vk_accounts_processed:
            account['login'] = account.get('user_name', 'Нет данных')
            account['active'] = account.get('is_active', 1) == 1
            account['requests_count'] = 0
            account['last_used'] = None
            
        for account in telegram_accounts_processed:
            account['login'] = account.get('phone', 'Нет данных')
            account['active'] = account.get('is_active', 1) == 1
            account['requests_count'] = 0
            account['last_used'] = None
    
    return {
        "timestamp": time.time(),
        "vk": vk_accounts_processed,
        "telegram": telegram_accounts_processed
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
    
    # Получаем пул соединений PostgreSQL
    from user_manager import get_db_pool
    import asyncpg
    
    pool = await get_db_pool()
    if not pool:
        logger.error(f"Не удалось получить пул соединений для изменения статуса аккаунта {account_id}")
        raise HTTPException(500, "Ошибка сервера: База данных недоступна")
    
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                if platform.lower() == 'vk':
                    # В PostgreSQL используем $1, $2 вместо ?
                    # Также для boolean значений используем True/False вместо 1/0
                    update_query = "UPDATE vk_accounts SET is_active = $1 WHERE id = $2"
                    await conn.execute(update_query, bool(active), account_id)
                    
                    # Также обновляем статус, чтобы он соответствовал активности
                    status_query = "UPDATE vk_accounts SET status = $1 WHERE id = $2"
                    await conn.execute(status_query, 'active' if active else 'inactive', account_id)
                    
                    # Проверяем, была ли запись обновлена
                    check_query = "SELECT COUNT(*) FROM vk_accounts WHERE id = $1"
                    count = await conn.fetchval(check_query, account_id)
                    if count == 0:
                        logger.warning(f"VK аккаунт с ID {account_id} не найден")
                        raise HTTPException(status_code=404, detail=f"VK аккаунт с ID {account_id} не найден")
                        
                elif platform.lower() == 'telegram':
                    update_query = "UPDATE telegram_accounts SET is_active = $1 WHERE id = $2"
                    await conn.execute(update_query, bool(active), account_id)
                    
                    # Также обновляем статус, чтобы он соответствовал активности
                    status_query = "UPDATE telegram_accounts SET status = $1 WHERE id = $2"
                    await conn.execute(status_query, 'active' if active else 'inactive', account_id)
                    
                    # Проверяем, была ли запись обновлена
                    check_query = "SELECT COUNT(*) FROM telegram_accounts WHERE id = $1"
                    count = await conn.fetchval(check_query, account_id)
                    if count == 0:
                        logger.warning(f"Telegram аккаунт с ID {account_id} не найден")
                        raise HTTPException(status_code=404, detail=f"Telegram аккаунт с ID {account_id} не найден")
                else:
                    raise HTTPException(status_code=400, detail="Неизвестная платформа")
                
                # Транзакция закоммитится автоматически при выходе из блока
                logger.info(f"Статус аккаунта {account_id} ({platform}) обновлен на {'активен' if active else 'неактивен'} в БД")
        
        # Обновляем статус в пуле клиентов
        if platform.lower() == 'vk':
            # Найдем клиент в пуле и обновим его
            try:
                # В VKPool нет структуры clients с вложенным словарем, как в TelegramPool
                # Поэтому обрабатываем его по-другому
                if hasattr(vk_pool, 'clients') and isinstance(vk_pool.clients, dict):
                    if account_id in vk_pool.clients:
                        # Если клиент есть в словаре, обновляем его статус
                        client = vk_pool.clients.get(account_id)
                        if client and hasattr(client, 'is_active'):
                            client.is_active = active
                # Также можно обновить статус в других структурах vk_pool, если они есть
            except Exception as e:
                logger.warning(f"Не удалось обновить статус клиента VK в пуле: {e}")
        elif platform.lower() == 'telegram':
            # Для Telegram также обновим статус в пуле
            for user_key, clients in telegram_pool.clients.items():
                for i, client in enumerate(clients):
                    if str(client.id) == str(account_id):
                        client.is_active = active
                        # Если деактивировали, возможно нужно отключить клиент
                        if not active and account_id in telegram_pool.connected_clients:
                            if hasattr(telegram_pool, 'disconnect_client'):
                                asyncio.create_task(telegram_pool.disconnect_client(account_id))
                        break
        
        return {"success": True, "message": f"Статус аккаунта {account_id} изменен на {'активен' if active else 'неактивен'}"}
    
    except HTTPException as http_exc:
        # Просто пробрасываем HTTP исключения
        raise http_exc
    except asyncpg.PostgresError as db_err:
        logger.error(f"Ошибка PostgreSQL при изменении статуса аккаунта {account_id}: {db_err}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка базы данных: {str(db_err)}")
    except Exception as e:
        logger.error(f"Непредвиденная ошибка при изменении статуса аккаунта {account_id}: {e}", exc_info=True)
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
    
    # Получаем пул соединений PostgreSQL вместо единичного соединения
    from user_manager import get_db_pool
    pool = await get_db_pool()
    
    try:
        async with pool.acquire() as conn:
            # Статистика по Telegram аккаунтам
            telegram_stats_by_status = await conn.fetch('''
                SELECT 
                    status, 
                    COUNT(*) as count, 
                    AVG(requests_count) as avg_requests,
                    MAX(requests_count) as max_requests,
                    MIN(requests_count) as min_requests
                FROM telegram_accounts 
                GROUP BY status
            ''')
            
            # Рассчитываем общее количество аккаунтов Telegram
            total_tg_accounts = await conn.fetchval('''
                SELECT COUNT(*) FROM telegram_accounts
            ''')
            
            # Рассчитываем количество активных аккаунтов Telegram
            active_tg_accounts = await conn.fetchval('''
                SELECT COUNT(*) FROM telegram_accounts WHERE status = 'active'
            ''')
            
            # Преобразуем результаты в список словарей и добавляем общую информацию
            telegram_stats = [dict(row) for row in telegram_stats_by_status]
            telegram_stats_dict = {
                "status_breakdown": telegram_stats,
                "total": total_tg_accounts or 0,
                "active": active_tg_accounts or 0
            }
            
            # Статистика по VK аккаунтам
            vk_stats_by_status = await conn.fetch('''
                SELECT 
                    status, 
                    COUNT(*) as count,
                    AVG(requests_count) as avg_requests,
                    MAX(requests_count) as max_requests,
                    MIN(requests_count) as min_requests
                FROM vk_accounts 
                GROUP BY status
            ''')
            
            # Рассчитываем общее количество аккаунтов VK
            total_vk_accounts = await conn.fetchval('''
                SELECT COUNT(*) FROM vk_accounts
            ''')
            
            # Рассчитываем количество активных аккаунтов VK
            active_vk_accounts = await conn.fetchval('''
                SELECT COUNT(*) FROM vk_accounts WHERE status = 'active'
            ''')
            
            # Преобразуем результаты в список словарей и добавляем общую информацию
            vk_stats = [dict(row) for row in vk_stats_by_status]
            vk_stats_dict = {
                "status_breakdown": vk_stats,
                "total": total_vk_accounts or 0,
                "active": active_vk_accounts or 0
            }
            
            # Получаем статистику по пользователям с их аккаунтами
            users_data = await conn.fetch('''
                SELECT 
                    u.username,
                    u.api_key,
                    COUNT(t.id) as telegram_count,
                    COUNT(v.id) as vk_count,
                    COALESCE(SUM(t.requests_count), 0) as telegram_requests,
                    COALESCE(SUM(v.requests_count), 0) as vk_requests
                FROM users u
                LEFT JOIN telegram_accounts t ON u.api_key = t.user_api_key
                LEFT JOIN vk_accounts v ON u.api_key = v.user_api_key
                GROUP BY u.api_key, u.username
            ''')
            
            # Преобразуем результаты в список словарей
            users_basic = [dict(row) for row in users_data]
            
            # Для каждого пользователя получаем подробную информацию об его аккаунтах
            detailed_users = []
            for user in users_basic:
                user_api_key = user["api_key"]
                
                # Получаем Telegram аккаунты пользователя
                tg_accounts = await conn.fetch('''
                    SELECT 
                        id, 
                        phone, 
                        status, 
                        requests_count, 
                        last_request_time
                    FROM telegram_accounts 
                    WHERE user_api_key = $1
                ''', user_api_key)

                # Получаем VK аккаунты пользователя
                vk_accounts = await conn.fetch('''
                    SELECT 
                        id, 
                        user_id, 
                        user_name, 
                        status, 
                        requests_count, 
                        last_request_time
                    FROM vk_accounts 
                    WHERE user_api_key = $1
                ''', user_api_key)
                
                # Формируем полную информацию о пользователе
                user_info = {
                    "username": user["username"],
                    "api_key": user_api_key,
                    "telegram_count": user["telegram_count"],
                    "vk_count": user["vk_count"],
                    "telegram_requests": user["telegram_requests"],
                    "vk_requests": user["vk_requests"],
                    "telegram_accounts": [dict(acc) for acc in tg_accounts],
                    "vk_accounts": [dict(acc) for acc in vk_accounts]
                }
                
                detailed_users.append(user_info)
                
    except Exception as e:
        logger.error(f"Ошибка при получении статистики аккаунтов: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(500, "Ошибка при получении статистики аккаунтов")
    
    # Статистика по использованию клиентов из пула
    try:
        # Используем метод get_clients_usage_statistics для VK клиентов
        vk_usage = vk_pool.get_clients_usage_statistics()
    except Exception as e:
        logger.error(f"Ошибка при получении статистики VK клиентов: {e}")
        # Используем старый способ, если метод недоступен
        vk_usage = {
            account_id: {
                "usage_count": count,
                "last_used": vk_pool.last_used.get(account_id, 0)
            } for account_id, count in vk_pool.usage_counts.items()
        }
    
    # Для Telegram клиентов формируем статистику с информацией о подключении
    try:
        # Пробуем использовать новый метод get_clients_usage_statistics для Telegram пула
        if hasattr(telegram_pool, 'get_clients_usage_statistics'):
            telegram_usage = telegram_pool.get_clients_usage_statistics()
        else:
            # Используем старый способ
            telegram_usage = {
                account_id: {
                    "usage_count": count,
                    "last_used": telegram_pool.last_used.get(account_id, 0),
                    "connected": account_id in telegram_pool.connected_clients
                } for account_id, count in telegram_pool.usage_counts.items()
            }
    except Exception as e:
        logger.error(f"Ошибка при получении статистики Telegram клиентов: {e}")
        telegram_usage = {}
    
    # В PostgreSQL мы не закрываем соединение, так как оно возвращается в пул
    
    return {
        "telegram": {
            "stats_by_status": telegram_stats_dict,
            "usage": telegram_usage,
            "connected_count": len(telegram_pool.connected_clients)
        },
        "vk": {
            "stats_by_status": vk_stats_dict,
            "usage": vk_usage
        },
        "users": detailed_users,
        "timestamp": time.time()
    }

# Новый эндпоинт для расширенного получения трендовых постов
@app.post("/api/trending-posts-extended")
async def api_trending_posts_extended(request: Request, data: dict):
    """
    Получение трендовых постов с расширенными параметрами фильтрации 
    и поддержкой медиа-альбомов.
    """
    try:
        # Инициализируем планировщик медиа
        from media_utils import init_scheduler
        await init_scheduler()
        
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
            
            account = await get_next_available_account(api_key, "telegram")
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
                min_forwards=min_forwards,
                api_key=api_key,
                non_blocking=True  # Устанавливаем non_blocking=True для более быстрого ответа
            )
            
            # Обновляем статистику использования аккаунта
            await update_account_usage(api_key, account["id"], "telegram")
            
            return posts
        elif platform == 'vk':
            # Здесь можно добавить аналогичную логику для VK
            raise HTTPException(status_code=501, detail="Расширенные параметры пока не поддерживаются для VK")
        else:
            raise HTTPException(status_code=400, detail="Платформа не поддерживается")
    except asyncpg.PostgresError as db_err:
        logger.error(f"Ошибка PostgreSQL при получении трендовых постов: {db_err}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка базы данных: {str(db_err)}")
    except Exception as e:
        logger.error(f"Непредвиденная ошибка при получении трендовых постов: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Внутренняя ошибка сервера: {str(e)}")

@app.post("/api/media/upload")
async def api_media_upload(request: Request):
    import uuid
    import os
    from fastapi import UploadFile
    local_path = None
    """
    Загрузка медиафайлов в хранилище S3.
    
    Поддерживает загрузку изображений и видео,
    создаёт превью для больших файлов и оптимизирует изображения.
    """
    try:
        # Инициализируем планировщик медиа
        from media_utils import init_scheduler
        await init_scheduler()
        
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
        
        file_id = str(uuid.uuid4())
        if isinstance(file, UploadFile) and file.filename:
            filename = str(file.filename)
            file_ext = os.path.splitext(filename)[1]
        else:
            file_ext = '.bin'  # Расширение по умолчанию
        s3_filename = f"media/{file_id}{file_ext}"
        local_path = f"temp_{file_id}{file_ext}"
        
        # Сохраняем файл на диск
        if isinstance(file, UploadFile):
            content = await file.read()
            with open(local_path, "wb") as buffer:
                buffer.write(content)
        else:
            # Обработка случая, когда file это строка (путь к файлу)
            file_path = str(file)
            with open(file_path, "rb") as source_file:
                with open(local_path, "wb") as buffer:
                    buffer.write(source_file.read())
        
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
    except asyncpg.PostgresError as db_err:
        # Удаляем локальный файл в случае ошибки
        if local_path is not None and os.path.exists(local_path):
            os.remove(local_path)
        
        logger.error(f"Ошибка PostgreSQL при загрузке файла: {db_err}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка базы данных: {str(db_err)}")
    except Exception as e:
        # Удаляем локальный файл в случае ошибки
        if local_path is not None and os.path.exists(local_path):
            os.remove(local_path)
        
        logger.error(f"Ошибка при загрузке файла: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка при загрузке файла: {str(e)}")

@app.get("/api/admin/test-vk-tokens")
async def test_vk_tokens(request: Request):
    """Тестирует расшифровку токенов VK для всех аккаунтов."""
    # Проверяем админ-ключ
    admin_key = request.headers.get("X-Admin-Key")
    if not admin_key:
        admin_key = request.cookies.get("admin_key")
    
    if not admin_key or not await verify_admin_key(admin_key):
        raise HTTPException(status_code=401, detail="Invalid admin key")
    
    try:
        from user_manager import get_db_pool, cipher
        pool = await get_db_pool()
        if not pool:
            logger.error("Не удалось получить пул соединений к БД")
            raise HTTPException(status_code=500, detail="Ошибка сервера: База данных недоступна")
        
        # Получаем все VK аккаунты
        async with pool.acquire() as conn:
            accounts = await conn.fetch("SELECT id, token, user_api_key FROM vk_accounts")
        
        results = []
        for account in accounts:
            account_id = account['id']
            token = account['token']
            user_api_key = account['user_api_key']
            
            result = {
                "account_id": account_id,
                "user_api_key": user_api_key,
                "token_exists": token is not None,
                "token_length": len(token) if token else 0,
                "token_start": token[:10] + "..." if token and len(token) > 10 else token,
                "decryption_success": False,
                "decrypted_token_valid": False,
                "error": None
            }
            
            if token:
                try:
                    # Пытаемся расшифровать токен
                    decrypted_token = cipher.decrypt(token.encode()).decode()
                    result["decryption_success"] = True
                    
                    # Проверяем валидность расшифрованного токена
                    if decrypted_token.startswith('vk1.a.'):
                        result["decrypted_token_valid"] = True
                        result["decrypted_token_start"] = decrypted_token[:10] + "..."
                    else:
                        result["decrypted_token_start"] = decrypted_token[:10] + "..."
                except Exception as e:
                    result["error"] = str(e)
            
            results.append(result)
        
        # Проверяем настройки шифрования
        from user_manager import ENCRYPTION_KEY
        encryption_key_info = {
            "length": len(ENCRYPTION_KEY),
            "start": ENCRYPTION_KEY[:5].decode() + "..." if len(ENCRYPTION_KEY) > 5 else ENCRYPTION_KEY.decode()
        }
        
        return {
            "accounts_count": len(results),
            "accounts": results,
            "encryption_key_info": encryption_key_info
        }
    except asyncpg.PostgresError as db_err:
        logger.error(f"Ошибка PostgreSQL при тестировании токенов VK: {db_err}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка базы данных: {str(db_err)}")
    except Exception as e:
        import traceback
        logger.error(f"Ошибка при тестировании токенов VK: {str(e)}")
        logger.error(f"Трассировка: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Ошибка при тестировании токенов VK: {str(e)}")

@app.post("/bulk-posts")
async def bulk_posts(request: Request, data: dict):
    """Получение постов из групп по нескольким ключевым словам с возвратом сгруппированных результатов."""
    api_key = request.headers.get('api-key') or request.headers.get('x-api-key')
    if not api_key:
        auth_header = request.headers.get('authorization')
        if auth_header and auth_header.startswith('Bearer '):
            api_key = auth_header.split(' ')[1]
        else:
            raise HTTPException(401, "API ключ обязателен")
    
    if not await verify_api_key(api_key):
        raise HTTPException(401, "Неверный API ключ")
        
    # Поддержка обоих форматов (JS и Python)
    group_keywords = data.get('group_keywords', data.get('groupKeywords', []))
    if not group_keywords or not isinstance(group_keywords, list):
        raise HTTPException(400, "Ключевые слова для групп должны быть массивом (group_keywords или groupKeywords)")
        
    search_keywords = data.get('search_keywords', data.get('searchKeywords', None))
    count = data.get('count', 10)
    min_views = data.get('min_views', data.get('minViews', 1000))
    days_back = data.get('days_back', data.get('daysBack', 7))
    max_groups = data.get('max_groups', data.get('maxGroups', 10))
    max_posts_per_group = data.get('max_posts_per_group', data.get('maxPostsPerGroup', 300))
    group_ids = data.get('group_ids', data.get('groupIds', None))
    
    logger.info(f"Получение постов для нескольких ключевых слов: {group_keywords}, search_keywords={search_keywords}")
    
    # Получаем VK клиент
    vk = await auth_middleware(request, 'vk')
    if not vk:
        raise HTTPException(500, "Не удалось получить VK клиент")
        
    from vk_utils import find_vk_groups, get_vk_posts, get_vk_posts_in_groups
    
    try:
        result = {}
        
        # Если переданы идентификаторы групп, то используем их
        if group_ids and isinstance(group_ids, list) and len(group_ids) > 0:
            # Форматируем ID групп
            formatted_group_ids = []
            for gid in group_ids:
                gid_str = str(gid)
                if not gid_str.startswith('-'):
                    gid_str = f"-{gid_str}"
                formatted_group_ids.append(gid_str)
                
            # Получаем посты из указанных групп
            posts = await get_vk_posts_in_groups(
                vk, 
                formatted_group_ids, 
                search_keywords, 
                count, 
                min_views, 
                days_back, 
                max_posts_per_group
            )
            
            # Используем первое ключевое слово как ключ
            key = group_keywords[0] if group_keywords else "posts"
            result[key] = posts
        else:
            # Для каждого ключевого слова получаем посты
            for keyword in group_keywords:
                # Получаем посты для текущего ключевого слова
                posts = await get_vk_posts(
                    vk, 
                    [keyword], 
                    search_keywords, 
                    count, 
                    min_views, 
                    days_back, 
                    max_groups, 
                    max_posts_per_group
                )
                
                # Добавляем результат в словарь с ключом = ключевому слову
                result[keyword] = posts
        
        return result
    
    except Exception as e:
        import traceback
        logger.error(f"Ошибка при получении постов по нескольким ключевым словам: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(500, f"Ошибка при получении постов по нескольким ключевым словам: {str(e)}")

@app.post("/api/admin/check-proxy")
async def check_proxy(request: Request):
    """Проверяет валидность прокси для указанного аккаунта."""
    admin_key = request.headers.get("X-Admin-Key")
    if not admin_key or not await verify_admin_key(admin_key):
        raise HTTPException(status_code=401, detail="Неверный admin ключ")
    
    try:
        data = await request.json()
        platform = data.get("platform")
        account_id = data.get("account_id")
        
        if not platform or not account_id:
            raise HTTPException(status_code=400, detail="Требуются platform и account_id")
        
        from user_manager import get_db_pool
        pool = await get_db_pool()
        
        if platform == "telegram":
            # Получаем данные аккаунта Telegram
            async with pool.acquire() as conn:
                row = await conn.fetchrow("SELECT * FROM telegram_accounts WHERE id = $1", account_id)
                
                if not row:
                    raise HTTPException(status_code=404, detail="Аккаунт не найден")
                
                # Преобразуем в словарь
                account_dict = dict(row)
                
                # Проверяем наличие прокси
                proxy = account_dict.get("proxy")
                if not proxy:
                    return {"valid": False, "message": "Прокси не указан для этого аккаунта"}
                
                # Валидируем прокси
                from telegram_utils import validate_proxy
                is_valid, proxy_type = validate_proxy(proxy)
                
                if not is_valid:
                    return {"valid": False, "message": "Неверный формат прокси"}
                
                # Попытка подключения с прокси
                try:
                    from telegram_utils import create_telegram_client
                    client = await create_telegram_client(
                        session_path=f"check_proxy_{account_dict['id']}",
                        api_id=int(account_dict['api_id']),
                        api_hash=account_dict['api_hash'],
                        proxy=proxy
                    )
                    
                    # Пробуем подключиться
                    await client.connect()
                    is_connected = client.is_connected()  # Не используем await, так как метод не корутина
                    if is_connected:
                        client.disconnect()  # Не используем await, так как метод не корутина
                        return {"valid": True, "message": f"Успешное подключение через {proxy_type} прокси"}
                    else:
                        return {"valid": False, "message": "Не удалось подключиться через прокси"}
                except Exception as e:
                    return {"valid": False, "message": f"Ошибка подключения: {str(e)}"}
                    
        elif platform == "vk":
            # Получаем данные аккаунта VK
            async with pool.acquire() as conn:
                row = await conn.fetchrow("SELECT * FROM vk_accounts WHERE id = $1", account_id)
                
                if not row:
                    raise HTTPException(status_code=404, detail="Аккаунт не найден")
                
                # Преобразуем в словарь
                account_dict = dict(row)
                
                # Проверяем наличие прокси
                proxy = account_dict.get("proxy")
                if not proxy:
                    return {"valid": False, "message": "Прокси не указан для этого аккаунта"}
                
                # Валидируем прокси
                from vk_utils import validate_proxy, validate_proxy_connection
                
                # Сначала проверяем формат
                is_valid = validate_proxy(proxy)
                if not is_valid:
                    return {"valid": False, "message": "Неверный формат прокси"}
            
            # Затем проверяем соединение
            is_valid, message = await validate_proxy_connection(proxy)
            return {"valid": is_valid, "message": message}
        else:
            raise HTTPException(status_code=400, detail="Неизвестная платформа")
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Некорректный формат JSON")
    except asyncpg.PostgresError as e:
        logger.error(f"Ошибка PostgreSQL при проверке прокси: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка базы данных: {str(e)}")
    except Exception as e:
        logger.error(f"Ошибка при проверке прокси: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Внутренняя ошибка: {str(e)}")


@app.post("/api/admin/update-proxy")
async def update_proxy(request: Request):
    """Обновляет прокси для указанного аккаунта."""
    auth_header = request.headers.get('authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        raise HTTPException(401, "API ключ обязателен")
    
    admin_key = auth_header.split(' ')[1]
    
    # Проверяем админ-ключ
    if not await verify_admin_key(admin_key):
        raise HTTPException(401, "Неверный админ-ключ")
    
    try:
        data = await request.json()
        platform = data.get("platform")
        account_id = data.get("account_id")
        user_id = data.get("user_id")
        proxy = data.get("proxy")
        
        if not all([platform, account_id, user_id]):
            raise HTTPException(status_code=400, detail="Требуются platform, account_id и user_id")
        
        from user_manager import get_db_pool, update_telegram_account, update_vk_account
        
        pool = await get_db_pool()
        
        if platform == "telegram":
            async with pool.acquire() as conn:
                async with conn.transaction():
                    row = await conn.fetchrow('SELECT * FROM telegram_accounts WHERE id = $1', account_id)
                    
                    if not row:
                        raise HTTPException(status_code=404, detail="Аккаунт не найден")
                    
                    if proxy is None:
                        account_data = {"proxy": ""}
                        update_result = await update_telegram_account(user_id, account_id, account_data)
                        
                        if not update_result:
                            raise HTTPException(status_code=404, detail="Не удалось обновить аккаунт")
                        
                        return {"success": True, "message": "Прокси успешно удален"}
                    
                    from telegram_utils import validate_proxy
                    is_valid, _ = validate_proxy(proxy)
                    
                    if not is_valid:
                        raise HTTPException(status_code=400, detail="Неверный формат прокси")
                    
                    account_data = {"proxy": proxy}
                    update_result = await update_telegram_account(user_id, account_id, account_data)
                    
                    if not update_result:
                        raise HTTPException(status_code=404, detail="Не удалось обновить аккаунт")
                    
                    return {"success": True, "message": "Прокси успешно обновлен"}
            
        elif platform == "vk":
            async with pool.acquire() as conn:
                async with conn.transaction():
                    row = await conn.fetchrow('SELECT * FROM vk_accounts WHERE id = $1', account_id)
                    
                    if not row:
                        raise HTTPException(status_code=404, detail="Аккаунт не найден")
                    
                    if proxy is None:
                        account_data = {"proxy": ""}
                        update_result = await update_vk_account(user_id, account_id, account_data)
                        
                        if not update_result:
                            raise HTTPException(status_code=404, detail="Не удалось обновить аккаунт")
                        
                        return {"success": True, "message": "Прокси успешно удален"}
                    
                    from vk_utils import validate_proxy
                    is_valid = validate_proxy(proxy)
                    
                    if not is_valid:
                        raise HTTPException(status_code=400, detail="Неверный формат прокси")
                    
                    account_data = {"proxy": proxy}
                    update_result = await update_vk_account(user_id, account_id, account_data)
                    
                    if not update_result:
                        raise HTTPException(status_code=404, detail="Не удалось обновить аккаунт")
                    
                    return {"success": True, "message": "Прокси успешно обновлен"}
        
        else:
            raise HTTPException(status_code=400, detail="Неизвестная платформа")
            
    except json.JSONDecodeError:
        logger.error("Ошибка декодирования JSON при обновлении прокси")
        raise HTTPException(status_code=400, detail="Некорректный формат JSON")
    except asyncpg.PostgresError as db_err:
        logger.error(f"Ошибка PostgreSQL при обновлении прокси: {db_err}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка базы данных: {str(db_err)}")
    except Exception as e:
        logger.error(f"Ошибка при обновлении прокси: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Внутренняя ошибка: {str(e)}")

# Добавляем эндпоинт для ручного сброса и проверки статистики аккаунтов (только для админов)
@app.post("/admin/accounts/reset-stats")
async def reset_accounts_stats(request: Request):
    """Ручной сброс статистики и режима пониженной производительности для всех аккаунтов."""
    # Проверяем админ-ключ
    admin_key = request.headers.get("X-Admin-Key")
    if not admin_key or not await verify_admin_key(admin_key):
        raise HTTPException(status_code=401, detail="Неверный admin ключ")
    
    try:
        reset_counts = 0
        reset_degraded = 0
        
        # Сбрасываем статистику для VK аккаунтов в памяти
        if vk_pool:
            for account_id in list(vk_pool.usage_counts.keys()):
                if vk_pool.usage_counts.get(account_id, 0) > 0:
                    vk_pool.usage_counts[account_id] = 0
                    reset_counts += 1
                
                client = vk_pool.get_client(account_id)
                if client and hasattr(client, 'set_degraded_mode'):
                    client.set_degraded_mode(False)
                    reset_degraded += 1
        
        # Сбрасываем статистику для Telegram аккаунтов в памяти
        if telegram_pool:
            for account_id in list(telegram_pool.usage_counts.keys()):
                if telegram_pool.usage_counts.get(account_id, 0) > 0:
                    telegram_pool.usage_counts[account_id] = 0
                    reset_counts += 1
                
                client = telegram_pool.get_client(account_id)
                if client and hasattr(client, 'set_degraded_mode'):
                    client.set_degraded_mode(False)
                    reset_degraded += 1
        
        # Сбрасываем статистику в Redis и синхронизируем с базой данных
        await reset_all_account_stats()
        
        # Логируем результаты
        logger.info(f"Сброшена статистика для всех аккаунтов")
        logger.info(f"Отключен режим пониженной производительности для {reset_degraded} аккаунтов")
        
        return {
            "status": "success",
            "reset_count": reset_counts,
            "reset_degraded": reset_degraded,
            "message": f"Сброшена статистика для всех аккаунтов, отключен режим пониженной производительности для {reset_degraded} аккаунтов"
        }
    except Exception as e:
        logger.error(f"Ошибка при сбросе статистики аккаунтов: {e}")
        raise HTTPException(500, f"Ошибка при сбросе статистики: {str(e)}")

@app.get("/health")
async def health_check():
    """Проверка работоспособности сервиса."""
    db_status = "error"
    redis_status = "error"
    
    try:
        # Проверка подключения к базе данных PostgreSQL
        from user_manager import get_db_pool
        pool = await get_db_pool()
        
        async with pool.acquire() as conn:
            result = await conn.fetchval("SELECT 1")
            if result == 1:
                db_status = "ok"
        
        # Проверка Redis, если доступен
        if redis_client:
            if redis_client.ping():
                redis_status = "ok"
        
        # Если база данных недоступна, возвращаем ошибку
        if db_status != "ok":
            logger.error("Ошибка подключения к базе данных")
            raise HTTPException(status_code=500, detail="Ошибка подключения к базе данных")
        
        return {
            "status": "ok", 
            "components": {
                "database": db_status,
                "redis": redis_status
            },
            "timestamp": datetime.now().isoformat()
        }
    except asyncpg.PostgresError as db_err:
        logger.error(f"Ошибка PostgreSQL: {db_err}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка базы данных: {str(db_err)}")
    except Exception as e:
        logger.error(f"Ошибка при проверке работоспособности: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка при проверке работоспособности: {str(e)}")


@app.post("/api/admin/fix-vk-tokens")
async def fix_vk_tokens_endpoint(request: Request):
    """Запускает процедуру исправления токенов VK."""
    admin_key = request.headers.get("X-Admin-Key")
    if not admin_key:
        admin_key = request.cookies.get("admin_key")
    
    if not admin_key or not await verify_admin_key(admin_key):
        raise HTTPException(status_code=401, detail="Неверный admin ключ")
    
    try:
        from user_manager import fix_vk_tokens, get_db_pool
        
        # Получаем пул соединений для передачи в функцию
        pool = await get_db_pool()
        if not pool:
            logger.error("Не удалось получить пул соединений к БД")
            raise HTTPException(status_code=500, detail="Ошибка сервера: База данных недоступна")
        
        # Вызываем асинхронную функцию для исправления токенов
        fixed_count = await fix_vk_tokens()
        
        # Логируем результат
        logger.info(f"Исправлено токенов VK: {fixed_count}")
        
        return {
            "status": "success", 
            "fixed_count": fixed_count,
            "timestamp": datetime.now().isoformat()
        }
    except asyncpg.PostgresError as db_err:
        logger.error(f"Ошибка PostgreSQL при исправлении токенов VK: {db_err}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка базы данных: {str(db_err)}")
    except Exception as e:
        logger.error(f"Ошибка при исправлении токенов VK: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Внутренняя ошибка: {str(e)}")

@app.post("/api/admin/fix-vk-token/{account_id}")
async def fix_single_vk_token_endpoint(account_id: str, request: Request):
    """Исправляет токен для конкретного VK аккаунта."""
    admin_key = request.headers.get("X-Admin-Key")
    if not admin_key:
        admin_key = request.cookies.get("admin_key")
    
    if not admin_key or not await verify_admin_key(admin_key):
        raise HTTPException(status_code=401, detail="Неверный admin ключ")
    
    # Получаем токен из базы данных
    from user_manager import get_db_pool, cipher
    from vk_utils import VKClient
    
    try:
        pool = await get_db_pool()
        if not pool:
            logger.error("Не удалось получить пул соединений к БД")
            raise HTTPException(status_code=500, detail="Ошибка сервера: База данных недоступна")
        
        async with pool.acquire() as conn:
            query = 'SELECT token, proxy FROM vk_accounts WHERE id = $1'
            account_row = await conn.fetchrow(query, account_id)
            
            if not account_row:
                raise HTTPException(status_code=404, detail="Аккаунт не найден")
            
            # Преобразуем объект Record в словарь
            account = dict(account_row)
            
            encrypted_token = account["token"]
            proxy = account["proxy"]
            fixed = False
            
            # Проверяем, зашифрован ли токен
            if encrypted_token.startswith('vk1.a.'):
                logger.info(f"Токен для аккаунта {account_id} уже в правильном формате (не зашифрован)")
                
                # Проверяем, работает ли токен через API
                try:
                    async with VKClient(encrypted_token, proxy) as vk:
                        result = await vk._make_request("users.get", {"fields": "photo_50,screen_name"})
                        if "response" in result:
                            logger.info(f"Токен для аккаунта {account_id} валиден и работает")
                            return {"success": True, "message": "Токен валиден и работает"}
                        else:
                            logger.warning(f"Токен для аккаунта {account_id} имеет правильный формат, но не работает в API")
                            return {"success": False, "message": "Формат токена корректен, но API-валидация не пройдена"}
                except Exception as e:
                    logger.error(f"Ошибка при проверке токена через API: {str(e)}")
                    return {"success": False, "message": f"Ошибка во время API-валидации: {str(e)}"}
            
            try:
                # Пытаемся расшифровать токен
                decrypted_once = cipher.decrypt(encrypted_token.encode()).decode()
                
                # Проверяем, выглядит ли он как валидный токен VK
                if decrypted_once.startswith('vk1.a.'):
                    # Токен был зашифрован один раз, проверяем его через API
                    logger.info(f"Токен для аккаунта {account_id} был зашифрован один раз, проверяем через API")
                    
                    try:
                        async with VKClient(decrypted_once, proxy) as vk:
                            result = await vk._make_request("users.get", {"fields": "photo_50,screen_name"})
                            if "response" in result:
                                logger.info(f"Токен для аккаунта {account_id} валиден и работает")
                                # Обновляем токен в базе данных, возвращая его к незашифрованному состоянию
                                update_query = 'UPDATE vk_accounts SET token = $1 WHERE id = $2'
                                await conn.execute(update_query, decrypted_once, account_id)
                                fixed = True
                            else:
                                logger.warning(f"Расшифрованный токен для аккаунта {account_id} имеет правильный формат, но не работает в API")
                                # Не меняем токен, так как он не работает
                    except Exception as e:
                        logger.error(f"Ошибка при проверке расшифрованного токена через API: {str(e)}")
                        # Продолжаем попытки исправления несмотря на ошибку API
                
                # Если токен не был исправлен, пробуем расшифровать его еще раз
                if not fixed:
                    try:
                        decrypted_twice = cipher.decrypt(decrypted_once.encode()).decode()
                        
                        # Проверяем, выглядит ли двойной расшифрованный токен как валидный
                        if decrypted_twice.startswith('vk1.a.'):
                            # Токен был зашифрован дважды, проверяем его через API
                            logger.info(f"Токен для аккаунта {account_id} был зашифрован дважды, проверяем через API")
                            
                            try:
                                async with VKClient(decrypted_twice, proxy) as vk:
                                    result = await vk._make_request("users.get", {"fields": "photo_50,screen_name"})
                                    if "response" in result:
                                        logger.info(f"Дважды расшифрованный токен для аккаунта {account_id} валиден и работает")
                                        # Обновляем токен в базе - используем незашифрованный токен
                                        update_query = 'UPDATE vk_accounts SET token = $1 WHERE id = $2'
                                        await conn.execute(update_query, decrypted_twice, account_id)
                                        fixed = True
                                    else:
                                        logger.warning(f"Дважды расшифрованный токен имеет правильный формат, но не работает в API")
                                        # Несмотря на ошибку API, обновляем токен если формат правильный
                                        update_query = 'UPDATE vk_accounts SET token = $1 WHERE id = $2'
                                        await conn.execute(update_query, decrypted_twice, account_id)
                                        fixed = True
                            except Exception as e:
                                logger.error(f"Ошибка при проверке дважды расшифрованного токена через API: {str(e)}")
                                # Обновляем токен несмотря на ошибку API, если формат правильный
                                update_query = 'UPDATE vk_accounts SET token = $1 WHERE id = $2'
                                await conn.execute(update_query, decrypted_twice, account_id)
                                fixed = True
                        else:
                            # Пробуем найти подстроку 'vk1.a.' в дважды расшифрованном токене
                            if len(decrypted_twice) > 30 and 'vk1.a.' in decrypted_twice:
                                start_pos = decrypted_twice.find('vk1.a.')
                                if start_pos >= 0:
                                    token_part = decrypted_twice[start_pos:]
                                    if len(token_part) > 30:
                                        logger.info(f"Извлечение токена из строки для аккаунта {account_id}")
                                        
                                        # Проверяем извлеченный токен через API
                                        try:
                                            async with VKClient(token_part, proxy) as vk:
                                                result = await vk._make_request("users.get", {"fields": "photo_50,screen_name"})
                                                if "response" in result:
                                                    logger.info(f"Извлеченный токен для аккаунта {account_id} валиден и работает")
                                                    # Обновляем токен в базе
                                                    update_query = 'UPDATE vk_accounts SET token = $1 WHERE id = $2'
                                                    await conn.execute(update_query, token_part, account_id)
                                                    fixed = True
                                                else:
                                                    logger.warning(f"Извлеченный токен имеет правильный формат, но не работает в API")
                                                    # Обновляем токен несмотря на ошибку API
                                                    update_query = 'UPDATE vk_accounts SET token = $1 WHERE id = $2'
                                                    await conn.execute(update_query, token_part, account_id)
                                                    fixed = True
                                        except Exception as e:
                                            logger.error(f"Ошибка при проверке извлеченного токена через API: {str(e)}")
                                            # Обновляем токен несмотря на ошибку API
                                            update_query = 'UPDATE vk_accounts SET token = $1 WHERE id = $2'
                                            await conn.execute(update_query, token_part, account_id)
                                            fixed = True
                    except Exception as inner_e:
                        logger.error(f"Ошибка при второй расшифровке токена для аккаунта {account_id}: {str(inner_e)}")
                
                if fixed:
                    return {"success": True, "message": "Токен был исправлен"}
                else:
                    return {"success": False, "message": "Токен не удалось исправить, формат недействителен"}
            except Exception as e:
                logger.error(f"Ошибка при первой расшифровке токена для аккаунта {account_id}: {str(e)}")
                return {"success": False, "message": f"Ошибка во время расшифровки: {str(e)}"}
                
    except asyncpg.PostgresError as db_err:
        logger.error(f"Ошибка PostgreSQL при исправлении токена VK: {db_err}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка базы данных: {str(db_err)}")
    except Exception as e:
        logger.error(f"Непредвиденная ошибка при исправлении токена VK: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Внутренняя ошибка: {str(e)}")

@app.post("/api/admin/normalize-vk-tokens")
async def normalize_vk_tokens_endpoint(request: Request):
    """Преобразует все токены VK в незашифрованный вид для хранения в базе данных."""
    admin_key = request.headers.get("X-Admin-Key")
    if not admin_key:
        admin_key = request.cookies.get("admin_key")
    
    if not admin_key or not await verify_admin_key(admin_key):
        raise HTTPException(status_code=401, detail="Invalid admin key")
    
    from user_manager import get_db_pool, cipher
    
    pool = await get_db_pool()
    
    async with pool.acquire() as conn:
        async with conn.transaction():
            # Получаем все токены VK
            query = 'SELECT id, token FROM vk_accounts'
            rows = await conn.fetch(query)
            accounts = [dict(row) for row in rows]
            
            normalized_count = 0
            skipped_count = 0
            error_count = 0
            
            for account in accounts:
                account_id = account['id']
                token_value = account['token']
                
                try:
                    # Если токен уже в правильном формате, пропускаем
                    if not token_value:
                        logger.warning(f"Пустой токен для аккаунта {account_id}")
                        skipped_count += 1
                        continue
                        
                    if token_value.startswith('vk1.a.'):
                        logger.info(f"Токен для аккаунта {account_id} уже в правильном формате")
                        skipped_count += 1
                        continue
                    
                    # Пытаемся расшифровать токен
                    try:
                        decrypted_token = cipher.decrypt(token_value.encode()).decode()
                        
                        # Проверяем формат расшифрованного токена
                        if decrypted_token.startswith('vk1.a.'):
                            # Обновляем токен в базе данных
                            update_query = 'UPDATE vk_accounts SET token = $1 WHERE id = $2'
                            await conn.execute(update_query, decrypted_token, account_id)
                            normalized_count += 1
                            logger.info(f"Токен для аккаунта {account_id} успешно нормализован")
                        else:
                            # Пробуем расшифровать второй раз (токен мог быть зашифрован дважды)
                            try:
                                decrypted_twice = cipher.decrypt(decrypted_token.encode()).decode()
                                
                                if decrypted_twice.startswith('vk1.a.'):
                                    # Обновляем токен в базе данных
                                    update_query = 'UPDATE vk_accounts SET token = $1 WHERE id = $2'
                                    await conn.execute(update_query, decrypted_twice, account_id)
                                    normalized_count += 1
                                    logger.info(f"Токен для аккаунта {account_id} был расшифрован дважды и успешно нормализован")
                                else:
                                    # Пробуем найти подстроку 'vk1.a.' в дважды расшифрованном токене
                                    if 'vk1.a.' in decrypted_twice:
                                        start_pos = decrypted_twice.find('vk1.a.')
                                        token_part = decrypted_twice[start_pos:]
                                        
                                        # Обновляем токен в базе данных
                                        update_query = 'UPDATE vk_accounts SET token = $1 WHERE id = $2'
                                        await conn.execute(update_query, token_part, account_id)
                                        normalized_count += 1
                                        logger.info(f"Токен для аккаунта {account_id} был извлечен из строки и успешно нормализован")
                                    else:
                                        logger.warning(f"Не удалось нормализовать токен для аккаунта {account_id}: токен не имеет правильного формата")
                                        error_count += 1
                            except Exception as e:
                                logger.error(f"Ошибка при второй расшифровке токена для аккаунта {account_id}: {str(e)}")
                                error_count += 1
                    except Exception as e:
                        logger.error(f"Ошибка при расшифровке токена для аккаунта {account_id}: {str(e)}")
                        error_count += 1
                except Exception as e:
                    logger.error(f"Ошибка при обработке аккаунта {account_id}: {str(e)}")
                    error_count += 1
    
    return {
        "success": True,
        "normalized_count": normalized_count,
        "skipped_count": skipped_count,
        "error_count": error_count,
        "total_accounts": len(accounts)
    }

@app.post("/api/check-proxy")
async def check_proxy_endpoint(request: Request):
    """Проверяет валидность и работоспособность прокси для указанной платформы."""
    auth_header = request.headers.get('authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        raise HTTPException(401, "API ключ обязателен")
    admin_key = auth_header.split(' ')[1]
    
    # Проверяем админ-ключ
    if not await verify_admin_key(admin_key):
        raise HTTPException(401, "Неверный админ-ключ")
    
    data = await request.json()
    proxy = data.get('proxy')
    platform = data.get('platform')
    
    if not proxy:
        raise HTTPException(400, "Прокси не указан")
    
    if not platform or platform not in ['telegram', 'vk']:
        raise HTTPException(400, "Указана неверная платформа")
    
    if platform == "telegram":
        from telegram_utils import validate_proxy
        is_valid, proxy_type = validate_proxy(proxy)
        
        if not is_valid:
            return {"valid": False, "message": "Неверный формат прокси"}
        
        # Проверяем соединение с сервером Telegram через прокси
        try:
            import aiohttp
            from aiohttp_socks import ProxyConnector
            
            if proxy_type == 'socks5':
                connector = ProxyConnector.from_url(proxy)
                async with aiohttp.ClientSession(connector=connector) as session:
                    async with session.get('https://api.telegram.org', timeout=10) as response:
                        if response.status == 200:
                            return {"valid": True, "message": "Прокси подключен успешно"}
                        else:
                            return {"valid": False, "message": f"Ошибка подключения: HTTP {response.status}"}
            else:
                # Для HTTP/HTTPS прокси
                async with aiohttp.ClientSession() as session:
                    async with session.get('https://api.telegram.org', proxy=proxy, timeout=10) as response:
                        if response.status == 200:
                            return {"valid": True, "message": "Прокси подключен успешно"}
                        else:
                            return {"valid": False, "message": f"Ошибка подключения: HTTP {response.status}"}
        except Exception as e:
            return {"valid": False, "message": f"Ошибка проверки прокси: {str(e)}"}
    
    elif platform == "vk":
        from vk_utils import validate_proxy
        is_valid = validate_proxy(proxy)
        
        if not is_valid:
            return {"valid": False, "message": "Неверный формат прокси"}
        
        # Проверяем соединение с сервером VK через прокси
        try:
            import aiohttp
            async with aiohttp.ClientSession() as session:
                async with session.get("https://api.vk.com/method/users.get", 
                                       params={"v": "5.131"}, 
                                       proxy=proxy, 
                                       timeout=10) as response:
                    if response.status == 200:
                        return {"valid": True, "message": "Прокси подключен успешно"}
                    else:
                        return {"valid": False, "message": f"Ошибка запроса: HTTP {response.status}"}
        except Exception as e:
            return {"valid": False, "message": f"Ошибка подключения: {str(e)}"}

@app.post("/api/v2/check-proxy")
async def check_proxy_endpoint_v2(request: Request):
    """Новая версия проверки валидности и работоспособности прокси для указанной платформы."""
    auth_header = request.headers.get('authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        raise HTTPException(401, "API ключ обязателен")
    admin_key = auth_header.split(' ')[1]
    
    # Проверяем админ-ключ
    if not await verify_admin_key(admin_key):
        raise HTTPException(401, "Неверный админ-ключ")
    
    data = await request.json()
    proxy = data.get('proxy')
    platform = data.get('platform')
    
    if not proxy:
        raise HTTPException(400, "Прокси не указан")
    
    if not platform or platform not in ['telegram', 'vk']:
        raise HTTPException(400, "Неверная платформа. Допустимые значения: 'telegram', 'vk'")
    
    if platform == "telegram":
        # Проверяем прокси для Telegram
        from telegram_utils import validate_proxy, validate_proxy_connection
        
        # Сначала проверяем формат прокси
        is_valid, proxy_type = validate_proxy(proxy)
        if not is_valid:
            return {"valid": False, "message": "Неверный формат прокси"}
        
        # Затем проверяем соединение
        is_valid, message = await validate_proxy_connection(proxy)
        return {"valid": is_valid, "message": message}
    
    elif platform == "vk":
        from vk_utils import validate_proxy, validate_proxy_connection
        
        # Сначала проверяем формат
        is_valid = validate_proxy(proxy)
        if not is_valid:
            return {"valid": False, "message": "Неверный формат прокси"}
        
        # Затем проверяем соединение
        is_valid, message = await validate_proxy_connection(proxy)
        return {"valid": is_valid, "message": message}

@app.get("/api/telegram/accounts/{account_id}/details")
async def get_telegram_account_details(request: Request, account_id: str):
    """Получает детали аккаунта Telegram."""
    auth_header = request.headers.get('authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        raise HTTPException(401, "API ключ обязателен")
    admin_key = auth_header.split(' ')[1]
    
    # Проверяем админ-ключ
    if not await verify_admin_key(admin_key):
        raise HTTPException(401, "Неверный админ-ключ")
    
    from user_manager import get_db_pool
    
    try:
        # Используем пул соединений
        pool = await get_db_pool()
        
        async with pool.acquire() as conn:
            # Получаем данные аккаунта
            query = 'SELECT * FROM telegram_accounts WHERE id = $1'
            account_record = await conn.fetchrow(query, account_id)
            
            if not account_record:
                raise HTTPException(status_code=404, detail="Аккаунт не найден")
            
            # Преобразуем Record в словарь
            account_dict = dict(account_record)
            
            return account_dict
    except asyncpg.PostgresError as db_err:
        logger.error(f"Ошибка PostgreSQL при получении деталей аккаунта Telegram {account_id}: {db_err}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка базы данных: {str(db_err)}")
    except Exception as e:
        logger.error(f"Ошибка при получении деталей аккаунта Telegram {account_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")

@app.get("/api/vk/accounts/{account_id}/details")
async def get_vk_account_details(request: Request, account_id: str):
    """Получает детали аккаунта VK."""
    auth_header = request.headers.get('authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        raise HTTPException(401, "API ключ обязателен")
    admin_key = auth_header.split(' ')[1]
    
    # Проверяем админ-ключ
    if not await verify_admin_key(admin_key):
        raise HTTPException(401, "Неверный админ-ключ")
    
    from user_manager import get_db_pool
    
    try:
        # Используем пул соединений
        pool = await get_db_pool()
        
        async with pool.acquire() as conn:
            # Получаем данные аккаунта
            query = 'SELECT * FROM vk_accounts WHERE id = $1'
            account_record = await conn.fetchrow(query, account_id)
            
            if not account_record:
                raise HTTPException(status_code=404, detail="Аккаунт не найден")
            
            # Преобразуем Record в словарь
            account_dict = dict(account_record)
            
            return account_dict
    except asyncpg.PostgresError as db_err:
        logger.error(f"Ошибка PostgreSQL при получении деталей аккаунта VK {account_id}: {db_err}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка базы данных: {str(db_err)}")
    except Exception as e:
        logger.error(f"Ошибка при получении деталей аккаунта VK {account_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")

@app.post("/api/telegram/accounts/{account_id}/auth")
async def auth_telegram_code(request: Request, account_id: str):
    """Выполняет авторизацию Telegram аккаунта с использованием кода."""
    logger.info(f"Запрос на авторизацию аккаунта с ID {account_id}")
    admin_key = request.headers.get("X-Admin-Key")
    if not admin_key:
        auth_header = request.headers.get('authorization')
        if auth_header and auth_header.startswith('Bearer '):
            admin_key = auth_header.split(' ')[1]
    
    if not admin_key or not await verify_admin_key(admin_key):
        logger.error("Неверный админ-ключ")
        raise HTTPException(401, "Неверный админ-ключ")
    
    form_data = await request.form()
    code = form_data.get("code")
    
    if not code:
        logger.error("Код авторизации не указан")
        raise HTTPException(400, "Код авторизации обязателен")
    
    # Проверяем, что код состоит только из цифр
    if not isinstance(code, str) or not code.isdigit():
        logger.error(f"Некорректный формат кода: {code}")
        raise HTTPException(400, "Код должен состоять только из цифр")
    
    # Находим аккаунт по ID
    from user_manager import get_db_pool
    
    client = None
    
    try:
        pool = await get_db_pool()
        
        async with pool.acquire() as conn:
            async with conn.transaction():
                # Получаем данные аккаунта
                query = '''
                    SELECT * FROM telegram_accounts 
                    WHERE id = $1
                '''
                account_record = await conn.fetchrow(query, account_id)
                
                if not account_record:
                    logger.error(f"Аккаунт с ID {account_id} не найден")
                    raise HTTPException(404, "Аккаунт не найден")
                
                # Преобразуем Record в словарь
                account_dict = dict(account_record)
                
                session_file = account_dict["session_file"]
                api_id = int(account_dict["api_id"])
                api_hash = account_dict["api_hash"]
                phone = account_dict["phone"]
                phone_code_hash = account_dict.get("phone_code_hash")
                proxy = account_dict.get("proxy")
                
                if not phone_code_hash:
                    logger.error(f"Хеш кода не найден для аккаунта {account_id}")
                    raise HTTPException(400, "Хеш кода не найден, пожалуйста, запросите код заново")
                
                logger.info(f"Авторизация с кодом {code} для аккаунта {phone} (ID: {account_id})")
                
                # Создаем клиент Telegram
                logger.info(f"Создание клиента Telegram с сессией {session_file}")
                client = await telegram_utils.create_telegram_client(session_file, api_id, api_hash, proxy)
                
                # Подключаемся к серверам Telegram
                logger.info(f"Подключение клиента к серверам Telegram")
                await client.connect()
                
                # Проверяем, авторизован ли уже клиент
                is_authorized = await client.is_user_authorized()
                
                if is_authorized:
                    logger.info(f"Клиент уже авторизован для {phone}")
                    # Корректная обработка отключения клиента
                    try:
                        if asyncio.iscoroutinefunction(client.disconnect):
                            await client.disconnect()
                        else:
                            client.disconnect()
                        client = None
                    except Exception as e:
                        logger.warning(f"Ошибка при отключении клиента: {e}")
                    
                    # Обновляем статус аккаунта
                    update_query = '''
                        UPDATE telegram_accounts
                        SET status = $1
                        WHERE id = $2
                    '''
                    await conn.execute(update_query, 'active', account_id)
                    
                    return {
                        "account_id": account_id,
                        "status": "success",
                        "message": "Аккаунт уже авторизован"
                    }
                
                # Выполняем авторизацию
                logger.info(f"Выполнение авторизации для аккаунта {phone} с кодом {code}")
                try:
                    # Авторизуемся с помощью кода
                    await client.sign_in(phone, int(code), phone_code_hash=phone_code_hash)
                    
                    # Если авторизация прошла успешно, обновляем статус аккаунта
                    update_query = '''
                        UPDATE telegram_accounts
                        SET status = $1
                        WHERE id = $2
                    '''
                    await conn.execute(update_query, 'active', account_id)
                    
                    # Получаем информацию о пользователе для обновления данных аккаунта
                    me = await client.get_me()
                    if me:
                        username = getattr(me, 'username', None)
                        if username:
                            update_username_query = '''
                                UPDATE telegram_accounts
                                SET username = $1
                                WHERE id = $2
                            '''
                            await conn.execute(update_username_query, username, account_id)
                    
                    # Корректная обработка отключения клиента
                    try:
                        if asyncio.iscoroutinefunction(client.disconnect):
                            await client.disconnect()
                        else:
                            client.disconnect()
                        client = None
                    except Exception as e:
                        logger.warning(f"Ошибка при отключении клиента: {e}")
                        
                    logger.info(f"Авторизация успешно выполнена для аккаунта {phone}")
                    
                    return {
                        "account_id": account_id,
                        "status": "success",
                        "message": "Авторизация выполнена успешно"
                    }
                except SessionPasswordNeededError:
                    # Если для аккаунта настроена двухфакторная аутентификация
                    logger.info(f"Для аккаунта {phone} требуется пароль 2FA")
                    
                    # Корректная обработка отключения клиента
                    try:
                        if client is not None:
                            if asyncio.iscoroutinefunction(client.disconnect):
                                await client.disconnect()
                            else:
                                client.disconnect()
                            client = None
                    except Exception as e:
                        logger.warning(f"Ошибка при отключении клиента: {e}")
                    
                    # Обновляем статус аккаунта
                    update_query = '''
                        UPDATE telegram_accounts
                        SET status = $1
                        WHERE id = $2
                    '''
                    await conn.execute(update_query, 'pending', account_id)
                    
                    return {
                        "account_id": account_id,
                        "status": "pending",
                        "requires_2fa": True,
                        "message": "Требуется пароль двухфакторной аутентификации"
                    }
                except Exception as e:
                    logger.error(f"Ошибка при авторизации: {str(e)}")
                    # Анализируем сообщение об ошибке
                    error_msg = str(e).lower()
                    
                    # Формируем понятное пользователю сообщение об ошибке
                    user_message = "Ошибка при авторизации"
                    
                    if "invalid phone code" in error_msg:
                        user_message = "Неверный код авторизации. Пожалуйста, попробуйте снова или запросите новый код."
                    elif "phone code expired" in error_msg:
                        user_message = "Срок действия кода истек. Пожалуйста, запросите новый код."
                    elif "too many attempts" in error_msg:
                        user_message = "Слишком много попыток ввода кода. Пожалуйста, попробуйте позже."
                    elif "flood" in error_msg:
                        user_message = "Слишком много запросов. Пожалуйста, попробуйте позже."
                    else:
                        user_message = f"Ошибка при авторизации: {str(e)}"
                    
                    # Корректная обработка отключения клиента
                    try:
                        if client:
                            if asyncio.iscoroutinefunction(client.disconnect):
                                await client.disconnect()
                            else:
                                client.disconnect()
                            client = None
                    except Exception as disconnect_err:
                        logger.warning(f"Ошибка при отключении клиента: {disconnect_err}")
                    
                    raise HTTPException(400, user_message)
    except HTTPException:
        # Пробрасываем HTTPException дальше
        raise
    except asyncpg.PostgresError as db_err:
        logger.error(f"Ошибка PostgreSQL при авторизации Telegram-аккаунта {account_id}: {db_err}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка базы данных: {str(db_err)}")
    except Exception as e:
        logger.error(f"Ошибка при авторизации с кодом: {str(e)}", exc_info=True)
        raise HTTPException(500, f"Ошибка при авторизации: {str(e)}")

# === Эндпоинт для запроса кода авторизации для существующего аккаунта ===
@app.post("/api/telegram/accounts/{account_id}/request-code")
async def request_telegram_auth_code(request: Request, account_id: str):
    """Запрашивает новый код авторизации для существующего аккаунта Telegram."""
    from user_manager import get_db_pool
    from telegram_utils import create_telegram_client
    from telethon import errors
    
    logger.info(f"Запрос кода авторизации для аккаунта {account_id}")
    auth_header = request.headers.get('authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        logger.error("API ключ не предоставлен или в неверном формате для запроса кода")
        raise HTTPException(401, "API ключ обязателен")
    admin_key = auth_header.split(' ')[1]

    if not await verify_admin_key(admin_key):
        logger.error("Неверный админ-ключ для запроса кода")
        raise HTTPException(401, "Неверный админ-ключ")

    client = None
    
    try:
        pool = await get_db_pool()
        
        async with pool.acquire() as conn:
            # Получаем данные аккаунта
            query = 'SELECT phone, api_id, api_hash, session_file, proxy FROM telegram_accounts WHERE id = $1'
            account_record = await conn.fetchrow(query, account_id)
            
            if not account_record:
                logger.warning(f"Аккаунт {account_id} не найден при запросе кода")
                raise HTTPException(status_code=404, detail="Аккаунт не найден")
            
            # Извлекаем необходимые данные
            phone = account_record.get('phone')
            api_id_str = account_record.get('api_id')
            api_hash = account_record.get('api_hash')
            session_file = account_record.get('session_file')
            proxy = account_record.get('proxy')
            
            # Проверка на неполные данные
            if not all([phone, api_id_str, api_hash, session_file]):
                logger.error(f"Неполные данные аккаунта {account_id} для запроса кода")
                
                async with conn.transaction():
                    update_query = 'UPDATE telegram_accounts SET status = $1 WHERE id = $2 AND status != $3'
                    await conn.execute(update_query, 'error', account_id, 'error')
                
                raise HTTPException(status_code=400, detail="Неполные данные аккаунта для запроса кода")

            try:
                api_id = int(api_id_str)
            except (ValueError, TypeError):
                logger.error(f"Неверный формат api_id {api_id_str} для запроса кода аккаунта {account_id}")
                
                async with conn.transaction():
                    update_query = 'UPDATE telegram_accounts SET status = $1 WHERE id = $2'
                    await conn.execute(update_query, 'error', account_id)
                
                raise HTTPException(status_code=400, detail=f"Неверный формат API ID: {api_id_str}")

            try:
                logger.info(f"Создание клиента для запроса кода, сессия: {session_file}")
                client = await create_telegram_client(session_file, api_id, api_hash, proxy)

                logger.info(f"Подключение клиента для запроса кода для аккаунта {account_id}")
                await client.connect()

                # Если аккаунт уже авторизован, просто возвращаем успех
                if await client.is_user_authorized():
                    logger.info(f"Аккаунт {account_id} уже авторизован, обновляем статус на 'active'")
                    
                    async with conn.transaction():
                        update_query = 'UPDATE telegram_accounts SET status = $1 WHERE id = $2 AND status != $3'
                        await conn.execute(update_query, 'active', account_id, 'active')
                    
                    # Правильно отключаем клиент перед возвратом
                    if asyncio.iscoroutinefunction(client.disconnect):
                        await client.disconnect()
                    else:
                        client.disconnect()
                    client = None
                    
                    return {
                        "success": True,
                        "message": "Аккаунт уже авторизован",
                        "status": "active"
                    }

                logger.info(f"Отправка запроса кода для телефона {phone} (аккаунт {account_id})")
                sent_code_info = await client.send_code_request(phone)
                logger.info(f"Запрос кода для {account_id} отправлен успешно. Phone code hash: {sent_code_info.phone_code_hash}")

                # Сохраняем phone_code_hash в БД для использования при проверке кода
                async with conn.transaction():
                    update_query = "UPDATE telegram_accounts SET phone_code_hash = $1, status = $2 WHERE id = $3"
                    await conn.execute(update_query, sent_code_info.phone_code_hash, 'pending_code', account_id)

                # Добавляем лог с используемым номером
                return {
                    "success": True,
                    "message": "Код авторизации запрошен, пожалуйста, введите его",
                    "phone": phone,
                    "status": "pending_code"
                }
                
            except errors.PhoneNumberInvalidError:
                logger.error(f"Неверный номер телефона {phone} для аккаунта {account_id}")
                
                async with conn.transaction():
                    update_query = 'UPDATE telegram_accounts SET status = $1 WHERE id = $2'
                    await conn.execute(update_query, 'error', account_id)
                
                raise HTTPException(status_code=400, detail="Неверный номер телефона")
                
            except Exception as e:
                logger.error(f"Ошибка при запросе кода для аккаунта {account_id}: {e}", exc_info=True)
                
                async with conn.transaction():
                    update_query = 'UPDATE telegram_accounts SET status = $1 WHERE id = $2'
                    await conn.execute(update_query, 'error', account_id)
                
                raise HTTPException(status_code=500, detail=f"Ошибка при запросе кода: {str(e)}")
    
    except asyncpg.PostgresError as db_err:
        logger.error(f"Ошибка PostgreSQL при запросе кода для аккаунта {account_id}: {db_err}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка базы данных: {str(db_err)}")
    except HTTPException:
        raise  # Пробрасываем HTTPException дальше
    except Exception as e:
        logger.error(f"Ошибка при запросе кода для аккаунта {account_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Внутренняя ошибка: {str(e)}")
    finally:
        # Гарантируем закрытие клиента в случае ошибки
        if client:
            try:
                if hasattr(client, 'is_connected') and callable(client.is_connected):
                    is_connected = client.is_connected()
                    if is_connected:
                        if asyncio.iscoroutinefunction(client.disconnect):
                            await client.disconnect()
                        else:
                            client.disconnect()
                        logger.info(f"Клиент для запроса кода аккаунта {account_id} отключен.")
            except Exception as e:
                logger.warning(f"Ошибка при отключении клиента в блоке finally: {e}")


# === Эндпоинт для верификации кода и 2FA (модифицирован для использования phone_code_hash) ===
# @app.post("/api/telegram/verify-code")
# async def verify_telegram_code(request: Request):
#     data = await request.json()
#     account_id = data.get('account_id')
#     code = data.get('code')
#     password = data.get('password') # Для 2FA
#     logger.info(f"Запрос на верификацию кода для аккаунта {account_id}. Наличие пароля: {'Да' if password else 'Нет'}")
# 
#     auth_header = request.headers.get('authorization')
#     if not auth_header or not auth_header.startswith('Bearer '):
#         logger.error("API ключ не предоставлен для верификации кода")
#         raise HTTPException(401, "API ключ обязателен")
#     admin_key = auth_header.split(' ')[1]
# 
#     if not await verify_admin_key(admin_key):
#         logger.error("Неверный админ-ключ для верификации кода")
#         raise HTTPException(401, "Неверный админ-ключ")
# 
#     if not account_id or not code:
#         logger.error("account_id или code отсутствуют в запросе на верификацию")
#         raise HTTPException(400, "Необходимы account_id и code")
# 
#     conn = get_db_connection()
#     cursor = conn.cursor()
#     # Получаем все необходимые данные, включая phone_code_hash
#     cursor.execute('SELECT phone, api_id, api_hash, session_file, proxy, phone_code_hash FROM telegram_accounts WHERE id = ?',
#                    (account_id,))
#     account = cursor.fetchone()
#     # conn НЕ закрываем здесь, он понадобится для обновления статуса
# 
#     if not account:
#         logger.error(f"Аккаунт {account_id} не найден для верификации кода")
#         conn.close()
#         raise HTTPException(404, "Аккаунт не найден")
# 
#     account_dict = dict(account)
#     phone = account_dict.get("phone")
#     api_id_str = account_dict.get("api_id")
#     api_hash = account_dict.get("api_hash")
#     session_file = account_dict.get("session_file")
#     proxy = account_dict.get("proxy")
#     phone_code_hash = account_dict.get("phone_code_hash") # Получаем сохраненный хэш
# 
#     if not all([phone, api_id_str, api_hash, session_file]):
#         logger.error(f"Неполные данные для верификации кода аккаунта {account_id}")
#         conn.close()
#         raise HTTPException(400, "Неполные данные аккаунта для верификации")
# 
#     if not phone_code_hash:
#          logger.error(f"Отсутствует phone_code_hash для аккаунта {account_id}. Невозможно верифицировать код.")
#          conn.close()
#          raise HTTPException(400, "Сначала нужно запросить код авторизации (phone_code_hash отсутствует)")
# 
#     try:
#         api_id = int(api_id_str)
#     except (ValueError, TypeError):
#         logger.error(f"Неверный формат api_id {api_id_str} для верификации кода аккаунта {account_id}")
#         conn.close()
#         raise HTTPException(400, f"Неверный формат api_id: {api_id_str}")
# 
#     client = None
#     try:
#         logger.info(f"Создание клиента для верификации кода, сессия: {session_file}")
#         client = await telegram_utils.create_telegram_client(session_file, api_id, api_hash, proxy)
# 
#         logger.info(f"Подключение клиента для верификации кода аккаунта {account_id}")
#         await client.connect()
# 
#         user = None
#         if not await client.is_user_authorized():
#             try:
#                 logger.info(f"Попытка входа с кодом для аккаунта {account_id}")
#                 # Используем phone_code_hash из базы данных
#                 user = await client.sign_in(phone, code, phone_code_hash=phone_code_hash)
#                 logger.info(f"Вход с кодом для {account_id} успешен.")
#             except SessionPasswordNeededError:
#                 logger.info(f"Для аккаунта {account_id} требуется пароль 2FA")
#                 if not password:
#                     # Если пароль нужен, но не предоставлен, возвращаем специальный статус
#                     logger.warning(f"Пароль 2FA не предоставлен для {account_id}, но он требуется.")
#                     conn.close()
#                     # Обновляем статус, чтобы UI знал, что нужен пароль
#                     try:
#                         conn_update = get_db_connection()
#                         cursor_update = conn_update.cursor()
#                         cursor_update.execute('UPDATE telegram_accounts SET status = ? WHERE id = ?', ('pending_2fa', account_id))
#                         conn_update.commit()
#                         conn_update.close()
#                     except Exception as db_err:
#                         logger.error(f"Не удалось обновить статус на 'pending_2fa' для {account_id}: {db_err}")
# 
#                     return JSONResponse(status_code=401, content={"message": "Требуется пароль 2FA", "account_id": account_id, "status": "pending_2fa"})
#                 try:
#                     logger.info(f"Попытка входа с паролем 2FA для аккаунта {account_id}")
#                     user = await client.sign_in(password=password)
#                     logger.info(f"Вход с паролем 2FA для {account_id} успешен.")
#                 except PasswordHashInvalidError:
#                     logger.error(f"Неверный пароль 2FA для аккаунта {account_id}")
#                     conn.close()
#                     # Не меняем статус, чтобы можно было попробовать снова ввести пароль
#                     raise HTTPException(status_code=400, detail="Неверный пароль 2FA")
#                 except Exception as e_pwd:
#                     logger.error(f"Ошибка при входе с паролем 2FA для {account_id}: {str(e_pwd)}", exc_info=True)
#                     conn.close()
#                     # Статус 'error' при других ошибках пароля
#                     try:
#                         conn_update = get_db_connection()
#                         cursor_update = conn_update.cursor()
#                         cursor_update.execute('UPDATE telegram_accounts SET status = ? WHERE id = ?', ('error', account_id))
#                         conn_update.commit()
#                         conn_update.close()
#                     except Exception as db_err:
#                         logger.error(f"Не удалось обновить статус на 'error' после ошибки 2FA для {account_id}: {db_err}")
#                     raise HTTPException(status_code=500, detail=f"Ошибка при входе с паролем 2FA: {str(e_pwd)}")
# 
#             except PhoneCodeInvalidError as e_code:
#                  logger.error(f"Ошибка кода (PhoneCodeInvalidError) для аккаунта {account_id}: {str(e_code)}")
#                  conn.close()
#                  # Статус 'pending_code' - код неверный или истек, нужно запросить новый
#                  try:
#                      conn_update = get_db_connection()
#                      cursor_update = conn_update.cursor()
#                      cursor_update.execute('UPDATE telegram_accounts SET status = ?, phone_code_hash = NULL WHERE id = ?', ('pending_code', account_id))
#                      conn_update.commit()
#                      conn_update.close()
#                  except Exception as db_err:
#                      logger.error(f"Не удалось обновить статус на 'pending_code' после ошибки кода для {account_id}: {db_err}")
#                  raise HTTPException(status_code=400, detail=f"Ошибка кода: {str(e_code)}")
#             except PhoneCodeExpiredError as e_code:
#                  logger.error(f"Ошибка кода (PhoneCodeExpiredError) для аккаунта {account_id}: {str(e_code)}")
#                  conn.close()
#                  # Статус 'pending_code' - код неверный или истек, нужно запросить новый
#                  try:
#                      conn_update = get_db_connection()
#                      cursor_update = conn_update.cursor()
#                      cursor_update.execute('UPDATE telegram_accounts SET status = ?, phone_code_hash = NULL WHERE id = ?', ('pending_code', account_id))
#                      conn_update.commit()
#                      conn_update.close()
#                  except Exception as db_err:
#                      logger.error(f"Не удалось обновить статус на 'pending_code' после ошибки кода для {account_id}: {db_err}")
#                  raise HTTPException(status_code=400, detail=f"Ошибка кода: {str(e_code)}")
#             except FloodWaitError as e_flood:
#                  logger.error(f"Ошибка FloodWait при верификации кода для {account_id}: ждите {e_flood.seconds} секунд")
#                  conn.close()
#                  raise HTTPException(status_code=429, detail=f"Слишком много попыток. Попробуйте через {e_flood.seconds} секунд.")
#             except Exception as e_signin:
#                  logger.error(f"Непредвиденная ошибка при входе для аккаунта {account_id}: {str(e_signin)}", exc_info=True)
#                  conn.close()
#                  # Статус 'error' при других ошибках входа
#                  try:
#                      conn_update = get_db_connection()
#                      cursor_update = conn_update.cursor()
#                      cursor_update.execute('UPDATE telegram_accounts SET status = ? WHERE id = ?', ('error', account_id))
#                      conn_update.commit()
#                      conn_update.close()
#                  except Exception as db_err:
#                      logger.error(f"Не удалось обновить статус на 'error' после ошибки входа для {account_id}: {db_err}")
#                  raise HTTPException(status_code=500, detail=f"Внутренняя ошибка сервера при входе: {str(e_signin)}")
#         else:
#              logger.info(f"Аккаунт {account_id} уже авторизован при попытке верификации кода.")
#              user = await client.get_me()
# 
#         # Если мы дошли сюда, значит авторизация прошла успешно
#         logger.info(f"Аккаунт {account_id} успешно авторизован/верифицирован.")
#         # Обновляем статус на 'active' и очищаем phone_code_hash
#         cursor.execute("UPDATE telegram_accounts SET status = ?, phone_code_hash = NULL WHERE id = ?",
#                        ('active', account_id))
#         conn.commit()
#         conn.close()
# 
#         user_info = {
#             "id": user.id,
#             "username": user.username,
#             "first_name": user.first_name,
#             "last_name": user.last_name,
#             "phone": user.phone
#         } if user else {}
# 
#         return {"message": "Аккаунт успешно авторизован", "account_id": account_id, "status": "active", "user_info": user_info}
# 
#     except Exception as e:
#         logger.error(f"Непредвиденная ошибка в процессе верификации для {account_id}: {str(e)}", exc_info=True)
#         if conn: # Закрываем соединение, если оно еще открыто
#             conn.close()
#         # Статус 'error' при глобальных ошибках
#         try:
#             conn_update = get_db_connection()
#             cursor_update = conn_update.cursor()
#             cursor_update.execute('UPDATE telegram_accounts SET status = ? WHERE id = ?', ('error', account_id))
#             conn_update.commit()
#             conn_update.close()
#         except Exception as db_err:
#              logger.error(f"Не удалось обновить статус на 'error' после глобальной ошибки верификации для {account_id}: {db_err}")
# 
#         raise HTTPException(status_code=500, detail=f"Внутренняя ошибка сервера при верификации: {str(e)}")
#     finally:
#         if client and client.is_connected():
#             await client.disconnect()
#             logger.info(f"Клиент для верификации кода аккаунта {account_id} отключен.")


# === Эндпоинт для получения списка аккаунтов ===
@app.get("/api/accounts")
async def get_accounts(request: Request):
    """Получает список всех аккаунтов."""
    # Проверяем админ-ключ
    admin_key = request.headers.get("X-Admin-Key")
    if not admin_key:
        admin_key = request.cookies.get("admin_key")
    
    if not admin_key or not await verify_admin_key(admin_key):
        raise HTTPException(status_code=401, detail="Неверный админ-ключ")
    
    from user_manager import get_db_pool
    
    try:
        pool = await get_db_pool()
        
        async with pool.acquire() as conn:
            # Получаем все аккаунты
            query = "SELECT id, api_id, api_hash, phone, proxy, session_file, status, phone_code_hash FROM telegram_accounts"
            rows = await conn.fetch(query)
            
            result = []
            for account_record in rows:
                account_dict = dict(account_record)
                result.append({
                    "id": account_dict["id"],
                    "api_id": account_dict["api_id"],
                    "api_hash": account_dict["api_hash"],
                    "phone": account_dict["phone"],
                    "proxy": account_dict["proxy"],
                    "session_file": account_dict["session_file"],
                    "status": account_dict["status"],
                    "phone_code_hash": account_dict["phone_code_hash"]
                })
            
            return result
            
    except asyncpg.PostgresError as db_err:
        logger.error(f"Ошибка PostgreSQL при получении списка аккаунтов: {db_err}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка базы данных: {str(db_err)}")
    except Exception as e:
        logger.error(f"Ошибка при получении списка аккаунтов: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")

if __name__ == "__main__":
    port = int(os.getenv("PORT", 3030))
    uvicorn.run(app, host="0.0.0.0", port=port)