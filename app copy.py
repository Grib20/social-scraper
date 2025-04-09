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
from datetime import datetime, timedelta
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
from datetime import datetime, timedelta
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
from asyncpg import ConnectionError

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
    get_posts_by_keywords, get_posts_by_period, 
    get_album_messages
)
from vk_utils import VKClient, find_vk_groups, get_vk_posts, get_vk_posts_in_groups
from user_manager import (
    register_user, set_vk_token, get_db_connection, get_vk_token, get_user, 
    get_next_available_account, update_account_usage, update_user_last_used,
    get_users_dict, verify_api_key, get_active_accounts, init_db
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

@app.get("/admin/users")
async def get_users(request: Request):
    # Пытаемся получить ключ из заголовка
    admin_key = request.headers.get("X-Admin-Key")
    
    # Если ключа нет в заголовке, пытаемся получить из cookie
    if not admin_key:
        admin_key = request.cookies.get("admin_key")
    
    if not admin_key or not await verify_admin_key(admin_key):
        raise HTTPException(status_code=401, detail="Invalid admin key")
    
    users_data = await user_manager.get_users_dict()
    users_list = []

    # Проверяем, что users_data это словарь (на всякий случай)
    if not isinstance(users_data, dict):
         logger.error(f"user_manager.get_users_dict() вернул не словарь, а {type(users_data)}")
         raise HTTPException(status_code=500, detail="Ошибка получения данных пользователей")
    
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
        logger.warning("Обнаружены пользователи без API ключа, генерируем новые и сохраняем...")
        # Проверяем, является ли save_users асинхронной
        if asyncio.iscoroutinefunction(user_manager.save_users):
            await user_manager.save_users(users_data)
            
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

    pool = None
    try:
        # Используем admin_delete_user из admin_panel, который уже адаптирован
        # или напрямую обращаемся к user_manager, если это логичнее
        # Текущая реализация в app.py удаляла напрямую, оставим пока так,
        # но перепишем под asyncpg.

        from user_manager import get_db_connection
        import asyncpg

        pool = await get_db_connection()
        if not pool:
             logger.error(f"Не удалось получить пул соединений для удаления пользователя {user_id}")
             raise HTTPException(status_code=503, detail="Не удалось получить пул БД")

        async with pool.acquire() as conn:
            # Начинаем транзакцию
            async with conn.transaction():
                # Проверяем существование пользователя
                # Используем fetchval для проверки существования
                exists = await conn.fetchval('SELECT 1 FROM users WHERE api_key = $1', user_id)

                if not exists:
                    logger.error(f"Пользователь с ID {user_id} не найден в базе данных")
                    raise HTTPException(status_code=404, detail="Пользователь не найден")

                logger.info(f"Пользователь найден, удаляем аккаунты пользователя")

                # Логируем ID аккаунтов перед удалением (опционально, но полезно)
                # Используем fetch для получения списка записей
                tg_records = await conn.fetch('SELECT id FROM telegram_accounts WHERE user_api_key = $1', user_id)
                telegram_accounts = [row['id'] for row in tg_records]
                logger.info(f"Telegram аккаунты для удаления: {telegram_accounts}")

                vk_records = await conn.fetch('SELECT id FROM vk_accounts WHERE user_api_key = $1', user_id)
                vk_accounts = [row['id'] for row in vk_records]
                logger.info(f"VK аккаунты для удаления: {vk_accounts}")

                # Удаляем аккаунты (ON DELETE CASCADE должен сработать, но можно оставить для явности)
                # await conn.execute('DELETE FROM telegram_accounts WHERE user_api_key = $1', user_id)
                # await conn.execute('DELETE FROM vk_accounts WHERE user_api_key = $1', user_id)

                # Удаляем самого пользователя
                # conn.execute возвращает строку статуса 'DELETE N'
                result_str = await conn.execute('DELETE FROM users WHERE api_key = $1', user_id)
                deleted_count = int(result_str.split()[1])
                logger.info(f"Удалено пользователей: {deleted_count}")

            # Транзакция коммитится автоматически при выходе из блока

        if deleted_count > 0:
            logger.info(f"Пользователь {user_id} успешно удален")
            return {"status": "success", "message": "User deleted successfully"}
        else:
            # Эта ветка не должна достигаться, если пользователь был найден ранее
            logger.error(f"Не удалось удалить пользователя {user_id} (хотя он был найден)")
            raise HTTPException(status_code=500, detail="Не удалось удалить пользователя")

    except HTTPException as e:
        # Перенаправляем ошибку (например, 404 Not Found)
        logger.error(f"HTTP ошибка при удалении пользователя {user_id}: {e.detail}")
        raise e
    except asyncpg.PostgresError as e:
        logger.error(f"Ошибка PostgreSQL при удалении пользователя {user_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка БД: {e}")
    except ConnectionError as e:
         logger.error(f"Ошибка соединения при удалении пользователя {user_id}: {e}")
         raise HTTPException(status_code=503, detail=f"Ошибка соединения с БД: {e}")
    except Exception as e:
        # Логируем другие ошибки
        logger.error(f"Неожиданная ошибка при удалении пользователя {user_id}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Внутренняя ошибка сервера: {e}")
    # finally блок не нужен

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
    users = await user_manager.get_users_dict()
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
    if not await verify_api_key(api_key):
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
    api_id = form_data.get('api_id')
    api_hash = form_data.get('api_hash')
    phone = form_data.get('phone')
    proxy = form_data.get('proxy')
    session_file = form_data.get('session_file')
    
    if not api_id or not api_hash or not phone:
        logger.error("Обязательные поля не заполнены")
        raise HTTPException(400, "Обязательные поля не заполнены")
        
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
            "api_id": api_id,
            "api_hash": api_hash,
            "phone": phone,
            "proxy": proxy,
            "session_file": session_path,
            "status": "pending"  # Изначально устанавливаем статус pending
        }
        
        try:
            # Создаем клиент Telegram и проверяем авторизацию
            from telegram_utils import create_telegram_client
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
        "api_id": api_id,
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
        client = await create_telegram_client(session_name, int(api_id), str(api_hash), proxy if isinstance(proxy, str) or proxy is None else None)
        
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

    # Используем асинхронное подключение к PostgreSQL
    pool = None
    # conn = None # conn будет управляться 'async with'
    account_dict = None
    try:
        from user_manager import get_db_connection
        import asyncpg
        import telegram_utils # Импортируем здесь, а не глобально, если используется только тут
        from telethon.errors import (SessionPasswordNeededError, PhoneCodeInvalidError,
                                   PasswordHashInvalidError, PhoneCodeExpiredError, FloodWaitError)

        pool = await get_db_connection()
        if not pool:
            logger.error(f"Не удалось получить пул для верификации кода {account_id}")
            raise ConnectionError("Не удалось получить пул БД")

        async with pool.acquire() as conn: # Используем 'async with' для управления соединением
            # Получаем все необходимые данные, включая phone_code_hash
            query = 'SELECT phone, api_id, api_hash, session_file, proxy, phone_code_hash FROM telegram_accounts WHERE id = $1'
            account_record = await conn.fetchrow(query, account_id) # Используем fetchrow и $1

            if not account_record:
                logger.error(f"Аккаунт {account_id} не найден для верификации кода")
                raise HTTPException(404, "Аккаунт не найден")

            account_dict = dict(account_record)
            phone = account_dict.get("phone")
            api_id_str = account_dict.get("api_id")
            api_hash = account_dict.get("api_hash")
            session_file = account_dict.get("session_file")
            proxy = account_dict.get("proxy")
            phone_code_hash = account_dict.get("phone_code_hash")

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
                client = await telegram_utils.create_telegram_client(session_file, api_id, api_hash, proxy)

                logger.info(f"Подключение клиента для верификации кода аккаунта {account_id}")
                await client.connect()

                user = None
                if not await client.is_user_authorized():
                    try:
                        logger.info(f"Попытка входа с кодом для аккаунта {account_id}")
                        user = await client.sign_in(phone, code, phone_code_hash=phone_code_hash)
                        logger.info(f"Вход с кодом для {account_id} успешен.")
                    except SessionPasswordNeededError:
                        logger.info(f"Для аккаунта {account_id} требуется пароль 2FA")
                        if not password:
                            logger.warning(f"Пароль 2FA не предоставлен для {account_id}, но он требуется.")
                            # Обновляем статус в БД (в этом же соединении conn)
                            update_query = 'UPDATE telegram_accounts SET status = $1 WHERE id = $2' # Используем $1, $2
                            await conn.execute(update_query, 'pending_2fa', account_id)
                            # conn.commit() не нужен для execute вне транзакции в asyncpg
                            # conn.close() не нужен, управляется 'async with'
                            return JSONResponse(status_code=401, content={"message": "Требуется пароль 2FA", "account_id": account_id, "status": "pending_2fa"})
                        try:
                            logger.info(f"Попытка входа с паролем 2FA для аккаунта {account_id}")
                            user = await client.sign_in(password=password)
                            logger.info(f"Вход с паролем 2FA для {account_id} успешен.")
                        except PasswordHashInvalidError:
                            logger.error(f"Неверный пароль 2FA для аккаунта {account_id}")
                            # conn.close() не нужен
                            # Не меняем статус, чтобы можно было попробовать снова ввести пароль
                            raise HTTPException(status_code=400, detail="Неверный пароль 2FA")
                        except Exception as e_pwd:
                            logger.error(f"Ошибка при входе с паролем 2FA для {account_id}: {str(e_pwd)}", exc_info=True)
                            # conn.close() не нужен
                            # Статус 'error' при других ошибках пароля
                            update_query = 'UPDATE telegram_accounts SET status = $1 WHERE id = $2' # Используем $1, $2
                            await conn.execute(update_query, 'error', account_id)
                            # conn.commit() / conn.close() не нужны
                            raise HTTPException(status_code=500, detail=f"Ошибка при входе с паролем 2FA: {str(e_pwd)}")

                    except (PhoneCodeInvalidError, PhoneCodeExpiredError) as e_code:
                         logger.error(f"Ошибка кода ({type(e_code).__name__}) для аккаунта {account_id}: {str(e_code)}")
                         # conn.close() не нужен
                         # Статус 'pending_code' - код неверный или истек, нужно запросить новый
                         update_query = 'UPDATE telegram_accounts SET status = $1, phone_code_hash = NULL WHERE id = $2' # Используем $1, $2
                         await conn.execute(update_query, 'pending_code', account_id)
                         # conn.commit() / conn.close() не нужны
                         raise HTTPException(status_code=400, detail=f"Ошибка кода: {str(e_code)}")
                    except FloodWaitError as e_flood:
                         logger.error(f"Ошибка FloodWait при верификации кода для {account_id}: ждите {e_flood.seconds} секунд")
                         # conn.close() не нужен
                         raise HTTPException(status_code=429, detail=f"Слишком много попыток. Попробуйте через {e_flood.seconds} секунд.")
                    except Exception as e_signin:
                         logger.error(f"Непредвиденная ошибка при входе для аккаунта {account_id}: {str(e_signin)}", exc_info=True)
                         # conn.close() не нужен
                         # Статус 'error' при других ошибках входа
                         update_query = 'UPDATE telegram_accounts SET status = $1 WHERE id = $2' # Используем $1, $2
                         await conn.execute(update_query, 'error', account_id)
                         # conn.commit() / conn.close() не нужны
                         raise HTTPException(status_code=500, detail=f"Внутренняя ошибка сервера при входе: {str(e_signin)}")
                else:
                     logger.info(f"Аккаунт {account_id} уже авторизован при попытке верификации кода.")
                     user = await client.get_me()

                # Если мы дошли сюда, значит авторизация прошла успешно
                logger.info(f"Аккаунт {account_id} успешно авторизован/верифицирован.")
                # Обновляем статус на 'active' и очищаем phone_code_hash (в том же соединении conn)
                update_query = "UPDATE telegram_accounts SET status = $1, phone_code_hash = NULL WHERE id = $2" # Используем $1, $2
                await conn.execute(update_query, 'active', account_id)
                # conn.commit() / conn.close() не нужны

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

                # Возвращаем результат ПОСЛЕ обновления статуса
                return {"message": "Аккаунт успешно авторизован", "account_id": account_id, "status": "active", "user_info": user_info}

            finally:
                if client and client.is_connected():
                    await client.disconnect()
                    logger.info(f"Клиент для верификации кода аккаунта {account_id} отключен.")
        # async with pool.acquire() as conn: блок завершен, соединение возвращено в пул

    except HTTPException as e:
        # Перехватываем HTTP исключения, чтобы убедиться, что соединение с БД не осталось открытым
        # (async with позаботится об этом)
        raise e
    except (asyncpg.PostgresError, ConnectionError) as db_err:
        logger.error(f"Ошибка БД при верификации кода для {account_id}: {db_err}")
        # Обновляем статус на 'error' (нужен новый коннект, так как текущий мог быть причиной ошибки)
        try:
            pool_upd = await get_db_connection() # Получаем пул снова
            if pool_upd:
                async with pool_upd.acquire() as conn_upd:
                    update_query = 'UPDATE telegram_accounts SET status = $1 WHERE id = $2' # Используем $1, $2
                    await conn_upd.execute(update_query, 'error', account_id)
            else:
                logger.error(f"Не удалось получить пул для обновления статуса на error для {account_id}")
        except Exception as db_upd_err:
            logger.error(f"Не удалось обновить статус на 'error' после ошибки БД для {account_id}: {db_upd_err}")
        raise HTTPException(status_code=500, detail=f"Ошибка базы данных: {db_err}")
    except Exception as e:
        logger.error(f"Непредвиденная ошибка в процессе верификации для {account_id}: {str(e)}", exc_info=True)
        # Обновляем статус на 'error' (нужен новый коннект)
        try:
            pool_upd = await get_db_connection() # Получаем пул снова
            if pool_upd:
                async with pool_upd.acquire() as conn_upd:
                    update_query = 'UPDATE telegram_accounts SET status = $1 WHERE id = $2' # Используем $1, $2
                    await conn_upd.execute(update_query, 'error', account_id)
            else:
                 logger.error(f"Не удалось получить пул для обновления статуса на error после глобальной ошибки для {account_id}")
        except Exception as db_upd_err:
             logger.error(f"Не удалось обновить статус на 'error' после глобальной ошибки верификации для {account_id}: {db_upd_err}")

        raise HTTPException(status_code=500, detail=f"Внутренняя ошибка сервера при верификации: {str(e)}")
    # finally блок не нужен для async with pool.acquire()

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
    conn = await get_db_connection()
    
    query = '''
        SELECT * FROM telegram_accounts 
        WHERE id = ?
    '''
    account = await conn.execute(query, (account_id,))
    account = await account.fetchone()
    
    if not account:
        await conn.close()
        raise HTTPException(404, "Аккаунт не найден")
    
    try:
        # Преобразуем объект sqlite3.Row в словарь
        account_dict = dict(account)
        session_file = account_dict["session_file"]
        client = await telegram_utils.create_telegram_client(session_file, int(account_dict["api_id"]), account_dict["api_hash"], account_dict.get("proxy"))
        
        if account_dict.get("proxy"):
            client.set_proxy(account_dict["proxy"])
        
        await client.connect()
        await client.sign_in(password=password)
        await client.disconnect()
        
        # Обновляем статус аккаунта
        update_query = '''
            UPDATE telegram_accounts
            SET status = ?
            WHERE id = ?
        '''
        await conn.execute(update_query, ('active', account_id))
        await conn.commit()
        
        # При использовании файловой сессии нет необходимости сохранять session_string
        logger.info("2FA авторизация выполнена успешно, сессия сохранена в файл")
        
        await conn.close()
        
        return {"status": "success"}
    except Exception as e:
        await conn.close()
        raise HTTPException(400, f"Ошибка авторизации: {str(e)}")

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
    from user_manager import get_db_connection
    conn = await get_db_connection()
    
    query = '''
        SELECT * FROM telegram_accounts 
        WHERE id = ? 
    '''
    account = await conn.execute(query, (account_id,))
    account = await account.fetchone()
    
    if not account:
        await conn.close()
        raise HTTPException(404, "Аккаунт с указанным ID не найден")
    
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
    await conn.execute('''
        DELETE FROM telegram_accounts 
        WHERE id = ?
    ''', (account_id,))
    
    await conn.commit()
    await conn.close()
    
    return {"status": "success"}

@app.put("/api/telegram/accounts/{account_id}")
async def update_telegram_account(account_id: str, request: Request):
    auth_header = request.headers.get('authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        raise HTTPException(401, "API ключ обязателен")
    admin_key = auth_header.split(' ')[1]
    
    # Проверяем админ-ключ
    if not await verify_admin_key(admin_key):
        raise HTTPException(401, "Неверный админ-ключ")
   
    # 2. Получить данные из тела запроса
    data = await request.json()
    new_proxy = data.get('proxy') # Может быть None или пустой строкой
    # 3. Обновить запись в БД
    conn = None
    try:
        from user_manager import get_db_connection
        conn = await get_db_connection()
        
        # Обновляем запись в БД
        update_query = "UPDATE telegram_accounts SET proxy = ? WHERE id = ?"
        result = await conn.execute(update_query, (new_proxy if new_proxy else None, account_id))
        
        # Проверяем, была ли запись обновлена
        if result.rowcount == 0:
             await conn.close()
             raise HTTPException(404, f"Telegram аккаунт с ID {account_id} не найден")
        
        await conn.commit()
        await conn.close()
        logger.info(f"Прокси для Telegram аккаунта {account_id} обновлен.")
        return {"message": "Прокси успешно обновлен"}
    except sqlite3.Error as e:
        logger.error(f"Ошибка БД при обновлении прокси для TG {account_id}: {e}")
        if conn:
            await conn.close()
        raise HTTPException(500, f"Ошибка базы данных: {e}")
    except Exception as e:
        logger.error(f"Непредвиденная ошибка при обновлении прокси для TG {account_id}: {e}")
        if conn:
            await conn.close()
        raise HTTPException(500, f"Внутренняя ошибка сервера: {e}")

# Эндпоинты для работы с VK аккаунтами
@app.post("/api/vk/accounts")
async def add_vk_account_endpoint(request: Request):
    """Добавляет новый VK аккаунт."""
    logger.info("Начало обработки запроса на добавление VK аккаунта")
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
    
    logger.info(f"Админ-ключ верифицирован, обрабатываем данные аккаунта для пользователя {user_id}")
    
    # Получаем данные из формы
    form_data = await request.form()
    token = form_data.get('token')
    proxy = form_data.get('proxy')
    
    if not token:
        logger.error("Токен VK обязателен")
        raise HTTPException(400, "Токен VK обязателен")
    
    # Проверяем формат токена
    if not token.startswith('vk1.a.'):
        logger.error("Неверный формат токена VK, должен начинаться с vk1.a.")
        raise HTTPException(400, "Неверный формат токена VK, должен начинаться с vk1.a.")
    
    # Проверяем прокси, если он указан
    if proxy:
        logger.info(f"Проверка прокси для VK: {proxy}")
        from vk_utils import validate_proxy
        is_valid = validate_proxy(proxy)
        
        if not is_valid:
            logger.error(f"Неверный формат прокси: {proxy}")
            raise HTTPException(400, "Неверный формат прокси")
        
        # Проверяем соединение через прокси
        try:
            import aiohttp
            async with aiohttp.ClientSession() as session:
                async with session.get("https://api.vk.com/method/users.get", 
                                     params={"v": "5.131", "access_token": token}, 
                                     proxy=proxy, 
                                     timeout=10) as response:
                    if response.status != 200:
                        logger.warning(f"Прокси не работает с VK API: статус {response.status}")
                        raise HTTPException(400, f"Прокси не работает с VK API: статус {response.status}")
        except Exception as e:
            logger.error(f"Ошибка проверки прокси для VK: {str(e)}")
            raise HTTPException(400, f"Ошибка проверки прокси для VK: {str(e)}")
    
    # Создаем новый аккаунт
    account_data = {
        "token": token,
        "proxy": proxy,
        "status": "active"
    }
    
    account_id = str(uuid.uuid4())
    account_data["id"] = account_id
    
    # Проверяем токен через API
    try:
        # Используем прокси при создании клиента, если он указан и валиден
        from vk_utils import VKClient
        async with VKClient(token, proxy, account_id, admin_key) as vk:
            result = await vk._make_request("users.get", {"fields": "photo_50,screen_name"})
            if "response" not in result:
                logger.error(f"Ошибка проверки токена VK: {result.get('error', {}).get('error_msg', 'Неизвестная ошибка')}")
                raise HTTPException(400, f"Ошибка проверки токена VK: {result.get('error', {}).get('error_msg', 'Неизвестная ошибка')}")
            
            # Если токен валиден, сохраняем информацию о пользователе
            if result.get("response") and len(result["response"]) > 0:
                user_info = result["response"][0]
                account_data["user_id"] = user_info.get("id")
                account_data["user_name"] = f"{user_info.get('first_name')} {user_info.get('last_name')}"
                logger.info(f"Токен VK принадлежит пользователю {account_data['user_name']} (ID: {account_data['user_id']})")
    except Exception as e:
        logger.error(f"Ошибка при проверке токена VK: {str(e)}")
        raise HTTPException(400, f"Ошибка при проверке токена VK: {str(e)}")
    
    # Добавляем аккаунт в базу данных
    try:
        await admin_add_vk_account(user_id, account_data)
        logger.info(f"VK аккаунт успешно добавлен, ID: {account_id}")
        
        return {
            "account_id": account_id,
            "status": "success",
            "user_name": account_data.get("user_name", "Неизвестный пользователь"),
            "user_id": account_data.get("user_id", 0)
        }
    except Exception as e:
        logger.error(f"Ошибка при добавлении VK аккаунта в базу данных: {str(e)}")
        raise HTTPException(500, f"Ошибка при добавлении VK аккаунта: {str(e)}")

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
        raise HTTPException(401, "API ключ обязателен")
    admin_key = auth_header.split(' ')[1]
    
    # Проверяем админ-ключ
    if not await verify_admin_key(admin_key):
        logger.error("Неверный админ-ключ")
        raise HTTPException(401, "Неверный админ-ключ")
    
    # Находим аккаунт по ID
    from user_manager import get_db_connection
    from telegram_utils import create_telegram_client
    conn = await get_db_connection()
    
    query = '''
        SELECT * FROM telegram_accounts 
        WHERE id = ?
    '''
    result = await conn.execute(query, (account_id,))
    account = await result.fetchone()
    
    if not account:
        logger.error(f"Аккаунт с ID {account_id} не найден")
        await conn.close()
        raise HTTPException(404, "Аккаунт не найден")
    
    # Преобразуем объект sqlite3.Row в словарь
    account_dict = dict(account)
    session_file = account_dict.get("session_file") # Используем .get() для безопасности
    api_id_str = account_dict.get("api_id")
    api_hash = account_dict.get("api_hash")
    proxy = account_dict.get("proxy")
    current_status = account_dict.get("status", "unknown")
    
    # Проверка на наличие необходимых данных
    if not session_file or not api_id_str or not api_hash:
        logger.error(f"Неполные данные для аккаунта {account_id}: session={session_file}, api_id={api_id_str}, api_hash={api_hash}")
        await conn.close()
        # Устанавливаем статус 'error' в БД
        try:
            conn_update = await get_db_connection()
            update_query = 'UPDATE telegram_accounts SET status = ? WHERE id = ?'
            await conn_update.execute(update_query, ('error', account_id))
            await conn_update.commit()
            await conn_update.close()
        except Exception as db_err:
             logger.error(f"Не удалось обновить статус на 'error' для аккаунта {account_id} из-за неполных данных: {db_err}")
        
        return JSONResponse(status_code=400, content={
            "account_id": account_id,
            "is_authorized": False,
            "status": "error",
            "error": "Неполные данные аккаунта (session_file, api_id, api_hash)"
        })
    
    try:
        api_id = int(api_id_str)
    except (ValueError, TypeError):
        logger.error(f"Неверный формат api_id для аккаунта {account_id}: {api_id_str}")
        await conn.close()
        # Устанавливаем статус 'error' в БД
        try:
            conn_update = await get_db_connection()
            update_query = 'UPDATE telegram_accounts SET status = ? WHERE id = ?'
            await conn_update.execute(update_query, ('error', account_id))
            await conn_update.commit()
            await conn_update.close()
        except Exception as db_err:
             logger.error(f"Не удалось обновить статус на 'error' для аккаунта {account_id} из-за неверного api_id: {db_err}")

        return JSONResponse(status_code=400, content={
            "account_id": account_id,
            "is_authorized": False,
            "status": "error",
            "error": f"Неверный формат api_id: {api_id_str}"
        })
    
    client = None # Инициализируем client как None
    try:
        logger.info(f"Создание клиента Telegram, сессия: {session_file}")
        client = await telegram_utils.create_telegram_client(session_file, api_id, api_hash, proxy)
        
        logger.info("Устанавливаем соединение с Telegram")
        await client.connect()
        
        # Проверяем, авторизован ли клиент
        is_authorized = await client.is_user_authorized()
        
        # Обновляем статус в базе данных, если есть изменения
        new_status = "active" if is_authorized else "pending"
        if current_status != new_status:
            update_query = '''
                UPDATE telegram_accounts
                SET status = ?
                WHERE id = ?
            '''
            await conn.execute(update_query, (new_status, account_id))
            await conn.commit()
            logger.info(f"Статус аккаунта {account_id} обновлен на '{new_status}'")
        
        await conn.close()
        # Добавляем лог перед возвратом
        logger.info(f"Возвращаем статус для аккаунта {account_id}: is_authorized={is_authorized}, status='{new_status}'")
        return {
            "account_id": account_id,
            "is_authorized": is_authorized,
            "status": new_status
        }
    except Exception as e:
        logger.error(f"Ошибка при проверке статуса аккаунта {account_id}: {str(e)}")
        await conn.close() # Закрываем основное соединение
        
        # Устанавливаем статус 'error' в БД
        # Нужно новое соединение, так как старое могло быть закрыто из-за ошибки
        try:
            conn_update = await get_db_connection()
            update_query = 'UPDATE telegram_accounts SET status = ? WHERE id = ?'
            await conn_update.execute(update_query, ('error', account_id))
            await conn_update.commit()
            await conn_update.close()
        except Exception as db_err:
            logger.error(f"Не удалось обновить статус на 'error' для аккаунта {account_id} после ошибки: {db_err}")
            
        # Возвращаем ошибку в JSON
        return JSONResponse(status_code=500, content={
            "account_id": account_id,
            "is_authorized": False,
            "status": "error",
            "error": str(e)
        })
    finally:
        # Гарантированно отключаем клиент, если он был создан и подключен
        if client and client.is_connected():
            await client.disconnect()
            logger.info(f"Соединение клиента Telegram для {account_id} закрыто в блоке finally")

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
    conn = await get_db_connection()
    
    query = '''
        SELECT * FROM telegram_accounts 
        WHERE id = ?
    '''
    account = await conn.execute(query, (account_id,))
    account = await account.fetchone()
    
    if not account:
        logger.error(f"Аккаунт с ID {account_id} не найден")
        await conn.close()
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
        client = await telegram_utils.create_telegram_client(session_file, api_id, api_hash, proxy)
        
        logger.info("Устанавливаем соединение с Telegram")
        await client.connect()
        
        # Проверяем, не авторизован ли уже клиент
        if await client.is_user_authorized():
            logger.info("Клиент уже авторизован")
            await client.disconnect()
            
            # Обновляем статус аккаунта
            update_query = '''
                UPDATE telegram_accounts
                SET status = ?
                WHERE id = ?
            '''
            await conn.execute(update_query, ('active', account_id))
            await conn.commit()
            await conn.close()
            
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
        update_query = '''
            UPDATE telegram_accounts
            SET phone_code_hash = ?
            WHERE id = ?
        '''
        await conn.execute(update_query, (result.phone_code_hash, account_id))
        await conn.commit()
        
        await client.disconnect()
        logger.info("Соединение с Telegram закрыто")
        
        await conn.close()
        return {
            "account_id": account_id,
            "requires_auth": True,
            "message": "Код авторизации отправлен"
        }
    except Exception as e:
        logger.error(f"Ошибка при отправке кода авторизации: {str(e)}")
        await conn.close()
        raise HTTPException(400, f"Ошибка при отправке кода авторизации: {str(e)}")

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

    from user_manager import get_db_connection, cipher
    from datetime import datetime
    from vk_utils import VKClient
    import traceback # Для детального логгирования

    token: Optional[str] = None
    proxy: Optional[str] = None
    account_dict: Optional[dict] = None
    user_info: Optional[dict] = None
    status: str = "unknown"
    error_message: Optional[str] = None
    error_code: int = 0
    last_checked_at = datetime.now().isoformat()

    try:
        # --- Используем ОДНО соединение на всю операцию ---
        async with await get_db_connection() as conn:
            # --- Шаг 1: Чтение данных и расшифровка токена ---
            async with conn.cursor() as cursor:
                await cursor.execute('SELECT * FROM vk_accounts WHERE id = ?', (account_id,))
                account = await cursor.fetchone()

                if not account:
                    logger.error(f"Аккаунт с ID {account_id} не найден в БД")
                    raise HTTPException(status_code=404, detail="Аккаунт не найден")

                account_dict = dict(account)
                token_value = account_dict["token"]
                proxy = account_dict.get("proxy")

                if not token_value:
                    logger.warning(f"Токен отсутствует в БД для аккаунта {account_id}")
                    status = "error"
                    error_message = "Токен отсутствует в БД"
                elif token_value.startswith('vk1.a.'):
                    logger.info(f"Токен для {account_id} не зашифрован.")
                    token = token_value
                else:
                    # Пытаемся расшифровать
                    try:
                        logger.info(f"Пытаемся расшифровать токен для {account_id}")
                        decrypted_token = cipher.decrypt(token_value.encode()).decode()

                        if decrypted_token.startswith('vk1.a.'):
                            logger.info(f"Токен для {account_id} успешно расшифрован.")
                            token = decrypted_token
                            # --- Обновляем токен сразу, если расшифровали ---
                            logger.info(f"Обновляем расшифрованный токен в БД для {account_id}")
                            # Используем conn.execute напрямую, без нового курсора
                            await conn.execute('UPDATE vk_accounts SET token = ? WHERE id = ?', (token, account_id))
                            await conn.commit() # Коммитим обновление токена
                        else:
                            # Проверяем двойное шифрование
                            try:
                                logger.info(f"Проверка двойного шифрования для {account_id}")
                                decrypted_twice = cipher.decrypt(decrypted_token.encode()).decode()
                                if decrypted_twice.startswith('vk1.a.'):
                                    logger.info(f"Токен для {account_id} был зашифрован дважды.")
                                    token = decrypted_twice
                                    # --- Обновляем токен сразу ---
                                    logger.info(f"Обновляем дважды расшифрованный токен в БД для {account_id}")
                                    await conn.execute('UPDATE vk_accounts SET token = ? WHERE id = ?', (token, account_id))
                                    await conn.commit() # Коммитим обновление токена
                                else:
                                    logger.error(f"Невалидный формат токена после двойной расшифровки для {account_id}")
                                    status = "error"
                                    error_message = "Невалидный формат расшифрованного токена (двойное шифрование?)"
                            except Exception:
                                logger.error(f"Невалидный формат расшифрованного токена для {account_id}")
                                status = "error"
                                error_message = "Невалидный формат расшифрованного токена"

                    except Exception as decrypt_error:
                        logger.error(f"Ошибка расшифровки токена для {account_id}: {decrypt_error}")
                        status = "error"
                        error_message = f"Ошибка расшифровки токена: {str(decrypt_error)}"

            # --- Шаг 2: Проверка через API VK (если есть токен и нет ошибки) ---
            if token and status == "unknown": # Только если токен есть и ошибки расшифровки не было
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
                    # Определяем статус по ошибке API
                    if "error_code" in error_message:
                        try: error_code = int(error_message.split("error_code")[1].split(":")[1].strip().split(",")[0])
                        except: pass

                    if "Токен недействителен" in error_message or "access_token has expired" in error_message or error_code == 5: status = "invalid"
                    elif "Ключ доступа сообщества недействителен" in error_message or error_code == 27: status = "invalid"
                    elif "Пользователь заблокирован" in error_message or error_code == 38: status = "banned"
                    elif "Превышен лимит запросов" in error_message or error_code == 29: status = "rate_limited"
                    elif "Требуется валидация" in error_message or error_code == 17: status = "validation_required"
                    else: status = "error"
            elif status == "unknown": # Если токена не было изначально
                 status = "error" # Устанавливаем ошибку, если статус еще не был установлен
                 if not error_message: error_message = "Токен отсутствует или не удалось расшифровать"


            # --- Шаг 3: Финальное обновление статуса в БД ---
            user_id_to_save = user_info.get('id') if user_info else None
            user_name_to_save = f"{user_info.get('first_name', '')} {user_info.get('last_name', '')}".strip() if user_info else None
            # Используем conn.execute напрямую в том же соединении
            await conn.execute('''
                UPDATE vk_accounts SET
                    status = ?, user_id = ?, user_name = ?,
                    error_message = ?, error_code = ?, last_checked_at = ?
                WHERE id = ?''',
                (status, user_id_to_save, user_name_to_save, error_message, error_code, last_checked_at, account_id))
            await conn.commit() # Коммитим финальное обновление
            logger.info(f"Статус аккаунта {account_id} обновлен на '{status}' в БД.")

        # --- async with conn завершается здесь, соединение закрывается ---

        # --- Возвращаем результат ---
        if status == "active":
            return {"account_id": account_id, "status": status, "user_info": user_info}
        else:
            response_data = {"account_id": account_id, "status": status, "error": error_message, "error_code": error_code}
            if user_info: response_data["user_info"] = user_info
            return response_data

    except HTTPException as http_exc: # Пробрасываем HTTP исключения
        raise http_exc
    except Exception as final_e:
        # Логируем любую другую ошибку
        logger.error(f"Непредвиденная ошибка в check_vk_account_status для {account_id}: {final_e}\n{traceback.format_exc()}")
        # В этом случае лучше вернуть 500, так как что-то пошло не так вне API/DB логики
        raise HTTPException(status_code=500, detail=f"Внутренняя ошибка сервера: {str(final_e)}")


async def shutdown_event():
    """Обработчик закрытия приложения."""
    logger.info("Приложение завершает работу, отключаем все клиенты Telegram")
    # Проверяем наличие метода disconnect_all у telegram_pool
    if hasattr(telegram_pool, 'disconnect_all'):
        await telegram_pool.disconnect_all()
    else:
        # Альтернативный способ отключения клиентов
        if hasattr(telegram_pool, 'clients'):
            for client in telegram_pool.clients:
                if hasattr(client, 'disconnect'):
                    await client.disconnect()
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
    
    # Используем асинхронное подключение к SQLite для получения основной информации об аккаунтах
    from user_manager import get_db_connection
    conn = await get_db_connection()
    
    # Проверяем существование таблиц
    tables_result = await conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables_rows = await tables_result.fetchall()
    tables = [row[0] for row in tables_rows]
    logger.info(f"Доступные таблицы в базе данных: {', '.join(tables)}")
    
    vk_accounts = []
    telegram_accounts = []
    
    # Получаем все аккаунты VK, если таблица существует
    if 'vk_accounts' in tables:
        try:
            vk_result = await conn.execute('''
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
            ''')
            vk_rows = await vk_result.fetchall()
            vk_accounts = [dict(row) for row in vk_rows]
        except Exception as e:
            logger.error(f"Ошибка при запросе VK аккаунтов: {e}")
    else:
        logger.warning("Таблица vk_accounts не найдена в базе данных")
    
    # Получаем все аккаунты Telegram, если таблица существует
    if 'telegram_accounts' in tables:
        try:
            tg_result = await conn.execute('''
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
            ''')
            tg_rows = await tg_result.fetchall()
            telegram_accounts = [dict(row) for row in tg_rows]
        except Exception as e:
            logger.error(f"Ошибка при запросе Telegram аккаунтов: {e}")
    else:
        logger.warning("Таблица telegram_accounts не найдена в базе данных")
    
    await conn.close()
    
    # Дополняем данные статистикой из Redis, если доступен
    if redis_client:
        # Обработка VK аккаунтов
        for account in vk_accounts:
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
        for account in telegram_accounts:
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
        for account in vk_accounts:
            account['login'] = account.get('user_name', 'Нет данных')
            account['active'] = account.get('is_active', 1) == 1
            account['requests_count'] = 0
            account['last_used'] = None
            
        for account in telegram_accounts:
            account['login'] = account.get('phone', 'Нет данных')
            account['active'] = account.get('is_active', 1) == 1
            account['requests_count'] = 0
            account['last_used'] = None
    
    return {
        "timestamp": time.time(),
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
    conn = await get_db_connection()
    
    try:
        if platform.lower() == 'vk':
            update_query = "UPDATE vk_accounts SET is_active = ? WHERE id = ?"
            await conn.execute(update_query, (1 if active else 0, account_id))
            
            # Также обновляем статус, чтобы он соответствовал активности
            status_query = "UPDATE vk_accounts SET status = ? WHERE id = ?"
            await conn.execute(status_query, ('active' if active else 'inactive', account_id))
        elif platform.lower() == 'telegram':
            update_query = "UPDATE telegram_accounts SET is_active = ? WHERE id = ?"
            await conn.execute(update_query, (1 if active else 0, account_id))
            
            # Также обновляем статус, чтобы он соответствовал активности
            status_query = "UPDATE telegram_accounts SET status = ? WHERE id = ?"
            await conn.execute(status_query, ('active' if active else 'inactive', account_id))
        else:
            await conn.close()
            raise HTTPException(status_code=400, detail="Неизвестная платформа")
        
        await conn.commit()
        await conn.close()
        
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
    
    except Exception as e:
        await conn.close()
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

    pool = None # Инициализируем pool
    telegram_stats = []
    vk_stats = []
    user_stats = []

    try:
        # Используем user_manager для получения пула asyncpg
        from user_manager import get_db_connection
        import asyncpg # Импортируем asyncpg для обработки ошибок

        pool = await get_db_connection() # Получаем пул
        if not pool:
            logger.error("Не удалось получить пул соединений в get_accounts_statistics_detailed")
            raise HTTPException(status_code=503, detail="Не удалось получить пул БД")

        async with pool.acquire() as conn: # Получаем соединение из пула
            # Статистика по Telegram аккаунтам
            telegram_records = await conn.fetch('''
                SELECT
                    status,
                    COUNT(*) as count,
                    AVG(requests_count) as avg_requests,
                    MAX(requests_count) as max_requests,
                    MIN(requests_count) as min_requests
                FROM telegram_accounts
                GROUP BY status
            ''') # Используем conn.fetch для SELECT
            telegram_stats = [dict(row) for row in telegram_records] # Преобразуем asyncpg.Record в dict

            # Статистика по VK аккаунтам
            vk_records = await conn.fetch('''
                SELECT
                    status,
                    COUNT(*) as count,
                    AVG(requests_count) as avg_requests,
                    MAX(requests_count) as max_requests,
                    MIN(requests_count) as min_requests
                FROM vk_accounts
                GROUP BY status
            ''') # Используем conn.fetch
            vk_stats = [dict(row) for row in vk_records] # Преобразуем asyncpg.Record в dict

            # Статистика по использованию аккаунтов в разрезе пользователей
            # Используем COALESCE для обработки NULL значений от LEFT JOIN
            user_records = await conn.fetch('''
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
                GROUP BY u.api_key, u.username -- Группируем по ключу и имени для PostgreSQL
            ''') # Используем conn.fetch
            user_stats = [dict(row) for row in user_records] # Преобразуем asyncpg.Record в dict

    # Ловим специфичные ошибки asyncpg
    except asyncpg.PostgresError as e:
        logger.error(f"Ошибка PostgreSQL в get_accounts_statistics_detailed: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка БД: {e}")
    # Ловим ошибки соединения (например, если пул не удалось получить)
    except ConnectionError as e:
         logger.error(f"Ошибка соединения в get_accounts_statistics_detailed: {e}")
         raise HTTPException(status_code=503, detail=f"Ошибка соединения с БД: {e}")
    except Exception as e:
        logger.error(f"Неожиданная ошибка в get_accounts_statistics_detailed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Внутренняя ошибка сервера: {e}")
    # finally блок не нужен, 'async with pool.acquire() as conn:' сам вернет соединение

    # Статистика по использованию клиентов из пула (эта часть не меняется, так как работает с объектами пула)
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

    # await conn.close() # Убираем conn.close(), пул управляет соединениями

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

@app.post("/api/media/upload")
async def api_media_upload(request: Request):
    """
    Загрузка медиафайлов в хранилище S3.
    
    Поддерживает загрузку изображений и видео,
    создаёт превью для больших файлов и оптимизирует изображения.
    """
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
        from user_manager import get_db_connection, cipher
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Получаем все VK аккаунты
        cursor.execute("SELECT id, token, user_api_key FROM vk_accounts")
        accounts = cursor.fetchall()
        
        results = []
        for account in accounts:
            account_id = account[0]
            token = account[1]
            user_api_key = account[2]
            
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
        
        conn.close()
        
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
    
    data = await request.json()
    platform = data.get("platform")
    account_id = data.get("account_id")
    
    if not platform or not account_id:
        raise HTTPException(status_code=400, detail="Требуются platform и account_id")
    
    try:
        from user_manager import get_db_connection
        
        if platform == "telegram":
            # Получаем данные аккаунта Telegram
            conn = await get_db_connection()
            query = "SELECT * FROM telegram_accounts WHERE id = ?"
            account_result = await conn.execute(query, (account_id,))
            account = await account_result.fetchone()
            
            if not account:
                await conn.close()
                raise HTTPException(status_code=404, detail="Аккаунт не найден")
            
            # Преобразуем в словарь
            account_dict = dict(account)
            
            # Проверяем наличие прокси
            proxy = account_dict.get("proxy")
            if not proxy:
                await conn.close()
                return {"valid": False, "message": "Прокси не указан для этого аккаунта"}
            
            # Валидируем прокси
            from telegram_utils import validate_proxy
            is_valid, proxy_type = validate_proxy(proxy)
            
            if not is_valid:
                await conn.close()
                return {"valid": False, "message": "Неверный формат прокси"}
            
            # Попытка подключения с прокси
            try:
                from telegram_utils import create_telegram_client
                client = await create_telegram_client(
                    session_file=f"check_proxy_{account_dict['id']}",
                    api_id=int(account_dict['api_id']),
                    api_hash=account_dict['api_hash'],
                    proxy=proxy
                )
                
                # Пробуем подключиться
                await client.connect()
                if await client.is_connected():
                    await client.disconnect()
                    await conn.close()
                    return {"valid": True, "message": f"Успешное подключение через {proxy_type} прокси"}
                else:
                    await conn.close()
                    return {"valid": False, "message": "Не удалось подключиться через прокси"}
            except Exception as e:
                await conn.close()
                return {"valid": False, "message": f"Ошибка подключения: {str(e)}"}
                
        elif platform == "vk":
            # Получаем данные аккаунта VK
            conn = await get_db_connection()
            query = "SELECT * FROM vk_accounts WHERE id = ?"
            account_result = await conn.execute(query, (account_id,))
            account = await account_result.fetchone()
            
            if not account:
                await conn.close()
                raise HTTPException(status_code=404, detail="Аккаунт не найден")
            
            # Преобразуем в словарь
            account_dict = dict(account)
            
            # Проверяем наличие прокси
            proxy = account_dict.get("proxy")
            if not proxy:
                await conn.close()
                return {"valid": False, "message": "Прокси не указан для этого аккаунта"}
            
            # Валидируем прокси
            from vk_utils import validate_proxy, validate_proxy_connection
            
            # Сначала проверяем формат
            is_valid = validate_proxy(proxy)
            if not is_valid:
                await conn.close()
                return {"valid": False, "message": "Неверный формат прокси"}
            
            # Затем проверяем соединение
            await conn.close()
            is_valid, message = await validate_proxy_connection(proxy)
            return {"valid": is_valid, "message": message}
        else:
            raise HTTPException(status_code=400, detail="Неизвестная платформа")
    except Exception as e:
        logger.error(f"Ошибка при проверке прокси: {e}")
        raise HTTPException(status_code=500, detail=f"Внутренняя ошибка: {str(e)}")

@app.post("/api/admin/update-proxy")
async def updateupdate_proxy(request: Request):
    """Обновляет прокси для указанного аккаунта."""
    auth_header = request.headers.get('authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        raise HTTPException(401, "API ключ обязателен")
    
    admin_key = auth_header.split(' ')[1]
    
    # Проверяем админ-ключ
    if not await verify_admin_key(admin_key):
        raise HTTPException(401, "Неверный админ-ключ")
    
    data = await request.json()
    platform = data.get("platform")
    account_id = data.get("account_id")
    user_id = data.get("user_id")
    proxy = data.get("proxy")
    
    if not all([platform, account_id, user_id]):
        raise HTTPException(status_code=400, detail="Требуются platform, account_id и user_id")
    
    try:
        from user_manager import get_db_connection, update_telegram_account, update_vk_account
        
        conn = await get_db_connection()
        
        if platform == "telegram":
            query = 'SELECT * FROM telegram_accounts WHERE id = ?'
            result = await conn.execute(query, (account_id,))
            account = await result.fetchone()
            
            if not account:
                await conn.close()
                raise HTTPException(status_code=404, detail="Аккаунт не найден")
            
            if proxy is None:
                account_data = {"proxy": ""}
                update_result = await update_telegram_account(user_id, account_id, account_data)
                
                if not update_result:
                    await conn.close()
                    raise HTTPException(status_code=404, detail="Не удалось обновить аккаунт")
                
                await conn.close()
                return {"success": True, "message": "Прокси успешно удален"}
            
            from telegram_utils import validate_proxy
            is_valid, _ = validate_proxy(proxy)
            
            if not is_valid:
                await conn.close()
                raise HTTPException(status_code=400, detail="Неверный формат прокси")
            
            account_data = {"proxy": proxy}
            update_result = await update_telegram_account(user_id, account_id, account_data)
            
            if not update_result:
                await conn.close()
                raise HTTPException(status_code=404, detail="Не удалось обновить аккаунт")
            
            await conn.close()
            return {"success": True, "message": "Прокси успешно обновлен"}
            
        elif platform == "vk":
            query = 'SELECT * FROM vk_accounts WHERE id = ?'
            result = await conn.execute(query, (account_id,))
            account = await result.fetchone()
            
            if not account:
                await conn.close()
                raise HTTPException(status_code=404, detail="Аккаунт не найден")
            
            if proxy is None:
                account_data = {"proxy": ""}
                update_result = await update_vk_account(user_id, account_id, account_data)
                
                if not update_result:
                    await conn.close()
                    raise HTTPException(status_code=404, detail="Не удалось обновить аккаунт")
                
                await conn.close()
                return {"success": True, "message": "Прокси успешно удален"}
            
            from vk_utils import validate_proxy
            is_valid = validate_proxy(proxy)
            
            if not is_valid:
                await conn.close()
                raise HTTPException(status_code=400, detail="Неверный формат прокси")
            
            account_data = {"proxy": proxy}
            update_result = await update_vk_account(user_id, account_id, account_data)
            
            if not update_result:
                await conn.close()
                raise HTTPException(status_code=404, detail="Не удалось обновить аккаунт")
            
            await conn.close()
            return {"success": True, "message": "Прокси успешно обновлен"}
        
        else:
            await conn.close()
            raise HTTPException(status_code=400, detail="Неизвестная платформа")
            
    except Exception as e:
        logger.error(f"Ошибка при обновлении прокси: {e}")
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
    try:
        # Проверка подключения к базе данных
        from user_manager import get_db_connection
        conn = await get_db_connection()
        await conn.execute("SELECT 1")
        await conn.close()
        
        # Проверка Redis, если доступен
        if redis_client:
            redis_client.ping()
        
        return {"status": "ok", "timestamp": datetime.now().isoformat()}
    except Exception as e:
        logger.error(f"Ошибка при проверке работоспособности: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка при проверке работоспособности: {str(e)}")

@app.post("/api/admin/fix-vk-tokens")
async def fix_vk_tokens_endpoint(request: Request):
    """Запускает процедуру исправления токенов VK."""
    admin_key = request.headers.get("X-Admin-Key")
    if not admin_key:
        admin_key = request.cookies.get("admin_key")
    
    if not admin_key or not await verify_admin_key(admin_key):
        raise HTTPException(status_code=401, detail="Invalid admin key")
    
    from user_manager import fix_vk_tokens
    fixed_count = fix_vk_tokens()
    
    return {"status": "success", "fixed_count": fixed_count}

@app.post("/api/admin/fix-vk-token/{account_id}")
async def fix_single_vk_token_endpoint(account_id: str, request: Request):
    """Исправляет токен для конкретного VK аккаунта."""
    admin_key = request.headers.get("X-Admin-Key")
    if not admin_key:
        admin_key = request.cookies.get("admin_key")
    
    if not admin_key or not await verify_admin_key(admin_key):
        raise HTTPException(status_code=401, detail="Invalid admin key")
    
    # Получаем токен из базы данных
    from user_manager import get_db_connection, cipher
    from vk_utils import VKClient
    
    conn = await get_db_connection()
    
    query = 'SELECT token, proxy FROM vk_accounts WHERE id = ?'
    result = await conn.execute(query, (account_id,))
    account_row = await result.fetchone()
    
    if not account_row:
        await conn.close()
        raise HTTPException(status_code=404, detail="Account not found")
    
    # Преобразуем объект sqlite3.Row в словарь
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
                    await conn.close()
                    return {"success": True, "message": "Token is valid and working"}
                else:
                    logger.warning(f"Токен для аккаунта {account_id} имеет правильный формат, но не работает в API")
                    await conn.close()
                    return {"success": False, "message": "Token format is correct but API validation failed"}
        except Exception as e:
            logger.error(f"Ошибка при проверке токена через API: {str(e)}")
            await conn.close()
            return {"success": False, "message": f"Error during API validation: {str(e)}"}
    
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
                        update_query = 'UPDATE vk_accounts SET token = ? WHERE id = ?'
                        await conn.execute(update_query, (decrypted_once, account_id))
                        await conn.commit()
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
                                update_query = 'UPDATE vk_accounts SET token = ? WHERE id = ?'
                                await conn.execute(update_query, (decrypted_twice, account_id))
                                await conn.commit()
                                fixed = True
                            else:
                                logger.warning(f"Дважды расшифрованный токен имеет правильный формат, но не работает в API")
                                # Несмотря на ошибку API, обновляем токен если формат правильный
                                update_query = 'UPDATE vk_accounts SET token = ? WHERE id = ?'
                                await conn.execute(update_query, (decrypted_twice, account_id))
                                await conn.commit()
                                fixed = True
                    except Exception as e:
                        logger.error(f"Ошибка при проверке дважды расшифрованного токена через API: {str(e)}")
                        # Обновляем токен несмотря на ошибку API, если формат правильный
                        update_query = 'UPDATE vk_accounts SET token = ? WHERE id = ?'
                        await conn.execute(update_query, (decrypted_twice, account_id))
                        await conn.commit()
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
                                            update_query = 'UPDATE vk_accounts SET token = ? WHERE id = ?'
                                            await conn.execute(update_query, (token_part, account_id))
                                            await conn.commit()
                                            fixed = True
                                        else:
                                            logger.warning(f"Извлеченный токен имеет правильный формат, но не работает в API")
                                            # Обновляем токен несмотря на ошибку API
                                            update_query = 'UPDATE vk_accounts SET token = ? WHERE id = ?'
                                            await conn.execute(update_query, (token_part, account_id))
                                            await conn.commit()
                                            fixed = True
                                except Exception as e:
                                    logger.error(f"Ошибка при проверке извлеченного токена через API: {str(e)}")
                                    # Обновляем токен несмотря на ошибку API
                                    update_query = 'UPDATE vk_accounts SET token = ? WHERE id = ?'
                                    await conn.execute(update_query, (token_part, account_id))
                                    await conn.commit()
                                    fixed = True
            except Exception as inner_e:
                logger.error(f"Ошибка при второй расшифровке токена для аккаунта {account_id}: {str(inner_e)}")
        
        await conn.close()
        
        if fixed:
            return {"success": True, "message": "Token has been fixed"}
        else:
            return {"success": False, "message": "Token could not be fixed, format is invalid"}
    except Exception as e:
        logger.error(f"Ошибка при первой расшифровке токена для аккаунта {account_id}: {str(e)}")
        await conn.close()
        return {"success": False, "message": f"Error during decryption: {str(e)}"}

@app.post("/api/admin/normalize-vk-tokens")
async def normalize_vk_tokens_endpoint(request: Request):
    """Преобразует все токены VK в незашифрованный вид для хранения в базе данных."""
    admin_key = request.headers.get("X-Admin-Key")
    if not admin_key:
        admin_key = request.cookies.get("admin_key")
    
    if not admin_key or not await verify_admin_key(admin_key):
        raise HTTPException(status_code=401, detail="Invalid admin key")
    
    from user_manager import get_db_connection, cipher
    
    conn = await get_db_connection()
    
    # Получаем все токены VK
    query = 'SELECT id, token FROM vk_accounts'
    result = await conn.execute(query)
    account_rows = await result.fetchall()
    accounts = [dict(row) for row in account_rows]
    
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
                    update_query = 'UPDATE vk_accounts SET token = ? WHERE id = ?'
                    await conn.execute(update_query, (decrypted_token, account_id))
                    normalized_count += 1
                    logger.info(f"Токен для аккаунта {account_id} успешно нормализован")
                else:
                    # Пробуем расшифровать второй раз (токен мог быть зашифрован дважды)
                    try:
                        decrypted_twice = cipher.decrypt(decrypted_token.encode()).decode()
                        
                        if decrypted_twice.startswith('vk1.a.'):
                            # Обновляем токен в базе данных
                            update_query = 'UPDATE vk_accounts SET token = ? WHERE id = ?'
                            await conn.execute(update_query, (decrypted_twice, account_id))
                            normalized_count += 1
                            logger.info(f"Токен для аккаунта {account_id} был расшифрован дважды и успешно нормализован")
                        else:
                            # Пробуем найти подстроку 'vk1.a.' в дважды расшифрованном токене
                            if 'vk1.a.' in decrypted_twice:
                                start_pos = decrypted_twice.find('vk1.a.')
                                token_part = decrypted_twice[start_pos:]
                                
                                # Обновляем токен в базе данных
                                update_query = 'UPDATE vk_accounts SET token = ? WHERE id = ?'
                                await conn.execute(update_query, (token_part, account_id))
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
    
    await conn.commit()
    await conn.close()
    
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
    
    from user_manager import get_db_connection
    
    # Используем асинхронное подключение к базе данных
    conn = await get_db_connection()
    
    # Получаем данные аккаунта
    query = 'SELECT * FROM telegram_accounts WHERE id = ?'
    result = await conn.execute(query, (account_id,))
    account = await result.fetchone()
    
    if not account:
        await conn.close()
        raise HTTPException(status_code=404, detail="Аккаунт не найден")
    
    # Преобразуем sqlite3.Row в словарь
    account_dict = dict(account)
    await conn.close()
    
    return account_dict

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
    
    from user_manager import get_db_connection
    
    # Используем асинхронное подключение к базе данных
    conn = await get_db_connection()
    
    # Получаем данные аккаунта
    query = 'SELECT * FROM vk_accounts WHERE id = ?'
    result = await conn.execute(query, (account_id,))
    account = await result.fetchone()
    
    if not account:
        await conn.close()
        raise HTTPException(status_code=404, detail="Аккаунт не найден")
    
    # Преобразуем sqlite3.Row в словарь
    account_dict = dict(account)
    await conn.close()
    
    return account_dict

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
    from user_manager import get_db_connection
    conn = await get_db_connection()
    
    query = '''
        SELECT * FROM telegram_accounts 
        WHERE id = ?
    '''
    account = await conn.execute(query, (account_id,))
    account = await account.fetchone()
    
    if not account:
        logger.error(f"Аккаунт с ID {account_id} не найден")
        await conn.close()
        raise HTTPException(404, "Аккаунт не найден")
    
    # Преобразуем объект sqlite3.Row в словарь
    account_dict = dict(account)
    
    session_file = account_dict["session_file"]
    api_id = int(account_dict["api_id"])
    api_hash = account_dict["api_hash"]
    phone = account_dict["phone"]
    phone_code_hash = account_dict.get("phone_code_hash")
    proxy = account_dict.get("proxy")
    
    if not phone_code_hash:
        logger.error(f"Хеш кода не найден для аккаунта {account_id}")
        await conn.close()
        raise HTTPException(400, "Хеш кода не найден, пожалуйста, запросите код заново")
    
    logger.info(f"Авторизация с кодом {code} для аккаунта {phone} (ID: {account_id})")
    
    try:
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
            await client.disconnect()
            
            # Обновляем статус аккаунта
            update_query = '''
                UPDATE telegram_accounts
                SET status = ?
                WHERE id = ?
            '''
            await conn.execute(update_query, ('active', account_id))
            await conn.commit()
            await conn.close()
            
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
                SET status = ?
                WHERE id = ?
            '''
            await conn.execute(update_query, ('active', account_id))
            await conn.commit()
            
            # Получаем информацию о пользователе для обновления данных аккаунта
            me = await client.get_me()
            if me:
                username = getattr(me, 'username', None)
                if username:
                    update_username_query = '''
                        UPDATE telegram_accounts
                        SET username = ?
                        WHERE id = ?
                    '''
                    await conn.execute(update_username_query, (username, account_id))
                    await conn.commit()
            
            await client.disconnect()
            logger.info(f"Авторизация успешно выполнена для аккаунта {phone}")
            
            await conn.close()
            return {
                "account_id": account_id,
                "status": "success",
                "message": "Авторизация выполнена успешно"
            }
        except SessionPasswordNeededError:
            # Если для аккаунта настроена двухфакторная аутентификация
            logger.info(f"Для аккаунта {phone} требуется пароль 2FA")
            await client.disconnect()
            
            # Обновляем статус аккаунта
            update_query = '''
                UPDATE telegram_accounts
                SET status = ?
                WHERE id = ?
            '''
            await conn.execute(update_query, ('pending', account_id))
            await conn.commit()
            await conn.close()
            
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
            
            await client.disconnect()
            await conn.close()
            
            raise HTTPException(400, user_message)
    except Exception as e:
        logger.error(f"Ошибка при авторизации с кодом: {str(e)}")
        await conn.close()
        raise HTTPException(500, f"Ошибка при авторизации: {str(e)}")

# === Эндпоинт для запроса кода авторизации для существующего аккаунта ===
@app.post("/api/telegram/accounts/{account_id}/request-code")
async def request_telegram_auth_code(request: Request, account_id: str):
    """Запрашивает новый код авторизации для существующего аккаунта Telegram."""
    from user_manager import get_db_connection
    from telegram_utils import create_telegram_client
    from telethon import errors
    logger.info(f"Запрос кода авторизации для аккаунта {account_id}")
    auth_header = request.headers.get('authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        logger.error("API ключ не предоставлен или в неверном формате для запроса кода")
        raise HTTPException(401, "API ключ обязателен")
    admin_key = auth_header.split(' ')[1]

    if not await verify_admin_key(admin_key):
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
from datetime import datetime, timedelta
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
from datetime import datetime, timedelta
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
from asyncpg import ConnectionError

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
    get_posts_by_keywords, get_posts_by_period, 
    get_album_messages
)
from vk_utils import VKClient, find_vk_groups, get_vk_posts, get_vk_posts_in_groups
from user_manager import (
    register_user, set_vk_token, get_db_connection, get_vk_token, get_user, 
    get_next_available_account, update_account_usage, update_user_last_used,
    get_users_dict, verify_api_key, get_active_accounts, init_db
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

@app.get("/admin/users")
async def get_users(request: Request):
    # Пытаемся получить ключ из заголовка
    admin_key = request.headers.get("X-Admin-Key")
    
    # Если ключа нет в заголовке, пытаемся получить из cookie
    if not admin_key:
        admin_key = request.cookies.get("admin_key")
    
    if not admin_key or not await verify_admin_key(admin_key):
        raise HTTPException(status_code=401, detail="Invalid admin key")
    
    users_data = await user_manager.get_users_dict()
    users_list = []

    # Проверяем, что users_data это словарь (на всякий случай)
    if not isinstance(users_data, dict):
         logger.error(f"user_manager.get_users_dict() вернул не словарь, а {type(users_data)}")
         raise HTTPException(status_code=500, detail="Ошибка получения данных пользователей")
    
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
        logger.warning("Обнаружены пользователи без API ключа, генерируем новые и сохраняем...")
        # Проверяем, является ли save_users асинхронной
        if asyncio.iscoroutinefunction(user_manager.save_users):
            await user_manager.save_users(users_data)
            
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

    pool = None
    try:
        # Используем admin_delete_user из admin_panel, который уже адаптирован
        # или напрямую обращаемся к user_manager, если это логичнее
        # Текущая реализация в app.py удаляла напрямую, оставим пока так,
        # но перепишем под asyncpg.

        from user_manager import get_db_connection
        import asyncpg

        pool = await get_db_connection()
        if not pool:
             logger.error(f"Не удалось получить пул соединений для удаления пользователя {user_id}")
             raise HTTPException(status_code=503, detail="Не удалось получить пул БД")

        async with pool.acquire() as conn:
            # Начинаем транзакцию
            async with conn.transaction():
                # Проверяем существование пользователя
                # Используем fetchval для проверки существования
                exists = await conn.fetchval('SELECT 1 FROM users WHERE api_key = $1', user_id)

                if not exists:
                    logger.error(f"Пользователь с ID {user_id} не найден в базе данных")
                    raise HTTPException(status_code=404, detail="Пользователь не найден")

                logger.info(f"Пользователь найден, удаляем аккаунты пользователя")

                # Логируем ID аккаунтов перед удалением (опционально, но полезно)
                # Используем fetch для получения списка записей
                tg_records = await conn.fetch('SELECT id FROM telegram_accounts WHERE user_api_key = $1', user_id)
                telegram_accounts = [row['id'] for row in tg_records]
                logger.info(f"Telegram аккаунты для удаления: {telegram_accounts}")

                vk_records = await conn.fetch('SELECT id FROM vk_accounts WHERE user_api_key = $1', user_id)
                vk_accounts = [row['id'] for row in vk_records]
                logger.info(f"VK аккаунты для удаления: {vk_accounts}")

                # Удаляем аккаунты (ON DELETE CASCADE должен сработать, но можно оставить для явности)
                # await conn.execute('DELETE FROM telegram_accounts WHERE user_api_key = $1', user_id)
                # await conn.execute('DELETE FROM vk_accounts WHERE user_api_key = $1', user_id)

                # Удаляем самого пользователя
                # conn.execute возвращает строку статуса 'DELETE N'
                result_str = await conn.execute('DELETE FROM users WHERE api_key = $1', user_id)
                deleted_count = int(result_str.split()[1])
                logger.info(f"Удалено пользователей: {deleted_count}")

            # Транзакция коммитится автоматически при выходе из блока

        if deleted_count > 0:
            logger.info(f"Пользователь {user_id} успешно удален")
            return {"status": "success", "message": "User deleted successfully"}
        else:
            # Эта ветка не должна достигаться, если пользователь был найден ранее
            logger.error(f"Не удалось удалить пользователя {user_id} (хотя он был найден)")
            raise HTTPException(status_code=500, detail="Не удалось удалить пользователя")

    except HTTPException as e:
        # Перенаправляем ошибку (например, 404 Not Found)
        logger.error(f"HTTP ошибка при удалении пользователя {user_id}: {e.detail}")
        raise e
    except asyncpg.PostgresError as e:
        logger.error(f"Ошибка PostgreSQL при удалении пользователя {user_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка БД: {e}")
    except ConnectionError as e:
         logger.error(f"Ошибка соединения при удалении пользователя {user_id}: {e}")
         raise HTTPException(status_code=503, detail=f"Ошибка соединения с БД: {e}")
    except Exception as e:
        # Логируем другие ошибки
        logger.error(f"Неожиданная ошибка при удалении пользователя {user_id}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Внутренняя ошибка сервера: {e}")
    # finally блок не нужен

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
    users = await user_manager.get_users_dict()
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
    if not await verify_api_key(api_key):
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
    api_id = form_data.get('api_id')
    api_hash = form_data.get('api_hash')
    phone = form_data.get('phone')
    proxy = form_data.get('proxy')
    session_file = form_data.get('session_file')
    
    if not api_id or not api_hash or not phone:
        logger.error("Обязательные поля не заполнены")
        raise HTTPException(400, "Обязательные поля не заполнены")
        
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
            "api_id": api_id,
            "api_hash": api_hash,
            "phone": phone,
            "proxy": proxy,
            "session_file": session_path,
            "status": "pending"  # Изначально устанавливаем статус pending
        }
        
        try:
            # Создаем клиент Telegram и проверяем авторизацию
            from telegram_utils import create_telegram_client
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
        "api_id": api_id,
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
        client = await create_telegram_client(session_name, int(api_id), str(api_hash), proxy if isinstance(proxy, str) or proxy is None else None)
        
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

    # Используем асинхронное подключение к базе данных
    conn = await get_db_connection()
    
    # Получаем все необходимые данные, включая phone_code_hash
    query = 'SELECT phone, api_id, api_hash, session_file, proxy, phone_code_hash FROM telegram_accounts WHERE id = ?'
    account = await conn.execute(query, (account_id,))
    account = await account.fetchone()
    # conn НЕ закрываем здесь, он понадобится для обновления статуса

    if not account:
        logger.error(f"Аккаунт {account_id} не найден для верификации кода")
        await conn.close()
        raise HTTPException(404, "Аккаунт не найден")

    account_dict = dict(account)
    phone = account_dict.get("phone")
    api_id_str = account_dict.get("api_id")
    api_hash = account_dict.get("api_hash")
    session_file = account_dict.get("session_file")
    proxy = account_dict.get("proxy")
    phone_code_hash = account_dict.get("phone_code_hash") # Получаем сохраненный хэш

    if not all([phone, api_id_str, api_hash, session_file]):
        logger.error(f"Неполные данные для верификации кода аккаунта {account_id}")
        await conn.close()
        raise HTTPException(400, "Неполные данные аккаунта для верификации")

    if not phone_code_hash:
         logger.error(f"Отсутствует phone_code_hash для аккаунта {account_id}. Невозможно верифицировать код.")
         await conn.close()
         raise HTTPException(400, "Сначала нужно запросить код авторизации (phone_code_hash отсутствует)")
    try:
        api_id = int(api_id_str) if api_id_str is not None else None
        if api_id is None:
            raise ValueError("api_id не может быть None")
    except (ValueError, TypeError):
        logger.error(f"Неверный формат api_id {api_id_str} для верификации кода аккаунта {account_id}")
        await conn.close()
        raise HTTPException(400, f"Неверный формат api_id: {api_id_str}")

    client = None
    try:
        logger.info(f"Создание клиента для верификации кода, сессия: {session_file}")
        client = await telegram_utils.create_telegram_client(session_file, api_id, api_hash, proxy)

        logger.info(f"Подключение клиента для верификации кода аккаунта {account_id}")
        await client.connect()

        user = None
        if not await client.is_user_authorized():
            try:
                logger.info(f"Попытка входа с кодом для аккаунта {account_id}")
                # Используем phone_code_hash из базы данных
                user = await client.sign_in(phone, code, phone_code_hash=phone_code_hash)
                logger.info(f"Вход с кодом для {account_id} успешен.")
            except SessionPasswordNeededError:
                logger.info(f"Для аккаунта {account_id} требуется пароль 2FA")
                if not password:
                    # Если пароль нужен, но не предоставлен, возвращаем специальный статус
                    logger.warning(f"Пароль 2FA не предоставлен для {account_id}, но он требуется.")
                    await conn.close()
                    # Обновляем статус, чтобы UI знал, что нужен пароль
                    try:
                        conn_update = await get_db_connection()
                        update_query = 'UPDATE telegram_accounts SET status = ? WHERE id = ?'
                        await conn_update.execute(update_query, ('pending_2fa', account_id))
                        await conn_update.commit()
                        await conn_update.close()
                    except Exception as db_err:
                        logger.error(f"Не удалось обновить статус на 'pending_2fa' для {account_id}: {db_err}")

                    return JSONResponse(status_code=401, content={"message": "Требуется пароль 2FA", "account_id": account_id, "status": "pending_2fa"})
                try:
                    logger.info(f"Попытка входа с паролем 2FA для аккаунта {account_id}")
                    user = await client.sign_in(password=password)
                    logger.info(f"Вход с паролем 2FA для {account_id} успешен.")
                except PasswordHashInvalidError:
                    logger.error(f"Неверный пароль 2FA для аккаунта {account_id}")
                    await conn.close()
                    # Не меняем статус, чтобы можно было попробовать снова ввести пароль
                    raise HTTPException(status_code=400, detail="Неверный пароль 2FA")
                except Exception as e_pwd:
                    logger.error(f"Ошибка при входе с паролем 2FA для {account_id}: {str(e_pwd)}", exc_info=True)
                    await conn.close()
                    # Статус 'error' при других ошибках пароля
                    try:
                        conn_update = await get_db_connection()
                        update_query = 'UPDATE telegram_accounts SET status = ? WHERE id = ?'
                        await conn_update.execute(update_query, ('error', account_id))
                        await conn_update.commit()
                        await conn_update.close()
                    except Exception as db_err:
                        logger.error(f"Не удалось обновить статус на 'error' после ошибки 2FA для {account_id}: {db_err}")
                    raise HTTPException(status_code=500, detail=f"Ошибка при входе с паролем 2FA: {str(e_pwd)}")

            except PhoneCodeInvalidError as e_code:
                 logger.error(f"Ошибка кода (PhoneCodeInvalidError) для аккаунта {account_id}: {str(e_code)}")
                 await conn.close()
                 # Статус 'pending_code' - код неверный или истек, нужно запросить новый
                 try:
                     conn_update = await get_db_connection()
                     update_query = 'UPDATE telegram_accounts SET status = ?, phone_code_hash = NULL WHERE id = ?'
                     await conn_update.execute(update_query, ('pending_code', account_id))
                     await conn_update.commit()
                     await conn_update.close()
                 except Exception as db_err:
                     logger.error(f"Не удалось обновить статус на 'pending_code' после ошибки кода для {account_id}: {db_err}")
                 raise HTTPException(status_code=400, detail=f"Ошибка кода: {str(e_code)}")
            except PhoneCodeExpiredError as e_code:
                 logger.error(f"Ошибка кода (PhoneCodeExpiredError) для аккаунта {account_id}: {str(e_code)}")
                 await conn.close()
                 # Статус 'pending_code' - код неверный или истек, нужно запросить новый
                 try:
                     conn_update = await get_db_connection()
                     update_query = 'UPDATE telegram_accounts SET status = ?, phone_code_hash = NULL WHERE id = ?'
                     await conn_update.execute(update_query, ('pending_code', account_id))
                     await conn_update.commit()
                     await conn_update.close()
                 except Exception as db_err:
                     logger.error(f"Не удалось обновить статус на 'pending_code' после ошибки кода для {account_id}: {db_err}")
                 raise HTTPException(status_code=400, detail=f"Ошибка кода: {str(e_code)}")
            except FloodWaitError as e_flood:
                 logger.error(f"Ошибка FloodWait при верификации кода для {account_id}: ждите {e_flood.seconds} секунд")
                 await conn.close()
                 raise HTTPException(status_code=429, detail=f"Слишком много попыток. Попробуйте через {e_flood.seconds} секунд.")
            except Exception as e_signin:
                 logger.error(f"Непредвиденная ошибка при входе для аккаунта {account_id}: {str(e_signin)}", exc_info=True)
                 await conn.close()
                 # Статус 'error' при других ошибках входа
                 try:
                     conn_update = await get_db_connection()
                     update_query = 'UPDATE telegram_accounts SET status = ? WHERE id = ?'
                     await conn_update.execute(update_query, ('error', account_id))
                     await conn_update.commit()
                     await conn_update.close()
                 except Exception as db_err:
                     logger.error(f"Не удалось обновить статус на 'error' после ошибки входа для {account_id}: {db_err}")
                 raise HTTPException(status_code=500, detail=f"Внутренняя ошибка сервера при входе: {str(e_signin)}")
        else:
             logger.info(f"Аккаунт {account_id} уже авторизован при попытке верификации кода.")
             user = await client.get_me()

        # Если мы дошли сюда, значит авторизация прошла успешно
        logger.info(f"Аккаунт {account_id} успешно авторизован/верифицирован.")
        # Обновляем статус на 'active' и очищаем phone_code_hash
        update_query = "UPDATE telegram_accounts SET status = ?, phone_code_hash = NULL WHERE id = ?"
        await conn.execute(update_query, ('active', account_id))
        await conn.commit()
        await conn.close()

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
        if conn: # Закрываем соединение, если оно еще открыто
            await conn.close()
        # Статус 'error' при глобальных ошибках
        try:
            conn_update = await get_db_connection()
            update_query = 'UPDATE telegram_accounts SET status = ? WHERE id = ?'
            await conn_update.execute(update_query, ('error', account_id))
            await conn_update.commit()
            await conn_update.close()
        except Exception as db_err:
             logger.error(f"Не удалось обновить статус на 'error' после глобальной ошибки верификации для {account_id}: {db_err}")

        raise HTTPException(status_code=500, detail=f"Внутренняя ошибка сервера при верификации: {str(e)}")
    finally:
        if client and client.is_connected():
            await client.disconnect()
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
        raise HTTPException(400, "Не указаны необходимые параметры")
    
    # Находим аккаунт по ID
    from user_manager import get_db_connection
    conn = await get_db_connection()
    
    query = '''
        SELECT * FROM telegram_accounts 
        WHERE id = ?
    '''
    account = await conn.execute(query, (account_id,))
    account = await account.fetchone()
    
    if not account:
        await conn.close()
        raise HTTPException(404, "Аккаунт не найден")
    
    try:
        # Преобразуем объект sqlite3.Row в словарь
        account_dict = dict(account)
        session_file = account_dict["session_file"]
        client = await telegram_utils.create_telegram_client(session_file, int(account_dict["api_id"]), account_dict["api_hash"], account_dict.get("proxy"))
        
        if account_dict.get("proxy"):
            client.set_proxy(account_dict["proxy"])
        
        await client.connect()
        await client.sign_in(password=password)
        await client.disconnect()
        
        # Обновляем статус аккаунта
        update_query = '''
            UPDATE telegram_accounts
            SET status = ?
            WHERE id = ?
        '''
        await conn.execute(update_query, ('active', account_id))
        await conn.commit()
        
        # При использовании файловой сессии нет необходимости сохранять session_string
        logger.info("2FA авторизация выполнена успешно, сессия сохранена в файл")
        
        await conn.close()
        
        return {"status": "success"}
    except Exception as e:
        await conn.close()
        raise HTTPException(400, f"Ошибка авторизации: {str(e)}")

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
    from user_manager import get_db_connection
    conn = await get_db_connection()
    
    query = '''
        SELECT * FROM telegram_accounts 
        WHERE id = ? 
    '''
    account = await conn.execute(query, (account_id,))
    account = await account.fetchone()
    
    if not account:
        await conn.close()
        raise HTTPException(404, "Аккаунт с указанным ID не найден")
    
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
    await conn.execute('''
        DELETE FROM telegram_accounts 
        WHERE id = ?
    ''', (account_id,))
    
    await conn.commit()
    await conn.close()
    
    return {"status": "success"}

@app.put("/api/telegram/accounts/{account_id}")
async def update_telegram_account(account_id: str, request: Request):
    auth_header = request.headers.get('authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        raise HTTPException(401, "API ключ обязателен")
    admin_key = auth_header.split(' ')[1]
    
    # Проверяем админ-ключ
    if not await verify_admin_key(admin_key):
        raise HTTPException(401, "Неверный админ-ключ")
   
    # 2. Получить данные из тела запроса
    data = await request.json()
    new_proxy = data.get('proxy') # Может быть None или пустой строкой
    # 3. Обновить запись в БД
    conn = None
    try:
        from user_manager import get_db_connection
        conn = await get_db_connection()
        
        # Обновляем запись в БД
        update_query = "UPDATE telegram_accounts SET proxy = ? WHERE id = ?"
        result = await conn.execute(update_query, (new_proxy if new_proxy else None, account_id))
        
        # Проверяем, была ли запись обновлена
        if result.rowcount == 0:
             await conn.close()
             raise HTTPException(404, f"Telegram аккаунт с ID {account_id} не найден")
        
        await conn.commit()
        await conn.close()
        logger.info(f"Прокси для Telegram аккаунта {account_id} обновлен.")
        return {"message": "Прокси успешно обновлен"}
    except sqlite3.Error as e:
        logger.error(f"Ошибка БД при обновлении прокси для TG {account_id}: {e}")
        if conn:
            await conn.close()
        raise HTTPException(500, f"Ошибка базы данных: {e}")
    except Exception as e:
        logger.error(f"Непредвиденная ошибка при обновлении прокси для TG {account_id}: {e}")
        if conn:
            await conn.close()
        raise HTTPException(500, f"Внутренняя ошибка сервера: {e}")

# Эндпоинты для работы с VK аккаунтами
@app.post("/api/vk/accounts")
async def add_vk_account_endpoint(request: Request):
    """Добавляет новый VK аккаунт."""
    logger.info("Начало обработки запроса на добавление VK аккаунта")
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
    
    logger.info(f"Админ-ключ верифицирован, обрабатываем данные аккаунта для пользователя {user_id}")
    
    # Получаем данные из формы
    form_data = await request.form()
    token = form_data.get('token')
    proxy = form_data.get('proxy')
    
    if not token:
        logger.error("Токен VK обязателен")
        raise HTTPException(400, "Токен VK обязателен")
    
    # Проверяем формат токена
    if not token.startswith('vk1.a.'):
        logger.error("Неверный формат токена VK, должен начинаться с vk1.a.")
        raise HTTPException(400, "Неверный формат токена VK, должен начинаться с vk1.a.")
    
    # Проверяем прокси, если он указан
    if proxy:
        logger.info(f"Проверка прокси для VK: {proxy}")
        from vk_utils import validate_proxy
        is_valid = validate_proxy(proxy)
        
        if not is_valid:
            logger.error(f"Неверный формат прокси: {proxy}")
            raise HTTPException(400, "Неверный формат прокси")
        
        # Проверяем соединение через прокси
        try:
            import aiohttp
            async with aiohttp.ClientSession() as session:
                async with session.get("https://api.vk.com/method/users.get", 
                                     params={"v": "5.131", "access_token": token}, 
                                     proxy=proxy, 
                                     timeout=10) as response:
                    if response.status != 200:
                        logger.warning(f"Прокси не работает с VK API: статус {response.status}")
                        raise HTTPException(400, f"Прокси не работает с VK API: статус {response.status}")
        except Exception as e:
            logger.error(f"Ошибка проверки прокси для VK: {str(e)}")
            raise HTTPException(400, f"Ошибка проверки прокси для VK: {str(e)}")
    
    # Создаем новый аккаунт
    account_data = {
        "token": token,
        "proxy": proxy,
        "status": "active"
    }
    
    account_id = str(uuid.uuid4())
    account_data["id"] = account_id
    
    # Проверяем токен через API
    try:
        # Используем прокси при создании клиента, если он указан и валиден
        from vk_utils import VKClient
        async with VKClient(token, proxy, account_id, admin_key) as vk:
            result = await vk._make_request("users.get", {"fields": "photo_50,screen_name"})
            if "response" not in result:
                logger.error(f"Ошибка проверки токена VK: {result.get('error', {}).get('error_msg', 'Неизвестная ошибка')}")
                raise HTTPException(400, f"Ошибка проверки токена VK: {result.get('error', {}).get('error_msg', 'Неизвестная ошибка')}")
            
            # Если токен валиден, сохраняем информацию о пользователе
            if result.get("response") and len(result["response"]) > 0:
                user_info = result["response"][0]
                account_data["user_id"] = user_info.get("id")
                account_data["user_name"] = f"{user_info.get('first_name')} {user_info.get('last_name')}"
                logger.info(f"Токен VK принадлежит пользователю {account_data['user_name']} (ID: {account_data['user_id']})")
    except Exception as e:
        logger.error(f"Ошибка при проверке токена VK: {str(e)}")
        raise HTTPException(400, f"Ошибка при проверке токена VK: {str(e)}")
    
    # Добавляем аккаунт в базу данных
    try:
        await admin_add_vk_account(user_id, account_data)
        logger.info(f"VK аккаунт успешно добавлен, ID: {account_id}")
        
        return {
            "account_id": account_id,
            "status": "success",
            "user_name": account_data.get("user_name", "Неизвестный пользователь"),
            "user_id": account_data.get("user_id", 0)
        }
    except Exception as e:
        logger.error(f"Ошибка при добавлении VK аккаунта в базу данных: {str(e)}")
        raise HTTPException(500, f"Ошибка при добавлении VK аккаунта: {str(e)}")

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
        raise HTTPException(401, "API ключ обязателен")
    admin_key = auth_header.split(' ')[1]
    
    # Проверяем админ-ключ
    if not await verify_admin_key(admin_key):
        logger.error("Неверный админ-ключ")
        raise HTTPException(401, "Неверный админ-ключ")
    
    # Находим аккаунт по ID
    from user_manager import get_db_connection
    from telegram_utils import create_telegram_client
    conn = await get_db_connection()
    
    query = '''
        SELECT * FROM telegram_accounts 
        WHERE id = ?
    '''
    result = await conn.execute(query, (account_id,))
    account = await result.fetchone()
    
    if not account:
        logger.error(f"Аккаунт с ID {account_id} не найден")
        await conn.close()
        raise HTTPException(404, "Аккаунт не найден")
    
    # Преобразуем объект sqlite3.Row в словарь
    account_dict = dict(account)
    session_file = account_dict.get("session_file") # Используем .get() для безопасности
    api_id_str = account_dict.get("api_id")
    api_hash = account_dict.get("api_hash")
    proxy = account_dict.get("proxy")
    current_status = account_dict.get("status", "unknown")
    
    # Проверка на наличие необходимых данных
    if not session_file or not api_id_str or not api_hash:
        logger.error(f"Неполные данные для аккаунта {account_id}: session={session_file}, api_id={api_id_str}, api_hash={api_hash}")
        await conn.close()
        # Устанавливаем статус 'error' в БД
        try:
            conn_update = await get_db_connection()
            update_query = 'UPDATE telegram_accounts SET status = ? WHERE id = ?'
            await conn_update.execute(update_query, ('error', account_id))
            await conn_update.commit()
            await conn_update.close()
        except Exception as db_err:
             logger.error(f"Не удалось обновить статус на 'error' для аккаунта {account_id} из-за неполных данных: {db_err}")
        
        return JSONResponse(status_code=400, content={
            "account_id": account_id,
            "is_authorized": False,
            "status": "error",
            "error": "Неполные данные аккаунта (session_file, api_id, api_hash)"
        })
    
    try:
        api_id = int(api_id_str)
    except (ValueError, TypeError):
        logger.error(f"Неверный формат api_id для аккаунта {account_id}: {api_id_str}")
        await conn.close()
        # Устанавливаем статус 'error' в БД
        try:
            conn_update = await get_db_connection()
            update_query = 'UPDATE telegram_accounts SET status = ? WHERE id = ?'
            await conn_update.execute(update_query, ('error', account_id))
            await conn_update.commit()
            await conn_update.close()
        except Exception as db_err:
             logger.error(f"Не удалось обновить статус на 'error' для аккаунта {account_id} из-за неверного api_id: {db_err}")

        return JSONResponse(status_code=400, content={
            "account_id": account_id,
            "is_authorized": False,
            "status": "error",
            "error": f"Неверный формат api_id: {api_id_str}"
        })
    
    client = None # Инициализируем client как None
    try:
        logger.info(f"Создание клиента Telegram, сессия: {session_file}")
        client = await telegram_utils.create_telegram_client(session_file, api_id, api_hash, proxy)
        
        logger.info("Устанавливаем соединение с Telegram")
        await client.connect()
        
        # Проверяем, авторизован ли клиент
        is_authorized = await client.is_user_authorized()
        
        # Обновляем статус в базе данных, если есть изменения
        new_status = "active" if is_authorized else "pending"
        if current_status != new_status:
            update_query = '''
                UPDATE telegram_accounts
                SET status = ?
                WHERE id = ?
            '''
            await conn.execute(update_query, (new_status, account_id))
            await conn.commit()
            logger.info(f"Статус аккаунта {account_id} обновлен на '{new_status}'")
        
        await conn.close()
        # Добавляем лог перед возвратом
        logger.info(f"Возвращаем статус для аккаунта {account_id}: is_authorized={is_authorized}, status='{new_status}'")
        return {
            "account_id": account_id,
            "is_authorized": is_authorized,
            "status": new_status
        }
    except Exception as e:
        logger.error(f"Ошибка при проверке статуса аккаунта {account_id}: {str(e)}")
        await conn.close() # Закрываем основное соединение
        
        # Устанавливаем статус 'error' в БД
        # Нужно новое соединение, так как старое могло быть закрыто из-за ошибки
        try:
            conn_update = await get_db_connection()
            update_query = 'UPDATE telegram_accounts SET status = ? WHERE id = ?'
            await conn_update.execute(update_query, ('error', account_id))
            await conn_update.commit()
            await conn_update.close()
        except Exception as db_err:
            logger.error(f"Не удалось обновить статус на 'error' для аккаунта {account_id} после ошибки: {db_err}")
            
        # Возвращаем ошибку в JSON
        return JSONResponse(status_code=500, content={
            "account_id": account_id,
            "is_authorized": False,
            "status": "error",
            "error": str(e)
        })
    finally:
        # Гарантированно отключаем клиент, если он был создан и подключен
        if client and client.is_connected():
            await client.disconnect()
            logger.info(f"Соединение клиента Telegram для {account_id} закрыто в блоке finally")

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
    conn = await get_db_connection()
    
    query = '''
        SELECT * FROM telegram_accounts 
        WHERE id = ?
    '''
    account = await conn.execute(query, (account_id,))
    account = await account.fetchone()
    
    if not account:
        logger.error(f"Аккаунт с ID {account_id} не найден")
        await conn.close()
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
        client = await telegram_utils.create_telegram_client(session_file, api_id, api_hash, proxy)
        
        logger.info("Устанавливаем соединение с Telegram")
        await client.connect()
        
        # Проверяем, не авторизован ли уже клиент
        if await client.is_user_authorized():
            logger.info("Клиент уже авторизован")
            await client.disconnect()
            
            # Обновляем статус аккаунта
            update_query = '''
                UPDATE telegram_accounts
                SET status = ?
                WHERE id = ?
            '''
            await conn.execute(update_query, ('active', account_id))
            await conn.commit()
            await conn.close()
            
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
        update_query = '''
            UPDATE telegram_accounts
            SET phone_code_hash = ?
            WHERE id = ?
        '''
        await conn.execute(update_query, (result.phone_code_hash, account_id))
        await conn.commit()
        
        await client.disconnect()
        logger.info("Соединение с Telegram закрыто")
        
        await conn.close()
        return {
            "account_id": account_id,
            "requires_auth": True,
            "message": "Код авторизации отправлен"
        }
    except Exception as e:
        logger.error(f"Ошибка при отправке кода авторизации: {str(e)}")
        await conn.close()
        raise HTTPException(400, f"Ошибка при отправке кода авторизации: {str(e)}")

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

    from user_manager import get_db_connection, cipher
    from datetime import datetime
    from vk_utils import VKClient
    import traceback # Для детального логгирования

    token: Optional[str] = None
    proxy: Optional[str] = None
    account_dict: Optional[dict] = None
    user_info: Optional[dict] = None
    status: str = "unknown"
    error_message: Optional[str] = None
    error_code: int = 0
    last_checked_at = datetime.now().isoformat()

    try:
        # --- Используем ОДНО соединение на всю операцию ---
        async with await get_db_connection() as conn:
            # --- Шаг 1: Чтение данных и расшифровка токена ---
            async with conn.cursor() as cursor:
                await cursor.execute('SELECT * FROM vk_accounts WHERE id = ?', (account_id,))
                account = await cursor.fetchone()

                if not account:
                    logger.error(f"Аккаунт с ID {account_id} не найден в БД")
                    raise HTTPException(status_code=404, detail="Аккаунт не найден")

                account_dict = dict(account)
                token_value = account_dict["token"]
                proxy = account_dict.get("proxy")

                if not token_value:
                    logger.warning(f"Токен отсутствует в БД для аккаунта {account_id}")
                    status = "error"
                    error_message = "Токен отсутствует в БД"
                elif token_value.startswith('vk1.a.'):
                    logger.info(f"Токен для {account_id} не зашифрован.")
                    token = token_value
                else:
                    # Пытаемся расшифровать
                    try:
                        logger.info(f"Пытаемся расшифровать токен для {account_id}")
                        decrypted_token = cipher.decrypt(token_value.encode()).decode()

                        if decrypted_token.startswith('vk1.a.'):
                            logger.info(f"Токен для {account_id} успешно расшифрован.")
                            token = decrypted_token
                            # --- Обновляем токен сразу, если расшифровали ---
                            logger.info(f"Обновляем расшифрованный токен в БД для {account_id}")
                            # Используем conn.execute напрямую, без нового курсора
                            await conn.execute('UPDATE vk_accounts SET token = ? WHERE id = ?', (token, account_id))
                            await conn.commit() # Коммитим обновление токена
                        else:
                            # Проверяем двойное шифрование
                            try:
                                logger.info(f"Проверка двойного шифрования для {account_id}")
                                decrypted_twice = cipher.decrypt(decrypted_token.encode()).decode()
                                if decrypted_twice.startswith('vk1.a.'):
                                    logger.info(f"Токен для {account_id} был зашифрован дважды.")
                                    token = decrypted_twice
                                    # --- Обновляем токен сразу ---
                                    logger.info(f"Обновляем дважды расшифрованный токен в БД для {account_id}")
                                    await conn.execute('UPDATE vk_accounts SET token = ? WHERE id = ?', (token, account_id))
                                    await conn.commit() # Коммитим обновление токена
                                else:
                                    logger.error(f"Невалидный формат токена после двойной расшифровки для {account_id}")
                                    status = "error"
                                    error_message = "Невалидный формат расшифрованного токена (двойное шифрование?)"
                            except Exception:
                                logger.error(f"Невалидный формат расшифрованного токена для {account_id}")
                                status = "error"
                                error_message = "Невалидный формат расшифрованного токена"

                    except Exception as decrypt_error:
                        logger.error(f"Ошибка расшифровки токена для {account_id}: {decrypt_error}")
                        status = "error"
                        error_message = f"Ошибка расшифровки токена: {str(decrypt_error)}"

            # --- Шаг 2: Проверка через API VK (если есть токен и нет ошибки) ---
            if token and status == "unknown": # Только если токен есть и ошибки расшифровки не было
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
                    # Определяем статус по ошибке API
                    if "error_code" in error_message:
                        try: error_code = int(error_message.split("error_code")[1].split(":")[1].strip().split(",")[0])
                        except: pass

                    if "Токен недействителен" in error_message or "access_token has expired" in error_message or error_code == 5: status = "invalid"
                    elif "Ключ доступа сообщества недействителен" in error_message or error_code == 27: status = "invalid"
                    elif "Пользователь заблокирован" in error_message or error_code == 38: status = "banned"
                    elif "Превышен лимит запросов" in error_message or error_code == 29: status = "rate_limited"
                    elif "Требуется валидация" in error_message or error_code == 17: status = "validation_required"
                    else: status = "error"
            elif status == "unknown": # Если токена не было изначально
                 status = "error" # Устанавливаем ошибку, если статус еще не был установлен
                 if not error_message: error_message = "Токен отсутствует или не удалось расшифровать"


            # --- Шаг 3: Финальное обновление статуса в БД ---
            user_id_to_save = user_info.get('id') if user_info else None
            user_name_to_save = f"{user_info.get('first_name', '')} {user_info.get('last_name', '')}".strip() if user_info else None
            # Используем conn.execute напрямую в том же соединении
            await conn.execute('''
                UPDATE vk_accounts SET
                    status = ?, user_id = ?, user_name = ?,
                    error_message = ?, error_code = ?, last_checked_at = ?
                WHERE id = ?''',
                (status, user_id_to_save, user_name_to_save, error_message, error_code, last_checked_at, account_id))
            await conn.commit() # Коммитим финальное обновление
            logger.info(f"Статус аккаунта {account_id} обновлен на '{status}' в БД.")

        # --- async with conn завершается здесь, соединение закрывается ---

        # --- Возвращаем результат ---
        if status == "active":
            return {"account_id": account_id, "status": status, "user_info": user_info}
        else:
            response_data = {"account_id": account_id, "status": status, "error": error_message, "error_code": error_code}
            if user_info: response_data["user_info"] = user_info
            return response_data

    except HTTPException as http_exc: # Пробрасываем HTTP исключения
        raise http_exc
    except Exception as final_e:
        # Логируем любую другую ошибку
        logger.error(f"Непредвиденная ошибка в check_vk_account_status для {account_id}: {final_e}\n{traceback.format_exc()}")
        # В этом случае лучше вернуть 500, так как что-то пошло не так вне API/DB логики
        raise HTTPException(status_code=500, detail=f"Внутренняя ошибка сервера: {str(final_e)}")


async def shutdown_event():
    """Обработчик закрытия приложения."""
    logger.info("Приложение завершает работу, отключаем все клиенты Telegram")
    # Проверяем наличие метода disconnect_all у telegram_pool
    if hasattr(telegram_pool, 'disconnect_all'):
        await telegram_pool.disconnect_all()
    else:
        # Альтернативный способ отключения клиентов
        if hasattr(telegram_pool, 'clients'):
            for client in telegram_pool.clients:
                if hasattr(client, 'disconnect'):
                    await client.disconnect()
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
    
    # Используем асинхронное подключение к SQLite для получения основной информации об аккаунтах
    from user_manager import get_db_connection
    conn = await get_db_connection()
    
    # Проверяем существование таблиц
    tables_result = await conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables_rows = await tables_result.fetchall()
    tables = [row[0] for row in tables_rows]
    logger.info(f"Доступные таблицы в базе данных: {', '.join(tables)}")
    
    vk_accounts = []
    telegram_accounts = []
    
    # Получаем все аккаунты VK, если таблица существует
    if 'vk_accounts' in tables:
        try:
            vk_result = await conn.execute('''
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
            ''')
            vk_rows = await vk_result.fetchall()
            vk_accounts = [dict(row) for row in vk_rows]
        except Exception as e:
            logger.error(f"Ошибка при запросе VK аккаунтов: {e}")
    else:
        logger.warning("Таблица vk_accounts не найдена в базе данных")
    
    # Получаем все аккаунты Telegram, если таблица существует
    if 'telegram_accounts' in tables:
        try:
            tg_result = await conn.execute('''
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
            ''')
            tg_rows = await tg_result.fetchall()
            telegram_accounts = [dict(row) for row in tg_rows]
        except Exception as e:
            logger.error(f"Ошибка при запросе Telegram аккаунтов: {e}")
    else:
        logger.warning("Таблица telegram_accounts не найдена в базе данных")
    
    await conn.close()
    
    # Дополняем данные статистикой из Redis, если доступен
    if redis_client:
        # Обработка VK аккаунтов
        for account in vk_accounts:
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
        for account in telegram_accounts:
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
        for account in vk_accounts:
            account['login'] = account.get('user_name', 'Нет данных')
            account['active'] = account.get('is_active', 1) == 1
            account['requests_count'] = 0
            account['last_used'] = None
            
        for account in telegram_accounts:
            account['login'] = account.get('phone', 'Нет данных')
            account['active'] = account.get('is_active', 1) == 1
            account['requests_count'] = 0
            account['last_used'] = None
    
    return {
        "timestamp": time.time(),
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
    conn = await get_db_connection()
    
    try:
        if platform.lower() == 'vk':
            update_query = "UPDATE vk_accounts SET is_active = ? WHERE id = ?"
            await conn.execute(update_query, (1 if active else 0, account_id))
            
            # Также обновляем статус, чтобы он соответствовал активности
            status_query = "UPDATE vk_accounts SET status = ? WHERE id = ?"
            await conn.execute(status_query, ('active' if active else 'inactive', account_id))
        elif platform.lower() == 'telegram':
            update_query = "UPDATE telegram_accounts SET is_active = ? WHERE id = ?"
            await conn.execute(update_query, (1 if active else 0, account_id))
            
            # Также обновляем статус, чтобы он соответствовал активности
            status_query = "UPDATE telegram_accounts SET status = ? WHERE id = ?"
            await conn.execute(status_query, ('active' if active else 'inactive', account_id))
        else:
            await conn.close()
            raise HTTPException(status_code=400, detail="Неизвестная платформа")
        
        await conn.commit()
        await conn.close()
        
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
    
    except Exception as e:
        await conn.close()
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

    pool = None # Инициализируем pool
    telegram_stats = []
    vk_stats = []
    user_stats = []

    try:
        # Используем user_manager для получения пула asyncpg
        from user_manager import get_db_connection
        import asyncpg # Импортируем asyncpg для обработки ошибок

        pool = await get_db_connection() # Получаем пул
        if not pool:
            logger.error("Не удалось получить пул соединений в get_accounts_statistics_detailed")
            raise HTTPException(status_code=503, detail="Не удалось получить пул БД")

        async with pool.acquire() as conn: # Получаем соединение из пула
            # Статистика по Telegram аккаунтам
            telegram_records = await conn.fetch('''
                SELECT
                    status,
                    COUNT(*) as count,
                    AVG(requests_count) as avg_requests,
                    MAX(requests_count) as max_requests,
                    MIN(requests_count) as min_requests
                FROM telegram_accounts
                GROUP BY status
            ''') # Используем conn.fetch для SELECT
            telegram_stats = [dict(row) for row in telegram_records] # Преобразуем asyncpg.Record в dict

            # Статистика по VK аккаунтам
            vk_records = await conn.fetch('''
                SELECT
                    status,
                    COUNT(*) as count,
                    AVG(requests_count) as avg_requests,
                    MAX(requests_count) as max_requests,
                    MIN(requests_count) as min_requests
                FROM vk_accounts
                GROUP BY status
            ''') # Используем conn.fetch
            vk_stats = [dict(row) for row in vk_records] # Преобразуем asyncpg.Record в dict

            # Статистика по использованию аккаунтов в разрезе пользователей
            # Используем COALESCE для обработки NULL значений от LEFT JOIN
            user_records = await conn.fetch('''
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
                GROUP BY u.api_key, u.username -- Группируем по ключу и имени для PostgreSQL
            ''') # Используем conn.fetch
            user_stats = [dict(row) for row in user_records] # Преобразуем asyncpg.Record в dict

    # Ловим специфичные ошибки asyncpg
    except asyncpg.PostgresError as e:
        logger.error(f"Ошибка PostgreSQL в get_accounts_statistics_detailed: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка БД: {e}")
    # Ловим ошибки соединения (например, если пул не удалось получить)
    except ConnectionError as e:
         logger.error(f"Ошибка соединения в get_accounts_statistics_detailed: {e}")
         raise HTTPException(status_code=503, detail=f"Ошибка соединения с БД: {e}")
    except Exception as e:
        logger.error(f"Неожиданная ошибка в get_accounts_statistics_detailed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Внутренняя ошибка сервера: {e}")
    # finally блок не нужен, 'async with pool.acquire() as conn:' сам вернет соединение

    # Статистика по использованию клиентов из пула (эта часть не меняется, так как работает с объектами пула)
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

    # await conn.close() # Убираем conn.close(), пул управляет соединениями

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

@app.post("/api/media/upload")
async def api_media_upload(request: Request):
    """
    Загрузка медиафайлов в хранилище S3.
    
    Поддерживает загрузку изображений и видео,
    создаёт превью для больших файлов и оптимизирует изображения.
    """
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
        from user_manager import get_db_connection, cipher
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Получаем все VK аккаунты
        cursor.execute("SELECT id, token, user_api_key FROM vk_accounts")
        accounts = cursor.fetchall()
        
        results = []
        for account in accounts:
            account_id = account[0]
            token = account[1]
            user_api_key = account[2]
            
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
        
        conn.close()
        
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
    
    data = await request.json()
    platform = data.get("platform")
    account_id = data.get("account_id")
    
    if not platform or not account_id:
        raise HTTPException(status_code=400, detail="Требуются platform и account_id")
    
    try:
        from user_manager import get_db_connection
        
        if platform == "telegram":
            # Получаем данные аккаунта Telegram
            conn = await get_db_connection()
            query = "SELECT * FROM telegram_accounts WHERE id = ?"
            account_result = await conn.execute(query, (account_id,))
            account = await account_result.fetchone()
            
            if not account:
                await conn.close()
                raise HTTPException(status_code=404, detail="Аккаунт не найден")
            
            # Преобразуем в словарь
            account_dict = dict(account)
            
            # Проверяем наличие прокси
            proxy = account_dict.get("proxy")
            if not proxy:
                await conn.close()
                return {"valid": False, "message": "Прокси не указан для этого аккаунта"}
            
            # Валидируем прокси
            from telegram_utils import validate_proxy
            is_valid, proxy_type = validate_proxy(proxy)
            
            if not is_valid:
                await conn.close()
                return {"valid": False, "message": "Неверный формат прокси"}
            
            # Попытка подключения с прокси
            try:
                from telegram_utils import create_telegram_client
                client = await create_telegram_client(
                    session_file=f"check_proxy_{account_dict['id']}",
                    api_id=int(account_dict['api_id']),
                    api_hash=account_dict['api_hash'],
                    proxy=proxy
                )
                
                # Пробуем подключиться
                await client.connect()
                if await client.is_connected():
                    await client.disconnect()
                    await conn.close()
                    return {"valid": True, "message": f"Успешное подключение через {proxy_type} прокси"}
                else:
                    await conn.close()
                    return {"valid": False, "message": "Не удалось подключиться через прокси"}
            except Exception as e:
                await conn.close()
                return {"valid": False, "message": f"Ошибка подключения: {str(e)}"}
                
        elif platform == "vk":
            # Получаем данные аккаунта VK
            conn = await get_db_connection()
            query = "SELECT * FROM vk_accounts WHERE id = ?"
            account_result = await conn.execute(query, (account_id,))
            account = await account_result.fetchone()
            
            if not account:
                await conn.close()
                raise HTTPException(status_code=404, detail="Аккаунт не найден")
            
            # Преобразуем в словарь
            account_dict = dict(account)
            
            # Проверяем наличие прокси
            proxy = account_dict.get("proxy")
            if not proxy:
                await conn.close()
                return {"valid": False, "message": "Прокси не указан для этого аккаунта"}
            
            # Валидируем прокси
            from vk_utils import validate_proxy, validate_proxy_connection
            
            # Сначала проверяем формат
            is_valid = validate_proxy(proxy)
            if not is_valid:
                await conn.close()
                return {"valid": False, "message": "Неверный формат прокси"}
            
            # Затем проверяем соединение
            await conn.close()
            is_valid, message = await validate_proxy_connection(proxy)
            return {"valid": is_valid, "message": message}
        else:
            raise HTTPException(status_code=400, detail="Неизвестная платформа")
    except Exception as e:
        logger.error(f"Ошибка при проверке прокси: {e}")
        raise HTTPException(status_code=500, detail=f"Внутренняя ошибка: {str(e)}")

@app.post("/api/admin/update-proxy")
async def updateupdate_proxy(request: Request):
    """Обновляет прокси для указанного аккаунта."""
    auth_header = request.headers.get('authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        raise HTTPException(401, "API ключ обязателен")
    
    admin_key = auth_header.split(' ')[1]
    
    # Проверяем админ-ключ
    if not await verify_admin_key(admin_key):
        raise HTTPException(401, "Неверный админ-ключ")
    
    data = await request.json()
    platform = data.get("platform")
    account_id = data.get("account_id")
    user_id = data.get("user_id")
    proxy = data.get("proxy")
    
    if not all([platform, account_id, user_id]):
        raise HTTPException(status_code=400, detail="Требуются platform, account_id и user_id")
    
    try:
        from user_manager import get_db_connection, update_telegram_account, update_vk_account
        
        conn = await get_db_connection()
        
        if platform == "telegram":
            query = 'SELECT * FROM telegram_accounts WHERE id = ?'
            result = await conn.execute(query, (account_id,))
            account = await result.fetchone()
            
            if not account:
                await conn.close()
                raise HTTPException(status_code=404, detail="Аккаунт не найден")
            
            if proxy is None:
                account_data = {"proxy": ""}
                update_result = await update_telegram_account(user_id, account_id, account_data)
                
                if not update_result:
                    await conn.close()
                    raise HTTPException(status_code=404, detail="Не удалось обновить аккаунт")
                
                await conn.close()
                return {"success": True, "message": "Прокси успешно удален"}
            
            from telegram_utils import validate_proxy
            is_valid, _ = validate_proxy(proxy)
            
            if not is_valid:
                await conn.close()
                raise HTTPException(status_code=400, detail="Неверный формат прокси")
            
            account_data = {"proxy": proxy}
            update_result = await update_telegram_account(user_id, account_id, account_data)
            
            if not update_result:
                await conn.close()
                raise HTTPException(status_code=404, detail="Не удалось обновить аккаунт")
            
            await conn.close()
            return {"success": True, "message": "Прокси успешно обновлен"}
            
        elif platform == "vk":
            query = 'SELECT * FROM vk_accounts WHERE id = ?'
            result = await conn.execute(query, (account_id,))
            account = await result.fetchone()
            
            if not account:
                await conn.close()
                raise HTTPException(status_code=404, detail="Аккаунт не найден")
            
            if proxy is None:
                account_data = {"proxy": ""}
                update_result = await update_vk_account(user_id, account_id, account_data)
                
                if not update_result:
                    await conn.close()
                    raise HTTPException(status_code=404, detail="Не удалось обновить аккаунт")
                
                await conn.close()
                return {"success": True, "message": "Прокси успешно удален"}
            
            from vk_utils import validate_proxy
            is_valid = validate_proxy(proxy)
            
            if not is_valid:
                await conn.close()
                raise HTTPException(status_code=400, detail="Неверный формат прокси")
            
            account_data = {"proxy": proxy}
            update_result = await update_vk_account(user_id, account_id, account_data)
            
            if not update_result:
                await conn.close()
                raise HTTPException(status_code=404, detail="Не удалось обновить аккаунт")
            
            await conn.close()
            return {"success": True, "message": "Прокси успешно обновлен"}
        
        else:
            await conn.close()
            raise HTTPException(status_code=400, detail="Неизвестная платформа")
            
    except Exception as e:
        logger.error(f"Ошибка при обновлении прокси: {e}")
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
    try:
        # Проверка подключения к базе данных
        from user_manager import get_db_connection
        conn = await get_db_connection()
        await conn.execute("SELECT 1")
        await conn.close()
        
        # Проверка Redis, если доступен
        if redis_client:
            redis_client.ping()
        
        return {"status": "ok", "timestamp": datetime.now().isoformat()}
    except Exception as e:
        logger.error(f"Ошибка при проверке работоспособности: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка при проверке работоспособности: {str(e)}")

@app.post("/api/admin/fix-vk-tokens")
async def fix_vk_tokens_endpoint(request: Request):
    """Запускает процедуру исправления токенов VK."""
    admin_key = request.headers.get("X-Admin-Key")
    if not admin_key:
        admin_key = request.cookies.get("admin_key")
    
    if not admin_key or not await verify_admin_key(admin_key):
        raise HTTPException(status_code=401, detail="Invalid admin key")
    
    from user_manager import fix_vk_tokens
    fixed_count = fix_vk_tokens()
    
    return {"status": "success", "fixed_count": fixed_count}

@app.post("/api/admin/fix-vk-token/{account_id}")
async def fix_single_vk_token_endpoint(account_id: str, request: Request):
    """Исправляет токен для конкретного VK аккаунта."""
    admin_key = request.headers.get("X-Admin-Key")
    if not admin_key:
        admin_key = request.cookies.get("admin_key")
    
    if not admin_key or not await verify_admin_key(admin_key):
        raise HTTPException(status_code=401, detail="Invalid admin key")
    
    # Получаем токен из базы данных
    from user_manager import get_db_connection, cipher
    from vk_utils import VKClient
    
    conn = await get_db_connection()
    
    query = 'SELECT token, proxy FROM vk_accounts WHERE id = ?'
    result = await conn.execute(query, (account_id,))
    account_row = await result.fetchone()
    
    if not account_row:
        await conn.close()
        raise HTTPException(status_code=404, detail="Account not found")
    
    # Преобразуем объект sqlite3.Row в словарь
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
                    await conn.close()
                    return {"success": True, "message": "Token is valid and working"}
                else:
                    logger.warning(f"Токен для аккаунта {account_id} имеет правильный формат, но не работает в API")
                    await conn.close()
                    return {"success": False, "message": "Token format is correct but API validation failed"}
        except Exception as e:
            logger.error(f"Ошибка при проверке токена через API: {str(e)}")
            await conn.close()
            return {"success": False, "message": f"Error during API validation: {str(e)}"}
    
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
                        update_query = 'UPDATE vk_accounts SET token = ? WHERE id = ?'
                        await conn.execute(update_query, (decrypted_once, account_id))
                        await conn.commit()
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
                                update_query = 'UPDATE vk_accounts SET token = ? WHERE id = ?'
                                await conn.execute(update_query, (decrypted_twice, account_id))
                                await conn.commit()
                                fixed = True
                            else:
                                logger.warning(f"Дважды расшифрованный токен имеет правильный формат, но не работает в API")
                                # Несмотря на ошибку API, обновляем токен если формат правильный
                                update_query = 'UPDATE vk_accounts SET token = ? WHERE id = ?'
                                await conn.execute(update_query, (decrypted_twice, account_id))
                                await conn.commit()
                                fixed = True
                    except Exception as e:
                        logger.error(f"Ошибка при проверке дважды расшифрованного токена через API: {str(e)}")
                        # Обновляем токен несмотря на ошибку API, если формат правильный
                        update_query = 'UPDATE vk_accounts SET token = ? WHERE id = ?'
                        await conn.execute(update_query, (decrypted_twice, account_id))
                        await conn.commit()
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
                                            update_query = 'UPDATE vk_accounts SET token = ? WHERE id = ?'
                                            await conn.execute(update_query, (token_part, account_id))
                                            await conn.commit()
                                            fixed = True
                                        else:
                                            logger.warning(f"Извлеченный токен имеет правильный формат, но не работает в API")
                                            # Обновляем токен несмотря на ошибку API
                                            update_query = 'UPDATE vk_accounts SET token = ? WHERE id = ?'
                                            await conn.execute(update_query, (token_part, account_id))
                                            await conn.commit()
                                            fixed = True
                                except Exception as e:
                                    logger.error(f"Ошибка при проверке извлеченного токена через API: {str(e)}")
                                    # Обновляем токен несмотря на ошибку API
                                    update_query = 'UPDATE vk_accounts SET token = ? WHERE id = ?'
                                    await conn.execute(update_query, (token_part, account_id))
                                    await conn.commit()
                                    fixed = True
            except Exception as inner_e:
                logger.error(f"Ошибка при второй расшифровке токена для аккаунта {account_id}: {str(inner_e)}")
        
        await conn.close()
        
        if fixed:
            return {"success": True, "message": "Token has been fixed"}
        else:
            return {"success": False, "message": "Token could not be fixed, format is invalid"}
    except Exception as e:
        logger.error(f"Ошибка при первой расшифровке токена для аккаунта {account_id}: {str(e)}")
        await conn.close()
        return {"success": False, "message": f"Error during decryption: {str(e)}"}

@app.post("/api/admin/normalize-vk-tokens")
async def normalize_vk_tokens_endpoint(request: Request):
    """Преобразует все токены VK в незашифрованный вид для хранения в базе данных."""
    admin_key = request.headers.get("X-Admin-Key")
    if not admin_key:
        admin_key = request.cookies.get("admin_key")
    
    if not admin_key or not await verify_admin_key(admin_key):
        raise HTTPException(status_code=401, detail="Invalid admin key")
    
    from user_manager import get_db_connection, cipher
    
    conn = await get_db_connection()
    
    # Получаем все токены VK
    query = 'SELECT id, token FROM vk_accounts'
    result = await conn.execute(query)
    account_rows = await result.fetchall()
    accounts = [dict(row) for row in account_rows]
    
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
                    update_query = 'UPDATE vk_accounts SET token = ? WHERE id = ?'
                    await conn.execute(update_query, (decrypted_token, account_id))
                    normalized_count += 1
                    logger.info(f"Токен для аккаунта {account_id} успешно нормализован")
                else:
                    # Пробуем расшифровать второй раз (токен мог быть зашифрован дважды)
                    try:
                        decrypted_twice = cipher.decrypt(decrypted_token.encode()).decode()
                        
                        if decrypted_twice.startswith('vk1.a.'):
                            # Обновляем токен в базе данных
                            update_query = 'UPDATE vk_accounts SET token = ? WHERE id = ?'
                            await conn.execute(update_query, (decrypted_twice, account_id))
                            normalized_count += 1
                            logger.info(f"Токен для аккаунта {account_id} был расшифрован дважды и успешно нормализован")
                        else:
                            # Пробуем найти подстроку 'vk1.a.' в дважды расшифрованном токене
                            if 'vk1.a.' in decrypted_twice:
                                start_pos = decrypted_twice.find('vk1.a.')
                                token_part = decrypted_twice[start_pos:]
                                
                                # Обновляем токен в базе данных
                                update_query = 'UPDATE vk_accounts SET token = ? WHERE id = ?'
                                await conn.execute(update_query, (token_part, account_id))
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
    
    await conn.commit()
    await conn.close()
    
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
    
    from user_manager import get_db_connection
    
    # Используем асинхронное подключение к базе данных
    conn = await get_db_connection()
    
    # Получаем данные аккаунта
    query = 'SELECT * FROM telegram_accounts WHERE id = ?'
    result = await conn.execute(query, (account_id,))
    account = await result.fetchone()
    
    if not account:
        await conn.close()
        raise HTTPException(status_code=404, detail="Аккаунт не найден")
    
    # Преобразуем sqlite3.Row в словарь
    account_dict = dict(account)
    await conn.close()
    
    return account_dict

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
    
    from user_manager import get_db_connection
    
    # Используем асинхронное подключение к базе данных
    conn = await get_db_connection()
    
    # Получаем данные аккаунта
    query = 'SELECT * FROM vk_accounts WHERE id = ?'
    result = await conn.execute(query, (account_id,))
    account = await result.fetchone()
    
    if not account:
        await conn.close()
        raise HTTPException(status_code=404, detail="Аккаунт не найден")
    
    # Преобразуем sqlite3.Row в словарь
    account_dict = dict(account)
    await conn.close()
    
    return account_dict

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
    from user_manager import get_db_connection
    conn = await get_db_connection()
    
    query = '''
        SELECT * FROM telegram_accounts 
        WHERE id = ?
    '''
    account = await conn.execute(query, (account_id,))
    account = await account.fetchone()
    
    if not account:
        logger.error(f"Аккаунт с ID {account_id} не найден")
        await conn.close()
        raise HTTPException(404, "Аккаунт не найден")
    
    # Преобразуем объект sqlite3.Row в словарь
    account_dict = dict(account)
    
    session_file = account_dict["session_file"]
    api_id = int(account_dict["api_id"])
    api_hash = account_dict["api_hash"]
    phone = account_dict["phone"]
    phone_code_hash = account_dict.get("phone_code_hash")
    proxy = account_dict.get("proxy")
    
    if not phone_code_hash:
        logger.error(f"Хеш кода не найден для аккаунта {account_id}")
        await conn.close()
        raise HTTPException(400, "Хеш кода не найден, пожалуйста, запросите код заново")
    
    logger.info(f"Авторизация с кодом {code} для аккаунта {phone} (ID: {account_id})")
    
    try:
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
            await client.disconnect()
            
            # Обновляем статус аккаунта
            update_query = '''
                UPDATE telegram_accounts
                SET status = ?
                WHERE id = ?
            '''
            await conn.execute(update_query, ('active', account_id))
            await conn.commit()
            await conn.close()
            
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
                SET status = ?
                WHERE id = ?
            '''
            await conn.execute(update_query, ('active', account_id))
            await conn.commit()
            
            # Получаем информацию о пользователе для обновления данных аккаунта
            me = await client.get_me()
            if me:
                username = getattr(me, 'username', None)
                if username:
                    update_username_query = '''
                        UPDATE telegram_accounts
                        SET username = ?
                        WHERE id = ?
                    '''
                    await conn.execute(update_username_query, (username, account_id))
                    await conn.commit()
            
            await client.disconnect()
            logger.info(f"Авторизация успешно выполнена для аккаунта {phone}")
            
            await conn.close()
            return {
                "account_id": account_id,
                "status": "success",
                "message": "Авторизация выполнена успешно"
            }
        except SessionPasswordNeededError:
            # Если для аккаунта настроена двухфакторная аутентификация
            logger.info(f"Для аккаунта {phone} требуется пароль 2FA")
            await client.disconnect()
            
            # Обновляем статус аккаунта
            update_query = '''
                UPDATE telegram_accounts
                SET status = ?
                WHERE id = ?
            '''
            await conn.execute(update_query, ('pending', account_id))
            await conn.commit()
            await conn.close()
            
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
            
            await client.disconnect()
            await conn.close()
            
            raise HTTPException(400, user_message)
    except Exception as e:
        logger.error(f"Ошибка при авторизации с кодом: {str(e)}")
        await conn.close()
        raise HTTPException(500, f"Ошибка при авторизации: {str(e)}")

# === Эндпоинт для запроса кода авторизации для существующего аккаунта ===
@app.post("/api/telegram/accounts/{account_id}/request-code")
async def request_telegram_auth_code(request: Request, account_id: str):
    """Запрашивает новый код авторизации для существующего аккаунта Telegram."""
    from user_manager import get_db_connection
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

    conn = await get_db_connection()
    query = 'SELECT phone, api_id, api_hash, session_file, proxy FROM telegram_accounts WHERE id = ?'
    result = await conn.execute(query, (account_id,))
    account = await result.fetchone()
    await conn.close() # Закрываем соединение сразу после получения данных

    if not account:
        logger.error(f"Аккаунт {account_id} не найден для запроса кода")
        raise HTTPException(404, "Аккаунт не найден")

    account_dict = dict(account)
    phone = account_dict.get("phone")
    api_id_str = account_dict.get("api_id")
    api_hash = account_dict.get("api_hash")
    session_file = account_dict.get("session_file")
    proxy = account_dict.get("proxy")

    if not all([phone, api_id_str, api_hash, session_file]):
        logger.error(f"Неполные данные для запроса кода аккаунта {account_id}")
        # Обновляем статус на error, если его еще нет
        try:
            conn_update = await get_db_connection()
            update_query = 'UPDATE telegram_accounts SET status = ? WHERE id = ? AND status != ?'
            await conn_update.execute(update_query, ('error', account_id, 'error'))
            await conn_update.commit()
            await conn_update.close()
        except Exception as db_err:
            logger.error(f"Не удалось обновить статус на 'error' для {account_id} из-за неполных данных при запросе кода: {db_err}")
        raise HTTPException(400, "Неполные данные аккаунта (phone, api_id, api_hash, session_file)")

    try:
        api_id = int(api_id_str)
    except (ValueError, TypeError):
        logger.error(f"Неверный формат api_id {api_id_str} для запроса кода аккаунта {account_id}")
        # Обновляем статус на error
        try:
            conn_update = await get_db_connection()
            update_query = 'UPDATE telegram_accounts SET status = ? WHERE id = ?'
            await conn_update.execute(update_query, ('error', account_id))
            await conn_update.commit()
            await conn_update.close()
        except Exception as db_err:
            logger.error(f"Не удалось обновить статус на 'error' для {account_id} из-за неверного api_id при запросе кода: {db_err}")
        raise HTTPException(400, f"Неверный формат api_id: {api_id_str}")

    client = None
    try:
        logger.info(f"Создание клиента для запроса кода, сессия: {session_file}")
        client = await create_telegram_client(session_file, api_id, api_hash, proxy)

        logger.info(f"Подключение клиента для запроса кода для аккаунта {account_id}")
        await client.connect()

        # Проверяем, вдруг уже авторизован?
        if await client.is_user_authorized():
             logger.warning(f"Попытка запроса кода для уже авторизованного аккаунта {account_id}")
             # Обновляем статус на active, если он был другим
             try:
                 conn_update = await get_db_connection()
                 update_query = 'UPDATE telegram_accounts SET status = ? WHERE id = ? AND status != ?'
                 await conn_update.execute(update_query, ('active', account_id, 'active'))
                 await conn_update.commit()
                 await conn_update.close()
             except Exception as db_err:
                 logger.error(f"Не удалось обновить статус на 'active' для {account_id} при запросе кода: {db_err}")
             raise HTTPException(400, "Аккаунт уже авторизован")

        logger.info(f"Отправка запроса кода для телефона {phone} (аккаунт {account_id})")
        sent_code_info = await client.send_code_request(phone)
        logger.info(f"Запрос кода для {account_id} отправлен успешно. Phone code hash: {sent_code_info.phone_code_hash}")

        # Сохраняем phone_code_hash в БД для использования при проверке кода
        # и обновляем статус на 'pending_code'
        conn = await get_db_connection()
        update_query = "UPDATE telegram_accounts SET phone_code_hash = ?, status = ? WHERE id = ?"
        await conn.execute(update_query, (sent_code_info.phone_code_hash, 'pending_code', account_id))
        await conn.commit()
        await conn.close()

        # Добавляем лог с используемым номером
        logger.info(f"Успешно запрошен код для номера {phone} (аккаунт {account_id})")
        return {"message": "Код отправлен успешно", "account_id": account_id, "status": "pending_code"}

    except errors.FloodWaitError as e:
        logger.error(f"Ошибка FloodWait при запросе кода для {account_id}: ждите {e.seconds} секунд")
        raise HTTPException(status_code=429, detail=f"Слишком много запросов. Попробуйте через {e.seconds} секунд.")
    except errors.PhoneNumberInvalidError:
        logger.error(f"Неверный номер телефона {phone} для аккаунта {account_id}")
        # Обновляем статус на 'error'
        try:
            conn_update = await get_db_connection()
            update_query = 'UPDATE telegram_accounts SET status = ? WHERE id = ?'
            await conn_update.execute(update_query, ('error', account_id))
            await conn_update.commit()
            await conn_update.close()
        except Exception as db_err:
             logger.error(f"Не удалось обновить статус на 'error' для {account_id} из-за неверного номера: {db_err}")
        raise HTTPException(status_code=400, detail="Неверный номер телефона.")
    except Exception as e:
        logger.error(f"Непредвиденная ошибка при запросе кода для {account_id}: {str(e)}", exc_info=True)
        # Обновляем статус на 'error'
        try:
            conn_update = await get_db_connection()
            update_query = 'UPDATE telegram_accounts SET status = ? WHERE id = ?'
            await conn_update.execute(update_query, ('error', account_id))
            await conn_update.commit()
            await conn_update.close()
        except Exception as db_err:
             logger.error(f"Не удалось обновить статус на 'error' для {account_id} после ошибки запроса кода: {db_err}")

        raise HTTPException(status_code=500, detail=f"Внутренняя ошибка сервера при запросе кода: {str(e)}")
    finally:
        if client:
            if hasattr(client, 'is_connected') and callable(client.is_connected) and await client.is_connected():
                await client.disconnect()
                logger.info(f"Клиент для запроса кода аккаунта {account_id} отключен.")


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
    # Проверяем админ-ключ
    admin_key = request.headers.get("X-Admin-Key")
    if not admin_key:
        admin_key = request.cookies.get("admin_key")
    
    if not admin_key or not await verify_admin_key(admin_key):
        raise HTTPException(status_code=401, detail="Неверный админ-ключ")
    
    from user_manager import get_db_connection
    conn = await get_db_connection()
    
    # Получаем все аккаунты
    query = "SELECT id, api_id, api_hash, phone, proxy, session_file, status, phone_code_hash FROM telegram_accounts"
    result = await conn.execute(query)
    accounts = await result.fetchall()
    
    result = []
    for account in accounts:
        account_dict = dict(account)
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
    
    await conn.close()
    
    return result

if __name__ == "__main__":
    port = int(os.getenv("PORT", 3030))
    uvicorn.run(app, host="0.0.0.0", port=port)