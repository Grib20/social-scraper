import asyncio
import json
import logging
import os
import traceback
import uuid
from typing import Optional

from fastapi import APIRouter, Depends, File, Form, Header, HTTPException, Request, UploadFile
from fastapi.responses import JSONResponse

import user_manager
from user_manager import add_telegram_account as add_tg_account_db # Используем переименованный импорт

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/telegram", # Префикс для всех маршрутов в этом файле
    tags=["telegram_v1"],      # Тег для документации API
)

# --- Константы ---
SESSION_DIR = "sessions"
# --- Убедимся, что папка сессий существует ---
os.makedirs(SESSION_DIR, exist_ok=True)
# --- Конец проверки папки ---


# === Эндпоинт для загрузки сессии и JSON ===
@router.post("/accounts/upload_session_json")
async def add_telegram_account_from_files(
    request: Request,
    session_file: UploadFile = File(..., description="Файл сессии Telegram (.session)"),
    json_file: UploadFile = File(..., description="JSON файл с метаданными аккаунта"),
    proxy: Optional[str] = Form(None, description="Прокси в формате socks5://user:pass@host:port или http://user:pass@host:port"),
    api_key: str = Header(...)
):
    """
    Добавляет аккаунт Telegram, используя файл сессии (.session) и JSON файл с метаданными.
    Автоматически извлекает app_id, app_hash, phone и другие параметры из JSON.
    """
    logger.info(f"Получен запрос на добавление TG аккаунта из файлов для api_key: {api_key[:5]}...")

    # 1. Проверка API ключа
    if not await user_manager.verify_api_key(api_key):
        logger.warning(f"Неверный API ключ: {api_key[:5]}... при добавлении TG аккаунта из файлов")
        raise HTTPException(status_code=403, detail="Неверный API ключ")

    json_data = {}
    account_data = {}
    session_filename = None
    saved_session_path = None

    try:
        # 2. Чтение и парсинг JSON файла
        try:
            json_content = await json_file.read()
            # Пытаемся декодировать с BOM, если есть
            try:
                decoded_content = json_content.decode('utf-8-sig')
            except UnicodeDecodeError:
                decoded_content = json_content.decode('utf-8') # Пробуем без BOM
            
            json_data = json.loads(decoded_content)
            logger.info(f"JSON файл {json_file.filename} успешно прочитан и распарсен.")
        except json.JSONDecodeError as e:
            logger.error(f"Ошибка парсинга JSON файла {json_file.filename}: {e}")
            raise HTTPException(status_code=400, detail=f"Некорректный формат JSON файла: {e}")
        except Exception as e:
            logger.error(f"Ошибка чтения JSON файла {json_file.filename}: {e}")
            raise HTTPException(status_code=500, detail=f"Ошибка чтения JSON файла: {e}")
        finally:
            await json_file.close() # Закрываем файл JSON

        # 3. Извлечение данных из JSON
        required_fields = ['app_id', 'app_hash']
        # Телефон важен для имени файла сессии
        phone = json_data.get('phone')
        if not phone:
             # Пытаемся взять из session_file в json, если phone отсутствует
             phone_from_session = json_data.get('session_file')
             if phone_from_session:
                 phone = str(phone_from_session) # Преобразуем в строку на всякий случай
                 logger.warning(f"Поле 'phone' отсутствует в JSON, используем 'session_file': {phone}")
             else:
                 logger.error("Отсутствует обязательное поле 'phone' или 'session_file' в JSON.")
                 raise HTTPException(status_code=400, detail="Отсутствует обязательное поле 'phone' или 'session_file' в JSON")
        else:
            phone = str(phone) # Убедимся, что это строка

        account_data['phone'] = phone

        for field in required_fields:
            value = json_data.get(field) # Используем get для безопасного извлечения
            if not value: # Проверяем на None или пустую строку/0
                logger.error(f"Отсутствует или пустое обязательное поле '{field}' в JSON файле.")
                raise HTTPException(status_code=400, detail=f"Отсутствует или пустое обязательное поле '{field}' в JSON файле.")
            account_data[field] = value

        # Извлечение необязательных, но рекомендуемых полей
        account_data['device_model'] = json_data.get('device')
        account_data['system_version'] = json_data.get('sdk')
        account_data['app_version'] = json_data.get('app_version')
        account_data['lang_code'] = json_data.get('lang_pack') or json_data.get('lang_code') # Пробуем оба варианта
        account_data['system_lang_code'] = json_data.get('system_lang_pack') or json_data.get('system_lang_pack') # Пробуем оба варианта
        # account_data['user_id'] = json_data.get('id') # user_id не хранится в telegram_accounts

        # Проверяем api_id на число
        try:
            account_data['api_id'] = int(account_data['api_id'])
        except (ValueError, TypeError):
            logger.error(f"Некорректное значение api_id: {account_data['api_id']}. Ожидалось целое число.")
            raise HTTPException(status_code=400, detail=f"Некорректное значение api_id: {account_data['api_id']}. Ожидалось целое число.")

        logger.info("Необходимые данные из JSON извлечены.")

        # 4. Проверка, существует ли аккаунт с таким телефоном
        user = await user_manager.get_user(api_key)
        if user:
            for existing_account in user.get('telegram_accounts', []):
                if existing_account.get('phone') == phone:
                     logger.warning(f"Аккаунт с телефоном {phone} уже существует для пользователя {api_key[:5]}...")
                     raise HTTPException(status_code=400, detail=f"Аккаунт с телефоном {phone} уже существует")

        # 5. Определение имени и пути для файла сессии
        # Убираем '+' из начала номера телефона для имени файла, если он есть
        clean_phone = phone.lstrip('+')
        session_filename = f"{clean_phone}.session"
        saved_session_path = os.path.join(SESSION_DIR, session_filename)
        account_data['session_file'] = saved_session_path # Сохраняем относительный путь в БД

        # 6. Сохранение файла сессии
        try:
            # Проверяем, что файл не слишком большой (например, 10MB)
            MAX_SESSION_SIZE = 10 * 1024 * 1024 
            size = 0
            with open(saved_session_path, "wb") as buffer:
                while content := await session_file.read(1024 * 1024): # Читаем по 1MB
                     size += len(content)
                     if size > MAX_SESSION_SIZE:
                         raise HTTPException(status_code=413, detail="Файл сессии слишком большой (макс. 10MB)")
                     buffer.write(content)
            logger.info(f"Файл сессии сохранен как: {saved_session_path} (размер: {size} байт)")
        except HTTPException as http_exc:
             raise http_exc # Перебрасываем ошибку размера файла
        except Exception as e:
            logger.error(f"Ошибка сохранения файла сессии {saved_session_path}: {e}")
            # Попытка удалить частично записанный файл
            if os.path.exists(saved_session_path):
                try: os.remove(saved_session_path)
                except OSError: pass
            raise HTTPException(status_code=500, detail=f"Ошибка сохранения файла сессии: {e}")
        finally:
            await session_file.close() # Закрываем файл сессии

        # 7. Добавляем прокси
        proxy_to_save = proxy # Используем переданный Form параметр
        if not proxy_to_save:
            proxy_to_save = json_data.get('proxy') # Или берем из JSON
            if proxy_to_save:
                logger.info(f"Используется прокси из JSON: {proxy_to_save}")
        elif proxy_to_save:
             logger.info(f"Используется прокси из Form: {proxy_to_save}")

        if proxy_to_save:
            # Простая валидация формата прокси (можно улучшить)
            # Допускаем разные схемы или без схемы (будет добавлена позже)
            # if not (proxy_to_save.startswith(('socks5://', 'socks4://', 'http://', 'https://'))):
            #     # Можно просто записывать как есть, Telethon разберется или выдаст ошибку
            #     logger.warning(f"Формат прокси '{proxy_to_save}' не стандартный, используется как есть.")
            account_data['proxy'] = proxy_to_save

        # Устанавливаем начальный статус
        account_data['status'] = 'active' # Считаем активным, раз сессия загружена
        account_data['is_active'] = True

        # 8. Добавление аккаунта в БД
        # Генерируем ID здесь, чтобы вернуть его
        account_id = str(uuid.uuid4()) 
        account_data['id'] = account_id # Добавляем ID в данные для сохранения

        success = await add_tg_account_db(api_key, account_data)
        if not success:
            logger.error("Ошибка добавления аккаунта Telegram в базу данных.")
            # Попытаемся удалить сохраненный файл сессии
            if saved_session_path and os.path.exists(saved_session_path):
                try:
                    os.remove(saved_session_path)
                    logger.info(f"Удален файл сессии {saved_session_path} из-за ошибки добавления в БД.")
                except OSError as remove_err:
                    logger.error(f"Не удалось удалить файл сессии {saved_session_path} после ошибки БД: {remove_err}")
            raise HTTPException(status_code=500, detail="Ошибка добавления аккаунта в базу данных")

        logger.info(f"Аккаунт Telegram {account_id} (телефон: {phone}) успешно добавлен для пользователя {api_key[:5]}...")

        # --- Попытка инициализировать и проверить клиент (опционально, но полезно) ---
        # Можно добавить вызов функции для создания клиента и проверки соединения здесь
        # Например: check_client_connection(account_id, account_data)
        # --- Конец проверки ---

        return JSONResponse(status_code=201, content={
            "message": "Аккаунт Telegram успешно добавлен",
            "account_id": account_id,
            "phone": phone,
            "status": account_data['status']
        })

    except HTTPException as http_exc:
        # Перебрасываем HTTP исключения
        # Удаляем файл сессии, если он был создан до ошибки
        if saved_session_path and os.path.exists(saved_session_path):
             try:
                 os.remove(saved_session_path)
                 logger.info(f"Удален файл сессии {saved_session_path} из-за HTTP ошибки: {http_exc.detail}")
             except OSError as remove_err:
                  logger.error(f"Не удалось удалить файл сессии {saved_session_path} после HTTP ошибки: {remove_err}")
        raise http_exc
    except Exception as e:
        logger.error(f"Неожиданная ошибка при добавлении TG аккаунта из файлов: {e}")
        logger.error(traceback.format_exc())
        # Попытка удалить файл сессии, если он был сохранен до ошибки
        if saved_session_path and os.path.exists(saved_session_path):
            try:
                os.remove(saved_session_path)
                logger.info(f"Удален файл сессии {saved_session_path} из-за неожиданной ошибки.")
            except OSError as remove_err:
                logger.error(f"Не удалось удалить файл сессии {saved_session_path} после ошибки: {remove_err}")
        raise HTTPException(status_code=500, detail=f"Внутренняя ошибка сервера: {e}")
    # Файлы уже закрыты в блоках finally или через UploadFile 