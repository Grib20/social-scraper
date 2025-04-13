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

    # Check if user exists before proceeding
    if not await user_manager.get_user(api_key):
        logger.warning(f"Попытка добавления аккаунта для несуществующего пользователя: {api_key[:5]}...")
        raise HTTPException(status_code=404, detail="Пользователь не найден")

    # === ИЗМЕНЕНО: Инициализация переменных и обработка JSON ===
    account_data = {}
    json_data = {}
    session_file_path = None # Инициализируем здесь

    try:
        # 1. Обработка JSON файла (если предоставлен)
        if json_file:
            logger.info(f"Обработка JSON-файла: {json_file.filename} для пользователя {api_key[:5]}...")
            try:
                json_content = await json_file.read()
                # Проверяем, что контент не пустой
                if not json_content:
                     logger.error("Предоставлен пустой JSON-файл.")
                     raise HTTPException(status_code=400, detail="JSON-файл не может быть пустым.")
                
                # Декодируем содержимое файла
                try:
                     json_data = json.loads(json_content.decode('utf-8'))
                     logger.info("JSON-файл успешно прочитан и распарсен.")
                except UnicodeDecodeError:
                     # Попытка с другой кодировкой, если UTF-8 не сработал
                     try:
                         json_data = json.loads(json_content.decode('cp1251')) # Популярная для Windows
                         logger.info("JSON-файл успешно прочитан и распарсен с кодировкой cp1251.")
                     except Exception as decode_err:
                         logger.error(f"Ошибка декодирования JSON-файла: {decode_err}. Файл не UTF-8 или cp1251.")
                         raise HTTPException(status_code=400, detail="Ошибка кодировки JSON-файла. Убедитесь, что он в UTF-8.")
                except json.JSONDecodeError as json_err:
                    logger.error(f"Ошибка парсинга JSON-файла: {json_err}")
                    raise HTTPException(status_code=400, detail=f"Невалидный JSON-файл: {json_err}")

                # 2. Извлечение и валидация данных из JSON
                phone = json_data.get('phone')
                if not phone or not isinstance(phone, str):
                    logger.error("Ключ 'phone' отсутствует или имеет неверный тип в JSON-файле.")
                    raise HTTPException(status_code=400, detail="Отсутствует или некорректен 'phone' в JSON-файле")
                account_data['phone'] = phone.strip() # Убираем лишние пробелы

                # Валидация app_id (должен быть целым числом или строкой, конвертируемой в целое)
                app_id_value = json_data.get('app_id')
                if app_id_value is None:
                    logger.error("Ключ 'app_id' отсутствует в JSON-файле.")
                    raise HTTPException(status_code=400, detail="Отсутствует 'app_id' в JSON-файле")
                
                api_id_int = None
                if isinstance(app_id_value, int):
                    api_id_int = app_id_value
                elif isinstance(app_id_value, str):
                    try:
                        api_id_int = int(app_id_value)
                    except ValueError:
                        logger.error(f"Не удалось конвертировать 'app_id' из строки в число: '{app_id_value}'")
                        raise HTTPException(status_code=400, detail="Некорректное значение для 'app_id'. Ожидается целое число или строка, содержащая целое число.")
                else:
                     logger.error(f"Некорректный тип для 'app_id' в JSON: {type(app_id_value)}. Ожидается строка или число.")
                     raise HTTPException(status_code=400, detail="Некорректный тип для 'app_id'. Ожидается строка или число.")

                account_data['api_id'] = api_id_int # Сохраняем как int
                logger.info(f"Получен api_id (int) из JSON: {account_data['api_id']}")

                # Валидация app_hash
                app_hash_value = json_data.get('app_hash')
                if not app_hash_value or not isinstance(app_hash_value, str):
                     logger.error("Ключ 'app_hash' отсутствует или имеет неверный тип в JSON-файле.")
                     raise HTTPException(status_code=400, detail="Отсутствует или некорректен 'app_hash' в JSON-файле")
                account_data['api_hash'] = app_hash_value.strip()
                logger.info(f"Получен api_hash из JSON: {account_data['api_hash']}")

            except HTTPException as http_exc:
                 raise http_exc # Перебрасываем HTTP ошибки валидации
            except Exception as e:
                logger.error(f"Неожиданная ошибка при обработке JSON-файла {json_file.filename}: {e}")
                raise HTTPException(status_code=500, detail=f"Ошибка обработки JSON-файла: {e}")
            finally:
                 # Убедимся, что JSON-файл закрыт, если он был предоставлен
                 if json_file:
                     await json_file.close()
        else:
            # JSON файл не обязателен, если данные приходят из другого источника (например, будущие доработки)
            # Но в данном случае он нужен для phone, api_id, api_hash
            logger.error("JSON-файл не был предоставлен.")
            raise HTTPException(status_code=400, detail="Необходим JSON-файл с данными аккаунта (phone, app_id, app_hash).")

        # 3. Проверка, существует ли аккаунт с таким телефоном
        user = await user_manager.get_user(api_key)
        if user:
            phone_to_check = account_data.get('phone')
            for existing_account in user.get('telegram_accounts', []):
                if existing_account.get('phone') == phone_to_check:
                     logger.warning(f"Аккаунт с телефоном {phone_to_check} уже существует для пользователя {api_key[:5]}...")
                     raise HTTPException(status_code=400, detail=f"Аккаунт с телефоном {phone_to_check} уже существует")

        # 4. Определение имени и пути для файла сессии
        phone = account_data.get('phone') # Берем уже проверенный телефон
        # Добавляем '+' если его нет (phone здесь гарантированно строка, но добавим проверку для линтера)
        if phone and not phone.startswith('+'):
            phone_with_plus = '+' + phone
        elif phone: # Если телефон есть, но уже с плюсом
            phone_with_plus = phone
        else: 
            # Этот случай не должен произойти из-за валидации выше, но обработаем
            logger.error("Критическая ошибка: телефон отсутствует на этапе формирования имени файла сессии.")
            raise HTTPException(status_code=500, detail="Внутренняя ошибка: не найден телефон для файла сессии.")

        sessions_dir = os.path.join('sessions', api_key) # Папка пользователя
        os.makedirs(sessions_dir, exist_ok=True)
        session_filename = f"{phone_with_plus}.session"
        session_file_path = os.path.join(sessions_dir, session_filename) # Определяем путь здесь
        logger.info(f"Путь для сохранения файла сессии: {session_file_path}")
        account_data['session_file'] = session_file_path # Сохраняем путь в БД

        # 5. Сохранение файла сессии (если предоставлен)
        if not session_file:
             logger.error("Файл сессии (.session) не был предоставлен.")
             raise HTTPException(status_code=400, detail="Необходим файл сессии (.session).")
        
        logger.info(f"Сохранение файла сессии: {session_file.filename} -> {session_file_path}")
        try:
            # Проверяем, что файл не слишком большой (например, 10MB)
            MAX_SESSION_SIZE = 10 * 1024 * 1024
            size = 0
            with open(session_file_path, "wb") as buffer:
                while content := await session_file.read(1024 * 1024): # Читаем по 1MB
                     size += len(content)
                     if size > MAX_SESSION_SIZE:
                         logger.error(f"Файл сессии {session_file.filename} слишком большой ({size} > {MAX_SESSION_SIZE})")
                         raise HTTPException(status_code=413, detail=f"Файл сессии слишком большой (макс. {MAX_SESSION_SIZE // 1024 // 1024}MB)")
                     buffer.write(content)
            logger.info(f"Файл сессии сохранен как: {session_file_path} (размер: {size} байт)")
        except HTTPException as http_exc:
             # Если ошибка размера файла, удаляем созданный пустой/частичный файл
             if os.path.exists(session_file_path):
                 try: os.remove(session_file_path)
                 except OSError: pass
             raise http_exc # Перебрасываем ошибку
        except Exception as e:
            logger.error(f"Ошибка сохранения файла сессии {session_file_path}: {e}")
            # Попытка удалить частично записанный файл
            if os.path.exists(session_file_path):
                try: os.remove(session_file_path)
                except OSError: pass
            raise HTTPException(status_code=500, detail=f"Ошибка сохранения файла сессии: {e}")
        finally:
            await session_file.close() # Закрываем файл сессии

        # 6. Добавляем прокси (после успешного сохранения сессии)
        proxy_to_save = proxy # Используем переданный Form параметр (приоритет)
        if proxy_to_save:
            logger.info(f"Используется прокси из Form: {proxy_to_save}")
        elif not proxy_to_save and json_data:
            proxy_to_save = json_data.get('proxy') # Или берем из JSON
            if proxy_to_save:
                logger.info(f"Используется прокси из JSON: {proxy_to_save}")

        if proxy_to_save:
             if isinstance(proxy_to_save, str):
                 account_data['proxy'] = proxy_to_save.strip()
                 logger.info(f"Прокси '{account_data['proxy']}' будет сохранен.")
             else:
                 logger.warning(f"Прокси из источника ({'Form' if proxy else 'JSON'}) имеет неверный тип: {type(proxy_to_save)}. Прокси не будет сохранен.")

        # 7. Устанавливаем начальный статус и ID
        account_data['status'] = 'need_check' # Требуется проверка после загрузки
        account_data['is_active'] = False # Неактивен до проверки
        account_id = str(uuid.uuid4())
        account_data['id'] = account_id
        
        # === ИСПРАВЛЕНИЕ: Сохраняем номер с '+' в account_data ===
        # phone_with_plus был создан ранее и содержит номер с '+'
        if phone_with_plus:
            account_data['phone'] = phone_with_plus 
            logger.info(f"Номер телефона для сохранения в БД: {account_data['phone']}")
        else:
            # Этот случай не должен произойти, но для безопасности
            logger.error("Критическая ошибка: phone_with_plus не был определен перед сохранением в БД.")
            raise HTTPException(status_code=500, detail="Внутренняя ошибка: не удалось определить номер телефона для сохранения.")
        # === КОНЕЦ ИСПРАВЛЕНИЯ ===

        # 8. Добавление аккаунта в БД
        logger.info(f"Попытка добавления аккаунта {account_id} в БД для пользователя {api_key[:5]}...")
        success = await add_tg_account_db(api_key, account_data)
        if not success:
            logger.error("Ошибка добавления аккаунта Telegram в базу данных.")
            # Попытаемся удалить сохраненный файл сессии, т.к. запись в БД не удалась
            if session_file_path and os.path.exists(session_file_path):
                try:
                    os.remove(session_file_path)
                    logger.info(f"Удален файл сессии {session_file_path} из-за ошибки добавления в БД.")
                except OSError as remove_err:
                    logger.error(f"Не удалось удалить файл сессии {session_file_path} после ошибки БД: {remove_err}")
            raise HTTPException(status_code=500, detail="Ошибка добавления аккаунта в базу данных")

        logger.info(f"Аккаунт Telegram {account_id} (телефон: {phone}) успешно добавлен для пользователя {api_key[:5]}... Статус: {account_data['status']}")
        # --- Конец ИЗМЕНЕНИЯ ---

        return JSONResponse(status_code=201, content={
            "message": "Аккаунт Telegram успешно добавлен",
            "account_id": account_id,
            "phone": phone,
            "status": account_data['status']
        })

    except HTTPException as http_exc:
        # Перебрасываем HTTP исключения
        # Удаляем файл сессии, если он был создан до ошибки
        if session_file_path and os.path.exists(session_file_path):
             try:
                 os.remove(session_file_path)
                 logger.info(f"Удален файл сессии {session_file_path} из-за HTTP ошибки: {http_exc.detail}")
             except OSError as remove_err:
                  logger.error(f"Не удалось удалить файл сессии {session_file_path} после HTTP ошибки: {remove_err}")
        raise http_exc
    except Exception as e:
        logger.error(f"Неожиданная ошибка при добавлении TG аккаунта из файлов: {e}")
        logger.error(traceback.format_exc())
        # Попытка удалить файл сессии, если он был сохранен до ошибки
        if session_file_path and os.path.exists(session_file_path):
            try:
                os.remove(session_file_path)
                logger.info(f"Удален файл сессии {session_file_path} из-за неожиданной ошибки.")
            except OSError as remove_err:
                logger.error(f"Не удалось удалить файл сессии {session_file_path} после ошибки: {remove_err}")
        raise HTTPException(status_code=500, detail=f"Внутренняя ошибка сервера: {e}")
    # Файлы уже закрыты в блоках finally или через UploadFile 