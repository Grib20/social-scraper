import asyncio
import pickle
import os
import sys
import logging
import time
import random
import subprocess
import tempfile
import shutil
from telethon import types
from telethon.errors import FloodWaitError, SessionPasswordNeededError
from collections import OrderedDict
import boto3
from PIL import Image, ImageDraw, ImageFont
import io
from aiojobs import create_scheduler
from dotenv import load_dotenv
from typing import Union, Callable, Any, Optional, List, Dict
import asyncpg

# Загружаем переменные окружения
load_dotenv()

# Настройка логирования
print("Инициализация media_utils.py")
logger = logging.getLogger(__name__)
# logger.setLevel(logging.INFO) # Уровень будет наследоваться от корневого

logger.info("Логирование в media_utils инициализировано (использует корневую конфигурацию)")

# Константы
S3_CACHE_FILE = "s3_file_cache.pkl"
MAX_CACHE_SIZE = 1000
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME', 'scraper')
S3_REGION = os.getenv('S3_REGION', 'ru-central1')
S3_LINK_TEMPLATE = os.getenv('S3_LINK_TEMPLATE', 'https://scraper.website.yandexcloud.net/{filename}')
MAX_FILE_SIZE = 50 * 1024 * 1024

# S3 клиент 
s3_client = None

# Планировщик для фоновых задач
scheduler = None

# Кэш файлов S3
s3_file_cache = OrderedDict()

# Семафоры
DOWNLOAD_SEMAPHORE = asyncio.Semaphore(2)
UPLOAD_SEMAPHORE = asyncio.Semaphore(5)

# Счетчик и время для лимита скачиваний
download_counter = 0
last_download_time = time.time()
DOWNLOAD_RATE_LIMIT = 20
MIN_DOWNLOAD_DELAY = 0.5

# История FloodWait
flood_wait_history = []
MAX_FLOOD_HISTORY = 5

# Очередь задач загрузки
upload_queue = asyncio.Queue(maxsize=100)
is_worker_running = False

# Добавляем новый семафор для параллельных скачиваний
PARALLEL_DOWNLOAD_SEMAPHORE = asyncio.Semaphore(5)  # Максимум 5 одновременных задач скачивания

# Директория для временного скачивания медиа-файлов
MEDIA_DOWNLOAD_DIR = os.path.join(os.getcwd(), 'media_downloads')
if not os.path.exists(MEDIA_DOWNLOAD_DIR):
    os.makedirs(MEDIA_DOWNLOAD_DIR)


# --- Новая функция для фоновой обработки ---
async def process_single_media_background(account_id: str, media_object, file_id, s3_filename):
    """Скачивает медиафайл, загружает в S3 и обновляет кэш (Фоновая задача)."""
    logger = logging.getLogger(__name__)
    logger.info(f"BG Start: Начинаем обработку медиа {file_id} -> {s3_filename} с использованием аккаунта {account_id}")
    temp_dir = None
    client = None # Инициализируем как None

    try:
        # --- Получение клиента из пула ---
        # Импортируем app здесь, чтобы получить доступ к глобальному telegram_pool
        import app 
        logger.debug(f"BG Get Client: Запрос клиента для {account_id} из пула...")
        # Обращаемся к пулу через app.telegram_pool
        client = app.telegram_pool.get_client(account_id) 

        if not client:
            logger.error(f"BG Client Error: Не удалось получить клиент для {account_id} из пула. Задача прервана.")
            return

        # --- Проверка и установка соединения ---
        is_connected = False
        try:
            if client.is_connected():
                is_connected = True
                # Дополнительно проверяем авторизацию, если уже подключены
                if not await client.is_user_authorized():
                    logger.error(f"BG Auth Error: Клиент {account_id} подключен, но НЕ АВТОРИЗОВАН. Задача прервана.")
                    return # Прерываем, если не авторизован
                logger.debug(f"BG Connect: Клиент {account_id} уже подключен и авторизован.")
            else:
                logger.info(f"BG Connect: Клиент {account_id} не подключен. Подключаемся...")
                await client.connect()
                if not await client.is_user_authorized():
                    logger.error(f"BG Auth Error: Клиент {account_id} НЕ АВТОРИЗОВАН после подключения. Задача прервана.")
                    await client.disconnect() # Отключаем, если не удалось авторизоваться
                    return
                is_connected = True
                logger.info(f"BG Connect: Клиент {account_id} успешно подключен и авторизован.")
        except SessionPasswordNeededError:
             logger.error(f"BG Auth Error: Клиент {account_id} требует 2FA пароль. Невозможно продолжить. Задача прервана.")
             # Пытаемся отключить, если возможно
             try: await client.disconnect() 
             except: pass
             return
        except Exception as conn_err:
            logger.error(f"BG Connect Error: Ошибка при проверке/установке соединения для клиента {account_id}: {conn_err}", exc_info=True)
            return # Прерываем задачу, если не удалось подключиться

        if not is_connected:
             logger.error(f"BG Connect Error: Соединение для клиента {account_id} не установлено. Задача прервана.")
             return
        # --- Конец получения и проверки клиента ---

        # --- Дальнейшая логика скачивания и загрузки (как и была) ---
        # Проверяем кэш S3 перед скачиванием
        cache_hit = False
        if file_id in s3_file_cache:
            cached_s3_file = s3_file_cache.get(file_id) # Используем get для безопасности
            if isinstance(cached_s3_file, str) and await check_s3_file(cached_s3_file):
                 logger.debug(f"BG Cache HIT: Файл {file_id} уже в S3 ({cached_s3_file}), выход.")
                 # Перемещаем в конец OrderedDict для LRU-подобного поведения
                 s3_file_cache.move_to_end(file_id)
                 cache_hit = True
            elif isinstance(cached_s3_file, dict) and cached_s3_file.get('is_preview'): # Обработка заглушек
                 logger.debug(f"BG Cache HIT: Файл {file_id} является заглушкой/превью, выход.")
                 s3_file_cache.move_to_end(file_id)
                 cache_hit = True
            else:
                 logger.debug(f"BG Cache Invalid: Запись для {file_id} в кэше некорректна или файл не найден в S3 ({cached_s3_file}).")
                 # Удаляем некорректную запись из кэша
                 if file_id in s3_file_cache:
                     try:
                         del s3_file_cache[file_id]
                     except KeyError:
                         pass # Уже удален

        # Если в кэше не нашли, проверяем напрямую S3 (на случай рассинхронизации кэша)
        if not cache_hit and await check_s3_file(s3_filename):
            logger.debug(f"BG S3 Check HIT: Файл {s3_filename} (ID: {file_id}) уже в S3, обновляем кэш.")
            s3_file_cache[file_id] = s3_filename
            s3_file_cache.move_to_end(file_id)
            cache_hit = True
            # Опционально: обрезать кэш
            while len(s3_file_cache) > MAX_CACHE_SIZE:
                s3_file_cache.popitem(last=False)

        if cache_hit:
            return # Файл уже обработан или существует

        # Если файла нет, скачиваем и загружаем
        logger.info(f"BG Start: Начинаем обработку медиа {file_id} -> {s3_filename}")
        temp_dir = None
        local_path = None
        try:
            async with DOWNLOAD_SEMAPHORE: # Используем семафор для скачивания
                temp_dir = tempfile.mkdtemp(dir=MEDIA_DOWNLOAD_DIR) # Создаем во временной директории
                local_path = os.path.join(temp_dir, os.path.basename(s3_filename)) # Используем имя S3 для локального файла

                # Обработка больших видео (создание заглушки вместо скачивания)
                file_size = getattr(media_object, 'size', 0) if hasattr(media_object, 'size') else 0
                is_video = False
                if hasattr(media_object, 'mime_type') and media_object.mime_type and media_object.mime_type.startswith('video/'):
                     is_video = True

                if is_video and file_size > MAX_FILE_SIZE:
                    logger.info(f"BG Large Video: Видео {file_id} ({file_size} байт) превышает лимит, создаем заглушку.")
                    placeholder_path = os.path.join(temp_dir, f"placeholder_{file_id}.jpg")
                    thumb_s3_filename = s3_filename.replace(os.path.splitext(s3_filename)[1], "_thumb.jpg")

                    # Создаем заглушку
                    try:
                         width, height = 640, 360
                         img = Image.new('RGB', (width, height), color=(50, 50, 50))
                         draw = ImageDraw.Draw(img)
                         try: # Пытаемся загрузить шрифт
                             font_dir = os.path.join(os.environ.get('WINDIR', 'C:\\Windows'), 'Fonts')
                             font_path_arial = os.path.join(font_dir, 'Arial.ttf')
                             font_path_verdana = os.path.join(font_dir, 'Verdana.ttf') # Запасной вариант
                             if os.path.exists(font_path_arial):
                                 title_font = ImageFont.truetype(font_path_arial, 30)
                                 regular_font = ImageFont.truetype(font_path_arial, 20)
                             elif os.path.exists(font_path_verdana):
                                  title_font = ImageFont.truetype(font_path_verdana, 30)
                                  regular_font = ImageFont.truetype(font_path_verdana, 20)
                             else:
                                 logger.warning("Шрифты Arial и Verdana не найдены, используется шрифт по умолчанию.")
                                 title_font = ImageFont.load_default()
                                 regular_font = ImageFont.load_default()
                         except Exception as font_err:
                             logger.warning(f"Ошибка загрузки шрифта ({font_err}), используется шрифт по умолчанию.")
                             title_font = ImageFont.load_default()
                             regular_font = ImageFont.load_default()

                         file_size_mb = round(file_size / (1024 * 1024), 1)
                         title_text = f"Большой видеофайл ({file_size_mb} МБ)"
                         subtitle_text = "Просмотр доступен только в оригинальном посте"

                         # Используем textbbox для центрирования (требует Pillow >= 8.0.0)
                         try:
                            title_box = draw.textbbox((0, 0), title_text, font=title_font)
                            subtitle_box = draw.textbbox((0, 0), subtitle_text, font=regular_font)
                            title_width = title_box[2] - title_box[0]
                            title_height = title_box[3] - title_box[1]
                            subtitle_width = subtitle_box[2] - subtitle_box[0]
                            # subtitle_height = subtitle_box[3] - subtitle_box[1] # не используется

                            title_x = (width - title_width) / 2
                            title_y = height / 2 - title_height # Сдвигаем выше
                            subtitle_x = (width - subtitle_width) / 2
                            subtitle_y = height / 2 + 10 # Оставляем ниже

                         except AttributeError: # Для старых версий Pillow
                            logger.warning("Метод textbbox недоступен, используется textlength для центрирования.")
                            title_width = draw.textlength(title_text, font=title_font)
                            subtitle_width = draw.textlength(subtitle_text, font=regular_font)
                            title_x = (width - title_width) / 2
                            title_y = height / 2 - 30
                            subtitle_x = (width - subtitle_width) / 2
                            subtitle_y = height / 2 + 10

                         draw.text((title_x, title_y), title_text, font=title_font, fill=(255, 255, 255))
                         draw.text((subtitle_x, subtitle_y), subtitle_text, font=regular_font, fill=(200, 200, 200))

                         # Рисуем значок "play" для видео
                         icon_y_center = title_y - 40 # Над заголовком
                         triangle_points = [
                            (width / 2 - 15, icon_y_center - 20), # top left
                            (width / 2 + 15, icon_y_center),     # middle right
                            (width / 2 - 15, icon_y_center + 20)  # bottom left
                         ]
                         draw.polygon(triangle_points, fill=(255, 255, 255))

                         img.save(placeholder_path, "JPEG", quality=90)

                         # Загружаем заглушку
                         upload_success, _ = await upload_to_s3(placeholder_path, thumb_s3_filename, check_size=False)
                         if upload_success:
                             logger.debug(f"BG Placeholder Upload: Заглушка для {file_id} загружена: {thumb_s3_filename}")
                             preview_info = {'is_preview': True, 'thumbnail': thumb_s3_filename, 'size': file_size}
                             s3_file_cache[file_id] = preview_info
                             s3_file_cache.move_to_end(file_id) # Обновляем порядок в кэше
                             while len(s3_file_cache) > MAX_CACHE_SIZE: s3_file_cache.popitem(last=False)
                         else:
                              logger.error(f"BG Placeholder Upload Error: Не удалось загрузить заглушку {thumb_s3_filename}")

                    except Exception as e:
                        logger.error(f"BG Placeholder Error: Ошибка при создании заглушки для {file_id}: {e}", exc_info=True)
                    # Выходим после обработки заглушки
                    return

                # Обычное скачивание (не большое видео)
                logger.debug(f"BG Download: Начинаем скачивание медиа {file_id} в {local_path}")
                try:
                    downloaded_path = await client.download_media(media_object, local_path)
                except ConnectionError as ce:
                    logger.warning(f"BG Download: Потеряно соединение, пробуем переподключиться для {file_id}: {ce}")
                    try:
                        await client.connect()
                        if not await client.is_user_authorized():
                            logger.error(f"BG Auth Error: Клиент {account_id} не авторизован после переподключения.")
                            return
                        downloaded_path = await client.download_media(media_object, local_path)
                    except Exception as reconnect_err:
                        logger.error(f"BG Download Error: Не удалось переподключиться и скачать медиа {file_id}: {reconnect_err}")
                        return
                # Проверяем, что download_media вернул путь и файл существует
                if downloaded_path and os.path.exists(downloaded_path):
                    local_path = downloaded_path # Используем путь, возвращенный download_media
                    logger.debug(f"BG Download OK: Медиа {file_id} скачано в {local_path}")

                    # Загрузка в S3
                    logger.debug(f"BG Upload: Файл {local_path} существует, начинаем загрузку в S3 как {s3_filename}")
                    # Определяем, нужно ли оптимизировать (только для фото)
                    optimize_upload = False
                    if hasattr(media_object, 'mime_type') and media_object.mime_type:
                         if media_object.mime_type.lower() in ['image/jpeg', 'image/png']:
                              optimize_upload = True

                    upload_success, _ = await upload_to_s3(local_path, s3_filename, optimize=optimize_upload)

                    if upload_success:
                        # Обновляем кэш после успешной загрузки
                        s3_file_cache[file_id] = s3_filename
                        s3_file_cache.move_to_end(file_id) # Обновляем порядок в кэше
                        logger.info(f"BG Upload OK: Файл {s3_filename} (ID: {file_id}) успешно загружен и добавлен в кэш.")
                        # Обрезаем кэш, если он превысил размер
                        while len(s3_file_cache) > MAX_CACHE_SIZE:
                             s3_file_cache.popitem(last=False)
                             # Возможно, стоит периодически сохранять кэш
                             # await save_cache()
                    else:
                        logger.error(f"BG Upload Error: Не удалось загрузить файл {local_path} в S3 как {s3_filename}")
                else:
                    logger.error(f"BG Download Error: Файл НЕ найден по пути '{downloaded_path}' после попытки скачивания медиа {file_id}")

        except FloodWaitError as flood_e:
            logger.warning(f"BG FloodWait: Flood wait на {flood_e.seconds} секунд при обработке медиа {file_id}. Задача не будет повторена автоматически.")
            # Здесь можно добавить логику повторного добавления задачи в очередь или другую обработку
        except Exception as e:
            logger.error(f"BG Error: Ошибка при фоновой обработке медиа {file_id} -> {s3_filename}: {e}", exc_info=True)
        finally:
            # Удаляем временную директорию и ее содержимое
            if temp_dir and os.path.exists(temp_dir):
                try:
                    shutil.rmtree(temp_dir, ignore_errors=True)
                    logger.debug(f"BG Cleanup: Временная директория {temp_dir} удалена для медиа {file_id}")
                except Exception as cleanup_err:
                     logger.error(f"BG Cleanup Error: Ошибка при удалении {temp_dir}: {cleanup_err}")

    except asyncio.CancelledError:
        logger.info("Процесс обработки очереди загрузки отменен")
    except Exception as e:
        logger.error(f"Фатальная ошибка в процессе обработки очереди: {e}", exc_info=True)
    

# --- Функции обновления статистики и инициализации --- 

async def update_account_usage(api_key, account_id, platform):
    """Обновляет статистику использования аккаунта через Redis"""
    try:
        from redis_utils import update_account_usage_redis
        # Вызываем функцию обновления статистики через Redis
        await update_account_usage_redis(api_key, account_id, platform)
    except ImportError:
        # Если Redis не доступен, пытаемся использовать обычное обновление
        try:
            from user_manager import update_account_usage as real_update
            await real_update(api_key, account_id, platform)
        except ImportError:
            logger.debug(f"Функции update_account_usage и update_account_usage_redis недоступны для {account_id}")
            pass

def init_s3_client():
    global s3_client
    s3_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    s3_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    s3_endpoint_url = os.getenv('S3_ENDPOINT_URL', 'https://storage.yandexcloud.net')
    
    if not s3_access_key_id or not s3_secret_access_key:
        logger.error("Ключи доступа S3 (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY) не найдены в .env")
        return
        
    s3_client = boto3.client(
        's3',
        region_name=S3_REGION,
        aws_access_key_id=s3_access_key_id,
        aws_secret_access_key=s3_secret_access_key,
        endpoint_url=s3_endpoint_url
    )
    logger.info("S3 клиент инициализирован")

def load_cache():
    global s3_file_cache
    if os.path.exists(S3_CACHE_FILE):
        try:
            with open(S3_CACHE_FILE, "rb") as f:
                s3_file_cache = pickle.load(f)
            if not isinstance(s3_file_cache, OrderedDict):
                s3_file_cache = OrderedDict(s3_file_cache)
            logger.info(f"Загружен кэш из {S3_CACHE_FILE}, размер: {len(s3_file_cache)} записей")
        except Exception as e:
            logger.error(f"Ошибка при загрузке кэша из {S3_CACHE_FILE}: {e}")
            s3_file_cache = OrderedDict()
    else:
        s3_file_cache = OrderedDict()

async def save_cache():
    loop = asyncio.get_running_loop()
    try:
        await loop.run_in_executor(None, lambda: pickle.dump(dict(s3_file_cache), open(S3_CACHE_FILE, "wb")))
        logger.info(f"Кэш сохранён в {S3_CACHE_FILE}")
    except Exception as e:
        logger.error(f"Ошибка при асинхронном сохранении кэша: {e}")

async def init_scheduler():
    global scheduler
    init_s3_client() # Инициализируем S3 клиент при старте
    load_cache()
    scheduler = await create_scheduler()
    logger.info("Планировщик инициализирован")
    # Запускаем воркер, если в очереди есть задачи (после загрузки кэша)
    if not is_worker_running and not upload_queue.empty():
         asyncio.create_task(process_upload_queue())

async def close_scheduler():
    global scheduler
    if scheduler:
        await scheduler.close()
        scheduler = None
        logger.info("Планировщик закрыт")
    if not upload_queue.empty():
        logger.info(f"Ожидание завершения {upload_queue.qsize()} задач в очереди загрузки...")
        await upload_queue.join()
        logger.info("Все задачи в очереди загрузки завершены")
    await save_cache() # Сохраняем кэш при выходе

# --- Функции обработки медиа --- 

async def optimize_image(file_path, output_path):
    """Сжимает изображение, сохраняя его в output_path."""
    try:
        with Image.open(file_path) as img:
            # Конвертируем в RGB, если изображение в RGBA (например, PNG с прозрачностью)
            if img.mode == 'RGBA':
                img = img.convert('RGB')
            # Сжимаем изображение
            img.save(output_path, 'JPEG', quality=80, optimize=True)
        logger.debug(f"Изображение сжато: {output_path}")
    except Exception as e:
        logger.error(f"Ошибка при сжатии изображения {file_path}: {e}")
        # Если сжатие не удалось, копируем оригинальный файл
        import shutil
        shutil.copyfile(file_path, output_path)

async def create_video_preview(video_path, thumbnail_path, preview_video_path):
    """Создает превью видео: кадр и короткий видеоклип."""
    loop = asyncio.get_running_loop()
    
    try:
        # Создаем превью-кадр (миниатюру) из видео
        ffmpeg_thumb_cmd = [
            'ffmpeg', '-i', video_path, 
            '-ss', '00:00:02', '-vframes', '1', 
            '-vf', 'scale=320:-1',
            thumbnail_path, '-y'
        ]
        
        await loop.run_in_executor(
            None, 
            lambda: subprocess.run(ffmpeg_thumb_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        )
        
        # Создаем короткий видеоклип (5 секунд)
        ffmpeg_preview_cmd = [
            'ffmpeg', '-i', video_path,
            '-ss', '00:00:00', '-t', '5', 
            '-vf', 'scale=640:-1', 
            '-c:v', 'libx264', '-preset', 'fast', '-crf', '30',
            '-c:a', 'aac', '-b:a', '96k',
            preview_video_path, '-y'
        ]
        
        await loop.run_in_executor(
            None, 
            lambda: subprocess.run(ffmpeg_preview_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        )
        
        logger.debug(f"Создано превью для видео: {thumbnail_path} и {preview_video_path}")
        return True
    except Exception as e:
        logger.error(f"Ошибка при создании превью видео: {e}")
        return False

async def upload_to_s3(file_path, s3_filename, optimize=False, check_size=True):
    if s3_client is None:
        logger.error("S3 клиент не инициализирован. Загрузка невозможна.")
        return False, {"reason": "s3_client_not_initialized"}
    
    async with UPLOAD_SEMAPHORE:
        try:
            if check_size and os.path.exists(file_path):
                file_size = os.path.getsize(file_path)
                if file_size > MAX_FILE_SIZE:
                    if file_path.lower().endswith(('.mp4', '.mov', '.avi')):
                        logger.info(f"Видео {file_path} ({file_size} байт) превышает лимит, создаем текстовую заглушку")
                        temp_dir = tempfile.mkdtemp()
                        try:
                            # Создаем текстовую заглушку вместо превью видео
                            placeholder_path = os.path.join(temp_dir, f"placeholder_{os.path.basename(file_path)}.jpg")
                            
                            # Используем PIL для создания заглушки с текстом
                            from PIL import Image, ImageDraw, ImageFont
                            
                            # Создаем изображение-заглушку
                            width, height = 640, 360  # Стандартное соотношение 16:9
                            img = Image.new('RGB', (width, height), color=(50, 50, 50))
                            draw = ImageDraw.Draw(img)
                            
                            # Пытаемся загрузить шрифт, или используем default
                            try:
                                # Попробуем использовать системный шрифт
                                font_path = os.path.join(os.environ.get('WINDIR', ''), 'Fonts', 'Arial.ttf')
                                if os.path.exists(font_path):
                                    title_font = ImageFont.truetype(font_path, 30)
                                    regular_font = ImageFont.truetype(font_path, 20)
                                else:
                                    title_font = ImageFont.load_default()
                                    regular_font = ImageFont.load_default()
                            except Exception:
                                title_font = ImageFont.load_default()
                                regular_font = ImageFont.load_default()
                                
                            # Определяем размер файла в МБ
                            file_size_mb = round(file_size / (1024 * 1024), 1)
                            
                            # Добавляем текст
                            title_text = f"Большой видеофайл ({file_size_mb} МБ)"
                            subtitle_text = "Просмотр доступен только в оригинальном посте"
                            
                            # Центрируем текст
                            title_width = draw.textlength(title_text, font=title_font)
                            subtitle_width = draw.textlength(subtitle_text, font=regular_font)
                            
                            # Рисуем текст
                            draw.text(((width - title_width) / 2, height / 2 - 30), title_text, font=title_font, fill=(255, 255, 255))
                            draw.text(((width - subtitle_width) / 2, height / 2 + 10), subtitle_text, font=regular_font, fill=(200, 200, 200))
                            
                            # Добавляем значок видео
                            draw.polygon([(width/2 - 40, height/2 - 80), (width/2 + 40, height/2 - 80), 
                                        (width/2 + 40, height/2 - 160), (width/2 - 40, height/2 - 160)], 
                                        fill=(200, 50, 50))
                            draw.polygon([(width/2 - 15, height/2 - 120), (width/2 + 25, height/2 - 140), 
                                        (width/2 - 15, height/2 - 160)], fill=(255, 255, 255))
                            
                            # Сохраняем изображение
                            img.save(placeholder_path, "JPEG", quality=90)
                            
                            # Загружаем заглушку в S3
                            thumb_s3_filename = s3_filename.replace(os.path.splitext(s3_filename)[1], "_thumb.jpg")
                            s3_client.upload_file(placeholder_path, S3_BUCKET_NAME, thumb_s3_filename)
                            logger.debug(f"Заглушка для большого видео загружена в S3: {thumb_s3_filename}")
                            
                            preview_info = {
                                    'is_preview': True,
                                    'thumbnail': thumb_s3_filename,
                                    'size': file_size
                                }
                            s3_file_cache[s3_filename] = preview_info 
                            return True, preview_info
                        except Exception as e:
                            logger.error(f"Ошибка при создании заглушки для {file_path}: {e}")
                        finally:
                            shutil.rmtree(temp_dir, ignore_errors=True)
                        return False, {'is_preview': False, 'reason': 'preview_creation_failed', 'size': file_size}
                    else:
                        logger.warning(f"Файл {file_path} ({file_size} байт) пропущен из-за превышения размера")
                        return False, {'is_preview': False, 'reason': 'size_limit_exceeded', 'size': file_size}
            
            if optimize and file_path.lower().endswith(('.jpg', '.jpeg', '.png')):
                optimized_path = file_path + "_optimized.jpg"
                await optimize_image(file_path, optimized_path)
                s3_client.upload_file(optimized_path, S3_BUCKET_NAME, s3_filename)
                os.remove(optimized_path)
                logger.debug(f"Сжатый файл {s3_filename} загружен в S3")
            else:
                s3_client.upload_file(file_path, S3_BUCKET_NAME, s3_filename)
                logger.debug(f"Файл {s3_filename} загружен в S3 без сжатия")
            return True, None
        except Exception as e:
            logger.error(f"Ошибка при загрузке файла {s3_filename} в S3: {e}")
            return False, {"reason": "upload_error", "error": str(e)}

async def check_s3_file(s3_filename):
    if s3_client is None: return False
    try:
        s3_client.head_object(Bucket=S3_BUCKET_NAME, Key=s3_filename)
        return True
    except s3_client.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
             return False # Файл не найден
        else:
             logger.error(f"Ошибка при проверке файла {s3_filename} в S3: {e}")
             return False # Другая ошибка
    except Exception as e:
         logger.error(f"Неожиданная ошибка при проверке файла {s3_filename} в S3: {e}")
         return False

async def calculate_download_delay():
    global download_counter, last_download_time, flood_wait_history

    # Проверяем, не превышен ли лимит скачиваний в минуту
    current_time = time.time()
    if current_time - last_download_time >= 60:
        download_counter = 0
        last_download_time = current_time
    if download_counter >= DOWNLOAD_RATE_LIMIT:
        return 60 - (current_time - last_download_time)

    # Если есть ошибки FloodWait, увеличиваем задержку
    if flood_wait_history:
        max_wait = max(error['wait_time'] for error in flood_wait_history)
        return max(max_wait, MIN_DOWNLOAD_DELAY)

    return MIN_DOWNLOAD_DELAY

# --- Новая логика get_media_info и обработчика очереди --- 

async def get_media_info(client, msg, album_messages=None, non_blocking=True) -> Optional[Dict]:
    """Получает информацию о медиафайлах в сообщении.
    
    Args:
        client: Клиент Telegram
        msg: Сообщение Telegram
        album_messages: Список сообщений альбома (если есть)
        non_blocking: Если True, не блокирует выполнение и сразу возвращает ссылки, даже если файлы не загружены в S3
        
    Returns:
        Dict: Информация о медиафайлах
    """
    try:
        # Если передан список сообщений альбома, используем его
        # Иначе, проверяем, является ли сообщение частью альбома и получаем все сообщения альбома
        messages_to_process = []
        if album_messages:
            messages_to_process = album_messages
        elif hasattr(msg, 'grouped_id') and msg.grouped_id:
            from telegram_utils import get_album_messages
            messages_to_process = await get_album_messages(client, msg.input_chat, msg)
        else:
            messages_to_process = [msg]
        
        # Базовая информация о медиа
        media_info = {
            'message_id': msg.id,
            'has_media': any(hasattr(m, 'media') and m.media for m in messages_to_process),
            'media_count': len([m for m in messages_to_process if hasattr(m, 'media') and m.media]),
            'is_album': hasattr(msg, 'grouped_id') and msg.grouped_id is not None,
            'media_urls': []
        }
        
        # Если нет медиа, возвращаем базовую информацию
        if not media_info['has_media']:
            return media_info
            
        # Если есть медиа, обрабатываем
        processed_file_ids = set()  # Множество для отслеживания уже обработанных file_id
        tasks_to_queue = []  # Список задач на загрузку

        cache_hit = False
            
        # Чтобы не дублировать задачи для одного file_id
        for current_msg in messages_to_process:
            if not current_msg.media: continue

            media = current_msg.media
            media_type = 'unknown'
            file_id = None
            media_object = None # Объект Telethon для скачивания
            file_ext = '.bin'
            mime_type = 'application/octet-stream'

            # Определяем тип медиа и получаем объект для скачивания
            if isinstance(media, types.MessageMediaPhoto) and media.photo:
                media_type = 'photo'
                media_object = media.photo
                if hasattr(media_object, 'id'):
                    file_id = str(media_object.id)
                    file_ext = '.jpg'
                    mime_type = 'image/jpeg'
                else:
                    logger.warning("Фото без id, пропускаем")
                    continue
            elif isinstance(media, types.MessageMediaDocument) and media.document:
                media_object = media.document
                if hasattr(media_object, 'id'):
                    file_id = str(media_object.id)
                    mime_type = getattr(media_object, 'mime_type', 'application/octet-stream').lower()  

                    if mime_type.startswith('video/'):
                        media_type = 'video'
                        file_ext = '.mp4'
                    elif mime_type.startswith('image/'):
                        media_type = 'photo'
                        file_ext = '.jpg'
                    elif mime_type.startswith('audio/'):
                        media_type = 'audio'
                        file_ext = '.mp3'
                    else:
                        media_type = 'document'
                        file_ext = '.bin'  # По умолчанию   

                        # Безопасно получаем расширение из атрибутов
                        if hasattr(media_object, 'attributes'):
                            try:
                                attributes = getattr(media_object, 'attributes', [])
                                fname_attr = next((attr.file_name for attr in attributes
                                                 if isinstance(attr, types.DocumentAttributeFilename)), None)
                                if fname_attr:
                                    _, _ext = os.path.splitext(fname_attr)
                                    if _ext: 
                                        file_ext = _ext.lower()
                            except Exception as e:
                                logger.debug(f"Ошибка при получении имени файла: {e}")  

                        # Уточняем тип для GIF/стикеров
                        if hasattr(media_object, 'attributes'):
                            try:
                                attributes = getattr(media_object, 'attributes', [])
                                for attr in attributes:
                                    if isinstance(attr, types.DocumentAttributeAnimated): 
                                        media_type = 'gif'
                                        file_ext = '.gif'
                                    elif isinstance(attr, types.DocumentAttributeSticker): 
                                        media_type = 'sticker'
                                        file_ext = '.webp'
                            except Exception as e:
                                logger.debug(f"Ошибка при обработке атрибутов: {e}")
                else:
                    logger.warning("Документ без id, пропускаем")
                    continue
            else:
                logger.debug(f"Неизвестный тип медиа: {type(media)}")
                continue
            # Если успешно определили медиа и file_id
            if file_id and media_object and file_id not in processed_file_ids:
                processed_file_ids.add(file_id)
                logger.debug(f"Подготовка задачи для file_id={file_id}, type={media_type}, ext={file_ext}")
                
                # Формируем имя S3 файла с префиксом
                s3_filename = f"mediaTg/{file_id}{file_ext}"
                s3_link = S3_LINK_TEMPLATE.format(filename=s3_filename)
                
                # Добавляем ссылку в результат сразу
                if s3_link not in media_info['media_urls']:
                    media_info['media_urls'].append(s3_link)

                # Проверяем кэш перед добавлением задачи
                cache_hit = False
                if file_id in s3_file_cache:
                    cached_entry = s3_file_cache[file_id]
                    if isinstance(cached_entry, dict) and cached_entry.get('is_preview'):
                        # Если есть превью, основную ссылку уже добавили, задачу не создаем
                        thumb_s3_link = S3_LINK_TEMPLATE.format(filename=cached_entry.get('thumbnail'))
                        if thumb_s3_link not in media_info['media_urls']: 
                            media_info['media_urls'].append(thumb_s3_link)
                        
                        # Проверяем, есть ли превью видео (для совместимости со старыми записями кэша)
                        if 'preview' in cached_entry:
                            preview_s3_link = S3_LINK_TEMPLATE.format(filename=cached_entry.get('preview'))
                            if preview_s3_link not in media_info['media_urls']: 
                                media_info['media_urls'].append(preview_s3_link)
                        
                        cache_hit = True
                    elif isinstance(cached_entry, str):
                        # Если в кэше имя файла, проверяем его наличие в S3
                        # В неблокирующем режиме пропускаем проверку наличия файла, считаем что кэш верный
                        if non_blocking or await check_s3_file(cached_entry):
                            logger.info(f"Файл {file_id} найден в S3 по кэшу ({cached_entry}). Задачу не добавляем.")
                            cache_hit = True
                        else:
                            logger.warning(f"Файл {cached_entry} из кэша для {file_id} не найден в S3. Удаляем из кэша.")
                            del s3_file_cache[file_id]

            # Если файла нет в S3 (проверено по кэшу), добавляем задачу
                if not cache_hit:
                    task_data = {
                        'client': client, # Передаем клиент для скачивания
                        'media': media_object,
                        'file_id': file_id,
                        'media_type': media_type,
                        's3_filename': s3_filename,
                        'file_ext': file_ext
                    }
                    tasks_to_queue.append(task_data)
        
        # Если есть задачи для загрузки, добавляем их в очередь
        if tasks_to_queue:
            # Добавляем задачи в очередь загрузки асинхронно
            for task_data in tasks_to_queue:
                await upload_queue.put(task_data)
            logger.info(f"Добавлено {len(tasks_to_queue)} задач в очередь загрузки")
            
            # Запускаем обработчик очереди, если он еще не запущен
            global is_worker_running
            if not is_worker_running:
                asyncio.create_task(process_upload_queue())
        
        return media_info
    except Exception as e:
        logger.error(f"Ошибка при получении информации о медиа: {e}")
        return None

async def add_to_upload_queue(task_data):
    await upload_queue.put(task_data)
    global is_worker_running
    if not is_worker_running:
        asyncio.create_task(process_upload_queue())

async def process_upload_queue():
    """Обрабатывает очередь задач загрузки (скачивание + загрузка)."""
    global is_worker_running
    is_worker_running = True
    logger.info("Процессор очереди загрузки запущен")
    
    try:
        # Создаем временную директорию для скачиваемых файлов
        if not os.path.exists(MEDIA_DOWNLOAD_DIR):
            os.makedirs(MEDIA_DOWNLOAD_DIR)

        while True:
            # Извлекаем задачу из очереди
            task = await upload_queue.get()
            try:
                # Получаем информацию о файле из задачи
                client = task['client']
                media = task['media']
                file_id = task['file_id']
                media_type = task['media_type']
                s3_filename = task['s3_filename']
                file_ext = task.get('file_ext', '.bin')
                
                # Формируем путь для скачивания
                local_path = os.path.join(MEDIA_DOWNLOAD_DIR, f"{file_id}{file_ext}")
                download_success = False
                
                logger.info(f"Воркер: НАЧАЛО скачивания медиа {file_id} (тип: {media_type})")
                
                # Пытаемся скачать файл с повторами в случае ошибки флуда
                retry_count = 0
                max_retries = 3
                while retry_count < max_retries:
                    try:
                        # Скачиваем файл
                        await client.download_media(media, local_path)
                        
                        # Проверяем, существует ли файл
                        if os.path.exists(local_path):
                            file_size = os.path.getsize(local_path)
                            logger.info(f"Воркер: УСПЕШНО скачан медиафайл {file_id}, размер: {file_size} байт")
                            download_success = True
                            break
                        else:
                            logger.error(f"Воркер: Файл {local_path} не был создан при скачивании")
                            retry_count += 1
                            await asyncio.sleep(1 * retry_count)  # Увеличиваем задержку с каждой попыткой
                    except FloodWaitError as flood_e:
                        retry_count += 1
                        wait_time = min(flood_e.seconds, 30)  # Ограничиваем время ожидания 30 секундами
                        logger.warning(f"Воркер: FloodWaitError при скачивании {file_id}, ожидание {wait_time} сек., попытка {retry_count}/{max_retries}")
                        await asyncio.sleep(wait_time)
                    except ConnectionError as ce:
                        logger.warning(f"Воркер: Потеряно соединение, пробуем переподключиться для {file_id}: {ce}")
                        try:
                            await client.connect()
                            if not await client.is_user_authorized():
                                logger.error(f"Воркер: Клиент не авторизован после переподключения для {file_id}")
                                break
                            await client.download_media(media, local_path)
                            if os.path.exists(local_path):
                                file_size = os.path.getsize(local_path)
                                logger.info(f"Воркер: УСПЕШНО скачан медиафайл {file_id} после переподключения, размер: {file_size} байт")
                                download_success = True
                                break
                        except Exception as reconnect_err:
                            logger.error(f"Воркер: Не удалось переподключиться и скачать медиа {file_id}: {reconnect_err}")
                            retry_count += 1
                            await asyncio.sleep(1 * retry_count)
                    except Exception as e:
                        logger.error(f"Воркер: Ошибка при скачивании {file_id}: {e}")
                        retry_count += 1
                        await asyncio.sleep(1 * retry_count)  # Увеличиваем задержку с каждой попыткой
                
                # --- Загрузка в S3 --- 
                if download_success:
                    # Дополнительная проверка на существование файла ПЕРЕД загрузкой
                    if os.path.exists(local_path):
                        logger.info(f"Воркер: НАЧАЛО загрузки {local_path} (ID: {file_id}) -> S3:{s3_filename}")
                        optimize = media_type == 'photo' # Оптимизируем только фото
                        try:
                            # Вызываем функцию загрузки с проверкой размера
                            upload_success, info = await upload_to_s3(local_path, s3_filename, optimize=optimize, check_size=True) 
                            
                            if upload_success:
                                cache_key = file_id
                                # Если было создано превью или заглушка, кэшируем информацию о них
                                if info and info.get('is_preview'):
                                    s3_file_cache[cache_key] = info 
                                    # Проверяем, есть ли превью видео (для совместимости со старыми записями кэша)
                                    if 'preview' in info:
                                        logger.info(f"Воркер: УСПЕШНО создано превью для {file_id} -> S3")
                                    else:
                                        logger.info(f"Воркер: УСПЕШНО создана заглушка (только миниатюра) для {file_id} -> S3")
                                # Иначе кэшируем имя основного файла
                                else:
                                    s3_file_cache[cache_key] = s3_filename
                                    logger.info(f"Воркер: УСПЕШНО загружен файл {file_id} -> S3:{s3_filename}")
                                
                                # Обновляем кэш
                                s3_file_cache.move_to_end(cache_key) # Перемещаем в конец для LRU
                                if len(s3_file_cache) > MAX_CACHE_SIZE: s3_file_cache.popitem(last=False) # Удаляем самый старый, если превышен лимит
                                asyncio.create_task(save_cache()) # Сохраняем кэш асинхронно
                                
                            elif info and info.get('reason') == 'size_limit_exceeded':
                                # Файл слишком большой, не загружен
                                logger.warning(f"Воркер: файл {file_id} ({s3_filename}) пропущен (слишком большой, {info.get('size')} байт)")
                                # Не кэшируем, т.к. загрузки не было
                            else:
                                # Ошибка во время загрузки
                                reason = info.get('reason', 'unknown') if info else 'unknown'
                                error_details = info.get('error', 'N/A') if info else 'N/A'
                                logger.error(f"Воркер: ОШИБКА загрузки {s3_filename} в S3. Причина: {reason}, Ошибка: {error_details}")
                        except Exception as upload_exc:
                            # Логируем исключения, возникшие при вызове upload_to_s3
                            logger.error(f"Воркер: Исключение во время вызова upload_to_s3 для {local_path} -> {s3_filename}: {upload_exc}", exc_info=True)
                    else:
                        # Эта ситуация критична и указывает на проблему: скачивание считалось успешным, но файла нет
                        logger.error(f"Воркер: КРИТИЧЕСКАЯ ОШИБКА! Файл {local_path} для {file_id} не найден ПЕРЕД загрузкой, хотя скачивание считалось успешным. Пропуск загрузки.")
                
                # Удаляем локальный файл независимо от результата загрузки
                try:
                    if os.path.exists(local_path):
                        os.remove(local_path)
                        logger.debug(f"Воркер: Локальный файл {local_path} удален")
                except Exception as e:
                    logger.error(f"Воркер: Ошибка при удалении локального файла {local_path}: {e}")
            except Exception as e:
                logger.error(f"Воркер: Общая ошибка при обработке задачи: {e}", exc_info=True)
            finally:
                # Отмечаем задачу выполненной, даже если произошла ошибка
                upload_queue.task_done()
    except asyncio.CancelledError:
        logger.info("Процесс обработки очереди загрузки отменен")
    except Exception as e:
        logger.error(f"Фатальная ошибка в процессе обработки очереди: {e}", exc_info=True)
    finally:
        is_worker_running = False
        logger.info("Процессор очереди загрузки остановлен")

async def download_media_parallel(media_info_list, api_key, max_workers=5):
    """
    Скачивает медиа параллельно, используя доступные аккаунты Telegram.
    
    Args:
        media_info_list: Список информации о медиа
        api_key: API ключ пользователя
        max_workers: Максимальное количество параллельных задач
    
    Returns:
        list: Список результатов обработки медиа
    """
    # Импортируем здесь, чтобы избежать циклических импортов
    from pools import telegram_pool
    
    # Настраиваем семафор для контроля числа параллельных задач
    download_semaphore = asyncio.Semaphore(max_workers)
    tasks = []
    
    # Проверяем и нормализуем API ключ
    if isinstance(api_key, (list, dict)):
        if isinstance(api_key, dict):
            actual_api_key = api_key.get('user_api_key')
        elif len(api_key) > 0 and isinstance(api_key[0], dict):
            actual_api_key = api_key[0].get('user_api_key')
        else:
            actual_api_key = None
    else:
        actual_api_key = api_key
    
    if not actual_api_key:
        logger.error(f"Не удалось извлечь API ключ из: {api_key}")
        return []
    
    logger.info(f"Запуск параллельного скачивания {len(media_info_list)} медиафайлов (max_workers={max_workers})")
    
    # Получаем активные аккаунты один раз
    try:
        active_accounts = await telegram_pool.get_active_clients(actual_api_key)
        if not active_accounts:
            logger.error(f"Не найдены активные аккаунты для API ключа: {actual_api_key}")
            return []
        
        # Логирование структуры аккаунтов для отладки
        logger.info(f"Получено {len(active_accounts)} активных аккаунтов для API ключа: {actual_api_key}")
        logger.info(f"Структура аккаунтов: {type(active_accounts)}, первый аккаунт: {type(active_accounts[0])}")
        
        # Проверяем структуру первого аккаунта для отладки
        if active_accounts and isinstance(active_accounts[0], dict):
            logger.info(f"Ключи первого аккаунта: {active_accounts[0].keys()}")
        elif active_accounts and isinstance(active_accounts[0], (list, tuple)):
            logger.info(f"Длина первого аккаунта (tuple/list): {len(active_accounts[0])}")
    except Exception as e:
        logger.error(f"Ошибка при получении активных аккаунтов: {e}")
        return []
    
    # Счетчик для выбора следующего аккаунта
    account_index = 0
    
    async def download_single_media(media_info):
        """Скачивает одиночный медиафайл с использованием доступного аккаунта"""
        nonlocal account_index
        account_id = None
        
        async with download_semaphore:
            try:
                # Выбираем следующий доступный аккаунт из пула
                # Используем простой round-robin для распределения задач
                account_data = active_accounts[account_index % len(active_accounts)]
                account_index += 1
                
                logger.debug(f"Обработка аккаунта: {account_data}")
                
                # Извлекаем клиент и account_id из данных аккаунта
                client = None
                account_id = None
                
                if isinstance(account_data, dict):
                    # Формат словаря
                    account_id = account_data.get('id') or account_data.get('account_id')
                    client = account_data.get('client')
                    
                    # Если клиента нет в словаре, попробуем получить его через telegram_pool по account_id
                    if not client and account_id:
                        try:
                            # Создаем клиент, если его нет
                            client = telegram_pool.clients.get(account_id)
                            if not client:
                                logger.info(f"Пытаемся создать клиент для аккаунта {account_id}")
                                await telegram_pool.connect_client(account_id) # type: ignore
                                client = telegram_pool.clients.get(account_id)
                        except Exception as e:
                            logger.error(f"Ошибка при создании клиента для аккаунта {account_id}: {e}")
                
                elif isinstance(account_data, (list, tuple)) and len(account_data) >= 2:
                    # Формат списка/кортежа: [client, account_id, ...]
                    client = account_data[0]
                    account_id = account_data[1]
                
                # Проверяем, что смогли извлечь клиент и account_id
                if not client or not account_id:
                    # Вместо возврата None попробуем получить другой аккаунт, если есть
                    if account_index < len(active_accounts) * 3:  # максимум 3 попытки на все аккаунты
                        logger.warning(f"Аккаунт {account_data} не содержит клиента. Пробуем следующий.")
                        return await download_single_media(media_info)
                    
                    logger.error(f"Не удалось получить клиент из данных аккаунта: {account_data}")
                    return None
                
                logger.info(f"Скачивание медиа через аккаунт {account_id}")
                
                # Подключаем клиента, если он не подключен
                await telegram_pool.connect_client(account_id) # type: ignore
                
                # Получаем информацию о медиа для скачивания
                msg = media_info.get('_msg')
                album_messages = media_info.get('_album_messages')
                
                if not msg:
                    logger.error("Медиа информация не содержит объект сообщения")
                    return None
                    
                # Вызываем существующую функцию для получения медиа информации
                result = await get_media_info(client, msg, album_messages)
                
                # Обновляем статистику использования аккаунта через Redis
                try:
                    from redis_utils import update_account_usage_redis
                    await update_account_usage_redis(actual_api_key, account_id, "telegram")
                except ImportError:
                    # Если Redis не доступен, используем прямое обновление
                    await update_account_usage(actual_api_key, account_id, "telegram")
                
                return result
                
            except asyncpg.PostgresError as db_err:
                # Общая обработка ошибок PostgreSQL
                logger.error(f"PostgreSQL ошибка при скачивании медиа через аккаунт {account_id if 'account_id' in locals() else 'неизвестно'}: {db_err}", exc_info=True)
                return None
            except Exception as e:
                # Общая обработка ошибок
                logger.error(f"Непредвиденная ошибка при скачивании медиа: {e}", exc_info=True)
                if 'account_id' in locals():
                    logger.error(f"Ошибка произошла при обработке аккаунта {account_id}")
                return None
    
    # Создаем задачи для каждого медиафайла
    for media_info in media_info_list:
        task = asyncio.create_task(download_single_media(media_info))
        tasks.append(task)
    
    # Ждем завершения всех задач
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Фильтруем результаты, исключая None и исключения
    valid_results = []
    for res in results:
        if isinstance(res, Exception):
            logger.error(f"Исключение при параллельном скачивании: {res}")
        elif res is not None:
            valid_results.append(res)
    
    logger.info(f"Параллельное скачивание завершено: {len(valid_results)} из {len(media_info_list)} файлов успешно обработано")
    return valid_results

# --- Вспомогательные функции для instant-ответа ---

def generate_media_links(msg):
    """
    Генерирует список ссылок на медиа-файлы без их скачивания.
    Используется тот же алгоритм формирования имен, что и в get_media_info,
    чтобы обеспечить соответствие ссылок при последующей обработке.
    
    Args:
        msg: Сообщение Telegram
    
    Returns:
        list: Список ссылок на медиа
    """
    try:
        # Проверяем наличие медиа
        if not hasattr(msg, 'media') or not msg.media:
            return []
        
        # Список для ссылок
        media_urls = []
        
        # Обрабатываем фото
        if isinstance(msg.media, types.MessageMediaPhoto):
            media_type = 'photo'
            media_object = msg.media.photo
            if media_object is not None and hasattr(media_object, 'id'):
                file_id = str(media_object.id)
            else:
                logger.warning("Объект медиа без id, пропускаем")
                return []
            file_ext = '.jpg'
            
            # Формируем имя S3 файла и ссылку
            s3_filename = f"mediaTg/{file_id}{file_ext}"
            s3_link = S3_LINK_TEMPLATE.format(filename=s3_filename)
            media_urls.append(s3_link)
            
        # Обрабатываем документы (видео, аудио, документы, анимации)
        elif isinstance(msg.media, types.MessageMediaDocument):
            media_object = msg.media.document
            if media_object is not None and hasattr(media_object, 'id'):
                file_id = str(media_object.id)
            else:
                logger.warning("Объект медиа без id, пропускаем")
                return []
            mime_type = getattr(media_object, 'mime_type', 'application/octet-stream').lower()
            
            # Определяем тип медиа и расширение
            file_ext = '.bin'
            if mime_type.startswith('video/'):
                media_type = 'video'
                file_ext = '.mp4'
            elif mime_type.startswith('image/'):
                media_type = 'photo'
                file_ext = '.jpg'
            elif mime_type.startswith('audio/'):
                media_type = 'audio'
                file_ext = '.mp3'
            else:
                media_type = 'document'
                # Пытаемся угадать расширение по имени файла
                if hasattr(media_object, 'attributes'):
                    attributes = getattr(media_object, 'attributes', [])
                    fname_attr = next((attr.file_name for attr in attributes
                       if isinstance(attr, types.DocumentAttributeFilename)), None)
                    if fname_attr:
                        _, _ext = os.path.splitext(fname_attr)
                        if _ext: 
                            file_ext = _ext.lower()
            
            # Уточняем тип для GIF/стикеров
            attributes = getattr(media_object, 'attributes', [])
            for attr in attributes:
                if isinstance(attr, types.DocumentAttributeAnimated): 
                    media_type = 'gif'
                    file_ext = '.gif'
                elif isinstance(attr, types.DocumentAttributeSticker): 
                    media_type = 'sticker'
                    file_ext = '.webp'
            
            # Для видео большого размера можем сразу формировать ссылку на заглушку
            # Определяем размер файла
            file_size = getattr(media_object, 'size', 0)
            if media_type == 'video' and file_size > MAX_FILE_SIZE:
                # Для больших видео используем специальный суффикс для превью
                thumb_s3_filename = f"mediaTg/{file_id}_thumb.jpg"
                thumb_s3_link = S3_LINK_TEMPLATE.format(filename=thumb_s3_filename)
                media_urls.append(thumb_s3_link)
            else:
                # Формируем обычную ссылку
                s3_filename = f"mediaTg/{file_id}{file_ext}"
                s3_link = S3_LINK_TEMPLATE.format(filename=s3_filename)
                media_urls.append(s3_link)
        
        return media_urls
    except Exception as e:
        logger.error(f"Ошибка при генерации ссылок на медиа: {e}")
        return []

async def generate_media_links_with_album(client, msg):
    """
    Асинхронная версия generate_media_links, которая также обрабатывает альбомы.
    Получает все сообщения альбома и генерирует ссылки для каждого из них.
    
    Args:
        client: Клиент Telegram
        msg: Сообщение Telegram
    
    Returns:
        list: Список ссылок на медиа, включая все файлы из альбома
    """
    try:
        # Проверяем наличие медиа
        if not hasattr(msg, 'media') or not msg.media:
            return []
        
        # Список для ссылок
        all_media_urls = []
        
        # Проверяем, является ли сообщение частью альбома
        album_messages = None
        if hasattr(msg, 'grouped_id') and msg.grouped_id:
            try:
                from telegram_utils import get_album_messages
                logger.info(f"Получение сообщений альбома для сообщения {msg.id} (grouped_id={msg.grouped_id})")
                album_messages = await get_album_messages(client, msg.input_chat, msg)
                logger.info(f"Найдено {len(album_messages)} сообщений в альбоме {msg.grouped_id}")
            except Exception as e:
                logger.error(f"Ошибка при получении альбома для {msg.id}: {e}")
        
        # Обрабатываем все сообщения в альбоме
        messages_to_process = album_messages if album_messages else [msg]
        
        for message in messages_to_process:
            if not hasattr(message, 'media') or not message.media:
                continue
            
            # Генерируем ссылки для текущего сообщения
            media_urls = generate_media_links(message)
            for url in media_urls:
                if url not in all_media_urls:  # Избегаем дубликатов
                    all_media_urls.append(url)
        
        return all_media_urls
    except Exception as e:
        logger.error(f"Ошибка при генерации ссылок на медиа с альбомом: {e}")
        return []

# async def process_media_later(client, msg, api_key=None):
#     """
#     Обрабатывает медиа в фоновом режиме после ответа API.
#     Включает проверку кэша и скачивание файлов, если их нет в S3.
    
#     Args:
#         client: Клиент Telegram
#         msg: Сообщение Telegram
#         api_key: API ключ пользователя (опционально)
#     """
#     try:
#         # Если нет медиа, нечего обрабатывать
#         if not hasattr(msg, 'media') or not msg.media:
#             return
            
#         # Проверяем, является ли сообщение частью альбома
#         album_messages = None
#         if hasattr(msg, 'grouped_id') and msg.grouped_id:
#             try:
#                 from telegram_utils import get_album_messages
#                 album_messages = await get_album_messages(client, msg.input_chat, msg)
#             except Exception as e:
#                 logger.error(f"Ошибка при получении альбома для {msg.id}: {e}")
        
#         # Для каждого медиа-файла проверяем кэш и при необходимости скачиваем
#         messages_to_process = album_messages if album_messages else [msg]
        
#         for current_msg in messages_to_process:
#             if not hasattr(current_msg, 'media') or not current_msg.media:
#                 continue
                
#             media = current_msg.media
#             media_type = 'unknown'
#             file_id = None
#             media_object = None
#             file_ext = '.bin'
            
#             # Определяем тип медиа и получаем file_id
#             if isinstance(media, types.MessageMediaPhoto):
#                 media_type = 'photo'
#                 media_object = media.photo
#                 if media_object is not None and hasattr(media_object, 'id'):
#                    file_id = str(media_object.id)
#                 else:
#                    logger.warning("Объект медиа без id, пропускаем")
#                    return []
#                 file_ext = '.jpg'
#             elif isinstance(media, types.MessageMediaDocument):
#                 media_object = media.document
#                 if media_object is not None and hasattr(media_object, 'id'):
#                     file_id = str(media_object.id)
#                 else:
#                     logger.warning("Объект медиа без id, пропускаем")
#                     return []
#                 mime_type = getattr(media_object, 'mime_type', 'application/octet-stream').lower()
                
#                 # Определяем тип и расширение файла
#                 if mime_type.startswith('video/'):
#                     media_type = 'video'
#                     file_ext = '.mp4'
#                 elif mime_type.startswith('image/'):
#                     media_type = 'photo'
#                     file_ext = '.jpg'
#                 elif mime_type.startswith('audio/'):
#                     media_type = 'audio'
#                     file_ext = '.mp3'
#                 else:
#                     media_type = 'document'
#                     if hasattr(media_object, 'attributes'):
#                         attributes = getattr(media_object, 'attributes', [])
#                         fname_attr = next((attr.file_name for attr in attributes
#                                            if isinstance(attr, types.DocumentAttributeFilename)), None)
#                         if fname_attr:
#                             _, _ext = os.path.splitext(fname_attr)
#                             if _ext: 
#                                 file_ext = _ext.lower()
                            
#                 # Уточняем тип для GIF/стикеров
#                 attributes = getattr(media_object, 'attributes', [])
#                 for attr in attributes:
#                     if isinstance(attr, types.DocumentAttributeAnimated): 
#                         media_type = 'gif'
#                         file_ext = '.gif'
#                     elif isinstance(attr, types.DocumentAttributeSticker): 
#                         media_type = 'sticker'
#                         file_ext = '.webp'
            
#             # Если успешно определили медиа
#             if file_id and media_object:
#                 # Формируем имя S3 файла
#                 s3_filename = f"mediaTg/{file_id}{file_ext}"
                
#                 # Проверяем кэш ТОЛЬКО здесь, в фоновом процессе, а не перед ответом
#                 cache_hit = False
#                 if file_id in s3_file_cache:
#                     cached_entry = s3_file_cache[file_id]
#                     if isinstance(cached_entry, dict) and cached_entry.get('is_preview'):
#                         # Если есть превью, считаем что файл уже обработан
#                         logger.debug(f"Медиа {file_id} уже есть в кэше как превью")
#                         cache_hit = True
#                     elif isinstance(cached_entry, str):
#                         # Если в кэше имя файла, проверяем его наличие в S3
#                         if await check_s3_file(cached_entry):
#                             logger.debug(f"Файл {file_id} найден в S3 по кэшу ({cached_entry}). Скачивание не требуется.")
#                             cache_hit = True
#                         else:
#                             logger.warning(f"Файл {cached_entry} из кэша для {file_id} не найден в S3. Удаляем из кэша.")
#                             del s3_file_cache[file_id]
                
#                 # Если файла нет в кэше S3, скачиваем и загружаем
#                 if not cache_hit:
#                     logger.info(f"Начинаем фоновую обработку медиа {file_id} (тип: {media_type})")
                    
#                     # Создаем временную директорию и путь для скачивания
#                     temp_dir = tempfile.mkdtemp()
#                     try:
#                         local_path = os.path.join(temp_dir, f"{file_id}{file_ext}")
                        
#                         # Для видео проверяем размер файла
#                         file_size = getattr(media_object, 'size', 0) if hasattr(media_object, 'size') else 0
                        
#                         # Для больших видео создаем заглушку вместо скачивания
#                         if media_type == 'video' and file_size > MAX_FILE_SIZE:
#                             logger.info(f"Видео {file_id} ({file_size} байт) превышает лимит, создаем текстовую заглушку")
                            
#                             # Вместо скачивания используем функцию для создания заглушки
#                             # (копия логики из upload_to_s3 для больших видео)
#                             placeholder_path = os.path.join(temp_dir, f"placeholder_{file_id}.jpg")
                            
#                             # Создаем заглушку с информацией о файле
#                             try:
#                                 from PIL import Image, ImageDraw, ImageFont
                                
#                                 # Создаем изображение-заглушку
#                                 width, height = 640, 360  # Стандартное соотношение 16:9
#                                 img = Image.new('RGB', (width, height), color=(50, 50, 50))
#                                 draw = ImageDraw.Draw(img)
                                
#                                 # Пытаемся загрузить шрифт, или используем default
#                                 try:
#                                     font_path = os.path.join(os.environ.get('WINDIR', ''), 'Fonts', 'Arial.ttf')
#                                     if os.path.exists(font_path):
#                                         title_font = ImageFont.truetype(font_path, 30)
#                                         regular_font = ImageFont.truetype(font_path, 20)
#                                     else:
#                                         title_font = ImageFont.load_default()
#                                         regular_font = ImageFont.load_default()
#                                 except Exception:
#                                     title_font = ImageFont.load_default()
#                                     regular_font = ImageFont.load_default()
                                    
#                                 # Определяем размер файла в МБ
#                                 file_size_mb = round(file_size / (1024 * 1024), 1)
                                
#                                 # Добавляем текст
#                                 title_text = f"Большой видеофайл ({file_size_mb} МБ)"
#                                 subtitle_text = "Просмотр доступен только в оригинальном посте"
                                
#                                 # Центрируем текст
#                                 title_width = draw.textlength(title_text, font=title_font)
#                                 subtitle_width = draw.textlength(subtitle_text, font=regular_font)
                                
#                                 # Рисуем текст
#                                 draw.text(((width - title_width) / 2, height / 2 - 30), title_text, font=title_font, fill=(255, 255, 255))
#                                 draw.text(((width - subtitle_width) / 2, height / 2 + 10), subtitle_text, font=regular_font, fill=(200, 200, 200))
                                
#                                 # Добавляем значок видео
#                                 draw.polygon([(width/2 - 40, height/2 - 80), (width/2 + 40, height/2 - 80), 
#                                             (width/2 + 40, height/2 - 160), (width/2 - 40, height/2 - 160)], 
#                                             fill=(200, 50, 50))
#                                 draw.polygon([(width/2 - 15, height/2 - 120), (width/2 + 25, height/2 - 140), 
#                                             (width/2 - 15, height/2 - 160)], fill=(255, 255, 255))
                                
#                                 # Сохраняем изображение
#                                 img.save(placeholder_path, "JPEG", quality=90)
                                
#                                 # Загружаем заглушку в S3
#                                 thumb_s3_filename = s3_filename.replace(os.path.splitext(s3_filename)[1], "_thumb.jpg")
#                                 upload_success, preview_info = await upload_to_s3(placeholder_path, thumb_s3_filename, check_size=False)
                                
#                                 if upload_success:
#                                     logger.debug(f"Заглушка для большого видео загружена в S3: {thumb_s3_filename}")
#                                     # Сохраняем информацию в кэш
#                                     preview_info = {
#                                         'is_preview': True,
#                                         'thumbnail': thumb_s3_filename,
#                                         'size': file_size
#                                     }
#                                     s3_file_cache[file_id] = preview_info
#                             except Exception as e:
#                                 logger.error(f"Ошибка при создании заглушки для {file_id}: {e}")
#                         else:
#                             # Для обычных файлов скачиваем и загружаем в S3
#                             try:
#                                 # Скачиваем файл
#                                 await client.download_media(media_object, local_path)
                                
#                                 # Проверяем, существует ли файл
#                                 if os.path.exists(local_path):
#                                     # Загружаем в S3
#                                     upload_success, _ = await upload_to_s3(local_path, s3_filename)
                                    
#                                     if upload_success:
#                                         logger.info(f"Файл {file_id} успешно загружен в S3 как {s3_filename}")
#                                         # Сохраняем в кэш
#                                         s3_file_cache[file_id] = s3_filename
#                                     else:
#                                         logger.error(f"Ошибка при загрузке {file_id} в S3")
#                                 else:
#                                     logger.error(f"Файл {local_path} не был создан при скачивании")
#                             except Exception as e:
#                                 logger.error(f"Ошибка при скачивании файла {file_id}: {e}")
#                     finally:
#                         # Удаляем временную директорию
#                         shutil.rmtree(temp_dir, ignore_errors=True)
                    
#         logger.info(f"Фоновая обработка медиа для сообщения {msg.id} завершена")
#     except Exception as e:
#         logger.error(f"Ошибка при фоновой обработке медиа для сообщения {msg.id}: {e}")