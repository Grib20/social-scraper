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
from telethon.errors import FloodWaitError
from collections import OrderedDict
import boto3
from PIL import Image, ImageDraw, ImageFont
import io
from aiojobs import create_scheduler
from dotenv import load_dotenv
from typing import Union, Callable, Any, Optional, List, Dict
import sqlite3

# Загружаем переменные окружения
load_dotenv()

# Настройка логирования
print("Инициализация media_utils.py")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Проверяем, не настроено ли уже логирование в других частях проекта
if not logger.handlers:
    file_handler = logging.FileHandler('media_utils.log', mode='a')
    file_handler.setLevel(logging.INFO)
    file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

logger.info("Логирование в media_utils инициализировано")

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

# --- Функции обновления статистики и инициализации --- 

def update_account_usage(api_key, account_id, platform):
    """Обновляет статистику использования аккаунта через Redis"""
    try:
        from redis_utils import update_account_usage_redis
        # Вызываем функцию обновления статистики через Redis
        update_account_usage_redis(api_key, account_id, platform)
    except ImportError:
        # Если Redis не доступен, пытаемся использовать обычное обновление
        try:
            from user_manager import update_account_usage as real_update
            real_update(api_key, account_id, platform)
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
            if isinstance(media, types.MessageMediaPhoto):
                media_type = 'photo'
                media_object = media.photo
                file_id = str(media_object.id)
                file_ext = '.jpg'
                mime_type = 'image/jpeg'
            elif isinstance(media, types.MessageMediaDocument):
                media_object = media.document
                file_id = str(media_object.id)
                mime_type = getattr(media_object, 'mime_type', mime_type).lower()
                if mime_type.startswith('video/'):
                    media_type = 'video'
                    file_ext = '.mp4' # Предполагаем mp4
                elif mime_type.startswith('image/'):
                    media_type = 'photo'
                    file_ext = '.jpg' # Предполагаем jpg
                elif mime_type.startswith('audio/'):
                    media_type = 'audio'
                    file_ext = '.mp3' # Предполагаем mp3
                else:
                    media_type = 'document'
                    # Пытаемся угадать расширение по имени файла, если есть
                    fname_attr = next((attr.file_name for attr in media_object.attributes if isinstance(attr, types.DocumentAttributeFilename)), None)
                    if fname_attr:
                         _root, _ext = os.path.splitext(fname_attr)
                         if _ext: file_ext = _ext.lower()

                # Уточняем тип для GIF/стикеров
                for attr in media_object.attributes:
                    if isinstance(attr, types.DocumentAttributeAnimated): media_type = 'gif'; file_ext='.gif'
                    elif isinstance(attr, types.DocumentAttributeSticker): media_type = 'sticker'; file_ext='.webp' # Стикеры часто webp

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
    from app import telegram_pool
    
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
                                await telegram_pool.connect_client(account_id)
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
                await telegram_pool.connect_client(account_id)
                
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
                    update_account_usage_redis(actual_api_key, account_id, "telegram")
                except ImportError:
                    # Если Redis не доступен, используем прямое обновление
                    update_account_usage(actual_api_key, account_id, "telegram")
                
                return result
                
            except sqlite3.OperationalError as db_err:
                # Обрабатываем ошибку блокировки базы данных
                if "database is locked" in str(db_err):
                    logger.warning(f"База данных заблокирована при скачивании медиа через аккаунт {account_id}, повторная попытка через 1 секунду")
                    # Добавляем задержку перед повторной попыткой
                    await asyncio.sleep(1.0)
                    # Рекурсивно повторяем операцию
                    return await download_single_media(media_info)
                else:
                    # Другие ошибки SQLite
                    logger.error(f"SQLite ошибка при скачивании медиа через аккаунт {account_id}: {db_err}", exc_info=True)
                    return None
            except Exception as e:
                # Проверяем на ошибку блокировки базы данных в строковом виде
                if "database is locked" in str(e):
                    logger.warning(f"База данных заблокирована при скачивании медиа через аккаунт {account_id}, повторная попытка через 1 секунду")
                    # Добавляем задержку перед повторной попыткой
                    await asyncio.sleep(1.0)
                    # Рекурсивно повторяем операцию
                    return await download_single_media(media_info)
                else:
                    # Другие ошибки
                    logger.error(f"Ошибка при скачивании медиа: {e}")
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
            file_id = str(media_object.id)
            file_ext = '.jpg'
            
            # Формируем имя S3 файла и ссылку
            s3_filename = f"mediaTg/{file_id}{file_ext}"
            s3_link = S3_LINK_TEMPLATE.format(filename=s3_filename)
            media_urls.append(s3_link)
            
        # Обрабатываем документы (видео, аудио, документы, анимации)
        elif isinstance(msg.media, types.MessageMediaDocument):
            media_object = msg.media.document
            file_id = str(media_object.id)
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
                fname_attr = next((attr.file_name for attr in media_object.attributes 
                                  if isinstance(attr, types.DocumentAttributeFilename)), None)
                if fname_attr:
                    _, _ext = os.path.splitext(fname_attr)
                    if _ext: 
                        file_ext = _ext.lower()
            
            # Уточняем тип для GIF/стикеров
            for attr in media_object.attributes:
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

async def process_media_later(client, msg, api_key=None):
    """
    Обрабатывает медиа в фоновом режиме после ответа API.
    Включает проверку кэша и скачивание файлов, если их нет в S3.
    
    Args:
        client: Клиент Telegram
        msg: Сообщение Telegram
        api_key: API ключ пользователя (опционально)
    """
    try:
        # Если нет медиа, нечего обрабатывать
        if not hasattr(msg, 'media') or not msg.media:
            return
            
        # Проверяем, является ли сообщение частью альбома
        album_messages = None
        if hasattr(msg, 'grouped_id') and msg.grouped_id:
            try:
                from telegram_utils import get_album_messages
                album_messages = await get_album_messages(client, msg.input_chat, msg)
            except Exception as e:
                logger.error(f"Ошибка при получении альбома для {msg.id}: {e}")
        
        # Для каждого медиа-файла проверяем кэш и при необходимости скачиваем
        messages_to_process = album_messages if album_messages else [msg]
        
        for current_msg in messages_to_process:
            if not hasattr(current_msg, 'media') or not current_msg.media:
                continue
                
            media = current_msg.media
            media_type = 'unknown'
            file_id = None
            media_object = None
            file_ext = '.bin'
            
            # Определяем тип медиа и получаем file_id
            if isinstance(media, types.MessageMediaPhoto):
                media_type = 'photo'
                media_object = media.photo
                file_id = str(media_object.id)
                file_ext = '.jpg'
            elif isinstance(media, types.MessageMediaDocument):
                media_object = media.document
                file_id = str(media_object.id)
                mime_type = getattr(media_object, 'mime_type', 'application/octet-stream').lower()
                
                # Определяем тип и расширение файла
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
                    fname_attr = next((attr.file_name for attr in media_object.attributes 
                                      if isinstance(attr, types.DocumentAttributeFilename)), None)
                    if fname_attr:
                        _, _ext = os.path.splitext(fname_attr)
                        if _ext: 
                            file_ext = _ext.lower()
                            
                # Уточняем тип для GIF/стикеров
                for attr in media_object.attributes:
                    if isinstance(attr, types.DocumentAttributeAnimated): 
                        media_type = 'gif'
                        file_ext = '.gif'
                    elif isinstance(attr, types.DocumentAttributeSticker): 
                        media_type = 'sticker'
                        file_ext = '.webp'
            
            # Если успешно определили медиа
            if file_id and media_object:
                # Формируем имя S3 файла
                s3_filename = f"mediaTg/{file_id}{file_ext}"
                
                # Проверяем кэш ТОЛЬКО здесь, в фоновом процессе, а не перед ответом
                cache_hit = False
                if file_id in s3_file_cache:
                    cached_entry = s3_file_cache[file_id]
                    if isinstance(cached_entry, dict) and cached_entry.get('is_preview'):
                        # Если есть превью, считаем что файл уже обработан
                        logger.debug(f"Медиа {file_id} уже есть в кэше как превью")
                        cache_hit = True
                    elif isinstance(cached_entry, str):
                        # Если в кэше имя файла, проверяем его наличие в S3
                        if await check_s3_file(cached_entry):
                            logger.debug(f"Файл {file_id} найден в S3 по кэшу ({cached_entry}). Скачивание не требуется.")
                            cache_hit = True
                        else:
                            logger.warning(f"Файл {cached_entry} из кэша для {file_id} не найден в S3. Удаляем из кэша.")
                            del s3_file_cache[file_id]
                
                # Если файла нет в кэше S3, скачиваем и загружаем
                if not cache_hit:
                    logger.info(f"Начинаем фоновую обработку медиа {file_id} (тип: {media_type})")
                    
                    # Создаем временную директорию и путь для скачивания
                    temp_dir = tempfile.mkdtemp()
                    try:
                        local_path = os.path.join(temp_dir, f"{file_id}{file_ext}")
                        
                        # Для видео проверяем размер файла
                        file_size = getattr(media_object, 'size', 0) if hasattr(media_object, 'size') else 0
                        
                        # Для больших видео создаем заглушку вместо скачивания
                        if media_type == 'video' and file_size > MAX_FILE_SIZE:
                            logger.info(f"Видео {file_id} ({file_size} байт) превышает лимит, создаем текстовую заглушку")
                            
                            # Вместо скачивания используем функцию для создания заглушки
                            # (копия логики из upload_to_s3 для больших видео)
                            placeholder_path = os.path.join(temp_dir, f"placeholder_{file_id}.jpg")
                            
                            # Создаем заглушку с информацией о файле
                            try:
                                from PIL import Image, ImageDraw, ImageFont
                                
                                # Создаем изображение-заглушку
                                width, height = 640, 360  # Стандартное соотношение 16:9
                                img = Image.new('RGB', (width, height), color=(50, 50, 50))
                                draw = ImageDraw.Draw(img)
                                
                                # Пытаемся загрузить шрифт, или используем default
                                try:
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
                                upload_success, preview_info = await upload_to_s3(placeholder_path, thumb_s3_filename, check_size=False)
                                
                                if upload_success:
                                    logger.debug(f"Заглушка для большого видео загружена в S3: {thumb_s3_filename}")
                                    # Сохраняем информацию в кэш
                                    preview_info = {
                                        'is_preview': True,
                                        'thumbnail': thumb_s3_filename,
                                        'size': file_size
                                    }
                                    s3_file_cache[file_id] = preview_info
                            except Exception as e:
                                logger.error(f"Ошибка при создании заглушки для {file_id}: {e}")
                        else:
                            # Для обычных файлов скачиваем и загружаем в S3
                            try:
                                # Скачиваем файл
                                await client.download_media(media_object, local_path)
                                
                                # Проверяем, существует ли файл
                                if os.path.exists(local_path):
                                    # Загружаем в S3
                                    upload_success, _ = await upload_to_s3(local_path, s3_filename)
                                    
                                    if upload_success:
                                        logger.info(f"Файл {file_id} успешно загружен в S3 как {s3_filename}")
                                        # Сохраняем в кэш
                                        s3_file_cache[file_id] = s3_filename
                                    else:
                                        logger.error(f"Ошибка при загрузке {file_id} в S3")
                                else:
                                    logger.error(f"Файл {local_path} не был создан при скачивании")
                            except Exception as e:
                                logger.error(f"Ошибка при скачивании файла {file_id}: {e}")
                    finally:
                        # Удаляем временную директорию
                        shutil.rmtree(temp_dir, ignore_errors=True)
                    
        logger.info(f"Фоновая обработка медиа для сообщения {msg.id} завершена")
    except Exception as e:
        logger.error(f"Ошибка при фоновой обработке медиа для сообщения {msg.id}: {e}")