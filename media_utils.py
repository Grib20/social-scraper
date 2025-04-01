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
from PIL import Image
import io
from aiojobs import create_scheduler
from dotenv import load_dotenv  # Импортируем для загрузки переменных окружения

# Загружаем переменные окружения
load_dotenv()

# Настройка логирования
print("Инициализация media_utils.py")
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Проверяем, не настроено ли уже логирование в других частях проекта
if not logger.handlers:
    file_handler = logging.FileHandler('media_utils.log', mode='a')
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

logger.info("Логирование в media_utils инициализировано")

# Константы
S3_CACHE_FILE = "s3_file_cache.pkl"
MAX_CACHE_SIZE = 1000
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME', 'scraper')  # Имя бакета из переменных окружения
S3_REGION = os.getenv('S3_REGION', 'ru-central1')  # Регион из переменных окружения
S3_LINK_TEMPLATE = os.getenv('S3_LINK_TEMPLATE', 'https://scraper.website.yandexcloud.net/{filename}')  # Шаблон ссылки из переменных окружения
MAX_FILE_SIZE = 50 * 1024 * 1024  # Максимальный размер файла для загрузки (50 МБ)

# S3 клиент 
s3_client = None

# Планировщик для фоновых задач
scheduler = None

# Кэш файлов S3
s3_file_cache = OrderedDict()

# Семафоры для контроля скорости запросов
REQUEST_SEMAPHORE = asyncio.Semaphore(3)  # Ограничение на API запросы
DOWNLOAD_SEMAPHORE = asyncio.Semaphore(2)  # Ограничение на скачивание медиа
UPLOAD_SEMAPHORE = asyncio.Semaphore(5)    # Ограничение на загрузку в S3

# Счетчик для контроля batch-загрузок
download_counter = 0
last_download_time = time.time()
DOWNLOAD_RATE_LIMIT = 20  # Максимальное количество скачиваний в минуту
MIN_DOWNLOAD_DELAY = 0.5  # Минимальная задержка между скачиваниями (в секундах)

# Отслеживание ошибок FloodWait
flood_wait_history = []
MAX_FLOOD_HISTORY = 5      # Количество последних ошибок FloodWait для анализа

# Очередь задач загрузки
upload_queue = asyncio.Queue(maxsize=100)
is_worker_running = False

# Функция инициализации S3 клиента
def init_s3_client():
    global s3_client
    # Используем переменные окружения для ключей доступа
    s3_client = boto3.client(
        's3',
        region_name=S3_REGION,
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        endpoint_url=os.getenv('S3_ENDPOINT_URL', 'https://storage.yandexcloud.net')
    )
    logger.info("S3 клиент инициализирован")

# Функция загрузки кэша
def load_cache():
    global s3_file_cache
    logger.debug(f"Проверка наличия файла кэша: {S3_CACHE_FILE}")
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
        logger.info(f"Файл кэша {S3_CACHE_FILE} не существует, создаём новый")
        s3_file_cache = OrderedDict()

async def save_cache():
    logger.debug("Начало асинхронного сохранения кэша")
    loop = asyncio.get_running_loop()
    try:
        await loop.run_in_executor(None, lambda: pickle.dump(s3_file_cache, open(S3_CACHE_FILE, "wb")))
        logger.info(f"Кэш сохранён в {S3_CACHE_FILE}, размер: {len(s3_file_cache)} записей")
    except Exception as e:
        logger.error(f"Ошибка при асинхронном сохранении кэша в {S3_CACHE_FILE}: {e}")

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
    """Загружает файл в S3, сжимая его, если это изображение или видео превышает максимальный размер."""
    if s3_client is None:
        init_s3_client()
    
    async with UPLOAD_SEMAPHORE:
        try:
            # Проверяем размер файла, если требуется
            if check_size and os.path.exists(file_path):
                file_size = os.path.getsize(file_path)
                # Если файл превышает максимальный размер
                if file_size > MAX_FILE_SIZE:
                    # Если это видео
                    if file_path.lower().endswith(('.mp4', '.mov', '.avi')):
                        logger.info(f"Файл {file_path} превышает максимальный размер ({file_size} байт), создаем превью")
                        
                        # Создаем временную директорию
                        temp_dir = tempfile.mkdtemp()
                        try:
                            # Пути для превью
                            thumbnail_path = os.path.join(temp_dir, f"thumb_{os.path.basename(file_path)}.jpg")
                            preview_video_path = os.path.join(temp_dir, f"preview_{os.path.basename(file_path)}")
                            
                            # Создаем превью
                            preview_success = await create_video_preview(file_path, thumbnail_path, preview_video_path)
                            
                            if preview_success:
                                # Загружаем миниатюру в S3
                                thumb_s3_filename = s3_filename.replace(os.path.splitext(s3_filename)[1], "_thumb.jpg")
                                s3_client.upload_file(thumbnail_path, S3_BUCKET_NAME, thumb_s3_filename)
                                logger.debug(f"Миниатюра видео загружена в S3: {thumb_s3_filename}")
                                
                                # Загружаем превью-видео в S3
                                preview_s3_filename = s3_filename.replace(os.path.splitext(s3_filename)[1], "_preview.mp4")
                                s3_client.upload_file(preview_video_path, S3_BUCKET_NAME, preview_s3_filename)
                                logger.debug(f"Превью видео загружено в S3: {preview_s3_filename}")
                                
                                # Добавляем информацию о превью в кэш
                                preview_info = {
                                    'is_preview': True,
                                    'thumbnail': thumb_s3_filename,
                                    'preview': preview_s3_filename,
                                    'size': file_size
                                }
                                s3_file_cache[f"preview_{os.path.basename(s3_filename)}"] = preview_info
                                
                                return True, preview_info
                        except Exception as e:
                            logger.error(f"Ошибка при создании превью: {e}")
                        finally:
                            # Удаляем временную директорию
                            shutil.rmtree(temp_dir, ignore_errors=True)
                            
                        # Если не удалось создать превью, пропускаем загрузку
                        logger.warning(f"Пропуск загрузки большого файла: {file_path} ({file_size} байт)")
                        return False, {'is_preview': False, 'reason': 'size_limit_exceeded', 'size': file_size}
                    else:
                        # Для других больших файлов просто пропускаем загрузку
                        logger.warning(f"Пропуск загрузки большого файла: {file_path} ({file_size} байт)")
                        return False, {'is_preview': False, 'reason': 'size_limit_exceeded', 'size': file_size}
            
            # Обычная загрузка изображений
            if optimize and file_path.lower().endswith(('.jpg', '.jpeg', '.png')):
                # Создаём временный файл для сжатого изображения
                optimized_path = file_path + "_optimized.jpg"
                await optimize_image(file_path, optimized_path)
                # Загружаем сжатый файл
                s3_client.upload_file(optimized_path, S3_BUCKET_NAME, s3_filename)
                # Удаляем временный файл
                os.remove(optimized_path)
                logger.debug(f"Сжатый файл загружен в S3: {s3_filename}")
            else:
                # Загружаем файл как есть (например, небольшое видео)
                s3_client.upload_file(file_path, S3_BUCKET_NAME, s3_filename)
                logger.debug(f"Файл загружен в S3 без сжатия: {s3_filename}")
            return True, None
        except Exception as e:
            logger.error(f"Ошибка при загрузке файла в S3 {s3_filename}: {e}")
            return False, None

async def check_s3_file(s3_filename):
    """Проверяет, существует ли файл в S3."""
    if s3_client is None:
        init_s3_client()
    try:
        s3_client.head_object(Bucket=S3_BUCKET_NAME, Key=s3_filename)
        return True
    except s3_client.exceptions.ClientError:
        return False

async def calculate_download_delay():
    """Рассчитывает задержку между скачиваниями на основе истории ошибок FloodWait."""
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

async def add_to_upload_queue(task_data):
    """Добавляет задачу в очередь загрузки."""
    await upload_queue.put(task_data)
    if not is_worker_running:
        asyncio.create_task(process_upload_queue())

async def process_upload_queue():
    """Обрабатывает очередь задач загрузки."""
    global is_worker_running
    is_worker_running = True
    while not upload_queue.empty():
        task_data = await upload_queue.get()
        file_path, s3_filename, optimize, check_size = task_data
        try:
            success, info = await upload_to_s3(file_path, s3_filename, optimize, check_size)
            if success:
                logger.info(f"Файл {s3_filename} успешно загружен в S3")
            else:
                logger.error(f"Ошибка при загрузке файла {s3_filename} в S3")
        finally:
            # Удаляем временный файл после загрузки (успешной или нет)
            if os.path.exists(file_path):
                os.remove(file_path)
            upload_queue.task_done()
    is_worker_running = False

async def process_media_file(client, media, file_id, media_type):
    """Обрабатывает медиафайл, загружает его в S3 и возвращает информацию о нем."""
    try:
        # Генерируем имя файла в S3
        s3_filename = f"{file_id}_{media_type}"
        
        # Создаем фоновую задачу для скачивания и загрузки файла
        async def download_and_upload():
            try:
                # Проверяем, существует ли файл в S3
                if await check_s3_file(s3_filename):
                    logger.info(f"Файл {s3_filename} уже существует в S3")
                    return
                
                # Используем семафор для контроля скачиваний
                async with DOWNLOAD_SEMAPHORE:
                    # Рассчитываем задержку перед скачиванием
                    delay = await calculate_download_delay()
                    if delay > 0:
                        await asyncio.sleep(delay)
                    
                    # Скачиваем медиафайл
                    file_path = f"temp_{file_id}_{media_type}"
                    await client.download_media(media, file_path)
                    
                    # Обновляем счетчик скачиваний
                    global download_counter
                    download_counter += 1
                    
                    # Добавляем задачу в очередь загрузки
                    await add_to_upload_queue((file_path, s3_filename, True, True))
            except FloodWaitError as e:
                # Обрабатываем FloodWaitError
                global flood_wait_history
                flood_wait_history.append({
                    'wait_time': e.seconds,
                    'timestamp': time.time()
                })
                # Оставляем только последние MAX_FLOOD_HISTORY ошибок
                if len(flood_wait_history) > MAX_FLOOD_HISTORY:
                    flood_wait_history = flood_wait_history[-MAX_FLOOD_HISTORY:]
                logger.warning(f"Получен FloodWaitError для файла {file_id}, ожидание {e.seconds} секунд")
            except Exception as e:
                logger.error(f"Ошибка при скачивании/загрузке медиафайла {file_id}: {e}")
        
        # Запускаем фоновую задачу
        asyncio.create_task(download_and_upload())
        
        # Возвращаем информацию о файле сразу
        return {
            'file_id': file_id,
            'media_type': media_type,
            's3_filename': s3_filename,
            's3_link': S3_LINK_TEMPLATE.format(filename=s3_filename),
            'status': 'processing'
        }
    except Exception as e:
        logger.error(f"Ошибка при обработке медиафайла {file_id}: {e}")
        return None

async def get_media_info(client, msg, album_messages=None):
    """Получает информацию о медиафайлах в сообщении."""
    media_info = []
    if msg.media:
        media = msg.media
        if isinstance(media, types.MessageMediaPhoto):
            file_id = msg.id
            media_type = 'photo'
            info = await process_media_file(client, media, file_id, media_type)
            if info:
                media_info.append(info)
        elif isinstance(media, types.MessageMediaDocument):
            file_id = msg.id
            media_type = 'document'
            info = await process_media_file(client, media, file_id, media_type)
            if info:
                media_info.append(info)
    if album_messages:
        for album_msg in album_messages:
            if album_msg.media:
                media = album_msg.media
                if isinstance(media, types.MessageMediaPhoto):
                    file_id = album_msg.id
                    media_type = 'photo'
                    info = await process_media_file(client, media, file_id, media_type)
                    if info:
                        media_info.append(info)
                elif isinstance(media, types.MessageMediaDocument):
                    file_id = album_msg.id
                    media_type = 'document'
                    info = await process_media_file(client, media, file_id, media_type)
                    if info:
                        media_info.append(info)
    return media_info

async def init_scheduler():
    """Инициализирует планировщик для фоновых задач."""
    global scheduler
    scheduler = await create_scheduler()
    logger.info("Планировщик инициализирован")

async def close_scheduler():
    """Закрывает планировщик."""
    global scheduler
    if scheduler:
        await scheduler.close()
        scheduler = None
        logger.info("Планировщик закрыт")