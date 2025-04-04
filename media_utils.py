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
                        logger.info(f"Видео {file_path} ({file_size} байт) превышает лимит, создаем превью")
                        temp_dir = tempfile.mkdtemp()
                        try:
                            thumbnail_path = os.path.join(temp_dir, f"thumb_{os.path.basename(file_path)}.jpg")
                            preview_video_path = os.path.join(temp_dir, f"preview_{os.path.basename(file_path)}.mp4")
                            preview_success = await create_video_preview(file_path, thumbnail_path, preview_video_path)
                            if preview_success:
                                thumb_s3_filename = s3_filename.replace(os.path.splitext(s3_filename)[1], "_thumb.jpg")
                                preview_s3_filename = s3_filename.replace(os.path.splitext(s3_filename)[1], "_preview.mp4")
                                s3_client.upload_file(thumbnail_path, S3_BUCKET_NAME, thumb_s3_filename)
                                logger.debug(f"Миниатюра видео загружена в S3: {thumb_s3_filename}")
                                s3_client.upload_file(preview_video_path, S3_BUCKET_NAME, preview_s3_filename)
                                logger.debug(f"Превью видео загружено в S3: {preview_s3_filename}")
                                preview_info = {
                                    'is_preview': True,
                                    'thumbnail': thumb_s3_filename,
                                    'preview': preview_s3_filename,
                                    'size': file_size
                                }
                                s3_file_cache[s3_filename] = preview_info 
                                return True, preview_info
                        except Exception as e:
                            logger.error(f"Ошибка при создании превью для {file_path}: {e}")
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

async def get_media_info(client, msg, album_messages=None) -> Optional[Dict]:
    """
    Получает информацию о медиа, формирует ссылки и добавляет задачи в очередь.
    Возвращает словарь с информацией о медиа или None.
    """
    logger.debug(f"Начало обработки сообщения {msg.id}")
    # Определяем, обрабатываем ли мы альбом
    is_album = bool(album_messages)
    
    if not msg.media and not is_album: # Проверяем и случай альбома
        logger.debug(f"Сообщение {msg.id} без медиа")
        return None

    media_info = {
        'type': 'album' if is_album else 'unknown', # Устанавливаем тип сразу, если это альбом
        'url': None,
        'media_urls': [] # Список S3 ссылок
    }
    tasks_to_queue = []

    try:
        post_url = f'https://t.me/{msg.chat.username}/{msg.id}' if msg.chat.username else f'https://t.me/c/{abs(msg.chat.id)}/{msg.id}'
        media_info['url'] = post_url
    except Exception as e:
        logger.warning(f"Не удалось сформировать URL для поста {msg.id}: {e}")

    messages_to_process = album_messages if is_album else [msg]
    # if album_messages: media_info['type'] = 'album' # Эта строка больше не нужна

    processed_file_ids = set() # Чтобы не дублировать задачи для одного file_id

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

        elif isinstance(media, types.MessageMediaWebPage):
            if media_info['type'] == 'unknown': media_info['type'] = 'webpage'
            webpage = media.webpage
            # Пытаемся извлечь фото или видео из веб-страницы
            if webpage and hasattr(webpage, 'photo') and webpage.photo:
                media_type = 'photo'
                media_object = webpage.photo
                file_id = str(media_object.id)
                file_ext = '.jpg'
            elif webpage and hasattr(webpage, 'document') and webpage.document:
                doc = webpage.document
                is_video = any(isinstance(attr, types.DocumentAttributeVideo) for attr in doc.attributes)
                is_image = any(isinstance(attr, types.DocumentAttributeImageSize) for attr in doc.attributes)
                if is_video:
                    media_type = 'video'
                    media_object = doc
                    file_id = str(doc.id)
                    file_ext = '.mp4'
                elif is_image:
                    media_type = 'photo'
                    media_object = doc
                    file_id = str(doc.id)
                    file_ext = '.jpg'
            # Если из веб-страницы не извлечь медиа, пропускаем

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
                    preview_s3_link = S3_LINK_TEMPLATE.format(filename=cached_entry.get('preview'))
                    thumb_s3_link = S3_LINK_TEMPLATE.format(filename=cached_entry.get('thumbnail'))
                    if preview_s3_link not in media_info['media_urls']: media_info['media_urls'].append(preview_s3_link)
                    if thumb_s3_link not in media_info['media_urls']: media_info['media_urls'].append(thumb_s3_link)
                    cache_hit = True
                elif isinstance(cached_entry, str):
                    # Если в кэше имя файла, проверяем его наличие в S3
                    if await check_s3_file(cached_entry):
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
        
        # Обновляем тип основного медиа, если он еще 'unknown'
        if not is_album and media_info['type'] == 'unknown' and media_type != 'unknown':
            media_info['type'] = media_type

    # Добавляем задачи в очередь
    if tasks_to_queue:
        logger.info(f"Добавление {len(tasks_to_queue)} задач в очередь загрузки для сообщения {msg.id}")
        for task in tasks_to_queue:
            await add_to_upload_queue(task)

    # Если нет URL медиа, но есть URL поста, возвращаем инфо
    if not media_info['media_urls'] and media_info['url']:
        logger.debug(f"Медиафайлы для поста {msg.id} не найдены или уже обработаны, возвращаем информацию без media_urls")
        return media_info
    # Если есть URL медиа, возвращаем инфо
    elif media_info['media_urls']:
        return media_info
    # Иначе возвращаем None
    else:
        logger.warning(f"Не удалось извлечь медиа или URL для поста {msg.id}")
        return None

async def add_to_upload_queue(task_data):
    await upload_queue.put(task_data)
    global is_worker_running
    if not is_worker_running:
        asyncio.create_task(process_upload_queue())

async def process_upload_queue():
    """Обрабатывает очередь задач загрузки (скачивание + загрузка)."""
    global is_worker_running
    if is_worker_running:
        return
    is_worker_running = True
    logger.info("Запуск рабочего процесса обработки очереди загрузки...")

    temp_dir = tempfile.gettempdir() # Получаем системную временную директорию
    logger.info(f"Используется временная директория: {temp_dir}")

    while True:
        task_data = None
        try:
            # Ожидаем задачу не более 5 секунд
            task_data = await asyncio.wait_for(upload_queue.get(), timeout=5.0) 
        except asyncio.TimeoutError:
            # Если время вышло, проверяем, пуста ли очередь
            if upload_queue.empty():
                logger.info("Очередь загрузки пуста, завершение рабочего процесса.")
                break # Выходим из цикла, если очередь пуста
            else:
                continue # Продолжаем ждать, если очередь не пуста
        except Exception as e:
            logger.error(f"Ошибка при получении задачи из очереди: {e}")
            await asyncio.sleep(1) # Пауза перед следующей попыткой
            continue

        if task_data is None: continue # На всякий случай

        # Распаковываем данные задачи
        client = task_data.get('client')
        media = task_data.get('media')
        file_id = task_data.get('file_id')
        media_type = task_data.get('media_type')
        s3_filename = task_data.get('s3_filename')
        file_ext = task_data.get('file_ext')
        
        # Генерируем уникальное имя файла во временной директории
        # Это важно, если несколько задач обрабатывают один file_id одновременно (хотя вряд ли)
        temp_filename = f"telethon_temp_{file_id}_{int(time.time()*1000)}{file_ext}"
        local_path = os.path.join(temp_dir, temp_filename)

        logger.debug(f"Воркер: обработка задачи для file_id: {file_id}, s3: {s3_filename}, temp_path: {local_path}")
        download_success = False
        
        try:
            # --- Скачивание --- 
            # Удаляем старый временный файл с таким же именем, если он вдруг остался (маловероятно с уникальным именем)
            if os.path.exists(local_path):
                 try: 
                     os.remove(local_path) 
                     logger.debug(f"Воркер: удален старый временный файл {local_path}")
                 except Exception as e_rem_old:
                     logger.warning(f"Воркер: не удалось удалить старый временный файл {local_path}: {e_rem_old}")
            
            # Используем семафор для ограничения одновременных скачиваний
            async with DOWNLOAD_SEMAPHORE:
                delay = await calculate_download_delay()
                if delay > 0: 
                    logger.debug(f"Воркер: задержка перед скачиванием {file_id}: {delay:.2f} сек.")
                    await asyncio.sleep(delay)
                
                logger.info(f"Воркер: НАЧАЛО скачивания {file_id} -> {local_path}")
                global download_counter, last_download_time
                download_counter += 1
                last_download_time = time.time() # Обновляем время последнего скачивания
                
                # Скачиваем медиа
                downloaded_file_path = await client.download_media(media, local_path)
                
                # Строгая проверка, что файл действительно скачан и путь совпадает
                if downloaded_file_path and os.path.exists(downloaded_file_path) and os.path.abspath(downloaded_file_path) == os.path.abspath(local_path):
                    download_success = True
                    try:
                        file_size = os.path.getsize(local_path)
                        logger.info(f"Воркер: УСПЕШНО скачан {file_id} -> {local_path}, Размер: {file_size} байт")
                    except OSError as size_err:
                         logger.warning(f"Воркер: Файл {local_path} скачан, но не удалось получить размер: {size_err}")
                         logger.info(f"Воркер: УСПЕШНО скачан {file_id} -> {local_path} (размер неизвестен)")

                else:
                    logger.error(f"Воркер: ОШИБКА скачивания {file_id}. Ожидался путь: {local_path}, Получен: {downloaded_file_path}. Файл существует: {os.path.exists(local_path)}")
                    download_success = False

        except FloodWaitError as e:
            logger.warning(f"Воркер: FloodWaitError при скачивании {file_id}: {e.seconds} сек. Возврат задачи в очередь.")
            global flood_wait_history
            # Сохраняем информацию о FloodWait
            flood_wait_history.append({'timestamp': time.time(), 'wait_time': e.seconds}) 
            if len(flood_wait_history) > MAX_FLOOD_HISTORY: flood_wait_history.pop(0) # Удаляем старые записи
            
            # Ждем указанное время + небольшая случайная добавка и возвращаем задачу в очередь
            await asyncio.sleep(e.seconds + random.uniform(0.5, 1.5)) 
            await upload_queue.put(task_data) 
            # Пропускаем оставшуюся часть обработки этой задачи
            upload_queue.task_done() # Отмечаем задачу как обработанную (хотя она вернулась в очередь)
            logger.debug(f"Воркер: задача {file_id} возвращена в очередь из-за FloodWait.")
            continue
        except sqlite3.OperationalError as db_err:
            # Обрабатываем ошибку блокировки базы данных
            if "database is locked" in str(db_err):
                logger.warning(f"База данных заблокирована при скачивании {file_id}, переставляем задачу в очередь")
                # Добавляем небольшую задержку перед повторной попыткой
                await asyncio.sleep(random.uniform(0.5, 2.0))
                # Возвращаем задачу в очередь
                await upload_queue.put(task_data)
                # Отмечаем задачу как обработанную и продолжаем с новой
                upload_queue.task_done()
                continue
            else:
                # Другие ошибки SQLite
                logger.error(f"Воркер: SQLite ошибка при скачивании {file_id}: {db_err}", exc_info=True)
                download_success = False
        except FileNotFoundError as fnf_err:
             logger.error(f"Воркер: FileNotFoundError при скачивании {file_id} в {local_path}: {fnf_err}", exc_info=True)
             download_success = False
        except PermissionError as perm_err:
             logger.error(f"Воркер: PermissionError при скачивании/сохранении {file_id} в {local_path}: {perm_err}", exc_info=True)
             download_success = False
        except Exception as e:
            # Проверяем на ошибку блокировки базы данных в строковом виде
            if isinstance(e, Exception) and "database is locked" in str(e):
                logger.warning(f"Воркер: База данных заблокирована при скачивании {file_id}, переставляем задачу в очередь")
                # Добавляем небольшую задержку перед повторной попыткой
                await asyncio.sleep(random.uniform(0.5, 2.0))
                # Возвращаем задачу в очередь
                await upload_queue.put(task_data)
                # Отмечаем задачу как обработанную и продолжаем с новой
                upload_queue.task_done()
                continue
            else:
                # Логируем любые другие непредвиденные ошибки при скачивании
                logger.error(f"Воркер: Непредвиденная ошибка при скачивании {file_id}: {type(e).__name__} - {e}", exc_info=True)
                download_success = False
        
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
                        # Если было создано превью, кэшируем информацию о нем
                        if info and info.get('is_preview'):
                            s3_file_cache[cache_key] = info 
                            logger.info(f"Воркер: УСПЕШНО создано превью для {file_id} -> S3")
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
        
        else: # download_success == False
            # Если скачивание не удалось, просто логируем пропуск загрузки
             logger.warning(f"Воркер: Пропуск загрузки для {file_id}, так как скачивание не удалось.")
        
        # --- Очистка и завершение задачи --- 
        # Пытаемся удалить временный файл, если он все еще существует (даже если были ошибки)
        if os.path.exists(local_path):
            try:
                os.remove(local_path)
                logger.debug(f"Воркер: Удален временный файл {local_path}")
            except Exception as e_rem:
                # Логируем ошибку, если не удалось удалить временный файл
                logger.error(f"Воркер: Не удалось удалить временный файл {local_path}: {e_rem}")
        
        # Отмечаем задачу как выполненную в очереди asyncio
        upload_queue.task_done()
        logger.debug(f"Воркер: Задача для file_id {file_id} завершена (обработана).")
        await asyncio.sleep(0.1) # Небольшая пауза для предотвращения 100% загрузки CPU

    # Цикл завершен (очередь пуста)
    is_worker_running = False
    logger.info("Рабочий процесс обработки очереди загрузки штатно завершен.")

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