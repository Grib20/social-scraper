import asyncio
import os
import re
from telethon import TelegramClient, functions, types
from telethon.errors import SessionPasswordNeededError, FloodWaitError
# Импортируем недостающие типы исключений
from telethon.errors.rpcerrorlist import (
    AuthKeyError, AuthKeyUnregisteredError, UserDeactivatedBanError,
    ChannelPrivateError, ChatForbiddenError, UsernameNotOccupiedError
)
import logging
import time
import math
from dotenv import load_dotenv
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.functions.channels import JoinChannelRequest, GetFullChannelRequest
from telethon.tl.types import InputPeerChannel
from telethon.tl.functions.messages import SearchGlobalRequest
from telethon.tl.types import Channel, User
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Callable, Any, Union, Tuple, Sequence
from user_manager import get_active_accounts, update_account_usage
from telethon.tl.functions import TLRequest
import sqlite3
import aiohttp
import json
import ssl
from telethon.tl.types import InputPeerUser, InputPeerChat
from urllib.parse import urlparse
import traceback

# Определим перечисление для типов прокси, так как ProxyType недоступен в Telethon
class ProxyType:
    HTTP = 'http'
    SOCKS4 = 'socks4'
    SOCKS5 = 'socks5'

# Импортируем пулы клиентов (УДАЛЕНО)
# try:
#     from pools import telegram_pool, vk_pool 
# except ImportError:
#     logger = logging.getLogger(__name__)
#     logger.warning("Не удалось импортировать пулы клиентов из app.py. Возможны проблемы с ротацией аккаунтов.")
#     telegram_pool = None
#     vk_pool = None

# Импортируем шаблон ссылки и функцию фоновой обработки
try:
    # S3_LINK_TEMPLATE используется для генерации URL
    # process_single_media_background для запуска фоновых задач
    from media_utils import S3_LINK_TEMPLATE, process_single_media_background
except ImportError as e:
    logger = logging.getLogger(__name__)
    logger.error(f"Критическая ошибка: Не удалось импортировать из media_utils: {e}. Функциональность медиа будет нарушена.")
    # Заглушки, чтобы код не падал при импорте
    S3_LINK_TEMPLATE = "https://error.com/{filename}"
    async def process_single_media_background(*args, **kwargs):
        logger.error("process_single_media_background не может быть вызвана из-за ошибки импорта.")
        pass


# load_dotenv()
# logging.basicConfig(level=logging.INFO) # <-- УДАЛЕНО
logger = logging.getLogger(__name__)

# Константы для ротации аккаунтов
REQUEST_SEMAPHORE = asyncio.Semaphore(5)  # Ограничиваем до 5 одновременных запросов к Telegram
REQUEST_DELAY = 0.1  # 100мс между запросами (10 запросов в секунду)
GROUP_DELAY = 1.0  # 1 секунда между запросами к разным группам
channel_members_cache = {}  # Кэш для количества участников в каналах
# Константа для режима пониженной производительности
DEGRADED_MODE_DELAY = 0.5  # Увеличенная задержка между запросами

# Глобальные переменные
message_views_cache = {}  # Кэш для просмотров сообщений

# --- Добавляем константу задержки для Telegram (дублируем из client_pools?) ---
TELEGRAM_DEGRADED_MODE_DELAY = 0.5
# ------------------------------------------------------------------------


class TelegramClientWrapper:
    def __init__(self, client: TelegramClient, account_id: str, api_key: Optional[str] = None):
        self.client = client
        self.account_id = account_id
        self.api_key = api_key
        self.last_request_time = 0
        self.last_group_request_time = 0
        self.requests_count = 0
        self.degraded_mode = False
        
        # Получаем информацию о прокси, если он установлен
        proxy_dict = getattr(client.session, 'proxy', None)
        self.has_proxy = proxy_dict is not None
        if self.has_proxy:
            self.proxy_type = proxy_dict.get('proxy_type', 'unknown') if isinstance(proxy_dict, dict) else 'unknown'
            host = proxy_dict.get('addr', 'unknown') if isinstance(proxy_dict, dict) else 'unknown'
            port = proxy_dict.get('port', 'unknown') if isinstance(proxy_dict, dict) else 'unknown'
            self.proxy_str = f"{self.proxy_type}://{host}:{port}"
            logger.info(f"Клиент {account_id} использует прокси.")
        else:
            logger.info(f"Клиент {account_id} работает без прокси")

    def set_degraded_mode(self, degraded: bool):
        """Устанавливает режим пониженной производительности."""
        self.degraded_mode = degraded

    async def _apply_delays(self):
        """Применяет задержки перед выполнением запроса."""
        current_time = time.time()
        request_delay = DEGRADED_MODE_DELAY if self.degraded_mode else REQUEST_DELAY
        group_delay = GROUP_DELAY * 2 if self.degraded_mode else GROUP_DELAY

        time_since_last_request = current_time - self.last_request_time
        if time_since_last_request < request_delay:
            await asyncio.sleep(request_delay - time_since_last_request)
            self.last_request_time = time.time()
        else:
            self.last_request_time = current_time

        time_since_last_group = current_time - self.last_group_request_time
        if time_since_last_group < group_delay:
            await asyncio.sleep(group_delay - time_since_last_group)
            self.last_group_request_time = time.time()
        else:
            self.last_group_request_time = current_time

    async def _make_request(self, func_or_req_type: Union[Callable[..., Any], type], *args, **kwargs):
        """Выполняет запрос (Request или метод клиента) с соблюдением задержек."""
        await self._apply_delays()
        self.requests_count += 1
        if self.api_key:
            try:
                # Используем Redis для обновления статистики аккаунтов
                from redis_utils import update_account_usage_redis
                result = await update_account_usage_redis(self.api_key, self.account_id, "telegram")
                if isinstance(result, bool):
                    if result:
                        logger.info(f"Статистика использования для аккаунта {self.account_id} успешно обновлена в Redis.")
                elif result is not None:
                    logger.warning(f"Неожиданный результат от update_account_usage_redis: {result}")
            except ImportError:
                # Если Redis не доступен, используем обычное обновление
                from user_manager import update_account_usage
                await update_account_usage(self.api_key, self.account_id, "telegram")
                logger.info(f"Статистика использования для аккаунта {self.account_id} обновлена через user_manager.")
        
        # Логируем информацию о запросе с учетом прокси
        proxy_info = " через прокси" if self.has_proxy else " без прокси"
        if isinstance(func_or_req_type, type):
            logger.info(f"Отправка запроса Telegram {func_or_req_type.__name__}{proxy_info}")
        else:
            logger.info(f"Выполнение метода Telegram {func_or_req_type.__name__}{proxy_info}")
        
        # Максимальное количество попыток при ошибке "database is locked"
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # Проверяем, передан ли тип запроса (Request) или функция/метод
                if isinstance(func_or_req_type, type) and issubclass(func_or_req_type, TLRequest):
                     # Это тип Request, создаем объект запроса и вызываем его
                     request_obj = func_or_req_type(*args, **kwargs)
                     return await self.client(request_obj)
                elif callable(func_or_req_type):
                     # Это вызываемая функция/метод клиента
                     return await func_or_req_type(*args, **kwargs)
                else:
                     raise TypeError(f"Unsupported type for _make_request: {type(func_or_req_type)}")
            except sqlite3.OperationalError as db_err:
                # Обрабатываем ошибку блокировки базы данных
                if "database is locked" in str(db_err):
                    retry_count += 1
                    if retry_count < max_retries:
                        logger.warning(f"База данных заблокирована при запросе {func_or_req_type.__name__}. Повторная попытка {retry_count}/{max_retries} через {retry_count} сек.")
                        await asyncio.sleep(retry_count)  # Увеличиваем паузу с каждой попыткой
                        continue
                    else:
                        logger.error(f"Превышено количество попыток при обработке ошибки блокировки БД: {db_err}")
                        raise
                else:
                    # Другие ошибки SQLite
                    logger.error(f"SQLite ошибка при запросе Telegram: {db_err}")
                    raise
            except Exception as e:
                # Проверяем другие исключения на наличие сообщения о блокировке базы данных
                if "database is locked" in str(e):
                    retry_count += 1
                    if retry_count < max_retries:
                        logger.warning(f"База данных заблокирована при запросе {func_or_req_type.__name__}. Повторная попытка {retry_count}/{max_retries} через {retry_count} сек.")
                        await asyncio.sleep(retry_count)  # Увеличиваем паузу с каждой попыткой
                        continue
                    else:
                        logger.error(f"Превышено количество попыток при обработке ошибки блокировки БД: {e}")
                        raise
                # Проверяем, связана ли ошибка с прокси по сообщению об ошибке
                error_msg = str(e).lower()
                if self.has_proxy and ("proxy" in error_msg or "socks" in error_msg or "connection" in error_msg):
                    logger.error(f"Ошибка соединения с прокси: {e}")
                    # Здесь можно добавить логику для переключения на резервный способ подключения
                else:
                    logger.error(f"Ошибка при выполнении запроса Telegram: {e}")
                raise

    async def _make_group_request(self, func_or_req_type: Union[Callable[..., Any], type], *args, **kwargs):
        """Выполняет запрос к группе с дополнительной задержкой."""
        group_delay = GROUP_DELAY * 2 if self.degraded_mode else GROUP_DELAY
        await asyncio.sleep(group_delay)
        return await self._make_request(func_or_req_type, *args, **kwargs)

    async def make_high_level_request(self, method, *args, **kwargs):
        """Выполняет высокоуровневый запрос к клиенту (не Request) с задержками."""
        await self._apply_delays()
        self.requests_count += 1
        if self.api_key:
            try:
                # Используем Redis для обновления статистики аккаунтов
                try:
                    from redis_utils import update_account_usage_redis
                    await update_account_usage_redis(self.api_key, self.account_id, "telegram")
                except ImportError:
                    logger.warning("Модуль redis_utils не найден, используется синхронное обновление статистики")
                    # Если Redis не доступен, используем обычное обновление (синхронное)
                    from user_manager import update_account_usage
                    await update_account_usage(self.api_key, self.account_id, "telegram")
            except Exception as e:
                logger.error(f"Ошибка при обновлении статистики использования: {e}")
        
        proxy_info = " через прокси" if self.has_proxy else " без прокси"
        logger.info(f"Выполнение высокоуровневого метода Telegram {method.__name__}{proxy_info}")
        
        # Максимальное количество попыток при ошибке "database is locked"
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                actual_method = getattr(self.client, method.__name__)
                return await actual_method(*args, **kwargs)
            except sqlite3.OperationalError as db_err:
                # Обрабатываем ошибку блокировки базы данных
                if "database is locked" in str(db_err):
                    retry_count += 1
                    if retry_count < max_retries:
                        logger.warning(f"База данных заблокирована при высокоуровневом запросе {method.__name__}. Повторная попытка {retry_count}/{max_retries} через {retry_count} сек.")
                        await asyncio.sleep(retry_count)  # Увеличиваем паузу с каждой попыткой
                        continue
                    else:
                        logger.error(f"Превышено количество попыток при обработке ошибки блокировки БД: {db_err}")
                        raise
                else:
                    # Другие ошибки SQLite
                    logger.error(f"SQLite ошибка при высокоуровневом запросе Telegram: {db_err}")
                    raise
            except Exception as e:
                # Проверяем другие исключения на наличие сообщения о блокировке базы данных
                if "database is locked" in str(e):
                    retry_count += 1
                    if retry_count < max_retries:
                        logger.warning(f"База данных заблокирована при высокоуровневом запросе {method.__name__}. Повторная попытка {retry_count}/{max_retries} через {retry_count} сек.")
                        await asyncio.sleep(retry_count)  # Увеличиваем паузу с каждой попыткой
                        continue
                    else:
                        logger.error(f"Превышено количество попыток при обработке ошибки блокировки БД: {e}")
                        raise
                # Проверяем, связана ли ошибка с прокси по сообщению об ошибке
                error_msg = str(e).lower()
                if self.has_proxy and ("proxy" in error_msg or "socks" in error_msg or "connection" in error_msg):
                    logger.error(f"Ошибка соединения с прокси при высокоуровневом запросе: {e}")
                else:
                    logger.error(f"Ошибка при выполнении высокоуровневого запроса Telegram: {e}")
                raise

async def start_client(client: TelegramClient) -> None:
    """Запускает клиент Telegram."""
    try:
        # Получаем информацию о прокси, если он установлен
        proxy_dict = getattr(client, '_proxy', None)
        proxy_info = ""
        if proxy_dict:
            proxy_info = " с прокси"
        
        session_filename = getattr(client.session, 'filename', 'unknown') if client.session else 'unknown'
        logger.info(f"Запуск клиента Telegram {session_filename}{proxy_info}")
        try:
            # Подключаем клиент без установки атрибутов, которые могут не существовать
            await client.connect()
            session_filename = getattr(client.session, 'filename', 'unknown') if client.session else 'unknown'
            logger.info(f"Клиент Telegram {session_filename} успешно подключен")
        except Exception as e:
            error_msg = str(e).lower()
            if proxy_dict and ("proxy" in error_msg or "socks" in error_msg or "connection" in error_msg):
                session_filename = getattr(client.session, 'filename', 'unknown') if client.session else 'unknown'
                logger.error(f"Ошибка соединения с прокси при запуске клиента {session_filename}: {e}")
                
                # Если возникла ошибка прокси, пробуем подключиться без прокси как запасной вариант
                logger.warning(f"Пробуем запустить клиент {session_filename} без прокси после ошибки")
                if client.is_connected():
                    client.disconnect()
                client.set_proxy({})
                await client.connect()
                session_filename = getattr(client.session, 'filename', 'unknown') if client.session else 'unknown'
                logger.info(f"Клиент Telegram {session_filename} успешно подключен без прокси")
                logger.error(f"Ошибка при запуске клиента: {e}")
                raise
                
        # Вместо полного client.start(), который запрашивает номер телефона интерактивно,
        # мы только проверяем авторизацию и инициализируем сессию
        if await client.is_user_authorized():
            session_filename = getattr(client.session, 'filename', 'unknown') if client.session else 'unknown'
            logger.info(f"Клиент Telegram {session_filename} успешно запущен и авторизован")
        else:
            session_filename = getattr(client.session, 'filename', 'unknown') if client.session else 'unknown'
            logger.info(f"Клиент Telegram {session_filename} запущен, требуется авторизация")
    except Exception as e:
        logger.error(f"Ошибка при запуске клиента Telegram: {e}")
        raise

async def auth_telegram_account(client: TelegramClient, phone: str, code: str) -> None:
    """Авторизует аккаунт Telegram с помощью кода."""
    try:
        await client.sign_in(phone, code)
        logger.info(f"Аккаунт {phone} успешно авторизован")
    except Exception as e:
        logger.error(f"Ошибка при авторизации аккаунта {phone}: {e}")
        raise

async def auth_telegram_2fa(client: TelegramClient, password: str) -> None:
    """Авторизует аккаунт Telegram с помощью пароля 2FA."""
    try:
        await client.sign_in(password=password)
        logger.info("2FA успешно пройдена")
    except Exception as e:
        logger.error(f"Ошибка при авторизации 2FA: {e}")
        raise

async def _find_channels_with_account(client: TelegramClient, keywords: List[str], min_members: int = 100000, max_channels: int = 20) -> Dict:
    """Вспомогательная функция для поиска каналов с одним аккаунтом."""
    # Используем словарь для хранения уникальных каналов по ID
    unique_channels = {}
    wrapper = TelegramClientWrapper(client, client.session.filename if client.session else 'unknown')
    
    for keyword in keywords:
        try:
            async with REQUEST_SEMAPHORE:
                # Используем правильный способ вызова поиска контактов
                # Вместо передачи класса SearchRequest, создаем его экземпляр
                search_request = functions.contacts.SearchRequest(
                    q=keyword,
                    limit=100 # Искать среди 100 первых результатов
                )
                
                # Вызываем напрямую через клиент
                result = await client(search_request)
                
                # Обрабатываем найденные чаты
                for chat in result.chats:
                    # Ищем только каналы (не мегагруппы)
                    if isinstance(chat, types.Channel) and not chat.megagroup:
                        # Прерываем, если уже нашли достаточно каналов
                        if len(unique_channels) >= max_channels:
                            break
                            
                        # Пропускаем, если канал уже был добавлен ранее
                        if chat.id in unique_channels:
                            logger.info(f"Канал {chat.title} (ID: {chat.id}) уже был добавлен по другому ключевому слову")
                            continue
                            
                        # Получаем полное инфо для проверки количества участников
                        try:
                            # Используем id и access_hash из объекта chat
                            if chat.access_hash is not None:
                                input_channel = types.InputChannel(channel_id=chat.id, access_hash=chat.access_hash)
                                full_request = functions.channels.GetFullChannelRequest(
                                    channel=input_channel # Передаем созданный InputChannel
                                )
                                full_chat = await client(full_request)
                            else:
                                logger.warning(f"Не удалось получить полную информацию о канале {chat.title} (ID: {chat.id}), так как access_hash отсутствует")
                                continue
                            
                            members_count = full_chat.full_chat.participants_count
                            if members_count >= min_members:
                                channel_id = f'@{chat.username}' if chat.username else str(chat.id)
                                unique_channels[chat.id] = {
                                    "id": chat.id,
                                    "title": chat.title,
                                    "username": chat.username,
                                    "members_count": members_count,
                                    "description": full_chat.full_chat.about
                                }
                                # Сохраняем в кэше количество участников канала
                                channel_members_cache[channel_id] = members_count
                                logger.info(f"Найден канал {chat.title} (ID: {chat.id}) по ключевому слову '{keyword}' с {members_count} участниками")
                        except FloodWaitError as flood_e:
                            logger.warning(f"Flood wait на {flood_e.seconds} секунд при получении информации о канале {getattr(chat, 'username', chat.id)}")
                            await asyncio.sleep(flood_e.seconds)
                        except Exception as e_inner:
                            logger.error(f"Ошибка при получении информации о канале {getattr(chat, 'username', chat.id)}: {e_inner}")
        except FloodWaitError as flood_e:
            logger.warning(f"Flood wait на {flood_e.seconds} секунд при поиске по слову '{keyword}'")
            await asyncio.sleep(flood_e.seconds)
        except Exception as e:
            logger.error(f"Ошибка при поиске каналов по ключевому слову {keyword}: {e}")
            continue
    
    return unique_channels

async def find_channels(client: TelegramClient, keywords: List[str], min_members: int = 100000, max_channels: int = 20, api_key: Optional[str] = None) -> List[Dict]:
    """Находит каналы по ключевым словам."""
    logger.info(f"Поиск каналов по ключевым словам: {keywords}")
    # Используем напрямую переданный клиент
    result_dict = await _find_channels_with_account(client, keywords, min_members, max_channels)
    
    # Преобразуем словарь в список
    channels_list = list(result_dict.values())
    
    # Сортируем найденные каналы по количеству участников (по убыванию)
    sorted_channels = sorted(channels_list, key=lambda x: x['members_count'], reverse=True)
    
    logger.info(f"Найдено {len(sorted_channels)} уникальных каналов Telegram")
    return sorted_channels

def _extract_media_details(media):
    """Извлекает детали медиа для дальнейшей обработки."""
    media_type = 'unknown'
    file_id = None
    media_object = None # Объект Telethon для скачивания
    file_ext = '.bin'
    mime_type = 'application/octet-stream'

    # Фото
    if isinstance(media, types.MessageMediaPhoto) and media.photo:
        media_type = 'photo'
        media_object = media.photo # Берем сам объект Photo
        if hasattr(media_object, 'id'):
            file_id = str(media_object.id)
            file_ext = '.jpg'
            mime_type = 'image/jpeg'
        else:
            logger.warning("Обнаружен объект Photo без ID.")
            return None # Не можем обработать без ID
    # Документ (видео, гиф, стикер, аудио, файл)
    elif isinstance(media, types.MessageMediaDocument) and media.document:
        media_object = media.document # Берем сам объект Document
        if hasattr(media_object, 'id'):
            file_id = str(media_object.id)
            mime_type = getattr(media_object, 'mime_type', 'application/octet-stream').lower()

            # Определяем тип по mime_type
            if mime_type.startswith('video/'):
                 media_type = 'video'
                 file_ext = '.mp4' # По умолчанию mp4 для видео
            elif mime_type.startswith('image/gif'): # Явно проверяем GIF
                 media_type = 'gif'
                 file_ext = '.gif'
            elif mime_type.startswith('image/'): # Другие изображения как фото
                 media_type = 'photo'
                 file_ext = '.jpg' # По умолчанию jpg
            elif mime_type.startswith('audio/'):
                 media_type = 'audio'
                 file_ext = '.mp3' # По умолчанию mp3
            else: # Остальное - документы
                 media_type = 'document'
                 file_ext = '.bin' # По умолчанию бинарный

            # Уточняем расширение и тип из атрибутов документа
            attributes = getattr(media_object, 'attributes', [])
            # Ищем имя файла для расширения
            filename_attr = next((attr.file_name for attr in attributes if isinstance(attr, types.DocumentAttributeFilename)), None)
            if filename_attr:
                 _, _ext = os.path.splitext(filename_attr)
                 if _ext: file_ext = _ext.lower()

            # Уточняем тип для анимированных (GIF) и стикеров
            is_animated = any(isinstance(attr, types.DocumentAttributeAnimated) for attr in attributes)
            is_sticker = any(isinstance(attr, types.DocumentAttributeSticker) for attr in attributes)
            # is_video = any(isinstance(attr, types.DocumentAttributeVideo) for attr in attributes) # Можно использовать для уточнения видео

            if is_animated: media_type = 'gif'; file_ext = '.gif'
            elif is_sticker: media_type = 'sticker'; file_ext = '.webp'
            # elif is_video: media_type = 'video'; file_ext = file_ext if file_ext != '.bin' else '.mp4' # Уточняем расширение для видео

        else:
            logger.warning("Обнаружен объект Document без ID.")
            return None # Не можем обработать без ID
    # Другие типы медиа (опросы, геолокация и т.д.)
    elif isinstance(media, (types.MessageMediaPoll, types.MessageMediaGeo, types.MessageMediaContact, types.MessageMediaVenue)):
        media_type = media.__class__.__name__.replace('MessageMedia', '').lower()
        # Для этих типов нет файла для скачивания/загрузки
        media_object = None
        file_id = None
    else:
         logger.warning(f"Обнаружен неподдерживаемый тип медиа: {type(media)}")
         return None # Неизвестный или неподдерживаемый тип

    # Возвращаем None если не удалось извлечь ID или объект
    if not file_id or not media_object:
         if media_type not in ['poll', 'geo', 'contact', 'venue']: # Для этих типов нормально отсутствие file_id/media_object
              logger.warning(f"Не удалось получить file_id или media_object для медиа типа {media_type}")
              return None

    return media_type, file_id, media_object, file_ext, mime_type


# --- Полная функция _process_channels_for_trending ---
async def _process_channels_for_trending(
    client: TelegramClient, # Клиент для выполнения запросов
    account_id: str,        # ID аккаунта, чтобы передать в фон. задачу
    channel_ids: List[Union[int, str]],
    cutoff_date: datetime,
    posts_per_channel: int,
    min_views: Optional[int],
    min_reactions: Optional[int],
    min_comments: Optional[int],
    min_forwards: Optional[int],
    background_tasks_queue: List[Dict]
    ) -> List[Dict]:
    processed_posts_for_these_channels = []
    logger = logging.getLogger(__name__)
 
    try: # Добавляем try/except вокруг проверки клиента
        # Проверка подключения клиента (остается здесь для основного потока)
        if not client.is_connected():
            # ... (код подключения, как и был) ...
            try:
                logger.info(f"[_process_channels_for_trending] Клиент {account_id} не подключен. Попытка подключения...")
                await client.connect()
                if not await client.is_user_authorized():
                     logger.error(f"[_process_channels_for_trending] Клиент {account_id} НЕ АВТОРИЗОВАН!")
                     return []
                logger.info(f"[_process_channels_for_trending] Клиент {account_id} успешно подключен.")
            except Exception as connect_err:
                logger.error(f"[_process_channels_for_trending] Ошибка подключения клиента {account_id}: {connect_err}", exc_info=True)
                return []
 
        for channel_id_input in channel_ids:
            channel_processed_posts_count = 0
            # --- Начало try для обработки одного канала --- 
            try:
                logger.info(f"--- [Acc: {account_id}] Начало обработки канала Input ID: {channel_id_input} ---")

                # --- Подготовка ID и username ---
                peer_identifier = channel_id_input # То, что будем передавать в API
                entity_username = None
                chat_entity_id_for_data = None # ID для сохранения в post_data
                try:
                    numeric_id = int(channel_id_input)
                    chat_entity_id_for_data = numeric_id # Сохраняем исходный числовой ID
                    if numeric_id > 0:
                        peer_identifier = int(f"-100{numeric_id}")
                        logger.debug(f"[Шаг 0] Преобразование ID: {numeric_id} -> {peer_identifier}")
                    else:
                        peer_identifier = numeric_id
                except ValueError:
                    if isinstance(channel_id_input, str):
                        peer_identifier = channel_id_input.lstrip('@')
                        entity_username = peer_identifier
                        logger.debug(f"[Шаг 0] ID '{channel_id_input}' -> username '{peer_identifier}'")
                    else:
                        logger.warning(f"[Шаг 0] Неожиданный тип ID: {type(channel_id_input)}. Пропуск.")
                        continue
                logger.debug(f"[Шаг 0] Идентификатор для запросов: {peer_identifier} (тип: {type(peer_identifier)})")

                # --- Шаг 1: Получение сущности (для title, username, subscribers) ---
                # ВАЖНО: Не используем chat_entity в iter_messages!
                chat_entity = None
                logger.debug(f"[Шаг 1] Вызов client.get_entity({peer_identifier}) для получения информации")
                try:
                    chat_entity = await client.get_entity(peer_identifier)
                    if not chat_entity:
                        logger.error(f"[Шаг 1] ОШИБКА: get_entity вернул None для {peer_identifier}. Пропуск.")
                        continue
                    logger.debug(f"[Шаг 1] Успех! Получена сущность: ID={chat_entity.id}, Type={type(chat_entity)}")
                    if not entity_username:
                        entity_username = getattr(chat_entity, 'username', None)
                    # Используем ID из полученной сущности для post_data
                    chat_entity_id_for_data = chat_entity.id
                except (ValueError, TypeError) as e_val_type:
                    logger.warning(f"[Шаг 1] ОШИБКА ({type(e_val_type).__name__}) для {peer_identifier}. Пропуск.", exc_info=True)
                    continue
                except FloodWaitError as flood:
                    logger.warning(f"[Шаг 1] Flood wait ({flood.seconds}s) для {peer_identifier}. Пропуск.")
                    await asyncio.sleep(flood.seconds)
                    continue
                except Exception as e_entity:
                    logger.error(f"[Шаг 1] Неожиданная ошибка get_entity для {peer_identifier}: {e_entity}", exc_info=True)
                    continue

                # --- Извлечение данных из chat_entity ---
                channel_title = getattr(chat_entity, 'title', None) or f"{getattr(chat_entity, 'first_name', '')} {getattr(chat_entity, 'last_name', '')}".strip() or f"Unknown ({chat_entity_id_for_data})"
                subscribers = getattr(chat_entity, 'participants_count', None) # Получаем None по умолчанию

                # Пытаемся получить подписчиков через GetFullChannelRequest, если их нет или 0
                if subscribers is None or subscribers == 0:
                    logger.debug(f"Subscribers count is {subscribers}. Trying GetFullChannelRequest...")
                    try:
                        # Явно создаем InputPeerChannel, если есть access_hash
                        input_peer = None
                        # Проверяем, что это канал и у него есть access_hash
                        if isinstance(chat_entity, types.Channel) and hasattr(chat_entity, 'access_hash') and chat_entity.access_hash is not None:
                            # Используем types.InputChannel вместо types.InputPeerChannel
                            input_peer = types.InputChannel(channel_id=chat_entity.id, access_hash=chat_entity.access_hash)
                        # else: # Блок else все еще закомментирован/удален
                        #    logger.warning(f"Access hash not found for {chat_entity.id}. Using chat_entity directly for GetFullChannelRequest.")
                        #    input_peer = chat_entity
                        
                        # Проверяем на types.InputChannel
                        if input_peer and isinstance(input_peer, types.InputChannel):
                            full_chat_result = await client(functions.channels.GetFullChannelRequest(channel=input_peer))
                            subscribers = getattr(getattr(full_chat_result, 'full_chat', None), 'participants_count', None)
                            if subscribers is not None:
                                logger.debug(f"Successfully retrieved subscribers ({subscribers}) via GetFullChannelRequest.")
                            else:
                                logger.warning(f"GetFullChannelRequest did not return participants_count for {chat_entity.id}.")
                        else:
                            logger.warning(f"Could not create InputPeerChannel for GetFullChannelRequest for {chat_entity.id}.")
                         
                    except Exception as e_full:
                        logger.warning(f"Error getting full channel info for {chat_entity.id}: {e_full}")
                        # Оставляем subscribers как None, если произошла ошибка

                # ---- НОВОЕ: Попытка получить подписчиков через get_entity(username), если другие способы не сработали ----
                if subscribers is None and entity_username:
                    logger.debug(f"Failed to get subscribers via ID/GetFullChannelRequest. Trying get_entity('{entity_username}') with force_fetch=True...")
                    try:
                        # Удаляем несуществующий параметр force_fetch=True
                        refreshed_entity = await client.get_entity(entity_username)
                        subscribers = getattr(refreshed_entity, 'participants_count', None)
                        if subscribers is not None:
                            # Убираем упоминание force_fetch из лога тоже
                            logger.debug(f"Successfully retrieved subscribers ({subscribers}) via get_entity(username).") 
                        else:
                            logger.warning(f"get_entity('{entity_username}', force_fetch=True) did not return participants_count.")
                    except Exception as e_refresh:
                        logger.warning(f"Error getting entity by username '{entity_username}' with force_fetch=True: {e_refresh}")
                # ---- Конец новой попытки ----

                # ---- НОВОЕ: Попытка получить подписчиков через SearchGlobalRequest(username), если другие способы не сработали ----
                if subscribers is None and entity_username:
                    logger.debug(f"Failed to get subscribers actively. Trying SearchGlobalRequest for '{entity_username}'...")
                    try:
                        search_result = await client(functions.contacts.SearchRequest(q=entity_username, limit=5))
                        found_chat_with_hash = None
                        for found_chat in search_result.chats:
                            # Ищем совпадение по ID и проверяем наличие access_hash
                            if found_chat.id == chat_entity.id and hasattr(found_chat, 'access_hash') and found_chat.access_hash:
                                found_chat_with_hash = found_chat
                                break
                        
                        if found_chat_with_hash:
                            logger.debug(f"Found channel via search with access_hash. Trying GetFullChannelRequest again...")
                            try:
                                input_channel = types.InputChannel(channel_id=found_chat_with_hash.id, access_hash=found_chat_with_hash.access_hash)
                                full_chat_result = await client(functions.channels.GetFullChannelRequest(channel=input_channel))
                                subscribers = getattr(getattr(full_chat_result, 'full_chat', None), 'participants_count', None)
                                if subscribers is not None:
                                    logger.info(f"Successfully retrieved subscribers ({subscribers}) via Search + GetFullChannelRequest.")
                                else:
                                    logger.warning("GetFullChannelRequest after search did not return participants_count.")
                            except Exception as e_gfc_search:
                                logger.warning(f"Error during GetFullChannelRequest after search: {e_gfc_search}")
                        else:
                            logger.warning(f"Channel '{entity_username}' not found via SearchRequest or no access_hash in result.")
                         
                    except FloodWaitError as flood:
                        logger.warning(f"Flood wait ({flood.seconds}s) during SearchRequest for '{entity_username}'.")
                        await asyncio.sleep(flood.seconds) 
                    except Exception as e_search:
                        logger.warning(f"Error during SearchRequest for '{entity_username}': {e_search}")
                # ---- Конец новой попытки через поиск ----

                # ---- Проверка кэша, если подписчики все еще None ----
                if subscribers is None:
                    # Определяем ключ для кэша (username или ID)
                    cache_key = f"@{entity_username}" if entity_username else str(chat_entity_id_for_data)
                    cached_subs = channel_members_cache.get(cache_key)
                    if cached_subs is not None:
                        logger.info(f"Subscribers count not found actively for {chat_entity_id_for_data}. Using cached value: {cached_subs}")
                        subscribers = cached_subs
                    else:
                        logger.warning(f"Subscribers count not found actively AND not in cache for {chat_entity_id_for_data}. Cache key tried: '{cache_key}'.")
                # ---- Конец проверки кэша ----

                # Устанавливаем значение для расчетов, с резервным значением 10 и логгированием
                if subscribers is None:
                    logger.warning(f"Using 10 for trend score calculation for channel {chat_entity_id_for_data} ('{entity_username}' / {peer_identifier}).")
                    subscribers_count_for_calc = 10  # Используем 10 вместо 0
                else:
                    subscribers_count_for_calc = int(subscribers)  # Убедимся, что это int

                # Используем минимум 10 для избежания деления на ноль или логарифма от нуля/единицы
                subscribers_for_calc = max(subscribers_count_for_calc, 10)
                logger.debug(f"Информация для '{channel_title}' (ID: {chat_entity_id_for_data}, User: {entity_username}): Подписчики={subscribers}, Используется для расчета={subscribers_for_calc}")

                # --- Шаг 2: Получение Истории через iter_messages ---
                logger.debug(f"[Шаг 2] Начинаем итерацию сообщений для {channel_id_input} (передаем: {peer_identifier})")
                processed_in_channel_count = 0
                try:
                    # *** Используем peer_identifier (ID или username), который сработал для get_entity ***
                    async for post in client.iter_messages(
                        entity=peer_identifier,  # Use the identifier that worked for get_entity
                        limit=3000,  # Используем большой лимит вместо 100 или None
                        # reverse=True  # Изменяем на True, чтобы итерировать от новых к старым
                    ):
                        # ... (вся остальная логика фильтрации, сборки post_data, обработки медиа - КАК РАНЬШЕ) ...
                        if not post or not isinstance(post, types.Message) or not post.date: continue

                        # --- Сравнение дат как NAIVE --- 
                        # Сначала получаем дату поста
                        post_date_original = post.date
                        # Делаем дату поста наивной, если она aware
                        post_date_naive = post_date_original.replace(tzinfo=None) if post_date_original.tzinfo is not None else post_date_original
                        # Делаем cutoff_date наивной (она изначально aware UTC)
                        cutoff_date_naive = cutoff_date.replace(tzinfo=None)

                        # Сравниваем наивные даты
                        if post_date_naive < cutoff_date_naive:
                            post_date_str = post.date.isoformat()
                            cutoff_date_str = cutoff_date.isoformat()
                            logger.debug(f"Пост {post.id} слишком старый ({post_date_str} <= {cutoff_date_str}), прекращаем итерацию для {channel_id_input}.")
                            break
                        if not post.message: continue
                        views = getattr(post, 'views', 0) if post.views is not None else 0
                        if min_views is not None and views < min_views: continue
                        reactions = 0
                        comments = 0
                        forwards = 0
                        if post.reactions and post.reactions.results: reactions = sum(r.count for r in post.reactions.results)
                        if min_reactions is not None and reactions < min_reactions: continue
                        if post.replies: comments = post.replies.replies
                        if min_comments is not None and comments < min_comments: continue
                        if post.forwards: forwards = post.forwards
                        if min_forwards is not None and forwards < min_forwards: continue

                        # --- Собираем данные поста ---
                        post_data = {
                            'id': post.id,
                            'channel_id': str(chat_entity_id_for_data),
                            'channel_title': channel_title,
                            'channel_username': entity_username,
                            'subscribers': subscribers,
                            'text': post.message,
                            'views': views,
                            'reactions': reactions,
                            'comments': comments,
                            'forwards': forwards,
                            'date': post.date.isoformat(),
                            'url': None,
                            'media': [],
                            'trend_score': 0.0
                        }
                        if entity_username:
                            post_data['url'] = f"https://t.me/{entity_username}/{post.id}"
                        else:
                            post_data['url'] = f"https://t.me/c/{abs(chat_entity_id_for_data)}/{post.id}"
                        # --- Обработка медиа ---
                        media_objects_to_process = []
                        if hasattr(post, 'grouped_id') and post.grouped_id:
                            try:
                                from telegram_utils import get_album_messages
                                # Передаем peer_identifier для получения альбома
                                album_messages = await get_album_messages(client, peer_identifier, post)
                                for album_msg in album_messages:
                                    if album_msg and album_msg.media: media_objects_to_process.append(album_msg.media)
                            except Exception as e_album:
                                logger.error(f"Ошибка получения альбома для поста {post.id}: {e_album}")
                            if post.media: media_objects_to_process.append(post.media)
                        elif post.media: media_objects_to_process.append(post.media)
                        media_tasks_for_post = {}
                        for media_obj_container in media_objects_to_process:
                            media_details = _extract_media_details(media_obj_container)
                            if not media_details: continue
                            media_type, file_id, media_object, file_ext, mime_type = media_details
                            if media_type in ['poll', 'geo', 'contact', 'venue'] or not file_id or not media_object: continue
                            s3_filename = f"mediaTg/{file_id}{file_ext}"
                            preliminary_s3_url = S3_LINK_TEMPLATE.format(filename=s3_filename)
                            if preliminary_s3_url not in post_data['media']: post_data['media'].append(preliminary_s3_url)
                            if file_id not in media_tasks_for_post:
                                media_tasks_for_post[file_id] = {
                                    "account_id": account_id,  # <--- ИЗМЕНЕНИЕ
                                    "media_object": media_object,
                                    "file_id": file_id,
                                    "s3_filename": s3_filename
                                }
                        background_tasks_queue.extend(media_tasks_for_post.values())
                        # --- Расчет trend_score ---
                        # log_views = math.log10(views + 1); log_subs = math.log10(subscribers_for_calc) if subscribers_for_calc > 0 else 1
                        # engagement_rate = (reactions + comments * 2 + forwards * 5) / subscribers_for_calc if subscribers_for_calc > 0 else 0
                        # engagement_rate = min(engagement_rate, 0.1)
                        # trend_score = (log_views * 0.6) + (engagement_rate * 100 * 0.4)

                        # Новая формула:
                        raw_engagement_score = views + (reactions * 10) + (comments * 20) + (forwards * 50)
                        log_subs = math.log10(subscribers_for_calc) if subscribers_for_calc > 1 else 1  # Используем 1, если подписчиков <=1, чтобы избежать log10(1)=0
                        trend_score = raw_engagement_score / log_subs if log_subs != 0 else raw_engagement_score  # Делим, если log_subs не 0

                        # Округляем до целого числа
                        post_data['trend_score'] = int(trend_score)

                        processed_posts_for_these_channels.append(post_data)
                        processed_in_channel_count += 1
                        if processed_in_channel_count >= posts_per_channel:
                            logger.debug(f"Достигнут лимит ({posts_per_channel}) постов для {channel_id_input}. Прерываем итерацию.")
                            break  # Прерываем async for

                except FloodWaitError as flood:
                    logger.warning(f"[Шаг 2] Flood wait ({flood.seconds}s) при iter_messages для {peer_identifier}. Прерываем.")
                    await asyncio.sleep(flood.seconds)
                except Exception as e_iter:
                    # Логируем ошибку с указанием идентификатора, который передавали
                    logger.error(f"[Шаг 2] ОШИБКА iter_messages для {peer_identifier}: {e_iter}", exc_info=True)
                    # Дополнительно проверяем на ошибку 'entity corresponding to'
                    if 'Cannot find any entity corresponding to' in str(e_iter):
                        logger.error(f"ПОДТВЕРЖДЕНИЕ: Ошибка 'Cannot find any entity' при iter_messages({peer_identifier}). Проблема с доступом?")

            except Exception as e_channel:
                logger.error(f"--- Критическая ошибка при обработке канала {channel_id_input}: {e_channel} ---", exc_info=True)
                # --- Конец except для обработки одного канала --- 

            logger.info(f"--- Завершена обработка канала {channel_id_input}. Собрано постов: {channel_processed_posts_count} ---")

    except Exception as main_err: # Добавляем except для внешней try/except
        logger.error(f"Критическая ошибка в _process_channels_for_trending перед циклом обработки каналов: {main_err}", exc_info=True)
        # В случае ошибки на этом этапе, возвращаем пустой список
        return []

    # Гарантированный возврат списка в конце функции
    logger.info(f"=== Завершена обработка ВСЕХ каналов для этого вызова, собрано итого: {len(processed_posts_for_these_channels)} постов ===")
    return processed_posts_for_these_channels


async def get_album_messages(client, chat, main_message):
    """Получает все сообщения альбома."""
    if not hasattr(main_message, 'grouped_id') or not main_message.grouped_id:
        return [main_message]  # Это не альбом
        
    album_id = main_message.grouped_id
    main_id = main_message.id
    
    # Получаем ID сообщений выше (предположительно более новые части альбома)
    # и ниже (предположительно более старые части)
    album_messages = [main_message]
    
    # Ищем в обоих направлениях от основного сообщения
    # Максимальное количество медиа в альбоме Telegram - 10
    for offset_id in [main_id + 1, main_id - 1]:  # Ищем в обе стороны
        direction = 1 if offset_id > main_id else -1
        for i in range(9):  # Максимум 9 дополнительных сообщений (10 всего в альбоме)
            try:
                msg = await client.get_messages(chat, ids=offset_id + (i * direction))
                if msg and hasattr(msg, 'grouped_id') and msg.grouped_id == album_id:
                    album_messages.append(msg)
                else:
                    # Если сообщение не часть альбома, прекращаем поиск в этом направлении
                    break
            except Exception as e:
                logger.error(f"Ошибка при получении сообщения {offset_id + (i * direction)}: {e}")
                break
    
    logger.info(f"Найдено {len(album_messages)} сообщений в альбоме {album_id}")
    return album_messages

# --- Основная функция get_trending_posts ---
async def get_trending_posts(
    client: TelegramClient, # Основной клиент (для случая без ротации)
    account_id_main: str,   # ID основного клиента
    # Используем Sequence вместо List для ковариантности
    channel_ids: Sequence[Union[int, str]], 
    days_back: int = 7,
    posts_per_channel: int = 10,
    min_views: Optional[int] = None,
    min_reactions: Optional[int] = None,
    min_comments: Optional[int] = None,
    min_forwards: Optional[int] = None,
    api_key: Optional[str] = None # Ключ для поиска активных аккаунтов
    ) -> List[Dict]:
    """
    Получает трендовые посты из каналов.
    Генерирует предварительные S3 URL и запускает фоновую обработку медиа.
    Поддерживает ротацию аккаунтов, если передан api_key и есть несколько активных.
    """
    logger = logging.getLogger(__name__)
    try:
        all_posts = []
        # Обрабатываем вложенные списки/кортежи и преобразуем все в строки для унификации
        flat_channel_ids_str = []
        def flatten(items):
             for x in items:
                 if isinstance(x, (list, tuple)):
                     yield from flatten(x)
                 elif x is not None: # Пропускаем None значения
                     yield str(x) # Преобразуем в строку
        flat_channel_ids_str = list(flatten(channel_ids))

        if not flat_channel_ids_str:
             logger.warning("Список ID каналов пуст после обработки.")
             return []

        logger.info(f"Начинаем поиск трендовых постов для каналов: {flat_channel_ids_str}")
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_back) # Используем UTC
        logger.info(f"Дата отсечки для постов: {cutoff_date}")

        background_tasks_to_run = [] # Общий список для данных фоновых задач

        active_accounts = []
        # Пытаемся использовать ротацию, если передан api_key
        if api_key:
            try:
                from user_manager import get_active_accounts # Импорт внутри try
                active_accounts = await get_active_accounts(api_key, "telegram")
                if not active_accounts:
                     logger.warning(f"Не найдено активных Telegram аккаунтов для ключа {api_key}. Будет использован основной клиент.")
                elif len(active_accounts) == 1:
                     logger.info(f"Найден 1 активный Telegram аккаунт для ключа {api_key}. Ротация не требуется.")
                     # Используем основной клиент, переданный в функцию
                     active_accounts = [] # Сбрасываем, чтобы использовать стандартный путь ниже
                else:
                     logger.info(f"Найдено {len(active_accounts)} активных Telegram аккаунтов. Распределение нагрузки...")

            except ImportError:
                 logger.error("Не удалось импортировать get_active_accounts из user_manager. Ротация аккаунтов невозможна.")
                 active_accounts = [] # Используем стандартный путь
            except Exception as e_acc:
                logger.error(f"Ошибка при получении активных аккаунтов: {e_acc}", exc_info=True)
                active_accounts = [] # Используем стандартный путь

        # Если есть несколько активных аккаунтов для ротации
        if active_accounts: # Если есть несколько активных аккаунтов для ротации
            num_accounts = len(active_accounts)
            # Распределяем каналы между аккаунтами
            # + (len(flat_channel_ids_str) % num_accounts > 0) # Добавляем 1, если есть остаток
            channels_per_account = (len(flat_channel_ids_str) + num_accounts - 1) // num_accounts # Округление вверх
            tasks = []
            start_index = 0
            logger.info(f"Распределяем {len(flat_channel_ids_str)} каналов по {num_accounts} аккаунтам (примерно по {channels_per_account})")

            for i, account_info in enumerate(active_accounts):
                 account_client = None
                 account_id_rot = None # ID аккаунта для ротации
                 if isinstance(account_info, dict):
                     account_client = account_info.get('client')
                     account_id_rot = account_info.get('id')
                 elif isinstance(account_info, (list, tuple)) and len(account_info) > 0:
                      account_client = account_info[0]
                      if len(account_info) > 1: account_id_rot = account_info[1]

                 if not account_client or not isinstance(account_client, TelegramClient) or not account_id_rot: # Проверяем и ID
                     logger.warning(f"Клиент Telegram или ID не найден/некорректен для аккаунта {i}. Пропуск.")
                     continue

                 # Определяем каналы для этого аккаунта
                 end_index = min(start_index + channels_per_account, len(flat_channel_ids_str))
                 channels_for_this_account = flat_channel_ids_str[start_index:end_index]
                 start_index = end_index

                 if not channels_for_this_account: continue # Пропускаем, если каналов не осталось

                 logger.info(f"Аккаунт {account_id_rot} будет обрабатывать {len(channels_for_this_account)} каналов: {channels_for_this_account[:3]}...")

                 # Создаем задачу для аккаунта
                 task = asyncio.create_task(
                    _process_channels_for_trending(
                        account_client, # Передаем клиента для выполнения запросов
                        account_id_rot, # Передаем ID для фоновых задач
                        channels_for_this_account,
                        cutoff_date.replace(tzinfo=None),
                        posts_per_channel,
                        min_views, min_reactions, min_comments, min_forwards,
                        background_tasks_to_run
                    )
                 )
                 tasks.append(task)

            # Ждем завершения всех задач обработки каналов
            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for i, result in enumerate(results):
                    acc_id_log = f"acc_{i}"
                    # Пытаемся получить реальный ID, если возможно
                    if i < len(active_accounts):
                         info = active_accounts[i]
                         if isinstance(info, dict): acc_id_log = info.get('id', acc_id_log)
                         elif isinstance(info, (list, tuple)) and len(info)>1: acc_id_log = info[1]

                    if isinstance(result, Exception):
                        logger.error(f"Ошибка при обработке каналов аккаунтом {acc_id_log}: {result}", exc_info=result)
                    elif isinstance(result, list):
                        logger.info(f"Аккаунт {acc_id_log} успешно обработал каналы, найдено постов: {len(result)}")
                        all_posts.extend(result)
                    else:
                         logger.warning(f"Неожиданный результат от аккаунта {acc_id_log}: {type(result)}")

        else:
            # Если ротация не используется
            logger.info(f"Ротация не используется. Обработка всех каналов основным клиентом {account_id_main}.")
            processed_posts = await _process_channels_for_trending(
                client, # Используем основной клиент
                account_id_main, # ID основного клиента
                flat_channel_ids_str,
                cutoff_date.replace(tzinfo=None),
                posts_per_channel,
                min_views, min_reactions, min_comments, min_forwards,
                background_tasks_to_run
            )
            all_posts.extend(processed_posts)

        # Запускаем все собранные фоновые задачи на обработку медиа
        if background_tasks_to_run:
             logger.info(f"Запуск {len(background_tasks_to_run)} фоновых задач на обработку медиа...")
             launched_file_ids = set()
             tasks_launched_count = 0
             for task_data in background_tasks_to_run:
                  file_id = task_data.get("file_id")
                  # Проверяем, что передали account_id
                  if file_id and task_data.get("account_id") and file_id not in launched_file_ids:
                       # Запускаем задачу (она пока ожидает client, а не account_id - исправим следующим шагом)
                       asyncio.create_task(process_single_media_background(**task_data))
                       launched_file_ids.add(file_id)
                       tasks_launched_count += 1
                  elif file_id and not task_data.get("account_id"):
                       logger.warning(f"Пропуск фоновой задачи для file_id {file_id} - отсутствует account_id.")
                  else:
                       logger.warning(f"Пропуск фоновой задачи без file_id: {task_data}")

             logger.info(f"Успешно запущено {tasks_launched_count} уникальных фоновых задач.")
        else:
             logger.info("Фоновых задач на обработку медиа нет.")

        # Сортируем финальный список постов
        all_posts.sort(key=lambda x: x.get('trend_score', 0), reverse=True)
        logger.info(f"Итоговый сбор постов завершен. Найдено {len(all_posts)} постов. Фоновая обработка медиа запущена.")
        return all_posts

    except Exception as e:
        logger.error(f"Критическая ошибка в get_trending_posts: {e}", exc_info=True)
        return [] # Возвращаем пустой список при серьезной ошибке

async def _process_groups_for_posts(client, group_ids, max_posts, cutoff_date, min_views, api_key=None, non_blocking=False):
    """Вспомогательная функция для параллельной обработки групп."""
    wrapper = TelegramClientWrapper(client, client.session.filename if client.session else 'unknown')
    group_posts = []
    
    for group_id in group_ids:
        try:
            # Получаем информацию о группе
            try:
                # Не преобразуем group_id в int, если это строка с @
                if isinstance(group_id, str) and group_id.startswith('@'):
                    channel_entity = await client.get_entity(group_id)
                else:
                    # Для целочисленных ID или строк без @, пробуем преобразовать в int
                    try:
                        channel_entity = await client.get_entity(int(group_id))
                    except ValueError:
                        # Если не удалось преобразовать в int, используем как есть
                        channel_entity = await client.get_entity(group_id)
                
                channel_title = getattr(channel_entity, 'title', 'Unknown')
                channel_username = getattr(channel_entity, 'username', None)
                subscribers = channel_entity.participants_count if hasattr(channel_entity, 'participants_count') else None
                
                # Если не удалось получить число подписчиков, запрашиваем полную информацию о канале
                if subscribers is None:
                    try:
                        full_chat = await client(functions.channels.GetFullChannelRequest(channel=channel_entity))
                        subscribers = full_chat.full_chat.participants_count
                    except Exception as e:
                        logger.warning(f"Ошибка при получении полной информации о канале {group_id}: {e}")
                        subscribers = 1000  # Значение по умолчанию
            except Exception as e:
                logger.warning(f"Ошибка при получении информации о канале {group_id}: {e}")
                # Убираем установку значений по умолчанию, так как пропускаем итерацию
                continue # <-- Добавляем continue здесь
            
            # Устанавливаем минимальное значение подписчиков для расчета
            subscribers_for_calc = max(subscribers, 10) # Минимум 10 участников для логарифма
            
            # Получаем историю сообщений
            channel_posts = []
            try:
                # Получаем в несколько раз больше постов, чем нужно, чтобы учесть фильтрацию
                limit = max_posts * 3
                # Используем group_id как есть, без преобразования в int
                async for message in client.iter_messages(group_id, limit=limit):
                    if message.date.replace(tzinfo=None) < cutoff_date:
                        break
                    channel_posts.append(message)
            except Exception as e:
                logger.error(f"Ошибка при получении истории сообщений канала {group_id}: {e}")
                continue
            
            # Формируем URL шаблон
            if channel_username:
                url_template = f"https://t.me/{channel_username}/{{id}}"
            else:
                assert channel_entity is not None # Добавляем assert здесь
                url_template = f"https://t.me/c/{abs(channel_entity.id)}/{{id}}"
            
            # Обрабатываем посты
            for post in channel_posts:
                if not post.message:
                    continue
                    
                # Проверяем дату
                post_date = post.date.replace(tzinfo=None)
                if post_date < cutoff_date:
                    continue
                    
                # Проверяем просмотры
                views = getattr(post, 'views', 0)
                if views < min_views:
                    continue
                    
                # Получаем дополнительные метрики
                reactions = len(post.reactions.results) if post.reactions else 0
                comments = post.replies.replies if post.replies else 0
                forwards = post.forwards or 0
                
                # Рассчитываем показатели вовлеченности
                raw_engagement_score = views + (reactions * 10) + (comments * 20) + (forwards * 50)
                trend_score = int(raw_engagement_score / math.log10(subscribers_for_calc)) if raw_engagement_score > 0 else 0
                
                # Формируем URL
                url = url_template.format(id=post.id)
                
                # Создаем данные поста
                post_data = {
                    "id": post.id,
                    "date": post_date.isoformat(),
                    "views": views,
                    "reactions": reactions,
                    "comments": comments,
                    "forwards": forwards,
                    "text": post.message,
                    "group_id": group_id,
                    "group_title": channel_title,
                    "group_username": channel_username,
                    "url": url,
                    "media": [],  # Просто пустой массив для медиа без загрузки
                    "subscribers": subscribers,
                    "trend_score": trend_score,
                    "raw_engagement_score": raw_engagement_score
                }
                
                # Не обрабатываем медиа, оставляем пустой массив
                
                group_posts.append(post_data)
        except Exception as e:
            logger.error(f"Ошибка при обработке группы {group_id}: {e}")
    
    return group_posts

async def process_media_file(media):
    """Обрабатывает медиафайл и возвращает информацию о нем."""
    try:
        # В этой функции мы просто создаём базовую информацию о медиафайле
        # Реальная загрузка происходит в get_media_info, которая вызывает process_media_file из media_utils.py
        media_type = "unknown"
        
        # Определяем тип медиа
        if isinstance(media, types.MessageMediaPhoto):
            media_type = "photo"
        elif isinstance(media, types.MessageMediaDocument):
            document = media.document
            if document and hasattr(document, 'mime_type'):
                mime_type = getattr(document, 'mime_type', '')
                if mime_type.startswith('video/'):
                    media_type = "video"
                elif mime_type.startswith('image/'):
                    media_type = "image"
                elif mime_type.startswith('audio/'):
                    media_type = "audio"
                else:
                     # Если mime_type есть, но не подходит, или getattr вернул ''
                    media_type = "document"
            else:
                # Если document или mime_type отсутствуют или пустые
                media_type = "document" # или "unknown"
                
        return {
            "type": media_type,
            "processing": True
        }
    except Exception as e:
        logger.error(f"Ошибка при обработке медиафайла: {e}")
        return None

async def _process_media_async(client, message, post_data):
    """Асинхронно обрабатывает медиа и обновляет данные поста."""
    try:
        # Используем get_media_info для получения информации о медиа
        from media_utils import get_media_info
        # Получаем инфо о медиа в неблокирующем режиме
        media_info = await get_media_info(client, message, non_blocking=True)
        if media_info and 'media_urls' in media_info:
            media_urls = media_info.get('media_urls', [])
            # Обновляем медиа-URLs в данных поста
            # В реальном приложении здесь нужно обновить данные в базе или отправить событие
            post_data['media'] = media_urls
            logger.info(f"Асинхронно обработаны медиа для поста {post_data['id']}")
    except Exception as e:
        logger.error(f"Ошибка при асинхронной обработке медиа для поста {post_data.get('id', 'unknown')}: {e}")

async def get_posts_in_channels(client: TelegramClient, channel_ids: List[Union[int, str]], keywords: Optional[List[str]] = None, count: int = 10, min_views: int = 1000, days_back: int = 3) -> List[Dict]:
    """Получает посты из каналов по ключевым словам."""
    posts = []
    wrapper = TelegramClientWrapper(client, client.session.filename if client.session else 'unknown')
    
    for channel_id in channel_ids:
        try:
            # Не преобразуем channel_id в int, если это строка с @
            if isinstance(channel_id, str) and channel_id.startswith('@'):
                channel = await client.get_entity(channel_id)
            else:
                # Для целочисленных ID или строк без @, пробуем преобразовать в int
                try:
                    channel = await client.get_entity(int(channel_id))
                except ValueError:
                    # Если не удалось преобразовать в int, используем как есть
                    channel = await client.get_entity(channel_id)
            
            # Получаем сообщения за указанный период
            cutoff_date = datetime.now().replace(tzinfo=None) - timedelta(days=days_back)
            channel_posts = []
            async for message in client.iter_messages(channel, limit=100):
                if message.date.replace(tzinfo=None) < cutoff_date:
                    break
                channel_posts.append(message)
            
            for message in channel_posts:
                views = getattr(message, 'views', 0)  # Обрабатываем случай, когда views = None
                if views >= min_views:
                    if not keywords or any(keyword.lower() in message.message.lower() for keyword in keywords):
                        post_data = {
                            "id": message.id,
                            "channel_id": channel_id,
                            "channel_title": getattr(channel, 'title', getattr(channel, 'first_name', 'Unknown')),
                            "text": message.message,
                            "views": views,
                            "date": message.date.isoformat(),
                            "url": f"https://t.me/c/{abs(channel.id)}/{message.id}",
                            "media": []
                        }
                        
                        # Обрабатываем медиа с быстрой генерацией ссылок
                        if message.media:
                            from media_utils import generate_media_links_with_album #, process_media_later <-- Комментируем несуществующий импорт
                            media_urls = await generate_media_links_with_album(client, message)
                            if media_urls:
                                post_data['media'] = media_urls
                                # Запускаем обработку медиа асинхронно
                                # asyncio.create_task(process_media_later(client, message)) <-- Комментируем использование
                        
                        posts.append(post_data)
        except Exception as e:
            logger.error(f"Ошибка при получении постов из канала {channel_id}: {e}")
            continue
    
    return sorted(posts, key=lambda x: x["views"], reverse=True)

async def get_posts_by_keywords(client: TelegramClient, group_keywords: List[str], post_keywords: List[str], count: int = 10, min_views: int = 1000, days_back: int = 3) -> List[Dict]:
    """Получает посты из каналов по ключевым словам для групп и постов."""
    posts = []
    wrapper = TelegramClientWrapper(client, client.session.filename if client.session else 'unknown')
    
    for group_keyword in group_keywords:
        try:
            async with REQUEST_SEMAPHORE:
                # Используем SearchRequest для поиска каналов
                result = await client(functions.contacts.SearchRequest(
                    q=group_keyword,
                    limit=100  # Искать среди 100 первых результатов
                ))
                
                # Обрабатываем найденные чаты
                for chat in result.chats:
                    if isinstance(chat, types.Channel) and not chat.megagroup:
                        try:
                            channel_posts = await get_posts_in_channels(
                                client,
                                [chat.id],
                                post_keywords,
                                count // len(group_keywords),
                                min_views,
                                days_back
                            )
                            posts.extend(channel_posts)
                        except Exception as e:
                            logger.error(f"Ошибка при получении постов из канала {chat.id}: {e}")
                            continue
        except Exception as e:
            logger.error(f"Ошибка при поиске каналов по ключевому слову {group_keyword}: {e}")
            continue
    
    return sorted(posts, key=lambda x: x["views"], reverse=True)[:count]

async def get_posts_by_period(
    client: TelegramClient,
    group_ids: List[Union[int, str]],
    # Убеждаемся, что параметр называется limit_per_channel
    limit_per_channel: int = 100, 
    days_back: int = 7,
    min_views: int = 0,
    api_key: Optional[str] = None, 
    non_blocking: bool = False,   
    is_degraded: bool = False 
    ) -> List[Dict]:
    """
    Асинхронно получает посты из указанных каналов за заданный период.
    Использует limit_per_channel для ограничения количества получаемых сообщений.
    """
    logger.info(f"Запрос постов за период {days_back} дней из {len(group_ids)} каналов. Лимит на канал: {limit_per_channel}. Деградация: {is_degraded}")

    if not client:
        logger.error("Клиент Telegram не был предоставлен для get_posts_by_period")
        return []

    # --- Идентификатор аккаунта для логов ---
    account_id_for_log = getattr(client.session, 'filename', 'UNKNOWN_ID')
    logger.info(f"[Acc: {account_id_for_log}] Используется для запроса.")

    # --- Проверка подключения и авторизации клиента ---
    try:
        if not client.is_connected():
            logger.warning(f"[Acc: {account_id_for_log}] Клиент не подключен. Попытка подключения...")
            await client.connect() # Подключаем, если отключен
        if not await client.is_user_authorized():
            logger.error(f"[Acc: {account_id_for_log}] Клиент не авторизован. Невозможно получить посты.")
            raise ConnectionAbortedError(f"Клиент {account_id_for_log} не авторизован")
    except (ConnectionAbortedError, AuthKeyError, AuthKeyUnregisteredError, UserDeactivatedBanError) as e:
        logger.error(f"[Acc: {account_id_for_log}] Критическая ошибка авторизации/сессии: {e}")
        # При таких ошибках аккаунт, вероятно, неработоспособен, сообщаем об этом
        raise  # Передаем ошибку выше, чтобы пул мог пометить аккаунт как проблемный
    except Exception as e:
        logger.error(f"[Acc: {account_id_for_log}] Ошибка при проверке/подключении клиента: {e}")
        # При других ошибках (сетевых?) просто возвращаем пустой список
        return []
    # -----------------------------------------------

    cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_back)
    logger.info(f"[Acc: {account_id_for_log}] Вычислена дата отсечки: {cutoff_date.isoformat()}")

    # --- Проверка и задержка для degraded_mode ---
    if is_degraded:
        logger.info(f"[Acc: {account_id_for_log}] Клиент в режиме деградации. Добавляем задержку {TELEGRAM_DEGRADED_MODE_DELAY} сек.")
        await asyncio.sleep(TELEGRAM_DEGRADED_MODE_DELAY)
    # ---------------------------------------------

    semaphore = asyncio.Semaphore(5) # Ограничиваем до 5 одновременных обработок каналов
    tasks = []

    async def process_single_channel(group_id):
        logger.info(f"[Acc: {account_id_for_log}] [Chan: {group_id}] Запуск process_single_channel.")
        async with semaphore:
            channel_posts = []
            channel_entity = None
            try:
                # --- Лог перед get_entity ---
                logger.info(f"[Acc: {account_id_for_log}] [Chan: {group_id}] Попытка получить entity...")
                # Универсальный поиск entity (username, link, ID)
                entity_id_to_find = None
                if isinstance(group_id, str) and group_id.startswith('@'):
                    entity_id_to_find = group_id
                elif isinstance(group_id, str) and ('t.me/' in group_id or 'telegram.me/' in group_id):
                    entity_id_to_find = group_id
                elif isinstance(group_id, (int, str)):
                    try:
                        numeric_id = int(group_id)
                        # Используем формат -100<ID> для числовых ID публичных каналов
                        if numeric_id > 0: # Если ID положительный, предполагаем, что это ID без префикса
                            entity_id_to_find = int(f"-100{numeric_id}")
                        else: # Если ID уже отрицательный, используем как есть
                            entity_id_to_find = numeric_id
                    except ValueError:
                        logger.warning(f"[Acc: {account_id_for_log}] [Chan: {group_id}] Некорректный числовой ID. Пропуск.")
                        return [] # Возвращаем пустой список
                else:
                     logger.warning(f"[Acc: {account_id_for_log}] [Chan: {group_id}] Неподдерживаемый тип идентификатора. Пропуск.")
                     return []

                if entity_id_to_find is None:
                     logger.warning(f"[Acc: {account_id_for_log}] [Chan: {group_id}] Не удалось определить entity_id_to_find. Пропуск.")
                     return []

                # Добавляем проверку подключения перед запросом
                if not client.is_connected():
                    logger.warning(f"[Acc: {account_id_for_log}] [Chan: {group_id}] Соединение потеряно перед get_entity. Попытка переподключения...")
                    await client.connect()

                channel_entity = await client.get_entity(entity_id_to_find)
                channel_title = getattr(channel_entity, 'title', 'Unknown Title')
                logger.info(f"[Acc: {account_id_for_log}] [Chan: {group_id}] Успешно получена entity: '{channel_title}' (ID: {channel_entity.id})")

                # --- Лог перед iter_messages --- 
                # Убираем offset_date из лога
                logger.info(f"[Acc: {account_id_for_log}] [Chan: {group_id}] Запуск client.iter_messages с limit=0, reverse=True")

                message_count_in_loop = 0 # Счетчик
                # Собираем ВСЕ посты, которые вернет итератор (от старых к новым)
                temp_posts = [] 

                # --- Используем limit_per_channel, итерация от новых к старым --- 
                logger.info(f"[Acc: {account_id_for_log}] [Chan: {group_id}] Запуск client.iter_messages с entity={group_id}, limit={limit_per_channel}")
                async for message in client.iter_messages(group_id, limit=limit_per_channel):
                    message_count_in_loop += 1
                    # --- Детальный лог КАЖДОГО сообщения из итератора ---
                    msg_date_str = message.date.isoformat() if message.date else "No Date"
                    logger.debug(f"[Acc: {account_id_for_log}] [Chan: {group_id}] Итератор -> msg ID={message.id}, Date={msg_date_str}, Views={message.views}")

                    # --- ВОЗВРАЩАЕМ ФИЛЬТР ДАТЫ с continue --- 
                    if not message.date or message.date <= cutoff_date:
                        logger.debug(f"[Acc: {account_id_for_log}] [Chan: {group_id}] Сообщение {message.id} ({msg_date_str}) слишком старое. Пропускаем.")
                        continue # Переходим к следующему сообщению
                    # ---------------------------------

                    # --- ВОЗВРАЩАЕМ ФИЛЬТР ПРОСМОТРОВ --- 
                    if message.views is not None and message.views >= min_views:
                        logger.debug(f"[Acc: {account_id_for_log}] [Chan: {group_id}] Сообщение ID={message.id} ПРОШЛО фильтр просмотров.")

                        # Убираем префикс -100 из ID канала для выходных данных
                        channel_id_str = str(channel_entity.id).replace('-100', '')

                        post_data = {
                            "id": message.id,
                            "channel_id": channel_id_str,
                            "channel_title": channel_title,
                            "channel_username": getattr(channel_entity, 'username', None),
                            "text": message.text or "",
                            "views": message.views,
                            "reactions": sum(r.count for r in message.reactions.results) if message.reactions and message.reactions.results else 0,
                            "comments": message.replies.replies if message.replies else 0,
                            "forwards": message.forwards or 0,
                            "date": message.date.isoformat(), # Дата в ISO формате
                            "url": f"https://t.me/{getattr(channel_entity, 'username', f'c/{channel_id_str}')}/{message.id}",
                            "media": []
                        }
                        logger.debug(f"[Acc: {account_id_for_log}] [Chan: {group_id}] Добавляем пост ID={message.id}. Текущее кол-во: {len(channel_posts)+1}")
                        channel_posts.append(post_data)
                    else:
                         # Логируем причину, почему не прошло (только по просмотрам)
                         reason = []
                         if message.views is None: reason.append("нет просмотров")
                         elif message.views < min_views: reason.append(f"просмотры ({message.views}) < минимума ({min_views})")
                         logger.debug(f"[Acc: {account_id_for_log}] [Chan: {group_id}] Сообщение ID={message.id} НЕ прошло фильтр просмотров. Причины: {', '.join(reason)}")
                    # --- КОНЕЦ ФИЛЬТРОВ --- 

                # --- Лог после завершения цикла iter_messages --- 
                logger.info(f"[Acc: {account_id_for_log}] [Chan: {group_id}] Завершен цикл iter_messages. Обработано: {message_count_in_loop}. Найдено подходящих: {len(channel_posts)}")

            except FloodWaitError as e:
                 logger.error(f"[Acc: {account_id_for_log}] [Chan: {group_id}] FloodWaitError при обработке: {e.seconds} сек. Пропуск канала.")
                 temp_posts = [] # Очищаем, если была ошибка
            except (ChannelPrivateError, ChatForbiddenError, UsernameNotOccupiedError, ValueError) as e:
                 # ValueError может быть от get_entity, если ID некорректен или не найден
                 logger.warning(f"[Acc: {account_id_for_log}] [Chan: {group_id}] Канал недоступен или не найден: {e}. Пропуск.")
            except SessionPasswordNeededError as e:
                logger.error(f"[Acc: {account_id_for_log}] Аккаунт требует пароль 2FA! {e}")
                # Передаем ошибку выше, чтобы обработать в /api/telegram/accounts/{account_id}/auth
                raise
            except Exception as e:
                logger.error(f"[Acc: {account_id_for_log}] [Chan: {group_id}] Непредвиденная Ошибка при обработке канала: {e.__class__.__name__}: {e}")
                logger.error(traceback.format_exc())
                temp_posts = [] # Очищаем, если была ошибка
            finally:
                # --- Убираем сортировку и срез, так как лимит применен в iter_messages --- 
                logger.info(f"[Acc: {account_id_for_log}] [Chan: {group_id}] Завершение process_single_channel. Возвращаем {len(channel_posts)} постов.")
                return channel_posts

    # Запускаем обработку каналов
    tasks = [process_single_channel(gid) for gid in group_ids]
    try:
        results = await asyncio.gather(*tasks, return_exceptions=True)
    except (ConnectionAbortedError, AuthKeyError, AuthKeyUnregisteredError, UserDeactivatedBanError, SessionPasswordNeededError) as critical_e:
        logger.error(f"[Acc: {account_id_for_log}] Критическая ошибка во время gather: {critical_e}. Прерываем выполнение.")
        # Если возникла критическая ошибка аккаунта, нет смысла продолжать
        raise critical_e # Передаем выше

    # Собираем все посты из результатов
    final_posts = []
    for result in results:
        if isinstance(result, list):
            final_posts.extend(result) # Используем extend для добавления списка постов
        elif isinstance(result, Exception):
            # Логируем некритические ошибки из gather (уже залогированы внутри process_single_channel)
            logger.warning(f"[Acc: {account_id_for_log}] Зафиксирована ошибка при обработке одного из каналов: {result}")
        else:
            logger.warning(f"[Acc: {account_id_for_log}] Неожиданный тип результата от process_single_channel: {type(result)}")

    # --- Финальная сортировка всех постов по дате ---
    final_posts.sort(key=lambda p: p.get('date', datetime.min.replace(tzinfo=timezone.utc).isoformat()), reverse=True)
    # ----------------------------------------------

    logger.info(f"[Acc: {account_id_for_log}] Завершено получение постов за период. Всего найдено: {len(final_posts)}")
    return final_posts

