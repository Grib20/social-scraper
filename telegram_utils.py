import asyncio
import os
import re
from telethon import TelegramClient, functions, types
from telethon.errors import SessionPasswordNeededError, FloodWaitError
import logging
import time
import math
from media_utils import get_media_info
from dotenv import load_dotenv
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.functions.channels import JoinChannelRequest, GetFullChannelRequest
from telethon.tl.types import InputPeerChannel
from telethon.tl.functions.messages import SearchGlobalRequest
from telethon.tl.types import Channel, User
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Callable, Any, Union, Tuple
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

# Импортируем пулы клиентов
try:
    from pools import telegram_pool, vk_pool
except ImportError:
    logger = logging.getLogger(__name__)
    logger.warning("Не удалось импортировать пулы клиентов из app.py. Возможны проблемы с ротацией аккаунтов.")
    telegram_pool = None
    vk_pool = None

load_dotenv()
logging.basicConfig(level=logging.INFO)
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

def validate_proxy(proxy: Optional[str]) -> Tuple[bool, str]:
    """
    Валидирует строку прокси и возвращает статус валидации и тип прокси.
    
    Args:
        proxy: Строка с прокси в формате scheme://host:port или scheme://user:password@host:port
        
    Returns:
        Tuple[bool, str]: (валиден ли прокси, тип прокси)
    """
    if not proxy:
        return False, "none"
    
    # Определяем тип прокси по схеме
    if proxy.startswith('socks5://'):
        proxy_type = 'socks5'
    elif proxy.startswith('socks4://'):
        proxy_type = 'socks4'
    elif proxy.startswith('http://'):
        proxy_type = 'http'
    elif proxy.startswith('https://'):
        proxy_type = 'https'
    else:
        # Если схема не указана, по умолчанию считаем http
        proxy_type = 'http'
        proxy = f'http://{proxy}'
    
    # Проверяем формат proxy:port или proxy:port@login:password
    proxy_pattern = r'^(https?://|socks[45]://)(([^:@]+)(:[^@]+)?@)?([^:@]+)(:(\d+))$'
    return bool(re.match(proxy_pattern, proxy)), proxy_type

def sanitize_proxy_for_logs(proxy: Optional[str]) -> str:
    """
    Подготавливает строку прокси для логирования (скрывая чувствительные данные).
    
    Args:
        proxy: Строка с прокси
        
    Returns:
        str: Безопасная для логирования версия строки прокси
    """
    if not proxy:
        return "None"
    
    try:
        # Нормализуем прокси URL
        proxy_url = proxy
        if '://' not in proxy_url:
            proxy_url = 'http://' + proxy_url
        
        # Если в прокси есть логин/пароль, то скрываем их
        if '@' in proxy_url:
            scheme_auth, host_part = proxy_url.split('@', 1)
            
            if '://' in scheme_auth:
                scheme, auth = scheme_auth.split('://', 1)
                scheme = scheme + '://'
            else:
                scheme = 'http://'
                auth = scheme_auth
                
            # Возвращаем только схему и хост:порт, скрывая данные авторизации
            return f"{scheme}***@{host_part}"
            
        return proxy_url
    except Exception:
        # В случае ошибки возвращаем исходную строку
        return proxy

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
            logger.info(f"Клиент {account_id} использует прокси: {sanitize_proxy_for_logs(self.proxy_str)}")
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
        proxy_info = f" через прокси {sanitize_proxy_for_logs(self.proxy_str)}" if self.has_proxy else " без прокси"
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
                    logger.error(f"Ошибка соединения с прокси {sanitize_proxy_for_logs(self.proxy_str)}: {e}")
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
        
        proxy_info = f" через прокси {sanitize_proxy_for_logs(self.proxy_str)}" if self.has_proxy else " без прокси"
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
            proxy_type = proxy_dict.get('proxy_type', 'unknown')
            host = proxy_dict.get('addr', 'unknown')
            port = proxy_dict.get('port', 'unknown')
            proxy_str = f"{proxy_type}://{host}:{port}"
            proxy_info = f" с прокси {sanitize_proxy_for_logs(proxy_str)}"
        
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

async def get_trending_posts(client: TelegramClient, channel_ids: List[int], days_back: int = 7, posts_per_channel: int = 10, 
                           min_views: Optional[int] = None, min_reactions: Optional[int] = None, 
                           min_comments: Optional[int] = None, min_forwards: Optional[int] = None, 
                           api_key: Optional[str] = None, non_blocking: bool = False) -> List[Dict]:
    """Получает трендовые посты из каналов с параллельной обработкой медиа."""
    try:
        all_posts = []
        # Чтобы была совместимость с API, нужно учесть, что channel_ids могут приходить разных типов
        flat_channel_ids = []
        for ch_id in channel_ids:
            if isinstance(ch_id, list) or isinstance(ch_id, tuple):
                flat_channel_ids.extend(list(ch_id))
            else:
                flat_channel_ids.append(ch_id)
        
        cutoff_date = datetime.now().replace(tzinfo=None) - timedelta(days=days_back)
        
        # Используем ротацию аккаунтов, если передан api_key
        if api_key:
            try:
                from user_manager import get_active_accounts
                active_accounts = await get_active_accounts(api_key, "telegram")
                if active_accounts and len(active_accounts) > 1:
                    logger.info(f"Распределение запросов между {len(active_accounts)} активными аккаунтами Telegram для trending-posts")
                    
                    # Распределяем каналы между аккаунтами
                    channels_per_account = len(flat_channel_ids) // len(active_accounts) + 1
                    account_channels = [flat_channel_ids[i:i + channels_per_account] for i in range(0, len(flat_channel_ids), channels_per_account)]
                    
                    # Создаем задачи для каждого аккаунта
                    tasks = []
                    for account_data, channels in zip(active_accounts, account_channels):
                        if isinstance(account_data, dict):
                            account_client = account_data.get('client')
                            account_id = account_data.get('id')
                        elif isinstance(account_data, (list, tuple)) and len(account_data) >= 2:
                            account_client = account_data[0]
                            account_id = account_data[1]
                        else:
                            logger.warning(f"Некорректный формат данных аккаунта: {account_data}")
                            continue
                        
                        if account_client:
                            # Подключаем клиента, если это необходимо
                            if telegram_pool is not None:
                                try:
                                    await telegram_pool.connect_client(account_id) # type: ignore
                                except AttributeError:
                                    logger.warning("Метод connect_client отсутствует в telegram_pool.")
                                # Можно добавить обработку других исключений, если connect_client может их вызвать
                            
                            # Добавляем задачу для текущего аккаунта и его каналы
                            task = asyncio.create_task(
                                _process_channels_for_trending(
                                    account_client, 
                                    channels, 
                                    cutoff_date,
                                    posts_per_channel, 
                                    min_views, 
                                    min_reactions, 
                                    min_comments, 
                                    min_forwards,
                                    api_key,
                                    non_blocking
                                )
                            )
                            tasks.append(task)
                            logger.info(f"Создана задача для аккаунта {account_id} с {len(channels)} каналами")
                    
                    # Ждем завершения всех задач
                    if tasks:
                        results = await asyncio.gather(*tasks, return_exceptions=True)
                        
                        # Обрабатываем результаты
                        for result in results:
                            if isinstance(result, Exception):
                                logger.error(f"Ошибка при обработке каналов: {result}")
                            elif isinstance(result, list):
                                all_posts.extend(result)
                        
                        # Сортируем по trend_score
                        all_posts.sort(key=lambda x: x.get('trend_score', 0), reverse=True)
                        return all_posts
            except Exception as e:
                logger.error(f"Ошибка при получении активных аккаунтов: {e}")
        
        # Стандартный метод с одним аккаунтом
        logger.info("Использование стандартного метода с одним аккаунтом для trending posts")
        wrapper = TelegramClientWrapper(client, client.session.filename if client.session else 'unknown')
        
        for channel_id in flat_channel_ids:
            if not isinstance(channel_id, str) and not isinstance(channel_id, int):
                logger.warning(f"Пропуск неверного формата channel_id: {channel_id}, тип {type(channel_id)}")
                continue
                
            try:
                async with REQUEST_SEMAPHORE:
                    # 1. Получаем input_peer с использованием универсальной обертки
                    try:
                        # Передаем метод клиента client.get_input_entity
                        input_peer = await wrapper._make_request(client.get_input_entity, channel_id)
                    except ValueError as e:
                        logger.error(f"Не удалось найти сущность для {channel_id}: {e}")
                        continue
                    except Exception as e:
                        logger.error(f"Ошибка при получении input_entity для {channel_id}: {e}")
                        continue
                    
                    # --- ВОССТАНОВИТЬ ПРОВЕРКУ ЗДЕСЬ ---
                    if input_peer is None:
                         logger.warning(f"Не удалось получить input_peer для {channel_id}, пропуск.")
                         continue

                    # 2. Запрашиваем полную информацию о канале
                    # Передаем тип запроса functions.channels.GetFullChannelRequest
                    full_chat_result = await wrapper._make_group_request(functions.channels.GetFullChannelRequest, input_peer)
                    
                    # --- И ЭТУ ПРОВЕРКУ ТОЖЕ ---
                    if not full_chat_result:
                        logger.warning(f"Не удалось получить результат GetFullChannelRequest для {channel_id}")
                        continue # Пропускаем этот канал

                    # --- И ЭТУ ПРОВЕРКУ ТОЖЕ ---
                    if not hasattr(full_chat_result, 'chats') or not full_chat_result.chats:
                        logger.warning(f"Результат GetFullChannelRequest не содержит chats для {channel_id}")
                        continue # Пропускаем этот канал, если результат невалидный

                    # 3. Извлекаем нужную сущность чата (канал)
                    chat_entity = None
                    for chat_res in full_chat_result.chats: # Теперь безопасно
                        # Сравниваем ID (теперь input_peer точно не None)
                        if hasattr(input_peer, 'channel_id') and str(chat_res.id) == str(input_peer.channel_id):
                             chat_entity = chat_res
                             break
                        elif hasattr(input_peer, 'user_id') and str(chat_res.id) == str(input_peer.user_id):
                             chat_entity = chat_res
                             break
                        elif hasattr(input_peer, 'chat_id') and str(chat_res.id) == str(input_peer.chat_id):
                             chat_entity = chat_res
                             break

                    if not chat_entity:
                         logger.error(f"Не удалось найти сущность чата для {channel_id} в результате GetFullChannelRequest")
                         continue

                    # Используем имя и username из найденной сущности
                    channel_username = getattr(chat_entity, 'username', None)
                    channel_title = getattr(chat_entity, 'title', 'Unknown Title')

                    # Получаем количество подписчиков (или используем 0, если не найдено)
                    subscribers = channel_members_cache.get(str(channel_id), 0)
                    # Дополнительно пытаемся получить из full_chat, если в кэше нет
                    if not subscribers and hasattr(full_chat_result, 'full_chat') and hasattr(full_chat_result.full_chat, 'participants_count'):
                         try:
                             subscribers = full_chat_result.full_chat.participants_count
                             channel_members_cache[str(channel_id)] = subscribers # Обновляем кэш
                         except AttributeError:
                             logger.warning(f"Не удалось получить participants_count из full_chat для {channel_id}")
                             subscribers = 10 # Ставим минимум, если не удалось

                    # Обеспечим, чтобы подписчиков было хотя бы 10 для логарифма
                    subscribers_for_calc = max(subscribers, 10)

                    # Инициализируем список для постов этого канала
                    filtered_posts = []

                    # --- ВОЗВРАЩАЕМ ПОЛУЧЕНИЕ СООБЩЕНИЙ ОТДЕЛЬНО ---
                    posts_result = await wrapper._make_request(client.get_messages,
                                                           input_peer,
                                                           limit=100)  # Получаем больше постов для фильтрации

                    # --- ПРОВЕРКА posts_result ОСТАЕТСЯ ЗДЕСЬ ---
                    if posts_result is not None:
                        # Фильтруем посты по дате и критериям
                        for post in posts_result: # Теперь posts_result не None
                            # Проверяем, что пост содержит хоть какой-то текст
                            if not post.message:
                                continue

                            # Фильтр по просмотрам
                            views = getattr(post, 'views', 0)
                            if min_views is not None and views < min_views:
                                continue

                            # Фильтр по дате
                            post_date = post.date.replace(tzinfo=None)
                            if post_date < cutoff_date:
                                continue

                            # Фильтры по дополнительным параметрам
                            reactions = len(post.reactions.results) if post.reactions else 0
                            if min_reactions is not None and reactions < min_reactions:
                                continue

                            comments = post.replies.replies if post.replies else 0
                            if min_comments is not None and comments < min_comments:
                                continue

                            forwards = post.forwards or 0
                            if min_forwards is not None and forwards < min_forwards:
                                continue

                            # Рассчитываем engagement score
                            post_data = {
                                'id': post.id,
                                'channel_id': str(channel_id),
                                'channel_title': getattr(chat_entity, 'title', getattr(chat_entity, 'first_name', 'Unknown')),
                                'channel_username': channel_username, # Теперь определено выше
                                'subscribers': subscribers, # Теперь определено выше
                                'text': post.message,
                                'views': views,
                                'reactions': reactions,
                                'comments': comments,
                                'forwards': forwards,
                                'date': post.date.isoformat(),
                                'media': []
                            }

                            # Формируем URL в зависимости от типа ID канала
                            if channel_username:
                                post_data['url'] = f"https://t.me/{channel_username}/{post.id}"
                            else:
                                channel_id_for_url = abs(getattr(chat_entity, 'id', 0)) # chat_entity теперь определено выше
                                post_data['url'] = f"https://t.me/c/{channel_id_for_url}/{post.id}"

                            # Обрабатываем медиа
                            if post.media:
                                # Используем instant-генерацию ссылок вместо get_media_info
                                if non_blocking:
                                    # Импортируем функции из media_utils
                                    from media_utils import generate_media_links_with_album, process_media_later

                                    # Получаем ссылки на медиа мгновенно, включая альбомы
                                    media_urls = await generate_media_links_with_album(client, post)
                                    if media_urls:
                                        post_data['media'] = media_urls

                                        # Запускаем обработку медиа асинхронно в фоне
                                        asyncio.create_task(process_media_later(client, post, api_key))
                                else:
                                    # В режиме блокировки используем стандартный метод
                                    from media_utils import get_media_info
                                    # Для вызова get_media_info нужно создать информацию о сообщении в формате API
                                    media_info = await get_media_info(client, post, non_blocking=non_blocking)
                                    if media_info and 'media_urls' in media_info:
                                        post_data["media"] = media_info.get('media_urls', [])

                            # Вычисляем общий score для тренда
                            raw_engagement_score = views + (reactions * 10) + (comments * 20) + (forwards * 50)
                            trend_score = int(raw_engagement_score / math.log10(subscribers_for_calc)) if raw_engagement_score > 0 else 0 # subscribers_for_calc теперь определено выше

                            post_data['trend_score'] = trend_score
                            filtered_posts.append(post_data) # filtered_posts теперь определено выше
                    else:
                         logger.warning(f"Не удалось получить сообщения (posts_result is None) для {channel_id}")

                    # Берем только нужное количество лучших постов из этого канала
                    # (этот код выполнится, даже если posts_result был None, filtered_posts останется пустым)
                    top_posts = sorted(filtered_posts, key=lambda x: x.get('trend_score', 0), reverse=True)[:posts_per_channel] # filtered_posts теперь определено выше
                    all_posts.extend(top_posts)
            except Exception as e:
                logger.error(f"Ошибка при обработке канала {channel_id}: {e}")
                continue
        
        # Сортируем все посты по трендовости
        all_posts.sort(key=lambda x: x.get('trend_score', 0), reverse=True)
        return all_posts
    except Exception as e:
        logger.error(f"Общая ошибка в get_trending_posts: {e}")
        return []

async def _process_channels_for_trending(client, channel_ids, cutoff_date, posts_per_channel, min_views, min_reactions, min_comments, min_forwards, api_key=None, non_blocking=False):
    """Вспомогательная функция для параллельной обработки каналов и получения трендовых постов."""
    wrapper = TelegramClientWrapper(client, client.session.filename if client.session else 'unknown')
    channel_posts = []
    
    for channel_id in channel_ids:
        try:
            logger.info(f"Обработка канала {channel_id} для трендов")
            filtered_posts = []
            
            # Получаем информацию о канале
            try:
                # Не преобразуем channel_id в int, если это строка с @
                if isinstance(channel_id, str) and channel_id.startswith('@'):
                    chat_entity = await client.get_entity(channel_id)
                else:
                    # Для целочисленных ID или строк без @, пробуем преобразовать в int
                    try:
                        chat_entity = await client.get_entity(int(channel_id))
                    except ValueError:
                        # Если не удалось преобразовать в int, используем как есть
                        chat_entity = await client.get_entity(channel_id)
                
                channel_title = getattr(chat_entity, 'title', 'Unknown')
                channel_username = getattr(chat_entity, 'username', None)
                subscribers = chat_entity.participants_count if hasattr(chat_entity, 'participants_count') else None
                
                # Если не удалось получить число подписчиков, запрашиваем полную информацию о канале
                if subscribers is None:
                    try:
                        full_chat = await client(functions.channels.GetFullChannelRequest(channel=chat_entity))
                        subscribers = full_chat.full_chat.participants_count
                    except Exception as e:
                        logger.error(f"Ошибка при получении полной информации о канале {channel_id}: {e}")
                        subscribers = 1000  # Значение по умолчанию, если не удалось получить число подписчиков
            except Exception as e:
                logger.error(f"Ошибка при получении информации о канале {channel_id}: {e}")
                # Убираем установку значений по умолчанию, так как пропускаем итерацию
                continue # <-- Добавляем continue здесь
            
            # Устанавливаем минимальное значение подписчиков для логарифма
            subscribers_for_calc = max(subscribers, 10)
                
            # Получаем историю сообщений
            posts_result = []
            try:
                # Получаем в несколько раз больше постов, чем нужно, чтобы учесть фильтрацию
                limit = posts_per_channel * 3
                # Используем channel_id без преобразования в int
                async for message in client.iter_messages(channel_id, limit=limit):
                    if message.date.replace(tzinfo=None) < cutoff_date:
                        break
                    posts_result.append(message)
            except Exception as e:
                logger.error(f"Ошибка при получении истории сообщений канала {channel_id}: {e}")
                continue
            
            # Фильтруем посты
            for post in posts_result:
                if not post.message:
                    continue
                    
                # Фильтр по просмотрам
                views = getattr(post, 'views', 0)
                if min_views is not None and views < min_views:
                    continue
                
                # Фильтр по дате
                post_date = post.date.replace(tzinfo=None)
                if post_date < cutoff_date:
                    continue
                    
                # Фильтры по дополнительным параметрам
                reactions = len(post.reactions.results) if post.reactions else 0
                if min_reactions is not None and reactions < min_reactions:
                    continue
    
                comments = post.replies.replies if post.replies else 0
                if min_comments is not None and comments < min_comments:
                    continue
                    
                forwards = post.forwards or 0
                if min_forwards is not None and forwards < min_forwards:
                    continue
                
                # Создаем данные поста
                post_data = {
                    'id': post.id,
                    'channel_id': str(channel_id),
                    'channel_title': getattr(chat_entity, 'title', getattr(chat_entity, 'first_name', 'Unknown')),
                    'channel_username': channel_username,
                    'subscribers': subscribers,
                    'text': post.message,
                    'views': views,
                    'reactions': reactions,
                    'comments': comments,
                    'forwards': forwards,
                    'date': post.date.isoformat(),
                    'media': []
                }
                
                # Формируем URL
                if channel_username:
                    if channel_username:
                        post_data['url'] = f"https://t.me/{channel_username}/{post.id}"
                    else:
                        chat_entity = await client.get_entity(channel_id)
                        channel_id_for_url = abs(chat_entity.id)
                        post_data['url'] = f"https://t.me/c/{channel_id_for_url}/{post.id}"
                
                # Обрабатываем медиа
                if post.media:
                    # Используем instant-генерацию ссылок вместо get_media_info
                    if non_blocking:
                        # Импортируем функции из media_utils
                        from media_utils import generate_media_links_with_album, process_media_later
                        
                        # Получаем ссылки на медиа мгновенно, включая альбомы
                        media_urls = await generate_media_links_with_album(client, post)
                        if media_urls:
                            post_data['media'] = media_urls
                            
                            # Запускаем обработку медиа асинхронно в фоне
                            asyncio.create_task(process_media_later(client, post, api_key))
                    else:
                        # В режиме блокировки используем стандартный метод
                        from media_utils import get_media_info
                        # Для вызова get_media_info нужно создать информацию о сообщении в формате API
                        media_info = await get_media_info(client, post, non_blocking=non_blocking)
                        if media_info and 'media_urls' in media_info:
                            post_data["media"] = media_info.get('media_urls', [])
                
                # Вычисляем тренд
                raw_engagement_score = views + (reactions * 10) + (comments * 20) + (forwards * 50)
                trend_score = int(raw_engagement_score / math.log10(subscribers_for_calc)) if raw_engagement_score > 0 else 0
                
                post_data['trend_score'] = trend_score
                filtered_posts.append(post_data)
            
            # Берем топовые посты канала
            top_posts = sorted(filtered_posts, key=lambda x: x.get('trend_score', 0), reverse=True)[:posts_per_channel]
            channel_posts.extend(top_posts)
            
        except Exception as e:
            logger.error(f"Ошибка при обработке канала {channel_id} для трендов: {e}")
    
    return channel_posts

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

# Функция для создания Telegram клиента
async def create_telegram_client(session_path: str, api_id: int, api_hash: str, proxy: Optional[str] = None) -> TelegramClient:
    """Создает клиент Telegram с указанными параметрами."""
    try:
        # Настройка прокси
        proxy_config = None
        if proxy:
            logger.info(f"Настройка прокси для клиента {session_path}: {sanitize_proxy_for_logs(proxy)}")
            # Нормализуем формат прокси
            if '://' not in proxy:
                proxy = 'socks5://' + proxy
                
            # Проверка формата прокси
            is_valid, proxy_type = validate_proxy(proxy)
            if not is_valid:
                logger.error(f"Неверный формат прокси: {sanitize_proxy_for_logs(proxy)}")
                raise ValueError(f"Неверный формат прокси: {sanitize_proxy_for_logs(proxy)}")
            
            # Простое определение типа прокси для Telethon
            proxy_parsed = urlparse(proxy)
            
            # Определим тип прокси на основе URL
            proxy_type_str = 'socks5'
            if proxy.startswith('http://') or proxy.startswith('https://'):
                proxy_type_str = 'http'
            elif proxy.startswith('socks4://'):
                proxy_type_str = 'socks4'
            
            # Создаем конфиг прокси
            proxy_config = {
                'proxy_type': proxy_type_str,
                'addr': proxy_parsed.hostname or '',
                'port': proxy_parsed.port or 1080
            }
            
            # Добавляем учетные данные, если они есть
            if proxy_parsed.username and proxy_parsed.password:
                proxy_config['username'] = proxy_parsed.username
                proxy_config['password'] = proxy_parsed.password
        
        # Создаем клиент с запретом интерактивного ввода
        logger.info(f"Создание клиента Telegram с сессией {session_path}")
        client = TelegramClient(
            session_path,
            api_id,
            api_hash,
            proxy=proxy_config if proxy_config else {},
            device_model="Social Scraper",
            system_version="1.0",
            app_version="1.0",
            lang_code="ru",
            system_lang_code="ru",
            connection_retries=3,  # Запрещаем бесконечные попытки
            retry_delay=1,  # Минимальная задержка между попытками
            auto_reconnect=False,  # Запрещаем автоматическое переподключение
            loop=asyncio.get_event_loop()
        )
        
        return client
    except Exception as e:
        logger.error(f"Ошибка при создании клиента Telegram: {e}")
        logger.error(traceback.format_exc())
        raise

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
                            from media_utils import generate_media_links_with_album, process_media_later
                            media_urls = await generate_media_links_with_album(client, message)
                            if media_urls:
                                post_data['media'] = media_urls
                                # Запускаем обработку медиа асинхронно
                                asyncio.create_task(process_media_later(client, message))
                        
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

async def get_posts_by_period(client, group_ids: List[Union[int, str]], max_posts: int = 100, days_back: int = 7, min_views: int = 0, api_key: Optional[str] = None, non_blocking: bool = False) -> List[Dict]:
    """Получение постов из групп за указанный период."""
    try:
        all_posts = []
        cutoff_date = datetime.now().replace(tzinfo=None) - timedelta(days=days_back)
        
        # Используем переданный клиент напрямую без работы с пулом
        logger.info(f"Получение постов за период от клиента {client.session.filename}")
        all_posts = await _process_groups_for_posts(client, group_ids, max_posts, cutoff_date, min_views, api_key, non_blocking)
        
        # Сортируем посты по дате (новые сверху)
        all_posts.sort(key=lambda x: x["date"], reverse=True)
        
        # Ограничиваем количество постов
        return all_posts[:max_posts]
        
    except Exception as e:
        logger.error(f"Ошибка при получении постов: {e}")
        return []

async def validate_proxy_connection(proxy: Optional[str]) -> Tuple[bool, str]:
    """
    Валидирует строку прокси и проверяет соединение с сервером Telegram.
    
    Args:
        proxy: Строка с прокси в формате scheme://host:port или scheme://user:password@host:port
        
    Returns:
        Tuple[bool, str]: (валиден ли прокси, сообщение)
    """
    if not proxy:
        return False, "Прокси не указан"
    
    # Проверяем формат прокси
    is_valid, proxy_type = validate_proxy(proxy)
    if not is_valid:
        return False, "Неверный формат прокси"
    
    # Нормализуем прокси URL
    if '://' not in proxy:
        if proxy_type == 'socks5':
            proxy = 'socks5://' + proxy
        elif proxy_type == 'socks4':
            proxy = 'socks4://' + proxy
        else:
            proxy = 'http://' + proxy
    
    # Проверяем, установлена ли библиотека aiohttp-socks
    try:
        import aiohttp
        
        # Для HTTP/HTTPS прокси можно использовать стандартный aiohttp
        if proxy.startswith(('http://', 'https://')):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get('https://api.telegram.org', proxy=proxy, timeout=10) as response:
                        if response.status == 200:
                            return True, "Прокси работает"
                        else:
                            return False, f"Ошибка соединения: HTTP {response.status}"
            except Exception as e:
                return False, f"Ошибка соединения: {str(e)}"
        
        # Для SOCKS прокси нужна библиотека aiohttp-socks
        # Проверяем, установлена ли она
        try:
            from aiohttp_socks import ProxyConnector
            
            # Если библиотека установлена, используем её
            connector = ProxyConnector.from_url(proxy)
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.get('https://api.telegram.org', timeout=10) as response:
                    if response.status == 200:
                        return True, "Прокси работает"
                    else:
                        return False, f"Ошибка соединения: HTTP {response.status}"
        except ImportError:
            # Если библиотека не установлена, вернем соответствующее сообщение
            return False, "Необходимо установить библиотеку aiohttp-socks для работы с SOCKS прокси"
        except Exception as e:
            return False, f"Ошибка соединения: {str(e)}"
    except Exception as e:
        return False, f"Ошибка при проверке прокси: {str(e)}"