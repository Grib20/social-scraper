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
import random
import base64
import io
from telethon import TelegramClient # Убедимся, что импортирован
from telethon.tl.types import InputPeerChannel, InputPeerUser, MessageMediaPhoto, MessageMediaDocument, DocumentAttributeFilename, Message # Добавили Message
from telethon.errors import ChannelInvalidError, ChannelPrivateError, UserDeactivatedBanError, AuthKeyError, FloodWaitError, UserNotParticipantError, UsernameNotOccupiedError, UsernameInvalidError
# Импортируем КЛАСС пула, а не экземпляр
from client_pools import TelegramClientPool, TELEGRAM_DEGRADED_MODE_DELAY 

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

# --- Добавляем константы (значения из media_utils.py) ---
MAX_FILE_SIZE = 50 * 1024 * 1024 
S3_LINK_TEMPLATE = os.getenv('S3_LINK_TEMPLATE', 'https://scraper.website.yandexcloud.net/{filename}')
# ----------------------------------------------------

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
        self.account_id = account_id # Используем переданный ID напрямую
        self.api_key = api_key
        logger.info(f"TelegramClientWrapper инициализирован с account_id: {self.account_id}")

        # --- Убираем сложную логику извлечения ID, оставляем только получение инфо о прокси ---
        proxy_dict = getattr(client.session, 'proxy', None)
        self.has_proxy = proxy_dict is not None
        if self.has_proxy:
            self.proxy_type = proxy_dict.get('proxy_type', 'unknown') if isinstance(proxy_dict, dict) else 'unknown'
            host = proxy_dict.get('addr', 'unknown') if isinstance(proxy_dict, dict) else 'unknown'
            port = proxy_dict.get('port', 'unknown') if isinstance(proxy_dict, dict) else 'unknown'
            self.proxy_str = f"{self.proxy_type}://{host}:{port}"
            logger.info(f"Клиент {self.account_id} использует прокси.")
        else:
            logger.info(f"Клиент {self.account_id} работает без прокси")
        # --- Конец упрощения ---

        self.last_request_time = 0
        self.last_group_request_time = 0
        self.requests_count = 0
        self.degraded_mode = False
        
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
                await update_account_usage_redis(self.api_key, self.account_id, "telegram")
            except ImportError:
                # Если Redis не доступен, используем обычное обновление
                from user_manager import update_account_usage
                await update_account_usage(self.api_key, self.account_id, "telegram")
        
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

# --- Вспомогательная функция (код без изменений, только проверяем сигнатуру) ---
async def _find_channels_with_account(
    client: TelegramClient,
    account_id: str, # <<<--- ДОБАВЛЕН ПАРАМЕТР
    keywords: List[str],
    min_members: int = 100000,
    max_channels: int = 20,
    api_key: Optional[str] = None
    ) -> Dict[int, Dict]:
    """Ищет каналы с использованием одного конкретного клиента и обновляет статистику.

    Args:
        client: Экземпляр TelegramClient.
        account_id: ID аккаунта (UUID из БД).
        keywords: Список ключевых слов.
        min_members: Минимальное количество участников.
        max_channels: Максимальное количество каналов.
        api_key: API ключ пользователя для обновления статистики.

    Returns:
        Словарь найденных каналов.
    """
    found_channels_dict = {}
    processed_keywords = set()

    # Используем переданный account_id для логирования
    logger.info(f"Запуск поиска каналов для клиента ({account_id}) по словам: {keywords}")

    # <<<--- Создаем wrapper с переданным account_id ---
    if not api_key:
        logger.warning(f"[Acc: {account_id}] API ключ не предоставлен для _find_channels_with_account, статистика не будет обновлена.")
    # Передаем корректный account_id (UUID) в wrapper
    wrapper = TelegramClientWrapper(client, account_id, api_key)
    # --------------------------------------------------

    try:
        # <<<--- Проверку подключения/авторизации можно убрать, если пул это гарантирует
        # if not client.is_connected(): ...
        # if not await client.is_user_authorized(): ...

        # --- Основной цикл поиска ---
        for keyword in keywords:
            if keyword in processed_keywords:
                continue
            processed_keywords.add(keyword)

            try:
                logger.debug(f"[Acc: {account_id}] Поиск по слову: '{keyword}'")
                # <<<--- Используем wrapper._make_request для SearchRequest ---
                result = await wrapper._make_request( # Заменяем client(...) на wrapper._make_request
                    functions.contacts.SearchRequest,
                    q=keyword,
                    limit=max_channels * 2
                )
                # ------------------------------------------------------------

                if result is not None and hasattr(result, 'chats'):
                    for chat in result.chats:
                        if isinstance(chat, types.Channel) and getattr(chat, 'megagroup', False) is False:
                            channel_id = chat.id
                            if channel_id not in found_channels_dict:
                                try:
                                    participants_count = None
                                    if chat.access_hash is not None:
                                        try:
                                            input_channel = types.InputChannel(channel_id=chat.id, access_hash=chat.access_hash)
                                            # <<<--- Используем wrapper._make_request для GetFullChannelRequest ---
                                            full_channel = await wrapper._make_request( # Заменяем client(...) на wrapper._make_request
                                                GetFullChannelRequest,
                                                channel=input_channel
                                            )
                                            # ------------------------------------------------------------------
                                            if full_channel and hasattr(full_channel, 'full_chat') and full_channel.full_chat:
                                                participants_count = full_channel.full_chat.participants_count
                                        except Exception as e_gfc:
                                            logger.error(f"[Acc: {account_id}] Ошибка GetFullChannelRequest для канала {chat.id}: {e_gfc}")
                                    else:
                                        logger.warning(f"[Acc: {account_id}] Канал {chat.id} ('{chat.title}') не имеет access_hash. Пропускаем GetFullChannelRequest.")

                                    if participants_count is not None and participants_count >= min_members:
                                        username = getattr(chat, 'username', None)
                                        link = f"https://t.me/{username}" if username else None
                                        found_channels_dict[channel_id] = {
                                            'id': channel_id,
                                            'title': chat.title,
                                            'username': username,
                                            'link': link,
                                            'members_count': participants_count
                                        }
                                        logger.debug(f"[Acc: {account_id}] Найден подходящий канал: {chat.title} ({participants_count} участников)")
                                        if len(found_channels_dict) >= max_channels:
                                            logger.info(f"[Acc: {account_id}] Достигнут лимит ({max_channels}) найденных каналов по слову '{keyword}'.")
                                            break
                                except ChannelPrivateError:
                                    pass
                                except Exception as e_full:
                                    logger.error(f"[Acc: {account_id}] Ошибка при получении полной информации о канале {channel_id} ('{chat.title}'): {e_full}")
                    if len(found_channels_dict) >= max_channels:
                         logger.info(f"[Acc: {account_id}] Достигнут общий лимит ({max_channels}) найденных каналов.")
                         break
                else:
                     logger.debug(f"[Acc: {account_id}] Результат поиска по '{keyword}' не содержит 'chats'.")

            except FloodWaitError as e:
                logger.warning(f"[Acc: {account_id}] FloodWaitError при поиске по слову '{keyword}': ждем {e.seconds} секунд")
                await asyncio.sleep(e.seconds + 1)
            except (UsernameNotOccupiedError, UsernameInvalidError):
                 pass
            except Exception as e_search:
                logger.error(f"[Acc: {account_id}] Ошибка при поиске по слову '{keyword}': {e_search}", exc_info=True)

    except AuthKeyError:
         logger.error(f"[Acc: {account_id}] Ключ авторизации невалиден. Поиск прерван.")
    except UserDeactivatedBanError:
         logger.error(f"[Acc: {account_id}] Аккаунт заблокирован. Поиск прерван.")
    except Exception as e_outer:
         logger.error(f"[Acc: {account_id}] Общая ошибка в _find_channels_with_account: {e_outer}", exc_info=True)

    logger.info(f"[Acc: {account_id}] Завершен поиск для клиента. Найдено уникальных: {len(found_channels_dict)}")
    return found_channels_dict

# --- ИЗМЕНЕННАЯ Функция find_channels ---
async def find_channels(
    telegram_pool: TelegramClientPool,
    keywords: List[str],
    min_members: int = 100000,
    max_channels: int = 20,
    api_key: Optional[str] = None
    ) -> List[Dict]:
    """Находит каналы по ключевым словам, распределяя слова между активными аккаунтами."""
    logger.info(f"Поиск каналов по ключевым словам: {keywords} для api_key: {api_key}")

    # Проверки входных данных
    if not telegram_pool: logger.error("Экземпляр telegram_pool не передан в find_channels."); return []
    if not api_key: logger.error("API ключ не передан в find_channels."); return []
    if not keywords: logger.warning("Список ключевых слов пуст."); return []

    # Получаем активные аккаунты
    try:
        active_accounts = await telegram_pool.get_active_clients(api_key)
        if not active_accounts:
             logger.warning(f"Не найдено активных Telegram аккаунтов для ключа {api_key} через пул."); return []
        num_accounts = len(active_accounts)
        logger.info(f"Найдено {num_accounts} активных аккаунтов. Распределение {len(keywords)} ключевых слов...")
    except Exception as e_acc:
        logger.error(f"Ошибка при получении активных аккаунтов через пул в find_channels: {e_acc}", exc_info=True)
        return []

    all_found_channels_dict: Dict[int, Dict] = {}
    tasks = []
    keywords_distributed = 0

    # --- Распределение ключевых слов и запуск задач --- 
    for i, account_info in enumerate(active_accounts):
        account_id = account_info.get('id')
        if not account_id: logger.warning(f"Найден аккаунт без ID в списке активных (индекс {i}). Пропуск."); continue
        
        client = telegram_pool.get_client(account_id)
        if not client or not isinstance(client, TelegramClient): logger.warning(f"Клиент не найден/некорректен для аккаунта {account_id}. Пропуск."); continue

        # --- Проверка подключения/авторизации ПЕРЕД созданием задачи ---
        try:
            if not client.is_connected():
                logger.info(f"[find_channels] Клиент {account_id} не подключен. Попытка подключения...")
                await client.connect()
                if not client.is_connected(): logger.error(f"[find_channels] Не удалось подключить клиент {account_id}. Пропуск."); continue
                logger.info(f"[find_channels] Клиент {account_id} успешно подключен.")
            if not await client.is_user_authorized():
                logger.warning(f"[find_channels] Клиент {account_id} НЕ авторизован. Пропуск.")
                continue
            logger.info(f"[find_channels] Клиент {account_id} готов к работе.")
        except (FloodWaitError, AuthKeyError, ConnectionError, Exception) as e_check:
            logger.error(f"[find_channels] Ошибка при проверке/подключении клиента {account_id}: {e_check}. Пропуск.")
            continue
        # --- Конец проверки --- 

        # Распределяем ключевые слова (round-robin)
        keywords_for_account = keywords[i::num_accounts] # Берем каждое N-ое слово, начиная с i
        
        if not keywords_for_account:
             logger.info(f"Для аккаунта {account_id} не осталось ключевых слов для обработки.")
             continue
        
        keywords_distributed += len(keywords_for_account)
        logger.info(f"Аккаунт {account_id} будет искать по {len(keywords_for_account)} словам: {keywords_for_account[:3]}..." )
        
        # Создаем задачу, передавая только нужные слова
        tasks.append(asyncio.create_task(
            _find_channels_with_account(
                client=client, # Передаем рабочий клиент
                account_id=account_id, 
                keywords=keywords_for_account, # <<< Передаем ЧАСТЬ слов
                min_members=min_members,
                max_channels=max_channels,
                api_key=api_key
            )
        ))

    logger.info(f"Всего распределено ключевых слов: {keywords_distributed}. Запускаем {len(tasks)} задач поиска...")

    # Ожидаем завершения всех задач
    if tasks:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for i, result in enumerate(results):
            # Получаем ID для лога, даже если результат - ошибка
            acc_id_log = "unknown_id"
            if i < len(active_accounts) and isinstance(active_accounts[i], dict):
                acc_id_log = active_accounts[i].get('id', f"index_{i}")

            if isinstance(result, Exception):
                logger.error(f"Ошибка при поиске каналов аккаунтом {acc_id_log}: {result}", exc_info=result)
            elif isinstance(result, dict):
                logger.info(f"Аккаунт {acc_id_log} нашел {len(result)} каналов.")
                # Объединяем результаты, новые каналы заменят старые, если ID совпадут
                all_found_channels_dict.update(result)
            else:
                logger.warning(f"Неожиданный результат поиска от аккаунта {acc_id_log}: {type(result)}")

    # Преобразуем объединенный словарь в список
    channels_list = list(all_found_channels_dict.values())

    # Сортируем найденные каналы по количеству участников (по убыванию)
    sorted_channels = sorted(channels_list, key=lambda x: x.get('members_count', 0), reverse=True)

    # Ограничиваем итоговый список
    final_channels = sorted_channels[:max_channels]

    logger.info(f"Итого найдено {len(final_channels)} уникальных каналов Telegram после ротации.")
    return final_channels

# --- Конец ИЗМЕНЕННОЙ Функции find_channels ---

def _extract_media_details(media):
    """Извлекает детали медиа для дальнейшей обработки."""
    media_type = 'unknown'
    file_id = None
    media_object = None # Объект Telethon для скачивания
    file_ext = '.bin'
    mime_type = 'application/octet-stream'
    file_size = 0 # <<< Инициализируем размер файла

    # Фото
    # <<< Добавляем проверку на PhotoEmpty перед доступом к photo.sizes >>>
    if isinstance(media, types.MessageMediaPhoto) and media.photo and not isinstance(media.photo, types.PhotoEmpty):
        media_type = 'photo'
        media_object = media.photo # Берем сам объект Photo
        if hasattr(media_object, 'id'):
            file_id = str(media_object.id)
            file_ext = '.jpg'
            mime_type = 'image/jpeg'
            # <<< Размер для фото: берем размер самой большой версии (photo.sizes)
            if hasattr(media_object, 'sizes') and media_object.sizes:
                largest_size = max(media_object.sizes, key=lambda s: getattr(s, 'size', 0), default=None)
                # --- Добавляем проверку типа перед доступом к size ---
                if largest_size and not isinstance(largest_size, (types.PhotoSizeEmpty, types.PhotoCachedSize, types.PhotoStrippedSize, types.PhotoPathSize, types.PhotoSizeProgressive)) and hasattr(largest_size, 'size'):
                # ----------------------------------------------------
                    file_size = largest_size.size 
        else:
            logger.warning("Обнаружен объект Photo без ID.")
            return None # Не можем обработать без ID
    # Документ (видео, гиф, стикер, аудио, файл)
    elif isinstance(media, types.MessageMediaDocument) and media.document:
        media_object = media.document # Берем сам объект Document
        if hasattr(media_object, 'id'):
            file_id = str(media_object.id)
            mime_type = getattr(media_object, 'mime_type', 'application/octet-stream').lower()
            file_size = getattr(media_object, 'size', 0) # <<< Получаем размер документа

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

    return media_type, file_id, media_object, file_ext, mime_type, file_size # <<< Возвращаем file_size


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
    background_tasks_queue: List[Dict],
    api_key: Optional[str] = None # <<<--- Добавляем api_key
    ) -> List[Dict]:
    processed_posts_for_these_channels = []
    logger = logging.getLogger(__name__)

    # <<<--- Создаем wrapper здесь ---
    if not api_key:
        logger.warning(f"[Acc: {account_id}] API ключ не предоставлен для _process_channels_for_trending, статистика не будет обновлена.")
    wrapper = TelegramClientWrapper(client, account_id, api_key)
    # ------------------------------

    try: # Добавляем try/except вокруг проверки клиента
        # <<<--- Проверку подключения/авторизации можно убрать, если пул это гарантирует
        # if not client.is_connected(): ...

        for channel_id_input in channel_ids:
            channel_processed_posts_count = 0
            peer_identifier = channel_id_input # Initialize for logging in except blocks
            try:
                logger.info(f"--- [Acc: {account_id}] Начало обработки канала Input ID: {channel_id_input} ---")

                # --- Подготовка ID и username ---
                entity_username = None
                chat_entity_id_for_data = None
                try:
                    numeric_id = int(channel_id_input)
                    chat_entity_id_for_data = numeric_id
                    if numeric_id > 0: peer_identifier = int(f"-100{numeric_id}")
                    else: peer_identifier = numeric_id
                except ValueError:
                    if isinstance(channel_id_input, str): peer_identifier = channel_id_input.lstrip('@'); entity_username = peer_identifier
                    else: logger.warning(f"[Шаг 0] Неожиданный тип ID: {type(channel_id_input)}. Пропуск."); continue
                logger.debug(f"[Шаг 0] Идентификатор для запросов: {peer_identifier} (тип: {type(peer_identifier)})")

                # --- Используем wrapper для get_entity ---
                chat_entity = None
                logger.debug(f"[Шаг 1] Вызов wrapper.get_entity({peer_identifier}) для получения информации")
                try:
                    # chat_entity = await client.get_entity(peer_identifier)
                    chat_entity = await wrapper.make_high_level_request(wrapper.client.get_entity, peer_identifier)
                    if not chat_entity: logger.error(f"[Шаг 1] ОШИБКА: get_entity через wrapper вернул None для {peer_identifier}. Пропуск."); continue
                    logger.debug(f"[Шаг 1] Успех! Получена сущность: ID={chat_entity.id}, Type={type(chat_entity)}")
                    if not entity_username: entity_username = getattr(chat_entity, 'username', None)
                    chat_entity_id_for_data = chat_entity.id
                except (ValueError, TypeError) as e_val_type: logger.warning(f"[Шаг 1] ОШИБКА ({type(e_val_type).__name__}) для {peer_identifier}. Пропуск.", exc_info=True); continue
                except FloodWaitError as flood: logger.warning(f"[Шаг 1] Flood wait ({flood.seconds}s) для {peer_identifier}. Пропуск."); await asyncio.sleep(flood.seconds); continue
                except Exception as e_entity: logger.error(f"[Шаг 1] Неожиданная ошибка get_entity для {peer_identifier}: {e_entity}", exc_info=True); continue
                # -----------------------------------------

                # --- Извлечение данных из chat_entity ---
                channel_title = getattr(chat_entity, 'title', None) or f"{getattr(chat_entity, 'first_name', '')} {getattr(chat_entity, 'last_name', '')}".strip() or f"Unknown ({chat_entity_id_for_data})"
                subscribers = getattr(chat_entity, 'participants_count', None)

                # --- Используем wrapper для GetFullChannelRequest ---
                if subscribers is None or subscribers == 0:
                    logger.debug(f"Subscribers count is {subscribers}. Trying GetFullChannelRequest via wrapper...")
                    try:
                        input_peer = None
                        if isinstance(chat_entity, types.Channel) and hasattr(chat_entity, 'access_hash') and chat_entity.access_hash is not None:
                            input_peer = types.InputChannel(channel_id=chat_entity.id, access_hash=chat_entity.access_hash)
                        if input_peer and isinstance(input_peer, types.InputChannel):
                            # full_chat_result = await client(functions.channels.GetFullChannelRequest(channel=input_peer))
                            full_chat_result = await wrapper._make_request(functions.channels.GetFullChannelRequest, channel=input_peer)
                            subscribers = getattr(getattr(full_chat_result, 'full_chat', None), 'participants_count', None)
                            if subscribers is not None: logger.debug(f"Successfully retrieved subscribers ({subscribers}) via GetFullChannelRequest.")
                            else: logger.warning(f"GetFullChannelRequest did not return participants_count for {chat_entity.id}.")
                        else: logger.warning(f"Could not create InputPeerChannel for GetFullChannelRequest for {chat_entity.id}.")
                    except Exception as e_full: logger.warning(f"Error getting full channel info for {chat_entity.id}: {e_full}")
                # -----------------------------------------------

                # --- Используем wrapper для get_entity(username) ---
                if subscribers is None and entity_username:
                    logger.debug(f"Failed to get subscribers via ID/GetFullChannelRequest. Trying wrapper.get_entity('{entity_username}')...")
                    try:
                        # refreshed_entity = await client.get_entity(entity_username)
                        refreshed_entity = await wrapper.make_high_level_request(wrapper.client.get_entity, entity_username)
                        subscribers = getattr(refreshed_entity, 'participants_count', None)
                        if subscribers is not None: logger.debug(f"Successfully retrieved subscribers ({subscribers}) via get_entity(username).")
                        else: logger.warning(f"get_entity('{entity_username}') did not return participants_count.")
                    except Exception as e_refresh: logger.warning(f"Error getting entity by username '{entity_username}': {e_refresh}")
                # -------------------------------------------------

                # --- Используем wrapper для SearchRequest ---
                if subscribers is None and entity_username:
                    logger.debug(f"Failed to get subscribers actively. Trying SearchRequest via wrapper for '{entity_username}'...")
                    try:
                        # search_result = await client(functions.contacts.SearchRequest(q=entity_username, limit=5))
                        search_result = await wrapper._make_request(functions.contacts.SearchRequest, q=entity_username, limit=5)
                        found_chat_with_hash = None
                        if search_result and hasattr(search_result, 'chats'): # Add check here
                            for found_chat in search_result.chats:
                                if found_chat.id == chat_entity.id and hasattr(found_chat, 'access_hash') and found_chat.access_hash: found_chat_with_hash = found_chat; break
                        if found_chat_with_hash:
                            logger.debug(f"Found channel via search with access_hash. Trying GetFullChannelRequest again via wrapper...")
                            try:
                                input_channel = types.InputChannel(channel_id=found_chat_with_hash.id, access_hash=found_chat_with_hash.access_hash)
                                # full_chat_result = await client(functions.channels.GetFullChannelRequest(channel=input_channel))
                                full_chat_result = await wrapper._make_request(functions.channels.GetFullChannelRequest, channel=input_channel)
                                subscribers = getattr(getattr(full_chat_result, 'full_chat', None), 'participants_count', None)
                                if subscribers is not None: logger.info(f"Successfully retrieved subscribers ({subscribers}) via Search + GetFullChannelRequest.")
                                else: logger.warning("GetFullChannelRequest after search did not return participants_count.")
                            except Exception as e_gfc_search: logger.warning(f"Error during GetFullChannelRequest after search: {e_gfc_search}")
                        else: logger.warning(f"Channel '{entity_username}' not found via SearchRequest or no access_hash in result.")
                    except FloodWaitError as flood: logger.warning(f"Flood wait ({flood.seconds}s) during SearchRequest for '{entity_username}'."); await asyncio.sleep(flood.seconds)
                    except Exception as e_search: logger.warning(f"Error during SearchRequest for '{entity_username}': {e_search}")
                # -------------------------------------------

                # ... (проверка кэша и установка subscribers_for_calc как раньше) ...
                if subscribers is None:
                    cache_key = f"@{entity_username}" if entity_username else str(chat_entity_id_for_data)
                    cached_subs = channel_members_cache.get(cache_key)
                    if cached_subs is not None: logger.info(f"Subscribers count not found actively for {chat_entity_id_for_data}. Using cached value: {cached_subs}"); subscribers = cached_subs
                    else: logger.warning(f"Subscribers count not found actively AND not in cache for {chat_entity_id_for_data}. Cache key tried: '{cache_key}'.")

                if subscribers is None: logger.warning(f"Using 10 for trend score calculation for channel {chat_entity_id_for_data} ('{entity_username}' / {peer_identifier})."); subscribers_count_for_calc = 10
                else: subscribers_count_for_calc = int(subscribers)
                subscribers_for_calc = max(subscribers_count_for_calc, 10)
                logger.debug(f"Информация для '{channel_title}' (ID: {chat_entity_id_for_data}, User: {entity_username}): Подписчики={subscribers}, Используется для расчета={subscribers_for_calc}")

                # --- Шаг 2: Получение Истории через iter_messages ---
                logger.debug(f"[Шаг 2] Начинаем итерацию сообщений для {channel_id_input} (передаем: {peer_identifier})")
                processed_in_channel_count = 0
                # --- Обновляем статистику перед iter_messages (приблизительно) ---
                if api_key:
                    try:
                        from redis_utils import update_account_usage_redis
                        await update_account_usage_redis(api_key, account_id, "telegram") # Используем account_id из параметров
                        logger.info(f"[Acc: {account_id}] [Chan: {channel_id_input}] Статистика обновлена (Redis) перед iter_messages.")
                    except ImportError:
                        from user_manager import update_account_usage
                        await update_account_usage(api_key, account_id, "telegram")
                        logger.info(f"[Acc: {account_id}] [Chan: {channel_id_input}] Статистика обновлена (user_manager) перед iter_messages.")
                    except Exception as stats_err:
                        logger.error(f"[Acc: {account_id}] [Chan: {channel_id_input}] Ошибка обновления статистики перед iter_messages: {stats_err}")
                else:
                    logger.warning(f"[Acc: {account_id}] [Chan: {channel_id_input}] API ключ не предоставлен, статистика не обновлена перед iter_messages.")
                # -----------------------------------------------------------------------

                # <<<--- ИСПРАВЛЯЕМ ОТСТУПЫ ЗДЕСЬ (УБИРАЕМ ЛИШНИЙ ОТСТУП) ---<<<
                cutoff_date_naive = cutoff_date.replace(tzinfo=None)
                iter_count = 0 # Счетчик итераций
                logger.debug(f"[Acc: {account_id}][Chan: {channel_id_input}] Начинаем client.iter_messages. Cutoff: {cutoff_date_naive.isoformat()}, Min Views: {min_views}")
                
                # --- ДИАГНОСТИКА: Пытаемся получить 1 сообщение напрямую ---
                try:
                    latest_messages = await client.get_messages(peer_identifier, limit=1)
                    if latest_messages:
                        logger.debug(f"[Acc: {account_id}][Chan: {channel_id_input}] DIAGNOSTIC: get_messages(limit=1) successful. Latest post ID: {latest_messages[0].id}, Date: {latest_messages[0].date}")
                    else:
                        logger.warning(f"[Acc: {account_id}][Chan: {channel_id_input}] DIAGNOSTIC: get_messages(limit=1) returned an empty list.")
                except Exception as e_get_msg:
                    logger.error(f"[Acc: {account_id}][Chan: {channel_id_input}] DIAGNOSTIC: Error calling get_messages(limit=1): {e_get_msg}", exc_info=True)
                # --------------------------------------------------------

                # --- ДИАГНОСТИКА: Оборачиваем iter_messages в try...except ---
                try:
                    async for post in client.iter_messages(entity=peer_identifier, limit=3000):
                        iter_count += 1
                        if not post or not isinstance(post, types.Message) or not post.date:
                            logger.debug(f"[Acc: {account_id}][Chan: {channel_id_input}] Iter {iter_count}: Пропуск (None, не Message или нет даты)")
                            continue
                        
                        post_date_naive = post.date.replace(tzinfo=None) if post.date.tzinfo is not None else post.date
                        logger.debug(f"[Acc: {account_id}][Chan: {channel_id_input}] Iter {iter_count}: Post ID={post.id}, Date={post_date_naive.isoformat()}")
                        
                        if post_date_naive < cutoff_date_naive:
                            logger.debug(f"[Acc: {account_id}][Chan: {channel_id_input}] Iter {iter_count}: Пост {post.id} слишком старый ({post_date_naive.isoformat()} < {cutoff_date_naive.isoformat()}). Прерываем цикл.")
                            break # Прерываем цикл, т.к. сообщения идут от новых к старым
                            
                        # --- УБИРАЕМ ПРОВЕРКУ ТЕКСТА ЗДЕСЬ ---
                        # if not post.message:
                        #     logger.debug(f"[Acc: {account_id}][Chan: {channel_id_input}] Iter {iter_count}: Пост {post.id} пропущен (нет текста).")
                        #     continue
                        # -------------------------------------
                            
                        views = getattr(post, 'views', 0) if post.views is not None else 0
                        if min_views is not None and views < min_views:
                            logger.debug(f"[Acc: {account_id}][Chan: {channel_id_input}] Iter {iter_count}: Пост {post.id} пропущен (просмотры {views} < min_views {min_views}).")
                            continue
                            
                        reactions = 0; comments = 0; forwards = 0
                        if post.reactions and post.reactions.results: 
                            reactions = sum(r.count for r in post.reactions.results)
                        if min_reactions is not None and reactions < min_reactions:
                            logger.debug(f"[Acc: {account_id}][Chan: {channel_id_input}] Iter {iter_count}: Пост {post.id} пропущен (реакции {reactions} < min_reactions {min_reactions}).")
                            continue
                            
                        if post.replies: 
                            comments = post.replies.replies
                        if min_comments is not None and comments < min_comments:
                            logger.debug(f"[Acc: {account_id}][Chan: {channel_id_input}] Iter {iter_count}: Пост {post.id} пропущен (комментарии {comments} < min_comments {min_comments}).")
                            continue
                            
                        if post.forwards: 
                            forwards = post.forwards
                        if min_forwards is not None and forwards < min_forwards:
                            logger.debug(f"[Acc: {account_id}][Chan: {channel_id_input}] Iter {iter_count}: Пост {post.id} пропущен (форварды {forwards} < min_forwards {min_forwards}).")
                            continue

                        # --- Если пост прошел все проверки, логируем это --- 
                        # logger.debug(f"[Acc: {account_id}][Chan: {channel_id_input}] Iter {iter_count}: Пост {post.id} ПРОШЕЛ все фильтры. Добавляем.") # Логируем после проверки текста
                        
                        # --- Собираем данные поста ---
                        post_data = {
                            'id': post.id,
                            'channel_id': str(chat_entity_id_for_data),
                            'channel_title': channel_title,
                            'channel_username': entity_username,
                            'subscribers': subscribers,
                            'text': "", # Инициализируем текст пустым
                            'views': views, 'reactions': reactions, 'comments': comments, 'forwards': forwards,
                            'date': post.date.isoformat(),
                            'url': f"https://t.me/{entity_username}/{post.id}" if entity_username else f"https://t.me/c/{abs(chat_entity_id_for_data)}/{post.id}",
                            'media': [],
                            'trend_score': 0.0
                        }

                        # --- Обработка медиа и Поиск Текста ---
                        media_objects_to_process = []
                        post_text_found = post.message or "" # Получаем текст из основного сообщения

                        if hasattr(post, 'grouped_id') and post.grouped_id: # Проверяем, является ли пост частью альбома
                            logger.debug(f"[Acc: {account_id}][Chan: {channel_id_input}] Iter {iter_count}: Post {post.id} is part of album {post.grouped_id}. Fetching album messages...")
                            try:
                                album_messages = await get_album_messages(wrapper, peer_identifier, post)
                                found_text_in_album = False
                                for album_msg in album_messages:
                                    # Собираем медиа со всех сообщений альбома
                                    if album_msg and album_msg.media: 
                                        media_objects_to_process.append(album_msg.media)
                                    # Ищем первый непустой текст в альбоме
                                    if album_msg and album_msg.message and not found_text_in_album:
                                        post_text_found = album_msg.message # Перезаписываем текст, если нашли в альбоме
                                        found_text_in_album = True
                                        logger.debug(f"[Acc: {account_id}][Chan: {channel_id_input}] Iter {iter_count}: Found text for album {post.grouped_id} in message {album_msg.id}")
                                
                                if not found_text_in_album:
                                    logger.debug(f"[Acc: {account_id}][Chan: {channel_id_input}] Iter {iter_count}: No text found in album {post.grouped_id}, using main message text ('{post_text_found[:20]}...')")
                            except Exception as e_album: 
                                logger.error(f"[Acc: {account_id}][Chan: {channel_id_input}] Error fetching album for post {post.id}: {e_album}")
                                # Используем текст из основного сообщения, если был

                        elif post.media: # Если это не альбом, но есть медиа
                            media_objects_to_process.append(post.media)

                        # --- НОВАЯ ПРОВЕРКА: Пропускаем, если текст так и не найден ---
                        if not post_text_found:
                            logger.debug(f"[Acc: {account_id}][Chan: {channel_id_input}] Iter {iter_count}: Пост {post.id} пропущен (нет текста ни в основном сообщении, ни в альбоме).")
                            continue
                        # ----------------------------------------------------------
                        
                        # Логируем успешное прохождение всех фильтров (включая текст)
                        logger.debug(f"[Acc: {account_id}][Chan: {channel_id_input}] Iter {iter_count}: Пост {post.id} ПРОШЕЛ все фильтры (включая текст). Добавляем.")

                        # Обновляем текст в post_data
                        post_data['text'] = post_text_found

                        # --- Модифицированная обработка медиа --- 
                        media_tasks_for_post = {} # Словарь для уникальных задач загрузки
                        processed_media_urls = set() # Чтобы не добавлять одинаковые URL
                        # Инициализируем post_data['media'] как пустой список
                        post_data['media'] = []

                        for media_obj_container in media_objects_to_process:
                            media_details_tuple = _extract_media_details(media_obj_container)
                            if not media_details_tuple: continue
                            
                            # Распаковываем с file_size
                            media_type, file_id, media_object, file_ext, mime_type, file_size = media_details_tuple
                            
                            # Пропускаем ненужные типы или если нет ID/объекта
                            if media_type in ['poll', 'geo', 'contact', 'venue'] or not file_id or not media_object: continue
                            
                            is_placeholder = False
                            s3_url_to_add = None
                            s3_filename = None # <<< Инициализируем None
                            s3_thumb_filename = None # <<< Инициализируем None
                            
                            # Проверяем размер видео
                            if media_type == 'video' and file_size > MAX_FILE_SIZE:
                                is_placeholder = True
                                s3_thumb_filename = f"mediaTg/{file_id}_thumb.jpg"
                                s3_url_to_add = S3_LINK_TEMPLATE.format(filename=s3_thumb_filename)
                                logger.debug(f"[Acc: {account_id}][Chan: {channel_id_input}] Post {post.id}: Media {file_id} ({media_type}) - Large file ({file_size} > {MAX_FILE_SIZE}), using placeholder URL: {s3_url_to_add}")
                            else:
                                s3_filename = f"mediaTg/{file_id}{file_ext}"
                                s3_url_to_add = S3_LINK_TEMPLATE.format(filename=s3_filename)
                                logger.debug(f"[Acc: {account_id}][Chan: {channel_id_input}] Post {post.id}: Media {file_id} ({media_type}) - Regular file ({file_size}), using main URL: {s3_url_to_add}")

                            # Добавляем информацию о медиа в структурированном виде, если URL еще не добавлен
                            if s3_url_to_add and s3_url_to_add not in processed_media_urls:
                                media_entry = {
                                    'type': media_type,
                                    'url': s3_url_to_add,
                                    'is_placeholder': is_placeholder,
                                    'mime_type': mime_type # Добавляем mime_type для информации
                                }
                                post_data['media'].append(media_entry)
                                processed_media_urls.add(s3_url_to_add)
                                logger.debug(f"[Acc: {account_id}][Chan: {channel_id_input}] Post {post.id}: Added media entry to post_data: {media_entry}")
                            elif not s3_url_to_add:
                                logger.warning(f"[Acc: {account_id}][Chan: {channel_id_input}] Post {post.id}: Could not determine S3 URL for media {file_id} ({media_type})")

                            # Добавляем задачу на скачивание/загрузку в S3 (если еще нет для этого file_id)
                            # Передаем file_size в задачу (хотя media_utils.py его сам получит)
                            if file_id not in media_tasks_for_post:
                                media_tasks_for_post[file_id] = {
                                    "account_id": account_id, 
                                    "media_object": media_object, 
                                    "file_id": file_id, 
                                    "s3_filename": s3_filename # <<< Убираем file_size и s3_thumb_filename
                                    # "s3_thumb_filename": s3_thumb_filename if is_placeholder else None, 
                                    # "file_size": file_size
                                }
                                logger.debug(f"[Acc: {account_id}][Chan: {channel_id_input}] Post {post.id}: Added background task for file_id {file_id}")
                                
                        # Добавляем все уникальные задачи для этого поста в общую очередь
                        # Важно: убедитесь, что background_tasks_queue ожидает такой формат словаря!
                        # Возможно, нужно будет скорректировать background_tasks.py/media_utils.py
                        background_tasks_queue.extend(media_tasks_for_post.values())
                        # --- Конец модифицированной обработки медиа ---

                        # --- Расчет trend_score ---
                        raw_engagement_score = views + (reactions * 10) + (comments * 20) + (forwards * 50)
                        subscribers_for_calc = subscribers if subscribers and subscribers > 0 else 1 # Защита от 0
                        log_subs = math.log10(subscribers_for_calc) if subscribers_for_calc > 1 else 1 # Защита от <=1
                        trend_score = raw_engagement_score / log_subs if log_subs != 0 else raw_engagement_score # Защита от деления на 0 (хотя log_subs не будет 0)
                        post_data['trend_score'] = int(trend_score)

                        processed_posts_for_these_channels.append(post_data)
                        processed_in_channel_count += 1
                        if processed_in_channel_count >= posts_per_channel: break
                except Exception as e_iter:
                     logger.error(f"[Acc: {account_id}][Chan: {channel_id_input}] DIAGNOSTIC: Error during client.iter_messages loop: {e_iter}", exc_info=True)
                # ------------------------------------------------------------

                # --- Логирование завершения (без изменений) ---
                logger.info(f"--- Завершена обработка канала {channel_id_input}. Собрано постов: {channel_processed_posts_count} ---")
            except FloodWaitError as e:
                logger.warning(f"[Шаг 2] Flood wait ({e.seconds}s) при iter_messages для {peer_identifier}. Прерываем."); await asyncio.sleep(e.seconds)
            except Exception as e_iter:
                logger.error(f"[Шаг 2] ОШИБКА iter_messages для {peer_identifier}: {e_iter}", exc_info=True)
            logger.info(f"--- Завершена обработка канала {channel_id_input}. Собрано постов: {channel_processed_posts_count} ---")
    except Exception as main_err:
        logger.error(f"Критическая ошибка в _process_channels_for_trending перед циклом обработки каналов: {main_err}", exc_info=True)
        return []
    logger.info(f"=== Завершена обработка ВСЕХ каналов для этого вызова, собрано итого: {len(processed_posts_for_these_channels)} постов ===")
    return processed_posts_for_these_channels


async def get_album_messages(wrapper: TelegramClientWrapper, chat, main_message): # <<< Принимаем wrapper
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
                # --- Используем wrapper для get_messages --- 
                # msg = await client.get_messages(chat, ids=offset_id + (i * direction))
                msg = await wrapper.make_high_level_request(wrapper.client.get_messages, chat, ids=offset_id + (i * direction))
                # ---------------------------------------------------------
                # Check if msg is a Message object before accessing grouped_id
                if msg and isinstance(msg, types.Message) and hasattr(msg, 'grouped_id') and msg.grouped_id == album_id:
                    album_messages.append(msg)
                else:
                    # Если сообщение не часть альбома, прекращаем поиск в этом направлении
                    break
            except Exception as e:
                # Лог ошибки остается прежним
                logger.error(f"Ошибка при получении сообщения {offset_id + (i * direction)}: {e}")
                break

    logger.info(f"Найдено {len(album_messages)} сообщений в альбоме {album_id}")
    return album_messages

# --- Основная функция get_trending_posts ---
async def get_trending_posts(
    telegram_pool: TelegramClientPool,
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
                # Логируем результат получения аккаунтов ДО проверки их количества
                logger.info(f"Найдено {len(active_accounts)} активных Telegram аккаунтов для ключа {api_key}.")

            except ImportError:
                 logger.error("Не удалось импортировать get_active_accounts из user_manager. Ротация аккаунтов невозможна.")
                 active_accounts = []
            except Exception as e_acc:
                logger.error(f"Ошибка при получении активных аккаунтов: {e_acc}", exc_info=True)
                active_accounts = []

        # --- НАЧАЛО ОБРАБОТКИ 0, 1 или >1 аккаунтов ---
        if not active_accounts:
            logger.warning("Не найдено активных аккаунтов для обработки каналов. Возвращаем пустой результат.")
            # all_posts останется пустым
        elif len(active_accounts) == 1:
            logger.info("Найден 1 активный аккаунт. Обработка без ротации...")
            account_info = active_accounts[0]
            account_id_single = account_info.get('id')
            if not account_id_single:
                 logger.error("Не удалось получить ID единственного активного аккаунта.")
            else:
                 client_single = telegram_pool.get_client(account_id_single)
                 if not client_single or not isinstance(client_single, TelegramClient):
                      logger.error(f"Клиент не найден/некорректен для единственного аккаунта {account_id_single}. Попытка создания...")
                      # --- ДОБАВЛЕНО: Попытка создать клиент, если его нет --- 
                      try:
                           client_single = await telegram_pool.create_client(account_info)
                           if client_single:
                                telegram_pool.add_client(account_id_single, client_single)
                                logger.info(f"Клиент для аккаунта {account_id_single} успешно создан и добавлен в пул.")
                           else:
                                logger.error(f"Не удалось создать клиент для аккаунта {account_id_single} при обработке одного аккаунта.")
                      except Exception as create_err:
                           logger.error(f"Ошибка при попытке создать клиент для {account_id_single}: {create_err}")
                           client_single = None # Убедимся, что клиент None если создание не удалось
                      # --- КОНЕЦ ДОБАВЛЕНИЯ ---
                 
                 # --- Проверяем client_single еще раз после возможного создания --- 
                 if not client_single:
                      logger.error(f"Пропускаем обработку, так как клиент для {account_id_single} не доступен.")
                 else:
                     # --- Проверка подключения/авторизации для одного аккаунта ---
                     is_ready_single = False
                     try:
                         if not client_single.is_connected():
                             logger.info(f"[Single Acc] Клиент {account_id_single} не подключен. Попытка подключения...")
                             await client_single.connect()
                             if not client_single.is_connected(): logger.error(f"[Single Acc] Не удалось подключить клиент {account_id_single}.")
                         if client_single.is_connected() and not await client_single.is_user_authorized():
                             logger.warning(f"[Single Acc] Клиент {account_id_single} НЕ авторизован.")

                         # Проверяем, готов ли клиент
                         if client_single.is_connected() and await client_single.is_user_authorized():
                             is_ready_single = True
                             logger.info(f"[Single Acc] Клиент {account_id_single} готов к работе.")
                         else:
                             logger.error(f"[Single Acc] Клиент {account_id_single} не готов к работе после проверки.")

                     except (FloodWaitError, AuthKeyError, ConnectionError, Exception) as e_check:
                         logger.error(f"[Single Acc] Ошибка при проверке/подключении клиента {account_id_single}: {e_check}.")
                     # --- Конец проверки ---

                     # Если клиент готов, вызываем обработку
                     if is_ready_single:
                         logger.info(f"[Single Acc] Вызов _process_channels_for_trending для {len(flat_channel_ids_str)} каналов...")
                         # Вызываем напрямую, без task
                         processed_posts = await _process_channels_for_trending(
                             client_single,
                             account_id_single,
                             flat_channel_ids_str, # Все каналы идут одному аккаунту
                             cutoff_date.replace(tzinfo=None),
                             posts_per_channel,
                             min_views, min_reactions, min_comments, min_forwards,
                             background_tasks_to_run, # Передаем список для задач
                             api_key
                         )
                         all_posts.extend(processed_posts)
                         logger.info(f"[Single Acc] Обработка завершена. Найдено постов: {len(processed_posts)}")
        elif len(active_accounts) > 1: # Ротация для >1 аккаунта (старый блок if)
            # Проверяем, передан ли пул
            if not telegram_pool:
                logger.error("Экземпляр telegram_pool не был передан в get_trending_posts, но найдено >1 активных аккаунтов. Ротация невозможна.")
            else:
                num_accounts = len(active_accounts)
                channels_per_account = (len(flat_channel_ids_str) + num_accounts - 1) // num_accounts # Округление вверх
                tasks = []
                start_index = 0
                logger.info(f"Распределяем {len(flat_channel_ids_str)} каналов по {num_accounts} аккаунтам (примерно по {channels_per_account})")

                for i, account_info in enumerate(active_accounts):
                     account_id_rot = None
                     if isinstance(account_info, dict):
                         account_id_rot = account_info.get('id')
                     else:
                         logger.warning(f"Неожиданный формат данных для аккаунта на позиции {i}. Пропуск.")
                         continue

                     if not account_id_rot:
                         logger.warning(f"Не удалось получить ID для аккаунта на позиции {i}. Пропуск.")
                         continue

                     account_client = telegram_pool.get_client(account_id_rot)

                     if not account_client or not isinstance(account_client, TelegramClient):
                         logger.warning(f"Клиент Telegram не найден/некорректен для аккаунта ID: {account_id_rot} в переданном пуле. Пропуск.")
                         continue

                     # --- Проверка подключения/авторизации перед созданием задачи ---
                     is_ready_rot = False
                     try:
                         if not account_client.is_connected():
                             logger.info(f"[Rotation] Клиент {account_id_rot} не подключен. Попытка подключения...")
                             await account_client.connect()
                             if not account_client.is_connected(): logger.error(f"[Rotation] Не удалось подключить клиент {account_id_rot}. Пропуск."); continue
                             logger.info(f"[Rotation] Клиент {account_id_rot} успешно подключен.")
                         if not await account_client.is_user_authorized():
                             logger.warning(f"[Rotation] Клиент {account_id_rot} НЕ авторизован. Пропуск.")
                             continue
                         logger.info(f"[Rotation] Клиент {account_id_rot} готов к работе.")
                         is_ready_rot = True
                     except (FloodWaitError, AuthKeyError, ConnectionError, Exception) as e_check:
                         logger.error(f"[Rotation] Ошибка при проверке/подключении клиента {account_id_rot}: {e_check}. Пропуск.")
                         continue
                     # --- КОНЕЦ ПРОВЕРКИ ---

                     if not is_ready_rot: continue # Пропускаем, если клиент не готов

                     # Определяем каналы для этого аккаунта
                     end_index = min(start_index + channels_per_account, len(flat_channel_ids_str))
                     channels_for_this_account = flat_channel_ids_str[start_index:end_index]
                     start_index = end_index

                     if not channels_for_this_account: continue # Пропускаем, если каналов не осталось

                     logger.info(f"Аккаунт ID: {account_id_rot} будет обрабатывать {len(channels_for_this_account)} каналов: {channels_for_this_account[:3]}...")

                     # Создаем задачу для аккаунта
                     task = asyncio.create_task(
                        _process_channels_for_trending(
                            account_client,
                            account_id_rot,
                            channels_for_this_account,
                            cutoff_date.replace(tzinfo=None),
                            posts_per_channel,
                            min_views, min_reactions, min_comments, min_forwards,
                            background_tasks_to_run,
                            api_key
                        )
                     )
                     tasks.append(task)

                # Ждем завершения всех задач обработки каналов
                if tasks:
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    for i, result in enumerate(results):
                        acc_id_log = f"index_{i}"
                        if i < len(active_accounts):
                             info = active_accounts[i]
                             if isinstance(info, dict): acc_id_log = info.get('id', acc_id_log)

                        if isinstance(result, Exception):
                            logger.error(f"Ошибка при обработке каналов аккаунтом ID: {acc_id_log}: {result}", exc_info=result)
                        elif isinstance(result, list):
                            logger.info(f"Аккаунт ID: {acc_id_log} успешно обработал каналы, найдено постов: {len(result)}")
                            all_posts.extend(result)
                        else:
                             logger.warning(f"Неожиданный результат от аккаунта ID: {acc_id_log}: {type(result)}")
        # --- КОНЕЦ ОБРАБОТКИ 0, 1 или >1 аккаунтов ---

        # Запускаем все собранные фоновые задачи на обработку медиа
        if background_tasks_to_run:
             logger.info(f"Запуск {len(background_tasks_to_run)} фоновых задач на обработку медиа...")
             launched_file_ids = set()
             tasks_launched_count = 0
             for task_data in background_tasks_to_run:
                  file_id = task_data.get("file_id")
                  # Проверяем, что передали account_id
                  if file_id and task_data.get("account_id") and file_id not in launched_file_ids:
                       asyncio.create_task(process_single_media_background(**task_data))
                       launched_file_ids.add(file_id)
                       tasks_launched_count += 1
                  elif file_id and not task_data.get("account_id"):
                       logger.warning(f"Пропуск фоновой задачи для file_id {file_id} - отсутствует account_id.")
                  # Убрали лишний else, который логировал пропуск без file_id

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
    telegram_pool: TelegramClientPool, # <<< ДОБАВЛЕНО
    group_ids: List[Union[int, str]],
    limit_per_channel: int = 100,
    days_back: int = 7,
    min_views: int = 0,
    api_key: Optional[str] = None,
    non_blocking: bool = False, # Этот параметр больше не используется напрямую здесь
    is_degraded: bool = False # Глобальный флаг деградации не используется, получаем для каждого аккаунта
    ) -> List[Dict]:
    """
    Асинхронно получает посты из указанных каналов за заданный период,
    распределяя каналы между доступными активными аккаунтами.
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Запрос постов за период {days_back} дней из {len(group_ids)} каналов. Лимит на канал: {limit_per_channel}. API Key: {'Есть' if api_key else 'Нет'}")

    # Проверки входных данных
    if not telegram_pool: logger.error("Пул клиентов telegram_pool не передан."); return []
    if not api_key: logger.error("API ключ не передан в get_posts_by_period."); return []
    if not group_ids: logger.warning("Список ID групп пуст."); return []

    # Получаем активные аккаунты
    try:
        active_accounts = await telegram_pool.get_active_clients(api_key)
        if not active_accounts:
             logger.warning(f"Не найдено активных Telegram аккаунтов для ключа {api_key}."); return []
        num_accounts = len(active_accounts)
        logger.info(f"Найдено {num_accounts} активных аккаунтов. Распределение {len(group_ids)} групп...")
    except Exception as e_acc:
        logger.error(f"Ошибка при получении активных аккаунтов: {e_acc}", exc_info=True)
        return []
    
    # Вычисляем дату отсечки один раз
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_back)
    cutoff_date_naive = cutoff_date.replace(tzinfo=None) # Для передачи в задачу
    logger.info(f"Вычислена дата отсечки: {cutoff_date.isoformat()} (naive: {cutoff_date_naive.isoformat()})")

    tasks = []
    groups_distributed = 0

    # --- Распределение ID групп и запуск задач --- 
    for i, account_info in enumerate(active_accounts):
        account_id_task = account_info.get('id')
        if not account_id_task: logger.warning(f"Найден аккаунт без ID в списке активных (индекс {i}). Пропуск."); continue
        
        client_task = telegram_pool.get_client(account_id_task)
        if not client_task or not isinstance(client_task, TelegramClient): logger.warning(f"Клиент не найден/некорректен для аккаунта {account_id_task}. Пропуск."); continue

        # --- Проверка подключения/авторизации ПЕРЕД созданием задачи ---
        is_ready = False
        is_degraded_task = False # Инициализация
        try:
            if not client_task.is_connected():
                logger.info(f"[get_posts_by_period] Клиент {account_id_task} не подключен. Попытка подключения...")
                await client_task.connect()
                if not client_task.is_connected(): logger.error(f"[get_posts_by_period] Не удалось подключить клиент {account_id_task}. Пропуск."); continue
                logger.info(f"[get_posts_by_period] Клиент {account_id_task} успешно подключен.")
            if not await client_task.is_user_authorized():
                logger.warning(f"[get_posts_by_period] Клиент {account_id_task} НЕ авторизован. Пропуск.")
                continue
            # Получаем статус деградации ИЗ ПУЛА для этого аккаунта
            is_degraded_task = telegram_pool.degraded_mode_status.get(account_id_task, False)
            logger.info(f"[get_posts_by_period] Клиент {account_id_task} готов к работе (Деградация: {is_degraded_task}).")
            is_ready = True
        except (FloodWaitError, AuthKeyError, ConnectionError, Exception) as e_check:
            logger.error(f"[get_posts_by_period] Ошибка при проверке/подключении клиента {account_id_task}: {e_check}. Пропуск.")
            continue
        # --- Конец проверки --- 
        
        if not is_ready: continue # Пропускаем, если клиент не готов

        # Распределяем ID групп (round-robin)
        groups_for_account = group_ids[i::num_accounts] # Берем каждый N-ый ID, начиная с i
        
        if not groups_for_account:
             logger.info(f"Для аккаунта {account_id_task} не осталось групп для обработки.")
             continue
        
        groups_distributed += len(groups_for_account)
        logger.info(f"Аккаунт {account_id_task} будет обрабатывать {len(groups_for_account)} групп: {groups_for_account[:3]}..." )
        
        # Создаем задачу, передавая нужные группы и параметры
        tasks.append(asyncio.create_task(
            _process_groups_for_period_task(
                client=client_task, 
                account_id=account_id_task, 
                api_key=api_key,
                group_ids_for_account=groups_for_account, 
                limit_per_channel=limit_per_channel,
                cutoff_date=cutoff_date_naive, # Передаем naive дату
                min_views=min_views,
                is_degraded=is_degraded_task # Передаем статус деградации из пула
            )
        ))

    logger.info(f"Всего распределено групп: {groups_distributed}. Запускаем {len(tasks)} задач обработки...")

    # Собираем результаты
    final_posts = []
    if tasks:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for i, result in enumerate(results):
            # Получаем ID для лога, даже если результат - ошибка
            acc_id_log = "unknown_id"
            # Проверяем границы перед доступом к active_accounts[i]
            if i < len(active_accounts):
                account_info = active_accounts[i]
                if isinstance(account_info, dict):
                    acc_id_log = account_info.get('id', f"index_{i}")
            else:
                acc_id_log = f"task_index_{i}" # Если индекс выходит за пределы

            if isinstance(result, Exception):
                logger.error(f"Ошибка при обработке групп аккаунтом {acc_id_log}: {result}", exc_info=result)
            elif isinstance(result, list):
                logger.info(f"Аккаунт {acc_id_log} успешно обработал группы, найдено постов: {len(result)}")
                final_posts.extend(result)
            else:
                logger.warning(f"Неожиданный результат обработки групп от аккаунта {acc_id_log}: {type(result)}")

    # --- Финальная сортировка всех постов по дате --- 
    final_posts.sort(key=lambda p: p.get('date', datetime.min.isoformat()), reverse=True)
    # ----------------------------------------------

    logger.info(f"Завершено получение постов за период. Всего найдено: {len(final_posts)}")
    return final_posts

# --- НОВАЯ Вспомогательная функция для параллельной обработки --- 
async def _process_groups_for_period_task(
    client: TelegramClient,
    account_id: str,
    api_key: Optional[str],
    group_ids_for_account: List[Union[int, str]],
    limit_per_channel: int,
    cutoff_date: datetime,
    min_views: int,
    is_degraded: bool
) -> List[Dict]:
    """Обрабатывает список ID групп, назначенных одному аккаунту."""
    logger = logging.getLogger(__name__)
    all_posts_for_account = []
    
    # Создаем wrapper внутри задачи для этого аккаунта
    wrapper = TelegramClientWrapper(client, account_id, api_key)
    wrapper.set_degraded_mode(is_degraded)
    logger.info(f"[Task Acc: {account_id}] Запуск задачи для обработки {len(group_ids_for_account)} групп.")

    # --- Цикл по группам, назначенным этому аккаунту ---
    for group_id in group_ids_for_account:
        logger.info(f"[Task Acc: {account_id}] [Chan: {group_id}] Начало обработки группы.")
        channel_posts = []
        channel_entity = None
        processed_grouped_ids = set() # Отслеживаем обработанные альбомы для ЭТОЙ группы
        try:
            # --- Получение Entity (аналогично старой process_single_channel) ---
            entity_id_to_find = None
            if isinstance(group_id, str) and group_id.startswith('@'): entity_id_to_find = group_id
            elif isinstance(group_id, str) and ('t.me/' in group_id or 'telegram.me/' in group_id): entity_id_to_find = group_id
            elif isinstance(group_id, (int, str)):
                try:
                    numeric_id = int(group_id)
                    entity_id_to_find = int(f"-100{numeric_id}") if numeric_id > 0 else numeric_id
                except ValueError: logger.warning(f"[Task Acc: {account_id}] [Chan: {group_id}] Некорректный ID. Пропуск группы."); continue
            else: logger.warning(f"[Task Acc: {account_id}] [Chan: {group_id}] Неподдерживаемый тип ID. Пропуск группы."); continue
            
            if entity_id_to_find is None: logger.warning(f"[Task Acc: {account_id}] [Chan: {group_id}] Не удалось определить entity_id_to_find. Пропуск группы."); continue

            channel_entity = await wrapper.make_high_level_request(wrapper.client.get_entity, entity_id_to_find)
            if not channel_entity: logger.warning(f"[Task Acc: {account_id}] [Chan: {group_id}] get_entity вернул None. Пропуск группы."); continue
            channel_title = getattr(channel_entity, 'title', 'Unknown Title')
            logger.info(f"[Task Acc: {account_id}] [Chan: {group_id}] Успешно получена entity: '{channel_title}' (ID: {channel_entity.id})")
            # -------------------------------------------------------

            # --- Обновление статистики перед iter_messages --- 
            if api_key: # Обновляем статистику один раз на группу
                try:
                    from redis_utils import update_account_usage_redis; await update_account_usage_redis(api_key, account_id, "telegram")
                    logger.info(f"[Task Acc: {account_id}] [Chan: {group_id}] Статистика обновлена (Redis) перед iter_messages.")
                except ImportError: 
                    from user_manager import update_account_usage; await update_account_usage(api_key, account_id, "telegram")
                    logger.info(f"[Task Acc: {account_id}] [Chan: {group_id}] Статистика обновлена (user_manager) перед iter_messages.")
                except Exception as stats_err: logger.error(f"[Task Acc: {account_id}] [Chan: {group_id}] Ошибка обновления статистики: {stats_err}")
            # -------------------------------------------------
            
            message_count_in_loop = 0
            # Используем channel_entity вместо group_id в iter_messages для большей надежности
            async for message in client.iter_messages(channel_entity, limit=limit_per_channel): 
                message_count_in_loop += 1
                # --- Логика обработки message (из старой process_single_channel) ---
                if not message or not message.date: continue # Добавим проверку на None
                
                msg_date_naive = message.date.replace(tzinfo=None)
                if msg_date_naive <= cutoff_date: # Сравниваем с cutoff_date без tzinfo
                    logger.debug(f"[Task Acc: {account_id}] [Chan: {group_id}] Сообщение {message.id} ({msg_date_naive}) слишком старое ({cutoff_date}). Прерываем.")
                    break # Прерываем, так как сообщения идут от новых к старым
                
                is_album = message.grouped_id is not None
                if is_album and message.grouped_id in processed_grouped_ids: continue
                
                views = getattr(message, 'views', 0) # Безопасно получаем просмотры
                if views is not None and views >= min_views:
                    post_text = ""
                    if is_album:
                        processed_grouped_ids.add(message.grouped_id)
                        try:
                            # Передаем wrapper в get_album_messages
                            album_messages = await get_album_messages(wrapper, channel_entity, message)
                            if album_messages:
                                album_messages.sort(key=lambda m: m.id)
                                for album_msg in album_messages:
                                    if album_msg.text: post_text = album_msg.text; break
                        except Exception as album_err: logger.error(f"[Task Acc: {account_id}] [Chan: {group_id}] Ошибка get_album_messages: {album_err}")
                        if not post_text: post_text = message.text or ""
                    else:
                        post_text = message.text or ""

                    channel_id_str = str(channel_entity.id).replace('-100', '')
                    post_data = {
                        "id": message.id,
                        "channel_id": channel_id_str,
                        "channel_title": channel_title,
                        "channel_username": getattr(channel_entity, 'username', None),
                        "text": post_text,
                        "views": message.views,
                        "reactions": sum(r.count for r in message.reactions.results) if message.reactions and message.reactions.results else 0,
                        "comments": message.replies.replies if message.replies else 0,
                        "forwards": message.forwards or 0,
                        "date": message.date.isoformat(),
                        "url": f"https://t.me/{getattr(channel_entity, 'username', f'c/{channel_id_str}')}/{message.id}",
                        "media": [] # Медиа здесь не обрабатываем
                    }
                    channel_posts.append(post_data)
                # --- Конец логики обработки message ---
                
            logger.info(f"[Task Acc: {account_id}] [Chan: {group_id}] Завершен цикл iter_messages. Найдено: {len(channel_posts)}")

        except FloodWaitError as e: logger.error(f"[Task Acc: {account_id}] [Chan: {group_id}] FloodWaitError: {e.seconds} сек. Пропуск группы."); channel_posts = []
        except (ChannelInvalidError, ChannelPrivateError, ChatForbiddenError, UsernameNotOccupiedError, UsernameInvalidError) as e_perm: logger.warning(f"[Task Acc: {account_id}] [Chan: {group_id}] Ошибка доступа/не найдено: {e_perm}. Пропуск группы."); channel_posts = []
        except Exception as e:
            logger.error(f"[Task Acc: {account_id}] [Chan: {group_id}] Непредвиденная Ошибка: {e.__class__.__name__}: {e}", exc_info=True)
            channel_posts = []
        finally:
            all_posts_for_account.extend(channel_posts)
            logger.info(f"[Task Acc: {account_id}] [Chan: {group_id}] Завершение обработки группы. Добавлено: {len(channel_posts)}.")
            
    logger.info(f"[Task Acc: {account_id}] ЗАВЕРШЕНА ЗАДАЧА. Возвращаем {len(all_posts_for_account)} постов.")
    return all_posts_for_account
# --- Конец новой вспомогательной функции ---

