import asyncio
import os
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
from typing import List, Dict, Optional, Callable, Any, Union
from user_manager import get_active_accounts, update_account_usage
from telethon.tl.functions import TLRequest

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Константы для ротации аккаунтов
REQUEST_SEMAPHORE = asyncio.Semaphore(2)  # Максимум 2 одновременных запроса
REQUEST_DELAY = 0.1  # 100мс между запросами (10 запросов в секунду)
GROUP_DELAY = 1.0  # 1 секунда между запросами к разным группам
channel_members_cache = {}
# Константа для режима пониженной производительности
DEGRADED_MODE_DELAY = 0.5  # Увеличенная задержка между запросами

class TelegramClientWrapper:
    def __init__(self, client: TelegramClient, account_id: str, api_key: Optional[str] = None):
        self.client = client
        self.account_id = account_id
        self.api_key = api_key
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
                update_account_usage(self.api_key, self.account_id, "telegram")
            except Exception as e:
                logger.error(f"Ошибка при обновлении статистики использования: {e}")
        
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
                update_account_usage(self.api_key, self.account_id, "telegram")
            except Exception as e:
                logger.error(f"Ошибка при обновлении статистики использования: {e}")
        
        actual_method = getattr(self.client, method.__name__)
        return await actual_method(*args, **kwargs)

async def start_client(client: TelegramClient) -> None:
    """Запускает клиент Telegram."""
    try:
        await client.start()
        logger.info(f"Клиент Telegram {client.session.filename} успешно запущен")
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

async def find_channels(client: TelegramClient, keywords: List[str], min_members: int = 100000, max_channels: int = 20) -> List[Dict]:
    """Находит каналы по ключевым словам."""
    channels = []
    wrapper = TelegramClientWrapper(client, client.session.filename)
    
    for keyword in keywords:
        try:
            async with REQUEST_SEMAPHORE:
                # Используем правильный запрос для поиска контактов/каналов
                result = await wrapper._make_request(functions.contacts.SearchRequest(
                    q=keyword,
                    limit=100 # Искать среди 100 первых результатов
                ))
                
                # Обрабатываем найденные чаты
                for chat in result.chats:
                    if len(channels) >= max_channels:
                        break
                        
                    # Ищем только каналы (не мегагруппы)
                    if isinstance(chat, types.Channel) and not chat.megagroup:
                        # Получаем полное инфо для проверки количества участников
                        try:
                            full_chat = await wrapper._make_group_request(GetFullChannelRequest, chat)
                            members_count = full_chat.full_chat.participants_count
                            if members_count >= min_members:
                                channel_id = f'@{chat.username}' if chat.username else str(chat.id)
                                channels.append({
                                    "id": chat.id,
                                    "title": chat.title,
                                    "username": chat.username,
                                    "members_count": members_count,
                                    "description": full_chat.full_chat.about
                                })
                                # Сохраняем в кэше количество участников канала
                                channel_members_cache[channel_id] = members_count
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
    
    # Сортируем найденные каналы по количеству участников (по убыванию)
    sorted_channels = sorted(channels, key=lambda x: x['members_count'], reverse=True)
    
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
                           min_comments: Optional[int] = None, min_forwards: Optional[int] = None) -> List[Dict]:
    """Получает трендовые посты из каналов."""
    now = int(time.time())
    start_time = now - (days_back * 24 * 60 * 60)
    all_posts = []
    
    if isinstance(channel_ids, list) and channel_ids and isinstance(channel_ids[0], list):
        flat_channel_ids = [item for sublist in channel_ids for item in sublist]
    else:
        flat_channel_ids = channel_ids
    
    logger.info(f"Сбор трендовых постов: channel_ids={flat_channel_ids}, days_back={days_back}, "
                f"posts_per_channel={posts_per_channel}, min_views={min_views}, "
                f"min_reactions={min_reactions}, min_comments={min_comments}, min_forwards={min_forwards}")
    
    wrapper = TelegramClientWrapper(client, client.session.filename)
    
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
                
                # 2. Запрашиваем полную информацию о канале
                # Передаем тип запроса functions.channels.GetFullChannelRequest
                full_chat_result = await wrapper._make_group_request(functions.channels.GetFullChannelRequest, input_peer)
                
                # 3. Извлекаем нужную сущность чата (канал)
                chat_entity = None
                for chat_res in full_chat_result.chats:
                    # Сравниваем ID
                    if hasattr(input_peer, 'channel_id') and str(chat_res.id) == str(input_peer.channel_id):
                         chat_entity = chat_res
                         break
                    elif hasattr(input_peer, 'user_id') and str(chat_res.id) == str(input_peer.user_id): # На случай, если передали ID юзера
                         chat_entity = chat_res
                         break
                    elif hasattr(input_peer, 'chat_id') and str(chat_res.id) == str(input_peer.chat_id): # На случай, если передали ID чата
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
                if not subscribers and hasattr(full_chat_result, 'full_chat') and hasattr(full_chat_result.full_chat, 'participants_count'):
                    subscribers = full_chat_result.full_chat.participants_count
                
                # Обеспечим, чтобы подписчиков было хотя бы 10 для логарифма
                subscribers_for_calc = max(subscribers, 10)
                
                # Шаг 1: Получаем и фильтруем посты
                filtered_posts = []
                async for msg in client.iter_messages(chat_entity, limit=100):
                    if msg.date.timestamp() < start_time:
                        continue
                    
                    views = msg.views or 0
                    reactions = len(msg.reactions.results) if msg.reactions else 0
                    comments = msg.replies.replies if msg.replies else 0
                    forwards = msg.forwards or 0
                    
                    if (min_views and views < min_views) or \
                       (min_reactions and reactions < min_reactions) or \
                       (min_comments and comments < min_comments) or \
                       (min_forwards and forwards < min_forwards):
                        continue
                    
                    raw_engagement_score = views + (reactions * 10) + (comments * 20) + (forwards * 50)
                    trend_score = int(raw_engagement_score / math.log10(subscribers_for_calc)) if raw_engagement_score > 0 else 0 
                    
                    channel_id_str = str(chat_entity.id)
                    post = {
                        'text': msg.message or '',
                        'views': views,
                        'reactions': reactions,
                        'comments': comments,
                        'forwards': forwards,
                        'date': msg.date.isoformat(),
                        'post_id': msg.id,
                        'channel_id': channel_id_str,
                        'channel_title': channel_title,
                        'url': f'https://t.me/{channel_username}/{msg.id}' if channel_username else f'https://t.me/c/{abs(chat_entity.id)}/{msg.id}',
                        'media': None,
                        'subscribers': subscribers,
                        'trend_score': trend_score,
                        'raw_engagement_score': raw_engagement_score,
                        '_msg': msg,
                        '_grouped_id': msg.grouped_id if hasattr(msg, 'grouped_id') else None
                    }
                    
                    filtered_posts.append(post)
                
                # Шаг 2: Сортируем и ограничиваем количество постов
                top_posts = sorted(filtered_posts, key=lambda x: x['trend_score'], reverse=True)[:posts_per_channel]
                
                # Шаг 3: Группировка постов по альбомам
                albums = {}
                individual_posts = []
                
                for post in top_posts:
                    grouped_id = post['_grouped_id']
                    if grouped_id:
                        if grouped_id not in albums:
                            albums[grouped_id] = []
                        albums[grouped_id].append(post)
                    else:
                        individual_posts.append(post)
                
                # Шаг 4: Обработка медиа для постов и альбомов
                channel_posts = []
                
                # Обрабатываем индивидуальные посты
                for post in individual_posts:
                    post['media'] = await get_media_info(client, post['_msg'])
                    del post['_msg']
                    del post['_grouped_id']
                    channel_posts.append(post)
                
                # Обрабатываем альбомы
                for album_id, album_posts in albums.items():
                    album_posts.sort(key=lambda x: x['post_id'], reverse=True)
                    main_post = album_posts[0]
                    all_album_msgs = await get_album_messages(client, chat_entity, main_post['_msg'])
                    main_post['media'] = await get_media_info(client, main_post['_msg'], album_messages=all_album_msgs)
                    del main_post['_msg']
                    del main_post['_grouped_id']
                    channel_posts.append(main_post)
                
                all_posts.extend(channel_posts)
                logger.info(f"Найдено {len(channel_posts)} постов в канале {channel_id}")
                
            await asyncio.sleep(1)
            
        except FloodWaitError as e:
            logger.warning(f"Flood wait на {e.seconds} секунд при сборе трендов для {channel_id}")
            await asyncio.sleep(e.seconds)
            continue
        except Exception as e:
            logger.error(f"Ошибка при сборе трендов для {channel_id}: {e}", exc_info=True)
            continue
    
    logger.info(f"Возвращено {len(all_posts)} трендовых постов")
    return sorted(all_posts, key=lambda x: x['trend_score'], reverse=True)

async def get_posts_in_channels(client: TelegramClient, channel_ids: List[int], keywords: Optional[List[str]] = None, count: int = 10, min_views: int = 1000, days_back: int = 3) -> List[Dict]:
    """Получает посты из каналов по ключевым словам."""
    posts = []
    for channel_id in channel_ids:
        try:
            channel = await client.get_entity(channel_id)
            result = await client(GetHistoryRequest(
                peer=channel,
                limit=count // len(channel_ids),
                offset_date=datetime.now() - timedelta(days=days_back),
                offset_id=0,
                max_id=0,
                min_id=0,
                add_offset=0,
                hash=0
            ))
            
            for message in result.messages:
                if message.views >= min_views:
                    if not keywords or any(keyword.lower() in message.message.lower() for keyword in keywords):
                        posts.append({
                            "id": message.id,
                            "channel_id": channel_id,
                            "channel_title": channel.title,
                            "text": message.message,
                            "views": message.views,
                            "date": message.date.isoformat(),
                            "url": f"https://t.me/c/{channel_id}/{message.id}"
                        })
        except Exception as e:
            logger.error(f"Ошибка при получении постов из канала {channel_id}: {e}")
            continue
    
    return sorted(posts, key=lambda x: x["views"], reverse=True)

async def get_posts_by_keywords(client: TelegramClient, group_keywords: List[str], post_keywords: List[str], count: int = 10, min_views: int = 1000, days_back: int = 3) -> List[Dict]:
    """Получает посты из каналов по ключевым словам для групп и постов."""
    posts = []
    for group_keyword in group_keywords:
        try:
            result = await client(SearchGlobalRequest(
                q=group_keyword,
                filter=InputPeerChannel,
                min_date=datetime.now() - timedelta(days=days_back)
            ))
            
            for chat in result.chats:
                if isinstance(chat, Channel):
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

async def get_posts_by_period(client, group_ids: List[int], max_posts: int = 100, days_back: int = 7, min_views: int = 0, api_key: str = None) -> List[Dict]:
    """Получение постов из групп за указанный период."""
    try:
        all_posts = []
        cutoff_date = datetime.now() - timedelta(days=days_back)
        wrapper = TelegramClientWrapper(client, client.session.filename, api_key)
        
        # Получаем активные аккаунты
        active_accounts = get_active_accounts(api_key, "telegram")
        if not active_accounts:
            logger.warning("Нет доступных аккаунтов")
            return []
        
        # Устанавливаем режим пониженной производительности, если необходимо
        if len(active_accounts) == 1 and active_accounts[0].get("degraded_mode", False):
            wrapper.set_degraded_mode(True)
            logger.info("Используется режим пониженной производительности")
        
        # Распределяем группы между аккаунтами
        groups_per_account = len(group_ids) // len(active_accounts) + 1
        account_groups = [group_ids[i:i + groups_per_account] for i in range(0, len(group_ids), groups_per_account)]
        
        # Создаем задачи для каждого аккаунта
        tasks = []
        for account, groups in zip(active_accounts, account_groups):
            task = asyncio.create_task(process_groups(wrapper, groups, max_posts // len(active_accounts), cutoff_date, min_views))
            tasks.append(task)
        
        # Ждем завершения всех задач
        results = await asyncio.gather(*tasks)
        
        # Объединяем результаты
        for result in results:
            all_posts.extend(result)
        
        # Сортируем посты по дате (новые сверху)
        all_posts.sort(key=lambda x: x["date"], reverse=True)
        
        # Ограничиваем количество постов
        return all_posts[:max_posts]
        
    except Exception as e:
        logger.error(f"Ошибка при получении постов: {e}")
        return []

async def process_groups(wrapper: TelegramClientWrapper, group_ids: List[int], max_posts: int, cutoff_date: datetime, min_views: int) -> List[Dict]:
    """Обрабатывает группы для одного аккаунта."""
    posts = []
    for group_id in group_ids:
        try:
            # Получаем информацию о канале
            channel_entity = await wrapper._make_request(wrapper.client.get_entity, group_id)
            if not isinstance(channel_entity, (Channel, User)):
                logger.warning(f"Не удалось получить информацию о канале {group_id}")
                continue
            
            # Получаем посты из канала
            channel_posts = await wrapper._make_request(wrapper.client.get_messages,
                channel_entity,
                limit=max_posts
            )
            
            for post in channel_posts:
                if not post.message:
                    continue
                    
                post_date = post.date
                if post_date < cutoff_date:
                    continue
                    
                views = getattr(post, 'views', 0)
                if views < min_views:
                    continue
                
                post_data = {
                    "id": post.id,
                    "date": post_date.isoformat(),
                    "views": views,
                    "text": post.message,
                    "group_id": group_id,
                    "group_title": channel_entity.title,
                    "group_username": getattr(channel_entity, 'username', None),
                    "url": f"https://t.me/c/{group_id}/{post.id}",
                    "media": []
                }
                
                # Обрабатываем медиафайлы
                if post.media:
                    media_data = await process_media_file(post.media)
                    if media_data:
                        post_data["media"].append(media_data)
                
                posts.append(post_data)
            
        except Exception as e:
            logger.error(f"Ошибка при получении постов из канала {group_id}: {e}")
            continue
    
    return posts

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
            if document.mime_type.startswith('video/'):
                media_type = "video"
            elif document.mime_type.startswith('image/'):
                media_type = "image"
            elif document.mime_type.startswith('audio/'):
                media_type = "audio"
            else:
                media_type = "document"
                
        return {
            "type": media_type,
            "processing": True
        }
    except Exception as e:
        logger.error(f"Ошибка при обработке медиафайла: {e}")
        return None

# Функция для создания Telegram клиента
async def create_telegram_client(session_name, api_id, api_hash, proxy=None):
    """Создает и настраивает клиент Telegram с файловой сессией"""
    # Используем файловую сессию вместо StringSession
    logger.info(f"Создаем клиент с файловой сессией: {session_name}")
    client = TelegramClient(
        session_name,  # Путь к файлу сессии (без .session)
        api_id,
        api_hash
    )
    
    if proxy:
        client.set_proxy(proxy)
        
    return client