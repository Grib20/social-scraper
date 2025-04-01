import asyncio
import os
from telethon import TelegramClient, functions, types
from telethon.errors import SessionPasswordNeededError, FloodWaitError
import logging
import time
from media_utils import get_media_info
from dotenv import load_dotenv
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.types import InputPeerChannel
from telethon.tl.functions.messages import SearchGlobalRequest
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.types import Channel, User
from datetime import datetime, timedelta
from typing import List, Dict, Optional

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Константы для ротации аккаунтов
REQUEST_SEMAPHORE = asyncio.Semaphore(2)  # Максимум 2 одновременных запроса
REQUEST_DELAY = 0.1  # 100мс между запросами (10 запросов в секунду)
GROUP_DELAY = 1.0  # 1 секунда между запросами к разным группам
channel_members_cache = {}

class TelegramClientWrapper:
    def __init__(self, client: TelegramClient, account_id: str):
        self.client = client
        self.account_id = account_id
        self.last_request_time = 0
        self.last_group_request_time = 0
        self.requests_count = 0
        self.degraded_mode = False

    def set_degraded_mode(self, degraded: bool):
        """Устанавливает режим пониженной производительности."""
        self.degraded_mode = degraded

    async def _make_request(self, *args, **kwargs):
        """Выполняет запрос к Telegram API с соблюдением задержек."""
        current_time = time.time()
        
        # Определяем задержки в зависимости от режима
        request_delay = DEGRADED_MODE_DELAY if self.degraded_mode else REQUEST_DELAY
        group_delay = GROUP_DELAY * 2 if self.degraded_mode else GROUP_DELAY
        
        # Проверяем задержку между запросами
        time_since_last_request = current_time - self.last_request_time
        if time_since_last_request < request_delay:
            await asyncio.sleep(request_delay - time_since_last_request)
            self.last_request_time = time.time()
        else:
            self.last_request_time = current_time
            
        # Проверяем задержку между группами
        time_since_last_group = current_time - self.last_group_request_time
        if time_since_last_group < group_delay:
            await asyncio.sleep(group_delay - time_since_last_group)
            self.last_group_request_time = time.time()
        else:
            self.last_group_request_time = current_time
            
        self.requests_count += 1
        return await self.client(*args, **kwargs)

    async def _make_group_request(self, *args, **kwargs):
        """Выполняет запрос к группе с дополнительной задержкой."""
        group_delay = GROUP_DELAY * 2 if self.degraded_mode else GROUP_DELAY
        await asyncio.sleep(group_delay)
        return await self._make_request(*args, **kwargs)

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
                result = await wrapper._make_request(SearchGlobalRequest(
                    q=keyword,
                    filter=InputPeerChannel,
                    min_date=datetime.now() - timedelta(days=30)
                ))
                
                for chat in result.chats:
                    if len(channels) >= max_channels:
                        break
                        
                    if isinstance(chat, Channel):
                        full_chat = await wrapper._make_group_request(GetFullChannelRequest(chat))
                        if full_chat.full_chat.participants_count >= min_members:
                            channels.append({
                                "id": chat.id,
                                "title": chat.title,
                                "username": chat.username,
                                "members_count": full_chat.full_chat.participants_count,
                                "description": full_chat.full_chat.about
                            })
        except Exception as e:
            logger.error(f"Ошибка при поиске каналов по ключевому слову {keyword}: {e}")
            continue
    
    return channels

async def get_trending_posts(client: TelegramClient, channel_ids: List[int], days_back: int = 7, posts_per_channel: int = 10, min_views: Optional[int] = None) -> List[Dict]:
    """Получает трендовые посты из каналов."""
    posts = []
    wrapper = TelegramClientWrapper(client, client.session.filename)
    
    for channel_id in channel_ids:
        try:
            async with REQUEST_SEMAPHORE:
                channel = await wrapper._make_group_request(client.get_entity(channel_id))
                result = await wrapper._make_request(GetHistoryRequest(
                    peer=channel,
                    limit=posts_per_channel,
                    offset_date=datetime.now() - timedelta(days=days_back),
                    offset_id=0,
                    max_id=0,
                    min_id=0,
                    add_offset=0,
                    hash=0
                ))
                
                for message in result.messages:
                    if message.views >= (min_views or 0):
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

async def get_posts_by_period(client, group_ids: List[int], max_posts: int = 100, days_back: int = 7, min_views: int = 0) -> List[Dict]:
    """Получение постов из групп за указанный период."""
    try:
        all_posts = []
        cutoff_date = datetime.now() - timedelta(days=days_back)
        wrapper = TelegramClientWrapper(client, client.session.filename)
        
        # Получаем активные аккаунты
        active_accounts = get_active_accounts(client.session.filename, "telegram")
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
            channel = await wrapper._make_group_request(wrapper.client.get_entity(group_id))
            if not isinstance(channel, (Channel, User)):
                logger.warning(f"Не удалось получить информацию о канале {group_id}")
                continue
            
            # Получаем посты из канала
            channel_posts = await wrapper._make_request(wrapper.client.get_messages(
                channel,
                limit=max_posts
            ))
            
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
                    "group_title": channel.title,
                    "group_username": getattr(channel, 'username', None),
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