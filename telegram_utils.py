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

REQUEST_SEMAPHORE = asyncio.Semaphore(3)
channel_members_cache = {}

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
    for keyword in keywords:
        try:
            result = await client(SearchGlobalRequest(
                q=keyword,
                filter=InputPeerChannel,
                min_date=datetime.now() - timedelta(days=30)
            ))
            
            for chat in result.chats:
                if len(channels) >= max_channels:
                    break
                    
                if isinstance(chat, Channel):
                    full_chat = await client(GetFullChannelRequest(chat))
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
    for channel_id in channel_ids:
        try:
            channel = await client.get_entity(channel_id)
            result = await client(GetHistoryRequest(
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