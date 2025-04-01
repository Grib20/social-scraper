import vk_api
import asyncio
import logging
import time
from vk_api.exceptions import ApiError
from dotenv import load_dotenv
import os
import aiohttp
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from media_utils import get_media_info
from user_manager import get_active_accounts, update_account_usage

load_dotenv()
logger = logging.getLogger(__name__)

# Константы для ротации аккаунтов
REQUEST_SEMAPHORE = asyncio.Semaphore(2)  # Максимум 2 одновременных запроса
REQUEST_DELAY = 0.1  # 100мс между запросами (10 запросов в секунду)
GROUP_DELAY = 1.0  # 1 секунда между запросами к разным группам
DEGRADED_MODE_DELAY = 0.5  # Задержка в режиме пониженной производительности (500мс)

class VKClient:
    def __init__(self, access_token: str, proxy: Optional[str] = None, account_id: Optional[str] = None):
        self.access_token = access_token
        self.proxy = proxy
        self.account_id = account_id
        self.session = None
        self.base_url = "https://api.vk.com/method"
        self.version = "5.131"
        self.last_request_time = 0
        self.last_group_request_time = 0
        self.requests_count = 0
        self.degraded_mode = False

    def set_degraded_mode(self, degraded: bool):
        """Устанавливает режим пониженной производительности."""
        self.degraded_mode = degraded

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def _make_request(self, method: str, params: Dict) -> Dict:
        """Выполняет запрос к VK API с соблюдением задержек."""
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

        params.update({
            "access_token": self.access_token,
            "v": self.version
        })

        async with REQUEST_SEMAPHORE:
            try:
                async with self.session.get(f"{self.base_url}/{method}", params=params, proxy=self.proxy) as response:
                    if response.status != 200:
                        logger.error(f"Ошибка при запросе к VK API: {response.status}")
                        return {}
                    
                    result = await response.json()
                    
                    # Проверяем ошибки VK API
                    if "error" in result:
                        error = result["error"]
                        logger.error(f"Ошибка VK API: {error.get('error_code')} - {error.get('error_msg')}")
                        
                        # Если токен недействителен или истек
                        if error.get("error_code") in [5, 27]:
                            logger.error("Токен недействителен или истек")
                            return {}
                        
                        # Если превышен лимит запросов
                        if error.get("error_code") == 29:
                            logger.warning("Превышен лимит запросов к VK API")
                            await asyncio.sleep(1)  # Увеличиваем задержку при превышении лимита
                            return await self._make_request(method, params)  # Повторяем запрос
                        
                        return {}
                    
                    self.requests_count += 1
                    if self.account_id:
                        update_account_usage(self.account_id, "vk")
                    
                    return result
            except Exception as e:
                logger.error(f"Ошибка при выполнении запроса к VK API: {e}")
                return {}

    async def _make_group_request(self, method: str, params: Dict) -> Dict:
        """Выполняет запрос к группе с дополнительной задержкой."""
        group_delay = GROUP_DELAY * 2 if self.degraded_mode else GROUP_DELAY
        await asyncio.sleep(group_delay)
        return await self._make_request(method, params)

    async def find_groups(self, keywords: List[str], min_members: int = 100000, max_groups: int = 20) -> List[Dict]:
        """Поиск групп по ключевым словам."""
        groups = []
        for keyword in keywords:
            try:
                result = await self._make_request("groups.search", {
                    "q": keyword,
                    "count": max_groups,
                    "type": "group"
                })
                
                if "response" in result and "items" in result["response"]:
                    for group in result["response"]["items"]:
                        if group["members_count"] >= min_members:
                            groups.append({
                                "id": group["id"],
                                "title": group["name"],
                                "screen_name": group["screen_name"],
                                "members_count": group["members_count"],
                                "description": group.get("description", "")
                            })
            except Exception as e:
                logger.error(f"Ошибка при поиске групп по ключевому слову {keyword}: {e}")
                continue
        
        return groups

    async def get_posts_in_groups(self, group_ids: List[int], keywords: Optional[List[str]] = None, count: int = 10, min_views: int = 1000, days_back: int = 3) -> List[Dict]:
        """Получение постов из групп."""
        posts = []
        cutoff_date = datetime.now() - timedelta(days=days_back)
        
        for group_id in group_ids:
            try:
                result = await self._make_group_request("wall.get", {
                    "owner_id": -group_id,
                    "count": count // len(group_ids),
                    "offset": 0
                })
                
                if "response" in result and "items" in result["response"]:
                    for post in result["response"]["items"]:
                        post_date = datetime.fromtimestamp(post["date"])
                        if post_date < cutoff_date:
                            continue
                            
                        views = post.get("views", {}).get("count", 0)
                        if views < min_views:
                            continue
                            
                        if not keywords or any(keyword.lower() in post["text"].lower() for keyword in keywords):
                            post_data = {
                                "id": post["id"],
                                "date": post_date.isoformat(),
                                "views": views,
                                "text": post["text"],
                                "group_id": group_id,
                                "group_title": post.get("group_title", ""),
                                "url": f"https://vk.com/wall-{group_id}_{post['id']}",
                                "likes": post.get("likes", {}).get("count", 0),
                                "reposts": post.get("reposts", {}).get("count", 0),
                                "comments": post.get("comments", {}).get("count", 0),
                                "trend_score": (views * 2) + post.get("likes", {}).get("count", 0) + (post.get("comments", {}).get("count", 0) * 3),
                                "media": []
                            }
                            
                            if "attachments" in post:
                                for attachment in post["attachments"]:
                                    media_data = await get_media_info(attachment)
                                    if media_data:
                                        post_data["media"].append(media_data)
                            
                            posts.append(post_data)
            except Exception as e:
                logger.error(f"Ошибка при получении постов из группы {group_id}: {e}")
                continue
        
        return sorted(posts, key=lambda x: x["trend_score"], reverse=True)

    async def get_vk_posts(self, group_keywords: List[str], post_keywords: List[str], count: int = 10, min_views: int = 1000, days_back: int = 3) -> List[Dict]:
        """Получение постов из групп по ключевым словам."""
        posts = []
        for group_keyword in group_keywords:
            try:
                groups = await self.find_groups([group_keyword])
                for group in groups:
                    try:
                        group_posts = await self.get_posts_in_groups(
                            [group["id"]],
                            post_keywords,
                            count // len(group_keywords),
                            min_views,
                            days_back
                        )
                        posts.extend(group_posts)
                    except Exception as e:
                        logger.error(f"Ошибка при получении постов из группы {group['id']}: {e}")
                        continue
            except Exception as e:
                logger.error(f"Ошибка при поиске групп по ключевому слову {group_keyword}: {e}")
                continue
        
        return sorted(posts, key=lambda x: x["trend_score"], reverse=True)[:count]

    async def get_posts_by_period(self, group_ids: List[int], max_posts: int = 100, days_back: int = 7, min_views: int = 0) -> List[Dict]:
        """Получение постов из групп за указанный период."""
        try:
            all_posts = []
            cutoff_date = datetime.now() - timedelta(days=days_back)
            
            # Получаем активные аккаунты
            active_accounts = get_active_accounts(self.account_id, "vk")
            if not active_accounts:
                logger.warning("Нет доступных аккаунтов")
                return []
            
            # Устанавливаем режим пониженной производительности, если необходимо
            if len(active_accounts) == 1 and active_accounts[0].get("degraded_mode", False):
                self.set_degraded_mode(True)
                logger.info("Используется режим пониженной производительности")
            
            # Распределяем группы между аккаунтами
            groups_per_account = len(group_ids) // len(active_accounts) + 1
            account_groups = [group_ids[i:i + groups_per_account] for i in range(0, len(group_ids), groups_per_account)]
            
            # Создаем задачи для каждого аккаунта
            tasks = []
            for account, groups in zip(active_accounts, account_groups):
                task = asyncio.create_task(self.process_groups(groups, max_posts // len(active_accounts), cutoff_date, min_views))
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

    async def process_groups(self, group_ids: List[int], max_posts: int, cutoff_date: datetime, min_views: int) -> List[Dict]:
        """Обрабатывает группы для одного аккаунта."""
        posts = []
        for group_id in group_ids:
            try:
                result = await self._make_group_request("wall.get", {
                    "owner_id": -group_id,
                    "count": max_posts,
                    "offset": 0
                })
                
                if "response" in result and "items" in result["response"]:
                    for post in result["response"]["items"]:
                        post_date = datetime.fromtimestamp(post["date"])
                        if post_date < cutoff_date:
                            continue
                            
                        views = post.get("views", {}).get("count", 0)
                        if views < min_views:
                            continue
                        
                        post_data = {
                            "id": post["id"],
                            "date": post_date.isoformat(),
                            "views": views,
                            "text": post["text"],
                            "group_id": group_id,
                            "group_title": post.get("group_title", ""),
                            "url": f"https://vk.com/wall-{group_id}_{post['id']}",
                            "likes": post.get("likes", {}).get("count", 0),
                            "reposts": post.get("reposts", {}).get("count", 0),
                            "comments": post.get("comments", {}).get("count", 0),
                            "trend_score": (views * 2) + post.get("likes", {}).get("count", 0) + (post.get("comments", {}).get("count", 0) * 3),
                            "media": []
                        }
                        
                        if "attachments" in post:
                            for attachment in post["attachments"]:
                                media_data = await get_media_info(attachment)
                                if media_data:
                                    post_data["media"].append(media_data)
                        
                        posts.append(post_data)
            except Exception as e:
                logger.error(f"Ошибка при получении постов из группы {group_id}: {e}")
                continue
        
        return posts

async def find_vk_groups(vk, keywords, min_members=1000, max_groups=10):
    keywords = keywords if isinstance(keywords, list) else [keywords]
    all_groups = []
    for keyword in keywords:
        async with REQUEST_SEMAPHORE:
            try:
                response = vk.api.groups.search(q=keyword, type='group', sort=6, count=100, fields='members_count')
                groups = [
                    {
                        'id': f"-{group['id']}",
                        'name': group['name'],
                        'members': group['members_count'],
                        'is_closed': group['is_closed']
                    }
                    for group in response['items']
                    if 'members_count' in group and group['members_count'] >= min_members and not group['is_closed']
                ]
                all_groups.extend(groups)
            except ApiError as e:
                logger.error(f"Ошибка: {e}")
            await asyncio.sleep(0.5)
    unique_groups = list({group['id']: group for group in all_groups}.values())
    return sorted(unique_groups, key=lambda x: x['members'], reverse=True)[:max_groups]

async def get_vk_posts_in_groups(vk, group_ids, keywords=None, count=10, min_views=1000, days_back=7, max_posts_per_group=300):
    now = int(time.time())
    start_time = now - (days_back * 24 * 60 * 60)
    all_posts = []
    keywords = keywords if keywords is None or isinstance(keywords, list) else [keywords]

    for group_id in group_ids:
        async with REQUEST_SEMAPHORE:
            try:
                response = vk.api.wall.get(owner_id=group_id, count=100, extended=1)
                posts = [
                    {
                        'text': post['text'] or '',
                        'likes': post['likes']['count'],
                        'reposts': post['reposts']['count'],
                        'comments': post['comments']['count'] if 'comments' in post else 0,
                        'views': post['views']['count'] if 'views' in post else post['likes']['count'],
                        'date': time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(post['date'])),
                        'post_id': post['id'],
                        'owner_id': group_id,
                        'url': f"https://vk.com/wall{group_id}_{post['id']}",
                        'trend_score': (post['views']['count'] if 'views' in post else 0) * 2 +
                                       post['likes']['count'] +
                                       (post['comments']['count'] if 'comments' in post else 0) * 3,
                        'media': None  # Пока без медиа
                    }
                    for post in response['items']
                    if post['date'] >= start_time and
                       (post.get('views', {}).get('count', post['likes']['count']) >= min_views) and
                       (not keywords or any(k.lower() in post['text'].lower() for k in keywords))
                ]
                all_posts.extend(posts)
            except ApiError as e:
                logger.error(f"Ошибка для {group_id}: {e}")
            await asyncio.sleep(0.333)
    unique_posts = list({f"{post['owner_id']}_{post['post_id']}": post for post in all_posts}.values())
    return sorted(unique_posts, key=lambda x: x['trend_score'], reverse=True)[:count]

async def get_vk_posts(vk, group_keywords, search_keywords=None, count=10, min_views=1000, days_back=7, max_groups=10, max_posts_per_group=300):
    groups = await find_vk_groups(vk, group_keywords, min_members=1000, max_groups=max_groups)
    group_ids = [g['id'] for g in groups]
    return await get_vk_posts_in_groups(vk, group_ids, search_keywords, count, min_views, days_back, max_posts_per_group)