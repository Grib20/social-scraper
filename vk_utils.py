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

load_dotenv()
logger = logging.getLogger(__name__)

REQUEST_SEMAPHORE = asyncio.Semaphore(3)

class VKClient:
    def __init__(self, token: str, proxy: Optional[str] = None):
        self.token = token
        self.proxy = proxy
        self.session = None
        self.base_url = "https://api.vk.com/method"
        self.version = "5.131"

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def _make_request(self, method: str, params: Dict) -> Dict:
        """Выполняет запрос к API VK."""
        if not self.session:
            self.session = aiohttp.ClientSession()

        params.update({
            "access_token": self.token,
            "v": self.version
        })

        proxy = self.proxy if self.proxy else None
        async with self.session.get(
            f"{self.base_url}/{method}",
            params=params,
            proxy=proxy
        ) as response:
            if response.status != 200:
                raise Exception(f"Ошибка API VK: {response.status}")
            return await response.json()

    async def find_groups(self, keywords: List[str], min_members: int = 1000, max_groups: int = 10) -> List[Dict]:
        """Находит группы по ключевым словам."""
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

        return sorted(groups, key=lambda x: x["members_count"], reverse=True)[:max_groups]

    async def get_posts_in_groups(self, group_ids: List[int], keywords: Optional[List[str]] = None, count: int = 10, min_views: int = 1000, days_back: int = 7) -> List[Dict]:
        """Получает посты из групп по ключевым словам."""
        posts = []
        for group_id in group_ids:
            try:
                result = await self._make_request("wall.get", {
                    "owner_id": -group_id,
                    "count": count // len(group_ids),
                    "offset": 0
                })

                if "response" in result and "items" in result["response"]:
                    for post in result["response"]["items"]:
                        post_date = datetime.fromtimestamp(post["date"])
                        if post_date < datetime.now() - timedelta(days=days_back):
                            continue

                        views = post.get("views", {}).get("count", 0)
                        if views < min_views:
                            continue

                        if keywords and not any(k.lower() in post["text"].lower() for k in keywords):
                            continue

                        posts.append({
                            "id": post["id"],
                            "group_id": group_id,
                            "text": post["text"],
                            "views": views,
                            "likes": post.get("likes", {}).get("count", 0),
                            "reposts": post.get("reposts", {}).get("count", 0),
                            "comments": post.get("comments", {}).get("count", 0),
                            "date": post_date.isoformat(),
                            "url": f"https://vk.com/wall-{group_id}_{post['id']}"
                        })
            except Exception as e:
                logger.error(f"Ошибка при получении постов из группы {group_id}: {e}")
                continue

        return sorted(posts, key=lambda x: x["views"], reverse=True)

    async def get_vk_posts(self, group_keywords: List[str], post_keywords: List[str], count: int = 10, min_views: int = 1000, days_back: int = 7, max_groups: int = 10) -> List[Dict]:
        """Получает посты из групп по ключевым словам для групп и постов."""
        groups = await self.find_groups(group_keywords, max_groups=max_groups)
        group_ids = [group["id"] for group in groups]
        return await self.get_posts_in_groups(group_ids, post_keywords, count, min_views, days_back)

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