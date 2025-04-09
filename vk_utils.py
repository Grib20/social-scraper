import vk_api
import asyncio
import logging
import time
from vk_api.exceptions import ApiError
from dotenv import load_dotenv
import os
import aiohttp
from typing import List, Dict, Optional, Union, Any, Tuple
from datetime import datetime, timedelta
from media_utils import get_media_info as telegram_get_media_info
from user_manager import get_active_accounts, update_account_usage
import math
import redis
import json
import re
import random

load_dotenv()
logger = logging.getLogger(__name__)

# Константы для ротации аккаунтов
REQUEST_SEMAPHORE = asyncio.Semaphore(2)  # Максимум 2 одновременных запроса
REQUEST_DELAY = 0.1  # 100мс между запросами (10 запросов в секунду)
GROUP_DELAY = 1.0  # 1 секунда между запросами к разным группам
DEGRADED_MODE_DELAY = 0.5  # Задержка в режиме пониженной производительности (500мс)

# TTL для кэша количества участников (1 час)
GROUP_MEMBERS_CACHE_TTL = 3600

# Инициализация Redis
try:
    REDIS_URL = os.getenv("REDIS_URL")
    if REDIS_URL:
        logger.info(f"Подключение к Redis по URL: {REDIS_URL.split('@')[0]}@...")
        redis_client = redis.from_url(REDIS_URL)
        # Проверка соединения
        if redis_client:
            redis_client.ping()
            logger.info("Успешное подключение к Redis")
        else:
            logger.warning("Не удалось создать клиент Redis")
            redis_client = None
    else:
        logger.warning("REDIS_URL не задан, использую локальный кэш")
        redis_client = None
except Exception as e:
    logger.error(f"Ошибка подключения к Redis: {e}")
    redis_client = None

# Глобальный кэш для количества участников групп (используется как fallback)
GROUP_MEMBERS_CACHE = {}

def validate_proxy(proxy: Optional[str]) -> bool:
    """
    Валидирует строку прокси и возвращает статус валидации.
    
    Args:
        proxy: Строка с прокси в формате scheme://host:port или scheme://user:password@host:port
        
    Returns:
        bool: True если прокси валиден, False в противном случае
    """
    if not proxy:
        return False
    
    # Проверяем схему прокси
    valid_schemes = ['http://', 'https://', 'socks4://', 'socks5://']
    
    # Если схема не указана, предполагаем http://
    normalized_proxy = proxy
    has_valid_scheme = any(proxy.startswith(scheme) for scheme in valid_schemes)
    if not has_valid_scheme:
        normalized_proxy = f'http://{proxy}'
    
    try:
        # Базовая проверка на формат
        if '@' in normalized_proxy:
            # С аутентификацией (user:pass@host:port)
            auth_part, host_part = normalized_proxy.split('@', 1)
            scheme = auth_part.split('://', 1)[0] + '://' if '://' in auth_part else 'http://'
            if not '://' in auth_part:
                auth_part = auth_part  # без схемы
            else:
                auth_part = auth_part.split('://', 1)[1]  # удаляем схему
                
            if ':' not in auth_part or ':' not in host_part:
                return False
                
            # Проверяем порт
            host, port = host_part.split(':', 1)
            try:
                port_num = int(port)
                if port_num <= 0 or port_num > 65535:
                    return False
            except ValueError:
                return False
        else:
            # Без аутентификации (host:port)
            if '://' in normalized_proxy:
                scheme, host_port = normalized_proxy.split('://', 1)
                scheme += '://'
            else:
                host_port = normalized_proxy
                scheme = 'http://'
            
            if ':' not in host_port:
                return False
                
            # Проверяем порт
            host, port = host_port.split(':', 1)
            try:
                port_num = int(port)
                if port_num <= 0 or port_num > 65535:
                    return False
            except ValueError:
                return False
        
        return True
    except Exception:
        # В случае ошибки возвращаем False вместо строки
        return False

async def validate_proxy_connection(proxy: Optional[str]) -> Tuple[bool, str]:
    """
    Валидирует строку прокси и проверяет соединение.
    
    Args:
        proxy: Строка с прокси в формате scheme://host:port или scheme://user:password@host:port
        
    Returns:
        Tuple[bool, str]: (валиден ли прокси, сообщение)
    """
    if not proxy:
        return False, "Прокси не указан"
    
    # Проверяем формат прокси
    if not validate_proxy(proxy):
        return False, "Неверный формат прокси"
    
    # Нормализуем прокси URL
    if '://' not in proxy:
        proxy = 'http://' + proxy
    
    # Проверяем соединение
    try:
        import aiohttp
        
        # Для HTTP/HTTPS прокси можно использовать стандартный aiohttp
        if proxy.startswith(('http://', 'https://')):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get("https://api.vk.com/method/users.get", 
                                        params={"v": "5.131"}, 
                                        proxy=proxy, 
                                        timeout=10) as response:
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
                async with session.get("https://api.vk.com/method/users.get", 
                                     params={"v": "5.131"}, 
                                     timeout=10) as response:
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

class VKClient:
    def __init__(self, access_token: str, proxy: Optional[str] = None, account_id: Optional[str] = None, api_key: Optional[str] = None):
        self.access_token = access_token
        self.proxy = proxy
        self.account_id = account_id
        self.api_key = api_key
        self.session = None
        self.base_url = "https://api.vk.com/method"
        self.version = "5.131"
        self.last_request_time = 0
        self.last_group_request_time = 0
        self.requests_count = 0
        self.degraded_mode = False
        self.group_members_cache = {}  # Кэш для хранения количества участников групп

    def set_degraded_mode(self, degraded: bool):
        """Устанавливает режим пониженной производительности."""
        self.degraded_mode = degraded

    async def __aenter__(self):
        try:
            logger.info(f"Инициализация VK клиента с токеном длиной {len(self.access_token) if self.access_token else 0} символов")
            if self.session is None:
                self.session = aiohttp.ClientSession()
                logger.info(f"Создана новая HTTP сессия для VK клиента")
            return self
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            logger.error(f"Ошибка при инициализации VK клиента: {e}")
            logger.error(f"Трассировка: {tb}")
            # Создаем сессию даже в случае ошибки
            if self.session is None:
                self.session = aiohttp.ClientSession()
            return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            if self.session:
                logger.info(f"Закрытие HTTP сессии для VK клиента")
                await self.session.close()
                self.session = None
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            logger.error(f"Ошибка при закрытии HTTP сессии VK клиента: {e}")
            logger.error(f"Трассировка: {tb}")

    async def _make_request(self, method: str, params: Dict) -> Dict:
        """Выполняет запрос к VK API с соблюдением задержек."""
        current_time = time.time()
        
        # Проверяем, что токен не пустой
        if not self.access_token:
            logger.error("Токен VK пуст или равен None")
            return {}
        
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

        # Формируем параметры запроса с токеном
        request_params = params.copy()  # Создаем копию, чтобы не изменять оригинальный словарь
        request_params.update({
            "access_token": self.access_token,
            "v": self.version
        })

        # Гарантируем, что сессия создана
        if self.session is None:
            logger.info("Сессия не была инициализирована, создаём новую сессию")
            self.session = aiohttp.ClientSession()

        async with REQUEST_SEMAPHORE:
            try:
                # Логируем запрос (без токена для безопасности)
                log_params = {k: v for k, v in request_params.items() if k != "access_token"}
                
                # Проверяем валидность прокси
                proxy_valid = validate_proxy(self.proxy) if self.proxy else False
                proxy_info = sanitize_proxy_for_logs(self.proxy) if self.proxy else "без прокси"
                
                if self.proxy:
                    logger.info(f"Отправка запроса к VK API: {method} через прокси {proxy_info} c параметрами {log_params}")
                else:
                    logger.info(f"Отправка запроса к VK API: {method} без прокси c параметрами {log_params}")
                
                # Сначала пробуем с прокси, если он задан и валиден
                if self.proxy and proxy_valid:
                    try:
                        # Нормализуем формат прокси для aiohttp
                        proxy_url = self.proxy
                        if '://' not in proxy_url:
                            proxy_url = 'http://' + proxy_url
                            
                        # Для HTTP/HTTPS прокси можно использовать стандартный aiohttp
                        if proxy_url.startswith(('http://', 'https://')):
                            async with self.session.get(f"{self.base_url}/{method}", params=request_params, proxy=proxy_url) as response:
                                return await self._process_response(response, method, params)
                        else:
                            # Для SOCKS прокси нужна библиотека aiohttp-socks
                            # Проверяем, установлена ли она
                            try:
                                from aiohttp_socks import ProxyConnector
                                
                                # Используем ProxyConnector для SOCKS прокси
                                connector = ProxyConnector.from_url(proxy_url)
                                async with aiohttp.ClientSession(connector=connector) as proxy_session:
                                    async with proxy_session.get(f"{self.base_url}/{method}", params=request_params) as response:
                                        return await self._process_response(response, method, params)
                            except ImportError:
                                # Если библиотека не установлена, логируем предупреждение и продолжаем без прокси
                                logger.warning(f"Библиотека aiohttp-socks не установлена, но требуется для SOCKS прокси. Продолжаем без прокси.")
                                # Пробуем запрос без прокси
                                async with self.session.get(f"{self.base_url}/{method}", params=request_params) as response:
                                    return await self._process_response(response, method, params)
                    except aiohttp.ClientProxyConnectionError as e:
                        logger.error(f"Ошибка подключения через прокси {proxy_info}: {e}")
                        logger.warning(f"Пробуем запрос без прокси после ошибки")
                    except Exception as e:
                        logger.error(f"Ошибка при запросе через прокси {proxy_info}: {e}")
                        logger.warning(f"Пробуем запрос без прокси после ошибки")
                
                # Если прокси не задан, не валиден или произошла ошибка - пробуем без прокси
                async with self.session.get(f"{self.base_url}/{method}", params=request_params) as response:
                    return await self._process_response(response, method, params)
                
            except Exception as e:
                import traceback
                tb = traceback.format_exc()
                logger.error(f"Ошибка при выполнении запроса к VK API: {e}")
                logger.error(f"Трассировка: {tb}")
                return {}

    async def test_connection(self) -> bool:
        """
        Проверяет работоспособность клиента VK путем тестового запроса.    
        Returns:
        bool: True если соединение работает, False в противном случае
        """
        try:
            # Пробуем выполнить простой запрос к API
            result = await self._make_request("users.get", {})
            return "response" in result
        except Exception as e:
            logger.error(f"Ошибка при проверке соединения VK: {e}")
            return False
    
    async def _process_response(self, response, method, params):
        """Обрабатывает ответ от API VK."""
        if response.status != 200:
            logger.error(f"Ошибка при запросе к VK API: статус {response.status}")
            try:
                error_text = await response.text()
                logger.error(f"Текст ошибки: {error_text}")
            except:
                pass
            return {}
        
        try:
            result = await response.json()
        except Exception as e:
            logger.error(f"Ошибка при декодировании JSON ответа: {e}")
            return {}
        
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
        if self.account_id and self.api_key:
            await update_account_usage(self.api_key, self.account_id, "vk")
        
        return result

    async def _make_group_request(self, method: str, params: Dict) -> Dict:
        """Выполняет запрос к группе с дополнительной задержкой."""
        group_delay = GROUP_DELAY * 2 if self.degraded_mode else GROUP_DELAY
        await asyncio.sleep(group_delay)
        return await self._make_request(method, params)

    async def find_groups(self, keywords: List[str], min_members: int = 10000, max_groups: int = 20) -> List[Dict]:
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
                                "group_members": await self._get_group_members_count(group_id),
                                "media": []
                            }
                            
                            # Рассчитываем показатели вовлеченности по формуле из Telegram
                            raw_engagement_score = views + (post_data["likes"] * 10) + (post_data["comments"] * 20) + (post_data["reposts"] * 50)
                            group_members_for_calc = max(post_data["group_members"], 10) # Минимум 10 участников для логарифма
                            post_data["trend_score"] = int(raw_engagement_score / math.log10(group_members_for_calc)) if raw_engagement_score > 0 else 0
                            
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
            from user_manager import get_active_accounts
            # Проверяем, что api_key не None перед передачей в функцию
            if self.api_key is None:
                logger.error("API ключ не установлен")
                return []
            active_accounts = await get_active_accounts(self.api_key, "vk")
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
                # Преобразуем ID группы в число, убираем минус, если есть
                group_id_str = str(group_id).replace('-', '')
                group_id_int = int(group_id_str)
                
                result = await self._make_group_request("wall.get", {
                    "owner_id": -group_id_int,  # Используем отрицательное числовое значение
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
                            "url": f"https://vk.com/wall-{group_id_str}_{post['id']}",
                            "likes": post.get("likes", {}).get("count", 0),
                            "reposts": post.get("reposts", {}).get("count", 0),
                            "comments": post.get("comments", {}).get("count", 0),
                            "group_members": await self._get_group_members_count(group_id_int),
                            "media": []
                        }
                        
                        # Рассчитываем показатели вовлеченности по формуле из Telegram
                        raw_engagement_score = views + (post_data["likes"] * 10) + (post_data["comments"] * 20) + (post_data["reposts"] * 50)
                        group_members_for_calc = max(post_data["group_members"], 10) # Минимум 10 участников для логарифма
                        post_data["trend_score"] = int(raw_engagement_score / math.log10(group_members_for_calc)) if raw_engagement_score > 0 else 0
                        
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

    async def _get_group_members_count(self, group_id: Union[int, str]) -> int:
        """Получает количество участников группы с использованием Redis."""
        # Преобразуем ID группы в число, убираем минус, если есть
        if isinstance(group_id, str):
            group_id_str = group_id.replace('-', '')
            group_id_int = int(group_id_str)
        else:
            group_id_int = abs(group_id)  # Убираем минус, если есть
            group_id_str = str(group_id_int)
        
        # Формируем ключ для Redis
        redis_key = f"vk:group:members:{group_id_int}"
        
        # Проверяем Redis, если доступен
        if redis_client:
            try:
                cached_value = redis_client.get(redis_key)
                if cached_value:
                    members_count = int(cached_value)
                    logger.info(f"Получено количество участников группы {group_id_int} из Redis: {members_count}")
                    return members_count
            except Exception as e:
                logger.error(f"Ошибка при чтении из Redis: {e}")
        
        # Проверяем глобальный кэш в памяти как fallback
        if group_id_int in GROUP_MEMBERS_CACHE:
            logger.info(f"Получено количество участников группы {group_id_int} из глобального кэша: {GROUP_MEMBERS_CACHE[group_id_int]}")
            return GROUP_MEMBERS_CACHE[group_id_int]
            
        # Затем локальный кэш экземпляра
        if group_id_int in self.group_members_cache:
            logger.info(f"Получено количество участников группы {group_id_int} из локального кэша: {self.group_members_cache[group_id_int]}")
            return self.group_members_cache[group_id_int]
            
        try:
            logger.info(f"Запрашиваем количество участников группы {group_id_int}")
            result = await self._make_request("groups.getById", {
                "group_id": group_id_int,
                "fields": "members_count"
            })
            
            if "response" in result and result["response"] and "members_count" in result["response"][0]:
                members_count = result["response"][0]["members_count"]
                logger.info(f"Получено количество участников группы {group_id_int}: {members_count}")
                
                # Сохраняем результат во всех кэшах
                self.group_members_cache[group_id_int] = members_count
                GROUP_MEMBERS_CACHE[group_id_int] = members_count
                
                # Сохраняем в Redis с TTL, если доступен
                if redis_client:
                    try:
                        redis_client.setex(redis_key, GROUP_MEMBERS_CACHE_TTL, members_count)
                        logger.info(f"Сохранено количество участников группы {group_id_int} в Redis с TTL {GROUP_MEMBERS_CACHE_TTL} сек")
                    except Exception as e:
                        logger.error(f"Ошибка при сохранении в Redis: {e}")
                
                return members_count
            else:
                logger.warning(f"Не удалось получить количество участников группы {group_id_int}")
                return 10000  # Возвращаем значение по умолчанию
        except Exception as e:
            logger.error(f"Ошибка при получении количества участников группы {group_id_int}: {e}")
            return 10000  # Возвращаем значение по умолчанию в случае ошибки

async def find_vk_groups(vk, keywords, min_members=10000, max_count=20):
    """
    Поиск групп ВКонтакте по ключевым словам.
    
    Args:
        vk (VKClient): Инициализированный клиент VK
        keywords (list): Список ключевых слов для поиска
        min_members (int): Минимальное количество участников в группе
        max_count (int): Максимальное количество групп для возврата
        
    Returns:
        list: Отсортированный список уникальных групп, отвечающих критериям
    """
    # Проверяем, что клиент VK не None
    if vk is None:
        logger.error("VK клиент не инициализирован")
        return []
    
    # Проверяем, что токен доступа не пустой
    if not vk.access_token:
        logger.error("Токен доступа VK пуст или недействителен")
        return []
    
    # Преобразуем ключевые слова в список, если передана строка
    if isinstance(keywords, str):
        keywords = [keywords]
    
    logger.info(f"Начинаем поиск групп по ключевым словам: {keywords}")
    
    all_groups = []
    
    for keyword in keywords:
        try:
            logger.info(f"Поиск групп по ключевому слову: '{keyword}'")
            
            # Выполняем поиск групп через метод _make_request с сортировкой (sort=6)
            response = await vk._make_request("groups.search", {
                "q": keyword,
                "type": "group",
                "count": 100,
                "sort": 6,  # Сортировка как в JS-версии
                "fields": "members_count"
            })
            
            if not response or "response" not in response or not response["response"].get("items"):
                logger.warning(f"Не найдено групп по ключевому слову: '{keyword}'")
                continue
                
            items = response["response"]["items"]
            logger.info(f"Найдено {len(items)} групп по ключевому слову '{keyword}'")
            
            # Преобразуем группы в формат как в JS-версии
            groups = []
            for group in items:
                groups.append({
                    "id": f"-{group['id']}",
                    "name": group.get("name", ""),
                    "members": group.get("members_count", 0),
                    "is_closed": group.get("is_closed", 1)
                })
            
            all_groups.extend(groups)
            # Добавляем задержку между запросами как в JS-версии
            await asyncio.sleep(0.5)
            
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            logger.error(f"Ошибка при поиске групп по ключевому слову '{keyword}': {str(e)}")
            logger.error(f"Трассировка: {tb}")
    
    # Делаем группы уникальными по ID, как в JS-версии
    unique_groups = {}
    for group in all_groups:
        if group["id"] not in unique_groups:
            unique_groups[group["id"]] = group
    
    # Фильтруем закрытые группы и по минимальному количеству участников
    filtered_groups = [
        group for group in unique_groups.values() 
        if group["is_closed"] == 0 and group["members"] >= min_members
    ]
    
    # Сортируем по количеству участников
    sorted_groups = sorted(filtered_groups, key=lambda x: x["members"], reverse=True)
    
    logger.info(f"После фильтрации и сортировки осталось {len(sorted_groups)} групп")
    
    # Возвращаем только max_count групп
    result = sorted_groups[:max_count]
    logger.info(f"Возвращаются первые {len(result)} групп")
    
    # Добавим логирование для отладки
    for i, group in enumerate(result):
        logger.info(f"Группа {i+1}: ID={group['id']}, members={group['members']}")
    
    return result

async def get_vk_posts_in_groups(vk, group_ids, keywords=None, count=10, min_views=1000, days_back=7, max_posts_per_group=300):
    """
    Получение постов из групп ВКонтакте.
    
    Args:
        vk (VKClient): Инициализированный клиент VK
        group_ids (list): Список ID групп
        keywords (list, optional): Список ключевых слов для фильтрации постов
        count (int): Общее количество постов для возврата
        min_views (int): Минимальное количество просмотров поста
        days_back (int): Количество дней назад для поиска
        max_posts_per_group (int): Максимальное количество постов из одной группы
        
    Returns:
        list: Отсортированный список постов, отвечающих критериям
    """
    # Проверки
    if vk is None or not vk.access_token or not group_ids:
        logger.error("VK клиент не инициализирован, токен пуст или не указаны ID групп")
        return []
    
    # Приводим входные параметры к нужному типу
    if isinstance(group_ids, str):
        group_ids = [group_ids]
    
    if keywords and isinstance(keywords, str):
        keywords = [keywords]
    
    logger.info(f"Поиск постов в группах {group_ids} за {days_back} дней{' по ключевым словам: ' + ', '.join(keywords) if keywords else ' (тренды)'}")
    
    # Рассчитываем timestamp для фильтрации по дате
    now = int(time.time())
    start_time = now - (days_back * 24 * 60 * 60)
    
    all_posts = []
    
    # Разбиваем на чанки для параллельного выполнения
    chunk_size = 3
    group_chunks = [group_ids[i:i+chunk_size] for i in range(0, len(group_ids), chunk_size)]
    
    for chunk in group_chunks:
        tasks = []
        
        # Создаем задачи для каждой группы в чанке
        for group_id in chunk:
            async def get_posts_from_group(gid):
                try:
                    # Приводим ID группы к нужному формату
                    gid_str = str(gid).replace('-', '')
                    owner_id = -int(gid_str)
                    
                    offset = 0
                    group_posts = []
                    
                    # Получаем посты порциями
                    while offset < max_posts_per_group:
                        response = await vk._make_request("wall.get", {
                            "owner_id": owner_id,
                            "count": 100,
                            "offset": offset,
                            "extended": 1
                        })
                        
                        if not response or "response" not in response:
                            logger.error(f"Ошибка получения постов из группы {gid}")
                            break
                        
                        posts = response["response"]["items"]
                        logger.info(f"Получено {len(posts)} постов из группы {gid}, offset: {offset}")
                        
                        if not posts:
                            break
                        
                        # Фильтруем посты
                        for post in posts:
                            if post["date"] < start_time or post["date"] > now:
                                continue
                                
                            views_count = post.get("views", {}).get("count", 0)
                            if views_count < min_views:
                                continue
                                
                            if keywords and not any(kw.lower() in post.get("text", "").lower() for kw in keywords):
                                continue
                                
                            group_posts.append(post)
                        
                        offset += 100
                        if len(posts) < 100:
                            break
                    
                    return group_posts
                except Exception as e:
                    logger.error(f"Ошибка при получении постов из группы {gid}: {str(e)}")
                    return []
            
            tasks.append(get_posts_from_group(group_id))
        
        # Запускаем задачи параллельно
        results = await asyncio.gather(*tasks)
        for posts in results:
            all_posts.extend(posts)
        
        # Делаем паузу между чанками
        await asyncio.sleep(0.333)
    
    # Делаем посты уникальными
    unique_posts = []
    seen_keys = set()
    
    for post in all_posts:
        post_key = f"{post['owner_id']}_{post['id']}"
        if post_key not in seen_keys:
            seen_keys.add(post_key)
            unique_posts.append(post)
    
    # Сортируем посты
    sorted_posts = []
    if keywords and len(keywords) > 0:
        # По просмотрам при поиске по ключевым словам
        sorted_posts = sorted(
            unique_posts, 
            key=lambda p: p.get("views", {}).get("count", 0), 
            reverse=True
        )[:count]
    else:
        # По "тренду" для обычного поиска
        for post in unique_posts:
            # Получаем количество участников группы
            gid_str = str(post.get("owner_id", "0")).replace('-', '')
            group_id = int(gid_str)
            
            # Проверяем Redis и кэши
            redis_key = f"vk:group:members:{group_id}"
            group_members = None
            
            # Проверяем Redis, если доступен
            if redis_client:
                try:
                    cached_value = redis_client.get(redis_key)
                    if cached_value:
                        group_members = int(cached_value)
                        logger.info(f"Использовано количество участников группы {group_id} из Redis: {group_members}")
                except Exception as e:
                    logger.error(f"Ошибка при чтении из Redis: {e}")
            
            # Проверяем глобальный кэш если Redis не сработал
            if group_members is None and group_id in GROUP_MEMBERS_CACHE:
                group_members = GROUP_MEMBERS_CACHE[group_id]
                logger.info(f"Использовано количество участников группы {group_id} из глобального кэша: {group_members}")
            
            # Запрашиваем через API если нет в кэшах
            if group_members is None:
                try:
                    group_members = await vk._get_group_members_count(group_id)
                except Exception as e:
                    logger.error(f"Ошибка при получении количества участников группы {group_id}: {e}")
                    group_members = 10000  # Значение по умолчанию
            
            # Рассчитываем показатели вовлеченности по формуле из Telegram
            raw_engagement_score = (
                post.get("views", {}).get("count", 0) + 
                (post.get("likes", {}).get("count", 0) * 10) + 
                (post.get("comments", {}).get("count", 0) * 20) + 
                (post.get("reposts", {}).get("count", 0) * 50)
            )
            group_members_for_calc = max(group_members, 10)  # Минимум 10 участников для логарифма
            post["trend_score"] = int(raw_engagement_score / math.log10(group_members_for_calc)) if raw_engagement_score > 0 else 0
        sorted_posts = sorted(
            unique_posts, 
            key=lambda p: p.get("trend_score", 0), 
            reverse=True
        )[:count]
    
    # Преобразуем в нужный формат
    result = []
    for post in sorted_posts:
        # Извлекаем медиа вложения
        media_links = []
        
        if "attachments" in post:
            for attachment in post["attachments"]:
                if attachment["type"] == "photo":
                    sizes = attachment["photo"]["sizes"]
                    largest = max(sizes, key=lambda s: s.get("width", 0) * s.get("height", 0))
                    media_links.append(largest["url"])
                elif attachment["type"] == "video":
                    media_links.append(f"https://vk.com/video{post['owner_id']}_{attachment['video']['id']}")
                elif attachment["type"] == "doc" and "url" in attachment["doc"]:
                    media_links.append(attachment["doc"]["url"])
        
        # Формируем пост
        formatted_post = {
            "text": post.get("text", ""),
            "likes": post.get("likes", {}).get("count", 0),
            "reposts": post.get("reposts", {}).get("count", 0),
            "comments": post.get("comments", {}).get("count", 0),
            "views": post.get("views", {}).get("count", 0),
            "date": datetime.fromtimestamp(post["date"]).isoformat(),
            "post_id": post["id"],
            "owner_id": post["owner_id"],
            "url": f"https://vk.com/wall{post['owner_id']}_{post['id']}",
            "trend_score": post.get("trend_score")
        }
        
        if media_links:
            formatted_post["media"] = media_links
        
        result.append(formatted_post)
    
    logger.info(f"Найдено {len(result)} постов, соответствующих критериям")
    return result

async def get_vk_posts(vk, group_keywords, search_keywords=None, count=10, min_views=1000, days_back=7, max_groups=10, max_posts_per_group=300):
    """Получение постов из групп по ключевым словам.
    
    Args:
        vk: Клиент VK API
        group_keywords: Ключевые слова для поиска групп
        search_keywords: Ключевые слова для фильтрации постов
        count: Максимальное количество постов для возврата
        min_views: Минимальное количество просмотров
        days_back: Количество дней назад для поиска постов
        max_groups: Максимальное количество групп для поиска
        max_posts_per_group: Максимальное количество постов от одной группы
    
    Returns:
        List[Dict]: Список найденных постов
    """
    groups = await find_vk_groups(vk, group_keywords, min_members=1000, max_count=max_groups)
    if not groups:
        logger.warning("Не найдены группы по заданным ключевым словам")
        return []
        
    group_ids = [g['id'] for g in groups]
    return await get_vk_posts_in_groups(vk, group_ids, search_keywords, count, min_views, days_back, max_posts_per_group)

# Функция для обработки вложений VK
async def get_media_info(attachment):
    """
    Обрабатывает вложение VK и возвращает информацию о медиа.
    
    Args:
        attachment (dict): Вложение VK
        
    Returns:
        dict: Информация о медиа или None, если не удалось обработать
    """
    try:
        attachment_type = attachment.get("type")
        if not attachment_type:
            return None
            
        result = {
            "type": attachment_type,
            "url": None,
            "width": None,
            "height": None
        }
        
        if attachment_type == "photo":
            photo = attachment["photo"]
            sizes = photo.get("sizes", [])
            if sizes:
                # Находим максимальный размер
                largest = max(sizes, key=lambda s: s.get("width", 0) * s.get("height", 0))
                result["url"] = largest.get("url")
                result["width"] = largest.get("width")
                result["height"] = largest.get("height")
                
        elif attachment_type == "video":
            video = attachment["video"]
            result["url"] = f"https://vk.com/video{video.get('owner_id')}_{video.get('id')}"
            result["width"] = video.get("width")
            result["height"] = video.get("height")
            if "image" in video and isinstance(video["image"], list) and video["image"]:
                # Находим максимальную картинку превью
                largest_image = max(video["image"], key=lambda i: i.get("width", 0) * i.get("height", 0))
                result["preview_url"] = largest_image.get("url")
                
        elif attachment_type == "doc":
            doc = attachment["doc"]
            result["url"] = doc.get("url")
            result["title"] = doc.get("title")
            result["size"] = doc.get("size")
            
        elif attachment_type == "link":
            link = attachment["link"]
            result["url"] = link.get("url")
            result["title"] = link.get("title")
            if "photo" in link:
                sizes = link["photo"].get("sizes", [])
                if sizes:
                    largest = max(sizes, key=lambda s: s.get("width", 0) * s.get("height", 0))
                    result["preview_url"] = largest.get("url")
        
        # Если не удалось получить URL, возвращаем None
        if not result["url"]:
            return None
            
        return result
    except Exception as e:
        logger.error(f"Ошибка при обработке вложения VK: {e}")
        return None

async def find_vk_groups_parallel(vk, keywords, min_members=10000, max_count=20, api_key=None):
    """
    Параллельный поиск групп ВКонтакте с использованием нескольких аккаунтов.
    
    Args:
        vk (VKClient): Основной клиент VK
        keywords (list): Список ключевых слов для поиска
        min_members (int): Минимальное количество участников в группе
        max_count (int): Максимальное количество групп для возврата
        api_key (str): API ключ пользователя для доступа к пулу аккаунтов
        
    Returns:
        list: Отсортированный список уникальных групп, отвечающих критериям
    """
    # Преобразуем ключевые слова в список, если передана строка
    if isinstance(keywords, str):
        keywords = [keywords]
    
    logger.info(f"Начинаем параллельный поиск групп по ключевым словам: {keywords}")
    
    # Получаем активные аккаунты VK, если передан API ключ
    try:
        # Получаем активные аккаунты VK, если передан API ключ
        if api_key:
            from user_manager import get_active_accounts
            active_accounts = await get_active_accounts(api_key, "vk")
            
            if active_accounts and len(active_accounts) > 1:
                logger.info(f"Распределение запросов между {len(active_accounts)} активными аккаунтами VK")
                
                # Распределяем ключевые слова между аккаунтами
                kw_per_account = len(keywords) // len(active_accounts) + 1
                account_keywords = [keywords[i:i + kw_per_account] for i in range(0, len(keywords), kw_per_account)]
                
                # Создаем клиенты и задачи для параллельного поиска
                tasks = []
                for i, acc in enumerate(active_accounts):
                    if i >= len(account_keywords) or not account_keywords[i]:
                        continue
                        
                    kw_list = account_keywords[i]
                    acc_token = acc.get("access_token")
                    acc_id = acc.get("id")
                    
                    if acc_token:
                        # Создаем отдельный клиент для каждого аккаунта
                        acc_client = VKClient(acc_token, None, acc_id)
                        
                        # Создаем задачу для поиска групп
                        task = find_vk_groups(acc_client, kw_list, min_members, max_count)
                        tasks.append(task)
                        logger.info(f"Создана задача для VK аккаунта {acc_id} с ключевыми словами: {kw_list}")
                
                # Запускаем все задачи параллельно
                if tasks:
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    # Объединяем результаты
                    all_groups = []
                    for result in results:
                        if isinstance(result, Exception):
                            logger.error(f"Ошибка при поиске групп: {result}")
                        elif isinstance(result, list):
                            all_groups.extend(result)
                    
                    # Делаем группы уникальными по ID
                    unique_groups = {}
                    for group in all_groups:
                        group_id = group.get("id")
                        if group_id and group_id not in unique_groups:
                            unique_groups[group_id] = group
                    
                    # Сортируем по количеству участников
                    sorted_groups = sorted(unique_groups.values(), key=lambda x: x.get("members", 0), reverse=True)
                    
                    # Возвращаем только max_count групп
                    result = sorted_groups[:max_count]
                    logger.info(f"Параллельный поиск групп: найдено {len(result)} групп")
                    return result
            else:
                logger.info(f"Использование одного аккаунта для поиска: найдено {len(active_accounts) if active_accounts else 0} аккаунтов")
                return await find_vk_groups(vk, keywords, min_members, max_count)
    
    
    # Если не смогли использовать параллельный поиск, 
    # используем стандартный метод с одним клиентом
        logger.info("Использование стандартного метода поиска с одним аккаунтом")
        return await find_vk_groups(vk, keywords, min_members, max_count)
    except Exception as e:
        logger.error(f"Ошибка при параллельном поиске групп: {e}")
        import traceback
        logger.error(traceback.format_exc())
        # В случае ошибки параллельного поиска, откатываемся к стандартному
        logger.warning("Откат к стандартному методу поиска из-за ошибки в параллельном")
        return await find_vk_groups(vk, keywords, min_members, max_count)

async def find_groups_by_keywords(vk, keywords, min_members=10000, max_count=20, api_key=None):
    """
    Обертка над find_vk_groups_parallel для совместимости с вызовом в app.py
    
    Args:
        vk (VKClient): Клиент VK
        keywords (list): Список ключевых слов для поиска
        min_members (int): Минимальное количество участников в группе
        max_count (int): Максимальное количество групп для возврата
        api_key (str): API ключ пользователя для доступа к пулу аккаунтов
        
    Returns:
        list: Отсортированный список уникальных групп, отвечающих критериям
    """
    logger.info(f"Вызов find_groups_by_keywords для поиска групп ВК по ключевым словам: {keywords}")
    result = await find_vk_groups_parallel(vk, keywords, min_members, max_count, api_key)
    # Если результат bool или None, вернуть пустой список
    if isinstance(result, bool) or result is None:
        return []
    return result