import asyncio
import logging
import time
import os
import inspect
from typing import Dict, List, Optional, Set, Tuple, Any, Union
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, FloodWaitError, AuthKeyError
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse
import re
import traceback
from redis_utils import get_account_stats_redis
from user_manager import get_active_accounts, update_account_usage

# --- Добавляем константу задержки (если ее нет) ---
# Нужно убедиться, что она не конфликтует с vk_utils
# Можно вынести в общий файл настроек?
TELEGRAM_DEGRADED_MODE_DELAY = 0.5 # Секунды
# -----------------------------------------------

logger = logging.getLogger(__name__)

class ClientPool:
    """Базовый класс для пула клиентов."""
    
    def __init__(self):
        self.clients = {}  # account_id -> client
        self.client_auth_status = {}  # account_id -> bool
        self.platform = None  # Должен быть установлен в подклассах ('vk' или 'telegram')
        
    def get_client(self, account_id: Union[int, str]) -> Optional[Any]:
        """
        Получает клиент из пула по ID аккаунта.

        Args:
            account_id (Union[int, str]): ID аккаунта

        Returns:
            Optional[Any]: Клиент или None, если не найден
        """
        logger.info(f"Получение клиента {self.platform} для аккаунта {account_id}")
        return self.clients.get(account_id)
    def add_client(self, account_id: str, client: Any) -> None:
        """Добавляет клиент в пул."""
        if not account_id:
            logger.error("Попытка добавить клиент с пустым account_id")
            return
            
        if client is None:
            logger.error(f"Попытка добавить None клиент для аккаунта {account_id}")
            return
            
        self.clients[account_id] = client
        self.client_auth_status[account_id] = False
        logger.info(f"Добавлен {self.platform} клиент для аккаунта {account_id}")

    async def select_next_client(self, api_key: str, strategy: str = "round_robin") -> Tuple[Any, str]:
        """
        Выбирает следующего клиента для использования.
        
        Args:
            api_key: API ключ пользователя
            strategy: Стратегия выбора клиента ("round_robin", "least_used", "random")
            
        Returns:
            Tuple[Any, str]: (клиент, account_id) или (None, None) если нет доступных клиентов
        """
        # Получаем активные аккаунты для данного API ключа
        active_accounts = await self.get_active_clients(api_key)
        if not active_accounts:
            logger.error(f"Нет активных аккаунтов {self.platform} для API ключа {api_key}")
            return None, ""

        # Выбираем аккаунт в зависимости от стратегии
        if strategy == "round_robin":
            # Временно оставляем сортировку по ID для round_robin, пока не будем получать last_used из Redis
            sorted_accounts = sorted(active_accounts, key=lambda acc: acc['id'])
        elif strategy == "least_used":
            # Временно возвращаем ошибку, так как usage_counts удален
            logger.error("Стратегия 'least_used' временно не поддерживается из-за удаления usage_counts из пула.")
            # Можно временно переключиться на round_robin
            sorted_accounts = sorted(active_accounts, key=lambda acc: acc['id'])
            # raise NotImplementedError("Стратегия 'least_used' требует доработки для работы с Redis")
        elif strategy == "random":
            # Случайный выбор
            import random
            random.shuffle(active_accounts)
            sorted_accounts = active_accounts
        else:
            # По умолчанию round_robin
            sorted_accounts = sorted(active_accounts, key=lambda acc: acc['id'])

        # Пробуем каждый аккаунт по очереди, пока не найдем работающий
        for account in sorted_accounts:
            account_id = account['id']
            
            client = self.get_client(account_id)
            if not client:
                client = await self.create_client(account)
                if not client:
                    logger.error(f"Не удалось создать {self.platform} клиент для аккаунта {account_id}")
                    continue
                self.add_client(account_id, client)

            # TODO: Позже добавить проверку degraded_mode здесь
            
            logger.info(f"Выбран {self.platform} клиент для аккаунта {account_id}")
            return client, account_id

        logger.error(f"Не найден подходящий {self.platform} аккаунт")
        return None, ""

    async def get_active_clients(self, api_key: str) -> List[Dict]:
        """
        Получает список активных аккаунтов для данного API ключа.
        Должен быть реализован в подклассах.
        """
        raise NotImplementedError("Метод должен быть реализован в подклассе")

    async def create_client(self, account: Dict) -> Any:
        """
        Создает нового клиента для аккаунта.
        Должен быть реализован в подклассах.
        """
        raise NotImplementedError("Метод должен быть реализован в подклассе")


class VKClientPool(ClientPool):
    """Пул клиентов VK."""
    
    def __init__(self):
        super().__init__()
        self.platform = 'vk'
        self.max_retries = 3
        self.retry_delay = 5  # секунды
        self.current_index = 0
        # Убираем usage_counts и last_used из локального состояния, так как используем Redis
        # self.usage_counts: Dict[str, int] = {}
        # self.last_used: Dict[str, datetime] = {}
    
    async def create_client(self, account):
        """Создает нового клиента VK."""
        from vk_utils import VKClient
        token = account.get('token')
        proxy = account.get('proxy')
        api_key = account.get('api_key')
        account_id = account.get('id')
        
        # Проверяем наличие токена
        if not token:
            logger.error(f"Невозможно создать клиент VK для аккаунта {account_id}: токен отсутствует")
            return None
            
        # Проверяем прокси, если он указан
        if proxy:
            try:
                from vk_utils import validate_proxy
                is_valid = validate_proxy(proxy)
                if not is_valid:
                    logger.error(f"Некорректный прокси для аккаунта {account_id}: {proxy}")
                    return None
            except Exception as e:
                logger.error(f"Ошибка при проверке прокси для аккаунта {account_id}: {e}")
                return None
            
        for attempt in range(self.max_retries):
            try:
                client = VKClient(token, proxy, account_id, api_key)
                # Проверяем работоспособность клиента
                test_result = await client.test_connection()  # Сначала получаем результат
                if test_result:  # Потом проверяем его
                    logger.info(f"Клиент VK успешно создан и протестирован для аккаунта {account_id}")
                    return client
                else:
                    logger.warning(f"Клиент VK создан, но тест соединения не пройден для аккаунта {account_id}")
                
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay)
            except Exception as e:
                logger.error(f"Попытка {attempt + 1}/{self.max_retries} создания клиента VK для аккаунта {account_id} не удалась: {e}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay)
                else:
                    logger.error(f"Не удалось создать клиент VK для аккаунта {account_id} после {self.max_retries} попыток")
                    return None
        
        return None
    
    async def get_active_clients(self, api_key):
        """Получает активные клиенты VK на основе активных аккаунтов."""
        from user_manager import get_active_accounts
        active_accounts = await get_active_accounts(api_key, "vk")
        
        # Проверяем, все ли активные аккаунты имеют клиентов
        for account in active_accounts:
            if account['id'] not in self.clients:
                client = await self.create_client(account)
                if client:
                    self.add_client(account['id'], client)
                    logger.info(f"Создан новый клиент VK для аккаунта {account['id']}")
        
        return active_accounts
    
    async def select_next_client(self, api_key: str, strategy: str = "round_robin") -> Tuple[Any, str]:
        """
        Выбирает следующего клиента VK для использования, учитывая degraded_mode и статистику из Redis.
        """
        active_accounts = await self.get_active_clients(api_key)
        if not active_accounts:
            logger.error(f"Нет активных аккаунтов VK для API ключа {api_key}")
            return None, ""
        
        # --- Получаем статистику из Redis для активных аккаунтов --- 
        account_stats = {}
        if active_accounts:
            tasks = [get_account_stats_redis(acc['id'], self.platform) for acc in active_accounts]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for i, result in enumerate(results):
                acc_id = active_accounts[i]['id']
                if isinstance(result, Exception):
                    logger.error(f"Ошибка получения статистики из Redis для аккаунта VK {acc_id}: {result}")
                    account_stats[acc_id] = {'requests_count': 0, 'last_used': datetime.min.replace(tzinfo=timezone.utc)}
                elif result:
                    last_used_dt = datetime.min.replace(tzinfo=timezone.utc)
                    requests_count = 0
                    if isinstance(result, dict): 
                        if result_last_used := result.get('last_used'): 
                            try:
                                # Используем сохраненное значение result_last_used
                                dt_from_redis = datetime.fromisoformat(result_last_used) 
                                if dt_from_redis.tzinfo is None:
                                    last_used_dt = dt_from_redis.replace(tzinfo=timezone.utc)
                                else:
                                    last_used_dt = dt_from_redis.astimezone(timezone.utc)
                            except (ValueError, TypeError):
                                 # Используем сохраненное значение result_last_used и в логе
                                 logger.warning(f"Не удалось преобразовать last_used '{result_last_used}' из Redis для VK {acc_id}")
                        
                        # Get requests_count safely within the type check
                        requests_count = result.get('requests_count', 0)
                    
                    # Assign account_stats only if result was a dict
                        account_stats[acc_id] = {
                            'requests_count': requests_count, 
                            'last_used': last_used_dt
                        }
                    else: 
                        # Handle case where result is truthy but not a dict (shouldn't happen often with current checks)
                         logger.warning(f"Получен не-словарный результат из Redis для VK {acc_id}, тип: {type(result)}. Используются значения по умолчанию.")
                         account_stats[acc_id] = {'requests_count': 0, 'last_used': datetime.min.replace(tzinfo=timezone.utc)}

                else: # Handle case where result is not True (e.g. None) or not a dict
                     account_stats[acc_id] = {'requests_count': 0, 'last_used': datetime.min.replace(tzinfo=timezone.utc)}
        # ---------------------------------------------------------

        # --- Переделываем логику выбора стратегии (с учетом Redis) --- 
        # 1. Фильтруем аккаунты: сначала недеградированные, потом деградированные
        non_degraded_accounts = []
        degraded_accounts = []
        for acc in active_accounts:
            client = self.get_client(acc['id'])
            # Проверяем degraded_mode у самого клиента VK
            # Добавим проверку, что клиент вообще существует
            if client and client.degraded_mode:
                degraded_accounts.append(acc)
            elif client:
                # Добавляем только если клиент существует и не деградирован
                non_degraded_accounts.append(acc)
            # Если клиента нет (маловероятно, но возможно), игнорируем аккаунт
        
        # 2. Применяем стратегию к недеградированным аккаунтам
        if non_degraded_accounts:
            target_list = non_degraded_accounts
            logger.debug(f"Выбор из {len(target_list)} недеградированных аккаунтов VK")
        elif degraded_accounts:
            target_list = degraded_accounts
            logger.warning(f"Нет доступных недеградированных аккаунтов VK, выбираем из {len(target_list)} деградированных.")
        else:
            logger.error("Непредвиденная ситуация: нет доступных клиентов VK (ни деградированных, ни недеградированных). Возможно, клиенты не создались.")
            return None, ""
        
        # --- Применяем сортировку/выбор по стратегии к выбранному списку (с учетом Redis) --- 
        selected_account = None
        default_stats = {'requests_count': 0, 'last_used': datetime.min.replace(tzinfo=timezone.utc)}
        if strategy == "round_robin":
            # Сортируем по времени последнего использования (старые сначала)
            target_list.sort(key=lambda acc: account_stats.get(acc['id'], default_stats)['last_used'])
            if not target_list: return None, "" 
            if self.current_index >= len(target_list):
                self.current_index = 0
            selected_account = target_list[self.current_index]
            self.current_index += 1
        elif strategy == "least_used":
            # Сортируем по количеству запросов (меньше сначала), затем по времени (старые сначала)
            target_list.sort(key=lambda acc: (
                 account_stats.get(acc['id'], default_stats)['requests_count'], 
                 account_stats.get(acc['id'], default_stats)['last_used']
                 ))
            if target_list:
                selected_account = target_list[0] # Выбираем первый после сортировки
        elif strategy == "random":
            import random
            if target_list:
                 selected_account = random.choice(target_list)
        else: # По умолчанию round_robin
            target_list.sort(key=lambda acc: account_stats.get(acc['id'], default_stats)['last_used'])
            if not target_list: return None, "" 
            if self.current_index >= len(target_list):
                self.current_index = 0
            selected_account = target_list[self.current_index]
            self.current_index += 1
            
        if not selected_account:
            logger.error(f"Не удалось выбрать аккаунт VK по стратегии '{strategy}'")
            return None, ""
        # ---------------------------------------------------------------------

        # Получаем ID и клиента для выбранного аккаунта
        account_id = selected_account['id']
        client = self.get_client(account_id)

        # Клиент должен существовать, так как мы фильтровали по нему
        if not client:
             logger.error(f"КРИТИЧЕСКАЯ ОШИБКА: Не найден клиент для выбранного активного аккаунта VK {account_id} ПОСЛЕ ФИЛЬТРАЦИИ!")
             # Это не должно происходить. Если произошло, что-то не так с логикой.
             return None, "" 

        # Задержка для degraded_mode уже встроена в _make_request клиента VK
        logger.info(f"Выбран {self.platform} клиент для аккаунта {account_id} (Деградация: {client.degraded_mode})")
        return client, account_id
    
    async def get_pool_status(self, api_key):
        """
        Получает статус пула клиентов VK, используя данные из Redis.
        
        Args:
            api_key: API ключ пользователя
        
        Returns:
            Dict: Статистика использования клиентов
        """
        from user_manager import get_active_accounts
        
        # Получаем активные аккаунты
        active_accounts = await get_active_accounts(api_key, "vk")
        if not active_accounts:
            return {
                "total_accounts": 0,
                "accounts": []
            }

        # Получаем статистику из Redis для активных аккаунтов
        account_stats_redis = {}
        account_ids = [acc['id'] for acc in active_accounts]
        tasks = [get_account_stats_redis(acc_id, self.platform) for acc_id in account_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        default_stats = {'requests_count': 0, 'last_used': None}
        for i, result in enumerate(results):
            acc_id = account_ids[i]
            if isinstance(result, Exception):
                logger.error(f"Ошибка получения статистики из Redis для VK {acc_id} в get_pool_status: {result}")
                account_stats_redis[acc_id] = default_stats
            elif isinstance(result, dict):
                # Преобразуем last_used в строку ISO, если это datetime
                last_used = result.get('last_used')
                last_used_str = None
                if isinstance(last_used, datetime):
                    last_used_str = last_used.isoformat()
                elif isinstance(last_used, str): # Если уже строка
                    last_used_str = last_used

                account_stats_redis[acc_id] = {
                    'requests_count': result.get('requests_count', 0),
                    'last_used': last_used_str # Используем строку
                }
            else:
                account_stats_redis[acc_id] = default_stats
        
        # Собираем статистику
        stats = {
            "total_accounts": len(active_accounts),
            "accounts": [
                {
                    "id": acc['id'],
                    "requests": account_stats_redis.get(acc['id'], default_stats)['requests_count'],
                    "last_used": account_stats_redis.get(acc['id'], default_stats)['last_used']
                }
                for acc in active_accounts
            ]
        }
        
        return stats

    def get_clients_usage_statistics(self) -> Dict[str, Dict[str, Any]]:
        """
        Получает статистику использования всех клиентов VK из Redis (асинхронно).
        ВНИМАНИЕ: Метод теперь асинхронный!
        
        Returns:
            Dict[str, Dict[str, Any]]: Словарь со статистикой использования всех клиентов
        """
        logger.info("Получение статистики использования всех клиентов VK из Redis")
        
        # Возвращаем пустой словарь, т.к. метод стал асинхронным
        # и его вызов из синхронного контекста (если такой есть) потребует рефакторинга.
        # Правильнее будет сделать этот метод асинхронным.
        # Для совместимости пока возвращаем пустой словарь, 
        # но нужно будет переделать вызовы этого метода на асинхронные.
        logger.warning("Метод get_clients_usage_statistics для VK должен быть асинхронным. Возвращается пустой словарь для совместимости.")
        return {} # Заглушка

    async def get_clients_usage_statistics_async(self) -> Dict[str, Dict[str, Any]]:
        """
        Асинхронно получает статистику использования всех клиентов VK из Redis.
        
        Returns:
            Dict[str, Dict[str, Any]]: Словарь со статистикой использования всех клиентов
        """
        logger.info("Асинхронное получение статистики использования всех клиентов VK из Redis")
        
        usage_stats = {}
        account_ids = list(self.clients.keys()) # Берем ID клиентов, которые есть в пуле
        if not account_ids:
            return {}

        tasks = [get_account_stats_redis(acc_id, self.platform) for acc_id in account_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        default_stats = {'usage_count': 0, 'last_used': None} # usage_count здесь синоним requests_count
        for i, result in enumerate(results):
            account_id = account_ids[i]
            if isinstance(result, Exception):
                logger.error(f"Ошибка получения статистики из Redis для VK {account_id} в get_clients_usage_statistics_async: {result}")
                usage_stats[account_id] = default_stats
            elif isinstance(result, dict):
                 # Преобразуем last_used в строку ISO, если это datetime
                last_used = result.get('last_used')
                last_used_str = None
                if isinstance(last_used, datetime):
                    last_used_str = last_used.isoformat()
                elif isinstance(last_used, str): # Если уже строка
                    last_used_str = last_used

                usage_stats[account_id] = {
                    "usage_count": result.get('requests_count', 0), # Используем requests_count из Redis
                    "last_used": last_used_str # Используем строку ISO
                }
            else:
                 usage_stats[account_id] = default_stats
                
        logger.info(f"Статистика использования всех клиентов VK успешно получена из Redis: {len(usage_stats)} клиентов")
        return usage_stats
            
    async def disconnect_inactive_clients(self, inactive_timeout_seconds: int = 3600):
        """Сбрасывает статистику в Redis для VK аккаунтов, которые не использовались дольше указанного времени."""
        logger.info(f"Запуск проверки неактивных клиентов VK (таймаут: {inactive_timeout_seconds} сек)...")
        reset_count = 0
        # Получаем все ID аккаунтов, которые есть в пуле (независимо от api_key)
        account_ids = list(self.clients.keys())
        if not account_ids:
            logger.info("Нет активных клиентов VK в пуле для проверки.")
            return

        current_time_utc = datetime.now(timezone.utc)

        # Получаем статистику из Redis для всех клиентов в пуле
        tasks = [get_account_stats_redis(acc_id, self.platform) for acc_id in account_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        from redis_utils import reset_account_stats_redis # Импортируем здесь, чтобы избежать циклической зависимости на уровне модуля

        for i, result in enumerate(results):
            account_id = account_ids[i]
            last_used_dt = datetime.min.replace(tzinfo=timezone.utc) # Значение по умолчанию

            if isinstance(result, Exception):
                logger.error(f"Ошибка получения статистики из Redis для VK {account_id} при проверке неактивности: {result}")
                # Пропускаем этот аккаунт, так как не можем определить время использования
                continue
            elif isinstance(result, dict):
                if last_used_str := result.get('last_used'):
                    try:
                        dt_from_redis = datetime.fromisoformat(last_used_str)
                        if dt_from_redis.tzinfo is None:
                            last_used_dt = dt_from_redis.replace(tzinfo=timezone.utc)
                        else:
                            last_used_dt = dt_from_redis.astimezone(timezone.utc)
                    except (ValueError, TypeError):
                         logger.warning(f"Не удалось преобразовать last_used '{last_used_str}' из Redis для VK {account_id} при проверке неактивности.")
                         # last_used_dt остается datetime.min
            # else: # result is None or other type - last_used_dt остается datetime.min

            # Вычисляем время неактивности
            time_since_last_use = (current_time_utc - last_used_dt).total_seconds()

            # Проверяем, что аккаунт неактивен достаточно долго
            # Используем > 0, чтобы не сбрасывать только что добавленные аккаунты (last_used_dt == min)
            if last_used_dt > datetime.min.replace(tzinfo=timezone.utc) and time_since_last_use > inactive_timeout_seconds:
                logger.info(f"VK аккаунт {account_id} неактивен ({time_since_last_use:.1f} сек), сбрасываем статистику в Redis...")
                reset_success = await reset_account_stats_redis(account_id, self.platform)
                if reset_success:
                    reset_count += 1
                    logger.info(f"Статистика для VK аккаунта {account_id} успешно сброшена в Redis.")
                    # Сбросим degraded_mode для клиента, если он существует
                    client = self.get_client(account_id)
                    if client and hasattr(client, 'set_degraded_mode'):
                         client.set_degraded_mode(False)
                         logger.info(f"Отключен режим деградации для неактивного VK аккаунта {account_id}")
                else:
                    logger.warning(f"Не удалось сбросить статистику в Redis для неактивного VK аккаунта {account_id}")

        if reset_count > 0:
            logger.info(f"Проверка неактивных клиентов VK завершена. Сброшена статистика для: {reset_count} аккаунтов.")
        else:
            logger.info("Проверка неактивных клиентов VK завершена. Активных для сброса статистики не найдено.")

# --- Перемещенные хелперы для прокси ---
def validate_proxy(proxy: Optional[str]) -> Tuple[bool, str]:
    """
    Валидирует строку прокси и возвращает статус валидации и тип прокси.
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
# --- Конец перемещенных хелперов ---

# --- Функция create_telegram_client ---
async def create_telegram_client(
    session_path: str, 
    api_id: int, 
    api_hash: str, 
    proxy: Optional[str] = None,
    # === Новые параметры ===
    device_model: str = "Social Scraper",
    system_version: str = "1.0",
    app_version: str = "1.0",
    lang_code: str = "en",
    system_lang_code: str = "en"
    # =======================
) -> TelegramClient:
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
            # === Использование новых параметров ===
            device_model=device_model,
            system_version=system_version,
            app_version=app_version,
            lang_code=lang_code,
            system_lang_code=system_lang_code,
            # =====================================
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
# --- Конец функции create_telegram_client ---

class TelegramClientPool(ClientPool):
    """Пул клиентов Telegram."""
    
    def __init__(self):
        super().__init__()
        self.platform = 'telegram'
        self.connected_clients = set()  # Множество подключенных клиентов (IDs as strings)
        self.locked_clients = set()     # Множество заблокированных клиентов (используемых в данный момент) (IDs as strings)
        self.client_auth_status: Dict[str, Union[bool, str]] = {} # Статус авторизации клиентов {'account_id': True/False/'2fa_required'} (IDs as strings)
        self.client_semaphores: Dict[str, asyncio.Semaphore] = {}     # Семафоры для контроля параллельного доступа к клиентам (IDs as strings)
        self.max_retries = 3
        self.retry_delay = 5  # секунды
        self.current_index = 0
        self.degraded_mode_status: Dict[str, bool] = {} # (IDs as strings)
        self.usage_counts: Dict[str, int] = {} # (IDs as strings)
        self.last_used: Dict[str, datetime] = {} # (IDs as strings)
        self.active_accounts: Dict[str, Dict] = {} # Инициализируем active_accounts (IDs as strings)

    async def get_active_clients(self, api_key):
        """Получает активные клиенты Telegram на основе активных аккаунтов."""
        from user_manager import get_active_accounts
        active_accounts = await get_active_accounts(api_key, "telegram")
        
        # Проверяем, все ли активные аккаунты имеют клиентов
        for account in active_accounts:
            if account['id'] not in self.clients:
                client = await self.create_client(account)
                if client:
                    self.add_client(account['id'], client)
                    logger.info(f"Создан новый клиент Telegram для аккаунта {account['id']}")
        
        return active_accounts
    
    async def create_client(self, account):
        """Создает нового клиента Telegram."""
        import os
        api_id = account.get('api_id')
        api_hash = account.get('api_hash')
        session_file = account.get('session_file')
        proxy = account.get('proxy')
        account_id = account.get('id')
        device_model = account.get('device_model') or 'Social Scraper'
        system_version = account.get('system_version') or '1.0'
        app_version = account.get('app_version') or '1.0'
        lang_code = account.get('lang_code') or 'en'
        system_lang_code = account.get('system_lang_code') or 'en'
        # Проверяем наличие необходимых данных
        if not all([api_id, api_hash, session_file, account_id]):
            logger.error(
                f"Невозможно создать клиент Telegram для аккаунта {account_id}: отсутствуют необходимые данные. "
                f"api_id={api_id}, api_hash={api_hash}, session_file={session_file}, account_id={account_id}, "
                f"proxy={proxy}, device_model={device_model}, system_version={system_version}, "
                f"app_version={app_version}, lang_code={lang_code}, system_lang_code={system_lang_code}"
            )
            if session_file and not os.path.exists(session_file):
                logger.error(f"Файл сессии {session_file} не найден на диске!")
            return None
        try:
            if api_id is not None:
                api_id = int(api_id)
            else:
                logger.error(f"api_id равен None для аккаунта {account_id}")
                return None
            if not isinstance(api_hash, str):
                api_hash = str(api_hash)
            if not isinstance(session_file, str):
                session_file = str(session_file)
        except (ValueError, TypeError) as e:
            logger.error(
                f"Ошибка преобразования типов данных для аккаунта {account_id}: {e}. "
                f"api_id={api_id}, api_hash={api_hash}, session_file={session_file}, account_id={account_id}"
            )
            return None
        for attempt in range(self.max_retries):
            try:
                client = await create_telegram_client(
                    session_path=session_file,
                    api_id=api_id,
                    api_hash=api_hash,
                    proxy=proxy,
                    device_model=device_model,
                    system_version=system_version,
                    app_version=app_version,
                    lang_code=lang_code,
                    system_lang_code=system_lang_code
                )
                if client:
                    logger.info(f"Успешно создан клиент Telegram для аккаунта {account_id}")
                    return client
                logger.warning(
                    f"Попытка {attempt + 1}/{self.max_retries} создания клиента Telegram для аккаунта {account_id} не удалась (client is None). "
                    f"session_file={session_file}, proxy={proxy}, api_id={api_id}, api_hash={api_hash}"
                )
                if session_file and not os.path.exists(session_file):
                    logger.error(f"Файл сессии {session_file} не найден на диске!")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay)
            except Exception as e:
                logger.error(
                    f"Попытка {attempt + 1}/{self.max_retries} создания клиента Telegram для аккаунта {account_id} не удалась: {e}. "
                    f"session_file={session_file}, proxy={proxy}, api_id={api_id}, api_hash={api_hash}"
                )
                if session_file and not os.path.exists(session_file):
                    logger.error(f"Файл сессии {session_file} не найден на диске!")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay)
        logger.error(
            f"Не удалось создать клиент Telegram для аккаунта {account_id} после {self.max_retries} попыток. "
            f"session_file={session_file}, proxy={proxy}, api_id={api_id}, api_hash={api_hash}"
        )
        if session_file and not os.path.exists(session_file):
            logger.error(f"Файл сессии {session_file} не найден на диске!")
        return None

    async def disconnect_client(self, account_id: str):
        """Отключает и удаляет клиента Telegram по его ID."""
        # --- ИЗМЕНЕНИЕ: Работаем с account_id как со строкой (UUID) --- 
        logger.info(f"Отключение клиента Telegram для аккаунта {account_id}")
        client = self.clients.pop(account_id, None)
        self.active_accounts.pop(account_id, None) # Удаляем из активных
        self.degraded_mode_status.pop(account_id, None) # Сбрасываем статус деградации

        # --- Убираем ненужное преобразование в int --- 
        # try:
        #     numeric_account_id = int(account_id)
        # except ValueError:
        #     logger.error(f"Не удалось преобразовать ID аккаунта в число: {account_id}")
        #     numeric_account_id = None # Или другое значение по умолчанию
        # ------------------------------------------
        
        if client:
            try:
                if client.is_connected():
                    await client.disconnect()
                    logger.info(f"Клиент Telegram для аккаунта {account_id} успешно отключен.")
                else:
                    logger.info(f"Клиент Telegram для аккаунта {account_id} уже был отключен.")
            except Exception as e:
                logger.error(f"Ошибка при отключении клиента Telegram для аккаунта {account_id}: {e}")
        else:
            logger.warning(f"Попытка отключить несуществующего клиента Telegram для аккаунта {account_id}")
        
        # Опционально: Обновить статус в основной базе данных?
        # await update_account_status(account_id, 'disconnected') # Пример

    def get_client_auth_status(self, account_id: Union[int, str]) -> Optional[Union[bool, str]]:
        """
        Получает статус авторизации клиента Telegram по ID аккаунта.
        
        Args:
            account_id (Union[int, str]): ID аккаунта
            
        Returns:
            Optional[Union[bool, str]]: Статус авторизации клиента (True, False, '2fa_required') или None
        """
        account_id_str = str(account_id) # Ensure string ID
        logger.debug(f"Получение статуса авторизации клиента Telegram для аккаунта {account_id_str}")

        try:
            # Получаем статус авторизации
            auth_status = self.client_auth_status.get(account_id_str) # Use string ID

            logger.debug(f"Статус авторизации клиента Telegram для аккаунта {account_id_str}: {auth_status}")
            return auth_status # Может быть None, если статус неизвестен

        except Exception as e:
            logger.error(f"Ошибка при получении статуса авторизации клиента Telegram: {str(e)}")
            logger.error(f"Трассировка: {traceback.format_exc()}")
            return None

    def set_client_auth_status(self, account_id: Union[int, str], status: Union[bool, str]) -> None:
        """
        Устанавливает статус авторизации клиента Telegram по ID аккаунта.
        
        Args:
            account_id (Union[int, str]): ID аккаунта
            status (Union[bool, str]): Статус авторизации (True, False, '2fa_required')
        """
        account_id_str = str(account_id) # Ensure string ID
        logger.info(f"Установка статуса авторизации клиента Telegram для аккаунта {account_id_str}: {status}")

        try:
            # Устанавливаем статус авторизации (даже если клиента еще нет в self.clients)
            self.client_auth_status[account_id_str] = status # Use string ID

            logger.info(f"Статус авторизации клиента Telegram успешно установлен для аккаунта {account_id_str}: {status}")

        except Exception as e:
            logger.error(f"Ошибка при установке статуса авторизации клиента Telegram: {str(e)}")
            logger.error(f"Трассировка: {traceback.format_exc()}")

    def get_client_usage_count(self, account_id: Union[int, str]) -> Optional[int]:
        """
        Получает количество использований клиента Telegram по ID аккаунта.
        
        Args:
            account_id (Union[int, str]): ID аккаунта
            
        Returns:
            Optional[int]: Количество использований клиента или 0, если клиент не найден
        """
        account_id_str = str(account_id) # Ensure string ID
        logger.debug(f"Получение количества использований клиента Telegram для аккаунта {account_id_str}")

        try:
            # Получаем количество использований, возвращаем 0 если нет
            usage_count = self.usage_counts.get(account_id_str, 0) # Use string ID

            logger.debug(f"Количество использований клиента Telegram для аккаунта {account_id_str}: {usage_count}")
            return usage_count

        except Exception as e:
            logger.error(f"Ошибка при получении количества использований клиента Telegram: {str(e)}")
            logger.error(f"Трассировка: {traceback.format_exc()}")
            return 0 # Return 0 on error

    def get_client_last_used(self, account_id: Union[int, str]) -> Optional[datetime]:
        """
        Получает время последнего использования клиента Telegram по ID аккаунта.
        
        Args:
            account_id (Union[int, str]): ID аккаунта
            
        Returns:
            Optional[datetime]: Время последнего использования клиента или None, если не использовался
        """
        account_id_str = str(account_id) # Ensure string ID
        logger.debug(f"Получение времени последнего использования клиента Telegram для аккаунта {account_id_str}")

        try:
            # Получаем время последнего использования
            last_used = self.last_used.get(account_id_str) # Use string ID

            logger.debug(f"Время последнего использования клиента Telegram для аккаунта {account_id_str}: {last_used}")
            return last_used # Может быть None

        except Exception as e:
            logger.error(f"Ошибка при получении времени последнего использования клиента Telegram: {str(e)}")
            logger.error(f"Трассировка: {traceback.format_exc()}")
            return None

    async def get_client_usage_stats(self, account_id: Union[int, str]) -> Optional[Dict[str, Any]]:
        """
        Асинхронно получает статистику использования клиента Telegram по ID аккаунта из Redis.
        
        Args:
            account_id (Union[int, str]): ID аккаунта
            
        Returns:
            Optional[Dict[str, Any]]: Статистика использования клиента или None, если данных нет
        """
        account_id_str = str(account_id) # Ensure string ID
        logger.debug(f"Получение статистики использования клиента Telegram для аккаунта {account_id_str} из Redis...")
        stats = {}
        try:
            # Получаем статистику из Redis
            redis_stats = await get_account_stats_redis(account_id_str, self.platform)

            if redis_stats:
                # Используем usage_count для совместимости с предыдущим форматом
                stats["usage_count"] = redis_stats.get('requests_count', 0)
                # last_used из Redis уже строка ISO или None
                stats["last_used"] = redis_stats.get('last_used')
            else:
                logger.warning(f"Не удалось получить статистику из Redis для аккаунта {account_id_str}. Проверяем только auth_status.")
                
            # Получаем статус авторизации из локального словаря пула (он управляется отдельно)
            auth_status = self.client_auth_status.get(account_id_str)
            if auth_status is not None:
                 stats["auth_status"] = auth_status

            if not stats: # Если словарь пуст (Redis недоступен И нет auth_status)
                 logger.warning(f"Нет статистики использования для аккаунта {account_id_str}")
                 return None

            logger.debug(f"Статистика использования клиента Telegram для аккаунта {account_id_str}: {stats}")
            return stats

        except Exception as e:
            logger.error(f"Ошибка при получении статистики использования клиента Telegram {account_id_str}: {str(e)}")
            logger.error(f"Трассировка: {traceback.format_exc()}")
            return None

    def get_all_clients_stats(self) -> Dict[int, Dict[str, Any]]:
        """
        Получает статистику использования всех клиентов Telegram.
        
        Returns:
            Dict[int, Dict[str, Any]]: Словарь со статистикой использования всех клиентов
        """
        logger.info("Получение статистики использования всех клиентов Telegram")
        
        try:
            # Формируем статистику для всех клиентов
            stats = {}
            for account_id in self.clients:
                # Получаем статистику для каждого клиента
                client_stats = self.get_client_usage_stats(account_id)
                if client_stats:
                    stats[account_id] = client_stats
                    
            logger.info(f"Статистика использования всех клиентов Telegram успешно получена: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Ошибка при получении статистики использования всех клиентов Telegram: {str(e)}")
            import traceback
            logger.error(f"Трассировка: {traceback.format_exc()}")
            return {}

    def get_all_clients_auth_status(self) -> Dict[int, bool]:
        """
        Получает статус авторизации всех клиентов Telegram.
        
        Returns:
            Dict[int, bool]: Словарь со статусом авторизации всех клиентов
        """
        logger.info("Получение статуса авторизации всех клиентов Telegram")
        
        try:
            # Формируем статус авторизации для всех клиентов
            auth_status = {}
            for account_id in self.clients:
                # Получаем статус авторизации для каждого клиента
                status = self.get_client_auth_status(account_id)
                if status is not None:
                    auth_status[account_id] = status
                    
            logger.info(f"Статус авторизации всех клиентов Telegram успешно получен: {auth_status}")
            return auth_status
            
        except Exception as e:
            logger.error(f"Ошибка при получении статуса авторизации всех клиентов Telegram: {str(e)}")
            import traceback
            logger.error(f"Трассировка: {traceback.format_exc()}")
            return {}

    def get_all_clients_usage_count(self) -> Dict[int, int]:
        """
        Получает количество использований всех клиентов Telegram.
        
        Returns:
            Dict[int, int]: Словарь с количеством использований всех клиентов
        """
        logger.info("Получение количества использований всех клиентов Telegram")
        
        try:
            # Формируем количество использований для всех клиентов
            usage_count = {}
            for account_id in self.clients:
                # Получаем количество использований для каждого клиента
                count = self.get_client_usage_count(account_id)
                if count is not None:
                    usage_count[account_id] = count
                    
            logger.info(f"Количество использований всех клиентов Telegram успешно получено: {usage_count}")
            return usage_count
            
        except Exception as e:
            logger.error(f"Ошибка при получении количества использований всех клиентов Telegram: {str(e)}")
            import traceback
            logger.error(f"Трассировка: {traceback.format_exc()}")
            return {}

    def get_all_clients_last_used(self) -> Dict[int, datetime]:
        """
        Получает время последнего использования всех клиентов Telegram.
        
        Returns:
            Dict[int, datetime]: Словарь со временем последнего использования всех клиентов
        """
        logger.info("Получение времени последнего использования всех клиентов Telegram")
        
        try:
            # Формируем время последнего использования для всех клиентов
            last_used = {}
            for account_id in self.clients:
                # Получаем время последнего использования для каждого клиента
                time = self.get_client_last_used(account_id)
                if time is not None:
                    last_used[account_id] = time
                    
            logger.info(f"Время последнего использования всех клиентов Telegram успешно получено: {last_used}")
            return last_used
            
        except Exception as e:
            logger.error(f"Ошибка при получении времени последнего использования всех клиентов Telegram: {str(e)}")
            import traceback
            logger.error(f"Трассировка: {traceback.format_exc()}")
            return {}

    def get_clients_usage_statistics(self) -> Dict[str, Dict[str, Any]]:
        """
        Получает статистику использования всех клиентов Telegram.
        
        Returns:
            Dict[str, Dict[str, Any]]: Словарь со статистикой использования всех клиентов
        """
        logger.info("Получение статистики использования всех клиентов Telegram")
        
        try:
            # Формируем статистику использования для всех клиентов
            usage_stats = {}
            for account_id in self.clients:
                usage_stats[account_id] = {
                    "usage_count": self.usage_counts.get(account_id, 0),
                    "last_used": self.last_used.get(account_id, 0),
                    "connected": account_id in self.connected_clients,
                    "auth_status": self.client_auth_status.get(account_id, False)
                }
                
            logger.info(f"Статистика использования всех клиентов Telegram успешно получена: {len(usage_stats)} клиентов")
            return usage_stats
            
        except Exception as e:
            logger.error(f"Ошибка при получении статистики использования всех клиентов Telegram: {str(e)}")
            import traceback
            logger.error(f"Трассировка: {traceback.format_exc()}")
            return {}

    def get_all_clients(self) -> Dict[int, TelegramClient]:
        """
        Получает все клиенты Telegram.
        
        Returns:
            Dict[int, TelegramClient]: Словарь со всеми клиентами
        """
        logger.info("Получение всех клиентов Telegram")
        
        try:
            # Формируем словарь со всеми клиентами
            clients = {}
            for account_id in self.clients:
                # Получаем клиент для каждого аккаунта
                client = self.get_client(account_id)
                if client is not None:
                    clients[account_id] = client
                    
            logger.info(f"Все клиенты Telegram успешно получены: {len(clients)} клиентов")
            return clients
            
        except Exception as e:
            logger.error(f"Ошибка при получении всех клиентов Telegram: {str(e)}")
            import traceback
            logger.error(f"Трассировка: {traceback.format_exc()}")
            return {}

    def get_all_clients_info(self) -> Dict[int, Dict[str, Any]]:
        """
        Получает информацию о всех клиентах Telegram.
        
        Returns:
            Dict[int, Dict[str, Any]]: Словарь с информацией о всех клиентах
        """
        logger.info("Получение информации о всех клиентах Telegram")
        
        try:
            # Формируем информацию о всех клиентах
            info = {}
            for account_id in self.clients:
                # Получаем информацию о каждом клиенте
                client = self.get_client(account_id)
                if client is not None:
                    # Получаем статистику использования
                    stats = self.get_client_usage_stats(account_id)
                    if stats is not None:
                        info[account_id] = {
                            "client": client,
                            "stats": stats
                        }
                        
            logger.info(f"Информация о всех клиентах Telegram успешно получена: {len(info)} клиентов")
            return info
            
        except Exception as e:
            logger.error(f"Ошибка при получении информации о всех клиентах Telegram: {str(e)}")
            import traceback
            logger.error(f"Трассировка: {traceback.format_exc()}")
            return {}

    def get_all_clients_info_with_auth_and_stats_and_usage_and_proxy(self) -> Dict[int, Dict[str, Any]]:
        """
        Получает информацию о всех клиентах Telegram с проверкой авторизации, статистикой, использованием и прокси.
        
        Returns:
            Dict[int, Dict[str, Any]]: Словарь с информацией о всех клиентах
        """
        logger.info("Получение информации о всех клиентах Telegram с проверкой авторизации, статистикой, использованием и прокси")
        
        try:
            # Формируем информацию о всех клиентах
            info = {}
            for account_id in self.clients:
                # Получаем информацию о каждом клиенте
                client = self.get_client(account_id)
                if client is not None:
                    # Получаем статистику использования
                    stats = self.get_client_usage_stats(account_id)
                    if stats is not None:
                        # Проверяем статус авторизации
                        auth_status = self.get_client_auth_status(account_id)
                        if auth_status is not None:
                            # Получаем количество использований
                            usage_count = self.get_client_usage_count(account_id)
                            if usage_count is not None:
                                # Получаем время последнего использования
                                last_used = self.get_client_last_used(account_id)
                                if last_used is not None:
                                    # Проверяем, подключен ли клиент
                                    is_connected = client.is_connected()
                                    # Получаем информацию о прокси
                                    proxy = client.proxy
                                    info[account_id] = {
                                        "client": client,
                                        "stats": stats,
                                        "auth_status": auth_status,
                                        "usage_count": usage_count,
                                        "last_used": last_used,
                                        "is_connected": is_connected,
                                        "proxy": proxy
                                    }
                                    
            logger.info(f"Информация о всех клиентах Telegram с проверкой авторизации, статистикой, использованием и прокси успешно получена: {len(info)} клиентов")
            return info
            
        except Exception as e:
            logger.error(f"Ошибка при получении информации о всех клиентах Telegram с проверкой авторизации, статистикой, использованием и прокси: {str(e)}")
            import traceback
            logger.error(f"Трассировка: {traceback.format_exc()}")
            return {}

    async def disconnect_inactive_clients(self, inactive_timeout_seconds: int = 300):
        """Отключает клиентов, которые не использовались дольше указанного времени."""
        logger.info(f"Запуск проверки неактивных клиентов Telegram (таймаут: {inactive_timeout_seconds} сек)...")
        disconnected_count = 0
        current_time = time.time()
        # Копируем ключи, чтобы избежать изменения словаря во время итерации
        account_ids = list(self.clients.keys())

        for account_id in account_ids:
            client = self.get_client(account_id)
            if not client:
                continue

            # Получаем время последнего использования (может быть datetime или 0)
            last_used_obj = self.last_used.get(account_id)
            last_used_timestamp = 0.0 # Инициализируем как float
            if isinstance(last_used_obj, datetime):
                 # Преобразуем datetime в float timestamp
                 last_used_timestamp = last_used_obj.timestamp()
            elif last_used_obj is not None:
                 # Если там что-то другое (например, старый timestamp float), пробуем использовать
                 try: last_used_timestamp = float(last_used_obj)
                 except (ValueError, TypeError): pass # Оставляем 0.0 если не float
            
            # Вычисляем разницу в секундах (float - float)
            time_since_last_use = current_time - last_used_timestamp

            # Проверяем, что клиент не используется и неактивен достаточно долго
            if time_since_last_use > inactive_timeout_seconds:
                try:
                    # --- Добавляем сброс статистики и деградации ПЕРЕД отключением ---
                    # 1. Сбрасываем статистику в Redis
                    from redis_utils import reset_account_stats_redis
                    reset_success = await reset_account_stats_redis(account_id, self.platform)
                    if reset_success:
                        logger.info(f"Сброшена статистика в Redis для неактивного аккаунта {account_id}")
                    else:
                        logger.warning(f"Не удалось сбросить статистику в Redis для неактивного аккаунта {account_id}")
                    
                    # 2. Отключаем режим деградации (если он был)
                    if hasattr(client, 'set_degraded_mode'):
                        client.set_degraded_mode(False)
                        logger.info(f"Отключен режим деградации для неактивного аккаунта {account_id}")
                    # --- Конец добавлений ---

                    # 3. Отключаем сам клиент
                    if client.is_connected():
                        logger.info(f"Клиент {account_id} неактивен ({time_since_last_use:.1f} сек), отключаем...")
                        await client.disconnect()
                        disconnected_count += 1
                        # Удаляем из множества подключенных клиентов (если используется)
                        # if account_id in self.connected_clients: 
                        #     self.connected_clients.remove(account_id)
                    
                    # 4. Удаляем клиент из основного словаря пула
                    if account_id in self.clients:
                        del self.clients[account_id]
                        logger.info(f"Удален клиент {account_id} из пула после отключения.")
                        
                except Exception as e:
                    logger.error(f"Ошибка при отключении неактивного клиента {account_id}: {e}")

        if disconnected_count > 0:
            logger.info(f"Проверка неактивных клиентов завершена. Отключено: {disconnected_count}")
        # else:
            # logger.info("Проверка неактивных клиентов завершена. Активных для отключения не найдено.")

    # Добавляем метод для установки/снятия режима деградации
    def set_degraded_mode(self, account_id: Union[int, str], degraded: bool):
        """Устанавливает режим пониженной производительности для Telegram аккаунта."""
        account_id_str = str(account_id) # Приводим к строке на всякий случай
        if degraded:
            logger.warning(f"Включение режима деградации для Telegram аккаунта {account_id_str}")
            self.degraded_mode_status[account_id_str] = True
        else:
            # Снимаем режим деградации, если он был установлен
            if self.degraded_mode_status.pop(account_id_str, None):
                logger.info(f"Отключение режима деградации для Telegram аккаунта {account_id_str}")
            # Если ключа не было, ничего не делаем

    async def select_next_client(self, api_key: str, strategy: str = "round_robin") -> Tuple[Any, str]:
        """
        Выбирает следующего клиента Telegram для использования.
        Проверяет подключение и авторизацию перед возвратом.
        """
        active_accounts = await self.get_active_clients(api_key)
        if not active_accounts:
            logger.error(f"Нет активных аккаунтов {self.platform} для API ключа {api_key}")
            return None, ""

        # --- Получаем статистику из Redis для активных аккаунтов --- 
        account_stats = {}
        if active_accounts:
            tasks = [get_account_stats_redis(acc['id'], self.platform) for acc in active_accounts]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for i, result in enumerate(results):
                acc_id = active_accounts[i]['id']
                if isinstance(result, Exception):
                    logger.error(f"Ошибка получения статистики из Redis для аккаунта {self.platform} {acc_id}: {result}")
                    # Используем значения по умолчанию при ошибке
                    account_stats[acc_id] = {'requests_count': 0, 'last_used': datetime.min.replace(tzinfo=timezone.utc)}
                elif isinstance(result, dict):
                    last_used_dt = datetime.min.replace(tzinfo=timezone.utc)
                    if last_used_iso := result.get('last_used'):
                        try:
                            dt_from_redis = datetime.fromisoformat(last_used_iso)
                            last_used_dt = dt_from_redis.astimezone(timezone.utc) if dt_from_redis.tzinfo else dt_from_redis.replace(tzinfo=timezone.utc)
                        except (ValueError, TypeError):
                            logger.warning(f"Не удалось преобразовать last_used '{last_used_iso}' из Redis для {self.platform} {acc_id}")
                    account_stats[acc_id] = {
                        'requests_count': result.get('requests_count', 0),
                        'last_used': last_used_dt
                    }
                else:
                    logger.warning(f"Неожиданный тип данных статистики из Redis для {self.platform} {acc_id}: {type(result)}")
                    account_stats[acc_id] = {'requests_count': 0, 'last_used': datetime.min.replace(tzinfo=timezone.utc)}
        # ----------------------------------------------------------

        # Сортируем аккаунты по стратегии, используя данные из Redis
        if strategy == "round_robin":
            sorted_accounts = sorted(active_accounts, key=lambda acc: account_stats.get(acc['id'], {}).get('last_used', datetime.min.replace(tzinfo=timezone.utc)))
        elif strategy == "least_used":
            sorted_accounts = sorted(active_accounts, key=lambda acc: account_stats.get(acc['id'], {}).get('requests_count', 0))
        elif strategy == "random":
            import random
            random.shuffle(active_accounts)
            sorted_accounts = active_accounts
        else:
            logger.warning(f"Неизвестная стратегия '{strategy}', используется round_robin")
            sorted_accounts = sorted(active_accounts, key=lambda acc: account_stats.get(acc['id'], {}).get('last_used', datetime.min.replace(tzinfo=timezone.utc)))

        # Пробуем каждый аккаунт по очереди, пока не найдем работающий
        for account in sorted_accounts:
            account_id = account['id']
            logger.info(f"Попытка выбрать аккаунт {account_id}...")

            client = self.get_client(account_id)
            if not client:
                logger.warning(f"Клиент для аккаунта {account_id} не найден в пуле, создаем новый...")
                client = await self.create_client(account) # create_client НЕ подключает
                if not client:
                    logger.error(f"Не удалось создать {self.platform} клиент для аккаунта {account_id}")
                    continue # Переходим к следующему аккаунту
                self.add_client(account_id, client)
                logger.info(f"Новый клиент для {account_id} создан и добавлен в пул.")
            else:
                 logger.info(f"Клиент для аккаунта {account_id} найден в пуле.")

            # --- Проверка и установка соединения/авторизации --- 
            try:
                if not client.is_connected():
                    logger.info(f"Клиент {account_id} не подключен. Попытка подключения...")
                    await client.connect()
                    if not client.is_connected(): # Повторная проверка после connect
                         logger.error(f"Не удалось подключить клиент {account_id} после вызова connect(). Пропуск.")
                         continue
                    logger.info(f"Клиент {account_id} успешно подключен.")
                else:
                    logger.info(f"Клиент {account_id} уже был подключен.")

                if not await client.is_user_authorized():
                    logger.warning(f"Клиент {account_id} подключен, но НЕ авторизован. Пропуск.")
                    # Возможно, стоит обновить статус в БД или пометить аккаунт как проблемный?
                    continue # Переходим к следующему аккаунту
                else:
                     logger.info(f"Клиент {account_id} авторизован.")

                # Если дошли сюда - клиент подключен и авторизован
                logger.info(f"*** Успешно выбран {self.platform} клиент для аккаунта {account_id} ***")
                return client, account_id

            except ConnectionError as ce:
                 logger.error(f"Ошибка ПОДКЛЮЧЕНИЯ для клиента {account_id}: {ce}. Пропуск.")
                 continue
            except AuthKeyError as ae:
                 logger.error(f"Ошибка АВТОРИЗАЦИИ (AuthKeyError) для клиента {account_id}: {ae}. Пропуск.")
                 # Считаем этот аккаунт нерабочим
                 continue
            except Exception as e:
                 logger.error(f"Непредвиденная ошибка при проверке/подключении клиента {account_id}: {e}", exc_info=True)
                 continue # Пробуем следующий аккаунт
            # ----------------------------------------------------

        logger.error(f"Не удалось выбрать РАБОЧИЙ {self.platform} аккаунт для API ключа {api_key} после перебора всех активных.")
        return None, ""

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
# Удаляем глобальные экземпляры пулов 