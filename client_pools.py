import asyncio
import logging
import time
import os
import inspect
from typing import Dict, List, Optional, Set, Tuple, Any, Union
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, FloodWaitError
from datetime import datetime

logger = logging.getLogger(__name__)

class ClientPool:
    """Базовый класс для пула клиентов."""
    
    def __init__(self):
        self.clients = {}  # account_id -> client
        self.usage_counts = {}  # account_id -> count
        self.last_used = {}  # account_id -> timestamp
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
        self.usage_counts[account_id] = 0
        self.last_used[account_id] = datetime.now()
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
            # Простая ротация по кругу
            sorted_accounts = sorted(
                active_accounts, 
                key=lambda acc: self.last_used.get(acc['id'], datetime.min)
            )
        elif strategy == "least_used":
            # Выбираем наименее использованный аккаунт
            sorted_accounts = sorted(
                active_accounts, 
                key=lambda acc: self.usage_counts.get(acc['id'], 0)
            )
        elif strategy == "random":
            # Случайный выбор аккаунта
            import random
            random.shuffle(active_accounts)
            sorted_accounts = active_accounts
        else:
            # По умолчанию используем round_robin
            sorted_accounts = sorted(
                active_accounts, 
                key=lambda acc: self.last_used.get(acc['id'], datetime.min)
            )

        # Пробуем каждый аккаунт по очереди, пока не найдем работающий
        for account in sorted_accounts:
            account_id = account['id']
            
            # Получаем существующий клиент или создаем новый
            client = self.get_client(account_id)
            if not client:
                client = await self.create_client(account)
                if not client:
                    logger.error(f"Не удалось создать {self.platform} клиент для аккаунта {account_id}")
                    continue
                self.add_client(account_id, client)

            # Обновляем статистику использования
            self.usage_counts[account_id] = self.usage_counts.get(account_id, 0) + 1
            self.last_used[account_id] = datetime.now()
            
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
        Выбирает следующего клиента VK для использования.
        
        Args:
            api_key: API ключ пользователя
            strategy: Стратегия выбора клиента ("round_robin", "least_used", "random")
            
        Returns:
            Tuple[Any, str]: (клиент VK, ID аккаунта) или (None, "")
        """
        # Получаем активные аккаунты
        active_accounts = await self.get_active_clients(api_key)
        if not active_accounts:
            logger.error(f"Нет активных аккаунтов VK для API ключа {api_key}")
            return None, ""  # Возвращаем пустую строку вместо None для второго элемента кортежа

        # Объявляем переменную selected_account перед условиями
        selected_account = None

        # Выбираем аккаунт в зависимости от стратегии
        if strategy == "round_robin":
            # Циклическая ротация
            if self.current_index >= len(active_accounts):
                self.current_index = 0
            selected_account = active_accounts[self.current_index]
            self.current_index += 1
            
        elif strategy == "least_used":
            # Выбираем наименее использованный аккаунт
            sorted_accounts = sorted(
                active_accounts,
                key=lambda acc: (
                    self.usage_counts.get(acc['id'], 0),  # Сначала по количеству использований
                    self.last_used.get(acc['id'], datetime.min)  # Потом по времени последнего использования
                )
            )
            if sorted_accounts:
                selected_account = sorted_accounts[0]
            
        elif strategy == "random":
            # Случайный выбор
            import random
            selected_account = random.choice(active_accounts)
            
        else:
            logger.warning(f"Неизвестная стратегия: {strategy}, используем round_robin")
            if self.current_index >= len(active_accounts):
                self.current_index = 0
            selected_account = active_accounts[self.current_index]
            self.current_index += 1

        # Проверяем, был ли выбран аккаунт
        if not selected_account:
            logger.error(f"Не удалось выбрать аккаунт VK для API ключа {api_key} со стратегией {strategy}")
            return None, ""

        account_id = selected_account.get('id')
        if not account_id:
            logger.error(f"Выбранный аккаунт VK не имеет ID: {selected_account}")
            return None, ""
        
        # Получаем или создаем клиента
        client = self.get_client(account_id)
        if not client:
            client = await self.create_client(selected_account)
            if not client:
                logger.error(f"Не удалось создать клиент VK для аккаунта {account_id}")
                # Возможно, стоит попробовать следующий аккаунт? Пока возвращаем None.
                return None, ""  # Исправлено: возвращаем (None, "") вместо (None, None)
            self.add_client(account_id, client)

        # Обновляем статистику использования
        self.usage_counts[account_id] = self.usage_counts.get(account_id, 0) + 1
        self.last_used[account_id] = datetime.now()
        
        # Обновляем статистику в Redis
        try:
            from redis_utils import update_account_usage_redis
            
            # --- Непосредственно вызываем и ожидаем асинхронную функцию ---
            logger.info(f"Попытка выполнить await update_account_usage_redis (acc: {account_id})...")
            # Напрямую ожидаем результат вызова асинхронной функции
            update_success = await update_account_usage_redis(api_key, account_id, "vk")
            if update_success:
                logger.info(f"Успешно обновлена статистика Redis для аккаунта {account_id}.")
            else:
                logger.warning(f"Не удалось обновить статистику Redis для аккаунта {account_id} (функция вернула False).")

        except ImportError:
             logger.error("Не удалось импортировать update_account_usage_redis из redis_utils.")
        except Exception as e:
             # Ловим любые исключения во время вызова await
             logger.error(f"Ошибка при вызове await update_account_usage_redis (acc: {account_id}): {e}")
             import traceback
             logger.error(traceback.format_exc())

        logger.info(f"Выбран VK клиент для аккаунта {account_id} (использований: {self.usage_counts[account_id]})")
        return client, account_id
        
    
    async def balance_load(self, api_key):
        """
        Балансирует нагрузку между аккаунтами VK.
        
        Args:
            api_key: API ключ пользователя
        """
        from user_manager import get_active_accounts
        
        # Получаем активные аккаунты
        active_accounts = await get_active_accounts(api_key, "vk")
        if not active_accounts or len(active_accounts) <= 1:
            return
        
        # Вычисляем среднее количество запросов
        total_requests = sum(self.usage_counts.get(acc['id'], 0) for acc in active_accounts)
        avg_requests = total_requests / len(active_accounts) if active_accounts else 0
        
        # Если разница между максимальным и средним количеством запросов слишком большая,
        # сбрасываем счетчики для аккаунтов с большим количеством запросов
        max_requests = max(self.usage_counts.get(acc['id'], 0) for acc in active_accounts)
        if max_requests > avg_requests * 1.5:  # Если максимальное количество запросов на 50% больше среднего
            for account in active_accounts:
                account_id = account['id']
                if self.usage_counts.get(account_id, 0) > avg_requests * 1.2:  # Если количество запросов на 20% больше среднего
                    self.usage_counts[account_id] = int(avg_requests)  # Сбрасываем счетчик до среднего значения
                    logger.info(f"Сброшен счетчик запросов для аккаунта VK {account_id} до {int(avg_requests)}")
    
    async def get_pool_status(self, api_key):
        """
        Получает статус пула клиентов VK.
        
        Args:
            api_key: API ключ пользователя
        
        Returns:
            Dict: Статистика использования клиентов
        """
        from user_manager import get_active_accounts
        
        # Получаем активные аккаунты
        active_accounts = await get_active_accounts(api_key, "vk")
        
        # Собираем статистику
        stats = {
            "total_accounts": len(active_accounts),
            "accounts": [
                {
                    "id": acc['id'],
                    "requests": self.usage_counts.get(acc['id'], 0),
                    "last_used": self.last_used.get(acc['id'], 0)
                }
                for acc in active_accounts
            ]
        }
        
        return stats

    def get_clients_usage_statistics(self) -> Dict[str, Dict[str, Any]]:
        """
        Получает статистику использования всех клиентов VK.
        
        Returns:
            Dict[str, Dict[str, Any]]: Словарь со статистикой использования всех клиентов
        """
        logger.info("Получение статистики использования всех клиентов VK")
        
        try:
            # Формируем статистику использования для всех клиентов
            usage_stats = {}
            for account_id in self.clients:
                usage_stats[account_id] = {
                    "usage_count": self.usage_counts.get(account_id, 0),
                    "last_used": self.last_used.get(account_id, 0)
                }
                
            logger.info(f"Статистика использования всех клиентов VK успешно получена: {len(usage_stats)} клиентов")
            return usage_stats
            
        except Exception as e:
            logger.error(f"Ошибка при получении статистики использования всех клиентов VK: {str(e)}")
            import traceback
            logger.error(f"Трассировка: {traceback.format_exc()}")
            return {}


class TelegramClientPool(ClientPool):
    """Пул клиентов Telegram."""
    
    def __init__(self):
        super().__init__()
        self.platform = 'telegram'
        self.connected_clients = set()  # Множество подключенных клиентов
        self.locked_clients = set()     # Множество заблокированных клиентов (используемых в данный момент)
        self.client_auth_status = {}    # Статус авторизации клиентов {'account_id': True/False/'2fa_required'}
        self.client_semaphores = {}     # Семафоры для контроля параллельного доступа к клиентам
        self.max_retries = 3
        self.retry_delay = 5  # секунды
        self.current_index = 0
        
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
        from telegram_utils import create_telegram_client
        
        api_id = account.get('api_id')
        api_hash = account.get('api_hash')
        session_file = account.get('session_file')
        proxy = account.get('proxy')
        account_id = account.get('id')
        
        # Проверяем наличие необходимых данных
        if not all([api_id, api_hash, session_file, account_id]):
            logger.error(f"Невозможно создать клиент Telegram для аккаунта {account_id}: отсутствуют необходимые данные")
            return None
            
        # Преобразуем типы данных для соответствия требуемым параметрам
        try:
            # Проверяем, что api_id не None перед преобразованием
            if api_id is not None:
                api_id = int(api_id)  # Преобразуем api_id в int
            else:
                logger.error(f"api_id равен None для аккаунта {account_id}")
                return None
                
            if not isinstance(api_hash, str):
                api_hash = str(api_hash)
            if not isinstance(session_file, str):
                session_file = str(session_file)
        except (ValueError, TypeError) as e:
            logger.error(f"Ошибка преобразования типов данных для аккаунта {account_id}: {e}")
            return None
            
        for attempt in range(self.max_retries):
            try:
                client = await create_telegram_client(
                    session_path=session_file,
                    api_id=api_id,
                    api_hash=api_hash,
                    proxy=proxy
                )
                
                if client:
                    logger.info(f"Успешно создан клиент Telegram для аккаунта {account_id}")
                    return client
                    
                logger.warning(f"Попытка {attempt + 1}/{self.max_retries} создания клиента Telegram для аккаунта {account_id} не удалась")
                
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay)
                    
            except Exception as e:
                logger.error(f"Попытка {attempt + 1}/{self.max_retries} создания клиента Telegram для аккаунта {account_id} не удалась: {e}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay)
                    
        logger.error(f"Не удалось создать клиент Telegram для аккаунта {account_id} после {self.max_retries} попыток")
        return None

    async def disconnect_client(self, account_id: Union[int, str]) -> bool:
        """
        Отключает клиент Telegram и удаляет его из множества подключенных клиентов.
        
        Args:
            account_id (Union[int, str]): ID аккаунта
            
        Returns:
            bool: True если клиент успешно отключен, False в противном случае
        """
        logger.info(f"Отключение клиента Telegram для аккаунта {account_id}")
        
        try:
            # Преобразуем ID в целое число, если это строка
            try:
                account_id = int(account_id) if isinstance(account_id, str) else account_id
            except (ValueError, TypeError):
                logger.error(f"Не удалось преобразовать ID аккаунта в число: {account_id}")
                return False
            
            # Проверяем, существует ли клиент в пуле
            client = self.get_client(account_id)
            if not client:
                logger.warning(f"Клиент для аккаунта {account_id} не найден в пуле")
                return False
            
            # Проверяем, подключен ли клиент
            try:
                if not client.is_connected():
                    logger.info(f"Клиент для аккаунта {account_id} уже отключен")
                    # Удаляем из множества подключенных клиентов, если он там есть
                    if account_id in self.connected_clients:
                        self.connected_clients.remove(account_id)
                    return True
            except Exception as e:
                logger.error(f"Ошибка при проверке соединения клиента: {e}")
            
            # Отключаем клиент
            try:
                # Используем метод disconnect() телеграм-клиента
                await client.disconnect()
                logger.info(f"Клиент для аккаунта {account_id} успешно отключен")
                
                # Удаляем из множества подключенных клиентов
                if account_id in self.connected_clients:
                    self.connected_clients.remove(account_id)
                
                return True
            except Exception as e:
                logger.error(f"Ошибка при отключении клиента: {e}")
                import traceback
                logger.error(f"Трассировка: {traceback.format_exc()}")
                return False
        
        except Exception as e:
            logger.error(f"Непредвиденная ошибка при отключении клиента Telegram: {e}")
            import traceback
            logger.error(f"Трассировка: {traceback.format_exc()}")
            return False

    def get_client_auth_status(self, account_id: Union[int, str]) -> Optional[bool]:
        """
        Получает статус авторизации клиента Telegram по ID аккаунта.
        
        Args:
            account_id (Union[int, str]): ID аккаунта
            
        Returns:
            Optional[bool]: Статус авторизации клиента или None, если клиент не найден
        """
        logger.info(f"Получение статуса авторизации клиента Telegram для аккаунта {account_id}")
        
        try:
            # Преобразуем ID в целое число, если это строка
            try:
                account_id = int(account_id) if isinstance(account_id, str) else account_id
            except (ValueError, TypeError):
                logger.error(f"Не удалось преобразовать ID аккаунта в число: {account_id}")
                return None
            
            # Проверяем, существует ли клиент в пуле
            if account_id not in self.clients:
                logger.warning(f"Клиент для аккаунта {account_id} не найден в пуле")
                return None
            
            # Получаем статус авторизации
            auth_status = self.client_auth_status.get(account_id)
            
            # Проверяем, что статус не None
            if auth_status is None:
                logger.warning(f"Статус авторизации для аккаунта {account_id} не найден")
                return None
            
            logger.info(f"Статус авторизации клиента Telegram успешно получен для аккаунта {account_id}: {auth_status}")
            return auth_status
            
        except Exception as e:
            logger.error(f"Ошибка при получении статуса авторизации клиента Telegram: {str(e)}")
            import traceback
            logger.error(f"Трассировка: {traceback.format_exc()}")
            return None

    def set_client_auth_status(self, account_id: Union[int, str], status: bool) -> None:
        """
        Устанавливает статус авторизации клиента Telegram по ID аккаунта.
        
        Args:
            account_id (Union[int, str]): ID аккаунта
            status (bool): Статус авторизации
        """
        logger.info(f"Установка статуса авторизации клиента Telegram для аккаунта {account_id}: {status}")
        
        try:
            # Преобразуем ID в целое число, если это строка
            try:
                account_id = int(account_id) if isinstance(account_id, str) else account_id
            except (ValueError, TypeError):
                logger.error(f"Не удалось преобразовать ID аккаунта в число: {account_id}")
                return
            
            # Проверяем, существует ли клиент в пуле
            if account_id not in self.clients:
                logger.warning(f"Клиент для аккаунта {account_id} не найден в пуле")
                return
            
            # Устанавливаем статус авторизации
            self.client_auth_status[account_id] = status
            
            logger.info(f"Статус авторизации клиента Telegram успешно установлен для аккаунта {account_id}: {status}")
            
        except Exception as e:
            logger.error(f"Ошибка при установке статуса авторизации клиента Telegram: {str(e)}")
            import traceback
            logger.error(f"Трассировка: {traceback.format_exc()}")

    def get_client_usage_count(self, account_id: Union[int, str]) -> Optional[int]:
        """
        Получает количество использований клиента Telegram по ID аккаунта.
        
        Args:
            account_id (Union[int, str]): ID аккаунта
            
        Returns:
            Optional[int]: Количество использований клиента или None, если клиент не найден
        """
        logger.info(f"Получение количества использований клиента Telegram для аккаунта {account_id}")
        
        try:
            # Преобразуем ID в целое число, если это строка
            try:
                account_id = int(account_id) if isinstance(account_id, str) else account_id
            except (ValueError, TypeError):
                logger.error(f"Не удалось преобразовать ID аккаунта в число: {account_id}")
                return None
            
            # Проверяем, существует ли клиент в пуле
            if account_id not in self.clients:
                logger.warning(f"Клиент для аккаунта {account_id} не найден в пуле")
                return None
            
            # Получаем количество использований
            usage_count = self.usage_counts.get(account_id)
            
            # Проверяем, что количество использований не None
            if usage_count is None:
                logger.warning(f"Количество использований для аккаунта {account_id} не найдено")
                return None
            
            logger.info(f"Количество использований клиента Telegram успешно получено для аккаунта {account_id}: {usage_count}")
            return usage_count
            
        except Exception as e:
            logger.error(f"Ошибка при получении количества использований клиента Telegram: {str(e)}")
            import traceback
            logger.error(f"Трассировка: {traceback.format_exc()}")
            return None

    def get_client_last_used(self, account_id: Union[int, str]) -> Optional[datetime]:
        """
        Получает время последнего использования клиента Telegram по ID аккаунта.
        
        Args:
            account_id (Union[int, str]): ID аккаунта
            
        Returns:
            Optional[datetime]: Время последнего использования клиента или None, если клиент не найден
        """
        logger.info(f"Получение времени последнего использования клиента Telegram для аккаунта {account_id}")
        
        try:
            # Преобразуем ID в целое число, если это строка
            try:
                account_id = int(account_id) if isinstance(account_id, str) else account_id
            except (ValueError, TypeError):
                logger.error(f"Не удалось преобразовать ID аккаунта в число: {account_id}")
                return None
            
            # Проверяем, существует ли клиент в пуле
            if account_id not in self.clients:
                logger.warning(f"Клиент для аккаунта {account_id} не найден в пуле")
                return None
            
            # Получаем время последнего использования
            last_used = self.last_used.get(account_id)
            
            # Проверяем, что время последнего использования не None
            if last_used is None:
                logger.warning(f"Время последнего использования для аккаунта {account_id} не найдено")
                return None
            
            logger.info(f"Время последнего использования клиента Telegram успешно получено для аккаунта {account_id}: {last_used}")
            return last_used
            
        except Exception as e:
            logger.error(f"Ошибка при получении времени последнего использования клиента Telegram: {str(e)}")
            import traceback
            logger.error(f"Трассировка: {traceback.format_exc()}")
            return None

    def get_client_usage_stats(self, account_id: Union[int, str]) -> Optional[Dict[str, Any]]:
        """
        Получает статистику использования клиента Telegram по ID аккаунта.
        
        Args:
            account_id (Union[int, str]): ID аккаунта
            
        Returns:
            Optional[Dict[str, Any]]: Статистика использования клиента или None, если клиент не найден
        """
        logger.info(f"Получение статистики использования клиента Telegram для аккаунта {account_id}")
        
        try:
            # Преобразуем ID в целое число, если это строка
            try:
                account_id = int(account_id) if isinstance(account_id, str) else account_id
            except (ValueError, TypeError):
                logger.error(f"Не удалось преобразовать ID аккаунта в число: {account_id}")
                return None
            
            # Проверяем, существует ли клиент в пуле
            if account_id not in self.clients:
                logger.warning(f"Клиент для аккаунта {account_id} не найден в пуле")
                return None
            
            # Получаем статистику использования
            usage_count = self.usage_counts.get(account_id)
            last_used = self.last_used.get(account_id)
            auth_status = self.client_auth_status.get(account_id)
            
            # Проверяем, что все данные не None
            if usage_count is None or last_used is None or auth_status is None:
                logger.warning(f"Неполная статистика использования для аккаунта {account_id}")
                return None
            
            # Формируем статистику
            stats = {
                "usage_count": usage_count,
                "last_used": last_used,
                "auth_status": auth_status
            }
            
            logger.info(f"Статистика использования клиента Telegram успешно получена для аккаунта {account_id}: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Ошибка при получении статистики использования клиента Telegram: {str(e)}")
            import traceback
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

# Удаляем глобальные экземпляры пулов 