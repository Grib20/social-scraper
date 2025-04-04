import asyncio
import logging
import time
from typing import Dict, Any, List

# Настройка логирования
logger = logging.getLogger(__name__)

# Время неактивности для сброса счетчика запросов (в секундах)
RESET_TIMEOUT = 3600  # 1 час

# Лимит запросов, после которого активируется режим пониженной производительности
REQUEST_LIMIT = 1000

class AccountStatsManager:
    """
    Менеджер статистики аккаунтов для управления лимитами запросов 
    и режимом пониженной производительности.
    """
    
    def __init__(self, vk_pool=None, telegram_pool=None):
        """
        Инициализация менеджера статистики аккаунтов.
        
        Args:
            vk_pool: Пул клиентов VK
            telegram_pool: Пул клиентов Telegram
        """
        self.vk_pool = vk_pool
        self.telegram_pool = telegram_pool
        self._task = None
        
    async def start(self):
        """Запускает задачу проверки статистики аккаунтов."""
        if self._task is None:
            logger.info("Запуск задачи проверки статистики аккаунтов")
            self._task = asyncio.create_task(self._check_accounts_task())
        
    async def stop(self):
        """Останавливает задачу проверки статистики аккаунтов."""
        if self._task is not None:
            logger.info("Остановка задачи проверки статистики аккаунтов")
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
    
    async def _check_accounts_task(self):
        """Фоновая задача для проверки и сброса статистики аккаунтов."""
        try:
            while True:
                logger.debug("Запуск проверки статистики аккаунтов")
                if self.vk_pool:
                    await self._process_client_pool(self.vk_pool, "VK")
                
                if self.telegram_pool:
                    await self._process_client_pool(self.telegram_pool, "Telegram")
                
                # Проверка каждые 5 минут
                await asyncio.sleep(300)
        except asyncio.CancelledError:
            logger.info("Задача проверки статистики аккаунтов отменена")
            raise
        except Exception as e:
            logger.error(f"Ошибка в задаче проверки статистики аккаунтов: {e}")
    
    async def _process_client_pool(self, client_pool, pool_name):
        """
        Обрабатывает статистику для клиентского пула.
        
        Args:
            client_pool: Пул клиентов (VK или Telegram)
            pool_name: Название пула для логирования
        """
        try:
            current_time = time.time()
            accounts_to_reset = []
            accounts_to_degrade = []
            
            # Проверяем все аккаунты в пуле
            for account_id, last_used_time in list(client_pool.last_used.items()):
                # Если прошло больше часа с момента последнего использования
                if current_time - last_used_time > RESET_TIMEOUT:
                    accounts_to_reset.append(account_id)
                
                # Проверяем лимит запросов
                if client_pool.usage_counts.get(account_id, 0) > REQUEST_LIMIT:
                    accounts_to_degrade.append(account_id)
            
            # Сбрасываем счетчики для неактивных аккаунтов
            for account_id in accounts_to_reset:
                if account_id in client_pool.usage_counts:
                    prev_count = client_pool.usage_counts[account_id]
                    client_pool.usage_counts[account_id] = 0
                    logger.info(f"Сброс статистики для {pool_name} аккаунта {account_id} после {RESET_TIMEOUT}с простоя (было {prev_count} запросов)")
            
            # Устанавливаем режим пониженной производительности
            for account_id in accounts_to_degrade:
                client = client_pool.get_client(account_id)
                if client and hasattr(client, 'set_degraded_mode'):
                    client.set_degraded_mode(True)
                    logger.warning(f"{pool_name} аккаунт {account_id} переведен в режим пониженной производительности после {client_pool.usage_counts.get(account_id, 0)} запросов")
        
        except Exception as e:
            logger.error(f"Ошибка при обработке статистики {pool_name}: {e}")

# Глобальный экземпляр менеджера статистики аккаунтов
stats_manager = None

def initialize_stats_manager(vk_pool, telegram_pool):
    """
    Инициализирует глобальный менеджер статистики аккаунтов.
    
    Args:
        vk_pool: Пул клиентов VK
        telegram_pool: Пул клиентов Telegram
    
    Returns:
        AccountStatsManager: Инициализированный менеджер статистики
    """
    global stats_manager
    stats_manager = AccountStatsManager(vk_pool, telegram_pool)
    return stats_manager 