import redis
import logging
import time
import random
# import sqlite3 # Убираем неиспользуемый импорт
from datetime import datetime
import os
from dotenv import load_dotenv
import redis.asyncio as aredis
import asyncio
import asyncpg # Добавляем для обработки ошибок
from typing import Optional, Union, Dict, Any, List # Add Optional here if not present, or modify existing import

# Импортируем нужные async функции из user_manager
# get_db_connection теперь возвращает пул
from user_manager import get_db_connection # get_active_accounts # get_active_accounts здесь не используется

load_dotenv()

# Настройка логирования
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s') # <-- УДАЛЕНО
logger = logging.getLogger(__name__)

# Получаем параметры подключения из .env
REDIS_URL = os.getenv('REDIS_URL')
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_DB = int(os.getenv('REDIS_DB', 0))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', None)

# Инициализация клиента Redis
aredis_client = None

def init_redis():
    """Инициализирует подключение к Redis."""
    global aredis_client
    try:
        # Сначала пробуем подключиться через URL, если он задан
        if REDIS_URL:
            logger.info(f"Пробуем подключиться к Redis через URL: {REDIS_URL.split('@')[0]}...")
            aredis_client = aredis.from_url(
                REDIS_URL, 
                decode_responses=True,
                socket_timeout=5,
                socket_connect_timeout=5,
                retry_on_timeout=True
            )
        else:
            # Если URL не задан, используем отдельные параметры
            logger.info(f"Подключение к Redis через параметры: {REDIS_HOST}:{REDIS_PORT}")
            aredis_client = aredis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                db=REDIS_DB,
                password=REDIS_PASSWORD,
                decode_responses=True,  # Автоматически декодируем ответы
                socket_timeout=5,
                socket_connect_timeout=5,
                retry_on_timeout=True
            )
        
        # Проверяем соединение

        if REDIS_URL:
            logger.info(f"Клиент Redis (URL) успешно сконфигурирован.")
        else:
            logger.info(f"Клиент Redis на {REDIS_HOST}:{REDIS_PORT} успешно сконфигурирован.")
        return True # Возвращаем True, если конфигурация прошла успешно
    except Exception as e:
        logger.error(f"Ошибка конфигурации Redis клиента: {e}")
        aredis_client = None
        return False

async def get_redis():
    """Асинхронно возвращает клиент Redis, при необходимости инициализирует и проверяет подключение."""
    global aredis_client
    if aredis_client is None:
        if not init_redis():  # Если инициализация не удалась
            logger.error("Инициализация Redis не удалась. Клиент недоступен.")
            return None

    # Проверяем соединение перед возвратом
    try:
        if aredis_client is None:
            raise aredis.ConnectionError("Redis клиент не инициализирован.")
        # logger.debug("Проверка Redis ping...")
        await aredis_client.ping()  # Проверяем ping асинхронно
        # logger.debug("Redis ping успешен")
        return aredis_client
    except aredis.ConnectionError as e:  # Используем aredis.ConnectionError
        logger.error(f"Ошибка соединения с Redis при проверке ping: {e}. Попытка переподключения...")
        aredis_client = None  # Сбрасываем клиент
        if init_redis():  # Пробуем инициализировать снова
            try:
                if aredis_client is None:
                    raise aredis.ConnectionError("Redis клиент не инициализирован после повторной инициализации.")
                # Здесь проблема: await не нужен для метода ping, так как он не является асинхронным
                aredis_client.ping()  # Проверяем снова
                logger.info("Переподключение к Redis успешно.")
                return aredis_client
            except aredis.ConnectionError as ping_e:
                logger.error(f"Не удалось переподключиться к Redis после ошибки: {ping_e}")
                aredis_client = None  # Все еще может быть None здесь
                return None
            else:
                # Если init_redis() вернул True, но клиент остался None (маловероятно, но для полноты)
                logger.error("Повторная инициализация Redis вернула True, но клиент не установлен.")
                return None  # Возвращаем None
        else:  # init_redis() failed
            logger.error("Повторная инициализация Redis не удалась.")
            return None  # Возвращаем None
    except Exception as e:
        logger.error(f"Неожиданная ошибка при проверке Redis ping: {e}")
        # В этом случае не сбрасываем клиент, т.к. проблема может быть временной
        # Но и не можем гарантировать работоспособность, поэтому лучше вернуть None
        # TODO: Рассмотреть возможность возврата aredis_client и обработки ошибки выше
        return None

async def update_account_usage_redis(api_key, account_id, platform) -> Optional[int]:
    """Обновляет статистику использования аккаунта в Redis и ВОЗВРАЩАЕТ новый счетчик."""
    redis_client = await get_redis()
    if not redis_client:
        logger.warning(f"Redis недоступен, пропускаем обновление статистики для {platform}:{account_id}")
        return None # Возвращаем None при ошибке Redis

    try:
        count_key = f"account:{platform}:{account_id}:requests_count"
        last_used_key = f"account:{platform}:{account_id}:last_used"
        current_time = datetime.now().isoformat()
        
        # Увеличиваем счетчик и получаем новое значение за одну операцию INCR
        new_count = await redis_client.incr(count_key)
        
        # Обновляем время последнего использования и устанавливаем TTL
        async with redis_client.pipeline(transaction=False) as pipe: # Можно без транзакции
            pipe.set(last_used_key, current_time)
            # Обновляем TTL для обоих ключей при каждом запросе
            pipe.expire(count_key, 60*60*24*30) # 30 дней TTL для счетчика
            pipe.expire(last_used_key, 60*60*24*30) # 30 дней TTL для времени
            await pipe.execute()

        logger.debug(f"Статистика Redis обновлена для {platform}:{account_id}. Новый счетчик: {new_count}")

        # Синхронизация с БД остается без изменений (происходит с некоторой вероятностью)
        if random.random() < 0.1:
            await sync_account_stats_to_db(account_id, platform)
        
        return new_count # Возвращаем новое значение счетчика
        
    except aredis.RedisError as e:
        logger.error(f"Ошибка Redis при обновлении статистики ({platform}:{account_id}): {e}")
        return None # Возвращаем None при ошибке Redis
    except Exception as e:
        logger.error(f"Неожиданная ошибка обновления статистики в Redis ({platform}:{account_id}): {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None # Возвращаем None при других ошибках

async def sync_account_stats_to_db(account_id, platform, force=False):
    """Асинхронно синхронизирует статистику из Redis в БД (PostgreSQL)."""
    redis_client = await get_redis()
    if not redis_client:
        logger.warning(f"Redis недоступен, синхронизация статистики ({platform}:{account_id}) невозможна")
        return False

    # conn = None # Больше не нужно инициализировать соединение здесь
    pool = None # Будем использовать пул
    try:
        count_key = f"account:{platform}:{account_id}:requests_count"
        last_used_key = f"account:{platform}:{account_id}:last_used"
        
        # Получаем данные из Redis асинхронно
        count_str, last_used_str = await asyncio.gather(
             redis_client.get(count_key),
             redis_client.get(last_used_key),
             return_exceptions=False # Явно указываем
        )

        count = int(count_str) if count_str else 0
        # Преобразуем last_used_str в datetime UTC, если он есть
        last_used_dt = None
        if last_used_str:
             try:
                 from datetime import timezone # Импортируем timezone
                 # Пытаемся разобрать как ISO формат
                 dt = datetime.fromisoformat(last_used_str)
                 # Если строка содержит информацию о зоне, конвертируем в UTC
                 if dt.tzinfo is not None and dt.tzinfo.utcoffset(dt) is not None:
                     last_used_dt = dt.astimezone(timezone.utc)
                 else:
                     # Если информации о зоне нет, считаем, что это UTC
                     last_used_dt = dt.replace(tzinfo=timezone.utc)
             except ValueError:
                 logger.warning(f"Не удалось преобразовать last_used '{last_used_str}' из Redis в datetime.")

        if count == 0 and last_used_dt is None and not force:
            # logger.debug(f"Нет данных в Redis для аккаунта {platform}:{account_id}, синхронизация не требуется")
            return False # Нет смысла синхронизировать нулевые значения, если не force
        
        # Определяем таблицу в зависимости от платформы
        table = 'telegram_accounts' if platform == 'telegram' else 'vk_accounts'
        
        # Обновляем данные в PostgreSQL асинхронно
        pool = await get_db_connection() # Получаем пул asyncpg
        if not pool:
            logger.error(f"Не удалось получить пул PostgreSQL для синхронизации ({platform}:{account_id})")
            return False
        
        async with pool.acquire() as conn: # Получаем соединение из пула
            # Используем last_used_dt (datetime или None), count (int)
            update_query = f'''
            UPDATE {table} 
            SET requests_count = $1, 
                last_used = $2
            WHERE id = $3
            '''
            # Выполняем execute
            result_str = await conn.execute(update_query, count, last_used_dt, account_id)
            
            # commit() не нужен, execute выполняется вне транзакции или внутри auto-commit
            # await conn.commit() 
            
            # execute возвращает строку типа "UPDATE N"
            rows_affected = int(result_str.split()[1])

            if rows_affected > 0:
                logger.debug(f"Синхронизирована статистика для аккаунта {platform}:{account_id} (Count: {count}, LastUsed: {last_used_dt})")
                return True # Успешная синхронизация
            else:
                # Аккаунт не найден в PostgreSQL, но статистика есть в Redis
                # Логируем это и удаляем "осиротевшие" ключи из Redis
                logger.warning(f"Аккаунт {platform}:{account_id} не найден в БД для синхронизации. "
                               f"Удаляем статистику из Redis (Count: {count}, LastUsed: {last_used_str}).")
                try:
                    # Пытаемся удалить ключи из Redis
                    deleted_count = await redis_client.delete(count_key, last_used_key)
                    if deleted_count > 0:
                         logger.info(f"Статистика из Redis для несуществующего аккаунта {platform}:{account_id} удалена ({deleted_count} ключей).")
                    else:
                         logger.warning(f"Не удалось удалить статистику из Redis для {platform}:{account_id} (ключи не найдены?).")
                except aredis.RedisError as del_err:
                    logger.error(f"Ошибка Redis при удалении статистики для {platform}:{account_id}: {del_err}")
                except Exception as del_exc: # Ловим ЛЮБУЮ другую ошибку при удалении
                     logger.error(f"Неожиданная ошибка при удалении статистики Redis для {platform}:{account_id}", exc_info=True)
                # Возвращаем False, так как синхронизация с БД не удалась
                return False

    except aredis.RedisError as e:
         logger.error(f"Ошибка Redis при синхронизации с БД ({platform}:{account_id}): {e}")
         return False
    except asyncpg.PostgresError as e: # Ловим ошибки PostgreSQL
         logger.error(f"Ошибка PostgreSQL при синхронизации с БД ({platform}:{account_id}): {e}")
         return False
    except ConnectionError as e: # Ошибка получения пула
         logger.error(f"Ошибка соединения PostgreSQL при синхронизации ({platform}:{account_id}): {e}")
         return False
    except Exception as e:
        logger.error(f"Ошибка синхронизации с БД ({platform}:{account_id}): {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False
    # finally:
        # Соединение возвращается в пул автоматически
        # if conn:
        #     await conn.close() 

async def sync_all_accounts_stats():
    """Асинхронно синхронизирует статистику всех аккаунтов из Redis в БД (PostgreSQL)."""
    redis_client = await get_redis()
    if not redis_client:
        logger.warning("Redis недоступен, полная синхронизация статистики невозможна")
        return False
    
    logger.info("Начало полной синхронизации статистики из Redis в БД...")
    success_count = 0
    error_count = 0
    accounts_processed = set()
    cursor = 0  # Для redis scan

    try:
        # Используем SCAN для итерации по ключам
        while True:
            # logger.debug(f"Выполняем SCAN с курсором {cursor}")
            cursor, keys = await redis_client.scan(cursor, match='account:*:*:*', count=500)
            # logger.debug(f"SCAN вернул {len(keys)} ключей. Новый курсор: {cursor}")

            sync_tasks = []  # Собираем задачи для асинхронного выполнения
            keys_in_batch = 0  # Счетчик ключей для логирования

            for key in keys:
                keys_in_batch += 1
                try:
                    parts = key.split(':')
                    if len(parts) >= 4 and parts[0] == 'account' and parts[3] in ('requests_count', 'last_used'):
                        platform = parts[1]
                        account_id = parts[2]
                        account_tuple = (account_id, platform)

                        # Синхронизируем каждый аккаунт только один раз за весь процесс
                        if account_tuple not in accounts_processed:
                            # Добавляем кортеж с данными аккаунта и корутиной в список
                            sync_tasks.append((account_id, platform, sync_account_stats_to_db(account_id, platform)))
                            accounts_processed.add(account_tuple)  # Отмечаем как запланированный к обработке
                        # else: # Отладка
                            # logger.debug(f"Аккаунт {account_tuple} уже обработан, пропуск.")

                except Exception as e:
                    logger.error(f"Ошибка обработки ключа '{key}' при массовой синхронизации: {e}")
                    error_count += 1  # Считаем ошибкой обработки ключа

            # Выполняем собранные задачи синхронизации для текущей пачки ключей
            if sync_tasks:
                # Получаем только корутины для gather
                coroutines = [task[2] for task in sync_tasks]
                # logger.debug(f"Запуск {len(coroutines)} задач синхронизации для пачки ключей...")
                results = await asyncio.gather(*coroutines, return_exceptions=True)

                # Итерируем по задачам и результатам вместе
                for task_info, result in zip(sync_tasks, results):
                    account_id, platform, _ = task_info # Извлекаем ID и платформу
                    if isinstance(result, Exception):
                        # Логируем ошибку с указанием аккаунта и полным трейсбеком
                        logger.error(f"Ошибка синхронизации для аккаунта {platform}:{account_id}", exc_info=result)
                        error_count += 1
                    elif result:  # sync_account_stats_to_db вернул True
                        success_count += 1
                    else:  # sync_account_stats_to_db вернул False
                        # Логируем как предупреждение, если sync_account_stats_to_db вернул False
                        logger.warning(f"Синхронизация для аккаунта {platform}:{account_id} не удалась (вернула False).")
                        error_count += 1
                # logger.debug(f"Задачи синхронизации для пачки завершены.")

            if cursor == 0:
                # logger.debug("SCAN завершен.")
                break  # Выход из цикла while True

        logger.info(f"Полная синхронизация завершена. Всего уникальных аккаунтов обработано: {len(accounts_processed)}. "
                    f"Успешно синхронизировано: {success_count}, Ошибок (включая обработку ключей и синхронизацию): {error_count}")
        return error_count == 0

    except aredis.RedisError as e:
        logger.error(f"Ошибка Redis при массовой синхронизации статистики: {e}")
        return False
    except Exception as e:
        logger.error(f"Ошибка при массовой синхронизации статистики: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


async def get_account_stats_redis(account_id, platform):
    """Получает статистику использования аккаунта из Redis."""
    redis_client = await get_redis()
    if not redis_client:
        logger.warning(f"Redis недоступен, не могу получить статистику для {platform}:{account_id}")
        return None
    
    try:
        # Ключи Redis
        count_key = f"account:{platform}:{account_id}:requests_count"
        last_used_key = f"account:{platform}:{account_id}:last_used"
        
        # Получаем данные из Redis
        count_str, last_used = await asyncio.gather(
             redis_client.get(count_key),
             redis_client.get(last_used_key),
             return_exceptions=False # Явно указываем
        )
        
        return {
            "requests_count": int(count_str) if count_str else 0,
            "last_used": last_used
        }
    except aredis.RedisError as e:
        logger.error(f"Ошибка Redis при получении статистики для {platform}:{account_id}: {e}")
        return None
    except Exception as e:
        logger.error(f"Ошибка получения статистики из Redis для {platform}:{account_id}: {e}")
        return None

async def reset_account_stats_redis(account_id, platform):
    """Асинхронно сбрасывает (удаляет) статистику для аккаунта в Redis."""
    redis_client = await get_redis()
    if not redis_client:
        logger.warning(f"Redis недоступен, сброс статистики ({platform}:{account_id}) невозможен")
        return False # Возвращаем False, если Redis недоступен

    try:
        count_key = f"account:{platform}:{account_id}:requests_count"
        last_used_key = f"account:{platform}:{account_id}:last_used"
        
        # Используем await для delete
        deleted_count = await redis_client.delete(count_key, last_used_key)
        
        if deleted_count > 0:
            logger.info(f"Статистика из Redis для аккаунта {platform}:{account_id} успешно сброшена (удалено {deleted_count} ключей).")
            # Сбросим статистику и в БД (не обязательно, но для консистентности)
            await sync_account_stats_to_db(account_id, platform, force=True) 
            return True
        else:
            # Ключи не найдены, значит, уже сброшено или не было
            logger.info(f"Статистика для аккаунта {platform}:{account_id} в Redis не найдена (уже сброшена?).")
            # Можно дополнительно убедиться, что в БД тоже 0
            await sync_account_stats_to_db(account_id, platform, force=True)
            return True # Считаем успехом, так как ключей нет
            
    except aredis.RedisError as e:
        logger.error(f"Ошибка Redis при сбросе статистики ({platform}:{account_id}): {e}")
        return False # Возвращаем False при ошибке Redis
    except Exception as e:
        logger.error(f"Неожиданная ошибка при сбросе статистики в Redis ({platform}:{account_id}): {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

async def reset_all_account_stats():
    """Асинхронно сбрасывает статистику для ВСЕХ аккаунтов в Redis."""
    redis_client = await get_redis()
    if not redis_client:
        logger.warning("Redis недоступен, сброс статистики невозможен")
        return False

    logger.info("Начало сброса статистики всех аккаунтов в Redis...")
    
    accounts_to_sync = [] # Сохраняем аккаунты для синхронизации с БД
    keys_to_delete = []
    cursor = 0 # Use integer 0 for scan cursor
    
    try:
        # Используем SCAN для итерации по ключам, чтобы не блокировать Redis надолго
        while True:
            cursor, keys = await redis_client.scan(cursor, match='account:*:*:*', count=500) # Ищем все ключи аккаунтов
            keys_to_delete.extend(keys)
            if cursor == 0:
                break
        
        if not keys_to_delete:
            logger.info("Не найдено ключей статистики аккаунтов для сброса.")
            return True # Нет работы - это успех

        # Собираем ID и платформы аккаунтов ДО удаления ключей
        for key in keys_to_delete:
            parts = key.split(':')
            if len(parts) >= 4 and parts[0] == 'account' and parts[3] in ('requests_count', 'last_used'):
                 platform = parts[1]
                 account_id = parts[2]
                 account_info = (account_id, platform)
                 if account_info not in accounts_to_sync:
                     accounts_to_sync.append(account_info)

        # Используем пайплайн для удаления всех найденных ключей
        deleted_count = 0
        if keys_to_delete:
            # logger.debug(f"Начинаем удаление {len(keys_to_delete)} ключей из Redis...")
            # Разделяем удаление на части, чтобы не создавать слишком большой пайплайн
            chunk_size = 500
            for i in range(0, len(keys_to_delete), chunk_size):
                chunk = keys_to_delete[i:i + chunk_size]
                async with redis_client.pipeline(transaction=False) as pipe:
                    for key in chunk:
                        pipe.delete(key)
                    results = await pipe.execute()
                    deleted_count += sum(results) # Считаем успешные удаления
            logger.info(f"Удалено {deleted_count} ключей статистики из Redis.")
        else:
             logger.info("Ключи для удаления в Redis не найдены.")

        # Синхронизируем сброшенные данные с БД
        sync_success_count = 0
        sync_error_count = 0
        if accounts_to_sync:
             logger.info(f"Начало синхронизации сброса для {len(accounts_to_sync)} аккаунтов в БД...")
             sync_tasks = []
             for account_id, platform in accounts_to_sync:
                 # Передаем force=True, чтобы гарантированно обнулить статистику в БД
                 sync_tasks.append(sync_account_stats_to_db(account_id, platform, force=True))

             results = await asyncio.gather(*sync_tasks, return_exceptions=True)

             for result in results:
                 if isinstance(result, Exception):
                     logger.error(f"Ошибка при синхронизации сброса с БД: {result}")
                     sync_error_count += 1
                 elif result: # sync_account_stats_to_db возвращает True при успехе
                     sync_success_count += 1
                 else: # Вернулся False
                      sync_error_count += 1 # Считаем False как ошибку синхронизации

             logger.info(f"Синхронизация сброса с БД завершена. Успешно: {sync_success_count}, Ошибок: {sync_error_count}")
        else:
            logger.info("Нет аккаунтов для синхронизации сброса с БД.")

        return sync_error_count == 0

    except aredis.RedisError as e:
        logger.error(f"Ошибка Redis при сбросе статистики: {e}")
        return False
    except Exception as e:
        logger.error(f"Неожиданная ошибка при сбросе статистики: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

# Запускаем инициализацию при импорте модуля
init_redis() 