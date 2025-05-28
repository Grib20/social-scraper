# Импорт пулов
from pools import telegram_pool, vk_pool
# Импорт VK функций
from vk_utils import fetch_vk_comments, VKClient
# ... остальной импорт ...

# ... существующий код ... 

import os
import json
import asyncio
import redis.asyncio as redis
from telethon.errors import ChannelPrivateError, UsernameNotOccupiedError, MessageIdInvalidError
from telegram_utils import TelegramClientWrapper
import aiohttp
import re
import random

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost")
redis_conn = redis.from_url(REDIS_URL)

VK_LINK_RE = re.compile(r"vk\.com/wall(-?\d+)_([0-9]+)")
MAX_RETRIES = 3
BATCH_SIZE = 5
BUSY_SET = "comment_subtasks:busy_accounts"

def extract_user_id(msg):
    # Универсально для Telethon Message
    if hasattr(msg, "from_id") and msg.from_id is not None:
        if hasattr(msg.from_id, "user_id"):
            return msg.from_id.user_id
        elif isinstance(msg.from_id, int):
            return msg.from_id
    if hasattr(msg, "sender_id") and isinstance(msg.sender_id, int):
        return msg.sender_id
    return None

async def fetch_comments_telegram(wrapper, entity, post_id, max_comments=100):
    comments = []
    try:
        async for msg in wrapper.client.iter_messages(entity, reply_to=post_id, limit=max_comments):
            comments.append({
                "id": msg.id,
                "text": msg.text,
                "date": msg.date.isoformat(),
                "from_id": extract_user_id(msg)
            })
        return comments, None
    except Exception as e:
        error_msg = str(e)
        if "The message ID used in the peer was invalid" in error_msg and "GetRepliesRequest" in error_msg:
            return [], "no_comments"
        return None, error_msg

async def send_callback(callback_url, result):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(callback_url, json=result, timeout=30) as resp:
                status = resp.status
                text = await resp.text()
                print(f"[callback] POST {callback_url} -> {status}, response: {text}")
                return status, text
    except Exception as e:
        print(f"[callback] Ошибка при отправке POST на {callback_url}: {e}")
        return None, str(e)

async def get_next_account(platform, api_key, exclude_account_ids=None):
    exclude_account_ids = exclude_account_ids or []
    if platform == "telegram":
        active_accounts = await telegram_pool.get_active_clients(api_key)
    elif platform == "vk":
        active_accounts = await vk_pool.get_active_clients(api_key)
    else:
        return None
    candidates = [a for a in active_accounts if str(a['id']) not in exclude_account_ids]
    if not candidates:
        return None
    return random.choice(candidates)

async def process_subtask(subtask):
    platform = subtask.get("platform")
    max_comments = subtask.get("max_comments_per_post") or 100
    api_key = subtask.get("api_key")
    post_links = subtask.get("post_links", [])
    account_id = str(subtask["account_id"]) if "account_id" in subtask and subtask["account_id"] is not None else None
    task_id = subtask.get("task_id")
    retry_count = subtask.get("retry_count", 0)
    callback_url = subtask.get("callback_url")
    results = []
    failed_links = []

    if platform == "telegram":
        client = telegram_pool.get_client(account_id) if account_id else None
        if not client and account_id:
            account = telegram_pool.active_accounts.get(account_id)
            if not account and api_key:
                try:
                    from user_manager import get_active_accounts
                    active_accounts = await get_active_accounts(api_key, "telegram")
                    account = next((a for a in active_accounts if str(a['id']) == account_id), None)
                except Exception as e:
                    print(f"[process_subtask] Ошибка при получении данных аккаунта {account_id}: {e}")
                    failed_links = post_links
                    return results, failed_links, f"Ошибка при получении данных аккаунта {account_id}: {e}"
            if not account:
                print(f"[process_subtask] Не найден аккаунт {account_id} (api_key={api_key})")
                failed_links = post_links
                return results, failed_links, f"Клиент и данные аккаунта {account_id} не найдены (api_key={api_key})"
            try:
                client = await telegram_pool.create_client(account)
                if client:
                    telegram_pool.add_client(account_id, client)
                    print(f"[process_subtask] Клиент для аккаунта {account_id} успешно создан и добавлен в пул.")
                else:
                    print(f"[process_subtask] Не удалось создать клиента для аккаунта {account_id}")
                    failed_links = post_links
                    return results, failed_links, f"Не удалось создать клиента для аккаунта {account_id}"
            except Exception as e:
                print(f"[process_subtask] Ошибка при создании клиента для аккаунта {account_id}: {e}")
                failed_links = post_links
                return results, failed_links, f"Ошибка при создании клиента для аккаунта {account_id}: {e}"
        wrapper = TelegramClientWrapper(client, account_id, api_key) if client and account_id else None
        for link in post_links:
            result = {"original_link": link}
            m = re.match(r"https://t.me/([^/]+)/([0-9]+)", link)
            if not m or not wrapper or not client:
                result["status"] = "parse_error"
                failed_links.append(link)
                results.append(result)
                continue
            username, post_id = m.group(1), int(m.group(2))
            try:
                if not client.is_connected():
                    await client.connect()
                if not await client.is_user_authorized():
                    result["status"] = "error"
                    result["error"] = "Клиент Telegram не авторизован"
                    failed_links.append(link)
                    results.append(result)
                    continue
                entity = await wrapper.make_high_level_request(wrapper.client.get_entity, username)
                msg = await wrapper.make_high_level_request(wrapper.client.get_messages, entity, ids=post_id)
                if not msg:
                    result["status"] = "not_found"
                    failed_links.append(link)
                    results.append(result)
                    continue
                comments, err = await fetch_comments_telegram(wrapper, entity, post_id, max_comments)
                if err == "no_comments":
                    result["status"] = "no_comments"
                    result["comments"] = []
                elif err:
                    result["status"] = "error"
                    result["error"] = err
                    failed_links.append(link)
                else:
                    result["status"] = "ok"
                    result["comments"] = comments
            except (ChannelPrivateError, UsernameNotOccupiedError, MessageIdInvalidError) as e:
                result["status"] = "access_denied"
                failed_links.append(link)
            except Exception as e:
                result["status"] = "error"
                result["error"] = str(e)
                failed_links.append(link)
            results.append(result)

    elif platform == "vk":
        client, _account_id = await vk_pool.select_next_client(api_key)
        if not client or not account_id:
            failed_links = post_links
            return results, failed_links, "Нет доступного VK аккаунта"
        for link in post_links:
            result = {"original_link": link}
            m = VK_LINK_RE.search(link)
            if not m:
                result["status"] = "parse_error"
                failed_links.append(link)
                results.append(result)
                continue
            owner_id, post_id = int(m.group(1)), int(m.group(2))
            try:
                comments, err = await fetch_vk_comments(owner_id, post_id, client.access_token, max_comments)
                if err == "no_comments":
                    result["status"] = "no_comments"
                    result["comments"] = []
                elif err:
                    result["status"] = "error"
                    result["error"] = err
                    failed_links.append(link)
                else:
                    result["status"] = "ok"
                    result["comments"] = comments
            except Exception as e:
                result["status"] = "error"
                result["error"] = str(e)
                failed_links.append(link)
            results.append(result)
    else:
        failed_links = post_links
        return results, failed_links, "Unknown platform"

    return results, failed_links, None

async def aggregate_and_callback_if_done(task_id, callback_url):
    pending_raw = await redis_conn.get(f"comments_task:{task_id}:pending")
    pending = int(pending_raw or 0)
    if pending > 0:
        return
    # Собираем все результаты
    all_results_raw = await redis_conn.lrange(f"comments_task:{task_id}:results", 0, -1)
    all_results = [json.loads(x.decode() if isinstance(x, bytes) else x) for x in all_results_raw]
    # Отправляем callback
    if callback_url:
        await send_callback(callback_url, all_results)
    # Сохраняем результат и статус
    await redis_conn.set(f"comment_task_result:{task_id}", json.dumps(all_results))
    await redis_conn.set(f"comment_task_status:{task_id}", json.dumps({"status": "done"}))
    # Чистим ключи
    await redis_conn.delete(f"comments_task:{task_id}:results")
    await redis_conn.delete(f"comments_task:{task_id}:pending")

async def process_and_finalize_subtask(subtask):
    task_id = subtask.get("task_id")
    callback_url = subtask.get("callback_url")
    retry_count = subtask.get("retry_count", 0)
    account_id = str(subtask.get("account_id"))
    platform = subtask.get("platform")
    api_key = subtask.get("api_key")
    # Помечаем аккаунт как занятый
    await redis_conn.sadd(BUSY_SET, account_id)
    try:
        print(f"[worker] Processing subtask for task_id={task_id}, account_id={account_id}, retry={retry_count}")
        results, failed_links, error = await process_subtask(subtask)
        for res in results:
            await redis_conn.rpush(f"comments_task:{task_id}:results", json.dumps(res))
        if failed_links and retry_count < MAX_RETRIES:
            exclude_ids = subtask.get("exclude_account_ids", []) + [account_id]
            next_account = await get_next_account(platform, api_key, exclude_account_ids=exclude_ids)
            if next_account:
                new_subtask = subtask.copy()
                new_subtask["account_id"] = next_account["id"]
                new_subtask["post_links"] = failed_links
                new_subtask["retry_count"] = retry_count + 1
                new_subtask["exclude_account_ids"] = exclude_ids
                await redis_conn.rpush("comment_subtasks", json.dumps(new_subtask))
            else:
                await redis_conn.decr(f"comments_task:{task_id}:pending")
        else:
            await redis_conn.decr(f"comments_task:{task_id}:pending")
        await aggregate_and_callback_if_done(task_id, callback_url)
    finally:
        # Освобождаем аккаунт
        await redis_conn.srem(BUSY_SET, account_id)

async def worker():
    while True:
        try:
            subtasks = []
            # blpop для первой задачи
            task_data = await redis_conn.blpop(["comment_subtasks"], timeout=5)
            if not task_data:
                await asyncio.sleep(1)
                continue
            _, subtask_bytes = task_data
            subtask_json = subtask_bytes.decode() if isinstance(subtask_bytes, bytes) else subtask_bytes
            subtask = json.loads(subtask_json)
            account_id = str(subtask.get("account_id"))
            # Проверяем, не занят ли аккаунт
            is_busy = await redis_conn.sismember(BUSY_SET, account_id)
            if is_busy:
                # Возвращаем задачу в конец очереди
                await redis_conn.rpush("comment_subtasks", json.dumps(subtask))
                await asyncio.sleep(0.1)
                continue
            subtasks.append(subtask)
            # lpop для остальных задач
            for _ in range(BATCH_SIZE - 1):
                more = await redis_conn.lpop("comment_subtasks")
                if not more:
                    break
                more_json = more.decode() if isinstance(more, bytes) else more
                more_subtask = json.loads(more_json)
                more_account_id = str(more_subtask.get("account_id"))
                is_busy = await redis_conn.sismember(BUSY_SET, more_account_id)
                if is_busy:
                    # Возвращаем задачу в конец очереди
                    await redis_conn.rpush("comment_subtasks", json.dumps(more_subtask))
                    continue
                subtasks.append(more_subtask)
            # Запускаем все параллельно
            await asyncio.gather(*(process_and_finalize_subtask(st) for st in subtasks))
        except Exception as e:
            print(f"Worker error: {e}")
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(worker()) 