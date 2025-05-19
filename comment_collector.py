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

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost")
redis_conn = redis.from_url(REDIS_URL)

VK_LINK_RE = re.compile(r"vk\.com/wall(-?\d+)_([0-9]+)")

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

async def process_task(task):
    platform = task.get("platform")
    max_comments = task.get("max_comments_per_post") or 100
    api_key = task.get("api_key")
    post_links = task.get("post_links", [])
    results = []

    if platform == "telegram":
        account_id = str(task["account_id"]) if "account_id" in task and task["account_id"] is not None else None
        print(f"[process_task] Telegram: аккаунт {account_id}, постов: {len(post_links)}")
        client = telegram_pool.get_client(account_id) if account_id else None
        if not client and account_id:
            account = telegram_pool.active_accounts.get(account_id)
            if not account and api_key:
                try:
                    from user_manager import get_active_accounts
                    active_accounts = await get_active_accounts(api_key, "telegram")
                    account = next((a for a in active_accounts if str(a['id']) == account_id), None)
                except Exception as e:
                    print(f"[process_task] Ошибка при получении данных аккаунта {account_id}: {e}")
                    return {"error": f"Ошибка при получении данных аккаунта {account_id}: {e}"}
            if not account:
                print(f"[process_task] Не найден аккаунт {account_id} (api_key={api_key})")
                return {"error": f"Клиент и данные аккаунта {account_id} не найдены (api_key={api_key})"}
            try:
                client = await telegram_pool.create_client(account)
                if client:
                    telegram_pool.add_client(account_id, client)
                    print(f"[process_task] Клиент для аккаунта {account_id} успешно создан и добавлен в пул.")
                else:
                    print(f"[process_task] Не удалось создать клиента для аккаунта {account_id}")
                    return {"error": f"Не удалось создать клиента для аккаунта {account_id}"}
            except Exception as e:
                print(f"[process_task] Ошибка при создании клиента для аккаунта {account_id}: {e}")
                return {"error": f"Ошибка при создании клиента для аккаунта {account_id}: {e}"}

        wrapper = TelegramClientWrapper(client, account_id, api_key) if client and account_id else None
        for link in post_links:
            result = {"original_link": link}
            m = re.match(r"https://t.me/([^/]+)/([0-9]+)", link)
            if not m or not wrapper or not client:
                result["status"] = "parse_error"
                results.append(result)
                continue
            username, post_id = m.group(1), int(m.group(2))
            try:
                if not client.is_connected():
                    await client.connect()
                if not await client.is_user_authorized():
                    result["status"] = "error"
                    result["error"] = "Клиент Telegram не авторизован"
                    results.append(result)
                    continue
                entity = await wrapper.make_high_level_request(wrapper.client.get_entity, username)
                msg = await wrapper.make_high_level_request(wrapper.client.get_messages, entity, ids=post_id)
                if not msg:
                    result["status"] = "not_found"
                    results.append(result)
                    continue
                comments, err = await fetch_comments_telegram(wrapper, entity, post_id, max_comments)
                if err == "no_comments":
                    result["status"] = "no_comments"
                    result["comments"] = []
                elif err:
                    result["status"] = "error"
                    result["error"] = err
                else:
                    result["status"] = "ok"
                    result["comments"] = comments
            except (ChannelPrivateError, UsernameNotOccupiedError, MessageIdInvalidError) as e:
                result["status"] = "access_denied"
            except Exception as e:
                result["status"] = "error"
                result["error"] = str(e)
            results.append(result)

    elif platform == "vk":
        client, account_id = await vk_pool.select_next_client(api_key)
        account_id = str(account_id) if account_id is not None else None
        if not client or not account_id:
            return {"error": "Нет доступного VK аккаунта"}
        for link in post_links:
            result = {"original_link": link}
            m = VK_LINK_RE.search(link)
            if not m:
                result["status"] = "parse_error"
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
                else:
                    result["status"] = "ok"
                    result["comments"] = comments
            except Exception as e:
                result["status"] = "error"
                result["error"] = str(e)
            results.append(result)
    else:
        return {"error": "Unknown platform"}

    return results

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

async def worker():
    while True:
        try:
            task_data = await redis_conn.blpop(["comment_tasks"], timeout=5) # type: ignore
            if not task_data:
                await asyncio.sleep(1)
                continue
            _, task_bytes = task_data
            task_json = task_bytes.decode() if isinstance(task_bytes, bytes) else task_bytes
            task = json.loads(task_json)
            task_id = task.get("task_id")
            print(f"Processing task {task_id} for platform {task.get('platform')}")
            result = await process_task(task)
            await redis_conn.set(f"comment_task_result:{task_id}", json.dumps(result))
            await redis_conn.set(f"comment_task_status:{task_id}", json.dumps({"status": "done"}))
            if task.get("callback_url"):
                await send_callback(task["callback_url"], result)
        except Exception as e:
            print(f"Worker error: {e}")
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(worker()) 