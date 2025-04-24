from fastapi import APIRouter, Request, HTTPException, Body
from typing import List, Optional
import uuid
import asyncpg
import logging
from user_manager import get_db_connection
from instagram_pool import InstagramAccount, InstagramPool
from instagram_worker import InstagramWorker
from instagram_utils import check_instagram_cookies_valid, playwright_login_worker
import ast
import os
import json
from redis_utils import get_redis
import asyncio
from playwright.async_api import async_playwright

instagram_router = APIRouter(prefix="/api/instagram/accounts", tags=["instagram_accounts"])

# --- Вспомогательная функция для валидации админ-ключа ---
async def verify_admin_key_from_request(request: Request) -> bool:
    admin_key = request.headers.get("Authorization")
    if not admin_key or not admin_key.startswith("Bearer "):
        return False
    from admin_panel import verify_admin_key
    return await verify_admin_key(admin_key.split(" ", 1)[1])

# --- Добавить Instagram-аккаунт ---
@instagram_router.post("")
async def add_instagram_account(request: Request, data: dict = Body(...)):
    if not await verify_admin_key_from_request(request):
        raise HTTPException(status_code=401, detail="Неверный админ-ключ")
    login = data.get("login")
    password = data.get("password")
    cookies = data.get("cookies", None)
    proxy = data.get("proxy")
    user_api_key = data.get("user_api_key")
    usage_type = data.get("usage_type", "api")
    if not (login and password and user_api_key):
        raise HTTPException(status_code=400, detail="Не все поля заполнены")
    # TODO: Валидация cookies и прокси (stub)
    account_id = str(uuid.uuid4())
    pool = await get_db_connection()
    async with pool.acquire() as conn:
        await conn.execute('''
            INSERT INTO instagram_accounts (id, user_api_key, login, password, cookies, proxy, status, is_active, usage_type)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ''', account_id, user_api_key, login, password, cookies, proxy, 'active', True, usage_type)
    return {"id": account_id, "message": "Instagram аккаунт добавлен"}

# --- Получить список Instagram-аккаунтов ---
@instagram_router.get("")
async def list_instagram_accounts(request: Request, user_api_key: Optional[str] = None):
    if not await verify_admin_key_from_request(request):
        raise HTTPException(status_code=401, detail="Неверный админ-ключ")
    pool = await get_db_connection()
    async with pool.acquire() as conn:
        if user_api_key:
            rows = await conn.fetch('SELECT * FROM instagram_accounts WHERE user_api_key = $1', user_api_key)
        else:
            rows = await conn.fetch('SELECT * FROM instagram_accounts')
        return [dict(row) for row in rows]

# --- Получить детали аккаунта ---
@instagram_router.get("/{account_id}/details")
async def get_instagram_account_details(request: Request, account_id: str):
    if not await verify_admin_key_from_request(request):
        raise HTTPException(status_code=401, detail="Неверный админ-ключ")
    pool = await get_db_connection()
    async with pool.acquire() as conn:
        row = await conn.fetchrow('SELECT * FROM instagram_accounts WHERE id = $1', account_id)
        if not row:
            raise HTTPException(status_code=404, detail="Аккаунт не найден")
        return dict(row)

# --- Редактировать аккаунт ---
@instagram_router.put("/{account_id}")
async def update_instagram_account(request: Request, account_id: str, data: dict = Body(...)):
    if not await verify_admin_key_from_request(request):
        raise HTTPException(status_code=401, detail="Неверный админ-ключ")
    fields = []
    values = []
    for key in ["login", "password", "cookies", "proxy", "status", "is_active", "usage_type"]:
        if key in data and data[key] not in [None, ""]:
            fields.append(f"{key} = ${len(values)+1}")
            values.append(data[key])
    if not fields:
        raise HTTPException(status_code=400, detail="Нет данных для обновления")
    values.append(account_id)
    pool = await get_db_connection()
    async with pool.acquire() as conn:
        result = await conn.execute(f'''UPDATE instagram_accounts SET {', '.join(fields)} WHERE id = ${len(values)}''', *values)
        if result.startswith('UPDATE'):
            return {"message": "Изменения успешно сохранены."}
        else:
            raise HTTPException(status_code=404, detail="Аккаунт Instagram не найден")

# --- Удалить аккаунт ---
@instagram_router.delete("/{account_id}")
async def delete_instagram_account(request: Request, account_id: str):
    if not await verify_admin_key_from_request(request):
        raise HTTPException(status_code=401, detail="Неверный админ-ключ")
    pool = await get_db_connection()
    async with pool.acquire() as conn:
        result = await conn.execute('DELETE FROM instagram_accounts WHERE id = $1', account_id)
        if result.startswith('DELETE'):
            return {"message": "Аккаунт Instagram удалён"}
        else:
            raise HTTPException(status_code=404, detail="Аккаунт Instagram не найден")

# --- Проверить прокси ---
@instagram_router.post("/{account_id}/check-proxy")
async def check_instagram_proxy(request: Request, account_id: str):
    if not await verify_admin_key_from_request(request):
        raise HTTPException(status_code=401, detail="Неверный админ-ключ")
    pool = await get_db_connection()
    async with pool.acquire() as conn:
        row = await conn.fetchrow('SELECT proxy FROM instagram_accounts WHERE id = $1', account_id)
        if not row:
            raise HTTPException(status_code=404, detail="Аккаунт не найден")
        proxy = row['proxy']
        # TODO: Проверка прокси (stub)
        # Здесь можно реализовать реальную проверку через requests/aiohttp
        if proxy and proxy.startswith(('http://', 'socks5://')):
            return {"valid": True, "message": "Прокси выглядит корректно (проверка-заглушка)"}
        else:
            return {"valid": False, "message": "Прокси не задан или некорректен"}

# --- Проверка валидности аккаунта через Playwright ---
@instagram_router.post("/check-login")
async def check_instagram_login(data: dict = Body(...)):
    account_id = data.get("account_id")
    method = data.get("method", "playwright")
    code = data.get("code")
    challenge_context = data.get("challenge_context")
    if not account_id:
        raise HTTPException(400, detail="account_id обязателен")
    pool = await get_db_connection()
    async with pool.acquire() as conn:
        row = await conn.fetchrow('SELECT login, password, proxy, cookies FROM instagram_accounts WHERE id = $1', account_id)
        if not row:
            raise HTTPException(404, detail="Аккаунт не найден")
        username = row['login']
        password = row['password']
        proxy = row['proxy']
        cookies_db = row['cookies']
        import ast
        cookies = None
        if cookies_db:
            try:
                # Если cookies_db — это путь к session-файлу, а не список cookies
                if isinstance(cookies_db, str) and cookies_db.endswith('.json') and os.path.exists(cookies_db):
                    # Попытка логина через сессию instagrapi
                    from instagram_utils import get_instagram_session_path, Client
                    cl = Client()
                    with open(cookies_db, "r", encoding="utf-8") as f:
                        cl.set_settings(json.load(f))
                    # Проверяем валидность сессии (например, через cl.get_timeline_feed() или другой запрос)
                    try:
                        cl.get_timeline_feed()
                        return {"success": True, "cookies": cookies_db, "usage_type": method, "from_cookies": True}
                    except Exception:
                        pass  # Если сессия невалидна — продолжаем обычную логику
                else:
                    cookies = ast.literal_eval(cookies_db) if isinstance(cookies_db, str) else cookies_db
            except Exception:
                cookies = None
        if cookies and not code:
            from instagram_utils import check_instagram_cookies_valid
            valid = await check_instagram_cookies_valid(cookies, proxy)
            if valid:
                return {"success": True, "cookies": cookies, "usage_type": method, "from_cookies": True}
        if not username or not password:
            raise HTTPException(400, detail="В базе нет логина или пароля для этого аккаунта. Пожалуйста, отредактируйте аккаунт и сохраните пароль.")
        if method == "api":
            from instagram_utils import instagram_login_and_check_api as login_func
            usage_type = "api"
            result = await login_func(username, password, proxy, code, challenge_context)
        else:
            from instagram_utils import instagram_login_and_check as login_func
            usage_type = "playwright"
            result = await login_func(username, password, proxy)
        if isinstance(result, dict) and result.get("need_code"):
            return {"success": False, "need_code": True, "message": result.get("message"), "challenge_context": result.get("challenge_context"), "usage_type": usage_type}
        ok, cookies = result if isinstance(result, (list, tuple)) else (False, str(result))
        if ok:
            await conn.execute(
                "UPDATE instagram_accounts SET cookies = $1, usage_type = $2 WHERE id = $3",
                cookies, usage_type, account_id
            )
            return {"success": True, "cookies": cookies, "usage_type": usage_type, "from_cookies": False}
        else:
            return {"success": False, "error": cookies, "usage_type": usage_type}

# Новый эндпоинт для отправки challenge/2FA кода
@instagram_router.post("/submit-code")
async def submit_instagram_code(data: dict = Body(...)):
    account_id = data.get("account_id")
    code = data.get("code")
    challenge_context = data.get("challenge_context")
    method = data.get("method", "api")
    if not account_id or not code or not challenge_context:
        raise HTTPException(400, detail="account_id, code и challenge_context обязательны")
    pool = await get_db_connection()
    async with pool.acquire() as conn:
        row = await conn.fetchrow('SELECT login, password, proxy FROM instagram_accounts WHERE id = $1', account_id)
        if not row:
            raise HTTPException(404, detail="Аккаунт не найден")
        username = row['login']
        password = row['password']
        proxy = row['proxy']
        from instagram_utils import instagram_login_and_check_api
        result = await instagram_login_and_check_api(username, password, proxy, code, challenge_context)
        if isinstance(result, dict) and result.get("need_code"):
            return {"success": False, "need_code": True, "message": result.get("message"), "challenge_context": result.get("challenge_context"), "usage_type": method}
        ok, cookies = result if isinstance(result, (list, tuple)) else (False, str(result))
        if ok:
            await conn.execute(
                "UPDATE instagram_accounts SET cookies = $1, usage_type = $2 WHERE id = $3",
                str(cookies), method, account_id
            )
            return {"success": True, "cookies": cookies, "usage_type": method, "from_cookies": False}
        else:
            return {"success": False, "error": cookies, "usage_type": method}

# --- Поиск по ключевым словам через пул ---
@instagram_router.post("/search-by-keyword")
async def search_by_keyword(data: dict = Body(...)):
    keyword = data.get("keyword")
    if not keyword:
        raise HTTPException(400, detail="keyword обязателен")
    # Пример инициализации пула (в реальном проекте пул должен быть глобальным!)
    accounts = [InstagramAccount("login1", "pass1"), InstagramAccount("login2", "pass2")]
    pool = InstagramPool(accounts)
    worker = InstagramWorker(pool)
    result = await worker.collect_by_keyword(keyword)
    return result

@instagram_router.post("/submit-playwright-code")
async def submit_playwright_code(data: dict = Body(...)):
    account_id = data.get("account_id")
    code = data.get("code")
    redis_key = data.get("redis_key")
    if not account_id or not code or not redis_key:
        raise HTTPException(400, detail="account_id, code и redis_key обязательны")
    redis = await get_redis()
    if not redis:
        raise HTTPException(500, detail="Redis недоступен")
    import json
    challenge_data_raw = await redis.get(redis_key)
    if not challenge_data_raw:
        raise HTTPException(400, detail="Состояние Playwright-челленджа не найдено или истекло")
    challenge_data = json.loads(challenge_data_raw)
    # Восстанавливаем playwright-сессию и вводим код
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, proxy={"server": challenge_data.get("proxy")} if challenge_data.get("proxy") else None)
            context = await browser.new_context(user_agent=challenge_data.get("user_agent"))
            await context.add_cookies(challenge_data.get("cookies", []))
            page = await context.new_page()
            await page.goto(challenge_data.get("url"), timeout=40000)
            # --- Расширенный поиск input для кода ---
            code_input = None
            selectors = [
                "input[name='code']",
                "input[name='email']",
                "input[aria-label*='Code']",
                "input[aria-label*='Код']",
                "input[autocomplete*='one-time-code']",
                "input[type='text']"
            ]
            for sel in selectors:
                try:
                    code_input = await page.query_selector(sel)
                    if code_input:
                        print(f"[Playwright] submit-code: Найдено поле для кода по селектору: {sel}")
                        break
                except Exception as e:
                    print(f"[Playwright] submit-code: Ошибка при поиске по селектору {sel}: {e}")
            if not code_input:
                try:
                    label = await page.query_selector("label:text('Код')")
                    if not label:
                        label = await page.query_selector("label:text('Code')")
                    if label:
                        input_id = await label.get_attribute("for")
                        if input_id:
                            code_input = await page.query_selector(f"#{input_id}")
                            if code_input:
                                print(f"[Playwright] submit-code: Найдено поле для кода по label 'Код'/'Code' и id: {input_id}")
                except Exception as e:
                    print(f"[Playwright] submit-code: Ошибка при поиске input по label: {e}")
            if not code_input:
                await browser.close()
                return {"success": False, "error": "Не найдено поле для ввода challenge-кода на странице Instagram"}
            await code_input.fill(code)
            await page.click("button[type='submit']")
            await asyncio.sleep(3)
            # Проверяем успешный вход
            try:
                await page.wait_for_selector("nav", timeout=10000)
                cookies = await context.cookies()
                await browser.close()
                # Можно сохранить cookies/session в базу, если нужно
                return {"success": True, "cookies": cookies, "message": "Логин завершён через Playwright"}
            except Exception:
                await browser.close()
                return {"success": False, "error": "Не удалось завершить логин после ввода кода"}
    except Exception as e:
        return {"success": False, "error": str(e) or "Ошибка playwright при вводе кода"}

@instagram_router.post("/login-worker")
async def start_instagram_login_worker(data: dict = Body(...)):
    """
    Запуск живого worker'а логина Instagram. Возвращает worker_id.
    """
    account_id = data.get("account_id")
    proxy = data.get("proxy")
    if not account_id:
        raise HTTPException(400, detail="account_id обязателен")
    pool = await get_db_connection()
    async with pool.acquire() as conn:
        row = await conn.fetchrow('SELECT login, password, proxy FROM instagram_accounts WHERE id = $1', account_id)
        if not row:
            raise HTTPException(404, detail="Аккаунт не найден")
        username = row['login']
        password = row['password']
        if not username or not password:
            raise HTTPException(400, detail="В базе нет логина или пароля для этого аккаунта. Пожалуйста, отредактируйте аккаунт и сохраните пароль.")
        # Если proxy не передан явно, берём из базы
        if not proxy:
            proxy = row['proxy']
    # Запускаем worker (fire-and-forget)
    asyncio.create_task(playwright_login_worker(account_id))
    # Для MVP возвращаем account_id как worker_id
    return {"success": True, "worker_id": account_id}

@instagram_router.get("/login-status")
async def get_instagram_login_status(worker_id: str):
    """
    Получить статус логина Instagram по worker_id.
    """
    redis = await get_redis()
    if not redis:
        raise HTTPException(500, detail="Redis недоступен")
    status_key = f"ig:login_status:{worker_id}"
    status_raw = await redis.get(status_key)
    if not status_raw:
        return {"status": "not_found", "message": "Статус не найден или истёк"}
    try:
        status = json.loads(status_raw)
    except Exception:
        status = {"status": "error", "message": "Ошибка декодирования статуса"}
    return status

@instagram_router.post("/submit-worker-code")
async def submit_instagram_worker_code(data: dict = Body(...)):
    """
    Передать challenge-код worker'у по worker_id.
    """
    worker_id = data.get("worker_id")
    code = data.get("code")
    if not worker_id or not code:
        raise HTTPException(400, detail="worker_id и code обязательны")
    redis = await get_redis()
    if not redis:
        raise HTTPException(500, detail="Redis недоступен")
    code_key = f"ig:code:{worker_id}"
    await redis.set(code_key, code, ex=600)
    return {"success": True, "message": "Код передан worker'у"} 