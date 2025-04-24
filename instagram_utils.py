import asyncio
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
import random
from instagrapi import Client
import os
from datetime import datetime
import json
from redis_utils import get_redis
import uuid
from user_manager import get_db_connection
import hashlib
try:
    from playwright_stealth import stealth_async
except ImportError:
    stealth_async = None

BROWSER_PROFILES = [
    {
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "headless": False,
        "stealth": True,
        "extra_headers": {"Accept-Language": "en-US,en;q=0.9", "Referer": "https://www.instagram.com/"},
        "viewport": {"width": 1280, "height": 800},
        "locale": "en-US",
    },
    {
        "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15",
        "headless": True,
        "stealth": False,
        "extra_headers": {"Accept-Language": "en-US,en;q=0.8", "Referer": "https://www.instagram.com/"},
        "viewport": {"width": 1440, "height": 900},
        "locale": "en-US",
    },
    {
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:115.0) Gecko/20100101 Firefox/115.0",
        "headless": False,
        "stealth": False,
        "extra_headers": {"Accept-Language": "ru-RU,ru;q=0.9", "Referer": "https://www.instagram.com/"},
        "viewport": {"width": 1366, "height": 768},
        "locale": "ru-RU",
    },
    {
        "user_agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "headless": True,
        "stealth": True,
        "extra_headers": {"Accept-Language": "en-GB,en;q=0.7", "Referer": "https://www.instagram.com/"},
        "viewport": {"width": 1024, "height": 768},
        "locale": "en-GB",
    },
    {
        "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "headless": False,
        "stealth": True,
        "extra_headers": {"Accept-Language": "fr-FR,fr;q=0.8", "Referer": "https://www.instagram.com/"},
        "viewport": {"width": 1280, "height": 720},
        "locale": "fr-FR",
    },
    {
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "headless": True,
        "stealth": False,
        "extra_headers": {"Accept-Language": "de-DE,de;q=0.9", "Referer": "https://www.instagram.com/"},
        "viewport": {"width": 1600, "height": 900},
        "locale": "de-DE",
    },
    {
        "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_2_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "headless": False,
        "stealth": True,
        "extra_headers": {"Accept-Language": "es-ES,es;q=0.8", "Referer": "https://www.instagram.com/"},
        "viewport": {"width": 1536, "height": 864},
        "locale": "es-ES",
    },
    {
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
        "headless": True,
        "stealth": True,
        "extra_headers": {"Accept-Language": "it-IT,it;q=0.8", "Referer": "https://www.instagram.com/"},
        "viewport": {"width": 1280, "height": 1024},
        "locale": "it-IT",
    },
    {
        "user_agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
        "headless": False,
        "stealth": False,
        "extra_headers": {"Accept-Language": "en-US,en;q=0.9", "Referer": "https://www.instagram.com/"},
        "viewport": {"width": 1920, "height": 1080},
        "locale": "en-US",
    },
    {
        "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.2 Safari/605.1.15",
        "headless": True,
        "stealth": True,
        "extra_headers": {"Accept-Language": "en-US,en;q=0.7", "Referer": "https://www.instagram.com/"},
        "viewport": {"width": 1280, "height": 800},
        "locale": "en-US",
    },
    {
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/109.0",
        "headless": False,
        "stealth": False,
        "extra_headers": {"Accept-Language": "pl-PL,pl;q=0.8", "Referer": "https://www.instagram.com/"},
        "viewport": {"width": 1366, "height": 768},
        "locale": "pl-PL",
    },
]

def get_profile_for_account(account_id):
    # Для одного аккаунта всегда один профиль (по хешу)
    idx = int(hashlib.sha256(str(account_id).encode()).hexdigest(), 16) % len(BROWSER_PROFILES)
    return BROWSER_PROFILES[idx]

def parse_cookie_string(cookie_str):
    cookies = []
    for part in cookie_str.split(';'):
        if '=' in part:
            k, v = part.strip().split('=', 1)
            cookies.append({'name': k, 'value': v, 'domain': '.instagram.com', 'path': '/'})
    return cookies

async def human_delay(min_sec=1, max_sec=3):
    await asyncio.sleep(random.uniform(min_sec, max_sec))

async def instagram_login_and_check(username, password, proxy=None, account_id=None):
    async with async_playwright() as p:
        user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
        browser = await p.chromium.launch(headless=True, proxy={"server": proxy} if proxy else None)
        context = await browser.new_context(
            user_agent=user_agent,
            viewport={"width": 1280, "height": 800},
            locale="en-US",
            device_scale_factor=1.0,
            is_mobile=False,
            has_touch=False
        )
        page = await context.new_page()
        console_logs = []
        page.on("console", lambda msg: console_logs.append(f"{msg.type}: {msg.text}"))
        try:
            print(f"[Playwright] Открываю страницу логина Instagram...")
            await page.goto("https://www.instagram.com/accounts/login/", timeout=90000)
            await human_delay(2, 4)
            await page.wait_for_selector("input[name='username']", timeout=90000)
            print(f"[Playwright] Ввожу username...")
            await page.fill("input[name='username']", username)
            await human_delay(1, 2)
            print(f"[Playwright] Ввожу password...")
            await page.fill("input[name='password']", password)
            await human_delay(1, 2)
            print(f"[Playwright] Кликаю submit...")
            await page.click("button[type='submit']")
            await human_delay(3, 6)

            for i in range(5):
                print(f"[Playwright] Попытка {i+1}: жду nav или challenge...")
                try:
                    await page.wait_for_selector("nav", timeout=5000)
                    print(f"[Playwright] Навигация найдена — вход успешен!")
                    cookies = await context.cookies()
                    await browser.close()
                    return True, cookies
                except PlaywrightTimeoutError:
                    print(f"[Playwright] nav не найден, проверяю challenge...")
                    pass
                # --- Challenge: расширенный поиск input для кода ---
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
                            print(f"[Playwright] Найдено поле для кода по селектору: {sel}")
                            break
                    except Exception as e:
                        print(f"[Playwright] Ошибка при поиске по селектору {sel}: {e}")
                # Если не нашли — ищем по label
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
                                    print(f"[Playwright] Найдено поле для кода по label 'Код'/'Code' и id: {input_id}")
                    except Exception as e:
                        print(f"[Playwright] Ошибка при поиске input по label: {e}")
                if code_input:
                    print(f"[Playwright] Challenge: поле для кода найдено, сохраняю challenge...")
                    redis = await get_redis()
                    if not redis:
                        await browser.close()
                        print(f"[Playwright] Redis недоступен для хранения challenge!")
                        return False, "Redis недоступен для хранения состояния challenge."
                    challenge_key = f"playwright_challenge:{account_id or username}"
                    challenge_data = {
                        "cookies": await context.cookies(),
                        "user_agent": user_agent,
                        "proxy": proxy,
                        "url": page.url,
                        "ts": datetime.now().isoformat(),
                    }
                    await redis.set(challenge_key, json.dumps(challenge_data), ex=600)
                    await browser.close()
                    print(f"[Playwright] Challenge сохранён, возвращаю need_code.")
                    return {
                        "need_code": True,
                        "challenge_type": "email_or_sms",
                        "message": "Введите код, отправленный на email/SMS (см. скриншот)",
                        "challenge_context": {"redis_key": challenge_key, "account_id": account_id or username}
                    }
                # Обработка экрана 'Something went wrong'
                try:
                    error_box = await page.query_selector("text=Something went wrong")
                    if error_box:
                        print(f"[Playwright] Найдено сообщение 'Something went wrong' — возможно, прокси заблокирован.")
                        await browser.close()
                        return False, "Instagram: Something went wrong — возможно, IP или прокси заблокирован. Попробуйте другой прокси или аккаунт."
                except Exception as e:
                    print(f"[Playwright] Ошибка при поиске 'Something went wrong': {e}")
                try:
                    save_btn = await page.query_selector("text=Save Info")
                    if save_btn:
                        print(f"[Playwright] Кликаю 'Save Info'...")
                        await save_btn.click()
                        await human_delay(1, 2)
                        continue
                except Exception as e:
                    print(f"[Playwright] Ошибка при поиске 'Save Info': {e}")
                try:
                    not_now_btn = await page.query_selector("text=Not Now")
                    if not_now_btn:
                        print(f"[Playwright] Кликаю 'Not Now'...")
                        await not_now_btn.click()
                        await human_delay(1, 2)
                        continue
                except Exception as e:
                    print(f"[Playwright] Ошибка при поиске 'Not Now': {e}")
                try:
                    notif_btn = await page.query_selector("text=Turn on Notifications")
                    if notif_btn:
                        print(f"[Playwright] Кликаю 'Turn on Notifications'...")
                        not_now_btn2 = await page.query_selector("text=Not Now")
                        if not_now_btn2:
                            await not_now_btn2.click()
                            await human_delay(1, 2)
                            continue
                except Exception as e:
                    print(f"[Playwright] Ошибка при поиске 'Turn on Notifications': {e}")
                try:
                    this_was_me_btn = await page.query_selector("text=This Was Me")
                    if this_was_me_btn:
                        print(f"[Playwright] Кликаю 'This Was Me'...")
                        await this_was_me_btn.click()
                        await human_delay(1, 2)
                        continue
                except Exception as e:
                    print(f"[Playwright] Ошибка при поиске 'This Was Me': {e}")
                try:
                    ok_btn = await page.query_selector("text=OK")
                    if ok_btn:
                        print(f"[Playwright] Кликаю 'OK'...")
                        await ok_btn.click()
                        await human_delay(1, 2)
                        continue
                except Exception as e:
                    print(f"[Playwright] Ошибка при поиске 'OK': {e}")
            # Проверка возврата на форму логина
            try:
                login_input = await page.query_selector("input[name='username']")
                login_btn = await page.query_selector("button[type='submit']")
                if login_input and login_btn:
                    print(f"[Playwright] Возврат на форму логина — возможно, неверный пароль или требуется подтверждение.")
                    await browser.close()
                    return False, "Instagram: Не удалось войти — возможно, неверный логин/пароль или требуется подтверждение."
            except Exception as e:
                print(f"[Playwright] Ошибка при проверке возврата на форму логина: {e}")
            # Если не удалось войти — делаем скриншот и сохраняем логи
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            safe_username = ''.join(c for c in username if c.isalnum() or c in ('-_'))
            screenshot_dir = "logs/screenshots"
            os.makedirs(screenshot_dir, exist_ok=True)
            screenshot_path = os.path.join(screenshot_dir, f"failed_login_{safe_username}_{ts}.png")
            log_path = screenshot_path.replace('.png', '.log')
            await page.screenshot(path=screenshot_path)
            with open(log_path, "w", encoding="utf-8") as f:
                f.write("\n".join(console_logs))
            # --- ДОБАВЛЕНО: если на странице есть поле для кода, возвращаем need_code ---
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
                        print(f"[Playwright] (final) Найдено поле для кода по селектору: {sel}")
                        break
                except Exception as e:
                    print(f"[Playwright] (final) Ошибка при поиске по селектору {sel}: {e}")
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
                                print(f"[Playwright] (final) Найдено поле для кода по label 'Код'/'Code' и id: {input_id}")
                except Exception as e:
                    print(f"[Playwright] (final) Ошибка при поиске input по label: {e}")
            if code_input:
                print(f"[Playwright] (final) Challenge: поле для кода найдено, возвращаю need_code. Скриншот: {screenshot_path}, логи: {log_path}")
                redis = await get_redis()
                if not redis:
                    await browser.close()
                    return False, f"Redis недоступен для хранения состояния challenge. Скриншот: {screenshot_path}, логи: {log_path}"
                challenge_key = f"playwright_challenge:{account_id or username}"
                challenge_data = {
                    "cookies": await context.cookies(),
                    "user_agent": user_agent,
                    "proxy": proxy,
                    "url": page.url,
                    "ts": datetime.now().isoformat(),
                    "screenshot": screenshot_path,
                    "log": log_path
                }
                await redis.set(challenge_key, json.dumps(challenge_data), ex=600)
                await browser.close()
                return {
                    "need_code": True,
                    "challenge_type": "email_or_sms",
                    "message": f"Введите код, отправленный на email/SMS (см. скриншот: {screenshot_path})",
                    "challenge_context": {"redis_key": challenge_key, "account_id": account_id or username, "screenshot": screenshot_path, "log": log_path}
                }
            await browser.close()
            print(f"[Playwright] Не удалось войти, возвращаю ошибку. Скриншот: {screenshot_path}, логи: {log_path}")
            return False, f'Не удалось войти в Instagram: требуется ручное подтверждение или неизвестное окно. Скриншот: {screenshot_path}, логи: {log_path}'
        except PlaywrightTimeoutError:
            # Скриншот и логи при таймауте
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            safe_username = ''.join(c for c in username if c.isalnum() or c in ('-_'))
            screenshot_dir = "logs/screenshots"
            os.makedirs(screenshot_dir, exist_ok=True)
            screenshot_path = os.path.join(screenshot_dir, f"timeout_{safe_username}_{ts}.png")
            log_path = screenshot_path.replace('.png', '.log')
            await page.screenshot(path=screenshot_path)
            with open(log_path, "w", encoding="utf-8") as f:
                f.write("\n".join(console_logs))
            await browser.close()
            return False, f'PlaywrightTimeoutError: Не удалось войти в Instagram (таймаут). Скриншот: {screenshot_path}, логи: {log_path}'
        except Exception as e:
            # Скриншот и логи при любой другой ошибке
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            safe_username = ''.join(c for c in username if c.isalnum() or c in ('-_'))
            screenshot_dir = "logs/screenshots"
            os.makedirs(screenshot_dir, exist_ok=True)
            screenshot_path = os.path.join(screenshot_dir, f"error_{safe_username}_{ts}.png")
            log_path = screenshot_path.replace('.png', '.log')
            await page.screenshot(path=screenshot_path)
            with open(log_path, "w", encoding="utf-8") as f:
                f.write("\n".join(console_logs))
            await browser.close()
            return False, f'{str(e) or "Неизвестная ошибка playwright"}. Скриншот: {screenshot_path}, логи: {log_path}'

def get_instagram_session_path(login):
    session_dir = "sessions/instagram"
    os.makedirs(session_dir, exist_ok=True)
    return os.path.join(session_dir, f"{login}.json")

async def instagram_login_and_check_api(username, password, proxy=None, code=None, challenge_context=None):
    cl = Client()
    session_path = get_instagram_session_path(username)
    # Загружаем сессию, если есть
    if os.path.exists(session_path):
        with open(session_path, "r", encoding="utf-8") as f:
            cl.set_settings(json.load(f))
    if proxy:
        cl.set_proxy(proxy)
    # --- Критично: всегда задаём challenge_code_handler ---
    if code:
        cl.challenge_code_handler = lambda: code
    else:
        cl.challenge_code_handler = lambda: (_ for _ in ()).throw(Exception("challenge_code_required"))
    try:
        if code and challenge_context:
            cl.challenge_context = challenge_context
            cl.complete_challenge()
        else:
            cl.login(username, password)
        # Сохраняем сессию
        with open(session_path, "w", encoding="utf-8") as f:
            json.dump(cl.get_settings(), f)
        # Получаем cookies для playwright-валидации (если нужно)
        cookies = cl.private.cookies.get_dict() if hasattr(cl, 'private') and hasattr(cl.private, 'cookies') else {}
        cookies_list = [
            {'name': k, 'value': v, 'domain': '.instagram.com', 'path': '/'}
            for k, v in cookies.items()
        ]
        # Возвращаем путь к сессии как cookies_list (для обновления в базе)
        return True, session_path
    except Exception as e:
        if str(e) == "challenge_code_required" or 'challenge' in str(e).lower() or '2fa' in str(e).lower():
            return {"need_code": True, "message": str(e), "challenge_context": getattr(cl, 'challenge_context', None)}
        return False, str(e) or 'Неизвестная ошибка instagrapi'

async def search_instagram_by_keyword(keyword, cookies, proxy=None):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, proxy={"server": proxy} if proxy else None)
        context = await browser.new_context()
        await context.add_cookies(cookies)
        page = await context.new_page()
        try:
            await page.goto(f"https://www.instagram.com/explore/tags/{keyword}/", timeout=20000)
            await human_delay(2, 5)
            # Скроллим страницу как человек
            for _ in range(random.randint(2, 5)):
                await page.mouse.wheel(0, random.randint(300, 800))
                await human_delay(1, 2)
            posts = await page.query_selector_all("article a")
            links = [await post.get_attribute("href") for post in posts]
            await browser.close()
            return links
        except Exception as e:
            await browser.close()
            return []

async def check_instagram_cookies_valid(cookies, proxy=None):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, proxy={"server": proxy} if proxy else None)
        context = await browser.new_context()
        await context.add_cookies(cookies)
        page = await context.new_page()
        try:
            await page.goto("https://www.instagram.com/", timeout=20000)
            await asyncio.sleep(2)
            # Проверяем наличие селектора навигации (виден только залогиненным)
            await page.wait_for_selector("nav", timeout=10000)
            await browser.close()
            return True
        except PlaywrightTimeoutError:
            await browser.close()
            return False
        except Exception:
            await browser.close()
            return False

async def save_cookies_to_db(account_id, cookies):
    pool = await get_db_connection()
    if not pool:
        print("Не удалось получить соединение с БД для сохранения cookies")
        return False
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE instagram_accounts SET cookies = $1 WHERE id = $2",
            json.dumps(cookies), account_id
        )
    return True

async def playwright_login_worker(account_id):
    """
    Живой worker для логина Instagram через Playwright с хранением статусов в Redis.
    Работает только по account_id: сам берёт login, password, proxy, cookies из базы.
    Сначала пробует логин через cookies, если невалидно — через пароль.
    После успешного входа сохраняет новые cookies в базу.
    """
    from redis_utils import get_redis
    worker_id = account_id
    redis = await get_redis()
    status_key = f"ig:login_status:{worker_id}"
    code_key = f"ig:code:{worker_id}"
    def now_iso():
        return datetime.now().isoformat(timespec='seconds')
    def status_obj(status, message, step, screenshot=None, log=None):
        s = {"status": status, "message": message, "step": step, "ts": now_iso()}
        if screenshot: s["screenshot"] = screenshot
        if log: s["log"] = log
        return s
    if not redis:
        print(f"[Worker] Redis недоступен, worker завершён с ошибкой")
        return worker_id
    pool = await get_db_connection()
    if not pool:
        await redis.set(status_key, json.dumps(status_obj("error", "Не удалось получить соединение с БД", "error")), ex=900)
        return worker_id
    async with pool.acquire() as conn:
        row = await conn.fetchrow('SELECT login, password, proxy, cookies FROM instagram_accounts WHERE id = $1', account_id)
        if not row:
            await redis.set(status_key, json.dumps(status_obj("error", "Аккаунт не найден в базе", "error")), ex=900)
            return worker_id
        username = row['login']
        password = row['password']
        proxy = row['proxy']
        cookies_db = row['cookies']
        cookies = None
        if cookies_db:
            try:
                cookies = json.loads(cookies_db) if isinstance(cookies_db, str) else cookies_db
            except Exception:
                cookies = None
    try:
        # 1. Пробуем через cookies
        if cookies:
            await redis.set(status_key, json.dumps(status_obj("checking_cookies", "Пробуем войти через cookies...", "cookies")), ex=900)
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True, proxy={"server": proxy} if proxy else None)
                context = await browser.new_context()
                await context.add_cookies(cookies)
                page = await context.new_page()
                await page.goto("https://www.instagram.com/", timeout=20000)
                await human_delay(2, 4)
                try:
                    await page.wait_for_selector("nav", timeout=10000)
                    cookies_new = await context.cookies()
                    await save_cookies_to_db(account_id, cookies_new)
                    await redis.set(status_key, json.dumps(status_obj("success", "Вход через cookies успешен!", "done")), ex=900)
                    await browser.close()
                    return worker_id
                except Exception:
                    # Сохраняем скриншот и лог
                    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                    screenshot_dir = "logs/screenshots"
                    os.makedirs(screenshot_dir, exist_ok=True)
                    screenshot_path = os.path.join(screenshot_dir, f"cookies_fail_{username}_{ts}.png")
                    await page.screenshot(path=screenshot_path)
                    await browser.close()
                    await redis.set(status_key, json.dumps(status_obj("cookies_invalid", "Cookies невалидны, пробуем логин по паролю", "cookies", screenshot=screenshot_path)), ex=900)
        # 2. Логин по паролю
        await redis.set(status_key, json.dumps(status_obj("login_form", "Ожидание формы логина...", "login_form")), ex=900)
        async with async_playwright() as p:
            profile = get_profile_for_account(account_id)
            browser = await p.chromium.launch(headless=profile["headless"], proxy={"server": proxy} if proxy else None)
            context = await browser.new_context(
                user_agent=profile["user_agent"],
                viewport=profile["viewport"],
                locale=profile["locale"]
            )
            if profile.get("extra_headers"):
                await context.set_extra_http_headers(profile["extra_headers"])
            if profile.get("stealth") and stealth_async:
                await stealth_async(context)
            page = await context.new_page()
            await page.goto("https://www.instagram.com/accounts/login/", timeout=90000)
            await human_delay(2, 4)
            console_logs = []
            page.on("console", lambda msg: console_logs.append(f"{msg.type}: {msg.text}"))
            try:
                await page.wait_for_selector("input[name='username']", timeout=90000)
            except Exception as e:
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                screenshot_dir = "logs/screenshots"
                os.makedirs(screenshot_dir, exist_ok=True)
                screenshot_path = os.path.join(screenshot_dir, f"timeout_username_{username}_{ts}.png")
                log_path = screenshot_path.replace('.png', '.log')
                await page.screenshot(path=screenshot_path)
                with open(log_path, "w", encoding="utf-8") as f:
                    f.write("\n".join(console_logs))
                await redis.set(status_key, json.dumps(status_obj(
                    "error",
                    f"Timeout ожидания поля username. Скриншот: {screenshot_path}, логи: {log_path}",
                    "error",
                    screenshot=screenshot_path,
                    log=log_path
                )), ex=900)
                await browser.close()
                return worker_id
            await page.fill("input[name='username']", username)
            await human_delay(1, 2)
            await page.fill("input[name='password']", password)
            await human_delay(1, 2)
            await redis.set(status_key, json.dumps(status_obj("submitting_login", "Отправка логина/пароля...", "submitting_login")), ex=900)
            await page.click("button[type='submit']")
            await human_delay(3, 6)
            for i in range(10):
                try:
                    await page.wait_for_selector("nav", timeout=5000)
                    cookies_new = await context.cookies()
                    await save_cookies_to_db(account_id, cookies_new)
                    await redis.set(status_key, json.dumps(status_obj("success", "Логин успешен!", "done")), ex=900)
                    await browser.close()
                    return worker_id
                except PlaywrightTimeoutError:
                    pass
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
                            break
                    except Exception:
                        pass
                if not code_input:
                    try:
                        label = await page.query_selector("label:text('Код')")
                        if not label:
                            label = await page.query_selector("label:text('Code')")
                        if label:
                            input_id = await label.get_attribute("for")
                            if input_id:
                                code_input = await page.query_selector(f"#{input_id}")
                    except Exception:
                        pass
                if code_input:
                    # Сохраняем скриншот challenge
                    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                    screenshot_dir = "logs/screenshots"
                    os.makedirs(screenshot_dir, exist_ok=True)
                    screenshot_path = os.path.join(screenshot_dir, f"challenge_{username}_{ts}.png")
                    await page.screenshot(path=screenshot_path)
                    await redis.set(status_key, json.dumps(status_obj("waiting_code", "Ожидание кода от пользователя", "challenge", screenshot=screenshot_path)), ex=900)
                    for _ in range(60):
                        code = await redis.get(code_key)
                        if code:
                            code = code.decode() if hasattr(code, 'decode') else code
                            break
                        await asyncio.sleep(5)
                    else:
                        await redis.set(status_key, json.dumps(status_obj("timeout", "Время ожидания кода истекло", "timeout")), ex=900)
                        await browser.close()
                        return worker_id
                    await code_input.fill(code)
                    # Переводим фокус на поле кода (иногда требуется для активации кнопки)
                    await code_input.focus()
                    await human_delay(0.5, 1)
                    # Ждём, пока кнопка станет активной
                    submit_selector = "button[type='submit']"
                    try:
                        await page.wait_for_selector(f"{submit_selector}:not([disabled])", timeout=10000)
                    except Exception:
                        # Если не дождались, пробуем всё равно
                        pass
                    # Несколько попыток нажатия
                    click_success = False
                    click_error = None
                    for attempt in range(3):
                        try:
                            await page.click(submit_selector)
                            click_success = True
                            break
                        except Exception as e:
                            click_error = e
                            await human_delay(1, 2)
                    await human_delay(3, 6)
                    try:
                        await page.wait_for_selector("nav", timeout=10000)
                        cookies_new = await context.cookies()
                        await save_cookies_to_db(account_id, cookies_new)
                        await redis.set(status_key, json.dumps(status_obj("success", "Логин успешен после ввода кода!", "done")), ex=900)
                        await browser.close()
                        return worker_id
                    except Exception as e:
                        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                        screenshot_path = os.path.join(screenshot_dir, f"fail_after_code_{username}_{ts}.png")
                        await page.screenshot(path=screenshot_path)
                        err_msg = f"Не удалось войти после ввода кода: {e}"
                        if not click_success and click_error:
                            err_msg += f" (ошибка клика: {click_error})"
                        await redis.set(status_key, json.dumps(status_obj("error", err_msg, "error", screenshot=screenshot_path)), ex=900)
                        await browser.close()
                        return worker_id
            # Если не удалось войти
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            screenshot_dir = "logs/screenshots"
            os.makedirs(screenshot_dir, exist_ok=True)
            screenshot_path = os.path.join(screenshot_dir, f"fail_login_{username}_{ts}.png")
            await page.screenshot(path=screenshot_path)
            await redis.set(status_key, json.dumps(status_obj("error", "Не удалось войти в Instagram", "error", screenshot=screenshot_path)), ex=900)
            await browser.close()
            return worker_id
    except Exception as e:
        await redis.set(status_key, json.dumps(status_obj("error", str(e), "error")), ex=900)
        return worker_id 