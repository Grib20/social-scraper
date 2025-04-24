import asyncio
import random
from collections import deque

class InstagramAccount:
    def __init__(self, username, password, proxy=None):
        self.username = username
        self.password = password
        self.proxy = proxy
        self.is_active = True
        self.last_used = None
        self.cookies = None
        self.fail_count = 0

class InstagramPool:
    def __init__(self, accounts, min_delay=10, max_delay=30, max_fails=3):
        self.accounts = deque(accounts)
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.max_fails = max_fails
        self.lock = asyncio.Lock()

    async def get_account(self):
        async with self.lock:
            for _ in range(len(self.accounts)):
                acc = self.accounts.popleft()
                if acc.is_active and (acc.fail_count < self.max_fails):
                    self.accounts.append(acc)
                    return acc
                self.accounts.append(acc)
            return None  # Нет активных аккаунтов

    async def mark_used(self, account):
        account.last_used = asyncio.get_event_loop().time()

    async def wait_delay(self):
        delay = random.uniform(self.min_delay, self.max_delay)
        await asyncio.sleep(delay)

    async def mark_fail(self, account):
        account.fail_count += 1
        if account.fail_count >= self.max_fails:
            account.is_active = False

    async def reset_account(self, account):
        account.fail_count = 0
        account.is_active = True 