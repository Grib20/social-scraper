import asyncio
from instagram_pool import InstagramPool, InstagramAccount
from instagram_utils import instagram_login_and_check, search_instagram_by_keyword

class InstagramWorker:
    def __init__(self, pool: InstagramPool):
        self.pool = pool

    async def collect_by_keyword(self, keyword):
        acc = await self.pool.get_account()
        if not acc:
            return {"error": "Нет доступных аккаунтов"}
        ok, cookies = await instagram_login_and_check(acc.username, acc.password, acc.proxy)
        if ok:
            result = await search_instagram_by_keyword(keyword, cookies, acc.proxy)
            await self.pool.mark_used(acc)
            await self.pool.wait_delay()
            return {"success": True, "data": result, "account": acc.username}
        else:
            await self.pool.mark_fail(acc)
            return {"success": False, "error": cookies, "account": acc.username} 