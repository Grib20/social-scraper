import asyncio
import redis.asyncio as aioredis

REDIS_URL = 'redis://localhost:6379/0'  # Измените при необходимости

async def clear_tg_group_search_cache():
    redis = await aioredis.from_url(REDIS_URL, decode_responses=True)
    keys = await redis.keys('tg_group_search:*')
    if not keys:
        print('Нет ключей tg_group_search:* для удаления.')
        return
    deleted = await redis.delete(*keys)
    print(f'Удалено {deleted} ключей:')
    for k in keys:
        print(f'  - {k}')
    await redis.close()

if __name__ == '__main__':
    asyncio.run(clear_tg_group_search_cache()) 