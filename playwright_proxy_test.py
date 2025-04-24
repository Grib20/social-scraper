import asyncio
import json
from playwright.async_api import async_playwright

launch_args = {
    "headless": False,
    "args": [
        "--proxy-server=http://U6rRs37KPoza:RNW78Fm5@pool.proxy.market:10001",
        "--window-size=1280,800",
        "--lang=ru-RU"
    ]
}
BROWSERLESS_WS = (
    "wss://browless.pushrom.ru"
    "?token=VzQrVKTn2Le56bK9iwTaLjIu5zcnGvyw"
    f"&launch={json.dumps(launch_args)}"
)

async def check_ip():
    async with async_playwright() as p:
        browser = await p.chromium.connect_over_cdp(BROWSERLESS_WS)
        context = await browser.new_context()
        page = await context.new_page()
        await page.goto("https://api.ipify.org")
        ip = await page.text_content("body")
        print("Ваш IP через прокси:", ip)
        await browser.close()

if __name__ == "__main__":
    asyncio.run(check_ip())
