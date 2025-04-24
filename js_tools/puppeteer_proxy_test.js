const puppeteer = require('puppeteer-core');

const browserWSEndpoint = "wss://browless.pushrom.ru?token=VzQrVKTn2Le56bK9iwTaLjIu5zcnGvyw&--proxy-server=http://pool.proxy.market:10001";

(async () => {
  const browser = await puppeteer.connect({ browserWSEndpoint });
  const page = await browser.newPage();

  // Авторизация на прокси
  await page.authenticate({
    username: 'U6rRs37KPoza',
    password: 'RNW78Fm5'
  });

  await page.goto('https://api.ipify.org');
  const ip = await page.evaluate(() => document.body.textContent);
  console.log('Ваш IP через прокси:', ip);

  await browser.close();
})(); 