# Social Media Scraper API

API для сбора данных из социальных сетей (Telegram и VK) с поддержкой ротации аккаунтов и управления через админ-панель.

## Возможности

- Сбор данных из Telegram и VK
- Ротация аккаунтов для обхода ограничений
- Управление аккаунтами через админ-панель
- Поддержка прокси
- Загрузка медиафайлов в S3 (или другое совместимое хранилище)

## Установка

1. Клонируйте репозиторий:
```bash
git clone https://github.com/your-username/social-scraper.git
cd social-scraper
```

2. Создайте виртуальное окружение и активируйте его:
```bash
python -m venv venv
source venv/bin/activate  # для Linux/Mac
venv\Scripts\activate     # для Windows
```

3. Установите зависимости:
```bash
pip install -r requirements.txt
```

4. Создайте файл `.env` и добавьте необходимые переменные окружения:
```env
PORT=3030
ADMIN_KEY=your-secret-admin-key
ENCRYPTION_KEY=your-encryption-key
AWS_ACCESS_KEY_ID=your-aws-access-key
AWS_SECRET_ACCESS_KEY=your-aws-secret-key
AWS_REGION=your-aws-region
S3_BUCKET=your-s3-bucket
DB_ENGINE=your-db-engine
DB_HOST=your-db-host
DB_PORT=your-db-port
DB_USER=your-db-user
DB_PASSWORD=your-db-password
DB_NAME=your-db-name
REDIS_URL=redis://redis:6379/0
```

## Запуск

1. Запустите сервер:
```bash
python app.py
```

2. Откройте админ-панель:
```
http://localhost:3030/admin
```

## API Endpoints

### Регистрация и авторизация

#### POST /register
Регистрация нового пользователя.

**Ответ:**
```json
{
    "api_key": "generated-api-key",
    "message": "Пользователь зарегистрирован"
}
```

### **`POST /find-groups`**

Поиск групп/каналов в указанной социальной сети по ключевым словам.

**Параметры запроса (JSON body):**

- `platform` (str): Платформа для поиска (`"telegram"` или `"vk"`). *Обязательный*.
- `keywords` (List[str]): Список ключевых слов для поиска групп. *Обязательный*.
- `api_key` (str, optional): Ваш API ключ. Можно также передать в заголовках (`api-key`, `x-api-key` или `Authorization: Bearer <ключ>`).
- `min_members` (int, optional): Минимальное количество участников в группе. По умолчанию: `10000` для Telegram, `1000` для VK.
- `max_groups` (int, optional): Максимальное количество групп для возврата. По умолчанию: `20`.

**Пример запроса (Telegram):**
```json
{
    "platform": "telegram",
    "keywords": ["новости", "технологии"],
    "min_members": 5000,
    "max_groups": 15
}
```

**Пример ответа:**
```json
{
    "groups": [
        {
            "id": 123456789,
            "title": "Техно Новости",
            "username": "technews_channel",
            "participants_count": 15000,
            "description": "Самые свежие новости из мира технологий."
            // ... другие поля ...
        },
        // ... другие найденные группы ...
    ],
    "count": 15
}
```

### **`POST /trending-posts`**

Получение популярных постов из указанных групп/каналов за определённый период.

**Параметры запроса (JSON body):**

- `platform` (str): Платформа (`"telegram"` или `"vk"`). *Обязательный*.
- `group_ids` (List[Union[int, str]]): Список ID групп/каналов. ID VK могут быть строками (например, `"-12345"` или `"public12345"`) или числами. ID Telegram должны быть числами. *Обязательный*.
- `days_back` (int, optional): За сколько последних дней искать посты. По умолчанию: `7`.
- `posts_per_group` (int, optional): Максимальное количество постов для возврата из каждой группы. По умолчанию: `10`.
- `min_views` (int, optional): Минимальное количество просмотров у поста. По умолчанию: `0`.
- `api_key` (str, optional): Ваш API ключ (также можно передать в заголовках).

**Пример запроса (Telegram):**
```json
{
    "platform": "telegram",
    "group_ids": [123456789, 987654321],
    "days_back": 3,
    "posts_per_group": 5,
    "min_views": 1000
}
```

**Пример ответа:**
```json
{
    "posts": [
        {
            "id": 55,
            "channel_id": 123456789,
            "date": "2023-10-27T10:30:00+00:00",
            "text": "Текст популярного поста...",
            "views": 1500,
            "media": [
                { "type": "photo", "url": "https://..." }
            ]
            // ... другие поля ...
        },
        // ... другие популярные посты ...
    ],
    "count": 8 // Общее количество найденных постов
}
```

### **`POST /posts-by-period`**

Получение всех постов из указанных групп/каналов за определённый период.

**Параметры запроса (JSON body):**

- `platform` (str): Платформа (`"telegram"` или `"vk"`). *Обязательный*.
- `group_ids` (List[Union[int, str]]): Список ID групп/каналов. ID VK могут быть строками или числами. ID Telegram должны быть числами. *Обязательный*.
- `days_back` (int, optional): За сколько последних дней получать посты. По умолчанию: `7`.
- `max_posts` (int, optional): Максимальное количество постов для возврата из *каждой* группы. По умолчанию: `100`.
- `min_views` (int, optional): Минимальное количество просмотров у поста. По умолчанию: `0`.
- `api_key` (str, optional): Ваш API ключ (также можно передать в заголовках).

**Пример запроса (VK):**
```json
{
    "platform": "vk",
    "group_ids": ["-12345", "public67890"],
    "days_back": 5,
    "max_posts": 50
}
```

**Пример ответа:**
```json
{
    "posts": [
        {
            "id": 101,
            "owner_id": -12345,
            "date": 1666880000, // Unix timestamp
            "text": "Текст поста из VK...",
            "views": { "count": 550 },
            "attachments": [
                { "type": "photo", "photo": { "sizes": [...] } }
            ]
            // ... другие поля ...
        },
        // ... другие посты за период ...
    ]
}
```

### VK API

#### POST /find-groups
Поиск групп в VK.

**Параметры:**
```json
{
    "platform": "vk",
    "keywords": ["keyword1", "keyword2"],
    "min_members": 1000,
    "max_groups": 10
}
```

#### POST /trending-posts
Получение трендовых постов из групп VK.

**Параметры:**
```json
{
    "platform": "vk",
    "group_ids": [123456789],
    "days_back": 7,
    "posts_per_group": 10,
    "min_views": 1000
}
```

#### POST /posts
Получение постов из групп VK по ключевым словам.

**Параметры:**
```json
{
    "platform": "vk",
    "group_ids": [123456789],
    "keywords": ["keyword1", "keyword2"],
    "count": 10,
    "min_views": 1000,
    "days_back": 7
}
```

### Админ-панель API

Все эндпоинты админ-панели требуют заголовок `X-Admin-Key`.

#### GET /admin/stats
Получение статистики системы.

#### GET /admin/users
Получение списка всех пользователей.

#### GET /admin/users/{api_key}
Получение информации о конкретном пользователе.

#### POST /admin/users/{api_key}/telegram-accounts
Добавление аккаунта Telegram.

#### PUT /admin/users/{api_key}/telegram-accounts/{account_id}
Обновление данных аккаунта Telegram.

#### DELETE /admin/users/{api_key}/telegram-accounts/{account_id}
Удаление аккаунта Telegram.

#### POST /admin/users/{api_key}/vk-accounts
Добавление аккаунта VK.

#### PUT /admin/users/{api_key}/vk-accounts/{account_id}
Обновление данных аккаунта VK.

#### DELETE /admin/users/{api_key}/vk-accounts/{account_id}
Удаление аккаунта VK.

## Безопасность

- Все запросы к API должны содержать заголовок `Authorization: Bearer <api_key>`
- Все запросы к админ-панели должны содержать заголовок `X-Admin-Key`
- Чувствительные данные (токены, ключи) шифруются перед сохранением
- Поддержка прокси для обхода блокировок

## Развертывание с Docker

Для простого и быстрого развертывания приложения используйте Docker и docker-compose.

### Подготовка к развертыванию

1.  **Клонируйте репозиторий:**
    ```bash
    git clone https://github.com/your-username/social-scraper.git
    cd social-scraper
    ```

2.  **Создайте файл `.env`:**
    Скопируйте `.env.example` в `.env`:
    ```bash
    cp .env.example .env
    ```
    Отредактируйте `.env`, указав необходимые значения:
    - `PORT`: Порт, на котором будет работать приложение внутри контейнера (например, `3030`).
    - `BASE_URL`: Полный URL, по которому будет доступно API (например, `https://your-domain.com`).
    - `ADMIN_KEY`: Секретный ключ для доступа к административным функциям.
    - `ENCRYPTION_KEY`: Ключ для шифрования чувствительных данных (например, токенов). Сгенерируйте его:
      ```bash
      python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
      ```
    - Настройки S3 (если используете): `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `S3_REGION`, `S3_BUCKET_NAME`, `S3_ENDPOINT_URL`, `S3_LINK_TEMPLATE`.
    - `REDIS_URL`: URL для подключения к Redis (уже настроен в `docker-compose.yml` как `redis://redis:6379/0`).
    - Настройки PostgreSQL: Укажите данные для подключения к вашей базе данных PostgreSQL (`DB_ENGINE`, `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`).

3.  **Создайте директорию `secrets` и файлы с секретами (необходимо для `docker-compose.yml`):**
    Docker Compose будет использовать эти файлы для безопасной передачи секретов в контейнер `app`. Убедитесь, что значения в этих файлах **совпадают** со значениями соответствующих переменных в `.env` файле.
    ```bash
    mkdir -p secrets
    echo "ВАШ_AWS_ACCESS_KEY_ID" > secrets/aws_access_key.txt
    echo "ВАШ_AWS_SECRET_ACCESS_KEY" > secrets/aws_secret_key.txt
    echo "ВАШ_ENCRYPTION_KEY" > secrets/encryption_key.txt
    echo "ВАШ_ADMIN_KEY" > secrets/admin_key.txt
    ```
    **Важно:** Не добавляйте директорию `secrets` в Git! Файл `.dockerignore` уже настроен, чтобы **не** исключать её из Docker-контекста, но `.gitignore` должен её содержать.

### Сборка и запуск

1.  **Запустите приложение:**
    ```bash
    docker-compose up --build -d
    ```
    - `--build`: Пересобрать образ, если были изменения в `Dockerfile` или коде.
    - `-d`: Запустить контейнеры в фоновом режиме.

2.  **Проверка статуса:**
    ```bash
    docker-compose ps
    ```
    Убедитесь, что контейнеры `social-scraper-app` и `social-scraper-redis` запущены (`State: Up`).

3.  **Просмотр логов:**
    ```bash
    docker-compose logs -f app  # Логи приложения
    docker-compose logs -f redis # Логи Redis
    ```

4.  **Остановка приложения:**
    ```bash
    docker-compose down
    ```

### Доступ к API

После успешного запуска API будет доступно по адресу, указанному в `BASE_URL` (если настроен прокси-сервер) или `http://localhost:ВАШ_ПОРТ_НА_ХОСТЕ`, где `ВАШ_ПОРТ_НА_ХОСТЕ` - это порт, указанный слева в `docker-compose.yml` (например, `3030`, если `PORT=3030` в `.env`).

## Разработка

### Структура проекта

## Node.js tools (Puppeteer + browserless)

В проекте есть папка `js_tools` для вспомогательных Node.js-скриптов, которые позволяют работать с browserless и авторизованными прокси через Puppeteer.

### Быстрый старт:

1. Перейдите в папку js_tools:
   ```sh
   cd js_tools
   ```
2. Инициализируйте npm и установите зависимости:
   ```sh
   npm init -y
   npm install puppeteer-core
   ```
3. Запустите скрипт для проверки прокси:
   ```sh
   node puppeteer_proxy_test.js
   ```

### Интеграция с Python

Вы можете вызывать Node.js-скрипты из Python через subprocess:

```python
import subprocess

result = subprocess.run(
    ["node", "js_tools/puppeteer_proxy_test.js"],
    capture_output=True,
    text=True
)
print(result.stdout)
```

### Важно
- Все зависимости Node.js устанавливаются только в папке js_tools.
- Не коммитьте папку js_tools/node_modules в git (она уже в .gitignore).