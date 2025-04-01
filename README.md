# Social Media Scraper API

API для сбора данных из социальных сетей (Telegram и VK) с поддержкой ротации аккаунтов и управления через админ-панель.

## Возможности

- Сбор данных из Telegram и VK
- Ротация аккаунтов для обхода ограничений
- Управление аккаунтами через админ-панель
- Поддержка прокси
- Двухфакторная аутентификация для Telegram
- Отслеживание статистики использования аккаунтов
- Загрузка медиафайлов в S3

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

### Telegram API

#### POST /find-groups
Поиск групп в Telegram.

**Параметры:**
```json
{
    "platform": "telegram",
    "keywords": ["keyword1", "keyword2"],
    "min_members": 100000,
    "max_groups": 20
}
```

#### POST /trending-posts
Получение трендовых постов из групп.

**Параметры:**
```json
{
    "platform": "telegram",
    "group_ids": [123456789],
    "days_back": 7,
    "posts_per_group": 10,
    "min_views": 1000
}
```

#### POST /posts
Получение постов из групп по ключевым словам.

**Параметры:**
```json
{
    "platform": "telegram",
    "group_ids": [123456789],
    "keywords": ["keyword1", "keyword2"],
    "count": 10,
    "min_views": 1000,
    "days_back": 3
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

## Разработка

### Структура проекта

```
social-scraper/
├── app.py              # Основной файл приложения
├── admin_panel.py      # Функции админ-панели
├── telegram_utils.py   # Утилиты для работы с Telegram
├── vk_utils.py         # Утилиты для работы с VK
├── user_manager.py     # Управление пользователями
├── media_utils.py      # Утилиты для работы с медиафайлами
├── requirements.txt    # Зависимости проекта
├── .env               # Переменные окружения
└── README.md          # Документация
```

### Тестирование

Для тестирования API можно использовать Postman. Коллекция с примерами запросов доступна в файле `social-scraper.postman_collection.json`.

## Лицензия

MIT
