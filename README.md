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

## Развертывание с Docker

Для простого и быстрого развертывания приложения вы можете использовать Docker и docker-compose.

### Подготовка к развертыванию

1. Клонируйте репозиторий:
```bash
git clone https://github.com/your-username/social-scraper.git
cd social-scraper
```

2. Создайте файл `.env` на основе `.env.example`:
```bash
cp .env.example .env
```

3. Отредактируйте `.env` файл, указав необходимые значения переменных окружения:
```
# Базовые настройки
PORT=3030
BASE_URL=https://ваш-домен.com
ADMIN_KEY=безопасный-ключ-администратора

# Ключ для шифрования данных (сгенерируйте командой ниже)
# python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
ENCRYPTION_KEY=ваш-ключ-шифрования

# Настройки S3
AWS_ACCESS_KEY_ID=ваш-aws-ключ
AWS_SECRET_ACCESS_KEY=ваш-aws-секрет
S3_REGION=регион-s3
S3_BUCKET_NAME=имя-бакета
S3_ENDPOINT_URL=ссылка-на-s3-хранилище
S3_LINK_TEMPLATE=шаблон-ссылки-s3

# Redis настройки
REDIS_URL=redis://redis:6379/0
```

4. Создайте директорию `secrets` и требуемые файлы с секретами:
```bash
mkdir -p secrets
echo "ваш-aws-ключ" > secrets/aws_access_key.txt
echo "ваш-aws-секрет" > secrets/aws_secret_key.txt
echo "ваш-ключ-шифрования" > secrets/encryption_key.txt
echo "безопасный-ключ-администратора" > secrets/admin_key.txt
```

### Сборка и запуск с помощью docker-compose

1. Запустите приложение с помощью docker-compose:
```bash
docker-compose up -d
```

2. Проверьте статус контейнеров:
```bash
docker-compose ps
```

3. Проверьте логи приложения:
```bash
docker-compose logs -f app
```

4. Откройте приложение в браузере:
```
http://localhost:3030
```
или по вашему домену, если он настроен.

### Обновление приложения

1. Получите последние изменения:
```bash
git pull
```

2. Пересоберите и запустите контейнеры:
```bash
docker-compose up -d --build
```

### Создание резервных копий

1. Данные хранятся в томах Docker, а также в файле базы данных `users.db`:
```bash
# Копирование базы данных
cp users.db users.db.backup_$(date +%s)
```

2. Для резервного копирования томов Docker используйте:
```bash
docker run --rm -v social-scraper_telegram_sessions:/source -v $(pwd)/backups:/dest -w /source alpine tar czf /dest/telegram_sessions_$(date +%Y%m%d).tar.gz .
docker run --rm -v social-scraper_data:/source -v $(pwd)/backups:/dest -w /source alpine tar czf /dest/data_$(date +%Y%m%d).tar.gz .
docker run --rm -v social-scraper_logs:/source -v $(pwd)/backups:/dest -w /source alpine tar czf /dest/logs_$(date +%Y%m%d).tar.gz .
docker run --rm -v social-scraper_redis_data:/source -v $(pwd)/backups:/dest -w /source alpine tar czf /dest/redis_data_$(date +%Y%m%d).tar.gz .
```

### Масштабирование и производительность

Для настройки ресурсов, выделяемых контейнерам, отредактируйте секцию `deploy` в файле `docker-compose.yml`:

```yaml
deploy:
  resources:
    limits:
      cpus: '1'
      memory: 2G
    reservations:
      cpus: '0.5'
      memory: 1G
```

## Разработка

### Структура проекта

```