# Используем официальный образ Python
FROM python:3.11-slim

# Устанавливаем рабочую директорию
WORKDIR /app

# Устанавливаем системные зависимости
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    libjpeg-dev \
    zlib1g-dev \
    libpng-dev \
    libmagic1 \
    && rm -rf /var/lib/apt/lists/*

# Копируем файлы зависимостей
COPY requirements.txt .

# Устанавливаем зависимости Python
RUN pip install --no-cache-dir -r requirements.txt

# Создаем директории для статических файлов и шаблонов
RUN mkdir -p /app/static /app/templates

# Копируем файлы приложения
COPY . .

# Создаем директорию для сессий Telegram
RUN mkdir -p telegram_sessions

# Создаем директорию для логов
RUN mkdir -p logs

# Устанавливаем переменные окружения
ENV PYTHONUNBUFFERED=1
ENV PORT=3030

# Открываем порт
EXPOSE 3030

# Запускаем приложение
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "3030"]
