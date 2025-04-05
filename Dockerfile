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
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# Копируем файлы зависимостей
COPY requirements.txt .

# Устанавливаем зависимости Python
RUN pip install --no-cache-dir -r requirements.txt

# Создаем директории для данных
RUN mkdir -p /app/static /app/templates /app/telegram_sessions /app/logs /app/data

# Создаем непривилегированного пользователя
RUN groupadd -r appuser && useradd -r -g appuser -d /app appuser

# Копируем файлы приложения
COPY . .

# Даем права на запись в нужные директории
RUN chown -R appuser:appuser /app/telegram_sessions /app/logs /app/data

# Переключаемся на непривилегированного пользователя
USER appuser

# Устанавливаем переменные окружения
ENV PYTHONUNBUFFERED=1
ENV PORT=3030

# Открываем порт
EXPOSE 3030

# Запускаем приложение
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "3030"]
