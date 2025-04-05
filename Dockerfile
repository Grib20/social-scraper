# Используем многоэтапную сборку
FROM python:3.11-slim as builder

# Устанавливаем рабочую директорию для сборки
WORKDIR /build

# Устанавливаем системные зависимости для сборки
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Копируем файлы зависимостей
COPY requirements.txt .

# Устанавливаем зависимости Python в виртуальное окружение
RUN python -m venv /venv
ENV PATH="/venv/bin:$PATH"
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Вторая стадия - финальный образ
FROM python:3.11-slim

# Устанавливаем рабочую директорию
WORKDIR /app

# Копируем виртуальное окружение из стадии сборки
COPY --from=builder /venv /venv
ENV PATH="/venv/bin:$PATH"

# Устанавливаем системные зависимости только необходимые для запуска
RUN apt-get update && apt-get install -y \
    libjpeg-dev \
    zlib1g-dev \
    libpng-dev \
    libmagic1 \
    ffmpeg \
    curl \
    sqlite3 \
    && rm -rf /var/lib/apt/lists/*

# Создаем директории для данных и базу данных
RUN mkdir -p /app/static /app/templates /app/telegram_sessions /app/logs /app/data /app/secrets \
    && touch /app/users.db \
    && chmod 666 /app/users.db

# Создаем непривилегированного пользователя
RUN groupadd -r appuser && useradd -r -g appuser -d /app appuser

# Копируем файлы приложения
COPY . .

# Даем права на запись во все необходимые директории и файлы
RUN chmod 666 /app/users.db && \
    chown -R appuser:appuser /app

# Переключаемся на непривилегированного пользователя
USER appuser

# Устанавливаем переменные окружения
ENV PYTHONUNBUFFERED=1
ENV PORT=3030
ENV LOG_DIR=/app/logs
ENV DB_PATH=/app/users.db

# Открываем порт
EXPOSE 3030

# Запускаем приложение через Healthcheck
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
  CMD curl -f http://localhost:3030/health || exit 1

# Запускаем приложение
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "3030"]
