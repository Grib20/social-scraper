# test_db.py
import asyncio
# import aiosqlite # Убираем
import asyncpg
import os
from dotenv import load_dotenv

# Загружаем переменные окружения (включая DATABASE_URL)
load_dotenv()

# Возвращаем DATABASE_URL
DATABASE_URL = os.getenv('DATABASE_URL')
# Убираем отдельные переменные
# DB_HOST = os.getenv('DB_HOST')
# DB_PORT = os.getenv('DB_PORT')
# DB_USER = os.getenv('DB_USER')
# DB_PASSWORD = os.getenv('DB_PASSWORD')
# DB_NAME = os.getenv('DB_NAME')

async def test_connection():
    print(f"Пытаемся подключиться к PostgreSQL ({DATABASE_URL})...")
    # Проверяем DATABASE_URL
    if not DATABASE_URL:
        print("Ошибка: DATABASE_URL не найден в переменных окружения.")
        return False
    
    # Убираем проверку отдельных переменных
    # if not all([DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME]):
    #     print("Ошибка: Одна или несколько переменных окружения для БД (DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME) не найдены.")
    #     return False
    conn = None
    try:
        # Используем DATABASE_URL для подключения
        conn = await asyncpg.connect(DATABASE_URL)
        
        print("Соединение с PostgreSQL успешно установлено.")
        # Выполняем простой запрос
        result = await conn.fetchval("SELECT version();") # или SELECT 1
        print(f"Версия PostgreSQL: {result}")
        print("Соединение успешно закрыто.")
        return True
    except Exception as e:
        print(f"Ошибка при подключении к PostgreSQL или выполнении запроса: {e}")
        import traceback
        print(traceback.format_exc())
        return False
    finally:
         if conn:
             await conn.close()

async def main():
    # Логика создания файла SQLite больше не нужна
    # if not os.path.exists(DB_PATH):
    #      print(f"Файл БД {DB_PATH} не найден, создаем пустой файл.")
    #      open(DB_PATH, 'a').close()

    print("Запуск теста подключения к PostgreSQL...")
    success = await test_connection()
    if success:
        print("Тест подключения к PostgreSQL прошел успешно.")
    else:
        print("Тест подключения к PostgreSQL НЕ прошел.")

if __name__ == "__main__":
    asyncio.run(main())