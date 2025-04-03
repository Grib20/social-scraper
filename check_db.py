import sqlite3

def check_db():
    print("Подключаемся к базе данных...")
    conn = sqlite3.connect('users.db')
    cursor = conn.cursor()
    
    print("\nСписок таблиц в базе данных:")
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    for table in tables:
        print(f"- {table[0]}")
    
    print("\nСтруктура таблицы users:")
    cursor.execute("PRAGMA table_info(users)")
    columns = cursor.fetchall()
    for col in columns:
        print(f"- {col[1]} ({col[2]})")
    
    print("\nСтруктура таблицы telegram_accounts:")
    cursor.execute("PRAGMA table_info(telegram_accounts)")
    columns = cursor.fetchall()
    for col in columns:
        print(f"- {col[1]} ({col[2]})")
    
    print("\nСтруктура таблицы vk_accounts:")
    cursor.execute("PRAGMA table_info(vk_accounts)")
    columns = cursor.fetchall()
    for col in columns:
        print(f"- {col[1]} ({col[2]})")
    
    conn.close()

if __name__ == "__main__":
    check_db() 