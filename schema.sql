-- Схема базы данных для PostgreSQL

-- Таблица пользователей
CREATE TABLE IF NOT EXISTS users (
    api_key VARCHAR(36) PRIMARY KEY, 
    username TEXT, 
    password TEXT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, 
    last_used TIMESTAMPTZ, 
    vk_token TEXT 
);

-- Таблица аккаунтов Telegram
CREATE TABLE IF NOT EXISTS telegram_accounts (
    id VARCHAR(36) PRIMARY KEY, 
    user_api_key VARCHAR(36) REFERENCES users(api_key) ON DELETE CASCADE, 
    api_id TEXT, 
    api_hash TEXT,
    phone TEXT, 
    proxy TEXT, 
    status TEXT, 
    session_file TEXT,
    requests_count INTEGER DEFAULT 0, 
    last_request_time DOUBLE PRECISION, -- Время последнего запроса (timestamp float)
    added_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, 
    session_string TEXT, 
    phone_code_hash TEXT,
    is_active BOOLEAN DEFAULT TRUE, 
    request_limit INTEGER DEFAULT 1000,
    last_used TIMESTAMPTZ
);

-- Таблица аккаунтов VK
CREATE TABLE IF NOT EXISTS vk_accounts (
    id VARCHAR(36) PRIMARY KEY, 
    user_api_key VARCHAR(36) REFERENCES users(api_key) ON DELETE CASCADE, 
    token TEXT, -- Зашифрованный токен
    proxy TEXT,
    status TEXT, 
    requests_count INTEGER DEFAULT 0, 
    last_request_time DOUBLE PRECISION, -- Время последнего запроса (timestamp float)
    added_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, 
    user_id BIGINT, -- ID пользователя VK
    user_name TEXT, 
    error_message TEXT,
    error_code INTEGER, 
    last_checked_at TIMESTAMPTZ, 
    is_active BOOLEAN DEFAULT TRUE,
    request_limit INTEGER DEFAULT 1000, 
    last_used TIMESTAMPTZ
);

-- Можно добавить индексы для часто используемых полей, например:
-- CREATE INDEX IF NOT EXISTS idx_telegram_accounts_user_api_key ON telegram_accounts (user_api_key);
-- CREATE INDEX IF NOT EXISTS idx_vk_accounts_user_api_key ON vk_accounts (user_api_key);
-- CREATE INDEX IF NOT EXISTS idx_telegram_accounts_is_active ON telegram_accounts (is_active);
-- CREATE INDEX IF NOT EXISTS idx_vk_accounts_is_active ON vk_accounts (is_active); 