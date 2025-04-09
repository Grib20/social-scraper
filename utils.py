import os
import logging

logger = logging.getLogger(__name__)

def read_docker_secret(secret_name: str) -> str:
    """
    Читает секрет из Docker secrets или переменных окружения.
    
    Args:
        secret_name (str): Имя секрета
        
    Returns:
        str: Значение секрета или пустая строка, если секрет не найден
    """
    try:
        with open(f"/run/secrets/{secret_name}", "r") as f:
            return f.read().strip()
    except Exception as e:
        logger.error(f"Ошибка при чтении Docker secret {secret_name}: {e}")
        return os.getenv(secret_name, "") 