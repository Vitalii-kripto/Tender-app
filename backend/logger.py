import os
import logging
import sys

# Путь к общему лог-файлу
LOG_DIR = os.path.join(os.getcwd(), 'backend', 'logs')
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, 'legal_ai.log')

def setup_unified_logger():
    """
    Настраивает единый логгер для всех сервисов анализа.
    Пишет в legal_ai.log в режиме append.
    """
    logger = logging.getLogger("LegalAI")
    
    # Если логгер уже настроен, не добавляем хендлеры повторно
    if logger.hasHandlers():
        return logger

    logger.setLevel(logging.INFO)
    
    # Формат логов
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Хендлер для файла (режим 'a' - append)
    file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8', mode='a')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Хендлер для консоли
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    
    # Запрещаем передачу логов корневому логгеру, чтобы избежать дублирования
    logger.propagate = False
    
    logger.info("--- [UNIFIED LOGGER INITIALIZED] ---")
    return logger

# Создаем экземпляр логгера для импорта
logger = setup_unified_logger()
