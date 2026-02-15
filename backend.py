import uvicorn
import logging
import os

# --- LOGGING SETUP ---
# Логгер для запускающего скрипта
log_file = "backend_runner_log.txt"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8', mode='w'), # mode='w' перезаписывает файл
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("BackendRunner")

if __name__ == "__main__":
    logger.info("==========================================")
    logger.info("INITIATING LOCAL SERVER LAUNCH")
    logger.info("==========================================")
    try:
        # Runs the FastAPI application located in backend/main.py
        # Reload=True enables auto-reload on code changes for development
        logger.info("Starting Uvicorn process...")
        uvicorn.run("backend.main:app", host="0.0.0.0", port=8000, reload=True)
    except Exception as e:
        logger.critical(f"CRITICAL ERROR during server startup: {e}", exc_info=True)
