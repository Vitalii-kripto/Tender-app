import os
from pypdf import PdfReader
from fastapi import UploadFile
import aiofiles
from pdf2image import convert_from_path
import platform
import logging

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("doc_service_log.txt", encoding='utf-8', mode='w'), # mode='w' перезаписывает файл
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("DocumentService")

# Безопасный импорт pytesseract
try:
    import pytesseract
except ImportError:
    pytesseract = None
    logger.warning("Pytesseract library not installed.")

class DocumentService:
    """
    Сервис для работы с документами.
    Поддерживает извлечение текста из PDF и OCR (распознавание сканов).
    """
    UPLOAD_DIR = "uploaded_docs"

    def __init__(self):
        os.makedirs(self.UPLOAD_DIR, exist_ok=True)
        # Опциональная настройка пути для Windows, если Tesseract установлен в стандартную папку
        if platform.system() == "Windows" and pytesseract:
            tesseract_path = r'C:\Program Files\Tesseract-OCR\tesseract.exe'
            if os.path.exists(tesseract_path):
                pytesseract.pytesseract.tesseract_cmd = tesseract_path
            else:
                logger.warning(f"Tesseract executable not found at: {tesseract_path}")

    async def save_file(self, file: UploadFile) -> str:
        """Сохраняет загруженный файл"""
        file_path = os.path.join(self.UPLOAD_DIR, file.filename)
        logger.info(f"Saving file to: {file_path}")
        async with aiofiles.open(file_path, 'wb') as out_file:
            content = await file.read()
            await out_file.write(content)
        return file_path

    def extract_text_from_pdf(self, file_path: str) -> str:
        """
        Умное извлечение текста с защитой от сбоев.
        1. Сначала пробуем быстрый pypdf (текстовый слой).
        2. Если текста мало (<50 символов), пробуем OCR, но безопасно.
        """
        full_text = ""
        logger.info(f"Extracting text from: {file_path}")
        
        # Шаг 1: Быстрое чтение (текстовый слой)
        try:
            reader = PdfReader(file_path)
            text_pages = []
            for page in reader.pages:
                extracted = page.extract_text()
                if extracted:
                    text_pages.append(extracted)
            full_text = "\n".join(text_pages)
            logger.info(f"PyPDF extracted {len(full_text)} characters.")
        except Exception as e:
            logger.error(f"PyPDF Error: {e}")

        # Шаг 2: Проверка на скан и OCR
        if len(full_text.strip()) < 50:
            logger.info("Detected scanned document or empty text layer. Attempting OCR...")
            
            if not pytesseract:
                logger.warning("OCR requested but Pytesseract is not available.")
                return f"{full_text}\n\n[SYSTEM INFO] Текст не распознан: требуется библиотека Pytesseract."

            try:
                # Попытка проверки Poppler (нужен для pdf2image)
                try:
                    images = convert_from_path(file_path, first_page=1, last_page=5) # Ограничим 5 страницами для скорости
                except Exception as poppler_error:
                    error_str = str(poppler_error).lower()
                    if "poppler" in error_str or "not found" in error_str:
                         logger.error("Poppler not found.")
                         return f"{full_text}\n\n[SYSTEM INFO] Для распознавания сканов установите Poppler (и добавьте в PATH)."
                    raise poppler_error

                ocr_text = []
                for i, image in enumerate(images):
                    # Пытаемся распознать
                    try:
                        text = pytesseract.image_to_string(image, lang='rus+eng')
                        ocr_text.append(f"--- Page {i+1} ---\n{text}")
                    except Exception as tess_err:
                        logger.error(f"Tesseract Error on page {i}: {tess_err}")
                        if "tesseract is not installed" in str(tess_err).lower():
                             return f"{full_text}\n\n[SYSTEM INFO] Tesseract OCR не найден. Установите его для распознавания сканов."
                
                if ocr_text:
                    full_text = "\n".join(ocr_text)
                    logger.info("OCR successful.")
                else:
                    logger.warning("OCR ran but found no text.")
                    full_text += "\n\n[INFO] OCR отработал, но текст не найден (возможно, качество изображения низкое)."

            except Exception as e:
                logger.error(f"OCR Global Error: {e}", exc_info=True)
                return f"{full_text}\n\n[OCR ERROR] Не удалось выполнить распознавание: {str(e)}"

        return full_text
