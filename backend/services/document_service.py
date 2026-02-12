import os
from pypdf import PdfReader
from fastapi import UploadFile
import aiofiles
from pdf2image import convert_from_path
import platform
import logging

# Настройка логгера
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

    async def save_file(self, file: UploadFile) -> str:
        """Сохраняет загруженный файл"""
        file_path = os.path.join(self.UPLOAD_DIR, file.filename)
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
        
        # Шаг 1: Быстрое чтение (текстовый слой)
        try:
            reader = PdfReader(file_path)
            text_pages = []
            for page in reader.pages:
                extracted = page.extract_text()
                if extracted:
                    text_pages.append(extracted)
            full_text = "\n".join(text_pages)
        except Exception as e:
            logger.error(f"PyPDF Error: {e}")

        # Шаг 2: Проверка на скан и OCR
        if len(full_text.strip()) < 50:
            logger.info("Detected scanned document or empty text layer. Attempting OCR...")
            
            if not pytesseract:
                return f"{full_text}\n\n[SYSTEM INFO] Текст не распознан: требуется библиотека Pytesseract."

            try:
                # Попытка проверки Poppler (нужен для pdf2image)
                try:
                    images = convert_from_path(file_path, first_page=1, last_page=5) # Ограничим 5 страницами для скорости
                except Exception as poppler_error:
                    error_str = str(poppler_error).lower()
                    if "poppler" in error_str or "not found" in error_str:
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
                else:
                    full_text += "\n\n[INFO] OCR отработал, но текст не найден (возможно, качество изображения низкое)."

            except Exception as e:
                logger.error(f"OCR Global Error: {e}")
                return f"{full_text}\n\n[OCR ERROR] Не удалось выполнить распознавание: {str(e)}"

        return full_text