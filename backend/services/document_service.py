import os
from pypdf import PdfReader
from fastapi import UploadFile
import aiofiles
from pdf2image import convert_from_path
import platform
import logging

# --- LOGGING SETUP ---
logger = logging.getLogger("DocumentService")
logger.setLevel(logging.INFO)

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

    def _convert_doc_to_docx(self, doc_path: str) -> str:
        """
        Пытается конвертировать .doc в .docx.
        Возвращает путь к новому файлу или оригинальный путь, если не удалось.
        """
        docx_path = doc_path + "x"
        if os.path.exists(docx_path):
            logger.info(f"DOCX version already exists: {docx_path}")
            return docx_path

        # 1. Попытка через win32com (только Windows + Word)
        if platform.system() == "Windows":
            try:
                import win32com.client as win32
                import pythoncom
                # Инициализация COM для текущего потока (важно для FastAPI/async)
                pythoncom.CoInitialize()
                
                word = win32.gencache.EnsureDispatch('Word.Application')
                word.Visible = False
                doc = word.Documents.Open(os.path.abspath(doc_path))
                # 16 = wdFormatXMLDocument (.docx)
                doc.SaveAs2(os.path.abspath(docx_path), FileFormat=16)
                doc.Close()
                word.Quit()
                logger.info(f"Successfully converted {doc_path} to {docx_path} using MS Word.")
                return docx_path
            except Exception as e:
                logger.warning(f"win32com conversion failed (check if Word is installed): {e}")
            finally:
                try:
                    pythoncom.CoUninitialize()
                except:
                    pass

        # 2. Попытка через извлечение текста и создание нового .docx (fallback)
        text = self._extract_text_from_doc(doc_path)
        if text and not text.startswith("[ОШИБКА"):
            try:
                import docx
                new_doc = docx.Document()
                new_doc.add_paragraph(f"--- АВТОМАТИЧЕСКАЯ КОНВЕРТАЦИЯ ИЗ .DOC ---\nОригинал: {os.path.basename(doc_path)}\n\n")
                new_doc.add_paragraph(text)
                new_doc.save(docx_path)
                logger.info(f"Created {docx_path} from extracted text of {doc_path}")
                return docx_path
            except Exception as e:
                logger.error(f"Failed to create docx from text: {e}")

        return doc_path

    def extract_text(self, file_path: str) -> str:
        """
        Умное извлечение текста с защитой от сбоев.
        Поддерживает PDF, DOCX, XLSX, XLS, DOC.
        """
        full_text = ""
        logger.info(f"Extracting text from: {file_path}")
        
        ext = os.path.splitext(file_path)[1].lower()

        # Автоматическая конвертация .doc -> .docx
        if ext == '.doc':
            logger.info(f"Legacy .doc detected. Attempting auto-conversion to .docx...")
            new_path = self._convert_doc_to_docx(file_path)
            if new_path.endswith('.docx'):
                file_path = new_path
                ext = '.docx'
                logger.info(f"Switched to converted file: {file_path}")
            else:
                # Если конвертация не удалась, используем старый метод извлечения напрямую
                return self._extract_text_from_doc(file_path)

        if ext == '.docx':
            try:
                import docx
                doc = docx.Document(file_path)
                full_text = "\n".join([para.text for para in doc.paragraphs])
                logger.info(f"DOCX extracted {len(full_text)} characters.")
                return full_text
            except ImportError:
                logger.warning("python-docx is not installed. Cannot read DOCX.")
                return "[SYSTEM INFO] python-docx не установлен."
            except Exception as e:
                logger.error(f"DOCX Error: {e}")
                return f"[DOCX ERROR] Не удалось прочитать файл: {str(e)}"
        
        elif ext == '.xlsx':
            try:
                import openpyxl
                wb = openpyxl.load_workbook(file_path, data_only=True)
                sheets_text = []
                for sheet in wb.worksheets:
                    sheet_data = []
                    for row in sheet.iter_rows(values_only=True):
                        row_text = "\t".join([str(cell) if cell is not None else "" for cell in row])
                        if row_text.strip():
                            sheet_data.append(row_text)
                    if sheet_data:
                        sheets_text.append(f"--- Sheet: {sheet.title} ---\n" + "\n".join(sheet_data))
                full_text = "\n\n".join(sheets_text)
                logger.info(f"XLSX extracted {len(full_text)} characters.")
                return full_text
            except ImportError:
                logger.warning("openpyxl is not installed. Cannot read XLSX.")
                return "[SYSTEM INFO] openpyxl не установлен."
            except Exception as e:
                logger.error(f"XLSX Error: {e}")
                return f"[XLSX ERROR] Не удалось прочитать файл: {str(e)}"
        
        elif ext == '.xls':
            try:
                import xlrd
                wb = xlrd.open_workbook(file_path)
                sheets_text = []
                for sheet in wb.sheets():
                    sheet_data = []
                    for row_idx in range(sheet.nrows):
                        row = sheet.row_values(row_idx)
                        row_text = "\t".join([str(cell) if cell is not None else "" for cell in row])
                        if row_text.strip():
                            sheet_data.append(row_text)
                    if sheet_data:
                        sheets_text.append(f"--- Sheet: {sheet.name} ---\n" + "\n".join(sheet_data))
                full_text = "\n\n".join(sheets_text)
                logger.info(f"XLS extracted {len(full_text)} characters.")
                return full_text
            except ImportError:
                logger.warning("xlrd is not installed. Cannot read XLS.")
                return "[SYSTEM INFO] xlrd не установлен."
            except Exception as e:
                logger.error(f"XLS Error: {e}")
                return f"[XLS ERROR] Не удалось прочитать файл: {str(e)}"
        
        elif ext == '.doc':
            logger.info(f"Legacy .doc format detected: {file_path}. Attempting extraction...")
            return self._extract_text_from_doc(file_path)

        elif ext != '.pdf':
            logger.warning(f"Unsupported file format: {ext}. Skipping.")
            return f"[SYSTEM INFO] Формат {ext} не поддерживается."

        # Шаг 1: Быстрое чтение (текстовый слой) для PDF
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
                         msg = "Для распознавания сканов (OCR) необходимо установить Poppler."
                         if platform.system() == "Windows":
                             msg += " Скачайте Poppler для Windows (например, с GitHub @oschwartz10612), распакуйте и добавьте папку 'bin' в системную переменную PATH."
                         return f"{full_text}\n\n[SYSTEM INFO] {msg}"
                    raise poppler_error

                ocr_text = []
                for i, image in enumerate(images):
                    # Пытаемся распознать
                    try:
                        text = pytesseract.image_to_string(image, lang='rus+eng')
                        ocr_text.append(f"--- Page {i+1} ---\n{text}")
                    except Exception as tess_err:
                        logger.error(f"Tesseract Error on page {i}: {tess_err}")
                        if "tesseract is not installed" in str(tess_err).lower() or "not found" in str(tess_err).lower():
                             msg = "Tesseract OCR не найден."
                             if platform.system() == "Windows":
                                 msg += " Установите Tesseract OCR (например, от UB Mannheim) и убедитесь, что он находится в C:\\Program Files\\Tesseract-OCR или добавлен в PATH."
                             return f"{full_text}\n\n[SYSTEM INFO] {msg}"
                
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

    def _extract_text_from_doc(self, file_path: str) -> str:
        """
        Извлечение текста из старого формата .doc.
        Использует striprtf (если это RTF) или системную команду antiword.
        """
        # 1. Попытка через striprtf (некоторые .doc - это на самом деле RTF)
        try:
            from striprtf.striprtf import rtf_to_text
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                if "{\\rtf" in content:
                    text = rtf_to_text(content)
                    if text.strip():
                        logger.info(f"Striprtf extracted {len(text)} characters from .doc (RTF)")
                        return text
        except Exception as e:
            logger.warning(f"Striprtf failed for .doc: {e}")

        # 2. Попытка через системную команду antiword
        import shutil
        import subprocess
        
        antiword_cmd = shutil.which('antiword')
        if antiword_cmd:
            try:
                result = subprocess.run([antiword_cmd, file_path], capture_output=True, text=True, errors='ignore')
                if result.returncode == 0 and result.stdout.strip():
                    logger.info(f"Antiword extracted {len(result.stdout)} characters from .doc")
                    return result.stdout
            except Exception as e:
                logger.warning(f"Antiword execution failed: {e}")
        else:
            logger.warning("Antiword executable not found in PATH.")

        # 3. Последняя попытка: чтение как простого текста (иногда помогает для очень старых или поврежденных файлов)
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                raw_content = f.read()
                # Пытаемся найти хоть какой-то осмысленный текст среди бинарных данных
                import re
                clean_text = re.sub(r'[^\x20-\x7E\u0400-\u04FF\n\t]', ' ', raw_content)
                clean_text = re.sub(r'\s+', ' ', clean_text).strip()
                if len(clean_text) > 100:
                    logger.info("Extracted partial text from .doc using raw fallback.")
                    return f"[ВНИМАНИЕ: Текст извлечен частично]\n\n{clean_text}"
        except:
            pass

        msg = f"Не удалось прочитать файл {os.path.basename(file_path)}."
        if platform.system() == "Windows":
            msg += "\n\nДЛЯ ИСПРАВЛЕНИЯ:\n1. Установите утилиту Antiword и добавьте её в PATH.\n2. ИЛИ (проще) пересохраните файл в формате .docx."
        else:
            msg += "\n\nУстановите пакет antiword (sudo apt install antiword)."
            
        return f"[ОШИБКА ФОРМАТА] {msg}"
