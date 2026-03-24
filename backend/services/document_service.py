import os
import re
import io
import numpy as np
from pypdf import PdfReader
from fastapi import UploadFile
import aiofiles
import platform
import logging

from backend.logger import logger

# Безопасный импорт pypdfium2 и paddleocr
try:
    import pypdfium2 as pdfium
except ImportError:
    pdfium = None
    logger.warning("pypdfium2 library not installed.")

try:
    from paddleocr import PaddleOCR
except ImportError:
    PaddleOCR = None
    logger.warning("paddleocr library not installed.")

class DocumentService:
    """
    Сервис для работы с документами.
    Поддерживает извлечение текста из PDF и OCR (распознавание сканов) через PaddleOCR.
    """
    UPLOAD_DIR = "uploaded_docs"

    def __init__(self):
        os.makedirs(self.UPLOAD_DIR, exist_ok=True)
        self.ocr_engine = None
        if PaddleOCR:
            try:
                # Инициализация PaddleOCR (модели скачиваются при первом запуске)
                self.ocr_engine = PaddleOCR(use_angle_cls=True, lang='ru', show_log=False)
                logger.info("PaddleOCR engine initialized successfully.")
            except Exception as e:
                logger.error(f"Failed to initialize PaddleOCR: {e}")

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
        ext = os.path.splitext(file_path)[1].lower()
        logger.info(f"--- [START EXTRACTION] ---")
        logger.info(f"File path: {file_path}")
        logger.info(f"Extension: {ext}")
        
        full_text = ""
        handler = "None"

        try:
            # 1. Обработка .doc (с попыткой конвертации и дедупликации)
            if ext == '.doc':
                docx_path = file_path + "x"
                if os.path.exists(docx_path):
                    logger.info(f"Skipping .doc extraction because .docx already exists: {docx_path}")
                    file_path = docx_path
                    ext = '.docx'
                else:
                    logger.info(f"Handler: .doc legacy converter/antiword")
                    handler = "doc_converter"
                    new_path = self._convert_doc_to_docx(file_path)
                    if new_path.endswith('.docx'):
                        logger.info(f"Successfully converted .doc to .docx: {new_path}")
                        file_path = new_path
                        ext = '.docx'
                        # Продолжаем как .docx ниже
                    else:
                        logger.info(f"Conversion failed or not applicable, using fallback extraction for .doc")
                        full_text = self._extract_text_from_doc(file_path)
                        logger.info(f"Extraction complete. Handler: {handler}. Characters: {len(full_text)}")
                        return full_text

            # 2. Обработка .docx
            if ext == '.docx':
                handler = "python-docx"
                import docx
                doc = docx.Document(file_path)
                full_text = "\n".join([para.text for para in doc.paragraphs])
            
            # 3. Обработка .xlsx
            elif ext == '.xlsx':
                handler = "openpyxl"
                import openpyxl
                wb = openpyxl.load_workbook(file_path, data_only=True)
                sheets_text = []
                for sheet in wb.worksheets:
                    sheet_data = []
                    for row in sheet.iter_rows(values_only=True):
                        row_text = " | ".join([str(cell) if cell is not None else "" for cell in row])
                        if row_text.strip().replace("|", "").strip():
                            sheet_data.append(row_text)
                    if sheet_data:
                        sheets_text.append(f"=== ЛИСТ: {sheet.title} ===\n" + "\n".join(sheet_data))
                full_text = "\n\n".join(sheets_text)
            
            # 4. Обработка .xls
            elif ext == '.xls':
                handler = "xlrd"
                import xlrd
                wb = xlrd.open_workbook(file_path)
                sheets_text = []
                for sheet in wb.sheets():
                    sheet_data = []
                    for row_idx in range(sheet.nrows):
                        row = sheet.row_values(row_idx)
                        row_text = " | ".join([str(cell) if cell is not None else "" for cell in row])
                        if row_text.strip().replace("|", "").strip():
                            sheet_data.append(row_text)
                    if sheet_data:
                        sheets_text.append(f"=== ЛИСТ: {sheet.name} ===\n" + "\n".join(sheet_data))
                full_text = "\n\n".join(sheets_text)
            
            # 5. Обработка .pdf
            elif ext == '.pdf':
                handler = "pypdf"
                reader = PdfReader(file_path)
                text_pages = []
                for page in reader.pages:
                    try:
                        extracted = page.extract_text()
                        if extracted:
                            text_pages.append(extracted)
                    except Exception as e:
                        logger.warning(f"Failed to extract text from a PDF page: {e}")
                
                full_text = "\n".join(text_pages)
                
                # Оценка качества извлеченного текста
                is_quality_good = self._is_text_quality_good(full_text)
                char_count = len(full_text)
                
                if not is_quality_good:
                    logger.info(f"PDF text quality is POOR (Chars: {char_count}). Triggering OCR fallback...")
                    ocr_result = self._perform_ocr(file_path, full_text)
                    
                    # Если OCR вернул системную инфу об ошибке, значит он не отработал полноценно
                    if "[SYSTEM INFO]" in ocr_result or "[OCR ERROR]" in ocr_result:
                        if "деградированный" in ocr_result.lower():
                            logger.warning("PDF marked as DEGRADED: OCR tools missing.")
                            handler = "pypdf (degraded, OCR unavailable)"
                        else:
                            logger.warning("OCR fallback failed. Using low-quality PyPDF text.")
                            handler = "pypdf (degraded, OCR failed)"
                        full_text = ocr_result
                    else:
                        logger.info("OCR fallback successful.")
                        handler = "pypdf + pypdfium2 + PaddleOCR (OCR)"
                        full_text = ocr_result
                else:
                    logger.info(f"PDF text quality is GOOD (Chars: {char_count}). Using native text layer.")

            # 6. Неподдерживаемый формат
            else:
                logger.warning(f"Unsupported file format: {ext} for file {file_path}")
                return f"[SYSTEM INFO] Формат {ext} не поддерживается."

            char_count = len(full_text)
            logger.info(f"Extraction complete. Handler: {handler}. Characters: {char_count}")
            return full_text

        except Exception as e:
            logger.error(f"Extraction failed for {file_path} using {handler}. Error: {str(e)}", exc_info=True)
            return f"[ERROR] Ошибка при чтении {ext} ({handler}): {str(e)}"

    def _is_text_quality_good(self, text: str) -> bool:
        """
        Проверяет качество извлеченного текста.
        Возвращает True, если текст качественный, и False, если требуется OCR.
        Ужесточенные критерии для PDF.
        """
        if not text or len(text.strip()) < 150: # Увеличили порог минимальной длины
            return False

        total_chars = len(text)
        
        # 1. Доля мусорных символов
        clean_pattern = r'[a-zA-Zа-яА-ЯёЁ0-9\s\.,!?;:()""\'\'\-\+=\[\]/\\<>@#\$%\^&\*«»№]'
        clean_chars_count = len(re.findall(clean_pattern, text))
        garbage_ratio = 1 - (clean_chars_count / total_chars) if total_chars > 0 else 1
        
        # 2. Доля нормальных слов на русском
        words = text.split()
        if not words:
            return False
            
        russian_words = [w for w in words if re.search(r'[а-яА-ЯёЁ]{3,}', w)]
        russian_words_ratio = len(russian_words) / len(words) if words else 0
        
        # 3. Наличие длинных испорченных строк
        max_word_len = max(len(w) for w in words)
        avg_word_len = sum(len(w) for w in words) / len(words)
        
        # 4. Доля нечитаемых фрагментов
        unreadable_chars = len(re.findall(r'[\?\x00-\x08\x0b\x0c\x0e-\x1f]', text))
        unreadable_ratio = unreadable_chars / total_chars if total_chars > 0 else 0

        logger.info(f"PDF Quality Metrics: Garbage={garbage_ratio:.2f}, RusWords={russian_words_ratio:.2f}, MaxWord={max_word_len}, AvgWord={avg_word_len:.1f}, Unreadable={unreadable_ratio:.2f}")

        # Ужесточенные пороговые значения:
        # - Мусора > 10% (было 15%)
        # - Русских слов < 30% (было 20%)
        # - Слишком длинные "слова" (> 80 символов, было 100)
        # - Слишком много нечитаемых знаков (> 3%, было 5%)
        
        if garbage_ratio > 0.10:
            return False
        if russian_words_ratio < 0.30:
            # Если это не технический документ на английском
            english_words = [w for w in words if re.search(r'[a-zA-Z]{3,}', w)]
            english_words_ratio = len(english_words) / len(words)
            if english_words_ratio < 0.40: # И не английский тоже
                return False
        if max_word_len > 80 or avg_word_len > 18:
            return False
        if unreadable_ratio > 0.03:
            return False

        return True

    def _perform_ocr(self, file_path: str, existing_text: str) -> str:
        """Вспомогательный метод для OCR распознавания через pypdfium2 и PaddleOCR"""
        if not pdfium or not self.ocr_engine:
            logger.warning("OCR requested but pypdfium2 or PaddleOCR is not available.")
            return f"{existing_text}\n\n[SYSTEM INFO] Статус: деградированный режим. Текст не распознан: требуются библиотеки pypdfium2 и paddleocr. Извлечение цен и спецификаций может быть неточным."

        try:
            logger.info(f"Starting OCR for {file_path} using pypdfium2 + PaddleOCR")
            
            # Открываем PDF через pypdfium2
            pdf = pdfium.PdfDocument(file_path)
            num_pages = len(pdf)
            # Ограничиваем количество страниц для OCR (первые 20 для баланса скорости и качества)
            pages_to_process = min(num_pages, 20)
            
            ocr_text_pages = []
            
            for i in range(pages_to_process):
                logger.info(f"Processing page {i+1}/{pages_to_process}...")
                page = pdf[i]
                
                # Рендерим страницу в изображение (scale=2 для 144 DPI, scale=3 для 216 DPI)
                # PaddleOCR хорошо работает на 144-200 DPI
                bitmap = page.render(scale=2)
                pil_image = bitmap.to_pil()
                
                # Конвертируем PIL Image в numpy array для PaddleOCR
                img_array = np.array(pil_image)
                
                # Выполняем OCR
                result = self.ocr_engine.ocr(img_array, cls=True)
                
                page_text = []
                if result and result[0]:
                    for line in result[0]:
                        # line[1][0] - это текст, line[1][1] - это уверенность (confidence)
                        text_line = line[1][0]
                        page_text.append(text_line)
                
                ocr_text_pages.append(f"--- Page {i+1} ---\n" + "\n".join(page_text))
            
            pdf.close()
            
            if ocr_text_pages:
                full_ocr_text = "\n\n".join(ocr_text_pages)
                logger.info(f"OCR successful. Extracted {len(full_ocr_text)} characters from {pages_to_process} pages.")
                return full_ocr_text
            else:
                logger.warning("OCR ran but found no text.")
                return f"{existing_text}\n\n[SYSTEM INFO] Статус: деградированный режим. OCR отработал, но текст не найден."

        except Exception as e:
            logger.error(f"OCR Global Error using PaddleOCR: {e}", exc_info=True)
            return f"{existing_text}\n\n[SYSTEM INFO] Статус: деградированный режим. Не удалось выполнить распознавание (PaddleOCR): {str(e)}. Извлечение цен и спецификаций может быть неточным."

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
