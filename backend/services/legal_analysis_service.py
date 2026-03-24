import time
import json
import logging
import re
import os
from datetime import datetime
from typing import List, Dict, Any
from google import genai
from google.genai import types
from .legal_prompts import PROMPT_FULL_PACKAGE

from dotenv import load_dotenv

# Загружаем переменные окружения (.env)
env_loaded = load_dotenv()

# Настройка логгера
def setup_legal_logger():
    env_debug_val = os.environ.get('LEGAL_AI_DEBUG', 'false')
    debug_mode = env_debug_val.lower() == 'true'
    
    loggers = [logging.getLogger("LegalAnalysisService")]
    
    log_dir = os.path.join(os.getcwd(), 'backend', 'logs')
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, 'legal_ai.log')
    
    # Используем 'a' (append) и utf-8
    file_handler = logging.FileHandler(log_file, encoding='utf-8', mode='a')
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    
    # Также в консоль
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    
    for l in loggers:
        l.setLevel(logging.DEBUG if debug_mode else logging.INFO)
        if l.hasHandlers():
            l.handlers.clear()
        l.addHandler(file_handler)
        l.addHandler(stream_handler)
        l.propagate = False
        
    logger = logging.getLogger("LegalAnalysisService")
    
    # Стартовые логи для проверки .env
    logger.info(f"--- [ENV INITIALIZATION] ---")
    logger.info(f".env file found and loaded: {env_loaded}")
    logger.info(f"LEGAL_AI_DEBUG from env: '{env_debug_val}'")
    logger.info(f"Actual DEBUG_MODE: {debug_mode}")
    logger.info(f"----------------------------")
    
    return logger, debug_mode

logger, DEBUG_MODE = setup_legal_logger()

class LegalAnalysisService:
    def __init__(self, ai_client):
        self.client = ai_client
        self.debug_mode = DEBUG_MODE
        logger.info(f"LegalAnalysisService initialized. Debug mode: {self.debug_mode}")

    def _assemble_prompt(self, template: str, text: str, prompt_type: str) -> str:
        """
        Безопасно собирает промпт, заменяя __TEXT__ на текст документа.
        """
        try:
            if "__TEXT__" not in template:
                logger.error(f"Prompt template for {prompt_type} missing __TEXT__ placeholder")
                return None
            
            assembled = template.replace("__TEXT__", text)
            
            # Защитная проверка: если остались маркеры, значит что-то пошло не так
            if "__TEXT__" in assembled:
                logger.error(f"Prompt assembly failed for {prompt_type}: placeholder still present")
                return None
                
            return assembled
        except Exception as e:
            logger.error(f"Error assembling prompt for {prompt_type}: {e}")
            return None

    def _call_ai_with_retry(self, prompt: str, prompt_type: str, tender_id: str = "unknown", filenames: List[str] = None, retries: int = 1) -> Dict[str, Any]:
        """
        Вызывает ИИ с поддержкой нового формата и legacy-fallback.
        """
        if not self.client:
            return {"final_report_markdown": "Ошибка: ИИ-клиент не инициализирован."}
            
        start_time = datetime.now()
        model_name = 'gemini-3-flash-preview'
        
        # Логирование перед вызовом
        logger.info(f"===== [AI REQUEST START] =====")
        logger.info(f"Tender ID: {tender_id}")
        logger.info(f"Prompt Type: {prompt_type}")
        logger.info(f"Model Name: {model_name}")
        logger.info(f"Files: {filenames if filenames else 'N/A'}")
        logger.info(f"Context Size: {len(prompt)} characters")
        logger.info(f"Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        logger.info("--- [FULL ASSEMBLED PROMPT] ---")
        logger.info(prompt)
        logger.info("--- [END OF PROMPT] ---")
        
        logger.info(f"===== [AI REQUEST END] =====")
            
        for attempt in range(retries + 1):
            try:
                current_prompt = prompt
                if attempt > 0:
                    logger.info(f"Retry attempt {attempt} for tender {tender_id}")

                response = self.client.models.generate_content(
                    model=model_name,
                    contents=current_prompt,
                    config=types.GenerateContentConfig(
                        temperature=0.1
                    )
                )
                
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()
                
                text = response.text.strip()
                
                # Логирование ответа
                logger.info(f"===== [AI RESPONSE START] =====")
                logger.info(f"Tender ID: {tender_id}")
                logger.info(f"End Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
                logger.info(f"Duration: {duration:.2f} seconds")
                logger.info(f"Response Length: {len(text)} characters")
                logger.info(f"Full Response Text:")
                logger.info(text)
                logger.info(f"===== [AI RESPONSE END] =====")
                
                return {"final_report_markdown": text}
            except Exception as e:
                logger.error(f"AI Call Error on attempt {attempt}: {str(e)}")
                if attempt == retries:
                    return {"final_report_markdown": f"Ошибка вызова ИИ: {str(e)}"}
                time.sleep(2)
        
        return {"final_report_markdown": "Неизвестная ошибка при вызове ИИ."}

    def _clean_text(self, text: str) -> str:
        """
        Выполняет глубокую очистку текста от технического мусора, сохраняя юридическую значимость.
        """
        if not text:
            return ""
        
        # 1. Удаление явно битых служебных символов (контрольные символы кроме \n \t)
        text = "".join(ch for ch in text if ch == '\n' or ch == '\t' or (ord(ch) >= 32 and ord(ch) != 127))
        
        # 2. Удаление артефактов OCR (длинные последовательности точек, подчеркиваний, тире)
        text = re.sub(r'\.{5,}', '...', text)
        text = re.sub(r'_{5,}', '___', text)
        text = re.sub(r'-{5,}', '---', text)
        
        # 3. Удаление повторяющихся пробелов и табуляций (сохраняем структуру строк)
        text = re.sub(r'[ \t]+', ' ', text)
        
        # 4. Удаление избыточных пустых строк (более двух подряд)
        text = re.sub(r'\n{3,}', '\n\n', text)
        
        # 5. Удаление дублей подряд идущих одинаковых строк (часто при ошибках OCR)
        lines = text.split('\n')
        deduped_lines = []
        for line in lines:
            trimmed = line.strip()
            if not trimmed:
                if not deduped_lines or deduped_lines[-1] != "":
                    deduped_lines.append("")
                continue
            if not deduped_lines or trimmed != deduped_lines[-1]:
                deduped_lines.append(line)
        
        return "\n".join(deduped_lines).strip()

    def _log_progress(self, stage: str, progress: int, status: str = "processing", callback=None):
        """Логирование прогресса и вызов callback."""
        if callback:
            callback(stage, progress, status)
        logger.info(f"Stage: {stage}, Progress: {progress}%")

    def _prepare_full_context(self, files: List[Dict[str, str]]) -> str:
        """
        Подготавливает полный очищенный контекст из всех документов.
        Извлекает текст, очищает его и размечает явными границами.
        """
        full_context = []
        
        for f in files:
            filename = f.get('filename', 'unknown')
            text = f.get('text', '')
            
            # Очистка текста
            cleaned_text = self._clean_text(text)
            
            # Разметка документа с явными границами (согласно требованиям)
            doc_block = (
                f"=== ДОКУМЕНТ: {filename} ===\n"
                f"{cleaned_text}\n"
                f"=== КОНЕЦ ДОКУМЕНТА ==="
            )
            full_context.append(doc_block)
            
        return "\n\n".join(full_context)

    def _extract_summary(self, markdown: str) -> str:
        """
        Извлекает раздел 0 (Summary) из отчета.
        """
        if not markdown:
            return ""
            
        # Ищем раздел 0
        summary_match = re.search(r'(?:##\s*)?0\)\s*Краткое резюме.*?(?=(?:##\s*)?1\)|$)', markdown, re.DOTALL | re.IGNORECASE)
        if not summary_match:
            # Если раздел 0 не найден, попробуем взять первые 300 символов
            return markdown[:300] + "..." if len(markdown) > 300 else markdown
            
        summary_content = summary_match.group(0)
        # Убираем заголовок
        summary_content = re.sub(r'(?:##\s*)?0\)\s*Краткое резюме.*?\n', '', summary_content, count=1, flags=re.IGNORECASE)
        return summary_content.strip()

    def analyze_full_package(self, files: List[Dict[str, str]], tender_id: str = "unknown", callback=None) -> Dict[str, Any]:
        """
        Основной метод анализа полного пакета документов.
        """
        logger.info(f"--- STARTING FULL PACKAGE ANALYSIS FOR TENDER: {tender_id} ---")
        
        self._log_progress("Сбор и очистка документов", 10, callback=callback)
        
        filenames = [f.get('filename', 'unknown') for f in files]
        file_statuses = [{"filename": f, "status": "processed"} for f in filenames]
        
        self._log_progress("Анализ документации", 30, callback=callback)

        # 2. Подготовка полного контекста (основной вход для модели)
        logger.info(f"Preparing full context from {len(files)} files...")
        full_context = self._prepare_full_context(files)
        cleaned_context_len = len(full_context)
        logger.info(f"Full cleaned context size: {cleaned_context_len} characters")
        
        logger.info("--- [ASSEMBLED FULL CLEANED CONTEXT START] ---")
        logger.info(full_context)
        logger.info("--- [ASSEMBLED FULL CLEANED CONTEXT END] ---")
        
        # 3. Интерпретация полного контекста с помощью ИИ
        assembled_prompt = self._assemble_prompt(PROMPT_FULL_PACKAGE, full_context, "full")
        if not assembled_prompt:
            logger.error(f"Prompt assembly failed for tender {tender_id}")
            res = {"final_report_markdown": "Ошибка формирования текста промпта для ИИ-анализа."}
        else:
            res = self._call_ai_with_retry(assembled_prompt, prompt_type="full", tender_id=tender_id, filenames=filenames)
        
        final_report_markdown = res.get('final_report_markdown') or ""
        final_report_len = len(final_report_markdown)
        summary_notes = self._extract_summary(final_report_markdown)
        
        logger.info(f"AI response: markdown length: {final_report_len}")
        
        # Финальное логирование всех требуемых метрик
        logger.info(f"--- [TENDER ANALYSIS SUMMARY: {tender_id}] ---")
        logger.info(f"1. Files analyzed: {filenames}")
        logger.info(f"2. Cleaned context length: {cleaned_context_len} chars")
        logger.info(f"3. Final markdown report length: {final_report_len} chars")
        logger.info(f"---------------------------------------------")

        logger.info("--- [FINAL REPORT MARKDOWN START] ---")
        logger.info(final_report_markdown)
        logger.info("--- [FINAL REPORT MARKDOWN END] ---")
        
        self._log_progress("Формирование отчета", 90, callback=callback)
        
        result = {
            "tender_id": tender_id,
            "file_statuses": file_statuses,
            "final_report_markdown": final_report_markdown,
            "summary_notes": summary_notes,
            "cleaned_context_len": cleaned_context_len,
            "final_report_len": final_report_len,
            "status": "success" if final_report_markdown else "partial",
            "stage": "Готово",
            "progress": 100
        }
        
        logger.info(f"--- ANALYSIS COMPLETED FOR TENDER: {tender_id} ---")
        logger.info(f"Final result summary: markdown length={final_report_len}")
        
        logger.info("--- [FINAL JSON RESULT] ---")
        logger.info(json.dumps(result, ensure_ascii=False, indent=2))
        logger.info("--- [END OF FINAL JSON] ---")
            
        return result

    def analyze_tender(self, files: List[Dict[str, str]], tender_id: str = "unknown", callback=None) -> Dict[str, Any]:
        """
        Legacy wrapper for analyze_full_package.
        """
        return self.analyze_full_package(files, tender_id=tender_id, callback=callback)

