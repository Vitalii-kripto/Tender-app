import os
import re
import logging
import time
from typing import List, Dict, Any, Optional, Callable
from google import genai
from google.genai import types
from .legal_prompts import PROMPT_FULL_PACKAGE

logger = logging.getLogger("LegalAnalysisService")

class LegalAnalysisService:
    """
    Сервис для проведения юридического анализа тендерной документации с использованием ИИ.
    Использует клиент из AiService.
    """

    def __init__(self, client: Optional[genai.Client] = None):
        self.client = client
        # Если клиент не передан, попробуем создать свой (хотя обычно он передается из main.py)
        if not self.client:
            api_key = os.getenv("API_KEY")
            if api_key:
                self.client = genai.Client(api_key=api_key)
                logger.info("LegalAnalysisService: New Gemini Client initialized.")
            else:
                logger.error("LegalAnalysisService: No API_KEY found and no client provided!")
        
        # Модель для анализа
        self.model_name = "gemini-1.5-pro" # Или gemini-1.5-flash для скорости

    def analyze_tender(
        self, 
        files_data: List[Dict[str, str]], 
        tender_id: str = "N/A",
        callback: Optional[Callable[[str, int, str], None]] = None
    ) -> Dict[str, Any]:
        """
        Выполняет полный анализ тендера по всем документам.
        """
        logger.info(f"Starting full AI analysis for tender {tender_id}")
        
        if callback:
            callback("Подготовка контекста", 30, "running")

        # 1. Сбор и очистка контекста
        full_context = ""
        for file in files_data:
            filename = file.get('filename', 'Unknown')
            text = file.get('text', '')
            cleaned_text = self._clean_text(text)
            full_context += f"\n\n--- ФАЙЛ: {filename} ---\n{cleaned_text}\n"

        cleaned_context_len = len(full_context)
        logger.info(f"Cleaned context length: {cleaned_context_len} chars")

        if callback:
            callback("Генерация отчета ИИ", 50, "running")

        # 2. Формирование промпта
        prompt = self._assemble_prompt(full_context)

        # 3. Вызов ИИ
        try:
            start_time = time.time()
            response = self._call_ai_with_retry(prompt)
            response_text = response.text if response else ""
            end_time = time.time()
            
            final_report_len = len(response_text)
            logger.info(f"AI Response received in {end_time - start_time:.2f}s. Length: {final_report_len} chars")

            if callback:
                callback("Обработка результата", 90, "running")

            # 4. Извлечение Summary
            summary = self._extract_summary(response_text)

            return {
                "status": "success",
                "final_report_markdown": response_text,
                "summary_notes": summary,
                "cleaned_context_len": cleaned_context_len,
                "final_report_len": final_report_len
            }

        except Exception as e:
            logger.error(f"AI Analysis error: {e}", exc_info=True)
            return {
                "status": "error",
                "final_report_markdown": f"Ошибка при вызове ИИ: {str(e)}",
                "summary_notes": "Ошибка анализа.",
                "cleaned_context_len": cleaned_context_len,
                "final_report_len": 0
            }

    def _clean_text(self, text: str) -> str:
        """
        Очистка текста от лишних пробелов и мусора для экономии токенов.
        """
        if not text:
            return ""
        # Удаляем множественные пробелы и переносы
        text = re.sub(r'\s+', ' ', text)
        # Удаляем явно нечитаемые символы
        text = re.sub(r'[^\x20-\x7E\u0400-\u04FF\n\t\.,!?;:()""\'\'\-\+=\[\]/\\<>@#\$%\^&\*«»№]', '', text)
        return text.strip()

    def _assemble_prompt(self, context: str) -> str:
        """
        Вставляет контекст в шаблон промпта.
        """
        return PROMPT_FULL_PACKAGE.replace("__TEXT__", context)

    def _call_ai_with_retry(self, prompt: str, retries: int = 3):
        """
        Вызов API Gemini с повторными попытками при ошибках.
        """
        if not self.client:
            raise Exception("Gemini Client not initialized")

        for i in range(retries + 1):
            try:
                response = self.client.models.generate_content(
                    model=self.model_name,
                    contents=prompt
                )
                if response:
                    return response
                else:
                    raise Exception("Empty response from AI")
            except Exception as e:
                logger.warning(f"AI Call attempt {i} failed: {e}")
                if i == retries:
                    raise e
                time.sleep(2 ** i) # Exponential backoff
        return None

    def _extract_summary(self, markdown: str) -> str:
        """
        Извлекает раздел 1 (Summary) из отчета.
        Обновлено: теперь Summary - это раздел 1.
        """
        if not markdown:
            return ""
            
        # Ищем раздел 1 (Краткое резюме)
        # Ищем текст между "1) Краткое резюме" и "2)"
        summary_match = re.search(r'(?:##\s*)?1\)\s*Краткое резюме.*?(?=(?:##\s*)?2\)|$)', markdown, re.DOTALL | re.IGNORECASE)
        
        if summary_match:
            content = summary_match.group(0)
            # Очищаем от заголовка
            content = re.sub(r'(?:##\s*)?1\)\s*Краткое резюме.*?\n', '', content, count=1, flags=re.IGNORECASE)
            return content.strip()
        
        return "Резюме не найдено в отчете."
