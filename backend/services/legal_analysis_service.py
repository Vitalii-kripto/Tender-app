import os
import re
import json
import logging
import time
from typing import List, Dict, Any, Optional, Callable
from google import genai
from google.genai import types
from .legal_prompts import PROMPT_EXTRACT_FACTS, PROMPT_GENERATE_REPORT
from backend.logger import logger
from .ai_service import AiService
from .fact_extraction_service import FactExtractionService, Fact

class LegalAnalysisService:
    """
    Сервис для проведения юридического анализа тендерной документации с использованием ИИ.
    Реализует двухэтапный анализ:
    1. Извлечение фактов по темам (JSON).
    2. Генерация итогового отчета на основе фактов (Markdown).
    """

    def __init__(self, ai_service: Optional[AiService] = None):
        self.ai_service = ai_service
        if not self.ai_service:
            self.ai_service = AiService()
        self.fact_service = FactExtractionService(self.ai_service)
        
        logger.info(f"LegalAnalysisService initialized.")

    def analyze_tender(
        self, 
        files_data: List[Dict[str, str]], 
        tender_id: str = "N/A",
        callback: Optional[Callable[[str, int, str], None]] = None
    ) -> Dict[str, Any]:
        """
        Выполняет полный анализ тендера по всем документам (2 этапа).
        """
        logger.info(f"Starting full AI analysis for tender {tender_id}")
        
        if callback:
            callback("Подготовка контекста", 20, "running")

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
            callback("Структурированное извлечение данных", 30, "running")

        # 1.5 Серверное структурированное извлечение данных (ДО вызова основного ИИ)
        structured_data = self.ai_service.extract_structured_tender_data(full_context)
        
        # 1.6 Серверная проверка на эквиваленты/аналоги
        structured_data["equivalents"] = self._check_equivalents_in_text(full_context, structured_data)
        
        logger.info(f"Structured data extracted: {len(str(structured_data))} chars")

        if callback:
            callback("Извлечение фактов по темам", 50, "running")

        try:
            start_time = time.time()
            
            # ЭТАП 1: Извлечение фактов
            logger.info("Extracting deterministic facts...")
            deterministic_start_time = time.time()
            deterministic_facts = self.fact_service.extract_deterministic_facts(files_data)
            deterministic_end_time = time.time()
            
            logger.info("Extracting thematic facts via AI...")
            # We can either use the fact_service's AI extraction or the one in this class.
            # The user requested to use FactExtractionService.
            ai_extraction_start_time = time.time()
            all_facts = self.fact_service.extract_thematic_facts_ai(files_data, deterministic_facts)
            ai_extraction_end_time = time.time()
            
            facts_data = [fact.to_dict() for fact in all_facts]
            
            # ЭТАП 2: Сверка и объединение фактов
            logger.info("Merging and reconciling facts...")
            merge_start_time = time.time()
            merged_facts = self.fact_service.merge_facts(all_facts)
            merge_end_time = time.time()
            merged_facts_json_str = json.dumps(merged_facts, ensure_ascii=False, indent=2)
            
            logger.info("Facts successfully extracted, merged and parsed.")

            if callback:
                callback("Генерация итогового отчета", 75, "running")

            # ЭТАП 3: Генерация отчета
            report_prompt = PROMPT_GENERATE_REPORT.replace("__FACTS__", merged_facts_json_str)
            
            logger.info("Calling AI for Report Generation (Markdown)...")
            report_start_time = time.time()
            report_response = self.ai_service._call_ai_with_retry(
                self.ai_service.client.models.generate_content,
                contents=report_prompt
            )
            report_end_time = time.time()
            
            response_text = report_response.text if report_response else ""
            
            end_time = time.time()
            
            # Сборка финального отчета
            response_text = re.sub(r'^#\s*Юридическое заключение по тендеру\s*\n', '', response_text, flags=re.IGNORECASE)
            final_report_markdown = f"# Юридическое заключение по тендеру\n\n{response_text.strip()}"
            
            final_report_len = len(final_report_markdown)
            logger.info(f"AI Analysis finished in {end_time - start_time:.2f}s. Final length: {final_report_len} chars")
            
            logger.info(f"--- [TIMING LOGS] ---")
            logger.info(f"- Deterministic extraction time: {deterministic_end_time - deterministic_start_time:.2f}s")
            logger.info(f"- AI extraction time: {ai_extraction_end_time - ai_extraction_start_time:.2f}s")
            logger.info(f"- Merge time: {merge_end_time - merge_start_time:.2f}s")
            logger.info(f"- Report generation time: {report_end_time - report_start_time:.2f}s")
            logger.info(f"---------------------")

            # Логирование в debug-лог
            from backend.logger import log_debug_event
            log_debug_event({
                "stage": "full_analysis",
                "model_name": "gemini-3.1-pro-preview",
                "extracted_facts": facts_data,
                "merged_facts": merged_facts,
                "raw_model_response": response_text,
                "final_decision": "success",
                "duration": end_time - start_time,
                "timing": {
                    "deterministic_extraction": deterministic_end_time - deterministic_start_time,
                    "ai_extraction": ai_extraction_end_time - ai_extraction_start_time,
                    "merge": merge_end_time - merge_start_time,
                    "report_generation": report_end_time - report_start_time
                }
            })

            if callback:
                callback("Обработка результата", 95, "running")

            # Извлечение Summary
            summary = self._extract_summary(final_report_markdown)

            return {
                "status": "success",
                "final_report_markdown": final_report_markdown,
                "summary_notes": summary,
                "cleaned_context_len": cleaned_context_len,
                "final_report_len": final_report_len,
                "structured_data": structured_data,
                "extracted_facts": facts_data,
                "merged_facts": merged_facts
            }

        except Exception as e:
            error_msg = f"Ошибка при вызове ИИ: {str(e)}"
            logger.error(f"AI Analysis error: {e}", exc_info=True)
            from backend.logger import log_debug_event
            log_debug_event({
                "stage": "full_analysis",
                "final_decision": "error",
                "validation_errors": str(e)
            })
            return {
                "status": "error",
                "final_report_markdown": "",
                "error_message": error_msg,
                "summary_notes": "Ошибка анализа.",
                "cleaned_context_len": cleaned_context_len,
                "final_report_len": 0,
                "structured_data": {}
            }

    def _clean_text(self, text: str) -> str:
        if not text:
            return ""
        text = re.sub(r'\s+', ' ', text)
        text = re.sub(r'[^\x20-\x7E\u0400-\u04FF\n\t\.,!?;:()""\'\'\-\+=\[\]/\\<>@#\$%\^&\*«»№]', '', text)
        return text.strip()

    def _extract_summary(self, markdown: str) -> str:
        if not markdown:
            return ""
        summary_match = re.search(r'##\s*Краткое резюме.*?(\n##|$)', markdown, re.DOTALL | re.IGNORECASE)
        if summary_match:
            content = summary_match.group(0)
            content = re.sub(r'##\s*Краткое резюме.*?\n', '', content, count=1, flags=re.IGNORECASE)
            content = re.sub(r'\n##$', '', content).strip()
            return content
        return "Резюме не найдено в отчете."

    def _check_equivalents_in_text(self, text: str, structured_data: dict) -> dict:
        """
        Серверная проверка на наличие слов 'эквивалент' или 'аналог' в тексте.
        """
        text_lower = text.lower()
        has_equivalent = "эквивалент" in text_lower or "аналог" in text_lower
        
        # Проверяем запреты
        forbidden_phrases = [
            "эквивалент не допускается", "без эквивалента", "аналоги не допускаются",
            "поставка эквивалента не предусмотрена", "эквивалент не предусмотрен",
            "не допускается поставка эквивалента", "не подлежит замене на эквивалент",
            "поставка аналогов не допускается", "без аналогов", "строго в соответствии"
        ]
        
        is_forbidden = any(phrase in text_lower for phrase in forbidden_phrases)
        
        status = "Неизвестно"
        if is_forbidden:
            status = "Запрещены"
        elif has_equivalent:
            status = "Разрешены"
            
        return {
            "status": status,
            "has_equivalent_word": has_equivalent,
            "has_forbidden_phrase": is_forbidden
        }
