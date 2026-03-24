import os
import re
import logging
import time
from typing import List, Dict, Any, Optional, Callable
from google import genai
from google.genai import types
from .legal_prompts import PROMPT_FULL_PACKAGE
from backend.logger import logger
from .ai_service import AiService

class LegalAnalysisService:
    """
    Сервис для проведения юридического анализа тендерной документации с использованием ИИ.
    Использует AiService для вызовов ИИ.
    """

    def __init__(self, ai_service: Optional[AiService] = None):
        self.ai_service = ai_service
        # Если сервис не передан, создаем свой
        if not self.ai_service:
            self.ai_service = AiService()
        
        logger.info(f"LegalAnalysisService initialized.")

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
        degraded_list = []
        for file in files_data:
            if "[SYSTEM INFO]" in file.get('text', '') and "деградированный" in file.get('text', '').lower():
                degraded_list.append(file.get('filename', 'Unknown'))
        
        prompt = self._assemble_prompt(full_context)
        if degraded_list:
            degraded_note = f"\n\nВНИМАНИЕ: Следующие файлы были обработаны в деградированном режиме (без качественного OCR), так как на сервере возникли сложности с распознаванием слоев: {', '.join(degraded_list)}. Текст из них может быть неполным или содержать ошибки. Если в этих файлах должны быть таблицы с ценами или спецификации, но ты их не видишь - обязательно отметь это в отчете как риск отсутствия данных из-за технического ограничения.\n"
            prompt = degraded_note + prompt

        # 3. Вызов ИИ (Первый проход)
        try:
            start_time = time.time()
            response = self._call_ai_with_retry(prompt)
            response_text = response.text if response else ""
            
            # 4. Проверка полноты (Backend-контроль)
            missing_sections = self._check_completeness(response_text)
            
            if missing_sections:
                logger.warning(f"Report is incomplete. Missing or poor sections: {missing_sections}. Triggering retry...")
                if callback:
                    callback(f"Дополнение отчета ({', '.join(missing_sections)})", 70, "running")
                
                retry_prompt = self._assemble_retry_prompt(full_context, response_text, missing_sections)
                retry_response = self._call_ai_with_retry(retry_prompt)
                
                if retry_response and retry_response.text:
                    logger.info("Retry response received. Merging results...")
                    response_text = self._merge_reports(response_text, retry_response.text, missing_sections)
            
            end_time = time.time()
            
            # 5. Пост-обработка (деградированный режим и т.д.)
            degraded_files = []
            for file in files_data:
                if "[SYSTEM INFO]" in file.get('text', '') and "деградированный" in file.get('text', '').lower():
                    degraded_files.append(file.get('filename', 'Unknown'))
            
            if degraded_files:
                warning_msg = f"\n\n> **⚠️ ВНИМАНИЕ: Техническое ограничение**\n> Некоторые документы ({', '.join(degraded_files)}) были обработаны в деградированном режиме (без качественного OCR). Качество анализа этих файлов может быть неполным.\n\n"
                response_text = warning_msg + response_text

            final_report_len = len(response_text)
            logger.info(f"AI Analysis finished in {end_time - start_time:.2f}s. Final length: {final_report_len} chars")

            if callback:
                callback("Обработка результата", 95, "running")

            # 6. Извлечение Summary
            summary = self._extract_summary(response_text)

            return {
                "status": "success",
                "final_report_markdown": response_text,
                "summary_notes": summary,
                "cleaned_context_len": cleaned_context_len,
                "final_report_len": final_report_len
            }

        except Exception as e:
            error_msg = f"Ошибка при вызове ИИ: {str(e)}"
            logger.error(f"AI Analysis error: {e}", exc_info=True)
            return {
                "status": "error",
                "final_report_markdown": "",
                "error_message": error_msg,
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

    def _call_ai_with_retry(self, prompt: str):
        """
        Вызов API Gemini через AiService с унифицированной обработкой ошибок.
        """
        if not self.ai_service or not self.ai_service.client:
            raise Exception("Gemini Client not initialized")

        return self.ai_service._call_ai_with_retry(
            self.ai_service.client.models.generate_content,
            contents=prompt
        )

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

    def _check_completeness(self, report: str) -> List[str]:
        """
        Проверяет наличие и полноту обязательных разделов.
        """
        missing = []
        
        # 1. Сведения о заказчике
        customer_match = re.search(r'##\s*Сведения о заказчике(.*?)(?=##\s*0\)|$)', report, re.DOTALL | re.IGNORECASE)
        if not customer_match or len(customer_match.group(1).strip()) < 50 or "не найдена" in customer_match.group(1).lower():
            missing.append("Сведения о заказчике")
            
        # 2. Подробный предмет закупки (Раздел 0)
        section_0_match = re.search(r'##\s*0\)\s*Подробное описание предмета закупки(.*?)(?=##\s*1\)|$)', report, re.DOTALL | re.IGNORECASE)
        if not section_0_match or len(section_0_match.group(1).strip()) < 100 or "не найдена" in section_0_match.group(1).lower():
            missing.append("Предмет закупки")
            
        # 3. Полный перечень документов (Раздел 7)
        section_7_match = re.search(r'##\s*7\)\s*Полный перечень документов(.*?)(?=##\s*8\)|$)', report, re.DOTALL | re.IGNORECASE)
        if not section_7_match or len(section_7_match.group(1).strip()) < 100 or "не найдена" in section_7_match.group(1).lower():
            missing.append("Перечень документов")
            
        return missing

    def _assemble_retry_prompt(self, context: str, partial_report: str, missing_sections: List[str]) -> str:
        """
        Формирует промпт для дополнения недостающих разделов.
        """
        sections_str = ", ".join(missing_sections)
        prompt = f"""Ты — тот же эксперт по тендерам. Твой предыдущий отчет оказался неполным. 
В нем отсутствуют или заполнены формально следующие обязательные разделы: {sections_str}.

Твоя задача: на основе того же контекста тендерной документации, предоставленного ниже, СТРОГО ДОПОЛНИ только эти разделы. 
Не переписывай весь отчет, верни только недостающие разделы в формате Markdown с соответствующими заголовками.

Требования к разделам:
- Если это 'Сведения о заказчике': укажи наименование, ИНН, адрес, контакты.
- Если это 'Предмет закупки': дай максимально подробный позиционный перечень, характеристики, цены, сроки, логистику и блок по эквивалентам.
- Если это 'Перечень документов': укажи ВСЕ документы для заявки и для поставки, найденные в ТД.

КОНТЕКСТ:
{context}

ПРЕДЫДУЩИЙ (НЕПОЛНЫЙ) ОТЧЕТ ДЛЯ СПРАВКИ:
{partial_report[:2000]}... (пропущено)
"""
        return prompt

    def _merge_reports(self, original: str, addition: str, missing_sections: List[str]) -> str:
        """
        Интегрирует дополненные разделы в основной отчет.
        """
        final_report = original
        
        # Заменяем или добавляем разделы
        if "Сведения о заказчике" in missing_sections:
            new_customer = self._extract_section(addition, r'##\s*Сведения о заказчике(.*?)(?=##|$)')
            if new_customer:
                final_report = re.sub(r'##\s*Сведения о заказчике(.*?)(?=##\s*0\)|$)', f"## Сведения о заказчике\n{new_customer}\n", final_report, flags=re.DOTALL | re.IGNORECASE)

        if "Предмет закупки" in missing_sections:
            new_s0 = self._extract_section(addition, r'##\s*0\)\s*Подробное описание предмета закупки(.*?)(?=##|$)')
            if new_s0:
                final_report = re.sub(r'##\s*0\)\s*Подробное описание предмета закупки(.*?)(?=##\s*1\)|$)', f"## 0) Подробное описание предмета закупки\n{new_s0}\n", final_report, flags=re.DOTALL | re.IGNORECASE)

        if "Перечень документов" in missing_sections:
            new_s7 = self._extract_section(addition, r'##\s*7\)\s*Полный перечень документов(.*?)(?=##|$)')
            if new_s7:
                final_report = re.sub(r'##\s*7\)\s*Полный перечень документов(.*?)(?=##\s*8\)|$)', f"## 7) Полный перечень документов\n{new_s7}\n", final_report, flags=re.DOTALL | re.IGNORECASE)

        return final_report

    def _extract_section(self, text: str, pattern: str) -> Optional[str]:
        """
        Вспомогательный метод для извлечения текста раздела из ответа.
        """
        match = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
        if match:
            return match.group(1).strip()
        return None
