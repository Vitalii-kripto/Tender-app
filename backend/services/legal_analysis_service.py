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
            callback("Структурированное извлечение данных", 40, "running")

        # 1.5 Серверное структурированное извлечение данных (ДО вызова основного ИИ)
        structured_data = self.ai_service.extract_structured_tender_data(full_context)
        
        # 1.6 Серверная проверка на эквиваленты/аналоги
        structured_data["equivalents"] = self._check_equivalents_in_text(full_context, structured_data)
        
        items = structured_data.get("items", [])
        nmcc = structured_data.get("nmcc")
        
        has_qty = any(item.get("quantity") and str(item.get("quantity")).strip() not in ["", "—", "Не указано"] for item in items)
        has_unit_price = any(item.get("unit_price") and str(item.get("unit_price")).strip() not in ["", "—", "Не указано"] for item in items)
        has_nmcc = bool(nmcc and str(nmcc).strip() not in ["", "—", "Не указано"])
        
        logger.info(f"--- [SERVER EXTRACTION SUMMARY] ---")
        logger.info(f"Items found: {len(items)}")
        logger.info(f"Quantities found: {'Yes' if has_qty else 'No'}")
        logger.info(f"NMCC found: {'Yes' if has_nmcc else 'No'}")
        logger.info(f"Unit prices found: {'Yes' if has_unit_price else 'No'}")
        logger.info(f"-----------------------------------")
        
        logger.info(f"Structured data extracted: {len(str(structured_data))} chars")

        if callback:
            callback("Генерация отчета ИИ", 50, "running")

        # 2. Формирование промпта
        import json
        structured_data_str = json.dumps(structured_data, ensure_ascii=False, indent=2)
        prompt = self._assemble_prompt(full_context, structured_data_str)

        # 3. Вызов ИИ (Первый проход)
        try:
            start_time = time.time()
            
            max_retries = 2
            attempt = 0
            response_text = ""
            sections_to_rewrite = []
            
            while attempt <= max_retries:
                if attempt == 0:
                    response = self._call_ai_with_retry(prompt)
                    current_text = response.text if response else ""
                else:
                    logger.warning(f"Retrying AI generation, attempt {attempt}")
                    response = self._call_ai_with_retry(retry_prompt)
                    retry_text = response.text if response else ""
                    current_text = self._merge_reports(response_text, retry_text, sections_to_rewrite)
                    
                # Проверка полноты
                missing_sections = self._check_completeness(current_text)
                
                # Проверка на ложные "не найдено" и отсутствие данных
                false_negatives = self._validate_final_report(current_text, structured_data)
                
                if not missing_sections and not false_negatives:
                    response_text = current_text
                    break
                    
                # Если есть ошибки, формируем retry_prompt
                if attempt < max_retries:
                    if callback:
                        callback(f"Исправление отчета (попытка {attempt + 1})", 70 + attempt * 10, "running")
                    retry_prompt, sections_to_rewrite = self._assemble_retry_prompt(full_context, current_text, missing_sections, false_negatives, structured_data_str)
                    response_text = current_text # Сохраняем текущий текст для следующего merge
                else:
                    # Исчерпаны попытки
                    if false_negatives or missing_sections:
                        all_errors = false_negatives + [f"Отсутствует раздел: {s}" for s in missing_sections]
                        raise ValueError(f"Отчет некорректен или неполный после всех попыток. Ошибки: {', '.join(all_errors)}")
                    response_text = current_text
                    
                attempt += 1
            
            end_time = time.time()
            
            # 4. Сборка финального отчета
            # Удаляем заголовок `# Юридическое заключение по тендеру` из ответа ИИ, если он там есть
            response_text = re.sub(r'^#\s*Юридическое заключение по тендеру\s*\n', '', response_text, flags=re.IGNORECASE)
            
            final_report_markdown = f"# Юридическое заключение по тендеру\n\n{response_text.strip()}"
            
            final_report_len = len(final_report_markdown)
            logger.info(f"AI Analysis finished in {end_time - start_time:.2f}s. Final length: {final_report_len} chars")

            if callback:
                callback("Обработка результата", 95, "running")

            # 6. Извлечение Summary
            summary = self._extract_summary(final_report_markdown)

            return {
                "status": "success",
                "final_report_markdown": final_report_markdown,
                "summary_notes": summary,
                "cleaned_context_len": cleaned_context_len,
                "final_report_len": final_report_len,
                "structured_data": structured_data
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
                "final_report_len": 0,
                "structured_data": {}
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

    def _assemble_prompt(self, context: str, structured_data_str: str) -> str:
        """
        Вставляет контекст в шаблон промпта.
        """
        prompt = PROMPT_FULL_PACKAGE.replace("__TEXT__", context)
        prompt = prompt.replace("__STRUCTURED_DATA__", structured_data_str)
        return prompt

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

    def _validate_final_report(self, report: str, structured_data: dict) -> List[str]:
        """
        Выполняет обязательную серверную валидацию финального текста.
        Проверяет наличие данных и отсутствие ложных утверждений об их отсутствии.
        """
        errors = []
        found_fields = []
        missing_fields = []
        report_lower = report.lower()
        
        # 1. Заказчик (наименование, ИНН, адрес)
        customer = structured_data.get("customer", {})
        customer_match = re.search(r'##\s*Сведения о заказчике(.*?)(?=##\s*0\)|$)', report, re.DOTALL | re.IGNORECASE)
        customer_text = customer_match.group(1).lower() if customer_match else ""
        
        if customer.get("name") and str(customer.get("name")).strip() not in ["", "—", "Не указано"]:
            if "наименование заказчика не указано" in customer_text or "информация не найдена" in customer_text:
                errors.append("Ложное утверждение об отсутствии наименования заказчика")
                missing_fields.append("Заказчик (наименование)")
            elif str(customer.get("name")).lower() not in customer_text:
                errors.append("В отчете отсутствует наименование заказчика из структурированных данных")
                missing_fields.append("Заказчик (наименование)")
            else:
                found_fields.append("Заказчик (наименование)")
                
        if customer.get("inn") and str(customer.get("inn")).strip() not in ["", "—", "Не указано"]:
            if str(customer.get("inn")).lower() not in customer_text:
                errors.append("В отчете отсутствует ИНН заказчика из структурированных данных")
                missing_fields.append("ИНН")
            else:
                found_fields.append("ИНН")
                
        if customer.get("address") and str(customer.get("address")).strip() not in ["", "—", "Не указано"]:
            # Адрес может быть перефразирован, проверяем хотя бы часть
            addr_parts = [p for p in re.split(r'[,\s]+', str(customer.get("address")).lower()) if len(p) > 3]
            if addr_parts and not any(p in customer_text for p in addr_parts):
                errors.append("В отчете отсутствует адрес заказчика из структурированных данных")
                missing_fields.append("Адрес")
            else:
                found_fields.append("Адрес")

        # 2. Предмет закупки (позиции, количества, НМЦК, сроки, эквиваленты)
        section_0_match = re.search(r'##\s*0\)\s*Подробное описание предмета закупки(.*?)(?=##\s*1\)|$)', report, re.DOTALL | re.IGNORECASE)
        section_0_text = section_0_match.group(1).lower() if section_0_match else ""
        
        items = structured_data.get("items", [])
        if items:
            forbidden_phrases = [
                "количество не указано", "объем не указан", "количества не указаны",
                "не указано в документации количество", "информация о количестве отсутствует",
                "не найдено количество", "информация по данному разделу не найдена",
                "позиции не указаны", "перечень не указан"
            ]
            has_false_negative = False
            for phrase in forbidden_phrases:
                if phrase in section_0_text:
                    errors.append(f"Ложное утверждение об отсутствии позиций/количества ('{phrase}')")
                    missing_fields.append("Позиции/Количества (ложное утверждение)")
                    has_false_negative = True
                    break
            
            if not has_false_negative:
                for item in items:
                    if item.get("name") and str(item.get("name")).strip() not in ["", "—", "Не указано"]:
                        # Проверяем хотя бы одно ключевое слово из названия позиции
                        name_parts = [p for p in re.split(r'[\s]+', str(item.get("name")).lower()) if len(p) > 4]
                        if name_parts and not any(p in section_0_text for p in name_parts):
                            errors.append(f"В отчете отсутствует позиция: {item.get('name')}")
                            missing_fields.append(f"Позиция: {item.get('name')}")
                        else:
                            found_fields.append(f"Позиция: {item.get('name')}")
                            
                    if item.get("quantity") and str(item.get("quantity")).strip() not in ["", "—", "Не указано"]:
                        qty_str = str(item.get("quantity")).lower()
                        if qty_str not in section_0_text:
                            errors.append(f"В отчете отсутствует количество для позиции: {qty_str}")
                            missing_fields.append(f"Количество: {qty_str}")
                        else:
                            found_fields.append(f"Количество: {qty_str}")

        nmcc = structured_data.get("nmcc")
        if nmcc and str(nmcc).strip() not in ["", "—", "Не указано"]:
            if "нмцк не указана" in report_lower or "начальная максимальная цена не указана" in report_lower:
                errors.append("Ложное утверждение об отсутствии НМЦК")
                missing_fields.append("НМЦК")
            else:
                # Извлекаем только цифры из НМЦК для проверки
                nmcc_digits = re.sub(r'\D', '', str(nmcc))
                if nmcc_digits and nmcc_digits not in re.sub(r'\D', '', section_0_text):
                    errors.append("В отчете отсутствует значение НМЦК из структурированных данных")
                    missing_fields.append("НМЦК")
                else:
                    found_fields.append("НМЦК")

        delivery_time = structured_data.get("delivery_time")
        if delivery_time and str(delivery_time).strip() not in ["", "—", "Не указано"]:
            if "сроки поставки не указаны" in section_0_text or "информация о сроках отсутствует" in section_0_text:
                errors.append("Ложное утверждение об отсутствии сроков поставки")
                missing_fields.append("Сроки поставки")
            else:
                del_parts = [p for p in re.split(r'[\s]+', str(delivery_time).lower()) if len(p) > 4]
                if del_parts and not any(p in section_0_text for p in del_parts):
                    errors.append("В отчете отсутствуют сроки поставки из структурированных данных")
                    missing_fields.append("Сроки поставки")
                else:
                    found_fields.append("Сроки поставки")

        # Блок по эквивалентам
        equivalents = structured_data.get("equivalents", {})
        if equivalents:
            if "эквивалент" not in section_0_text and "аналог" not in section_0_text and "точные показатели" not in section_0_text:
                errors.append("В отчете отсутствует блок по эквивалентам / аналогам")
                missing_fields.append("Блок по эквивалентам / аналогам")
            else:
                found_fields.append("Блок по эквивалентам / аналогам")

        # Проверка перечня документов (Раздел 7)
        docs = structured_data.get("documents", [])
        if docs:
            section_7_match = re.search(r'##\s*7\)\s*Полный перечень документов(.*?)(?=##\s*8\)|$)', report, re.DOTALL | re.IGNORECASE)
            if section_7_match:
                section_7_text = section_7_match.group(1).lower()
                if "документы не указаны" in section_7_text or "информация не найдена" in section_7_text:
                    errors.append("Ложное утверждение об отсутствии документов")
                    missing_fields.append("Документы")
                else:
                    found_fields.append("Документы")
                    
        logger.info(f"--- [FINAL VALIDATION SUMMARY] ---")
        logger.info(f"Mandatory fields found: {', '.join(found_fields) if found_fields else 'None'}")
        logger.info(f"Mandatory fields missing: {', '.join(missing_fields) if missing_fields else 'None'}")
        logger.info(f"Report Status: {'REJECTED' if errors else 'ACCEPTED'}")
        logger.info(f"----------------------------------")
                
        return errors

    def _assemble_retry_prompt(self, context: str, partial_report: str, missing_sections: List[str], false_negatives: List[str], structured_data_str: str) -> tuple[str, List[str]]:
        """
        Формирует промпт для дополнения недостающих разделов или исправления ложных утверждений.
        """
        prompt = f"Ты — тот же эксперт по тендерам. Твой предыдущий отчет содержит ОШИБКИ.\n\n"
        
        sections_to_rewrite = []
        
        if missing_sections:
            sections_str = ", ".join(missing_sections)
            prompt += f"В нем отсутствуют или заполнены формально следующие обязательные разделы: {sections_str}.\n"
            sections_to_rewrite.extend(missing_sections)
            
        if false_negatives:
            errors_str = "; ".join(false_negatives)
            prompt += f"КРИТИЧЕСКАЯ ОШИБКА: Ты написал, что данные отсутствуют, хотя они ЕСТЬ в структурированных данных, либо пропустил важные данные. Ошибки: {errors_str}.\n"
            if any(kw in err for err in false_negatives for kw in ["количеств", "НМЦК", "позици", "срок", "эквивалент", "аналог"]):
                if "Предмет закупки" not in sections_to_rewrite:
                    sections_to_rewrite.append("Предмет закупки")
            if any(kw in err for err in false_negatives for kw in ["заказчик", "ИНН", "адрес"]):
                if "Сведения о заказчике" not in sections_to_rewrite:
                    sections_to_rewrite.append("Сведения о заказчике")
            if any("документ" in err for err in false_negatives):
                if "Перечень документов" not in sections_to_rewrite:
                    sections_to_rewrite.append("Перечень документов")

        sections_to_rewrite_str = ", ".join(sections_to_rewrite)
        
        prompt += f"""
Твоя задача: СТРОГО ПЕРЕПИШИ только эти разделы: {sections_to_rewrite_str}. 
Не переписывай весь отчет, верни только исправленные разделы в формате Markdown с соответствующими заголовками.

КРИТИЧЕСКОЕ ПРАВИЛО:
ИСТОЧНИКОМ ИСТИНЫ являются следующие структурированные данные. Ты ОБЯЗАН перенести все факты из них в свой ответ без искажений и без фраз "не указано".

СТРУКТУРИРОВАННЫЕ ДАННЫЕ:
{structured_data_str}

КОНТЕКСТ ТЕНДЕРНОЙ ДОКУМЕНТАЦИИ:
{context}

ПРЕДЫДУЩИЙ (ОШИБОЧНЫЙ) ОТЧЕТ ДЛЯ СПРАВКИ:
{partial_report[:2000]}... (пропущено)
"""
        return prompt, sections_to_rewrite

    def _merge_reports(self, original: str, addition: str, sections_to_rewrite: List[str]) -> str:
        """
        Интегрирует дополненные разделы в основной отчет.
        """
        final_report = original
        
        # Заменяем или добавляем разделы
        if "Сведения о заказчике" in sections_to_rewrite:
            new_cust = self._extract_section(addition, r'##\s*Сведения о заказчике(.*?)(?=##|$)')
            if new_cust:
                final_report = re.sub(r'##\s*Сведения о заказчике(.*?)(?=##\s*0\)|$)', f"## Сведения о заказчике\n{new_cust}\n", final_report, flags=re.DOTALL | re.IGNORECASE)
                
        if "Предмет закупки" in sections_to_rewrite:
            new_s0 = self._extract_section(addition, r'##\s*0\)\s*Подробное описание предмета закупки(.*?)(?=##|$)')
            if new_s0:
                final_report = re.sub(r'##\s*0\)\s*Подробное описание предмета закупки(.*?)(?=##\s*1\)|$)', f"## 0) Подробное описание предмета закупки\n{new_s0}\n", final_report, flags=re.DOTALL | re.IGNORECASE)
                
        if "Перечень документов" in sections_to_rewrite:
            new_s7 = self._extract_section(addition, r'##\s*7\)\s*Полный перечень документов(.*?)(?=##|$)')
            if new_s7:
                final_report = re.sub(r'##\s*7\)\s*Полный перечень документов(.*?)(?=##\s*8\)|$)', f"## 7) Полный перечень документов\n{new_s7}\n", final_report, flags=re.DOTALL | re.IGNORECASE)

        return final_report

    def _check_equivalents_in_text(self, text: str, structured_data: dict) -> dict:
        """
        Выполняет обязательную серверную проверку текста на предмет запрета слов 'эквивалент'/'аналог'
        и требования точных показателей.
        """
        text_lower = text.lower()
        
        equivalents = structured_data.get("equivalents", {})
        if not isinstance(equivalents, dict):
            equivalents = {}
            
        exact_required = equivalents.get("exact_indicators_required", False)
        is_allowed = equivalents.get("is_allowed", False)
        is_contradictory = equivalents.get("is_contradictory", False)
        
        # Паттерны, указывающие на запрет слов "эквивалент", "аналог" или требование точных показателей
        strict_patterns = [
            r'не допускается.*?слов.*?эквивалент',
            r'запрещается.*?слов.*?эквивалент',
            r'не допускается.*?слов.*?аналог',
            r'без слов.*?(?:эквивалент|аналог|не более|не менее)',
            r'точные показатели',
            r'конкретные показатели',
            r'точные значения',
            r'не должны сопровождаться словами.*?эквивалент'
        ]
        
        for pattern in strict_patterns:
            if re.search(pattern, text_lower):
                exact_required = True
                break
                
        equivalents["exact_indicators_required"] = exact_required
        equivalents["is_allowed"] = is_allowed
        equivalents["is_contradictory"] = is_contradictory
        
        return equivalents

    def _extract_section(self, text: str, pattern: str) -> Optional[str]:
        """
        Вспомогательный метод для извлечения текста раздела из ответа.
        """
        match = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
        if match:
            return match.group(1).strip()
        return None
