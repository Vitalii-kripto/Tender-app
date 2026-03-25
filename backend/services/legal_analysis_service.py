import json
import re
import time
from typing import Any, Callable, Dict, List, Optional

from backend.logger import logger, log_debug_event
from .ai_service import AiService
from .fact_extraction_service import FactExtractionService
from .legal_prompts import PROMPT_GENERATE_REPORT


class LegalAnalysisService:
    """
    Новый режим анализа:
    1) детерминированные факты кодом
    2) один AI-вызов на извлечение всех тем сразу
    3) merge
    4) один AI-вызов на итоговый отчет
    """

    def __init__(self, ai_service: Optional[AiService] = None):
        self.ai_service = ai_service or AiService()
        self.fact_service = FactExtractionService(self.ai_service)
        logger.info("LegalAnalysisService initialized.")

    def analyze_tender(
        self,
        files_data: List[Dict[str, Any]],
        tender_id: str = "N/A",
        job_id: str = "N/A",
        callback: Optional[Callable[[str, int, str], None]] = None,
    ) -> Dict[str, Any]:
        logger.info(f"Starting compact AI analysis for tender {tender_id} (Job: {job_id})")

        if callback:
            callback("Подготовка контекста", 15, "running")

        cleaned_context_len = sum(len(self._clean_text(f.get("text", "") or "")) for f in files_data)

        try:
            start_time = time.time()

            if callback:
                callback("Детерминированное извлечение", 25, "running")

            deterministic_start = time.time()
            deterministic_facts = self.fact_service.extract_deterministic_facts(files_data)
            deterministic_end = time.time()

            if callback:
                callback("Единый AI-вызов: сбор фактов", 45, "running")

            ai_extract_start = time.time()
            all_facts = self.fact_service.extract_thematic_facts_ai(
                files_data,
                deterministic_facts,
                tender_id=tender_id,
                job_id=job_id,
            )
            ai_extract_end = time.time()

            facts_data = [fact.to_dict() for fact in all_facts]

            if callback:
                callback("Сверка фактов", 60, "running")

            merge_start = time.time()
            merged_facts = self.fact_service.merge_facts(all_facts, tender_id=tender_id)
            merge_end = time.time()

            if callback:
                callback("Генерация итогового отчета", 80, "running")

            report_prompt = PROMPT_GENERATE_REPORT.replace(
                "__FACTS__",
                json.dumps(merged_facts, ensure_ascii=False, indent=2),
            )

            report_start = time.time()
            report_response = self.ai_service._call_ai_with_retry(
                self.ai_service.client.models.generate_content,
                contents=report_prompt,
            )
            report_end = time.time()

            response_text = report_response.text if report_response else ""
            response_text = re.sub(
                r"^#\s*Юридическое заключение по тендеру\s*\n",
                "",
                response_text,
                flags=re.IGNORECASE,
            )
            final_report_markdown = f"# Юридическое заключение по тендеру\n\n{response_text.strip()}".strip()
            final_report_len = len(final_report_markdown)

            final_status = self._calculate_final_status(merged_facts, final_report_markdown)

            summary = self._extract_summary(final_report_markdown)
            end_time = time.time()

            log_debug_event(
                {
                    "stage": "report_generation",
                    "job_id": job_id,
                    "tender_id": tender_id,
                    "report_generation_prompt_size": len(report_prompt),
                    "raw_report_response": response_text,
                    "final_status": final_status,
                    "validation_result": {
                        "report_length": final_report_len,
                        "key_topic_summary": self._build_key_topic_summary(merged_facts),
                    },
                }
            )

            log_debug_event(
                {
                    "stage": "full_analysis",
                    "job_id": job_id,
                    "tender_id": tender_id,
                    "model_name": "compact-two-call-mode",
                    "extracted_facts": facts_data,
                    "merged_facts": merged_facts,
                    "final_decision": final_status,
                    "duration": end_time - start_time,
                    "timing": {
                        "deterministic_extraction": deterministic_end - deterministic_start,
                        "ai_single_fact_extraction": ai_extract_end - ai_extract_start,
                        "merge": merge_end - merge_start,
                        "report_generation": report_end - report_start,
                    },
                }
            )

            logger.info(
                f"Compact AI analysis finished for tender {tender_id} "
                f"in {end_time - start_time:.2f}s, status={final_status}, report_len={final_report_len}"
            )

            return {
                "status": final_status,
                "final_report_markdown": final_report_markdown,
                "summary_notes": summary,
                "cleaned_context_len": cleaned_context_len,
                "final_report_len": final_report_len,
                "structured_data": {},
                "extracted_facts": facts_data,
                "merged_facts": merged_facts,
            }

        except Exception as e:
            logger.error(f"Compact AI analysis error for tender {tender_id}: {e}", exc_info=True)
            log_debug_event(
                {
                    "stage": "full_analysis",
                    "job_id": job_id,
                    "tender_id": tender_id,
                    "final_decision": "error",
                    "validation_errors": str(e),
                }
            )
            return {
                "status": "error",
                "final_report_markdown": f"# Ошибка анализа\n\nПроизошла ошибка при обработке тендера: {str(e)}",
                "summary_notes": "Ошибка анализа.",
                "cleaned_context_len": cleaned_context_len,
                "final_report_len": 0,
                "structured_data": {},
                "extracted_facts": [],
                "merged_facts": {},
                "error_message": str(e),
            }

    def _clean_text(self, text: str) -> str:
        if not text:
            return ""
        text = re.sub(r"\s+", " ", text)
        text = re.sub(r"[^\x20-\x7E\u0400-\u04FF\n\t\.,!?;:()\"\"''\-\+=\[\]/\\<>@#\$%\^&\*«»№]", "", text)
        return text.strip()

    def _extract_summary(self, markdown: str) -> str:
        if not markdown:
            return ""
        match = re.search(r"##\s*Краткое резюме.*?(\n##|$)", markdown, re.DOTALL | re.IGNORECASE)
        if not match:
            return "Резюме не найдено в отчете."
        content = match.group(0)
        content = re.sub(r"##\s*Краткое резюме.*?\n", "", content, count=1, flags=re.IGNORECASE)
        content = re.sub(r"\n##$", "", content).strip()
        return content or "Резюме не найдено в отчете."

    def _topic_has_reliable_data(self, topic_data: Dict[str, Any]) -> bool:
        if not topic_data:
            return False

        final_value = topic_data.get("final_value")

        if final_value in (None, "not_found", "conflict", "", [], {}):
            return False

        if isinstance(final_value, list):
            return len(final_value) > 0

        if isinstance(final_value, dict):
            return len(final_value) > 0

        if isinstance(final_value, str):
            return bool(final_value.strip())

        return True

    def _topic_is_hard_conflict(self, topic_data: Dict[str, Any]) -> bool:
        if not topic_data:
            return False
        return bool(topic_data.get("conflict_flag")) and topic_data.get("merge_mode") == "select"

    def _build_key_topic_summary(self, merged_facts: Dict[str, Any]) -> Dict[str, Any]:
        key_topics = ["subject", "items_quantities", "nmcc_prices", "delivery_terms", "payment"]
        out = {}
        for topic in key_topics:
            topic_data = merged_facts.get(topic, {})
            out[topic] = {
                "has_data": self._topic_has_reliable_data(topic_data),
                "hard_conflict": self._topic_is_hard_conflict(topic_data),
                "merge_mode": topic_data.get("merge_mode"),
            }
        return out

    def _calculate_final_status(self, merged_facts: Dict[str, Any], report_markdown: str) -> str:
        key_topics = ["subject", "items_quantities", "nmcc_prices", "delivery_terms", "payment"]

        missing_count = 0
        hard_conflict_count = 0

        for topic in key_topics:
            topic_data = merged_facts.get(topic, {})
            if not self._topic_has_reliable_data(topic_data):
                missing_count += 1
            if self._topic_is_hard_conflict(topic_data):
                hard_conflict_count += 1

        report_len = len(report_markdown or "")

        if report_len < 400:
            return "error"

        if missing_count >= 4:
            return "error"

        if missing_count >= 2 or hard_conflict_count >= 1:
            return "partial"

        return "success"
