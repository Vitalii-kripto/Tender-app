import re
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from backend.logger import logger, log_debug_event
from .ai_service import AiService
from .legal_prompts import PROMPT_UNIFIED_LEGAL_ANALYSIS, REQUIRED_REPORT_HEADERS


class LegalAnalysisService:
    """
    Новый режим анализа:
    - один основной юридический промт на весь пакет документов;
    - никакого JSON-фактоизвлечения по темам;
    - никакого merge_facts в основном контуре;
    - цель: максимальная полнота извлечения по составу заявки, поставке, оплате, ответственности и ограничениям.
    """

    def __init__(self, ai_service: Optional[AiService] = None):
        self.ai_service = ai_service or AiService()
        logger.info("LegalAnalysisService initialized (unified prompt mode).")

    def analyze_tender(
        self,
        files_data: List[Dict[str, Any]],
        tender_id: str = "N/A",
        job_id: str = "N/A",
        callback: Optional[Callable[[str, int, str], None]] = None,
    ) -> Dict[str, Any]:
        logger.info(f"Starting unified legal analysis for tender {tender_id} (Job: {job_id})")

        if callback:
            callback("Подготовка полного контекста документов", 20, "running")

        try:
            start_time = time.time()

            documents_block, context_meta = self._build_documents_block(files_data)
            prompt = PROMPT_UNIFIED_LEGAL_ANALYSIS.replace("__DOCUMENTS__", documents_block)

            if callback:
                callback("Единый юридический анализ ИИ", 55, "running")

            ai_start = time.time()
            response = self.ai_service._call_ai_with_retry(
                self.ai_service.client.models.generate_content,
                contents=prompt,
            )
            ai_end = time.time()

            raw_text = response.text if response else ""
            final_report_markdown = self._normalize_report(raw_text)
            validation = self._validate_report(final_report_markdown)

            if callback:
                callback("Финальная проверка отчета", 85, "running")

            final_status = self._calculate_status(validation, final_report_markdown)
            summary = self._extract_summary(final_report_markdown)
            end_time = time.time()

            log_debug_event({
                "stage": "unified_prompt_analysis",
                "job_id": job_id,
                "tender_id": tender_id,
                "model_name": "unified-legal-prompt-mode",
                "prompt_size": len(prompt),
                "documents_count": context_meta["documents_count"],
                "included_files": context_meta["included_files"],
                "skipped_files": context_meta["skipped_files"],
                "documents_block_size": len(documents_block),
                "raw_model_response": raw_text,
                "final_status": final_status,
                "validation": validation,
                "duration": end_time - start_time,
                "ai_duration": ai_end - ai_start,
            })

            logger.info(
                f"Unified legal analysis finished for tender {tender_id} "
                f"in {end_time - start_time:.2f}s, status={final_status}, report_len={len(final_report_markdown)}"
            )

            return {
                "status": final_status,
                "final_report_markdown": final_report_markdown,
                "summary_notes": summary,
                "cleaned_context_len": len(documents_block),
                "final_report_len": len(final_report_markdown),
                "structured_data": {},
                "extracted_facts": [],
                "merged_facts": {},
            }

        except Exception as e:
            logger.error(f"Unified legal analysis error for tender {tender_id}: {e}", exc_info=True)
            log_debug_event({
                "stage": "unified_prompt_analysis_error",
                "job_id": job_id,
                "tender_id": tender_id,
                "error": str(e),
            })
            return {
                "status": "error",
                "final_report_markdown": f"# Ошибка анализа\n\nПроизошла ошибка при обработке тендера: {str(e)}",
                "summary_notes": "Ошибка анализа.",
                "cleaned_context_len": 0,
                "final_report_len": 0,
                "structured_data": {},
                "extracted_facts": [],
                "merged_facts": {},
                "error_message": str(e),
            }

    def _document_priority(self, filename: str) -> Tuple[int, str]:
        name = (filename or "").lower()

        if "заявк" in name or "инструкц" in name or "состав" in name:
            return (1, name)
        if "проект" in name or "контракт" in name or "договор" in name or "пгк" in name:
            return (2, name)
        if "извещ" in name:
            return (3, name)
        if "описан" in name or "объект" in name or "тех" in name or "тз" in name:
            return (4, name)
        if "нмцк" in name or "обоснован" in name or "смет" in name:
            return (5, name)
        if name.endswith(".xls") or name.endswith(".xlsx"):
            return (6, name)
        return (10, name)

    def _clean_text(self, text: str) -> str:
        if not text:
            return ""
        text = re.sub(r"\s+\n", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r"[ \t]{2,}", " ", text)
        return text.strip()

    def _render_pages(self, file_data: Dict[str, Any]) -> str:
        filename = file_data.get("filename", "Unknown")
        status = file_data.get("status", "ok")
        error_message = file_data.get("error_message", "")
        pages = file_data.get("pages", []) or []
        text = file_data.get("text", "") or ""

        header = f"=== ФАЙЛ: {filename} | STATUS: {status} ===\n"

        if status != "ok":
            fallback_text = self._clean_text(text)
            return (
                header
                + f"ПРЕДУПРЕЖДЕНИЕ ПО ФАЙЛУ: {error_message or status}\n\n"
                + fallback_text
                + "\n"
            )

        if not pages:
            return header + self._clean_text(text) + "\n"

        blocks: List[str] = [header]
        for page in pages:
            page_num = page.get("page_num", "")
            page_text = self._clean_text(page.get("text", "") or "")
            tables = page.get("tables", []) or []

            page_header = f"--- СТРАНИЦА/ЛИСТ: {page_num} ---"
            blocks.append(page_header)

            if page_text:
                blocks.append(page_text)

            if tables:
                blocks.append("--- ТАБЛИЦЫ ---")
                for idx, table_text in enumerate(tables, start=1):
                    table_clean = self._clean_text(table_text or "")
                    if table_clean:
                        blocks.append(f"[ТАБЛИЦА {idx}]")
                        blocks.append(table_clean)

        return "\n\n".join(blocks).strip() + "\n"

    def _build_documents_block(self, files_data: List[Dict[str, Any]], max_total_chars: int = 180000) -> Tuple[str, Dict[str, Any]]:
        sorted_files = sorted(files_data, key=lambda f: self._document_priority(f.get("filename", "")))

        included_files: List[str] = []
        skipped_files: List[Dict[str, Any]] = []
        rendered_parts: List[str] = []
        total_chars = 0

        for file_data in sorted_files:
            filename = file_data.get("filename", "Unknown")
            rendered = self._render_pages(file_data)
            if not rendered.strip():
                skipped_files.append({"filename": filename, "reason": "empty_render"})
                continue

            if total_chars + len(rendered) > max_total_chars:
                skipped_files.append({"filename": filename, "reason": "max_total_chars_exceeded"})
                continue

            rendered_parts.append(rendered)
            included_files.append(filename)
            total_chars += len(rendered)

        block = "\n\n".join(rendered_parts).strip()

        meta = {
            "documents_count": len(included_files),
            "included_files": included_files,
            "skipped_files": skipped_files,
            "total_chars": total_chars,
        }
        return block, meta

    def _normalize_report(self, report_text: str) -> str:
        text = (report_text or "").strip()
        if not text:
            return "# Юридическое заключение по тендеру\n\nОтчет не сформирован."
        if not text.startswith("# Юридическое заключение по тендеру"):
            text = "# Юридическое заключение по тендеру\n\n" + text
        return text

    def _extract_summary(self, markdown: str) -> str:
        if not markdown:
            return ""
        match = re.search(r"##\s*1\..*?(?=\n##\s*2\.|\Z)", markdown, re.DOTALL | re.IGNORECASE)
        if match:
            return match.group(0)[:1000]
        return "Краткое резюме не выделено отдельно."

    def _validate_report(self, report_markdown: str) -> Dict[str, Any]:
        missing_headers = [header for header in REQUIRED_REPORT_HEADERS if header not in report_markdown]

        has_tables = "|" in report_markdown and "---" in report_markdown
        report_len = len(report_markdown)

        critical_markers = {
            "has_bid_docs_section": "## 7. Полный перечень документов, которые должны входить в состав заявки" in report_markdown,
            "has_delivery_docs_section": "## 8. Отдельный перечень документов, предоставляемых при поставке" in report_markdown,
            "has_payment_section": "## 5. Условия оплаты" in report_markdown,
            "has_delivery_acceptance_section": "## 4. Условия поставки и приёмки" in report_markdown,
            "has_liability_section": "## 6. Ответственность сторон" in report_markdown,
            "mentions_unloading": ("разгруз" in report_markdown.lower()),
            "mentions_application_docs": ("заявк" in report_markdown.lower()),
        }

        return {
            "missing_headers": missing_headers,
            "missing_headers_count": len(missing_headers),
            "has_tables": has_tables,
            "report_length": report_len,
            "critical_markers": critical_markers,
        }

    def _calculate_status(self, validation: Dict[str, Any], report_markdown: str) -> str:
        if validation["report_length"] < 2500:
            return "error"

        if not validation["has_tables"]:
            return "partial"

        if validation["missing_headers_count"] >= 3:
            return "partial"

        critical = validation["critical_markers"]
        critical_false_count = sum(1 for _, value in critical.items() if not value)

        if critical_false_count >= 2:
            return "partial"

        return "success"
