import json
import re
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from backend.logger import logger, log_debug_event
from .ai_service import AiService
from .legal_prompts import PROMPT_CONTRACT_ANALYSIS, PROMPT_DOCS_ANALYSIS

class LegalAnalysisService:
    def __init__(self, ai_service: Optional[AiService] = None):
        self.ai_service = ai_service or AiService()
        logger.info("LegalAnalysisService initialized (JSON output mode).")

    def analyze_tender(
        self,
        files_data: List[Dict[str, Any]],
        tender_id: str = "N/A",
        job_id: str = "N/A",
        callback: Optional[Callable[[str, int, str], None]] = None,
    ) -> Dict[str, Any]:
        logger.info(f"Starting legal analysis for tender {tender_id} (Job: {job_id})")

        if callback:
            callback("Классификация документов", 10, "running")

        contract_files, other_files = self._classify_documents(files_data)
        has_contract = len(contract_files) > 0

        all_rows = []
        
        if callback:
            callback("Анализ договора", 30, "running")
            
        if contract_files:
            contract_rows = self._run_analysis_prompt(contract_files, PROMPT_CONTRACT_ANALYSIS, "contract")
            all_rows.extend(contract_rows)
            
        if callback:
            callback("Анализ остальной документации", 60, "running")
            
        if other_files:
            other_rows = self._run_analysis_prompt(other_files, PROMPT_DOCS_ANALYSIS, "other")
            all_rows.extend(other_rows)

        if callback:
            callback("Формирование отчета", 90, "running")

        # Deduplicate and sort
        all_rows = self._deduplicate_and_sort(all_rows)
        
        # Calculate summary
        high_risks = sum(1 for r in all_rows if r.get("risk_level") == "High")
        medium_risks = sum(1 for r in all_rows if r.get("risk_level") == "Medium")
        low_risks = sum(1 for r in all_rows if r.get("risk_level") == "Low")
        
        summary_notes = f"Риски: Высоких - {high_risks}, Средних - {medium_risks}, Низких - {low_risks}. "
        summary_notes += "Проект договора найден." if has_contract else "Проект договора НЕ найден."
        
        # File statuses
        file_statuses = []
        unread_count = 0
        for f in files_data:
            status = f.get("status", "ok")
            if status != "ok":
                unread_count += 1
            file_statuses.append({
                "filename": f.get("filename", "Unknown"),
                "status": status,
                "message": f.get("error_message", "")
            })

        if unread_count > 0:
            summary_notes += f" Проблемных файлов: {unread_count}."

        return {
            "status": "success" if all_rows else "error",
            "summary_notes": summary_notes,
            "rows": all_rows,
            "has_contract": has_contract,
            "file_statuses": file_statuses,
            "unread_files_count": unread_count
        }

    def _classify_documents(self, files_data: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        contract_files = []
        other_files = []
        
        contract_keywords = ["проект контракта", "проект договора", "контракт", "договор", "приложение к договору", "спецификация", "график поставки", "условия поставки"]
        
        for f in files_data:
            filename = (f.get("filename", "")).lower()
            text = (f.get("text", "")).lower()[:1000] # Check first 1000 chars
            
            is_contract = False
            for kw in contract_keywords:
                if kw in filename or kw in text:
                    is_contract = True
                    break
            
            if is_contract:
                contract_files.append(f)
            else:
                other_files.append(f)
                
        return contract_files, other_files

    def _run_analysis_prompt(self, files_data: List[Dict[str, Any]], prompt_template: str, doc_group: str) -> List[Dict[str, Any]]:
        documents_block, _ = self._build_documents_block(files_data)
        prompt = prompt_template.replace("__DOCUMENTS__", documents_block)
        
        for attempt in range(3):
            try:
                response = self.ai_service._call_ai_with_retry(
                    self.ai_service.client.models.generate_content,
                    contents=prompt,
                )
                raw_text = response.text if response else ""
                
                # Extract JSON array
                match = re.search(r'\[.*\]', raw_text, re.DOTALL)
                if match:
                    json_str = match.group(0)
                    rows = json.loads(json_str)
                    
                    valid_rows = []
                    for row in rows:
                        if not isinstance(row, dict):
                            continue
                        if not row.get("source_document") or not row.get("source_reference"):
                            continue
                        
                        valid_row = {
                            "block": str(row.get("block", "Общее")),
                            "finding": str(row.get("finding", "")),
                            "risk_level": str(row.get("risk_level", "Info")),
                            "supplier_action": str(row.get("supplier_action", "")),
                            "source_document": str(row.get("source_document", "")),
                            "source_reference": str(row.get("source_reference", "")),
                            "legal_basis": str(row.get("legal_basis", "")),
                            "doc_group": doc_group
                        }
                        valid_rows.append(valid_row)
                    return valid_rows
            except Exception as e:
                logger.warning(f"JSON parsing failed on attempt {attempt+1}: {e}")
                
        return []

    def _deduplicate_and_sort(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        seen = set()
        unique_rows = []
        for r in rows:
            key = (r["block"], r["finding"], r["source_document"], r["source_reference"])
            if key not in seen:
                seen.add(key)
                unique_rows.append(r)
                
        risk_order = {"High": 1, "Medium": 2, "Low": 3, "Info": 4}
        unique_rows.sort(key=lambda x: (risk_order.get(x.get("risk_level"), 5), x.get("block", "")))
        return unique_rows

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
        included_files: List[str] = []
        skipped_files: List[Dict[str, Any]] = []
        rendered_parts: List[str] = []
        total_chars = 0

        for file_data in files_data:
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
