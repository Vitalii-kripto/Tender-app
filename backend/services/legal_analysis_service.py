import time
import json
import logging
from typing import List, Dict, Any
from google import genai
from google.genai import types
from .legal_prompts import PROMPT_CONTRACT, PROMPT_OTHER_DOCS

logger = logging.getLogger("LegalAnalysisService")

class LegalAnalysisService:
    def __init__(self, ai_client):
        self.client = ai_client

    def classify_documents(self, files: List[Dict[str, str]]) -> Dict[str, Any]:
        """
        Classifies documents into Group 1 (Contract) and Group 2 (Other).
        files: list of dicts with 'filename' and 'text'
        """
        group1 = []
        group2 = []
        notes = []
        
        contract_filename_kw = ['контракт', 'договор', 'соглашение', 'приложение', 'спецификация', 'проект', 'смета', 'график', 'акт']
        contract_text_kw = ['предмет контракта', 'предмет договора', 'цена контракта', 'порядок расчетов', 'ответственность сторон', 'срок поставки', 'место поставки']
        
        other_filename_kw = ['извещение', 'информационная карта', 'инструкция', 'описание объекта', 'обоснование', 'требования', 'заявка', 'тз', 'техническое задание']
        other_text_kw = ['информационная карта', 'требования к участникам', 'критерии оценки', 'порядок подачи', 'обеспечение заявки', 'национальный режим']
        
        for file in files:
            filename = file['filename'].lower()
            text_sample = file['text'][:3000].lower()
            
            # Check filename first
            if any(kw in filename for kw in contract_filename_kw):
                group1.append(file)
                notes.append(f"Файл '{file['filename']}' отнесен к проекту договора (по имени).")
                continue
            if any(kw in filename for kw in other_filename_kw):
                group2.append(file)
                notes.append(f"Файл '{file['filename']}' отнесен к прочей документации (по имени).")
                continue
                
            # Check text
            if any(kw in text_sample for kw in contract_text_kw):
                group1.append(file)
                notes.append(f"Файл '{file['filename']}' отнесен к проекту договора (по тексту).")
                continue
            if any(kw in text_sample for kw in other_text_kw):
                group2.append(file)
                notes.append(f"Файл '{file['filename']}' отнесен к прочей документации (по тексту).")
                continue
                
            # Unsure
            group2.append(file)
            notes.append(f"Файл '{file['filename']}' классифицирован неуверенно, отнесен к прочей документации.")
                
        return {'group1': group1, 'group2': group2, 'notes': notes}

    def _call_ai_with_retry(self, prompt: str, retries: int = 3) -> Dict[str, Any]:
        if not self.client:
            logger.error("AI Client is not initialized.")
            return {"rows": [], "summary_notes": []}
            
        for attempt in range(retries + 1):
            try:
                response = self.client.models.generate_content(
                    model='gemini-3-flash-preview',
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json",
                        temperature=0.1
                    )
                )
                data = json.loads(response.text)
                
                if isinstance(data, dict) and 'rows' in data:
                    return {
                        "rows": data.get("rows", []),
                        "summary_notes": data.get("summary_notes", [])
                    }
                elif isinstance(data, list):
                    return {"rows": data, "summary_notes": []}
                elif isinstance(data, dict) and 'risks' in data:
                    return {"rows": data['risks'], "summary_notes": []}
                else:
                    logger.warning(f"Unexpected JSON structure on attempt {attempt}: {data}")
            except Exception as e:
                logger.error(f"AI Error on attempt {attempt}: {e}")
                if attempt < retries:
                    wait_time = (2 ** attempt) + 1
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    return {"rows": [], "summary_notes": []}
        return {"rows": [], "summary_notes": []}

    def _validate_and_filter_rows(self, rows: List[Dict[str, Any]], group_name: str) -> List[Dict[str, Any]]:
        valid_rows = []
        for row in rows:
            if not row.get('source_document') or not row.get('source_reference'):
                continue
            
            valid_row = {
                "block": row.get("block", "Прочее"),
                "finding": row.get("finding", ""),
                "risk_level": row.get("risk_level", "Low"),
                "supplier_action": row.get("supplier_action", ""),
                "source_document": row.get("source_document", ""),
                "source_reference": row.get("source_reference", ""),
                "legal_basis": row.get("legal_basis", ""),
                "doc_group": group_name
            }
            # Normalize risk level
            risk = valid_row["risk_level"].lower()
            if "high" in risk or "высок" in risk:
                valid_row["risk_level"] = "High"
            elif "medium" in risk or "средн" in risk:
                valid_row["risk_level"] = "Medium"
            else:
                valid_row["risk_level"] = "Low"
                
            valid_rows.append(valid_row)
        return valid_rows

    def _chunk_text(self, text: str, max_chars: int = 12000) -> str:
        if len(text) <= max_chars:
            return text
            
        keywords = ['оплат', 'ответственност', 'поставк', 'приемк', 'заявк', 'ограничен', 'критери', 'оценк', 'штраф', 'пени', 'расторжен']
        
        result = text[:4000]
        remaining_chars = max_chars - 4000
        
        if remaining_chars > 0:
            for kw in keywords:
                if remaining_chars <= 0:
                    break
                idx = text.lower().find(kw, 4000)
                if idx != -1:
                    start = max(4000, idx - 500)
                    end = min(len(text), idx + 1500)
                    chunk = text[start:end]
                    result += "\n...\n" + chunk
                    remaining_chars -= len(chunk)
                    
        return result[:max_chars]

    def analyze_tender(self, files: List[Dict[str, str]]) -> Dict[str, Any]:
        if not files:
            return {
                "rows": [],
                "summary_notes": ["Техническая ошибка: нет файлов для анализа."],
                "has_contract": False
            }

        classified = self.classify_documents(files)
        group1 = classified['group1']
        group2 = classified['group2']
        
        all_rows = []
        summary_notes = classified['notes']
        
        if not group1 and not group2:
             return {
                "rows": [],
                "summary_notes": ["Техническая ошибка: документы пусты или нечитаемы."],
                "has_contract": False
            }
        
        if not group1:
            summary_notes.append("Проект контракта отсутствует в документации.")
        else:
            text1 = "\n\n".join([f"--- ФАЙЛ: {f['filename']} ---\n{self._chunk_text(f['text'])}" for f in group1])
            prompt1 = PROMPT_CONTRACT.replace("{text}", text1)
            res1 = self._call_ai_with_retry(prompt1)
            all_rows.extend(self._validate_and_filter_rows(res1.get("rows", []), "contract"))
            summary_notes.extend(res1.get("summary_notes", []))
            
        if not group2:
            summary_notes.append("Иная документация (извещение, инструкции) отсутствует.")
        else:
            text2 = "\n\n".join([f"--- ФАЙЛ: {f['filename']} ---\n{self._chunk_text(f['text'])}" for f in group2])
            prompt2 = PROMPT_OTHER_DOCS.replace("{text}", text2)
            res2 = self._call_ai_with_retry(prompt2)
            all_rows.extend(self._validate_and_filter_rows(res2.get("rows", []), "other"))
            summary_notes.extend(res2.get("summary_notes", []))
            
        # Deduplicate rows by block + finding + source_document + source_reference
        seen = set()
        unique_rows = []
        for row in all_rows:
            key = (row.get('block', ''), row.get('finding', ''), row.get('source_document', ''), row.get('source_reference', ''))
            if key not in seen:
                seen.add(key)
                unique_rows.append(row)
                
        # Sort by risk level (High > Medium > Low) then by block
        risk_order = {"High": 0, "Medium": 1, "Low": 2}
        unique_rows.sort(key=lambda x: (risk_order.get(x['risk_level'], 3), x['block']))
        
        # Limit to 24 rows
        unique_rows = unique_rows[:24]

        # Deduplicate summary notes
        unique_notes = []
        seen_notes = set()
        for note in summary_notes:
            if note not in seen_notes:
                seen_notes.add(note)
                unique_notes.append(note)
        
        return {
            "rows": unique_rows,
            "summary_notes": unique_notes,
            "has_contract": len(group1) > 0
        }
