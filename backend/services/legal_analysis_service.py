import os
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

    def classify_documents(self, files: List[Dict[str, str]]) -> Dict[str, List[Dict[str, str]]]:
        """
        Classifies documents into Group 1 (Contract) and Group 2 (Other).
        files: list of dicts with 'filename' and 'text'
        """
        group1 = []
        group2 = []
        
        contract_keywords = ['контракт', 'договор', 'соглашение', 'приложение', 'спецификация', 'проект']
        
        for file in files:
            filename = file['filename'].lower()
            if any(kw in filename for kw in contract_keywords):
                group1.append(file)
            else:
                group2.append(file)
                
        return {'group1': group1, 'group2': group2}

    def _call_ai_with_retry(self, prompt: str, retries: int = 2) -> List[Dict[str, Any]]:
        if not self.client:
            logger.error("AI Client is not initialized.")
            return []
            
        for attempt in range(retries + 1):
            try:
                response = self.client.models.generate_content(
                    model='gemini-2.5-flash',
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json",
                        temperature=0.2
                    )
                )
                data = json.loads(response.text)
                if isinstance(data, list):
                    return data
                elif isinstance(data, dict) and 'risks' in data:
                    return data['risks']
                else:
                    logger.warning(f"Unexpected JSON structure on attempt {attempt}: {data}")
            except Exception as e:
                logger.error(f"AI Error on attempt {attempt}: {e}")
                if attempt == retries:
                    return []
        return []

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

    def analyze_tender(self, files: List[Dict[str, str]]) -> Dict[str, Any]:
        classified = self.classify_documents(files)
        group1 = classified['group1']
        group2 = classified['group2']
        
        all_rows = []
        summary_notes = []
        
        if not group1:
            summary_notes.append("Проект контракта отсутствует в документации.")
        else:
            text1 = "\n\n".join([f"--- ФАЙЛ: {f['filename']} ---\n{f['text'][:15000]}" for f in group1])
            prompt1 = PROMPT_CONTRACT.replace("{text}", text1)
            rows1 = self._call_ai_with_retry(prompt1)
            all_rows.extend(self._validate_and_filter_rows(rows1, "contract"))
            
        if not group2:
            summary_notes.append("Иная документация (извещение, инструкции) отсутствует.")
        else:
            text2 = "\n\n".join([f"--- ФАЙЛ: {f['filename']} ---\n{f['text'][:15000]}" for f in group2])
            prompt2 = PROMPT_OTHER_DOCS.replace("{text}", text2)
            rows2 = self._call_ai_with_retry(prompt2)
            all_rows.extend(self._validate_and_filter_rows(rows2, "other"))
            
        # Deduplicate and sort
        seen = set()
        unique_rows = []
        for row in all_rows:
            key = (row['finding'], row['source_reference'])
            if key not in seen:
                seen.add(key)
                unique_rows.append(row)
                
        # Sort by risk level (High > Medium > Low) then by block
        risk_order = {"High": 0, "Medium": 1, "Low": 2}
        unique_rows.sort(key=lambda x: (risk_order.get(x['risk_level'], 3), x['block']))
        
        return {
            "rows": unique_rows,
            "summary_notes": summary_notes,
            "has_contract": len(group1) > 0
        }
