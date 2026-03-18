import time
import json
import logging
import re
from typing import List, Dict, Any
from google import genai
from google.genai import types
from .legal_prompts import PROMPT_CONTRACT, PROMPT_OTHER_DOCS

logger = logging.getLogger("LegalAnalysisService")

class LegalAnalysisService:
    def __init__(self, ai_client):
        self.client = ai_client
        self.valid_blocks = [
            "Поставка и приемка", "Оплата", "Ответственность", 
            "Односторонний отказ", "Документы при поставке", 
            "Спорные условия договора", "Документы заявки", 
            "Недопуск/оценка", "Реестры/ограничения", "Спорные условия"
        ]

    def classify_documents(self, files: List[Dict[str, str]]) -> Dict[str, Any]:
        """
        Классифицирует документы на Группу 1 (Контракт) и Группу 2 (Прочее).
        Использует расширенные признаки по имени и тексту.
        """
        group1 = []
        group2 = []
        notes = []
        uncertain_files = []
        
        # Признаки договорной группы
        contract_kw = [
            'контракт', 'договор', 'проект договора', 'проект контракта', 
            'спецификация', 'график поставки', 'условия поставки', 
            'приложение к договору', 'порядок приемки', 'условия оплаты', 
            'ответственность сторон', 'односторонний отказ', 'реквизиты сторон',
            'contract', 'agreement', 'draft', 'specification'
        ]
        
        # Признаки закупочной документации
        other_kw = [
            'извещение', 'информационная карта', 'инструкция', 
            'описание объекта закупки', 'техническое задание', 
            'обоснование нмцк', 'критерии оценки', 'требования к заявке', 
            'форма заявки', 'состав заявки', 'национальный режим', 
            'реестр', 'страна происхождения', 'notice', 'requirements', 'tender'
        ]
        
        for file in files:
            filename = file.get('filename', '').lower()
            text_sample = file.get('text', '')[:3000].lower()
            
            score_contract = 0
            score_other = 0
            reasons = []

            # Проверка по имени
            for kw in contract_kw:
                if kw in filename:
                    score_contract += 2
                    reasons.append(f"имя содержит '{kw}'")
            for kw in other_kw:
                if kw in filename:
                    score_other += 2
                    reasons.append(f"имя содержит '{kw}'")

            # Проверка по тексту
            for kw in contract_kw:
                if kw in text_sample:
                    score_contract += 1
                    reasons.append(f"текст содержит '{kw}'")
            for kw in other_kw:
                if kw in text_sample:
                    score_other += 1
                    reasons.append(f"текст содержит '{kw}'")

            if score_contract > score_other:
                group1.append(file)
                notes.append(f"Файл '{file['filename']}' отнесен к договорной группе ({', '.join(reasons[:2])}).")
            elif score_other > score_contract:
                group2.append(file)
                notes.append(f"Файл '{file['filename']}' отнесен к закупочной документации ({', '.join(reasons[:2])}).")
            else:
                group2.append(file)
                uncertain_files.append(file['filename'])
                notes.append(f"Файл '{file['filename']}' классифицирован как прочая документация (неуверенно).")
                
        return {
            'group1': group1, 
            'group2': group2, 
            'classification_notes': notes,
            'uncertain_files': uncertain_files
        }

    def _call_ai_with_retry(self, prompt: str, retries: int = 1) -> Dict[str, Any]:
        """
        Вызывает ИИ с поддержкой нового формата и legacy-fallback.
        """
        if not self.client:
            return {"rows": [], "summary_notes": ["Ошибка: ИИ-клиент не инициализирован."]}
            
        for attempt in range(retries + 1):
            try:
                # Если это повторная попытка, добавляем жесткую инструкцию
                current_prompt = prompt
                if attempt > 0:
                    current_prompt += "\n\nВАЖНО: Верни строго JSON объект с полями 'rows' (массив объектов) и 'summary_notes' (массив строк). Не пиши ничего кроме JSON."

                response = self.client.models.generate_content(
                    model='gemini-3-flash-preview',
                    contents=current_prompt,
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json",
                        temperature=0.1
                    )
                )
                
                text = response.text.strip()
                # Очистка от markdown если есть
                if text.startswith("```json"):
                    text = text.replace("```json", "", 1).replace("```", "", 1).strip()
                
                data = json.loads(text)
                
                # 1. Целевой формат
                if isinstance(data, dict) and 'rows' in data:
                    return {
                        "rows": data.get("rows", []),
                        "summary_notes": data.get("summary_notes", [])
                    }
                
                # 2. Legacy: массив
                if isinstance(data, list):
                    return {"rows": data, "summary_notes": ["(Legacy) Ответ получен в виде массива."]}
                
                # 3. Legacy: объект risks
                if isinstance(data, dict) and 'risks' in data:
                    return {"rows": data['risks'], "summary_notes": ["(Legacy) Ответ получен в формате risks."]}
                
                logger.warning(f"Unexpected JSON structure on attempt {attempt}: {data}")
                
            except Exception as e:
                logger.error(f"AI Error on attempt {attempt}: {e}")
                if attempt < retries:
                    time.sleep(1)
                else:
                    return {"rows": [], "summary_notes": [f"Техническая ошибка ИИ: {str(e)}"]}
                    
        return {"rows": [], "summary_notes": ["Не удалось получить валидный ответ от ИИ."], "status": "partial"}

    def _normalize_block(self, block: str) -> str:
        block_lower = block.lower()
        for valid in self.valid_blocks:
            if valid.lower() in block_lower or block_lower in valid.lower():
                return valid
        return "Прочее"

    def _validate_and_filter_rows(self, rows: List[Dict[str, Any]], group_name: str) -> List[Dict[str, Any]]:
        valid_rows = []
        for row in rows:
            # Обязательные поля
            if not row.get('source_document') or not row.get('source_reference') or not row.get('finding'):
                continue
            
            # Нормализация и обрезка
            valid_row = {
                "block": self._normalize_block(row.get("block", "Прочее")),
                "finding": str(row.get("finding", ""))[:500],
                "risk_level": row.get("risk_level", "Low"),
                "supplier_action": str(row.get("supplier_action", ""))[:500],
                "source_document": str(row.get("source_document", ""))[:100],
                "source_reference": str(row.get("source_reference", ""))[:100],
                "legal_basis": str(row.get("legal_basis", ""))[:200],
                "doc_group": "contract" if group_name == "contract" else "other"
            }
            
            # Нормализация риска
            risk = str(valid_row["risk_level"]).lower()
            if "high" in risk or "высок" in risk:
                valid_row["risk_level"] = "High"
            elif "medium" in risk or "средн" in risk:
                valid_row["risk_level"] = "Medium"
            else:
                valid_row["risk_level"] = "Low"
                
            valid_rows.append(valid_row)
        return valid_rows

    def _chunk_text(self, text: str, max_chars: int = 12000) -> str:
        """
        Выделяет наиболее важные фрагменты текста для анализа.
        """
        if len(text) <= max_chars:
            return text
            
        # Ищем ключевые слова и берем контекст вокруг них
        keywords = [
            'оплата', 'срок поставки', 'приемка', 'штраф', 'неустойка', 
            'односторонний отказ', 'состав заявки', 'отклонение', 'реестр'
        ]
        
        important_parts = []
        # Всегда берем начало (предмет)
        important_parts.append(text[:2000])
        
        for kw in keywords:
            matches = list(re.finditer(re.escape(kw), text, re.IGNORECASE))
            for m in matches[:2]: # Берем первые 2 вхождения каждого слова
                start = max(0, m.start() - 500)
                end = min(len(text), m.end() + 1500)
                important_parts.append(text[start:end])
        
        # Всегда берем конец (реквизиты/подписи)
        important_parts.append(text[-2000:])
        
        combined = "\n\n... [CHUNK] ...\n\n".join(important_parts)
        return combined[:max_chars]

    def _add_missing_critical_topics(self, rows: List[Dict[str, Any]], doc_group: str) -> List[Dict[str, Any]]:
        """
        Добавляет строки "не найдено" для критически важных тем.
        """
        existing_findings = " ".join([r['finding'].lower() for r in rows])
        added_rows = []
        
        if doc_group == "contract":
            topics = [
                ("разгрузка", "Поставка и приемка", "условие о разгрузке (кто и за чей счет)"),
                ("аванс", "Оплата", "условие о наличии или отсутствии аванса"),
                ("эдо", "Оплата", "условие об использовании ЭДО (электронного документооборота)"),
                ("казначейск", "Оплата", "условие о казначейском сопровождении"),
                ("срок оплаты", "Оплата", "точный срок оплаты"),
                ("односторонний отказ", "Односторонний отказ", "порядок одностороннего отказа"),
                ("приемк", "Поставка и приемка", "перечень документов о приемке (акты, накладные)")
            ]
        else:
            topics = [
                ("реестр", "Реестры/ограничения", "требования о включении в реестры (РФ/ЕАЭС/РРП)"),
                ("национальный режим", "Реестры/ограничения", "применение национального режима (ПП 616/617/878)"),
                ("состав заявки", "Документы заявки", "полный перечень документов в составе заявки")
            ]
            
        for kw, block, label in topics:
            if kw not in existing_findings:
                added_rows.append({
                    "block": block,
                    "finding": f"В просмотренных документах не найдено {label}.",
                    "risk_level": "Medium",
                    "supplier_action": "Проверить наличие условия в полном комплекте документации до подачи заявки или подписания договора.",
                    "source_document": "Не найдено",
                    "source_reference": "Критичное условие не выявлено в просмотренных документах",
                    "legal_basis": "",
                    "doc_group": doc_group
                })
        return added_rows

    def analyze_tender(self, files: List[Dict[str, str]], callback=None) -> Dict[str, Any]:
        """
        Основной метод анализа тендера с этапами и прогрессом.
        """
        def update_stage(stage, progress, status="process"):
            if callback:
                callback(stage, progress, status)

        if not files:
            return {
                "rows": [],
                "summary_notes": ["Ошибка: нет файлов для анализа."],
                "status": "error",
                "stage": "Ошибка",
                "progress": 100
            }
        
        update_stage("Классификация", 30)
        classified = self.classify_documents(files)
        group1 = classified['group1']
        group2 = classified['group2']
        uncertain_files = classified.get('uncertain_files', [])
        
        all_rows = []
        all_notes = classified.get('classification_notes', [])
        
        # 1. Анализ контракта
        if group1:
            update_stage("Анализ договора", 50)
            combined_text = "\n\n".join([f"ФАЙЛ: {f['filename']}\n{f['text']}" for f in group1])
            chunked_text = self._chunk_text(combined_text)
            res = self._call_ai_with_retry(PROMPT_CONTRACT.format(text=chunked_text))
            
            rows = self._validate_and_filter_rows(res.get('rows', []), "contract")
            rows += self._add_missing_critical_topics(rows, "contract")
            all_rows.extend(rows)
            all_notes.extend(res.get('summary_notes', []))
        else:
            update_stage("Анализ договора (пропущено)", 50, "skipped")
            all_notes.append("Проект договора/контракта среди обработанных файлов не найден.")
            # Если контракта нет, все равно добавляем "не найдено" для критических тем контракта
            all_rows += self._add_missing_critical_topics([], "contract")

        # 2. Анализ прочей документации
        if group2:
            update_stage("Анализ остальной документации", 80)
            combined_text = "\n\n".join([f"ФАЙЛ: {f['filename']}\n{f['text']}" for f in group2])
            chunked_text = self._chunk_text(combined_text)
            res = self._call_ai_with_retry(PROMPT_OTHER_DOCS.format(text=chunked_text))
            
            rows = self._validate_and_filter_rows(res.get('rows', []), "other")
            rows += self._add_missing_critical_topics(rows, "other")
            all_rows.extend(rows)
            all_notes.extend(res.get('summary_notes', []))
        else:
            update_stage("Анализ остальной документации (пропущено)", 80, "skipped")
            all_notes.append("Иная закупочная документация среди обработанных файлов не найдена.")
            all_rows += self._add_missing_critical_topics([], "other")

        update_stage("Формирование отчета", 95)
        
        # Пост-обработка
        # 1. Дедупликация с нормализацией
        def normalize(s):
            return re.sub(r'[^\w\s]', '', str(s).lower().strip())

        unique_rows = []
        seen_keys = set()
        for r in all_rows:
            key = f"{normalize(r['block'])}_{normalize(r['finding'])}_{normalize(r['source_document'])}_{normalize(r['source_reference'])}"
            if key not in seen_keys:
                seen_keys.add(key)
                unique_rows.append(r)

        # 2. Сортировка
        risk_order = {"High": 0, "Medium": 1, "Low": 2}
        unique_rows.sort(key=lambda x: (
            risk_order.get(x['risk_level'], 3), 
            x['block'], 
            x['source_document']
        ))

        # 3. Лимит и фильтрация заметок
        final_rows = unique_rows[:30] # Увеличим лимит
        final_notes = []
        seen_notes = set()
        for note in all_notes:
            note_clean = note.strip()
            if note_clean and note_clean not in seen_notes:
                seen_notes.add(note_clean)
                final_notes.append(note_clean)
        
        final_notes = final_notes[:12]

        update_stage("Готово", 100, "success")
        
        return {
            "rows": final_rows,
            "summary_notes": final_notes,
            "has_contract": len(group1) > 0,
            "classification_notes": classified['classification_notes'],
            "uncertain_files": uncertain_files,
            "status": "success" if final_rows else "partial",
            "stage": "Готово",
            "progress": 100
        }
