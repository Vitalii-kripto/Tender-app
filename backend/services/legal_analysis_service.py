import time
import json
import logging
import re
import os
from typing import List, Dict, Any
from google import genai
from google.genai import types
from .legal_prompts import PROMPT_FULL_PACKAGE

logger = logging.getLogger("LegalAnalysisService")

class LegalAnalysisService:
    def __init__(self, ai_client):
        self.client = ai_client
        self.valid_blocks = [
            "Риски участия и исполнения", "Недопуск/оценка", "Проверка соответствия", 
            "Поставка и приемка", "Оплата", "Ответственность", 
            "Документы заявки", "Документы при поставке", 
            "Реестры/ограничения", "Рекомендации поставщику"
        ]
        self.block_normalization = {
            "риски участия": "Риски участия и исполнения",
            "риски исполнения": "Риски участия и исполнения",
            "юридические риски": "Риски участия и исполнения",
            "финансовые риски": "Риски участия и исполнения",
            "операционные риски": "Риски участия и исполнения",
            "административные риски": "Риски участия и исполнения",
            "репутационные риски": "Риски участия и исполнения",
            "отклонение": "Недопуск/оценка",
            "недопуск": "Недопуск/оценка",
            "потеря баллов": "Недопуск/оценка",
            "критерии оценки": "Недопуск/оценка",
            "оценка": "Недопуск/оценка",
            "несоответствие": "Проверка соответствия",
            "противоречия": "Проверка соответствия",
            "ошибки документации": "Проверка соответствия",
            "рисковые формулировки": "Проверка соответствия",
            "поставка": "Поставка и приемка",
            "приемка": "Поставка и приемка",
            "условия поставки": "Поставка и приемка",
            "разгрузка": "Поставка и приемка",
            "доставка": "Поставка и приемка",
            "расчеты": "Оплата",
            "условия оплаты": "Оплата",
            "аванс": "Оплата",
            "эдо": "Оплата",
            "казначейское сопровождение": "Оплата",
            "штрафы": "Ответственность",
            "пени": "Ответственность",
            "неустойка": "Ответственность",
            "односторонний отказ": "Ответственность",
            "расторжение": "Ответственность",
            "санкции": "Ответственность",
            "состав заявки": "Документы заявки",
            "требования к заявке": "Документы заявки",
            "сопроводительные документы": "Документы при поставке",
            "приемочные документы": "Документы при поставке",
            "нацрежим": "Реестры/ограничения",
            "национальный режим": "Реестры/ограничения",
            "реестр": "Реестры/ограничения",
            "ограничения": "Реестры/ограничения",
            "преференции": "Реестры/ограничения",
            "условие допуска": "Реестры/ограничения",
            "запрет": "Реестры/ограничения",
            "рекомендации": "Рекомендации поставщику",
            "что сделать поставщику": "Рекомендации поставщику"
        }
        # Настройка логирования в файл
        if not os.path.exists('backend/logs'):
            os.makedirs('backend/logs')
        
        logger.setLevel(logging.INFO)
        if not logger.handlers:
            file_handler = logging.FileHandler('backend/logs/legal_ai.log')
            file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
            logger.addHandler(file_handler)

    def _assemble_prompt(self, template: str, text: str, prompt_type: str) -> str:
        """
        Безопасно собирает промпт, заменяя __TEXT__ на текст документа.
        """
        try:
            if "__TEXT__" not in template:
                logger.error(f"Prompt template for {prompt_type} missing __TEXT__ placeholder")
                return None
            
            assembled = template.replace("__TEXT__", text)
            
            # Защитная проверка: если остались маркеры, значит что-то пошло не так
            if "__TEXT__" in assembled:
                logger.error(f"Prompt assembly failed for {prompt_type}: placeholder still present")
                return None
                
            return assembled
        except Exception as e:
            logger.error(f"Error assembling prompt for {prompt_type}: {e}")
            return None

    def classify_documents(self, files: List[Dict[str, str]]) -> Dict[str, Any]:
        """
        Техническая классификация документов для UI и логов.
        Определяет наличие контракта и формирует заметки.
        """
        has_contract = False
        classification_notes = []
        uncertain_files = []
        
        contract_keywords = ['договор', 'контракт', 'проект', 'соглашение']
        
        for f in files:
            filename = f.get('filename', '').lower()
            is_contract = any(kw in filename for kw in contract_keywords)
            
            if is_contract:
                has_contract = True
                classification_notes.append(f"Найден проект договора: {f['filename']}")
            else:
                uncertain_files.append(f['filename'])
                
        if not has_contract:
            classification_notes.append("Внимание: Проект договора не найден. Анализ может быть неполным.")
            
        return {
            "has_contract": has_contract,
            "classification_notes": classification_notes,
            "uncertain_files": uncertain_files
        }

    def _call_ai_with_retry(self, prompt: str, prompt_type: str, retries: int = 1) -> Dict[str, Any]:
        """
        Вызывает ИИ с поддержкой нового формата и legacy-fallback.
        """
        if not self.client:
            return {"rows": [], "summary_notes": ["Ошибка: ИИ-клиент не инициализирован."]}
            
        # Логирование перед вызовом
        logger.info(f"Calling AI with prompt type: {prompt_type}, context size: {len(prompt)}")
            
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
                # Логирование ответа
                logger.info(f"первые 2000 символов ответа модели: {text[:2000]}")
                
                # Очистка от markdown если есть
                if text.startswith("```json"):
                    text = text.replace("```json", "", 1).replace("```", "", 1).strip()
                elif text.startswith("```"):
                    text = text.replace("```", "", 1).replace("```", "", 1).strip()
                
                try:
                    data = json.loads(text)
                    # Логирование структуры JSON
                    keys = list(data.keys())
                    rows_count = len(data.get("rows", [])) if isinstance(data, dict) else 0
                    summary_notes_count = len(data.get("summary_notes", [])) if isinstance(data, dict) else 0
                    logger.info(f"Parsed JSON structure: keys={keys}, rows_count={rows_count}, summary_notes_count={summary_notes_count}")
                except json.JSONDecodeError as e:
                    logger.error(f"JSON Decode Error on attempt {attempt}: {e}")
                    logger.error(f"Raw AI Response:\n{text}")
                    if attempt == retries:
                        from fastapi import HTTPException
                        raise HTTPException(status_code=500, detail=f"AI returned invalid JSON: {e}. Raw response: {text}")
                    time.sleep(1)
                    continue
                
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
                from fastapi import HTTPException
                if isinstance(e, HTTPException):
                    raise e
                logger.error(f"AI Error on attempt {attempt}: {e}")
                if attempt < retries:
                    time.sleep(1)
                else:
                    return {"rows": [], "summary_notes": [f"Техническая ошибка ИИ: {str(e)}"]}
                    
        return {"rows": [], "summary_notes": ["Не удалось получить валидный ответ от ИИ."], "status": "partial"}

    def _validate_and_filter_rows(self, rows: List[Dict[str, Any]], group_name: str) -> List[Dict[str, Any]]:
        valid_rows = []
        for row in rows:
            if not isinstance(row, dict):
                logger.warning(f"причины отбраковки строк: строка не является словарем. Row: {row}")
                continue
            
            # Обязательные поля
            block = str(row.get("block", "")).strip()
            finding = str(row.get("finding", "")).strip()
            risk_level = str(row.get("risk_level", "Medium")).strip()
            supplier_action = str(row.get("supplier_action", "")).strip()
            source_document = str(row.get("source_document", "")).strip()
            source_reference = str(row.get("source_reference", "")).strip()
            legal_basis = str(row.get("legal_basis", "")).strip()
            
            # Валидация
            if not block:
                logger.warning(f"причины отбраковки строк: отсутствует block. Row: {row}")
                continue
            if not finding:
                logger.warning(f"причины отбраковки строк: отсутствует finding. Row: {row}")
                continue
            if not supplier_action:
                logger.warning(f"Row missing supplier_action (will use default): {row}")
            if not source_document:
                logger.warning(f"причины отбраковки строк: отсутствует source_document. Row: {row}")
                continue
            if not source_reference:
                logger.warning(f"причины отбраковки строк: отсутствует source_reference. Row: {row}")
                continue
            
            # Нормализация
            normalized_block = block.lower()
            for key, value in self.block_normalization.items():
                if key in normalized_block:
                    block = value
                    break
            
            if block not in self.valid_blocks:
                logger.warning(f"причины отбраковки строк: неизвестный блок '{block}'. Row: {row}")
                continue
            
            if risk_level not in ["High", "Medium", "Low"]:
                logger.warning(f"Unknown risk_level: {risk_level}, defaulting to Medium")
                risk_level = "Medium"
            
            valid_row = {
                "block": block,
                "finding": finding[:1000] if finding else "Нет описания",
                "risk_level": risk_level,
                "supplier_action": supplier_action if supplier_action else "Проверить условие по первоисточнику документа и учесть его при подготовке заявки или исполнении договора.",
                "source_document": source_document[:200],
                "source_reference": source_reference[:200],
                "legal_basis": legal_basis[:1000] if legal_basis else "",
                "doc_group": group_name
            }
            
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
        existing_text = " ".join([f"{r.get('block', '')} {r.get('finding', '')}".lower() for r in rows])
        added_rows = []
        
        # В новой архитектуре всегда doc_group == "full"
        topics = [
            ("разгрузка", "Поставка и приемка", "условие о разгрузке"),
            ("сроки приемки", "Поставка и приемка", "сроки приемки"),
            ("основания отказа в приемке", "Поставка и приемка", "основания отказа в приемке"),
            ("срок оплаты", "Оплата", "срок оплаты"),
            ("аванс", "Оплата", "условие об авансе"),
            ("эдо", "Оплата", "условие об ЭДО"),
            ("казначейск", "Оплата", "условие о казначейском сопровождении"),
            ("документы при поставке", "Документы при поставке", "документы при поставке"),
            ("состав заявки", "Документы заявки", "полный перечень документов в составе заявки"),
            ("реестр", "Реестры/ограничения", "требования о включении в реестры"),
            ("нацрежим", "Реестры/ограничения", "применение национального режима"),
            ("ограничения", "Реестры/ограничения", "ограничения"),
            ("противоречия", "Проверка соответствия", "противоречия"),
            ("ошибки документации", "Проверка соответствия", "ошибки документации"),
            ("действия поставщику", "Рекомендации поставщику", "рекомендованные действия поставщику")
        ]
            
        for kw, block, label in topics:
            if kw not in existing_text:
                added_rows.append({
                    "block": block,
                    "finding": f"В просмотренных документах не найдено {label}.",
                    "risk_level": "Medium",
                    "supplier_action": "Проверить условие по первоисточнику документа и учесть его при подготовке заявки или исполнении договора.",
                    "source_document": "Не найдено",
                    "source_reference": "Критичное условие не выявлено в просмотренных документах",
                    "legal_basis": "",
                    "doc_group": "full"
                })
        return added_rows

    def analyze_full_package(self, files: List[Dict[str, str]], callback=None) -> Dict[str, Any]:
        """
        Основной метод анализа всего пакета тендерной документации.
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
        
        filenames = [f.get('filename', 'unknown') for f in files]
        logger.info(f"prompt_type=full")
        logger.info(f"количество документов: {len(files)}")
        logger.info(f"список имен файлов: {filenames}")
        
        update_stage("Классификация", 10)
        
        # Техническая классификация для логов и UI
        classified = self.classify_documents(files)
        has_contract = classified["has_contract"]
        classification_notes = classified["classification_notes"]
        uncertain_files = classified["uncertain_files"]
        
        file_statuses = [{"filename": f["filename"], "status": "processed"} for f in files]
        
        update_stage("Анализ документации", 30)

        # Собираем все документы в один контекст
        all_text = ""
        for f in files:
            all_text += f"=== ДОКУМЕНТ: {f['filename']} ===\n{f['text']}\n=== КОНЕЦ ДОКУМЕНТА ===\n\n"
        
        chunked_text = self._chunk_text(all_text)
        logger.info(f"размер общего контекста: {len(chunked_text)} символов")
        
        # Используем новый промпт для всего пакета
        assembled_prompt = self._assemble_prompt(PROMPT_FULL_PACKAGE, chunked_text, "full")
        if not assembled_prompt:
            res = {"rows": [], "summary_notes": ["Ошибка формирования текста промпта для ИИ-анализа."]}
        else:
            res = self._call_ai_with_retry(assembled_prompt, prompt_type="full")
        
        rows = res.get('rows', [])
        logger.info(f"число строк до валидации: {len(rows)}")
        rows = self._validate_and_filter_rows(rows, "full")
        logger.info(f"число строк после валидации: {len(rows)}")
        rows += self._add_missing_critical_topics(rows, "full")
        logger.info(f"число строк после автодобавления критичных тем: {len(rows)}")
        
        all_rows = rows
        all_notes = res.get('summary_notes', [])
        
        update_stage("Формирование отчета", 95)
        
        # Пост-обработка
        # 1. Дедупликация с нормализацией
        def normalize(s):
            return re.sub(r'[^\w\s]', '', str(s).lower().strip())

        unique_rows = []
        seen_keys = set()
        for r in all_rows:
            # Защитная проверка
            if not all(k in r for k in ['block', 'finding', 'source_document', 'source_reference']):
                logger.warning(f"причины отбраковки строк: отсутствуют обязательные ключи для дедупликации. Row: {r}")
                continue
                
            key = f"{normalize(r['block'])}_{normalize(r['finding'])}_{normalize(r['source_document'])}_{normalize(r['source_reference'])}"
            if key not in seen_keys:
                seen_keys.add(key)
                unique_rows.append(r)
        
        logger.info(f"число строк после дедупликации: {len(unique_rows)}")

        # 2. Сортировка
        risk_order = {"High": 0, "Medium": 1, "Low": 2}
        unique_rows.sort(key=lambda x: (
            risk_order.get(x.get('risk_level', 'Medium'), 3), 
            x.get('block', ''), 
            x.get('source_document', '')
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
        
        final_notes = final_notes[:5]

        update_stage("Готово", 100, "success")
        
        return {
            "rows": final_rows,
            "summary_notes": final_notes,
            "has_contract": has_contract,
            "classification_notes": classification_notes,
            "file_statuses": file_statuses,
            "uncertain_files": uncertain_files,
            "status": "success" if final_rows else "partial",
            "stage": "Готово",
            "progress": 100
        }

    def analyze_tender(self, files: List[Dict[str, str]], callback=None) -> Dict[str, Any]:
        """
        Legacy wrapper for analyze_full_package.
        """
        return self.analyze_full_package(files, callback)

