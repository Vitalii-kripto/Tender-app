import time
import json
import logging
import re
import os
from datetime import datetime
from typing import List, Dict, Any
from google import genai
from google.genai import types
from .legal_prompts import PROMPT_FULL_PACKAGE
from .evidence_collector import EvidenceCollector

from dotenv import load_dotenv

# Загружаем переменные окружения (.env)
env_loaded = load_dotenv()

# Настройка логгера
def setup_legal_logger():
    env_debug_val = os.environ.get('LEGAL_AI_DEBUG', 'false')
    debug_mode = env_debug_val.lower() == 'true'
    
    loggers = [logging.getLogger("LegalAnalysisService"), logging.getLogger("EvidenceCollector")]
    
    log_dir = os.path.join(os.getcwd(), 'backend', 'logs')
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, 'legal_ai.log')
    
    # Используем 'a' (append) и utf-8
    file_handler = logging.FileHandler(log_file, encoding='utf-8', mode='a')
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    
    # Также в консоль
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    
    for l in loggers:
        l.setLevel(logging.DEBUG if debug_mode else logging.INFO)
        if l.hasHandlers():
            l.handlers.clear()
        l.addHandler(file_handler)
        l.addHandler(stream_handler)
        l.propagate = False
        
    logger = logging.getLogger("LegalAnalysisService")
    
    # Стартовые логи для проверки .env
    logger.info(f"--- [ENV INITIALIZATION] ---")
    logger.info(f".env file found and loaded: {env_loaded}")
    logger.info(f"LEGAL_AI_DEBUG from env: '{env_debug_val}'")
    logger.info(f"Actual DEBUG_MODE: {debug_mode}")
    logger.info(f"----------------------------")
    
    return logger, debug_mode

logger, DEBUG_MODE = setup_legal_logger()

class LegalAnalysisService:
    def __init__(self, ai_client):
        self.client = ai_client
        self.debug_mode = DEBUG_MODE
        self.evidence_collector = EvidenceCollector()
        
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
        logger.info(f"LegalAnalysisService initialized. Debug mode: {self.debug_mode}")

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
        file_classifications = []
        
        contract_signals = [
            "проект контракта", "проект договора", "контракт", "договор",
            "поставщик", "заказчик", "предмет контракта", "цена контракта",
            "порядок расчетов", "срок оплаты", "условия оплаты", "поставка товара",
            "приемка товара", "документ о приемке", "ответственность сторон",
            "неустойка", "штраф", "пеня", "односторонний отказ",
            "расторжение контракта", "реквизиты сторон"
        ]
        
        procurement_signals = [
            "извещение", "описание объекта закупки", "техническое задание",
            "инструкция по заполнению заявки", "требования к содержанию заявки",
            "состав заявки", "критерии оценки", "порядок рассмотрения заявок",
            "основания отклонения", "участник закупки", "национальный режим",
            "реестровый номер", "страна происхождения", "обоснование нмцк",
            "начальная максимальная цена", "условия допуска", "ограничение допуска",
            "преимущества", "запрет", "характеристики товара"
        ]

        strong_contract_signals = ["проект контракта", "проект договора", "цена контракта", "ответственность сторон", "порядок расчетов"]
        strong_procurement_signals = ["извещение", "требования к содержанию заявки", "инструкция по заполнению заявки"]

        for f in files:
            filename = f.get('filename', 'unknown')
            text = f.get('text', '')
            
            if not text or len(text.strip()) < 100:
                logger.info(f"classification skipped: no extracted text for {filename}")
                file_classifications.append({
                    "filename": filename,
                    "category": "unclassified_due_to_no_text",
                    "contract_score": 0,
                    "procurement_score": 0,
                    "matched_contract_signals": [],
                    "matched_procurement_signals": [],
                    "classification_reason": "Текст слишком короткий или не извлечен"
                })
                continue

            text_to_analyze = text[:15000].lower()
            
            c_score = 0
            p_score = 0
            matched_c = []
            matched_p = []
            
            for sig in contract_signals:
                if sig in text_to_analyze:
                    weight = 3 if sig in strong_contract_signals else 1
                    c_score += weight
                    matched_c.append(sig)
                    
            for sig in procurement_signals:
                if sig in text_to_analyze:
                    weight = 3 if sig in strong_procurement_signals else 1
                    p_score += weight
                    matched_p.append(sig)

            if c_score >= 3 and p_score < 3:
                category = "contract"
                reason = f"Найдено {len(matched_c)} договорных признаков: {', '.join(matched_c[:3])}..."
            elif p_score >= 3 and c_score < 3:
                category = "procurement"
                reason = f"Найдено {len(matched_p)} закупочных признаков: {', '.join(matched_p[:3])}..."
            elif c_score >= 3 and p_score >= 3:
                category = "mixed"
                reason = f"Файл содержит признаки обоих типов (договорные: {len(matched_c)}, закупочные: {len(matched_p)}), поэтому помечен как mixed"
            else:
                category = "unclassified"
                reason = "Текст слишком короткий или признаки не обнаружены"

            if any(sig in text_to_analyze for sig in strong_contract_signals):
                has_contract = True

            classification_data = {
                "filename": filename,
                "category": category,
                "contract_score": c_score,
                "procurement_score": p_score,
                "matched_contract_signals": matched_c,
                "matched_procurement_signals": matched_p,
                "classification_reason": reason
            }
            file_classifications.append(classification_data)
            
            logger.info(f"Classified {filename} (len: {len(text)}): category={category}, c_score={c_score}, p_score={p_score}, matched_c={matched_c}, matched_p={matched_p}, reason={reason}")

        classification_notes = []
        if not has_contract:
            classification_notes.append("Внимание: Явные признаки проекта договора не найдены в текстах документов. Анализ может быть неполным.")
            
        return {
            "has_contract": has_contract,
            "classification_notes": classification_notes,
            "file_classifications": file_classifications
        }

    def _call_ai_with_retry(self, prompt: str, prompt_type: str, tender_id: str = "unknown", filenames: List[str] = None, retries: int = 1) -> Dict[str, Any]:
        """
        Вызывает ИИ с поддержкой нового формата и legacy-fallback.
        """
        if not self.client:
            return {"rows": [], "summary_notes": ["Ошибка: ИИ-клиент не инициализирован."]}
            
        start_time = datetime.now()
        model_name = 'gemini-3-flash-preview'
        
        # Логирование перед вызовом
        logger.info(f"===== [AI REQUEST START] =====")
        logger.info(f"Tender ID: {tender_id}")
        logger.info(f"Prompt Type: {prompt_type}")
        logger.info(f"Model Name: {model_name}")
        logger.info(f"Files: {filenames if filenames else 'N/A'}")
        logger.info(f"Context Size: {len(prompt)} characters")
        logger.info(f"Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        if self.debug_mode:
            logger.info("--- [FULL ASSEMBLED PROMPT] ---")
            logger.info(prompt)
            logger.info("--- [END OF PROMPT] ---")
        
        logger.info(f"===== [AI REQUEST END] =====")
            
        for attempt in range(retries + 1):
            try:
                # Если это повторная попытка, добавляем жесткую инструкцию
                current_prompt = prompt
                if attempt > 0:
                    current_prompt += "\n\nВАЖНО: Верни строго JSON объект с полями 'rows' (массив объектов) и 'summary_notes' (массив строк). Не пиши ничего кроме JSON."
                    logger.info(f"Retry attempt {attempt} for tender {tender_id}")

                response = self.client.models.generate_content(
                    model=model_name,
                    contents=current_prompt,
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json",
                        temperature=0.1
                    )
                )
                
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()
                
                text = response.text.strip()
                
                # Логирование ответа
                logger.info(f"===== [AI RESPONSE START] =====")
                logger.info(f"Tender ID: {tender_id}")
                logger.info(f"End Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
                logger.info(f"Duration: {duration:.2f} seconds")
                logger.info(f"Attempt: {attempt}")
                
                logger.info("--- [FULL RAW RESPONSE] ---")
                logger.info(text)
                logger.info("--- [END OF RAW RESPONSE] ---")
                
                logger.info(f"===== [AI RESPONSE END] =====")
                
                # Очистка от markdown если есть
                if text.startswith("```json"):
                    text = text.replace("```json", "", 1).replace("```", "", 1).strip()
                elif text.startswith("```"):
                    text = text.replace("```", "", 1).replace("```", "", 1).strip()
                
                try:
                    data = json.loads(text)
                    # Логирование структуры JSON
                    if isinstance(data, dict):
                        keys = list(data.keys())
                        rows_count = len(data.get("rows", []))
                        summary_notes_count = len(data.get("summary_notes", []))
                        logger.info(f"Parsed JSON structure: keys={keys}, rows_count={rows_count}, summary_notes_count={summary_notes_count}")
                    else:
                        logger.info(f"Parsed JSON is of type: {type(data)}")
                except json.JSONDecodeError as e:
                    logger.error(f"JSON Decode Error on attempt {attempt}: {e}")
                    if not self.debug_mode:
                        logger.error(f"Raw AI Response (first 2000 chars):\n{text[:2000]}")
                    
                    if attempt == retries:
                        from fastapi import HTTPException
                        raise HTTPException(status_code=500, detail=f"AI returned invalid JSON: {e}. Raw response: {text[:500]}...")
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
                    logger.info("Legacy format detected: array")
                    return {"rows": data, "summary_notes": ["(Legacy) Ответ получен в виде массива."]}
                
                # 3. Legacy: объект risks
                if isinstance(data, dict) and 'risks' in data:
                    logger.info("Legacy format detected: risks object")
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

    def _validate_and_filter_rows(self, rows: List[Dict[str, Any]], group_name: str, evidence_package: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        valid_rows = []
        rejected_count = 0
        
        valid_docs = []
        valid_docs_no_ext = []
        valid_refs_text = ""
        
        if evidence_package:
            valid_docs = [d.lower() for d in evidence_package.get("all_sources", [])]
            valid_docs_no_ext = [os.path.splitext(d)[0] for d in valid_docs]
            
            for items in evidence_package.get("slots", {}).values():
                for item in items:
                    valid_refs_text += " " + str(item.get("source_reference", "")).lower()
        
        for row in rows:
            if not isinstance(row, dict):
                logger.warning(f"Row rejection: not a dictionary. Row: {row}")
                rejected_count += 1
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
            rejection_reason = None
            if not block:
                rejection_reason = "missing 'block'"
            elif not finding:
                rejection_reason = "missing 'finding'"
            elif not source_document:
                rejection_reason = "missing 'source_document'"
            elif not source_reference:
                rejection_reason = "missing 'source_reference'"
            
            if rejection_reason:
                logger.warning(f"Row rejection: {rejection_reason}. Row: {row}")
                rejected_count += 1
                continue
                
            # Строгая проверка evidence (Post-validation)
            if evidence_package and source_document.lower() != "не найдено":
                doc_str = source_document.lower()
                
                # Проверка документа
                doc_is_valid = False
                for vd, vd_no_ext in zip(valid_docs, valid_docs_no_ext):
                    if vd in doc_str or vd_no_ext in doc_str:
                        doc_is_valid = True
                        break
                
                if not doc_is_valid:
                    logger.warning(f"Row rejection: hallucinated source_document '{source_document}'. Row: {row}")
                    rejected_count += 1
                    continue
                    
                # Проверка ссылки (коррекция, если галлюцинация)
                ref_str = source_reference.lower()
                ref_numbers = re.findall(r'\d+[\d.]*', ref_str)
                if ref_numbers:
                    ref_is_valid = any(num in valid_refs_text for num in ref_numbers)
                    if not ref_is_valid:
                        logger.info(f"Row correction: hallucinated source_reference '{source_reference}'. Correcting to generic.")
                        source_reference = "Найдено в тексте (точный пункт не определен)"
            
            # Нормализация
            normalized_block = block.lower()
            for key, value in self.block_normalization.items():
                if key in normalized_block:
                    block = value
                    break
            
            if block not in self.valid_blocks:
                logger.warning(f"Row rejection: unknown block '{block}'. Row: {row}")
                rejected_count += 1
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
            
        logger.info(f"Validation summary: total_rows={len(rows)}, valid_rows={len(valid_rows)}, rejected_rows={rejected_count}")
        return valid_rows

    def _add_missing_critical_topics(self, evidence_package: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Добавляет строки "не найдено" для критически важных тем на основе извлеченных слотов.
        """
        added_rows = []
        
        # Маппинг слотов на блоки и названия для отчета
        critical_slots = {
            "unloading": ("Поставка и приемка", "условие о разгрузке"),
            "acceptance_deadline": ("Поставка и приемка", "сроки приемки"),
            "acceptance_rejection_grounds": ("Поставка и приемка", "основания отказа в приемке"),
            "payment_deadline": ("Оплата", "срок оплаты"),
            "advance_payment": ("Оплата", "условие об авансе"),
            "edo_eis": ("Оплата", "условие об ЭДО"),
            "treasury_support": ("Оплата", "условие о казначейском сопровождении"),
            "delivery_documents": ("Документы при поставке", "документы при поставке"),
            "application_composition": ("Документы заявки", "полный перечень документов в составе заявки"),
            "national_regime_registries": ("Реестры/ограничения", "требования о включении в реестры и применении национального режима")
        }
            
        for slot_id, (block, label) in critical_slots.items():
            # Если слот не найден ни в одном документе
            if not evidence_package["slots"].get(slot_id):
                new_row = {
                    "block": block,
                    "finding": f"В просмотренных документах не найдено {label}.",
                    "risk_level": "Medium",
                    "supplier_action": "Проверить условие по первоисточнику документа и учесть его при подготовке заявки или исполнении договора.",
                    "source_document": "Не найдено",
                    "source_reference": "Критичное условие не выявлено в просмотренных документах",
                    "legal_basis": "",
                    "doc_group": "full"
                }
                added_rows.append(new_row)
                logger.info(f"Auto-added critical topic: {label} in block {block} (reason: slot '{slot_id}' not found in evidence package)")
        
        return added_rows

    def analyze_full_package(self, files: List[Dict[str, str]], tender_id: str = "unknown", callback=None) -> Dict[str, Any]:
        """
        Основной метод анализа всего пакета тендерной документации.
        """
        def update_stage(stage, progress, status="process"):
            if callback:
                callback(stage, progress, status)

        if not files:
            logger.error(f"Analysis failed for tender {tender_id}: no files provided")
            return {
                "rows": [],
                "summary_notes": ["Ошибка: нет файлов для анализа."],
                "status": "error",
                "stage": "Ошибка",
                "progress": 100
            }
        
        filenames = [f.get('filename', 'unknown') for f in files]
        logger.info(f"--- STARTING ANALYSIS FOR TENDER: {tender_id} ---")
        logger.info(f"Prompt Type: full")
        logger.info(f"Document Count: {len(files)}")
        logger.info(f"Filenames: {filenames}")
        
        update_stage("Классификация", 10)
        
        # Техническая классификация для логов и UI
        classified = self.classify_documents(files)
        has_contract = classified["has_contract"]
        classification_notes = classified["classification_notes"]
        file_classifications = classified["file_classifications"]
        
        file_statuses = [{"filename": f["filename"], "status": "processed"} for f in files]
        
        update_stage("Анализ документации", 30)

        # 1. Извлекаем структурированные улики (Evidence Extraction)
        logger.info(f"Extracting evidence from {len(files)} files...")
        evidence_package = self.evidence_collector.collect_evidence(files)
        
        found_slots = [slot_id for slot_id, items in evidence_package["slots"].items() if items]
        not_found_slots = [slot_id for slot_id, items in evidence_package["slots"].items() if not items]
        logger.info(f"Found slots: {found_slots}")
        logger.info(f"Not found slots: {not_found_slots}")
        
        formatted_evidence = self.evidence_collector.format_for_llm(evidence_package)
        
        logger.info(f"Evidence package size: {len(formatted_evidence)} characters")
        
        logger.info("--- [FORMATTED EVIDENCE PACKAGE] ---")
        logger.info(formatted_evidence)
        logger.info("--- [END OF EVIDENCE PACKAGE] ---")

        # 2. Интерпретация улик с помощью ИИ (LLM Interpretation)
        assembled_prompt = self._assemble_prompt(PROMPT_FULL_PACKAGE, formatted_evidence, "full")
        if not assembled_prompt:
            logger.error(f"Prompt assembly failed for tender {tender_id}")
            res = {"rows": [], "summary_notes": ["Ошибка формирования текста промпта для ИИ-анализа."]}
        else:
            res = self._call_ai_with_retry(assembled_prompt, prompt_type="full", tender_id=tender_id, filenames=filenames)
        
        rows = res.get('rows', [])
        logger.info(f"AI response: {len(rows)} raw rows received")
        
        rows = self._validate_and_filter_rows(rows, "full", evidence_package)
        
        added_rows = self._add_missing_critical_topics(evidence_package)
        rows += added_rows
        logger.info(f"Rows after adding {len(added_rows)} critical topics: {len(rows)}")
        
        all_rows = rows
        all_notes = res.get('summary_notes', [])
        
        update_stage("Формирование отчета", 95)
        
        # Пост-обработка
        # 1. Дедупликация с нормализацией
        def normalize(s):
            return re.sub(r'[^\w\s]', '', str(s).lower().strip())

        unique_rows = []
        seen_keys = set()
        duplicates_count = 0
        
        for r in all_rows:
            # Защитная проверка
            if not all(k in r for k in ['block', 'finding', 'source_document', 'source_reference']):
                logger.warning(f"Row rejection (post-processing): missing mandatory keys. Row: {r}")
                continue
                
            key = f"{normalize(r['block'])}_{normalize(r['finding'])}_{normalize(r['source_document'])}_{normalize(r['source_reference'])}"
            if key not in seen_keys:
                seen_keys.add(key)
                unique_rows.append(r)
            else:
                duplicates_count += 1
                if self.debug_mode:
                    logger.info(f"Duplicate removed: {r['block']} - {r['finding'][:50]}...")
        
        logger.info(f"Deduplication summary: before={len(all_rows)}, after={len(unique_rows)}, removed={duplicates_count}")

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
        
        result = {
            "rows": final_rows,
            "summary_notes": final_notes,
            "has_contract": has_contract,
            "classification_notes": classification_notes,
            "file_statuses": file_statuses,
            "file_classifications": file_classifications,
            "status": "success" if final_rows else "partial",
            "stage": "Готово",
            "progress": 100
        }
        
        logger.info(f"--- ANALYSIS COMPLETED FOR TENDER: {tender_id} ---")
        logger.info(f"Final result summary: rows={len(final_rows)}, notes={len(final_notes)}, has_contract={has_contract}")
        
        logger.info("--- FINAL JSON RESULT ---")
        logger.info(json.dumps(result, ensure_ascii=False, indent=2))
        logger.info("--- END OF FINAL JSON ---")
            
        return result

    def analyze_tender(self, files: List[Dict[str, str]], tender_id: str = "unknown", callback=None) -> Dict[str, Any]:
        """
        Legacy wrapper for analyze_full_package.
        """
        return self.analyze_full_package(files, tender_id=tender_id, callback=callback)

