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
            "Риски участия и исполнения", 
            "Риски отклонения заявки", 
            "Проверка на соответствие и противоречия", 
            "Поставка и приемка", 
            "Условия оплаты", 
            "Ответственность сторон", 
            "Документы для заявки", 
            "Документы при поставке", 
            "Реестры и ограничения",
            "Рекомендации поставщику"
        ]
        self.block_normalization = {
            "риски участия": "Риски участия и исполнения",
            "риски исполнения": "Риски участия и исполнения",
            "юридические риски": "Риски участия и исполнения",
            "финансовые риски": "Риски участия и исполнения",
            "операционные риски": "Риски участия и исполнения",
            "административные риски": "Риски участия и исполнения",
            "репутационные риски": "Риски участия и исполнения",
            "отклонение": "Риски отклонения заявки",
            "недопуск": "Риски отклонения заявки",
            "потеря баллов": "Риски отклонения заявки",
            "критерии оценки": "Риски отклонения заявки",
            "оценка": "Риски отклонения заявки",
            "несоответствие": "Проверка на соответствие и противоречия",
            "противоречия": "Проверка на соответствие и противоречия",
            "ошибки документации": "Проверка на соответствие и противоречия",
            "рисковые формулировки": "Проверка на соответствие и противоречия",
            "поставка": "Поставка и приемка",
            "приемка": "Поставка и приемка",
            "условия поставки": "Поставка и приемка",
            "разгрузка": "Поставка и приемка",
            "доставка": "Поставка и приемка",
            "расчеты": "Условия оплаты",
            "условия оплаты": "Условия оплаты",
            "аванс": "Условия оплаты",
            "эдо": "Условия оплаты",
            "казначейское сопровождение": "Условия оплаты",
            "штрафы": "Ответственность сторон",
            "пени": "Ответственность сторон",
            "неустойка": "Ответственность сторон",
            "односторонний отказ": "Ответственность сторон",
            "расторжение": "Ответственность сторон",
            "санкции": "Ответственность сторон",
            "состав заявки": "Документы для заявки",
            "требования к заявке": "Документы для заявки",
            "сопроводительные документы": "Документы при поставке",
            "приемочные документы": "Документы при поставке",
            "нацрежим": "Реестры и ограничения",
            "национальный режим": "Реестры и ограничения",
            "реестр": "Реестры и ограничения",
            "ограничения": "Реестры и ограничения",
            "преференции": "Реестры и ограничения",
            "условие допуска": "Реестры и ограничения",
            "запрет": "Реестры и ограничения",
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
                        "detailed_report": data.get("detailed_report", []),
                        "summary_notes": data.get("summary_notes", [])
                    }
                
                # 2. Legacy: массив
                if isinstance(data, list):
                    logger.info("Legacy format detected: array")
                    return {"rows": data, "detailed_report": [], "summary_notes": ["(Legacy) Ответ получен в виде массива."]}
                
                # 3. Legacy: объект risks
                if isinstance(data, dict) and 'risks' in data:
                    logger.info("Legacy format detected: risks object")
                    return {"rows": data['risks'], "detailed_report": [], "summary_notes": ["(Legacy) Ответ получен в формате risks."]}
                
                logger.warning(f"Unexpected JSON structure on attempt {attempt}: {data}")
                
            except Exception as e:
                from fastapi import HTTPException
                if isinstance(e, HTTPException):
                    raise e
                logger.error(f"AI Error on attempt {attempt}: {e}")
                if attempt < retries:
                    time.sleep(1)
                else:
                    return {"rows": [], "detailed_report": [], "summary_notes": [f"Техническая ошибка ИИ: {str(e)}"]}
                    
        return {"rows": [], "detailed_report": [], "summary_notes": ["Не удалось получить валидный ответ от ИИ."], "status": "partial"}

    def _clean_text(self, text: str) -> str:
        """
        Выполняет глубокую очистку текста от технического мусора, сохраняя юридическую значимость.
        """
        if not text:
            return ""
        
        # 1. Удаление явно битых служебных символов (контрольные символы кроме \n \t)
        text = "".join(ch for ch in text if ch == '\n' or ch == '\t' or (ord(ch) >= 32 and ord(ch) != 127))
        
        # 2. Удаление артефактов OCR (длинные последовательности точек, подчеркиваний, тире)
        text = re.sub(r'\.{5,}', '...', text)
        text = re.sub(r'_{5,}', '___', text)
        text = re.sub(r'-{5,}', '---', text)
        
        # 3. Удаление повторяющихся пробелов и табуляций (сохраняем структуру строк)
        text = re.sub(r'[ \t]+', ' ', text)
        
        # 4. Удаление избыточных пустых строк (более двух подряд)
        text = re.sub(r'\n{3,}', '\n\n', text)
        
        # 5. Удаление дублей подряд идущих одинаковых строк (часто при ошибках OCR)
        lines = text.split('\n')
        deduped_lines = []
        for line in lines:
            trimmed = line.strip()
            if not trimmed:
                if not deduped_lines or deduped_lines[-1] != "":
                    deduped_lines.append("")
                continue
            if not deduped_lines or trimmed != deduped_lines[-1]:
                deduped_lines.append(line)
        
        return "\n".join(deduped_lines).strip()

    def _prepare_full_context(self, files: List[Dict[str, str]], file_classifications: List[Dict[str, Any]] = None) -> str:
        """
        Подготавливает полный очищенный контекст из всех документов.
        Извлекает текст, очищает его и размечает явными границами.
        """
        full_context = []
        
        # Маппинг ролей документов для разметки
        roles_map = {fc.get('filename'): fc.get('category') for fc in file_classifications} if file_classifications else {}
        
        for f in files:
            filename = f.get('filename', 'unknown')
            text = f.get('text', '')
            role = roles_map.get(filename, "не определен")
            
            if not text or len(text.strip()) < 10:
                logger.warning(f"Document {filename} has no meaningful text, skipping from context")
                continue
                
            # Очистка текста
            cleaned_text = self._clean_text(text)
            
            logger.info(f"--- CLEANED TEXT FOR {filename} (len: {len(cleaned_text)}) ---")
            logger.info(cleaned_text[:5000] + ("..." if len(cleaned_text) > 5000 else ""))
            logger.info(f"--- END CLEANED TEXT FOR {filename} ---")
            
            # Разметка документа с явными границами
            doc_block = (
                f"=== ДОКУМЕНТ: {filename} ===\n"
                f"=== ТИП ДОКУМЕНТА: {role} ===\n"
                f"{cleaned_text}\n"
                f"=== КОНЕЦ ДОКУМЕНТА ==="
            )
            full_context.append(doc_block)
            
        return "\n\n".join(full_context)

    def _validate_and_filter_rows(self, rows: List[Dict[str, Any]], group_name: str, files: List[Dict[str, str]] = None) -> List[Dict[str, Any]]:
        valid_rows = []
        rejected_count = 0
        
        valid_filenames = [f.get('filename', '').lower() for f in files] if files else []
        valid_filenames_no_ext = [os.path.splitext(f)[0].lower() for f in valid_filenames]
        
        for row in rows:
            if not isinstance(row, dict):
                logger.warning(f"Row rejection: not a dictionary. Row: {row}")
                rejected_count += 1
                continue
            
            # Обязательные поля (минимальный набор)
            block = str(row.get("block", "")).strip()
            finding = str(row.get("finding", "")).strip()
            risk_level = str(row.get("risk_level", "Medium")).strip()
            supplier_action = str(row.get("supplier_action", "")).strip()
            source_document = str(row.get("source_document", "")).strip()
            source_reference = str(row.get("source_reference", "")).strip()
            legal_basis = str(row.get("legal_basis", "")).strip()
            
            # Если нет описания - пытаемся спасти или отбрасываем
            if not finding or len(finding) < 5:
                logger.warning(f"Row rejection: missing or too short 'finding'. Row: {row}")
                rejected_count += 1
                continue
            
            # Нормализация блока
            normalized_block = block.lower()
            found_normalized = False
            for key, value in self.block_normalization.items():
                if key in normalized_block:
                    block = value
                    found_normalized = True
                    break
            
            if not found_normalized and block not in self.valid_blocks:
                # Если блок совсем странный, но есть finding, относим к общей категории
                block = "Риски участия и исполнения"
                
            if not source_document or source_document.lower() in ["", "none", "null", "не найдено"]:
                source_document = "Весь пакет документов"
            
            if not source_reference or source_reference.lower() in ["", "none", "null", "не найдено"]:
                source_reference = "По тексту документов"

            if risk_level not in ["High", "Medium", "Low"]:
                risk_level = "Medium"
            
            # Пытаемся сопоставить source_document с реальными файлами для красоты
            if valid_filenames:
                doc_str = source_document.lower()
                for vf, vf_no_ext in zip(valid_filenames, valid_filenames_no_ext):
                    if vf == doc_str or vf_no_ext == doc_str:
                        source_document = vf # Точное совпадение
                        break
            
            valid_row = {
                "block": block,
                "finding": finding[:3000],
                "risk_level": risk_level,
                "supplier_action": supplier_action if supplier_action else "Проверить условие по первоисточнику документа.",
                "source_document": source_document[:250],
                "source_reference": source_reference[:250],
                "legal_basis": legal_basis[:1000] if legal_basis else "",
                "doc_group": group_name
            }
            
            valid_rows.append(valid_row)
            
        logger.info(f"Validation summary: total_rows={len(rows)}, valid_rows={len(valid_rows)}, rejected_rows={rejected_count}")
        return valid_rows

    def _add_missing_critical_topics(self, evidence_package: Dict[str, Any], existing_rows: List[Dict[str, Any]], detailed_report: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """
        Добавляет строки "не найдено" для критически важных тем, если они отсутствуют и в ИИ-ответе, и в слотах.
        """
        added_rows = []
        
        # Маппинг слотов на блоки и названия для отчета
        critical_slots = {
            "unloading": ("Поставка и приемка", "условие о разгрузке"),
            "acceptance_deadline": ("Поставка и приемка", "сроки приемки"),
            "refusal_grounds": ("Поставка и приемка", "основания отказа в приемке"),
            "payment_deadline": ("Условия оплаты", "срок оплаты"),
            "advance_payment": ("Условия оплаты", "условие об авансе"),
            "edo_eis": ("Условия оплаты", "условие об ЭДО"),
            "treasury_support": ("Условия оплаты", "условие о казначейском сопровождении"),
            "delivery_documents": ("Документы при поставке", "документы при поставке"),
            "application_composition": ("Документы для заявки", "полный перечень документов в составе заявки"),
            "national_regime_registries": ("Реестры и ограничения", "требования о включении в реестры и применении национального режима")
        }
        
        # Собираем все найденные темы из существующих строк для проверки
        existing_text = " ".join([r.get("finding", "").lower() for r in existing_rows])
        
        # Также проверяем подробный отчет
        if detailed_report:
            if isinstance(detailed_report, dict):
                for val in detailed_report.values():
                    if isinstance(val, list):
                        existing_text += " " + " ".join([str(v).lower() for v in val])
            elif isinstance(detailed_report, list):
                existing_text += " " + " ".join([str(v).lower() for v in detailed_report])
            
        for slot_id, (block, label) in critical_slots.items():
            # Проверяем, упоминал ли ИИ эту тему (более мягкий поиск)
            topic_keywords = [kw for kw in label.split() if len(kw) > 3]
            # Если хотя бы 2 ключевых слова из названия темы есть в тексте - считаем что тема затронута
            match_count = sum(1 for kw in topic_keywords if kw in existing_text)
            mentioned_by_ai = match_count >= min(2, len(topic_keywords))
            
            # Проверяем, нашел ли бэкенд слот
            found_in_slots = bool(evidence_package.get("slots", {}).get(slot_id))
            
            # Добавляем только если ни ИИ, ни бэкенд не нашли тему
            if not mentioned_by_ai and not found_in_slots:
                new_row = {
                    "block": block,
                    "finding": f"В просмотренных документах не найдено {label}.",
                    "risk_level": "Medium",
                    "supplier_action": "Проверить наличие данного условия в документации самостоятельно или направить запрос на разъяснение.",
                    "source_document": "Не найдено",
                    "source_reference": "Критичное условие не выявлено",
                    "legal_basis": "",
                    "doc_group": "full"
                }
                added_rows.append(new_row)
                logger.info(f"Auto-added secondary critical topic: {label} (not found by AI or Slots)")
        
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
        
        # Логируем роли документов
        logger.info("Document roles:")
        for fc in file_classifications:
            logger.info(f"  - {fc.get('filename', 'unknown')}: {fc.get('category', 'unknown')} (contract_score: {fc.get('contract_score', 0)}, procurement_score: {fc.get('procurement_score', 0)}, reason: {fc.get('classification_reason', '')})")
        
        file_statuses = [{"filename": f.get("filename", "unknown"), "status": "processed"} for f in files]
        
        update_stage("Анализ документации", 30)

        # 1. Подготовка полного контекста (Full Context Preparation)
        logger.info(f"Preparing full context from {len(files)} files...")
        full_context = self._prepare_full_context(files, file_classifications)
        logger.info(f"Full context size: {len(full_context)} characters")
        
        logger.info("--- ASSEMBLED FULL CLEANED CONTEXT ---")
        logger.info(full_context[:10000] + ("..." if len(full_context) > 10000 else ""))
        logger.info("--- END ASSEMBLED FULL CLEANED CONTEXT ---")
        
        # 2. Вспомогательное извлечение улик (Auxiliary Evidence Extraction)
        # Используется для логов, противоречий и автодобавления критических тем
        logger.info("Running auxiliary evidence extraction...")
        evidence_package = self.evidence_collector.collect_evidence(files)
        
        found_slots = [slot_id for slot_id, items in evidence_package.get("slots", {}).items() if items]
        logger.info(f"Auxiliary found slots: {found_slots}")
        
        contradictions = evidence_package.get("contradictions", [])
        if contradictions:
            logger.info(f"Found {len(contradictions)} contradictions (auxiliary)")
        
        # 3. Интерпретация полного контекста с помощью ИИ (LLM Interpretation)
        assembled_prompt = self._assemble_prompt(PROMPT_FULL_PACKAGE, full_context, "full")
        if not assembled_prompt:
            logger.error(f"Prompt assembly failed for tender {tender_id}")
            res = {"rows": [], "summary_notes": ["Ошибка формирования текста промпта для ИИ-анализа."]}
        else:
            res = self._call_ai_with_retry(assembled_prompt, prompt_type="full", tender_id=tender_id, filenames=filenames)
        
        rows = res.get('rows', [])
        detailed_report = res.get('detailed_report', [])
        logger.info(f"AI response: {len(rows)} raw rows received, {len(detailed_report)} detailed report sections")
        
        # Мягкая валидация строк
        rows = self._validate_and_filter_rows(rows, "full", files)
        
        # Add contradictions directly to rows to guarantee they are not smoothed out
        contradiction_rows = []
        contradiction_notes = []
        for c in contradictions:
            finding = f"ПРОТИВОРЕЧИЕ ({c.get('slot_name', 'unknown')}): В документе '{c.get('source_1', '')}' указано '{c.get('value_1', '')}', а в документе '{c.get('source_2', '')}' — '{c.get('value_2', '')}'."
            contradiction_rows.append({
                "block": "Проверка на соответствие и противоречия",
                "finding": finding,
                "risk_level": c.get('severity', 'High'),
                "supplier_action": "Направить запрос на разъяснение для устранения противоречия до подачи заявки.",
                "source_document": f"{c.get('source_1', '')} / {c.get('source_2', '')}",
                "source_reference": "Сравнение документов",
                "legal_basis": "ч. 4 ст. 105 Закона № 44-ФЗ",
                "doc_group": "full"
            })
            contradiction_notes.append(finding)
        
        rows = contradiction_rows + rows
        
        # Валидация detailed_report
        valid_detailed_report = []
        if isinstance(detailed_report, dict):
            section_titles = {
                "risks_execution": "Риски участия и исполнения",
                "rejection_risks": "Риски недопуска заявки и потери баллов",
                "compliance_check": "Проверка на соответствие и противоречия",
                "delivery_acceptance": "Поставка и приемка",
                "payment_terms": "Условия оплаты",
                "liability": "Ответственность сторон",
                "application_documents": "Документы для заявки",
                "delivery_documents": "Документы при поставке",
                "registries_restrictions": "Реестры и ограничения",
                "supplier_recommendations": "Рекомендации поставщику"
            }
            for key, title in section_titles.items():
                content_items = detailed_report.get(key, [])
                if not isinstance(content_items, list):
                    content_items = [content_items]
                
                # Добавляем противоречия в compliance_check
                if key == "compliance_check" and contradiction_notes:
                    content_items = contradiction_notes + content_items
                
                if content_items:
                    content_str = "\n\n".join([str(item) for item in content_items if item])
                    if content_str.strip():
                        valid_detailed_report.append({
                            "section_title": title,
                            "content": content_str.strip()
                        })
        elif isinstance(detailed_report, list):
            for section in detailed_report:
                if isinstance(section, dict) and 'section_title' in section and 'content' in section:
                    valid_detailed_report.append(section)
        
        added_rows = self._add_missing_critical_topics(evidence_package, rows, detailed_report)
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
            if not all(k in r for k in ['block', 'finding', 'source_document']):
                logger.warning(f"Row rejection (post-processing): missing mandatory keys. Row: {r}")
                continue
                
            key = f"{normalize(r['block'])}_{normalize(r['finding'])}_{normalize(r['source_document'])}"
            if key not in seen_keys:
                seen_keys.add(key)
                unique_rows.append(r)
            else:
                duplicates_count += 1
        
        logger.info(f"Deduplication summary: before={len(all_rows)}, after={len(unique_rows)}, removed={duplicates_count}")

        # 2. Сортировка
        risk_order = {"High": 0, "Medium": 1, "Low": 2}
        unique_rows.sort(key=lambda x: (
            risk_order.get(x.get('risk_level', 'Medium'), 3), 
            x.get('block', ''), 
            x.get('source_document', '')
        ))

        # 3. Лимит и фильтрация заметок
        final_rows = unique_rows[:50] # Увеличим лимит
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
            "detailed_report": valid_detailed_report,
            "summary_notes": final_notes,
            "has_contract": has_contract,
            "classification_notes": classification_notes,
            "file_statuses": file_statuses,
            "file_classifications": file_classifications,
            "contradictions": contradictions,
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

