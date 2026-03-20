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
            "Риски участия и исполнения договора", 
            "Риски недопуска заявки и потери баллов", 
            "Проверка соответствия документации и закона", 
            "Условия поставки и приемки", 
            "Условия оплаты", 
            "Ответственность сторон", 
            "Перечень документов", 
            "Требования по реестрам и ограничениям",
            "Рекомендации Поставщику"
        ]
        self.block_normalization = {
            "риски участия": "Риски участия и исполнения договора",
            "риски исполнения": "Риски участия и исполнения договора",
            "юридические риски": "Риски участия и исполнения договора",
            "финансовые риски": "Риски участия и исполнения договора",
            "операционные риски": "Риски участия и исполнения договора",
            "административные риски": "Риски участия и исполнения договора",
            "репутационные риски": "Риски участия и исполнения договора",
            "отклонение": "Риски недопуска заявки и потери баллов",
            "недопуск": "Риски недопуска заявки и потери баллов",
            "потеря баллов": "Риски недопуска заявки и потери баллов",
            "критерии оценки": "Риски недопуска заявки и потери баллов",
            "оценка": "Риски недопуска заявки и потери баллов",
            "несоответствие": "Проверка соответствия документации и закона",
            "противоречия": "Проверка соответствия документации и закона",
            "ошибки документации": "Проверка соответствия документации и закона",
            "рисковые формулировки": "Проверка соответствия документации и закона",
            "поставка": "Условия поставки и приемки",
            "приемка": "Условия поставки и приемки",
            "условия поставки": "Условия поставки и приемки",
            "разгрузка": "Условия поставки и приемки",
            "доставка": "Условия поставки и приемки",
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
            "состав заявки": "Перечень документов",
            "требования к заявке": "Перечень документов",
            "сопроводительные документы": "Перечень документов",
            "приемочные документы": "Перечень документов",
            "документы для заявки": "Перечень документов",
            "документы при поставке": "Перечень документов",
            "нацрежим": "Требования по реестрам и ограничениям",
            "национальный режим": "Требования по реестрам и ограничениям",
            "реестр": "Требования по реестрам и ограничениям",
            "ограничения": "Требования по реестрам и ограничениям",
            "преференции": "Требования по реестрам и ограничениям",
            "условие допуска": "Требования по реестрам и ограничениям",
            "запрет": "Требования по реестрам и ограничениям",
            "рекомендации": "Рекомендации Поставщику",
            "что сделать поставщику": "Рекомендации Поставщику"
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
                logger.info(f"Response Length: {len(text)} characters")
                logger.info(f"Full Response Text:")
                logger.info(text)
                logger.info(f"===== [AI RESPONSE END] =====")
                
                try:
                    data = json.loads(text)
                    # Если ИИ вернул не объект, а массив (старый формат), оборачиваем
                    if isinstance(data, list):
                        return {"rows": data, "summary_notes": []}
                    return data
                except json.JSONDecodeError as e:
                    logger.error(f"JSON Decode Error on attempt {attempt}: {e}")
                    if attempt == retries:
                        # На последней попытке пытаемся вытащить JSON регуляркой
                        json_match = re.search(r'(\{.*\})', text, re.DOTALL)
                        if json_match:
                            try:
                                return json.loads(json_match.group(1))
                            except:
                                pass
                        return {"rows": [], "summary_notes": [f"Ошибка парсинга JSON: {str(e)}"]}
                    continue
            except Exception as e:
                logger.error(f"AI Call Error on attempt {attempt}: {str(e)}")
                if attempt == retries:
                    return {"rows": [], "summary_notes": [f"Ошибка вызова ИИ: {str(e)}"]}
                time.sleep(2)
        
        return {"rows": [], "summary_notes": ["Неизвестная ошибка при вызове ИИ."]}

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

    def _normalize_text(self, s: Any) -> str:
        """Нормализация текста для сравнения и дедупликации."""
        return re.sub(r'[^\w\s]', '', str(s).lower().strip())

    def _log_progress(self, stage: str, progress: int, status: str = "processing", callback=None):
        """Логирование прогресса и вызов callback."""
        if callback:
            callback(stage, progress, status)
        logger.info(f"Stage: {stage}, Progress: {progress}%")

    def _prepare_full_context(self, files: List[Dict[str, str]], evidence_package: Dict[str, Any] = None) -> str:
        """
        Подготавливает полный очищенный контекст из всех документов.
        Извлекает текст, очищает его и размечает явными границами.
        """
        full_context = []
        
        # Маппинг ролей документов из EvidenceCollector
        roles_map = {}
        if evidence_package and "documents" in evidence_package:
            for doc in evidence_package["documents"]:
                roles_map[doc.get('filename')] = doc.get('role')
        
        # Русские названия для ролей
        role_labels = {
            "contract": "Проект контракта",
            "procurement": "Извещение / Информационная карта",
            "tz": "Техническое задание / Описание объекта закупки",
            "application_rules": "Требования к заявке / Инструкция",
            "nmck": "Обоснование НМЦК",
            "mixed": "Смешанный документ (закупка + контракт)",
            "unknown": "Не определен"
        }
        
        for f in files:
            filename = f.get('filename', 'unknown')
            text = f.get('text', '')
            raw_role = roles_map.get(filename, "unknown")
            role_label = role_labels.get(raw_role, "Не определен")
            
            logger.info(f"Document role: {filename} -> {role_label} ({raw_role})")
            
            if not text or len(text.strip()) < 10:
                logger.warning(f"Document {filename} has no meaningful text, skipping from context")
                continue
                
            # Очистка текста
            cleaned_text = self._clean_text(text)
            
            # Разметка документа с явными границами (согласно требованиям)
            doc_block = (
                f"=== ДОКУМЕНТ: {filename} ===\n"
                f"=== ТИП ДОКУМЕНТА: {role_label} ===\n"
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
        
        # Подготавливаем тексты файлов для поиска фрагментов (мягкая привязка)
        file_texts = {f.get('filename', ''): f.get('text', '') for f in files} if files else {}
        
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
            
            # СУПЕР-МЯГКАЯ ПРОВЕРКА: если есть хоть какой-то смысл, оставляем
            if not finding or len(finding) < 2:
                if supplier_action and len(supplier_action) > 5:
                    finding = f"Рекомендация: {supplier_action}"
                else:
                    logger.warning(f"Row rejection: empty finding and no supplier_action. Row: {json.dumps(row, ensure_ascii=False)}")
                    rejected_count += 1
                    continue
            
            # Нормализация блока (приведение к каноническим названиям)
            normalized_block_input = block.lower()
            found_normalized = False
            for key, value in self.block_normalization.items():
                if key in normalized_block_input:
                    block = value
                    found_normalized = True
                    break
            
            if not found_normalized and block not in self.valid_blocks:
                # Если блок не распознан, пытаемся найти по смыслу в finding
                finding_lower = finding.lower()
                for key, value in self.block_normalization.items():
                    if key in finding_lower:
                        block = value
                        found_normalized = True
                        break
                
                if not found_normalized:
                    block = "Риски участия и исполнения договора"
            
            # Нормализация источника
            if not source_document or source_document.lower() in ["", "none", "null", "не найдено", "unknown", "весь пакет"]:
                source_document = "Весь пакет документов"
            
            # Пытаемся сопоставить source_document с реальными файлами
            if valid_filenames and source_document != "Весь пакет документов":
                doc_str = source_document.lower()
                matched_filename = None
                
                # 1. Точное совпадение
                for vf, vf_no_ext in zip(valid_filenames, valid_filenames_no_ext):
                    if vf == doc_str or vf_no_ext == doc_str:
                        matched_filename = vf
                        break
                
                # 2. Частичное совпадение
                if not matched_filename:
                    for vf in valid_filenames:
                        if vf in doc_str or doc_str in vf:
                            matched_filename = vf
                            break
                
                if matched_filename:
                    source_document = matched_filename
                else:
                    # Если не совпало, но ссылка похожа на файл, оставляем как есть (не отбрасываем!)
                    pass
            
            # Мягкая привязка source_reference
            if not source_reference or source_reference.lower() in ["", "none", "null", "не найдено"]:
                # Пытаемся найти фрагмент finding в тексте документа
                if source_document != "Весь пакет документов" and source_document in file_texts:
                    doc_text = file_texts[source_document]
                    # Ищем первые 20 символов находки
                    search_snippet = finding[:20].strip()
                    if len(search_snippet) > 10 and search_snippet in doc_text:
                        source_reference = f"По тексту (фрагмент: '{search_snippet}...')"
                    else:
                        source_reference = "По тексту документа"
                else:
                    source_reference = "По тексту документов"

            if risk_level not in ["High", "Medium", "Low"]:
                risk_level = "Medium"
            
            valid_row = {
                "block": block,
                "finding": finding[:5000], # Увеличили лимит, чтобы не обрезать полезное
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



    def _add_missing_critical_topics(self, evidence_package: Dict[str, Any], existing_rows: List[Dict[str, Any]], final_report_sections: Dict[str, Any] = None, final_report_markdown: str = "") -> List[Dict[str, Any]]:
        """
        Добавляет строки "не найдено" для критически важных тем, если они отсутствуют и в ИИ-ответе, и в слотах.
        """
        added_rows = []
        
        # Маппинг слотов на блоки и названия для отчета
        critical_slots = {
            "unloading": ("Условия поставки и приемки", "условие о разгрузке"),
            "delivery_deadline": ("Условия поставки и приемки", "срок поставки"),
            "acceptance_deadline": ("Условия поставки и приемки", "сроки приемки"),
            "refusal_grounds": ("Условия поставки и приемки", "основания отказа в приемке"),
            "payment_deadline": ("Условия оплаты", "срок оплаты"),
            "advance_payment": ("Условия оплаты", "условие об авансе"),
            "penalties": ("Ответственность сторон", "штрафы, пени и неустойки"),
            "edo_eis": ("Условия оплаты", "условие об ЭДО"),
            "treasury_support": ("Условия оплаты", "условие о казначейском сопровождении"),
            "delivery_documents": ("Перечень документов", "документы при поставке"),
            "application_composition": ("Перечень документов", "полный перечень документов в составе заявки"),
            "national_regime_registries": ("Требования по реестрам и ограничениям", "требования о включении в реестры и применении национального режима")
        }
        
        # Собираем все найденные темы из существующих строк для проверки
        existing_text = " ".join([r.get("finding", "").lower() for r in existing_rows])
        
        # Также проверяем подробный отчет
        if final_report_sections:
            if isinstance(final_report_sections, dict):
                for val in final_report_sections.values():
                    if isinstance(val, list):
                        existing_text += " " + " ".join([str(v).lower() for v in val])
                    else:
                        existing_text += " " + str(val).lower()
            elif isinstance(final_report_sections, list):
                existing_text += " " + " ".join([str(v).lower() for v in final_report_sections])
        
        # И markdown отчет
        if final_report_markdown:
            existing_text += " " + final_report_markdown.lower()
            
        for slot_id, (block, label) in critical_slots.items():
            # Проверяем, упоминал ли ИИ эту тему (более мягкий поиск)
            words = label.split()
            # Ищем хотя бы одно значимое слово (длиной > 4) в тексте
            found_by_ai = any(word in existing_text for word in words if len(word) > 4)
            
            # Проверяем, нашел ли EvidenceCollector что-то в этом слоте
            found_by_collector = False
            if evidence_package and "slots" in evidence_package:
                slot_data = evidence_package["slots"].get(slot_id, [])
                if slot_data:
                    found_by_collector = True
            
            if not found_by_ai and not found_by_collector:
                new_row = {
                    "block": block,
                    "finding": f"Информация про {label} в документации не обнаружена.",
                    "risk_level": "Low",
                    "supplier_action": f"Уточнить {label} через запрос на разъяснение, если это критично для исполнения.",
                    "source_document": "Весь пакет документов",
                    "source_reference": "Не найдено",
                    "legal_basis": "",
                    "doc_group": "full"
                }
                added_rows.append(new_row)
                logger.info(f"Added missing critical topic row: {label}")
                
        if added_rows:
            logger.info(f"Total critical topic rows added: {len(added_rows)}")
        return added_rows

    def analyze_full_package(self, files: List[Dict[str, str]], tender_id: str = "unknown", callback=None) -> Dict[str, Any]:
        """
        Основной метод анализа полного пакета документов.
        """
        logger.info(f"--- STARTING FULL PACKAGE ANALYSIS FOR TENDER: {tender_id} ---")
        
        self._log_progress("Классификация документов", 10, callback=callback)
        
        filenames = [f.get('filename', 'unknown') for f in files]
        
        # 0. Классификация
        classification_res = self.classify_documents(files)
        has_contract = classification_res.get("has_contract", False)
        classification_notes = classification_res.get("classification_notes", [])
        file_classifications = classification_res.get("file_classifications", [])
        
        for fc in file_classifications:
            logger.info(f"File classification: {fc.get('filename', 'unknown')}: {fc.get('category', 'unknown')} (contract_score: {fc.get('contract_score', 0)}, procurement_score: {fc.get('procurement_score', 0)}, reason: {fc.get('classification_reason', '')})")
        
        file_statuses = [{"filename": f.get("filename", "unknown"), "status": "processed"} for f in files]
        
        self._log_progress("Анализ документации", 30, callback=callback)

        # 1. Вспомогательное извлечение улик (теперь идет первым для определения ролей)
        logger.info("--- [AUXILIARY SLOT EXTRACTION START] ---")
        evidence_package = self.evidence_collector.collect_evidence(files)
        
        formatted_evidence = self.evidence_collector.format_for_llm(evidence_package)
        logger.info(formatted_evidence)
        logger.info("--- [AUXILIARY SLOT EXTRACTION END] ---")
        
        found_slots = [slot_id for slot_id, items in evidence_package.get("slots", {}).items() if items]
        logger.info(f"Summary of found slots: {found_slots}")
        
        contradictions = evidence_package.get("contradictions", [])
        if contradictions:
            logger.info(f"Found {len(contradictions)} contradictions (auxiliary)")
            for c in contradictions:
                logger.info(f"Contradiction: {c.get('slot_name')} - {c.get('source_1')} vs {c.get('source_2')}")

        # 2. Подготовка полного контекста (основной вход для модели)
        logger.info(f"Preparing full context from {len(files)} files...")
        full_context = self._prepare_full_context(files, evidence_package)
        logger.info(f"Full context size: {len(full_context)} characters")
        
        logger.info("--- [ASSEMBLED FULL CLEANED CONTEXT START] ---")
        logger.info(full_context)
        logger.info("--- [ASSEMBLED FULL CLEANED CONTEXT END] ---")
        
        # 3. Интерпретация полного контекста с помощью ИИ
        assembled_prompt = self._assemble_prompt(PROMPT_FULL_PACKAGE, full_context, "full")
        if not assembled_prompt:
            logger.error(f"Prompt assembly failed for tender {tender_id}")
            res = {"rows": [], "summary_notes": ["Ошибка формирования текста промпта для ИИ-анализа."]}
        else:
            res = self._call_ai_with_retry(assembled_prompt, prompt_type="full", tender_id=tender_id, filenames=filenames)
        
        rows = res.get('rows', [])
        final_report_sections = res.get('final_report_sections', {})
        final_report_markdown = res.get('final_report_markdown', "")
        logger.info(f"AI response: {len(rows)} raw rows received, {len(final_report_sections)} detailed report sections, markdown length: {len(final_report_markdown)}")
        
        # Мягкая валидация строк
        rows = self._validate_and_filter_rows(rows, "full", files)
        
        # Добавляем противоречия напрямую в строки
        contradiction_rows = []
        contradiction_notes = []
        for c in contradictions:
            finding = f"ПРОТИВОРЕЧИЕ ({c.get('slot_name', 'unknown')}): В документе '{c.get('source_1', '')}' указано '{c.get('value_1', '')}', а в документе '{c.get('source_2', '')}' — '{c.get('value_2', '')}'."
            contradiction_rows.append({
                "block": "Проверка соответствия документации и закона",
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
        
        # Валидация final_report_sections и формирование final_report_markdown если его нет
        valid_final_report_sections = []
        section_titles = {
            "risks_execution": "1) Риски участия и исполнения договора",
            "rejection_risks": "2) Риски недопуска заявки и потери баллов",
            "compliance_check": "3) Проверка соответствия документации и закона",
            "delivery_acceptance": "4) Условия поставки и приемки",
            "payment_terms": "5) Условия оплаты",
            "liability": "6) Ответственность сторон",
            "documents_list": "7) Перечень документов",
            "registries_restrictions": "8) Требования по реестрам и ограничениям",
            "supplier_recommendations": "9) Рекомендации Поставщику"
        }
        
        # Если ИИ не вернул markdown, соберем его из final_report_sections
        if not final_report_markdown:
            logger.info("Markdown report missing in AI response, assembling from final_report_sections")
            markdown_parts = ["# Подробный юридический отчет по тендеру\n"]
            
            for key, title in section_titles.items():
                content_items = final_report_sections.get(key, []) if isinstance(final_report_sections, dict) else []
                
                if key == "documents_list" and isinstance(content_items, dict):
                    # Специальная обработка для раздела 7 (Перечень документов)
                    in_app = content_items.get("in_application", [])
                    on_del = content_items.get("on_delivery", [])
                    
                    content_str = "**В составе заявки**:\n"
                    content_str += "\n".join([f"- {i}" for i in in_app]) if in_app else "Информация не обнаружена."
                    content_str += "\n\n**При поставке**:\n"
                    content_str += "\n".join([f"- {i}" for i in on_del]) if on_del else "Информация не обнаружена."
                else:
                    if not isinstance(content_items, list):
                        content_items = [content_items]
                    
                    # Добавляем противоречия в compliance_check
                    if key == "compliance_check" and contradiction_notes:
                        content_items = contradiction_notes + content_items
                    
                    content_str = "\n\n".join([str(item) for item in content_items if item])
                    if not content_str.strip():
                        content_str = "Информация в предоставленной документации не обнаружена."
                
                valid_final_report_sections.append({
                    "section_title": title,
                    "content": content_str.strip()
                })
                
                markdown_parts.append(f"## {title}\n{content_str.strip()}\n")
            
            final_report_markdown = "\n".join(markdown_parts)
        else:
            # Если markdown есть, все равно подготовим valid_final_report_sections для совместимости
            for key, title in section_titles.items():
                content_items = final_report_sections.get(key, []) if isinstance(final_report_sections, dict) else []
                
                if key == "documents_list" and isinstance(content_items, dict):
                    in_app = content_items.get("in_application", [])
                    on_del = content_items.get("on_delivery", [])
                    content_str = "**В составе заявки**:\n"
                    content_str += "\n".join([f"- {i}" for i in in_app]) if in_app else "Информация не обнаружена."
                    content_str += "\n\n**При поставке**:\n"
                    content_str += "\n".join([f"- {i}" for i in on_del]) if on_del else "Информация не обнаружена."
                else:
                    if not isinstance(content_items, list):
                        content_items = [content_items]
                    
                    if key == "compliance_check" and contradiction_notes:
                        content_items = contradiction_notes + content_items
                    
                    content_str = "\n\n".join([str(item) for item in content_items if item])
                    if not content_str.strip():
                        content_str = "Информация в предоставленной документации не обнаружена."
                
                valid_final_report_sections.append({
                    "section_title": title,
                    "content": content_str.strip()
                })
            
            # Внедряем противоречия в готовый markdown, если их там нет
            if contradiction_notes:
                section_3_pattern = r"(Раздел 3:.*?\n)"
                if re.search(section_3_pattern, final_report_markdown, re.IGNORECASE):
                    parts = re.split(section_3_pattern, final_report_markdown, flags=re.IGNORECASE)
                    if len(parts) >= 3:
                        # parts[1] - заголовок раздела 3
                        # parts[2] - контент раздела 3 до следующего раздела
                        new_content = "\n" + "\n\n".join(contradiction_notes) + "\n\n"
                        # Простая проверка на дубликаты
                        if not any(self._normalize_text(c[:30]) in self._normalize_text(parts[2]) for c in contradiction_notes):
                            parts[2] = new_content + parts[2]
                            final_report_markdown = "".join(parts)
                else:
                    # Если раздела 3 нет, просто добавим в начало
                    final_report_markdown = "# Выявленные противоречия\n" + "\n\n".join(contradiction_notes) + "\n\n" + final_report_markdown

        # Добавляем недостающие критические темы
        added_rows = self._add_missing_critical_topics(evidence_package, rows, final_report_sections, final_report_markdown)
        rows += added_rows
        logger.info(f"Rows after adding {len(added_rows)} critical topics: {len(rows)}")
        
        all_notes = res.get('summary_notes', [])
        
        self._log_progress("Формирование отчета", 95, callback=callback)
        
        # Пост-обработка
        logger.info("Starting post-processing (deduplication, sorting, formatting)...")
        # 1. Дедупликация с нормализацией
        unique_rows = []
        seen_keys = set()
        duplicates_count = 0
        
        for r in rows:
            if not all(k in r for k in ['block', 'finding', 'source_document']):
                logger.warning(f"Row rejection during deduplication: missing mandatory fields. Row: {json.dumps(r, ensure_ascii=False)}")
                continue
                
            key = f"{self._normalize_text(r['block'])}_{self._normalize_text(r['finding'])}_{self._normalize_text(r['source_document'])}"
            if key not in seen_keys:
                seen_keys.add(key)
                unique_rows.append(r)
            else:
                duplicates_count += 1
        
        logger.info(f"Deduplication summary: before={len(rows)}, after={len(unique_rows)}, removed={duplicates_count}")
        logger.info("Post-processing: deduplication completed successfully.")

        # 2. Сортировка
        risk_order = {"High": 0, "Medium": 1, "Low": 2}
        unique_rows.sort(key=lambda x: (
            risk_order.get(x.get('risk_level', 'Medium'), 3), 
            x.get('block', ''), 
            x.get('source_document', '')
        ))

        # 3. Лимит и фильтрация заметок
        final_rows = unique_rows[:100]
        final_notes = []
        seen_notes = set()
        for note in all_notes:
            note_clean = note.strip()
            if note_clean and note_clean not in seen_notes:
                seen_notes.add(note_clean)
                final_notes.append(note_clean)
        
        final_notes = final_notes[:5]

        logger.info("Post-processing completed successfully.")
        self._log_progress("Готово", 100, "success", callback=callback)
        
        result = {
            "rows": final_rows,
            "final_report_sections": valid_final_report_sections,
            "final_report_markdown": final_report_markdown,
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
        
        logger.info("--- [FINAL USER REPORT (MARKDOWN)] ---")
        logger.info(final_report_markdown)
        logger.info("--- [END OF FINAL USER REPORT] ---")
        
        logger.info("--- [FINAL JSON RESULT] ---")
        logger.info(json.dumps(result, ensure_ascii=False, indent=2))
        logger.info("--- [END OF FINAL JSON] ---")
            
        return result

    def analyze_tender(self, files: List[Dict[str, str]], tender_id: str = "unknown", callback=None) -> Dict[str, Any]:
        """
        Legacy wrapper for analyze_full_package.
        """
        return self.analyze_full_package(files, tender_id=tender_id, callback=callback)

