import re
import os
import logging
from typing import List, Dict, Any

logger = logging.getLogger("EvidenceCollector")

class EvidenceCollector:
    def __init__(self):
        # Определение смысловых ролей документов
        self.document_roles = {
            "contract": {
                "patterns": [r"проект (контракта|договора)", r"предмет (контракта|договора)", r"права и обязанности сторон", r"реквизиты сторон", r"проект государственного контракта"],
                "label": "contract"
            },
            "procurement": {
                "patterns": [r"извещение о проведении", r"электронный аукцион", r"открытый конкурс", r"запрос котировок", r"информационная карта"],
                "label": "procurement"
            },
            "tz": {
                "patterns": [r"техническое задание", r"описание объекта закупки", r"характеристики (товара|работы|услуги)", r"спецификация", r"технические требования"],
                "label": "tz"
            },
            "application_rules": {
                "patterns": [r"инструкция по заполнению", r"требования к (содержанию|составу) заявки", r"порядок подачи заявок", r"требования к участникам"],
                "label": "application_rules"
            },
            "nmck": {
                "patterns": [r"обоснование (начальной|нмцк)", r"расчет цены", r"коммерческое предложение", r"анализ рынка"],
                "label": "nmck"
            }
        }

        # Смысловые слоты для извлечения фактов
        self.slots = {
            "delivery_deadline": {
                "patterns": [r"срок поставки", r"период поставки", r"график поставки", r"в течение \d+ (рабочих|календарных) дней", r"не позднее \d{2}\.\d{2}\.\d{4}", r"поставка товара осуществляется", r"сроки (выполнения|оказания|поставки)"],
                "description": "Срок поставки"
            },
            "delivery_place": {
                "patterns": [r"место поставки", r"адрес поставки", r"пункт назначения", r"местонахождение заказчика", r"место (выполнения|оказания|передачи)"],
                "description": "Место поставки"
            },
            "shipping_order": {
                "patterns": [r"порядок (отгрузки|поставки)", r"условия (отгрузки|поставки)", r"транспортировка"],
                "description": "Порядок отгрузки"
            },
            "unloading": {
                "patterns": [r"разгрузка", r"подъем на этаж", r"силами (поставщика|заказчика)", r"разгрузочные работы", r"погрузочно-разгрузочн", r"погрузка"],
                "description": "Разгрузка"
            },
            "acceptance_procedure": {
                "patterns": [r"приемка", r"порядок приемки", r"экспертиза", r"документ о приемке", r"приемочная комиссия"],
                "description": "Порядок приемки"
            },
            "acceptance_deadline": {
                "patterns": [r"срок приемки", r"в течение \d+ (рабочих|календарных) дней после поставки", r"срок подписания документа о приемке", r"приемка осуществляется в течение"],
                "description": "Сроки приемки"
            },
            "refusal_grounds": {
                "patterns": [r"основания (отказа в приемке|неприемки)", r"несоответствие (качеству|количеству)", r"мотивированный отказ", r"отказ от приемки"],
                "description": "Основания отказа в приемке"
            },
            "delivery_documents": {
                "patterns": [
                    r"упд", r"накладная", r"акт (приемки|передачи)", r"счет-фактура", 
                    r"паспорт (качества|изделия)", r"сертификат", r"сопроводительные документы",
                    r"документы, подтверждающие (качество|происхождение)", r"товарно-транспортная накладная",
                    r"деклараци[яи]", r"вместе с товаром передаются", r"при передаче товара"
                ],
                "description": "Документы при поставке"
            },
            "payment_deadline": {
                "patterns": [r"срок оплаты", r"в течение \d+ (рабочих|календарных) дней (с даты|после)", r"оплата производится в течение", r"оплата.*?(?:в течение|не позднее)"],
                "description": "Срок оплаты"
            },
            "payment_procedure": {
                "patterns": [r"порядок (расчетов|оплаты)", r"безналичный расчет", r"платежное поручение", r"источник финансирования"],
                "description": "Порядок расчетов"
            },
            "advance_payment": {
                "patterns": [r"аванс", r"предоплата", r"размер аванса", r"без аванса", r"авансирование", r"выплата аванса"],
                "description": "Аванс"
            },
            "edo_eis": {
                "patterns": [r"эдо", r"электронный документооборот", r"еис", r"электронное актирование", r"цифровой контракт", r"электронной форме"],
                "description": "ЭДО/ЕИС"
            },
            "treasury_support": {
                "patterns": [r"казначейское сопровождение", r"лицевой счет в казначействе", r"казначейский счет", r"казначейств"],
                "description": "Казначейское сопровождение"
            },
            "setoff_or_penalty_deduction": {
                "patterns": [r"удержания", r"удержание (неустойки|штрафа)", r"обеспечение гарантийных обязательств", r"удержание из суммы оплаты"],
                "description": "Удержания"
            },
            "penalties": {
                "patterns": [r"штраф", r"пеня", r"неустойка", r"ответственность сторон", r"размер штрафа"],
                "description": "Штрафы/пени/неустойка"
            },
            "unilateral_refusal": {
                "patterns": [r"односторонний отказ", r"расторжение в одностороннем порядке", r"право на односторонний отказ"],
                "description": "Односторонний отказ"
            },
            "application_composition": {
                "patterns": [
                    r"состав заявки", r"требования к (содержанию|составу) заявки", 
                    r"перечень документов заявки", r"участник должен (предоставить|указать|задекларировать|подтвердить)",
                    r"в составе заявки должны быть", r"документы, подтверждающие соответствие", r"заявка на участие"
                ],
                "description": "Состав заявки"
            },
            "participant_requirements": {
                "patterns": [r"требования к участнику", r"единые требования", r"дополнительные требования", r"отсутствие в рнп", r"членство в сро"],
                "description": "Требования к участнику"
            },
            "evaluation_criteria": {
                "patterns": [r"критерии оценки", r"порядок оценки заявок", r"баллы", r"значимость критериев"],
                "description": "Критерии оценки"
            },
            "rejection_grounds": {
                "patterns": [r"основания (отклонения|отказа в допуске)", r"несоответствие требованиям", r"отклонение заявки"],
                "description": "Основания отклонения"
            },
            "origin_country_requirement": {
                "patterns": [r"страна происхождения", r"наименование страны", r"происхождение товара"],
                "description": "Требование к стране происхождения"
            },
            "national_regime_registries": {
                "patterns": [r"нацрежим", r"национальный режим", r"пп рф \d+", r"реестр (рф|еаэс)", r"реестровый номер", r"приказ минфина \d+н", r"запрет на допуск", r"ограничения допуска", r"условия допуска"],
                "description": "Нацрежим/реестры"
            },
            "security_requirements": {
                "patterns": [r"обеспечение заявки", r"размер обеспечения заявки", r"независимая гарантия", r"банковская гарантия", r"обеспечение исполнения контракта", r"размер обеспечения исполнения", r"обеспечение гарантийных обязательств"],
                "description": "Требования к обеспечению"
            },
            "nmck_method": {
                "patterns": [r"метод сопоставимых рыночных цен", r"анализ рынка", r"затратный метод", r"тарифный метод", r"проектно-сметный метод", r"нормативный метод", r"метод определения нмцк"],
                "description": "Метод НМЦК"
            },
            "nmck_value": {
                "patterns": [r"нмцк", r"начальная (максимальная|) цена", r"цена контракта", r"итого"],
                "description": "Значение НМЦК"
            }
        }


    def _detect_role(self, text: str) -> str:
        """
        Определяет смысловую роль документа по его содержимому.
        """
        scores = {role: 0 for role in self.document_roles}
        sample_text = text[:20000].lower() # Анализируем начало документа (увеличено до 20к)

        for role, config in self.document_roles.items():
            for pattern in config["patterns"]:
                if re.search(pattern, sample_text):
                    scores[role] += 1
        
        max_role = max(scores, key=scores.get)
        if scores[max_role] > 0:
            # Если есть несколько сильных ролей, это смешанный документ
            top_roles = [r for r, s in scores.items() if s == scores[max_role]]
            if len(top_roles) > 1:
                return "mixed"
            return self.document_roles[max_role]["label"]
        
        return "unknown"

    def _extract_slot_value(self, slot_id: str, text: str, match: re.Match) -> str:
        """
        Извлекает конкретное значение факта, а не просто заголовок.
        """
        start = match.end()
        # Берем следующие 500 символов после найденного паттерна
        context = text[start:start+500].strip()
        
        # Очистка от лишних пробелов и переносов
        context = re.sub(r'\s+', ' ', context)
        
        if slot_id in ["delivery_deadline", "payment_deadline", "acceptance_deadline"]:
            # Ищем даты или количество дней
            val_match = re.search(r'(\d{2}\.\d{2}\.\d{4}|\d+\s*(?:\([^)]+\)\s*)?(?:рабочи[хм]|календарны[хм]|банковски[хм])?\s*дн[ейя])', context, re.IGNORECASE)
            if val_match:
                # Попробуем захватить условие (например, "с момента подписания")
                condition_match = re.search(r'((?:с даты|с момента|после|от даты|со дня).*?)(?:\.|\;|$)', context[val_match.end():], re.IGNORECASE)
                condition = f" {condition_match.group(1).strip()}" if condition_match else ""
                return f"{val_match.group(0).strip()}{condition}"
                
        elif slot_id == "delivery_place":
            # Ищем адрес (начинается с г., ул., обл., край, республика, индекс)
            val_match = re.search(r'((?:г\.|город|ул\.|обл\.|край|республика|Российская Федерация|РФ|\d{6}).{5,150}?)(?:\.|\;|$)', context, re.IGNORECASE)
            if val_match:
                return val_match.group(1).strip()
                
        elif slot_id == "unloading":
            # Ищем кто выполняет, за чей счет, входит ли в цену
            who = re.search(r'(силами.*?поставщика|силами.*?заказчика|поставщик.*?своими силами|разгрузка.*?поставщиком)', context, re.IGNORECASE)
            expense = re.search(r'(за счет.*?поставщика|за счет.*?заказчика)', context, re.IGNORECASE)
            included = re.search(r'(включает.*?разгрузк|входит в цену|включена в цену)', context, re.IGNORECASE)
            
            res = []
            if who: res.append(f"Кто выполняет: {who.group(1).strip()}")
            if expense: res.append(f"За чей счет: {expense.group(1).strip()}")
            if included: res.append(f"Входит ли в цену: {included.group(1).strip()}")
            
            if res:
                return "; ".join(res)
                
        elif slot_id in ["delivery_documents", "application_composition"]:
            # Берем первые 300 символов как перечень
            return context[:300] + "..." if len(context) > 300 else context
            
        # По умолчанию берем первые 100 символов после паттерна
        default_val = context[:100]
        # Если контекст пустой, возвращаем сам паттерн
        if not default_val:
            return match.group(0).strip()
            
        return f"{default_val}..."

    def _extract_nmcc_facts(self, text: str) -> Dict[str, Any]:
        """
        Извлекает нормализованные факты по НМЦК.
        """
        facts = {
            "total_sum": "не найдено",
            "method": "не определен",
            "sources_count": 0,
            "has_cp": False
        }
        
        # Ищем итоговую сумму (более надежный паттерн)
        sum_patterns = [
            r"(?:итого|всего|нмцк|начальная цена|максимальная цена).*?(\d[\d\s,.]*)\s*(?:руб|₽)",
            r"цена контракта.*?(\d[\d\s,.]*)\s*(?:руб|₽)"
        ]
        for p in sum_patterns:
            sum_match = re.search(p, text, re.IGNORECASE)
            if sum_match:
                facts["total_sum"] = sum_match.group(1).strip()
                break
            
        # Метод расчета
        methods = {
            "сопоставимые рыночные цены": [r"метод сопоставимых рыночных цен", r"анализ рынка"],
            "затратный": [r"затратный метод"],
            "тарифный": [r"тарифный метод"],
            "проектно-сметный": [r"проектно-сметный метод"],
            "нормативный": [r"нормативный метод"]
        }
        
        for method_name, patterns in methods.items():
            for p in patterns:
                if re.search(p, text, re.IGNORECASE):
                    facts["method"] = method_name
                    break
            if facts["method"] != "не определен":
                break
            
        # Коммерческие предложения
        cp_matches = re.findall(r"коммерческое предложение", text, re.IGNORECASE)
        facts["sources_count"] = len(cp_matches)
        facts["has_cp"] = len(cp_matches) > 0
        
        return facts

    def collect_evidence(self, files: List[Dict[str, str]]) -> Dict[str, Any]:
        """
        Извлекает факты (слоты) из документов детерминированно.
        """
        # Дедупликация файлов (.doc и .docx)
        unique_files = {}
        for f in files:
            filename = f.get('filename', 'unknown')
            base_name, ext = os.path.splitext(filename)
            ext = ext.lower()
            
            # Если уже есть файл с таким же базовым именем
            if base_name in unique_files:
                existing_ext = os.path.splitext(unique_files[base_name]['filename'])[1].lower()
                # Предпочитаем .docx перед .doc
                if ext == '.docx' and existing_ext == '.doc':
                    logger.info(f"Deduplication: replacing {unique_files[base_name]['filename']} with {filename}")
                    unique_files[base_name] = f
                else:
                    logger.info(f"Deduplication: skipping {filename} in favor of {unique_files[base_name]['filename']}")
            else:
                unique_files[base_name] = f
                
        deduped_files = list(unique_files.values())

        evidence_package = {
            "documents": [],
            "slots": {slot_id: [] for slot_id in self.slots.keys()},
            "contradictions": [],
            "all_sources": [f.get('filename', 'unknown') for f in deduped_files]
        }

        for f in deduped_files:
            filename = f.get('filename', 'unknown')
            text = f.get('text', '')
            if not text:
                continue

            role = self._detect_role(text)
            doc_info = {
                "filename": filename,
                "role": role,
                "slots_found": 0
            }

            # НМЦК специфичный анализ
            if role == "nmck":
                nmcc_facts = self._extract_nmcc_facts(text)
                evidence_package["slots"]["nmck_value"].append({
                    "source_document": filename,
                    "slot_name": "Значение НМЦК",
                    "slot_value": f"Сумма: {nmcc_facts['total_sum']}, Источников КП: {nmcc_facts['sources_count']}",
                    "confidence": 0.95,
                    "evidence_text": text[:1000],
                    "source_reference": "весь документ"
                })
                evidence_package["slots"]["nmck_method"].append({
                    "source_document": filename,
                    "slot_name": "Метод НМЦК",
                    "slot_value": nmcc_facts['method'],
                    "confidence": 0.95,
                    "evidence_text": text[:1000],
                    "source_reference": "весь документ"
                })
                doc_info["slots_found"] += 2

            # Ищем паттерны в тексте для каждого слота по всему документу
            for slot_id, config in self.slots.items():
                # Пропускаем НМЦК если уже обработали как роль
                if slot_id in ["nmck_value", "nmck_method"] and role == "nmck":
                    continue

                for pattern in config["patterns"]:
                    # Ищем все вхождения по всему тексту
                    matches = list(re.finditer(pattern, text, re.IGNORECASE))
                    for m in matches[:15]: # Увеличили лимит вхождений
                        # Извлекаем контекст (слот)
                        start = max(0, m.start() - 600)
                        end = min(len(text), m.end() + 1200)
                        chunk = text[start:end].strip()
                        
                        # Извлекаем точное значение (matched text)
                        value = self._extract_slot_value(slot_id, text, m)
                        
                        # Пытаемся найти ссылку на пункт (улучшено)
                        ref_match = re.search(r"(?:п\.|пункт|раздел|статья|приложение)\s*(\d+[\d.]*)", text[max(0, m.start()-150):m.start()])
                        source_ref = ref_match.group(1) if ref_match else "не указан"

                        evidence_package["slots"][slot_id].append({
                            "source_document": filename,
                            "slot_name": config["description"],
                            "slot_value": value,
                            "evidence_text": chunk,
                            "source_reference": source_ref,
                            "confidence": 0.85 if role != "unknown" else 0.65,
                            "offset": m.start()
                        })
                        doc_info["slots_found"] += 1
                        logger.info(f"Extracted slot '{slot_id}' from '{filename}': {value[:100]}")

            if doc_info["slots_found"] == 0:
                doc_info["status"] = "слоты не найдены"
            else:
                doc_info["status"] = "обработано"

            evidence_package["documents"].append(doc_info)
            logger.info(f"Processed {filename}: role={role}, slots={doc_info['slots_found']}")

        # Поиск противоречий между документами
        self._detect_contradictions(evidence_package)

        return evidence_package

    def _detect_contradictions(self, package: Dict[str, Any]):
        """
        Ищет потенциальные противоречия между данными из разных файлов.
        """
        slots_to_compare = [
            "delivery_deadline", "payment_deadline", "delivery_place", 
            "acceptance_deadline", "delivery_documents", "application_composition", 
            "rejection_grounds", "evaluation_criteria", "national_regime_registries",
            "security_requirements"
        ]
        
        for slot_id in slots_to_compare:
            if slot_id not in package["slots"]:
                continue
                
            items = package["slots"][slot_id]
            if not items:
                continue
                
            # Группируем по источникам
            by_source = {}
            for item in items:
                src = item["source_document"]
                if src not in by_source:
                    by_source[src] = []
                by_source[src].append(item)
                
            sources = list(by_source.keys())
            if len(sources) > 1:
                desc = self.slots[slot_id]["description"]
                
                # Сравниваем все пары источников
                for i in range(len(sources)):
                    for j in range(i + 1, len(sources)):
                        src1 = sources[i]
                        src2 = sources[j]
                        
                        val1 = by_source[src1][0].get("slot_value", "н/д")
                        val2 = by_source[src2][0].get("slot_value", "н/д")
                        
                        # Сравниваем значения, чтобы убедиться, что они действительно разные
                        if val1.lower() != val2.lower() and val1.lower() not in val2.lower() and val2.lower() not in val1.lower():
                            package["contradictions"].append({
                                "slot_name": desc,
                                "value_1": val1,
                                "source_1": src1,
                                "value_2": val2,
                                "source_2": src2,
                                "contradiction_reason": "Разные значения в разных документах",
                                "severity": "High"
                            })
                            logger.info(f"Detected contradiction for slot '{slot_id}': '{val1}' ({src1}) vs '{val2}' ({src2})")

    def format_for_llm(self, package: Dict[str, Any]) -> str:
        """
        Формирует структурированный Evidence Package для LLM.
        """
        output = "=== EVIDENCE Package (СТРУКТУРИРОВАННЫЕ УЛИКИ) ===\n\n"
        
        # Список документов и их ролей
        output += "--- СПИСОК ДОКУМЕНТОВ И ИХ РОЛЕЙ ---\n"
        for doc in package["documents"]:
            output += f"- {doc['filename']}: {doc['role']} ({doc['status']})\n"
        output += "\n"

        # Список пустых слотов
        empty_slots = []
        for slot_id, items in package["slots"].items():
            if not items:
                empty_slots.append(self.slots[slot_id]["description"])
                
        if empty_slots:
            output += "--- ПУСТЫЕ СЛОТЫ (ИНФОРМАЦИЯ ОТСУТСТВУЕТ В ДОКУМЕНТАХ) ---\n"
            for es in empty_slots:
                output += f"- {es}\n"
            output += "\n"

        # 1. Потенциальные противоречия
        if package["contradictions"]:
            output += "!!! ВНИМАНИЕ: ПОТЕНЦИАЛЬНЫЕ ПРОТИВОРЕЧИЯ МЕЖДУ ДОКУМЕНТАМИ !!!\n"
            for con in package["contradictions"]:
                output += f"- ТЕМА: {con['slot_name']}\n"
                output += f"  ИСТОЧНИК 1: {con['source_1']} (Значение: '{con['value_1']}')\n"
                output += f"  ИСТОЧНИК 2: {con['source_2']} (Значение: '{con['value_2']}')\n"
                output += f"  ПРИЧИНА: {con.get('contradiction_reason', 'Разные значения')}, ВАЖНОСТЬ: {con['severity']}\n"
            output += "\n"

        # 2. Данные по слотам
        for slot_id, items in package["slots"].items():
            if not items:
                continue
                
            desc = self.slots[slot_id]["description"]
            output += f"--- СЛОТ: {desc} ({slot_id}) ---\n"
            
            # Группируем по источникам
            by_source = {}
            for item in items:
                src = item.get("source_document", "unknown")
                if src not in by_source:
                    by_source[src] = []
                by_source[src].append(item)
            
            for src, slot_items in by_source.items():
                output += f"ИСТОЧНИК: {src}\n"
                for i, item in enumerate(slot_items):
                    if i >= 5: break # Лимит фрагментов увеличен
                    output += f"ФАКТ {i+1} (Значение: '{item.get('slot_value')}', Ссылка: {item.get('source_reference', 'н/д')})\n"
                output += "\n"
            output += "\n"

        return output
