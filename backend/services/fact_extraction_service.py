import re
import json
import logging
from typing import List, Dict, Any, Optional
from backend.logger import logger

class Fact:
    def __init__(self, topic: str, value: Any, source_file: str = "", source_fragment: str = "", 
                 source_page_or_sheet: str = "", confidence: str = "high", status: str = "ok", comment: str = ""):
        self.topic = topic
        self.value = value
        self.source_file = source_file
        self.source_fragment = source_fragment
        self.source_page_or_sheet = source_page_or_sheet
        self.confidence = confidence
        self.status = status
        self.comment = comment

    def to_dict(self):
        return {
            "topic": self.topic,
            "value": self.value,
            "source_file": self.source_file,
            "source_fragment": self.source_fragment,
            "source_page_or_sheet": self.source_page_or_sheet,
            "confidence": self.confidence,
            "status": self.status,
            "comment": self.comment
        }

class FactExtractionService:
    """
    Сервис для детерминированного и ИИ-извлечения фактов по темам.
    """
    def __init__(self, ai_service):
        self.ai_service = ai_service
        self.chunk_cache = {} # {(tender_id, files_key): chunks}

    def normalize_number(self, text: str) -> Optional[float]:
        """Нормализует число из строки (убирает пробелы, меняет запятую на точку)."""
        if not text:
            return None
        # Убираем все пробелы
        clean_text = re.sub(r'\s+', '', str(text))
        # Ищем числовой паттерн
        match = re.search(r'(\d+[.,\d]*)', clean_text)
        if match:
            num_str = match.group(1).replace(',', '.')
            try:
                return float(num_str)
            except ValueError:
                return None
        return None

    def extract_deterministic_facts(self, files_data: List[Dict[str, Any]]) -> List[Fact]:
        """
        Извлекает факты детерминированно (регулярки, таблицы).
        """
        facts = []
        for file in files_data:
            filename = file.get('filename', 'Unknown')
            text = file.get('text', '')
            status = file.get('status', 'ok')
            
            if status != 'ok':
                facts.append(Fact(
                    topic="file_status",
                    value=status,
                    source_file=filename,
                    status=status,
                    comment=file.get('error_message', '')
                ))
                continue

            # Поиск НМЦК
            nmcc_matches = re.finditer(r'(нмцк|начальная\s*\(?максимальная\)?\s*цена\s*контракта)[\s:.-]*([\d\s.,]+)\s*(руб|₽)', text, re.IGNORECASE)
            for match in nmcc_matches:
                raw_val = match.group(2)
                norm_val = self.normalize_number(raw_val)
                if norm_val is not None:
                    fragment = text[max(0, match.start() - 50):min(len(text), match.end() + 50)]
                    facts.append(Fact(
                        topic="nmcc_prices",
                        value={"raw": raw_val.strip(), "normalized": norm_val, "type": "nmcc"},
                        source_file=filename,
                        source_fragment=fragment,
                        confidence="high",
                        status="ok"
                    ))

            # Поиск аванса
            advance_matches = re.finditer(r'(аванс|авансовый\s*платеж)[^\.]*?(\d+(?:[.,]\d+)?)\s*%', text, re.IGNORECASE)
            for match in advance_matches:
                raw_val = match.group(2)
                norm_val = self.normalize_number(raw_val)
                if norm_val is not None:
                    fragment = text[max(0, match.start() - 50):min(len(text), match.end() + 50)]
                    facts.append(Fact(
                        topic="payment",
                        value={"raw": raw_val.strip() + "%", "normalized": norm_val, "type": "advance_percent"},
                        source_file=filename,
                        source_fragment=fragment,
                        confidence="medium",
                        status="ok"
                    ))

            # Поиск штрафов/пеней
            penalty_matches = re.finditer(r'(штраф|пеня|пени|неустойка)[^\.]*?(\d+(?:[.,]\d+)?)\s*%', text, re.IGNORECASE)
            for match in penalty_matches:
                raw_val = match.group(2)
                norm_val = self.normalize_number(raw_val)
                if norm_val is not None:
                    fragment = text[max(0, match.start() - 50):min(len(text), match.end() + 50)]
                    facts.append(Fact(
                        topic="liability",
                        value={"raw": raw_val.strip() + "%", "normalized": norm_val, "type": "penalty_percent"},
                        source_file=filename,
                        source_fragment=fragment,
                        confidence="medium",
                        status="ok"
                    ))
            
            ref_rate_matches = re.finditer(r'(одна|1/)\s*(трехсотая|300)[^\.]*?ставки\s*рефинансирования', text, re.IGNORECASE)
            for match in ref_rate_matches:
                fragment = text[max(0, match.start() - 50):min(len(text), match.end() + 50)]
                facts.append(Fact(
                    topic="liability",
                    value={"raw": "1/300 ставки рефинансирования", "normalized": 1/300, "type": "penalty_ref_rate"},
                    source_file=filename,
                    source_fragment=fragment,
                    confidence="high",
                    status="ok"
                ))

            # Поиск сроков оплаты (в днях)
            payment_days_matches = re.finditer(r'оплат[^\.]*?в\s*течение\s*(\d+)\s*(рабочих|календарных)?\s*дней', text, re.IGNORECASE)
            for match in payment_days_matches:
                raw_val = match.group(1)
                day_type = match.group(2) or "дней"
                norm_val = self.normalize_number(raw_val)
                if norm_val is not None:
                    fragment = text[max(0, match.start() - 50):min(len(text), match.end() + 50)]
                    facts.append(Fact(
                        topic="payment",
                        value={"raw": f"{raw_val} {day_type}", "normalized": norm_val, "type": "payment_days", "day_type": day_type.lower()},
                        source_file=filename,
                        source_fragment=fragment,
                        confidence="high",
                        status="ok"
                    ))

            # Поиск эквивалентов
            text_lower = text.lower()
            forbidden_phrases = [
                "эквивалент не допускается", "без эквивалента", "аналоги не допускаются",
                "поставка эквивалента не предусмотрена", "эквивалент не предусмотрен",
                "не допускается поставка эквивалента", "не подлежит замене на эквивалент",
                "поставка аналогов не допускается", "без аналогов", "строго в соответствии"
            ]
            
            is_forbidden = any(phrase in text_lower for phrase in forbidden_phrases)
            has_equivalent = "эквивалент" in text_lower or "аналог" in text_lower
            
            if is_forbidden:
                facts.append(Fact(topic="equivalents", value="Запрещены", source_file=filename, confidence="high"))
            elif has_equivalent:
                facts.append(Fact(topic="equivalents", value="Разрешены", source_file=filename, confidence="medium"))

            # Поиск количеств и цен в таблицах
            pages = file.get('pages', [])
            for page in pages:
                tables = page.get('tables', [])
                for table_text in tables:
                    lines = table_text.split('\n')
                    if not lines:
                        continue
                    header = lines[0].lower()
                    
                    if any(kw in header for kw in ["наименование", "товар", "услуг", "работ"]) and \
                       any(kw in header for kw in ["количеств", "объем", "кол-во"]):
                        
                        items = []
                        for line in lines[1:]:
                            cells = [c.strip() for c in line.split('|')]
                            if len(cells) >= 2:
                                items.append(line)
                        
                        if items:
                            facts.append(Fact(
                                topic="items_quantities",
                                value={"raw": "\n".join(items[:5]) + ("\n..." if len(items) > 5 else ""), "type": "table_extract"},
                                source_file=filename,
                                source_page_or_sheet=str(page.get('page_num', '')),
                                confidence="medium",
                                status="ok"
                            ))

        return facts

    def _chunk_documents(self, files_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        chunks = []
        chunk_id = 0
        for file in files_data:
            if file.get('status', 'ok') != 'ok':
                continue
            filename = file.get('filename', 'Unknown')
            pages = file.get('pages', [])
            
            if pages:
                for page in pages:
                    page_num = page.get('page_num', 'Unknown')
                    text = page.get('text', '')
                    if not text.strip():
                        continue
                    
                    # Разделяем на параграфы или небольшие блоки
                    paragraphs = [p.strip() for p in text.split('\n\n') if p.strip()]
                    current_chunk_text = ""
                    for p in paragraphs:
                        if len(current_chunk_text) + len(p) > 1500:
                            if current_chunk_text:
                                chunks.append({
                                    "chunk_id": chunk_id,
                                    "source_file": filename,
                                    "source_page_or_sheet": str(page_num),
                                    "text": current_chunk_text
                                })
                                chunk_id += 1
                            current_chunk_text = p + "\n\n"
                        else:
                            current_chunk_text += p + "\n\n"
                    
                    if current_chunk_text:
                        chunks.append({
                            "chunk_id": chunk_id,
                            "source_file": filename,
                            "source_page_or_sheet": str(page_num),
                            "text": current_chunk_text
                        })
                        chunk_id += 1
            else:
                text = file.get('text', '')
                if text.strip():
                    paragraphs = [p.strip() for p in text.split('\n\n') if p.strip()]
                    current_chunk_text = ""
                    for p in paragraphs:
                        if len(current_chunk_text) + len(p) > 1500:
                            if current_chunk_text:
                                chunks.append({
                                    "chunk_id": chunk_id,
                                    "source_file": filename,
                                    "source_page_or_sheet": "Unknown",
                                    "text": current_chunk_text
                                })
                                chunk_id += 1
                            current_chunk_text = p + "\n\n"
                        else:
                            current_chunk_text += p + "\n\n"
                    
                    if current_chunk_text:
                        chunks.append({
                            "chunk_id": chunk_id,
                            "source_file": filename,
                            "source_page_or_sheet": "Unknown",
                            "text": current_chunk_text
                        })
                        chunk_id += 1
        return chunks

    def _filter_chunks_for_topic(self, chunks: List[Dict[str, Any]], topic_id: str) -> List[Dict[str, Any]]:
        keywords = {
            "customer_info": ["заказчик", "инн", "кпп", "огрн", "адрес", "контакт", "телефон", "email", "почта", "реквизит"],
            "subject": ["предмет", "объект", "закупк", "товар", "работ", "услуг", "наименование", "описание"],
            "items_quantities": ["количеств", "объем", "штук", "ед", "измерени", "спецификаци", "таблиц"],
            "nmcc_prices": ["нмцк", "цен", "стоимост", "руб", "коп", "начальн", "максимальн", "смет"],
            "delivery_terms": ["срок", "поставк", "выполнени", "оказани", "этап", "график", "период", "календарн", "рабоч", "дней"],
            "logistics": ["место", "доставк", "адрес", "разгрузк", "приемк", "передач", "транспорт", "склад"],
            "payment": ["оплат", "расчет", "аванс", "казначейск", "сопровождени", "счет", "срок", "дней"],
            "bid_docs": ["заявк", "состав", "документ", "требовани", "участник", "деклараци", "выписк", "свидетельств"],
            "delivery_docs": ["приемк", "документ", "накладн", "акт", "счет-фактур", "упд", "сертификат", "паспорт"],
            "liability": ["ответственност", "штраф", "пен", "неустойк", "расторжени", "односторон", "отказ"],
            "equivalents": ["эквивалент", "аналог", "заменител", "товарный знак", "или"],
            "restrictions": ["ограничени", "национальн", "режим", "запрет", "допуск", "пп рф", "постановлени", "реестр", "российск"],
            "conflicts": ["противоречи", "разногласи", "приоритет", "преимуществ"]
        }
        
        topic_keywords = keywords.get(topic_id, [])
        if not topic_keywords:
            return chunks # Если нет ключевых слов, возвращаем все (fallback)
            
        filtered_chunks = []
        for chunk in chunks:
            text_lower = chunk['text'].lower()
            if any(kw in text_lower for kw in topic_keywords):
                filtered_chunks.append(chunk)
                
        # Если ничего не нашли по ключевым словам, возвращаем все чанки, чтобы не потерять данные
        return filtered_chunks if filtered_chunks else chunks

    def extract_thematic_facts_ai(self, files_data: List[Dict[str, Any]], existing_facts: List[Fact], 
                                 tender_id: str = "N/A", job_id: str = "N/A") -> List[Fact]:
        """
        Извлекает факты по темам с помощью ИИ, отправляя релевантные фрагменты.
        """
        from backend.logger import log_debug_event
        
        # Логируем извлечение документов
        for file in files_data:
            log_debug_event({
                "stage": "document_extraction",
                "job_id": job_id,
                "tender_id": tender_id,
                "filename": file.get('filename', 'Unknown'),
                "status": file.get('status', 'ok'),
                "native_text_length": len(file.get('text', '')),
                "ocr_used": file.get('ocr_used', False),
                "ocr_time": file.get('ocr_time', 0),
                "error_message": file.get('error_message', '')
            })

        topics = [
            {"id": "customer_info", "desc": "заказчик и контакты"},
            {"id": "subject", "desc": "предмет закупки"},
            {"id": "items_quantities", "desc": "позиции и количества"},
            {"id": "nmcc_prices", "desc": "НМЦК и цены"},
            {"id": "delivery_terms", "desc": "сроки поставки / исполнения"},
            {"id": "logistics", "desc": "приемка, разгрузка, логистика"},
            {"id": "payment", "desc": "условия оплаты"},
            {"id": "bid_docs", "desc": "документы заявки"},
            {"id": "delivery_docs", "desc": "документы при поставке"},
            {"id": "liability", "desc": "штрафы, пени, ответственность"},
            {"id": "equivalents", "desc": "эквиваленты / аналоги"},
            {"id": "restrictions", "desc": "национальный режим / реестры / ограничения"},
            {"id": "conflicts", "desc": "противоречия между документами"}
        ]

        all_facts = list(existing_facts)
        
        # Определяем темы, по которым уже есть надежные факты
        high_confidence_topics = set()
        for fact in existing_facts:
            if fact.confidence == "high" and fact.status == "ok":
                high_confidence_topics.add(fact.topic)
        
        # Разбиваем документы на чанки (с кэшированием)
        files_key = tuple(sorted([f.get('filename', '') for f in files_data]))
        cache_key = (tender_id, files_key)
        
        if cache_key in self.chunk_cache:
            logger.info(f"Using cached chunks for tender {tender_id}")
            chunks = self.chunk_cache[cache_key]
        else:
            chunks = self._chunk_documents(files_data)
            self.chunk_cache[cache_key] = chunks

        import concurrent.futures

        def extract_topic(topic):
            if topic['id'] in high_confidence_topics:
                logger.info(f"Skipping AI extraction for topic {topic['id']} (already found with high confidence)")
                return []
                
            # Фильтруем чанки для текущей темы
            topic_chunks = self._filter_chunks_for_topic(chunks, topic['id'])
            
            # Формируем контекст из отфильтрованных чанков
            context_text = ""
            for chunk in topic_chunks:
                context_text += f"\n--- ЧАНК {chunk['chunk_id']} (Файл: {chunk['source_file']}, Стр/Лист: {chunk['source_page_or_sheet']}) ---\n{chunk['text']}\n"
                
            prompt = f"""Ты — юридический ИИ-аналитик. Твоя задача — извлечь факты из предоставленных фрагментов тендерных документов строго по теме: {topic['desc']} ({topic['id']}).

ПРАВИЛА:
1. Ищи информацию только по заданной теме.
2. Верни структурированный результат в формате JSON.
3. Если найдено несколько разных значений, верни их все (массив) и укажи status="conflict".
4. Если данных нет, верни status="not_found", value=null.
5. Не выдумывай данные.

ФОРМАТ ОТВЕТА (JSON):
{{
  "topic": "{topic['id']}",
  "values": [
    {{
      "value": "найденное значение",
      "source_file": "имя файла",
      "source_fragment": "цитата из текста",
      "source_page_or_sheet": "номер страницы или название листа",
      "confidence": "high/medium/low",
      "comment": "пояснение"
    }}
  ],
  "status": "ok/not_found/conflict"
}}

ФРАГМЕНТЫ ДОКУМЕНТОВ:
{context_text}
"""
            topic_facts = []
            attempt = 1
            try:
                from google.genai import types
                response = self.ai_service._call_ai_with_retry(
                    self.ai_service.client.models.generate_content,
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json",
                    )
                )
                
                raw_response = response.text if response else None
                status = "error"
                values = []
                
                if raw_response:
                    try:
                        data = json.loads(raw_response)
                        status = data.get("status", "ok")
                        values = data.get("values", [])
                        
                        if not values and status != "not_found":
                            status = "not_found"
                            
                        if status == "not_found":
                            topic_facts.append(Fact(topic=topic['id'], value=None, status="not_found"))
                        else:
                            for val in values:
                                topic_facts.append(Fact(
                                    topic=topic['id'],
                                    value=val.get("value"),
                                    source_file=val.get("source_file", ""),
                                    source_fragment=val.get("source_fragment", ""),
                                    source_page_or_sheet=val.get("source_page_or_sheet", ""),
                                    confidence=val.get("confidence", "medium"),
                                    status=status,
                                    comment=val.get("comment", "")
                                ))
                    except json.JSONDecodeError as je:
                        logger.error(f"Invalid JSON from AI for topic {topic['id']}: {je}")
                        log_debug_event({
                            "stage": "ai_topic_extraction_error",
                            "job_id": job_id,
                            "tender_id": tender_id,
                            "topic": topic['id'],
                            "error": "Invalid JSON",
                            "raw_response": raw_response
                        })
                        topic_facts.append(Fact(topic=topic['id'], value=None, status="error", comment="Invalid JSON response"))
                
                # Логируем запрос и ответ
                log_debug_event({
                    "stage": "ai_topic_extraction",
                    "job_id": job_id,
                    "tender_id": tender_id,
                    "topic": topic['id'],
                    "model_name": "gemini-3.1-pro-preview",
                    "attempt_number": attempt,
                    "chunk_count": len(topic_chunks),
                    "input_fragments": [c['chunk_id'] for c in topic_chunks],
                    "source_files": list(set([c['source_file'] for c in topic_chunks])),
                    "total_text_size": len(context_text),
                    "raw_model_response": raw_response,
                    "parsed_facts": [f.to_dict() for f in topic_facts],
                    "final_topic_status": status
                })
                
            except Exception as e:
                logger.error(f"Error extracting facts for topic {topic['id']}: {e}")
                log_debug_event({
                    "stage": "ai_topic_extraction_error",
                    "job_id": job_id,
                    "tender_id": tender_id,
                    "topic": topic['id'],
                    "error": str(e)
                })
                topic_facts.append(Fact(topic=topic['id'], value=None, status="error", comment=str(e)))
            return topic_facts

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_topic = {executor.submit(extract_topic, topic): topic for topic in topics}
            for future in concurrent.futures.as_completed(future_to_topic):
                topic = future_to_topic[future]
                try:
                    facts = future.result()
                    all_facts.extend(facts)
                except Exception as exc:
                    logger.error(f"Topic {topic['id']} generated an exception: {exc}")
                    all_facts.append(Fact(topic=topic['id'], value=None, status="error", comment=str(exc)))

        return all_facts

    def merge_facts(self, facts: List[Fact], tender_id: str = "N/A") -> Dict[str, Any]:
        """
        Сверяет и объединяет факты по темам.
        Возвращает словарь, где ключ - тема, значение - итоговый факт (или конфликт).
        """
        from backend.logger import log_debug_event
        merged = {}
        grouped_facts = {}
        
        for fact in facts:
            if fact.topic not in grouped_facts:
                grouped_facts[fact.topic] = []
            grouped_facts[fact.topic].append(fact)
            
        for topic, topic_facts in grouped_facts.items():
            # Фильтруем пустые или ошибочные факты
            valid_facts = [f for f in topic_facts if f.status not in ('not_found', 'error', 'empty') and f.value is not None]
            
            if not valid_facts:
                res = {
                    "topic": topic,
                    "final_value": "not_found",
                    "all_sources": [],
                    "conflict_flag": False,
                    "explanation": "Данные не найдены ни в одном документе."
                }
                merged[topic] = res
                log_debug_event({
                    "stage": "merge",
                    "tender_id": tender_id,
                    "topic": topic,
                    "merge_result": "not_found",
                    "conflict_flag": False
                })
                continue
                
            # Группируем значения по их "смысловому" равенству
            value_groups = [] # List of { "canonical_value": Any, "facts": List[Fact], "normalized_key": str }
            
            for f in valid_facts:
                norm_key = self._get_normalized_key(topic, f.value)
                
                found_group = False
                for group in value_groups:
                    if group["normalized_key"] == norm_key:
                        group["facts"].append(f)
                        found_group = True
                        break
                
                if not found_group:
                    value_groups.append({
                        "canonical_value": f.value,
                        "facts": [f],
                        "normalized_key": norm_key
                    })
            
            all_sources = []
            for f in valid_facts:
                all_sources.append({
                    "file": f.source_file,
                    "page": f.source_page_or_sheet,
                    "fragment": f.source_fragment,
                    "value": f.value,
                    "confidence": f.confidence,
                    "status": f.status
                })
            
            # Логика выбора финального значения
            if len(value_groups) == 1:
                # Все значения совпадают по смыслу
                final_val = value_groups[0]["canonical_value"]
                explanation = "Данные совпадают во всех источниках."
                conflict_flag = False
            else:
                # Есть разные значения. Проверяем надежность источников.
                # Считаем "вес" каждой группы
                for group in value_groups:
                    weight = 0
                    for f in group["facts"]:
                        f_weight = 10
                        if f.confidence == "high": f_weight += 5
                        if f.confidence == "low": f_weight -= 5
                        if f.status == "partial": f_weight -= 3
                        if f.status == "ocr_failed": f_weight -= 7
                        weight += f_weight
                    group["weight"] = weight
                
                # Сортируем по весу
                value_groups.sort(key=lambda x: x["weight"], reverse=True)
                
                top_group = value_groups[0]
                second_group = value_groups[1]
                
                # Если перевес значительный (например, в 2 раза), выбираем лидера
                if top_group["weight"] >= second_group["weight"] * 2:
                    final_val = top_group["canonical_value"]
                    explanation = f"Обнаружены расхождения, но выбран наиболее надежный вариант (вес {top_group['weight']} против {second_group['weight']})."
                    conflict_flag = False
                else:
                    # Реальный конфликт
                    final_val = "conflict"
                    conflict_flag = True
                    
                    # Формируем пояснение конфликта
                    diff_desc = []
                    for i, group in enumerate(value_groups[:3]): # Берем топ-3
                        val_str = str(group["canonical_value"])
                        files = ", ".join(list(set([f.source_file for f in group["facts"]])))
                        diff_desc.append(f"Вариант {i+1}: '{val_str}' (в файлах: {files})")
                    
                    explanation = "Обнаружен конфликт данных: " + "; ".join(diff_desc)
            
            merged[topic] = {
                "topic": topic,
                "final_value": final_val,
                "all_sources": all_sources,
                "conflict_flag": conflict_flag,
                "explanation": explanation
            }
            
            log_debug_event({
                "stage": "merge",
                "tender_id": tender_id,
                "topic": topic,
                "raw_values": [str(f.value) for f in valid_facts],
                "normalized_values": [g["normalized_key"] for g in value_groups],
                "merge_result": str(final_val),
                "conflict_flag": conflict_flag,
                "explanation": explanation
            })
                
        return merged

    def _get_normalized_key(self, topic: str, value: Any) -> str:
        """Возвращает нормализованный ключ для сравнения значений."""
        if value is None:
            return "none"
            
        # 1. Если это числовая структура (из детерминированного извлечения)
        if isinstance(value, dict) and "normalized" in value:
            norm_val = value.get("normalized")
            v_type = value.get("type", "")
            return f"num:{v_type}:{norm_val}"
            
        # 2. Если это список
        if isinstance(value, list):
            # Нормализуем каждый элемент и сортируем
            norm_list = sorted([str(self._normalize_string(item)) for item in value])
            return "list:" + "|".join(norm_list)
            
        # 3. Темы с датами
        date_topics = ["delivery_terms", "payment"] # Упрощенно
        if topic in date_topics:
            # Пытаемся найти дату в строке (очень упрощенно)
            date_match = re.search(r'(\d{2}\.\d{2}\.\d{4})', str(value))
            if date_match:
                return f"date:{date_match.group(1)}"
        
        # 4. По умолчанию - очищенная строка
        return "str:" + self._normalize_string(value)

    def _normalize_string(self, s: Any) -> str:
        if s is None: return ""
        # Убираем лишние пробелы, пунктуацию в конце, приводим к нижнему регистру
        res = re.sub(r'\s+', ' ', str(s)).strip().lower()
        res = re.sub(r'[^\w\s\d]', '', res) # Убираем спецсимволы для сравнения
        return res
