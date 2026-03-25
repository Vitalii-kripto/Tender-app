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
            nmcc_matches = re.finditer(r'(нмцк|начальная\s*\(максимальная\)\s*цена\s*контракта)[\s:.-]*([\d\s.,]+)\s*(руб|₽)', text, re.IGNORECASE)
            for match in nmcc_matches:
                raw_val = match.group(2)
                norm_val = self.normalize_number(raw_val)
                if norm_val is not None:
                    fragment = text[max(0, match.start() - 50):min(len(text), match.end() + 50)]
                    facts.append(Fact(
                        topic="nmcc_prices",
                        value={"raw": raw_val.strip(), "normalized": norm_val},
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

        return facts

    def extract_thematic_facts_ai(self, files_data: List[Dict[str, Any]], existing_facts: List[Fact]) -> List[Fact]:
        """
        Извлекает факты по темам с помощью ИИ, отправляя релевантные фрагменты.
        """
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
        
        # Собираем весь текст с разбивкой по страницам/листам
        full_context = ""
        for file in files_data:
            if file.get('status', 'ok') == 'ok':
                filename = file.get('filename', 'Unknown')
                full_context += f"\n\n--- ФАЙЛ: {filename} ---\n"
                pages = file.get('pages', [])
                if pages:
                    for page in pages:
                        page_num = page.get('page_num', 'Unknown')
                        is_sheet = page.get('is_sheet', False)
                        label = "ЛИСТ" if is_sheet else "СТРАНИЦА"
                        full_context += f"\n[{label} {page_num}]\n{page.get('text', '')}\n"
                else:
                    full_context += f"{file.get('text', '')}\n"

        import concurrent.futures

        def extract_topic(topic):
            if topic['id'] in high_confidence_topics:
                logger.info(f"Skipping AI extraction for topic {topic['id']} (already found with high confidence)")
                return []
                
            prompt = f"""Ты — юридический ИИ-аналитик. Твоя задача — извлечь факты из предоставленных тендерных документов строго по теме: {topic['desc']} ({topic['id']}).

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

ДОКУМЕНТЫ:
{full_context}
"""
            topic_facts = []
            try:
                from google.genai import types
                response = self.ai_service._call_ai_with_retry(
                    self.ai_service.client.models.generate_content,
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json",
                    )
                )
                if response and response.text:
                    data = json.loads(response.text)
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
            except Exception as e:
                logger.error(f"Error extracting facts for topic {topic['id']}: {e}")
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

    def merge_facts(self, facts: List[Fact]) -> Dict[str, Any]:
        """
        Сверяет и объединяет факты по темам.
        Возвращает словарь, где ключ - тема, значение - итоговый факт (или конфликт).
        """
        merged = {}
        grouped_facts = {}
        
        for fact in facts:
            if fact.topic not in grouped_facts:
                grouped_facts[fact.topic] = []
            grouped_facts[fact.topic].append(fact)
            
        for topic, topic_facts in grouped_facts.items():
            valid_facts = [f for f in topic_facts if f.status not in ('not_found', 'error', 'ocr_failed', 'empty') and f.value is not None]
            
            if not valid_facts:
                merged[topic] = {
                    "topic": topic,
                    "final_value": "not_found",
                    "all_sources": [],
                    "conflict_flag": False,
                    "explanation": "Данные не найдены ни в одном документе."
                }
                continue
                
            # Сравниваем значения
            unique_values = {}
            for f in valid_facts:
                # Преобразуем значение в строку для сравнения, если это dict (например, nmcc_prices)
                val_key = json.dumps(f.value, sort_keys=True) if isinstance(f.value, dict) else str(f.value).strip().lower()
                if val_key not in unique_values:
                    unique_values[val_key] = []
                unique_values[val_key].append(f)
                
            all_sources = [{"file": f.source_file, "page": f.source_page_or_sheet, "fragment": f.source_fragment, "value": f.value} for f in valid_facts]
            
            if len(unique_values) == 1:
                # Все значения совпадают
                first_fact = valid_facts[0]
                merged[topic] = {
                    "topic": topic,
                    "final_value": first_fact.value,
                    "all_sources": all_sources,
                    "conflict_flag": False,
                    "explanation": "Данные совпадают во всех источниках."
                }
            else:
                # Конфликт
                merged[topic] = {
                    "topic": topic,
                    "final_value": "conflict",
                    "all_sources": all_sources,
                    "conflict_flag": True,
                    "explanation": "Обнаружены противоречивые данные в разных документах."
                }
                
        return merged
