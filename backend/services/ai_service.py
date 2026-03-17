import os
from google import genai
from google.genai import types
from dotenv import load_dotenv
import json
import re
import logging

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("ai_service_log.txt", encoding='utf-8', mode='w'), # mode='w' перезаписывает файл
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("AiService")

# Загружаем переменные окружения (.env)
load_dotenv()

class AiService:
    """
    Сервис для работы с Google Gemini API.
    Выполняет анализ рисков, подбор аналогов и проверку соответствия.
    Используется стабильная модель gemini-2.5-flash.
    """
    def __init__(self):
        self.api_key = os.getenv("API_KEY")
        if not self.api_key:
            logger.warning("API_KEY not found in environment variables.")
            self.client = None
        else:
            self.client = genai.Client(api_key=self.api_key)
            logger.info("Gemini Client initialized.")

    def analyze_legal_risks(self, text: str):
        if not self.client:
            logger.error("Analyze Legal Risks called without API Key.")
            return [{"risk_level": "High", "description": "API Key не настроен на сервере."}]

        logger.info(f"Analyzing legal risks. Text length: {len(text)}")
        prompt = f"""
        Роль: Юрист по тендерному праву РФ (44-ФЗ, 223-ФЗ).
        Задача: Проанализируй текст тендерной документации и найди риски.
        
        ТЕКСТ:
        {text[:30000]}
        
        Верни JSON массив рисков:
        [{{ "document": "...", "requirement": "...", "deadline": "...", "risk_level": "High/Medium/Low", "description": "..." }}]
        """
        
        try:
            response = self.client.models.generate_content(
                model='gemini-2.5-flash',
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json"
                )
            )
            return json.loads(response.text)
        except Exception as e:
            logger.error(f"AI Error (analyze_legal_risks): {e}", exc_info=True)
            return []

    def find_product_equivalent(self, tender_specs: str, catalog: list):
        if not self.client:
            logger.error("Find product equivalent called without API Key.")
            return [{"id": "error", "match_reason": "API Key missing", "similarity_score": 0}]

        logger.info(f"Finding product equivalent. Specs: {tender_specs[:50]}... Catalog size: {len(catalog)}")
        # Превращаем каталог в легкий контекст
        catalog_context = json.dumps([{"id": p['id'], "title": p['title'], "specs": p['specs']} for p in catalog])

        prompt = f"""
        Роль: Технический эксперт по гидроизоляции.
        Задача: Подобрать НАИЛУЧШИЙ аналог из каталога для запроса.
        
        ЗАПРОС (Товар/Характеристики): {tender_specs[:1000]}
        
        КАТАЛОГ ПОСТАВЩИКА: {catalog_context}
        
        ИНСТРУКЦИЯ:
        1. Сравни характеристики запроса с каталогом.
        2. Если точного совпадения нет, ищи ближайший аналог по свойствам (основа, толщина, гибкость).
        3. Если запрос слишком общий, предложи самый популярный товар этой категории.
        
        Верни JSON (массив):
        [{{ "id": "id товара", "match_reason": "Объяснение: совпадает основа, толщина и т.д.", "similarity_score": 95 }}]
        """

        try:
            response = self.client.models.generate_content(
                model='gemini-2.5-flash',
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json"
                )
            )
            return json.loads(response.text)
        except Exception as e:
            logger.error(f"AI Error (find_product_equivalent): {e}", exc_info=True)
            return []

    def analyze_legal_batch(self, tender, files_text: dict):
        if not self.client:
            raise Exception("API Key missing")

        logger.info(f"Analyzing legal batch for tender {tender.id}. Files: {list(files_text.keys())}")
        
        # 1. Классификация документов
        contract_texts = []
        other_texts = []
        
        for filename, text in files_text.items():
            fname_lower = filename.lower()
            if any(kw in fname_lower for kw in ['договор', 'контракт', 'проект', 'соглашение', 'приложение']):
                contract_texts.append(f"--- ФАЙЛ: {filename} ---\n{text}")
            else:
                other_texts.append(f"--- ФАЙЛ: {filename} ---\n{text}")
                
        has_contract_project = len(contract_texts) > 0
        
        all_rows = []
        
        # 2. Анализ договора (если есть)
        if contract_texts:
            contract_combined = "\n\n".join(contract_texts)
            # Ограничиваем размер текста для API
            contract_combined = contract_combined[:100000] 
            
            prompt_contract = f"""
            Роль: Старший юрист по тендерному праву РФ (44-ФЗ, 223-ФЗ).
            Задача: Проанализируй проект контракта/договора и найди критичные для поставщика условия и риски.
            
            ТЕКСТ КОНТРАКТА:
            {contract_combined}
            
            ИНСТРУКЦИЯ:
            1. Ищи условия по: срокам и порядку поставки (в т.ч. разгрузка), порядку оплаты (аванс, ЭДО, казначейское сопровождение), ответственности (штрафы, пени), одностороннему отказу, гарантийным обязательствам.
            2. Если важного условия (например, аванса или разгрузки) НЕТ в тексте, явно укажи это как риск.
            3. Опирайся ТОЛЬКО на предоставленный текст. Не выдумывай.
            4. Верни массив JSON объектов со следующей структурой:
            [
              {{
                "block": "Название блока (например, 'Оплата', 'Поставка', 'Ответственность')",
                "finding": "Что именно сказано в тексте (или чего нет)",
                "risk_level": "High", "Medium" или "Low",
                "supplier_action": "Рекомендация для поставщика (что учесть, что сделать)",
                "source_document": "Название файла из текста",
                "source_reference": "Пункт/раздел договора (если есть)",
                "legal_basis": "Ссылка на закон (опционально)",
                "doc_group": 1
              }}
            ]
            """
            
            try:
                response = self.client.models.generate_content(
                    model='gemini-2.5-flash',
                    contents=prompt_contract,
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json",
                        temperature=0.1
                    )
                )
                contract_results = json.loads(response.text)
                if isinstance(contract_results, list):
                    all_rows.extend(contract_results)
            except Exception as e:
                logger.error(f"Error analyzing contract: {e}", exc_info=True)

        # 3. Анализ остальной документации
        if other_texts:
            other_combined = "\n\n".join(other_texts)
            other_combined = other_combined[:100000]
            
            prompt_other = f"""
            Роль: Специалист по тендерной документации РФ (44-ФЗ, 223-ФЗ).
            Задача: Проанализируй извещение, информационную карту и требования к участникам.
            
            ТЕКСТ ДОКУМЕНТАЦИИ:
            {other_combined}
            
            ИНСТРУКЦИЯ:
            1. Ищи: требования к участникам (лицензии, СРО, реестры), ограничения/преимущества (СМП, нацрежим), критерии оценки заявок, обеспечение заявки/контракта, требования к составу заявки (какие конкретно документы нужны).
            2. Опирайся ТОЛЬКО на текст.
            3. Верни массив JSON объектов со следующей структурой:
            [
              {{
                "block": "Название блока (например, 'Требования к участникам', 'Обеспечение', 'Состав заявки')",
                "finding": "Что именно сказано в тексте",
                "risk_level": "High", "Medium" или "Low",
                "supplier_action": "Рекомендация для поставщика",
                "source_document": "Название файла из текста",
                "source_reference": "Пункт/раздел (если есть)",
                "legal_basis": "Ссылка на закон (опционально)",
                "doc_group": 2
              }}
            ]
            """
            
            try:
                response = self.client.models.generate_content(
                    model='gemini-2.5-flash',
                    contents=prompt_other,
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json",
                        temperature=0.1
                    )
                )
                other_results = json.loads(response.text)
                if isinstance(other_results, list):
                    all_rows.extend(other_results)
            except Exception as e:
                logger.error(f"Error analyzing other docs: {e}", exc_info=True)

        # 4. Агрегация и подсчет рисков
        high_risks = sum(1 for r in all_rows if r.get('risk_level') == 'High')
        medium_risks = sum(1 for r in all_rows if r.get('risk_level') == 'Medium')
        low_risks = sum(1 for r in all_rows if r.get('risk_level') == 'Low')
        
        # Сортировка по уровню риска
        risk_weight = {'High': 3, 'Medium': 2, 'Low': 1}
        all_rows.sort(key=lambda x: risk_weight.get(x.get('risk_level', 'Low'), 0), reverse=True)

        return {
            "tender_id": tender.id,
            "eis_number": tender.eis_number or tender.id,
            "title": tender.title,
            "description": tender.description,
            "initial_price": tender.initial_price,
            "initial_price_text": tender.initial_price_text,
            "initial_price_value": tender.initial_price_value,
            "status": "success",
            "summary": {
                "high_risks": high_risks,
                "medium_risks": medium_risks,
                "low_risks": low_risks,
                "has_contract_project": has_contract_project,
                "unread_files": 0 # Будет обновлено в main.py
            },
            "rows": all_rows
        }

    def search_products_internet(self, query: str):
        """Поиск аналогов в интернете с использованием Google Search Grounding"""
        if not self.client:
            return "API Key missing"

        logger.info(f"Searching internet for product: {query}")
        # Промпт усилен для поиска АНАЛОГОВ и ЦЕН
        prompt = f"""
        ЗАДАЧА: Выполни поиск в Google и найди доступные в РФ гидроизоляционные материалы по запросу: "{query}".
        
        ЕСЛИ ЗАПРОШЕН БРЕНД (например, Технониколь):
        - Найди этот товар.
        - Найди 1-2 прямых АНАЛОГА от других производителей (Изофлекс, Оргкровля, КРЗ и др.), если они сопоставимы по качеству.
        
        ДЛЯ КАЖДОГО ТОВАРА УКАЖИ:
        1. **Полное название** (Бренд + Марка).
        2. *Характеристики*: Основа (Стеклоткань/Полиэфир), Толщина (мм), Гибкость (°C).
        3. *Цена*: Найди актуальную розничную или оптовую цену (укажи дату или источник, если видно).
        4. *Статус*: Является ли это прямым аналогом запрошенного товара.
        
        Используй актуальные данные с сайтов: tstn.ru, gidroizol.ru, petrovich.ru, krovlya-opt.ru.
        
        Ответ верни в формате Markdown. Сделай акцент на сравнении цены и характеристик.
        """
        
        try:
            # Используем Google Search Tool
            response = self.client.models.generate_content(
                model='gemini-2.5-flash',
                contents=prompt,
                config=types.GenerateContentConfig(
                    tools=[types.Tool(google_search=types.GoogleSearch())]
                )
            )
            return response.text
        except Exception as e:
            logger.error(f"Error searching internet: {e}", exc_info=True)
            return f"Error searching internet: {e}"

    def enrich_product_specs(self, product_name: str):
        """Ищет реальные характеристики товара в интернете по названию (для волшебной палочки)"""
        if not self.client:
            return "API Key missing"

        logger.info(f"Enriching specs for: {product_name}")
        prompt = f"""
        ЗАДАЧА: Найти официальный Технический Лист (TDS) или страницу товара в магазине для: "{product_name}".
        ОБЯЗАТЕЛЬНО ИСПОЛЬЗУЙ GOOGLE SEARCH. Мне нужны точные цифры, а не галлюцинации.
        
        Найди параметры:
        1. Толщина (мм)
        2. Вес (кг/м2)
        3. Гибкость на брусе (градусы Цельсия)
        4. Теплостойкость (градусы Цельсия)
        5. Разрывная сила (Н)
        6. Тип основы (Полиэфир / Стеклоткань / Стеклохолст)

        СФОРМИРУЙ ОТВЕТ ОДНОЙ СТРОКОЙ:
        "Основа: [Тип], Толщина: [X]мм, Вес: [Y]кг/м2, Гибкость: [Z]С, Теплостойкость: [W]С."
        
        Если данных нет в поиске, напиши: "Спецификация не найдена в интернете."
        """
        
        try:
            response = self.client.models.generate_content(
                model='gemini-2.5-flash',
                contents=prompt,
                config=types.GenerateContentConfig(
                    tools=[types.Tool(google_search=types.GoogleSearch())]
                )
            )
            
            if not response.text:
                return "Характеристики не найдены в поисковой выдаче."
                
            # Очистка от лишнего форматирования, если модель решит добавить markdown
            text = response.text.strip().replace('**', '').replace('*', '')
            return text
        except Exception as e:
            logger.error(f"Error enriching specs: {e}", exc_info=True)
            return f"Ошибка поиска характеристик: {e}"

    def extract_products_from_text(self, text: str):
        """Извлекает список товаров и их характеристик из неструктурированного текста (КП, Смета)"""
        if not self.client:
            return []

        logger.info(f"Extracting products from text. Length: {len(text)}")
        prompt = f"""
        Роль: Парсер строительных смет.
        Задача: Извлеки из текста список гидроизоляционных материалов и их характеристики.
        Игнорируй работы (укладка, монтаж), только материалы.
        
        ТЕКСТ:
        {text[:20000]}
        
        ВЕРНИ JSON массив:
        [
          {{
            "name": "Название материала",
            "quantity": "количество (если есть)",
            "specs": "строка с характеристиками (толщина, вес, основа и т.д.)"
          }}
        ]
        """
        try:
            response = self.client.models.generate_content(
                model='gemini-2.5-flash',
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json"
                )
            )
            return json.loads(response.text)
        except Exception as e:
            logger.error(f"Extraction Error: {e}", exc_info=True)
            return []

    def compare_requirements_vs_proposal(self, requirements_text: str, proposal_json_str: str):
        """
        ТРОЙНАЯ ПРОВЕРКА:
        1. ТЗ (Требования)
        2. Предложение (Заявленное)
        3. Интернет (Реальные ТТХ) - выполняется через Grounding
        """
        if not self.client:
            return {"score": 0, "summary": "API Key missing", "items": []}

        logger.info("Comparing requirements vs proposal.")
        prompt = f"""
        Роль: Строгий технадзор и аудитор.
        Задача: Проведи аудит предложения поставщика на соответствие ТЗ.
        
        ВАЖНО: Для каждого товара в предложении используй Google Search, чтобы найти его РЕАЛЬНЫЕ характеристики (TDS) и проверить, не обманывает ли поставщик.
        
        ТРЕБОВАНИЯ ЗАКАЗЧИКА (ТЗ):
        {requirements_text[:10000]}
        
        ПРЕДЛОЖЕНИЕ ПОСТАВЩИКА (ТОВАРЫ):
        {proposal_json_str}
        
        ИНСТРУКЦИЯ:
        1. Сопоставь товары из Предложения с пунктами ТЗ.
        2. ИСПОЛЬЗУЙ Google Search, чтобы найти реальные характеристики предложенных товаров.
        3. Сравни: Требование <-> Заявленное в КП <-> Реальное (из интернета).
        4. Если Заявленное совпадает с Реальным, но не подходит под ТЗ -> FAIL (Несоответствие).
        5. Если Заявленное подходит под ТЗ, но в Реальности характеристики хуже -> FAKE (Обман/Ошибка в КП).
        
        ВЕРНИ JSON:
        {{
            "score": 0-100,
            "summary": "Общий вывод.",
            "items": [
                {{
                    "requirement_name": "Требование ТЗ",
                    "proposal_name": "Товар в КП",
                    "real_specs_found": "Кратко что нашел в интернете",
                    "status": "OK" | "FAIL" | "FAKE" | "MISSING",
                    "comment": "Пояснение: например 'В КП написано -25С, но по факту у Бикроста 0С. Это обман.'"
                }}
            ]
        }}
        """
        try:
            response = self.client.models.generate_content(
                model='gemini-2.5-flash',
                contents=prompt,
                config=types.GenerateContentConfig(
                    tools=[types.Tool(google_search=types.GoogleSearch())],
                    response_mime_type="application/json"
                )
            )
            return json.loads(response.text)
        except Exception as e:
            logger.error(f"Comparison Error: {e}", exc_info=True)
            return {"score": 0, "summary": f"Error: {e}", "items": []}

    def check_compliance(self, title: str, description: str, filenames: list):
        if not self.client:
            return {"overallStatus": "failed", "summary": "API Key missing"}
            
        logger.info(f"Checking compliance for: {title}")
        prompt = f"""
        Role: Tender Compliance Officer (Russian FZ-44/223).
        Analyze if uploaded files match requirements for: "{title}".
        Files: {json.dumps(filenames)}
        
        Return JSON:
        {{ "missingDocuments": [], "checkedFiles": [{{ "fileName": "...", "status": "valid/invalid", "comments": [] }}], "overallStatus": "passed/failed/warning", "summary": "..." }}
        """

        try:
            response = self.client.models.generate_content(
                model='gemini-2.5-flash',
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json"
                )
            )
            return json.loads(response.text)
        except Exception as e:
            logger.error(f"Compliance Check Error: {e}", exc_info=True)
            return {"overallStatus": "failed", "summary": str(e)}

    def extract_tender_details(self, text: str):
        """Извлекает структурированные данные о тендере из сырого текста (OCR)"""
        if not self.client:
            return {}

        logger.info(f"Extracting details from text. Length: {len(text)}")
        prompt = f"""
        Ты - ассистент по закупкам. Извлеки данные из текста документации.
        
        ТЕКСТ:
        {text[:15000]}
        
        Извлеки:
        1. Название закупки (title) - коротко и ясно.
        2. НМЦК (initial_price) - числом (float).
        3. Дата окончания подачи (deadline).
        4. Номер закупки (eis_number) - если есть, формат 11-19 цифр.
        5. Описание/ТЗ (description) - краткая выжимка (что нужно).
        
        ВЕРНИ JSON:
        {{
            "title": "...",
            "initial_price": 1000.00,
            "deadline": "dd.mm.yyyy",
            "eis_number": "...",
            "description": "..."
        }}
        """

        try:
            response = self.client.models.generate_content(
                model='gemini-2.5-flash',
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json"
                )
            )
            return json.loads(response.text)
        except Exception as e:
            logger.error(f"Extraction Error: {e}", exc_info=True)
            return {}
