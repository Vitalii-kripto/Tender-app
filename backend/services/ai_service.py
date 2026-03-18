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
    Используется стабильная модель gemini-3-flash-preview.
    """
    def __init__(self):
        print("🤖 Initializing AiService...")
        self.api_key = os.getenv("GEMINI_API_KEY") or os.getenv("API_KEY")
        if not self.api_key:
            logger.warning("GEMINI_API_KEY not found in environment variables.")
            self.client = None
        else:
            try:
                self.client = genai.Client(api_key=self.api_key)
                logger.info("Gemini Client initialized.")
            except Exception as e:
                logger.error(f"Failed to initialize Gemini Client: {e}")
                self.client = None

    def classify_documents(self, docs: list):
        """
        Классифицирует документы на 'contract' и 'other'.
        docs: list of dicts {"filename": str, "text": str}
        """
        print(f"🤖 AI Classifying {len(docs)} documents...")
        if not self.client:
            return {"contract": [], "other": [d['filename'] for d in docs]}
            
        filenames = [d['filename'] for d in docs]
        logger.info(f"Classifying documents: {filenames}")
        
        prompt = f"""
        Роль: Юрист по тендерам.
        Задача: Раздели список файлов на две группы по их названиям и смыслу (если понятно из названия):
        1. "contract": проекты контрактов, договоров и приложения к ним (спецификации, ТЗ к контракту, графики).
        2. "other": вся остальная закупочная документация (извещение, требования к участнику, критерии оценки, инструкции, обоснование цены).
        
        ВАЖНО: Используй только те имена файлов, которые даны в списке ниже. Не меняй их и не придумывай новые.
        
        СПИСОК ФАЙЛОВ:
        {json.dumps(filenames, ensure_ascii=False)}
        
        ВЕРНИ ТОЛЬКО JSON:
        {{ "contract": ["имя_файла1", ...], "other": ["имя_файла2", ...] }}
        """
        try:
            response = self.client.models.generate_content(
                model='gemini-3-flash-preview',
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json"
                )
            )
            text = response.text
            if "```json" in text:
                text = text.split("```json")[1].split("```")[0].strip()
            elif "```" in text:
                text = text.split("```")[1].split("```")[0].strip()
            return json.loads(text)
        except Exception as e:
            logger.error(f"Classification Error: {e}")
            return {"contract": [], "other": filenames}

    def analyze_legal_v2(self, contract_text: str = "", other_text: str = ""):
        """Выполняет юридический анализ по двум промтам"""
        if not self.client:
            return []

        results = []
        
        # Промпт №1: Контракт
        if contract_text:
            logger.info("Running Prompt #1 (Contract)")
            prompt1 = f"""
            Ты — юрист по тендерному праву, работающий в интересах Поставщика материалов. Проанализируй только проект контракта/договора и приложения к нему. Используй только текст переданных документов и применимые нормы закона. Ничего не додумывай. Не добавляй требования и документы, которых нет в документации.

            Задача: вернуть только самые важные условия для поставщика в виде JSON-массива строк одной таблицы.

            Проверь и извлеки только следующие блоки:
            1. Поставка и приемка:
            - сроки поставки;
            - порядок поставки;
            - объемы;
            - комплектность;
            - упаковка;
            - маркировка;
            - порядок приемки по количеству и качеству;
            - сроки приемки;
            - обязательные документы при поставке;
            - кто выполняет разгрузку;
            - за чей счет выполняется разгрузка.
            2. Оплата:
            - срок оплаты;
            - порядок расчетов;
            - есть ли аванс;
            - есть ли ЭДО;
            - есть ли казначейское сопровождение;
            - есть ли условия, влияющие на дату оплаты.
            3. Ответственность:
            - штрафы;
            - пени;
            - убытки;
            - ответственность поставщика;
            - ответственность заказчика.
            4. Односторонний отказ:
            - основания;
            - порядок;
            - рисковые формулировки.
            5. Иные явно рисковые условия договора, которые прямо влияют на поставщика.

            Если по критически важным вопросам информации нет, верни отдельную строку с пометкой «не указано в просмотренных документах» только для:
            - разгрузки;
            - аванса;
            - срока оплаты;
            - ЭДО;
            - казначейского сопровождения;
            - одностороннего отказа.

            Формат ответа:
            верни только JSON без markdown и без пояснений.

            Структура каждой строки:
            {{
              "block": "Поставка и приемка | Оплата | Ответственность | Односторонний отказ | Документы при поставке | Спорные условия",
              "finding": "кратко, по сути, что найдено",
              "risk_level": "high | medium | low",
              "supplier_action": "только действие, прямо следующее из документации или закона",
              "source_document": "имя файла",
              "source_reference": "пункт / раздел / фрагмент",
              "legal_basis": "если прямо применимо, кратко"
            }}

            Обязательные правила:
            - каждая строка должна содержать source_document и source_reference;
            - не пиши общих рассуждений;
            - не пиши советы приложить документы, если они прямо не указаны;
            - не объединяй разные условия в одну длинную строку;
            - если условие явно выгодно или нейтрально, тоже можно вернуть строку с low risk;
            - если в тексте есть противоречие или двусмысленность, обязательно отрази это как риск.

            ТЕКСТ ДОКУМЕНТОВ:
            {contract_text[:100000]}
            """
            try:
                resp1 = self.client.models.generate_content(
                    model='gemini-3-flash-preview',
                    contents=prompt1,
                    config=types.GenerateContentConfig(response_mime_type="application/json")
                )
                text = resp1.text
                # Очистка от markdown если он есть
                if "```json" in text:
                    text = text.split("```json")[1].split("```")[0].strip()
                elif "```" in text:
                    text = text.split("```")[1].split("```")[0].strip()
                
                results.extend(json.loads(text))
            except Exception as e:
                logger.error(f"Prompt 1 Error: {e}")

        # Промпт №2: Остальная документация
        if other_text:
            logger.info("Running Prompt #2 (Other Docs)")
            prompt2 = f"""
            Ты — юрист по тендерному праву, работающий в интересах Поставщика материалов. Проанализируй только остальную закупочную документацию, кроме проекта контракта/договора и приложений к нему. Используй только текст переданных документов и применимые нормы закона. Ничего не додумывай. Не добавляй требования и документы, которых нет в документации.

            Задача: вернуть только самые важные для поставщика выводы в виде JSON-массива строк одной таблицы.

            Проверь и извлеки только следующие блоки:
            1. Документы заявки:
            - какие документы прямо и недвусмысленно входят в состав заявки.
            2. Недопуск и оценка:
            - требования к участнику;
            - требования к заявке;
            - основания отклонения;
            - критерии оценки;
            - условия, из-за которых можно потерять баллы.
            3. Реестры и ограничения:
            - запреты;
            - ограничения;
            - условия допуска;
            - преференции;
            - требования к реестрам РФ/ЕАЭС;
            - обязательность реестрового номера.
            4. Спорные условия:
            - скрытые;
            - двусмысленные;
            - противоречивые;
            - рисковые формулировки.
            5. Иные существенные условия, прямо влияющие на допуск и результат участия.

            Если по критически важным вопросам информации нет, верни отдельную строку с пометкой «не выявлено в просмотренных документах» только для:
            - реестровых требований;
            - ограничений/запретов/условий допуска;
            - обязательности реестрового номера.

            Формат ответа:
            верни только JSON без markdown и без пояснений.

            Структура каждой строки:
            {{
              "block": "Документы заявки | Недопуск/оценка | Реестры/ограничения | Спорные условия",
              "finding": "кратко, по сути, что найдено",
              "risk_level": "high | medium | low",
              "supplier_action": "только действие, прямо следующее из документации или закона",
              "source_document": "имя файла",
              "source_reference": "пункт / раздел / фрагмент",
              "legal_basis": "если прямо применимо, кратко"
            }}

            Обязательные правила:
            - возвращай только те документы заявки, которые прямо перечислены в документации;
            - не строй состав заявки по предположению;
            - каждая строка должна содержать source_document и source_reference;
            - не пиши общих рассуждений;
            - если в документации есть противоречие или неясность, обязательно отражай это;
            - не добавляй требований, которых нет в тексте;
            - если условие нейтральное, допускается строка с low risk.

            ТЕКСТ ДОКУМЕНТОВ:
            {other_text[:100000]}
            """
            try:
                resp2 = self.client.models.generate_content(
                    model='gemini-3-flash-preview',
                    contents=prompt2,
                    config=types.GenerateContentConfig(response_mime_type="application/json")
                )
                text = resp2.text
                # Очистка от markdown если он есть
                if "```json" in text:
                    text = text.split("```json")[1].split("```")[0].strip()
                elif "```" in text:
                    text = text.split("```")[1].split("```")[0].strip()
                
                results.extend(json.loads(text))
            except Exception as e:
                logger.error(f"Prompt 2 Error: {e}")

        # Нормализация и дедупликация
        unique_results = []
        seen = set()
        for r in results:
            # Валидация: отбрасываем без источника
            if not r.get('source_document') or not r.get('source_reference'):
                continue
                
            key = (r.get('block'), r.get('finding'))
            if key not in seen:
                seen.add(key)
                # Нормализация уровня риска
                r['risk_level'] = r.get('risk_level', 'low').lower()
                if r['risk_level'] not in ['high', 'medium', 'low']:
                    r['risk_level'] = 'low'
                unique_results.append(r)
        
        # Сортировка по приоритету
        priority = {'high': 0, 'medium': 1, 'low': 2}
        unique_results.sort(key=lambda x: priority.get(x['risk_level'], 2))
        
        return unique_results

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
                model='gemini-3-flash-preview',
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json"
                )
            )
            text = response.text
            if "```json" in text:
                text = text.split("```json")[1].split("```")[0].strip()
            elif "```" in text:
                text = text.split("```")[1].split("```")[0].strip()
            return json.loads(text)
        except Exception as e:
            logger.error(f"AI Error (find_product_equivalent): {e}", exc_info=True)
            return []

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
                model='gemini-3-flash-preview',
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
                model='gemini-3-flash-preview',
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
                model='gemini-3-flash-preview',
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json"
                )
            )
            text = response.text
            if "```json" in text:
                text = text.split("```json")[1].split("```")[0].strip()
            elif "```" in text:
                text = text.split("```")[1].split("```")[0].strip()
            return json.loads(text)
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
                model='gemini-3-flash-preview',
                contents=prompt,
                config=types.GenerateContentConfig(
                    tools=[types.Tool(google_search=types.GoogleSearch())],
                    response_mime_type="application/json"
                )
            )
            text = response.text
            if "```json" in text:
                text = text.split("```json")[1].split("```")[0].strip()
            return json.loads(text)
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
                model='gemini-3-flash-preview',
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json"
                )
            )
            text = response.text
            if "```json" in text:
                text = text.split("```json")[1].split("```")[0].strip()
            elif "```" in text:
                text = text.split("```")[1].split("```")[0].strip()
            return json.loads(text)
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
                model='gemini-3-flash-preview',
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json"
                )
            )
            text = response.text
            if "```json" in text:
                text = text.split("```json")[1].split("```")[0].strip()
            elif "```" in text:
                text = text.split("```")[1].split("```")[0].strip()
            return json.loads(text)
        except Exception as e:
            logger.error(f"Extraction Error (details): {e}", exc_info=True)
            return {}
