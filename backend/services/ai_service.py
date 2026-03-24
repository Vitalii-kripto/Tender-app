import os
import time
from google import genai
from google.genai import types
from dotenv import load_dotenv
import json
import re
import logging
from backend.config import GEMINI_MODEL

# --- LOGGING SETUP ---
# Загружаем переменные окружения (.env)
env_loaded = load_dotenv()

def setup_ai_logger():
    env_debug_val = os.getenv("LEGAL_AI_DEBUG", "false")
    debug_mode = env_debug_val.lower() == "true"
    logger = logging.getLogger("AiService")
    logger.setLevel(logging.DEBUG if debug_mode else logging.INFO)
    
    # Очищаем старые хендлеры
    if logger.hasHandlers():
        logger.handlers.clear()
        
    log_dir = os.path.join(os.getcwd(), 'backend', 'logs')
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, 'legal_ai.log')
    
    # Используем 'w' (write) для перезаписи при старте и utf-8
    file_handler = logging.FileHandler(log_file, encoding='utf-8', mode='w')
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Также в консоль
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    
    # Чтобы не дублировать в корневой логгер
    logger.propagate = False
    
    # Стартовые логи для проверки .env
    logger.info(f"--- [ENV INITIALIZATION - AI SERVICE] ---")
    logger.info(f".env file found and loaded: {env_loaded}")
    logger.info(f"LEGAL_AI_DEBUG from env: '{env_debug_val}'")
    logger.info(f"Actual DEBUG_MODE: {debug_mode}")
    logger.info(f"GEMINI_MODEL from config: '{GEMINI_MODEL}'")
    logger.info(f"-----------------------------------------")
    
    return logger, debug_mode

logger, DEBUG_MODE = setup_ai_logger()

class AiService:
    """
    Сервис для работы с Google Gemini API.
    Выполняет анализ рисков, подбор аналогов и проверку соответствия.
    Используется модель, указанная в GEMINI_MODEL.
    """
    def __init__(self):
        self.api_key = os.getenv("API_KEY")
        self.model_name = GEMINI_MODEL
        if not self.api_key:
            logger.warning("API_KEY not found in environment variables.")
            self.client = None
        else:
            self.client = genai.Client(api_key=self.api_key)
            logger.info(f"Gemini Client initialized with model: {self.model_name}")

    def _call_ai_with_retry(self, method, **kwargs):
        retries = 3
        start_time = time.time()
        model_name = kwargs.get('model', 'unknown')
        
        # Логирование перед вызовом
        logger.info(f"===== [AI REQUEST START] =====")
        logger.info(f"Method: {method.__name__}")
        logger.info(f"Model: {model_name}")
        
        if DEBUG_MODE:
            prompt_to_log = kwargs.get('contents', 'No contents')
            logger.info("--- [FULL ASSEMBLED PROMPT] ---")
            logger.info(prompt_to_log)
            logger.info("--- [END OF PROMPT] ---")
        
        logger.info(f"===== [AI REQUEST END] =====")

        for attempt in range(retries + 1):
            try:
                response = method(**kwargs)
                
                end_time = time.time()
                duration = end_time - start_time
                
                # Логирование ответа
                logger.info(f"===== [AI RESPONSE START] =====")
                logger.info(f"Duration: {duration:.2f} seconds")
                logger.info(f"Attempt: {attempt}")
                
                if response:
                    text = response.text
                    if DEBUG_MODE:
                        logger.info("--- [FULL RAW RESPONSE] ---")
                        logger.info(text)
                        logger.info("--- [END OF RAW RESPONSE] ---")
                    else:
                        logger.info(f"Raw AI response (first 2000 chars): {text[:2000]}")
                else:
                    logger.warning("AI returned empty response.")
                
                logger.info(f"===== [AI RESPONSE END] =====")
                
                return response
            except Exception as e:
                logger.error(f"AI Error on attempt {attempt}: {e}")
                if attempt < retries:
                    wait_time = (2 ** attempt) + 1
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    raise e
        return None

    def _parse_json_response(self, text: str):
        text = text.strip()
        if text.startswith("```json"):
            text = text.replace("```json", "", 1).replace("```", "", 1).strip()
        elif text.startswith("```"):
            text = text.replace("```", "", 1).replace("```", "", 1).strip()
        
        try:
            return json.loads(text)
        except json.JSONDecodeError as e:
            logger.error(f"JSON Decode Error: {e}")
            logger.error(f"Raw AI Response:\n{text}")
            from fastapi import HTTPException
            raise HTTPException(status_code=500, detail=f"AI returned invalid JSON: {e}. Raw response: {text}")

    def find_product_equivalent(self, tender_specs: str, catalog: list):
        if not self.client:
            logger.error("Find product equivalent called without API Key.")
            return [{"id": "error", "match_reason": "API Key missing", "similarity_score": 0}]

        logger.info(f"Finding product equivalent. Specs: {tender_specs[:50]}... Catalog size: {len(catalog)}")
        # Превращаем каталог в легкий контекст
        catalog_context = json.dumps([{"id": p['id'], "title": p['title'], "specs": p['specs']} for p in catalog], ensure_ascii=False)

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
            response = self._call_ai_with_retry(
                self.client.models.generate_content,
                model=self.model_name,
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json"
                )
            )
            return self._parse_json_response(response.text)
        except Exception as e:
            from fastapi import HTTPException
            if isinstance(e, HTTPException):
                raise e
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
            response = self._call_ai_with_retry(
                self.client.models.generate_content,
                model=self.model_name,
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
            response = self._call_ai_with_retry(
                self.client.models.generate_content,
                model=self.model_name,
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
            response = self._call_ai_with_retry(
                self.client.models.generate_content,
                model=self.model_name,
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json"
                )
            )
            return self._parse_json_response(response.text)
        except Exception as e:
            from fastapi import HTTPException
            if isinstance(e, HTTPException):
                raise e
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
            response = self._call_ai_with_retry(
                self.client.models.generate_content,
                model=self.model_name,
                contents=prompt,
                config=types.GenerateContentConfig(
                    tools=[types.Tool(google_search=types.GoogleSearch())],
                    response_mime_type="application/json"
                )
            )
            return self._parse_json_response(response.text)
        except Exception as e:
            from fastapi import HTTPException
            if isinstance(e, HTTPException):
                raise e
            logger.error(f"Comparison Error: {e}", exc_info=True)
            return {"score": 0, "summary": f"Error: {e}", "items": []}

    def check_compliance(self, title: str, description: str, filenames: list):
        if not self.client:
            return {"overallStatus": "failed", "summary": "API Key missing"}
            
        logger.info(f"Checking compliance for: {title}")
        prompt = f"""
        Role: Tender Compliance Officer (Russian FZ-44/223).
        Analyze if uploaded files match requirements for: "{title}".
        Files: {json.dumps(filenames, ensure_ascii=False)}
        
        Return JSON:
        {{ "missingDocuments": [], "checkedFiles": [{{ "fileName": "...", "status": "valid/invalid", "comments": [] }}], "overallStatus": "passed/failed/warning", "summary": "..." }}
        """

        try:
            response = self._call_ai_with_retry(
                self.client.models.generate_content,
                model=self.model_name,
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json"
                )
            )
            return self._parse_json_response(response.text)
        except Exception as e:
            from fastapi import HTTPException
            if isinstance(e, HTTPException):
                raise e
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
            response = self._call_ai_with_retry(
                self.client.models.generate_content,
                model=self.model_name,
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json"
                )
            )
            return self._parse_json_response(response.text)
        except Exception as e:
            from fastapi import HTTPException
            if isinstance(e, HTTPException):
                raise e
            logger.error(f"Extraction Error: {e}", exc_info=True)
            return {}
