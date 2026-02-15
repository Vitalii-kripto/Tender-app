import asyncio
import logging
import re
from typing import Dict, List, Optional, Set, Tuple
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import (
    urlparse, urljoin, urlunparse, parse_qsl, urlencode
)

import aiohttp
from bs4 import BeautifulSoup
from langchain_community.document_loaders.recursive_url_loader import RecursiveUrlLoader
from sqlalchemy.orm import Session

# ----------------------------
# LOGGING
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("parser_service_log.txt", encoding="utf-8", mode="w"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("GidroizolParser")

# ----------------------------
# ASYNC SETTINGS
# ----------------------------
ASYNC_CONCURRENCY = 12
ASYNC_TIMEOUT = 25
ASYNC_RETRIES = 2


class GidroizolParser:
    """
    Парсер каталога gidroizol.ru:
    - Основной обход: RecursiveUrlLoader
    - Расширение охвата: async обход найденных ссылок (категории/пагинация)
    - Определение "конечной" страницы товара: по твоему рабочему принципу (кнопка "Заказать")
    - Категоризация: из хлебных крошек (родитель -> подкатегория), сохраняем строкой "A / B"
    """

    BASE_URL = "https://gidroizol.ru"
    DOMAIN = "gidroizol.ru"

    # Входные точки каталога (можно расширять при необходимости)
    START_URLS = [
        "https://gidroizol.ru/9",    # общий каталог
        "https://gidroizol.ru/18",   # старшая категория
        "https://gidroizol.ru/149",  # вложенная категория
    ]

    # Чтобы НЕ терять пагинацию: query сохраняем, но чистим мусорные параметры
    DROP_QUERY_PREFIXES = ("utm_",)
    DROP_QUERY_KEYS = {"gclid", "fbclid", "yclid"}

    def __init__(self):
        self.visited_urls: Set[str] = set()   # для обхода
        self.product_urls: Set[str] = set()   # для товаров
        self.product_data: List[Dict] = []

        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0 Safari/537.36"
            ),
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }

    # ----------------------------
    # URL helpers
    # ----------------------------
    def normalize_url(self, url: str) -> str:
        """
        Нормализация URL:
        - приводим домен без www
        - НЕ выкидываем query целиком (важно для PAGEN_*, page=)
        - выкидываем только мусорные query (utm_*, gclid/fbclid/yclid)
        - убираем trailing slash
        """
        url = (url or "").strip()
        if not url:
            return url

        p = urlparse(url)
        scheme = p.scheme or "https"
        netloc = (p.netloc or self.DOMAIN).replace("www.", "")

        path = (p.path or "").rstrip("/")

        kept = []
        for k, v in parse_qsl(p.query, keep_blank_values=True):
            lk = k.lower()
            if lk in self.DROP_QUERY_KEYS:
                continue
            if any(lk.startswith(pref) for pref in self.DROP_QUERY_PREFIXES):
                continue
            kept.append((k, v))

        query = urlencode(kept, doseq=True)
        return urlunparse((scheme, netloc, path, "", query, ""))

    def _is_same_domain(self, url: str) -> bool:
        p = urlparse(url)
        netloc = (p.netloc or "").replace("www.", "")
        return (netloc == "" or netloc.endswith(self.DOMAIN))

    def _is_asset(self, url: str) -> bool:
        return url.lower().endswith((".jpg", ".jpeg", ".png", ".svg", ".ico", ".webp", ".css", ".js", ".pdf", ".zip"))

    # ----------------------------
    # Product page detection (как в твоём рабочем коде)
    # ----------------------------
    def is_product_page(self, soup: BeautifulSoup, url: str = "") -> bool:
        """
        ТВОЙ ПРИНЦИП: конечная страница товара определяется наличием кнопки "Заказать".
        На gidroizol.ru это встречается на карточке товара.
        """
        # Основной селектор
        order_button = soup.select_one('a.order__item.open__popup[href="#order__item"]')
        if order_button:
            button_text = order_button.get_text(strip=True).lower()
            if "заказать" in button_text:
                logger.debug(f"✅ Кнопка 'Заказать' на {url}")
                return True

        # Альтернативные селекторы (как в рабочем коде)
        alternative_selectors = [
            'a[href="#order__item"][class*="order__item"]',
            'a[href="#order__item"][class*="open__popup"]',
            'a.order__item[href="#order__item"]',
            'a.open__popup[href="#order__item"]'
        ]
        for selector in alternative_selectors:
            for element in soup.select(selector):
                if element and "заказать" in element.get_text(strip=True).lower():
                    logger.debug(f"✅ Кнопка 'Заказать' (альт) на {url}")
                    return True

        # Fallback: текст "заказать" встречается, но кнопка может быть другой
        if url:
            order_texts = soup.find_all(string=lambda x: x and "заказать" in x.lower())
            if order_texts:
                # НЕ делаем True по одному слову, но логируем для диагностики
                logger.debug(f"⚠️ На {url} есть текст 'заказать', но кнопка не найдена")

        return False

    # ----------------------------
    # Category extraction (хлебные крошки -> путь категорий)
    # ----------------------------
    def extract_category_path(self, soup: BeautifulSoup) -> str:
        """
        Требование: сохранить категории "как на сайте":
        Старшая категория -> подкатегория -> ... (без Главная/Каталог/Все категории).
        Возвращаем строку вида: "Рулонные ... / ИЗОПЛАСТ"
        """
        breadcrumb_selectors = [
            'ul.col-12[itemtype*="BreadcrumbList"] a span[itemprop="name"]',
            'section#breadcrumbs ul li a',
            'div.breadcrumbs a',
            'nav.breadcrumb a',
            'div.breadcrumb-trail a',
            'div.breadcrumb-nav a',
            'ul.breadcrumb-list a',
        ]

        breadcrumbs: List[str] = []
        for selector in breadcrumb_selectors:
            items = soup.select(selector)
            if items:
                breadcrumbs = [x.get_text(strip=True).strip() for x in items if x.get_text(strip=True).strip()]
                break

        # чистим мусор
        drop = {"главная", "home", "каталог", "все категории", ""}
        categories = [b for b in breadcrumbs if b and b.strip().lower() not in drop]

        # В некоторых случаях хлебные крошки могут включать название товара,
        # поэтому оставим максимум 2-4 уровня категорий (обычно достаточно).
        # Если нужно строго "как на сайте" — можешь убрать срез.
        if len(categories) > 4:
            categories = categories[:4]

        if not categories:
            return "Каталог"

        return " / ".join(categories)

    # ----------------------------
    # Price parse helper
    # ----------------------------
    def _clean_price(self, price_text: str) -> float:
        if not price_text:
            return 0.0
        clean = price_text.replace("\xa0", " ").replace("&nbsp;", " ")
        clean = re.sub(r"[^\d.,]", "", clean.replace(" ", ""))
        clean = clean.replace(",", ".")
        try:
            return float(clean)
        except Exception:
            return 0.0

    def parse_price(self, soup: BeautifulSoup) -> float:
        """
        Достаём цену:
        - как текст "446.28 р./м2 РОЗН"
        - или по селекторам
        """
        page_text = soup.get_text(" ", strip=True)

        m = re.search(r"(\d+[.,]\d+|\d+)\s*р\./[^\s]*\s*РОЗН", page_text)
        if m:
            return self._clean_price(m.group(1))

        m = re.search(r"(\d+[.,]\d+|\d+)\s*р\./", page_text)
        if m:
            return self._clean_price(m.group(1))

        for sel in [".price_val", ".price", ".product-price", ".detail-price", "[itemprop='price']"]:
            el = soup.select_one(sel)
            if el:
                val = self._clean_price(el.get_text(" ", strip=True))
                if val > 0:
                    return val

        return 0.0

    # ----------------------------
    # Product parsing
    # ----------------------------
    def parse_product_page(self, url: str, html: str) -> Optional[Dict]:
        """
        Парсинг карточки товара:
        - определяем конечную по is_product_page (твой принцип)
        - вытаскиваем title, category_path, price, description, specs (если есть)
        """
        try:
            if not self._is_same_domain(url):
                return None
            if self._is_asset(url):
                return None

            soup = BeautifulSoup(html, "html.parser")

            if not self.is_product_page(soup, url):
                return None

            title = "Не указано"
            for sel in [
                "section#tovar h1", "h1", "div.product-title", "header.product-header h1",
                "div.product__title h1"
            ]:
                el = soup.select_one(sel)
                if el and el.get_text(strip=True):
                    title = el.get_text(strip=True)
                    break

            category_path = self.extract_category_path(soup)

            price = self.parse_price(soup)

            # описание (широкие селекторы)
            description = ""
            for sel in [
                "div.product-description",
                "div.desc",
                "[itemprop='description']",
                "div.detail-text",
                ".tabs__content",
            ]:
                el = soup.select_one(sel)
                if el:
                    txt = el.get_text(" ", strip=True)
                    if txt and len(txt) > 30:
                        description = txt[:2000]
                        break

            # характеристики (если найдём)
            specs: Dict[str, str] = {}
            rows = soup.select("div.table_dop-info .table-row, table.specs tr, .product-features li")
            for row in rows:
                cells = row.select("div.table-cell, td, span")
                if len(cells) >= 2:
                    k = cells[0].get_text(strip=True).replace(":", "")
                    v = cells[1].get_text(strip=True)
                    if k and v:
                        specs[k] = v

            return {
                "url": self.normalize_url(url),
                "title": title,
                "category": category_path,  # <-- ключевое поле: "Рулонные ... / ИЗОПЛАСТ"
                "price": price,
                "description": description,
                "specs": specs,
                "material_type": "Рулонный" if "рулон" in category_path.lower() else "Гидроизоляция",
                "type": "product",
            }

        except Exception as e:
            logger.error(f"Error parsing {url}: {e}")
            return None

    # ----------------------------
    # RecursiveUrlLoader extractor
    # ----------------------------
    def extractor(self, html: str) -> str:
        return html

    # ----------------------------
    # Async crawling (расширяем охват)
    # ----------------------------
    async def _fetch_html(self, session: aiohttp.ClientSession, url: str) -> Optional[str]:
        for attempt in range(ASYNC_RETRIES + 1):
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=ASYNC_TIMEOUT)) as resp:
                    if resp.status != 200:
                        return None
                    return await resp.text(errors="ignore")
            except Exception:
                if attempt >= ASYNC_RETRIES:
                    return None
                await asyncio.sleep(0.6)
        return None

    def _extract_links_from_soup(self, soup: BeautifulSoup, base_url: str) -> Set[str]:
        out: Set[str] = set()
        for a in soup.select("a[href]"):
            href = a.get("href")
            if not href:
                continue
            href = href.strip()
            if href.startswith("#") or href.lower().startswith("javascript:"):
                continue
            full = urljoin(base_url, href)
            if not self._is_same_domain(full):
                continue
            if self._is_asset(full):
                continue
            out.add(self.normalize_url(full))
        return out

    async def _process_urls_async(self, initial_urls: Set[str], base_url: str):
        """
        Асинхронный обход ссылок:
        - берём новые URL
        - добавляем в visited
        - если это товар -> парсим
        - если это категория/пагинация -> вытаскиваем новые ссылки
        """
        semaphore = asyncio.Semaphore(ASYNC_CONCURRENCY)

        async with aiohttp.ClientSession(headers=self.headers) as session:
            queue = set(initial_urls)

            while queue:
                batch = list(queue)[:ASYNC_CONCURRENCY * 3]
                queue.difference_update(batch)

                async def handle(url: str):
                    async with semaphore:
                        norm = self.normalize_url(url)
                        if norm in self.visited_urls:
                            return

                        self.visited_urls.add(norm)
                        html = await self._fetch_html(session, url)
                        if not html:
                            return

                        soup = BeautifulSoup(html, "html.parser")

                        # попытка распарсить как товар
                        data = self.parse_product_page(url, html)
                        if data:
                            purl = data["url"]
                            if purl not in self.product_urls:
                                self.product_urls.add(purl)
                                self.product_data.append(data)
                                logger.info(f"✅ PRODUCT: {data['title']} | {data['category']} | {purl}")
                            return

                        # иначе вытаскиваем ссылки дальше (категории/пагинация)
                        links = self._extract_links_from_soup(soup, url)
                        for l in links:
                            if l not in self.visited_urls:
                                queue.add(l)

                await asyncio.gather(*(handle(u) for u in batch))

    # ----------------------------
    # Main crawl
    # ----------------------------
    async def crawl_pages(self, start_url: str, max_depth: int = 7):
        """
        1) RecursiveUrlLoader даёт первичный пул страниц
        2) Из них вытаскиваем дополнительные ссылки и догружаем async-обходом
        """
        logger.info(f"Starting crawl from: {start_url}")

        loader = RecursiveUrlLoader(
            url=start_url,
            max_depth=max_depth,
            extractor=self.extractor,
            prevent_outside=True,
            timeout=15,
            use_async=False,
            # ВАЖНО: не режем каталог агрессивно (иначе будет 13 документов)
            exclude_dirs=["contacts", "about", "news", "blog", "articles", "login", "auth", "basket"],
            headers=self.headers,
        )

        # loader.load() блокирующий — выносим в thread
        documents = await asyncio.to_thread(loader.load)
        logger.info(f"RecursiveUrlLoader finished. Loaded {len(documents)} documents from {start_url}")

        # Диагностика: если вдруг снова мало — увидишь, что реально загрузилось
        if len(documents) < 50:
            logger.warning("Loaded very few documents. Possible reason: redirect/www, exclude_dirs, or blocking.")
            for d in documents[:20]:
                logger.warning(f"Loaded URL sample: {d.metadata.get('source')}")

        filtered_urls: Set[str] = set()

        # 1) парсим то, что уже загружено loader-ом
        for doc in documents:
            url = doc.metadata.get("source", "")
            if not url:
                continue
            if not self._is_same_domain(url):
                continue
            if self._is_asset(url):
                continue

            norm = self.normalize_url(url)
            if norm in self.visited_urls:
                continue
            self.visited_urls.add(norm)

            parsed = self.parse_product_page(url, doc.page_content)
            if parsed:
                purl = parsed["url"]
                if purl not in self.product_urls:
                    self.product_urls.add(purl)
                    self.product_data.append(parsed)

            # вытаскиваем ссылки из страниц (для async-догрузки)
            soup = BeautifulSoup(doc.page_content, "html.parser")
            links = self._extract_links_from_soup(soup, url)
            for l in links:
                if l not in self.visited_urls:
                    filtered_urls.add(l)

        # 2) догружаем async-обходом
        await self._process_urls_async(filtered_urls, start_url)

        logger.info(f"Finished crawl from {start_url}. Total products: {len(self.product_data)}")

    # ----------------------------
    # ---- DB UPSERT ----
    # ----------------------------
    def _upsert_products_to_db(self, db: Session) -> int:
        """
        Сохраняем товары в БД.
        Подстрой импорт/поля под свой проект при необходимости.
        """
        try:
            from backend.models import ProductModel  # <-- если путь другой, поменяй
        except Exception:
            # fallback на относительный импорт внутри backend/services
            from ..models import ProductModel  # type: ignore

        saved = 0
        for p in self.product_data:
            url = p.get("url")
            if not url:
                continue

            existing = db.query(ProductModel).filter(ProductModel.url == url).first()
            if existing:
                existing.title = p.get("title", existing.title)
                existing.category = p.get("category", getattr(existing, "category", "Каталог"))
                existing.material_type = p.get("material_type", getattr(existing, "material_type", ""))
                existing.price = p.get("price", getattr(existing, "price", 0))
                existing.specs = p.get("specs", getattr(existing, "specs", {}))
                existing.description = p.get("description", getattr(existing, "description", "")) if hasattr(existing, "description") else getattr(existing, "description", "")
            else:
                obj = ProductModel(
                    title=p.get("title", "Не указано"),
                    category=p.get("category", "Каталог"),
                    material_type=p.get("material_type", ""),
                    price=p.get("price", 0),
                    specs=p.get("specs", {}),
                    url=url,
                    description=p.get("description", ""),
                )
                db.add(obj)
                saved += 1

        db.commit()
        return saved

    # ----------------------------
    # Public API used by FastAPI service
    # ----------------------------
    async def parse_and_save(self, db: Session) -> int:
        """
        Основной метод, который вызывает твой эндпоинт /api/parse-catalog
        """
        # сбрасываем состояние
        self.visited_urls.clear()
        self.product_urls.clear()
        self.product_data.clear()

        # запускаем обход с нескольких входных точек (как на сайте)
        for u in self.START_URLS:
            await self.crawl_pages(u, max_depth=7)

        saved = self._upsert_products_to_db(db)
        logger.info(f"DB upsert done. New saved: {saved}. Total parsed: {len(self.product_data)}")
        return saved
