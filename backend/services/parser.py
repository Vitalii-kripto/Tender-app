import asyncio
import logging
import re
from typing import Dict, List, Optional, Set
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse, urljoin, urlunparse, parse_qsl, urlencode

import aiohttp
from bs4 import BeautifulSoup
from langchain_community.document_loaders.recursive_url_loader import RecursiveUrlLoader
from sqlalchemy.orm import Session

logger = logging.getLogger("GidroizolParser")
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler("parser_service_log.txt", encoding="utf-8", mode="w"),
            logging.StreamHandler(),
        ],
    )
logger.setLevel(logging.INFO)

ASYNC_CONCURRENCY = 12
ASYNC_TIMEOUT = 30
ASYNC_RETRIES = 2


class GidroizolParser:
    BASE_URL = "https://gidroizol.ru"
    DOMAIN = "gidroizol.ru"

    # входные точки (чтобы быстрее собрать дерево)
    START_URLS = [
        "https://gidroizol.ru/9",    # общий каталог
        "https://gidroizol.ru/18",   # старшая категория
        "https://gidroizol.ru/149",  # вложенная категория
    ]

    DROP_QUERY_PREFIXES = ("utm_",)
    DROP_QUERY_KEYS = {"gclid", "fbclid", "yclid"}

    def __init__(self):
        self.visited_urls: Set[str] = set()
        self.product_urls: Set[str] = set()
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
        Нормализуем URL, но НЕ убиваем query (важно для пагинации).
        Чистим только мусорные параметры (utm_*, gclid/fbclid/yclid).
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
        netloc = (urlparse(url).netloc or "").replace("www.", "")
        return netloc == "" or netloc.endswith(self.DOMAIN)

    def _is_asset(self, url: str) -> bool:
        return url.lower().endswith(
            (".jpg", ".jpeg", ".png", ".svg", ".ico", ".webp", ".css", ".js", ".pdf", ".zip")
        )

    # ----------------------------
    # Product detection
    # ----------------------------
    def _has_price_text(self, soup: BeautifulSoup) -> bool:
        txt = soup.get_text(" ", strip=True)
        return bool(re.search(r"(\d+[.,]\d+|\d+)\s*р\./", txt))

    def is_product_page(self, soup: BeautifulSoup, url: str = "") -> bool:
        """
        Принцип "кнопка Заказать" сохраняем, НО добавляем fallback:
        если есть h1 и есть цена => считаем карточкой товара.
        (Иначе ты теряешь реальные карточки, как показал тест на /izoplast-p-epp-4-0)
        """

        # 1) основной признак: кнопка "Заказать"
        btn = soup.select_one('a.order__item.open__popup[href="#order__item"]')
        if btn:
            if "заказать" in btn.get_text(strip=True).lower():
                return True

        # 2) альтернативные селекторы "заказать"
        alternative_selectors = [
            'a[href="#order__item"][class*="order__item"]',
            'a[href="#order__item"][class*="open__popup"]',
            'a.order__item[href="#order__item"]',
            'a.open__popup[href="#order__item"]',
            'button[class*="order"]',
            'button[class*="buy"]',
            'a[class*="buy"]',
        ]
        for selector in alternative_selectors:
            for el in soup.select(selector):
                if "заказать" in el.get_text(strip=True).lower():
                    return True
                if "в корзину" in el.get_text(strip=True).lower():
                    return True
                if "купить" in el.get_text(strip=True).lower():
                    return True

        # 3) Fallback-эвристика: h1 + цена
        h1 = soup.select_one("h1")
        if h1 and self._has_price_text(soup):
            return True

        return False

    # ----------------------------
    # Category extraction
    # ----------------------------
    def extract_category_path(self, soup: BeautifulSoup) -> str:
        """
        Категории как на сайте: "Рулонные ... / ИЗОПЛАСТ"
        Берём хлебные крошки, выкидываем "Главная/Каталог".
        """
        breadcrumb_selectors = [
            'ul.col-12[itemtype*="BreadcrumbList"] a span[itemprop="name"]',
            'section#breadcrumbs ul li a',
            'div.breadcrumbs a',
            'nav.breadcrumb a',
            'ul.breadcrumb a',
            '.breadcrumb a',
        ]

        crumbs: List[str] = []
        for sel in breadcrumb_selectors:
            items = soup.select(sel)
            if items:
                crumbs = [x.get_text(strip=True) for x in items if x.get_text(strip=True)]
                break

        drop = {"главная", "home", "каталог", "все категории"}
        cats = [c for c in crumbs if c and c.strip().lower() not in drop]

        if not cats:
            return "Каталог"

        # Иногда последний элемент крошек — товар. Если он совпадает с h1 — убираем.
        h1 = soup.select_one("h1")
        if h1:
            h1t = h1.get_text(strip=True)
            if cats and cats[-1] == h1t:
                cats = cats[:-1]

        # Обычно достаточно 1–3 уровней категорий
        return " / ".join(cats[-3:]) if len(cats) > 3 else " / ".join(cats)

    # ----------------------------
    # Price + specs
    # ----------------------------
    def _clean_price(self, s: str) -> float:
        if not s:
            return 0.0
        s = s.replace("\xa0", " ").replace("&nbsp;", " ")
        s = re.sub(r"[^\d.,]", "", s.replace(" ", ""))
        s = s.replace(",", ".")
        try:
            return float(s)
        except Exception:
            return 0.0

    def parse_price(self, soup: BeautifulSoup) -> float:
        text = soup.get_text(" ", strip=True)

        # РОЗН
        m = re.search(r"(\d+[.,]\d+|\d+)\s*р\./[^\s]*\s*РОЗН", text)
        if m:
            return self._clean_price(m.group(1))

        # любая цена
        m = re.search(r"(\d+[.,]\d+|\d+)\s*р\./", text)
        if m:
            return self._clean_price(m.group(1))

        # селекторы
        for sel in [".price_val", ".price", ".product-price", ".detail-price", "[itemprop='price']"]:
            el = soup.select_one(sel)
            if el:
                val = self._clean_price(el.get_text(" ", strip=True))
                if val > 0:
                    return val

        return 0.0

    def parse_specs(self, soup: BeautifulSoup) -> Dict[str, str]:
        specs: Dict[str, str] = {}

        # несколько вариантов таблиц/блоков характеристик
        row_selectors = [
            "div.table_dop-info .table-row",
            "table.specs tr",
            "table tr",
            ".product-features li",
            ".характеристики li",
        ]

        rows = []
        for rs in row_selectors:
            rows = soup.select(rs)
            if rows:
                break

        for row in rows:
            # table rows
            tds = row.select("td")
            if len(tds) >= 2:
                k = tds[0].get_text(" ", strip=True).replace(":", "")
                v = tds[1].get_text(" ", strip=True)
                if k and v:
                    specs[k] = v
                continue

            # div rows
            cells = row.select("div.table-cell")
            if len(cells) >= 2:
                k = cells[0].get_text(" ", strip=True).replace(":", "")
                v = cells[1].get_text(" ", strip=True)
                if k and v:
                    specs[k] = v
                continue

            # list items "Ключ: Значение"
            txt = row.get_text(" ", strip=True)
            if ":" in txt:
                k, v = txt.split(":", 1)
                k = k.strip().replace(":", "")
                v = v.strip()
                if k and v:
                    specs[k] = v

        return specs

    # ----------------------------
    # Product parsing
    # ----------------------------
    def parse_product_page(self, url: str, html: str) -> Optional[Dict]:
        try:
            if not self._is_same_domain(url) or self._is_asset(url):
                return None

            soup = BeautifulSoup(html, "html.parser")

            if not self.is_product_page(soup, url):
                return None

            # title
            title = None
            for sel in ["section#tovar h1", "h1", "div.product-title h1", "div.product-title"]:
                el = soup.select_one(sel)
                if el and el.get_text(strip=True):
                    title = el.get_text(strip=True)
                    break
            if not title:
                return None

            category_path = self.extract_category_path(soup)
            price = self.parse_price(soup)

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

            specs = self.parse_specs(soup)

            return {
                "url": self.normalize_url(url),
                "title": title,
                "category": category_path,
                "price": price,
                "description": description,
                "specs": specs,
                "material_type": "Рулонный" if "рулон" in category_path.lower() else "Гидроизоляция",
                "type": "product",
            }
        except Exception as e:
            logger.error(f"parse_product_page error {url}: {e}")
            return None

    # ----------------------------
    # Async helpers
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

    def _extract_links(self, soup: BeautifulSoup, base_url: str) -> Set[str]:
        out: Set[str] = set()
        for a in soup.select("a[href]"):
            href = a.get("href")
            if not href:
                continue
            href = href.strip()
            if href.startswith("#") or href.lower().startswith("javascript:"):
                continue
            full = urljoin(base_url, href)
            if not self._is_same_domain(full) or self._is_asset(full):
                continue
            out.add(self.normalize_url(full))
        return out

    async def _process_urls_async(self, initial_urls: Set[str]):
        sem = asyncio.Semaphore(ASYNC_CONCURRENCY)
        async with aiohttp.ClientSession(headers=self.headers) as session:
            queue: Set[str] = set(initial_urls)

            while queue:
                batch = list(queue)[: ASYNC_CONCURRENCY * 3]
                queue.difference_update(batch)

                async def handle(u: str):
                    async with sem:
                        nu = self.normalize_url(u)
                        if nu in self.visited_urls:
                            return
                        self.visited_urls.add(nu)

                        html = await self._fetch_html(session, u)
                        if not html:
                            return

                        soup = BeautifulSoup(html, "html.parser")
                        data = self.parse_product_page(u, html)
                        if data:
                            purl = data["url"]
                            if purl not in self.product_urls:
                                self.product_urls.add(purl)
                                self.product_data.append(data)
                                logger.info(f"✅ PRODUCT: {data['title']} | {data['category']} | {purl}")
                            return

                        # не товар — идём глубже
                        links = self._extract_links(soup, u)
                        for l in links:
                            if l not in self.visited_urls:
                                queue.add(l)

                await asyncio.gather(*(handle(u) for u in batch))

    # ----------------------------
    # RecursiveUrlLoader
    # ----------------------------
    def extractor(self, html: str) -> str:
        return html

    async def crawl_pages(self, start_url: str, max_depth: int = 7):
        logger.info(f"Starting crawl from: {start_url}")

        loader = RecursiveUrlLoader(
            url=start_url,
            max_depth=max_depth,
            extractor=self.extractor,
            prevent_outside=True,
            timeout=20,
            use_async=False,
            # Важно: не отрезаем каталог агрессивно
            exclude_dirs=["contacts", "about", "news", "blog", "articles", "login", "auth", "basket"],
            headers=self.headers,
        )

        documents = await asyncio.to_thread(loader.load)
        logger.info(f"RecursiveUrlLoader finished. Loaded {len(documents)} documents from {start_url}")

        if len(documents) < 50:
            logger.warning("Loaded very few documents. Check redirects/www/exclude_dirs/blocking.")
            for d in documents[:20]:
                logger.warning(f"Loaded URL sample: {d.metadata.get('source')}")

        next_urls: Set[str] = set()

        # Сначала парсим то, что уже загрузил loader, и собираем ссылки на догрузку
        for doc in documents:
            url = doc.metadata.get("source", "")
            if not url or not self._is_same_domain(url) or self._is_asset(url):
                continue

            nurl = self.normalize_url(url)
            if nurl in self.visited_urls:
                continue
            self.visited_urls.add(nurl)

            data = self.parse_product_page(url, doc.page_content)
            if data:
                purl = data["url"]
                if purl not in self.product_urls:
                    self.product_urls.add(purl)
                    self.product_data.append(data)

            soup = BeautifulSoup(doc.page_content, "html.parser")
            for l in self._extract_links(soup, url):
                if l not in self.visited_urls:
                    next_urls.add(l)

        # Догружаем асинхронно
        await self._process_urls_async(next_urls)

        logger.info(f"Finished crawl from {start_url}. Total products: {len(self.product_data)}")

    # ----------------------------
    # DB upsert
    # ----------------------------
    def _upsert_products_to_db(self, db: Session) -> int:
        # подстрой импорт под свой проект при необходимости
        try:
            from backend.models import ProductModel  # type: ignore
        except Exception:
            from ..models import ProductModel  # type: ignore

        created = 0
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
                if hasattr(existing, "description"):
                    existing.description = p.get("description", getattr(existing, "description", ""))
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
                created += 1

        db.commit()
        return created

    # ----------------------------
    # Public entry (FastAPI uses this)
    # ----------------------------
    async def parse_and_save(self, db: Session) -> int:
        self.visited_urls.clear()
        self.product_urls.clear()
        self.product_data.clear()

        for u in self.START_URLS:
            await self.crawl_pages(u, max_depth=7)

        created = self._upsert_products_to_db(db)
        logger.info(f"DB upsert done. New created: {created}. Total parsed: {len(self.product_data)}")
        return created
