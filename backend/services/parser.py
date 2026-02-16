import asyncio
import logging
import re
from typing import Dict, List, Optional, Set
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

    # Москва = city=1 (по ссылке выбора города на сайте) :contentReference[oaicite:1]{index=1}
    MOSCOW_CITY_ID = "1"
    CITY_PARAM = "city"

    START_URLS = [
        "https://gidroizol.ru/9",    # каталог
        "https://gidroizol.ru/18",
        "https://gidroizol.ru/149",
    ]

    DROP_QUERY_PREFIXES = ("utm_",)
    DROP_QUERY_KEYS = {"gclid", "fbclid", "yclid"}

    def __init__(self):
        self.visited_urls: Set[str] = set()
        self.product_urls: Set[str] = set()
        self.product_data: List[Dict] = []

        # Держим “московский контекст” и через query, и через cookie.
        # (У сайта после ?city=1 может быть редирект на чистый URL, а город хранится в cookie.)
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0 Safari/537.36"
            ),
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            # важно: подстраховка, если сайт читает город из cookie
            "Cookie": f"{self.CITY_PARAM}={self.MOSCOW_CITY_ID}",
        }

    # ----------------------------
    # URL helpers (Москва-only)
    # ----------------------------
    def _is_asset(self, url: str) -> bool:
        return url.lower().endswith(
            (".jpg", ".jpeg", ".png", ".svg", ".ico", ".webp", ".css", ".js", ".pdf", ".zip")
        )

    def _is_same_domain(self, url: str) -> bool:
        netloc = (urlparse(url).netloc or "").replace("www.", "")
        return netloc == "" or netloc.endswith(self.DOMAIN)

    def _drop_tracking_qs(self, pairs: List[tuple]) -> List[tuple]:
        kept = []
        for k, v in pairs:
            lk = k.lower()
            if lk in self.DROP_QUERY_KEYS:
                continue
            if any(lk.startswith(pref) for pref in self.DROP_QUERY_PREFIXES):
                continue
            kept.append((k, v))
        return kept

    def _ensure_moscow_city(self, url: str) -> str:
        """
        Всегда приводим URL к московскому контексту:
        - сохраняем query (пагинация и т.п.)
        - удаляем мусорные query (utm/gclid/...)
        - параметр city всегда = 1
        """
        url = (url or "").strip()
        if not url:
            return url

        p = urlparse(url)
        scheme = p.scheme or "https"
        netloc = (p.netloc or self.DOMAIN).replace("www.", "")
        path = (p.path or "").rstrip("/")

        qs = self._drop_tracking_qs(list(parse_qsl(p.query, keep_blank_values=True)))

        # убираем любые city=... и ставим city=1
        qs = [(k, v) for (k, v) in qs if k.lower() != self.CITY_PARAM]
        qs.append((self.CITY_PARAM, self.MOSCOW_CITY_ID))

        query = urlencode(qs, doseq=True)
        return urlunparse((scheme, netloc, path, "", query, ""))

    def _get_city_from_url(self, url: str) -> Optional[str]:
        p = urlparse(url)
        for k, v in parse_qsl(p.query, keep_blank_values=True):
            if k.lower() == self.CITY_PARAM:
                return v
        return None

    def normalize_url(self, url: str) -> str:
        """
        Нормализация = “московский URL” (city=1 всегда присутствует).
        """
        return self._ensure_moscow_city(url)

    def _is_allowed_url(self, url: str) -> bool:
        """
        Фильтр ссылок:
        - только наш домен
        - не ассеты
        - не ссылки смены города на другой
        """
        if not url or not self._is_same_domain(url) or self._is_asset(url):
            return False

        city = self._get_city_from_url(url)
        if city is not None and city != self.MOSCOW_CITY_ID:
            # отбрасываем ссылки на другие города, чтобы не было дублей
            return False

        return True

    # ----------------------------
    # Product detection
    # ----------------------------
    def _has_price_text(self, soup: BeautifulSoup) -> bool:
        txt = soup.get_text(" ", strip=True)
        return bool(re.search(r"(\d+[.,]\d+|\d+)\s*р\./", txt))

    def is_product_page(self, soup: BeautifulSoup, url: str = "") -> bool:
        # 1) кнопки “заказать/купить/в корзину”
        selectors = [
            'a.order__item.open__popup[href="#order__item"]',
            'a[href="#order__item"][class*="order__item"]',
            'a[href="#order__item"][class*="open__popup"]',
            'button[class*="order"]',
            'button[class*="buy"]',
            'a[class*="buy"]',
        ]
        for sel in selectors:
            for el in soup.select(sel):
                t = el.get_text(strip=True).lower()
                if "заказать" in t or "в корзину" in t or "купить" in t:
                    return True

        # 2) fallback: h1 + цена
        if soup.select_one("h1") and self._has_price_text(soup):
            return True

        return False

    # ----------------------------
    # Category extraction
    # ----------------------------
    def extract_category_path(self, soup: BeautifulSoup) -> str:
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

        h1 = soup.select_one("h1")
        if h1:
            h1t = h1.get_text(strip=True)
            if cats and cats[-1] == h1t:
                cats = cats[:-1]

        return " / ".join(cats[-3:]) if cats else "Каталог"

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

        m = re.search(r"(\d+[.,]\d+|\d+)\s*р\./[^\s]*\s*РОЗН", text)
        if m:
            return self._clean_price(m.group(1))

        m = re.search(r"(\d+[.,]\d+|\d+)\s*р\./", text)
        if m:
            return self._clean_price(m.group(1))

        for sel in [".price_val", ".price", ".product-price", ".detail-price", "[itemprop='price']"]:
            el = soup.select_one(sel)
            if el:
                val = self._clean_price(el.get_text(" ", strip=True))
                if val > 0:
                    return val

        return 0.0

    def parse_specs(self, soup: BeautifulSoup) -> Dict[str, str]:
        specs: Dict[str, str] = {}
        row_selectors = [
            "div.table_dop-info .table-row",
            "table.specs tr",
            "table tr",
            ".product-features li",
        ]

        rows = []
        for rs in row_selectors:
            rows = soup.select(rs)
            if rows:
                break

        for row in rows:
            tds = row.select("td")
            if len(tds) >= 2:
                k = tds[0].get_text(" ", strip=True).replace(":", "")
                v = tds[1].get_text(" ", strip=True)
                if k and v:
                    specs[k] = v
                continue

            cells = row.select("div.table-cell")
            if len(cells) >= 2:
                k = cells[0].get_text(" ", strip=True).replace(":", "")
                v = cells[1].get_text(" ", strip=True)
                if k and v:
                    specs[k] = v
                continue

            txt = row.get_text(" ", strip=True)
            if ":" in txt:
                k, v = txt.split(":", 1)
                k = k.strip().replace(":", "")
                v = v.strip()
                if k and v:
                    specs[k] = v

        return specs

    # ----------------------------
    # Product parsing (Москва-only)
    # ----------------------------
    def parse_product_page(self, url: str, html: str) -> Optional[Dict]:
        """
        Важно: url здесь уже должен быть “московским” (city=1).
        """
        try:
            if not self._is_allowed_url(url):
                return None

            soup = BeautifulSoup(html, "html.parser")

            if not self.is_product_page(soup, url):
                return None

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
            specs = self.parse_specs(soup)

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

            # сохраняем ссылку ТОЛЬКО в “московском” виде
            product_url = self.normalize_url(url)

            return {
                "url": product_url,
                "title": title,
                "category": category_path,
                "price": price,
                "description": description,
                "specs": specs,
                "material_type": "Рулонный" if "рулон" in category_path.lower() else "Гидроизоляция",
                "type": "product",
                "city_id": self.MOSCOW_CITY_ID,
                "city_name": "Москва",
            }
        except Exception as e:
            logger.error(f"parse_product_page error {url}: {e}")
            return None

    # ----------------------------
    # Async crawling (Москва-only)
    # ----------------------------
    async def _fetch_html(self, session: aiohttp.ClientSession, url: str) -> Optional[str]:
        url = self.normalize_url(url)
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
            if not self._is_allowed_url(full):
                continue

            out.add(self.normalize_url(full))
        return out

    async def _process_urls_async(self, initial_urls: Set[str]):
        sem = asyncio.Semaphore(ASYNC_CONCURRENCY)
        async with aiohttp.ClientSession(headers=self.headers) as session:
            queue: Set[str] = set(self.normalize_url(u) for u in initial_urls if self._is_allowed_url(u))

            while queue:
                batch = list(queue)[: ASYNC_CONCURRENCY * 3]
                queue.difference_update(batch)

                async def handle(u: str):
                    async with sem:
                        nu = self.normalize_url(u)
                        if nu in self.visited_urls:
                            return
                        self.visited_urls.add(nu)

                        html = await self._fetch_html(session, nu)
                        if not html:
                            return

                        soup = BeautifulSoup(html, "html.parser")
                        data = self.parse_product_page(nu, html)
                        if data:
                            purl = data["url"]
                            if purl not in self.product_urls:
                                self.product_urls.add(purl)
                                self.product_data.append(data)
                                logger.info(f"✅ PRODUCT(MSK): {data['title']} | {data['category']} | {purl}")
                            return

                        # не товар — идём глубже (категории/пагинация), но только в Москве
                        links = self._extract_links(soup, nu)
                        for l in links:
                            nl = self.normalize_url(l)
                            if nl not in self.visited_urls:
                                queue.add(nl)

                await asyncio.gather(*(handle(u) for u in batch))

    # ----------------------------
    # RecursiveUrlLoader (Москва-only)
    # ----------------------------
    def extractor(self, html: str) -> str:
        return html

    async def crawl_pages(self, start_url: str, max_depth: int = 7):
        start_url = self.normalize_url(start_url)
        logger.info(f"Starting crawl (MSK) from: {start_url}")

        loader = RecursiveUrlLoader(
            url=start_url,
            max_depth=max_depth,
            extractor=self.extractor,
            prevent_outside=True,
            timeout=20,
            use_async=False,
            # не режем каталог агрессивно, но отсекаем мусор
            exclude_dirs=["contacts", "about", "news", "blog", "articles", "login", "auth", "basket"],
            headers=self.headers,
        )

        documents = await asyncio.to_thread(loader.load)
        logger.info(f"RecursiveUrlLoader finished. Loaded {len(documents)} documents from {start_url}")

        next_urls: Set[str] = set()

        for doc in documents:
            raw_url = doc.metadata.get("source", "")
            if not raw_url:
                continue

            url = self.normalize_url(raw_url)
            if not self._is_allowed_url(url):
                continue

            if url in self.visited_urls:
                continue
            self.visited_urls.add(url)

            data = self.parse_product_page(url, doc.page_content)
            if data:
                purl = data["url"]
                if purl not in self.product_urls:
                    self.product_urls.add(purl)
                    self.product_data.append(data)

            soup = BeautifulSoup(doc.page_content, "html.parser")
            for l in self._extract_links(soup, url):
                nl = self.normalize_url(l)
                if nl not in self.visited_urls:
                    next_urls.add(nl)

        await self._process_urls_async(next_urls)
        logger.info(f"Finished crawl (MSK) from {start_url}. Total products: {len(self.product_data)}")

    # ----------------------------
    # DB upsert
    # ----------------------------
    def _upsert_products_to_db(self, db: Session) -> int:
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
    # Public entry
    # ----------------------------
    async def parse_and_save(self, db: Session) -> int:
        self.visited_urls.clear()
        self.product_urls.clear()
        self.product_data.clear()

        # Обязательно прогоняем START_URLS тоже в московском виде
        for u in self.START_URLS:
            await self.crawl_pages(u, max_depth=7)

        created = self._upsert_products_to_db(db)
        logger.info(f"DB upsert done (MSK). New created: {created}. Total parsed: {len(self.product_data)}")
        return created
