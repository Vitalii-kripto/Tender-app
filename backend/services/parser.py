import asyncio
import aiohttp
from bs4 import BeautifulSoup
from sqlalchemy.orm import Session
from ..models import ProductModel
import re
import logging
from urllib.parse import urljoin, urlparse, urlunparse
from typing import List, Dict, Optional, Set

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("parser_service_log.txt", encoding='utf-8', mode='w'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("GidroizolParser")

class GidroizolParser:
    """
    Асинхронный парсер для gidroizol.ru.
    Логика портирована из рабочего скрипта пользователя.
    """
    BASE_URL = "https://gidroizol.ru"
    CATALOG_URL = "https://gidroizol.ru/9"
    ASYNC_CONCURRENCY = 8
    ASYNC_TIMEOUT = 20

    def __init__(self):
        self.visited_urls = set()
        self.categories_cache = {}

    def _clean_price(self, price_text: str) -> float:
        """Очищает цену и возвращает float"""
        if not price_text: return 0.0
        # Удаляем нечисловые символы кроме точки и запятой
        clean = re.sub(r'[^\d.,]', '', price_text.replace('&nbsp;', '').replace(' ', ''))
        clean = clean.replace(',', '.')
        try:
            return float(clean)
        except ValueError:
            return 0.0

    def normalize_url(self, url: str) -> str:
        parsed = urlparse(url)
        scheme = parsed.scheme or 'https'
        netloc = parsed.netloc.replace('www.', '')
        path = parsed.path.rstrip('/')
        return urlunparse((scheme, netloc, path, '', '', ''))

    async def _fetch(self, session, url):
        """Асинхронная загрузка страницы"""
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
        try:
            async with session.get(url, headers=headers, timeout=self.ASYNC_TIMEOUT, ssl=False) as response:
                if response.status == 200:
                    return await response.text()
                else:
                    logger.warning(f"Status {response.status} for {url}")
                    return None
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            return None

    async def _extract_site_categories(self, session) -> Dict[str, str]:
        """Извлекает категории с /9 (логика из рабочего скрипта)"""
        categories = {}
        html = await self._fetch(session, self.CATALOG_URL)
        if not html:
            return categories
        
        soup = BeautifulSoup(html, 'html.parser')
        # Селекторы из скрипта
        selectors = [
            'nav.nav-box ul.nav-wrap > li.nav-item > a',
            'nav.nav-box ul.nav-wrap li.nav-item > a',
            'ul.nav-wrap > li.nav-item > a',
            'div.catalog a', 
            'nav.catalog a'
        ]
        
        for sel in selectors:
            links = soup.select(sel)
            for link in links:
                href = link.get('href')
                text = link.get_text(strip=True)
                if href and text:
                    full_href = urljoin(self.BASE_URL, href)
                    # Нормализация ключа (как в скрипте)
                    key = text.strip().lower()
                    if key not in categories:
                        categories[key] = {'name': text, 'href': full_href}
        
        logger.info(f"Extracted {len(categories)} categories.")
        return categories

    def is_product_page(self, soup: BeautifulSoup, url: str) -> bool:
        """
        Проверка валидности страницы товара (логика из скрипта).
        Ищет кнопку 'Заказать' или специфичные попапы.
        """
        # Основной признак из скрипта: a.order__item.open__popup[href="#order__item"]
        order_btn = soup.select_one('a.order__item.open__popup[href="#order__item"]')
        if order_btn:
            btn_text = order_btn.get_text(strip=True).lower()
            if 'заказать' in btn_text:
                return True
            
        # Альтернативные селекторы из скрипта
        alt_selectors = [
            'a[href="#order__item"][class*="order__item"]',
            'a[href="#order__item"][class*="open__popup"]',
            'a.order__item[href="#order__item"]',
            'a.open__popup[href="#order__item"]'
        ]
        for sel in alt_selectors:
            el = soup.select_one(sel)
            if el and 'заказать' in el.get_text(strip=True).lower():
                return True
                
        return False

    def parse_product_page(self, url: str, html: str) -> Optional[Dict]:
        """Парсинг данных товара (логика из скрипта)"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            if not self.is_product_page(soup, url):
                return None

            # 1. Заголовок
            title_selectors = [
                'section#tovar h1', 'h1', 'div.product-title', 'header.product-header h1',
                'div.product-info h1', 'article.product h1', 'div.product-card h1'
            ]
            title = "Не указано"
            for s in title_selectors:
                el = soup.select_one(s)
                if el:
                    title = el.get_text(strip=True)
                    break

            # 2. Категория (Breadcrumbs)
            breadcrumbs = [b.get_text(strip=True) for b in soup.select('div.breadcrumbs a, nav.breadcrumb a, ul.breadcrumb-list a')]
            # Фильтруем "Главная", "Каталог"
            valid_crumbs = [b for b in breadcrumbs if b.lower() not in ['главная', 'home', 'каталог', '', 'все категории']]
            category = valid_crumbs[-1] if valid_crumbs else "Гидроизоляция"

            # 3. Характеристики (из скрипта)
            specs = {}
            char_selectors = [
                'div.table_dop-info.ver2 .table-row', 'div.product-specs .spec-row', 'table.product-table tr',
                'div.product-info .spec-item', 'ul.specifications li'
            ]
            
            for sel in char_selectors:
                rows = soup.select(sel)
                if rows:
                    for row in rows:
                        cells = row.select('div.table-cell, td')
                        if len(cells) >= 2:
                            k = cells[0].get_text(strip=True).replace(':', '')
                            v = cells[1].get_text(strip=True)
                            
                            k_lower = k.lower()
                            # Маппинг в системные имена полей
                            if 'толщина' in k_lower: specs['thickness_mm'] = self._clean_price(v)
                            elif 'вес' in k_lower and 'м2' in k_lower: specs['weight_kg_m2'] = self._clean_price(v)
                            elif 'гибкость' in k_lower: specs['flexibility_temp_c'] = v
                            elif 'разрывная' in k_lower: specs['tensile_strength_n'] = self._clean_price(v)
                            else:
                                specs[k] = v
                    break # Если нашли таблицу характеристик, выходим

            # 4. Цена (Логика из скрипта: РОЗН -> ОПТ -> Мета)
            price = 0.0
            price_blocks = soup.select('div.price-block-wrap .price-block')
            
            found_retail = False
            for block in price_blocks:
                txt = block.get_text(strip=True)
                span = block.select_one('span')
                val_text = span.get_text(strip=True) if span else ""
                
                if 'РОЗН' in txt:
                    price = self._clean_price(val_text)
                    found_retail = True
                    break
            
            if not found_retail:
                # Fallback selectors from script
                fallback_sels = [
                    ('div.dop-price strong', lambda x: x.get_text(strip=True)),
                    ('span.price', lambda x: x.get_text(strip=True)),
                    ('div.price-block', lambda x: x.get_text(strip=True)),
                    ('meta[itemprop="price"]', lambda x: x.get('content', ''))
                ]
                for sel, extractor in fallback_sels:
                    el = soup.select_one(sel)
                    if el:
                        raw = extractor(el)
                        p = self._clean_price(raw)
                        if p > 0:
                            price = p
                            break

            # 5. Описание
            desc_el = soup.select_one('div.product-description, section#tovar .description, div.item-description')
            description = desc_el.get_text(strip=True)[:1000] if desc_el else ""

            return {
                "title": title,
                "url": url,
                "price": price,
                "category": category,
                "material_type": "Рулонный" if "рулон" in category.lower() or "рулон" in title.lower() else "Гидроизоляция",
                "specs": specs,
                "description": description
            }

        except Exception as e:
            logger.error(f"Error parsing HTML {url}: {e}")
            return None

    async def crawl(self, db: Session):
        """Основной цикл обхода"""
        logger.info("Starting Crawl based on provided script logic...")
        
        async with aiohttp.ClientSession() as session:
            # 1. Извлекаем категории (как в скрипте)
            self.categories_cache = await self._extract_site_categories(session)
            
            # 2. Собираем ссылки с ключевых страниц
            start_urls = [self.BASE_URL, self.CATALOG_URL]
            product_links = set()
            
            # Селекторы ссылок из скрипта (полный список)
            link_selectors = [
                'a.product-link', 'a.item-link', 'div.product-list a', 'div.product-card a',
                'div.item-product a', 'h2.left__heading a', 'div.item__left a', 'a[href*="product"]',
                'a[href*="tovar"]', 'a[href*="item"]', 'a[href*="-epp-"]', 'a[href*="-hpp-"]',
                'a[href*="-ekp-"]', 'a[href*="-tkp-"]', 'a[href*="/9/"]', 'a[href*="/lenta-gerlen"]',
                'div.catalog a[href*="/rubiteks-"]', 'div.category a[href*="/9/"]', 'div.product-item a',
                'div.item a[href*="/gidroizol.ru/"]', 'a[href*="/material/"]'
            ]

            for start_url in start_urls:
                html = await self._fetch(session, start_url)
                if not html: continue
                
                soup = BeautifulSoup(html, 'html.parser')
                for selector in link_selectors:
                    links = soup.select(selector)
                    for link in links:
                        href = link.get('href')
                        if href:
                            full_url = urljoin(self.BASE_URL, href)
                            # Простые фильтры
                            if self.BASE_URL not in full_url: continue
                            if any(x in full_url for x in ['login', 'filter', 'sort', 'city']): continue
                            
                            product_links.add(full_url)

            logger.info(f"Found {len(product_links)} potential links. Processing limit: 50.")
            
            # 3. Парсинг товаров
            links_to_process = list(product_links)[:50] # Лимит для демо
            tasks = []
            sem = asyncio.Semaphore(self.ASYNC_CONCURRENCY)

            async def process_url(url):
                async with sem:
                    if url in self.visited_urls: return None
                    self.visited_urls.add(url)
                    
                    html = await self._fetch(session, url)
                    if html:
                        return self.parse_product_page(url, html)
                    return None

            for url in links_to_process:
                tasks.append(process_url(url))
            
            results = await asyncio.gather(*tasks)
            
            # 4. Сохранение
            saved_count = 0
            for data in results:
                if not data: continue
                
                existing = db.query(ProductModel).filter(ProductModel.url == data['url']).first()
                if not existing:
                    prod = ProductModel(
                        title=data['title'],
                        category=data['category'],
                        material_type=data['material_type'],
                        price=data['price'],
                        specs=data['specs'],
                        url=data['url'],
                        description=data['description']
                    )
                    db.add(prod)
                    saved_count += 1
                else:
                    existing.price = data['price']
                    existing.specs = data['specs']
            
            db.commit()
            logger.info(f"Crawl finished. Saved/Updated {saved_count} products.")
            return db.query(ProductModel).all()

    async def parse_and_save(self, db: Session):
        return await self.crawl(db)
