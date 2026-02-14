import asyncio
import aiohttp
from bs4 import BeautifulSoup
from sqlalchemy.orm import Session
from ..models import ProductModel
import re
import logging
from urllib.parse import urlparse, urljoin, urlunparse, parse_qs, urlencode, urlsplit, urlunsplit
import requests
from concurrent.futures import ThreadPoolExecutor
from langchain_community.document_loaders import RecursiveUrlLoader
from typing import Optional, Dict, List, Set

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
    Парсер для gidroizol.ru, использующий RecursiveUrlLoader и логику из исходного скрипта пользователя.
    """
    BASE_URL = "https://gidroizol.ru"
    CATALOG_URL = "https://gidroizol.ru/9"
    
    def __init__(self):
        self.visited_urls = set()
        self.categories_cache = {}

    def _clean_price(self, price_text: str) -> float:
        """Очищает цену и возвращает float"""
        if not price_text: return 0.0
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

    def is_product_page(self, soup: BeautifulSoup, url: str) -> bool:
        """
        Проверка валидности страницы товара.
        """
        # 1. По кнопке "Заказать" или "В корзину"
        buy_buttons = soup.select('a.order__item, button.to-cart, a.btn-buy, div.buy-block')
        if buy_buttons:
            for btn in buy_buttons:
                txt = btn.get_text(strip=True).lower()
                if 'заказать' in txt or 'корзин' in txt or 'купить' in txt:
                    return True
        
        # 2. По наличию цены и заголовка товара
        price = soup.select_one('.price_val, .price-block, .detail-price, div.price')
        h1 = soup.select_one('h1')
        if price and h1:
            return True

        return False

    def _parse_prices_sync(self, html: str) -> float:
        """Синхронный парсинг цены"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            price = 0.0
            
            # Попытка 1: Блоки с ценами (розница/опт)
            price_blocks = soup.select('div.price-block-wrap .price-block')
            for block in price_blocks:
                txt = block.get_text(strip=True)
                val = block.select_one('span')
                val_text = val.get_text(strip=True) if val else ""
                if 'РОЗН' in txt:
                    p = self._clean_price(val_text)
                    if p > 0: return p
            
            # Попытка 2: Мета-теги
            meta_price = soup.select_one('meta[itemprop="price"]')
            if meta_price:
                return self._clean_price(meta_price.get('content', ''))

            # Попытка 3: Общие селекторы
            fallback_sels = ['div.dop-price strong', 'span.price', 'div.price-block', '.product-price']
            for sel in fallback_sels:
                el = soup.select_one(sel)
                if el:
                    p = self._clean_price(el.get_text(strip=True))
                    if p > 0: return p
            
            return 0.0
        except Exception:
            return 0.0

    def parse_product_page(self, url: str, html: str) -> Optional[Dict]:
        """Парсинг данных товара"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            if not self.is_product_page(soup, url):
                return None

            # 1. Заголовок
            title = "Не указано"
            h1 = soup.select_one('h1, div.product-title')
            if h1:
                title = h1.get_text(strip=True)

            # 2. Категория
            breadcrumbs = [b.get_text(strip=True) for b in soup.select('div.breadcrumbs a, ul.breadcrumb li')]
            category = breadcrumbs[-2] if len(breadcrumbs) >= 2 else "Гидроизоляция"

            # 3. Характеристики
            specs = {}
            rows = soup.select('div.table_dop-info .table-row, table.specs tr, .product-features li')
            for row in rows:
                cells = row.select('div.table-cell, td, span')
                if len(cells) >= 2:
                    k = cells[0].get_text(strip=True).replace(':', '')
                    v = cells[1].get_text(strip=True)
                    
                    k_lower = k.lower()
                    if 'толщина' in k_lower: specs['thickness_mm'] = self._clean_price(v)
                    elif 'вес' in k_lower and 'м2' in k_lower: specs['weight_kg_m2'] = self._clean_price(v)
                    elif 'гибкость' in k_lower: specs['flexibility_temp_c'] = v
                    elif 'разрывная' in k_lower: specs['tensile_strength_n'] = self._clean_price(v)
                    else:
                        specs[k] = v

            # 4. Цена
            price = self._parse_prices_sync(html)

            # 5. Описание
            desc_el = soup.select_one('div.product-description, div.desc')
            description = desc_el.get_text(strip=True)[:1000] if desc_el else ""

            return {
                "title": title,
                "url": url,
                "price": price,
                "category": category,
                "material_type": "Рулонный" if "рулон" in category.lower() else "Гидроизоляция",
                "specs": specs,
                "description": description
            }

        except Exception as e:
            logger.error(f"Error parsing HTML {url}: {e}")
            return None

    def _extractor_raw(self, html: str) -> str:
        return html

    async def crawl(self, db: Session):
        """Основной цикл обхода, использующий RecursiveUrlLoader"""
        logger.info("Starting Crawl using RecursiveUrlLoader logic...")
        
        # HEADERS are crucial for scraping to look like a browser
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }

        # Используем BASE_URL, чтобы не ограничиваться префиксом /9
        # prevent_outside=True оставим True, но для BASE_URL, чтобы не уходить с домена
        loader = RecursiveUrlLoader(
            url=self.BASE_URL,
            max_depth=3, # Увеличиваем глубину
            extractor=self._extractor_raw,
            prevent_outside=True, # Не уходим с gidroizol.ru
            timeout=20,
            use_async=False, 
            exclude_dirs=["contacts", "about", "news", "blog", "articles", "login", "filter", "basket", "search", "auth"],
            headers=headers
        )

        logger.info("Invoking loader.load() in thread pool...")
        
        try:
            # Запускаем блокирующий load() в отдельном потоке
            documents = await asyncio.to_thread(loader.load)
            logger.info(f"RecursiveUrlLoader finished. Loaded {len(documents)} documents.")
        except Exception as e:
            logger.error(f"RecursiveUrlLoader failed: {e}")
            return []

        saved_count = 0
        
        with ThreadPoolExecutor(max_workers=8) as executor:
            loop = asyncio.get_event_loop()
            tasks = []
            
            for doc in documents:
                url = doc.metadata.get('source', '')
                if not url: continue
                
                # Фильтрация мусора
                if any(x in url for x in ['login', 'filter', 'sort', 'view', 'print']): continue
                
                tasks.append(
                    loop.run_in_executor(executor, self.parse_product_page, url, doc.page_content)
                )
            
            if not tasks:
                logger.warning("No tasks created for parsing.")
                return []

            results = await asyncio.gather(*tasks)

            for data in results:
                if not data: continue
                
                # Валидация: должна быть цена или хотя бы название
                if data['price'] == 0 and len(data['title']) < 3: continue

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
