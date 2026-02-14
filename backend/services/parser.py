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
        Проверка валидности страницы товара (логика из скрипта).
        """
        # 1. По кнопке "Заказать"
        order_btn = soup.select_one('a.order__item.open__popup[href="#order__item"]')
        if order_btn:
            btn_text = order_btn.get_text(strip=True).lower()
            if 'заказать' in btn_text:
                return True
            
        # 2. Альтернативные селекторы кнопки
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
        
        # 3. Дополнительная проверка из скрипта пользователя: поиск текста, но строгий фильтр
        return False

    def _parse_prices_sync(self, html: str) -> float:
        """Синхронный парсинг цены (как в скрипте)"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
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
            return price
        except Exception:
            return 0.0

    def parse_product_page(self, url: str, html: str) -> Optional[Dict]:
        """Парсинг данных товара"""
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

            # 2. Категория
            breadcrumbs = [b.get_text(strip=True) for b in soup.select('div.breadcrumbs a, nav.breadcrumb a, ul.breadcrumb-list a')]
            valid_crumbs = [b for b in breadcrumbs if b.lower() not in ['главная', 'home', 'каталог', '', 'все категории']]
            category = valid_crumbs[-1] if valid_crumbs else "Гидроизоляция"

            # 3. Характеристики
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
                            if 'толщина' in k_lower: specs['thickness_mm'] = self._clean_price(v)
                            elif 'вес' in k_lower and 'м2' in k_lower: specs['weight_kg_m2'] = self._clean_price(v)
                            elif 'гибкость' in k_lower: specs['flexibility_temp_c'] = v
                            elif 'разрывная' in k_lower: specs['tensile_strength_n'] = self._clean_price(v)
                            else:
                                specs[k] = v
                    break

            # 4. Цена (вынесено в метод)
            price = self._parse_prices_sync(html)

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

    def _extractor_raw(self, html: str) -> str:
        """Простой экстрактор для RecursiveUrlLoader, возвращающий сырой HTML"""
        return html

    async def crawl(self, db: Session):
        """Основной цикл обхода, использующий RecursiveUrlLoader"""
        logger.info("Starting Crawl using RecursiveUrlLoader logic...")
        
        # Конфигурация Loader как в скрипте пользователя
        # Ограничиваем глубину 3, чтобы не зависнуть в вебе
        loader = RecursiveUrlLoader(
            url=self.CATALOG_URL,
            max_depth=3, 
            extractor=self._extractor_raw,
            prevent_outside=True,
            timeout=10,
            use_async=False, # Как в скрипте пользователя
            exclude_dirs=["contacts", "about", "news", "blog", "articles", "login", "filter"]
        )

        logger.info("Invoking loader.load() in thread pool...")
        
        # Запускаем блокирующий load() в отдельном потоке
        documents = await asyncio.to_thread(loader.load)
        logger.info(f"RecursiveUrlLoader finished. Loaded {len(documents)} documents.")

        saved_count = 0
        
        # Обработка документов
        # В скрипте пользователя здесь использовался ThreadPoolExecutor для доп. запросов (города),
        # но мы используем его для парсинга, чтобы не блокировать event loop.
        
        with ThreadPoolExecutor(max_workers=8) as executor:
            loop = asyncio.get_event_loop()
            tasks = []
            
            for doc in documents:
                url = doc.metadata.get('source', '')
                if not url: continue
                
                # Фильтрация
                if 'city=' in url: continue
                if any(ext in url for ext in ['.jpg', '.png', '.pdf']): continue

                # Запускаем парсинг в пуле потоков
                tasks.append(
                    loop.run_in_executor(executor, self.parse_product_page, url, doc.page_content)
                )
            
            if not tasks:
                logger.warning("No tasks created for parsing.")
                return []

            results = await asyncio.gather(*tasks)

            for data in results:
                if not data: continue
                
                # Валидация
                if data['price'] == 0 and not data['title']: continue

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
