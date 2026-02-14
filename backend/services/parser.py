import asyncio
import aiohttp
from bs4 import BeautifulSoup
from sqlalchemy.orm import Session
from ..models import ProductModel
import re
import logging
from urllib.parse import urljoin, urlparse, parse_qs, urlunparse
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
    Адаптирован для работы внутри FastAPI сервиса.
    """
    BASE_URL = "https://gidroizol.ru"
    ASYNC_CONCURRENCY = 5  # Ограничение кол-ва одновременных запросов
    ASYNC_TIMEOUT = 15

    def __init__(self):
        self.visited_urls = set()
        self.product_urls = set()

    def _clean_price(self, price_text: str) -> float:
        """Очищает цену и возвращает float"""
        if not price_text: return 0.0
        clean = re.sub(r'[^\d.]', '', price_text.replace(',', '.').replace(' ', ''))
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
        """Асинхронная загрузка страницы с повторными попытками"""
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
        try:
            async with session.get(url, headers=headers, timeout=self.ASYNC_TIMEOUT) as response:
                if response.status == 200:
                    return await response.text()
                else:
                    logger.warning(f"Status {response.status} for {url}")
                    return None
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            return None

    def is_product_page(self, soup: BeautifulSoup, url: str) -> bool:
        """Определяет, является ли страница карточкой товара"""
        # Проверка по наличию кнопки "Заказать" или специфичных блоков цены
        order_btn = soup.select_one('a[href="#order__item"], .price-block')
        if order_btn:
            return True
        return False

    def parse_product_page(self, url: str, html: str) -> Optional[Dict]:
        """
        Логика извлечения данных со страницы товара (из рабочего скрипта)
        """
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            if not self.is_product_page(soup, url):
                return None

            # 1. Заголовок
            title_selectors = ['h1', 'div.product-title', 'section#tovar h1']
            title = "Не указано"
            for s in title_selectors:
                el = soup.select_one(s)
                if el:
                    title = el.get_text(strip=True)
                    break
            
            # 2. Цена (Приоритет: Розничная -> Обычная -> Мета)
            price = 0.0
            price_blocks = soup.select('div.price-block')
            for block in price_blocks:
                txt = block.get_text(strip=True)
                if 'РОЗН' in txt or not price:
                    val_el = block.select_one('span')
                    if val_el:
                        price = self._clean_price(val_el.get_text(strip=True))
            
            if price == 0:
                meta_price = soup.select_one('meta[itemprop="price"]')
                if meta_price:
                    price = self._clean_price(meta_price.get('content'))

            # 3. Характеристики (Specs)
            specs = {}
            char_rows = soup.select('div.table_dop-info .table-row, table.product-table tr')
            for row in char_rows:
                cells = row.select('div.table-cell, td')
                if len(cells) >= 2:
                    k = cells[0].get_text(strip=True).replace(':', '')
                    v = cells[1].get_text(strip=True)
                    
                    # Маппинг ключей для БД
                    if 'толщина' in k.lower(): specs['thickness_mm'] = self._clean_price(v)
                    elif 'вес' in k.lower(): specs['weight_kg_m2'] = self._clean_price(v)
                    elif 'гибкость' in k.lower(): specs['flexibility_temp_c'] = v
                    elif 'разрывная' in k.lower(): specs['tensile_strength_n'] = self._clean_price(v)
                    else:
                        specs[k] = v

            # 4. Категория (Хлебные крошки)
            breadcrumbs = [b.get_text(strip=True) for b in soup.select('.breadcrumbs a, .breadcrumb a')]
            category = breadcrumbs[-2] if len(breadcrumbs) > 1 else "Гидроизоляция"

            # 5. Описание
            desc_el = soup.select_one('.product-description, .description')
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

    async def crawl(self, db: Session):
        """
        Основной метод обхода:
        1. Загружает главную страницу и основные разделы.
        2. Ищет ссылки на товары.
        3. Загружает товары параллельно.
        4. Сохраняет в БД.
        """
        logger.info("Starting Async Crawl...")
        
        async with aiohttp.ClientSession() as session:
            # 1. Получаем ссылки с главной и основных разделов каталога
            start_urls = [
                self.BASE_URL,
                f"{self.BASE_URL}/catalog/gidroizolyaciya/",
                f"{self.BASE_URL}/9" # Часто используемый ID раздела
            ]
            
            found_links = set()
            
            for start_url in start_urls:
                html = await self._fetch(session, start_url)
                if not html: continue
                
                soup = BeautifulSoup(html, 'html.parser')
                # Ищем ссылки на товары (эвристика по URL или классам)
                links = soup.select('a[href*="/product/"], a[href*="/item/"], a.product-link, div.catalog-item a')
                
                # Если специфичных нет, берем все внутренние и фильтруем
                if not links:
                    links = soup.find_all('a', href=True)

                for link in links:
                    href = link['href']
                    full_url = urljoin(self.BASE_URL, href)
                    
                    # Фильтры
                    if self.BASE_URL not in full_url: continue
                    if 'filter' in full_url or 'sort' in full_url: continue
                    if full_url in self.visited_urls: continue
                    
                    # Простая эвристика товарной ссылки (обычно глубокая вложенность или ключевые слова)
                    # Но лучше мы просто попробуем спарсить топ-50 ссылок, похожих на товары
                    if len(full_url.split('/')) > 4: 
                        found_links.add(full_url)

            logger.info(f"Found {len(found_links)} potential product links. Processing limit: 50.")
            
            # Ограничиваем кол-во для демо/скорости
            links_to_process = list(found_links)[:50]
            
            # 2. Параллельная загрузка товаров
            tasks = []
            sem = asyncio.Semaphore(self.ASYNC_CONCURRENCY)

            async def process_url(url):
                async with sem:
                    html = await self._fetch(session, url)
                    if html:
                        return self.parse_product_page(url, html)
                    return None

            for url in links_to_process:
                tasks.append(process_url(url))
            
            results = await asyncio.gather(*tasks)
            
            # 3. Сохранение в БД
            saved_count = 0
            for data in results:
                if not data: continue
                
                # Upsert logic
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
                    # Update price
                    existing.price = data['price']
                    existing.specs = data['specs']
            
            db.commit()
            logger.info(f"Crawl finished. Saved/Updated {saved_count} products.")
            return db.query(ProductModel).all()

    async def parse_and_save(self, db: Session):
        """Wrapper to be called from Main"""
        return await self.crawl(db)
