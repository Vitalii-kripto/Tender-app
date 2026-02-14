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
    Реализует динамический поиск категорий для предотвращения ошибок 404 при смене структуры сайта.
    """
    BASE_URL = "https://gidroizol.ru"
    ASYNC_CONCURRENCY = 10  # Увеличим для скорости, т.к. многие ссылки могут быть не товарами
    ASYNC_TIMEOUT = 20

    def __init__(self):
        self.visited_urls = set()

    def _clean_price(self, price_text: str) -> float:
        """Очищает цену и возвращает float"""
        if not price_text: return 0.0
        # Удаляем &nbsp; и пробелы, заменяем запятую на точку
        clean = re.sub(r'[^\d.,]', '', price_text.replace('&nbsp;', '').replace(' ', ''))
        clean = clean.replace(',', '.')
        try:
            # Если есть несколько точек/запятых, берем первую валидную часть
            # Например 1.200.00 -> 1200.00
            return float(clean)
        except ValueError:
            return 0.0

    async def _fetch(self, session, url):
        """Асинхронная загрузка страницы с имитацией браузера"""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7'
        }
        try:
            async with session.get(url, headers=headers, timeout=self.ASYNC_TIMEOUT, ssl=False) as response:
                if response.status == 200:
                    return await response.text()
                elif response.status == 404:
                    logger.warning(f"Страница не найдена (404): {url}")
                    return None
                else:
                    logger.warning(f"Status {response.status} for {url}")
                    return None
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            return None

    def is_product_page(self, soup: BeautifulSoup) -> bool:
        """
        Проверяет, является ли страница карточкой товара.
        Ищет признаки: цена, кнопка купить, наличие характеристик.
        """
        # 1. Наличие цены
        has_price = bool(soup.find(itemprop="price") or soup.select_one('.price, .product-price, .detail-price'))
        # 2. Наличие кнопки "В корзину" или "Заказать"
        has_buy_btn = bool(soup.select_one('.btn-buy, .add-to-cart, button[name="add"], a[href*="add2basket"]'))
        
        return has_price or has_buy_btn

    def parse_product_page(self, url: str, html: str) -> Optional[Dict]:
        """Парсинг карточки товара"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            if not self.is_product_page(soup):
                return None

            # 1. Заголовок (H1)
            title_el = soup.find('h1')
            title = title_el.get_text(strip=True) if title_el else "Без названия"
            
            # 2. Цена
            price = 0.0
            # Пробуем мета-тег (самый надежный способ для Schema.org)
            meta_price = soup.find(itemprop="price")
            if meta_price:
                content = meta_price.get('content') or meta_price.get_text(strip=True)
                price = self._clean_price(content)
            
            # Если нет мета-тега, ищем визуальную цену
            if price == 0:
                price_selectors = ['.price_val', '.price', '.catalog-element-price', '.detail_price']
                for sel in price_selectors:
                    el = soup.select_one(sel)
                    if el:
                        price = self._clean_price(el.get_text(strip=True))
                        if price > 0: break

            # 3. Характеристики
            specs = {}
            # Ищем таблицы характеристик (обычно table или список div)
            rows = soup.select('table tr, .props_group .prop_line')
            for row in rows:
                cols = row.select('td, .prop_name, .prop_value')
                if len(cols) >= 2:
                    key = cols[0].get_text(strip=True).replace(':', '')
                    val = cols[1].get_text(strip=True)
                    
                    k_lower = key.lower()
                    if 'толщина' in k_lower: specs['thickness_mm'] = self._clean_price(val)
                    elif 'вес' in k_lower and 'м2' in k_lower: specs['weight_kg_m2'] = self._clean_price(val)
                    elif 'гибкость' in k_lower: specs['flexibility_temp_c'] = val
                    elif 'разрывная' in k_lower: specs['tensile_strength_n'] = self._clean_price(val)
                    else:
                        specs[key] = val

            # 4. Категория (Breadcrumbs)
            breadcrumbs = soup.select('.breadcrumbs a, .breadcrumb-item a')
            category = "Гидроизоляция"
            if len(breadcrumbs) >= 3:
                # Обычно 0=Главная, 1=Каталог, 2=Категория
                category = breadcrumbs[-2].get_text(strip=True)

            # 5. Описание
            desc_el = soup.find(itemprop="description") or soup.select_one('.detail_text, .product-desc')
            description = desc_el.get_text(strip=True)[:2000] if desc_el else ""

            return {
                "title": title,
                "url": url,
                "price": price,
                "category": category,
                "material_type": "Рулонный" if "рулон" in title.lower() else "Обмазочный",
                "specs": specs,
                "description": description
            }

        except Exception as e:
            logger.error(f"Error parsing HTML {url}: {e}")
            return None

    async def crawl(self, db: Session):
        """
        Умный обход:
        1. Сначала сканируем главную страницу, чтобы найти актуальные ссылки на категории (/catalog/...).
        2. Затем заходим в категории и собираем ссылки на товары.
        3. Парсим товары.
        """
        logger.info("Starting Intelligent Crawl...")
        
        async with aiohttp.ClientSession() as session:
            # --- ЭТАП 1: Обнаружение категорий ---
            category_links = set()
            
            # Пробуем несколько точек входа
            entry_points = [
                self.BASE_URL,
                urljoin(self.BASE_URL, '/catalog/'),
                urljoin(self.BASE_URL, '/katalog/')
            ]
            
            logger.info("Discovering categories...")
            for ep in entry_points:
                html = await self._fetch(session, ep)
                if not html: continue
                
                soup = BeautifulSoup(html, 'html.parser')
                for a in soup.find_all('a', href=True):
                    href = a['href']
                    # Фильтруем ссылки, похожие на категории
                    if '/catalog/' in href or '/katalog/' in href:
                        full_url = urljoin(self.BASE_URL, href)
                        # Исключаем ссылки на фильтры, сортировки и т.д.
                        if '?' not in full_url and full_url not in entry_points:
                            category_links.add(full_url)
            
            logger.info(f"Found {len(category_links)} category pages.")
            if not category_links:
                logger.warning("No categories found via crawling. Adding fallback.")
                category_links.add(urljoin(self.BASE_URL, '/catalog/'))

            # --- ЭТАП 2: Сбор ссылок на товары ---
            product_links = set()
            
            # Ограничиваем кол-во категорий для сканирования (чтобы не ждать вечно)
            # Берем первые 10 категорий
            target_categories = list(category_links)[:10]
            
            logger.info(f"Scanning products in {len(target_categories)} categories...")
            
            for cat_url in target_categories:
                html = await self._fetch(session, cat_url)
                if not html: continue
                
                soup = BeautifulSoup(html, 'html.parser')
                # Ищем ссылки на товары. Обычно они вложены глубже чем категории
                links = soup.find_all('a', href=True)
                for link in links:
                    href = link['href']
                    full_url = urljoin(self.BASE_URL, href)
                    
                    # Эвристика: ссылка на товар обычно содержит .html или глубже 3 уровней
                    # И она должна быть внутри домена
                    if self.BASE_URL in full_url:
                        # Проверка на мусор
                        if any(x in full_url for x in ['login', 'logout', 'register', 'basket', 'compare', 'filter']):
                            continue
                        
                        # Если ссылка длинная и отличается от категории, считаем кандидатом
                        if len(full_url) > len(cat_url) + 5:
                            product_links.add(full_url)

            logger.info(f"Found {len(product_links)} potential product links. Selecting 50 to parse.")
            
            # --- ЭТАП 3: Парсинг товаров ---
            links_to_process = list(product_links)[:50]
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
            
            # --- ЭТАП 4: Сохранение ---
            saved_count = 0
            for data in results:
                if not data: continue
                if not data.get('title') or data['title'] == "Без названия": continue
                
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
                    existing.price = data['price']
                    existing.specs = data['specs']
            
            db.commit()
            logger.info(f"Crawl finished. Saved/Updated {saved_count} products.")
            return db.query(ProductModel).all()

    async def parse_and_save(self, db: Session):
        return await self.crawl(db)
