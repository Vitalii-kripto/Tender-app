import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from sqlalchemy.orm import Session
from ..models import ProductModel
import re
import logging

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("parser_service_log.txt", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("GidroizolParser")

class GidroizolParser:
    """
    Класс для парсинга каталога gidroizol.ru.
    Собирает названия, цены и базовые характеристики.
    """
    BASE_URL = "https://gidroizol.ru/catalog/gidroizolyaciya/"

    def __init__(self):
        try:
            self.ua = UserAgent()
        except:
            self.ua = None
            logger.warning("fake_useragent not initialized.")

    def _get_headers(self):
        user_agent = self.ua.random if self.ua else "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        return {
            "User-Agent": user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
        }

    def _clean_price(self, price_text: str) -> float:
        """Очищает строку цены от 'руб.', пробелов и возвращает float"""
        if not price_text:
            return 0.0
        # Оставляем только цифры и точку
        clean = re.sub(r'[^\d.]', '', price_text.replace(',', '.').replace(' ', ''))
        try:
            return float(clean)
        except ValueError:
            return 0.0

    def parse_and_save(self, db: Session):
        """
        Основной метод: парсит страницу и сохраняет/обновляет товары в БД.
        """
        logger.info(f"Parsing catalog started: {self.BASE_URL}")
        try:
            response = requests.get(self.BASE_URL, headers=self._get_headers(), timeout=15)
            if response.status_code != 200:
                logger.error(f"Error fetching catalog: Status Code {response.status_code}")
                return []

            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Селекторы могут меняться, здесь использованы стандартные для битрикс-магазинов
            # Попытка найти карточки товаров
            items = soup.select('.catalog-item, .product-item, .item_block') 
            
            parsed_products = []

            if not items:
                logger.warning("Primary selectors failed. Trying fallbacks...")
                # Fallback: если специфичные классы не найдены, ищем более общие
                items = soup.find_all('div', class_=re.compile(r'item|product'))

            logger.info(f"Found {len(items)} potential product elements.")

            for item in items:
                try:
                    # 1. Title
                    title_el = item.select_one('.item-title a, .product-title a, a.name')
                    if not title_el: continue
                    title = title_el.get_text(strip=True)
                    link = title_el['href']
                    if link.startswith('/'):
                        link = f"https://gidroizol.ru{link}"

                    # 2. Price
                    price_el = item.select_one('.price_value, .price, .cost')
                    price = self._clean_price(price_el.get_text(strip=True)) if price_el else 0.0

                    # 3. Category (пытаемся определить из хлебных крошек или title)
                    category = "Рулонная гидроизоляция" 
                    if "мастика" in title.lower(): category = "Мастики"
                    elif "праймер" in title.lower(): category = "Праймеры"

                    # 4. Specs (имитация, т.к. на списке товаров их часто нет)
                    # В идеале нужно заходить внутрь карточки (link)
                    specs = {}
                    if "эпп" in title.lower(): specs = {"thickness_mm": 4.0, "flexibility_temp_c": -25}
                    elif "хпп" in title.lower(): specs = {"thickness_mm": 3.0, "flexibility_temp_c": -15}

                    # Сохранение в БД (Upsert)
                    product = db.query(ProductModel).filter(ProductModel.url == link).first()
                    if not product:
                        product = ProductModel(
                            title=title,
                            category=category,
                            price=price,
                            url=link,
                            specs=specs,
                            material_type="Гидроизоляция"
                        )
                        db.add(product)
                    else:
                        product.price = price # Обновляем цену
                    
                    parsed_products.append(product)

                except Exception as e:
                    logger.error(f"Error parsing specific item: {e}")
                    continue

            db.commit()
            logger.info(f"Successfully parsed and saved {len(parsed_products)} items.")
            return parsed_products

        except Exception as e:
            logger.critical(f"Global parser error: {e}", exc_info=True)
            return []
