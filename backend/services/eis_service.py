from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import re
import logging

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("eis_service_log.txt", encoding='utf-8', mode='w'), # mode='w' перезаписывает файл
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("EIS_Service")

class EisService:
    """
    Сервис для поиска по Единой Информационной Системе (zakupki.gov.ru).
    Использует Playwright (Headless Browser) для обхода защиты от ботов.
    """
    SEARCH_URL = "https://zakupki.gov.ru/epz/order/extendedsearch/results.html"

    def _clean_price(self, price_str):
        if not price_str: return 0.0
        clean = re.sub(r'[^\d,.]', '', price_str).replace(',', '.')
        try:
            return float(clean)
        except ValueError:
            return 0.0

    def search_tenders(self, query: str):
        """
        Запускает реальный браузер, выполняет поиск и парсит результаты.
        """
        logger.info(f"Searching EIS via Playwright for: {query}")
        results = []

        try:
            with sync_playwright() as p:
                try:
                    # Запускаем браузер
                    logger.info("Launching Chromium...")
                    browser = p.chromium.launch(headless=True)
                except Exception as browser_err:
                    logger.critical(f"Failed to launch browser. Error: {browser_err}")
                    print("HINT: Run 'playwright install' or 'python -m playwright install' in terminal.")
                    return []
                
                try:
                    context = browser.new_context(
                        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                        viewport={"width": 1920, "height": 1080}
                    )
                    page = context.new_page()

                    # Формируем URL
                    params = [
                        f"searchString={query}",
                        "morphology=on",
                        "pageNumber=1",
                        "sortDirection=false",
                        "recordsPerPage=_10",
                        "sortBy=UPDATE_DATE",
                        "fz44=on",
                        "fz223=on"
                    ]
                    full_url = f"{self.SEARCH_URL}?{'&'.join(params)}"
                    
                    logger.info(f"Navigating to: {full_url}")
                    # Переход на страницу с увеличенным таймаутом
                    page.goto(full_url, timeout=45000, wait_until="domcontentloaded")
                    
                    # Ждем селектор
                    try:
                        page.wait_for_selector("div.search-registry-entry-block", timeout=10000)
                    except:
                        logger.warning("No results found selector appeared or timeout.")

                    html_content = page.content()
                except Exception as nav_err:
                    logger.error(f"Navigation/Page Error: {nav_err}")
                    return []
                finally:
                    browser.close()
                    logger.info("Browser closed.")

                # Парсинг
                soup = BeautifulSoup(html_content, 'html.parser')
                items = soup.find_all('div', class_='search-registry-entry-block')
                logger.info(f"Found {len(items)} items on page.")

                for item in items:
                    try:
                        number_div = item.find('div', class_='registry-entry__header-mid__number')
                        link_el = number_div.find('a') if number_div else None
                        eis_number = link_el.get_text(strip=True).replace('№', '').strip() if link_el else "Unknown"
                        url = f"https://zakupki.gov.ru{link_el['href']}" if link_el else ""

                        price_div = item.find('div', class_='price-block__value')
                        price_text = price_div.get_text(strip=True) if price_div else "0"
                        price = self._clean_price(price_text)

                        title_div = item.find('div', class_='registry-entry__body-value')
                        title = title_div.get_text(strip=True) if title_div else "Без описания"

                        org_div = item.find('div', class_='registry-entry__body-href')
                        org_name = org_div.get_text(strip=True) if org_div else "Заказчик скрыт"

                        law_div = item.find('div', class_='registry-entry__header-top__title')
                        law_text = law_div.get_text(strip=True) if law_div else ""
                        law_type = "223-ФЗ" if "223" in law_text else "44-ФЗ"

                        results.append({
                            "id": eis_number,
                            "eis_number": eis_number,
                            "title": title,
                            "description": f"{title}. Заказчик: {org_name}",
                            "initial_price": price,
                            "deadline": "См. ЕИС", 
                            "status": "Found",
                            "risk_level": "Low",
                            "region": "РФ",
                            "law_type": law_type,
                            "url": url
                        })
                    except Exception as parse_err:
                        logger.error(f"Error parsing item: {parse_err}")
                        continue

        except Exception as e:
            logger.error(f"Playwright Global Error: {e}", exc_info=True)
            return []

        logger.info(f"Returning {len(results)} tenders.")
        return results
