import os
import re
import csv
import sqlite3
import time
import random
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional, Set
from urllib.parse import urlencode, urljoin, urlparse, parse_qs, unquote
import logging

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PwTimeoutError

logger = logging.getLogger("EIS_Service")

# =========================
# РЕГЕКСЫ И КОНСТАНТЫ
# =========================
BASE = "https://zakupki.gov.ru"
SEARCH_URL = f"{BASE}/epz/order/extendedsearch/results.html"

NOTICE_HREF_RE = re.compile(r"/epz/order/notice/([^/]+)/view/[^?]+\.html\?[^#]*regNumber=(\d+)")
NOTICE_LINK_SELECTOR = "a[href*='/epz/order/notice/']"

RUB_PRICE_RE = re.compile(
    r"(\d[\d\s\xa0]*,\d{2}\s*(?:₽|руб\.?|рублей))",
    flags=re.IGNORECASE,
)

DEADLINE_RE = re.compile(
    r"\b\d{2}\.\d{2}\.\d{4}(?:\s+\d{2}:\d{2})?\b"
)

NO_RESULTS_PATTERNS = [
    "по вашему запросу ничего не найдено",
    "ничего не найдено",
    "результаты не найдены",
    "не найдено",
]

@dataclass
class Notice:
    reg: str
    ntype: str
    keyword: str
    search_url: str
    title: str = ""
    href: str = ""
    object_info: str = ""
    initial_price: str = ""
    application_deadline: str = ""
    seen: bool = False

class EisService:
    def __init__(self):
        self.RECORDS_PER_PAGE = 50
        self.MAX_PAGES = 5
        self.OKPD2_IDS_WITH_NESTED = True
        self.OKPD2_IDS = "8873861,8873862,8873863"
        self.OKPD2_IDS_CODES = "A,B,C"
        self.HEADLESS = True
        self.REQ_HEADERS = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36",
            "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
        }

    def _publish_date_from_str(self, days_back: int) -> str:
        dt = datetime.now() - timedelta(days=days_back)
        return dt.strftime("%d.%m.%Y")

    def build_search_url(self, keyword: str, page_number: int, fz44: bool, fz223: bool, only_application_stage: bool, publish_days_back: int) -> str:
        params = {
            "searchString": keyword,
            "morphology": "on",
            "search-filter": "Дате размещения",
            "pageNumber": str(page_number),
            "sortDirection": "false",
            "recordsPerPage": f"_{self.RECORDS_PER_PAGE}",
            "showLotsInfoHidden": "false",
            "sortBy": "PUBLISH_DATE",
            "publishDateFrom": self._publish_date_from_str(publish_days_back),
            "currencyIdGeneral": "-1",
        }

        if only_application_stage:
            params["af"] = "on"
        if fz44:
            params["fz44"] = "on"
        if fz223:
            params["fz223"] = "on"

        if self.OKPD2_IDS_WITH_NESTED:
            params["okpd2IdsWithNested"] = "on"
        if self.OKPD2_IDS:
            params["okpd2Ids"] = self.OKPD2_IDS
        if self.OKPD2_IDS_CODES:
            params["okpd2IdsCodes"] = self.OKPD2_IDS_CODES

        return SEARCH_URL + "?" + urlencode(params)

    def _normalize_text(self, text: str) -> str:
        return re.sub(r"\s+", " ", (text or "").strip())

    def _extract_field_by_label(self, block, labels: List[str]) -> str:
        if block is None:
            return ""

        text = block.get_text("\n", strip=True)
        text = re.sub(r"\n+", "\n", text)

        stop_labels = [
            "Объект закупки", "Начальная цена", "Начальная (максимальная) цена контракта",
            "Начальная (максимальная) цена договора", "Начальная сумма цен единиц товара, работы, услуги",
            "Окончание подачи заявок", "Дата окончания срока подачи заявок", "Цена", "Заказчик",
            "Организация, осуществляющая размещение", "Дата размещения", "Размещено", "Обновлено",
            "Способ определения поставщика", "Регион", "Валюта", "Преимущества, требования к участникам",
            "Информация о лоте", "Этап закупки",
        ]

        for label in labels:
            other_labels = [x for x in stop_labels if x != label]
            stop_pattern = "|".join(re.escape(x) for x in other_labels)
            pattern = rf"{re.escape(label)}\s*(.*?)(?=\n(?:{stop_pattern})\b|$)"
            m = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
            if m:
                value = self._normalize_text(m.group(1))
                if value:
                    return value
        return ""

    def _extract_initial_price(self, block) -> str:
        if block is None:
            return ""

        value = self._extract_field_by_label(
            block,
            [
                "Начальная цена", "Начальная (максимальная) цена контракта",
                "Начальная (максимальная) цена договора", "Начальная сумма цен единиц товара, работы, услуги",
            ],
        )
        if value:
            m = RUB_PRICE_RE.search(value)
            if m:
                return self._normalize_text(m.group(1))
            return value

        text = block.get_text(" ", strip=True)
        text = self._normalize_text(text)
        m = RUB_PRICE_RE.search(text)
        if m:
            return self._normalize_text(m.group(1))
        return ""

    def _extract_application_deadline(self, block) -> str:
        if block is None:
            return ""

        value = self._extract_field_by_label(
            block,
            ["Окончание подачи заявок", "Дата окончания срока подачи заявок"],
        )
        if value:
            m = DEADLINE_RE.search(value)
            if m:
                return self._normalize_text(m.group(0))
            return value

        text = block.get_text(" ", strip=True)
        text = self._normalize_text(text)
        m = DEADLINE_RE.search(text)
        if m:
            return self._normalize_text(m.group(0))
        return ""

    def _extract_notices_from_results(self, html: str, keyword: str, search_url: str) -> List[Notice]:
        soup = BeautifulSoup(html, "html.parser")
        found_by_reg: dict[str, Notice] = {}

        for a in soup.select(NOTICE_LINK_SELECTOR):
            href = (a.get("href") or "").strip()
            match = NOTICE_HREF_RE.search(href)
            if not match:
                continue

            ntype = match.group(1)
            reg = match.group(2)
            full_href = urljoin(BASE, href)

            title = self._normalize_text(a.get_text(" ", strip=True))
            if not title:
                title = self._normalize_text(a.get("title") or "")

            card = None
            for parent in a.parents:
                try:
                    parent_text = parent.get_text(" ", strip=True)
                except Exception:
                    continue
                if (
                    "Объект закупки" in parent_text
                    or "Начальная цена" in parent_text
                    or "Начальная (максимальная) цена" in parent_text
                    or "Окончание подачи заявок" in parent_text
                ):
                    card = parent
                    break

            object_info = self._extract_field_by_label(card, ["Объект закупки"]) if card else ""
            initial_price = self._extract_initial_price(card) if card else ""
            application_deadline = self._extract_application_deadline(card) if card else ""

            if reg not in found_by_reg:
                found_by_reg[reg] = Notice(
                    reg=reg,
                    ntype=ntype,
                    keyword=keyword,
                    search_url=search_url,
                    title=title,
                    href=full_href,
                    object_info=object_info,
                    initial_price=initial_price,
                    application_deadline=application_deadline,
                )
            else:
                current = found_by_reg[reg]
                if len(title) > len(current.title):
                    current.title = title
                if object_info and len(object_info) > len(current.object_info):
                    current.object_info = object_info
                if initial_price and len(initial_price) > len(current.initial_price):
                    current.initial_price = initial_price
                if application_deadline and len(application_deadline) > len(current.application_deadline):
                    current.application_deadline = application_deadline
                if not current.href:
                    current.href = full_href

        return list(found_by_reg.values())

    def _has_notice_results(self, page) -> bool:
        try:
            return page.locator(NOTICE_LINK_SELECTOR).count() > 0
        except Exception:
            return False

    def _has_no_results_banner(self, page) -> bool:
        try:
            html = page.content().lower()
        except Exception:
            return False
        return any(p in html for p in NO_RESULTS_PATTERNS)

    def _wait_results_or_empty(self, page, timeout_ms: int = 15000) -> bool:
        deadline = time.time() + timeout_ms / 1000.0
        while time.time() < deadline:
            if self._has_notice_results(page):
                return True
            if self._has_no_results_banner(page):
                return False
            page.wait_for_timeout(400)
        return False

    def _get_first_notice_href(self, page) -> str:
        try:
            return page.eval_on_selector(NOTICE_LINK_SELECTOR, "el => el.getAttribute('href') || ''") or ""
        except Exception:
            return ""

    def _ensure_fresh_search_results(self, page) -> bool:
        initial_has_results = self._wait_results_or_empty(page, timeout_ms=15000)
        if not initial_has_results:
            logger.info("На странице результатов карточек нет")
            return False

        page.wait_for_timeout(300)
        before_href = self._get_first_notice_href(page)

        try:
            btn = page.get_by_role("button", name=re.compile(r"применить", re.I))
            if btn.count() > 0:
                btn.first.click()
                logger.info("Нажата кнопка 'Применить' через get_by_role")
            else:
                btn2 = page.locator("input[type='submit'][value*='Применить'], button:has-text('Применить')")
                if btn2.count() > 0:
                    btn2.first.click()
                    logger.info("Нажата кнопка 'Применить' через locator")
                else:
                    logger.info("Кнопка 'Применить' не найдена, используем текущую выдачу")
                    return initial_has_results
        except Exception as e:
            logger.info(f"Не удалось нажать 'Применить': {e}")
            return initial_has_results

        page.wait_for_timeout(500)

        try:
            page.wait_for_function(
                """(sel, before) => {
                    const a = document.querySelector(sel);
                    if (!a) return false;
                    const now = a.getAttribute('href') || '';
                    return now && now !== before;
                }""",
                arg=(NOTICE_LINK_SELECTOR, before_href),
                timeout=8000,
            )
        except Exception:
            pass

        page.wait_for_timeout(700)
        has_results = self._wait_results_or_empty(page, timeout_ms=12000)
        logger.info(f"После 'Применить': has_results={has_results}")
        return has_results

    def _clean_price_for_db(self, price_str):
        if not price_str: return 0.0
        clean = re.sub(r'[^\d,.]', '', price_str).replace(',', '.')
        try:
            return float(clean)
        except ValueError:
            return 0.0

    def search_tenders(self, query: str, fz44: bool = True, fz223: bool = True, only_application_stage: bool = True, publish_days_back: int = 30):
        """
        Запускает реальный браузер, выполняет поиск и парсит результаты.
        """
        logger.info(f"Searching EIS via Playwright for: {query}")
        results = []
        collected: List[Notice] = []

        try:
            with sync_playwright() as p:
                try:
                    logger.info("Launching Chromium...")
                    browser = p.chromium.launch(headless=self.HEADLESS)
                except Exception as browser_err:
                    logger.critical(f"Failed to launch browser. Error: {browser_err}")
                    return []
                
                try:
                    context = browser.new_context(
                        locale="ru-RU",
                        user_agent=self.REQ_HEADERS["User-Agent"],
                        viewport={"width": 1920, "height": 1080}
                    )
                    page = context.new_page()

                    # Разделяем запрос на ключевые слова, если их несколько (через запятую)
                    keywords = [k.strip() for k in query.split(',')] if ',' in query else [query]

                    for kw in keywords:
                        for pn in range(1, self.MAX_PAGES + 1):
                            url = self.build_search_url(kw, pn, fz44, fz223, only_application_stage, publish_days_back)
                            logger.info(f"[SEARCH] kw='{kw}' page={pn}")
                            logger.info(f"[SEARCH] url: {url}")

                            try:
                                time.sleep(random.uniform(1.2, 3.2))
                                page.goto(url, wait_until="domcontentloaded", timeout=30000)
                                time.sleep(random.uniform(0.8, 2.2))

                                has_results = self._ensure_fresh_search_results(page)
                                if not has_results:
                                    logger.info(f"[SEARCH] no results on page {pn} for kw='{kw}' -> stop pages for this keyword")
                                    break

                            except PwTimeoutError as e:
                                logger.error(f"[SEARCH] timeout kw='{kw}' page={pn}: {e}")
                                break
                            except Exception as e:
                                logger.error(f"[SEARCH] error kw='{kw}' page={pn}: {e}")
                                break

                            items = self._extract_notices_from_results(page.content(), kw, url)
                            logger.info(f"[SEARCH] found notices: {len(items)}")
                            
                            if not items:
                                break

                            collected.extend(items)

                except Exception as nav_err:
                    logger.error(f"Navigation/Page Error: {nav_err}")
                finally:
                    browser.close()
                    logger.info("Browser closed.")

                # Преобразуем собранные Notice в формат для фронтенда
                merged: dict[str, Notice] = {}
                for n in collected:
                    if n.reg not in merged:
                        merged[n.reg] = n
                    else:
                        current = merged[n.reg]
                        if len(n.title) > len(current.title):
                            current.title = n.title
                        if n.object_info and len(n.object_info) > len(current.object_info):
                            current.object_info = n.object_info
                        if n.initial_price and len(n.initial_price) > len(current.initial_price):
                            current.initial_price = n.initial_price
                        if n.application_deadline and len(n.application_deadline) > len(current.application_deadline):
                            current.application_deadline = n.application_deadline
                        if not current.href:
                            current.href = n.href

                for reg, n in merged.items():
                    law_type = "223-ФЗ" if "223" in n.ntype else "44-ФЗ"
                    price = self._clean_price_for_db(n.initial_price)
                    
                    results.append({
                        "id": n.reg,
                        "eis_number": n.reg,
                        "title": n.title or n.object_info or "Без названия",
                        "description": n.object_info,
                        "initial_price": price,
                        "deadline": n.application_deadline or "См. ЕИС", 
                        "status": "Found",
                        "risk_level": "Low",
                        "region": "РФ",
                        "law_type": law_type,
                        "url": n.href
                    })

        except Exception as e:
            logger.error(f"Playwright Global Error: {e}", exc_info=True)
            return []

        logger.info(f"Returning {len(results)} tenders.")
        return results
