import uvicorn
import asyncio
import sys
import os
import logging
import tempfile
import openpyxl
import re
import sqlite3
from dotenv import load_dotenv

# Загружаем переменные окружения в самом начале
load_dotenv()
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
from openpyxl.cell.cell import MergedCell
from typing import List, Dict, Any

from fastapi import FastAPI, Depends, HTTPException, UploadFile, File, Body, BackgroundTasks
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

from .database import engine, Base, get_db
from .models import TenderModel, ProductModel
from .services.eis_service import EisService, Notice, mark_seen, csv_append_row, OUT_DIR
from .services.parser import GidroizolParser
from .services.document_service import DocumentService
from .services.ai_service import AiService
from .services.legal_analysis_service import LegalAnalysisService
from .services.batch_analysis import analyze_tenders_batch_job
from .services.job_service import job_service

# --- LOGGING SETUP ---
log_file = "fastapi_app_log.txt"

# Diagnostic logs for encoding
print(f"DEBUG: sys.getdefaultencoding() = {sys.getdefaultencoding()}")
import locale
print(f"DEBUG: locale.getpreferredencoding(False) = {locale.getpreferredencoding(False)}")
print(f"DEBUG: os.environ.get('PYTHONUTF8') = {os.environ.get('PYTHONUTF8')}")

# Test case for Russian text
test_file = "russian_test.txt"
test_text = "Проверка кириллицы: Привет, мир!"
try:
    with open(test_file, "w", encoding="utf-8") as f:
        f.write(test_text)
    with open(test_file, "r", encoding="utf-8") as f:
        read_text = f.read()
    if read_text == test_text:
        print(f"✅ Russian text test passed: {read_text}")
    else:
        print(f"❌ Russian text test failed: expected '{test_text}', got '{read_text}'")
except Exception as e:
    print(f"❌ Russian text test error: {e}")

# Удаляем существующие хендлеры, если они есть, чтобы избежать дублирования при релоаде
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8', mode='w'), # mode='w' перезаписывает файл
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("FastAPI_Main")

# --- SETUP ---
def migrate_db():
    """Простейшая миграция для добавления недостающих колонок в SQLite"""
    from .database import DB_PATH
    
    logger.info(f"Checking schema for {DB_PATH}...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Список колонок, которые могли быть добавлены позже
    # (column_name, table_name, column_type)
    new_columns = [
        ("docs_url", "tenders", "TEXT"),
        ("search_url", "tenders", "TEXT"),
        ("keyword", "tenders", "TEXT"),
        ("ntype", "tenders", "TEXT"),
        ("local_file_path", "tenders", "TEXT"),
        ("extracted_text", "tenders", "TEXT"),
        ("created_at", "tenders", "DATETIME"),
        ("description", "products", "TEXT"),
        ("updated_at", "products", "DATETIME"),
    ]
    
    for col_name, table_name, col_type in new_columns:
        try:
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type}")
            logger.info(f"Added column {col_name} to table {table_name}")
        except sqlite3.OperationalError:
            # Колонка уже существует
            pass
            
    conn.commit()
    conn.close()

try:
    logger.info("Initializing Database...")
    Base.metadata.create_all(bind=engine)
    migrate_db()
    logger.info("Database initialized successfully.")
except Exception as e:
    logger.critical(f"Database initialization failed: {e}", exc_info=True)

app = FastAPI(title="TenderSmart Gidroizol API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Разрешить все для локальной разработки
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Services
try:
    logger.info("Initializing Services...")
    eis_service = EisService()
    parser_service = GidroizolParser()
    doc_service = DocumentService()
    ai_service = AiService()
    legal_analysis_service = LegalAnalysisService(ai_service.client)
    logger.info("Services initialized.")
except Exception as e:
    logger.error(f"Service initialization error: {e}", exc_info=True)

# --- ENDPOINTS ---

@app.get("/")
def read_root():
    logger.info("Health check endpoint hit.")
    return {"status": "online", "system": "TenderSmart PRO Backend"}

# --- CRM ENDPOINTS (Database Sync) ---

@app.get("/api/crm/tenders")
def get_crm_tenders(db: Session = Depends(get_db)):
    """Получить все тендеры из базы"""
    logger.info("Fetching all CRM tenders.")
    return db.query(TenderModel).all()

@app.post("/api/crm/tenders")
def add_update_tender(background_tasks: BackgroundTasks, tender: dict = Body(...), db: Session = Depends(get_db)):
    """Добавить или обновить тендер в CRM"""
    logger.info(f"Add/Update tender request: {tender.get('id')}")
    try:
        existing = db.query(TenderModel).filter(TenderModel.id == tender['id']).first()
        
        # Parse initial_price to float
        raw_price = tender.get('initial_price', 0)
        parsed_price = 0.0
        if isinstance(raw_price, str):
            cleaned = re.sub(r'[^\d,.-]', '', raw_price).replace(',', '.')
            try:
                parsed_price = float(cleaned)
            except ValueError:
                parsed_price = 0.0
        else:
            parsed_price = float(raw_price)

        if existing:
            existing.status = tender.get('status', existing.status)
            existing.risk_level = tender.get('risk_level', existing.risk_level)
            logger.info(f"Updated existing tender: {tender['id']}")
        else:
            new_tender = TenderModel(
                id=tender['id'],
                title=tender['title'],
                description=tender.get('description', ''),
                initial_price=parsed_price,
                deadline=tender.get('deadline', '-'),
                status=tender.get('status', 'Found'),
                risk_level=tender.get('risk_level', 'Low'),
                region=tender.get('region', 'РФ'),
                law_type=tender.get('law_type', '44-ФЗ'),
                url=tender.get('url', ''),
                docs_url=tender.get('docs_url', ''),
                search_url=tender.get('search_url', ''),
                keyword=tender.get('keyword', ''),
                ntype=tender.get('ntype', '')
            )
            db.add(new_tender)
            logger.info(f"Created new tender: {tender['id']}")
            
            # Если это новый тендер, запускаем скачивание документов
            if tender.get('docs_url'):
                notice = Notice(
                    reg=tender['id'],
                    ntype=tender.get('ntype', ''),
                    keyword=tender.get('keyword', ''),
                    search_url=tender.get('search_url', ''),
                    href=tender.get('url', ''),
                    docs_url=tender.get('docs_url', ''),
                    title=tender.get('title', ''),
                    object_info=tender.get('description', ''),
                    initial_price=str(tender.get('initial_price', '')),
                    application_deadline=tender.get('deadline', '')
                )
                background_tasks.add_task(eis_service.process_tenders, [notice])
        
        db.commit()
        return {"status": "success"}
    except Exception as e:
        logger.error(f"Error saving tender: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/crm/tenders/{tender_id}")
def delete_tender(tender_id: str, db: Session = Depends(get_db)):
    """Удалить тендер из базы"""
    logger.info(f"Deleting tender: {tender_id}")
    tender = db.query(TenderModel).filter(TenderModel.id == tender_id).first()
    if tender:
        db.delete(tender)
        db.commit()
    return {"status": "deleted"}

# --- SEARCH & PARSING ---

@app.post("/api/search-tenders/cancel")
def cancel_search():
    """Отменить текущий поиск"""
    logger.info("Cancel search request received")
    eis_service.cancel_search()
    return {"status": "cancelled"}

@app.get("/api/search-tenders")
def search_tenders_endpoint(
    query: str, 
    fz44: bool = True, 
    fz223: bool = True, 
    only_application_stage: bool = True, 
    publish_days_back: int = 30
):
    """Поиск через Playwright"""
    logger.info(f"Search request received: {query}")
    try:
        notices = eis_service.search_tenders(
            query=query, 
            fz44=fz44, 
            fz223=fz223, 
            only_application_stage=only_application_stage, 
            publish_days_back=publish_days_back
        )
    except RuntimeError as e:
        logger.error(f"Search failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        logger.error(f"Search failed with unexpected error: {e}")
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")
    
    # Convert Notice dataclass to dict for JSON response
    result = []
    for n in notices:
        raw_price = n.initial_price
        parsed_price = 0.0
        if isinstance(raw_price, str):
            cleaned = re.sub(r'[^\d,.-]', '', raw_price).replace(',', '.')
            try:
                parsed_price = float(cleaned)
            except ValueError:
                parsed_price = 0.0
        else:
            parsed_price = float(raw_price)

        result.append({
            "id": n.reg,
            "eis_number": n.reg,
            "title": n.title,
            "description": n.object_info,
            "initial_price": parsed_price,
            "initial_price_text": str(raw_price),
            "initial_price_value": parsed_price,
            "deadline": n.application_deadline,
            "status": "Found",
            "risk_level": "Low",
            "region": "РФ",
            "law_type": n.ntype,
            "url": n.href,
            "docs_url": f"https://zakupki.gov.ru/epz/order/notice/{n.ntype}/view/documents.html?regNumber={n.reg}",
            "search_url": n.search_url,
            "keyword": n.keyword,
            "ntype": n.ntype,
            "seen": n.seen
        })
    return result

@app.post("/api/search-tenders/process")
def process_tenders(background_tasks: BackgroundTasks, tenders: list = Body(...), db: Session = Depends(get_db)):
    """Обработать выбранные тендеры"""
    logger.info(f"Processing {len(tenders)} selected tenders")
    try:
        for tender in tenders:
            existing = db.query(TenderModel).filter(TenderModel.id == tender['id']).first()
            
            # Parse initial_price to float
            raw_price = tender.get('initial_price', 0)
            parsed_price = 0.0
            if isinstance(raw_price, str):
                cleaned = re.sub(r'[^\d,.-]', '', raw_price).replace(',', '.')
                try:
                    parsed_price = float(cleaned)
                except ValueError:
                    parsed_price = 0.0
            else:
                parsed_price = float(raw_price)

            if existing:
                existing.status = tender.get('status', existing.status)
                existing.risk_level = tender.get('risk_level', existing.risk_level)
                logger.info(f"Updated existing tender: {tender['id']}")
            else:
                new_tender = TenderModel(
                    id=tender['id'],
                    title=tender['title'],
                    description=tender.get('description', ''),
                    initial_price=parsed_price,
                    deadline=tender.get('deadline', '-'),
                    status=tender.get('status', 'Found'),
                    risk_level=tender.get('risk_level', 'Low'),
                    region=tender.get('region', 'РФ'),
                    law_type=tender.get('law_type', '44-ФЗ'),
                    url=tender.get('url', ''),
                    docs_url=tender.get('docs_url', ''),
                    search_url=tender.get('search_url', ''),
                    keyword=tender.get('keyword', ''),
                    ntype=tender.get('ntype', '')
                )
                db.add(new_tender)
                logger.info(f"Created new tender: {tender['id']}")
                
                # Если это новый тендер, запускаем скачивание документов
                if tender.get('docs_url'):
                    notice = Notice(
                        reg=tender['id'],
                        ntype=tender.get('ntype', ''),
                        keyword=tender.get('keyword', ''),
                        search_url=tender.get('search_url', ''),
                        href=tender.get('url', ''),
                        docs_url=tender.get('docs_url', ''),
                        title=tender.get('title', ''),
                        object_info=tender.get('description', ''),
                        initial_price=str(tender.get('initial_price', '')),
                        application_deadline=tender.get('deadline', '')
                    )
                    background_tasks.add_task(eis_service.process_tenders, [notice])
            
        db.commit()
        return {"status": "success", "processed": len(tenders)}
    except Exception as e:
        logger.error(f"Error processing tenders: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/search-tenders/skip")
def skip_tender(tender: dict = Body(...)):
    """Пропустить тендер (отметить как просмотренный)"""
    logger.info(f"Skipping tender: {tender.get('id')}")
    try:
        mark_seen(tender['id'])
        csv_append_row(
            tender['id'], 
            tender.get('ntype', ''), 
            tender.get('keyword', ''), 
            tender.get('search_url', ''), 
            tender.get('docs_url', ''), 
            "SKIP:user_not_selected", 
            tender.get('title', ''), 
            tender.get('description', ''), 
            str(tender.get('initial_price', '')), 
            tender.get('deadline', '')
        )
        return {"status": "success"}
    except Exception as e:
        logger.error(f"Error skipping tender: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/products")
def get_products_endpoint(db: Session = Depends(get_db)):
    """Получение сохраненных товаров из БД без запуска парсера"""
    logger.info("Fetching products from DB.")
    products = db.query(ProductModel).all()
    result = []
    for p in products:
        result.append({
            "id": str(p.id),
            "title": p.title,
            "category": p.category,
            "material_type": p.material_type,
            "price": p.price,
            "specs": p.specs if p.specs else {},
            "url": p.url,
            "description": p.description # Added description
        })
    return result

@app.get("/api/parse-catalog")
async def parse_catalog_endpoint(db: Session = Depends(get_db)):
    """Запуск парсера каталога Gidroizol.ru и обновление БД"""
    logger.info("Starting catalog parser manually.")
    
    # Новый парсер возвращает количество сохраненных записей (int), а не список объектов
    saved_count = await parser_service.parse_and_save(db)
    logger.info(f"Parser finished. Saved/Updated {saved_count} items.")
    
    # После парсинга забираем актуальные данные из БД
    products = db.query(ProductModel).all()
    
    result = []
    for p in products:
        result.append({
            "id": str(p.id),
            "title": p.title,
            "category": p.category,
            "material_type": p.material_type,
            "price": p.price,
            "specs": p.specs if p.specs else {},
            "url": p.url,
            "description": p.description # Added description
        })
    return result

# --- AI & DOCS ENDPOINTS ---

@app.get("/api/tenders/{tender_id}/files")
def get_tender_files(tender_id: str):
    """Получить список скачанных файлов для тендера"""
    logger.info(f"Fetching files for tender {tender_id}")
    tender_dir = os.path.join(OUT_DIR, tender_id)
    if not os.path.exists(tender_dir):
        return []
    
    files = []
    for filename in os.listdir(tender_dir):
        filepath = os.path.join(tender_dir, filename)
        if os.path.isfile(filepath):
            files.append({
                "name": filename,
                "size": os.path.getsize(filepath),
                "ext": os.path.splitext(filename)[1].lower()
            })
    return files

@app.post("/api/ai/analyze-tenders-batch")
async def api_analyze_tenders_batch(background_tasks: BackgroundTasks, data: dict = Body(...)):
    logger.info("Batch AI Analysis request received.")
    tender_ids = data.get('tender_ids', [])
    selected_files = data.get('selected_files', {}) # {tender_id: [filenames]}
    
    if not tender_ids:
        raise HTTPException(status_code=400, detail="No tender IDs provided")
    
    job_id = job_service.create_job(tender_ids)
    background_tasks.add_task(analyze_tenders_batch_job, job_id, tender_ids, doc_service, legal_analysis_service, selected_files)
    
    return {"job_id": job_id}

@app.get("/api/ai/jobs/{job_id}")
async def get_job_status(job_id: str):
    job = job_service.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job

@app.post("/api/ai/extract-details")
async def api_extract_details(data: dict = Body(...)):
    """Извлечение данных о тендере из текста"""
    logger.info("AI Extract Details request.")
    text = data.get('text', '')
    return ai_service.extract_tender_details(text)

@app.post("/api/ai/extract-products")
async def api_extract_products(data: dict = Body(...)):
    """Извлечение списка товаров из сметы/КП"""
    logger.info("AI Extract Products request.")
    text = data.get('text', '')
    return ai_service.extract_products_from_text(text)

@app.post("/api/ai/enrich-specs")
async def api_enrich_specs(data: dict = Body(...)):
    """Поиск характеристик товара в интернете"""
    logger.info("AI Enrich Specs request.")
    product_name = data.get('product_name', '')
    result = ai_service.enrich_product_specs(product_name)
    return {"specs": result}

@app.post("/api/ai/match-product")
async def api_match_product(data: dict = Body(...), db: Session = Depends(get_db)):
    specs = data.get('specs', '')
    mode = data.get('mode', 'database') # 'database' or 'internet'
    logger.info(f"AI Match Product request. Mode: {mode}, Query len: {len(specs)}")

    if mode == 'internet':
        # Поиск в интернете через Grounding
        result_text = ai_service.search_products_internet(specs)
        return {"mode": "internet", "text": result_text}
    else:
        # Поиск по базе
        products_db = db.query(ProductModel).limit(50).all()
        catalog = [{"id": str(p.id), "title": p.title, "specs": p.specs} for p in products_db]
        matches = ai_service.find_product_equivalent(specs, catalog)
        return {"mode": "database", "matches": matches}

@app.post("/api/ai/validate-compliance")
async def api_validate_compliance(data: dict = Body(...)):
    """Валидация ТЗ vs Материал (Complex)"""
    logger.info("AI Compliance Validation request.")
    requirements = data.get('requirements', '')
    proposal = data.get('proposal', '[]')
    return ai_service.compare_requirements_vs_proposal(requirements, proposal)

@app.post("/api/ai/check-compliance")
async def api_check_compliance(data: dict = Body(...)):
    """Проверка пакета документов"""
    logger.info("AI Document Package Check request.")
    return ai_service.check_compliance(data['title'], data['description'], data['filenames'])

@app.post("/api/tenders/upload")
async def upload_file(file: UploadFile = File(...)):
    logger.info(f"File upload request: {file.filename}")
    try:
        file_path = await doc_service.save_file(file)
        # Запускаем OCR или извлечение текста
        text = doc_service.extract_text(file_path)
        logger.info("File processed successfully.")
        return {"text": text, "path": file_path}
    except Exception as e:
        logger.error(f"Upload Error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/dashboard-stats")
async def get_dashboard_stats(db: Session = Depends(get_db)):
    logger.info("Dashboard stats requested.")
    count = db.query(TenderModel).count()
    return {
        "active_tenders": count,
        "margin_val": "₽14.2M",
        "risks_count": 5,
        "contracts_count": 12,
        "chart_data": [{"name": "Пн", "Тендеры": 10, "Выиграно": 2}],
        "tasks": [{"id": "1", "title": "Запустить парсер", "time": "Сейчас", "type": "info"}],
        "is_demo": False
    }

@app.post("/api/ai/export-risks-excel")
async def api_export_risks_excel(data: dict = Body(...)):
    """Экспорт результатов анализа рисков в Excel .xlsx"""
    logger.info("Exporting risk analysis to Excel.")
    results = data.get('results', [])
    if not results:
        raise HTTPException(status_code=400, detail="No results to export")

    try:
        wb = openpyxl.Workbook()
        
        # 1. Сводка
        ws_summary = wb.active
        ws_summary.title = "Сводка"
        ws_summary.append(["ID Тендера", "Наименование / Описание", "Кол-во файлов", "Проект контракта", "Кол-во рисков (rows)", "Наличие противоречий", "Краткие выводы (summary_notes)"])
        
        # 2. Краткие риски
        ws_risks = wb.create_sheet(title="Краткие риски")
        ws_risks.append(["ID Тендера", "Блок", "Что найдено", "Риск", "Что делать поставщику", "Источник", "Основание"])
        
        # 3. Подробный отчет
        ws_report = wb.create_sheet(title="Подробный отчет")
        ws_report.append(["ID Тендера", "Раздел", "Содержание"])
        
        # 4. Документы заявки
        ws_app_docs = wb.create_sheet(title="Документы заявки")
        ws_app_docs.append(["ID Тендера", "Документы в составе заявки"])
        
        # 5. Документы при поставке
        ws_del_docs = wb.create_sheet(title="Документы при поставке")
        ws_del_docs.append(["ID Тендера", "Документы при поставке"])
        
        # 6. Противоречия и соответствие
        ws_comp = wb.create_sheet(title="Противоречия и соответствие")
        ws_comp.append(["ID Тендера", "Противоречия, ошибки и соответствие"])
        
        # 7. Полный текст отчета
        ws_full_report = wb.create_sheet(title="Полный текст отчета")
        ws_full_report.append(["ID Тендера", "Полный текст отчета (Markdown)"])
        
        # 8. Источники и служебная информация
        ws_meta = wb.create_sheet(title="Источники и служебная инфо")
        ws_meta.append(["ID Тендера", "Источник", "Ссылка", "Основание", "Статусы файлов", "Заметки классификации", "Служебные выводы"])
        
        # Style headers
        header_font = Font(bold=True, color="FFFFFF")
        header_fill = PatternFill(start_color="1F4E78", end_color="1F4E78", fill_type="solid")
        for ws in wb.worksheets:
            for cell in ws[1]:
                cell.font = header_font
                cell.fill = header_fill
                cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
                
        for tender in results:
            tid = str(tender.get('id', 'N/A'))
            desc = tender.get('description', 'Нет описания')
            has_contract = "Да" if tender.get('has_contract') else "Нет"
            
            file_statuses = tender.get('file_statuses', [])
            file_count = len(file_statuses)
            
            rows = tender.get('rows', [])
            risk_count = len(rows)
            
            notes_full = "\n".join(tender.get('summary_notes', []))
            notes_short = notes_full if len(notes_full) <= 300 else notes_full[:297] + "..."
            desc_short = desc if len(desc) <= 150 else desc[:147] + "..."
            
            sections = tender.get('final_report_sections', [])
            detailed_report = tender.get('detailed_report', {})
            contradictions = tender.get('contradictions', [])
            final_md = tender.get('final_report_markdown', '')
            
            logger.info(f"--- [EXCEL EXPORT PREPARATION FOR TENDER: {tid}] ---")
            logger.info(f"Rows count: {risk_count}")
            logger.info(f"Sections count: {len(sections) if isinstance(sections, list) else len(sections) if isinstance(sections, dict) else 0}")
            logger.info(f"Markdown length: {len(final_md)}")
            logger.info(f"Contradictions count: {len(contradictions)}")
            
            # Parse sections into a dict for easier access
            sections_dict = {}
            if isinstance(sections, list) and len(sections) > 0 and isinstance(sections[0], dict):
                for sec in sections:
                    sections_dict[sec.get('section_title', '')] = sec.get('content', '')
            elif isinstance(sections, dict) and sections:
                for k, v in sections.items():
                    sections_dict[k] = "\n".join(v) if isinstance(v, list) else str(v)
            elif isinstance(detailed_report, dict) and detailed_report:
                for k, v in detailed_report.items():
                    sections_dict[k] = "\n".join(v) if isinstance(v, list) else str(v)
            
            # Parse final_md into sections as a fallback
            md_sections_dict = {}
            if final_md:
                # Robust split by section headers like "### 1) ", "## 1.", "Раздел 1:", "1) Риски..."
                # We look for lines that start with a number and a closing parenthesis or dot, or start with "Раздел"
                md_parts = re.split(r'(?m)^(?:#+\s+)?(?:Раздел\s+)?(\d+[\)\.]\s*.*)', final_md)
                if len(md_parts) > 1:
                    # The first part is usually the title or intro before any numbered section
                    for i in range(1, len(md_parts), 2):
                        title = md_parts[i].strip()
                        content = md_parts[i+1].strip() if i+1 < len(md_parts) else ""
                        # Clean title from markdown artifacts
                        clean_title = re.sub(r'^#+\s*', '', title).strip()
                        md_sections_dict[clean_title] = content
                else:
                    # Fallback if no numbered sections found: split by any ### or ## headers
                    md_parts = re.split(r'(?m)^#+\s+(.*)', final_md)
                    if len(md_parts) > 1:
                        for i in range(1, len(md_parts), 2):
                            title = md_parts[i].strip()
                            content = md_parts[i+1].strip() if i+1 < len(md_parts) else ""
                            md_sections_dict[title] = content
            
            compliance_content = ""
            app_docs_content = ""
            del_docs_content = ""
            
            def get_content(keywords, dicts, fallback_rows=None, block_name=None):
                # Priority 1: Check provided dictionaries (sections_dict, md_sections_dict)
                for d in dicts:
                    if not d: continue
                    for k, v in d.items():
                        k_lower = k.lower()
                        # Match by keyword or if the key starts with the keyword (e.g. "3)" matches "3) Проверка...")
                        if any(kw.lower() in k_lower for kw in keywords):
                            v_clean = v.strip().lower()
                            # Skip placeholders
                            if v_clean in ["информация не обнаружена.", "информация не обнаружена", "информация в предоставленной документации не обнаружена.", "информация в предоставленной документации не обнаружена"]:
                                continue
                            if v.strip():
                                return v.strip()
                
                # Priority 2: Last resort fallback to rows if block_name matches
                if fallback_rows and block_name:
                    row_findings = [r.get('finding', '') for r in fallback_rows if r.get('block') == block_name]
                    if row_findings:
                        return "\n".join(row_findings)
                
                return ""
            
            # Extract content for specialized sheets
            compliance_content = get_content(["проверка соответствия", "compliance_check", "3)"], [md_sections_dict, sections_dict], rows, "Проверка соответствия документации и закона")
            docs_content = get_content(["перечень документов", "documents_list", "7)"], [md_sections_dict, sections_dict], rows, "Перечень документов")
            
            if docs_content:
                # Robust split for documents list
                # Look for "В составе заявки" and "При поставке" markers
                app_marker = re.search(r'(?i)(?:В составе заявки|Для участия|В заявке)', docs_content)
                del_marker = re.search(r'(?i)(?:При поставке|Для исполнения|При приемке)', docs_content)
                
                if app_marker and del_marker:
                    if app_marker.start() < del_marker.start():
                        app_docs_content = docs_content[app_marker.start():del_marker.start()].strip()
                        del_docs_content = docs_content[del_marker.start():].strip()
                    else:
                        del_docs_content = docs_content[del_marker.start():app_marker.start()].strip()
                        app_docs_content = docs_content[app_marker.start():].strip()
                else:
                    # If markers not found, try splitting by common patterns
                    parts = re.split(r'(?i)(?:\*\*|#|\d\.)?\s*При поставке.*?:?', docs_content)
                    if len(parts) > 1:
                        app_docs_content = re.sub(r'(?i)(?:\*\*|#|\d\.)?\s*В составе заявки.*?:?', '', parts[0]).strip()
                        del_docs_content = parts[1].strip()
                    else:
                        app_docs_content = docs_content.strip()
            
            # Add contradictions to compliance if not already there
            if contradictions:
                contradictions_text = "\n\n".join(contradictions) if isinstance(contradictions, list) else str(contradictions)
                if compliance_content:
                    if "противореч" not in compliance_content.lower() and "ошибк" not in compliance_content.lower():
                        compliance_content = "ВЫЯВЛЕННЫЕ ПРОТИВОРЕЧИЯ:\n" + contradictions_text + "\n\nДЕТАЛИ СООТВЕТСТВИЯ:\n" + compliance_content
                else:
                    compliance_content = contradictions_text
            
            FALLBACK_TEXT = "Информация в предоставленной документации не обнаружена"
            
            if not compliance_content:
                compliance_content = FALLBACK_TEXT
            if not app_docs_content:
                app_docs_content = FALLBACK_TEXT
            if not del_docs_content:
                del_docs_content = FALLBACK_TEXT
            
            has_contradictions = "Да" if compliance_content and compliance_content != FALLBACK_TEXT and ("противореч" in compliance_content.lower() or "ошибк" in compliance_content.lower()) and "не обнаружено" not in compliance_content.lower() else "Нет"
            
            # --- Visual separation for all sheets except Сводка ---
            for ws in [ws_risks, ws_report, ws_app_docs, ws_del_docs, ws_comp, ws_full_report, ws_meta]:
                ws.append([f"ТЕНДЕР: {tid}"])
                ws[ws.max_row][0].font = Font(bold=True, size=12, color="FFFFFF")
                ws[ws.max_row][0].fill = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
                ws.merge_cells(start_row=ws.max_row, start_column=1, end_row=ws.max_row, end_column=ws.max_column if ws.max_column > 1 else 2)
                
                ws.append([f"Описание: {desc}"])
                ws[ws.max_row][0].font = Font(italic=True)
                ws[ws.max_row][0].fill = PatternFill(start_color="D9E1F2", end_color="D9E1F2", fill_type="solid")
                ws.merge_cells(start_row=ws.max_row, start_column=1, end_row=ws.max_row, end_column=ws.max_column if ws.max_column > 1 else 2)
            
            # --- Build Подробный отчет ---
            # We want to show all 9 sections here
            merged_sections = {} # Initialize here to avoid UnboundLocalError
            section_names = [
                "1) Риски участия и исполнения договора",
                "2) Риски недопуска заявки и потери баллов",
                "3) Проверка соответствия документации и закона",
                "4) Условия поставки и приемки",
                "5) Условия оплаты",
                "6) Ответственность сторон",
                "7) Перечень документов",
                "8) Требования по реестрам и ограничениям",
                "9) Рекомендации Поставщику"
            ]
            
            found_any_section = False
            for s_name in section_names:
                # Try to find content for this section name
                s_content = get_content([s_name.split(") ")[1], s_name[:3]], [md_sections_dict, sections_dict], rows, s_name.split(") ")[1])
                if s_content:
                    ws_report.append([tid, s_name, s_content])
                    found_any_section = True
                else:
                    ws_report.append([tid, s_name, FALLBACK_TEXT])
            
            if not found_any_section:
                # If we didn't find specific sections, just dump whatever we have in merged_sections
                if md_sections_dict:
                    merged_sections = md_sections_dict
                elif sections_dict:
                    merged_sections = sections_dict
                
                if merged_sections:
                    for sec_title, sec_content in merged_sections.items():
                        ws_report.append([tid, sec_title, sec_content])
                else:
                    ws_report.append([tid, "Отчет", FALLBACK_TEXT])
            else:
                # If we found sections via section_names, we might want to keep track of them for logging
                # although ws_report already has them. For logging purposes, we can populate merged_sections
                # if it's empty, but it's better to just log what we found.
                pass
            
            # --- Build Полный текст отчета ---
            if final_md:
                ws_full_report.append([tid, final_md])
            else:
                ws_full_report.append([tid, FALLBACK_TEXT])
            
            ws_summary.append([tid, desc_short, file_count, has_contract, risk_count, has_contradictions, notes_short])
            
            ws_app_docs.append([tid, app_docs_content])
            ws_del_docs.append([tid, del_docs_content])
            ws_comp.append([tid, compliance_content])
            
            if rows:
                for row in rows:
                    ws_risks.append([
                        tid,
                        row.get('block', ''),
                        row.get('finding', ''),
                        row.get('risk_level', ''),
                        row.get('supplier_action', ''),
                        f"{row.get('source_document', '')} {row.get('source_reference', '')}".strip(),
                        row.get('legal_basis', '')
                    ])
                    
                    ws_meta.append([
                        tid,
                        row.get('source_document', ''),
                        row.get('source_reference', ''),
                        row.get('legal_basis', ''),
                        "\n".join([f"{f.get('filename')}: {f.get('status')}" for f in file_statuses]),
                        "\n".join(tender.get('classification_notes', [])),
                        notes_full
                    ])
            else:
                ws_risks.append([tid, "Нет данных", "-", "-", "-", "-", "-"])
                ws_meta.append([
                    tid, "-", "-", "-",
                    "\n".join([f"{f.get('filename')}: {f.get('status')}" for f in file_statuses]),
                    "\n".join(tender.get('classification_notes', [])),
                    notes_full
                ])
                
            logger.info(f"--- [EXCEL EXPORT COMPLETED FOR TENDER: {tid}] ---")
            # Safe logging for merged_sections
            sections_count = len(merged_sections) if merged_sections else (len(section_names) if found_any_section else 0)
            logger.info(f"Exported {sections_count} sections to 'Подробный отчет'")
            logger.info(f"Exported {len(rows)} rows to 'Краткие риски'")
            logger.info(f"Exported app_docs_content length: {len(app_docs_content)}")
            logger.info(f"Exported del_docs_content length: {len(del_docs_content)}")
            logger.info(f"Exported compliance_content length: {len(compliance_content)}")
            
            # Empty row before next tender
            for ws in [ws_risks, ws_report, ws_app_docs, ws_del_docs, ws_comp, ws_full_report, ws_meta]:
                ws.append([])

        # Add filters
        ws_risks.auto_filter.ref = ws_risks.dimensions
        ws_report.auto_filter.ref = ws_report.dimensions
        ws_app_docs.auto_filter.ref = ws_app_docs.dimensions
        ws_del_docs.auto_filter.ref = ws_del_docs.dimensions
        ws_comp.auto_filter.ref = ws_comp.dimensions
        ws_full_report.auto_filter.ref = ws_full_report.dimensions

        # Set column widths and wrap text
        for ws in wb.worksheets:
            for col_idx in range(1, ws.max_column + 1):
                column = get_column_letter(col_idx)
                if ws.title == "Сводка":
                    widths = {'A': 15, 'B': 30, 'C': 15, 'D': 20, 'E': 20, 'F': 25, 'G': 50}
                    ws.column_dimensions[column].width = widths.get(column, 20)
                elif ws.title == "Краткие риски":
                    widths = {'A': 15, 'B': 20, 'C': 40, 'D': 15, 'E': 40, 'F': 30, 'G': 30}
                    ws.column_dimensions[column].width = widths.get(column, 20)
                elif ws.title == "Подробный отчет":
                    widths = {'A': 15, 'B': 40, 'C': 100}
                    ws.column_dimensions[column].width = widths.get(column, 20)
                elif ws.title in ["Документы заявки", "Документы при поставке", "Противоречия и соответствие"]:
                    widths = {'A': 15, 'B': 100}
                    ws.column_dimensions[column].width = widths.get(column, 20)
                elif ws.title == "Полный текст отчета":
                    widths = {'A': 15, 'B': 120}
                    ws.column_dimensions[column].width = widths.get(column, 20)
                elif ws.title == "Источники и служебная инфо":
                    widths = {'A': 15, 'B': 30, 'C': 30, 'D': 30, 'E': 40, 'F': 40, 'G': 40}
                    ws.column_dimensions[column].width = widths.get(column, 20)
                
            for row in ws.iter_rows(min_row=2):
                for cell in row:
                    if isinstance(cell, MergedCell):
                        continue
                    cell.alignment = Alignment(wrap_text=True, vertical="top")
                    thin_border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))
                    cell.border = thin_border

        # Save to temp file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            wb.save(tmp.name)
            return FileResponse(tmp.name, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", filename="tender_risks_report.xlsx")

    except Exception as e:
        logger.error(f"Excel Export Error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run("backend.main:app", host="0.0.0.0", port=8000, reload=True)
