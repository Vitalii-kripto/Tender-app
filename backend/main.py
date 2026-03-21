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
        
        # 4. Документы заявки
        ws_app_docs = wb.create_sheet(title="Документы заявки")
        ws_app_docs.append(["ID Тендера", "Документы в составе заявки"])
        
        # 5. Документы при поставке
        ws_del_docs = wb.create_sheet(title="Документы при поставке")
        ws_del_docs.append(["ID Тендера", "Документы при поставке"])
        
        # 6. Противоречия и соответствие
        ws_comp = wb.create_sheet(title="Противоречия и соответствие")
        ws_comp.append(["ID Тендера", "Противоречия, ошибки и соответствие"])
        
        # 7. Источники и служебная информация
        ws_meta = wb.create_sheet(title="Источники и служебная инфо")
        ws_meta.append(["ID Тендера", "Источник", "Ссылка", "Основание", "Статусы файлов", "Заметки классификации", "Служебные выводы"])
        
        # Style headers
        header_font = Font(bold=True, color="FFFFFF")
        header_fill = PatternFill(start_color="1F4E78", end_color="1F4E78", fill_type="solid")
        for ws in wb.worksheets:
            if ws.title == "Подробный отчет":
                continue
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
            
            compliance_content = ""
            app_docs_content = ""
            del_docs_content = ""
            
            for k, v in sections_dict.items():
                if "Проверка соответствия" in k or k.startswith("3)") or k == "compliance_check":
                    compliance_content = v
                elif "Перечень документов" in k or k.startswith("7)") or k == "documents_list":
                    if "**При поставке**:" in v:
                        parts = v.split("**При поставке**:")
                        app_docs_content = parts[0].replace("**В составе заявки**:", "").strip()
                        del_docs_content = parts[1].strip()
                    else:
                        app_docs_content = v
            
            if contradictions:
                contradictions_text = "\n".join(contradictions) if isinstance(contradictions, list) else str(contradictions)
                if compliance_content:
                    compliance_content += "\n\nДополнительные противоречия:\n" + contradictions_text
                else:
                    compliance_content = contradictions_text
            
            FALLBACK_TEXT = "Данные по этому разделу отсутствуют в результате анализа"
            
            if not compliance_content:
                compliance_content = FALLBACK_TEXT
            if not app_docs_content:
                app_docs_content = FALLBACK_TEXT
            if not del_docs_content:
                del_docs_content = FALLBACK_TEXT
            
            has_contradictions = "Да" if compliance_content and compliance_content != FALLBACK_TEXT and ("противореч" in compliance_content.lower() or "ошибк" in compliance_content.lower()) else "Нет"
            
            # --- Visual separation for all sheets except Сводка ---
            for ws in [ws_risks, ws_app_docs, ws_del_docs, ws_comp, ws_meta]:
                ws.append([f"ТЕНДЕР: {tid}"])
                ws[ws.max_row][0].font = Font(bold=True, size=12, color="FFFFFF")
                ws[ws.max_row][0].fill = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
                ws.merge_cells(start_row=ws.max_row, start_column=1, end_row=ws.max_row, end_column=ws.max_column)
                
                ws.append([f"Описание: {desc}"])
                ws[ws.max_row][0].font = Font(italic=True)
                ws[ws.max_row][0].fill = PatternFill(start_color="D9E1F2", end_color="D9E1F2", fill_type="solid")
                ws.merge_cells(start_row=ws.max_row, start_column=1, end_row=ws.max_row, end_column=ws.max_column)
            
            # --- Build Подробный отчет ---
            ws_report.append([f"ТЕНДЕР: {tid} - {desc}"])
            ws_report[ws_report.max_row][0].font = Font(bold=True, size=14, color="FFFFFF")
            ws_report[ws_report.max_row][0].fill = PatternFill(start_color="2F75B5", end_color="2F75B5", fill_type="solid")
            ws_report.merge_cells(start_row=ws_report.max_row, start_column=1, end_row=ws_report.max_row, end_column=4)
            ws_report.append([])
            
            SECTIONS_CONFIG = [
                {"title": "1) Риски участия и исполнения договора", "columns": ["Вид риска", "Описание риска", "Основание", "Меры защиты"]},
                {"title": "2) Риски недопуска заявки и потери баллов", "columns": ["Риск", "Описание", "Основание"]},
                {"title": "3) Проверка соответствия документации и закона", "columns": ["Параметр", "Вывод", "Основание"]},
                {"title": "4) Условия поставки и приемки", "columns": ["Условие", "Описание", "Основание"]},
                {"title": "5) Условия оплаты", "columns": ["Условие", "Описание", "Основание"]},
                {"title": "6) Ответственность сторон", "columns": ["Сторона", "Описание ответственности", "Основание"]},
                {"title": "7) Перечень документов", "columns": []},
                {"title": "8) Требования по реестрам и ограничениям", "columns": ["Требование", "Описание", "Основание"]},
                {"title": "9) Рекомендации Поставщику", "columns": ["№", "Рекомендация", "Основание"]}
            ]
            
            for sec_config in SECTIONS_CONFIG:
                sec_title = sec_config["title"]
                cols = sec_config["columns"]
                
                content = ""
                for k, v in sections_dict.items():
                    if sec_title.lower() in k.lower() or k.lower() in sec_title.lower() or (sec_title[:2] in k):
                        content = v
                        break
                
                if not content:
                    content = FALLBACK_TEXT
                    
                ws_report.append([sec_title])
                ws_report[ws_report.max_row][0].font = Font(bold=True, size=13, color="FFFFFF")
                ws_report[ws_report.max_row][0].fill = PatternFill(start_color="305496", end_color="305496", fill_type="solid")
                ws_report.merge_cells(start_row=ws_report.max_row, start_column=1, end_row=ws_report.max_row, end_column=4)
                
                if sec_title.startswith("7)"):
                    ws_report.append(["В составе заявки", "При поставке"])
                    for cell in ws_report[ws_report.max_row]:
                        cell.font = Font(bold=True)
                        cell.fill = PatternFill(start_color="BDD7EE", end_color="BDD7EE", fill_type="solid")
                    ws_report.append([app_docs_content, del_docs_content])
                else:
                    ws_report.append(cols)
                    for cell in ws_report[ws_report.max_row]:
                        cell.font = Font(bold=True)
                        cell.fill = PatternFill(start_color="BDD7EE", end_color="BDD7EE", fill_type="solid")
                        
                    parsed_rows = []
                    lines = content.split('\n')
                    for line in lines:
                        line = line.strip()
                        if not line:
                            continue
                            
                        is_list_item = False
                        if line.startswith('- ') or line.startswith('* '):
                            line = line[2:]
                            is_list_item = True
                        elif re.match(r'^\d+\.\s', line):
                            line = re.sub(r'^\d+\.\s', '', line)
                            is_list_item = True
                            
                        prefix = "-"
                        desc_text = line
                        match = re.match(r'^\*\*(.*?)\*\*\s*[:\-]?\s*(.*)', line)
                        if match:
                            prefix = match.group(1).strip()
                            desc_text = match.group(2).strip()
                            is_list_item = True
                            
                        if is_list_item or len(parsed_rows) == 0:
                            new_row = ["-"] * len(cols)
                            if len(cols) > 0:
                                if prefix != "-":
                                    new_row[0] = prefix
                                    if len(cols) > 1:
                                        new_row[1] = desc_text
                                else:
                                    if len(cols) > 1:
                                        new_row[1] = desc_text
                                    else:
                                        new_row[0] = desc_text
                            parsed_rows.append(new_row)
                        else:
                            if len(cols) > 1:
                                parsed_rows[-1][1] += "\n" + line
                            elif len(cols) > 0:
                                parsed_rows[-1][0] += "\n" + line
                                
                    if not parsed_rows:
                        parsed_rows.append(["-"] * len(cols))
                        
                    if cols and cols[0] == "№":
                        for i, r in enumerate(parsed_rows):
                            r[0] = str(i + 1)
                            
                    for r in parsed_rows:
                        ws_report.append(r)
                        
                ws_report.append([]) # Empty row
            
            final_md = tender.get('final_report_markdown', '')
            if final_md:
                ws_report.append(["Полный текст отчета (Markdown)"])
                ws_report[ws_report.max_row][0].font = Font(bold=True, size=13, color="FFFFFF")
                ws_report[ws_report.max_row][0].fill = PatternFill(start_color="305496", end_color="305496", fill_type="solid")
                ws_report.merge_cells(start_row=ws_report.max_row, start_column=1, end_row=ws_report.max_row, end_column=4)
                ws_report.append([final_md])
                ws_report.merge_cells(start_row=ws_report.max_row, start_column=1, end_row=ws_report.max_row, end_column=4)
                ws_report.append([])
            
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
                
            # Empty row before next tender
            for ws in [ws_risks, ws_app_docs, ws_del_docs, ws_comp, ws_meta]:
                ws.append([])

        # Add filters
        ws_risks.auto_filter.ref = ws_risks.dimensions
        ws_app_docs.auto_filter.ref = ws_app_docs.dimensions
        ws_del_docs.auto_filter.ref = ws_del_docs.dimensions
        ws_comp.auto_filter.ref = ws_comp.dimensions

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
                    widths = {'A': 30, 'B': 50, 'C': 30, 'D': 30}
                    ws.column_dimensions[column].width = widths.get(column, 20)
                elif ws.title in ["Документы заявки", "Документы при поставке", "Противоречия и соответствие"]:
                    widths = {'A': 15, 'B': 80}
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
