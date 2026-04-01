import os
import logging
import zipfile
import time
from typing import List, Dict, Any
from .document_service import DocumentService
from .legal_analysis_service import LegalAnalysisService
from backend.config import DOCUMENTS_ROOT
from .job_service import job_service

from backend.logger import logger

def analyze_tenders_batch_job(
    job_id: str, 
    tender_ids: List[str], 
    doc_service: DocumentService, 
    legal_service: LegalAnalysisService, 
    selected_files: Dict[str, List[str]] = None
):
    """
    Основной воркер для пакетного анализа тендеров.
    Реализует архитектуру с возвратом JSON (row-based):
    1. Извлечение текста (DocumentService).
    2. Полнотекстовый ИИ-анализ (LegalAnalysisService).
    3. Сохранение результатов в JobService.
    """
    selected_files = selected_files or {}
    documents_root = DOCUMENTS_ROOT
    
    for tid in tender_ids:
        logger.info(f"--- [START TENDER ANALYSIS: {tid}] ---")
        tender_dir = os.path.join(documents_root, tid)
        
        # 0. Инициализация статуса
        job_service.update_tender_stage(job_id, tid, "Подготовка документов", 10)
        
        # 1. Проверка выбора файлов
        requested_files = selected_files.get(tid, [])
        if not requested_files:
            logger.warning(f"No files selected for tender {tid}")
            job_service.complete_tender(job_id, tid, {
                "status": "error",
                "summary_notes": "Не выбрано ни одного файла для анализа.",
                "rows": [],
                "has_contract": False,
                "file_statuses": [],
                "unread_files_count": 0
            })
            continue

        # 2. Проверка существования директории
        if not os.path.exists(tender_dir):
            logger.warning(f"Tender directory not found: {tender_dir}")
            job_service.complete_tender(job_id, tid, {
                "status": "error",
                "summary_notes": "Директория с документами не найдена.",
                "rows": [],
                "has_contract": False,
                "file_statuses": [{"filename": f, "status": "error", "message": "Директория не найдена"} for f in requested_files],
                "unread_files_count": len(requested_files)
            })
            continue
            
        # 3. Извлечение текста
        job_service.update_tender_stage(job_id, tid, "Извлечение текста", 20)
        
        files_data = []
        file_statuses = []
        available_files = os.listdir(tender_dir)
        
        logger.info(f"--- [STARTING TEXT EXTRACTION FOR TENDER {tid}] ---")
        logger.info(f"Requested files: {requested_files}")
        
        # Дедупликация: если есть и .doc и .docx с одним именем, берем только .docx
        docx_bases = {os.path.splitext(f)[0] for f in requested_files if f.lower().endswith('.docx')}
        filtered_files = []
        for f in requested_files:
            base, ext = os.path.splitext(f)
            if ext.lower() == '.doc' and base in docx_bases:
                logger.info(f"Skipping {f} because {base}.docx is also in requested files.")
                continue
            filtered_files.append(f)
        
        logger.info(f"Filtered files to process: {filtered_files}")
        
        extraction_start_time = time.time()
        for filename in filtered_files:
            if filename not in available_files:
                logger.warning(f"File {filename} not found in {tender_dir}")
                file_statuses.append({"filename": filename, "status": "error", "message": "Файл не найден на диске"})
                continue
                
            filepath = os.path.join(tender_dir, filename)
            logger.info(f"Processing file: {filename}")
            try:
                doc_data = doc_service.extract_document_data(filepath)
                text = doc_data.get("text", "")
                if text and len(text.strip()) > 0:
                    char_count = len(text)
                    logger.info(f"Successfully extracted {char_count} chars from {filename}")
                    
                    if "[SYSTEM INFO]" in text:
                        logger.warning(f"File {filename} processed with SYSTEM INFO (possibly DEGRADED).")
                    
                    files_data.append(doc_data)
                    file_statuses.append({"filename": filename, "status": doc_data.get("status", "ok"), "message": doc_data.get("error_message", "Текст успешно извлечен")})
                    
                    if doc_data.get("critical_for_analysis"):
                        logger.warning(f"Critical document degraded for tender {tid}: {filename}")
                else:
                    logger.warning(f"File {filename} is empty or no text extracted.")
                    file_statuses.append({"filename": filename, "status": "warning", "message": "Файл пуст или текст не извлечен"})
            except Exception as e:
                logger.error(f"Failed to extract text from {filename}: {e}")
                file_statuses.append({"filename": filename, "status": "error", "message": f"Ошибка извлечения: {str(e)}"})
        extraction_end_time = time.time()
        extraction_time = extraction_end_time - extraction_start_time

        has_critical_degraded_file = any(
            f.get("critical_for_analysis") is True for f in files_data
        )

        if not files_data:
            logger.error(f"No text extracted from any of the selected files for tender {tid}")
            job_service.complete_tender(job_id, tid, {
                "status": "error",
                "summary_notes": "Не удалось извлечь текст ни из одного файла.",
                "rows": [],
                "has_contract": False,
                "file_statuses": file_statuses,
                "unread_files_count": len(file_statuses)
            })
            continue

        # 4. ИИ-анализ (LegalAnalysisService)
        try:
            def stage_callback(stage, progress, status="running"):
                job_service.update_tender_stage(job_id, tid, stage, progress, status)

            analysis_start_time = time.time()
            analysis_result = legal_service.analyze_tender(
                files_data, 
                tender_id=tid, 
                job_id=job_id,
                callback=stage_callback
            )
            analysis_end_time = time.time()
            
            if has_critical_degraded_file and analysis_result.get("status") == "success":
                analysis_result["status"] = "partial"
            
            # 5. Финальное логирование требуемых метрик
            logger.info(f"--- [ANALYSIS LOGS FOR TENDER {tid}] ---")
            logger.info(f"- Extraction time: {extraction_time:.2f}s")
            logger.info(f"- AI Analysis time: {analysis_end_time - analysis_start_time:.2f}s")
            logger.info(f"----------------------------------------")

            # 6. Завершение задачи для тендера
            job_service.complete_tender(job_id, tid, {
                "status": analysis_result.get('status', 'success'),
                "summary_notes": analysis_result.get('summary_notes', ''),
                "rows": analysis_result.get('rows', []),
                "has_contract": analysis_result.get('has_contract', False),
                "file_statuses": analysis_result.get('file_statuses', file_statuses),
                "unread_files_count": analysis_result.get('unread_files_count', 0)
            })
            logger.info(f"--- [END TENDER ANALYSIS: {tid}] ---")
            
        except Exception as e:
            logger.error(f"Analysis failed for tender {tid}: {e}", exc_info=True)
            job_service.complete_tender(job_id, tid, {
                "status": "error",
                "summary_notes": f"Критическая ошибка анализа: {str(e)}",
                "rows": [],
                "has_contract": False,
                "file_statuses": file_statuses,
                "unread_files_count": len(file_statuses)
            })
            
    # 8. Проверка завершения всего задания
    job_service.check_job_completion(job_id)
