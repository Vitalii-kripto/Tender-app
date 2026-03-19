import os
import logging
from typing import List, Dict, Any
from fastapi import APIRouter, HTTPException
from .document_service import DocumentService
from .legal_analysis_service import LegalAnalysisService
from .eis_service import OUT_DIR
from .job_service import job_service

logger = logging.getLogger("BatchAnalysis")

def analyze_tenders_batch_job(job_id: str, tender_ids: List[str], doc_service: DocumentService, legal_service: LegalAnalysisService, selected_files: Dict[str, List[str]] = None):
    selected_files = selected_files or {}
    
    for tid in tender_ids:
        logger.info(f"Starting analysis for tender {tid} in job {job_id}")
        tender_dir = os.path.join(OUT_DIR, tid)
        
        job_service.update_tender_stage(job_id, tid, "Подготовка документов", 10)
        
        # 1. Проверка выбора файлов
        if tid not in selected_files or not selected_files[tid]:
            logger.warning(f"No files selected for tender {tid}")
            job_service.complete_tender(job_id, tid, {
                "status": "error",
                "summary_notes": ["Ошибка: не выбрано ни одного файла для анализа. Пожалуйста, выберите хотя бы один документ."],
                "rows": [],
                "has_contract": False,
                "file_statuses": [],
                "selected_files_count": 0
            })
            continue

        selected_count = len(selected_files[tid])

        if not os.path.exists(tender_dir):
            logger.warning(f"No documents found for tender {tid}")
            job_service.complete_tender(job_id, tid, {
                "status": "error",
                "summary_notes": ["Документы не найдены. Возможно, они еще скачиваются или не были загружены."],
                "rows": [],
                "has_contract": False,
                "file_statuses": [{"filename": "N/A", "status": "file_not_read", "message": "Директория с документами не найдена"}],
                "selected_files_count": selected_count
            })
            continue
            
        files_data = []
        file_statuses = []
        missing_files = []
        
        # 2. Фильтрация и проверка существования файлов
        available_files = os.listdir(tender_dir)
        requested_files = selected_files[tid]
        
        target_files = []
        for f in requested_files:
            if f in available_files:
                target_files.append(f)
            else:
                missing_files.append(f)
                file_statuses.append({"filename": f, "status": "file_not_read", "message": "Файл не найден на диске"})
        
        if missing_files:
            logger.warning(f"Some selected files are missing for {tid}: {missing_files}")

        if not target_files:
            logger.warning(f"No valid files remaining after filtering for tender {tid}")
            job_service.complete_tender(job_id, tid, {
                "status": "error",
                "summary_notes": [f"Ошибка: ни один из выбранных файлов не найден на диске. Пропущенные файлы: {', '.join(missing_files)}"],
                "rows": [],
                "has_contract": False,
                "file_statuses": file_statuses,
                "selected_files_count": selected_count
            })
            continue

        job_service.update_tender_stage(job_id, tid, "Извлечение текста", 20)

        for filename in target_files:
            filepath = os.path.join(tender_dir, filename)
            if not os.path.isfile(filepath):
                continue
                
            try:
                text = doc_service.extract_text(filepath)
                if len(text.strip()) < 50:
                    file_statuses.append({"filename": filename, "status": "empty_file", "message": "Текст не найден или файл пуст"})
                else:
                    files_data.append({"filename": filename, "text": text})
                    file_statuses.append({"filename": filename, "status": "ok", "message": "Текст успешно извлечен"})
            except Exception as e:
                logger.error(f"Failed to extract text from {filename}: {e}")
                file_statuses.append({"filename": filename, "status": "extract_error", "message": str(e)})
                
        if not files_data:
            summary = ["Не удалось извлечь текст ни из одного документа."]
            if missing_files:
                summary.append(f"Некоторые выбранные файлы не найдены на диске: {', '.join(missing_files)}")
                
            job_service.complete_tender(job_id, tid, {
                "status": "error",
                "summary_notes": summary,
                "rows": [],
                "has_contract": False,
                "file_statuses": file_statuses,
                "selected_files_count": selected_count
            })
            continue
            
        try:
            def stage_callback(stage, progress, status="running"):
                job_service.update_tender_stage(job_id, tid, stage, progress, status)

            analysis_result = legal_service.analyze_tender(files_data, callback=stage_callback)
            
            # Добавляем инфо о пропущенных файлах в summary_notes
            final_summary = analysis_result.get('summary_notes', [])
            if missing_files:
                final_summary.insert(0, f"ВНИМАНИЕ: Следующие выбранные файлы отсутствуют в системе: {', '.join(missing_files)}")

            job_service.complete_tender(job_id, tid, {
                "status": analysis_result.get('status', 'success'),
                "summary_notes": final_summary,
                "rows": analysis_result.get('rows', []),
                "has_contract": analysis_result.get('has_contract', False),
                "classification_notes": analysis_result.get('classification_notes', []),
                "file_classifications": analysis_result.get('file_classifications', []),
                "file_statuses": file_statuses,
                "selected_files_count": selected_count
            })
        except Exception as e:
            logger.error(f"Analysis failed for tender {tid}: {e}")
            job_service.complete_tender(job_id, tid, {
                "status": "error",
                "summary_notes": [f"Ошибка ИИ-анализа: {str(e)}"],
                "rows": [],
                "has_contract": False,
                "classification_notes": [],
                "file_classifications": [],
                "file_statuses": file_statuses,
                "selected_files_count": selected_count
            })
            
    job_service.check_job_completion(job_id)

