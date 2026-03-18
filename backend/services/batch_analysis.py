import os
import logging
from typing import List, Dict, Any
from fastapi import APIRouter, HTTPException
from .document_service import DocumentService
from .legal_analysis_service import LegalAnalysisService
from .eis_service import OUT_DIR

logger = logging.getLogger("BatchAnalysis")

def analyze_tenders_batch(tender_ids: List[str], doc_service: DocumentService, legal_service: LegalAnalysisService, selected_files: Dict[str, List[str]] = None) -> List[Dict[str, Any]]:
    results = []
    selected_files = selected_files or {}
    
    for tid in tender_ids:
        logger.info(f"Starting analysis for tender {tid}")
        tender_dir = os.path.join(OUT_DIR, tid)
        
        if not os.path.exists(tender_dir):
            logger.warning(f"No documents found for tender {tid}")
            results.append({
                "id": tid,
                "status": "error",
                "summary_notes": ["Документы не найдены. Возможно, они еще скачиваются или не были загружены."],
                "rows": [],
                "has_contract": False,
                "file_statuses": [{"filename": "N/A", "status": "file_not_read", "message": "Директория с документами не найдена"}]
            })
            continue
            
        files_data = []
        file_statuses = []
        
        # Get list of files to process
        target_files = os.listdir(tender_dir)
        if tid in selected_files and selected_files[tid]:
            target_files = [f for f in target_files if f in selected_files[tid]]
            logger.info(f"Filtering files for {tid}: {len(target_files)} selected out of {len(os.listdir(tender_dir))}")

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
            results.append({
                "id": tid,
                "status": "error",
                "summary_notes": ["Не удалось извлечь текст ни из одного документа."],
                "rows": [],
                "has_contract": False,
                "file_statuses": file_statuses
            })
            continue
            
        try:
            analysis_result = legal_service.analyze_tender(files_data)
            results.append({
                "id": tid,
                "status": analysis_result.get('status', 'success'),
                "summary_notes": analysis_result.get('summary_notes', []),
                "rows": analysis_result.get('rows', []),
                "has_contract": analysis_result.get('has_contract', False),
                "classification_notes": analysis_result.get('classification_notes', []),
                "file_statuses": file_statuses,
                "stage": analysis_result.get('stage', 'Готово'),
                "progress": analysis_result.get('progress', 100)
            })
        except Exception as e:
            logger.error(f"Analysis failed for tender {tid}: {e}")
            results.append({
                "id": tid,
                "status": "error",
                "summary_notes": [f"Ошибка ИИ-анализа: {str(e)}"],
                "rows": [],
                "has_contract": False,
                "classification_notes": [],
                "file_statuses": file_statuses,
                "stage": "Ошибка",
                "progress": 100
            })
            
    return results
