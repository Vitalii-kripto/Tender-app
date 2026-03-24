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
                "final_report_markdown": "Ошибка: не выбрано ни одного файла для анализа. Пожалуйста, выберите хотя бы один документ.",
                "file_statuses": [],
                "selected_files_count": 0
            })
            continue

        selected_count = len(selected_files[tid])

        if not os.path.exists(tender_dir):
            logger.warning(f"No documents found for tender {tid}")
            job_service.complete_tender(job_id, tid, {
                "status": "error",
                "final_report_markdown": "Документы не найдены. Возможно, они еще скачиваются или не были загружены.",
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
                "final_report_markdown": f"Ошибка: ни один из выбранных файлов не найден на диске. Пропущенные файлы: {', '.join(missing_files)}",
                "file_statuses": file_statuses,
                "selected_files_count": selected_count
            })
            continue

        job_service.update_tender_stage(job_id, tid, "Извлечение текста", 20)

        # Метаданные для логирования
        excel_files_info = []
        all_filenames = target_files

        for filename in target_files:
            filepath = os.path.join(tender_dir, filename)
            if not os.path.isfile(filepath):
                continue
                
            try:
                text = doc_service.extract_text(filepath)
                files_data.append({"filename": filename, "text": text})
                file_statuses.append({"filename": filename, "status": "ok", "message": "Текст успешно извлечен"})
                
                # Собираем инфо об Excel
                ext = os.path.splitext(filename)[1].lower()
                if ext in ['.xlsx', '.xls']:
                    excel_files_info.append({
                        "filename": filename,
                        "char_count": len(text)
                    })
            except Exception as e:
                logger.error(f"Failed to extract text from {filename}: {e}")
                file_statuses.append({"filename": filename, "status": "extract_error", "message": str(e)})
                
        if not files_data:
            summary = "Не удалось извлечь текст ни из одного документа."
            if missing_files:
                summary += f"\nНекоторые выбранные файлы не найдены на диске: {', '.join(missing_files)}"
                
            job_service.complete_tender(job_id, tid, {
                "status": "error",
                "final_report_markdown": summary,
                "file_statuses": file_statuses,
                "selected_files_count": selected_count
            })
            continue
            
        try:
            def stage_callback(stage, progress, status="running"):
                job_service.update_tender_stage(job_id, tid, stage, progress, status)

            analysis_result = legal_service.analyze_tender(
                files_data, 
                tender_id=tid, 
                callback=stage_callback,
                metadata={
                    "all_filenames": all_filenames,
                    "excel_files_info": excel_files_info
                }
            )
            
            # Добавляем инфо о пропущенных файлах в final_report_markdown
            final_markdown = analysis_result.get('final_report_markdown', '')
            if missing_files:
                final_markdown = f"**ВНИМАНИЕ: Следующие выбранные файлы отсутствуют в системе: {', '.join(missing_files)}**\n\n" + final_markdown

            # Генерируем Word-отчет и сохраняем его на диск для логов
            report_path = "N/A"
            try:
                from docx import Document
                from backend.markdown_parser import add_markdown_to_docx
                from backend.services.eis_service import OUT_DIR
                
                doc = Document()
                add_markdown_to_docx(doc, final_markdown)
                
                tender_dir = os.path.join(OUT_DIR, str(tid))
                os.makedirs(tender_dir, exist_ok=True)
                report_path = os.path.abspath(os.path.join(tender_dir, f"report_{tid}.docx"))
                doc.save(report_path)
                logger.info(f"5. Path to Word report: {report_path}")
                logger.info(f"--- [END TENDER ANALYSIS: {tid}] ---")
            except Exception as e:
                logger.error(f"Error generating Word report for tender {tid}: {e}")

            job_service.complete_tender(job_id, tid, {
                "status": analysis_result.get('status', 'success'),
                "final_report_markdown": final_markdown,
                "file_statuses": file_statuses,
                "selected_files_count": selected_count,
                "report_path": report_path
            })
        except Exception as e:
            logger.error(f"Analysis failed for tender {tid}: {e}")
            job_service.complete_tender(job_id, tid, {
                "status": "error",
                "final_report_markdown": f"Ошибка ИИ-анализа: {str(e)}",
                "file_statuses": file_statuses,
                "selected_files_count": selected_count
            })
            
    job_service.check_job_completion(job_id)

