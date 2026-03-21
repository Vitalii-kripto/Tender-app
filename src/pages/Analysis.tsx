import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { MOCK_CATALOG } from './ProductCatalog';
import { findProductEquivalent, startBatchAnalysisJob, getJobStatus, getTendersFromBackend, deleteTenderFromBackend } from '../services/geminiService';
import { AnalysisResult, Tender, LegalAnalysisResult } from '../types';
import { FileText, Shield, ArrowRight, CheckCircle, AlertTriangle, Cpu, Trash2, FileDown, ScanEye, Loader2, Square, CheckSquare, Printer, ShieldAlert, Layout, ChevronDown, Table } from 'lucide-react';

const Analysis = () => {
  const navigate = useNavigate();
  const [inputText, setInputText] = useState('');
  const [loading, setLoading] = useState(false);
  const [activeTab, setActiveTab] = useState<'match' | 'batch'>('batch');
  const [statusText, setStatusText] = useState('');
  
  // Single analysis state
  const [matchResult, setMatchResult] = useState<AnalysisResult | null>(null);

  // Batch analysis state
  const [crmTenders, setCrmTenders] = useState<Tender[]>([]);
  const [selectedTenderIds, setSelectedTenderIds] = useState<Set<string>>(new Set());
  const [tenderFiles, setTenderFiles] = useState<Record<string, any[]>>({});
  const [selectedFiles, setSelectedFiles] = useState<Record<string, Set<string>>>({});
  const [batchResults, setBatchResults] = useState<Record<string, LegalAnalysisResult>>({});
  const [analysisError, setAnalysisError] = useState('');
  const [analysisStages, setAnalysisStages] = useState<Record<string, { stage: string, progress: number }>>({});

  // Filtering & Sorting state
  const [filterBlock, setFilterBlock] = useState<string>('all');
  const [filterRisk, setFilterRisk] = useState<string>('all');
  const [filterProblematic, setFilterProblematic] = useState<boolean>(false);
  const [filterProblematicFiles, setFilterProblematicFiles] = useState<boolean>(false);
  const [sortBy, setSortBy] = useState<'risk' | 'block'>('risk');

  useEffect(() => {
    const loadData = async () => {
        try {
            const tenders = await getTendersFromBackend();
            setCrmTenders(tenders);
            if (tenders.length > 0) setActiveTab('batch');
            
            // Fetch files for all tenders
            tenders.forEach(async (t) => {
                try {
                    const response = await fetch(`/api/tenders/${t.id}/files`);
                    const files = await response.json();
                    setTenderFiles(prev => ({ ...prev, [t.id]: files }));
                    // Manual selection: don't select all by default
                    setSelectedFiles(prev => ({ ...prev, [t.id]: new Set() }));
                } catch (e) {
                    console.error(`Failed to load files for tender ${t.id}`, e);
                }
            });
        } catch (e) {
            console.error("Failed to load analysis tenders", e);
        }
    };
    loadData();
  }, []);

  const handleSingleAnalyze = async () => {
    if (!inputText) return;
    setLoading(true);
    setStatusText("Анализ...");
    
    try {
      if (activeTab === 'match') {
        const result = await findProductEquivalent(inputText);
        setMatchResult(result);
      }
    } catch (e) {
      console.error(e);
    } finally {
      setLoading(false);
    }
  };

  const runBatchAnalysis = async () => {
    if (selectedTenderIds.size === 0) {
        setAnalysisError('Выберите хотя бы один тендер для анализа.');
        return;
    }
    setAnalysisError('');
    setLoading(true);
    setStatusText("Запуск пакетного анализа...");
    
    try {
        const idsArray = Array.from(selectedTenderIds);
        
        // Validation: check if files are selected for each tender
        const tendersWithoutFiles = [];
        for (const tid of idsArray) {
            if (!selectedFiles[tid] || selectedFiles[tid].size === 0) {
                const tender = crmTenders.find(t => t.id === tid);
                tendersWithoutFiles.push(tender ? tender.eis_number || tid : tid);
            }
        }
        
        if (tendersWithoutFiles.length > 0) {
            setAnalysisError(`Выберите файлы для следующих тендеров: ${tendersWithoutFiles.join(', ')}`);
            setLoading(false);
            return;
        }

        // Prepare selected files mapping
        const filesMapping: Record<string, string[]> = {};
        idsArray.forEach(id => {
            if (selectedFiles[id]) {
                filesMapping[id] = Array.from(selectedFiles[id]);
            }
        });

        // Initialize stages
        const initialStages: Record<string, { stage: string, progress: number }> = {};
        idsArray.forEach(id => {
            initialStages[id] = { stage: 'Ожидание', progress: 0 };
        });
        setAnalysisStages(initialStages);

        // Start analysis
        const jobId = await startBatchAnalysisJob(idsArray, filesMapping);
        
        // Polling
        const pollInterval = setInterval(async () => {
            try {
                const job = await getJobStatus(jobId);
                
                const newStages: Record<string, { stage: string, progress: number }> = {};
                for (const tid in job.tenders) {
                    newStages[tid] = {
                        stage: job.tenders[tid].stage,
                        progress: job.tenders[tid].progress
                    };
                }
                setAnalysisStages(prev => ({ ...prev, ...newStages }));
                
                if (job.status === 'completed') {
                    clearInterval(pollInterval);
                    const newResults: Record<string, LegalAnalysisResult> = { ...batchResults };
                    for (const tid in job.tenders) {
                        newResults[tid] = {
                            id: tid,
                            ...job.tenders[tid]
                        };
                    }
                    setBatchResults(newResults);
                    setLoading(false);
                    setStatusText("");
                }
            } catch (e) {
                console.error("Error polling job status", e);
                clearInterval(pollInterval);
                setAnalysisError('Произошла ошибка при получении статуса анализа.');
                setLoading(false);
                setStatusText("");
            }
        }, 1500);
        
    } catch (e) {
        console.error("Batch analysis failed", e);
        setAnalysisError('Произошла ошибка при запуске анализа.');
        setLoading(false);
        setStatusText("");
    }
  };

  const removeTender = async (id: string) => {
    if(confirm("Убрать этот тендер из CRM?")) {
        const updated = crmTenders.filter(t => t.id !== id);
        setCrmTenders(updated);
        
        const newSelected = new Set(selectedTenderIds);
        newSelected.delete(id);
        setSelectedTenderIds(newSelected);
        
        await deleteTenderFromBackend(id);
        if (updated.length === 0) setActiveTab('match');
    }
  };

  const toggleSelection = (id: string) => {
      if (loading) return;
      const newSelected = new Set(selectedTenderIds);
      if (newSelected.has(id)) {
          newSelected.delete(id);
      } else {
          newSelected.add(id);
      }
      setSelectedTenderIds(newSelected);
      setAnalysisError('');
  };

  const selectAll = () => {
      if (loading) return;
      setSelectedTenderIds(new Set(crmTenders.map(t => t.id)));
      setAnalysisError('');
  };

  const deselectAll = () => {
      if (loading) return;
      setSelectedTenderIds(new Set());
      setAnalysisError('');
  };

  const toggleFileSelection = (tenderId: string, fileName: string) => {
    if (loading) return;
    const newSelected = new Set(selectedFiles[tenderId] || new Set());
    if (newSelected.has(fileName)) {
        newSelected.delete(fileName);
    } else {
        newSelected.add(fileName);
    }
    setSelectedFiles(prev => ({ ...prev, [tenderId]: newSelected }));
  };

  const selectAllFiles = (tenderId: string) => {
    if (loading) return;
    const files = tenderFiles[tenderId] || [];
    setSelectedFiles(prev => ({ ...prev, [tenderId]: new Set(files.map(f => f.name)) }));
  };

  const deselectAllFiles = (tenderId: string) => {
    if (loading) return;
    setSelectedFiles(prev => ({ ...prev, [tenderId]: new Set() }));
  };

  const exportToExcel = async (results: LegalAnalysisResult[]) => {
    try {
        // Attach tender title and description to results
        const resultsWithMeta = results.map(result => {
            const tender = crmTenders.find(t => t.id === result.id);
            return {
                ...result,
                description: tender ? `${tender.title}\n${tender.description}` : 'Нет описания'
            };
        });

        const response = await fetch('/api/ai/export-risks-excel', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ results: resultsWithMeta })
        });
        if (!response.ok) throw new Error('Export failed');
        
        const blob = await response.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `tender_risks_report_${new Date().toISOString().split('T')[0]}.xlsx`;
        document.body.appendChild(a);
        a.click();
        window.URL.revokeObjectURL(url);
    } catch (e) {
        console.error("Excel export failed", e);
        alert("Не удалось экспортировать в Excel.");
    }
  };

  const getFilteredRows = (rows: any[]) => {
    return rows;
  };

  const getUniqueBlocks = (rows: any[]) => {
    return [];
  };

  const exportToExcelFiltered = async (tenderId: string) => {
    const result = batchResults[tenderId];
    if (!result) return;
    
    const filteredRows = getFilteredRows(result.rows);
    const filteredResult = { ...result, rows: filteredRows };
    exportToExcel([filteredResult]);
  };

  const getRecommendedProduct = (id: string) => {
    return MOCK_CATALOG.find(p => p.id === id);
  };

  const formatSpecKey = (key: string) => {
    const map: Record<string, string> = {
      thickness_mm: 'Толщина (мм)',
      weight_kg_m2: 'Вес (кг/м²)',
      flexibility_temp_c: 'Гибкость на брусе (°C)',
      tensile_strength_n: 'Разрывная сила (Н)'
    };
    return map[key] || key;
  };

  const exportToCSV = (result: LegalAnalysisResult, tenderNumber: string) => {
    const headers = ['Блок', 'Риск', 'Уровень риска', 'Действие поставщика', 'Документ', 'Ссылка', 'Обоснование'];
    const rows = result.rows.map(r => [
      `"${r.block.replace(/"/g, '""')}"`,
      `"${r.finding.replace(/"/g, '""')}"`,
      r.risk_level,
      `"${r.supplier_action.replace(/"/g, '""')}"`,
      `"${r.source_document.replace(/"/g, '""')}"`,
      `"${r.source_reference.replace(/"/g, '""')}"`,
      `"${(r.legal_basis || '').replace(/"/g, '""')}"`
    ]);
    const csvContent = [headers.join(','), ...rows.map(r => r.join(','))].join('\n');
    const blob = new Blob([new Uint8Array([0xEF, 0xBB, 0xBF]), csvContent], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.setAttribute('download', `risks_report_${tenderNumber}.csv`);
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  const exportToPDF = () => {
    window.print();
  };

  const formatCurrency = (amount: number | string) => {
      const num = typeof amount === 'string' ? parseFloat(amount) : amount;
      if (isNaN(num)) return amount;
      return new Intl.NumberFormat('ru-RU', { style: 'currency', currency: 'RUB' }).format(num);
  };

  return (
    <div className="p-6 max-w-6xl mx-auto pb-20">
      <div className="mb-8">
        <h2 className="text-2xl font-bold text-slate-900 flex items-center gap-2">
          <Cpu className="text-blue-600" />
          ИИ Юрист
        </h2>
        <p className="text-slate-500 text-sm mt-1">
          Анализ тендерной документации на юридические риски.
        </p>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
        {/* Input/Selection Section */}
        <div className="bg-white rounded-xl border border-slate-200 shadow-sm flex flex-col h-[700px] overflow-hidden">
          <div className="border-b border-slate-200 flex bg-slate-50">
            <button 
              onClick={() => setActiveTab('batch')}
              className={`flex-1 py-3 text-sm font-medium flex items-center justify-center gap-2 transition-colors ${activeTab === 'batch' ? 'bg-white text-blue-600 border-t-2 border-t-blue-600 shadow-sm' : 'text-slate-500 hover:text-slate-700'}`}
            >
              <div className="relative">
                <FileText size={16} />
                {crmTenders.length > 0 && <span className="absolute -top-1 -right-2 w-3 h-3 bg-red-500 rounded-full border border-white"></span>}
              </div>
              Тендеры ({crmTenders.length})
            </button>
            <button 
              onClick={() => setActiveTab('match')}
              className={`flex-1 py-3 text-sm font-medium flex items-center justify-center gap-2 transition-colors ${activeTab === 'match' ? 'bg-white text-blue-600 border-t-2 border-t-blue-600 shadow-sm' : 'text-slate-500 hover:text-slate-700'}`}
            >
              <ScanEye size={16} />
              Ручной ввод
            </button>
          </div>
          
          {activeTab === 'batch' ? (
            <div className="flex-1 flex flex-col p-4 bg-slate-50/50">
                {crmTenders.length === 0 ? (
                    <div className="flex-1 flex flex-col items-center justify-center text-center p-6">
                        <FileText size={48} className="text-slate-300 mb-4" />
                        <h3 className="text-lg font-medium text-slate-700">Нет тендеров в CRM</h3>
                        <p className="text-sm text-slate-500 mb-6">Перейдите в поиск и добавьте закупки в работу.</p>
                        <button onClick={() => navigate('/tenders')} className="px-4 py-2 bg-blue-100 text-blue-700 rounded-lg hover:bg-blue-200 transition-colors text-sm font-medium">
                            Перейти к поиску
                        </button>
                    </div>
                ) : (
                    <>
                        <div className="flex justify-between items-center mb-3 px-1">
                            <div className="flex gap-3">
                                <button onClick={selectAll} disabled={loading} className="text-xs text-blue-600 hover:underline font-medium disabled:opacity-50">Выбрать все</button>
                                <button onClick={deselectAll} disabled={loading} className="text-xs text-slate-500 hover:underline font-medium disabled:opacity-50">Снять все</button>
                            </div>
                            <div className="text-xs font-medium text-slate-600">
                                Всего: {crmTenders.length} | Выбрано: <span className={selectedTenderIds.size > 0 ? "text-blue-600 font-bold" : ""}>{selectedTenderIds.size}</span>
                            </div>
                        </div>
                        
                        <div className="flex-1 overflow-y-auto space-y-3 pr-2 mb-4 custom-scrollbar">
                            {crmTenders.map(tender => {
                                const isSelected = selectedTenderIds.has(tender.id);
                                return (
                                <div key={tender.id} className={`bg-white p-3 rounded-lg border shadow-sm flex gap-3 transition-colors ${isSelected ? 'border-blue-400 ring-1 ring-blue-400/20' : 'border-slate-200 hover:border-blue-300'}`}>
                                    <button 
                                        onClick={() => toggleSelection(tender.id)}
                                        disabled={loading}
                                        className="mt-1 text-slate-400 hover:text-blue-600 disabled:opacity-50"
                                    >
                                        {isSelected ? <CheckSquare size={20} className="text-blue-600" /> : <Square size={20} />}
                                    </button>
                                    <div className="flex-1 min-w-0 cursor-pointer" onClick={() => toggleSelection(tender.id)}>
                                        <div className="flex items-center gap-2 mb-1">
                                            <span className="text-[10px] font-mono bg-slate-100 px-1.5 py-0.5 rounded text-slate-500 border border-slate-200">#{tender.eis_number}</span>
                                            <span className="text-xs font-black text-blue-700">{formatCurrency(tender.initial_price)}</span>
                                            {batchResults[tender.id] && (
                                                <span className={`ml-auto text-[9px] font-black px-1.5 py-0.5 rounded uppercase tracking-tighter ${
                                                    batchResults[tender.id].rows.some(r => r.risk_level === 'High') ? 'bg-red-100 text-red-600' : 
                                                    batchResults[tender.id].rows.some(r => r.risk_level === 'Medium') ? 'bg-amber-100 text-amber-600' : 
                                                    'bg-emerald-100 text-emerald-600'
                                                }`}>
                                                    {batchResults[tender.id].rows.some(r => r.risk_level === 'High') ? 'High Risk' : 'Analyzed'}
                                                </span>
                                            )}
                                        </div>
                                        <h4 className="text-sm font-black text-slate-800 line-clamp-1 mb-1 group-hover:text-blue-600 transition-colors">{tender.title}</h4>
                                        <p className="text-[11px] text-slate-500 line-clamp-2 mb-2 leading-tight">{tender.description}</p>
                                        
                                        {/* File List for Selection */}
                                        {tenderFiles[tender.id] && tenderFiles[tender.id].length > 0 && (
                                            <div className="mt-2 pt-2 border-t border-slate-100">
                                                <div className="flex justify-between items-center mb-1">
                                                    <span className="text-[10px] font-bold text-slate-400 uppercase">Документы ({tenderFiles[tender.id].length})</span>
                                                    <div className="flex gap-2">
                                                        <button onClick={(e) => { e.stopPropagation(); selectAllFiles(tender.id); }} className="text-[10px] text-blue-500 hover:underline">Все</button>
                                                        <button onClick={(e) => { e.stopPropagation(); deselectAllFiles(tender.id); }} className="text-[10px] text-slate-400 hover:underline">Ничего</button>
                                                    </div>
                                                </div>
                                                <div className="max-h-24 overflow-y-auto space-y-1 pr-1 custom-scrollbar">
                                                    {tenderFiles[tender.id].map(file => (
                                                        <div 
                                                            key={file.name} 
                                                            className="flex items-center gap-2 text-[11px] text-slate-600 hover:bg-slate-50 p-0.5 rounded"
                                                            onClick={(e) => { e.stopPropagation(); toggleFileSelection(tender.id, file.name); }}
                                                        >
                                                            {selectedFiles[tender.id]?.has(file.name) ? (
                                                                <CheckSquare size={12} className="text-blue-500" />
                                                            ) : (
                                                                <Square size={12} className="text-slate-300" />
                                                            )}
                                                            <span className="truncate flex-1">{file.name}</span>
                                                            <span className="text-[9px] text-slate-400">{(file.size / 1024).toFixed(0)} KB</span>
                                                        </div>
                                                    ))}
                                                </div>
                                            </div>
                                        )}
                                    </div>
                                    <button 
                                        onClick={(e) => { e.stopPropagation(); removeTender(tender.id); }}
                                        disabled={loading}
                                        className="text-slate-400 hover:text-red-500 p-1 self-start disabled:opacity-50"
                                        title="Удалить из CRM"
                                    >
                                        <Trash2 size={16} />
                                    </button>
                                </div>
                            )})}
                        </div>
                        
                        {analysisError && (
                            <div className="mb-3 p-2 bg-red-50 border border-red-200 text-red-600 text-sm rounded-lg flex items-center gap-2">
                                <AlertTriangle size={16} /> {analysisError}
                            </div>
                        )}
                        
                        <div className="border-t border-slate-200 pt-4">
                            <button 
                                onClick={runBatchAnalysis}
                                disabled={loading || selectedTenderIds.size === 0}
                                className={`w-full flex items-center justify-center gap-2 py-3 rounded-xl text-white font-bold transition-all shadow-md ${loading || selectedTenderIds.size === 0 ? 'bg-slate-400 cursor-not-allowed' : 'bg-gradient-to-r from-blue-600 to-indigo-600 hover:shadow-lg hover:scale-[1.01]'}`}
                            >
                                {loading ? (
                                    <>
                                        <Loader2 size={20} className="animate-spin" />
                                        Анализ...
                                    </>
                                ) : (
                                    <>
                                        <Cpu size={20} />
                                        Анализировать выбранные тендеры
                                    </>
                                )}
                            </button>
                        </div>
                    </>
                )}
            </div>
          ) : (
             <div className="p-4 flex-1 flex flex-col">
                <textarea
                className="w-full flex-1 p-4 bg-slate-50 border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 resize-none font-mono text-sm"
                placeholder="Вставьте тех. характеристики или текст контракта вручную..."
                value={inputText}
                onChange={(e) => setInputText(e.target.value)}
                />
                <div className="flex items-center gap-4 mt-4">
                     <label className="flex items-center gap-2 text-sm text-slate-600">
                        <input type="radio" name="mode" checked={activeTab === 'match'} onChange={() => setActiveTab('match')} />
                        Подбор товара
                     </label>
                     <button 
                        onClick={handleSingleAnalyze}
                        disabled={loading || !inputText}
                        className={`ml-auto flex items-center gap-2 px-6 py-2.5 rounded-lg text-white font-medium transition-all ${loading || !inputText ? 'bg-slate-300 cursor-not-allowed' : 'bg-blue-600 hover:bg-blue-700 shadow-md'}`}
                    >
                        {loading ? 'Анализ...' : 'Запустить'}
                    </button>
                </div>
            </div>
          )}
        </div>

        {/* Results Section */}
        <div className="space-y-6 h-[700px] overflow-y-auto pr-2 custom-scrollbar">
          
          {/* 1. Loading State */}
          {loading && (
            <div className="h-full flex flex-col items-center justify-center text-slate-400 space-y-6">
              <div className="relative">
                <div className="w-20 h-20 border-4 border-slate-100 border-t-blue-600 rounded-full animate-spin"></div>
                <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2">
                    <Cpu size={32} className="text-blue-600" />
                </div>
              </div>
              <div className="text-center">
                <p className="font-black text-slate-700 text-lg mb-1">{statusText || "Анализ запущен"}</p>
                <p className="text-xs text-slate-400 uppercase tracking-widest font-bold">ИИ обрабатывает документы...</p>
              </div>
              
              <div className="w-64 space-y-3">
                 {Object.entries(analysisStages).map(([tid, stage]) => (
                    <div key={tid} className="bg-white p-3 rounded-lg border border-slate-200 shadow-sm">
                        <div className="flex justify-between text-[10px] font-black uppercase mb-1">
                            <span className="truncate max-w-[100px]">Тендер {tid}</span>
                            <span className="text-blue-600">{stage.progress}%</span>
                        </div>
                        <div className="w-full bg-slate-100 h-1.5 rounded-full overflow-hidden">
                            <div className="bg-blue-600 h-full transition-all duration-500" style={{ width: `${stage.progress}%` }}></div>
                        </div>
                        <div className="text-[9px] text-slate-500 mt-1 font-bold">{stage.stage}</div>
                    </div>
                 ))}
              </div>
            </div>
          )}

          {/* 2. Batch Results State */}
          {!loading && activeTab === 'batch' && Object.keys(batchResults).length > 0 && (
             <div className="space-y-8">
                <div className="flex justify-between items-center bg-white p-4 rounded-xl border border-slate-200 shadow-sm">
                    <h2 className="text-lg font-bold text-slate-800">Результаты анализа ({Object.keys(batchResults).length})</h2>
                    <button 
                        onClick={() => exportToExcel(Object.values(batchResults))}
                        className="flex items-center gap-2 px-4 py-2 bg-emerald-50 text-emerald-700 hover:bg-emerald-100 border border-emerald-200 rounded-lg text-sm font-bold transition-colors shadow-sm"
                    >
                        <FileDown size={16} />
                        Экспорт всех в Excel
                    </button>
                </div>
                {Object.values(batchResults).map(result => {
                    const tender = crmTenders.find(t => t.id === result.id);
                    if (!tender) return null;

                    return (
                        <div key={result.id} className="animate-in slide-in-from-bottom-4 fade-in duration-500">
                            <div className="bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden mb-8">
                                {/* Tender Header Info */}
                                <div className="p-5 border-b border-slate-100 bg-slate-50/50">
                                    <div className="flex flex-wrap items-center justify-between gap-4 mb-3">
                                        <div className="flex items-center gap-3">
                                            <span className="bg-blue-600 text-white text-xs font-mono px-2.5 py-1 rounded font-bold shadow-sm">#{tender.eis_number}</span>
                                            <h3 className="font-bold text-slate-900 text-xl">{tender.title}</h3>
                                        </div>
                                        <div className="text-xl font-black text-slate-900 bg-white px-4 py-1 rounded-lg border border-slate-200 shadow-sm">
                                            {formatCurrency(tender.initial_price)}
                                        </div>
                                    </div>
                                    <p className="text-sm text-slate-600 mb-4 leading-relaxed">{tender.description}</p>
                                    
                                    <div className="flex flex-wrap gap-3">
                                        <span className={`flex items-center gap-1.5 px-3 py-1.5 rounded-full text-[10px] font-bold uppercase tracking-wider shadow-sm ${result.has_contract ? 'bg-emerald-100 text-emerald-700 border border-emerald-200' : 'bg-red-100 text-red-700 border border-red-200'}`}>
                                            {result.has_contract ? <CheckCircle size={14}/> : <AlertTriangle size={14}/>}
                                            Проект договора {result.has_contract ? 'найден' : 'не найден'}
                                        </span>
                                        <span className="flex items-center gap-1.5 px-3 py-1.5 rounded-full text-[10px] font-bold uppercase tracking-wider bg-blue-50 text-blue-700 border border-blue-200 shadow-sm">
                                            <FileText size={14}/>
                                            Файлов выбрано пользователем: {result.selected_files_count !== undefined ? result.selected_files_count : result.file_statuses.length}
                                        </span>
                                        <span className="flex items-center gap-1.5 px-3 py-1.5 rounded-full text-[10px] font-bold uppercase tracking-wider bg-slate-100 text-slate-600 border border-slate-200 shadow-sm">
                                            <Shield size={14}/>
                                            Успешно: {result.file_statuses.filter(f => f.status === 'ok').length} / Ошибок: {result.file_statuses.filter(f => f.status !== 'ok').length}
                                        </span>
                                    </div>
                                </div>

                                {/* Risk Counters - Big & Bold */}
                                <div className="grid grid-cols-3 divide-x divide-slate-100 border-b border-slate-100 bg-white">
                                    <div className="p-4 text-center hover:bg-red-50/30 transition-colors">
                                        <div className="text-3xl font-black text-red-600">{result.rows.filter(r => r.risk_level === 'High').length}</div>
                                        <div className="text-[10px] text-slate-400 uppercase font-black tracking-widest mt-1">Высокий риск</div>
                                    </div>
                                    <div className="p-4 text-center hover:bg-amber-50/30 transition-colors">
                                        <div className="text-3xl font-black text-amber-600">{result.rows.filter(r => r.risk_level === 'Medium').length}</div>
                                        <div className="text-[10px] text-slate-400 uppercase font-black tracking-widest mt-1">Средний риск</div>
                                    </div>
                                    <div className="p-4 text-center hover:bg-blue-50/30 transition-colors">
                                        <div className="text-3xl font-black text-blue-600">{result.rows.filter(r => r.risk_level === 'Low').length}</div>
                                        <div className="text-[10px] text-slate-400 uppercase font-black tracking-widest mt-1">Низкий риск</div>
                                    </div>
                                </div>

                                {/* File Statuses Block */}
                                <div className="px-5 py-4 bg-slate-50 border-b border-slate-100 grid grid-cols-1 md:grid-cols-2 gap-4">
                                    <div>
                                        <div className="flex justify-between items-center mb-2">
                                            <h4 className="text-[10px] font-black text-slate-400 uppercase tracking-widest">Статус документов</h4>
                                            <label className="flex items-center gap-1.5 text-[10px] text-slate-500 cursor-pointer hover:text-slate-700">
                                                <input type="checkbox" checked={filterProblematicFiles} onChange={(e) => setFilterProblematicFiles(e.target.checked)} className="rounded border-slate-300 text-blue-600 focus:ring-blue-500" />
                                                Только проблемные
                                            </label>
                                        </div>
                                        <div className="flex flex-col gap-1.5 max-h-48 overflow-y-auto custom-scrollbar pr-1">
                                            {result.file_statuses
                                                .filter(fs => !filterProblematicFiles || fs.status !== 'ok')
                                                .sort((a, b) => (a.status === 'ok' ? 1 : -1) - (b.status === 'ok' ? 1 : -1))
                                                .map((fs, i) => (
                                                <div key={i} className={`flex items-start gap-2 px-2.5 py-1.5 rounded-lg border text-[11px] font-medium transition-colors ${fs.status === 'ok' ? 'bg-white border-slate-200 text-slate-600' : 'bg-red-50 border-red-200 text-red-700'}`}>
                                                    {fs.status === 'ok' ? <CheckCircle size={14} className="text-emerald-500 mt-0.5 shrink-0" /> : <AlertTriangle size={14} className="text-red-500 mt-0.5 shrink-0" />}
                                                    <div className="flex flex-col min-w-0">
                                                        <span className="truncate font-bold">{fs.filename}</span>
                                                        {fs.status !== 'ok' && <span className="text-[10px] text-red-600/80 mt-0.5 leading-tight">{fs.message}</span>}
                                                    </div>
                                                </div>
                                            ))}
                                        </div>
                                    </div>
                                    <div className="space-y-4">
                                        {result.file_classifications && result.file_classifications.length > 0 && (
                                            <div>
                                                <h4 className="text-[10px] font-black text-slate-400 uppercase tracking-widest mb-2">Классификация файлов</h4>
                                                <div className="space-y-3 max-h-48 overflow-y-auto custom-scrollbar pr-1">
                                                    {['contract', 'procurement', 'mixed', 'unclassified', 'unclassified_due_to_no_text'].map((category) => {
                                                        const filesInCategory = result.file_classifications!.filter(fc => fc.category === category);
                                                        if (filesInCategory.length === 0) return null;
                                                        
                                                        let categoryTitle = '';
                                                        let categoryColor = '';
                                                        if (category === 'contract') { categoryTitle = 'Договорные документы'; categoryColor = 'text-emerald-600 bg-emerald-50 border-emerald-100'; }
                                                        else if (category === 'procurement') { categoryTitle = 'Закупочная документация'; categoryColor = 'text-blue-600 bg-blue-50 border-blue-100'; }
                                                        else if (category === 'mixed') { categoryTitle = 'Смешанные документы'; categoryColor = 'text-amber-600 bg-amber-50 border-amber-100'; }
                                                        else if (category === 'unclassified') { categoryTitle = 'Не классифицировано'; categoryColor = 'text-slate-600 bg-slate-50 border-slate-200'; }
                                                        else if (category === 'unclassified_due_to_no_text') { categoryTitle = 'Не классифицировано (нет текста)'; categoryColor = 'text-red-600 bg-red-50 border-red-100'; }

                                                        return (
                                                            <div key={category} className={`p-2 rounded-lg border ${categoryColor}`}>
                                                                <div className="text-[10px] font-bold uppercase tracking-wider mb-1.5 opacity-80">{categoryTitle}</div>
                                                                <div className="space-y-1.5">
                                                                    {filesInCategory.map((fc, i) => (
                                                                        <div key={i} className="flex flex-col gap-0.5">
                                                                            <span className="text-[11px] font-semibold truncate">{fc.filename}</span>
                                                                            <span className="text-[10px] opacity-75 leading-tight">{fc.classification_reason}</span>
                                                                        </div>
                                                                    ))}
                                                                </div>
                                                            </div>
                                                        );
                                                    })}
                                                </div>
                                            </div>
                                        )}
                                    </div>
                                </div>

                                {result.summary_notes && result.summary_notes.length > 0 && (
                                    <div className="px-5 py-4 bg-blue-50/40 border-b border-blue-100">
                                        <div className="flex items-center gap-2 mb-3">
                                            <div className="p-1 bg-blue-600 rounded text-white">
                                                <ScanEye size={14} />
                                            </div>
                                            <h4 className="text-xs font-black text-blue-900 uppercase tracking-widest">Сводка юридического анализа</h4>
                                        </div>
                                        <div className="grid grid-cols-1 md:grid-cols-2 gap-x-6 gap-y-2">
                                            {result.summary_notes.slice(0, 12).map((note, i) => (
                                                <div key={i} className="flex items-start gap-2.5 text-[11px] text-blue-800 leading-relaxed font-medium bg-white/50 p-2 rounded-lg border border-blue-100/50">
                                                    <div className="w-1.5 h-1.5 rounded-full bg-blue-500 mt-1.5 shrink-0 shadow-sm"></div>
                                                    {note.length > 200 ? note.substring(0, 200) + '...' : note}
                                                </div>
                                            ))}
                                        </div>
                                    </div>
                                )}

                                {/* Filtering & Sorting UI */}
                                <div className="px-5 py-3 bg-white border-b border-slate-100 flex flex-wrap items-center gap-4">
                                </div>

                                 {/* 1. Main Legal Report (Markdown) - PRIMARY OUTPUT */}
                                 {result.status === 'success' && result.final_report_markdown && (
                                     <div className="px-6 py-10 bg-white border-b border-slate-100">
                                         <div className="flex items-center justify-between mb-8">
                                             <div className="flex items-center gap-3">
                                                 <div className="p-2 bg-indigo-600 rounded-xl text-white shadow-lg shadow-indigo-200">
                                                     <FileText size={20} />
                                                 </div>
                                                 <h4 className="text-2xl font-black text-slate-900 tracking-tight">Юридический отчет по тендеру</h4>
                                             </div>
                                             <div className="flex gap-2">
                                                 <button onClick={exportToPDF} className="flex items-center gap-2 px-4 py-2 bg-white border border-slate-200 rounded-lg text-sm font-bold text-slate-700 hover:bg-slate-50 transition-all shadow-sm">
                                                     <Printer size={16} /> Печать / PDF
                                                 </button>
                                             </div>
                                         </div>
                                         <div className="markdown-body prose prose-slate max-w-none prose-headings:text-slate-900 prose-strong:text-slate-900 prose-table:border prose-table:border-slate-200 prose-th:bg-slate-50 prose-th:px-4 prose-th:py-2 prose-td:px-4 prose-td:py-2">
                                             <ReactMarkdown remarkPlugins={[remarkGfm]}>
                                                 {result.final_report_markdown}
                                             </ReactMarkdown>
                                         </div>
                                     </div>
                                 )}

                                 {/* 2. Secondary Data Layers */}
                                 <div className="bg-slate-50/50 p-6 space-y-6">
                                     <div className="flex items-center gap-3 px-2">
                                         <h5 className="text-[10px] font-black text-slate-400 uppercase tracking-widest">Дополнительные слои данных</h5>
                                         <div className="h-px flex-1 bg-slate-200"></div>
                                     </div>

                                     <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                                         {/* Risk Summary Table */}
                                         <div className="bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden">
                                             <div className="px-4 py-3 bg-slate-50/50 border-b border-slate-100 flex justify-between items-center">
                                                 <h6 className="text-xs font-black text-slate-700 uppercase tracking-wider flex items-center gap-2">
                                                     <ShieldAlert size={14} className="text-amber-500" />
                                                     Краткая таблица рисков
                                                 </h6>
                                             </div>
                                             <div className="overflow-x-auto">
                                                 <table className="w-full text-left text-[11px] border-collapse">
                                                     <thead className="bg-slate-50/30 text-slate-500 uppercase font-black tracking-widest border-b border-slate-100">
                                                         <tr>
                                                             <th className="px-4 py-2">Блок</th>
                                                             <th className="px-4 py-2">Риск</th>
                                                             <th className="px-4 py-2">Ур.</th>
                                                         </tr>
                                                     </thead>
                                                     <tbody className="divide-y divide-slate-50">
                                                         {result.rows.slice(0, 8).map((row, idx) => (
                                                             <tr key={idx} className="hover:bg-slate-50/50 transition-colors">
                                                                 <td className="px-4 py-2 font-bold text-slate-600 truncate max-w-[100px]">{row.block}</td>
                                                                 <td className="px-4 py-2 text-slate-900 line-clamp-1">{row.finding}</td>
                                                                 <td className="px-4 py-2">
                                                                     <div className={`w-2 h-2 rounded-full ${
                                                                         row.risk_level === 'High' ? 'bg-red-500' : 
                                                                         row.risk_level === 'Medium' ? 'bg-amber-500' : 
                                                                         'bg-emerald-500'
                                                                     }`}></div>
                                                                 </td>
                                                             </tr>
                                                         ))}
                                                     </tbody>
                                                 </table>
                                             </div>
                                         </div>

                                         {/* Service Sections */}
                                         <div className="bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden">
                                             <div className="px-4 py-3 bg-slate-50/50 border-b border-slate-100">
                                                 <h6 className="text-xs font-black text-slate-700 uppercase tracking-wider flex items-center gap-2">
                                                     <Layout size={14} className="text-indigo-500" />
                                                     Служебные секции
                                                 </h6>
                                             </div>
                                             <div className="p-3 space-y-2 max-h-[250px] overflow-y-auto custom-scrollbar">
                                                 {result.final_report_sections?.map((section, idx) => (
                                                     <details key={idx} className="group border border-slate-100 rounded-lg overflow-hidden">
                                                         <summary className="px-3 py-2 text-[11px] font-bold text-slate-600 bg-slate-50/50 cursor-pointer hover:bg-slate-100 transition-colors flex justify-between items-center group-open:bg-indigo-50 group-open:text-indigo-700">
                                                             {section.section_title}
                                                             <ChevronDown size={12} className="transition-transform group-open:rotate-180" />
                                                         </summary>
                                                         <div className="p-3 text-[11px] text-slate-500 whitespace-pre-wrap bg-white leading-relaxed">
                                                             {section.content}
                                                         </div>
                                                     </details>
                                                 ))}
                                             </div>
                                         </div>
                                     </div>

                                     {/* Full Technical Table (Collapsible) */}
                                     <div className="bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden">
                                         <details className="group">
                                             <summary className="px-5 py-4 cursor-pointer hover:bg-slate-50 transition-colors flex justify-between items-center">
                                                 <div className="flex items-center gap-3">
                                                     <Table size={18} className="text-slate-400" />
                                                     <span className="text-sm font-black text-slate-700 uppercase tracking-wider">Полная техническая таблица (JSON-слой)</span>
                                                     <span className="px-2 py-0.5 bg-slate-100 text-slate-500 text-[10px] font-black rounded-full">
                                                         {result.rows.length} записей
                                                     </span>
                                                 </div>
                                                 <ChevronDown size={18} className="text-slate-400 transition-transform group-open:rotate-180" />
                                             </summary>
                                             <div className="border-t border-slate-100">
                                                 <div className="overflow-x-auto">
                                                     <table className="w-full text-left text-xs border-collapse">
                                                         <thead className="bg-slate-50 text-slate-500 uppercase font-black tracking-widest border-b border-slate-200">
                                                             <tr>
                                                                 <th className="px-5 py-3">Блок</th>
                                                                 <th className="px-5 py-3">Находка</th>
                                                                 <th className="px-5 py-3">Риск</th>
                                                                 <th className="px-5 py-3">Действие</th>
                                                                 <th className="px-5 py-3">Источник</th>
                                                             </tr>
                                                         </thead>
                                                         <tbody className="divide-y divide-slate-100">
                                                             {getFilteredRows(result.rows).map((row, idx) => (
                                                                 <tr key={idx} className="transition-colors hover:bg-slate-50/30">
                                                                     <td className="px-5 py-3 align-top font-bold text-slate-900">{row.block}</td>
                                                                     <td className="px-5 py-3 align-top text-slate-700 leading-relaxed">{row.finding}</td>
                                                                     <td className="px-5 py-3 align-top">
                                                                         <span className={`text-[9px] font-black px-1.5 py-0.5 rounded uppercase ${
                                                                             row.risk_level === 'High' ? 'bg-red-100 text-red-600' : 
                                                                             row.risk_level === 'Medium' ? 'bg-amber-100 text-amber-600' : 
                                                                             'bg-emerald-100 text-emerald-600'
                                                                         }`}>
                                                                             {row.risk_level}
                                                                         </span>
                                                                     </td>
                                                                     <td className="px-5 py-3 align-top text-slate-600 leading-relaxed">{row.supplier_action}</td>
                                                                     <td className="px-5 py-3 align-top text-slate-500 text-[10px]">
                                                                         <div className="font-bold">{row.source_document}</div>
                                                                         <div>{row.source_reference}</div>
                                                                     </td>
                                                                 </tr>
                                                             ))}
                                                         </tbody>
                                                     </table>
                                                 </div>
                                             </div>
                                         </details>
                                     </div>
                                 </div>

                                 {result.status === 'success' && result.rows.length === 0 && !result.final_report_markdown && (
                                    <div className="p-10 text-center text-slate-400 bg-white">
                                        <Shield size={48} className="mx-auto mb-4 opacity-20" />
                                        <p className="text-sm font-medium">Документация выглядит стандартной. Критических условий не найдено.</p>
                                    </div>
                                 )}

                                {result.status === 'success' && (
                                    <div className="bg-slate-50 p-4 border-t border-slate-100 flex justify-end gap-6">
                                        <button onClick={() => exportToExcelFiltered(result.id)} className="text-[10px] text-emerald-600 font-black hover:underline flex items-center gap-1.5 uppercase tracking-wider">
                                            <FileDown size={14} /> Excel (фильтры)
                                        </button>
                                        <button onClick={() => exportToExcel([result])} className="text-[10px] text-emerald-600 font-black hover:underline flex items-center gap-1.5 uppercase tracking-wider">
                                            <FileDown size={14} /> Полный Excel
                                        </button>
                                        <button onClick={() => exportToCSV(result, tender.eis_number)} className="text-[10px] text-blue-600 font-black hover:underline flex items-center gap-1.5 uppercase tracking-wider">
                                            <FileDown size={14} /> CSV
                                        </button>
                                        <button onClick={exportToPDF} className="text-[10px] text-slate-600 font-black hover:underline flex items-center gap-1.5 uppercase tracking-wider">
                                            <Printer size={14} /> Печать / PDF
                                        </button>
                                    </div>
                                )}
                            </div>
                        </div>
                    );
                })}
             </div>
          )}

          {/* 3. Single Result State (Match) */}
          {!loading && matchResult && activeTab === 'match' && (
            <div className="space-y-6">
              <div className={`p-6 rounded-xl border ${matchResult.is_equivalent ? 'bg-emerald-50 border-emerald-200' : 'bg-red-50 border-red-200'}`}>
                <div className="flex items-start gap-4">
                  <div className={`p-3 rounded-full ${matchResult.is_equivalent ? 'bg-emerald-100 text-emerald-600' : 'bg-red-100 text-red-600'}`}>
                    {matchResult.is_equivalent ? <CheckCircle size={24} /> : <AlertTriangle size={24} />}
                  </div>
                  <div>
                    <h3 className={`text-lg font-bold ${matchResult.is_equivalent ? 'text-emerald-800' : 'text-red-800'}`}>
                      {matchResult.is_equivalent ? 'Найден эквивалент' : 'Нет прямого аналога'}
                    </h3>
                    <p className="text-sm mt-1 opacity-80">
                      Уверенность ИИ: <strong>{(matchResult.confidence * 100).toFixed(0)}%</strong>
                    </p>
                  </div>
                </div>
              </div>

              {matchResult.recommended_product_id && (
                 <div className="bg-white p-6 rounded-xl border border-slate-200 shadow-sm">
                   <p className="text-xs text-slate-400 uppercase tracking-wider font-bold mb-3">Рекомендованный продукт</p>
                   {(() => {
                     const p = getRecommendedProduct(matchResult.recommended_product_id);
                     if (!p) return null;
                     return (
                       <div>
                         <h4 className="text-xl font-bold text-slate-800">{p.title}</h4>
                         <p className="text-sm text-slate-500 mb-4">{p.category} | {p.material_type}</p>
                         <div className="grid grid-cols-2 gap-4 bg-slate-50 p-4 rounded-lg">
                            {Object.entries(p.specs).map(([k,v]) => (
                                <div key={k}>
                                    <span className="block text-xs text-slate-400 capitalize">{formatSpecKey(k)}</span>
                                    <span className="font-medium text-slate-800">{v}</span>
                                </div>
                            ))}
                         </div>
                       </div>
                     );
                   })()}
                 </div>
              )}

              <div className="bg-white p-6 rounded-xl border border-slate-200 shadow-sm">
                <h4 className="font-bold text-slate-800 mb-2">Обоснование ИИ</h4>
                <p className="text-slate-600 text-sm leading-relaxed">{matchResult.reasoning}</p>
                {matchResult.critical_mismatches.length > 0 && (
                  <div className="mt-4 pt-4 border-t border-slate-100">
                     <h5 className="text-red-600 text-sm font-bold mb-2">Критические расхождения:</h5>
                     <ul className="list-disc pl-5 text-sm text-slate-600 space-y-1">
                        {matchResult.critical_mismatches.map((m, i) => (
                            <li key={i}>{m}</li>
                        ))}
                     </ul>
                  </div>
                )}
              </div>
            </div>
          )}
          
          {/* Empty State */}
          {!loading && !matchResult && Object.keys(batchResults).length === 0 && (
              <div className="h-full flex flex-col items-center justify-center text-slate-400 opacity-50">
                  <Cpu size={48} className="mb-4"/>
                  <p>Результаты появятся здесь</p>
              </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default Analysis;
