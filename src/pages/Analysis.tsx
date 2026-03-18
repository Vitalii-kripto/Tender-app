import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { MOCK_CATALOG } from './ProductCatalog';
import { findProductEquivalent, analyzeTendersBatch, getTendersFromBackend, deleteTenderFromBackend } from '../services/geminiService';
import { AnalysisResult, Tender, LegalAnalysisResult } from '../types';
import { FileText, Shield, ArrowRight, CheckCircle, AlertTriangle, Cpu, Trash2, FileDown, ScanEye, Loader2, Square, CheckSquare } from 'lucide-react';

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
                    // Select all by default
                    setSelectedFiles(prev => ({ ...prev, [t.id]: new Set(files.map((f: any) => f.name)) }));
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
        
        // Prepare selected files mapping
        const filesMapping: Record<string, string[]> = {};
        idsArray.forEach(id => {
            if (selectedFiles[id]) {
                filesMapping[id] = Array.from(selectedFiles[id]);
            }
        });

        const results = await analyzeTendersBatch(idsArray, filesMapping);
        
        const newResults: Record<string, LegalAnalysisResult> = { ...batchResults };
        results.forEach(res => {
            newResults[res.id] = res;
        });
        setBatchResults(newResults);
    } catch (e) {
        console.error("Batch analysis failed", e);
        setAnalysisError('Произошла ошибка при выполнении анализа.');
    } finally {
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
        const response = await fetch('/api/ai/export-risks-excel', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ results })
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

  const getRecommendedProduct = (id?: string) => MOCK_CATALOG.find(p => p.id === id);

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
                                            <span className="text-xs font-mono bg-slate-100 px-1.5 rounded text-slate-500">#{tender.eis_number}</span>
                                            <span className="text-xs font-bold text-slate-700">{formatCurrency(tender.initial_price)}</span>
                                        </div>
                                        <h4 className="text-sm font-bold text-slate-800 line-clamp-1 mb-1">{tender.title}</h4>
                                        <p className="text-xs text-slate-500 line-clamp-2 mb-2">{tender.description}</p>
                                        
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
            <div className="h-full flex flex-col items-center justify-center text-slate-400 space-y-4">
              <div className="relative">
                <div className="w-16 h-16 border-4 border-slate-100 border-t-blue-600 rounded-full animate-spin"></div>
                <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2">
                    <Cpu size={24} className="text-blue-600" />
                </div>
              </div>
              <p className="animate-pulse font-medium">{statusText || "Агент работает..."}</p>
            </div>
          )}

          {/* 2. Batch Results State */}
          {!loading && activeTab === 'batch' && Object.keys(batchResults).length > 0 && (
             <div className="space-y-8">
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
                                        <span className="flex items-center gap-1.5 px-3 py-1.5 rounded-full text-[10px] font-bold uppercase tracking-wider bg-slate-100 text-slate-600 border border-slate-200 shadow-sm">
                                            <FileText size={14}/>
                                            Файлов: {result.file_statuses.filter(f => f.status === 'ok').length} ОК / {result.file_statuses.filter(f => f.status !== 'ok').length} ОШИБКИ
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
                                <div className="px-5 py-3 bg-slate-50 border-b border-slate-100">
                                    <h4 className="text-[10px] font-black text-slate-400 uppercase tracking-widest mb-2">Статус документов</h4>
                                    <div className="flex flex-wrap gap-2">
                                        {result.file_statuses.map((fs, i) => (
                                            <div key={i} className={`flex items-center gap-2 px-2 py-1 rounded border text-[11px] font-medium ${fs.status === 'ok' ? 'bg-white border-slate-200 text-slate-600' : 'bg-red-50 border-red-200 text-red-700'}`} title={fs.message}>
                                                {fs.status === 'ok' ? <CheckCircle size={12} className="text-emerald-500" /> : <AlertTriangle size={12} className="text-red-500" />}
                                                <span className="truncate max-w-[200px]">{fs.filename}</span>
                                                {fs.status !== 'ok' && <span className="text-[9px] font-bold opacity-70">({fs.status})</span>}
                                            </div>
                                        ))}
                                    </div>
                                </div>

                                {result.summary_notes && result.summary_notes.length > 0 && (
                                    <div className="px-5 py-4 bg-blue-50/30 border-b border-blue-100">
                                        <h4 className="text-xs font-black text-blue-800 uppercase tracking-wider mb-2">Сводка анализа:</h4>
                                        <ul className="space-y-1.5">
                                            {result.summary_notes.map((note, i) => (
                                                <li key={i} className="flex items-start gap-2 text-xs text-blue-700 leading-relaxed">
                                                    <div className="w-1.5 h-1.5 rounded-full bg-blue-400 mt-1.5 shrink-0"></div>
                                                    {note}
                                                </li>
                                            ))}
                                        </ul>
                                    </div>
                                )}
                                
                                {result.status === 'success' && result.rows.length === 0 ? (
                                    <div className="p-10 text-center text-slate-400 bg-white">
                                        <Shield size={48} className="mx-auto mb-4 opacity-20" />
                                        <p className="text-sm font-medium">Документация выглядит стандартной. Критических условий не найдено.</p>
                                    </div>
                                ) : result.status === 'success' && result.rows.length > 0 ? (
                                    <div className="overflow-x-auto bg-white">
                                        <table className="w-full text-left text-sm border-collapse">
                                            <thead className="bg-slate-50 text-slate-500 text-[10px] uppercase font-black tracking-widest border-b border-slate-200">
                                                <tr>
                                                    <th className="px-5 py-4 font-black">Блок / Риск</th>
                                                    <th className="px-5 py-4 font-black w-24">Уровень</th>
                                                    <th className="px-5 py-4 font-black">Действие поставщика</th>
                                                    <th className="px-5 py-4 font-black">Источник</th>
                                                </tr>
                                            </thead>
                                            <tbody className="divide-y divide-slate-100">
                                                {result.rows.map((row, idx) => (
                                                    <tr key={idx} className={`transition-colors ${row.risk_level === 'High' ? 'bg-red-50/20 hover:bg-red-50/40' : row.risk_level === 'Medium' ? 'bg-amber-50/20 hover:bg-amber-50/40' : 'hover:bg-slate-50/50'}`}>
                                                        <td className="px-5 py-4 align-top">
                                                            <div className="font-black text-slate-900 mb-1.5">{row.block}</div>
                                                            <div className="text-slate-700 text-xs leading-relaxed font-medium">{row.finding}</div>
                                                        </td>
                                                        <td className="px-5 py-4 align-top">
                                                            <span className={`text-[10px] uppercase font-black px-2.5 py-1 rounded-full whitespace-nowrap shadow-sm ${row.risk_level === 'High' ? 'bg-red-600 text-white' : row.risk_level === 'Medium' ? 'bg-amber-500 text-white' : 'bg-blue-500 text-white'}`}>
                                                                {row.risk_level === 'High' ? 'Высокий' : row.risk_level === 'Medium' ? 'Средний' : 'Низкий'}
                                                            </span>
                                                        </td>
                                                        <td className="px-5 py-4 align-top text-slate-700 text-xs leading-relaxed font-medium">
                                                            {row.supplier_action}
                                                        </td>
                                                        <td className="px-5 py-4 align-top">
                                                            <div className="text-xs font-black text-slate-800 mb-1">{row.source_document}</div>
                                                            <div className="text-[10px] text-slate-500 font-bold italic">{row.source_reference}</div>
                                                            {row.legal_basis && <div className="text-[10px] text-blue-600 mt-2 font-black bg-blue-50 px-2 py-1 rounded border border-blue-100 inline-block">{row.legal_basis}</div>}
                                                        </td>
                                                    </tr>
                                                ))}
                                            </tbody>
                                        </table>
                                    </div>
                                ) : null}
                                
                                {result.status === 'success' && (
                                    <div className="bg-slate-50 p-4 border-t border-slate-100 flex justify-end gap-6">
                                        <button onClick={() => exportToExcel([result])} className="text-xs text-emerald-600 font-black hover:underline flex items-center gap-2 uppercase tracking-wider">
                                            <FileDown size={16} /> Скачать Excel
                                        </button>
                                        <button onClick={() => exportToCSV(result, tender.eis_number)} className="text-xs text-blue-600 font-black hover:underline flex items-center gap-2 uppercase tracking-wider">
                                            <FileDown size={16} /> Скачать CSV
                                        </button>
                                        <button onClick={exportToPDF} className="text-xs text-slate-600 font-black hover:underline flex items-center gap-2 uppercase tracking-wider">
                                            <FileDown size={16} /> Печать / PDF
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