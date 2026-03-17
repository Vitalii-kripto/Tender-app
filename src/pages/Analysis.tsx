import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { getTendersFromBackend, deleteTenderFromBackend, processTendersBatch } from '../services/geminiService';
import { AnalysisResult, LegalAnalysisRow, TenderLegalResult, Tender } from '../types';
import { FileText, Shield, ArrowRight, CheckCircle, AlertTriangle, Cpu, Trash2, FileDown, Loader2, CheckSquare, Square, Info, ChevronDown, ChevronUp } from 'lucide-react';

const Analysis = () => {
  const navigate = useNavigate();
  const [loading, setLoading] = useState(false);
  const [statusText, setStatusText] = useState('');
  
  // Batch analysis state
  const [allTenders, setAllTenders] = useState<Tender[]>([]);
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());
  const [analysisResults, setAnalysisResults] = useState<TenderLegalResult[]>([]);
  const [processingStatus, setProcessingStatus] = useState<'idle' | 'loading' | 'completed' | 'error'>('idle');
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [expandedTenders, setExpandedTenders] = useState<Set<string>>(new Set());

  useEffect(() => {
    const loadData = async () => {
        try {
            const tenders = await getTendersFromBackend();
            setAllTenders(tenders);
        } catch (e) {
            console.error("Failed to load analysis tenders", e);
        }
    };
    loadData();
  }, []);

  const toggleTenderSelection = (id: string) => {
    if (processingStatus === 'loading') return;
    const newSelected = new Set(selectedIds);
    if (newSelected.has(id)) {
      newSelected.delete(id);
    } else {
      newSelected.add(id);
    }
    setSelectedIds(newSelected);
  };

  const toggleSelectAll = () => {
    if (processingStatus === 'loading') return;
    if (selectedIds.size === allTenders.length && allTenders.length > 0) {
      setSelectedIds(new Set());
    } else {
      setSelectedIds(new Set(allTenders.map(t => t.id)));
    }
  };

  const toggleExpand = (id: string) => {
    const newExpanded = new Set(expandedTenders);
    if (newExpanded.has(id)) {
      newExpanded.delete(id);
    } else {
      newExpanded.add(id);
    }
    setExpandedTenders(newExpanded);
  };

  const handleBatchAnalyze = async () => {
    if (selectedIds.size === 0) {
      setErrorMessage("Выберите хотя бы один тендер для анализа.");
      return;
    }
    
    setErrorMessage(null);
    setLoading(true);
    setProcessingStatus('loading');
    setStatusText("Запуск пакетного анализа...");
    
    try {
      const results = await processTendersBatch(Array.from(selectedIds));
      setAnalysisResults(results);
      setProcessingStatus('completed');
      // Expand all results by default
      setExpandedTenders(new Set(results.map(r => r.tender_id)));
    } catch (e: any) {
      console.error(e);
      setErrorMessage(e.message || "Произошла ошибка при анализе.");
      setProcessingStatus('error');
    } finally {
      setLoading(false);
      setStatusText("");
    }
  };

  const removeTender = async (id: string) => {
    if (processingStatus === 'loading') return;
    if(confirm("Убрать этот тендер из CRM?")) {
        const updated = allTenders.filter(t => t.id !== id);
        setAllTenders(updated);
        
        if (selectedIds.has(id)) {
          const newSelected = new Set(selectedIds);
          newSelected.delete(id);
          setSelectedIds(newSelected);
        }
        
        setAnalysisResults(prev => prev.filter(r => r.tender_id !== id));
        await deleteTenderFromBackend(id);
    }
  };

  const formatPrice = (price: any) => {
    if (price === null || price === undefined || price === "") return "Сумма не указана";
    const num = typeof price === 'string' ? parseFloat(price.replace(/[^\d.]/g, '')) : price;
    if (isNaN(num)) return "Сумма не указана";
    return new Intl.NumberFormat('ru-RU', { style: 'currency', currency: 'RUB', maximumFractionDigits: 0 }).format(num);
  };

  const getRiskBadgeClass = (level: string) => {
    switch (level.toLowerCase()) {
      case 'high': return 'bg-red-100 text-red-700 border-red-200';
      case 'medium': return 'bg-amber-100 text-amber-700 border-amber-200';
      case 'low': return 'bg-emerald-100 text-emerald-700 border-emerald-200';
      default: return 'bg-slate-100 text-slate-700 border-slate-200';
    }
  };

  const getRiskLabel = (level: string) => {
    switch (level.toLowerCase()) {
      case 'high': return 'Высокий';
      case 'medium': return 'Средний';
      case 'low': return 'Низкий';
      default: return level;
    }
  };

  const exportToCSV = (rows: LegalAnalysisRow[], tenderNumber: string) => {
    const headers = ['Блок', 'Находка', 'Уровень риска', 'Действие поставщика', 'Документ', 'Ссылка', 'Основание'];
    const csvRows = rows.map(r => [
      `"${r.block}"`,
      `"${r.finding.replace(/"/g, '""')}"`,
      `"${r.risk_level}"`,
      `"${r.supplier_action.replace(/"/g, '""')}"`,
      `"${r.source_document.replace(/"/g, '""')}"`,
      `"${r.source_reference.replace(/"/g, '""')}"`,
      `"${(r.legal_basis || '').replace(/"/g, '""')}"`
    ]);
    const csvContent = [headers.join(','), ...csvRows.map(r => r.join(','))].join('\n');
    const blob = new Blob([new Uint8Array([0xEF, 0xBB, 0xBF]), csvContent], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.setAttribute('download', `legal_analysis_${tenderNumber}.csv`);
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  return (
    <div className="p-6 max-w-7xl mx-auto pb-20">
      <div className="mb-8 flex flex-col md:flex-row md:items-end justify-between gap-4">
        <div>
          <h2 className="text-3xl font-bold text-slate-900 flex items-center gap-3">
            <Shield className="text-blue-600 w-8 h-8" />
            ИИ Юрист
          </h2>
          <p className="text-slate-500 mt-2 max-w-2xl">
            Пакетный юридический анализ тендерной документации. Система классифицирует документы, находит скрытые риски и дает рекомендации поставщику.
          </p>
        </div>
        
        <div className="flex items-center gap-4 bg-white p-2 rounded-xl border border-slate-200 shadow-sm">
          <div className="px-4 border-r border-slate-100">
            <span className="block text-[10px] uppercase text-slate-400 font-bold">Всего тендеров</span>
            <span className="text-xl font-bold text-slate-700">{allTenders.length}</span>
          </div>
          <div className="px-4">
            <span className="block text-[10px] uppercase text-slate-400 font-bold">Выбрано для анализа</span>
            <span className={`text-xl font-bold ${selectedIds.size > 0 ? 'text-blue-600' : 'text-slate-300'}`}>{selectedIds.size}</span>
          </div>
        </div>
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-12 gap-8">
        {/* Tender List Section */}
        <div className="xl:col-span-5 flex flex-col gap-4">
          <div className="bg-white rounded-2xl border border-slate-200 shadow-sm overflow-hidden flex flex-col h-[700px]">
            <div className="p-4 border-b border-slate-100 bg-slate-50/50 flex items-center justify-between">
              <div className="flex items-center gap-3">
                <button 
                  onClick={toggleSelectAll}
                  disabled={processingStatus === 'loading'}
                  className="text-slate-500 hover:text-blue-600 transition-colors"
                >
                  {selectedIds.size === allTenders.length && allTenders.length > 0 ? (
                    <CheckSquare className="w-5 h-5 text-blue-600" />
                  ) : (
                    <Square className="w-5 h-5" />
                  )}
                </button>
                <span className="text-sm font-bold text-slate-700">Список тендеров</span>
              </div>
              
              <div className="flex gap-2">
                <button 
                  onClick={() => setSelectedIds(new Set(allTenders.map(t => t.id)))}
                  disabled={processingStatus === 'loading'}
                  className="text-[10px] uppercase font-bold text-blue-600 hover:underline"
                >
                  Выбрать все
                </button>
                <span className="text-slate-300">|</span>
                <button 
                  onClick={() => setSelectedIds(new Set())}
                  disabled={processingStatus === 'loading'}
                  className="text-[10px] uppercase font-bold text-slate-400 hover:underline"
                >
                  Сбросить
                </button>
              </div>
            </div>

            <div className="flex-1 overflow-y-auto p-4 space-y-3 custom-scrollbar">
              {allTenders.length === 0 ? (
                <div className="h-full flex flex-col items-center justify-center text-center p-8">
                  <FileText size={48} className="text-slate-200 mb-4" />
                  <h3 className="text-lg font-medium text-slate-700">Тендеры не найдены</h3>
                  <p className="text-sm text-slate-400 mt-2">Добавьте тендеры из поиска, чтобы начать анализ.</p>
                  <button 
                    onClick={() => navigate('/tenders')}
                    className="mt-6 px-6 py-2 bg-blue-600 text-white rounded-xl font-medium hover:bg-blue-700 transition-all shadow-md"
                  >
                    Перейти к поиску
                  </button>
                </div>
              ) : (
                allTenders.map(tender => (
                  <div 
                    key={tender.id} 
                    onClick={() => toggleTenderSelection(tender.id)}
                    className={`group relative p-4 rounded-xl border transition-all cursor-pointer ${selectedIds.has(tender.id) ? 'border-blue-200 bg-blue-50/30' : 'border-slate-100 hover:border-slate-200 hover:bg-slate-50'}`}
                  >
                    <div className="flex items-start gap-4">
                      <div className="mt-1">
                        {selectedIds.has(tender.id) ? (
                          <CheckSquare className="w-5 h-5 text-blue-600" />
                        ) : (
                          <Square className="w-5 h-5 text-slate-300 group-hover:text-slate-400" />
                        )}
                      </div>
                      
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center justify-between mb-1">
                          <span className="text-[10px] font-mono font-bold text-slate-400">№ {tender.eis_number}</span>
                          <span className="text-[10px] font-bold text-slate-500 bg-slate-100 px-1.5 py-0.5 rounded">{tender.law_type}</span>
                        </div>
                        <h4 className="text-sm font-bold text-slate-800 line-clamp-1 mb-1">{tender.title}</h4>
                        <p className="text-xs text-slate-500 line-clamp-2 mb-3 leading-relaxed">{tender.description}</p>
                        
                        <div className="flex items-center justify-between mt-auto pt-3 border-t border-slate-100/50">
                          <span className="text-sm font-black text-slate-900">{formatPrice(tender.initial_price)}</span>
                          <div className="flex items-center gap-3">
                            {tender.deadline && (
                              <span className="text-[10px] text-slate-400 flex items-center gap-1">
                                До {tender.deadline}
                              </span>
                            )}
                            <button 
                              onClick={(e) => { e.stopPropagation(); removeTender(tender.id); }}
                              className="text-slate-300 hover:text-red-500 transition-colors"
                            >
                              <Trash2 size={14} />
                            </button>
                          </div>
                        </div>
                      </div>
                    </div>
                  </div>
                ))
              )}
            </div>

            <div className="p-4 bg-white border-t border-slate-100">
              {errorMessage && (
                <div className="mb-4 p-3 bg-red-50 border border-red-100 rounded-xl flex items-center gap-3 text-red-600 text-xs animate-in fade-in slide-in-from-top-2">
                  <AlertTriangle size={16} />
                  {errorMessage}
                </div>
              )}
              
              <button 
                onClick={handleBatchAnalyze}
                disabled={loading || selectedIds.size === 0}
                className={`w-full flex items-center justify-center gap-3 py-4 rounded-2xl text-white font-black transition-all shadow-lg ${loading || selectedIds.size === 0 ? 'bg-slate-300 cursor-not-allowed shadow-none' : 'bg-gradient-to-r from-blue-600 to-indigo-600 hover:shadow-blue-200 hover:scale-[1.02] active:scale-[0.98]'}`}
              >
                {loading ? (
                  <>
                    <Loader2 size={20} className="animate-spin" />
                    Анализируем...
                  </>
                ) : (
                  <>
                    <Cpu size={20} />
                    Проанализировать выбранные ({selectedIds.size})
                  </>
                )}
              </button>
              <p className="text-[10px] text-center text-slate-400 mt-3 uppercase tracking-widest font-bold">
                ИИ изучит все документы и составит отчет
              </p>
            </div>
          </div>
        </div>

        {/* Results Section */}
        <div className="xl:col-span-7">
          <div className="bg-white rounded-2xl border border-slate-200 shadow-sm overflow-hidden flex flex-col h-[700px]">
            <div className="p-4 border-b border-slate-100 bg-slate-50/50 flex items-center justify-between">
              <h3 className="text-sm font-bold text-slate-700">Результаты анализа</h3>
              {analysisResults.length > 0 && (
                <span className="text-[10px] font-bold text-slate-400 uppercase">Готово: {analysisResults.length} из {selectedIds.size}</span>
              )}
            </div>

            <div className="flex-1 overflow-y-auto p-6 space-y-8 custom-scrollbar">
              {processingStatus === 'idle' && analysisResults.length === 0 && (
                <div className="h-full flex flex-col items-center justify-center text-center opacity-40">
                  <div className="w-24 h-24 bg-slate-50 rounded-full flex items-center justify-center mb-6">
                    <Shield size={48} className="text-slate-300" />
                  </div>
                  <h3 className="text-xl font-bold text-slate-800">Готов к работе</h3>
                  <p className="text-sm text-slate-500 mt-2 max-w-xs">Выберите тендеры слева и нажмите кнопку запуска анализа.</p>
                </div>
              )}

              {processingStatus === 'loading' && (
                <div className="h-full flex flex-col items-center justify-center text-center">
                  <div className="relative mb-8">
                    <div className="w-20 h-20 border-4 border-slate-100 border-t-blue-600 rounded-full animate-spin"></div>
                    <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2">
                      <Cpu size={32} className="text-blue-600" />
                    </div>
                  </div>
                  <h3 className="text-xl font-bold text-slate-800 animate-pulse">{statusText}</h3>
                  <p className="text-sm text-slate-400 mt-4 max-w-sm">Это может занять до 2-3 минут в зависимости от объема документации.</p>
                </div>
              )}

              {analysisResults.map((result, idx) => (
                <div key={result.tender_id} className="animate-in fade-in slide-in-from-bottom-4 duration-500" style={{ animationDelay: `${idx * 150}ms` }}>
                  <div className="flex items-center justify-between mb-4">
                    <div className="flex items-center gap-3">
                      <div className={`w-10 h-10 rounded-xl flex items-center justify-center ${result.status === 'error' ? 'bg-red-50 text-red-600' : 'bg-blue-50 text-blue-600'}`}>
                        {result.status === 'error' ? <AlertTriangle size={20} /> : <FileText size={20} />}
                      </div>
                      <div>
                        <div className="flex items-center gap-2">
                          <span className="text-[10px] font-mono font-bold text-slate-400">№ {result.eis_number}</span>
                          {result.status === 'ready' && (
                            <span className="text-[10px] font-bold text-emerald-600 flex items-center gap-1">
                              <CheckCircle size={10} /> Анализ завершен
                            </span>
                          )}
                        </div>
                        <h4 className="text-sm font-bold text-slate-800 line-clamp-1">{result.title}</h4>
                      </div>
                    </div>
                    
                    <button 
                      onClick={() => toggleExpand(result.tender_id)}
                      className="p-2 hover:bg-slate-50 rounded-lg transition-colors text-slate-400"
                    >
                      {expandedTenders.has(result.tender_id) ? <ChevronUp size={20} /> : <ChevronDown size={20} />}
                    </button>
                  </div>

                  {result.status === 'error' ? (
                    <div className="bg-red-50 border border-red-100 rounded-2xl p-4 flex items-start gap-3">
                      <AlertTriangle className="text-red-500 shrink-0 mt-0.5" size={18} />
                      <div>
                        <p className="text-sm font-bold text-red-800">Ошибка анализа</p>
                        <p className="text-xs text-red-600 mt-1">{result.error_message || "Не удалось обработать документы."}</p>
                      </div>
                    </div>
                  ) : (
                    <div className="space-y-4">
                      {/* Summary Cards */}
                      <div className="grid grid-cols-3 gap-3">
                        <div className="bg-white p-3 rounded-2xl border border-slate-100 shadow-sm flex flex-col items-center justify-center text-center">
                          <span className="text-[10px] font-bold text-slate-400 uppercase mb-1">Высокий риск</span>
                          <span className={`text-xl font-black ${result.summary.high_risks > 0 ? 'text-red-600' : 'text-slate-300'}`}>{result.summary.high_risks}</span>
                        </div>
                        <div className="bg-white p-3 rounded-2xl border border-slate-100 shadow-sm flex flex-col items-center justify-center text-center">
                          <span className="text-[10px] font-bold text-slate-400 uppercase mb-1">Средний риск</span>
                          <span className={`text-xl font-black ${result.summary.medium_risks > 0 ? 'text-amber-600' : 'text-slate-300'}`}>{result.summary.medium_risks}</span>
                        </div>
                        <div className="bg-white p-3 rounded-2xl border border-slate-100 shadow-sm flex flex-col items-center justify-center text-center">
                          <span className="text-[10px] font-bold text-slate-400 uppercase mb-1">Низкий риск</span>
                          <span className={`text-xl font-black ${result.summary.low_risks > 0 ? 'text-blue-600' : 'text-slate-300'}`}>{result.summary.low_risks}</span>
                        </div>
                      </div>

                      {/* Analysis Table */}
                      {expandedTenders.has(result.tender_id) && (
                        <div className="bg-white rounded-2xl border border-slate-200 shadow-sm overflow-hidden animate-in slide-in-from-top-2 duration-300">
                          <div className="overflow-x-auto">
                            <table className="w-full text-left text-xs border-collapse">
                              <thead>
                                <tr className="bg-slate-50 border-b border-slate-100">
                                  <th className="px-4 py-3 font-bold text-slate-500 uppercase tracking-wider w-32">Блок</th>
                                  <th className="px-4 py-3 font-bold text-slate-500 uppercase tracking-wider">Находка и Риск</th>
                                  <th className="px-4 py-3 font-bold text-slate-500 uppercase tracking-wider">Действие поставщика</th>
                                  <th className="px-4 py-3 font-bold text-slate-500 uppercase tracking-wider w-40">Источник</th>
                                </tr>
                              </thead>
                              <tbody className="divide-y divide-slate-50">
                                {result.rows.length === 0 ? (
                                  <tr>
                                    <td colSpan={4} className="px-4 py-8 text-center text-slate-400 italic">
                                      Критических условий не выявлено
                                    </td>
                                  </tr>
                                ) : (
                                  result.rows.map((row, rIdx) => (
                                    <tr key={rIdx} className="hover:bg-slate-50/50 transition-colors">
                                      <td className="px-4 py-4 align-top">
                                        <span className="font-bold text-slate-700">{row.block}</span>
                                      </td>
                                      <td className="px-4 py-4 align-top">
                                        <div className="flex flex-col gap-2">
                                          <div className="flex items-center gap-2">
                                            <span className={`text-[9px] font-black uppercase px-1.5 py-0.5 rounded-md border ${getRiskBadgeClass(row.risk_level)}`}>
                                              {getRiskLabel(row.risk_level)}
                                            </span>
                                          </div>
                                          <p className="text-slate-800 leading-relaxed">{row.finding}</p>
                                          {row.legal_basis && (
                                            <p className="text-[10px] text-slate-400 italic">Основание: {row.legal_basis}</p>
                                          )}
                                        </div>
                                      </td>
                                      <td className="px-4 py-4 align-top">
                                        <div className="flex items-start gap-2 text-blue-700 bg-blue-50/50 p-2 rounded-lg border border-blue-100/50">
                                          <ArrowRight size={12} className="shrink-0 mt-0.5" />
                                          <p className="font-medium">{row.supplier_action}</p>
                                        </div>
                                      </td>
                                      <td className="px-4 py-4 align-top">
                                        <div className="flex flex-col gap-1">
                                          <span className="text-slate-500 font-medium truncate max-w-[140px]" title={row.source_document}>
                                            {row.source_document}
                                          </span>
                                          <span className="text-[10px] text-slate-400">{row.source_reference}</span>
                                        </div>
                                      </td>
                                    </tr>
                                  ))
                                )}
                              </tbody>
                            </table>
                          </div>
                          
                          <div className="p-3 bg-slate-50 border-t border-slate-100 flex justify-between items-center">
                            <div className="flex items-center gap-2 text-[10px] text-slate-400">
                              <Info size={12} />
                              <span>{result.summary.unread_files > 0 ? `Не удалось прочитать файлов: ${result.summary.unread_files}` : 'Все файлы успешно обработаны'}</span>
                            </div>
                            <div className="flex gap-3">
                              <button 
                                onClick={() => exportToCSV(result.rows, result.eis_number)}
                                className="flex items-center gap-1.5 px-3 py-1.5 bg-white border border-slate-200 rounded-lg text-[10px] font-bold text-slate-600 hover:bg-slate-50 transition-colors shadow-sm"
                              >
                                <FileDown size={12} /> CSV
                              </button>
                              <button 
                                onClick={() => window.print()}
                                className="flex items-center gap-1.5 px-3 py-1.5 bg-white border border-slate-200 rounded-lg text-[10px] font-bold text-slate-600 hover:bg-slate-50 transition-colors shadow-sm"
                              >
                                <FileDown size={12} /> PDF
                              </button>
                            </div>
                          </div>
                        </div>
                      )}
                    </div>
                  )}
                  
                  {idx < analysisResults.length - 1 && <div className="my-8 border-t border-slate-100 border-dashed" />}
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default Analysis;
