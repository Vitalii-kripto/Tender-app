import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { getTendersFromBackend, analyzeSelectedTendersLegal } from '../services/geminiService';
import { Tender, AILawyerTenderResult, AILawyerRow } from '../types';
import { Shield, CheckSquare, Square, Loader2, AlertTriangle, FileText, CheckCircle, XCircle } from 'lucide-react';

const Analysis = () => {
  const navigate = useNavigate();
  
  // State
  const [allTenders, setAllTenders] = useState<Tender[]>([]);
  const [checkedTenderIds, setCheckedTenderIds] = useState<string[]>([]);
  const [resultsByTenderId, setResultsByTenderId] = useState<Record<string, AILawyerTenderResult>>({});
  const [isBatchRunning, setIsBatchRunning] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    loadTenders();
  }, []);

  const loadTenders = async () => {
    try {
      const tenders = await getTendersFromBackend();
      setAllTenders(tenders);
      // Remove checked IDs that are no longer in CRM
      setCheckedTenderIds(prev => prev.filter(id => tenders.some(t => t.id === id)));
      // Remove results for deleted tenders
      setResultsByTenderId(prev => {
        const next = { ...prev };
        Object.keys(next).forEach(id => {
          if (!tenders.some(t => t.id === id)) {
            delete next[id];
          }
        });
        return next;
      });
    } catch (e) {
      console.error("Failed to load tenders", e);
    }
  };

  const toggleTender = (id: string) => {
    if (isBatchRunning) return;
    setCheckedTenderIds(prev => 
      prev.includes(id) ? prev.filter(tId => tId !== id) : [...prev, id]
    );
  };

  const selectAll = () => {
    if (isBatchRunning) return;
    setCheckedTenderIds(allTenders.map(t => t.id));
  };

  const deselectAll = () => {
    if (isBatchRunning) return;
    setCheckedTenderIds([]);
  };

  const formatPrice = (tender: Tender) => {
    if (tender.initial_price_text && tender.initial_price_text !== '0' && tender.initial_price_text !== '0.0') {
        return tender.initial_price_text;
    }
    if (tender.initial_price === 0 || tender.initial_price === '0' || tender.initial_price === '0.0') {
        return 'Сумма не указана';
    }
    
    const price = typeof tender.initial_price === 'string' ? parseFloat(tender.initial_price) : tender.initial_price;
    
    if (isNaN(price)) {
        return 'Сумма не указана';
    }
    
    return new Intl.NumberFormat('ru-RU', { style: 'currency', currency: 'RUB', maximumFractionDigits: 0 }).format(price);
  };

  const handleAnalyze = async () => {
    if (checkedTenderIds.length === 0) {
      alert("Выберите хотя бы один тендер для анализа");
      return;
    }

    setIsBatchRunning(true);
    setError(null);

    try {
      const results = await analyzeSelectedTendersLegal(checkedTenderIds);
      
      const newResultsMap = { ...resultsByTenderId };
      results.forEach(res => {
        newResultsMap[res.tender_id] = res;
      });
      
      setResultsByTenderId(newResultsMap);
    } catch (err: any) {
      console.error(err);
      setError(err.message || "Произошла ошибка при анализе");
    } finally {
      setIsBatchRunning(false);
    }
  };

  const renderRiskBadge = (level: string) => {
    switch (level) {
      case 'High': return <span className="px-2 py-1 bg-red-100 text-red-700 rounded text-xs font-bold">Высокий</span>;
      case 'Medium': return <span className="px-2 py-1 bg-amber-100 text-amber-700 rounded text-xs font-bold">Средний</span>;
      case 'Low': return <span className="px-2 py-1 bg-green-100 text-green-700 rounded text-xs font-bold">Низкий</span>;
      default: return <span className="px-2 py-1 bg-slate-100 text-slate-700 rounded text-xs font-bold">{level}</span>;
    }
  };

  return (
    <div className="p-6 max-w-7xl mx-auto h-[calc(100vh-64px)] flex flex-col">
      <div className="mb-6 flex items-center gap-3">
        <div className="p-3 bg-indigo-100 text-indigo-600 rounded-xl">
          <Shield size={24} />
        </div>
        <div>
          <h2 className="text-2xl font-bold text-slate-900">ИИ Юрист</h2>
          <p className="text-slate-500 text-sm">Глубокий правовой анализ документации закупки</p>
        </div>
      </div>

      <div className="flex gap-6 flex-1 min-h-0">
        {/* Left Sidebar: Tender Selection */}
        <div className="w-1/3 bg-white border border-slate-200 rounded-xl flex flex-col shadow-sm">
          <div className="p-4 border-b border-slate-200 bg-slate-50 rounded-t-xl">
            <h3 className="font-bold text-slate-800 mb-2">Тендеры в CRM</h3>
            <div className="flex justify-between items-center text-sm text-slate-600 mb-3">
              <span>Всего: {allTenders.length}</span>
              <span className="font-medium text-indigo-600">Выбрано: {checkedTenderIds.length}</span>
            </div>
            <div className="flex gap-2">
              <button 
                onClick={selectAll} 
                disabled={isBatchRunning || allTenders.length === 0}
                className="text-xs px-3 py-1.5 bg-white border border-slate-300 rounded hover:bg-slate-50 disabled:opacity-50"
              >
                Выбрать все
              </button>
              <button 
                onClick={deselectAll} 
                disabled={isBatchRunning || checkedTenderIds.length === 0}
                className="text-xs px-3 py-1.5 bg-white border border-slate-300 rounded hover:bg-slate-50 disabled:opacity-50"
              >
                Снять все
              </button>
            </div>
          </div>
          
          <div className="flex-1 overflow-y-auto p-2 space-y-2">
            {allTenders.length === 0 ? (
              <div className="text-center p-6 text-slate-500 text-sm">
                Нет тендеров в CRM. Сначала добавьте тендеры через поиск.
              </div>
            ) : (
              allTenders.map(tender => {
                const isChecked = checkedTenderIds.includes(tender.id);
                return (
                  <div 
                    key={tender.id} 
                    onClick={() => toggleTender(tender.id)}
                    className={`p-3 rounded-lg border cursor-pointer transition-colors flex gap-3 ${
                      isChecked ? 'border-indigo-500 bg-indigo-50/30' : 'border-slate-200 hover:border-indigo-300'
                    } ${isBatchRunning ? 'opacity-60 cursor-not-allowed' : ''}`}
                  >
                    <div className="pt-0.5 text-indigo-600">
                      {isChecked ? <CheckSquare size={18} /> : <Square size={18} className="text-slate-400" />}
                    </div>
                    <div className="flex-1 min-w-0">
                      <div className="flex justify-between items-start mb-1">
                        <span className="text-xs font-medium text-slate-500">№ {tender.eis_number}</span>
                        {tender.law_type && <span className="text-[10px] px-1.5 py-0.5 bg-slate-100 text-slate-600 rounded">{tender.law_type}</span>}
                      </div>
                      <h4 className="text-sm font-bold text-slate-800 leading-tight mb-1 line-clamp-2" title={tender.title}>
                        {tender.title}
                      </h4>
                      <p className="text-xs text-slate-600 line-clamp-2 mb-2" title={tender.description}>
                        {tender.description}
                      </p>
                      <div className="flex justify-between items-center mt-auto">
                        <span className="text-sm font-bold text-slate-900">{formatPrice(tender)}</span>
                        {tender.deadline && tender.deadline !== '-' && (
                           <span className="text-[10px] text-slate-500">До {tender.deadline}</span>
                        )}
                      </div>
                    </div>
                  </div>
                );
              })
            )}
          </div>
          
          <div className="p-4 border-t border-slate-200 bg-slate-50 rounded-b-xl">
            <button
              onClick={handleAnalyze}
              disabled={isBatchRunning || checkedTenderIds.length === 0}
              className="w-full py-3 bg-indigo-600 text-white rounded-lg font-medium flex items-center justify-center gap-2 hover:bg-indigo-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
            >
              {isBatchRunning ? <Loader2 size={18} className="animate-spin" /> : <Shield size={18} />}
              {isBatchRunning ? 'Анализ выполняется...' : 'Анализировать выбранные'}
            </button>
          </div>
        </div>

        {/* Right Content: Results */}
        <div className="w-2/3 bg-white border border-slate-200 rounded-xl shadow-sm flex flex-col overflow-hidden">
          <div className="p-4 border-b border-slate-200 bg-slate-50 flex justify-between items-center">
            <h3 className="font-bold text-slate-800">Результаты анализа</h3>
            {isBatchRunning && (
              <span className="text-sm text-indigo-600 flex items-center gap-2 font-medium">
                <Loader2 size={16} className="animate-spin" />
                Идет обработка документов...
              </span>
            )}
          </div>
          
          <div className="flex-1 overflow-y-auto p-6 bg-slate-50/50">
            {error && (
              <div className="mb-6 p-4 bg-red-50 border border-red-200 rounded-lg flex items-start gap-3 text-red-700">
                <AlertTriangle size={20} className="shrink-0 mt-0.5" />
                <div>
                  <h4 className="font-bold">Ошибка выполнения</h4>
                  <p className="text-sm mt-1">{error}</p>
                </div>
              </div>
            )}

            {Object.keys(resultsByTenderId).length === 0 && !isBatchRunning && !error ? (
              <div className="h-full flex flex-col items-center justify-center text-slate-400 space-y-4">
                <FileText size={48} className="opacity-20" />
                <p>Выберите тендеры слева и нажмите "Анализировать"</p>
              </div>
            ) : (
              <div className="space-y-8">
                {Object.values(resultsByTenderId).map(result => (
                  <div key={result.tender_id} className="bg-white border border-slate-200 rounded-xl shadow-sm overflow-hidden">
                    {/* Header */}
                    <div className="p-5 border-b border-slate-200 bg-slate-50">
                      <div className="flex justify-between items-start mb-2">
                        <span className="text-sm font-medium text-slate-500">№ {result.eis_number}</span>
                        {result.status === 'success' ? (
                          <span className="flex items-center gap-1 text-xs font-bold text-emerald-600 bg-emerald-50 px-2 py-1 rounded border border-emerald-200">
                            <CheckCircle size={14} /> Успешно
                          </span>
                        ) : (
                          <span className="flex items-center gap-1 text-xs font-bold text-red-600 bg-red-50 px-2 py-1 rounded border border-red-200">
                            <XCircle size={14} /> Ошибка
                          </span>
                        )}
                      </div>
                      <h4 className="text-lg font-bold text-slate-900 mb-2">{result.title}</h4>
                      <p className="text-sm text-slate-600 mb-4 line-clamp-2">{result.description}</p>
                      
                      {/* Summary Stats */}
                      {result.status === 'success' && result.summary && (
                        <div className="flex flex-wrap gap-3 mt-4">
                          <div className="flex items-center gap-2 bg-white px-3 py-2 rounded-lg border border-slate-200 text-sm">
                            <span className="text-slate-500">Риски:</span>
                            <span className="font-bold text-red-600" title="Высокие">{result.summary.high_risks}</span> /
                            <span className="font-bold text-amber-600" title="Средние">{result.summary.medium_risks}</span> /
                            <span className="font-bold text-green-600" title="Низкие">{result.summary.low_risks}</span>
                          </div>
                          <div className={`flex items-center gap-2 px-3 py-2 rounded-lg border text-sm ${result.summary.has_contract_project ? 'bg-emerald-50 border-emerald-200 text-emerald-700' : 'bg-amber-50 border-amber-200 text-amber-700'}`}>
                            {result.summary.has_contract_project ? <CheckCircle size={16} /> : <AlertTriangle size={16} />}
                            {result.summary.has_contract_project ? 'Проект контракта найден' : 'Проект контракта не найден'}
                          </div>
                          {result.summary.unread_files > 0 && (
                            <div className="flex items-center gap-2 bg-red-50 border border-red-200 text-red-700 px-3 py-2 rounded-lg text-sm">
                              <AlertTriangle size={16} />
                              Непрочитанных файлов: {result.summary.unread_files}
                            </div>
                          )}
                        </div>
                      )}
                      
                      {result.status === 'error' && (
                        <div className="mt-3 p-3 bg-red-50 text-red-700 text-sm rounded border border-red-100">
                          {result.error || "Неизвестная ошибка при анализе"}
                        </div>
                      )}
                    </div>

                    {/* Table */}
                    {result.status === 'success' && result.rows && result.rows.length > 0 && (
                      <div className="overflow-x-auto">
                        <table className="w-full text-left text-sm">
                          <thead className="bg-slate-50 text-slate-600 border-b border-slate-200">
                            <tr>
                              <th className="p-3 font-semibold w-32">Блок</th>
                              <th className="p-3 font-semibold">Что найдено</th>
                              <th className="p-3 font-semibold w-24">Риск</th>
                              <th className="p-3 font-semibold">Что сделать поставщику</th>
                              <th className="p-3 font-semibold w-48">Документ / Ссылка</th>
                            </tr>
                          </thead>
                          <tbody className="divide-y divide-slate-100">
                            {result.rows.map((row, idx) => (
                              <tr key={idx} className="hover:bg-slate-50/50 transition-colors">
                                <td className="p-3 align-top font-medium text-slate-800">{row.block}</td>
                                <td className="p-3 align-top text-slate-700">{row.finding}</td>
                                <td className="p-3 align-top">{renderRiskBadge(row.risk_level)}</td>
                                <td className="p-3 align-top text-slate-700">{row.supplier_action}</td>
                                <td className="p-3 align-top text-xs">
                                  <div className="font-medium text-slate-800 mb-1 break-words">{row.source_document}</div>
                                  <div className="text-slate-500 break-words">{row.source_reference}</div>
                                  {row.legal_basis && <div className="mt-1 text-indigo-600 italic break-words">{row.legal_basis}</div>}
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    )}
                    
                    {result.status === 'success' && (!result.rows || result.rows.length === 0) && (
                      <div className="p-8 text-center text-slate-500">
                        Рисков и значимых условий не обнаружено.
                      </div>
                    )}
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
};

export default Analysis;
