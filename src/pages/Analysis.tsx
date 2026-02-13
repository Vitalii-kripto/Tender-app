import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { MOCK_CATALOG } from './ProductCatalog';
import { findProductEquivalent, analyzeLegalRisks, fetchTenderDocsText, getTendersFromBackend, deleteTenderFromBackend } from '../services/geminiService';
import { AnalysisResult, LegalRisk, Tender } from '../types';
import { FileText, Shield, ArrowRight, CheckCircle, AlertTriangle, Cpu, Trash2, FileDown, ScanEye, Loader2 } from 'lucide-react';

const Analysis = () => {
  const navigate = useNavigate();
  const [inputText, setInputText] = useState('');
  const [loading, setLoading] = useState(false);
  const [activeTab, setActiveTab] = useState<'match' | 'legal' | 'batch'>('batch');
  const [statusText, setStatusText] = useState('');
  
  // Single analysis state
  const [matchResult, setMatchResult] = useState<AnalysisResult | null>(null);
  const [legalRisks, setLegalRisks] = useState<LegalRisk[]>([]);

  // Batch analysis state
  const [selectedTenders, setSelectedTenders] = useState<Tender[]>([]);
  const [batchResults, setBatchResults] = useState<Record<string, { risks: LegalRisk[], status: 'pending' | 'loading' | 'done', docText?: string }>>({});

  useEffect(() => {
    // Load from Unified Service (Backend + LocalStorage Fallback)
    const loadData = async () => {
        try {
            const tenders = await getTendersFromBackend();
            setSelectedTenders(tenders);
            
            const initialResults: any = {};
            tenders.forEach(t => {
                initialResults[t.id] = { risks: [], status: 'pending' };
            });
            setBatchResults(initialResults);
            
            if (tenders.length > 0) setActiveTab('batch');
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
      } else {
        const risks = await analyzeLegalRisks(inputText);
        setLegalRisks(risks);
      }
    } catch (e) {
      console.error(e);
    } finally {
      setLoading(false);
    }
  };

  const runBatchAnalysis = async () => {
    setLoading(true);
    
    for (const tender of selectedTenders) {
        // 1. Статус: скачивание
        setBatchResults(prev => ({
            ...prev,
            [tender.id]: { ...prev[tender.id], status: 'loading' }
        }));
        setStatusText(`Скачивание документации для №${tender.eis_number}...`);

        // 2. Реальное скачивание текста через Backend
        let extractedText = "";
        if (tender.url) {
            extractedText = await fetchTenderDocsText(tender.url, tender.eis_number);
        }

        if (!extractedText || extractedText.length < 50) {
             extractedText = `Не удалось скачать документы автоматически. Анализируем описание: ${tender.description}`;
        }

        setStatusText(`ИИ анализ условий контракта №${tender.eis_number}...`);

        // 3. Отправка в Gemini
        try {
            const risks = await analyzeLegalRisks(extractedText);
            setBatchResults(prev => ({
                ...prev,
                [tender.id]: { risks: risks, status: 'done', docText: extractedText.substring(0, 200) + "..." }
            }));
        } catch (e) {
            console.error(e);
            setBatchResults(prev => ({
                ...prev,
                [tender.id]: { risks: [], status: 'done' }
            }));
        }
    }
    setLoading(false);
    setStatusText("");
  };

  const removeTender = async (id: string) => {
    if(confirm("Убрать этот тендер из CRM?")) {
        const updated = selectedTenders.filter(t => t.id !== id);
        setSelectedTenders(updated);
        await deleteTenderFromBackend(id); // Sync delete
        if (updated.length === 0) setActiveTab('match');
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

  return (
    <div className="p-6 max-w-6xl mx-auto pb-20">
      <div className="mb-8">
        <h2 className="text-2xl font-bold text-slate-900 flex items-center gap-2">
          <Cpu className="text-blue-600" />
          ИИ Анализ (AI Engine)
        </h2>
        <p className="text-slate-500 text-sm mt-1">
          Реальный анализ тендерной документации на риски и подбор аналогов продукции. Источник данных: CRM.
        </p>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
        {/* Input/Selection Section */}
        <div className="bg-white rounded-xl border border-slate-200 shadow-sm flex flex-col h-[600px] overflow-hidden">
          <div className="border-b border-slate-200 flex bg-slate-50">
            <button 
              onClick={() => setActiveTab('batch')}
              className={`flex-1 py-3 text-sm font-medium flex items-center justify-center gap-2 transition-colors ${activeTab === 'batch' ? 'bg-white text-blue-600 border-t-2 border-t-blue-600 shadow-sm' : 'text-slate-500 hover:text-slate-700'}`}
            >
              <div className="relative">
                <FileText size={16} />
                {selectedTenders.length > 0 && <span className="absolute -top-1 -right-2 w-3 h-3 bg-red-500 rounded-full border border-white"></span>}
              </div>
              Тендеры в работе ({selectedTenders.length})
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
                {selectedTenders.length === 0 ? (
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
                        <div className="flex-1 overflow-y-auto space-y-3 pr-2 mb-4">
                            {selectedTenders.map(tender => (
                                <div key={tender.id} className="bg-white p-3 rounded-lg border border-slate-200 shadow-sm flex justify-between items-start gap-3">
                                    <div className="flex-1 min-w-0">
                                        <div className="flex items-center gap-2 mb-1">
                                            <span className="text-xs font-mono bg-slate-100 px-1.5 rounded text-slate-500">#{tender.eis_number}</span>
                                            {batchResults[tender.id]?.status === 'loading' && <span className="text-xs text-blue-600 animate-pulse font-medium">Загрузка...</span>}
                                            {batchResults[tender.id]?.status === 'done' && <span className="text-xs text-emerald-600 font-medium flex items-center gap-1"><CheckCircle size={10}/> Готово</span>}
                                        </div>
                                        <h4 className="text-sm font-medium text-slate-800 line-clamp-2">{tender.title}</h4>
                                    </div>
                                    <button 
                                        onClick={() => removeTender(tender.id)}
                                        disabled={loading}
                                        className="text-slate-400 hover:text-red-500 p-1"
                                        title="Удалить из CRM"
                                    >
                                        <Trash2 size={16} />
                                    </button>
                                </div>
                            ))}
                        </div>
                        <div className="border-t border-slate-200 pt-4">
                            <button 
                                onClick={runBatchAnalysis}
                                disabled={loading || selectedTenders.length === 0}
                                className={`w-full flex items-center justify-center gap-2 py-3 rounded-xl text-white font-bold transition-all shadow-md ${loading ? 'bg-slate-700 cursor-wait' : 'bg-gradient-to-r from-blue-600 to-indigo-600 hover:shadow-lg hover:scale-[1.01]'}`}
                            >
                                {loading ? (
                                    <>
                                        <Loader2 size={20} className="animate-spin" />
                                        Идет работа...
                                    </>
                                ) : (
                                    <>
                                        <Cpu size={20} />
                                        ИИ Анализ документации (Real)
                                    </>
                                )}
                            </button>
                            <p className="text-xs text-center text-slate-400 mt-2">
                                Система скачает документы с zakupki.gov.ru, прочитает PDF и найдет риски.
                            </p>
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
                     <label className="flex items-center gap-2 text-sm text-slate-600">
                        <input type="radio" name="mode" checked={activeTab === 'legal'} onChange={() => setActiveTab('legal')} />
                        Юр. риски
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
        <div className="space-y-6 h-[600px] overflow-y-auto pr-2 custom-scrollbar">
          
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
          {!loading && activeTab === 'batch' && selectedTenders.length > 0 && (
             <div className="space-y-8">
                {selectedTenders.map(tender => {
                    const result = batchResults[tender.id];
                    if (!result || result.status !== 'done') return null;

                    return (
                        <div key={tender.id} className="animate-in slide-in-from-bottom-4 fade-in duration-500">
                            <div className="flex items-center gap-2 mb-3 sticky top-0 bg-slate-50/90 backdrop-blur py-2 z-10">
                                <span className="bg-slate-200 text-slate-600 text-xs font-mono px-2 py-0.5 rounded">#{tender.eis_number}</span>
                                <h3 className="font-bold text-slate-800 text-sm truncate max-w-md">{tender.title}</h3>
                            </div>
                            
                            <div className="bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden">
                                <div className="bg-slate-50 px-4 py-2 border-b border-slate-100 flex justify-between items-center">
                                    <span className="text-xs font-bold text-slate-500 uppercase tracking-wider">Отчет ИИ-Юриста</span>
                                    {result.risks.length > 0 ? (
                                        <span className="text-xs font-bold text-red-600 flex items-center gap-1"><AlertTriangle size={12}/> Найдено рисков: {result.risks.length}</span>
                                    ) : (
                                        <span className="text-xs font-bold text-emerald-600 flex items-center gap-1"><CheckCircle size={12}/> Рисков не обнаружено</span>
                                    )}
                                </div>

                                {result.docText && (
                                    <div className="px-4 py-2 bg-amber-50 text-[10px] text-amber-800 border-b border-amber-100">
                                        Проанализировано: {result.docText}
                                    </div>
                                )}
                                
                                {result.risks.length === 0 ? (
                                    <div className="p-6 text-center text-slate-400">
                                        <p className="text-sm">Документация выглядит стандартной. Критических условий не найдено.</p>
                                    </div>
                                ) : (
                                    <div className="divide-y divide-slate-100">
                                        {result.risks.map((risk, idx) => (
                                            <div key={idx} className="p-4 hover:bg-slate-50 transition-colors">
                                                <div className="flex justify-between items-start mb-1">
                                                    <span className={`text-[10px] uppercase font-bold px-1.5 py-0.5 rounded ${risk.risk_level === 'High' ? 'bg-red-100 text-red-700' : risk.risk_level === 'Medium' ? 'bg-amber-100 text-amber-700' : 'bg-blue-100 text-blue-700'}`}>
                                                        {risk.risk_level === 'High' ? 'Высокий' : risk.risk_level === 'Medium' ? 'Средний' : 'НИЗКИЙ'}
                                                    </span>
                                                    <span className="text-xs text-slate-400">{risk.document}</span>
                                                </div>
                                                <p className="text-sm font-semibold text-slate-800 mt-1">{risk.requirement}</p>
                                                <p className="text-xs text-slate-500 mt-1 leading-relaxed">{risk.description}</p>
                                            </div>
                                        ))}
                                    </div>
                                )}
                                <div className="bg-slate-50 p-2 border-t border-slate-100 flex justify-end">
                                    <button className="text-xs text-blue-600 font-medium hover:underline flex items-center gap-1">
                                        <FileDown size={12} /> Скачать полный отчет
                                    </button>
                                </div>
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

          {/* 4. Single Result State (Legal) */}
          {!loading && legalRisks.length > 0 && activeTab === 'legal' && (
            <div className="space-y-4">
               {legalRisks.map((risk, idx) => (
                 <div key={idx} className="bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden">
                    <div className={`h-1.5 w-full ${risk.risk_level === 'High' ? 'bg-red-500' : risk.risk_level === 'Medium' ? 'bg-amber-500' : 'bg-emerald-500'}`} />
                    <div className="p-5">
                       <div className="flex justify-between items-start mb-2">
                          <span className="text-xs font-mono text-slate-400 bg-slate-100 px-2 py-1 rounded">
                             {risk.document}
                          </span>
                          <span className={`text-xs font-bold px-2 py-1 rounded ${risk.risk_level === 'High' ? 'text-red-600 bg-red-50' : risk.risk_level === 'Medium' ? 'text-amber-600 bg-amber-50' : 'text-emerald-600 bg-emerald-50'}`}>
                            {risk.risk_level === 'High' ? 'ВЫСОКИЙ' : risk.risk_level === 'Medium' ? 'СРЕДНИЙ' : 'НИЗКИЙ'}
                          </span>
                       </div>
                       <h4 className="font-bold text-slate-800 mb-2">Требование: {risk.requirement}</h4>
                       <p className="text-sm text-slate-600 mb-3">{risk.description}</p>
                       <div className="flex items-center gap-2 text-xs text-slate-500">
                          <span className="font-semibold">Срок/Дедлайн:</span> {risk.deadline}
                       </div>
                    </div>
                 </div>
               ))}
            </div>
          )}
          
          {/* Empty State */}
          {!loading && !matchResult && legalRisks.length === 0 && selectedTenders.length === 0 && (
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