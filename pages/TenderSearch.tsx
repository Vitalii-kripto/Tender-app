import React, { useState, useEffect } from 'react';
import { Search, Filter, Play, CheckCircle, ExternalLink, AlertCircle, Loader2, CheckSquare, Square, WifiOff, Briefcase } from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import { MOCK_CATALOG } from './ProductCatalog';
import { searchTenders, getTendersFromBackend, addOrUpdateTender, deleteTenderFromBackend } from '../services/geminiService';
import { Tender } from '../types';

const TenderSearch = () => {
  const navigate = useNavigate();
  const [query, setQuery] = useState('');
  const [results, setResults] = useState<Tender[]>([]);
  const [loading, setLoading] = useState(false);
  const [searchMode, setSearchMode] = useState<'keyword' | 'catalog'>('keyword');
  const [isActive, setIsActive] = useState(true);

  // CRM State from Backend
  const [crmTenders, setCrmTenders] = useState<Tender[]>([]);

  useEffect(() => {
    // Initial fetch of what is already in CRM to show correct checkboxes
    getTendersFromBackend().then(setCrmTenders).catch(console.error);
  }, []);

  const isInCrm = (id: string) => crmTenders.some(t => t.id === id);

  const toggleTenderSelection = async (tender: Tender) => {
    if (tender.id === 'err_msg') return;
    
    if (isInCrm(tender.id)) {
      // Remove
      setCrmTenders(prev => prev.filter(t => t.id !== tender.id));
      await deleteTenderFromBackend(tender.id);
    } else {
      // Add
      const newTender = { ...tender, status: 'Found' as const };
      setCrmTenders(prev => [...prev, newTender]);
      await addOrUpdateTender(newTender);
    }
  };

  const handleSearch = async () => {
    setLoading(true);
    try {
      const catalogContext = MOCK_CATALOG.map(p => `${p.title} (${p.category})`).join(', ');
      const effectiveQuery = searchMode === 'catalog' 
        ? `Найти тендеры, где требуются товары из списка: ${catalogContext}.`
        : query;

      if (!effectiveQuery && searchMode === 'keyword') {
        setLoading(false);
        return;
      }

      const tenders = await searchTenders(effectiveQuery, catalogContext, isActive);
      setResults(tenders);
    } catch (error) {
      console.error(error);
    } finally {
      setLoading(false);
    }
  };

  const formatPrice = (price: number) => {
    return new Intl.NumberFormat('ru-RU', { style: 'currency', currency: 'RUB', maximumFractionDigits: 0 }).format(price);
  };

  return (
    <div className="p-6 max-w-7xl mx-auto h-[calc(100vh-64px)] flex flex-col relative">
      <div className="mb-6">
        <h2 className="text-2xl font-bold text-slate-900">Поиск тендеров (ЕИС)</h2>
        <p className="text-slate-500 text-sm">Используется браузерный движок Playwright для обхода защиты Zakupki.gov.ru</p>
      </div>

      {/* Floating Action Bar */}
      {crmTenders.length > 0 && (
        <div className="absolute top-6 right-6 z-10 flex gap-3">
            <button 
                onClick={() => navigate('/crm')}
                className="bg-white text-slate-700 px-4 py-3 rounded-full shadow-lg hover:bg-slate-50 transition-all flex items-center gap-2 border border-slate-200"
            >
                <Briefcase size={16} />
                <span className="text-sm font-bold">CRM: {crmTenders.length} активных</span>
            </button>
        </div>
      )}

      {/* Search UI */}
      <div className="bg-white p-4 rounded-xl border border-slate-200 shadow-sm mb-6">
        <div className="flex gap-4 mb-4">
             <button onClick={() => setSearchMode('keyword')} className={`px-4 py-2 text-sm font-medium rounded-md ${searchMode === 'keyword' ? 'bg-blue-50 text-blue-600' : 'text-slate-500'}`}>Ключевые слова</button>
             <button onClick={() => setSearchMode('catalog')} className={`px-4 py-2 text-sm font-medium rounded-md ${searchMode === 'catalog' ? 'bg-blue-50 text-blue-600' : 'text-slate-500'}`}>По каталогу</button>
        </div>
        <div className="flex gap-4">
          <div className="flex-1 relative">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" size={20} />
            <input 
              type="text" 
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder="Введите запрос..."
              className="w-full pl-10 pr-4 py-3 rounded-lg border border-slate-300 bg-white text-slate-900 placeholder-slate-400 focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
          </div>
          <button onClick={handleSearch} disabled={loading} className="px-6 py-3 bg-blue-600 text-white rounded-lg flex items-center gap-2">
            {loading ? <Loader2 className="animate-spin" /> : <Play size={20} />}
            Найти
          </button>
        </div>
      </div>

      {/* Results List */}
      <div className="flex-1 overflow-auto space-y-4 pb-6">
         {results.map((tender) => {
             const isSelected = isInCrm(tender.id);
             return (
                 <div key={tender.id} className={`relative bg-white p-5 rounded-xl border ${isSelected ? 'border-blue-500 bg-blue-50/10' : 'border-slate-200'}`}>
                    <div className="absolute top-5 right-5">
                        <button onClick={() => toggleTenderSelection(tender)} className={`flex items-center gap-2 px-3 py-1.5 rounded-lg text-sm font-medium ${isSelected ? 'bg-blue-600 text-white' : 'bg-slate-100'}`}>
                            {isSelected ? <CheckSquare size={16} /> : <Square size={16} />}
                            {isSelected ? 'В CRM' : 'В работу'}
                        </button>
                    </div>
                    <h3 className="text-lg font-bold text-slate-800 pr-24">{tender.title}</h3>
                    <p className="text-sm text-slate-600 mt-2 line-clamp-2 w-3/4">{tender.description}</p>
                    <div className="mt-4 flex justify-between items-end border-t pt-3">
                        <span className="text-xl font-bold text-slate-900">{formatPrice(tender.initial_price)}</span>
                        <div className="text-right">
                             <span className="text-xs text-slate-500 block">№ {tender.eis_number}</span>
                             {tender.url && <a href={tender.url} target="_blank" className="text-blue-600 text-sm hover:underline flex items-center gap-1 justify-end">ЕИС <ExternalLink size={12}/></a>}
                        </div>
                    </div>
                 </div>
             )
         })}
      </div>
    </div>
  );
};

export default TenderSearch;