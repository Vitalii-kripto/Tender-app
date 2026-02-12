import React, { useState, useEffect } from 'react';
import { Building2, Users, Save, Trash2, Plus, FileText, Upload, Briefcase, Pencil, X } from 'lucide-react';
import { CompanyProfile, Employee, CompanyDocument } from '../types';
import { getCompanyProfile, saveCompanyProfile, getEmployees, saveEmployee, deleteEmployee } from '../services/geminiService';

const Settings = () => {
  const [activeTab, setActiveTab] = useState<'company' | 'team'>('company');
  const [company, setCompany] = useState<CompanyProfile>(getCompanyProfile());
  const [employees, setEmployees] = useState<Employee[]>(getEmployees());
  
  // Form State
  const [editingId, setEditingId] = useState<string | null>(null);
  const [newEmployeeName, setNewEmployeeName] = useState('');
  const [newEmployeeEmail, setNewEmployeeEmail] = useState('');
  const [newEmployeeRole, setNewEmployeeRole] = useState('manager');

  useEffect(() => {
    // Refresh data on mount
    setCompany(getCompanyProfile());
    setEmployees(getEmployees());
  }, []);

  const handleSaveCompany = () => {
    saveCompanyProfile(company);
    alert('Данные компании сохранены!');
  };

  const handleAddDocument = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files && e.target.files.length > 0) {
        const file = e.target.files[0];
        const newDoc: CompanyDocument = {
            id: Date.now().toString(),
            name: file.name,
            type: 'other',
            uploadDate: new Date().toLocaleDateString(),
            size: `${(file.size / 1024).toFixed(1)} KB`
        };
        const updated = { ...company, documents: [...company.documents, newDoc] };
        setCompany(updated);
        saveCompanyProfile(updated);
    }
  };

  const handleDeleteDoc = (id: string) => {
      const updated = { ...company, documents: company.documents.filter(d => d.id !== id) };
      setCompany(updated);
      saveCompanyProfile(updated);
  };

  const handleEditClick = (emp: Employee) => {
      setEditingId(emp.id);
      setNewEmployeeName(emp.name);
      setNewEmployeeEmail(emp.email);
      setNewEmployeeRole(emp.role);
  };

  const handleCancelEdit = () => {
      setEditingId(null);
      setNewEmployeeName('');
      setNewEmployeeEmail('');
      setNewEmployeeRole('manager');
  };

  const handleSaveEmployee = (e: React.FormEvent) => {
    e.preventDefault();
    if (!newEmployeeName || !newEmployeeEmail) return;

    const empData: Employee = {
        id: editingId || `emp_${Date.now()}`,
        name: newEmployeeName,
        email: newEmployeeEmail,
        role: newEmployeeRole
    };
    
    saveEmployee(empData);
    
    if (editingId) {
        setEmployees(prev => prev.map(e => e.id === editingId ? empData : e));
    } else {
        setEmployees(prev => [...prev, empData]);
    }

    handleCancelEdit();
  };

  const handleDeleteEmployee = (id: string) => {
      if (confirm('Удалить сотрудника?')) {
          deleteEmployee(id);
          setEmployees(prev => prev.filter(e => e.id !== id));
          if (editingId === id) handleCancelEdit();
      }
  };

  const inputClasses = "w-full border border-slate-300 rounded-lg p-2 text-sm bg-white text-slate-900 focus:outline-none focus:ring-2 focus:ring-blue-500 placeholder-slate-400";

  return (
    <div className="p-6 max-w-5xl mx-auto pb-20">
      <div className="mb-8">
        <h2 className="text-2xl font-bold text-slate-900 flex items-center gap-2">
          Настройки системы
        </h2>
        <p className="text-slate-500 text-sm">Управление реквизитами, документами и командой.</p>
      </div>

      <div className="flex gap-6 flex-col md:flex-row">
          {/* Sidebar Tabs */}
          <div className="w-full md:w-64 flex flex-col gap-2">
              <button 
                onClick={() => setActiveTab('company')}
                className={`flex items-center gap-3 px-4 py-3 rounded-lg text-sm font-medium transition-colors ${activeTab === 'company' ? 'bg-white shadow text-blue-600' : 'text-slate-500 hover:bg-slate-100'}`}
              >
                  <Building2 size={18} />
                  Профиль компании
              </button>
              <button 
                onClick={() => setActiveTab('team')}
                className={`flex items-center gap-3 px-4 py-3 rounded-lg text-sm font-medium transition-colors ${activeTab === 'team' ? 'bg-white shadow text-blue-600' : 'text-slate-500 hover:bg-slate-100'}`}
              >
                  <Users size={18} />
                  Управление командой
              </button>
          </div>

          {/* Content Area */}
          <div className="flex-1 bg-white rounded-xl border border-slate-200 shadow-sm p-6 min-h-[600px]">
              
              {activeTab === 'company' && (
                  <div className="space-y-8 animate-in fade-in">
                      <div>
                          <h3 className="text-lg font-bold text-slate-800 mb-4 border-b pb-2">Реквизиты организации</h3>
                          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                              <div>
                                  <label className="block text-sm font-medium text-slate-700 mb-1">Название организации</label>
                                  <input type="text" className={inputClasses} value={company.name} onChange={e => setCompany({...company, name: e.target.value})} />
                              </div>
                              <div>
                                  <label className="block text-sm font-medium text-slate-700 mb-1">Генеральный директор</label>
                                  <input type="text" className={inputClasses} value={company.ceo} onChange={e => setCompany({...company, ceo: e.target.value})} />
                              </div>
                              <div>
                                  <label className="block text-sm font-medium text-slate-700 mb-1">ИНН</label>
                                  <input type="text" className={inputClasses} value={company.inn} onChange={e => setCompany({...company, inn: e.target.value})} />
                              </div>
                              <div>
                                  <label className="block text-sm font-medium text-slate-700 mb-1">КПП</label>
                                  <input type="text" className={inputClasses} value={company.kpp} onChange={e => setCompany({...company, kpp: e.target.value})} />
                              </div>
                              <div className="md:col-span-2">
                                  <label className="block text-sm font-medium text-slate-700 mb-1">Юридический адрес</label>
                                  <input type="text" className={inputClasses} value={company.address} onChange={e => setCompany({...company, address: e.target.value})} />
                              </div>
                          </div>
                          <div className="mt-4 flex justify-end">
                              <button onClick={handleSaveCompany} className="flex items-center gap-2 bg-blue-600 text-white px-4 py-2 rounded-lg text-sm font-medium hover:bg-blue-700">
                                  <Save size={16} /> Сохранить изменения
                              </button>
                          </div>
                      </div>

                      <div>
                          <h3 className="text-lg font-bold text-slate-800 mb-4 border-b pb-2 flex justify-between items-center">
                              <span>Пакет документов</span>
                              <div className="relative overflow-hidden inline-block">
                                  <button className="flex items-center gap-2 text-blue-600 text-sm font-medium hover:bg-blue-50 px-3 py-1.5 rounded transition-colors">
                                      <Upload size={16} /> Загрузить
                                  </button>
                                  <input type="file" className="absolute inset-0 opacity-0 cursor-pointer" onChange={handleAddDocument} />
                              </div>
                          </h3>
                          <p className="text-xs text-slate-500 mb-4">
                              Документы (Устав, ЕГРЮЛ, Бух. баланс) используются для автоматического формирования заявок и проверки требований.
                          </p>
                          
                          {company.documents.length === 0 ? (
                              <div className="text-center py-8 text-slate-400 bg-slate-50 rounded-lg border border-dashed border-slate-200">
                                  <FileText size={32} className="mx-auto mb-2 opacity-50" />
                                  <p className="text-sm">Нет загруженных документов</p>
                              </div>
                          ) : (
                              <div className="space-y-2">
                                  {company.documents.map(doc => (
                                      <div key={doc.id} className="flex items-center justify-between p-3 bg-slate-50 border border-slate-100 rounded-lg">
                                          <div className="flex items-center gap-3">
                                              <div className="bg-white p-2 rounded border border-slate-200 text-blue-500">
                                                  <FileText size={18} />
                                              </div>
                                              <div>
                                                  <p className="text-sm font-medium text-slate-800">{doc.name}</p>
                                                  <p className="text-xs text-slate-500">{doc.uploadDate} • {doc.size}</p>
                                              </div>
                                          </div>
                                          <button onClick={() => handleDeleteDoc(doc.id)} className="text-slate-400 hover:text-red-500 p-2">
                                              <Trash2 size={16} />
                                          </button>
                                      </div>
                                  ))}
                              </div>
                          )}
                      </div>
                  </div>
              )}

              {activeTab === 'team' && (
                  <div className="space-y-8 animate-in fade-in">
                       <div>
                          <h3 className="text-lg font-bold text-slate-800 mb-4 border-b pb-2">
                              {editingId ? 'Редактировать сотрудника' : 'Добавить сотрудника'}
                          </h3>
                          <form onSubmit={handleSaveEmployee} className="flex flex-col md:flex-row gap-4 items-end bg-slate-50 p-4 rounded-lg border border-slate-100">
                              <div className="flex-1 w-full">
                                  <label className="block text-xs font-bold text-slate-500 mb-1">ФИО</label>
                                  <input type="text" required className={inputClasses} placeholder="Иванов Иван" value={newEmployeeName} onChange={e => setNewEmployeeName(e.target.value)} />
                              </div>
                              <div className="flex-1 w-full">
                                  <label className="block text-xs font-bold text-slate-500 mb-1">Email</label>
                                  <input type="email" required className={inputClasses} placeholder="user@company.com" value={newEmployeeEmail} onChange={e => setNewEmployeeEmail(e.target.value)} />
                              </div>
                              <div className="w-full md:w-40">
                                  <label className="block text-xs font-bold text-slate-500 mb-1">Роль</label>
                                  <select className={inputClasses} value={newEmployeeRole} onChange={e => setNewEmployeeRole(e.target.value)}>
                                      <option value="manager">Менеджер</option>
                                      <option value="analyst">Аналитик</option>
                                      <option value="admin">Админ</option>
                                  </select>
                              </div>
                              <div className="flex gap-2 w-full md:w-auto">
                                  <button type="submit" className="flex-1 md:flex-none bg-blue-600 text-white px-4 py-2 rounded-lg text-sm font-medium hover:bg-blue-700 flex items-center justify-center gap-2 min-w-[120px]">
                                      {editingId ? <Save size={16} /> : <Plus size={16} />}
                                      {editingId ? 'Сохранить' : 'Добавить'}
                                  </button>
                                  {editingId && (
                                      <button type="button" onClick={handleCancelEdit} className="bg-slate-200 text-slate-600 px-3 py-2 rounded-lg hover:bg-slate-300">
                                          <X size={18} />
                                      </button>
                                  )}
                              </div>
                          </form>
                       </div>

                       <div>
                           <h3 className="text-lg font-bold text-slate-800 mb-4">Список сотрудников</h3>
                           <div className="overflow-hidden border border-slate-200 rounded-lg">
                               <table className="w-full text-sm text-left">
                                   <thead className="bg-slate-50 text-slate-500 font-medium border-b border-slate-200">
                                       <tr>
                                           <th className="px-4 py-3">Сотрудник</th>
                                           <th className="px-4 py-3">Роль</th>
                                           <th className="px-4 py-3">Email</th>
                                           <th className="px-4 py-3 text-right">Действия</th>
                                       </tr>
                                   </thead>
                                   <tbody className="divide-y divide-slate-100">
                                       {employees.map(emp => (
                                           <tr key={emp.id} className={`transition-colors ${editingId === emp.id ? 'bg-blue-50' : 'hover:bg-slate-50'}`}>
                                               <td className="px-4 py-3 font-medium text-slate-800 flex items-center gap-2">
                                                   <div className="w-8 h-8 rounded-full bg-blue-100 text-blue-600 flex items-center justify-center font-bold text-xs border border-blue-200">
                                                       {emp.name.substring(0,2).toUpperCase()}
                                                   </div>
                                                   {emp.name}
                                               </td>
                                               <td className="px-4 py-3">
                                                   <span className={`px-2 py-0.5 rounded text-xs font-bold uppercase ${
                                                       emp.role === 'admin' ? 'bg-purple-100 text-purple-700' :
                                                       emp.role === 'manager' ? 'bg-emerald-100 text-emerald-700' : 'bg-blue-100 text-blue-700'
                                                   }`}>
                                                       {emp.role}
                                                   </span>
                                               </td>
                                               <td className="px-4 py-3 text-slate-500">{emp.email}</td>
                                               <td className="px-4 py-3 text-right">
                                                   <div className="flex justify-end gap-1">
                                                       <button 
                                                            onClick={() => handleEditClick(emp)} 
                                                            className="text-slate-400 hover:text-blue-600 p-1.5 hover:bg-blue-50 rounded"
                                                            title="Редактировать"
                                                       >
                                                           <Pencil size={16} />
                                                       </button>
                                                       <button 
                                                            onClick={() => handleDeleteEmployee(emp.id)} 
                                                            className="text-slate-400 hover:text-red-600 p-1.5 hover:bg-red-50 rounded"
                                                            title="Удалить"
                                                       >
                                                           <Trash2 size={16} />
                                                       </button>
                                                   </div>
                                               </td>
                                           </tr>
                                       ))}
                                   </tbody>
                               </table>
                           </div>
                       </div>
                  </div>
              )}

          </div>
      </div>
    </div>
  );
};

export default Settings;
