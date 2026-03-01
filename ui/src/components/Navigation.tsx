import React from 'react';
import { Home, PieChart, Clock, HelpCircle, Settings, ChevronDown } from 'lucide-react';
import PlaidLinkButton from './PlaidLink';
import { useAccounts } from '../hooks/useBackend';
import type { Account } from '../types';

export type Page = 'dashboard' | 'investments' | 'retirement';

interface NavigationProps {
  activePage: Page;
  onNavigate: (page: Page) => void;
  onAccountsChanged?: () => void;
}

const Navigation: React.FC<NavigationProps> = ({ activePage, onNavigate, onAccountsChanged }) => {
  const { accounts, refresh: refreshAccounts } = useAccounts();

  const menuItems: { icon: typeof Home; label: string; page: Page }[] = [
    { icon: Home, label: 'Dashboard', page: 'dashboard' },
    { icon: PieChart, label: 'Investments', page: 'investments' },
    { icon: Clock, label: 'Retirement', page: 'retirement' },
  ];

  const bottomItems = [
    { icon: Home, label: 'Start here' },
    { icon: HelpCircle, label: 'Get help' },
    { icon: Settings, label: 'Settings' },
  ];

  const groupedAccounts = accounts.reduce<Record<string, Account[]>>((groups, account) => {
    const type = account.account_type === 'credit' ? 'Credit cards' : 'Depository';
    if (!groups[type]) groups[type] = [];
    groups[type].push(account);
    return groups;
  }, {});

  const typeColors: Record<string, string> = {
    'Credit cards': 'text-purple-400',
    'Depository': 'text-green-400',
  };

  const handlePlaidSuccess = () => {
    refreshAccounts();
    onAccountsChanged?.();
  };

  return (
    <div className="w-72 bg-gradient-to-b from-gray-900 to-gray-800 border-r border-gray-700 p-6 flex flex-col shadow-2xl">
      <div className="mb-8">
        <h2 className="text-2xl font-bold mb-2 bg-gradient-to-r from-blue-400 to-purple-500 bg-clip-text text-transparent">
          AlgoFlow
        </h2>
        <p className="text-xs text-gray-400">Smart Financial Management</p>
      </div>
      
      <nav className="mb-8">
        <div className="space-y-1">
          {menuItems.map((item) => {
            const isActive = item.page === activePage;
            return (
              <button
                key={item.page}
                onClick={() => onNavigate(item.page)}
                className={`w-full flex items-center space-x-3 px-4 py-3 rounded-xl transition-all duration-200 group ${
                  isActive
                    ? 'bg-gradient-to-r from-blue-600 to-blue-700 text-white shadow-lg transform scale-105' 
                    : 'text-gray-300 hover:bg-gray-700 hover:text-white hover:translate-x-1'
                }`}
              >
                <div className={`transition-transform duration-200 ${isActive ? 'scale-110' : 'group-hover:scale-110'}`}>
                  <item.icon size={20} />
                </div>
                <span className="font-medium">{item.label}</span>
                {isActive && (
                  <div className="ml-auto w-2 h-2 bg-white rounded-full animate-pulse"></div>
                )}
              </button>
            );
          })}
        </div>
      </nav>

      <div className="mb-4 flex-1 overflow-y-auto">
        <h3 className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-4 flex items-center">
          MY ACCOUNTS
          <ChevronDown size={14} className="ml-2" />
        </h3>

        {accounts.length === 0 ? (
          <div className="text-sm text-gray-500 mb-4 px-2">
            No accounts connected yet. Connect a bank to get started.
          </div>
        ) : (
          Object.entries(groupedAccounts).map(([type, accts]) => (
            <div key={type} className="mb-6">
              <div className={`text-sm font-semibold mb-3 ${typeColors[type] || 'text-gray-400'} flex items-center`}>
                <div className="w-2 h-2 bg-current rounded-full mr-2"></div>
                {type}
              </div>
              <div className="space-y-2 ml-4">
                {accts.map((account) => (
                  <div
                    key={account.id}
                    className="flex items-center justify-between text-sm px-3 py-2 rounded-lg bg-gray-700/50 hover:bg-gray-700 transition-colors cursor-pointer hover:text-gray-200"
                  >
                    <span className="text-gray-400 truncate mr-2">
                      {account.name}
                      {account.mask && <span className="text-gray-500"> ••{account.mask}</span>}
                    </span>
                    {account.current_balance != null && (
                      <span className="text-gray-300 font-medium whitespace-nowrap">
                        ${Math.abs(account.current_balance).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                      </span>
                    )}
                  </div>
                ))}
              </div>
            </div>
          ))
        )}

        <div className="mt-4">
          <PlaidLinkButton onSuccess={handlePlaidSuccess} />
        </div>
      </div>

      <div className="border-t border-gray-700 pt-6">
        <nav className="space-y-1">
          {bottomItems.map((item, index) => (
            <button
              key={index}
              className="w-full flex items-center space-x-3 px-4 py-3 rounded-xl text-gray-400 hover:bg-gray-700 hover:text-white transition-all duration-200 hover:translate-x-1"
            >
              <item.icon size={18} />
              <span className="text-sm font-medium">{item.label}</span>
            </button>
          ))}
        </nav>
      </div>
    </div>
  );
};

export default Navigation;
