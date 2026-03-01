import React from 'react';
import MonthlySpending from './MonthlySpending';
import NetWorth from './NetWorth';
import TransactionsReview from './TransactionsReview';
import TopCategories from './TopCategories';
import { Search, Bell, User, RefreshCw } from 'lucide-react';
import { useDashboard, syncAllAccounts } from '../hooks/useBackend';

const Dashboard: React.FC = () => {
  const { data, loading, refresh } = useDashboard();
  const [syncing, setSyncing] = React.useState(false);

  const handleSync = async () => {
    try {
      setSyncing(true);
      await syncAllAccounts();
      await refresh();
    } catch (err) {
      console.error('Sync failed:', err);
    } finally {
      setSyncing(false);
    }
  };

  return (
    <div className="flex-1 overflow-auto">
      {/* Header */}
      <header className="bg-gray-800/50 backdrop-blur-sm border-b border-gray-700/50 sticky top-0 z-10">
        <div className="px-8 py-6">
          <div className="flex items-center justify-between mb-6">
            <div>
              <h1 className="text-3xl font-bold bg-gradient-to-r from-blue-400 to-purple-500 bg-clip-text text-transparent">
                Dashboard
              </h1>
              <p className="text-gray-400 mt-1">Welcome back! Here's your financial overview</p>
            </div>
            <div className="flex items-center space-x-4">
              <button
                onClick={handleSync}
                disabled={syncing}
                className="flex items-center space-x-2 px-4 py-2 rounded-lg bg-gray-700 hover:bg-gray-600 transition-colors disabled:opacity-50"
              >
                <RefreshCw size={16} className={syncing ? 'animate-spin' : ''} />
                <span className="text-sm">{syncing ? 'Syncing...' : 'Sync'}</span>
              </button>
              <button className="relative p-2 rounded-lg bg-gray-700 hover:bg-gray-600 transition-colors">
                <Bell size={20} />
                <div className="absolute top-1 right-1 w-2 h-2 bg-red-500 rounded-full"></div>
              </button>
              <button className="p-2 rounded-lg bg-gray-700 hover:bg-gray-600 transition-colors">
                <User size={20} />
              </button>
            </div>
          </div>
          
          <div className="relative max-w-md">
            <Search className="absolute left-4 top-1/2 transform -translate-y-1/2 text-gray-400" size={20} />
            <input
              type="text"
              placeholder="Search transactions, categories, or accounts..."
              className="w-full pl-12 pr-4 py-3 bg-gray-700/50 border border-gray-600 rounded-xl focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all"
            />
          </div>
        </div>
      </header>
      
      {/* Main Content */}
      <div className="p-8">
        <div className="grid grid-cols-1 xl:grid-cols-2 gap-6 mb-6">
          <MonthlySpending
            transactions={data?.recent_transactions ?? []}
            loading={loading}
          />
          <NetWorth
            accounts={data?.accounts ?? []}
            totalBalance={data?.total_balance ?? 0}
            loading={loading}
          />
        </div>
        
        <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
          <TransactionsReview
            transactions={data?.recent_transactions ?? []}
            loading={loading}
          />
          <TopCategories
            categories={data?.categories ?? []}
            loading={loading}
          />
        </div>
      </div>
    </div>
  );
};

export default Dashboard;
