import React, { useMemo } from 'react';
import MonthlySpending from './MonthlySpending';
import NetWorth from './NetWorth';
import TransactionsReview from './TransactionsReview';
import TopCategories from './TopCategories';
import { RefreshCw, TrendingUp, CreditCard, Landmark } from 'lucide-react';
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

  const netWorth = useMemo(() => {
    if (!data) return 0;
    const assets = data.accounts
      .filter((a) => a.account_type === 'depository' || a.account_type === 'investment')
      .reduce((s, a) => s + (a.current_balance ?? 0), 0);
    return assets - data.total_credit_debt;
  }, [data]);

  const today = new Date().toLocaleDateString('en-US', {
    weekday: 'long', month: 'long', day: 'numeric',
  });

  const fmt = (n: number) =>
    n.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });

  return (
    <div className="flex-1 overflow-auto">
      {/* Header */}
      <header className="bg-gray-800/50 backdrop-blur-sm border-b border-gray-700/50 sticky top-0 z-10">
        <div className="px-8 py-5 flex items-center justify-between">
          <div>
            <h1 className="text-3xl font-bold bg-gradient-to-r from-blue-400 to-purple-500 bg-clip-text text-transparent">
              Dashboard
            </h1>
            <p className="text-sm text-gray-400 mt-0.5">{today}</p>
          </div>
          <button
            onClick={handleSync}
            disabled={syncing}
            className="flex items-center space-x-2 px-4 py-2 rounded-lg bg-gray-700 hover:bg-gray-600 transition-colors disabled:opacity-50"
          >
            <RefreshCw size={16} className={syncing ? 'animate-spin' : ''} />
            <span className="text-sm">{syncing ? 'Syncing…' : 'Sync'}</span>
          </button>
        </div>
      </header>

      {/* Summary stats */}
      <div className="px-8 pt-6 pb-0">
        <div className="grid grid-cols-3 gap-4">
          <div className="bg-gray-800/60 border border-gray-700/50 rounded-2xl p-5 flex items-center space-x-4">
            <div className="p-2.5 bg-blue-500/20 rounded-xl border border-blue-500/30">
              <Landmark size={20} className="text-blue-400" />
            </div>
            <div>
              <p className="text-xs text-gray-400 mb-0.5">Total Balance</p>
              <p className="text-xl font-bold text-white">
                {loading ? '—' : `$${fmt(data?.total_balance ?? 0)}`}
              </p>
            </div>
          </div>

          <div className="bg-gray-800/60 border border-gray-700/50 rounded-2xl p-5 flex items-center space-x-4">
            <div className="p-2.5 bg-red-500/20 rounded-xl border border-red-500/30">
              <CreditCard size={20} className="text-red-400" />
            </div>
            <div>
              <p className="text-xs text-gray-400 mb-0.5">Credit Debt</p>
              <p className="text-xl font-bold text-red-400">
                {loading ? '—' : `$${fmt(data?.total_credit_debt ?? 0)}`}
              </p>
            </div>
          </div>

          <div className="bg-gray-800/60 border border-gray-700/50 rounded-2xl p-5 flex items-center space-x-4">
            <div className="p-2.5 bg-green-500/20 rounded-xl border border-green-500/30">
              <TrendingUp size={20} className="text-green-400" />
            </div>
            <div>
              <p className="text-xs text-gray-400 mb-0.5">Net Worth</p>
              <p className={`text-xl font-bold ${netWorth >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                {loading ? '—' : `$${fmt(netWorth)}`}
              </p>
            </div>
          </div>
        </div>
      </div>

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
