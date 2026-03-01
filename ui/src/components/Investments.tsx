import React, { useMemo, useState } from 'react';
import { XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, PieChart, Pie, Cell, Area, AreaChart } from 'recharts';
import { TrendingUp, Loader2, Wallet, ArrowUp, ArrowDown } from 'lucide-react';
import { useAccounts, useInvestmentTransactions } from '../hooks/useBackend';
import type { Account } from '../types';

const COLORS = ['#3B82F6', '#8B5CF6', '#10B981', '#F59E0B', '#EF4444', '#06B6D4', '#EC4899', '#6366F1'];

type ViewMode = 'list' | 'allocation';

const Investments: React.FC = () => {
  const { accounts, loading } = useAccounts();
  const { transactions: investmentTxns, loading: txnLoading } = useInvestmentTransactions();
  const [viewMode, setViewMode] = useState<ViewMode>('list');

  const investmentAccounts = useMemo(
    () => accounts.filter((a) => a.account_type === 'investment'),
    [accounts],
  );

  const totalValue = useMemo(
    () => investmentAccounts.reduce((sum, a) => sum + (a.current_balance ?? 0), 0),
    [investmentAccounts],
  );

  const allocationData = useMemo(
    () =>
      investmentAccounts.map((a) => ({
        name: a.name,
        value: Math.abs(a.current_balance ?? 0),
      })),
    [investmentAccounts],
  );

  const growthData = useMemo(() => {
    if (investmentTxns.length === 0 && totalValue > 0) {
      const today = new Date();
      return [{ date: today.toLocaleDateString('en-US', { month: 'short', day: 'numeric' }), value: totalValue }];
    }

    const totalTxnAmount = investmentTxns.reduce((sum, t) => sum + t.amount, 0);
    const startingBalance = totalValue + totalTxnAmount;

    const dailyMap = new Map<string, number>();
    let running = startingBalance;

    for (const txn of investmentTxns) {
      running -= txn.amount;
      dailyMap.set(txn.date, running);
    }

    const sorted = Array.from(dailyMap.entries())
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([date, value]) => ({
        date: new Date(date).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }),
        value: Math.round(value * 100) / 100,
      }));

    if (sorted.length > 0) {
      const today = new Date();
      const todayLabel = today.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
      const last = sorted[sorted.length - 1];
      if (last.date !== todayLabel) {
        sorted.push({ date: todayLabel, value: totalValue });
      }
    }

    return sorted;
  }, [investmentTxns, totalValue]);

  if (loading) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <Loader2 className="animate-spin text-gray-400" size={40} />
      </div>
    );
  }

  return (
    <div className="flex-1 overflow-auto">
      {/* Header */}
      <header className="bg-gray-800/50 backdrop-blur-sm border-b border-gray-700/50 sticky top-0 z-10">
        <div className="px-8 py-6">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-3xl font-bold bg-gradient-to-r from-blue-400 to-purple-500 bg-clip-text text-transparent">
                Investments
              </h1>
              <p className="text-gray-400 mt-1">Track your investment portfolio</p>
            </div>
            <div className="flex items-center space-x-2">
              <button
                onClick={() => setViewMode('list')}
                className={`px-4 py-2 text-sm rounded-lg transition-all ${
                  viewMode === 'list'
                    ? 'bg-gradient-to-r from-blue-600 to-purple-600 text-white shadow-lg'
                    : 'bg-gray-700 text-gray-300 hover:bg-gray-600'
                }`}
              >
                Account List
              </button>
              <button
                onClick={() => setViewMode('allocation')}
                className={`px-4 py-2 text-sm rounded-lg transition-all ${
                  viewMode === 'allocation'
                    ? 'bg-gradient-to-r from-blue-600 to-purple-600 text-white shadow-lg'
                    : 'bg-gray-700 text-gray-300 hover:bg-gray-600'
                }`}
              >
                Allocation
              </button>
            </div>
          </div>
        </div>
      </header>

      <div className="p-8">
        {/* Summary Cards */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
          <div className="bg-gradient-to-br from-gray-800 to-gray-800/50 rounded-2xl p-6 border border-gray-700/50">
            <div className="flex items-center justify-between mb-3">
              <span className="text-sm text-gray-400">Total Portfolio</span>
              <div className="p-2 bg-blue-500/20 rounded-lg border border-blue-500/30">
                <Wallet className="text-blue-400" size={18} />
              </div>
            </div>
            <div className="text-2xl font-bold text-white">
              ${totalValue.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
            </div>
          </div>

          <div className="bg-gradient-to-br from-gray-800 to-gray-800/50 rounded-2xl p-6 border border-gray-700/50">
            <div className="flex items-center justify-between mb-3">
              <span className="text-sm text-gray-400">Accounts</span>
              <div className="p-2 bg-purple-500/20 rounded-lg border border-purple-500/30">
                <TrendingUp className="text-purple-400" size={18} />
              </div>
            </div>
            <div className="text-2xl font-bold text-white">{investmentAccounts.length}</div>
          </div>

          <div className="bg-gradient-to-br from-gray-800 to-gray-800/50 rounded-2xl p-6 border border-gray-700/50">
            <div className="flex items-center justify-between mb-3">
              <span className="text-sm text-gray-400">Avg per Account</span>
              <div className="p-2 bg-green-500/20 rounded-lg border border-green-500/30">
                <TrendingUp className="text-green-400" size={18} />
              </div>
            </div>
            <div className="text-2xl font-bold text-white">
              ${investmentAccounts.length > 0
                ? (totalValue / investmentAccounts.length).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })
                : '0.00'}
            </div>
          </div>
        </div>

        {investmentAccounts.length === 0 ? (
          <div className="bg-gradient-to-br from-gray-800 to-gray-800/50 rounded-2xl p-12 border border-gray-700/50 text-center">
            <TrendingUp size={64} className="mx-auto mb-4 text-gray-600" />
            <h3 className="text-xl font-bold text-white mb-2">No Investment Accounts</h3>
            <p className="text-gray-400 max-w-md mx-auto">
              Connect a brokerage or investment account through Plaid to see your portfolio here.
            </p>
          </div>
        ) : viewMode === 'list' ? (
          <>
            {/* Account List */}
            <div className="bg-gradient-to-br from-gray-800 to-gray-800/50 rounded-2xl border border-gray-700/50 mb-8 overflow-hidden">
              <div className="px-6 py-4 border-b border-gray-700/50">
                <h3 className="text-lg font-bold text-white">Investment Accounts</h3>
              </div>
              <div className="divide-y divide-gray-700/30">
                {investmentAccounts.map((account, index) => (
                  <AccountRow key={account.id} account={account} color={COLORS[index % COLORS.length]} totalValue={totalValue} />
                ))}
              </div>
            </div>

            {/* Portfolio Growth Line Chart */}
            <div className="bg-gradient-to-br from-gray-800 to-gray-800/50 rounded-2xl p-6 border border-gray-700/50">
              <h3 className="text-lg font-bold text-white mb-6">Portfolio Growth Over Time</h3>
              {txnLoading ? (
                <div className="h-80 flex items-center justify-center">
                  <Loader2 className="animate-spin text-gray-400" size={32} />
                </div>
              ) : growthData.length <= 1 ? (
                <div className="h-80 flex flex-col items-center justify-center text-gray-500">
                  <TrendingUp size={48} className="mb-3 opacity-30" />
                  <p>Not enough transaction history to chart growth yet</p>
                  <p className="text-sm mt-1">Sync your accounts to build a timeline</p>
                </div>
              ) : (
                <div className="h-80">
                  <ResponsiveContainer width="100%" height="100%">
                    <AreaChart data={growthData}>
                      <defs>
                        <linearGradient id="colorGrowth" x1="0" y1="0" x2="0" y2="1">
                          <stop offset="5%" stopColor="#3B82F6" stopOpacity={0.6} />
                          <stop offset="95%" stopColor="#3B82F6" stopOpacity={0.05} />
                        </linearGradient>
                      </defs>
                      <CartesianGrid strokeDasharray="3 3" stroke="#374151" strokeOpacity={0.3} />
                      <XAxis
                        dataKey="date"
                        stroke="#9CA3AF"
                        fontSize={12}
                        tickLine={false}
                      />
                      <YAxis
                        stroke="#9CA3AF"
                        fontSize={12}
                        tickLine={false}
                        tickFormatter={(v: number) => `$${(v / 1000).toFixed(0)}k`}
                      />
                      <Tooltip
                        contentStyle={{
                          backgroundColor: '#1F2937',
                          border: '1px solid #374151',
                          borderRadius: '12px',
                          padding: '12px',
                        }}
                        labelStyle={{ color: '#9CA3AF', fontSize: '12px' }}
                        formatter={(value: number | undefined) => [`$${(value ?? 0).toLocaleString(undefined, { minimumFractionDigits: 2 })}`, 'Portfolio Value']}
                      />
                      <Area
                        type="monotone"
                        dataKey="value"
                        stroke="#3B82F6"
                        strokeWidth={3}
                        fill="url(#colorGrowth)"
                        dot={{ fill: '#3B82F6', strokeWidth: 2, r: 4 }}
                        activeDot={{ r: 6, fill: '#3B82F6' }}
                      />
                    </AreaChart>
                  </ResponsiveContainer>
                </div>
              )}
            </div>
          </>
        ) : (
          /* Allocation Pie Chart */
          <div className="bg-gradient-to-br from-gray-800 to-gray-800/50 rounded-2xl p-6 border border-gray-700/50">
            <h3 className="text-lg font-bold text-white mb-6">Portfolio Allocation</h3>
            <div className="flex flex-col xl:flex-row items-center gap-8">
              <div className="h-80 w-full xl:w-1/2">
                <ResponsiveContainer width="100%" height="100%">
                  <PieChart>
                    <Pie
                      data={allocationData}
                      cx="50%"
                      cy="50%"
                      innerRadius={70}
                      outerRadius={120}
                      paddingAngle={3}
                      dataKey="value"
                    >
                      {allocationData.map((_, index) => (
                        <Cell key={index} fill={COLORS[index % COLORS.length]} />
                      ))}
                    </Pie>
                    <Tooltip
                      contentStyle={{
                        backgroundColor: '#1F2937',
                        border: '1px solid #374151',
                        borderRadius: '12px',
                        padding: '12px',
                      }}
                      formatter={(value: number | undefined) => [`$${(value ?? 0).toLocaleString(undefined, { minimumFractionDigits: 2 })}`, 'Value']}
                    />
                  </PieChart>
                </ResponsiveContainer>
              </div>

              <div className="w-full xl:w-1/2 space-y-3">
                {allocationData.map((item, index) => {
                  const pct = totalValue > 0 ? (item.value / totalValue) * 100 : 0;
                  return (
                    <div key={item.name} className="flex items-center justify-between p-3 bg-gray-700/30 rounded-xl border border-gray-600/30">
                      <div className="flex items-center space-x-3">
                        <div className="w-3 h-3 rounded-full" style={{ backgroundColor: COLORS[index % COLORS.length] }} />
                        <span className="text-sm font-medium text-white">{item.name}</span>
                      </div>
                      <div className="text-right">
                        <div className="text-sm font-bold text-white">
                          ${item.value.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                        </div>
                        <div className="text-xs text-gray-400">{pct.toFixed(1)}%</div>
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

interface AccountRowProps {
  account: Account;
  color: string;
  totalValue: number;
}

const AccountRow: React.FC<AccountRowProps> = ({ account, color, totalValue }) => {
  const balance = account.current_balance ?? 0;
  const pct = totalValue > 0 ? (Math.abs(balance) / totalValue) * 100 : 0;

  return (
    <div className="flex items-center px-6 py-4 hover:bg-gray-700/30 transition-colors group">
      <div className="w-3 h-3 rounded-full mr-4 flex-shrink-0" style={{ backgroundColor: color }} />
      <div className="flex-1 min-w-0">
        <div className="text-white font-medium group-hover:text-blue-400 transition-colors truncate">
          {account.name}
        </div>
        <div className="text-xs text-gray-500">
          {account.official_name || account.account_subtype || 'Investment'}
          {account.mask && <span> •• {account.mask}</span>}
        </div>
      </div>
      <div className="text-right ml-4">
        <div className="flex items-center justify-end space-x-1">
          {balance >= 0 ? (
            <ArrowUp size={14} className="text-green-400" />
          ) : (
            <ArrowDown size={14} className="text-red-400" />
          )}
          <span className="text-white font-bold">
            ${Math.abs(balance).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
          </span>
        </div>
        <div className="text-xs text-gray-400">{pct.toFixed(1)}% of portfolio</div>
      </div>
    </div>
  );
};

export default Investments;
