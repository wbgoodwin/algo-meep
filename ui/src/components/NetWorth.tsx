import React, { useMemo, useState } from 'react';
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import { Wallet, ArrowUp, ArrowDown, Loader2 } from 'lucide-react';
import { useNetWorthHistory } from '../hooks/useBackend';
import type { Account } from '../types';

interface NetWorthProps {
  accounts: Account[];
  totalBalance: number;
  loading: boolean;
}

type Period = '1M' | '3M' | '6M' | '1Y' | 'ALL';

function daysAgo(days: number): string {
  const d = new Date();
  d.setDate(d.getDate() - days);
  return d.toISOString().slice(0, 10);
}

const NetWorth: React.FC<NetWorthProps> = ({ accounts, loading }) => {
  const [selectedPeriod, setSelectedPeriod] = useState<Period>('ALL');
  const { snapshots, loading: histLoading } = useNetWorthHistory();

  const periods: Period[] = ['1M', '3M', '6M', '1Y', 'ALL'];

  const totalAssets = accounts
    .filter((a) => a.account_type === 'depository' || a.account_type === 'investment')
    .reduce((sum, a) => sum + (a.current_balance ?? 0), 0);

  const totalDebt = accounts
    .filter((a) => a.account_type === 'credit' || a.account_type === 'loan')
    .reduce((sum, a) => sum + (a.current_balance ?? 0), 0);

  const netWorth = totalAssets - totalDebt;

  const filteredSnapshots = useMemo(() => {
    if (selectedPeriod === 'ALL') return snapshots;
    const cutoffDays: Record<Period, number> = { '1M': 30, '3M': 90, '6M': 180, '1Y': 365, 'ALL': Infinity };
    const cutoff = daysAgo(cutoffDays[selectedPeriod]);
    return snapshots.filter((s) => s.snapshot_date >= cutoff);
  }, [snapshots, selectedPeriod]);

  const chartData = useMemo(() => {
    const points = filteredSnapshots.map((s) => ({
      date: new Date(s.snapshot_date).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }),
      netWorth: s.net_worth,
      assets: s.total_assets,
      liabilities: s.total_liabilities,
    }));

    const todayLabel = new Date().toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
    const last = points[points.length - 1];
    if (!last || last.date !== todayLabel) {
      points.push({ date: todayLabel, netWorth, assets: totalAssets, liabilities: totalDebt });
    }

    return points;
  }, [filteredSnapshots, netWorth, totalAssets, totalDebt]);

  const change = useMemo(() => {
    if (chartData.length < 2) return null;
    const first = chartData[0].netWorth;
    const latest = chartData[chartData.length - 1].netWorth;
    const diff = latest - first;
    const pct = first !== 0 ? (diff / Math.abs(first)) * 100 : 0;
    return { diff, pct };
  }, [chartData]);

  if (loading) {
    return (
      <div className="bg-gradient-to-br from-gray-800 to-gray-800/50 rounded-2xl p-6 border border-gray-700/50 flex items-center justify-center h-96">
        <Loader2 className="animate-spin text-gray-400" size={32} />
      </div>
    );
  }

  return (
    <div className="bg-gradient-to-br from-gray-800 to-gray-800/50 rounded-2xl p-6 border border-gray-700/50 hover:border-gray-600/50 transition-all duration-300 hover:shadow-2xl hover:shadow-blue-500/10">
      <div className="flex items-center justify-between mb-6">
        <div>
          <h3 className="text-xl font-bold text-white mb-1">Net Worth</h3>
          <p className="text-sm text-gray-400">Track your wealth growth</p>
        </div>
        <div className="p-3 bg-gradient-to-r from-blue-500/20 to-purple-600/20 rounded-xl border border-blue-500/30">
          <Wallet className="text-blue-400" size={24} />
        </div>
      </div>
      
      <div className="flex justify-between items-center mb-6">
        <div>
          <div className="flex items-baseline space-x-3 mb-2">
            <div className="text-3xl font-bold text-white">
              ${netWorth.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
            </div>
            {change && (
              <div className={`flex items-center text-sm px-2 py-1 rounded-lg ${
                change.diff >= 0
                  ? 'text-green-400 bg-green-500/10'
                  : 'text-red-400 bg-red-500/10'
              }`}>
                {change.diff >= 0 ? <ArrowUp size={14} className="mr-1" /> : <ArrowDown size={14} className="mr-1" />}
                {change.diff >= 0 ? '+' : ''}${Math.abs(change.diff).toLocaleString(undefined, { maximumFractionDigits: 0 })} ({change.pct >= 0 ? '+' : ''}{change.pct.toFixed(1)}%)
              </div>
            )}
            {!change && netWorth > 0 && (
              <div className="flex items-center text-sm text-green-400 bg-green-500/10 px-2 py-1 rounded-lg">
                <ArrowUp size={14} className="mr-1" />
                Net positive
              </div>
            )}
          </div>
          <div className="text-sm text-gray-400">Total portfolio value</div>
        </div>
        
        <div className="flex space-x-1">
          {periods.map((period) => (
            <button
              key={period}
              onClick={() => setSelectedPeriod(period)}
              className={`px-3 py-1.5 text-xs rounded-lg transition-all duration-200 font-medium ${
                selectedPeriod === period
                  ? 'bg-gradient-to-r from-blue-600 to-purple-600 text-white shadow-lg'
                  : 'text-gray-400 hover:bg-gray-700 hover:text-white'
              }`}
            >
              {period}
            </button>
          ))}
        </div>
      </div>

      <div className="grid grid-cols-2 gap-4 mb-6">
        <div className="bg-gray-700/30 rounded-xl p-4 border border-gray-600/30">
          <div className="text-sm text-gray-400 mb-1">Assets</div>
          <div className="text-lg font-bold text-green-400">
            ${totalAssets.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
          </div>
        </div>
        <div className="bg-gray-700/30 rounded-xl p-4 border border-gray-600/30">
          <div className="text-sm text-gray-400 mb-1">Liabilities</div>
          <div className="text-lg font-bold text-red-400">
            ${totalDebt.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
          </div>
        </div>
      </div>
      
      <div className="h-48">
        {histLoading ? (
          <div className="h-full flex items-center justify-center">
            <Loader2 className="animate-spin text-gray-400" size={24} />
          </div>
        ) : (
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={chartData}>
              <defs>
                <linearGradient id="colorNetWorth" x1="0" y1="0" x2="0" y2="1">
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
                tickFormatter={(v: number) => {
                  if (Math.abs(v) >= 1_000_000) return `$${(v / 1_000_000).toFixed(1)}M`;
                  if (Math.abs(v) >= 1_000) return `$${(v / 1_000).toFixed(0)}k`;
                  return `$${v}`;
                }}
              />
              <Tooltip 
                contentStyle={{ 
                  backgroundColor: '#1F2937', 
                  border: '1px solid #374151', 
                  borderRadius: '12px',
                  padding: '12px'
                }}
                labelStyle={{ color: '#9CA3AF', fontSize: '12px' }}
                formatter={(value: number | undefined) => [
                  `$${(value ?? 0).toLocaleString(undefined, { minimumFractionDigits: 2 })}`,
                  'Net Worth',
                ]}
              />
              <Area 
                type="monotone" 
                dataKey="netWorth" 
                stroke="#3B82F6" 
                strokeWidth={3}
                fill="url(#colorNetWorth)"
                dot={{ fill: '#3B82F6', strokeWidth: 2, r: 4 }}
                activeDot={{ r: 6, fill: '#3B82F6' }}
              />
            </AreaChart>
          </ResponsiveContainer>
        )}
      </div>
    </div>
  );
};

export default NetWorth;
