import React, { useMemo } from 'react';
import { XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Area, AreaChart } from 'recharts';
import { TrendingDown, DollarSign, Loader2 } from 'lucide-react';
import type { Transaction } from '../types';

interface MonthlySpendingProps {
  transactions: Transaction[];
  loading: boolean;
}

const MonthlySpending: React.FC<MonthlySpendingProps> = ({ transactions, loading }) => {
  const { chartData, totalSpent } = useMemo(() => {
    const now = new Date();
    const thisMonth = now.getMonth();
    const thisYear = now.getFullYear();

    const monthlyTxns = transactions.filter((t) => {
      const d = new Date(t.date);
      return d.getMonth() === thisMonth && d.getFullYear() === thisYear && t.amount > 0;
    });

    const dailyMap: Record<number, number> = {};
    let runningTotal = 0;
    for (const txn of monthlyTxns.sort((a, b) => a.date.localeCompare(b.date))) {
      const day = new Date(txn.date).getDate();
      runningTotal += txn.amount;
      dailyMap[day] = runningTotal;
    }

    const chartData = Object.entries(dailyMap)
      .map(([day, amount]) => ({ day: String(day), amount: Math.round(amount * 100) / 100 }))
      .sort((a, b) => Number(a.day) - Number(b.day));

    return { chartData, totalSpent: runningTotal };
  }, [transactions]);

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
          <h3 className="text-xl font-bold text-white mb-1">Monthly Spending</h3>
          <p className="text-sm text-gray-400">Track your daily expenses</p>
        </div>
        <div className="p-3 bg-gradient-to-r from-green-500/20 to-green-600/20 rounded-xl border border-green-500/30">
          <DollarSign className="text-green-400" size={24} />
        </div>
      </div>
      
      <div className="mb-6">
        <div className="flex items-baseline space-x-3 mb-2">
          <div className="text-3xl font-bold text-green-400">${totalSpent.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</div>
          <div className="flex items-center text-sm text-green-400 bg-green-500/10 px-2 py-1 rounded-lg">
            <TrendingDown size={14} className="mr-1" />
            This month
          </div>
        </div>
        <div className="text-sm text-gray-400">Total spending this month</div>
      </div>
      
      <div className="h-64">
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart data={chartData}>
            <defs>
              <linearGradient id="colorAmount" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor="#10B981" stopOpacity={0.8}/>
                <stop offset="95%" stopColor="#10B981" stopOpacity={0.1}/>
              </linearGradient>
            </defs>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" strokeOpacity={0.3} />
            <XAxis 
              dataKey="day" 
              stroke="#9CA3AF" 
              fontSize={12}
              tickLine={false}
            />
            <YAxis 
              stroke="#9CA3AF" 
              fontSize={12}
              tickLine={false}
            />
            <Tooltip 
              contentStyle={{ 
                backgroundColor: '#1F2937', 
                border: '1px solid #374151', 
                borderRadius: '12px',
                padding: '12px'
              }}
              labelStyle={{ color: '#9CA3AF', fontSize: '12px' }}
              itemStyle={{ color: '#10B981', fontWeight: 'bold' }}
            />
            <Area 
              type="monotone" 
              dataKey="amount" 
              stroke="#10B981" 
              strokeWidth={3}
              fill="url(#colorAmount)"
              dot={{ fill: '#10B981', strokeWidth: 2, r: 4 }}
              activeDot={{ r: 6, fill: '#10B981' }}
            />
          </AreaChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
};

export default MonthlySpending;
