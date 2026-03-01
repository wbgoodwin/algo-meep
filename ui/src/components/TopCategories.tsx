import React from 'react';
import * as Progress from '@radix-ui/react-progress';
import { PieChart, Loader2 } from 'lucide-react';
import type { CategorySpending } from '../types';

const CATEGORY_COLORS = [
  'bg-blue-500',
  'bg-green-500',
  'bg-yellow-500',
  'bg-purple-500',
  'bg-pink-500',
  'bg-orange-500',
  'bg-cyan-500',
  'bg-red-500',
];

interface TopCategoriesProps {
  categories: CategorySpending[];
  loading: boolean;
}

const TopCategories: React.FC<TopCategoriesProps> = ({ categories, loading }) => {
  const totalSpent = categories.reduce((sum, cat) => sum + cat.amount, 0);

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
          <h3 className="text-xl font-bold text-white mb-1">Top Categories</h3>
          <p className="text-sm text-gray-400">Spending breakdown by category</p>
        </div>
        <div className="p-3 bg-gradient-to-r from-purple-500/20 to-pink-600/20 rounded-xl border border-purple-500/30">
          <PieChart className="text-purple-400" size={24} />
        </div>
      </div>
      
      <div className="mb-6">
        <div className="flex items-baseline space-x-3 mb-2">
          <div className="text-3xl font-bold text-white">
            ${totalSpent.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
          </div>
        </div>
        <div className="text-sm text-gray-400">Total spent this month</div>
      </div>

      {categories.length === 0 ? (
        <div className="text-center py-8 text-gray-500">
          <PieChart size={48} className="mx-auto mb-3 opacity-30" />
          <p>No spending data yet</p>
          <p className="text-sm mt-1">Connect a bank account to see category breakdown</p>
        </div>
      ) : (
        <div className="space-y-4">
          {categories.map((category, index) => {
            const pct = totalSpent > 0 ? (category.amount / totalSpent) * 100 : 0;
            const color = CATEGORY_COLORS[index % CATEGORY_COLORS.length];

            return (
              <div key={category.name} className="group">
                <div className="flex justify-between items-center mb-2">
                  <div className="flex items-center space-x-2">
                    <div className={`w-3 h-3 rounded-full ${color}`} />
                    <span className="text-sm font-semibold text-white group-hover:text-blue-400 transition-colors">
                      {category.name.replace(/_/g, ' ')}
                    </span>
                  </div>
                  <div className="flex items-center space-x-3">
                    <span className="text-xs text-gray-400">{pct.toFixed(1)}%</span>
                    <span className="text-sm font-medium text-white">
                      ${category.amount.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                    </span>
                  </div>
                </div>

                <Progress.Root className="relative h-2.5 bg-gray-700 rounded-full overflow-hidden">
                  <Progress.Indicator
                    className={`h-full rounded-full transition-all duration-500 ease-out ${color}`}
                    style={{ width: `${Math.min(pct, 100)}%` }}
                  />
                </Progress.Root>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
};

export default TopCategories;
