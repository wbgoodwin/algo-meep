import React, { useState } from 'react';
import * as Checkbox from '@radix-ui/react-checkbox';
import { Check, Receipt, CreditCard, ShoppingCart, Zap, Loader2 } from 'lucide-react';
import { reviewTransaction } from '../hooks/useBackend';
import type { Transaction } from '../types';

interface TransactionsReviewProps {
  transactions: Transaction[];
  loading: boolean;
}

const TransactionsReview: React.FC<TransactionsReviewProps> = ({ transactions, loading }) => {
  const [localReviewed, setLocalReviewed] = useState<Set<string>>(new Set());

  const unreviewedTxns = transactions
    .filter((t) => !t.reviewed && !localReviewed.has(t.id))
    .slice(0, 10);

  const handleReview = async (id: string) => {
    setLocalReviewed((prev) => new Set(prev).add(id));
    try {
      await reviewTransaction(id);
    } catch (err) {
      console.error('Failed to review transaction:', err);
      setLocalReviewed((prev) => {
        const next = new Set(prev);
        next.delete(id);
        return next;
      });
    }
  };

  const getCategoryIcon = (category?: string | null) => {
    switch (category?.toLowerCase()) {
      case 'loan_payments': return <CreditCard size={16} className="text-purple-400" />;
      case 'food_and_drink': return <ShoppingCart size={16} className="text-orange-400" />;
      case 'utilities': return <Zap size={16} className="text-yellow-400" />;
      default: return <Receipt size={16} className="text-gray-400" />;
    }
  };

  const getCategoryColor = (category?: string | null) => {
    switch (category?.toLowerCase()) {
      case 'loan_payments': return 'bg-purple-500/20 text-purple-400 border-purple-500/30';
      case 'food_and_drink': return 'bg-orange-500/20 text-orange-400 border-orange-500/30';
      case 'utilities': return 'bg-yellow-500/20 text-yellow-400 border-yellow-500/30';
      default: return 'bg-gray-500/20 text-gray-400 border-gray-500/30';
    }
  };

  const formatDate = (dateStr: string) => {
    const txnDate = new Date(dateStr);
    const today = new Date();
    const yesterday = new Date();
    yesterday.setDate(today.getDate() - 1);

    if (txnDate.toDateString() === today.toDateString()) return 'TODAY';
    if (txnDate.toDateString() === yesterday.toDateString()) return 'YESTERDAY';
    return txnDate.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
  };

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
          <h3 className="text-xl font-bold text-white mb-1">Transactions to Review</h3>
          <p className="text-sm text-gray-400">
            {unreviewedTxns.length > 0
              ? `${unreviewedTxns.length} transactions need attention`
              : 'All caught up!'}
          </p>
        </div>
        <div className="p-3 bg-gradient-to-r from-orange-500/20 to-red-600/20 rounded-xl border border-orange-500/30">
          <Receipt className="text-orange-400" size={24} />
        </div>
      </div>
      
      {unreviewedTxns.length === 0 ? (
        <div className="text-center py-8 text-gray-500">
          <Receipt size={48} className="mx-auto mb-3 opacity-30" />
          <p>No transactions to review</p>
          <p className="text-sm mt-1">Connect a bank account to see transactions here</p>
        </div>
      ) : (
        <div className="space-y-3">
          {unreviewedTxns.map((txn) => (
            <div
              key={txn.id}
              className="group flex items-center space-x-4 p-4 bg-gray-700/30 rounded-xl border border-gray-600/30 hover:bg-gray-700/50 hover:border-gray-600/50 transition-all duration-200 hover:shadow-lg"
            >
              <Checkbox.Root
                id={txn.id}
                checked={txn.reviewed}
                onCheckedChange={() => handleReview(txn.id)}
                className="w-5 h-5 rounded-lg border-2 border-gray-500 bg-gray-600 data-[state=checked]:bg-gradient-to-r data-[state=checked]:from-blue-500 data-[state=checked]:to-purple-500 data-[state=checked]:border-blue-500 transition-all duration-200"
              >
                <Checkbox.Indicator className="flex items-center justify-center">
                  <Check size={14} className="text-white" />
                </Checkbox.Indicator>
              </Checkbox.Root>
              
              <div className="flex-1">
                <div className="flex items-center space-x-2 mb-1">
                  <span className="text-xs font-semibold text-gray-500">{formatDate(txn.date)}</span>
                  {txn.category_primary && (
                    <div className={`inline-flex items-center space-x-1 px-2 py-0.5 rounded-full text-xs font-medium border ${getCategoryColor(txn.category_primary)}`}>
                      {getCategoryIcon(txn.category_primary)}
                      <span>{txn.category_primary.replace(/_/g, ' ')}</span>
                    </div>
                  )}
                </div>
                <div className="text-white font-medium group-hover:text-blue-400 transition-colors">
                  {txn.merchant_name || txn.name}
                </div>
              </div>
              
              <div className={`font-bold text-lg ${
                txn.amount < 0 ? 'text-green-400' : 'text-red-400'
              }`}>
                {txn.amount < 0 ? '+' : '-'}${Math.abs(txn.amount).toFixed(2)}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
};

export default TransactionsReview;
