import { useState, useEffect, useCallback } from 'react';
import { invoke } from '@tauri-apps/api/core';
import type { DashboardData, Transaction, Account, NetWorthSnapshot } from '../types';

export function useDashboard() {
  const [data, setData] = useState<DashboardData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    try {
      setLoading(true);
      setError(null);
      const result = await invoke<DashboardData>('get_dashboard_data');
      setData(result);
    } catch (err) {
      console.error('Failed to load dashboard data:', err);
      setError(String(err));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    refresh();
  }, [refresh]);

  return { data, loading, error, refresh };
}

export function useTransactions(limit = 100, offset = 0) {
  const [transactions, setTransactions] = useState<Transaction[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    try {
      setLoading(true);
      setError(null);
      const result = await invoke<Transaction[]>('get_transactions', { limit, offset });
      setTransactions(result);
    } catch (err) {
      console.error('Failed to load transactions:', err);
      setError(String(err));
    } finally {
      setLoading(false);
    }
  }, [limit, offset]);

  useEffect(() => {
    refresh();
  }, [refresh]);

  return { transactions, loading, error, refresh };
}

export function useAccounts() {
  const [accounts, setAccounts] = useState<Account[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    try {
      setLoading(true);
      setError(null);
      const result = await invoke<Account[]>('get_accounts');
      setAccounts(result);
    } catch (err) {
      console.error('Failed to load accounts:', err);
      setError(String(err));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    refresh();
  }, [refresh]);

  return { accounts, loading, error, refresh };
}

export function useInvestmentTransactions() {
  const [transactions, setTransactions] = useState<Transaction[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    try {
      setLoading(true);
      setError(null);
      const result = await invoke<Transaction[]>('get_investment_transactions');
      setTransactions(result);
    } catch (err) {
      console.error('Failed to load investment transactions:', err);
      setError(String(err));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    refresh();
  }, [refresh]);

  return { transactions, loading, error, refresh };
}

export function useNetWorthHistory() {
  const [snapshots, setSnapshots] = useState<NetWorthSnapshot[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    try {
      setLoading(true);
      setError(null);
      const result = await invoke<NetWorthSnapshot[]>('get_net_worth_history');
      setSnapshots(result);
    } catch (err) {
      console.error('Failed to load net worth history:', err);
      setError(String(err));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    refresh();
  }, [refresh]);

  return { snapshots, loading, error, refresh };
}

export async function syncAllAccounts(): Promise<string> {
  return invoke<string>('sync_all_accounts');
}

export async function reviewTransaction(transactionId: string): Promise<void> {
  return invoke<void>('review_transaction', { transactionId });
}
