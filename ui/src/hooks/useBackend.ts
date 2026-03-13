import { useState, useEffect, useCallback } from 'react';
import { invoke } from '@tauri-apps/api/core';
import type {
  DashboardData,
  Transaction,
  Account,
  NetWorthSnapshot,
  AuthResult,
  BankEnrollResult,
} from '../types';

// --- Config ---

export async function getApiUrl(): Promise<string> {
  return invoke<string>('get_api_url');
}

export async function setApiUrl(url: string): Promise<void> {
  return invoke<void>('set_api_url', { url });
}

// --- Auth ---

export function useAuth() {
  const [authenticated, setAuthenticated] = useState<boolean | null>(null);

  const checkAuth = useCallback(async () => {
    try {
      const result = await invoke<AuthResult>('check_auth');
      setAuthenticated(result.authenticated);
    } catch {
      setAuthenticated(false);
    }
  }, []);

  useEffect(() => {
    checkAuth();
  }, [checkAuth]);

  const login = useCallback(async (email: string, password: string) => {
    const result = await invoke<AuthResult>('login', { email, password });
    setAuthenticated(result.authenticated);
    return result;
  }, []);

  const register = useCallback(async (email: string, password: string) => {
    return invoke<string>('register', { email, password });
  }, []);

  const confirmSignup = useCallback(async (email: string, code: string) => {
    return invoke<void>('confirm_signup', { email, code });
  }, []);

  const logout = useCallback(async () => {
    await invoke<void>('logout');
    setAuthenticated(false);
  }, []);

  return { authenticated, login, register, confirmSignup, logout };
}

// --- Bank ---

export async function bankEnroll(
  institutionId: string,
  provider: string
): Promise<BankEnrollResult> {
  return invoke<BankEnrollResult>('bank_enroll', {
    institutionId,
    provider,
  });
}

export async function bankExchangeToken(
  provider: string,
  enrollmentToken: string
): Promise<string> {
  return invoke<string>('bank_exchange_token', { provider, enrollmentToken });
}

// --- Dashboard ---

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
      const result = await invoke<Transaction[]>('get_transactions', {
        limit,
        offset,
      });
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
