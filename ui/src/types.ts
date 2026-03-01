export interface Account {
  id: string;
  institution_id: string;
  plaid_account_id: string;
  name: string;
  official_name: string | null;
  account_type: string;
  account_subtype: string | null;
  mask: string | null;
  current_balance: number | null;
  available_balance: number | null;
  credit_limit: number | null;
  iso_currency_code: string | null;
}

export interface Transaction {
  id: string;
  account_id: string;
  plaid_transaction_id: string | null;
  amount: number;
  date: string;
  name: string;
  merchant_name: string | null;
  category_primary: string | null;
  category_detailed: string | null;
  pending: boolean;
  payment_channel: string | null;
  iso_currency_code: string | null;
  reviewed: boolean;
}

export interface Institution {
  id: string;
  name: string;
  plaid_institution_id: string | null;
  logo: string | null;
  primary_color: string | null;
}

export interface CategorySpending {
  name: string;
  amount: number;
  budget: number | null;
  color: string | null;
}

export interface NetWorthSnapshot {
  id: string;
  snapshot_date: string;
  total_assets: number;
  total_liabilities: number;
  net_worth: number;
}

export interface DashboardData {
  accounts: Account[];
  recent_transactions: Transaction[];
  institutions: Institution[];
  total_balance: number;
  total_credit_debt: number;
  categories: CategorySpending[];
}
