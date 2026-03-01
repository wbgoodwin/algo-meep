import React, { useCallback, useEffect, useState } from 'react';
import { usePlaidLink } from 'react-plaid-link';
import { invoke } from '@tauri-apps/api/core';
import { Plus, Loader2, CheckCircle, AlertCircle } from 'lucide-react';

interface PlaidLinkButtonProps {
  onSuccess: () => void;
}

const PlaidLinkButton: React.FC<PlaidLinkButtonProps> = ({ onSuccess }) => {
  const [linkToken, setLinkToken] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [status, setStatus] = useState<'idle' | 'linking' | 'syncing' | 'success' | 'error'>('idle');
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  const fetchLinkToken = useCallback(async () => {
    try {
      setLoading(true);
      const token = await invoke<string>('create_link_token');
      setLinkToken(token);
    } catch (err) {
      console.error('Failed to create link token:', err);
      setErrorMessage('Failed to connect to Plaid. Check your credentials.');
      setStatus('error');
    } finally {
      setLoading(false);
    }
  }, []);

  const onPlaidSuccess = useCallback(
    async (publicToken: string) => {
      try {
        setStatus('syncing');
        await invoke('exchange_token_and_sync', { publicToken });
        setStatus('success');
        onSuccess();
        setTimeout(() => setStatus('idle'), 3000);
      } catch (err) {
        console.error('Failed to exchange token:', err);
        setErrorMessage('Failed to sync account data.');
        setStatus('error');
        setTimeout(() => setStatus('idle'), 5000);
      }
    },
    [onSuccess]
  );

  const { open, ready } = usePlaidLink({
    token: linkToken,
    onSuccess: onPlaidSuccess,
    onExit: () => {
      setStatus('idle');
    },
  });

  const handleClick = async () => {
    if (!linkToken) {
      await fetchLinkToken();
    }
  };

  useEffect(() => {
    if (linkToken && ready) {
      setStatus('linking');
      open();
    }
  }, [linkToken, ready, open]);

  return (
    <div>
      <button
        onClick={handleClick}
        disabled={loading || status === 'syncing'}
        className="w-full flex items-center justify-center space-x-2 px-4 py-3 rounded-xl bg-gradient-to-r from-blue-600 to-purple-600 text-white font-medium hover:from-blue-700 hover:to-purple-700 transition-all duration-200 disabled:opacity-50 disabled:cursor-not-allowed shadow-lg hover:shadow-xl"
      >
        {status === 'syncing' ? (
          <>
            <Loader2 size={18} className="animate-spin" />
            <span>Syncing account...</span>
          </>
        ) : status === 'success' ? (
          <>
            <CheckCircle size={18} />
            <span>Account connected!</span>
          </>
        ) : status === 'error' ? (
          <>
            <AlertCircle size={18} />
            <span>{errorMessage || 'Connection failed'}</span>
          </>
        ) : loading ? (
          <>
            <Loader2 size={18} className="animate-spin" />
            <span>Connecting...</span>
          </>
        ) : (
          <>
            <Plus size={18} />
            <span>Connect a bank account</span>
          </>
        )}
      </button>
    </div>
  );
};

export default PlaidLinkButton;
