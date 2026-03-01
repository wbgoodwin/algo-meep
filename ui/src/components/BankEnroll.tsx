import React, { useState, useCallback } from 'react';
import { Plus, Loader2, CheckCircle, AlertCircle } from 'lucide-react';
import { bankEnroll, bankExchangeToken } from '../hooks/useBackend';

// Teller Connect global types (CDN-loaded script)
declare global {
  interface Window {
    TellerConnect?: {
      setup(config: TellerConnectConfig): TellerConnectInstance;
    };
  }
}

interface TellerConnectConfig {
  applicationId: string;
  onSuccess: (enrollment: TellerEnrollment) => void;
  onExit: () => void;
  onEvent?: (name: string, data: object) => void;
}

interface TellerEnrollment {
  accessToken: string;
  user: { id: string };
  enrollment: {
    id: string;
    institution: { name: string; id: string };
  };
}

interface TellerConnectInstance {
  open: () => void;
}

function loadTellerScript(): Promise<void> {
  if (window.TellerConnect) return Promise.resolve();

  return new Promise((resolve, reject) => {
    // Avoid loading twice if a script tag already exists
    if (document.querySelector('script[data-teller-connect]')) {
      // Wait for it to finish loading
      const interval = setInterval(() => {
        if (window.TellerConnect) {
          clearInterval(interval);
          resolve();
        }
      }, 50);
      return;
    }

    const script = document.createElement('script');
    script.src = 'https://cdn.teller.io/connect/connect.js';
    script.dataset.tellerConnect = 'true';
    script.onload = () => resolve();
    script.onerror = () =>
      reject(new Error('Failed to load Teller Connect SDK'));
    document.head.appendChild(script);
  });
}

function parseApplicationId(sessionUrl: string): string | null {
  try {
    return new URL(sessionUrl).searchParams.get('application_id');
  } catch {
    return null;
  }
}

interface BankEnrollButtonProps {
  onSuccess: () => void;
}

type EnrollStep = 'idle' | 'loading' | 'connecting' | 'exchanging' | 'success' | 'error';

const BankEnrollButton: React.FC<BankEnrollButtonProps> = ({ onSuccess }) => {
  const [step, setStep] = useState<EnrollStep>('idle');
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  const handleConnect = useCallback(async () => {
    setErrorMessage(null);
    setStep('loading');

    try {
      // 1. Ask backend for the Teller session URL (contains applicationId)
      const enrollResult = await bankEnroll('', 'teller');
      const applicationId = parseApplicationId(enrollResult.session_url);

      if (!applicationId) {
        throw new Error('Could not parse application ID from enrollment response');
      }

      // 2. Load Teller Connect SDK from CDN
      await loadTellerScript();

      if (!window.TellerConnect) {
        throw new Error('Teller Connect SDK did not load');
      }

      setStep('connecting');

      // 3. Set up and open the Teller Connect widget in-app
      const connect = window.TellerConnect.setup({
        applicationId,
        onSuccess: async (enrollment) => {
          setStep('exchanging');
          try {
            // 4. Exchange the Teller access token via the backend
            await bankExchangeToken(enrollResult.provider, enrollment.accessToken);
            setStep('success');
            onSuccess();
            setTimeout(() => setStep('idle'), 3000);
          } catch (err) {
            setErrorMessage(String(err));
            setStep('error');
            setTimeout(() => setStep('idle'), 5000);
          }
        },
        onExit: () => {
          setStep('idle');
        },
      });

      connect.open();
    } catch (err) {
      setErrorMessage(String(err));
      setStep('error');
      setTimeout(() => setStep('idle'), 5000);
    }
  }, [onSuccess]);

  const isDisabled = step !== 'idle' && step !== 'error';

  const buttonContent = () => {
    switch (step) {
      case 'loading':
        return (
          <>
            <Loader2 size={18} className="animate-spin" />
            <span>Loading...</span>
          </>
        );
      case 'connecting':
        return (
          <>
            <Loader2 size={18} className="animate-spin" />
            <span>Connecting to bank...</span>
          </>
        );
      case 'exchanging':
        return (
          <>
            <Loader2 size={18} className="animate-spin" />
            <span>Securing account...</span>
          </>
        );
      case 'success':
        return (
          <>
            <CheckCircle size={18} />
            <span>Account connected!</span>
          </>
        );
      case 'error':
        return (
          <>
            <AlertCircle size={18} />
            <span>{errorMessage || 'Connection failed'}</span>
          </>
        );
      default:
        return (
          <>
            <Plus size={18} />
            <span>Connect a bank account</span>
          </>
        );
    }
  };

  return (
    <button
      onClick={handleConnect}
      disabled={isDisabled}
      className="w-full flex items-center justify-center space-x-2 px-4 py-3 rounded-xl bg-gradient-to-r from-blue-600 to-purple-600 text-white font-medium hover:from-blue-700 hover:to-purple-700 transition-all duration-200 disabled:opacity-50 disabled:cursor-not-allowed shadow-lg hover:shadow-xl"
    >
      {buttonContent()}
    </button>
  );
};

export default BankEnrollButton;
