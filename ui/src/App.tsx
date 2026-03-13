import { useState, useEffect } from 'react';
import Dashboard from './components/Dashboard';
import Investments from './components/Investments';
import Retirement from './components/Retirement';
import Navigation, { type Page } from './components/Navigation';
import AuthScreen from './components/AuthScreen';
import Settings from './components/Settings';
import { useAuth, getApiUrl } from './hooks/useBackend';
import { Loader2 } from 'lucide-react';
import './App.css';

function App() {
  const [activePage, setActivePage] = useState<Page>('dashboard');
  const { authenticated, login, register, confirmSignup, logout } = useAuth();

  // True while we check if an API URL is configured
  const [apiUrlChecked, setApiUrlChecked] = useState(false);
  const [hasApiUrl, setHasApiUrl] = useState(false);

  useEffect(() => {
    getApiUrl()
      .then((url) => {
        setHasApiUrl(!!url);
        setApiUrlChecked(true);
      })
      .catch(() => setApiUrlChecked(true));
  }, []);

  // Loading spinner while we check auth + API URL
  if (authenticated === null || !apiUrlChecked) {
    return (
      <div className="flex h-screen items-center justify-center bg-gradient-to-br from-gray-900 via-gray-800 to-gray-900">
        <Loader2 size={32} className="animate-spin text-blue-400" />
      </div>
    );
  }

  // No API URL → force settings screen first
  if (!hasApiUrl || activePage === 'settings') {
    return (
      <div className="flex h-screen bg-gradient-to-br from-gray-900 via-gray-800 to-gray-900 text-white">
        {authenticated && (
          <Navigation
            activePage={activePage}
            onNavigate={(page) => {
              if (page !== 'settings') setHasApiUrl(true);
              setActivePage(page);
            }}
          />
        )}
        <Settings
          onLogout={async () => {
            await logout();
            setActivePage('dashboard');
          }}
          onSaved={() => {
            setHasApiUrl(true);
            if (activePage === 'settings') setActivePage('dashboard');
          }}
        />
      </div>
    );
  }

  // Not authenticated → show login/register
  if (!authenticated) {
    return <AuthScreen onLogin={login} onRegister={register} onConfirm={confirmSignup} />;
  }

  const renderPage = () => {
    switch (activePage) {
      case 'investments':
        return <Investments />;
      case 'retirement':
        return <Retirement />;
      case 'dashboard':
      default:
        return <Dashboard />;
    }
  };

  return (
    <div className="flex h-screen bg-gradient-to-br from-gray-900 via-gray-800 to-gray-900 text-white">
      <Navigation activePage={activePage} onNavigate={setActivePage} />
      {renderPage()}
    </div>
  );
}

export default App;
