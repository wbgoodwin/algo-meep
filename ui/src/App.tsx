import { useState } from 'react';
import Dashboard from './components/Dashboard';
import Investments from './components/Investments';
import Retirement from './components/Retirement';
import Navigation, { type Page } from './components/Navigation';
import './App.css';

function App() {
  const [activePage, setActivePage] = useState<Page>('dashboard');

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
