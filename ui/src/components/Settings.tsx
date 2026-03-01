import React, { useState, useEffect } from 'react';
import { Save, CheckCircle, AlertCircle, Loader2, LogOut, ExternalLink } from 'lucide-react';
import { invoke } from '@tauri-apps/api/core';

interface SettingsProps {
  onLogout: () => void;
  onSaved?: () => void;
}

const Settings: React.FC<SettingsProps> = ({ onLogout, onSaved }) => {
  const [apiUrl, setApiUrl] = useState('');
  const [saving, setSaving] = useState(false);
  const [saveStatus, setSaveStatus] = useState<'idle' | 'saved' | 'error'>('idle');
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    invoke<string>('get_api_url').then(setApiUrl).catch(console.error);
  }, []);

  const handleSave = async (e: React.FormEvent) => {
    e.preventDefault();
    setSaving(true);
    setError(null);
    setSaveStatus('idle');

    try {
      await invoke<void>('set_api_url', { url: apiUrl });
      setSaveStatus('saved');
      onSaved?.();
      setTimeout(() => setSaveStatus('idle'), 2500);
    } catch (err) {
      setError(String(err));
      setSaveStatus('error');
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="flex-1 overflow-auto p-8">
      <h1 className="text-3xl font-bold bg-gradient-to-r from-blue-400 to-purple-500 bg-clip-text text-transparent mb-8">
        Settings
      </h1>

      <div className="max-w-xl space-y-6">
        {/* API Connection */}
        <div className="bg-gray-800/60 border border-gray-700/50 rounded-2xl p-6">
          <h2 className="text-lg font-semibold text-white mb-1">API Connection</h2>
          <p className="text-sm text-gray-400 mb-4">
            Enter the URL of your AlgoFlow API Gateway endpoint.
          </p>

          <form onSubmit={handleSave} className="space-y-4">
            <div>
              <label className="block text-sm font-medium text-gray-300 mb-1.5">
                API Base URL
              </label>
              <input
                type="url"
                value={apiUrl}
                onChange={(e) => setApiUrl(e.target.value)}
                placeholder="https://xxxxxxxxxx.execute-api.us-east-1.amazonaws.com"
                className="w-full px-4 py-3 bg-gray-700/50 border border-gray-600 rounded-xl text-white placeholder-gray-500 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all font-mono text-sm"
              />
              <p className="text-xs text-gray-500 mt-1.5">
                Find this in your AWS Console under API Gateway → Stages, or in CDK outputs.
              </p>
            </div>

            {error && (
              <div className="flex items-center space-x-2 px-4 py-3 rounded-xl bg-red-900/30 border border-red-700/50 text-red-300 text-sm">
                <AlertCircle size={16} />
                <span>{error}</span>
              </div>
            )}

            <button
              type="submit"
              disabled={saving || !apiUrl.trim()}
              className="flex items-center space-x-2 px-5 py-2.5 rounded-xl bg-gradient-to-r from-blue-600 to-purple-600 text-white font-medium hover:from-blue-700 hover:to-purple-700 disabled:opacity-50 disabled:cursor-not-allowed transition-all shadow-lg"
            >
              {saving ? (
                <>
                  <Loader2 size={16} className="animate-spin" />
                  <span>Saving...</span>
                </>
              ) : saveStatus === 'saved' ? (
                <>
                  <CheckCircle size={16} />
                  <span>Saved!</span>
                </>
              ) : (
                <>
                  <Save size={16} />
                  <span>Save</span>
                </>
              )}
            </button>
          </form>
        </div>

        {/* CDK Outputs hint */}
        <div className="bg-gray-800/40 border border-gray-700/30 rounded-2xl p-5">
          <h3 className="text-sm font-semibold text-gray-300 mb-2 flex items-center space-x-2">
            <ExternalLink size={14} />
            <span>How to find your API URL</span>
          </h3>
          <ol className="text-sm text-gray-400 space-y-1 list-decimal list-inside">
            <li>Run <code className="bg-gray-700 px-1.5 py-0.5 rounded text-xs font-mono">cd infrastructure && cdk deploy</code></li>
            <li>Copy the <code className="bg-gray-700 px-1.5 py-0.5 rounded text-xs font-mono">AlgoFlow-Api.ApiUrl</code> output</li>
            <li>Paste it above and save</li>
          </ol>
        </div>

        {/* Account */}
        <div className="bg-gray-800/60 border border-gray-700/50 rounded-2xl p-6">
          <h2 className="text-lg font-semibold text-white mb-1">Account</h2>
          <p className="text-sm text-gray-400 mb-4">
            Sign out of your AlgoFlow account on this device.
          </p>
          <button
            onClick={onLogout}
            className="flex items-center space-x-2 px-5 py-2.5 rounded-xl bg-gray-700 hover:bg-gray-600 text-white font-medium transition-all"
          >
            <LogOut size={16} />
            <span>Sign Out</span>
          </button>
        </div>
      </div>
    </div>
  );
};

export default Settings;
