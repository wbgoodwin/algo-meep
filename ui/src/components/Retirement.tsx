import React, { useMemo, useState, useCallback } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, ReferenceLine, Legend } from 'recharts';
import { Loader2, TrendingUp, DollarSign, ShieldCheck, AlertTriangle, Plus, Trash2 } from 'lucide-react';
import { useAccounts } from '../hooks/useBackend';

// --- Types ---

interface ScenarioInputs {
  id: string;
  name: string;
  retirementAge: number;
  monthlyContribution: number;
  growthRate: number;
  inflationRate: number;
  color: string;
}

interface ProjectionPoint {
  age: number;
  [key: string]: number | string;
}

// --- Computation ---

const SCENARIO_COLORS = ['#8B5CF6', '#3B82F6', '#10B981', '#F59E0B'];

function computeProjection(
  currentAge: number,
  scenario: ScenarioInputs,
  netWorth: number,
): { points: Map<number, number>; retirementBalance: number } {
  const realGrowth = scenario.growthRate - scenario.inflationRate;
  const annualContribution = scenario.monthlyContribution * 12;
  const endAge = Math.max(scenario.retirementAge + 25, 90);

  const points = new Map<number, number>();
  let balance = netWorth;
  let retBal = 0;

  for (let age = currentAge; age <= endAge; age++) {
    points.set(age, Math.round(balance * 100) / 100);

    if (age === scenario.retirementAge) retBal = balance;

    if (age < scenario.retirementAge) {
      balance = balance * (1 + realGrowth) + annualContribution;
    } else {
      balance = balance * (1 + realGrowth * 0.4);
    }

    if (balance < 0) {
      points.set(age + 1, 0);
      break;
    }
  }

  return { points, retirementBalance: retBal };
}

function runMonteCarlo(
  currentAge: number,
  scenario: ScenarioInputs,
  netWorth: number,
  simulations = 1000,
  yearsInRetirement = 30,
): number {
  let successes = 0;
  const realGrowth = scenario.growthRate - scenario.inflationRate;
  const annualContribution = scenario.monthlyContribution * 12;
  const volatility = 0.12;

  for (let sim = 0; sim < simulations; sim++) {
    let balance = netWorth;
    let survived = true;

    for (let year = 0; year < (scenario.retirementAge - currentAge) + yearsInRetirement; year++) {
      const age = currentAge + year;
      const randReturn = realGrowth + volatility * gaussianRandom();

      if (age < scenario.retirementAge) {
        balance = balance * (1 + randReturn) + annualContribution;
      } else {
        balance = balance * (1 + randReturn * 0.4);
      }

      if (balance <= 0) { survived = false; break; }
    }
    if (survived) successes++;
  }

  return successes / simulations;
}

function gaussianRandom(): number {
  let u = 0, v = 0;
  while (u === 0) u = Math.random();
  while (v === 0) v = Math.random();
  return Math.sqrt(-2.0 * Math.log(u)) * Math.cos(2.0 * Math.PI * v);
}

function getConfidenceColor(c: number): string {
  if (c >= 0.8) return 'text-green-400';
  if (c >= 0.5) return 'text-yellow-400';
  return 'text-red-400';
}

function getConfidenceBg(c: number): string {
  if (c >= 0.8) return 'from-green-500/20 to-green-600/20 border-green-500/30';
  if (c >= 0.5) return 'from-yellow-500/20 to-yellow-600/20 border-yellow-500/30';
  return 'from-red-500/20 to-red-600/20 border-red-500/30';
}

function getConfidenceLabel(c: number): string {
  if (c >= 0.9) return 'Very High';
  if (c >= 0.75) return 'High';
  if (c >= 0.5) return 'Moderate';
  if (c >= 0.25) return 'Low';
  return 'Very Low';
}

function fmtDollar(v: number): string {
  return '$' + v.toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 0 });
}

// --- Component ---

const Retirement: React.FC = () => {
  const { accounts, loading } = useAccounts();

  const [currentAge, setCurrentAge] = useState(30);

  const [scenarios, setScenarios] = useState<ScenarioInputs[]>([
    {
      id: 'base',
      name: 'Base',
      retirementAge: 65,
      monthlyContribution: 500,
      growthRate: 0.07,
      inflationRate: 0.03,
      color: SCENARIO_COLORS[0],
    },
  ]);

  const netWorth = useMemo(() => {
    const assets = accounts
      .filter((a) => a.account_type !== 'credit')
      .reduce((sum, a) => sum + (a.current_balance ?? 0), 0);
    const liabilities = accounts
      .filter((a) => a.account_type === 'credit')
      .reduce((sum, a) => sum + Math.abs(a.current_balance ?? 0), 0);
    return assets - liabilities;
  }, [accounts]);

  const scenarioResults = useMemo(() => {
    return scenarios.map((s) => {
      const { points, retirementBalance } = computeProjection(currentAge, s, netWorth);
      const confidence = runMonteCarlo(currentAge, s, netWorth);
      return { scenario: s, points, retirementBalance, confidence };
    });
  }, [scenarios, currentAge, netWorth]);

  const chartData = useMemo(() => {
    const allAges = new Set<number>();
    scenarioResults.forEach((r) => r.points.forEach((_, age) => allAges.add(age)));
    const sorted = Array.from(allAges).sort((a, b) => a - b);

    return sorted.map((age) => {
      const point: ProjectionPoint = { age };
      scenarioResults.forEach((r) => {
        point[r.scenario.id] = r.points.get(age) ?? 0;
      });
      return point;
    });
  }, [scenarioResults]);

  const primaryResult = scenarioResults[0];

  const addScenario = useCallback(() => {
    if (scenarios.length >= 4) return;
    const idx = scenarios.length;
    setScenarios((prev) => [
      ...prev,
      {
        id: `scenario_${Date.now()}`,
        name: `Scenario ${idx + 1}`,
        retirementAge: 65,
        monthlyContribution: 500,
        growthRate: 0.07,
        inflationRate: 0.03,
        color: SCENARIO_COLORS[idx % SCENARIO_COLORS.length],
      },
    ]);
  }, [scenarios.length]);

  const removeScenario = useCallback((id: string) => {
    setScenarios((prev) => prev.filter((s) => s.id !== id));
  }, []);

  const updateScenario = useCallback((id: string, field: keyof ScenarioInputs, value: string | number) => {
    setScenarios((prev) =>
      prev.map((s) => (s.id === id ? { ...s, [field]: value } : s)),
    );
  }, []);

  if (loading) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <Loader2 className="animate-spin text-gray-400" size={40} />
      </div>
    );
  }

  return (
    <div className="flex-1 overflow-auto">
      <header className="bg-gray-800/50 backdrop-blur-sm border-b border-gray-700/50 sticky top-0 z-10">
        <div className="px-8 py-6">
          <h1 className="text-3xl font-bold bg-gradient-to-r from-blue-400 to-purple-500 bg-clip-text text-transparent">
            Retirement Planner
          </h1>
          <p className="text-gray-400 mt-1">Project your financial future and compare scenarios</p>
        </div>
      </header>

      <div className="p-8">
        {/* Global + Summary */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
          <div className="bg-gradient-to-br from-gray-800 to-gray-800/50 rounded-2xl p-6 border border-gray-700/50">
            <label className="block text-sm text-gray-400 mb-2">Current Age</label>
            <input
              type="number"
              min={18}
              max={80}
              value={currentAge}
              onChange={(e) => setCurrentAge(Math.max(18, Math.min(80, parseInt(e.target.value) || 18)))}
              className="w-full px-4 py-3 bg-gray-700/50 border border-gray-600 rounded-xl text-white text-2xl font-bold focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all"
            />
          </div>

          <div className="bg-gradient-to-br from-gray-800 to-gray-800/50 rounded-2xl p-6 border border-gray-700/50">
            <div className="flex items-center justify-between mb-3">
              <span className="text-sm text-gray-400">Current Net Worth</span>
              <DollarSign className="text-blue-400" size={18} />
            </div>
            <div className="text-2xl font-bold text-white">{fmtDollar(netWorth)}</div>
            <p className="text-xs text-gray-500 mt-1">Based on linked accounts</p>
          </div>

          {primaryResult && (
            <div className={`bg-gradient-to-br ${getConfidenceBg(primaryResult.confidence)} rounded-2xl p-6 border`}>
              <div className="flex items-center justify-between mb-3">
                <span className="text-sm text-gray-400">Base Confidence</span>
                {primaryResult.confidence >= 0.5 ? (
                  <ShieldCheck className={getConfidenceColor(primaryResult.confidence)} size={22} />
                ) : (
                  <AlertTriangle className={getConfidenceColor(primaryResult.confidence)} size={22} />
                )}
              </div>
              <div className={`text-3xl font-bold ${getConfidenceColor(primaryResult.confidence)}`}>
                {(primaryResult.confidence * 100).toFixed(0)}%
              </div>
              <p className={`text-sm mt-1 ${getConfidenceColor(primaryResult.confidence)}`}>{getConfidenceLabel(primaryResult.confidence)}</p>
            </div>
          )}
        </div>

        {/* Scenario Cards */}
        <div className="mb-8">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-bold text-white">Scenarios</h3>
            {scenarios.length < 4 && (
              <button
                onClick={addScenario}
                className="flex items-center space-x-2 px-4 py-2 text-sm rounded-lg bg-gray-700 hover:bg-gray-600 text-gray-300 hover:text-white transition-all"
              >
                <Plus size={16} />
                <span>Add Scenario</span>
              </button>
            )}
          </div>

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            {scenarios.map((s, idx) => {
              const result = scenarioResults[idx];
              return (
                <div key={s.id} className="bg-gradient-to-br from-gray-800 to-gray-800/50 rounded-2xl p-5 border border-gray-700/50">
                  <div className="flex items-center justify-between mb-4">
                    <div className="flex items-center space-x-3">
                      <div className="w-3 h-3 rounded-full" style={{ backgroundColor: s.color }} />
                      <input
                        type="text"
                        value={s.name}
                        onChange={(e) => updateScenario(s.id, 'name', e.target.value)}
                        className="bg-transparent text-white font-bold text-lg focus:outline-none border-b border-transparent focus:border-gray-500 transition-all"
                      />
                    </div>
                    <div className="flex items-center space-x-3">
                      {result && (
                        <span className={`text-sm font-bold ${getConfidenceColor(result.confidence)}`}>
                          {(result.confidence * 100).toFixed(0)}%
                        </span>
                      )}
                      {scenarios.length > 1 && (
                        <button
                          onClick={() => removeScenario(s.id)}
                          className="p-1.5 rounded-lg text-gray-500 hover:text-red-400 hover:bg-red-500/10 transition-all"
                        >
                          <Trash2 size={14} />
                        </button>
                      )}
                    </div>
                  </div>

                  <div className="grid grid-cols-2 gap-3">
                    <div>
                      <label className="block text-xs text-gray-500 mb-1">Retirement Age</label>
                      <input
                        type="number"
                        min={currentAge + 1}
                        max={100}
                        value={s.retirementAge}
                        onChange={(e) => updateScenario(s.id, 'retirementAge', Math.max(currentAge + 1, Math.min(100, parseInt(e.target.value) || 65)))}
                        className="w-full px-3 py-2 bg-gray-700/50 border border-gray-600 rounded-lg text-white text-sm font-medium focus:outline-none focus:ring-2 focus:ring-blue-500 transition-all"
                      />
                    </div>
                    <div>
                      <label className="block text-xs text-gray-500 mb-1">Monthly Contribution</label>
                      <input
                        type="number"
                        min={0}
                        step={100}
                        value={s.monthlyContribution}
                        onChange={(e) => updateScenario(s.id, 'monthlyContribution', Math.max(0, parseInt(e.target.value) || 0))}
                        className="w-full px-3 py-2 bg-gray-700/50 border border-gray-600 rounded-lg text-white text-sm font-medium focus:outline-none focus:ring-2 focus:ring-blue-500 transition-all"
                      />
                    </div>
                    <div>
                      <label className="block text-xs text-gray-500 mb-1">Growth Rate (%)</label>
                      <input
                        type="number"
                        step={0.5}
                        min={0}
                        max={30}
                        value={(s.growthRate * 100).toFixed(1)}
                        onChange={(e) => updateScenario(s.id, 'growthRate', (parseFloat(e.target.value) || 7) / 100)}
                        className="w-full px-3 py-2 bg-gray-700/50 border border-gray-600 rounded-lg text-white text-sm font-medium focus:outline-none focus:ring-2 focus:ring-blue-500 transition-all"
                      />
                    </div>
                    <div>
                      <label className="block text-xs text-gray-500 mb-1">Inflation Rate (%)</label>
                      <input
                        type="number"
                        step={0.5}
                        min={0}
                        max={15}
                        value={(s.inflationRate * 100).toFixed(1)}
                        onChange={(e) => updateScenario(s.id, 'inflationRate', (parseFloat(e.target.value) || 3) / 100)}
                        className="w-full px-3 py-2 bg-gray-700/50 border border-gray-600 rounded-lg text-white text-sm font-medium focus:outline-none focus:ring-2 focus:ring-blue-500 transition-all"
                      />
                    </div>
                  </div>

                  {result && (
                    <div className="mt-4 pt-3 border-t border-gray-700/50 flex items-center justify-between text-sm">
                      <span className="text-gray-400">Projected at {s.retirementAge}:</span>
                      <span className="text-white font-bold">{fmtDollar(result.retirementBalance)}</span>
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        </div>

        {/* Projection Chart */}
        <div className="bg-gradient-to-br from-gray-800 to-gray-800/50 rounded-2xl p-6 border border-gray-700/50 mb-8">
          <h3 className="text-lg font-bold text-white mb-6">Projected Balance Comparison</h3>
          {chartData.length <= 1 ? (
            <div className="h-80 flex flex-col items-center justify-center text-gray-500">
              <DollarSign size={48} className="mb-3 opacity-30" />
              <p>Connect accounts to see your retirement projection</p>
            </div>
          ) : (
            <div className="h-96">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={chartData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#374151" strokeOpacity={0.3} />
                  <XAxis
                    dataKey="age"
                    stroke="#9CA3AF"
                    fontSize={12}
                    tickLine={false}
                    label={{ value: 'Age', position: 'insideBottom', offset: -5, fill: '#9CA3AF', fontSize: 12 }}
                  />
                  <YAxis
                    stroke="#9CA3AF"
                    fontSize={12}
                    tickLine={false}
                    tickFormatter={(v: number) => {
                      if (v >= 1_000_000) return `$${(v / 1_000_000).toFixed(1)}M`;
                      if (v >= 1_000) return `$${(v / 1_000).toFixed(0)}k`;
                      return `$${v}`;
                    }}
                  />
                  <Tooltip
                    contentStyle={{
                      backgroundColor: '#1F2937',
                      border: '1px solid #374151',
                      borderRadius: '12px',
                      padding: '12px',
                    }}
                    labelStyle={{ color: '#9CA3AF', fontSize: '12px' }}
                    labelFormatter={(age) => `Age ${age}`}
                    formatter={(value: number | undefined, name?: string) => {
                      const scenario = scenarios.find((s) => s.id === name);
                      return [fmtDollar(value ?? 0), scenario?.name ?? name ?? ''];
                    }}
                  />
                  <Legend
                    formatter={(value: string) => {
                      const scenario = scenarios.find((s) => s.id === value);
                      return scenario?.name ?? value;
                    }}
                  />
                  {scenarios.map((s) => (
                    <Line
                      key={s.id}
                      type="monotone"
                      dataKey={s.id}
                      stroke={s.color}
                      strokeWidth={s.id === scenarios[0].id ? 3 : 2}
                      strokeDasharray={s.id === scenarios[0].id ? undefined : '6 3'}
                      dot={false}
                      activeDot={{ r: 5, fill: s.color }}
                    />
                  ))}
                  {scenarios.map((s) => (
                    <ReferenceLine
                      key={`ref-${s.id}`}
                      x={s.retirementAge}
                      stroke={s.color}
                      strokeDasharray="8 4"
                      strokeWidth={1}
                      strokeOpacity={0.5}
                    />
                  ))}
                </LineChart>
              </ResponsiveContainer>
            </div>
          )}
        </div>

        {/* Comparison Table */}
        {scenarioResults.length > 1 && (
          <div className="bg-gradient-to-br from-gray-800 to-gray-800/50 rounded-2xl p-6 border border-gray-700/50 mb-8 overflow-x-auto">
            <h3 className="text-lg font-bold text-white mb-4">Scenario Comparison</h3>
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-gray-700/50">
                  <th className="text-left text-gray-400 py-3 pr-4">Scenario</th>
                  <th className="text-right text-gray-400 py-3 px-4">Retire At</th>
                  <th className="text-right text-gray-400 py-3 px-4">Monthly Contrib.</th>
                  <th className="text-right text-gray-400 py-3 px-4">Real Return</th>
                  <th className="text-right text-gray-400 py-3 px-4">Balance at Retirement</th>
                  <th className="text-right text-gray-400 py-3 pl-4">Confidence</th>
                </tr>
              </thead>
              <tbody>
                {scenarioResults.map((r) => (
                  <tr key={r.scenario.id} className="border-b border-gray-700/30">
                    <td className="py-3 pr-4">
                      <div className="flex items-center space-x-2">
                        <div className="w-2.5 h-2.5 rounded-full" style={{ backgroundColor: r.scenario.color }} />
                        <span className="text-white font-medium">{r.scenario.name}</span>
                      </div>
                    </td>
                    <td className="text-right text-gray-300 py-3 px-4">{r.scenario.retirementAge}</td>
                    <td className="text-right text-gray-300 py-3 px-4">{fmtDollar(r.scenario.monthlyContribution)}/mo</td>
                    <td className="text-right text-gray-300 py-3 px-4">{((r.scenario.growthRate - r.scenario.inflationRate) * 100).toFixed(1)}%</td>
                    <td className="text-right text-white font-bold py-3 px-4">{fmtDollar(r.retirementBalance)}</td>
                    <td className={`text-right font-bold py-3 pl-4 ${getConfidenceColor(r.confidence)}`}>{(r.confidence * 100).toFixed(0)}%</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        {/* Insights */}
        <div className="bg-gradient-to-br from-gray-800 to-gray-800/50 rounded-2xl p-6 border border-gray-700/50">
          <h3 className="text-lg font-bold text-white mb-4">Insights</h3>
          <div className="space-y-3">
            {primaryResult && (
              <>
                <InsightRow
                  icon={<TrendingUp size={16} />}
                  text={`Starting from ${fmtDollar(netWorth)} net worth with ${fmtDollar(scenarios[0].monthlyContribution)}/mo contributions at a ${((scenarios[0].growthRate - scenarios[0].inflationRate) * 100).toFixed(1)}% real return (${(scenarios[0].growthRate * 100).toFixed(1)}% growth − ${(scenarios[0].inflationRate * 100).toFixed(1)}% inflation), your portfolio could reach ${fmtDollar(primaryResult.retirementBalance)} in today's dollars by age ${scenarios[0].retirementAge}.`}
                />
                <InsightRow
                  icon={<DollarSign size={16} />}
                  text={`Your ${fmtDollar(scenarios[0].monthlyContribution)}/mo contribution adds ${fmtDollar(scenarios[0].monthlyContribution * 12)}/year to your portfolio before growth. Over ${scenarios[0].retirementAge - currentAge} years, that's ${fmtDollar(scenarios[0].monthlyContribution * 12 * (scenarios[0].retirementAge - currentAge))} in contributions alone.`}
                />
                <InsightRow
                  icon={primaryResult.confidence >= 0.5 ? <ShieldCheck size={16} /> : <AlertTriangle size={16} />}
                  text={
                    primaryResult.confidence >= 0.8
                      ? 'Based on 1,000 Monte Carlo simulations with 12% market volatility, your base scenario has a strong chance of sustaining 30 years in retirement.'
                      : primaryResult.confidence >= 0.5
                        ? 'Your base scenario has moderate resilience. Try adding a scenario with higher contributions or a later retirement age to compare.'
                        : 'Your base scenario carries significant risk. Use the scenario comparison to find a plan that reaches at least 80% confidence.'
                  }
                />
                {scenarioResults.length > 1 && (
                  <InsightRow
                    icon={<TrendingUp size={16} />}
                    text={(() => {
                      const best = scenarioResults.reduce((a, b) => (b.confidence > a.confidence ? b : a));
                      return best.scenario.id !== scenarios[0].id
                        ? `"${best.scenario.name}" has the highest confidence at ${(best.confidence * 100).toFixed(0)}%, outperforming your base scenario by ${((best.confidence - primaryResult.confidence) * 100).toFixed(0)} percentage points.`
                        : `Your base scenario "${best.scenario.name}" currently has the highest confidence among all scenarios.`;
                    })()}
                  />
                )}
              </>
            )}
          </div>
        </div>
      </div>
    </div>
  );
};

const InsightRow: React.FC<{ icon: React.ReactNode; text: string }> = ({ icon, text }) => (
  <div className="flex items-start space-x-3 p-3 bg-gray-700/30 rounded-xl border border-gray-600/30">
    <div className="text-blue-400 mt-0.5 flex-shrink-0">{icon}</div>
    <p className="text-sm text-gray-300">{text}</p>
  </div>
);

export default Retirement;
