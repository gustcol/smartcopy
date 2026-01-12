import { useEffect, useState } from 'react'
import { useSearchParams } from 'react-router-dom'
import { useApi } from '../hooks/useApi'
import api, { TransferComparison } from '../services/api'
import { formatBytes, formatDuration, formatPercent } from '../utils/format'
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  LineChart,
  Line,
  Legend,
} from 'recharts'

export default function Compare() {
  const [searchParams] = useSearchParams()
  const idsParam = searchParams.get('ids')
  const ids = idsParam ? idsParam.split(',') : []

  const { data: comparison, loading, error } = useApi(
    () => api.compareTransfers(ids),
    { enabled: ids.length >= 2 }
  )

  if (ids.length < 2) {
    return (
      <div className="space-y-6">
        <h1 className="text-2xl font-bold text-white">Compare Transfers</h1>
        <div className="card text-center py-12">
          <p className="text-slate-400">
            Select at least 2 transfers from the History page to compare
          </p>
          <a href="/history" className="btn-primary mt-4 inline-block">
            Go to History
          </a>
        </div>
      </div>
    )
  }

  if (loading) {
    return (
      <div className="space-y-6">
        <h1 className="text-2xl font-bold text-white">Compare Transfers</h1>
        <div className="card text-center py-12 text-slate-400">
          Loading comparison...
        </div>
      </div>
    )
  }

  if (error || !comparison) {
    return (
      <div className="space-y-6">
        <h1 className="text-2xl font-bold text-white">Compare Transfers</h1>
        <div className="card text-center py-12 text-red-400">
          Failed to load comparison: {error?.message}
        </div>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold text-white">Transfer Comparison</h1>

      {/* Trend indicator */}
      <div className="card">
        <h2 className="text-lg font-semibold text-white mb-4">Performance Trend</h2>
        <div className="flex items-center gap-8">
          <TrendIndicator trend={comparison.trend} />
          <div className="text-slate-300">
            <p>
              Trend direction: <strong className="text-white">{comparison.trend.direction}</strong>
            </p>
            <p>
              Confidence: <strong className="text-white">{formatPercent(comparison.trend.confidence * 100)}</strong>
            </p>
            {comparison.trend.prediction && (
              <p>
                Predicted next throughput:{' '}
                <strong className="text-white">
                  {formatBytes(comparison.trend.prediction)}/s
                </strong>
              </p>
            )}
          </div>
        </div>
      </div>

      {/* Throughput comparison chart */}
      <div className="card">
        <h2 className="text-lg font-semibold text-white mb-4">Throughput Comparison</h2>
        <ResponsiveContainer width="100%" height={300}>
          <BarChart
            data={comparison.entries.map((e, i) => ({
              name: `Transfer ${i + 1}`,
              throughput: e.avg_throughput / 1_000_000,
              files: e.files_transferred,
            }))}
          >
            <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
            <XAxis dataKey="name" stroke="#94a3b8" />
            <YAxis stroke="#94a3b8" tickFormatter={(v) => `${v} MB/s`} />
            <Tooltip
              contentStyle={{ backgroundColor: '#1e293b', border: '1px solid #334155' }}
              labelStyle={{ color: '#f1f5f9' }}
            />
            <Bar dataKey="throughput" fill="#3b82f6" name="Throughput (MB/s)" />
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Metrics comparison */}
      <div className="grid grid-cols-2 gap-6">
        <MetricCard
          title="Throughput"
          metric={comparison.comparison.throughput}
          formatter={(v) => `${formatBytes(v)}/s`}
        />
        <MetricCard
          title="Duration"
          metric={comparison.comparison.duration}
          formatter={(v) => formatDuration(v)}
        />
        <MetricCard
          title="Success Rate"
          metric={comparison.comparison.success_rate}
          formatter={(v) => formatPercent(v)}
        />
        <MetricCard
          title="Files/Second"
          metric={comparison.comparison.files_per_second}
          formatter={(v) => v.toFixed(2)}
        />
      </div>

      {/* Anomalies */}
      {comparison.anomalies.length > 0 && (
        <div className="card">
          <h2 className="text-lg font-semibold text-white mb-4">Detected Anomalies</h2>
          <div className="space-y-3">
            {comparison.anomalies.map((anomaly, i) => (
              <div
                key={i}
                className="p-3 bg-yellow-900/30 border border-yellow-800 rounded"
              >
                <div className="flex items-center justify-between">
                  <span className="text-yellow-300 font-medium">
                    {anomaly.anomaly_type.replace('_', ' ')}
                  </span>
                  <span className="text-sm text-yellow-400">
                    Severity: {formatPercent(anomaly.severity * 100)}
                  </span>
                </div>
                <p className="text-yellow-200 text-sm mt-1">{anomaly.description}</p>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Recommendations */}
      {comparison.recommendations.length > 0 && (
        <div className="card">
          <h2 className="text-lg font-semibold text-white mb-4">Recommendations</h2>
          <ul className="space-y-2">
            {comparison.recommendations.map((rec, i) => (
              <li key={i} className="flex items-start gap-3 text-slate-300">
                <span className="text-primary-400 mt-1">•</span>
                <span>{rec}</span>
              </li>
            ))}
          </ul>
        </div>
      )}

      {/* Transfer details */}
      <div className="card">
        <h2 className="text-lg font-semibold text-white mb-4">Transfer Details</h2>
        <table className="w-full">
          <thead>
            <tr className="text-left text-slate-400 text-sm border-b border-slate-700">
              <th className="pb-3">#</th>
              <th className="pb-3">Date</th>
              <th className="pb-3">Status</th>
              <th className="pb-3">Files</th>
              <th className="pb-3">Size</th>
              <th className="pb-3">Throughput</th>
              <th className="pb-3">Duration</th>
            </tr>
          </thead>
          <tbody className="text-slate-300">
            {comparison.entries.map((entry, i) => (
              <tr key={entry.id} className="border-t border-slate-700">
                <td className="py-3">{i + 1}</td>
                <td className="py-3">{new Date(entry.timestamp).toLocaleDateString()}</td>
                <td className="py-3">{entry.status}</td>
                <td className="py-3">{entry.files_transferred.toLocaleString()}</td>
                <td className="py-3">{formatBytes(entry.bytes_transferred)}</td>
                <td className="py-3">{formatBytes(entry.avg_throughput)}/s</td>
                <td className="py-3">{formatDuration(entry.duration_seconds)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

function TrendIndicator({ trend }: { trend: TransferComparison['trend'] }) {
  const colors = {
    improving: 'text-green-400 bg-green-400/20',
    stable: 'text-blue-400 bg-blue-400/20',
    degrading: 'text-red-400 bg-red-400/20',
    volatile: 'text-yellow-400 bg-yellow-400/20',
  }

  const icons = {
    improving: '↑',
    stable: '→',
    degrading: '↓',
    volatile: '↕',
  }

  return (
    <div className={`w-16 h-16 rounded-full flex items-center justify-center text-3xl ${colors[trend.direction]}`}>
      {icons[trend.direction]}
    </div>
  )
}

function MetricCard({
  title,
  metric,
  formatter,
}: {
  title: string
  metric: { min: number; max: number; avg: number; stddev: number; percent_change: number }
  formatter: (v: number) => string
}) {
  const changeColor = metric.percent_change > 0 ? 'text-green-400' : metric.percent_change < 0 ? 'text-red-400' : 'text-slate-400'

  return (
    <div className="card">
      <h3 className="text-sm font-medium text-slate-400 uppercase">{title}</h3>
      <div className="mt-4 grid grid-cols-2 gap-4">
        <div>
          <div className="text-2xl font-bold text-white">{formatter(metric.avg)}</div>
          <div className="text-xs text-slate-500">Average</div>
        </div>
        <div className={`text-right ${changeColor}`}>
          <div className="text-2xl font-bold">
            {metric.percent_change > 0 ? '+' : ''}{metric.percent_change.toFixed(1)}%
          </div>
          <div className="text-xs text-slate-500">Change</div>
        </div>
      </div>
      <div className="mt-4 pt-4 border-t border-slate-700 grid grid-cols-3 gap-2 text-sm">
        <div>
          <div className="text-slate-400">Min</div>
          <div className="text-white">{formatter(metric.min)}</div>
        </div>
        <div>
          <div className="text-slate-400">Max</div>
          <div className="text-white">{formatter(metric.max)}</div>
        </div>
        <div>
          <div className="text-slate-400">Std Dev</div>
          <div className="text-white">{formatter(metric.stddev)}</div>
        </div>
      </div>
    </div>
  )
}
