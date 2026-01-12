import { usePolling } from '../hooks/useApi'
import api from '../services/api'
import { formatBytes, formatDuration, formatNumber } from '../utils/format'
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  AreaChart,
  Area,
} from 'recharts'

export default function Dashboard() {
  const { data: status, loading: statusLoading } = usePolling(() => api.getStatus(), 5000)
  const { data: stats } = usePolling(() => api.getHistoryStats(7), 30000)
  const { data: history } = usePolling(() => api.getHistory(1, 10), 10000)

  // Build throughput chart data from recent history
  const chartData = history?.items
    .slice()
    .reverse()
    .map((entry) => ({
      name: new Date(entry.started_at).toLocaleDateString(),
      throughput: entry.stats.avg_throughput / 1_000_000, // MB/s
      files: entry.stats.files_transferred,
    })) ?? []

  return (
    <div className="space-y-8">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold text-white">Dashboard</h1>
        <div className="text-sm text-slate-400">
          {status ? `Last updated: ${new Date(status.timestamp).toLocaleTimeString()}` : 'Loading...'}
        </div>
      </div>

      {/* Important notice for large environments */}
      <div className="bg-blue-900/30 border border-blue-800 rounded-lg p-4">
        <p className="text-blue-300 text-sm">
          This dashboard is designed for <strong>large-scale HPC environments</strong>.
          For small-scale use, the CLI provides all necessary functionality.
          <strong> Do NOT run SmartCopy inside Docker</strong> - only the dashboard should be containerized.
        </p>
      </div>

      {/* Stats grid */}
      <div className="grid grid-cols-4 gap-6">
        <StatCard
          label="Active Jobs"
          value={status?.active_jobs ?? 0}
          loading={statusLoading}
          trend={status?.active_jobs && status.active_jobs > 0 ? 'running' : undefined}
        />
        <StatCard
          label="Connected Agents"
          value={status?.connected_agents ?? 0}
          loading={statusLoading}
        />
        <StatCard
          label="Total Transferred"
          value={formatBytes(status?.total_bytes_transferred ?? 0)}
          loading={statusLoading}
        />
        <StatCard
          label="Files Transferred"
          value={formatNumber(status?.total_files_transferred ?? 0)}
          loading={statusLoading}
        />
      </div>

      {/* Last 7 days summary */}
      {stats && (
        <div className="card">
          <h2 className="text-lg font-semibold text-white mb-4">Last 7 Days Summary</h2>
          <div className="grid grid-cols-5 gap-6">
            <div>
              <div className="stat-value">{stats.total_jobs}</div>
              <div className="stat-label">Total Jobs</div>
            </div>
            <div>
              <div className="stat-value text-green-400">{stats.successful_jobs}</div>
              <div className="stat-label">Successful</div>
            </div>
            <div>
              <div className="stat-value text-red-400">{stats.failed_jobs}</div>
              <div className="stat-label">Failed</div>
            </div>
            <div>
              <div className="stat-value">{stats.success_rate.toFixed(1)}%</div>
              <div className="stat-label">Success Rate</div>
            </div>
            <div>
              <div className="stat-value">{formatBytes(stats.avg_throughput)}/s</div>
              <div className="stat-label">Avg Throughput</div>
            </div>
          </div>
        </div>
      )}

      {/* Throughput chart */}
      <div className="card">
        <h2 className="text-lg font-semibold text-white mb-4">Recent Transfer Throughput</h2>
        {chartData.length > 0 ? (
          <ResponsiveContainer width="100%" height={300}>
            <AreaChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
              <XAxis dataKey="name" stroke="#94a3b8" />
              <YAxis stroke="#94a3b8" tickFormatter={(v) => `${v} MB/s`} />
              <Tooltip
                contentStyle={{ backgroundColor: '#1e293b', border: '1px solid #334155' }}
                labelStyle={{ color: '#f1f5f9' }}
              />
              <Area
                type="monotone"
                dataKey="throughput"
                stroke="#3b82f6"
                fill="#3b82f6"
                fillOpacity={0.3}
                name="Throughput (MB/s)"
              />
            </AreaChart>
          </ResponsiveContainer>
        ) : (
          <div className="h-64 flex items-center justify-center text-slate-400">
            No recent transfer data available
          </div>
        )}
      </div>

      {/* Recent transfers */}
      <div className="card">
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-lg font-semibold text-white">Recent Transfers</h2>
          <a href="/history" className="text-sm text-primary-400 hover:text-primary-300">
            View all
          </a>
        </div>
        {history?.items && history.items.length > 0 ? (
          <table className="w-full">
            <thead>
              <tr className="text-left text-slate-400 text-sm">
                <th className="pb-3">Name</th>
                <th className="pb-3">Status</th>
                <th className="pb-3">Size</th>
                <th className="pb-3">Throughput</th>
                <th className="pb-3">Duration</th>
              </tr>
            </thead>
            <tbody className="text-slate-300">
              {history.items.slice(0, 5).map((entry) => (
                <tr key={entry.id} className="border-t border-slate-700">
                  <td className="py-3">{entry.name}</td>
                  <td className="py-3">
                    <StatusBadge status={entry.status} />
                  </td>
                  <td className="py-3">{formatBytes(entry.stats.bytes_transferred)}</td>
                  <td className="py-3">{formatBytes(entry.stats.avg_throughput)}/s</td>
                  <td className="py-3">{formatDuration(entry.duration_seconds)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
          <div className="text-center text-slate-400 py-8">No recent transfers</div>
        )}
      </div>
    </div>
  )
}

interface StatCardProps {
  label: string
  value: string | number
  loading?: boolean
  trend?: 'running' | 'up' | 'down'
}

function StatCard({ label, value, loading, trend }: StatCardProps) {
  return (
    <div className="card">
      {loading ? (
        <div className="animate-pulse">
          <div className="h-8 bg-slate-700 rounded w-20 mb-2"></div>
          <div className="h-4 bg-slate-700 rounded w-16"></div>
        </div>
      ) : (
        <>
          <div className="flex items-center gap-2">
            <div className="stat-value">{value}</div>
            {trend === 'running' && (
              <span className="w-2 h-2 rounded-full bg-green-500 animate-pulse"></span>
            )}
          </div>
          <div className="stat-label">{label}</div>
        </>
      )}
    </div>
  )
}

function StatusBadge({ status }: { status: string }) {
  const colors = {
    success: 'bg-green-500/20 text-green-400',
    partial_success: 'bg-yellow-500/20 text-yellow-400',
    failed: 'bg-red-500/20 text-red-400',
    cancelled: 'bg-slate-500/20 text-slate-400',
    running: 'bg-blue-500/20 text-blue-400',
    pending: 'bg-slate-500/20 text-slate-400',
  }

  return (
    <span className={`px-2 py-1 rounded text-xs font-medium ${colors[status as keyof typeof colors] ?? colors.pending}`}>
      {status.replace('_', ' ')}
    </span>
  )
}
