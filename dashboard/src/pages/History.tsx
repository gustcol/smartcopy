import { useState } from 'react'
import { usePolling } from '../hooks/useApi'
import api, { HistoryEntry } from '../services/api'
import { formatBytes, formatDuration, formatDateTime } from '../utils/format'

export default function History() {
  const [page, setPage] = useState(1)
  const [selectedIds, setSelectedIds] = useState<string[]>([])

  const { data: historyResponse, loading } = usePolling(
    () => api.getHistory(page, 20),
    10000
  )
  const { data: stats } = usePolling(() => api.getHistoryStats(30), 60000)

  const history = historyResponse?.items ?? []

  const toggleSelection = (id: string) => {
    setSelectedIds((prev) =>
      prev.includes(id) ? prev.filter((x) => x !== id) : [...prev, id]
    )
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold text-white">Transfer History</h1>
        {selectedIds.length >= 2 && (
          <a
            href={`/compare?ids=${selectedIds.join(',')}`}
            className="btn-primary"
          >
            Compare Selected ({selectedIds.length})
          </a>
        )}
      </div>

      {/* 30-day summary */}
      {stats && (
        <div className="card">
          <h2 className="text-lg font-semibold text-white mb-4">Last 30 Days</h2>
          <div className="grid grid-cols-5 gap-6">
            <div>
              <div className="stat-value">{stats.total_jobs}</div>
              <div className="stat-label">Total Transfers</div>
            </div>
            <div>
              <div className="stat-value text-green-400">{stats.success_rate.toFixed(1)}%</div>
              <div className="stat-label">Success Rate</div>
            </div>
            <div>
              <div className="stat-value">{formatBytes(stats.total_bytes_transferred)}</div>
              <div className="stat-label">Total Transferred</div>
            </div>
            <div>
              <div className="stat-value">{formatBytes(stats.avg_throughput)}/s</div>
              <div className="stat-label">Avg Throughput</div>
            </div>
            <div>
              <div className="stat-value">{formatDuration(stats.avg_job_duration)}</div>
              <div className="stat-label">Avg Duration</div>
            </div>
          </div>
        </div>
      )}

      {/* History table */}
      <div className="card overflow-hidden">
        {loading && history.length === 0 ? (
          <LoadingState />
        ) : history.length === 0 ? (
          <EmptyState />
        ) : (
          <table className="w-full">
            <thead>
              <tr className="text-left text-slate-400 text-sm border-b border-slate-700">
                <th className="pb-3 pr-4 w-8">
                  <input
                    type="checkbox"
                    checked={selectedIds.length === history.length}
                    onChange={() =>
                      setSelectedIds(
                        selectedIds.length === history.length
                          ? []
                          : history.map((h) => h.id)
                      )
                    }
                    className="rounded border-slate-600"
                  />
                </th>
                <th className="pb-3">Name</th>
                <th className="pb-3">Status</th>
                <th className="pb-3">Type</th>
                <th className="pb-3">Size</th>
                <th className="pb-3">Throughput</th>
                <th className="pb-3">Duration</th>
                <th className="pb-3">Date</th>
              </tr>
            </thead>
            <tbody className="text-slate-300">
              {history.map((entry) => (
                <HistoryRow
                  key={entry.id}
                  entry={entry}
                  selected={selectedIds.includes(entry.id)}
                  onToggle={() => toggleSelection(entry.id)}
                />
              ))}
            </tbody>
          </table>
        )}
      </div>

      {/* Pagination */}
      {historyResponse && historyResponse.total_pages > 1 && (
        <div className="flex items-center justify-center gap-2">
          <button
            onClick={() => setPage((p) => Math.max(1, p - 1))}
            disabled={page === 1}
            className="btn-secondary disabled:opacity-50"
          >
            Previous
          </button>
          <span className="text-slate-400 px-4">
            Page {page} of {historyResponse.total_pages}
          </span>
          <button
            onClick={() => setPage((p) => Math.min(historyResponse.total_pages, p + 1))}
            disabled={page === historyResponse.total_pages}
            className="btn-secondary disabled:opacity-50"
          >
            Next
          </button>
        </div>
      )}
    </div>
  )
}

function HistoryRow({
  entry,
  selected,
  onToggle,
}: {
  entry: HistoryEntry
  selected: boolean
  onToggle: () => void
}) {
  const statusColors: Record<string, string> = {
    success: 'text-green-400',
    partial_success: 'text-yellow-400',
    failed: 'text-red-400',
    cancelled: 'text-slate-400',
  }

  const typeLabels: Record<string, string> = {
    local: 'Local',
    ssh: 'SSH',
    tcp: 'TCP',
    quic: 'QUIC',
    agent: 'Agent',
  }

  return (
    <tr className="border-t border-slate-700 hover:bg-slate-700/50">
      <td className="py-3 pr-4">
        <input
          type="checkbox"
          checked={selected}
          onChange={onToggle}
          className="rounded border-slate-600"
        />
      </td>
      <td className="py-3">
        <div className="font-medium">{entry.name}</div>
        <div className="text-xs text-slate-500 truncate max-w-xs">
          {entry.source} → {entry.destination}
        </div>
      </td>
      <td className={`py-3 ${statusColors[entry.status] ?? ''}`}>
        {entry.status.replace('_', ' ')}
      </td>
      <td className="py-3 text-slate-400">
        {typeLabels[entry.transfer_type] ?? entry.transfer_type}
      </td>
      <td className="py-3">{formatBytes(entry.stats.bytes_transferred)}</td>
      <td className="py-3">{formatBytes(entry.stats.avg_throughput)}/s</td>
      <td className="py-3">{formatDuration(entry.duration_seconds)}</td>
      <td className="py-3 text-slate-400">{formatDateTime(entry.started_at)}</td>
    </tr>
  )
}

function LoadingState() {
  return (
    <div className="p-8 text-center text-slate-400">
      Loading history...
    </div>
  )
}

function EmptyState() {
  return (
    <div className="p-8 text-center text-slate-400">
      No transfer history found
    </div>
  )
}
