import { useState } from 'react'
import { usePolling } from '../hooks/useApi'
import api, { TransferJob } from '../services/api'
import { formatBytes, formatDuration, formatPercent } from '../utils/format'

export default function Jobs() {
  const [page, setPage] = useState(1)
  const { data: jobsResponse, loading, refetch } = usePolling(
    () => api.getJobs(page, 20),
    5000
  )

  const jobs = jobsResponse?.items ?? []

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold text-white">Transfer Jobs</h1>
        <button
          onClick={refetch}
          className="btn-secondary"
          disabled={loading}
        >
          Refresh
        </button>
      </div>

      {loading && jobs.length === 0 ? (
        <LoadingState />
      ) : jobs.length === 0 ? (
        <EmptyState />
      ) : (
        <>
          <div className="space-y-4">
            {jobs.map((job) => (
              <JobCard key={job.id} job={job} onCancel={refetch} />
            ))}
          </div>

          {/* Pagination */}
          {jobsResponse && jobsResponse.total_pages > 1 && (
            <div className="flex items-center justify-center gap-2">
              <button
                onClick={() => setPage((p) => Math.max(1, p - 1))}
                disabled={page === 1}
                className="btn-secondary disabled:opacity-50"
              >
                Previous
              </button>
              <span className="text-slate-400 px-4">
                Page {page} of {jobsResponse.total_pages}
              </span>
              <button
                onClick={() => setPage((p) => Math.min(jobsResponse.total_pages, p + 1))}
                disabled={page === jobsResponse.total_pages}
                className="btn-secondary disabled:opacity-50"
              >
                Next
              </button>
            </div>
          )}
        </>
      )}
    </div>
  )
}

function JobCard({ job, onCancel }: { job: TransferJob; onCancel: () => void }) {
  const [cancelling, setCancelling] = useState(false)

  const handleCancel = async () => {
    if (!confirm('Are you sure you want to cancel this job?')) return

    setCancelling(true)
    try {
      await api.cancelJob(job.id)
      onCancel()
    } catch (error) {
      console.error('Failed to cancel job:', error)
    } finally {
      setCancelling(false)
    }
  }

  const isRunning = job.status === 'running'
  const canCancel = job.status === 'running' || job.status === 'pending'

  return (
    <div className="card">
      <div className="flex items-start justify-between">
        <div>
          <h3 className="text-lg font-semibold text-white">{job.name}</h3>
          <p className="text-sm text-slate-400 mt-1">
            {job.source} → {job.destination}
          </p>
        </div>
        <div className="flex items-center gap-2">
          <StatusBadge status={job.status} />
          {canCancel && (
            <button
              onClick={handleCancel}
              disabled={cancelling}
              className="text-red-400 hover:text-red-300 text-sm"
            >
              {cancelling ? 'Cancelling...' : 'Cancel'}
            </button>
          )}
        </div>
      </div>

      {/* Progress bar for running jobs */}
      {isRunning && (
        <div className="mt-4">
          <div className="flex items-center justify-between text-sm text-slate-400 mb-2">
            <span>{formatPercent(job.progress.percent)} complete</span>
            <span>
              {formatBytes(job.progress.bytes_transferred)} / {formatBytes(job.progress.total_bytes)}
            </span>
          </div>
          <div className="h-2 bg-slate-700 rounded-full overflow-hidden">
            <div
              className="h-full bg-primary-500 transition-all duration-300"
              style={{ width: `${job.progress.percent}%` }}
            />
          </div>
          <div className="flex items-center justify-between text-sm text-slate-400 mt-2">
            <span>{formatBytes(job.progress.throughput)}/s</span>
            {job.progress.eta_seconds && (
              <span>ETA: {formatDuration(job.progress.eta_seconds)}</span>
            )}
          </div>
          {job.progress.current_file && (
            <p className="text-xs text-slate-500 mt-2 truncate">
              Current: {job.progress.current_file}
            </p>
          )}
        </div>
      )}

      {/* Stats for completed/failed jobs */}
      {(job.status === 'completed' || job.status === 'failed') && (
        <div className="mt-4 grid grid-cols-4 gap-4 text-sm">
          <div>
            <div className="text-slate-400">Files</div>
            <div className="text-white">
              {job.progress.files_transferred} / {job.progress.total_files}
            </div>
          </div>
          <div>
            <div className="text-slate-400">Size</div>
            <div className="text-white">{formatBytes(job.progress.bytes_transferred)}</div>
          </div>
          <div>
            <div className="text-slate-400">Throughput</div>
            <div className="text-white">{formatBytes(job.progress.throughput)}/s</div>
          </div>
          <div>
            <div className="text-slate-400">Duration</div>
            <div className="text-white">
              {job.started_at && job.ended_at
                ? formatDuration(
                    (new Date(job.ended_at).getTime() - new Date(job.started_at).getTime()) / 1000
                  )
                : 'N/A'}
            </div>
          </div>
        </div>
      )}

      {/* Error message */}
      {job.error && (
        <div className="mt-4 p-3 bg-red-900/30 border border-red-800 rounded text-red-300 text-sm">
          {job.error}
        </div>
      )}
    </div>
  )
}

function StatusBadge({ status }: { status: string }) {
  const styles: Record<string, string> = {
    pending: 'bg-slate-500/20 text-slate-400',
    running: 'bg-blue-500/20 text-blue-400',
    completed: 'bg-green-500/20 text-green-400',
    failed: 'bg-red-500/20 text-red-400',
    cancelled: 'bg-slate-500/20 text-slate-400',
    paused: 'bg-yellow-500/20 text-yellow-400',
  }

  return (
    <span className={`px-2 py-1 rounded text-xs font-medium ${styles[status] ?? styles.pending}`}>
      {status}
    </span>
  )
}

function LoadingState() {
  return (
    <div className="space-y-4">
      {[1, 2, 3].map((i) => (
        <div key={i} className="card animate-pulse">
          <div className="h-6 bg-slate-700 rounded w-1/3 mb-2"></div>
          <div className="h-4 bg-slate-700 rounded w-2/3"></div>
        </div>
      ))}
    </div>
  )
}

function EmptyState() {
  return (
    <div className="card text-center py-12">
      <p className="text-slate-400">No transfer jobs found</p>
      <p className="text-sm text-slate-500 mt-2">
        Create a job using the SmartCopy CLI or API
      </p>
    </div>
  )
}
