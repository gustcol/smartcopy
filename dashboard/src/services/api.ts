/**
 * SmartCopy Dashboard API Client
 *
 * This module provides typed API calls to the SmartCopy backend.
 * The dashboard is designed for large-scale HPC environments where
 * visibility into transfer operations is critical.
 */

const API_BASE = '/api'

export interface SystemStatus {
  version: string
  uptime_seconds: number
  active_jobs: number
  connected_agents: number
  total_bytes_transferred: number
  total_files_transferred: number
  health: 'healthy' | 'degraded' | 'unhealthy'
  timestamp: string
}

export interface TransferJob {
  id: string
  name: string
  source: string
  destination: string
  status: 'pending' | 'running' | 'completed' | 'failed' | 'cancelled' | 'paused'
  config: JobConfig
  progress: JobProgress
  started_at: string | null
  ended_at: string | null
  error: string | null
  created_at: string
}

export interface JobConfig {
  threads: number
  buffer_size: number
  verify: boolean
  verify_algorithm: string | null
  compression: boolean
  incremental: boolean
  delta: boolean
  bandwidth_limit: number
  parallel_connections: number
}

export interface JobProgress {
  total_files: number
  files_transferred: number
  files_failed: number
  files_skipped: number
  total_bytes: number
  bytes_transferred: number
  throughput: number
  eta_seconds: number | null
  percent: number
  current_file: string | null
}

export interface HistoryEntry {
  id: string
  job_id: string
  name: string
  source: string
  destination: string
  transfer_type: 'local' | 'ssh' | 'tcp' | 'quic' | 'agent'
  started_at: string
  ended_at: string
  duration_seconds: number
  status: 'success' | 'partial_success' | 'failed' | 'cancelled'
  stats: TransferStats
}

export interface TransferStats {
  total_files: number
  files_transferred: number
  files_failed: number
  files_skipped: number
  bytes_transferred: number
  total_source_bytes: number
  avg_throughput: number
  peak_throughput: number
  min_throughput: number
  files_per_second: number
}

export interface TransferComparison {
  entries: HistoryEntrySummary[]
  comparison: ComparisonMetrics
  trend: PerformanceTrend
  anomalies: Anomaly[]
  recommendations: string[]
}

export interface HistoryEntrySummary {
  id: string
  timestamp: string
  duration_seconds: number
  bytes_transferred: number
  files_transferred: number
  avg_throughput: number
  status: string
}

export interface ComparisonMetrics {
  throughput: MetricComparison
  duration: MetricComparison
  success_rate: MetricComparison
  files_per_second: MetricComparison
}

export interface MetricComparison {
  min: number
  max: number
  avg: number
  stddev: number
  percent_change: number
  values: number[]
}

export interface PerformanceTrend {
  direction: 'improving' | 'stable' | 'degrading' | 'volatile'
  strength: number
  prediction: number | null
  confidence: number
}

export interface Anomaly {
  entry_id: string
  anomaly_type: string
  severity: number
  description: string
  metric: string
  expected: number
  actual: number
}

export interface AgentInfo {
  id: string
  hostname: string
  ip_address: string
  port: number
  protocol: string
  version: string
  status: 'connected' | 'disconnected' | 'busy' | 'error'
  connected_at: string
  last_heartbeat: string
}

export interface SystemInfo {
  hostname: string
  os: string
  cpu_model: string
  cpu_cores_physical: number
  cpu_cores_logical: number
  total_memory: number
  available_memory: number
  storage_devices: StorageDevice[]
  io_uring_supported: boolean
  kernel_version: string | null
}

export interface StorageDevice {
  name: string
  mount_point: string
  fs_type: string
  total_bytes: number
  available_bytes: number
  device_type: string
}

export interface AggregateStats {
  period_days: number
  total_jobs: number
  successful_jobs: number
  failed_jobs: number
  success_rate: number
  total_bytes_transferred: number
  total_files_transferred: number
  total_duration_seconds: number
  avg_throughput: number
  avg_job_duration: number
}

export interface PaginatedResponse<T> {
  items: T[]
  total: number
  page: number
  per_page: number
  total_pages: number
}

async function fetchApi<T>(endpoint: string, options?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE}${endpoint}`, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...options?.headers,
    },
  })

  if (!response.ok) {
    const error = await response.json().catch(() => ({ message: response.statusText }))
    throw new Error(error.message || 'API request failed')
  }

  return response.json()
}

export const api = {
  // System status
  getStatus: () => fetchApi<SystemStatus>('/status'),

  // Jobs
  getJobs: (page = 1, perPage = 20) =>
    fetchApi<PaginatedResponse<TransferJob>>(`/jobs?page=${page}&per_page=${perPage}`),

  getJob: (id: string) => fetchApi<TransferJob>(`/jobs/${id}`),

  createJob: (data: {
    name?: string
    source: string
    destination: string
    config?: Partial<JobConfig>
  }) =>
    fetchApi<TransferJob>('/jobs', {
      method: 'POST',
      body: JSON.stringify(data),
    }),

  cancelJob: (id: string) =>
    fetchApi<TransferJob>(`/jobs/${id}`, { method: 'DELETE' }),

  // History
  getHistory: (page = 1, perPage = 20, source?: string, destination?: string) => {
    const params = new URLSearchParams({ page: String(page), per_page: String(perPage) })
    if (source) params.set('source', source)
    if (destination) params.set('destination', destination)
    return fetchApi<PaginatedResponse<HistoryEntry>>(`/history?${params}`)
  },

  getHistoryEntry: (id: string) => fetchApi<HistoryEntry>(`/history/${id}`),

  getHistoryStats: (days = 30) =>
    fetchApi<AggregateStats>(`/history/stats?days=${days}`),

  // Compare
  compareTransfers: (ids: string[]) =>
    fetchApi<TransferComparison>(`/compare?ids=${ids.join(',')}`),

  // Agents
  getAgents: () => fetchApi<AgentInfo[]>('/agents'),

  // System
  getSystemInfo: () => fetchApi<SystemInfo>('/system'),

  // Metrics (raw text)
  getMetrics: async () => {
    const response = await fetch(`${API_BASE}/metrics`)
    return response.text()
  },
}

export default api
