import { usePolling } from '../hooks/useApi'
import api, { AgentInfo } from '../services/api'
import { formatRelativeTime } from '../utils/format'
import clsx from 'clsx'

export default function Agents() {
  const { data: agents, loading, refetch } = usePolling(() => api.getAgents(), 10000)

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold text-white">Connected Agents</h1>
        <button onClick={refetch} className="btn-secondary" disabled={loading}>
          Refresh
        </button>
      </div>

      {/* Info card */}
      <div className="bg-slate-800/50 border border-slate-700 rounded-lg p-4">
        <p className="text-slate-300 text-sm">
          Agents are remote SmartCopy instances that enable high-performance delta sync
          and distributed transfers. Deploy agents using:{' '}
          <code className="bg-slate-700 px-1 rounded">smartcopy agent --protocol tcp --port 9878</code>
        </p>
      </div>

      {loading && !agents ? (
        <LoadingState />
      ) : !agents || agents.length === 0 ? (
        <EmptyState />
      ) : (
        <div className="grid grid-cols-2 gap-6">
          {agents.map((agent) => (
            <AgentCard key={agent.id} agent={agent} />
          ))}
        </div>
      )}
    </div>
  )
}

function AgentCard({ agent }: { agent: AgentInfo }) {
  const statusColors = {
    connected: 'bg-green-500',
    busy: 'bg-yellow-500',
    disconnected: 'bg-slate-500',
    error: 'bg-red-500',
  }

  const statusLabels = {
    connected: 'Connected',
    busy: 'Busy',
    disconnected: 'Disconnected',
    error: 'Error',
  }

  return (
    <div className="card">
      <div className="flex items-start justify-between">
        <div>
          <h3 className="text-lg font-semibold text-white">{agent.hostname}</h3>
          <p className="text-sm text-slate-400">
            {agent.ip_address}:{agent.port}
          </p>
        </div>
        <div className="flex items-center gap-2">
          <div className={clsx('w-2 h-2 rounded-full', statusColors[agent.status])} />
          <span className="text-sm text-slate-300">{statusLabels[agent.status]}</span>
        </div>
      </div>

      <div className="mt-4 grid grid-cols-2 gap-4 text-sm">
        <div>
          <div className="text-slate-400">Protocol</div>
          <div className="text-white uppercase">{agent.protocol}</div>
        </div>
        <div>
          <div className="text-slate-400">Version</div>
          <div className="text-white">{agent.version}</div>
        </div>
        <div>
          <div className="text-slate-400">Connected</div>
          <div className="text-white">{formatRelativeTime(agent.connected_at)}</div>
        </div>
        <div>
          <div className="text-slate-400">Last Heartbeat</div>
          <div className="text-white">{formatRelativeTime(agent.last_heartbeat)}</div>
        </div>
      </div>

      {agent.system_info && (
        <div className="mt-4 pt-4 border-t border-slate-700">
          <h4 className="text-sm font-medium text-slate-400 mb-2">System Info</h4>
          <div className="grid grid-cols-3 gap-4 text-sm">
            <div>
              <div className="text-slate-500">CPU</div>
              <div className="text-white">{agent.system_info.cpu_cores} cores</div>
            </div>
            <div>
              <div className="text-slate-500">Memory</div>
              <div className="text-white">
                {(agent.system_info.total_memory / 1073741824).toFixed(1)} GB
              </div>
            </div>
            <div>
              <div className="text-slate-500">Disk</div>
              <div className="text-white">
                {(agent.system_info.disk_available / 1099511627776).toFixed(1)} TB free
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

function LoadingState() {
  return (
    <div className="grid grid-cols-2 gap-6">
      {[1, 2].map((i) => (
        <div key={i} className="card animate-pulse">
          <div className="h-6 bg-slate-700 rounded w-1/3 mb-2"></div>
          <div className="h-4 bg-slate-700 rounded w-1/2"></div>
        </div>
      ))}
    </div>
  )
}

function EmptyState() {
  return (
    <div className="card text-center py-12">
      <div className="text-4xl mb-4">🖥️</div>
      <p className="text-slate-400">No agents connected</p>
      <p className="text-sm text-slate-500 mt-2">
        Deploy agents to enable remote delta sync and distributed transfers
      </p>
      <div className="mt-4 p-4 bg-slate-900 rounded text-left text-sm">
        <pre className="text-slate-300">
          <code># On remote host{'\n'}smartcopy agent --protocol tcp --port 9878 --bind 0.0.0.0</code>
        </pre>
      </div>
    </div>
  )
}
