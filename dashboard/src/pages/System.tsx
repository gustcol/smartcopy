import { useApi } from '../hooks/useApi'
import api from '../services/api'
import { formatBytes, formatPercent } from '../utils/format'
import clsx from 'clsx'

export default function System() {
  const { data: system, loading, refetch } = useApi(() => api.getSystemInfo())

  if (loading) {
    return (
      <div className="space-y-6">
        <h1 className="text-2xl font-bold text-white">System Information</h1>
        <div className="card animate-pulse">
          <div className="h-8 bg-slate-700 rounded w-1/3 mb-4"></div>
          <div className="space-y-3">
            {[1, 2, 3, 4].map((i) => (
              <div key={i} className="h-4 bg-slate-700 rounded w-full"></div>
            ))}
          </div>
        </div>
      </div>
    )
  }

  if (!system) {
    return (
      <div className="space-y-6">
        <h1 className="text-2xl font-bold text-white">System Information</h1>
        <div className="card text-center py-12 text-red-400">
          Failed to load system information
        </div>
      </div>
    )
  }

  const memoryUsage = ((system.total_memory - system.available_memory) / system.total_memory) * 100

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold text-white">System Information</h1>
        <button onClick={refetch} className="btn-secondary">
          Refresh
        </button>
      </div>

      {/* Host info */}
      <div className="card">
        <h2 className="text-lg font-semibold text-white mb-4">Host</h2>
        <div className="grid grid-cols-2 gap-6">
          <div>
            <div className="text-slate-400 text-sm">Hostname</div>
            <div className="text-white text-lg">{system.hostname}</div>
          </div>
          <div>
            <div className="text-slate-400 text-sm">Operating System</div>
            <div className="text-white text-lg">{system.os}</div>
          </div>
          {system.kernel_version && (
            <div>
              <div className="text-slate-400 text-sm">Kernel</div>
              <div className="text-white text-lg">{system.kernel_version}</div>
            </div>
          )}
        </div>
      </div>

      {/* CPU info */}
      <div className="card">
        <h2 className="text-lg font-semibold text-white mb-4">CPU</h2>
        <div className="grid grid-cols-3 gap-6">
          <div>
            <div className="text-slate-400 text-sm">Model</div>
            <div className="text-white">{system.cpu_model}</div>
          </div>
          <div>
            <div className="text-slate-400 text-sm">Physical Cores</div>
            <div className="text-white text-2xl font-bold">{system.cpu_cores_physical}</div>
          </div>
          <div>
            <div className="text-slate-400 text-sm">Logical Cores</div>
            <div className="text-white text-2xl font-bold">{system.cpu_cores_logical}</div>
          </div>
        </div>
        {system.numa_nodes && (
          <div className="mt-4 pt-4 border-t border-slate-700">
            <div className="text-slate-400 text-sm">NUMA Nodes</div>
            <div className="text-white">{system.numa_nodes}</div>
          </div>
        )}
      </div>

      {/* Memory info */}
      <div className="card">
        <h2 className="text-lg font-semibold text-white mb-4">Memory</h2>
        <div className="grid grid-cols-3 gap-6 mb-4">
          <div>
            <div className="text-slate-400 text-sm">Total</div>
            <div className="text-white text-2xl font-bold">
              {formatBytes(system.total_memory)}
            </div>
          </div>
          <div>
            <div className="text-slate-400 text-sm">Available</div>
            <div className="text-white text-2xl font-bold">
              {formatBytes(system.available_memory)}
            </div>
          </div>
          <div>
            <div className="text-slate-400 text-sm">Usage</div>
            <div className="text-white text-2xl font-bold">
              {formatPercent(memoryUsage)}
            </div>
          </div>
        </div>
        <div className="h-3 bg-slate-700 rounded-full overflow-hidden">
          <div
            className={clsx(
              'h-full transition-all',
              memoryUsage > 90 ? 'bg-red-500' : memoryUsage > 70 ? 'bg-yellow-500' : 'bg-green-500'
            )}
            style={{ width: `${memoryUsage}%` }}
          />
        </div>
      </div>

      {/* Storage devices */}
      <div className="card">
        <h2 className="text-lg font-semibold text-white mb-4">Storage Devices</h2>
        <div className="space-y-4">
          {system.storage_devices.map((device, i) => {
            const usage = ((device.total_bytes - device.available_bytes) / device.total_bytes) * 100

            return (
              <div key={i} className="p-4 bg-slate-900 rounded-lg">
                <div className="flex items-center justify-between mb-2">
                  <div>
                    <span className="text-white font-medium">{device.name}</span>
                    <span className="text-slate-400 text-sm ml-2">({device.mount_point})</span>
                  </div>
                  <div className="flex items-center gap-4 text-sm">
                    <span className="text-slate-400">
                      {device.fs_type}
                    </span>
                    <DeviceTypeBadge type={device.device_type} />
                  </div>
                </div>
                <div className="flex items-center gap-4">
                  <div className="flex-1">
                    <div className="h-2 bg-slate-700 rounded-full overflow-hidden">
                      <div
                        className={clsx(
                          'h-full',
                          usage > 90 ? 'bg-red-500' : usage > 70 ? 'bg-yellow-500' : 'bg-blue-500'
                        )}
                        style={{ width: `${usage}%` }}
                      />
                    </div>
                  </div>
                  <div className="text-sm text-slate-300 whitespace-nowrap">
                    {formatBytes(device.available_bytes)} free of {formatBytes(device.total_bytes)}
                  </div>
                </div>
              </div>
            )
          })}
        </div>
      </div>

      {/* Features */}
      <div className="card">
        <h2 className="text-lg font-semibold text-white mb-4">SmartCopy Features</h2>
        <div className="grid grid-cols-2 gap-4">
          <FeatureItem
            name="io_uring"
            enabled={system.io_uring_supported}
            description="Async I/O for maximum throughput"
          />
          <FeatureItem
            name="NUMA Awareness"
            enabled={!!system.numa_nodes}
            description="Thread pinning for multi-socket systems"
          />
        </div>
      </div>
    </div>
  )
}

function DeviceTypeBadge({ type }: { type: string }) {
  const styles: Record<string, string> = {
    nvme: 'bg-purple-500/20 text-purple-400',
    ssd: 'bg-blue-500/20 text-blue-400',
    hdd: 'bg-slate-500/20 text-slate-400',
    network: 'bg-green-500/20 text-green-400',
    unknown: 'bg-slate-500/20 text-slate-400',
  }

  return (
    <span className={clsx('px-2 py-0.5 rounded text-xs font-medium uppercase', styles[type] ?? styles.unknown)}>
      {type}
    </span>
  )
}

function FeatureItem({
  name,
  enabled,
  description,
}: {
  name: string
  enabled: boolean
  description: string
}) {
  return (
    <div className="flex items-start gap-3 p-3 bg-slate-900 rounded-lg">
      <div
        className={clsx(
          'w-5 h-5 rounded-full flex items-center justify-center text-xs',
          enabled ? 'bg-green-500/20 text-green-400' : 'bg-slate-600/20 text-slate-400'
        )}
      >
        {enabled ? '✓' : '✗'}
      </div>
      <div>
        <div className="text-white font-medium">{name}</div>
        <div className="text-slate-400 text-sm">{description}</div>
      </div>
    </div>
  )
}
