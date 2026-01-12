import { useState, useEffect, useCallback } from 'react'

interface UseApiOptions {
  refetchInterval?: number
  enabled?: boolean
}

interface UseApiResult<T> {
  data: T | null
  error: Error | null
  loading: boolean
  refetch: () => void
}

export function useApi<T>(
  fetcher: () => Promise<T>,
  options: UseApiOptions = {}
): UseApiResult<T> {
  const { refetchInterval, enabled = true } = options
  const [data, setData] = useState<T | null>(null)
  const [error, setError] = useState<Error | null>(null)
  const [loading, setLoading] = useState(true)

  const fetch = useCallback(async () => {
    if (!enabled) return

    try {
      setLoading(true)
      const result = await fetcher()
      setData(result)
      setError(null)
    } catch (e) {
      setError(e instanceof Error ? e : new Error(String(e)))
    } finally {
      setLoading(false)
    }
  }, [fetcher, enabled])

  useEffect(() => {
    fetch()
  }, [fetch])

  useEffect(() => {
    if (!refetchInterval || !enabled) return

    const interval = setInterval(fetch, refetchInterval)
    return () => clearInterval(interval)
  }, [fetch, refetchInterval, enabled])

  return { data, error, loading, refetch: fetch }
}

export function usePolling<T>(
  fetcher: () => Promise<T>,
  intervalMs: number = 5000
): UseApiResult<T> {
  return useApi(fetcher, { refetchInterval: intervalMs })
}
