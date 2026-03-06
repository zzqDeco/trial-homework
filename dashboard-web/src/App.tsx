import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import * as echarts from 'echarts'

type Locale = 'en' | 'zh'
type Source = 'redis' | 'postgres' | 'mixed'
type Resolution = 'minute' | 'hour' | 'day'

type Summary = {
  source: Source
  bid_requests: number
  deduped_impressions: number
  unknown_impressions: number
  view_rate: number
  last_projected_at?: string
  projection_lag_seconds?: number
}

type CampaignMetrics = {
  campaign_id: string
  bid_requests: number
  deduped_impressions: number
  unknown_impressions: number
  view_rate: number
}

type TimeSeriesPoint = {
  ts: string
  bid_requests: number
  deduped_impressions: number
  unknown_impressions: number
  view_rate: number
}

type TimeSeriesResponse = {
  source: Source
  resolution: Resolution
  points: TimeSeriesPoint[]
}

type CampaignResponse = {
  source: Source
  campaigns: CampaignMetrics[]
}

type Dictionary = Record<keyof typeof messages.en, string>

type ChartPanelProps = {
  title: string
  caption: string
  option: echarts.EChartsOption
  height?: number
}

const localeStorageKey = 'dashboard-locale'
const minuteMs = 60_000
const refreshBufferMs = 1_000

const messages = {
  en: {
    title: 'Mini Ads Dashboard',
    subtitle: 'Independent monitoring surface for the Redis-backed read model and historical Postgres fallback.',
    range: 'Range',
    from: 'From',
    to: 'To',
    refresh: 'Refresh now',
    autoRefresh: 'Auto refresh every minute',
    nextRefresh: 'Next refresh',
    source: 'Source',
    lastProjected: 'Last projected',
    lag: 'Projection lag',
    kpiViewRate: 'View Rate',
    kpiBidRequests: 'Bid Requests',
    kpiDeduped: 'Deduped Impressions',
    kpiUnknown: 'Unknown Impressions',
    last1h: 'Last 1h',
    last6h: 'Last 6h',
    last24h: 'Last 24h',
    last7d: 'Last 7d',
    last30d: 'Last 30d',
    loading: 'Loading latest metrics…',
    empty: 'No data available for the selected range.',
    campaign: 'Campaign',
    bidRequests: 'Bid Requests',
    dedupedImpressions: 'Deduped Impressions',
    unknownImpressions: 'Unknown Impressions',
    tableTitle: 'Campaign breakdown',
    tableCaption: 'Sorted by bid request volume. UNKNOWN stays visible when unmatched impressions exist.',
    lineTitle: 'Demand vs matched delivery',
    lineCaption: 'Bid requests and deduped impressions across the selected range.',
    unknownTitle: 'Unknown impression volume',
    unknownCaption: 'Immediate unmatched counts based on current projection state.',
    campaignChartTitle: 'Top campaigns by bid requests',
    campaignChartCaption: 'Horizontal comparison of the heaviest campaign segments.',
    statusReady: 'Live data',
    errorPrefix: 'Dashboard request failed:',
    invalidRange: 'From must be earlier than To.',
    language: 'Language',
    english: 'EN',
    chinese: '中文',
    minute: 'Minute',
    hour: 'Hour',
    day: 'Day',
    resolution: 'Resolution',
  },
  zh: {
    title: 'Mini Ads 仪表盘',
    subtitle: '用于 Redis 读模型与 Postgres 历史回查的独立监控界面。',
    range: '时间范围',
    from: '开始时间',
    to: '结束时间',
    refresh: '立即刷新',
    autoRefresh: '每分钟自动刷新',
    nextRefresh: '下次刷新',
    source: '数据源',
    lastProjected: '最近投影时间',
    lag: '投影延迟',
    kpiViewRate: '展示率',
    kpiBidRequests: 'Bid 请求数',
    kpiDeduped: '去重 Impression 数',
    kpiUnknown: 'Unknown Impression 数',
    last1h: '最近 1 小时',
    last6h: '最近 6 小时',
    last24h: '最近 24 小时',
    last7d: '最近 7 天',
    last30d: '最近 30 天',
    loading: '正在加载最新指标…',
    empty: '当前时间范围内没有数据。',
    campaign: 'Campaign',
    bidRequests: 'Bid 请求数',
    dedupedImpressions: '去重 Impression 数',
    unknownImpressions: 'Unknown Impression 数',
    tableTitle: 'Campaign 分组明细',
    tableCaption: '按 bid request 数量降序排列；存在 unmatched 数据时会保留 UNKNOWN。',
    lineTitle: '需求与匹配展示',
    lineCaption: '展示所选时间范围内的 bid requests 与 deduped impressions。',
    unknownTitle: 'Unknown impression 体量',
    unknownCaption: '基于当前投影状态的即时 unmatched 数量。',
    campaignChartTitle: 'Top campaign bid requests',
    campaignChartCaption: '横向对比主要 campaign 的 bid request 体量。',
    statusReady: '实时数据',
    errorPrefix: '仪表盘请求失败：',
    invalidRange: '开始时间必须早于结束时间。',
    language: '语言',
    english: 'EN',
    chinese: '中文',
    minute: '分钟',
    hour: '小时',
    day: '天',
    resolution: '粒度',
  },
} as const

const presets = [
  { key: '1h', durationMs: 1 * 60 * 60 * 1000, label: (m: Dictionary) => m.last1h },
  { key: '6h', durationMs: 6 * 60 * 60 * 1000, label: (m: Dictionary) => m.last6h },
  { key: '24h', durationMs: 24 * 60 * 60 * 1000, label: (m: Dictionary) => m.last24h },
  { key: '7d', durationMs: 7 * 24 * 60 * 60 * 1000, label: (m: Dictionary) => m.last7d },
  { key: '30d', durationMs: 30 * 24 * 60 * 60 * 1000, label: (m: Dictionary) => m.last30d },
] as const

function getInitialLocale(): Locale {
  const saved = localStorage.getItem(localeStorageKey)
  return saved === 'zh' ? 'zh' : 'en'
}

function toLocalInputValue(date: Date): string {
  const adjusted = new Date(date.getTime() - date.getTimezoneOffset() * 60_000)
  return adjusted.toISOString().slice(0, 16)
}

function fromLocalInputValue(value: string): Date {
  return new Date(value)
}

function formatNumber(locale: Locale, value: number): string {
  return new Intl.NumberFormat(locale === 'zh' ? 'zh-CN' : 'en-US').format(value)
}

function formatPercent(locale: Locale, value: number): string {
  return new Intl.NumberFormat(locale === 'zh' ? 'zh-CN' : 'en-US', {
    style: 'percent',
    maximumFractionDigits: 2,
  }).format(value)
}

function formatDateTime(locale: Locale, value?: string | Date): string {
  if (!value) {
    return 'n/a'
  }
  const date = value instanceof Date ? value : new Date(value)
  return new Intl.DateTimeFormat(locale === 'zh' ? 'zh-CN' : 'en-US', {
    year: 'numeric',
    month: 'short',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  }).format(date)
}

function resolutionLabel(locale: Locale, resolution: Resolution): string {
  const m = messages[locale]
  if (resolution === 'hour') return m.hour
  if (resolution === 'day') return m.day
  return m.minute
}

function computeNextRefreshTime(now = Date.now()): Date {
  const next = Math.floor(now / minuteMs + 1) * minuteMs + refreshBufferMs
  return new Date(next)
}

function useMinuteAlignedRefresh(onRefresh: () => void) {
  const timerRef = useRef<number | null>(null)
  const callbackRef = useRef(onRefresh)
  const [nextRefreshAt, setNextRefreshAt] = useState<Date>(computeNextRefreshTime())

  useEffect(() => {
    callbackRef.current = onRefresh
  }, [onRefresh])

  const clear = useCallback(() => {
    if (timerRef.current !== null) {
      window.clearTimeout(timerRef.current)
      timerRef.current = null
    }
  }, [])

  const schedule = useCallback(() => {
    clear()
    const next = computeNextRefreshTime()
    setNextRefreshAt(next)
    if (document.hidden) {
      return
    }
    timerRef.current = window.setTimeout(() => {
      callbackRef.current()
      schedule()
    }, Math.max(next.getTime() - Date.now(), 0))
  }, [clear])

  const resync = useCallback(() => {
    callbackRef.current()
    schedule()
  }, [schedule])

  useEffect(() => {
    schedule()
    const onVisibility = () => {
      if (document.hidden) {
        clear()
        return
      }
      resync()
    }
    document.addEventListener('visibilitychange', onVisibility)
    return () => {
      document.removeEventListener('visibilitychange', onVisibility)
      clear()
    }
  }, [clear, resync, schedule])

  return { nextRefreshAt, resync }
}

function reorderCampaigns(items: CampaignMetrics[]): CampaignMetrics[] {
  const sorted = [...items].sort((a, b) => {
    if (a.campaign_id === 'UNKNOWN' && b.campaign_id !== 'UNKNOWN') return -1
    if (b.campaign_id === 'UNKNOWN' && a.campaign_id !== 'UNKNOWN') return 1
    if (b.bid_requests === a.bid_requests) return a.campaign_id.localeCompare(b.campaign_id)
    return b.bid_requests - a.bid_requests
  })
  return sorted
}

function chartTextColor(): string {
  return '#111111'
}

function chartGridColor(): string {
  return '#E8E8E2'
}

function formatAxisLabel(locale: Locale, iso: string, resolution: Resolution): string {
  const date = new Date(iso)
  const intl = new Intl.DateTimeFormat(locale === 'zh' ? 'zh-CN' : 'en-US',
    resolution === 'day'
      ? { month: 'short', day: '2-digit' }
      : resolution === 'hour'
        ? { month: 'short', day: '2-digit', hour: '2-digit' }
        : { hour: '2-digit', minute: '2-digit' })
  return intl.format(date)
}

function buildTrendOption(locale: Locale, labels: Dictionary, series: TimeSeriesResponse): echarts.EChartsOption {
  return {
    animationDuration: 180,
    textStyle: {
      fontFamily: 'IBM Plex Sans, Noto Sans SC, sans-serif',
      color: chartTextColor(),
    },
    grid: { top: 18, right: 8, bottom: 26, left: 48 },
    tooltip: {
      trigger: 'axis',
      backgroundColor: '#FFFFFF',
      borderWidth: 0,
      textStyle: { color: '#111111' },
    },
    legend: {
      top: 0,
      right: 0,
      textStyle: { color: chartTextColor() },
      data: [labels.bidRequests, labels.dedupedImpressions],
    },
    xAxis: {
      type: 'category',
      boundaryGap: false,
      axisLine: { lineStyle: { color: chartGridColor() } },
      axisTick: { show: false },
      axisLabel: {
        color: '#6A6A6A',
        formatter: (value: string) => formatAxisLabel(locale, value, series.resolution),
      },
      data: series.points.map((point) => point.ts),
    },
    yAxis: {
      type: 'value',
      splitLine: { lineStyle: { color: chartGridColor() } },
      axisLine: { show: false },
      axisTick: { show: false },
      axisLabel: { color: '#6A6A6A' },
    },
    series: [
      {
        name: labels.bidRequests,
        type: 'line',
        data: series.points.map((point) => point.bid_requests),
        smooth: false,
        symbol: 'none',
        lineStyle: { color: '#111111', width: 2 },
      },
      {
        name: labels.dedupedImpressions,
        type: 'line',
        data: series.points.map((point) => point.deduped_impressions),
        smooth: false,
        symbol: 'none',
        lineStyle: { color: '#8B8B86', width: 2 },
      },
    ],
  }
}

function buildUnknownOption(locale: Locale, labels: Dictionary, series: TimeSeriesResponse): echarts.EChartsOption {
  return {
    animationDuration: 180,
    textStyle: {
      fontFamily: 'IBM Plex Sans, Noto Sans SC, sans-serif',
      color: chartTextColor(),
    },
    grid: { top: 20, right: 8, bottom: 26, left: 48 },
    tooltip: {
      trigger: 'axis',
      backgroundColor: '#FFFFFF',
      borderWidth: 0,
      textStyle: { color: '#111111' },
    },
    xAxis: {
      type: 'category',
      axisLine: { lineStyle: { color: chartGridColor() } },
      axisTick: { show: false },
      axisLabel: {
        color: '#6A6A6A',
        formatter: (value: string) => formatAxisLabel(locale, value, series.resolution),
      },
      data: series.points.map((point) => point.ts),
    },
    yAxis: {
      type: 'value',
      splitLine: { lineStyle: { color: chartGridColor() } },
      axisLine: { show: false },
      axisTick: { show: false },
      axisLabel: { color: '#6A6A6A' },
    },
    series: [
      {
        name: labels.unknownImpressions,
        type: 'bar',
        barMaxWidth: 18,
        itemStyle: { color: '#B7B7B1' },
        data: series.points.map((point) => point.unknown_impressions),
      },
    ],
  }
}

function buildCampaignOption(labels: Dictionary, campaigns: CampaignMetrics[]): echarts.EChartsOption {
  const top = reorderCampaigns(campaigns)
    .filter((item) => item.bid_requests > 0)
    .slice(0, 8)
    .reverse()

  return {
    animationDuration: 180,
    textStyle: {
      fontFamily: 'IBM Plex Sans, Noto Sans SC, sans-serif',
      color: chartTextColor(),
    },
    grid: { top: 8, right: 12, bottom: 8, left: 96 },
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'shadow' },
      backgroundColor: '#FFFFFF',
      borderWidth: 0,
      textStyle: { color: '#111111' },
    },
    xAxis: {
      type: 'value',
      splitLine: { lineStyle: { color: chartGridColor() } },
      axisLine: { show: false },
      axisTick: { show: false },
      axisLabel: { color: '#6A6A6A' },
    },
    yAxis: {
      type: 'category',
      axisLine: { show: false },
      axisTick: { show: false },
      axisLabel: { color: chartTextColor() },
      data: top.map((item) => item.campaign_id),
    },
    series: [
      {
        name: labels.bidRequests,
        type: 'bar',
        barMaxWidth: 24,
        itemStyle: { color: '#111111' },
        data: top.map((item) => item.bid_requests),
      },
    ],
  }
}

function ChartPanel({ title, caption, option, height = 280 }: ChartPanelProps) {
  const ref = useRef<HTMLDivElement | null>(null)

  useEffect(() => {
    const node = ref.current
    if (!node) return
    const chart = echarts.init(node, undefined, { renderer: 'svg' })
    chart.setOption(option)
    const onResize = () => chart.resize()
    window.addEventListener('resize', onResize)
    return () => {
      window.removeEventListener('resize', onResize)
      chart.dispose()
    }
  }, [option])

  return (
    <section className="panel chart-panel">
      <div className="panel-header">
        <div>
          <p className="eyebrow">Chart</p>
          <h2>{title}</h2>
        </div>
      </div>
      <div ref={ref} className="chart-canvas" style={{ height }} aria-hidden="true" />
      <p className="panel-caption">{caption}</p>
    </section>
  )
}

export default function App() {
  const [locale, setLocale] = useState<Locale>(() => getInitialLocale())
  const [activePreset, setActivePreset] = useState<string>('1h')
  const [fromValue, setFromValue] = useState(() => toLocalInputValue(new Date(Date.now() - 60 * 60 * 1000)))
  const [toValue, setToValue] = useState(() => toLocalInputValue(new Date()))
  const [summary, setSummary] = useState<Summary | null>(null)
  const [campaigns, setCampaigns] = useState<CampaignMetrics[]>([])
  const [series, setSeries] = useState<TimeSeriesResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string>('')
  const [reloadNonce, setReloadNonce] = useState(0)

  const text = messages[locale]
  const fromDate = useMemo(() => fromLocalInputValue(fromValue), [fromValue])
  const toDate = useMemo(() => fromLocalInputValue(toValue), [toValue])
  const rangeValid = Number.isFinite(fromDate.getTime()) && Number.isFinite(toDate.getTime()) && fromDate < toDate

  useEffect(() => {
    localStorage.setItem(localeStorageKey, locale)
  }, [locale])

  const triggerRefresh = useCallback(() => {
    setReloadNonce((value) => value + 1)
  }, [])

  const { nextRefreshAt, resync } = useMinuteAlignedRefresh(triggerRefresh)

  useEffect(() => {
    if (!rangeValid) {
      setError(text.invalidRange)
      setLoading(false)
      return
    }

    const controller = new AbortController()
    setLoading(true)
    setError('')

    const from = fromDate.toISOString()
    const to = toDate.toISOString()

    Promise.all([
      fetch(`/api/metrics/summary?from=${encodeURIComponent(from)}&to=${encodeURIComponent(to)}`, { signal: controller.signal }),
      fetch(`/api/metrics/by-campaign?from=${encodeURIComponent(from)}&to=${encodeURIComponent(to)}`, { signal: controller.signal }),
      fetch(`/api/metrics/timeseries?from=${encodeURIComponent(from)}&to=${encodeURIComponent(to)}&resolution=auto`, { signal: controller.signal }),
    ])
      .then(async ([summaryRes, campaignsRes, seriesRes]) => {
        if (!summaryRes.ok || !campaignsRes.ok || !seriesRes.ok) {
          const failed = [summaryRes, campaignsRes, seriesRes].find((res) => !res.ok)
          throw new Error(`${failed?.status} ${failed?.statusText}`)
        }
        const [summaryJson, campaignsJson, seriesJson] = await Promise.all([
          summaryRes.json() as Promise<Summary>,
          campaignsRes.json() as Promise<CampaignResponse>,
          seriesRes.json() as Promise<TimeSeriesResponse>,
        ])
        setSummary(summaryJson)
        setCampaigns(reorderCampaigns(campaignsJson.campaigns))
        setSeries(seriesJson)
        setLoading(false)
      })
      .catch((err: unknown) => {
        if (controller.signal.aborted) return
        setError(`${text.errorPrefix} ${err instanceof Error ? err.message : String(err)}`)
        setLoading(false)
      })

    return () => controller.abort()
  }, [fromDate, rangeValid, reloadNonce, text.errorPrefix, toDate])

  const onPreset = useCallback((presetKey: string, durationMs: number) => {
    const now = new Date()
    setFromValue(toLocalInputValue(new Date(now.getTime() - durationMs)))
    setToValue(toLocalInputValue(now))
    setActivePreset(presetKey)
    window.setTimeout(() => {
      resync()
    }, 0)
  }, [resync])

  const onManualRefresh = useCallback(() => {
    if (!rangeValid) {
      setError(text.invalidRange)
      return
    }
    resync()
  }, [rangeValid, resync])

  const currentResolution = series?.resolution ?? 'minute'
  const infoLine = summary
    ? `${text.source}: ${summary.source.toUpperCase()}  |  ${text.lastProjected}: ${formatDateTime(locale, summary.last_projected_at)}  |  ${text.lag}: ${summary.projection_lag_seconds?.toFixed(2) ?? '0.00'}s`
    : text.loading

  return (
    <div className="app-shell">
      <main className="layout">
        <header className="hero">
          <div className="hero-copy">
            <p className="eyebrow">Mini Ads</p>
            <h1>{text.title}</h1>
            <p className="hero-subtitle">{text.subtitle}</p>
          </div>
          <div className="hero-meta">
            <div className="language-toggle" role="group" aria-label={text.language}>
              <button className={locale === 'en' ? 'toggle active' : 'toggle'} onClick={() => setLocale('en')}>
                {text.english}
              </button>
              <button className={locale === 'zh' ? 'toggle active' : 'toggle'} onClick={() => setLocale('zh')}>
                {text.chinese}
              </button>
            </div>
            <div className="status-block" aria-live="polite">
              <span>{loading ? text.loading : text.statusReady}</span>
              <span>{infoLine}</span>
              <span>{text.nextRefresh}: {formatDateTime(locale, nextRefreshAt)}</span>
            </div>
          </div>
        </header>

        <section className="control-strip panel">
          <div className="control-header">
            <div>
              <p className="eyebrow">{text.range}</p>
              <h2>{text.autoRefresh}</h2>
            </div>
            <div className="resolution-chip">{text.resolution}: {resolutionLabel(locale, currentResolution)}</div>
          </div>
          <div className="preset-row">
            {presets.map((preset) => (
              <button
                key={preset.key}
                className={activePreset === preset.key ? 'preset active' : 'preset'}
                onClick={() => onPreset(preset.key, preset.durationMs)}
              >
                {preset.label(text)}
              </button>
            ))}
          </div>
          <div className="range-grid">
            <label>
              <span>{text.from}</span>
              <input
                type="datetime-local"
                value={fromValue}
                onChange={(event) => {
                  setFromValue(event.target.value)
                  setActivePreset('custom')
                }}
              />
            </label>
            <label>
              <span>{text.to}</span>
              <input
                type="datetime-local"
                value={toValue}
                onChange={(event) => {
                  setToValue(event.target.value)
                  setActivePreset('custom')
                }}
              />
            </label>
            <button className="action-button" onClick={onManualRefresh}>{text.refresh}</button>
          </div>
          {error ? <p className="error-text" role="alert">{error}</p> : null}
        </section>

        <section className="kpi-grid">
          <article className="kpi panel">
            <p className="eyebrow">KPI</p>
            <span>{text.kpiViewRate}</span>
            <strong>{formatPercent(locale, summary?.view_rate ?? 0)}</strong>
          </article>
          <article className="kpi panel">
            <p className="eyebrow">KPI</p>
            <span>{text.kpiBidRequests}</span>
            <strong>{formatNumber(locale, summary?.bid_requests ?? 0)}</strong>
          </article>
          <article className="kpi panel">
            <p className="eyebrow">KPI</p>
            <span>{text.kpiDeduped}</span>
            <strong>{formatNumber(locale, summary?.deduped_impressions ?? 0)}</strong>
          </article>
          <article className="kpi panel">
            <p className="eyebrow">KPI</p>
            <span>{text.kpiUnknown}</span>
            <strong>{formatNumber(locale, summary?.unknown_impressions ?? 0)}</strong>
          </article>
        </section>

        <section className="chart-grid">
          <ChartPanel
            title={text.lineTitle}
            caption={text.lineCaption}
            option={buildTrendOption(locale, text, series ?? { source: 'redis', resolution: 'minute', points: [] })}
          />
          <ChartPanel
            title={text.unknownTitle}
            caption={text.unknownCaption}
            option={buildUnknownOption(locale, text, series ?? { source: 'redis', resolution: 'minute', points: [] })}
          />
        </section>

        <ChartPanel
          title={text.campaignChartTitle}
          caption={text.campaignChartCaption}
          option={buildCampaignOption(text, campaigns)}
          height={360}
        />

        <section className="panel table-panel">
          <div className="panel-header">
            <div>
              <p className="eyebrow">Table</p>
              <h2>{text.tableTitle}</h2>
            </div>
            <p className="panel-caption table-note">{text.tableCaption}</p>
          </div>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>{text.campaign}</th>
                  <th>{text.bidRequests}</th>
                  <th>{text.dedupedImpressions}</th>
                  <th>{text.unknownImpressions}</th>
                  <th>{text.kpiViewRate}</th>
                </tr>
              </thead>
              <tbody>
                {campaigns.length === 0 ? (
                  <tr>
                    <td colSpan={5} className="empty-cell">{text.empty}</td>
                  </tr>
                ) : campaigns.map((item) => (
                  <tr key={item.campaign_id}>
                    <td>{item.campaign_id}</td>
                    <td className="numeric">{formatNumber(locale, item.bid_requests)}</td>
                    <td className="numeric">{formatNumber(locale, item.deduped_impressions)}</td>
                    <td className="numeric">{formatNumber(locale, item.unknown_impressions)}</td>
                    <td className="numeric">{formatPercent(locale, item.view_rate)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      </main>
    </div>
  )
}
