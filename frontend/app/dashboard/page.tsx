'use client';

/**
 * VWAP + RSI Scalp — Live Dashboard
 * ===================================
 * Streams live state from the FastAPI backend (port 8056) via WebSocket.
 * Reads 30-day historical trades from DynamoDB via REST.
 *
 * Strategy recap:
 *   RSI(14) on 5-min NIFTY futures candles
 *   Buy CE on RSI cross above 48 · Buy PE on cross below 52
 *   Price must be within 20 pts of VWAP
 *   Target +7 pts · SL -5 pts · 15-min time stop · max 3 trades/day
 */

import { useState, useEffect, useCallback, useRef } from 'react';
import {
  Card, CardContent, CardHeader, CardTitle, CardDescription,
} from '@/components/ui/card';
import { Badge }      from '@/components/ui/badge';
import { Button }     from '@/components/ui/button';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Progress }   from '@/components/ui/progress';
import { Separator }  from '@/components/ui/separator';
import {
  TrendingUp, TrendingDown, PlayCircle, StopCircle,
  RefreshCw, Activity, Clock, BarChart3, Zap, Target,
  Wifi, WifiOff,
} from 'lucide-react';

// ─── Types ────────────────────────────────────────────────────────────────────

interface Position {
  symbol:         string;
  direction:      'CE' | 'PE';
  atm:            number;
  entry_price:    number;
  current_ltp:    number;
  entry_ts:       string;
  qty:            number;
  unrealized_pnl: number;
  vwap_at_entry?: number | null;
  rsi_at_entry?:  number | null;
  spot_at_entry?: number | null;
}

interface TodayTrade {
  entry_ts:    string;
  exit_ts:     string;
  direction:   'CE' | 'PE';
  symbol:      string;
  atm:         number;
  entry_price: number;
  exit_price:  number;
  reason:      string;
  qty:         number;
  pnl:         number;
}

interface LiveState {
  is_running:          boolean;
  updated_at:          string | null;
  fut_symbol:          string | null;
  fut_ltp:             number | null;
  vwap:                number | null;
  rsi14:               number | null;
  prev_rsi:            number | null;
  trades_today:        number;
  daily_pnl:           number;
  position:            Position | null;
  today_trades:        TodayTrade[];
  trader_pid?:         number | null;
  api_trader_running?: boolean;
}

interface HistoricalTrade {
  trade_date:  string;
  entry_ts:    string;
  exit_ts:     string;
  direction:   string;
  symbol:      string;
  atm:         number;
  entry_price: number;
  exit_price:  number;
  reason:      string;
  qty:         number;
  pnl_total:   number;
  spot_entry:  number;
  vwap_entry:  number;
  rsi_entry:   number;
}

// ─── Constants ────────────────────────────────────────────────────────────────

const API = '/api';
const TIME_STOP_MIN = 15;

// ─── Helpers ──────────────────────────────────────────────────────────────────

const fmt  = (n: number, d = 2) => n.toFixed(d);
const fmtINR = (n: number) => {
  const abs  = Math.abs(n).toLocaleString('en-IN', { maximumFractionDigits: 0 });
  return `${n >= 0 ? '+' : '-'}₹${abs}`;
};
const fmtTime = (iso: string | null): string => {
  if (!iso) return '—';
  try {
    return new Date(iso).toLocaleTimeString('en-IN', {
      timeZone: 'Asia/Kolkata', hour: '2-digit', minute: '2-digit', second: '2-digit',
    });
  } catch { return '—'; }
};
const fmtDate = (iso: string | null): string => {
  if (!iso) return '—';
  try {
    return new Date(iso).toLocaleDateString('en-IN', {
      timeZone: 'Asia/Kolkata', day: '2-digit', month: 'short',
    });
  } catch { return '—'; }
};
const elapsedMin = (entryIso: string) => {
  try { return (Date.now() - new Date(entryIso).getTime()) / 60000; }
  catch { return 0; }
};

// ─── Component ────────────────────────────────────────────────────────────────

export default function Dashboard() {
  const [live, setLive]               = useState<LiveState | null>(null);
  const [history, setHistory]         = useState<HistoricalTrade[]>([]);
  const [connected, setConnected]     = useState(false);
  const [loading, setLoading]         = useState(false);
  const [error, setError]             = useState<string | null>(null);
  const [lastUpdate, setLastUpdate]   = useState<Date | null>(null);
  const [ltpFlash, setLtpFlash]       = useState<'up' | 'down' | null>(null);
  const [, tick]                      = useState(0);   // 1-sec re-render for progress bars
  const prevLtp                       = useRef<number | null>(null);

  // ── WebSocket URL ──────────────────────────────────────────────────────────
  const wsUrl = typeof window !== 'undefined'
    ? `${window.location.protocol === 'https:' ? 'wss:' : 'ws:'}//${window.location.host}${API}/ws`
    : `ws://localhost${API}/ws`;

  // ── Fetch helpers ──────────────────────────────────────────────────────────
  const fetchHistory = useCallback(async () => {
    try {
      const r = await fetch(`${API}/trades`);
      if (!r.ok) return;
      const d = await r.json();
      setHistory(d.trades ?? []);
    } catch { /* silent */ }
  }, []);

  const fetchState = useCallback(async () => {
    try {
      const r = await fetch(`${API}/state`);
      if (!r.ok) return;
      setLive(await r.json());
      setLastUpdate(new Date());
    } catch { /* silent */ }
  }, []);

  // ── WebSocket ──────────────────────────────────────────────────────────────
  useEffect(() => {
    if (typeof window === 'undefined') return;

    let ws: WebSocket | null = null;
    let retry = 0;
    let timer: ReturnType<typeof setTimeout> | null = null;
    let dead  = false;

    const connect = () => {
      if (dead) return;
      try {
        ws = new WebSocket(wsUrl);
        ws.onopen   = () => { setConnected(true); retry = 0; setError(null); };
        ws.onmessage = (ev) => {
          try {
            const data: LiveState = JSON.parse(ev.data);
            setLive(prev => {
              if (prev?.fut_ltp != null && data.fut_ltp != null) {
                if (data.fut_ltp > prev.fut_ltp)      setLtpFlash('up');
                else if (data.fut_ltp < prev.fut_ltp) setLtpFlash('down');
              }
              return data;
            });
            setLastUpdate(new Date());
          } catch { /* ignore */ }
        };
        ws.onerror = () => { if (retry >= 3) setError('WebSocket reconnecting…'); };
        ws.onclose = () => {
          setConnected(false);
          if (!dead) {
            retry++;
            timer = setTimeout(connect, Math.min(1000 * 2 ** retry, 30_000));
          }
        };
      } catch { setError('WebSocket failed to connect'); }
    };

    connect();
    return () => {
      dead = true;
      if (timer) clearTimeout(timer);
      ws?.close();
      setConnected(false);
    };
  }, [wsUrl]);

  // ── LTP flash reset ────────────────────────────────────────────────────────
  useEffect(() => {
    if (!ltpFlash) return;
    const t = setTimeout(() => setLtpFlash(null), 400);
    return () => clearTimeout(t);
  }, [ltpFlash]);

  // ── 1-sec ticker ──────────────────────────────────────────────────────────
  useEffect(() => {
    const t = setInterval(() => tick(n => n + 1), 1000);
    return () => clearInterval(t);
  }, []);

  // ── Initial loads ──────────────────────────────────────────────────────────
  useEffect(() => { fetchState(); fetchHistory(); }, [fetchState, fetchHistory]);

  // ── Refresh history every 60 s ─────────────────────────────────────────────
  useEffect(() => {
    const t = setInterval(fetchHistory, 60_000);
    return () => clearInterval(t);
  }, [fetchHistory]);

  // ── Controls ───────────────────────────────────────────────────────────────
  const startTrader = async () => {
    setLoading(true);
    try {
      const r = await fetch(`${API}/start`, { method: 'POST' });
      const d = await r.json();
      if (!d.success) setError(d.message); else { setError(null); fetchState(); }
    } catch (e) { setError(String(e)); }
    finally { setLoading(false); }
  };

  const stopTrader = async () => {
    setLoading(true);
    try {
      const r = await fetch(`${API}/stop`, { method: 'POST' });
      const d = await r.json();
      if (!d.success) setError(d.message); else { setError(null); fetchState(); }
    } catch (e) { setError(String(e)); }
    finally { setLoading(false); }
  };

  const forceTrade = async () => {
    if (!confirm('Force a paper trade NOW (bypasses all conditions)? Use only for testing.')) return;
    try {
      const r = await fetch(`${API}/debug/force-trade`, { method: 'POST' });
      const d = await r.json();
      if (!d.success) setError(d.message); else setError(null);
    } catch (e) { setError(String(e)); }
  };

  // ── Derived ────────────────────────────────────────────────────────────────
  const wins       = history.filter(t => t.pnl_total > 0).length;
  const total      = history.length;
  const winRate    = total > 0 ? Math.round((wins / total) * 100) : 0;
  const totalPnl   = history.reduce((s, t) => s + t.pnl_total, 0);

  const todayTrades = live?.today_trades ?? [];
  const todayWins   = todayTrades.filter(t => t.pnl > 0).length;
  const todayTotal  = live?.trades_today ?? 0;
  const todayWR     = todayTotal > 0 ? Math.round((todayWins / todayTotal) * 100) : 0;

  const pos          = live?.position ?? null;
  const timeElapsed  = pos ? Math.min(elapsedMin(pos.entry_ts), TIME_STOP_MIN) : 0;
  const timeProgress = (timeElapsed / TIME_STOP_MIN) * 100;

  const rsi    = live?.rsi14 ?? null;
  const rsiPct = rsi !== null ? Math.min(Math.max(rsi, 0), 100) : null;

  const vwapDelta = live?.vwap && live?.fut_ltp
    ? live.fut_ltp - live.vwap : null;

  // ── Render ─────────────────────────────────────────────────────────────────
  return (
    <div className="min-h-screen bg-gray-950 text-gray-100 p-4 md:p-6">
      <div className="max-w-7xl mx-auto space-y-5">

        {/* ── Header ── */}
        <div className="flex flex-col sm:flex-row justify-between items-start sm:items-center gap-3">
          <div className="flex items-center gap-3">
            <div className="p-2 bg-gradient-to-br from-emerald-500 to-teal-600 rounded-xl shadow-lg shadow-emerald-900/40">
              <Zap className="h-7 w-7 text-white" />
            </div>
            <div>
              <h1 className="text-2xl md:text-3xl font-black bg-gradient-to-r from-emerald-400 to-teal-300 bg-clip-text text-transparent">
                VWAP + RSI Scalper
              </h1>
              <p className="text-xs text-gray-500 mt-0.5">
                NIFTY Futures · RSI(14) · 5 lots paper · {live?.fut_symbol ?? '—'}
              </p>
            </div>
          </div>

          <div className="flex items-center gap-3 self-end sm:self-auto">
            {lastUpdate && (
              <span className="text-xs text-gray-600 hidden md:inline">
                {lastUpdate.toLocaleTimeString()}
              </span>
            )}
            <Badge
              className={`px-3 py-1.5 text-sm font-semibold flex items-center gap-1.5 ${
                connected
                  ? 'bg-emerald-700 text-emerald-100'
                  : 'bg-red-900/60 text-red-300'
              }`}
            >
              {connected
                ? <><Wifi className="h-3.5 w-3.5" /> Live</>
                : <><WifiOff className="h-3.5 w-3.5" /> Disconnected</>}
            </Badge>
          </div>
        </div>

        {/* ── Error banner ── */}
        {error && (
          <div className="px-4 py-3 bg-red-900/40 border border-red-700/50 rounded-lg text-sm text-red-300">
            {error}
          </div>
        )}

        {/* ── Row 1: LTP · Controls · Indicators ── */}
        <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">

          {/* NIFTY LTP */}
          <Card className="bg-gray-900 border-gray-800">
            <CardHeader className="pb-1">
              <CardTitle className="text-xs text-gray-500 font-medium uppercase tracking-widest flex items-center gap-1.5">
                <Activity className="h-3.5 w-3.5" /> NIFTY Futures LTP
              </CardTitle>
            </CardHeader>
            <CardContent>
              <p className={`text-6xl font-black tabular-nums tracking-tight transition-colors duration-300 ${
                ltpFlash === 'up'   ? 'text-emerald-400' :
                ltpFlash === 'down' ? 'text-red-400'     : 'text-white'
              }`}>
                {live?.fut_ltp != null ? fmt(live.fut_ltp, 1) : '—'}
              </p>
              {vwapDelta !== null && (
                <p className="text-xs text-gray-500 mt-2">
                  VWAP&nbsp;
                  <span className="text-gray-400 font-mono">{fmt(live!.vwap!, 1)}</span>
                  &nbsp;·&nbsp;Δ&nbsp;
                  <span className={`font-mono font-semibold ${
                    vwapDelta > 0 ? 'text-emerald-400' : 'text-red-400'
                  }`}>
                    {vwapDelta > 0 ? '+' : ''}{fmt(vwapDelta, 1)}
                  </span>
                  &nbsp;
                  {Math.abs(vwapDelta) <= 20
                    ? <span className="text-emerald-500 text-[10px]">≤ 20 ✓</span>
                    : <span className="text-gray-600 text-[10px]">far</span>}
                </p>
              )}
            </CardContent>
          </Card>

          {/* Controls */}
          <Card className="bg-gray-900 border-gray-800">
            <CardHeader className="pb-1">
              <CardTitle className="text-xs text-gray-500 font-medium uppercase tracking-widest">
                Strategy Control
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              <div className="flex gap-2">
                <Button
                  onClick={startTrader}
                  disabled={!!live?.api_trader_running || loading}
                  className="flex-1 bg-emerald-700 hover:bg-emerald-600 disabled:opacity-40 text-white text-sm"
                  size="sm"
                >
                  <PlayCircle className="h-4 w-4 mr-1.5" /> Start
                </Button>
                <Button
                  onClick={stopTrader}
                  disabled={!live?.api_trader_running || loading}
                  variant="destructive"
                  className="flex-1 disabled:opacity-40 text-sm"
                  size="sm"
                >
                  <StopCircle className="h-4 w-4 mr-1.5" /> Stop
                </Button>
                <Button
                  onClick={fetchState}
                  variant="outline"
                  size="sm"
                  className="border-gray-700 text-gray-400 hover:bg-gray-800"
                >
                  <RefreshCw className="h-4 w-4" />
                </Button>
              </div>

              {live?.api_trader_running && !live?.position && (
                <button
                  onClick={forceTrade}
                  className="w-full text-xs text-yellow-600 border border-yellow-800/40 rounded-lg py-1.5 hover:bg-yellow-900/20 transition-colors"
                >
                  ⚡ Force Test Trade (debug only)
                </button>
              )}

              <div className={`flex items-center gap-2 text-xs px-3 py-2 rounded-lg border ${
                live?.is_running
                  ? 'bg-emerald-950/50 text-emerald-400 border-emerald-800'
                  : 'bg-gray-800/50 text-gray-500 border-gray-700'
              }`}>
                <span className={`h-2 w-2 rounded-full flex-shrink-0 ${
                  live?.is_running ? 'bg-emerald-400 animate-pulse' : 'bg-gray-600'
                }`} />
                <span className="truncate">
                  {live?.is_running ? 'Trader running' : 'Trader stopped'}
                </span>
                {live?.trader_pid && (
                  <span className="text-gray-600 ml-auto text-[10px] flex-shrink-0">
                    PID {live.trader_pid}
                  </span>
                )}
              </div>
            </CardContent>
          </Card>

          {/* Indicators */}
          <Card className="bg-gray-900 border-gray-800">
            <CardHeader className="pb-1">
              <CardTitle className="text-xs text-gray-500 font-medium uppercase tracking-widest flex items-center gap-1.5">
                <BarChart3 className="h-3.5 w-3.5" /> Indicators
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              {/* RSI gauge */}
              <div>
                <div className="flex justify-between mb-1.5">
                  <span className="text-xs text-gray-400">RSI(14)</span>
                  <span className={`text-sm font-bold tabular-nums ${
                    rsi == null ? 'text-gray-600' :
                    rsi >= 70   ? 'text-red-400'  :
                    rsi <= 30   ? 'text-emerald-400' :
                    rsi >= 52   ? 'text-orange-400' :
                    rsi <= 48   ? 'text-sky-400' : 'text-gray-300'
                  }`}>
                    {rsi != null ? fmt(rsi, 1) : 'seeding…'}
                  </span>
                </div>
                <div className="relative h-2.5 bg-gray-800 rounded-full overflow-hidden">
                  {rsiPct !== null && (
                    <div
                      className="absolute top-0 left-0 h-full rounded-full bg-gradient-to-r from-emerald-500 via-yellow-400 to-red-500 transition-all duration-500"
                      style={{ width: `${rsiPct}%` }}
                    />
                  )}
                  {/* CE signal line at 48 */}
                  <div className="absolute top-0 bottom-0 w-0.5 bg-sky-400/70" style={{ left: '48%' }} />
                  {/* PE signal line at 52 */}
                  <div className="absolute top-0 bottom-0 w-0.5 bg-orange-400/70" style={{ left: '52%' }} />
                </div>
                {live?.prev_rsi != null && rsi != null && (
                  <p className="text-[10px] text-gray-500 mt-1 flex items-center gap-1">
                    <span>{fmt(live.prev_rsi, 1)} → {fmt(rsi, 1)}</span>
                    {live.prev_rsi < 48 && rsi >= 48 && (
                      <span className="text-emerald-400 font-semibold">⬆ CE signal</span>
                    )}
                    {live.prev_rsi > 52 && rsi <= 52 && (
                      <span className="text-red-400 font-semibold">⬇ PE signal</span>
                    )}
                  </p>
                )}
              </div>

              {/* VWAP zone indicator */}
              <div className="flex items-center justify-between">
                <span className="text-xs text-gray-400">VWAP zone (±20)</span>
                {vwapDelta !== null ? (
                  <span className={`text-xs font-semibold ${
                    Math.abs(vwapDelta) <= 20 ? 'text-emerald-400' : 'text-gray-600'
                  }`}>
                    {Math.abs(vwapDelta) <= 20 ? '● In range' : `● ${fmt(Math.abs(vwapDelta), 0)} pt away`}
                  </span>
                ) : <span className="text-xs text-gray-700">—</span>}
              </div>
            </CardContent>
          </Card>
        </div>

        {/* ── Open position ── */}
        {pos ? (
          <Card className={`border-2 ${
            pos.direction === 'CE'
              ? 'border-emerald-700 bg-emerald-950/20'
              : 'border-red-700 bg-red-950/20'
          }`}>
            <CardHeader className="pb-2">
              <div className="flex flex-wrap items-center justify-between gap-2">
                <CardTitle className="flex items-center gap-2 text-white text-base">
                  {pos.direction === 'CE'
                    ? <TrendingUp className="h-5 w-5 text-emerald-400" />
                    : <TrendingDown className="h-5 w-5 text-red-400" />}
                  Open Position
                </CardTitle>
                <div className="flex items-center gap-2">
                  <Badge className={`text-xs ${
                    pos.direction === 'CE'
                      ? 'bg-emerald-800 text-emerald-100'
                      : 'bg-red-800 text-red-100'
                  }`}>
                    {pos.direction} {pos.atm}
                  </Badge>
                  <span className="text-xs text-gray-400 font-mono">{pos.symbol}</span>
                </div>
              </div>
            </CardHeader>
            <CardContent>
              {/* P&L metrics */}
              <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 mb-4">
                <div className="bg-gray-900/60 rounded-lg p-3 text-center">
                  <p className="text-[10px] text-gray-500 uppercase mb-1">Entry Price</p>
                  <p className="text-2xl font-black text-white tabular-nums">{fmt(pos.entry_price)}</p>
                </div>
                <div className="bg-gray-900/60 rounded-lg p-3 text-center">
                  <p className="text-[10px] text-gray-500 uppercase mb-1">Current LTP</p>
                  <p className={`text-2xl font-black tabular-nums ${
                    pos.current_ltp >= pos.entry_price ? 'text-emerald-400' : 'text-red-400'
                  }`}>{fmt(pos.current_ltp)}</p>
                </div>
                <div className={`rounded-lg p-3 text-center ${
                  pos.unrealized_pnl >= 0 ? 'bg-emerald-950/40' : 'bg-red-950/40'
                }`}>
                  <p className="text-[10px] text-gray-500 uppercase mb-1">Unrealized P&amp;L</p>
                  <p className={`text-2xl font-black tabular-nums ${
                    pos.unrealized_pnl >= 0 ? 'text-emerald-400' : 'text-red-400'
                  }`}>{fmtINR(pos.unrealized_pnl)}</p>
                </div>
                <div className="bg-gray-900/60 rounded-lg p-3 text-center">
                  <p className="text-[10px] text-gray-500 uppercase mb-1">Entry Time</p>
                  <p className="text-lg font-bold text-white">{fmtTime(pos.entry_ts)}</p>
                </div>
              </div>

              {/* Target / SL levels */}
              <div className="grid grid-cols-3 gap-3 mb-4">
                <div className="bg-emerald-950/40 border border-emerald-800/40 rounded-lg p-3 text-center">
                  <p className="text-[10px] text-emerald-600 uppercase mb-1">🎯 Target (+7pts)</p>
                  <p className="text-xl font-black text-emerald-400 tabular-nums">{fmt(pos.entry_price + 7)}</p>
                  <p className="text-[10px] text-emerald-700 mt-0.5">
                    {pos.current_ltp < pos.entry_price + 7
                      ? `${fmt(pos.entry_price + 7 - pos.current_ltp)} pts away`
                      : '✓ HIT'}
                  </p>
                </div>
                <div className="bg-gray-900/60 rounded-lg p-3 text-center">
                  <p className="text-[10px] text-gray-500 uppercase mb-1">📍 Entry</p>
                  <p className="text-xl font-black text-white tabular-nums">{fmt(pos.entry_price)}</p>
                  <p className="text-[10px] text-gray-600 mt-0.5">qty {pos.qty}</p>
                </div>
                <div className="bg-red-950/40 border border-red-800/40 rounded-lg p-3 text-center">
                  <p className="text-[10px] text-red-600 uppercase mb-1">🛑 Stop (-5pts)</p>
                  <p className="text-xl font-black text-red-400 tabular-nums">{fmt(pos.entry_price - 5)}</p>
                  <p className="text-[10px] text-red-700 mt-0.5">
                    {pos.current_ltp > pos.entry_price - 5
                      ? `${fmt(pos.current_ltp - (pos.entry_price - 5))} pts buffer`
                      : '✗ HIT'}
                  </p>
                </div>
              </div>

              {/* Time-stop progress */}
              <div className="space-y-1.5 mb-3">
                <div className="flex justify-between text-xs text-gray-400">
                  <span className="flex items-center gap-1">
                    <Clock className="h-3 w-3" /> Time stop (15 min max)
                  </span>
                  <span className="tabular-nums font-mono">
                    {Math.floor(timeElapsed)}m {Math.floor((timeElapsed % 1) * 60)}s / {TIME_STOP_MIN}m
                    {timeElapsed >= TIME_STOP_MIN && <span className="text-red-400 ml-1">→ CLOSING</span>}
                  </span>
                </div>
                <Progress
                  value={timeProgress}
                  className={`h-2.5 ${timeProgress > 80 ? '[&>div]:bg-red-500' : timeProgress > 50 ? '[&>div]:bg-yellow-500' : '[&>div]:bg-emerald-600'}`}
                />
              </div>

              {/* Exit conditions summary */}
              <div className="bg-gray-900/60 rounded-lg px-3 py-2 text-xs text-gray-500 space-y-1">
                <p className="text-gray-400 font-semibold mb-1">Trade closes when:</p>
                <p>✅ Option LTP reaches <span className="text-emerald-400 font-mono">{fmt(pos.entry_price + 7)}</span> → Target hit (+7pts)</p>
                <p>🛑 Option LTP drops to <span className="text-red-400 font-mono">{fmt(pos.entry_price - 5)}</span> → Stop loss (-5pts)</p>
                <p>⏱ 15 minutes elapsed since entry → Time stop</p>
                <p>🕒 3:15 PM IST → Market close</p>
              </div>

              {/* Entry context */}
              {(pos.spot_at_entry || pos.rsi_at_entry || pos.vwap_at_entry) && (
                <div className="flex flex-wrap gap-3 mt-3 text-[10px] text-gray-600">
                  {pos.spot_at_entry && <span>Spot@entry: <span className="text-gray-500">{fmt(pos.spot_at_entry, 0)}</span></span>}
                  {pos.vwap_at_entry && <span>VWAP@entry: <span className="text-gray-500">{fmt(pos.vwap_at_entry, 0)}</span></span>}
                  {pos.rsi_at_entry  && <span>RSI@entry: <span className="text-gray-500">{fmt(pos.rsi_at_entry, 1)}</span></span>}
                </div>
              )}
            </CardContent>
          </Card>
        ) : (
          /* ── No position: strategy watch panel ── */
          <Card className="bg-gray-900 border-gray-800">
            <CardHeader className="pb-2">
              <CardTitle className="text-sm text-gray-400 font-medium flex items-center gap-2">
                <Activity className="h-4 w-4" /> Strategy Watch · No Open Position
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">

              {/* Entry conditions */}
              <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">

                {/* RSI condition */}
                {(() => {
                  const r = live?.rsi14 ?? null;
                  const pr = live?.prev_rsi ?? null;
                  const ceSignal = pr !== null && r !== null && pr < 48 && r >= 48;
                  const peSignal = pr !== null && r !== null && pr > 52 && r <= 52;
                  const ok = ceSignal || peSignal;
                  return (
                    <div className={`rounded-lg p-3 border ${ok ? 'bg-emerald-950/30 border-emerald-700' : 'bg-gray-800/40 border-gray-700'}`}>
                      <p className="text-[10px] text-gray-500 uppercase mb-2">RSI Signal</p>
                      <p className={`text-lg font-black tabular-nums ${ok ? 'text-emerald-400' : 'text-gray-300'}`}>
                        {r !== null ? fmt(r, 1) : 'seeding…'}
                      </p>
                      <p className="text-[10px] mt-1 text-gray-500">
                        {r === null ? 'Warming up RSI…'
                          : ceSignal ? '✅ CE signal fired (RSI crossed 48↑)'
                          : peSignal ? '✅ PE signal fired (RSI crossed 52↓)'
                          : r < 48 ? `Waiting for RSI to cross 48↑ (${fmt(48 - r, 1)} away)`
                          : r > 52 ? `Waiting for RSI to cross 52↓ (${fmt(r - 52, 1)} away)`
                          : `RSI in neutral zone (48–52) · no signal`}
                      </p>
                    </div>
                  );
                })()}

                {/* VWAP zone condition */}
                {(() => {
                  const inZone = vwapDelta !== null && Math.abs(vwapDelta) <= 20;
                  return (
                    <div className={`rounded-lg p-3 border ${inZone ? 'bg-emerald-950/30 border-emerald-700' : 'bg-gray-800/40 border-gray-700'}`}>
                      <p className="text-[10px] text-gray-500 uppercase mb-2">VWAP Zone (entry only)</p>
                      <p className={`text-lg font-black tabular-nums ${inZone ? 'text-emerald-400' : 'text-gray-400'}`}>
                        {vwapDelta !== null ? `${vwapDelta > 0 ? '+' : ''}${fmt(vwapDelta, 0)} pts` : '—'}
                      </p>
                      <p className="text-[10px] mt-1 text-gray-500">
                        {vwapDelta === null ? '—'
                          : inZone
                            ? `✅ Within ±20pts of VWAP ${fmt(live!.vwap!, 0)}`
                            : `❌ ${fmt(Math.abs(vwapDelta), 0)}pts from VWAP ${live?.vwap ? fmt(live.vwap, 0) : '—'} · entries blocked`}
                      </p>
                      <p className="text-[10px] text-gray-600 mt-1">⚠ VWAP zone only blocks new entries. Open trades close on Target / SL / Time only.</p>
                    </div>
                  );
                })()}

                {/* Trades remaining */}
                {(() => {
                  const done = live?.trades_today ?? 0;
                  return (
                    <div className="rounded-lg p-3 border bg-gray-800/40 border-gray-700">
                      <p className="text-[10px] text-gray-500 uppercase mb-2">Trades Today</p>
                      <p className="text-lg font-black text-white">{done}</p>
                      <p className="text-[10px] mt-1 text-gray-500">
                        {done === 0 ? '✅ No trades taken yet today'
                          : `✅ ${done} trade${done > 1 ? 's' : ''} taken · dry run`}
                      </p>
                    </div>
                  );
                })()}
              </div>

              {/* Strategy rules reference */}
              <div className="bg-gray-800/30 rounded-lg p-3 text-[10px] text-gray-500 grid grid-cols-2 sm:grid-cols-4 gap-2">
                <div><span className="text-gray-400 font-semibold block mb-0.5">Entry window</span>9:45 AM – 3:15 PM IST</div>
                <div><span className="text-gray-400 font-semibold block mb-0.5">Signal</span>RSI cross 48↑ CE · 52↓ PE</div>
                <div><span className="text-gray-400 font-semibold block mb-0.5">Exit rules</span>+7pt target · -5pt SL · 15min</div>
                <div><span className="text-gray-400 font-semibold block mb-0.5">Size</span>5 lots · 15min cooldown</div>
              </div>

            </CardContent>
          </Card>
        )}

        {/* ── Row 3: Today + All-time ── */}
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">

          {/* Today */}
          <Card className="bg-gray-900 border-gray-800">
            <CardHeader className="pb-2">
              <CardTitle className="text-sm text-gray-400 font-medium">Today</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="grid grid-cols-3 gap-2 mb-3">
                <div className="bg-gray-800 rounded-lg p-3 text-center">
                  <p className="text-[10px] text-gray-500 uppercase mb-1">Trades</p>
                  <p className="text-2xl font-black text-white">
                    {todayTotal}
                  </p>
                </div>
                <div className="bg-gray-800 rounded-lg p-3 text-center">
                  <p className="text-[10px] text-gray-500 uppercase mb-1">Win rate</p>
                  <p className={`text-2xl font-black ${todayWR >= 50 ? 'text-emerald-400' : 'text-red-400'}`}>
                    {todayTotal > 0 ? `${todayWR}%` : '—'}
                  </p>
                </div>
                <div className={`rounded-lg p-3 text-center ${
                  (live?.daily_pnl ?? 0) >= 0 ? 'bg-emerald-950/40' : 'bg-red-950/40'
                }`}>
                  <p className="text-[10px] text-gray-500 uppercase mb-1">P&amp;L</p>
                  <p className={`text-xl font-black ${
                    (live?.daily_pnl ?? 0) >= 0 ? 'text-emerald-400' : 'text-red-400'
                  }`}>
                    {fmtINR(live?.daily_pnl ?? 0)}
                  </p>
                </div>
              </div>

              {todayTrades.length > 0 && (
                <>
                  <Separator className="bg-gray-800 mb-2" />
                  <div className="space-y-1.5">
                    {todayTrades.map((t, i) => (
                      <div key={i} className="flex items-center gap-2 text-xs">
                        <span className={`font-bold w-6 text-center ${t.direction === 'CE' ? 'text-emerald-400' : 'text-red-400'}`}>
                          {t.direction}
                        </span>
                        <span className="text-gray-500 flex-1 truncate">{t.symbol}</span>
                        <span className="text-gray-500">{fmtTime(t.entry_ts)}</span>
                        <span className={`capitalize px-1.5 py-0.5 rounded text-[10px] ${
                          t.reason === 'target' ? 'bg-emerald-900/50 text-emerald-400' :
                          t.reason === 'stop'   ? 'bg-red-900/50 text-red-400' :
                          'bg-gray-800 text-gray-500'
                        }`}>{t.reason}</span>
                        <span className={`font-bold tabular-nums w-20 text-right ${t.pnl >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
                          {fmtINR(t.pnl)}
                        </span>
                      </div>
                    ))}
                  </div>
                </>
              )}
            </CardContent>
          </Card>

          {/* All-time */}
          <Card className="bg-gray-900 border-gray-800">
            <CardHeader className="pb-2">
              <CardTitle className="text-sm text-gray-400 font-medium">30-day Summary</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="grid grid-cols-3 gap-2">
                <div className="bg-gray-800 rounded-lg p-3 text-center">
                  <p className="text-[10px] text-gray-500 uppercase mb-1">Trades</p>
                  <p className="text-2xl font-black text-white">{total}</p>
                </div>
                <div className="bg-gray-800 rounded-lg p-3 text-center">
                  <p className="text-[10px] text-gray-500 uppercase mb-1">Win rate</p>
                  <p className={`text-2xl font-black ${winRate >= 50 ? 'text-emerald-400' : 'text-red-400'}`}>
                    {total > 0 ? `${winRate}%` : '—'}
                  </p>
                </div>
                <div className={`rounded-lg p-3 text-center ${totalPnl >= 0 ? 'bg-emerald-950/40' : 'bg-red-950/40'}`}>
                  <p className="text-[10px] text-gray-500 uppercase mb-1">Total P&amp;L</p>
                  <p className={`text-xl font-black ${totalPnl >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
                    {fmtINR(totalPnl)}
                  </p>
                </div>
              </div>

              {/* Win/Loss breakdown bar */}
              {total > 0 && (
                <div className="mt-4">
                  <div className="flex text-[10px] text-gray-500 justify-between mb-1">
                    <span>Wins: {wins}</span>
                    <span>Losses: {total - wins}</span>
                  </div>
                  <div className="h-2 bg-red-900/40 rounded-full overflow-hidden">
                    <div
                      className="h-full bg-emerald-600 rounded-full transition-all duration-500"
                      style={{ width: `${winRate}%` }}
                    />
                  </div>
                </div>
              )}
            </CardContent>
          </Card>
        </div>

        {/* ── Historical Trades Table ── */}
        <Card className="bg-gray-900 border-gray-800">
          <CardHeader>
            <div className="flex items-center justify-between">
              <div>
                <CardTitle className="flex items-center gap-2 text-base">
                  <BarChart3 className="h-4 w-4 text-gray-400" />
                  Historical Trades
                </CardTitle>
                <CardDescription className="text-gray-600 text-xs mt-1">
                  Last 30 days from DynamoDB · {total} records
                </CardDescription>
              </div>
              <Button
                onClick={fetchHistory}
                variant="outline"
                size="sm"
                className="border-gray-700 text-gray-400 hover:bg-gray-800"
              >
                <RefreshCw className="h-3.5 w-3.5 mr-1" /> Refresh
              </Button>
            </div>
          </CardHeader>
          <CardContent className="p-0">
            {history.length === 0 ? (
              <div className="py-12 text-center text-gray-700 text-sm">
                No trades yet — start the trader and come back after your first trade
              </div>
            ) : (
              <ScrollArea className="h-[400px]">
                <table className="w-full text-xs">
                  <thead className="sticky top-0 bg-gray-900 border-b border-gray-800 z-10">
                    <tr className="text-gray-600 text-left">
                      <th className="px-4 py-3 font-medium">Date</th>
                      <th className="px-4 py-3 font-medium">Dir</th>
                      <th className="px-4 py-3 font-medium">Symbol</th>
                      <th className="px-4 py-3 font-medium">Entry</th>
                      <th className="px-4 py-3 font-medium">Exit</th>
                      <th className="px-4 py-3 font-medium">Buy ₹</th>
                      <th className="px-4 py-3 font-medium">Sell ₹</th>
                      <th className="px-4 py-3 font-medium">Reason</th>
                      <th className="px-4 py-3 font-medium text-right">P&amp;L</th>
                    </tr>
                  </thead>
                  <tbody>
                    {history.map((t, i) => (
                      <tr
                        key={`${t.entry_ts}-${i}`}
                        className="border-b border-gray-800/40 hover:bg-gray-800/30 transition-colors"
                      >
                        <td className="px-4 py-2.5 text-gray-500 whitespace-nowrap">{fmtDate(t.entry_ts)}</td>
                        <td className="px-4 py-2.5">
                          <span className={`font-bold ${t.direction === 'CE' ? 'text-emerald-400' : 'text-red-400'}`}>
                            {t.direction}
                          </span>
                        </td>
                        <td className="px-4 py-2.5 font-mono text-gray-400 text-[10px] whitespace-nowrap">{t.symbol}</td>
                        <td className="px-4 py-2.5 text-gray-500 whitespace-nowrap">{fmtTime(t.entry_ts)}</td>
                        <td className="px-4 py-2.5 text-gray-500 whitespace-nowrap">{fmtTime(t.exit_ts)}</td>
                        <td className="px-4 py-2.5 font-mono text-gray-300 tabular-nums">{fmt(t.entry_price)}</td>
                        <td className="px-4 py-2.5 font-mono text-gray-300 tabular-nums">{fmt(t.exit_price)}</td>
                        <td className="px-4 py-2.5">
                          <span className={`text-[10px] px-1.5 py-0.5 rounded capitalize ${
                            t.reason === 'target' ? 'bg-emerald-900/50 text-emerald-400' :
                            t.reason === 'stop'   ? 'bg-red-900/50 text-red-400' :
                            'bg-gray-800 text-gray-500'
                          }`}>{t.reason}</span>
                        </td>
                        <td className={`px-4 py-2.5 text-right font-black tabular-nums ${
                          t.pnl_total >= 0 ? 'text-emerald-400' : 'text-red-400'
                        }`}>
                          {fmtINR(t.pnl_total)}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </ScrollArea>
            )}
          </CardContent>
        </Card>

        <p className="text-center text-[10px] text-gray-800 pb-2">
          Paper trading · VWAP+RSI · WebSocket 500 ms · DynamoDB vwap_rsi_trades
        </p>
      </div>
    </div>
  );
}
