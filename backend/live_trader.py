"""
VWAP+RSI Live Paper Trader
──────────────────────────────────────────────────────────────────────────────
Gets Zerodha token from kite-auth-service (http://localhost:8050).
All orders are paper-simulated — no real orders placed.

Strategy:
  • RSI(14) on 5-min NIFTY futures → buy CE on 48 cross-up, PE on 52 cross-down
  • Price must be within 20 pts of VWAP (computed from futures 1-min volume)
  • Entry: next tick after signal, +1 pt slippage
  • Target: +7 pts  |  Stop: -5 pts  |  Time-stop: 15 min  |  Max 3 trades/day
  • Window: 9:45 AM – 3:15 PM IST
  • 5 lots (lot size read from Kite instruments)

Late-start handling:
  If started after 9:15 AM, fetches historical 1-min candles from Kite API
  and replays them to seed VWAP + RSI before going live.

State output:
  Writes ../vwap_rsi_state.json (project root) every ~1 second.
  Completed trades are written to DynamoDB table vwap_rsi_trades.

Usage (from D:\\vwaprasiscalper):
  python -m backend.live_trader
  # or directly:
  python backend/live_trader.py

Prerequisites:
  kite-auth-service must be running on http://localhost:8050
  Set KITE_AUTH_URL env var to override the default URL.
"""

import logging
import threading
import time
import json
from datetime import datetime, date, timezone, timedelta
from pathlib import Path
from decimal import Decimal

import boto3
from kiteconnect import KiteConnect

from backend.kite_client import get_kite, build_instrument_cache, resolve_nearest_expiry, TickerClient

# ── File / cloud sinks ─────────────────────────────────────────────────────────
# Project root = two levels up from this file (backend/live_trader.py)
PROJECT_ROOT = Path(__file__).parent.parent
STATE_FILE   = PROJECT_ROOT / "vwap_rsi_state.json"
TRADES_CSV   = PROJECT_ROOT / "vwap_rsi_paper_trades.csv"
DYNAMO_TABLE = "vwap_rsi_trades"
AWS_REGION   = "ap-south-1"

_EOD_DOWNLOADER_SCHEDULED = False
_last_state_write: float  = 0.0

# ── Strategy config ────────────────────────────────────────────────────────────
LOTS              = 5
TARGET_PTS        = 7
STOP_PTS          = 5
TIME_STOP_MIN     = 15
MAX_TRADES        = 999   # dry run: unlimited — analyse after 20 sessions
TRADE_COOLDOWN_MIN = 15   # min gap between any two trades
RSI_PERIOD        = 14
RSI_BULL          = 48
RSI_BEAR          = 52
VWAP_BAND         = 20
ENTRY_SLIP        = 1.0

IST = timezone(timedelta(hours=5, minutes=30))

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("vwap_rsi")

# ── RSI (Wilder EWMA, online) ──────────────────────────────────────────────────

class OnlineRSI:
    """Wilder's RSI computed incrementally on 5-min closes."""

    def __init__(self, period: int = 14):
        self.period    = period
        self.alpha     = 1.0 / period
        self._seed     : list[tuple[float, float]] = []
        self._avg_gain : float | None = None
        self._avg_loss : float | None = None
        self._prev     : float | None = None

    def update(self, close: float) -> float | None:
        if self._prev is None:
            self._prev = close
            return None

        delta = close - self._prev
        gain  = max(delta, 0.0)
        loss  = max(-delta, 0.0)
        self._prev = close

        if self._avg_gain is None:
            self._seed.append((gain, loss))
            if len(self._seed) < self.period:
                return None
            self._avg_gain = sum(g for g, _ in self._seed) / self.period
            self._avg_loss = sum(l for _, l in self._seed) / self.period
            self._seed = []
        else:
            self._avg_gain = self.alpha * gain + (1 - self.alpha) * self._avg_gain
            self._avg_loss = self.alpha * loss + (1 - self.alpha) * self._avg_loss

        if self._avg_loss == 0:
            return 100.0
        return 100.0 - (100.0 / (1.0 + self._avg_gain / self._avg_loss))


# ── Shared state ───────────────────────────────────────────────────────────────
_lock = threading.Lock()
_rsi  = OnlineRSI(RSI_PERIOD)


state: dict = {
    "today":           date.today(),
    "trades_today":    0,
    "daily_pnl":       0.0,
    "today_trades":    [],
    "position":        None,
    "last_stop_ts":    None,   # datetime of last stop-loss exit
    "cur_candle":      None,
    "candles_1m":      [],
    "candles_5m_buf":  [],
    "cum_tpv":         0.0,
    "cum_vol":         0.0,
    "vwap":            None,
    "rsi14":           None,
    "prev_rsi":        None,
    "fut_ltp":         None,
    "opt_ltp":         {},
    "fut_token":       None,
    "fut_symbol":      None,
    "kite":            None,
    "cache":           None,
    "notifier":        None,
    "ws":              None,
}


def now_ist() -> datetime:
    return datetime.now(IST)


# ── State JSON writer ──────────────────────────────────────────────────────────

def _write_state(is_running: bool = True) -> None:
    """Snapshot state to vwap_rsi_state.json (throttled to 1/sec)."""
    global _last_state_write
    now = time.time()
    if is_running and now - _last_state_write < 1.0:
        return
    _last_state_write = now

    try:
        with _lock:
            pos = state["position"]
            pos_snap = None
            if pos:
                tok     = pos["opt_token"]
                cur_ltp = state["opt_ltp"].get(tok, pos["entry_price"])
                unreal  = round((cur_ltp - pos["entry_price"]) * pos["qty"], 2)
                pos_snap = {
                    "symbol":         pos["symbol"],
                    "direction":      pos["direction"],
                    "atm":            pos["atm"],
                    "entry_price":    pos["entry_price"],
                    "current_ltp":    cur_ltp,
                    "entry_ts":       pos["entry_ts"].isoformat(),
                    "qty":            pos["qty"],
                    "unrealized_pnl": unreal,
                    "vwap_at_entry":  pos.get("vwap_at_entry"),
                    "rsi_at_entry":   pos.get("rsi_at_entry"),
                    "spot_at_entry":  pos.get("spot_at_entry"),
                }

            snap = {
                "is_running":   is_running,
                "updated_at":   now_ist().isoformat(),
                "fut_symbol":   state["fut_symbol"],
                "fut_ltp":      state["fut_ltp"],
                "vwap":         round(state["vwap"], 2) if state["vwap"] else None,
                "rsi14":        round(state["rsi14"], 2) if state["rsi14"] else None,
                "prev_rsi":     round(state["prev_rsi"], 2) if state["prev_rsi"] else None,
                "trades_today": state["trades_today"],
                "daily_pnl":    state["daily_pnl"],
                "position":     pos_snap,
                "today_trades": state["today_trades"],
            }

        STATE_FILE.write_text(json.dumps(snap, indent=2, default=str), encoding="utf-8")
    except Exception as exc:
        log.debug("State write error: %s", exc)


def _dynamo_save_trade(pos: dict, exit_price: float, reason: str, pnl: float,
                       spot_exit: float = 0, vwap_exit: float = 0, rsi_exit: float = 0) -> None:
    """Persist completed trade to DynamoDB."""
    try:
        table = boto3.resource("dynamodb", region_name=AWS_REGION).Table(DYNAMO_TABLE)
        table.put_item(Item={
            "trade_date":   pos["entry_ts"].strftime("%Y-%m-%d"),
            "entry_ts":     pos["entry_ts"].isoformat(),
            "exit_ts":      now_ist().isoformat(),
            "direction":    pos["direction"],
            "symbol":       pos["symbol"],
            "atm":          pos["atm"],
            "entry_price":  Decimal(str(round(pos["entry_price"], 2))),
            "exit_price":   Decimal(str(round(exit_price, 2))),
            "reason":       reason,
            "qty":          pos["qty"],
            "lots":         LOTS,
            "pnl_total":    Decimal(str(round(pnl, 2))),
            "spot_entry":   Decimal(str(round(pos.get("spot_at_entry") or 0, 2))),
            "vwap_entry":   Decimal(str(round(pos.get("vwap_at_entry") or 0, 2))),
            "rsi_entry":    Decimal(str(round(pos.get("rsi_at_entry") or 0, 2))),
            "spot_exit":    Decimal(str(round(spot_exit, 2))),
            "vwap_exit":    Decimal(str(round(vwap_exit, 2))),
            "rsi_exit":     Decimal(str(round(rsi_exit, 2))),
        })
        log.info("Trade saved to DynamoDB: %s | P&L=%.0f", pos["symbol"], pnl)
    except Exception as exc:
        log.warning("DynamoDB save failed: %s", exc)


# ── Candle builder ─────────────────────────────────────────────────────────────

def _finalize_candle(candle: dict) -> None:
    """Called when a 1-min candle closes. Updates VWAP and maybe RSI."""
    today = date.today()
    if state["today"] != today:
        state["today"]          = today
        state["trades_today"]   = 0
        state["daily_pnl"]      = 0.0
        state["today_trades"]   = []
        state["position"]       = None
        state["cum_tpv"]        = 0.0
        state["cum_vol"]        = 0.0
        state["vwap"]           = None
        state["rsi14"]          = None
        state["prev_rsi"]       = None
        state["candles_1m"]     = []
        state["candles_5m_buf"] = []
        log.info("Day rollover — state reset")

    state["candles_1m"].append(candle)

    vol = candle["volume"]
    if vol > 0:
        tp            = (candle["high"] + candle["low"] + candle["close"]) / 3
        state["cum_tpv"] += tp * vol
        state["cum_vol"] += vol
        state["vwap"]     = state["cum_tpv"] / state["cum_vol"]

    state["candles_5m_buf"].append(candle)
    if len(state["candles_5m_buf"]) >= 5:
        close_5m = state["candles_5m_buf"][-1]["close"]
        rsi_val  = _rsi.update(close_5m)
        if rsi_val is not None:
            state["prev_rsi"] = state["rsi14"]
            state["rsi14"]    = rsi_val
        state["candles_5m_buf"] = []


def process_fut_tick(tick: dict) -> None:
    """Called on every NIFTY futures tick. Builds 1-min OHLCV and checks strategy."""
    ltp     = float(tick.get("last_price") or tick.get("last_traded_price") or 0)
    day_vol = int(tick.get("volume_traded") or 0)
    if not ltp:
        return

    ts        = now_ist()
    minute_ts = ts.replace(second=0, microsecond=0)

    with _lock:
        state["fut_ltp"] = ltp
        cc = state["cur_candle"]

        if cc is None or cc["ts"] != minute_ts:
            if cc is not None:
                _finalize_candle(cc)
            state["cur_candle"] = {
                "ts": minute_ts, "open": ltp, "high": ltp,
                "low": ltp, "close": ltp, "volume": 0, "_base_vol": day_vol,
            }
        else:
            cc["high"]  = max(cc["high"], ltp)
            cc["low"]   = min(cc["low"],  ltp)
            cc["close"] = ltp
            cc["volume"] = max(day_vol - cc["_base_vol"], 0)

        _check_strategy(ltp, ts)

    _write_state()


# ── Strategy ───────────────────────────────────────────────────────────────────

def _check_strategy(ltp: float, ts: datetime) -> None:
    """Signal detection + exit monitoring. Called inside _lock."""
    h, m = ts.hour, ts.minute
    if h < 9 or (h == 9 and m < 45):
        return
    if h > 15 or (h == 15 and m >= 15):
        return

    if state["position"] is not None:
        _check_exit(ltp, ts)
        return

    if state["trades_today"] >= MAX_TRADES:
        return

    # Cooldown after any exit — enforce min gap between trades
    last_stop = state["last_stop_ts"]
    if last_stop is not None:
        mins_since_last = (ts - last_stop).total_seconds() / 60
        if mins_since_last < TRADE_COOLDOWN_MIN:
            return

    curr_rsi = state["rsi14"]
    prev_rsi = state["prev_rsi"]
    vwap     = state["vwap"]
    if curr_rsi is None or prev_rsi is None or vwap is None:
        return
    if abs(ltp - vwap) > VWAP_BAND:
        return

    direction = None
    if prev_rsi < RSI_BULL <= curr_rsi:
        direction = "CE"
    elif prev_rsi > RSI_BEAR >= curr_rsi:
        direction = "PE"

    if direction:
        log.info("SIGNAL %s | spot=%.0f | vwap=%.0f | RSI %.1f→%.1f",
                 direction, ltp, vwap, prev_rsi, curr_rsi)
        threading.Thread(target=_enter_trade, args=(direction, ltp, curr_rsi, ts), daemon=True).start()


def _check_exit(ltp: float, ts: datetime) -> None:
    """Check if open position should close. Called inside _lock."""
    pos     = state["position"]
    tok     = pos["opt_token"]
    opt_ltp = state["opt_ltp"].get(tok, pos["entry_price"])
    entry   = pos["entry_price"]
    elapsed = (ts - pos["entry_ts"]).total_seconds() / 60.0

    reason = None
    if opt_ltp >= entry + TARGET_PTS:
        reason = "target"
    elif opt_ltp <= entry - STOP_PTS:
        reason = "stop"
    elif elapsed >= TIME_STOP_MIN:
        reason = "time"

    if reason:
        threading.Thread(target=_exit_trade, args=(reason, opt_ltp), daemon=True).start()


# ── Trade entry / exit ─────────────────────────────────────────────────────────

def _enter_trade(direction: str, spot: float, rsi_val: float, ts: datetime) -> None:
    with _lock:
        if state["position"] is not None or state["trades_today"] >= MAX_TRADES:
            return
        cache = state["cache"]
        vwap  = state["vwap"]

    atm  = round(spot / 50) * 50
    inst = resolve_nearest_expiry(cache, atm, direction)
    if inst is None:
        log.warning("No option found: ATM=%d %s", atm, direction)
        return

    sym      = inst["tradingsymbol"]
    token    = inst["instrument_token"]
    lot_size = inst["lot_size"]
    qty      = LOTS * lot_size

    opt_price = 0.0
    try:
        ltp_data  = state["kite"].ltp([f"NFO:{sym}"])
        opt_price = float(ltp_data.get(f"NFO:{sym}", {}).get("last_price", 0) or 0)
    except Exception as e:
        log.warning("Option LTP fetch failed %s: %s", sym, e)

    entry_price = round(opt_price + ENTRY_SLIP, 2)

    with _lock:
        if state["position"] is not None:
            return
        state["position"] = {
            "direction":    direction,
            "symbol":       sym,
            "opt_token":    token,
            "lot_size":     lot_size,
            "qty":          qty,
            "atm":          atm,
            "entry_price":  entry_price,
            "entry_ts":     ts,
            "vwap_at_entry": vwap,
            "rsi_at_entry":  rsi_val,
            "spot_at_entry": spot,
        }
        state["trades_today"] += 1
        count = state["trades_today"]

    ws = state["ws"]
    if ws:
        try:
            ws.subscribe([token])
            ws.set_mode(ws.MODE_LTP, [token])
        except Exception as e:
            log.warning("Option subscribe failed: %s", e)

    log.info("[PAPER ENTRY] BUY %s | entry=%.2f | spot=%.0f | RSI=%.1f | qty=%d | trade %d/%d",
             sym, entry_price, spot, rsi_val, qty, count, MAX_TRADES)

    notifier = state["notifier"]
    if notifier:
        notifier.send_system_alert(
            f"PAPER TRADE: BUY {direction}\n"
            f"Symbol : {sym}\nEntry  : ₹{entry_price:.2f}\n"
            f"Spot   : {spot:.0f} | VWAP: {vwap:.0f if vwap else 'N/A'}\n"
            f"RSI    : {rsi_val:.1f}\nTgt +{TARGET_PTS}pts · SL -{STOP_PTS}pts\n"
            f"Trade  : {count}/{MAX_TRADES}",
            "INFO",
        )


def _exit_trade(reason: str, exit_price: float) -> None:
    with _lock:
        pos = state["position"]
        if pos is None:
            return
        state["position"] = None

    entry    = pos["entry_price"]
    qty      = pos["qty"]
    pnl      = round((exit_price - entry) * qty, 2)
    exit_ts  = now_ist()
    outcome  = "WIN" if pnl > 0 else ("LOSS" if pnl < 0 else "SCRATCH")

    record = {
        "entry_ts":    pos["entry_ts"].isoformat(),
        "exit_ts":     exit_ts.isoformat(),
        "direction":   pos["direction"],
        "symbol":      pos["symbol"],
        "atm":         pos["atm"],
        "entry_price": pos["entry_price"],
        "exit_price":  exit_price,
        "reason":      reason,
        "qty":         qty,
        "pnl":         pnl,
    }
    with _lock:
        state["daily_pnl"]     = round(state["daily_pnl"] + pnl, 2)
        state["today_trades"].append(record)
        count = state["trades_today"]
        state["last_stop_ts"] = exit_ts
        log.info("Trade cooldown started — no new entries for %d min", TRADE_COOLDOWN_MIN)

    with _lock:
        spot_exit = state["fut_ltp"] or 0
        vwap_exit = state["vwap"]    or 0
        rsi_exit  = state["rsi14"]   or 0

    log.info("[PAPER EXIT] %s | %s | entry=%.2f exit=%.2f | spot=%.0f vwap=%.0f rsi=%.1f | P&L=₹%.0f",
             reason.upper(), pos["symbol"], entry, exit_price, spot_exit, vwap_exit, rsi_exit, pnl)

    threading.Thread(target=_dynamo_save_trade,
                     args=(pos, exit_price, reason, pnl, spot_exit, vwap_exit, rsi_exit),
                     daemon=True).start()
    _log_trade_csv(pos, exit_price, reason, pnl)

    global _last_state_write
    _last_state_write = 0.0
    _write_state()

    notifier = state["notifier"]
    if notifier:
        notifier.send_system_alert(
            f"PAPER EXIT: {outcome} — {reason.upper()}\n"
            f"Symbol : {pos['symbol']}\n"
            f"Entry ₹{entry:.2f} → Exit ₹{exit_price:.2f}\n"
            f"P&L    : ₹{pnl:,.0f}\nTrades : {count}/{MAX_TRADES}",
            "INFO",
        )


def _log_trade_csv(pos: dict, exit_price: float, reason: str, pnl: float) -> None:
    header = not TRADES_CSV.exists()
    row    = {
        "date":        pos["entry_ts"].strftime("%Y-%m-%d"),
        "entry_ts":    pos["entry_ts"].isoformat(),
        "exit_ts":     now_ist().isoformat(),
        "direction":   pos["direction"],
        "symbol":      pos["symbol"],
        "atm":         pos["atm"],
        "entry_price": pos["entry_price"],
        "exit_price":  exit_price,
        "reason":      reason,
        "qty":         pos["qty"],
        "pnl_total":   pnl,
        "spot_entry":  pos["spot_at_entry"],
        "vwap_entry":  round(pos.get("vwap_at_entry") or 0, 2),
        "rsi_entry":   round(pos.get("rsi_at_entry") or 0, 2),
    }
    try:
        with open(TRADES_CSV, "a", encoding="utf-8") as f:
            if header:
                f.write(",".join(row.keys()) + "\n")
            f.write(",".join(str(v) for v in row.values()) + "\n")
    except Exception as e:
        log.warning("CSV write failed: %s", e)


# ── Historical catch-up ────────────────────────────────────────────────────────

def _seed_rsi_historical(kite, fut_token: int) -> None:
    """
    Fetch up to 60 days of 5-min closes for the current futures contract and
    warm up the RSI EMA. VWAP is not touched — it is always intraday only.
    """
    ist_now  = now_ist()
    # to = yesterday (last completed trading day)
    to_date  = ist_now.date() - timedelta(days=1)
    while to_date.weekday() >= 5:
        to_date -= timedelta(days=1)
    # from = 60 calendar days back (covers ~43 trading days)
    from_date = ist_now.date() - timedelta(days=60)

    from_dt = datetime(from_date.year, from_date.month, from_date.day, 9, 15, 0)
    to_dt   = datetime(to_date.year,   to_date.month,   to_date.day,  15, 30, 0)

    try:
        candles = kite.historical_data(
            instrument_token=fut_token, from_date=from_dt, to_date=to_dt,
            interval="5minute", continuous=False, oi=False,
        )
    except Exception as exc:
        log.warning("RSI historical seed failed: %s — RSI will seed from today's candles", exc)
        return

    if not candles:
        log.warning("RSI historical seed: no candles returned for range %s to %s", from_date, to_date)
        return

    log.info("Seeding RSI from %d historical 5-min candles (%s → %s)",
             len(candles),
             candles[0]["date"].strftime("%Y-%m-%d"),
             candles[-1]["date"].strftime("%Y-%m-%d"))

    rsi_val  = None
    prev_val = None
    for c in candles:
        prev_val = rsi_val
        rsi_val  = _rsi.update(float(c["close"]))

    if rsi_val is not None:
        with _lock:
            state["rsi14"]    = rsi_val
            state["prev_rsi"] = prev_val
        log.info("RSI seeded from %d candles: %.1f → %.1f", len(candles), prev_val or 0, rsi_val)
    else:
        log.warning("RSI still seeding after %d candles — need more data", len(candles))


def catchup_historical(kite, fut_token: int) -> None:
    ist_now     = now_ist()
    market_open = ist_now.replace(hour=9, minute=15, second=0, microsecond=0)

    # Always seed RSI from full 60-day history first
    _seed_rsi_historical(kite, fut_token)

    if ist_now <= market_open:
        log.info("Pre-market start — RSI seeded, VWAP will build from first tick at 9:15 AM.")
        return

    # Late start: replay today's 1-min candles to build VWAP from 9:15 AM
    minutes_late = int((ist_now - market_open).total_seconds() / 60)
    log.info("Late start (%s, %d min after open) — fetching today's 1-min candles for VWAP...",
             ist_now.strftime("%H:%M"), minutes_late)

    from_dt = market_open.replace(tzinfo=None)
    to_dt   = ist_now.replace(tzinfo=None)
    try:
        candles = kite.historical_data(
            instrument_token=fut_token, from_date=from_dt, to_date=to_dt,
            interval="minute", continuous=False, oi=False,
        )
    except Exception as exc:
        log.warning("VWAP catch-up failed: %s", exc)
        return

    if not candles:
        log.warning("VWAP catch-up: no candles returned.")
        return

    log.info("Replaying %d today's 1-min candles for VWAP...", len(candles))
    with _lock:
        for c in candles:
            ts = c["date"]
            if hasattr(ts, "tzinfo") and ts.tzinfo:
                ts = ts.astimezone(IST)
            else:
                ts = ts.replace(tzinfo=IST)
            _finalize_candle({
                "ts": ts.replace(second=0, microsecond=0),
                "open": float(c["open"]), "high": float(c["high"]),
                "low": float(c["low"]),   "close": float(c["close"]),
                "volume": int(c["volume"]),
            })

    vwap = state["vwap"]
    rsi  = state["rsi14"]
    prsi = state["prev_rsi"]
    log.info("Catch-up done | VWAP=%.0f | RSI=%s",
             vwap or 0,
             f"{prsi:.1f}→{rsi:.1f}" if rsi and prsi else "seeding")


# ── Ticker ─────────────────────────────────────────────────────────────────────

def start_ticker(fut_token: int) -> TickerClient:
    """Connect to kite-auth-service WebSocket ticker proxy."""
    ws = TickerClient()

    def on_ticks(ws, ticks):
        for tick in ticks:
            token = tick.get("instrument_token")
            if token == fut_token:
                process_fut_tick(tick)
            else:
                ltp = tick.get("last_price")
                if ltp:
                    with _lock:
                        state["opt_ltp"][token] = float(ltp)
                    _write_state()

    def on_connect(ws, _):
        log.info("Ticker connected — subscribing fut_token=%s", fut_token)
        ws.subscribe([fut_token])
        ws.set_mode(ws.MODE_FULL, [fut_token])

    ws.on_ticks     = on_ticks
    ws.on_connect   = on_connect
    ws.on_close     = lambda ws, c, r: log.warning("Ticker closed: %s %s", c, r)
    ws.on_error     = lambda ws, c, r: log.error("Ticker error: %s %s", c, r)
    ws.on_reconnect = lambda ws, a, d: log.info("Ticker reconnect #%d (delay %ds)", a, d)

    ws.connect(threaded=True)
    return ws


# ── Main ────────────────────────────────────────────────────────────────────────

def main() -> None:
    log.info("=" * 70)
    log.info("VWAP+RSI Live Paper Trader")
    log.info("RSI %d/%d | Tgt +%dpts | SL -%dpts | %d-min stop | Max %d/day | %d lots",
             RSI_BULL, RSI_BEAR, TARGET_PTS, STOP_PTS, TIME_STOP_MIN, MAX_TRADES, LOTS)
    log.info("State file: %s", STATE_FILE)
    log.info("=" * 70)

    # ── Auth via kite-auth-service ────────────────────────────────────────────
    log.info("Fetching Kite session from kite-auth-service...")
    kite = get_kite()
    state["kite"] = kite
    log.info("Kite ready. token=...%s", (kite.access_token or "")[-8:])

    # Telegram notifier removed (no TRADINGWORLD dependency).
    # Set state["notifier"] to a real notifier here if you add one later.
    state["notifier"] = None

    # ── Instrument cache ──────────────────────────────────────────────────────
    log.info("Building NIFTY instrument cache...")
    cache = build_instrument_cache(kite)
    state["cache"] = cache
    log.info("Cache: %d contracts | %d expiries", cache["_count"], len(cache["_by_expiry"]))

    # ── Front-month NIFTY futures ─────────────────────────────────────────────
    today = date.today()
    nifty_futs = sorted(
        [i for i in kite.instruments("NFO")
         if i.get("name") == "NIFTY" and i.get("instrument_type") == "FUT"
         and i.get("expiry") >= today],
        key=lambda x: x["expiry"],
    )
    if not nifty_futs:
        raise SystemExit("No active NIFTY futures found.")

    fut_inst   = nifty_futs[0]
    fut_token  = int(fut_inst["instrument_token"])
    fut_symbol = fut_inst["tradingsymbol"]
    state["fut_token"]  = fut_token
    state["fut_symbol"] = fut_symbol
    log.info("Futures: %s | token=%d | expiry=%s", fut_symbol, fut_token, fut_inst["expiry"])

    # ── Restore today's trade count from DynamoDB ────────────────────────────
    try:
        today_str = date.today().strftime("%Y-%m-%d")
        table = boto3.resource("dynamodb", region_name=AWS_REGION).Table(DYNAMO_TABLE)
        resp  = table.query(
            KeyConditionExpression="trade_date = :d",
            ExpressionAttributeValues={":d": today_str},
        )
        items = resp.get("Items", [])
        done  = len(items)
        if done > 0:
            from decimal import Decimal
            items_sorted = sorted(items, key=lambda x: x.get("entry_ts", ""))
            restored_pnl = float(sum(i.get("pnl_total", Decimal("0")) for i in items_sorted))
            state["trades_today"] = done
            state["daily_pnl"]    = round(restored_pnl, 2)
            state["today_trades"] = [
                {
                    "entry_ts":    i.get("entry_ts", ""),
                    "exit_ts":     i.get("exit_ts", ""),
                    "direction":   i.get("direction", ""),
                    "symbol":      i.get("symbol", ""),
                    "atm":         int(i.get("atm", 0)),
                    "entry_price": float(i.get("entry_price", 0)),
                    "exit_price":  float(i.get("exit_price", 0)),
                    "reason":      i.get("reason", ""),
                    "qty":         int(i.get("qty", 0)),
                    "pnl":         float(i.get("pnl_total", Decimal("0"))),
                }
                for i in items_sorted
            ]
            log.info("Restored from DynamoDB: %d trade(s), daily P&L=₹%.0f", done, restored_pnl)
        else:
            log.info("No trades in DynamoDB for today — starting fresh")
    except Exception as exc:
        log.warning("Could not restore trade count from DynamoDB: %s", exc)

    # ── Historical catch-up ───────────────────────────────────────────────────
    catchup_historical(kite, fut_token)

    # ── Ticker via kite-auth-service WS proxy ────────────────────────────────
    ws = start_ticker(fut_token)
    state["ws"] = ws
    time.sleep(3)

    # Write initial running state
    global _last_state_write
    _last_state_write = 0.0
    _write_state(is_running=True)

    vwap_str = f"{state['vwap']:.0f}" if state["vwap"] else "building..."
    rsi_str  = f"{state['rsi14']:.1f}" if state["rsi14"] else "seeding..."
    log.info("Trader STARTED (%s IST) | Futures=%s | VWAP=%s | RSI=%s",
             now_ist().strftime("%H:%M"), fut_symbol, vwap_str, rsi_str)
    log.info("Trader running. Ctrl+C to stop.")

    # ── Debug: force-trade via SIGUSR1 ───────────────────────────────────────
    import signal as _sig
    def _force_trade_handler(signum, frame):
        with _lock:
            ltp  = state["fut_ltp"]
            rsi  = state["rsi14"] or 50.0
            vwap = state["vwap"]  or ltp
        if not ltp:
            log.warning("[DEBUG] force-trade: no LTP yet, skipping")
            return
        direction = "CE" if (rsi or 50) <= 52 else "PE"
        log.info("[DEBUG] force-trade signal received → entering %s at LTP=%.0f", direction, ltp)
        threading.Thread(
            target=_enter_trade,
            args=(direction, ltp, rsi or 50.0, now_ist()),
            daemon=True,
        ).start()
    _sig.signal(_sig.SIGUSR1, _force_trade_handler)

    # ── EOD downloader at 3:40 PM ─────────────────────────────────────────────
    def _schedule_eod():
        global _EOD_DOWNLOADER_SCHEDULED
        if _EOD_DOWNLOADER_SCHEDULED:
            return
        _EOD_DOWNLOADER_SCHEDULED = True

        def _run():
            while True:
                n = now_ist()
                if n.hour > 15 or (n.hour == 15 and n.minute >= 40):
                    break
                time.sleep(30)
            log.info("3:40 PM — starting EOD download...")
            try:
                sys.path.insert(0, str(PROJECT_ROOT / "backend"))
                from eod_downloader import run_eod_download
                run_eod_download(notifier=state["notifier"])
            except Exception as e:
                log.error("EOD download failed: %s", e)

        threading.Thread(target=_run, name="EOD-Downloader", daemon=True).start()
        log.info("EOD downloader scheduled for 3:40 PM.")

    # EOD download now handled by nifty-eod-sync systemd timer
    # _schedule_eod()

    # ── Main loop ─────────────────────────────────────────────────────────────
    try:
        while True:
            time.sleep(10)
            ist = now_ist()
            if ist.hour >= 15 and ist.minute >= 50:
                log.info("3:50 PM — shutting down.")
                break
            if ist.second < 10 and ist.minute % 5 == 0 and 9 <= ist.hour < 16:
                with _lock:
                    ltp = state["fut_ltp"]; vwap = state["vwap"]
                    rsi = state["rsi14"];  prsi = state["prev_rsi"]
                    pos = state["position"]; cnt  = state["trades_today"]
                log.info("STATUS %s | LTP=%.0f | VWAP=%s | RSI=%s→%s | pos=%s | trades=%d/%d",
                         ist.strftime("%H:%M"), ltp or 0,
                         f"{vwap:.0f}" if vwap else "N/A",
                         f"{prsi:.1f}" if prsi else "?",
                         f"{rsi:.1f}" if rsi else "?",
                         f"OPEN({pos['symbol']})" if pos else "NONE",
                         cnt, MAX_TRADES)
    except KeyboardInterrupt:
        log.info("Stopped by Ctrl+C.")
    finally:
        try:
            ws.close()
        except Exception:
            pass
        with _lock:
            pos = state["position"]
            ltp = state.get("fut_ltp")
        if pos:
            log.warning("Open position at shutdown — force-closing: %s", pos["symbol"])
            opt_ltp = state["opt_ltp"].get(pos.get("opt_token")) or pos["entry_price"]
            _exit_trade("time", opt_ltp)
        _last_state_write = 0.0
        _write_state(is_running=False)
        log.info("Trader STOPPED.")
        log.info("Done.")


if __name__ == "__main__":
    main()
