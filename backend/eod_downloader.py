"""
VWAP+RSI EOD Historical Data Downloader
────────────────────────────────────────────────────────────────────────────────
Runs at 3:40 PM IST every trading day (triggered automatically by live_trader.py).
Downloads today's 1-min candles for:
  • NIFTY front-month futures
  • NIFTY nearest weekly expiry options — ATM ± 300 pts (CE + PE)

Saves to S3 in parquet format:
  s3://naveen-trading-data/nifty-50/{SYMBOL}/{YEAR}/{SYMBOL}_{YEAR}_minute.parquet

Incremental: loads existing S3 file, appends today's candles, deduplicates,
saves back. Safe to re-run multiple times.

Usage:
  Standalone : python backend/eod_downloader.py
  From cron  : 40 15 * * 1-5  cd D:\\vwaprasiscalper && python backend/eod_downloader.py
  Imported   : from backend.eod_downloader import run_eod_download
"""

import sys
import os
import io
import logging
import time
from datetime import datetime, date, timezone, timedelta
from pathlib import Path

import boto3
import pandas as pd
from botocore.exceptions import ClientError

# ── TRADINGWORLD auth ──────────────────────────────────────────────────────────
TW_ROOT = Path("D:/TRADINGWORLD/TRADINGWORLD")
if not TW_ROOT.exists():
    raise SystemExit(f"TRADINGWORLD not found at {TW_ROOT}")
sys.path.insert(0, str(TW_ROOT))
os.chdir(str(TW_ROOT))

from tools._kite_auth import _kite_client
from notifications.telegram import TelegramNotifier

# ── Config ─────────────────────────────────────────────────────────────────────
BUCKET       = "naveen-trading-data"
DATA_PREFIX  = "nifty-50"
AWS_REGION   = "ap-south-1"
RATE_DELAY   = 0.35        # seconds between Kite API calls (~3 req/sec limit)
STRIKE_RANGE = 300         # download ATM ± this many pts
STRIKE_STEP  = 50
MAX_RETRIES  = 3

IST = timezone(timedelta(hours=5, minutes=30))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("eod_downloader")


# ── S3 helpers ─────────────────────────────────────────────────────────────────

def s3_key(symbol: str, year: int) -> str:
    return f"{DATA_PREFIX}/{symbol}/{year}/{symbol}_{year}_minute.parquet"


def load_from_s3(s3, symbol: str, year: int) -> pd.DataFrame:
    key = s3_key(symbol, year)
    try:
        resp = s3.get_object(Bucket=BUCKET, Key=key)
        df   = pd.read_parquet(io.BytesIO(resp["Body"].read()))
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        return df
    except ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return pd.DataFrame()
        raise


def save_to_s3(s3, df: pd.DataFrame, symbol: str, year: int) -> bool:
    key = s3_key(symbol, year)
    try:
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        buf.seek(0)
        s3.put_object(Bucket=BUCKET, Key=key,
                      Body=buf.getvalue(),
                      ContentType="application/octet-stream")
        log.info("  S3 saved: %s  (%d rows)", key, len(df))
        return True
    except Exception as e:
        log.error("  S3 save FAILED %s: %s", key, e)
        return False


def merge_and_save(s3, new_df: pd.DataFrame, symbol: str) -> int:
    """Merge new_df into existing S3 data. Returns total new rows saved."""
    if new_df.empty:
        return 0

    new_df         = new_df.copy()
    new_df["timestamp"] = pd.to_datetime(new_df["timestamp"])
    new_df["_year"]     = new_df["timestamp"].dt.year
    total_saved    = 0

    for yr, chunk in new_df.groupby("_year"):
        chunk    = chunk.drop(columns=["_year"])
        existing = load_from_s3(s3, symbol, yr)

        combined = chunk if existing.empty else (
            pd.concat([existing, chunk])
            .drop_duplicates(subset="timestamp")
            .sort_values("timestamp")
            .reset_index(drop=True)
        )

        if save_to_s3(s3, combined, symbol, yr):
            total_saved += len(chunk)

    return total_saved


# ── Kite historical download ───────────────────────────────────────────────────

def fetch_minute_data(kite, token: int, from_dt: datetime,
                      to_dt: datetime, label: str) -> pd.DataFrame:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            candles = kite.historical_data(
                instrument_token=token,
                from_date=from_dt,
                to_date=to_dt,
                interval="minute",
                continuous=False,
                oi=False,
            )
            if not candles:
                log.debug("  %s: no candles returned", label)
                return pd.DataFrame()

            df = pd.DataFrame(candles)
            df["timestamp"] = pd.to_datetime(df["date"])
            df = df.drop(columns=["date"], errors="ignore")
            df = df[["timestamp", "open", "high", "low", "close", "volume"]]
            log.info("  %s: %d candles", label, len(df))
            return df
        except Exception as e:
            log.warning("  %s attempt %d/%d failed: %s", label, attempt, MAX_RETRIES, e)
            if attempt < MAX_RETRIES:
                time.sleep(2 * attempt)

    log.error("  %s: all retries failed", label)
    return pd.DataFrame()


# ── Symbol discovery ───────────────────────────────────────────────────────────

def get_futures_instrument(kite) -> dict | None:
    today = date.today()
    futs  = sorted(
        [i for i in kite.instruments("NFO")
         if i.get("name") == "NIFTY"
         and i.get("instrument_type") == "FUT"
         and i.get("expiry") >= today],
        key=lambda x: x["expiry"],
    )
    return futs[0] if futs else None


def get_options_instruments(kite, spot: float) -> list[dict]:
    today  = date.today()
    atm    = round(spot / STRIKE_STEP) * STRIKE_STEP
    lo, hi = atm - STRIKE_RANGE, atm + STRIKE_RANGE

    instruments      = kite.instruments("NFO")
    weekly_expiries  = sorted(set(
        i["expiry"] for i in instruments
        if i.get("name") == "NIFTY"
        and i.get("segment") == "NFO-OPT"
        and i.get("expiry") > today
    ))
    if not weekly_expiries:
        log.warning("No NIFTY option expiries found.")
        return []

    nearest = weekly_expiries[0]
    log.info("Options expiry: %s | ATM: %d | Range: %d–%d", nearest, atm, lo, hi)

    return [
        i for i in instruments
        if i.get("name") == "NIFTY"
        and i.get("segment") == "NFO-OPT"
        and i.get("expiry") == nearest
        and i.get("instrument_type") in ("CE", "PE")
        and lo <= i.get("strike", 0) <= hi
    ]


# ── Core entry point ───────────────────────────────────────────────────────────

def run_eod_download(notifier: TelegramNotifier | None = None) -> dict:
    """
    Download today's NIFTY futures + options 1-min data and save to S3.
    Safe to call multiple times (incremental, deduplicates).
    Returns a summary dict.
    """
    start_ts = datetime.now(IST)
    today    = start_ts.date()

    log.info("=" * 65)
    log.info("EOD DOWNLOADER — %s", today)
    log.info("=" * 65)

    session_start = datetime(today.year, today.month, today.day, 9, 15, 0)
    session_end   = datetime(today.year, today.month, today.day, 15, 30, 0)

    summary = {
        "date":            str(today),
        "futures_saved":   0,
        "options_saved":   0,
        "options_skipped": 0,
        "errors":          [],
    }

    # Auth
    log.info("Authenticating...")
    try:
        kite = _kite_client()
    except Exception as e:
        msg = f"Kite auth failed: {e}"
        log.error(msg); summary["errors"].append(msg); return summary

    s3 = boto3.client("s3", region_name=AWS_REGION)

    # Step 1: Futures
    log.info("\n[1/3] Downloading NIFTY futures...")
    fut_inst = get_futures_instrument(kite)
    if fut_inst is None:
        msg = "No active NIFTY futures found."
        log.error(msg); summary["errors"].append(msg)
    else:
        sym   = fut_inst["tradingsymbol"]
        token = int(fut_inst["instrument_token"])
        log.info("Futures: %s (token=%d, expiry=%s)", sym, token, fut_inst["expiry"])
        df = fetch_minute_data(kite, token, session_start, session_end, sym)
        if not df.empty:
            summary["futures_saved"] = merge_and_save(s3, df, sym)
            log.info("Futures saved: %d candles", summary["futures_saved"])
        else:
            log.warning("No futures data downloaded.")
        time.sleep(RATE_DELAY)

    # Step 2: NIFTY spot for ATM
    log.info("\n[2/3] Getting NIFTY spot for ATM range...")
    spot = 0.0
    try:
        ltp_data = kite.ltp(["NSE:NIFTY 50"])
        spot     = float(ltp_data.get("NSE:NIFTY 50", {}).get("last_price", 0) or 0)
    except Exception:
        pass
    if spot <= 0 and fut_inst:
        try:
            key      = f"NFO:{fut_inst['tradingsymbol']}"
            ltp_data = kite.ltp([key])
            spot     = float(ltp_data.get(key, {}).get("last_price", 0) or 0)
        except Exception as e:
            log.warning("Could not fetch spot: %s", e)
    if spot <= 0:
        msg = "Could not determine NIFTY spot — skipping options."
        log.error(msg); summary["errors"].append(msg); return summary

    log.info("NIFTY spot: %.0f", spot)

    # Step 3: Options
    log.info("\n[3/3] Downloading NIFTY options (ATM ±%d pts)...", STRIKE_RANGE)
    opts = get_options_instruments(kite, spot)
    log.info("Options in range: %d contracts", len(opts))

    for idx, inst in enumerate(opts, 1):
        sym   = inst["tradingsymbol"]
        token = int(inst["instrument_token"])
        df    = fetch_minute_data(kite, token, session_start, session_end, f"{sym} [{idx}/{len(opts)}]")
        if not df.empty:
            summary["options_saved"] += merge_and_save(s3, df, sym)
        else:
            summary["options_skipped"] += 1
        time.sleep(RATE_DELAY)

    # Summary
    elapsed = (datetime.now(IST) - start_ts).total_seconds()
    log.info("")
    log.info("=" * 65)
    log.info("EOD COMPLETE in %.0f sec", elapsed)
    log.info("  Futures : %d candles", summary["futures_saved"])
    log.info("  Options : %d candles (%d contracts)",
             summary["options_saved"], len(opts) - summary["options_skipped"])
    log.info("  Skipped : %d", summary["options_skipped"])
    if summary["errors"]:
        log.warning("  Errors  : %s", summary["errors"])
    log.info("=" * 65)

    # Telegram alert
    _notifier = notifier
    if _notifier is None:
        try:
            _notifier = TelegramNotifier()
        except Exception:
            pass
    if _notifier:
        status = "OK" if not summary["errors"] else f"WARN ({len(summary['errors'])} errors)"
        _notifier.send_system_alert(
            f"EOD DATA DOWNLOAD — {today} [{status}]\n"
            f"Futures : {summary['futures_saved']} candles\n"
            f"Options : {summary['options_saved']} candles "
            f"({len(opts) - summary['options_skipped']} contracts)\n"
            f"Time    : {elapsed:.0f}s",
            "INFO" if not summary["errors"] else "WARNING",
        )

    return summary


if __name__ == "__main__":
    run_eod_download()
