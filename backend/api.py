"""
VWAP+RSI Scalper — FastAPI Backend
────────────────────────────────────────────────────────────────────────────────
Port  : 8056
Routes:
  GET  /health           → liveness check
  GET  /state            → current live state (reads state.json in project root)
  GET  /trades           → all trades from DynamoDB (last 30 days)
  GET  /trades/{date}    → trades for YYYY-MM-DD
  POST /start            → launch live_trader.py as subprocess
  POST /stop             → gracefully stop it (SIGINT → kill)
  WS   /ws               → streams state.json every 500 ms

Run (from D:\\vwaprasiscalper):
  uvicorn backend.api:app --host 0.0.0.0 --port 8056 --reload
"""

import json
import asyncio
import subprocess
import signal
import os
import sys
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import boto3
from botocore.exceptions import ClientError
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ── Config ─────────────────────────────────────────────────────────────────────
# Project root is one level above this file (backend/)
PROJECT_ROOT  = Path(__file__).parent.parent
STATE_FILE    = PROJECT_ROOT / "vwap_rsi_state.json"
TRADER_SCRIPT = PROJECT_ROOT / "backend" / "live_trader.py"
DYNAMO_TABLE  = "vwap_rsi_trades"
AWS_REGION    = "ap-south-1"
IST           = timezone(timedelta(hours=5, minutes=30))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("vwap_api")

# ── FastAPI app ────────────────────────────────────────────────────────────────
app = FastAPI(title="VWAP RSI Scalper API", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── DynamoDB ───────────────────────────────────────────────────────────────────
_dynamo = None

def get_dynamo():
    global _dynamo
    if _dynamo is None:
        _dynamo = boto3.resource("dynamodb", region_name=AWS_REGION)
    return _dynamo

def get_table():
    return get_dynamo().Table(DYNAMO_TABLE)

# ── Trader process ─────────────────────────────────────────────────────────────
_trader_proc: Optional[subprocess.Popen] = None

# ── Helpers ────────────────────────────────────────────────────────────────────

def read_state() -> dict:
    """Read the live state.json written by live_trader.py."""
    try:
        if STATE_FILE.exists():
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception as e:
        log.warning("read_state error: %s", e)
    return {}


def _decimal_to_float(obj):
    """DynamoDB returns Decimal — convert recursively for JSON serialisation."""
    from decimal import Decimal
    if isinstance(obj, dict):
        return {k: _decimal_to_float(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_decimal_to_float(i) for i in obj]
    if isinstance(obj, Decimal):
        return float(obj)
    return obj


def fetch_trades(date_str: Optional[str] = None) -> list[dict]:
    """
    Query DynamoDB for trades.
    date_str → single day query; None → scan last 30 days.
    """
    table = get_table()
    try:
        if date_str:
            resp  = table.query(
                KeyConditionExpression="trade_date = :d",
                ExpressionAttributeValues={":d": date_str},
                ScanIndexForward=False,
            )
            items = resp.get("Items", [])
        else:
            from boto3.dynamodb.conditions import Attr
            cutoff = (datetime.now(IST) - timedelta(days=30)).strftime("%Y-%m-%d")
            resp   = table.scan(FilterExpression=Attr("trade_date").gte(cutoff))
            items  = resp.get("Items", [])
            while "LastEvaluatedKey" in resp:
                resp   = table.scan(
                    FilterExpression=Attr("trade_date").gte(cutoff),
                    ExclusiveStartKey=resp["LastEvaluatedKey"],
                )
                items.extend(resp.get("Items", []))

        items = _decimal_to_float(items)
        items.sort(key=lambda x: x.get("entry_ts", ""), reverse=True)
        return items
    except ClientError as e:
        log.error("DynamoDB error: %s", e)
        return []
    except Exception as e:
        log.error("fetch_trades error: %s", e)
        return []

# ── Response models ────────────────────────────────────────────────────────────

class StateResponse(BaseModel):
    is_running:   bool
    updated_at:   Optional[str]  = None
    fut_symbol:   Optional[str]  = None
    fut_ltp:      Optional[float]= None
    vwap:         Optional[float]= None
    rsi14:        Optional[float]= None
    prev_rsi:     Optional[float]= None
    trades_today: int   = 0
    daily_pnl:    float = 0.0
    position:     Optional[dict] = None
    today_trades: list  = []
    trader_pid:   Optional[int]  = None


class ActionResponse(BaseModel):
    success: bool
    message: str

# ── Routes ─────────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.now(IST).isoformat()}


@app.get("/state", response_model=StateResponse)
def get_state():
    s = read_state()
    global _trader_proc
    pid = None
    if _trader_proc and _trader_proc.poll() is None:
        pid = _trader_proc.pid
    return StateResponse(
        is_running   = s.get("is_running", False),
        updated_at   = s.get("updated_at"),
        fut_symbol   = s.get("fut_symbol"),
        fut_ltp      = s.get("fut_ltp"),
        vwap         = s.get("vwap"),
        rsi14        = s.get("rsi14"),
        prev_rsi     = s.get("prev_rsi"),
        trades_today = s.get("trades_today", 0),
        daily_pnl    = s.get("daily_pnl", 0.0),
        position     = s.get("position"),
        today_trades = s.get("today_trades", []),
        trader_pid   = pid,
    )


@app.get("/trades")
def get_all_trades():
    return {"trades": fetch_trades()}


@app.get("/trades/{date_str}")
def get_trades_by_date(date_str: str):
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(400, "Date must be YYYY-MM-DD")
    return {"trades": fetch_trades(date_str)}


def _stream_trader_logs(proc: subprocess.Popen) -> None:
    """Forward live_trader stdout/stderr into the backend logger."""
    import threading
    def _read():
        for line in proc.stdout:
            line = line.rstrip()
            if line:
                log.info("[trader] %s", line)
    threading.Thread(target=_read, name="TraderLogReader", daemon=True).start()


@app.post("/start", response_model=ActionResponse)
def start_trader():
    global _trader_proc
    if _trader_proc and _trader_proc.poll() is None:
        return ActionResponse(success=False, message="Trader is already running")
    try:
        _trader_proc = subprocess.Popen(
            [sys.executable, "-m", "backend.live_trader"],
            cwd=str(PROJECT_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        _stream_trader_logs(_trader_proc)
        log.info("Trader started: PID=%d", _trader_proc.pid)
        return ActionResponse(success=True, message=f"Trader started (PID {_trader_proc.pid})")
    except Exception as e:
        return ActionResponse(success=False, message=str(e))


@app.post("/stop", response_model=ActionResponse)
def stop_trader():
    global _trader_proc
    if _trader_proc is None or _trader_proc.poll() is not None:
        return ActionResponse(success=False, message="Trader is not running")
    try:
        _trader_proc.send_signal(signal.SIGINT)
        _trader_proc.wait(timeout=10)
        log.info("Trader stopped.")
        return ActionResponse(success=True, message="Trader stopped")
    except subprocess.TimeoutExpired:
        _trader_proc.kill()
        return ActionResponse(success=True, message="Trader force-killed (timeout)")
    except Exception as e:
        return ActionResponse(success=False, message=str(e))


# ── WebSocket ──────────────────────────────────────────────────────────────────

@app.websocket("/ws")
async def websocket_state(websocket: WebSocket):
    await websocket.accept()
    log.info("WS connected: %s", websocket.client)
    try:
        while True:
            state = read_state()
            global _trader_proc
            state["api_trader_running"] = bool(
                _trader_proc and _trader_proc.poll() is None
            )
            await websocket.send_json(state)
            await asyncio.sleep(0.5)
    except WebSocketDisconnect:
        log.info("WS disconnected")
    except Exception as e:
        log.warning("WS error: %s", e)


# ── DynamoDB bootstrap ─────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    """Ensure DynamoDB table vwap_rsi_trades exists (idempotent)."""
    try:
        client = boto3.client("dynamodb", region_name=AWS_REGION)
        client.describe_table(TableName=DYNAMO_TABLE)
        log.info("DynamoDB table '%s' exists.", DYNAMO_TABLE)
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            log.info("Creating DynamoDB table '%s'...", DYNAMO_TABLE)
            client.create_table(
                TableName=DYNAMO_TABLE,
                KeySchema=[
                    {"AttributeName": "trade_date", "KeyType": "HASH"},
                    {"AttributeName": "entry_ts",   "KeyType": "RANGE"},
                ],
                AttributeDefinitions=[
                    {"AttributeName": "trade_date", "AttributeType": "S"},
                    {"AttributeName": "entry_ts",   "AttributeType": "S"},
                ],
                BillingMode="PAY_PER_REQUEST",
            )
            log.info("Table created.")
        else:
            log.error("DynamoDB startup error: %s", e)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("backend.api:app", host="0.0.0.0", port=8056, reload=True)
