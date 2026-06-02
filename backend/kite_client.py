"""
backend/kite_client.py
======================
Thin client for kite-auth-service (default: http://localhost:8050).
Replaces all TRADINGWORLD imports.

Provides:
  get_kite()                                    → KiteConnect ready to use
  build_instrument_cache(kite)                  → cache dict for option lookup
  resolve_nearest_expiry(cache, atm, direction) → instrument dict
  TickerClient                                  → WS proxy client (same interface as KiteTicker)
"""

import json
import logging
import os
import threading
from datetime import date

import requests
from kiteconnect import KiteConnect

KITE_AUTH_URL = os.environ.get("KITE_AUTH_URL", "http://localhost:8050").rstrip("/")
log = logging.getLogger("vwap_rsi.kite_client")


# ── Auth ──────────────────────────────────────────────────────────────────────

def get_kite() -> KiteConnect:
    """
    Fetch the current access token from kite-auth-service and return
    a ready KiteConnect instance. Raises RuntimeError if service is
    unreachable or not logged in.
    """
    try:
        resp = requests.get(f"{KITE_AUTH_URL}/token", timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        raise RuntimeError(
            f"kite-auth-service not reachable at {KITE_AUTH_URL}: {e}\n"
            f"Make sure kite-auth-service is running (start.bat or uvicorn api:app --port 8050)"
        ) from e

    if not data.get("is_logged_in"):
        raise RuntimeError(
            "kite-auth-service is not logged in. "
            f"POST {KITE_AUTH_URL}/login to trigger authentication."
        )

    kite = KiteConnect(api_key=data["api_key"])
    kite.set_access_token(data["access_token"])
    log.info("Kite session from kite-auth-service. token=...%s", data["access_token"][-8:])
    return kite


# ── Instrument cache ──────────────────────────────────────────────────────────

def build_instrument_cache(kite: KiteConnect) -> dict:
    """
    Download NFO instruments and build a lookup cache for NIFTY options.
    Returns a dict with _count, _by_expiry, _all keys.
    """
    instruments = kite.instruments("NFO")

    nifty_opts = [
        i for i in instruments
        if i.get("name") == "NIFTY"
        and i.get("instrument_type") in ("CE", "PE")
    ]

    by_expiry: dict[str, list] = {}
    for inst in nifty_opts:
        exp = str(inst["expiry"])
        by_expiry.setdefault(exp, []).append(inst)

    return {
        "_count":     len(nifty_opts),
        "_by_expiry": by_expiry,
        "_all":       nifty_opts,
    }


def resolve_nearest_expiry(cache: dict, atm: int, direction: str) -> dict | None:
    """
    Find the nearest-expiry NIFTY option at the given ATM strike.
    direction: "CE" or "PE"
    Returns the instrument dict or None.
    """
    today = str(date.today())
    expiries = sorted(e for e in cache["_by_expiry"] if e >= today)
    if not expiries:
        return None

    for expiry in expiries:
        candidates = [
            i for i in cache["_by_expiry"][expiry]
            if i.get("instrument_type") == direction
            and i.get("strike") == atm
        ]
        if candidates:
            return candidates[0]

    return None


# ── TickerClient ──────────────────────────────────────────────────────────────

class TickerClient:
    """
    WebSocket client for kite-auth-service WS /ticker endpoint.
    Mirrors the KiteTicker interface (subscribe, set_mode, close, callbacks)
    so live_trader.py needs zero interface changes.

    Requires: pip install websocket-client
    """

    MODE_LTP   = "ltp"
    MODE_QUOTE = "quote"
    MODE_FULL  = "full"

    def __init__(self, auth_url: str | None = None):
        base = (auth_url or KITE_AUTH_URL)
        ws_base = base.replace("https://", "wss://").replace("http://", "ws://")
        self._ws_url = f"{ws_base}/ticker"
        self._ws     = None          # websocket.WebSocketApp instance
        self._conn   = None          # the underlying connection used for send

        # Callbacks — same names as KiteTicker
        self.on_ticks      = None
        self.on_connect    = None
        self.on_close      = None
        self.on_error      = None
        self.on_reconnect  = None
        self.on_noreconnect = None

    def connect(self, threaded: bool = True):
        """Connect to kite-auth-service /ticker. threaded=True returns immediately."""
        import websocket  # websocket-client

        def _on_open(ws_conn):
            self._conn = ws_conn
            log.info("TickerClient connected → %s", self._ws_url)
            if self.on_connect:
                self.on_connect(self, None)

        def _on_message(ws_conn, raw):
            try:
                msg = json.loads(raw)
                # Only forward actual tick dicts (have instrument_token)
                if "instrument_token" in msg and self.on_ticks:
                    self.on_ticks(self, [msg])
            except Exception as e:
                log.debug("Tick parse error: %s", e)

        def _on_close(ws_conn, code, reason):
            log.warning("TickerClient disconnected: code=%s reason=%s", code, reason)
            if self.on_close:
                self.on_close(self, code, reason)

        def _on_error(ws_conn, error):
            log.error("TickerClient error: %s", error)
            if self.on_error:
                self.on_error(self, None, str(error))

        self._ws = websocket.WebSocketApp(
            self._ws_url,
            on_open=_on_open,
            on_message=_on_message,
            on_close=_on_close,
            on_error=_on_error,
        )

        if threaded:
            t = threading.Thread(
                target=self._ws.run_forever,
                kwargs={"reconnect": 5},
                name="TickerClientWS",
                daemon=True,
            )
            t.start()
        else:
            self._ws.run_forever(reconnect=5)

    def subscribe(self, tokens: list[int]):
        self._send({"action": "subscribe", "tokens": tokens, "mode": self.MODE_FULL})

    def set_mode(self, mode: str, tokens: list[int]):
        self._send({"action": "set_mode", "tokens": tokens, "mode": mode})

    def unsubscribe(self, tokens: list[int]):
        self._send({"action": "unsubscribe", "tokens": tokens})

    def close(self):
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass

    def _send(self, msg: dict):
        if self._conn:
            try:
                self._conn.send(json.dumps(msg))
            except Exception as e:
                log.warning("TickerClient send failed: %s", e)
        else:
            log.warning("TickerClient not connected yet — cannot send: %s", msg)
