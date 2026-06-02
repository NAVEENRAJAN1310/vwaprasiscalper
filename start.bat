@echo off
REM ── VWAP+RSI Scalper — Quick Start ──────────────────────────────────────────
REM Run this from D:\vwaprasiscalper to start both backend and frontend.
REM PREREQUISITE: kite-auth-service must already be running on port 8050
REM   cd D:\kite-auth-service && start.bat

echo Starting VWAP+RSI Scalper...
echo   Requires kite-auth-service on http://localhost:8050
echo.

REM Backend (FastAPI on port 8056)
echo [1/2] Starting FastAPI backend on port 8056...
start "VWAP Backend" cmd /k "cd /d D:\vwaprasiscalper && uvicorn backend.api:app --host 0.0.0.0 --port 8056 --reload"

timeout /t 2 /nobreak >nul

REM Frontend (Next.js on port 3000)
echo [2/2] Starting Next.js frontend on port 3000...
start "VWAP Frontend" cmd /k "cd /d D:\vwaprasiscalper\frontend && npm run dev"

echo.
echo Both services starting...
echo   Backend : http://localhost:8056/docs
echo   Frontend: http://localhost:3000
echo   Dashboard: http://localhost:3000/dashboard
echo.
echo To start the live trader, open http://localhost:3000/dashboard and click Start.
