import asyncio
import json
import logging
import os
import random
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Dict, List, Set, Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

from risk_manager import RiskManager
from xm_api import XM_API
from cache import cache

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("server")

SYMBOL = "EURUSD"
LOOP_INTERVAL = 5.0
TRADE_SIMULATION_DELAY = 5.0

# ---------------------------------------------------------------------------
# Shared engine state
# ---------------------------------------------------------------------------
is_running = False
kill_switch_active = False
current_price = 2341.28
DEMO_MODE = False  # Set to True for testing without real API keys

# ---------------------------------------------------------------------------
# WebSocket connection manager
# ---------------------------------------------------------------------------


class ConnectionManager:
    def __init__(self) -> None:
        self.active_connections: Dict[str, Set[WebSocket]] = {
            "prices": set(),
            "risk": set(),
            "trades": set(),
            "logs": set(),
            "book": set(),
            "signals": set(),
        }

    async def connect(self, websocket: WebSocket, channel: str):
        if channel not in self.active_connections:
            await websocket.close()
            return
        await websocket.accept()
        self.active_connections[channel].add(websocket)

    def disconnect(self, websocket: WebSocket, channel: str):
        self.active_connections[channel].discard(websocket)

    async def broadcast(self, channel: str, message: dict):
        if channel not in self.active_connections:
            return
        payload = json.dumps(message)
        dead: Set[WebSocket] = set()
        for connection in list(self.active_connections[channel]):
            try:
                await connection.send_text(payload)
            except Exception:
                dead.add(connection)
        for conn in dead:
            self.active_connections[channel].discard(conn)


manager = ConnectionManager()

# ---------------------------------------------------------------------------
# API + Risk instances
# ---------------------------------------------------------------------------

api_key = os.environ.get("MTAPI_KEY", "YOUR_API_KEY_HERE")
account_id = os.environ.get("MTAPI_ACCOUNT_ID", "YOUR_ACCOUNT_ID_HERE")
xm = XM_API(api_key=api_key, account_id=account_id)
risk = RiskManager()

account_balance = 10000.0
risk_limits = risk.calculate_limits(account_balance)


# ---------------------------------------------------------------------------
# Engine loop
# ---------------------------------------------------------------------------


async def _run_sync(func, *args):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, func, *args)


async def engine_loop():
    """Background loop: risk check -> quote -> signal -> order -> P&L -> broadcast."""
    global is_running, kill_switch_active, current_price, account_balance, risk_limits

    logger.info(
        f"Engine loop started | DEMO_MODE={DEMO_MODE} | Symbol: {SYMBOL}"
    )

    while True:
        try:
            # Refresh balance periodically (skip API in demo mode)
            if not DEMO_MODE:
                summary = await _run_sync(xm.get_account_summary)
                if summary:
                    account_balance = float(summary["balance"])
                    risk_limits = risk.calculate_limits(account_balance)

            state = await risk.get_state()

            if not is_running or kill_switch_active:
                await asyncio.sleep(LOOP_INTERVAL)
                continue

            can_trade = await risk.can_trade(account_balance)
            if not can_trade:
                reasons = []
                if state["is_locked"]:
                    reasons.append("account locked")
                if state["daily_pnl"] <= -risk_limits["max_loss_usd"]:
                    reasons.append("max daily loss reached")
                if (
                    state["peak_pnl"] >= risk_limits["target_usd"]
                    and state["daily_pnl"] <= (state["peak_pnl"] - (account_balance * 0.005))
                ):
                    reasons.append("trailing lock hit")
                reason_str = ", ".join(reasons) if reasons else "risk limit"

                await manager.broadcast("logs", {
                    "msg": f"SKIP — {reason_str}",
                    "type": "warn",
                    "time": datetime.now().strftime("%H:%M:%S"),
                })
                await manager.broadcast("risk", await _build_risk_payload())
                await asyncio.sleep(LOOP_INTERVAL)
                continue

            # Fetch live quote
            if DEMO_MODE:
                noise = (random.random() - 0.5) * 0.5
                price = round(2340.50 + noise, 2)
                quote = {"bid": round(price - 0.06, 2), "ask": round(price + 0.06, 2)}
                current_price = price
            else:
                quote = await _run_sync(xm.get_live_quote, SYMBOL)
                if not quote:
                    await manager.broadcast("logs", {
                        "msg": "Failed to fetch live quote",
                        "type": "error",
                        "time": datetime.now().strftime("%H:%M:%S"),
                    })
                    await asyncio.sleep(LOOP_INTERVAL)
                    continue

                price = round((quote["bid"] + quote["ask"]) / 2, 2)
                current_price = price

            # Broadcast live price
            change = "up" if random.random() > 0.5 else "down"
            await manager.broadcast("prices", {
                "price": price,
                "change": change,
                "time": datetime.now().strftime("%H:%M:%S"),
            })

            # Mock signal: BUY if price cents even, SELL if odd
            price_cents = int(round(price * 100)) % 100
            side = "BUY" if price_cents % 2 == 0 else "SELL"

            # Calculate SL / TP prices (absolute prices for the API)
            # Pip size per symbol: GOLD = 0.1, EURUSD = 0.0001
            pip_size = 0.1 if SYMBOL == "GOLD" else 0.0001
            if side == "BUY":
                sl_price = round(price - risk_limits["sl_pips"] * pip_size, 2)
                tp_price = round(price + risk_limits["tp_pips"] * pip_size, 2)
            else:
                sl_price = round(price + risk_limits["sl_pips"] * pip_size, 2)
                tp_price = round(price - risk_limits["tp_pips"] * pip_size, 2)

            lot_size = risk_limits["lot_size"]
            logger.info(
                f"SIGNAL — {SYMBOL} {side} {lot_size} lots @ {price:.2f} | SL: {sl_price:.2f} | TP: {tp_price:.2f}"
            )

            # Broadcast signal to frontend signal overlay
            await manager.broadcast("signals", {
                "signal": side,
                "price": round(price, 2),
                "sl": sl_price,
                "tp": tp_price,
                "confidence": round(random.uniform(70, 95), 1),
                "time": datetime.now().strftime("%H:%M:%S"),
            })

            trade_id = int(datetime.now().timestamp())
            trade_time = datetime.now().strftime("%H:%M:%S")

            # Check if market is open for trading
            trade_session = await _run_sync(xm.get_trade_session, SYMBOL)
            if not trade_session:
                await manager.broadcast("logs", {
                    "msg": f"Market closed for {SYMBOL} — waiting for open",
                    "type": "warn",
                    "time": trade_time,
                })
                await asyncio.sleep(LOOP_INTERVAL)
                continue

            # Send order via XM_API (simulated fill in demo mode)
            if DEMO_MODE:
                order_result = {
                    "status": "filled",
                    "order_id": f"DEMO_{trade_id}",
                    "symbol": SYMBOL,
                    "side": side,
                    "volume": lot_size,
                    "price": price,
                    "time": trade_time,
                    "ticket": trade_id,
                    "state": 10009,
                }
                logger.info(f"[DEMO] Simulated order fill: {SYMBOL} {side} {lot_size} lots @ {price:.2f}")
            else:
                order_result = await _run_sync(
                    xm.send_order, SYMBOL, side, lot_size, sl_price, tp_price,
                )

            if not order_result:
                await manager.broadcast("logs", {
                    "msg": "ORDER FAILED — no response from XM_API",
                    "type": "error",
                    "time": trade_time,
                })
                await asyncio.sleep(LOOP_INTERVAL)
                continue

            # Check broker state: 10009 = done/filled
            order_state = order_result.get("state", 0)
            if order_state != 10009:
                await manager.broadcast("logs", {
                    "msg": f"ORDER REJECTED — broker state {order_state}",
                    "type": "error",
                    "time": trade_time,
                })
                await asyncio.sleep(LOOP_INTERVAL)
                continue

            order_ticket = order_result.get("ticket", trade_id)

            # Broadcast trade open
            await manager.broadcast("trades", {
                "id": trade_id,
                "time": trade_time,
                "symbol": SYMBOL,
                "side": side,
                "price": price,
                "size": lot_size,
                "pnl": None,
                "result": "OPEN",
            })
            await manager.broadcast("logs", {
                "msg": f"ORDER ACCEPTED — {SYMBOL} {side} {lot_size} lots @ {price:.2f}",
                "type": "success",
                "time": trade_time,
            })

            # Close the trade after delay (simulated P&L in demo, real close in live)
            await asyncio.sleep(TRADE_SIMULATION_DELAY)

            if DEMO_MODE:
                # Simulate P&L for demo mode
                is_win = random.random() < 0.60
                pnl = round(random.uniform(5.0, 25.0), 2) if is_win else round(random.uniform(-15.0, -5.0), 2)
                result_str = "WIN" if pnl > 0 else "LOSS"
                logger.info(f"[DEMO] Simulated close: {result_str} | P&L: ${pnl:,.2f}")
            else:
                # Real close via XM_API
                close_result = await _run_sync(xm.close_order, order_ticket)
                if close_result:
                    pnl = round(float(close_result.get("profit", 0.0)), 2)
                    result_str = "WIN" if pnl > 0 else "LOSS"
                    logger.info(f"REAL CLOSE — ticket {order_ticket}: {result_str} | P&L: ${pnl:,.2f}")
                else:
                    pnl = 0.0
                    result_str = "UNKNOWN"
                    logger.error(f"CLOSE FAILED — ticket {order_ticket}")

            await risk.record_trade(pnl, balance=account_balance, symbol=SYMBOL, side=side, price=price, size=lot_size, result=result_str)

            # Broadcast trade close
            await manager.broadcast("trades", {
                "id": order_ticket,
                "time": datetime.now().strftime("%H:%M:%S"),
                "symbol": SYMBOL,
                "side": side,
                "price": price,
                "size": lot_size,
                "pnl": pnl,
                "result": result_str,
            })

            # Broadcast updated risk state
            await manager.broadcast("risk", await _build_risk_payload())

            # Broadcast log
            await manager.broadcast("logs", {
                "msg": f"CLOSED — {SYMBOL} {result_str} | P&L: ${pnl:,.2f}",
                "type": "success" if pnl > 0 else "danger",
                "time": datetime.now().strftime("%H:%M:%S"),
            })

        except Exception as exc:
            logger.error(f"Engine loop error: {exc}")
            await manager.broadcast("logs", {
                "msg": f"Engine error: {exc}",
                "type": "error",
                "time": datetime.now().strftime("%H:%M:%S"),
            })

        await asyncio.sleep(LOOP_INTERVAL)


async def _build_risk_payload() -> dict:
    state = await risk.get_state()
    total = state.get("total_wins", 0) + state.get("total_losses", 0)
    win_rate = round((state.get("total_wins", 0) / total) * 100, 1) if total > 0 else 0.0
    return {
        "balance": round(account_balance, 2),
        "equity": round(account_balance + state["daily_pnl"], 2),
        "day_pnl": round(state["daily_pnl"], 2),
        "daily_loss_used": round(
            (abs(state["daily_pnl"]) / account_balance) * 100, 2
        ) if state["daily_pnl"] < 0 else 0.0,
        "consecutive_losses": state["consecutive_losses"],
        "open_positions": 0,
        "win_rate": win_rate,
        "target_usd": risk_limits["target_usd"],
        "max_loss_usd": risk_limits["max_loss_usd"],
        "is_locked": state["is_locked"],
    }


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    global account_balance, risk_limits

    # Initialize balance from XM API
    try:
        summary = await _run_sync(xm.get_account_summary)
        if summary:
            account_balance = float(summary["balance"])
        else:
            account_balance = risk.fetch_balance()
    except Exception:
        account_balance = risk.fetch_balance()

    risk_limits = risk.calculate_limits(account_balance)

    # Initialize Redis cache
    await cache.connect()

    logger.info(
        f"Server initialized | DEMO_MODE={DEMO_MODE} | Symbol: {SYMBOL} | "
        f"Balance: ${account_balance:,.2f} | Target: ${risk_limits['target_usd']:,.2f} | "
        f"Max Loss: ${risk_limits['max_loss_usd']:,.2f} | Lot Size: {risk_limits['lot_size']}"
    )

    task = asyncio.create_task(engine_loop())
    logger.info("Master server starting — engine loop in background.")
    yield
    task.cancel()
    await risk.close()
    await cache.disconnect()
    logger.info("Master server shutting down.")


app = FastAPI(lifespan=lifespan)

# CORS for production frontend (Vercel)
allowed_origins = os.environ.get("CORS_ORIGINS", "*")
if allowed_origins == "*":
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
else:
    origins = [o.strip() for o in allowed_origins.split(",") if o.strip()]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )


# --- REST API ---


@app.get("/api/status")
async def get_status():
    state = await risk.get_state()
    return {
        "is_running": is_running,
        "kill_switch": kill_switch_active,
        "is_locked": state["is_locked"],
        "daily_pnl": state["daily_pnl"],
        "symbol": SYMBOL,
    }


@app.get("/api/health")
async def health_check():
    from database import engine
    from sqlalchemy import text
    db_status = "unknown"
    try:
        async with asyncio.timeout(3):
            async with engine.begin() as conn:
                await conn.execute(text("SELECT 1"))
            db_status = "healthy"
    except Exception as exc:
        db_status = f"unhealthy: {type(exc).__name__}"
    return {
        "status": "ok" if db_status == "healthy" else "degraded",
        "database": db_status,
        "engine_running": is_running,
        "symbol": SYMBOL,
    }


@app.post("/api/engine/start")
async def start_engine():
    global is_running, kill_switch_active
    if kill_switch_active:
        raise HTTPException(status_code=400, detail="Kill switch is active. Reset required.")
    is_running = True
    await manager.broadcast("logs", {
        "msg": "ENGINE STARTED",
        "type": "success",
        "time": datetime.now().strftime("%H:%M:%S"),
    })
    return {"status": "started"}


@app.post("/api/engine/kill")
async def kill_engine():
    global is_running, kill_switch_active
    is_running = False
    kill_switch_active = True
    await manager.broadcast("logs", {
        "msg": "KILL SWITCH ACTIVATED",
        "type": "danger",
        "time": datetime.now().strftime("%H:%M:%S"),
    })
    return {"status": "killed"}


@app.post("/api/engine/reset")
async def reset_engine():
    global kill_switch_active
    kill_switch_active = False
    await manager.broadcast("logs", {
        "msg": "Kill switch reset. Ready to start.",
        "type": "info",
        "time": datetime.now().strftime("%H:%M:%S"),
    })
    return {"status": "reset"}


@app.get("/api/risk")
async def get_risk():
    return await _build_risk_payload()


@app.get("/api/report/daily")
async def get_daily_report(report_date: str | None = None):
    return await risk.get_daily_report(report_date)


@app.get("/api/report/hourly")
async def get_hourly_report(report_date: str | None = None, hour: int | None = None):
    return await risk.get_hourly_report(report_date, hour)


# --- WebSockets ---


@app.websocket("/ws/{channel}")
async def websocket_endpoint(websocket: WebSocket, channel: str):
    await manager.connect(websocket, channel)
    try:
        # Send initial state immediately
        if channel == "risk":
            await websocket.send_text(json.dumps(await _build_risk_payload()))
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket, channel)


# --- Serve frontend ---

app.mount("/", StaticFiles(directory="frontend", html=True), name="frontend")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
