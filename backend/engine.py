import asyncio
import logging
import os
import random
from datetime import datetime

from risk_manager import RiskManager
from xm_api import XM_API

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("engine")

SYMBOL = "GOLD"
LOOP_INTERVAL = 5.0
TRADE_SIMULATION_DELAY = 5.0


class TradingEngine:
    """Orchestrates RiskManager and XM_API in a continuous async loop."""

    def __init__(self) -> None:
        api_key = os.environ.get("MTAPI_KEY", "YOUR_API_KEY_HERE")
        account_id = os.environ.get("MTAPI_ACCOUNT_ID", "YOUR_ACCOUNT_ID_HERE")
        self.xm = XM_API(api_key=api_key, account_id=account_id)
        self.risk = RiskManager()
        self.running = False

    async def _run_sync(self, func, *args):
        """Run a blocking function in a thread executor so the event loop isn't blocked."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, func, *args)

    async def start(self) -> None:
        self.running = True
        logger.info("=" * 60)
        logger.info("   ScalpAI Master Engine Started")
        logger.info("=" * 60)
        logger.info(f"Symbol: {SYMBOL}")

        # Initialize balance from XM_API (fall back to mock if API unavailable)
        balance = await self._initialize_balance()
        if balance is None:
            logger.warning("API unavailable — using mock balance from RiskManager.")
            balance = self.risk.fetch_balance()

        limits = self.risk.calculate_limits(balance)
        logger.info(
            f"Balance: ${balance:,.2f} | "
            f"Target: ${limits['target_usd']:,.2f} | "
            f"Max Loss: ${limits['max_loss_usd']:,.2f} | "
            f"Risk/Trade: ${limits['risk_per_trade_usd']:,.2f} | "
            f"Lot Size: {limits['lot_size']} | "
            f"SL: {limits['sl_pips']} pips | TP: {limits['tp_pips']} pips"
        )
        logger.info("-" * 60)

        while self.running:
            try:
                await self._tick(balance)
            except Exception as exc:
                logger.error(f"Unexpected error in engine loop: {exc}")

            await asyncio.sleep(LOOP_INTERVAL)

    async def _initialize_balance(self) -> float | None:
        try:
            summary = await self._run_sync(self.xm.get_account_summary)
            if summary:
                return float(summary["balance"])
        except Exception as exc:
            logger.warning(f"Could not fetch balance from API: {exc}")
        return None

    async def _tick(self, balance: float) -> None:
        state = self.risk.get_state()
        limits = self.risk.calculate_limits(balance)

        # --- Risk check ---
        can_trade = self.risk.can_trade(balance)
        if not can_trade:
            reasons = []
            if state["is_locked"]:
                reasons.append("account locked")
            if state["daily_pnl"] <= -limits["max_loss_usd"]:
                reasons.append("max daily loss reached")
            if (
                state["peak_pnl"] >= limits["target_usd"]
                and state["daily_pnl"] <= (state["peak_pnl"] - (balance * 0.005))
            ):
                reasons.append("trailing lock hit")
            reason_str = ", ".join(reasons) if reasons else "risk limit"
            logger.info(
                f"SKIP — {reason_str} | Daily PnL: ${state['daily_pnl']:,.2f} | "
                f"Peak: ${state['peak_pnl']:,.2f} | Locked: {state['is_locked']}"
            )
            return

        # --- Fetch live quote ---
        quote = await self._run_sync(self.xm.get_live_quote, SYMBOL)
        if not quote:
            logger.warning("Failed to fetch live quote. Skipping tick.")
            return

        price = round((quote["bid"] + quote["ask"]) / 2, 2)
        logger.info(
            f"TICK — {SYMBOL} @ {price:.2f} | "
            f"Bid: {quote['bid']:.2f} | Ask: {quote['ask']:.2f} | "
            f"Daily PnL: ${state['daily_pnl']:,.2f}"
        )

        # --- Mock signal: BUY if price cents even, SELL if odd ---
        price_cents = int(round(price * 100)) % 100
        side = "BUY" if price_cents % 2 == 0 else "SELL"
        logger.info(f"SIGNAL — {side} (price cents: {price_cents})")

        # --- Calculate SL / TP prices ---
        point = 0.01  # 1 pip for GOLD
        if side == "BUY":
            sl_price = round(price - limits["sl_pips"] * point, 2)
            tp_price = round(price + limits["tp_pips"] * point, 2)
        else:
            sl_price = round(price + limits["sl_pips"] * point, 2)
            tp_price = round(price - limits["tp_pips"] * point, 2)

        logger.info(
            f"ORDER — {side} {limits['lot_size']} lots | "
            f"Entry: {price:.2f} | SL: {sl_price:.2f} | TP: {tp_price:.2f}"
        )

        # --- Send order via XM_API ---
        order_result = await self._run_sync(
            self.xm.send_order, SYMBOL, side, limits["lot_size"],
            limits["sl_pips"], limits["tp_pips"],
        )

        if not order_result:
            logger.error("ORDER FAILED — no response from XM_API.")
            return

        logger.info("ORDER ACCEPTED — simulating trade close in 5s...")

        # --- Simulate trade close after delay ---
        await asyncio.sleep(TRADE_SIMULATION_DELAY)

        # Mock P&L: 60% win rate
        is_win = random.random() < 0.60
        pnl = round(random.uniform(5.0, 25.0), 2) if is_win else round(random.uniform(-15.0, -5.0), 2)
        result_str = "WIN" if pnl > 0 else "LOSS"
        logger.info(f"CLOSED — {result_str} | P&L: ${pnl:,.2f}")

        # --- Record trade in RiskManager ---
        self.risk.record_trade(pnl)

        new_state = self.risk.get_state()
        logger.info(
            f"STATE — Daily PnL: ${new_state['daily_pnl']:,.2f} | "
            f"Peak: ${new_state['peak_pnl']:,.2f} | "
            f"Consecutive Losses: {new_state['consecutive_losses']} | "
            f"Locked: {new_state['is_locked']}"
        )
        logger.info("-" * 60)

    def stop(self) -> None:
        self.running = False
        self.risk.close()
        logger.info("Engine stopped.")


async def main() -> None:
    engine = TradingEngine()
    try:
        await engine.start()
    except KeyboardInterrupt:
        engine.stop()
        logger.info("Shutdown complete.")


if __name__ == "__main__":
    asyncio.run(main())
