import asyncio
from datetime import date, datetime
from typing import Dict, List, Optional

from sqlalchemy import select, func, update, insert, delete, case, cast, Date
from sqlalchemy.ext.asyncio import AsyncSession

from database import AsyncSessionLocal
from models import DailyState, Trade, Position, AuditLog
from config import settings


class RiskManager:
    """Handles dynamic capital calculations and persists daily trading state
    using Supabase PostgreSQL so the bot survives crashes."""

    async def _get_session(self) -> AsyncSession:
        """Return a fresh async session per operation to avoid aborted transactions."""
        return AsyncSessionLocal()

    async def close(self) -> None:
        pass

    async def _ensure_daily_state(self, session: AsyncSession, today: str) -> DailyState:
        """Fetch or create today's daily state row."""
        result = await session.execute(
            select(DailyState).where(DailyState.date == today)
        )
        state = result.scalar_one_or_none()
        if state is None:
            state = DailyState(
                date=today,
                daily_pnl=0.0,
                peak_pnl=0.0,
                consecutive_losses=0,
                is_locked=False,
                total_wins=0,
                total_losses=0,
            )
            session.add(state)
            await session.commit()
            await session.refresh(state)
        return state

    def fetch_balance(self) -> float:
        """Mock balance fetch. Will be connected to the live API later."""
        return 10000.0

    def calculate_limits(self, balance: float, params: dict | None = None) -> Dict[str, float]:
        """Return risk-based capital limits derived from the account balance."""
        p = params or {
            "daily_loss_limit_pct": 2.0,
            "max_consecutive_losses": 3,
            "single_trade_risk_pct": 1.0,
            "max_positions": 2,
            "lot_size": 0.01,
            "sl_pips": 150,
            "tp_pips": 300,
        }
        target_usd = round(balance * p.get("target_pct", 0.02), 2)
        max_loss_usd = round(balance * p.get("daily_loss_limit_pct", 2.0) / 100, 2)
        risk_per_trade_usd = round(balance * p.get("single_trade_risk_pct", 1.0) / 100, 2)
        sl_pips = p.get("sl_pips", 150)
        tp_pips = p.get("tp_pips", 300)

        lot_size = p.get("lot_size", 0.01)

        return {
            "target_usd": target_usd,
            "max_loss_usd": max_loss_usd,
            "risk_per_trade_usd": risk_per_trade_usd,
            "lot_size": lot_size,
            "sl_pips": sl_pips,
            "tp_pips": tp_pips,
        }

    async def get_state(self) -> Dict:
        """Fetch the current day's state from Supabase. Resets if it is a new day."""
        today = date.today().isoformat()
        session = await self._get_session()
        try:
            state = await self._ensure_daily_state(session, today)
            return {
                "date": today,
                "daily_pnl": state.daily_pnl,
                "peak_pnl": state.peak_pnl,
                "consecutive_losses": state.consecutive_losses,
                "is_locked": state.is_locked,
            }
        finally:
            await session.close()

    async def can_trade(self, balance: float, params: dict | None = None) -> bool:
        """Determine whether a new trade is allowed based on risk limits.
        Harvests profit every PROFIT_WITHDRAW_THRESHOLD ($100) and continues trading."""
        state = await self.get_state()
        p = params or {
            "max_consecutive_losses": 3,
            "daily_loss_limit_pct": 2.0,
        }
        limits = self.calculate_limits(balance, p)

        if state["is_locked"]:
            return False

        if state["daily_pnl"] <= -limits["max_loss_usd"]:
            return False

        # Consecutive losses check
        if state["consecutive_losses"] >= p.get("max_consecutive_losses", 3):
            return False

        # Profit harvesting: every $100 profit, "withdraw" it and keep trading
        threshold = settings.PROFIT_WITHDRAW_THRESHOLD
        if state["daily_pnl"] >= threshold:
            session = await self._get_session()
            try:
                today = date.today().isoformat()
                db_state = await self._ensure_daily_state(session, today)
                harvested = int(db_state.daily_pnl // threshold)
                if harvested > 0:
                    remaining = round(db_state.daily_pnl - (harvested * threshold), 2)
                    db_state.daily_pnl = remaining
                    if db_state.peak_pnl > remaining:
                        db_state.peak_pnl = remaining
                    await session.commit()
                    await self.audit_log(
                        event_type="PROFIT_WITHDRAWN",
                        message=f"Withdrew ${harvested * threshold:.2f} profit ({harvested} x $100), remaining ${remaining:.2f}",
                        level="INFO",
                        meta_data=f'{{"harvested": {harvested * threshold:.2f}, "remaining": {remaining:.2f}}}',
                    )
            finally:
                await session.close()
            return True

        # Trailing lock: peak reached and given back 0.5% of balance
        if (
            state["peak_pnl"] >= limits["target_usd"]
            and state["daily_pnl"] <= (state["peak_pnl"] - (balance * 0.005))
        ):
            return False

        return True

    async def record_trade(
        self,
        pnl: float,
        balance: float,
        symbol: str = "GOLD",
        side: str = "",
        price: float = 0.0,
        size: float = 0.0,
        result: str = "",
    ) -> None:
        """Persist a trade result and update daily state / lock status."""
        session = await self._get_session()
        try:
            today = date.today().isoformat()
            now = datetime.now()

            state = await self._ensure_daily_state(session, today)
            limits = self.calculate_limits(balance)

            new_daily_pnl = round(state.daily_pnl + pnl, 2)
            new_peak_pnl = max(state.peak_pnl, new_daily_pnl)

            if pnl < 0:
                new_consecutive = state.consecutive_losses + 1
            elif pnl > 0:
                new_consecutive = 0
            else:
                new_consecutive = state.consecutive_losses

            is_locked = state.is_locked

            if new_daily_pnl <= -limits["max_loss_usd"]:
                is_locked = True

            if (
                new_peak_pnl >= limits["target_usd"]
                and new_daily_pnl <= (new_peak_pnl - (balance * 0.005))
            ):
                is_locked = True

            # Update daily state
            state.daily_pnl = new_daily_pnl
            state.peak_pnl = new_peak_pnl
            state.consecutive_losses = new_consecutive
            state.is_locked = is_locked
            if pnl > 0:
                state.total_wins += 1
            elif pnl < 0:
                state.total_losses += 1

            # Insert trade record
            trade = Trade(
                trade_id=str(int(now.timestamp() * 1000)),
                symbol=symbol,
                side=side,
                price=price,
                size=size,
                sl_price=None,
                tp_price=None,
                pnl=round(pnl, 2),
                result=result if result else ("WIN" if pnl > 0 else "LOSS" if pnl < 0 else "OPEN"),
                broker_ticket=None,
                broker_state=None,
                client_order_id=None,
                opened_at=now,
                closed_at=now if result else None,
            )
            session.add(trade)
            await session.commit()
        finally:
            await session.close()

    async def get_daily_report(self, report_date: str | None = None) -> Dict:
        """Get P&L summary for a specific date (defaults to today)."""
        if report_date is None:
            report_date = date.today().isoformat()

        session = await self._get_session()
        try:
            result = await session.execute(
                select(DailyState).where(DailyState.date == report_date)
            )
            row = result.scalar_one_or_none()

            if row is None:
                return {
                    "date": report_date,
                    "daily_pnl": 0.0,
                    "peak_pnl": 0.0,
                    "consecutive_losses": 0,
                    "is_locked": False,
                    "trade_count": 0,
                    "wins": 0,
                    "losses": 0,
                    "gross_pnl": 0.0,
                }

            trades_result = await session.execute(
                select(
                    func.count(Trade.id),
                    func.sum(case((Trade.pnl > 0, 1), else_=0)),
                    func.sum(case((Trade.pnl < 0, 1), else_=0)),
                    func.coalesce(func.sum(Trade.pnl), 0),
                ).where(func.to_char(Trade.opened_at, 'YYYY-MM-DD') == report_date)
            )
            trades = trades_result.one()

            return {
                "date": report_date,
                "daily_pnl": row.daily_pnl,
                "peak_pnl": row.peak_pnl,
                "consecutive_losses": row.consecutive_losses,
                "is_locked": row.is_locked,
                "trade_count": trades[0] or 0,
                "wins": trades[1] or 0,
                "losses": trades[2] or 0,
                "gross_pnl": round(trades[3] or 0.0, 2),
            }
        finally:
            await session.close()

    async def get_hourly_report(
        self, report_date: str | None = None, hour: int | None = None
    ) -> Dict:
        """Get P&L summary for a specific hour (defaults to current hour today)."""
        if report_date is None:
            report_date = date.today().isoformat()
        if hour is None:
            hour = datetime.now().hour

        session = await self._get_session()
        try:
            hour_prefix = f"{report_date} {hour:02d}:"
            trades_result = await session.execute(
                select(
                    func.count(Trade.id),
                    func.coalesce(func.sum(Trade.pnl), 0),
                    func.sum(case((Trade.pnl > 0, 1), else_=0)),
                    func.sum(case((Trade.pnl < 0, 1), else_=0)),
                ).where(func.to_char(Trade.opened_at, 'YYYY-MM-DD HH24:') == hour_prefix)
            )
            trades = trades_result.one()

            return {
                "date": report_date,
                "hour": hour,
                "trade_count": trades[0] or 0,
                "gross_pnl": round(trades[1] or 0.0, 2),
                "wins": trades[2] or 0,
                "losses": trades[3] or 0,
            }
        finally:
            await session.close()


if __name__ == "__main__":

    async def main() -> None:
        rm = RiskManager()

        balance = rm.fetch_balance()
        print(f"Balance: ${balance}")

        limits = rm.calculate_limits(balance)
        print(f"Limits: {limits}")

        state = await rm.get_state()
        print(f"Initial state: {state}")

        print(f"Can trade (fresh): {await rm.can_trade(balance)}")

        # Simulate a winning trade
        await rm.record_trade(150.0, balance=balance, symbol="EURUSD", side="BUY", price=1.10, size=0.1, result="WIN")
        state = await rm.get_state()
        print(f"After +150: {state}")
        print(f"Can trade: {await rm.can_trade(balance)}")

        # Simulate losses to hit max loss limit
        await rm.record_trade(-120.0, balance=balance, symbol="EURUSD", side="SELL", price=1.09, size=0.1, result="LOSS")
        state = await rm.get_state()
        print(f"After -120: {state}")
        print(f"Can trade: {await rm.can_trade(balance)}")

        await rm.close()
        print("Test complete. Database saved to Supabase.")

    async def reconcile_positions(self, xm_client) -> List[Dict]:
        """Reconcile local positions with broker's open positions."""
        session = await self._get_session()
        try:
            # Fetch open positions from broker
            broker_positions = await asyncio.get_event_loop().run_in_executor(
                None, xm_client.get_open_positions
            )
            
            if not broker_positions:
                # Mark all local positions as inactive
                await session.execute(
                    update(Position).where(Position.is_active == True).values(is_active=False)
                )
                await session.commit()
                return []
            
            reconciled = []
            broker_tickets = set()
            
            for bp in broker_positions:
                ticket = bp.get("ticket") or bp.get("id") or bp.get("broker_ticket")
                if ticket is None:
                    continue
                broker_tickets.add(int(ticket))
                
                symbol = bp.get("symbol", "")
                side = bp.get("type", bp.get("side", "BUY"))
                volume = float(bp.get("volume", bp.get("lots", 0)))
                open_price = float(bp.get("open_price", bp.get("price", 0)))
                current_price = float(bp.get("current_price", bp.get("price", open_price)))
                profit = float(bp.get("profit", 0))
                sl = bp.get("sl")
                tp = bp.get("tp")
                
                # Check if position exists locally
                result = await session.execute(
                    select(Position).where(Position.broker_ticket == int(ticket))
                )
                local_pos = result.scalar_one_or_none()
                
                if local_pos:
                    # Update existing position
                    local_pos.current_price = current_price
                    local_pos.profit = profit
                    local_pos.sl = sl
                    local_pos.tp = tp
                    local_pos.is_active = True
                    local_pos.updated_at = datetime.now()
                else:
                    # Create new position
                    local_pos = Position(
                        broker_ticket=int(ticket),
                        symbol=symbol,
                        side=side,
                        volume=volume,
                        open_price=open_price,
                        current_price=current_price,
                        sl=sl,
                        tp=tp,
                        profit=profit,
                        is_active=True,
                    )
                    session.add(local_pos)
                
                reconciled.append({
                    "broker_ticket": int(ticket),
                    "symbol": symbol,
                    "side": side,
                    "volume": volume,
                    "open_price": open_price,
                    "current_price": current_price,
                    "profit": profit,
                    "is_active": True,
                })
            
            # Mark positions not in broker as inactive
            await session.execute(
                update(Position)
                .where(Position.broker_ticket.not_in(list(broker_tickets)))
                .where(Position.is_active == True)
                .values(is_active=False)
            )
            
            await session.commit()
            return reconciled
        finally:
            await session.close()

    async def audit_log(
        self,
        event_type: str,
        message: str,
        symbol: str | None = None,
        level: str = "INFO",
        meta_data: str | None = None,
    ) -> None:
        """Write an audit log entry."""
        session = await self._get_session()
        try:
            entry = AuditLog(
                event_type=event_type,
                message=message,
                symbol=symbol,
                level=level,
                meta_data=meta_data,
            )
            session.add(entry)
            await session.commit()
        finally:
            await session.close()


if __name__ == "__main__":

    async def main() -> None:
        rm = RiskManager()

        balance = rm.fetch_balance()
        print(f"Balance: ${balance}")

        limits = rm.calculate_limits(balance)
        print(f"Limits: {limits}")

        state = await rm.get_state()
        print(f"Initial state: {state}")

        print(f"Can trade (fresh): {await rm.can_trade(balance)}")

        # Simulate a winning trade
        await rm.record_trade(150.0, balance=balance, symbol="EURUSD", side="BUY", price=1.10, size=0.1, result="WIN")
        await rm.audit_log("TRADE_EXECUTED", "Winning trade recorded", symbol="EURUSD", level="INFO", meta_data='{"pnl": 150.0}')
        state = await rm.get_state()
        print(f"After +150: {state}")
        print(f"Can trade: {await rm.can_trade(balance)}")

        # Simulate losses to hit max loss limit
        await rm.record_trade(-120.0, balance=balance, symbol="EURUSD", side="SELL", price=1.09, size=0.1, result="LOSS")
        await rm.audit_log("TRADE_EXECUTED", "Losing trade recorded", symbol="EURUSD", level="WARN", meta_data='{"pnl": -120.0}')
        state = await rm.get_state()
        print(f"After -120: {state}")
        print(f"Can trade: {await rm.can_trade(balance)}")

        await rm.close()
        print("Test complete. Database saved to Supabase.")

    asyncio.run(main())

