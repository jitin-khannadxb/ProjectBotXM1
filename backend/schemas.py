from datetime import date, datetime
from enum import Enum as PyEnum
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class TradeSide(str, PyEnum):
    BUY = "BUY"
    SELL = "SELL"


class TradeResult(str, PyEnum):
    WIN = "WIN"
    LOSS = "LOSS"
    OPEN = "OPEN"
    PENDING = "PENDING"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"


class AccountType(str, PyEnum):
    DEMO = "demo"
    LIVE = "live"


class EngineControlRequest(BaseModel):
    symbol: Optional[str] = Field(
        default="EURUSD",
        min_length=1,
        max_length=20,
        pattern=r"^[A-Z]{3,6}$",
    )


class EngineControlResponse(BaseModel):
    status: str
    message: str
    timestamp: str


class RiskResponse(BaseModel):
    balance: float
    equity: float
    day_pnl: float
    daily_loss_used: float
    consecutive_losses: int
    open_positions: int
    win_rate: float
    target_usd: float
    max_loss_usd: float
    is_locked: bool
    user_role: Optional[str] = None


class DailyReportResponse(BaseModel):
    date: str
    daily_pnl: float
    peak_pnl: float
    consecutive_losses: int
    is_locked: bool
    trade_count: int
    wins: int
    losses: int
    gross_pnl: float


class HourlyReportResponse(BaseModel):
    date: str
    hour: int
    trade_count: int
    gross_pnl: float
    wins: int
    losses: int


class HealthResponse(BaseModel):
    status: str
    database: str
    engine_running: bool
    symbol: str


class ErrorResponse(BaseModel):
    detail: str
    error_code: str
    timestamp: str


class SymbolInfo(BaseModel):
    symbol: str
    pip_size: float
    min_lot: float = 0.01
    max_lot: float = 100.0


class SymbolsResponse(BaseModel):
    symbols: list[SymbolInfo]
    count: int


class TradeRecord(BaseModel):
    trade_id: str
    symbol: str
    side: TradeSide
    price: float
    size: float
    pnl: Optional[float] = None
    result: TradeResult
    opened_at: datetime
    closed_at: Optional[datetime] = None


class PositionRecord(BaseModel):
    broker_ticket: int
    symbol: str
    side: TradeSide
    volume: float
    open_price: float
    current_price: float
    profit: float
    is_active: bool
    opened_at: datetime


class AccountSnapshot(BaseModel):
    balance: float
    equity: float
    margin: Optional[float] = None
    free_margin: Optional[float] = None
    currency: str = "USD"
    account_type: AccountType = AccountType.DEMO


# --- Validation helpers ---

def validate_symbol(symbol: str, valid_symbols: list[str]) -> str:
    """Validate that a symbol is in the configured list."""
    if symbol not in valid_symbols:
        raise ValueError(f"Symbol '{symbol}' not in configured symbols: {valid_symbols}")
    return symbol


def validate_price(price: float, field_name: str = "price") -> float:
    """Validate price is positive."""
    if price <= 0:
        raise ValueError(f"{field_name} must be positive, got {price}")
    return price


def validate_lot_size(lot_size: float) -> float:
    """Validate lot size is within acceptable range."""
    if lot_size <= 0 or lot_size > 100:
        raise ValueError(f"Lot size must be between 0 and 100, got {lot_size}")
    return round(lot_size, 2)
