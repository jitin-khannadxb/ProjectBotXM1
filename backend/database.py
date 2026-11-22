import ssl
import asyncio
import logging
from typing import AsyncGenerator, Optional
from urllib.parse import urlparse, urlunparse

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase

from config import settings

logger = logging.getLogger("server")


class Base(DeclarativeBase):
    pass


def _build_engine_url_and_connect_args(url_str: str) -> tuple:
    """Build engine URL (without query params) and connect args for asyncpg."""
    parsed = urlparse(url_str)

    # Remove query string - asyncpg doesn't accept sslmode as query param
    clean_url = urlunparse(
        (parsed.scheme, parsed.netloc, parsed.path, parsed.params, "", parsed.fragment)
    )

    connect_args = {}
    if "sslmode=require" in url_str:
        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE
        connect_args["ssl"] = ssl_ctx

    # Supabase pooled connection uses PgBouncer (transaction mode) -
    # disable statement caching to avoid DuplicatePreparedStatementError
    connect_args["statement_cache_size"] = 0

    return clean_url, connect_args


engine_url, connect_args = _build_engine_url_and_connect_args(settings.get_asyncpg_database_url())

engine = create_async_engine(
    engine_url,
    pool_size=settings.DATABASE_POOL_SIZE,
    max_overflow=settings.DATABASE_MAX_OVERFLOW,
    echo=settings.DEBUG,
    connect_args=connect_args,
    pool_pre_ping=True,
    pool_recycle=3600,
    pool_timeout=30,
)

AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


async def wait_for_db(max_retries: int = 30, retry_delay: float = 2.0) -> bool:
    """Wait for database to become available with exponential backoff retries."""
    for attempt in range(max_retries):
        try:
            async with asyncio.timeout(10):
                async with engine.begin() as conn:
                    await conn.execute(text("SELECT 1"))
            logger.info("Database connection established")
            return True
        except Exception as exc:
            if attempt < max_retries - 1:
                backoff = min(retry_delay * (2 ** attempt), 30.0)
                logger.warning(
                    f"Database not ready (attempt {attempt + 1}/{max_retries}): {exc} — retrying in {backoff:.1f}s"
                )
                await asyncio.sleep(backoff)
            else:
                logger.error(f"Database connection failed after {max_retries} attempts: {exc}")
    return False


async def check_db_health() -> dict:
    """Return database health status for /api/health endpoint."""
    try:
        async with asyncio.timeout(5):
            async with engine.begin() as conn:
                await conn.execute(text("SELECT 1"))
        return {"status": "healthy", "pool_size": settings.DATABASE_POOL_SIZE}
    except Exception as exc:
        return {"status": "unhealthy", "error": type(exc).__name__}


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()


async def init_db() -> None:
    """Initialize database tables with retry logic."""
    if not await wait_for_db():
        raise RuntimeError("Could not connect to database after retries")
    # Supabase PgBouncer transaction mode doesn't support run_sync prepared statements.
    # Create tables explicitly with CREATE TABLE IF NOT EXISTS.
    from sqlalchemy import text
    async with engine.begin() as conn:
        await conn.execute(text('''
            CREATE TABLE IF NOT EXISTS daily_state (
                id BIGSERIAL PRIMARY KEY,
                date VARCHAR(10) UNIQUE NOT NULL,
                daily_pnl DOUBLE PRECISION DEFAULT 0.0 NOT NULL,
                peak_pnl DOUBLE PRECISION DEFAULT 0.0 NOT NULL,
                consecutive_losses INTEGER DEFAULT 0 NOT NULL,
                is_locked BOOLEAN DEFAULT FALSE NOT NULL,
                total_wins INTEGER DEFAULT 0 NOT NULL,
                total_losses INTEGER DEFAULT 0 NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        '''))
        await conn.execute(text('''
            CREATE TABLE IF NOT EXISTS trades (
                id BIGSERIAL PRIMARY KEY,
                trade_id VARCHAR(64) UNIQUE NOT NULL,
                symbol VARCHAR(20) NOT NULL,
                side VARCHAR(10) NOT NULL,
                price DOUBLE PRECISION NOT NULL,
                size DOUBLE PRECISION NOT NULL,
                sl_price DOUBLE PRECISION,
                tp_price DOUBLE PRECISION,
                pnl DOUBLE PRECISION,
                result VARCHAR(20) DEFAULT 'OPEN' NOT NULL,
                broker_ticket BIGINT,
                broker_state INTEGER,
                client_order_id VARCHAR(64),
                opened_at TIMESTAMPTZ DEFAULT NOW(),
                closed_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        '''))
        await conn.execute(text('''
            CREATE TABLE IF NOT EXISTS positions (
                id BIGSERIAL PRIMARY KEY,
                broker_ticket BIGINT UNIQUE NOT NULL,
                symbol VARCHAR(20) NOT NULL,
                side VARCHAR(10) NOT NULL,
                volume DOUBLE PRECISION NOT NULL,
                open_price DOUBLE PRECISION NOT NULL,
                current_price DOUBLE PRECISION NOT NULL,
                sl DOUBLE PRECISION,
                tp DOUBLE PRECISION,
                profit DOUBLE PRECISION DEFAULT 0.0 NOT NULL,
                is_active BOOLEAN DEFAULT TRUE NOT NULL,
                opened_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        '''))
        await conn.execute(text('''
            CREATE TABLE IF NOT EXISTS audit_log (
                id BIGSERIAL PRIMARY KEY,
                event_type VARCHAR(50) NOT NULL,
                message TEXT NOT NULL,
                symbol VARCHAR(20),
                level VARCHAR(20) DEFAULT 'INFO' NOT NULL,
                meta_data TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        '''))
        await conn.execute(text('''
            CREATE TABLE IF NOT EXISTS account_snapshots (
                id BIGSERIAL PRIMARY KEY,
                balance DOUBLE PRECISION NOT NULL,
                equity DOUBLE PRECISION NOT NULL,
                margin DOUBLE PRECISION,
                free_margin DOUBLE PRECISION,
                margin_level DOUBLE PRECISION,
                currency VARCHAR(10) DEFAULT 'USD' NOT NULL,
                account_type VARCHAR(20) DEFAULT 'demo' NOT NULL,
                synced BOOLEAN DEFAULT FALSE NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        '''))
    logger.info("Database tables initialized (Supabase)")
