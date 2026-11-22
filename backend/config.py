from pydantic_settings import BaseSettings
from typing import List, Optional, Set
import urllib.parse


class Settings(BaseSettings):
    # --- API ---
    MTAPI_KEY: str
    MTAPI_ACCOUNT_ID: str
    MTAPI_BASE_URL: str = "https://api.metatraderapi.net"
    MTAPI_TIMEOUT: int = 15
    MTAPI_MAX_RETRIES: int = 3
    MTAPI_RETRY_BACKOFF: float = 2.0

    # --- Server ---
    HOST: str = "0.0.0.0"
    PORT: int = 8000
    DEBUG: bool = False
    LOG_LEVEL: str = "INFO"

    # --- Database (Supabase PostgreSQL) ---
    DATABASE_URL: Optional[str] = None
    SUPABASE_URL: Optional[str] = None
    SUPABASE_DB_PASSWORD: Optional[str] = None
    SUPABASE_USE_POOLER: bool = True
    DATABASE_POOL_SIZE: int = 20
    DATABASE_MAX_OVERFLOW: int = 40

    # --- Redis ---
    REDIS_URL: Optional[str] = None
    REDIS_CACHE_TTL: int = 60

    # --- Risk ---
    DEFAULT_BALANCE: float = 10000.0
    DEFAULT_TARGET_PCT: float = 0.02
    DEFAULT_MAX_LOSS_PCT: float = 0.01
    DEFAULT_RISK_PER_TRADE_PCT: float = 0.015
    DEFAULT_SL_PIPS: int = 150
    DEFAULT_TP_PIPS: int = 300
    DEFAULT_LOT_SIZE: float = 0.01
    PROFIT_WITHDRAW_THRESHOLD: float = 100.0
    MAX_DAILY_TRADES: int = 50
    MAX_POSITIONS_PER_SYMBOL: int = 1
    MAX_SYMBOLS: int = 5

    # --- Symbols ---
    _symbols_str: str = "GOLD"

    @property
    def SYMBOLS(self) -> List[str]:
        return [s.strip() for s in self._symbols_str.split(",") if s.strip()]

    # --- Security ---
    API_KEY_HEADER: str = "X-API-Key"
    RATE_LIMIT_PER_MINUTE: int = 60
    ADMIN_API_KEYS: str = ""
    TRADER_API_KEYS: str = ""
    VIEWER_API_KEYS: str = ""

    @property
    def admin_keys(self) -> Set[str]:
        return {k.strip() for k in self.ADMIN_API_KEYS.split(",") if k.strip()}

    @property
    def trader_keys(self) -> Set[str]:
        return {k.strip() for k in self.TRADER_API_KEYS.split(",") if k.strip()}

    @property
    def viewer_keys(self) -> Set[str]:
        return {k.strip() for k in self.VIEWER_API_KEYS.split(",") if k.strip()}

    @property
    def all_valid_keys(self) -> Set[str]:
        return self.admin_keys | self.trader_keys | self.viewer_keys

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "case_sensitive": True,
        "extra": "ignore",
    }

    def get_asyncpg_database_url(self) -> str:
        """Return asyncpg-compatible database URL for Supabase."""
        if self.DATABASE_URL:
            url = self.DATABASE_URL
            if url.startswith("postgresql://"):
                url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
            if "@db." in url:
                url = url.replace("@db.", "@")
            if "supabase.co" in url and "sslmode" not in url:
                separator = "&" if "?" in url else "?"
                url += f"{separator}sslmode=require"
            return url

        if self.SUPABASE_URL and self.SUPABASE_DB_PASSWORD:
            project_ref = self.SUPABASE_URL.replace("https://", "").replace("http://", "").split(".")[0]
            encoded_password = urllib.parse.quote(self.SUPABASE_DB_PASSWORD, safe="")
            port = 6543 if self.SUPABASE_USE_POOLER else 5432
            host = f"db.{project_ref}.supabase.co" if self.SUPABASE_USE_POOLER else f"{project_ref}.supabase.co"
            return (
                f"postgresql+asyncpg://postgres:{encoded_password}@{host}:{port}/postgres"
                f"?sslmode=require"
            )

        raise ValueError("Either DATABASE_URL or SUPABASE_URL + SUPABASE_DB_PASSWORD must be set")


settings = Settings()
