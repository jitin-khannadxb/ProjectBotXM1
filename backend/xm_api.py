import logging
import os
import time
import uuid
from typing import Any, Dict, List, Optional

import requests

from config import settings

logger = logging.getLogger(__name__)


class XM_API:
    """REST API wrapper for metatraderapi.net.

    Auth: x-api-key header + ?id=<account UUID> on every request.
    Endpoints follow the classic REST style (GET with query params).
    """

    def __init__(self, api_key: str, account_id: str) -> None:
        self.base_url = settings.MTAPI_BASE_URL
        self.account_id = account_id
        self.headers = {
            "x-api-key": api_key,
        }
        self.max_retries = settings.MTAPI_MAX_RETRIES
        self.retry_backoff = settings.MTAPI_RETRY_BACKOFF
        self.timeout = settings.MTAPI_TIMEOUT

    def _get(
        self,
        endpoint: str,
        params: Optional[dict] = None,
        retry_on_429: bool = True,
    ) -> Optional[dict]:
        """Internal helper for GET requests with timeout, error handling, and retry."""
        url = f"{self.base_url}{endpoint}"
        request_params = {"id": self.account_id}
        if params:
            request_params.update(params)

        last_exc: Optional[Exception] = None
        for attempt in range(self.max_retries):
            try:
                response = requests.get(
                    url,
                    headers=self.headers,
                    params=request_params,
                    timeout=self.timeout,
                )

                if response.status_code == 429:
                    if attempt < self.max_retries - 1 and retry_on_429:
                        wait = self.retry_backoff * (2 ** attempt)
                        logger.warning(
                            f"GET {endpoint} rate limited (429) — retrying in {wait:.1f}s (attempt {attempt + 1}/{self.max_retries})"
                        )
                        time.sleep(wait)
                        continue
                    logger.error(
                        f"GET {endpoint} failed: 429 Too Many Requests after {self.max_retries} attempts"
                    )
                    return None

                response.raise_for_status()
                return response.json()

            except requests.RequestException as exc:
                last_exc = exc
                if attempt < self.max_retries - 1:
                    wait = self.retry_backoff * (2 ** attempt)
                    logger.warning(
                        f"GET {endpoint} failed: {exc} — retrying in {wait:.1f}s (attempt {attempt + 1}/{self.max_retries})"
                    )
                    time.sleep(wait)
                    continue

        logger.error(f"GET {endpoint} failed after {self.max_retries} attempts: {last_exc}")
        return None

    def get_account_summary(self) -> Optional[Dict[str, float]]:
        """Fetch account balance and equity from /AccountSummary."""
        data = self._get("/AccountSummary")
        if not data:
            return None
        return {
            "balance": float(data.get("balance", 0.0)),
            "equity": float(data.get("equity", 0.0)),
            "profit": float(data.get("profit", 0.0)),
            "margin": float(data.get("margin", 0.0)),
            "free_margin": float(data.get("freeMargin", 0.0)),
            "currency": data.get("currency", "USD"),
            "synced": bool(data.get("synced", False)),
        }

    def get_live_quote(self, symbol: str) -> Optional[Dict[str, float]]:
        """Fetch live bid/ask for a symbol from /GetQuote."""
        data = self._get("/GetQuote", {"symbol": symbol})
        if not data:
            return None
        return {
            "bid": float(data.get("bid", 0.0)),
            "ask": float(data.get("ask", 0.0)),
            "symbol": data.get("symbol", symbol),
        }

    def send_order(
        self,
        symbol: str,
        side: str,
        volume: float,
        stoploss_price: float,
        takeprofit_price: float,
        slippage: int = 10,
    ) -> Optional[dict]:
        """Send a market order via /OrderSend (GET with query params).

        stoploss_price / takeprofit_price are absolute prices (not pips).
        Returns the order response with ticket and state.
        """
        params = {
            "symbol": symbol,
            "operation": side.upper(),  # Buy or Sell
            "volume": volume,
            "stoploss": stoploss_price,
            "takeprofit": takeprofit_price,
            "slippage": slippage,
            "clientOrderId": str(uuid.uuid4()),
        }
        return self._get("/OrderSend", params)

    def close_order(self, ticket: int) -> Optional[dict]:
        """Close an open order by ticket via /OrderClose."""
        return self._get("/OrderClose", {"ticket": ticket})

    def get_trade_session(self, symbol: str) -> bool:
        """Check if trading session is open for a symbol via /IsTradeSession."""
        data = self._get("/IsTradeSession", {"symbol": symbol})
        if not data:
            return False
        # API returns boolean or dict with 'tradeSession' field
        if isinstance(data, bool):
            return data
        if isinstance(data, dict):
            return bool(data.get("tradeSession", data.get("isTradeSession", False)))
        return False

    def get_open_positions(self) -> Optional[List[dict]]:
        """Fetch the list of currently open positions from /OpenedOrders."""
        data = self._get("/OpenedOrders")
        if not data:
            return None
        return data if isinstance(data, list) else data.get("orders", [])

    def get_symbol_list(self) -> Optional[List[str]]:
        """Fetch the list of available symbols from /SymbolList."""
        data = self._get("/SymbolList")
        if not data:
            return None
        return data if isinstance(data, list) else data.get("symbols", [])


if __name__ == "__main__":
    api_key = os.environ.get("MTAPI_KEY", "YOUR_API_KEY_HERE")
    account_id = os.environ.get("MTAPI_ACCOUNT_ID", "YOUR_ACCOUNT_ID_HERE")

    client = XM_API(api_key=api_key, account_id=account_id)

    print("XM_API client initialized.")
    print(f"Account ID: {client.account_id}")
    print("Live mode enabled - real API calls will be made.")
