import asyncio
import json
import logging
from typing import Any, Optional

import redis.asyncio as redis

from config import settings

logger = logging.getLogger("server")


class CacheManager:
    """Redis-based caching for price data, signals, and rate limiting."""

    def __init__(self) -> None:
        self._redis: Optional[redis.Redis] = None

    async def connect(self) -> None:
        if not settings.REDIS_URL:
            logger.warning("REDIS_URL not set — caching disabled")
            return
        try:
            self._redis = redis.from_url(settings.REDIS_URL, decode_responses=True)
            await self._redis.ping()
            logger.info("Redis cache connected")
        except Exception as exc:
            logger.warning(f"Redis connection failed — caching disabled: {exc}")
            self._redis = None

    async def disconnect(self) -> None:
        if self._redis:
            await self._redis.close()

    async def get(self, key: str) -> Optional[Any]:
        if not self._redis:
            return None
        try:
            value = await self._redis.get(key)
            if value:
                return json.loads(value)
        except Exception as exc:
            logger.error(f"Cache get error for {key}: {exc}")
        return None

    async def set(self, key: str, value: Any, ttl: int = 60) -> None:
        if not self._redis:
            return
        try:
            await self._redis.setex(key, ttl, json.dumps(value))
        except Exception as exc:
            logger.error(f"Cache set error for {key}: {exc}")

    async def delete(self, key: str) -> None:
        if not self._redis:
            return
        try:
            await self._redis.delete(key)
        except Exception as exc:
            logger.error(f"Cache delete error for {key}: {exc}")

    async def rate_limit_check(self, key: str, limit: int, window: int = 60) -> bool:
        """Returns True if request is allowed, False if rate limited."""
        if not self._redis:
            return True
        try:
            current = await self._redis.get(key)
            if current is None:
                await self._redis.setex(key, window, 1)
                return True
            if int(current) >= limit:
                return False
            await self._redis.incr(key)
            return True
        except Exception as exc:
            logger.error(f"Rate limit error for {key}: {exc}")
            return True


cache = CacheManager()
