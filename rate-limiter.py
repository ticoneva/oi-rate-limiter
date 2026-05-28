"""
title: Rate Limit Filter
author: justinh-rahb with improvements by Yanyitin753
author_url: https://github.com/justinh-rahb
funding_url: https://github.com/open-webui
version: 0.4.0
license: MIT
"""

import time
import uuid
from typing import Callable, Awaitable, Any, Optional, Tuple
from pydantic import BaseModel, Field


class Filter:
    class Valves(BaseModel):
        priority: int = Field(
            default=0, description="Priority level for the filter operations."
        )
        requests_per_minute: Optional[int] = Field(
            default=10, description="Maximum number of requests allowed per minute."
        )
        requests_per_hour: Optional[int] = Field(
            default=50, description="Maximum number of requests allowed per hour."
        )
        sliding_window_limit: Optional[int] = Field(
            default=100,
            description="Maximum number of requests allowed within the sliding window.",
        )
        sliding_window_minutes: Optional[int] = Field(
            default=180, description="Duration of the sliding window in minutes."
        )
        enabled_for_admins: bool = Field(
            default=True,
            description="Whether rate limiting is enabled for admin users.",
        )
        redis_url: str = Field(
            default="redis://redis-valkey:6379/0",
            description="Redis URL for shared rate limit tracking.",
        )
        rate_limiter_id: str = Field(
            default="",
            description=(
                "Optional namespace for this rate limiter. If empty, the model ID is used automatically. "
                "Set the same ID on multiple rate-limiter instances to share tracking (global limiting)."
            ),
        )

    def __init__(self):
        self._redis: Optional["redis.Redis"] = None
        self.valves = self.Valves()

    def _get_redis(self) -> "redis.Redis":
        if self._redis is None:
            import redis as _redis

            self._redis = _redis.from_url(self.valves.redis_url, decode_responses=True)
        return self._redis

    @staticmethod
    def _make_key(namespace: str, user_id: str) -> str:
        return f"oi-rate-limiter:{namespace}:{user_id}"

    def _get_namespace(self, model_id: str) -> str:
        ns = self.valves.rate_limiter_id.strip()
        return ns if ns else model_id

    def _prune_key(self, key: str, now: float):
        cutoff = now - self.valves.sliding_window_minutes * 60
        self._get_redis().zremrangebyscore(key, "-inf", cutoff)

    def _count_key(self, key: str, start: float, end: float) -> int:
        return self._get_redis().zcount(key, start, end)

    def _earliest_key(
        self, key: str, min_score: float, max_score: float
    ) -> Optional[float]:
        results = self._get_redis().zrangebyscore(
            key, min_score, max_score, start=0, num=1, withscores=True
        )
        if results:
            return results[0][1]
        return None

    def rate_limited(
        self, user_id: str, model_id: str
    ) -> Tuple[bool, Optional[int], int]:
        namespace = self._get_namespace(model_id)
        key = self._make_key(namespace, user_id)
        now = time.time()

        # Prune expired entries outside the sliding window
        self._prune_key(key, now)

        # Check per-minute limit
        if self.valves.requests_per_minute is not None:
            count = self._count_key(key, now - 60, "+inf")
            if count >= self.valves.requests_per_minute:
                earliest = self._earliest_key(key, now - 60, "+inf")
                return True, int(60 - (now - earliest)), count

        # Check per-hour limit
        if self.valves.requests_per_hour is not None:
            count = self._count_key(key, now - 3600, "+inf")
            if count >= self.valves.requests_per_hour:
                earliest = self._earliest_key(key, now - 3600, "+inf")
                return True, int(3600 - (now - earliest)), count

        # Check sliding window limit
        slide_secs = self.valves.sliding_window_minutes * 60
        count = self._count_key(key, now - slide_secs, "+inf")
        if count >= self.valves.sliding_window_limit:
            earliest = self._earliest_key(key, now - slide_secs, "+inf")
            return True, int(slide_secs - (now - earliest)), count

        return False, None, 0

    def log_request(self, user_id: str, model_id: str):
        namespace = self._get_namespace(model_id)
        key = self._make_key(namespace, user_id)
        r = self._get_redis()

        score = time.time()
        member = f"{score}:{uuid.uuid4().hex}"

        with r.pipeline() as pipe:
            pipe.zadd(key, {member: score})
            pipe.expire(key, self.valves.sliding_window_minutes * 60)
            pipe.execute()

    def inlet(
        self,
        body: dict,
        __event_emitter__: Callable[[Any], Awaitable[None]],
        __user__: Optional[dict] = None,
        __model__: Optional[dict] = None,
    ) -> dict:
        print(f"inlet:{__name__}")
        print(f"inlet:body:{body}")
        print(f"inlet:user:{__user__}")

        if __user__ is not None and (
            __user__.get("role") != "admin" or self.valves.enabled_for_admins
        ):
            user_id = __user__["id"]
            model_id = __model__["id"] if __model__ is not None else "default_model"
            rate_limited, wait_time, request_count = self.rate_limited(
                user_id, model_id
            )
            if rate_limited:
                from datetime import datetime, timedelta

                current_time = datetime.now()
                future_time = current_time + timedelta(seconds=wait_time)
                future_time_str = future_time.strftime("%I:%M %p")

                raise Exception(
                    f"Rate limit exceeded. You have made {request_count} requests to model '{model_id}'. Please try again at {future_time_str}."
                )

            self.log_request(user_id, model_id)

        return body
