"""
title: Rate and Credit Limit Filter
author: justinh-rahb with improvements by Yanyitin753
author_url: https://github.com/justinh-rahb
funding_url: https://github.com/open-webui
version: 0.9.0
license: MIT
"""

import asyncio
import json
import time
import uuid
from datetime import datetime, timedelta
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
        credits_per_minute: Optional[int] = Field(
            default=None,
            description=(
                "Credit budget per minute per user. Credits are weighted tokens: "
                "input_weight x uncached prefill + cached_input_weight x cached prefill "
                "+ output_weight x decode. None disables this period."
            ),
        )
        credits_per_session: Optional[int] = Field(
            default=2000,
            description=(
                "Credit budget per session window per user. Defaults to z.ai's "
                "GLM Coding Plan Lite 5-hour allowance (2,000 credits)."
            ),
        )
        session_window_minutes: int = Field(
            default=300,
            description="Duration of the session credit window in minutes.",
        )
        sliding_window_credits: Optional[int] = Field(
            default=10000,
            description=(
                "Credit budget within the credit sliding window. Defaults to "
                "z.ai's GLM Coding Plan Lite weekly allowance (10,000 credits)."
            ),
        )
        credit_window_minutes: int = Field(
            default=10080,
            description="Duration of the credit sliding window in minutes.",
        )
        input_weight: float = Field(
            default=0.00069,
            description=(
                "Credit cost of one uncached prefill (input) token. Default is "
                "z.ai's GLM-5.3 input multiplier (6.9) / 10,000, so one unit "
                "equals one z.ai credit."
            ),
        )
        cached_input_weight: float = Field(
            default=0.00017,
            description=(
                "Credit cost of one cache-hit prefill token (vLLM prefix cache and "
                "LMCache hits). Default is z.ai's GLM-5.3 cached-input multiplier "
                "(1.7) / 10,000. Requires vLLM started with "
                "--enable-prompt-tokens-details; falls back to full input cost when "
                "the backend does not report cached_tokens."
            ),
        )
        output_weight: float = Field(
            default=0.0024,
            description=(
                "Credit cost of one decode (output) token. Default is z.ai's "
                "GLM-5.3 output multiplier (24) / 10,000."
            ),
        )
        inject_usage_stream_options: bool = Field(
            default=True,
            description=(
                "Inject stream_options.include_usage into streaming requests so "
                "OpenAI-compatible backends report token usage in the final chunk."
            ),
        )
        estimate_nonstreaming_tokens: bool = Field(
            default=True,
            description=(
                "Charge an input-token estimate for non-streaming API calls, whose "
                "responses are not visible to any filter hook."
            ),
        )
        chars_per_token: float = Field(
            default=3.0,
            description=(
                "Characters per token used for the non-streaming estimate. "
                "Use ~1.5-2 for CJK-heavy traffic."
            ),
        )
        cap_max_tokens_to_budget: bool = Field(
            default=False,
            description=(
                "Clamp the request's max_tokens to the remaining credit budget "
                "divided by output_weight, bounding overshoot on the final request "
                "before a limit is hit."
            ),
        )
        hold_credits_at_inlet: bool = Field(
            default=True,
            description=(
                "Reserve an estimated credit charge when a request is admitted, so "
                "simultaneous requests from the same user each see their siblings' "
                "pending cost and are blocked at the door instead of overrunning the "
                "budget. Holds are released (and replaced by exact charges) when usage "
                "records, and expire if a request dies without reporting usage."
            ),
        )
        hold_output_reserve_tokens: int = Field(
            default=0,
            description=(
                "Decode tokens reserved per hold on top of the input estimate, so "
                "many small-prompt large-output requests cannot under-reserve. "
                "0 disables the output reserve."
            ),
        )
        hold_ttl_minutes: int = Field(
            default=15,
            description=(
                "Lifespan of a hold whose request never reports usage (e.g. client "
                "disconnect mid-stream). Should exceed your longest normal request "
                "duration."
            ),
        )
        show_usage_status: bool = Field(
            default=False,
            description=(
                "Emit a status event at request time showing the user's current "
                "credit consumption for the session and sliding windows, e.g. "
                "'Credits: 340/2000 session (17%) - 4210/10000 week (42%)'. "
                "Visible in the web UI; API clients ignore status events."
            ),
        )
        status_interval_seconds: int = Field(
            default=5,
            description=(
                "Minimum interval between live status updates while a response "
                "streams. The running count includes an estimate of the tokens "
                "generated so far in the in-flight response."
            ),
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
                "Set the same ID on multiple rate-limiter instances to share tracking (global limiting). "
                "Credit records store the charge computed with each instance's own weights, "
                "so models with different weights can share one credit budget correctly."
            ),
        )

    class UserValves(BaseModel):
        show_usage_status: bool = Field(
            default=True,
            description="Show the credit usage status line on your requests. "
            "Limit enforcement is unaffected.",
        )

    def __init__(self):
        self._redis: Optional["redis.Redis"] = None
        self.valves = self.Valves()
        # Per-instance live-status state: (user_id, chat_id, message_id) ->
        # [generated_chars, last_emit_ts]. Cleared when usage records.
        self._live: dict = {}

    def _get_redis(self) -> "redis.Redis":
        if self._redis is None:
            import redis as _redis

            self._redis = _redis.from_url(self.valves.redis_url, decode_responses=True)
        return self._redis

    @staticmethod
    def _make_key(namespace: str, user_id: str) -> str:
        return f"oi-rate-limiter:{namespace}:{user_id}"

    @staticmethod
    def _credit_key(namespace: str, user_id: str) -> str:
        return f"oi-rate-limiter:{namespace}:{user_id}:credits"

    @staticmethod
    def _holds_key(namespace: str, user_id: str) -> str:
        return f"oi-rate-limiter:{namespace}:{user_id}:holds"

    @staticmethod
    def _done_marker_key(
        namespace: str, user_id: str, chat_id: str, message_id: str
    ) -> str:
        return f"oi-rate-limiter:{namespace}:{user_id}:done:{chat_id}:{message_id}"

    def _get_namespace(self, model_id: str) -> str:
        ns = self.valves.rate_limiter_id.strip()
        return ns if ns else model_id

    def _prune_key(self, key: str, now: float, window_secs: float):
        self._get_redis().zremrangebyscore(key, "-inf", now - window_secs)

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
        self._prune_key(key, now, self.valves.sliding_window_minutes * 60)

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
        if self.valves.sliding_window_limit is not None:
            slide_secs = self.valves.sliding_window_minutes * 60
            count = self._count_key(key, now - slide_secs, "+inf")
            if count >= self.valves.sliding_window_limit:
                earliest = self._earliest_key(key, now - slide_secs, "+inf")
                return True, int(slide_secs - (now - earliest)), count

        return False, None, 0

    def _credit_limits_configured(self) -> bool:
        return any(
            v is not None
            for v in (
                self.valves.credits_per_minute,
                self.valves.credits_per_session,
                self.valves.sliding_window_credits,
            )
        )

    def _credit_windows(self) -> list:
        """Active (limit, window_seconds) pairs, ascending window length."""
        return [
            (limit, secs)
            for limit, secs in (
                (self.valves.credits_per_minute, 60),
                (self.valves.credits_per_session, self.valves.session_window_minutes * 60),
                (self.valves.sliding_window_credits, self.valves.credit_window_minutes * 60),
            )
            if limit is not None
        ]

    def _credit_retention_secs(self) -> int:
        """Records are kept at least as long as the longest active window."""
        retention = self.valves.credit_window_minutes * 60
        if self.valves.credits_per_minute is not None:
            retention = max(retention, 60)
        if self.valves.credits_per_session is not None:
            retention = max(retention, self.valves.session_window_minutes * 60)
        return retention

    def _sum_credits(self, key: str, start: float, end: float) -> int:
        # Members are "{ts}:{uuid}:{uncached_in}:{cached_in}:{out}:{charged}"
        total = 0
        for member in self._get_redis().zrangebyscore(key, start, end):
            try:
                total += int(float(member.rsplit(":", 1)[1]))
            except (ValueError, IndexError):
                continue
        return total

    def _used_with_holds(
        self, user_id: str, model_id: str, now: float, win_secs: float
    ) -> Tuple[int, Optional[float]]:
        """Committed credits plus live holds within the window. Returns
        (used, earliest_record_ts) — the earliest record (not hold) drives the
        retry-time message."""
        namespace = self._get_namespace(model_id)
        credit_key = self._credit_key(namespace, user_id)
        holds_key = self._holds_key(namespace, user_id)

        used = self._sum_credits(credit_key, now - win_secs, "+inf")
        earliest = self._earliest_key(credit_key, now - win_secs, "+inf")

        # Holds are "{ts}:{uuid}:{amount}" with a key TTL bounding their life;
        # score-based pruning mirrors it in case the TTL has not fired yet.
        for member in self._get_redis().zrangebyscore(holds_key, now - win_secs, "+inf"):
            try:
                used += int(float(member.rsplit(":", 1)[1]))
            except (ValueError, IndexError):
                continue
        return used, earliest

    def credit_limited(
        self, user_id: str, model_id: str
    ) -> Tuple[bool, Optional[int], int, Optional[int]]:
        """Returns (limited, wait_seconds, used, limit) where (used, limit)
        describe the window closest to its limit, even when not limited.
        Pending holds count toward usage."""
        namespace = self._get_namespace(model_id)
        key = self._credit_key(namespace, user_id)
        now = time.time()
        window_secs = self.valves.credit_window_minutes * 60

        self._prune_key(key, now, window_secs)

        best_used, best_limit = 0, None
        best_ratio = -1.0
        for limit, win_secs in self._credit_windows():
            used, earliest = self._used_with_holds(user_id, model_id, now, win_secs)
            if used >= limit:
                wait = int(win_secs - (now - (earliest if earliest else now)))
                return True, wait, used, limit
            ratio = used / limit
            if ratio > best_ratio:
                best_ratio, best_used, best_limit = ratio, used, limit

        return False, None, best_used, best_limit

    def _remaining_credits(self, user_id: str, model_id: str) -> Optional[int]:
        now = time.time()

        remaining = None
        for limit, win_secs in self._credit_windows():
            used, _ = self._used_with_holds(user_id, model_id, now, win_secs)
            room = limit - used
            if remaining is None or room < remaining:
                remaining = room
        return remaining

    def _charge_estimate(self, input_tokens: int) -> int:
        """Credits reserved for a request at admission time."""
        output_reserve = max(0, self.valves.hold_output_reserve_tokens)
        return round(
            self.valves.input_weight * input_tokens
            + self.valves.output_weight * output_reserve
        )

    def _place_hold(self, user_id: str, model_id: str, estimate: int):
        namespace = self._get_namespace(model_id)
        key = self._holds_key(namespace, user_id)

        score = time.time()
        member = f"{score}:{uuid.uuid4().hex}:{estimate}"

        r = self._get_redis()
        with r.pipeline() as pipe:
            pipe.zadd(key, {member: score})
            pipe.expire(key, self.valves.hold_ttl_minutes * 60)
            pipe.execute()

    def _release_hold(self, user_id: str, model_id: str):
        """Remove the oldest live hold (FIFO), called when exact usage is
        recorded for a request that placed one."""
        namespace = self._get_namespace(model_id)
        key = self._holds_key(namespace, user_id)
        oldest = self._get_redis().zrange(key, 0, 0)
        if oldest:
            self._get_redis().zrem(key, oldest[0])

    def _record_usage(
        self,
        user_id: str,
        model_id: str,
        input_tokens: int,
        cached_tokens: int,
        output_tokens: int,
    ) -> int:
        namespace = self._get_namespace(model_id)
        key = self._credit_key(namespace, user_id)

        cached = max(0, min(int(cached_tokens), int(input_tokens)))
        uncached = max(0, int(input_tokens) - cached)
        output = max(0, int(output_tokens))

        # Charge with this instance's weights at write time, so models with
        # different pricing can share one credit budget via rate_limiter_id.
        charged = round(
            self.valves.input_weight * uncached
            + self.valves.cached_input_weight * cached
            + self.valves.output_weight * output
        )

        score = time.time()
        member = f"{score}:{uuid.uuid4().hex}:{uncached}:{cached}:{output}:{charged}"

        r = self._get_redis()
        with r.pipeline() as pipe:
            pipe.zadd(key, {member: score})
            pipe.expire(key, self._credit_retention_secs())
            pipe.execute()
        self._release_hold(user_id, model_id)
        return charged

    @staticmethod
    def _extract_usage(usage: Any) -> Tuple[int, int, int]:
        """Return (input_tokens, cached_tokens, output_tokens) from a usage
        object in any of the formats Open WebUI passes through: raw OpenAI
        (prompt_tokens/prompt_tokens_details), normalized Open WebUI
        (input_tokens/output_tokens), Ollama (prompt_eval_count/eval_count),
        or Responses API (nested under response.usage)."""
        if not isinstance(usage, dict):
            return 0, 0, 0

        input_tokens = (
            usage.get("input_tokens")
            or usage.get("prompt_tokens")
            or usage.get("prompt_eval_count")
            or usage.get("prompt_n")
            or 0
        )
        output_tokens = (
            usage.get("output_tokens")
            or usage.get("completion_tokens")
            or usage.get("eval_count")
            or usage.get("predicted_n")
            or 0
        )
        details = usage.get("prompt_tokens_details")
        cached = (details.get("cached_tokens") if isinstance(details, dict) else 0) or 0

        try:
            return int(input_tokens), int(cached), int(output_tokens)
        except (TypeError, ValueError):
            return 0, 0, 0

    def _estimate_input_tokens(self, body: dict) -> int:
        chars = 0
        for message in body.get("messages", []):
            if not isinstance(message, dict):
                continue
            content = message.get("content")
            if isinstance(content, str):
                chars += len(content)
            elif isinstance(content, list):
                for part in content:
                    if isinstance(part, dict) and part.get("type") == "text":
                        chars += len(part.get("text") or "")
        divisor = self.valves.chars_per_token or 3.0
        return int(chars / divisor)

    def _cap_max_tokens(self, body: dict, user_id: str, model_id: str):
        remaining = self._remaining_credits(user_id, model_id)
        if remaining is None or self.valves.output_weight <= 0:
            return
        allowed = max(1, int(remaining / self.valves.output_weight))
        capped = False
        for field in ("max_tokens", "max_completion_tokens"):
            current = body.get(field)
            if isinstance(current, (int, float)) and current > allowed:
                body[field] = allowed
                capped = True
        if not capped and "max_tokens" not in body and "max_completion_tokens" not in body:
            body["max_tokens"] = allowed

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

    @staticmethod
    def _retry_time(wait_seconds: int) -> str:
        future_time = datetime.now() + timedelta(seconds=wait_seconds)
        return future_time.strftime("%I:%M %p")

    def _enforced_for(self, __user__: Optional[dict]) -> bool:
        return __user__ is not None and (
            __user__.get("role") != "admin" or self.valves.enabled_for_admins
        )

    @staticmethod
    def _fmt_credits(n: float) -> str:
        # One significant decimal for small credit values: with provider-style
        # weights (z.ai multipliers / 10,000) a single request charges single
        # digits, which integer formatting would hide entirely.
        if n >= 100000:
            return f"{n / 1000000:.1f}M"
        if n >= 10000:
            return f"{n / 1000:.0f}K"
        if n >= 1000:
            return f"{n / 1000:.1f}K"
        if n >= 10:
            return f"{n:.0f}"
        return f"{n:.1f}"

    def _usage_status_message(
        self, user_id: str, model_id: str, extra_credits: int = 0
    ) -> str:
        now = time.time()
        parts = []
        for label, limit, win_secs in (
            (
                "session",
                self.valves.credits_per_session,
                self.valves.session_window_minutes * 60,
            ),
            (
                "week",
                self.valves.sliding_window_credits,
                self.valves.credit_window_minutes * 60,
            ),
        ):
            if limit is None:
                continue
            used, _ = self._used_with_holds(user_id, model_id, now, win_secs)
            used += extra_credits
            pct = (used / limit * 100) if limit > 0 else 0
            # One decimal below 10%: with provider-scaled weights each
            # request moves the percentage by well under a point.
            if pct < 10 and limit > 0:
                parts.append(
                    f"{self._fmt_credits(used)}/{self._fmt_credits(limit)} {label} ({pct:.1f}%)"
                )
            else:
                parts.append(
                    f"{self._fmt_credits(used)}/{self._fmt_credits(limit)} {label} ({pct:.0f}%)"
                )
        if not parts:
            return "Credits: no limits configured"
        return f"Credits: {' - '.join(parts)}"

    async def _tick_live_status(
        self,
        event: Any,
        emitter: Callable[[Any], Awaitable[None]],
        __user__: dict,
        __model__: Optional[dict],
        __metadata__: Optional[dict],
    ):
        """Called per stream chunk: accumulate generated characters and emit
        a status line every status_interval_seconds showing committed credits
        plus the in-flight response's estimated decode cost."""
        metadata = __metadata__ or {}
        key = (
            __user__.get("id"),
            metadata.get("chat_id") or "",
            metadata.get("message_id") or "",
        )
        state = self._live.get(key)
        if state is None:
            state = [0, 0.0]
            self._live[key] = state

        state[0] += len(self._delta_text(event))

        now = time.time()
        if now - state[1] < self.valves.status_interval_seconds:
            return
        state[1] = now

        # Estimate decode tokens from generated chars; add on top of the
        # user's committed credits + holds (which already include this
        # request's input estimate).
        divisor = self.valves.chars_per_token or 3.0
        est_tokens = int(state[0] / divisor)
        extra = round(self.valves.output_weight * est_tokens)

        message = self._usage_status_message(
            __user__["id"],
            __model__["id"] if __model__ is not None else "default_model",
            extra,
        )
        user_valves = __user__.get("valves")
        if user_valves is not None and not getattr(
            user_valves, "show_usage_status", True
        ):
            return
        await emitter(
            {"type": "status", "data": {"description": message, "done": False}}
        )

    def _clear_live_status(self, __user__: dict, __metadata__: Optional[dict]):
        metadata = __metadata__ or {}
        key = (
            __user__.get("id"),
            metadata.get("chat_id") or "",
            metadata.get("message_id") or "",
        )
        self._live.pop(key, None)

    async def _emit_usage_status(
        self,
        __event_emitter__: Optional[Callable[[Any], Awaitable[None]]],
        __user__: dict,
        user_id: str,
        model_id: str,
        extra_credits: int = 0,
    ):
        if __event_emitter__ is None:
            return
        user_valves = __user__.get("valves")
        if user_valves is not None and not getattr(
            user_valves, "show_usage_status", True
        ):
            return
        try:
            await __event_emitter__(
                {
                    "type": "status",
                    "data": {
                        "description": self._usage_status_message(
                            user_id, model_id, extra_credits
                        ),
                        "done": True,
                    },
                }
            )
        except Exception as e:
            print(f"rate-limiter: failed to emit usage status: {e}")

    @staticmethod
    def _delta_text(chunk: Any) -> str:
        """Content delta of a streamed chunk, if any (dict or raw SSE line)."""
        if isinstance(chunk, dict):
            choices = chunk.get("choices") or []
            if choices and isinstance(choices[0], dict):
                delta = choices[0].get("delta") or {}
                content = delta.get("content")
                return content if isinstance(content, str) else ""
        elif isinstance(chunk, (str, bytes)):
            text = (
                chunk.decode("utf-8", "replace") if isinstance(chunk, bytes) else chunk
            )
            stripped = text.strip()
            if stripped.startswith("data:"):
                payload = stripped[len("data:") :].strip()
                if payload and payload != "[DONE]":
                    try:
                        data = json.loads(payload)
                    except json.JSONDecodeError:
                        return ""
                    if isinstance(data, dict):
                        choices = data.get("choices") or []
                        if choices and isinstance(choices[0], dict):
                            delta = choices[0].get("delta") or {}
                            content = delta.get("content")
                            if isinstance(content, str):
                                return content
        return ""

    async def inlet(
        self,
        body: dict,
        __event_emitter__: Callable[[Any], Awaitable[None]],
        __user__: Optional[dict] = None,
        __model__: Optional[dict] = None,
        __metadata__: Optional[dict] = None,
    ) -> dict:
        if self._enforced_for(__user__):
            user_id = __user__["id"]
            model_id = __model__["id"] if __model__ is not None else "default_model"

            rate_limited, wait_time, request_count = self.rate_limited(
                user_id, model_id
            )
            if rate_limited:
                raise Exception(
                    f"Rate limit exceeded. You have made {request_count} requests to model '{model_id}'. Please try again at {self._retry_time(wait_time)}."
                )

            if self._credit_limits_configured():
                credit_limited, credit_wait, used_credits, credit_limit = (
                    self.credit_limited(user_id, model_id)
                )
                if credit_limited:
                    raise Exception(
                        f"Credit limit exceeded. You have used {used_credits} of {credit_limit} credits on model '{model_id}'. Please try again at {self._retry_time(credit_wait)}."
                    )

            self.log_request(user_id, model_id)

            if self.valves.show_usage_status:
                await self._emit_usage_status(
                    __event_emitter__, __user__, user_id, model_id
                )

            streaming = bool(body.get("stream"))

            # Ask OpenAI-compatible backends to report token usage in the
            # final stream chunk. The Ollama converter drops this field
            # harmlessly; Ollama reports usage regardless. A client-supplied
            # stream_options is left alone — it controls this already.
            if streaming and self.valves.inject_usage_stream_options:
                if not isinstance(body.get("stream_options"), dict):
                    body["stream_options"] = {"include_usage": True}
                else:
                    body["stream_options"].setdefault("include_usage", True)

            # Non-streaming API responses pass through no filter hook, so
            # charge an input estimate now. Web UI calls are excluded: their
            # outlet hook records exact usage once the turn completes.
            # Streaming requests place a transient hold instead: their exact
            # usage is recorded when the response completes, which also
            # releases the hold.
            metadata = __metadata__ or {}
            is_web_ui = bool(metadata.get("session_id") or metadata.get("chat_id"))
            if streaming:
                if self.valves.hold_credits_at_inlet:
                    estimate = self._estimate_input_tokens(body)
                    hold = self._charge_estimate(estimate)
                    if hold > 0:
                        self._place_hold(user_id, model_id, hold)
            elif self.valves.estimate_nonstreaming_tokens and not is_web_ui:
                estimate = self._estimate_input_tokens(body)
                if estimate > 0:
                    self._record_usage(user_id, model_id, estimate, 0, 0)

            if self.valves.cap_max_tokens_to_budget and self._credit_limits_configured():
                self._cap_max_tokens(body, user_id, model_id)

        return body

    async def stream(
        self,
        event: Any,
        __event_emitter__: Optional[Callable[[Any], Awaitable[None]]] = None,
        __user__: Optional[dict] = None,
        __model__: Optional[dict] = None,
        __metadata__: Optional[dict] = None,
    ) -> Any:
        """Record actual token usage from usage-bearing stream chunks, and
        keep the usage status line ticking while a response streams.

        The web UI path delivers each chunk as a parsed dict; the API path
        (no websocket session) delivers raw SSE lines. The event is always
        returned unmodified — bytes and framing must reach the client intact.
        """
        if not self._credit_limits_configured() or not self._enforced_for(__user__):
            return event

        # Live status: estimate in-flight decode cost from content deltas.
        # The stream hook is invoked per chunk, so per-chunk state lives in
        # a shared per-instance dict keyed by (user, message).
        if (
            self.valves.show_usage_status
            and __event_emitter__ is not None
            and self.valves.output_weight > 0
        ):
            try:
                await self._tick_live_status(
                    event, __event_emitter__, __user__, __model__, __metadata__
                )
            except Exception as e:
                print(f"rate-limiter: live status tick failed: {e}")

        if isinstance(event, dict):
            usage = event.get("usage")
            if usage is None:
                response = event.get("response")
                if isinstance(response, dict):
                    usage = response.get("usage")  # Responses API events
        elif isinstance(event, (str, bytes)):
            text = (
                event.decode("utf-8", "replace")
                if isinstance(event, bytes)
                else event
            )
            usage = None
            stripped = text.strip()
            if stripped.startswith("data:"):
                payload = stripped[len("data:") :].strip()
                if payload and payload != "[DONE]":
                    try:
                        data = json.loads(payload)
                    except json.JSONDecodeError:
                        data = None
                    if isinstance(data, dict):
                        usage = data.get("usage")
                        if usage is None:
                            response = data.get("response")
                            if isinstance(response, dict):
                                usage = response.get("usage")
        else:
            return event

        if usage is None:
            return event

        input_tokens, cached_tokens, output_tokens = self._extract_usage(usage)
        if input_tokens <= 0 and output_tokens <= 0:
            return event

        user_id = __user__["id"]
        model_id = __model__["id"] if __model__ is not None else "default_model"
        try:
            self._clear_live_status(__user__, __metadata__)
            self._record_usage(user_id, model_id, input_tokens, cached_tokens, output_tokens)

            # Mark this chat turn as accounted, so the outlet hook (which
            # fires after streaming turns too) does not double-charge it.
            metadata = __metadata__ or {}
            chat_id = metadata.get("chat_id")
            message_id = metadata.get("message_id")
            if chat_id and message_id:
                marker = self._done_marker_key(
                    self._get_namespace(model_id), user_id, chat_id, message_id
                )
                self._get_redis().set(marker, "1", ex=900)
        except Exception as e:
            print(f"rate-limiter: failed to record stream usage: {e}")

        return event

    async def outlet(
        self,
        body: dict,
        __event_emitter__: Optional[Callable[[Any], Awaitable[None]]] = None,
        __user__: Optional[dict] = None,
        __model__: Optional[dict] = None,
    ) -> dict:
        """Record exact usage for non-streaming web UI turns, as a fallback
        for turns the stream hook did not already account for."""
        if not self._credit_limits_configured() or not self._enforced_for(__user__):
            return body

        user_id = __user__["id"]
        model_id = (__model__ or {}).get("id") or body.get("model") or "default_model"
        namespace = self._get_namespace(model_id)

        chat_id = body.get("chat_id")
        message_id = body.get("id")
        already_recorded = False
        if chat_id and message_id:
            try:
                already_recorded = bool(
                    self._get_redis().exists(
                        self._done_marker_key(namespace, user_id, chat_id, message_id)
                    )
                )
            except Exception:
                return body
        if already_recorded:
            # The stream hook already accounted this turn; still refresh the
            # status line so the user sees post-turn numbers.
            if self.valves.show_usage_status:
                await self._emit_usage_status(
                    __event_emitter__, __user__, user_id, model_id
                )
            return body

        usage = body.get("usage")
        if not usage:
            for message in reversed(body.get("messages", [])):
                if (
                    isinstance(message, dict)
                    and message.get("role") == "assistant"
                    and message.get("usage")
                ):
                    usage = message["usage"]
                    break
        if not usage:
            return body

        input_tokens, cached_tokens, output_tokens = self._extract_usage(usage)
        if input_tokens <= 0 and output_tokens <= 0:
            return body

        try:
            self._record_usage(user_id, model_id, input_tokens, cached_tokens, output_tokens)
        except Exception as e:
            print(f"rate-limiter: failed to record outlet usage: {e}")

        if self.valves.show_usage_status:
            await self._emit_usage_status(
                __event_emitter__, __user__, user_id, model_id
            )

        return body
