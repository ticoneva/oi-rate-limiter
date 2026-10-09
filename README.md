# Open WebUI Rate Limiter

A filter for [Open WebUI](https://github.com/open-webui/open-webui) that enforces
per-user request rate limits and credit budgets (weighted token limits).

## Why Redis?

Open WebUI can run with **multiple workers**, each handling requests independently. A rate limiter that stores counts in memory (e.g. a Python `dict`) only knows about the requests it has personally seen. The same user hitting two different workers would never exceed the limit, even though they've made twice as many requests.

This rate limiter uses **Redis sorted sets** as a shared, centralized counter so all workers agree on how many requests each user has made, and how many credits they have consumed, and limits are enforced consistently regardless of which worker processes a request.

## Two kinds of limits

**Request limits** count requests per user, exactly as previous versions did.

**Credit budgets** count *credits* — weighted tokens. When a model response
finishes, its token usage is charged as:

```
credits = input_weight       × (prompt_tokens - cached_tokens)   (uncached prefill)
        + cached_input_weight × cached_tokens                    (prefix-cache / LMCache hits)
        + output_weight       × completion_tokens                (decode)
```

This mirrors how prefill and decode are priced differently in practice, and
lets cache hits be discounted. Set all weights to `1.0` (the default) and the
budget is a plain total-token limit.

Because credits are charged with each filter instance's own weights **at write
time**, models with different pricing can share one budget via the same
`rate_limiter_id` — a budget of 1,000,000 credits means 1M *base-model
equivalent* tokens, not 1M raw tokens across mixed models.

### Where usage is counted

| Call type | Enforcement | Token accounting |
|---|---|---|
| Streaming API (`/api/chat/completions`, `/api/v1/messages`) | `inlet` check | `stream` hook records actual usage from the final SSE chunk |
| Non-streaming API | `inlet` check | `inlet` records an estimated input-token charge (no hook sees the response) |
| Web UI streaming | `inlet` check | `stream` hook records actuals |
| Web UI non-streaming | `inlet` check | `outlet` hook records actuals, deduplicated against the stream hook via a short-lived marker |
| Internal task calls (title/tag generation) | not counted — they bypass chat filters in Open WebUI 0.8.x | |

The filter injects `stream_options: {include_usage: true}` into streaming
requests so OpenAI-compatible backends (vLLM, etc.) report usage in the final
chunk; a client-supplied `stream_options` is preserved.

To discount cache hits, vLLM must be started with
`--enable-prompt-tokens-details` so usage includes
`prompt_tokens_details.cached_tokens`. When the field is absent (other
backends, or vLLM without the flag) the full prefill is charged — the filter
fails toward overcharging, never undercharging. `cached_tokens` covers both
vLLM's own prefix cache and LMCache-served KV (external hits are folded into
the same field).

## Configuration

| Setting | Default | Description |
|---|---|---|
| `requests_per_minute` | 10 | Max requests per minute per user |
| `requests_per_hour` | 50 | Max requests per hour per user |
| `sliding_window_limit` | 100 | Max requests per sliding window |
| `sliding_window_minutes` | 180 | Request sliding window duration in minutes |
| `credits_per_minute` | (disabled) | Credit budget per minute per user |
| `credits_per_hour` | (disabled) | Credit budget per hour per user |
| `sliding_window_credits` | (disabled) | Credit budget per credit window |
| `credit_window_minutes` | 1440 | Credit sliding window duration in minutes |
| `input_weight` | 1.0 | Credit cost of one uncached prefill token |
| `cached_input_weight` | 1.0 | Credit cost of one cache-hit prefill token |
| `output_weight` | 1.0 | Credit cost of one decode token |
| `inject_usage_stream_options` | True | Inject `stream_options.include_usage` into streaming requests |
| `estimate_nonstreaming_tokens` | True | Charge an input estimate for non-streaming API calls |
| `chars_per_token` | 3.0 | Characters per token for the non-streaming estimate (use ~1.5–2 for CJK-heavy traffic) |
| `cap_max_tokens_to_budget` | False | Clamp `max_tokens` to the remaining budget ÷ `output_weight` |
| `enabled_for_admins` | True | Whether admins are also rate-limited |
| `redis_url` | `redis://redis-valkey:6379/0` | Redis connection URL |
| `rate_limiter_id` | (empty) | Namespace for this limiter; leave empty to use the model ID, or set the same ID on multiple instances to share a combined budget |

Deploying the filter with default valves is a behavioral no-op for credits:
all credit limits are disabled and all weights neutral, so existing
request-limiting behavior is unchanged until a credit limit is set.

## Redis layout

- `oi-rate-limiter:{namespace}:{user_id}` — request log (unchanged from 0.4.0)
- `oi-rate-limiter:{namespace}:{user_id}:credits` — credit records; sorted-set
  members are `{timestamp}:{uuid}:{uncached_in}:{cached_in}:{out}:{charged}`
  so raw splits survive for reporting and re-derivation
- `oi-rate-limiter:{namespace}:{user_id}:done:{chat_id}:{message_id}` —
  15-minute markers preventing stream/outlet double-counting

The namespace is `rate_limiter_id` when set, else the model ID. Credit weights
are baked into records at write time (per instance), so retuning a weight
affects future usage only.

## Installation

1. Install the `redis` Python package:

```bash
pip install redis
```

2. Place `rate-limiter.py` in the Open WebUI filters directory (or paste it
   into Admin Panel → Functions).

3. Set `redis_url` to match your Redis/Valkey instance.

4. Optional, for cache-hit discounts: restart vLLM with
   `--enable-prompt-tokens-details` and set `cached_input_weight` below 1.0.

## Testing

`test-rate-limiter.py` runs the filter against `fakeredis` with all three
hooks simulated (web-UI dict chunks, API raw-SSE chunks, outlet payloads) and
shared-state multi-instance scenarios:

```bash
pip install --user fakeredis pydantic
python3 test-rate-limiter.py
```

## License

MIT
