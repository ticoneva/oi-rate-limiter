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

### Admission control (holds)

Usage is only visible when a response completes, so without further measures a
user firing many simultaneous requests could pass every per-request check
against the same (empty) history and overrun the budget — throttling would
only start with the next request. To prevent this, every admitted streaming
request *reserves* an estimated charge (an input-token estimate plus an
optional per-request decode reserve) as a **hold** in a short-lived Redis
sorted set. Holds count toward every credit check, so sibling requests see
each other's pending cost and are blocked at the door. When a request's exact
usage is recorded, its hold is released (FIFO) and replaced by the actual
charge; if a request dies without reporting usage (e.g. a client disconnects
mid-stream), the hold expires via its key TTL. With the default
`hold_output_reserve_tokens: 0`, holds reserve prefill cost only — pair them
with `cap_max_tokens_to_budget` to also bound decode on parallel bursts, or
set an output reserve to cover it directly.

### Usage status display

With `show_usage_status` enabled, each request emits a status event the web UI
shows above the reply:

```
Credits: 340/2.0K session (17%) - 4210/10K week (42%)
```

The line shows committed credits plus live holds for each configured window
(session and weekly; only configured limits appear). It is emitted at request
time (showing where the user stands before the new request is charged) and
refreshed after a web-UI turn completes. Users can hide the line for
themselves via the filter's user valves — hiding it does not affect
enforcement. API clients ignore status events entirely.

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
| `credits_per_session` | 2000 | Credit budget per session window — z.ai GLM Coding Plan Lite 5-hour allowance |
| `session_window_minutes` | 300 | Session credit window duration in minutes |
| `sliding_window_credits` | 10000 | Credit budget per credit window — z.ai Lite weekly allowance |
| `credit_window_minutes` | 10080 | Credit sliding window duration in minutes (7 days) |
| `input_weight` | 0.00069 | Credit cost of one uncached prefill token (z.ai GLM-5.3 input multiplier ÷ 10,000) |
| `cached_input_weight` | 0.00017 | Credit cost of one cache-hit prefill token (z.ai GLM-5.3 cached multiplier ÷ 10,000) |
| `output_weight` | 0.0024 | Credit cost of one decode token (z.ai GLM-5.3 output multiplier ÷ 10,000) |
| `inject_usage_stream_options` | True | Inject `stream_options.include_usage` into streaming requests |
| `estimate_nonstreaming_tokens` | True | Charge an input estimate for non-streaming API calls |
| `chars_per_token` | 3.0 | Characters per token for the non-streaming estimate (use ~1.5–2 for CJK-heavy traffic) |
| `cap_max_tokens_to_budget` | False | Clamp `max_tokens` to the remaining budget ÷ `output_weight` |
| `hold_credits_at_inlet` | True | Reserve estimated credits at admission so simultaneous requests block at the door |
| `hold_output_reserve_tokens` | 0 | Decode tokens reserved per hold, on top of the input estimate |
| `hold_ttl_minutes` | 15 | Lifespan of a hold whose request never reports usage |
| `show_usage_status` | False | Emit a status line at request time showing credit consumption per session/window |
| `enabled_for_admins` | True | Whether admins are also rate-limited |
| `redis_url` | `redis://redis-valkey:6379/0` | Redis connection URL |
| `rate_limiter_id` | (empty) | Namespace for this limiter; leave empty to use the model ID, or set the same ID on multiple instances to share a combined budget |

Deploying the filter with default valves enforces z.ai's GLM Coding Plan
**Lite** allowances (2,000 credits per 5 hours, 10,000 per week, GLM-5.3
multipliers) — one credit unit equals one z.ai credit. For the Pro plan
($80/mo), set `credits_per_session: 12000` and `sliding_window_credits:
60000`; for Lite's weights on GLM-5.3-Flash, set the multipliers to
2.3/0.56/8 (÷10,000). Request-count limits remain on top of the credit
budgets and may need loosening (the defaults, 10/minute and 100/3h, are
stricter than z.ai's plan semantics).

## Redis layout

- `oi-rate-limiter:{namespace}:{user_id}` — request log (unchanged from 0.4.0)
- `oi-rate-limiter:{namespace}:{user_id}:credits` — credit records; sorted-set
  members are `{timestamp}:{uuid}:{uncached_in}:{cached_in}:{out}:{charged}`
  so raw splits survive for reporting and re-derivation
- `oi-rate-limiter:{namespace}:{user_id}:holds` — admission holds; members are
  `{timestamp}:{uuid}:{amount}`, TTL-bounded
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
