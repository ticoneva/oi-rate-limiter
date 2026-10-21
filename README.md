# Open WebUI Rate Limiter

A filter for [Open WebUI](https://github.com/open-webui/open-webui) that enforces
a per-user concurrency limit and credit budgets (weighted token limits).

## How many credits does my usage cost?

With the default (z.ai GLM Coding Plan Lite) weights, every **10,000 tokens** consumed costs:

| What | Credits |
|---|---|
| Input the model reads for the **first time** | **6.9** |
| Input served from **cache** (prefix cache / LMCache hits) | **1.7** |
| Output the model **writes** — *including thinking* | **24** |

Equivalently, one credit buys ~1,450 fresh input tokens, ~5,900 cached
input tokens, or ~420 output tokens.

Rules of thumb:

1. **Output costs 3.5× input, thinking included.** A long reasoning trace
   before a short answer can cost more than the question's context.
2. **Continuing a conversation is much cheaper than starting one** —
   repeated history hits cache (1.7 vs 6.9 per 10K tokens).
3. **Budgets in plain terms**: the 2,000-credit session is ~830K output
   tokens or ~2.9M fresh input tokens; the 10,000-credit week is ~4.2M
   output or ~14.5M fresh input tokens.

Typical coding scenarios:

| Scenario | ~Credits |
|---|---|
| Quick chat question (few hundred tokens) | ~0.1–0.3 (fractional, accumulates) |
| One coding exchange in a warm session (10K cached context + 800 output) | 3–4 |
| Fresh 100K-token context (new repo, first read) | ~70 |
| Heavy agentic hour (~150 calls × 8 credits) | ~1,200 |

So the session budget supports roughly 1.5–2 hours of full-speed agentic
coding per 5-hour window, and the weekly budget 8–9 hours of it — before
cache hits, which in practice stretch it considerably.

## Why Redis?

Open WebUI can run with **multiple workers**, each handling requests independently. A rate limiter that stores counts in memory (e.g. a Python `dict`) only knows about the requests it has personally seen. The same user hitting two different workers would never exceed the limit, even though they've made twice as many requests.

This rate limiter uses **Redis sorted sets** as a shared, centralized counter so all workers agree on how many requests each user has in flight, and how many credits they have consumed, and limits are enforced consistently regardless of which worker processes a request.

## Two kinds of limits

**Concurrency** caps how many of a user's requests may be *in flight* at once
(simultaneously admitted, not yet finished) — mirroring how coding-plan
providers actually limit request traffic.

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

Charges are stored **fractionally** (full precision, 6 decimals) — a short
prompt that costs 0.141 credits records 0.141, so many small requests
accumulate exactly instead of each rounding away to zero. Rounding happens
only at display time, and the budget comparison uses the exact sum.

Because credits are charged with each filter instance's own weights **at write
time**, models with different pricing can share one budget via the same
`rate_limiter_id` — a budget of 1,000,000 credits means 1M *base-model
equivalent* tokens, not 1M raw tokens across mixed models.

### Concurrency limit (in-flight slots)

`max_concurrent_requests` (default 10) caps how many of a user's requests may
run simultaneously per namespace. A request takes a **slot** at admission and
frees it when its usage is recorded (stream final chunk, or the outlet hook
for non-streaming turns). The design's core constraint is that end-of-request
signals are not guaranteed — Open WebUI skips the outlet hook when a client
disconnects mid-stream or the upstream errors — so slots must recover on
their own:

- **Liveness refresh**: every streamed chunk bumps the request's slot, so a
  live stream holds its slot for as long as it generates, however long that is.
- **Silence TTL** (`in_flight_ttl_minutes`, default 5): a slot silent for
  this long is treated as dead and pruned. A cancelled or errored request
  stops producing chunks, so its slot frees within minutes — the TTL is a
  *silence window*, not a maximum request duration.

The failure direction is chosen deliberately: on the odd edge (a genuinely
silent-but-alive request, e.g. a very long time-to-first-token), a slot may
be pruned early and the user briefly exceeds the cap — the limit *softens*
rather than locking anyone out. A leaked slot can never strand a user,
because it self-expires in minutes.

Slots are identified by the turn's `(chat_id, message_id)` when the web UI
supplies them, giving exact release; bare API calls get a per-request **turn
id** the inlet mints and stashes into the request-scoped metadata dict
(`oi_rate_limiter_turn_id`), which Open WebUI threads to the stream and
outlet hooks — so API turns are also exactly releasable (FIFO remains only
when no metadata dict reaches the hooks). Two smaller caveats: admission
check-then-add is not atomic, so a burst of near-simultaneous inlets can
exceed the cap by one or two (the same accepted race the credit holds have);
and API-path outlet accounting relies on Open WebUI's
`ENABLE_API_OUTLET_FILTERS` (v0.11+ only, default on; v0.9.x has no API
outlet at all) — without it, API slots are recovered solely by the stream
hook's usage-bearing final chunk or the silence TTL, and non-streaming API
turns keep their inlet-time estimate charge.
The admin tool's `clear_credit_usage` also deletes in-flight slots, the
manual escape hatch if a user ever reports being stuck.

### Where usage is counted

| Call type | Enforcement | Token accounting |
|---|---|---|
| Streaming API (`/api/chat/completions`, `/api/v1/messages`) | `inlet` check | `stream` hook records actual usage from the final SSE chunk |
| Non-streaming API | `inlet` check | `inlet` records an estimated input-token charge; when the outlet hook fires (v0.12+ with `ENABLE_API_OUTLET_FILTERS`) it is replaced by exact usage |
| Web UI streaming | `inlet` check | `stream` hook records actuals |
| Web UI non-streaming | `inlet` check | `outlet` hook records actuals, deduplicated against the stream hook via a short-lived marker |
| Internal task calls (title/tag generation) | not counted — they bypass chat filters in Open WebUI 0.8.x | |

The filter injects `stream_options: {include_usage: true}` into streaming
requests so OpenAI-compatible backends (vLLM, etc.) report usage in the final
chunk; a client-supplied `stream_options` is preserved.

It also strips an empty `tools: []` from request bodies. Open WebUI's
Anthropic-compatible endpoint (`/api/v1/messages`, used by Claude Code)
passes the array through to OpenAI-compatible backends, which reject it
with a 400 *after* the request was admitted — leaking an in-flight slot
(until the silence TTL) and a credit hold (until `hold_ttl_minutes`).
Claude Code's background helper calls (topic/command detection) are the
known trigger; an empty tools list is semantically identical to no tools,
so removing it is always safe.

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
time (showing where the user stands before the new request is charged),
**ticks live while a response streams** (every `status_interval_seconds`,
adding an estimate of the in-flight response's decode cost from its generated
content), and is refreshed after a web-UI turn completes. Users can hide the
line for themselves via the filter's user valves — hiding it does not affect
enforcement. API clients ignore status events entirely.

The `status_unit` valve (filter-wide, with a per-user override in the user
valves) picks the unit and precision of the amounts. It is a dropdown in the
valve editor, with the explanation in the option labels. Three Open WebUI
editor quirks shape it: a pydantic `Enum` field renders as an untitled text
box (not a dropdown); a select field's `description` is shown as the
dropdown's placeholder — i.e. duplicated as its first item — so select
valves carry `{value, label}` options and no description; and Open WebUI
always attaches a `UserValves` instance with field defaults to users who
never saved their own, so the per-user override must default to None
("Default" in the editor) rather than any unit, or it would permanently
shadow the filter-wide valve:

| Unit | Example | Use |
|---|---|---|
| `credits` (default) | `0.14/2000 session` | Whole credits; two decimals below 100 so light usage is still visible |
| `milli-credits` | `141 mC/2M mC session` | Thousandths — a single short prompt's effect is visible; for debugging |
| `raw` | `0.141/2000 session` | The unrounded fractional value |

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
| `max_concurrent_requests` | 10 | Max simultaneous in-flight requests per user (None disables) |
| `in_flight_ttl_minutes` | 5 | Silence window after which an unresponsive in-flight slot is released |
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
| `status_unit` | credits | Display unit in the status line: credits, milli-credits (thousandths), or raw |
| `status_interval_seconds` | 5 | Minimum interval between live status updates during streaming |
| `enabled_for_admins` | True | Whether admins are also rate-limited |
| `redis_url` | `redis://redis-valkey:6379/0` | Redis connection URL |
| `rate_limiter_id` | (empty) | Namespace for this limiter; leave empty to use the model ID, or set the same ID on multiple instances to share a combined budget |

Deploying the filter with default valves enforces z.ai's GLM Coding Plan
**Lite** allowances (2,000 credits per 5 hours, 10,000 per week, GLM-5.3
multipliers) — one credit unit equals one z.ai credit. For the Pro plan
($80/mo), set `credits_per_session: 12000` and `sliding_window_credits:
60000`; for Lite's weights on GLM-5.3-Flash, set the multipliers to
2.3/0.56/8 (÷10,000).

## Redis layout

- `oi-rate-limiter:{namespace}:{user_id}:inflight` — in-flight request slots;
  sorted-set members are `{chat_id}:{message_id}` (web UI turns) or
  `api:{turn_id}` (bare API calls; the inlet mints the turn id and stashes
  it in the request metadata), scored by
  last activity and TTL-bounded by the silence window
- `oi-rate-limiter:{namespace}:{user_id}:credits` — credit records; sorted-set
  members are `{timestamp}:{uuid}:{uncached_in}:{cached_in}:{out}:{charged}`
  so raw splits survive for reporting and re-derivation
- `oi-rate-limiter:{namespace}:{user_id}:holds` — admission holds; members are
  `{timestamp}:{uuid}:{amount}`, TTL-bounded
- `oi-rate-limiter:{namespace}:{user_id}:done:{chat_id}:{message_id}` —
  15-minute markers preventing stream/outlet double-counting; bare API turns
  use `done:api-turn:{turn_id}`
- `oi-rate-limiter:{namespace}:{user_id}:est:{turn_id}` — short-lived pointer
  from a bare API turn to the credit record holding its inlet-time estimate,
  so the outlet can replace the estimate with exact usage

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

## Admin tool

`credit-admin-tool.py` is a companion Open WebUI **tool** (not a filter). It
reads the same Redis data the filter writes, so point its `redis_url` at the
same instance.

- **`query_credit_usage(user="", namespace="")`** — per-namespace summary of
  a user's consumption: total charged, token split (uncached/cached input,
  output, cache-hit rate), and credit totals for the session (5h), weekly,
  and all-time windows. The session and weekly lines each report usage as
  `used of budget (percent)` against the tool's `credits_per_session` and
  `sliding_window_credits` valves — keep them equal to the filter's valves
  of the same names (the window-length valves too) for exact percentages,
  or set a limit to None to report that window without a budget. `user`
  accepts a user ID, email, or name. Names are
  resolved case-insensitively; an exact name match wins over substring
  siblings, and a name matching multiple users returns an error listing the
  candidates instead of guessing.
- **`adjust_credit_usage(user, amount, reason="", namespace="", confirm=false)`** —
  admin-only. Grants (positive) or deducts (negative) credits by appending a
  signed manual-adjustment record to the same credit ledger the filter sums.
  The user's remaining budget changes immediately; the record keeps a
  sanitized copy of `reason` and shows up in usage reports as a "manual
  adjustments" line. Scoped to one namespace or all of the user's namespaces.
- **`clear_credit_usage(user, namespace="", confirm=false)`** — admin-only.
  Deletes the user's credit records (and pending holds and in-flight slots)
  for one namespace or all, resetting their budget and freeing their
  concurrency slots. Requires `confirm=true` to act, so a model
  misunderstanding cannot wipe data. This is also the escape hatch if a user
  ever reports being stuck at the concurrency limit.

Permissions: **admins** may query, adjust, and clear anyone's usage (an empty
`user` means themselves). **Everyone else** may only **query their own**
usage — leave `user` empty (or pass their own ID); naming any other account
is refused without confirming whether it exists. `adjust` and `clear` are
admin-only in all cases.

All failures return as error messages rather than exceptions. Being a tool,
it is invoked by the model when someone asks in chat ("how many credits do I
have left?" / "how many credits has Alice used this week?"); give the model
the tool via Workspace → Tools. Restrict tool access to admins if you don't
want regular users asking about their own usage via chat, or leave it
accessible to let users self-serve.

User lookup works against both Open WebUI generations — the sync user
repository of v0.8.x and the async one of v0.9+ — and any backend lookup
failure is returned as an error message rather than silently querying Redis
with an unresolved name.

## Testing

`test-rate-limiter.py` runs the filter against `fakeredis` with all three
hooks simulated (web-UI dict chunks, API raw-SSE chunks, outlet payloads) and
shared-state multi-instance scenarios:

```bash
pip install --user fakeredis pydantic
python3 test-rate-limiter.py
python3 test-credit-admin-tool.py
```

## License

MIT
