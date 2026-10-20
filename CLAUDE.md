# CLAUDE.md

Open WebUI rate limiter: a filter (`rate-limiter.py`) enforcing a per-user
concurrency limit + fractional credit budgets (z.ai GLM Coding Plan
semantics), and an admin tool (`credit-admin-tool.py`) for querying,
adjusting, and clearing usage. Full user documentation is in README.md —
read it for valves, pricing math, and design rationale. This file carries
what an agent needs operationally and the traps that cost debugging time.

## Testing

```bash
python3 test-rate-limiter.py        # filter suite
python3 test-credit-admin-tool.py   # admin tool suite
```

Plain scripts, no framework — `check(name, cond)` counters, exit 1 on any
failure. Both must pass before committing. Tests exec the source files via
`importlib` and run against `fakeredis`; Open WebUI's user repository is
mocked (both sync v0.8-style and async v0.9+ APIs).

fakeredis does not implement Lua `eval` — anything needing atomicity (e.g.
check-and-add admission) cannot be tested with scripts; we accept the race
instead.

## The live deployment (important)

- Runs at https://scrp-chat.econ.cuhk.edu.hk / scrp-chat-2, backed by
  Postgres and Redis (Valkey).
- **The running Open WebUI is v0.9.6** (verified 2026-10-20 via
  `GET /api/version` -> `{"version":"0.9.6"}`; the deployed behavior also
  matches v0.9.6 source, see hook wiring below). `oi-launch.sh`'s image tag
  (v0.8.10) is stale — never trust the launch script. The DB's
  `alembic_version` (461111b60977) does NOT distinguish 0.9.6 from 0.11.x —
  that migration is a head in both; `/api/version` is the direct check.
  Diffing deployed `function`/`tool` content against local files confirms
  what filter/tool code is actually running.
- Postgres (reachable from login nodes): `scrp-control-2:5432`, db
  `openwebui_test`, user `openwebui_user`, password in `oi-launch.sh`. The
  `function` and `tool` tables hold the deployed source of filters/tools —
  diff against local files to confirm what's actually running.
- Deployment = paste the .py into the function/tool in the Open WebUI UI
  (Workspace → Functions/Tools). Editing reloads the module. There is no
  file-based deploy.
- Open WebUI made its whole user repository `async` in v0.9.0. Code calling
  `Users.*` must handle both sync and coroutine returns (see
  `_resolve_user`'s `run()` helper in credit-admin-tool.py).

## Open WebUI valve-editor quirks (all regression-tested)

1. A pydantic `Enum` valve does NOT render as a dropdown — it serializes as
   a `$ref` with no inline title and shows up as an untitled text box.
   Dropdowns require a plain `str` field with
   `json_schema_extra={"input": {"type": "select", "options": [...]}}`.
2. A select field's `description` is passed as the dropdown's placeholder,
   which renders as a duplicated disabled first item. Select valves must
   carry `{value, label}` option objects and no description.
3. Open WebUI ALWAYS attaches a `UserValves` instance to `__user__` — with
   field defaults for users who never saved anything. A UserValves field
   with a non-None default therefore permanently shadows the filter valve.
   Optional per-user overrides must default to None ("Default" in the UI).

`__user__` is a flat `model_dump()` dict: id/name/email/role at top level
(verified v0.8.10 and v0.11.0). Filters may define `async def` hooks; the
middleware awaits them.

## Redis layout (namespace = rate_limiter_id, else model id)

- `oi-rate-limiter:{ns}:{user}:inflight` — concurrency slots; members
  `{chat_id}:{message_id}` (web UI, exact release) or `{ts}:{uuid}` (bare
  API, FIFO release); score = last activity; TTL = silence window
- `oi-rate-limiter:{ns}:{user}:credits` — `{ts}:{uuid}:{uncached}:{cached}:{out}:{charged}`,
  charged is fractional (6 decimals); whole amounts serialize as integers
  so legacy records coexist
- `oi-rate-limiter:{ns}:{user}:holds` — `{ts}:{uuid}:{amount}`, fractional
- `oi-rate-limiter:{ns}:{user}:done:{chat}:{message}` — 15-min markers
  deduplicating stream/outlet double-recording

## Hook wiring (verified against v0.9.6 middleware source AND live probes)

- `inlet`: concurrency check → credit check → admit slot (+ hold for
  streaming, estimate for non-streaming API calls)
- `stream`: per-chunk liveness refresh of the in-flight slot (runs even
  with credits disabled); usage-bearing final chunk records exact charge,
  releases the slot, sets done marker
- `outlet`: marker-gated release + exact usage fallback. **v0.9.6 runs the
  outlet ONLY for web UI (event-emitter) flows — bare API calls
  (`/api/chat/completions` and `/api/v1/messages` alike) have NO outlet
  path at all** (ENABLE_API_OUTLET_FILTERS first appears in v0.11.x). API
  slots/holds are therefore released only by the stream hook's
  usage-bearing final chunk, else the TTLs.
- Inlet exceptions on API paths always surface as **HTTP 400**
  `{"detail": ...}` — `process_chat` wraps every filter exception as 400,
  never 429 — so Claude Code/OpenCode treat limiter rejections as
  non-retryable and fail the turn.
- A request admitted then rejected by the BACKEND (e.g. vLLM 400) fires no
  release hook: its slot leaks for the full silence TTL and its hold for
  hold_ttl. Client disconnect mid-stream likewise fires NO hook. The
  failure direction is deliberately over-admission, never lockout.
- `/api/v1/messages` (Anthropic Messages API, used by Claude Code) exists
  in v0.9.6: main.py converts the payload to OpenAI format, routes through
  the same `chat_completion` pipeline (all filters apply), then converts
  the OpenAI SSE stream back to Anthropic events — filter stream hooks see
  the OpenAI-format chunks, before conversion.
- Known v0.9.6 conversion traps hitting Claude Code specifically (verified
  live 2026-10-20): `convert_anthropic_to_openai_payload` passes an empty
  `tools: []` through to vLLM, which 400s — every such helper request
  (Claude Code sends them for topic/command detection) leaks a slot for
  `in_flight_ttl_minutes` and a hold for `hold_ttl_minutes` (the filter
  strips `tools: []` at inlet since v1.1.4, neutralizing the leak); and the
  stream converter drops `reasoning_content` and reports `input_tokens: 0`
  in `message_start`. The `tools: []` passthrough is still present in the
  v0.11.0 source — upgrading does not fix it.
- Verify Open WebUI behavior by fetching the specific tag's source, e.g.
  `https://raw.githubusercontent.com/open-webui/open-webui/v0.9.6/backend/open_webui/utils/middleware.py`

## Conventions

- Releases bump the frontmatter `version:` in both files; commits are
  conventional-style one-liners with detail paragraphs.
- Frontmatter: `author: Vinci Chow`, `author_url: https://github.com/ticoneva`.
- All error paths in the admin tool return strings, never raise — the model
  reads them in chat.
- z.ai Lite weights: input 6.9, cached 1.7, output 24 (per 10K tokens) —
  defaults in the Valves class mirror the plan (2000/5h, 10000/week).
