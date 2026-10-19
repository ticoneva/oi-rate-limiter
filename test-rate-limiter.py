"""Tests for rate-limiter.py simulating the Open WebUI filter runtime.

Covers: the concurrency limit (admission, release on stream/outlet, marker
dedup, liveness refresh, silence expiry, FIFO fallback for bare API calls,
shared namespaces), credit budgets, weight math, cache-hit discount,
stream-hook formats (dict / raw SSE bytes), outlet dedup via done markers,
non-streaming input estimation, max_tokens capping, and multi-worker
consistency via fakeredis shared state.

Run: python3 test-rate-limiter.py
"""

import asyncio
import importlib.util
import json
import os
import sys
import time
import uuid

import fakeredis
import pydantic

_spec = importlib.util.spec_from_file_location(
    "rate_limiter", os.path.join(os.path.dirname(os.path.abspath(__file__)), "rate-limiter.py")
)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
Filter = _mod.Filter

PASS = 0
FAIL = 0


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS: {name}")
    else:
        FAIL += 1
        print(f"  FAIL: {name} {detail}")


class TestEventEmitter:
    def __init__(self):
        self.events = []

    async def __call__(self, event):
        self.events.append(event)


def make_filter(**valves):
    f = Filter()
    shared = fakeredis.FakeStrictRedis(decode_responses=True)
    f._redis = shared
    # Base: neutral weights and all credit limits disabled. Tests that
    # exercise credit behavior set their own limits; the z.ai Lite plan
    # defaults live in the Valves class and are covered separately.
    base = dict(
        input_weight=1.0,
        cached_input_weight=1.0,
        output_weight=1.0,
        credits_per_minute=None,
        credits_per_session=None,
        sliding_window_credits=None,
        max_concurrent_requests=None,
    )
    base.update(valves)
    f.valves = f.Valves(**base)
    return f


def user(uid="u1", role="user"):
    return {"id": uid, "role": role, "email": "x@y.z"}


MODEL = {"id": "glm-5.3", "info": {}}


def run_inlet(f, body, metadata=None, model=MODEL, u=None, emitter=None):
    return asyncio.run(
        f.inlet(
            body,
            emitter if emitter is not None else TestEventEmitter(),
            __user__=u or user(),
            __model__=model,
            __metadata__=metadata or {},
        )
    )


def run_stream(f, chunk, metadata=None, model=MODEL, u=None, emitter=None):
    async def _call():
        return await f.stream(
            chunk,
            __event_emitter__=emitter,
            __user__=u or user(),
            __model__=model,
            __metadata__=metadata or {},
        )

    return asyncio.run(_call())


def run_outlet(f, body, model=MODEL, u=None):
    return asyncio.run(f.outlet(body, __user__=u or user(), __model__=model))


def vllm_final_chunk(prompt_tokens, completion_tokens, cached_tokens=None):
    usage = {
        "prompt_tokens": prompt_tokens,
        "total_tokens": prompt_tokens + completion_tokens,
        "completion_tokens": completion_tokens,
    }
    if cached_tokens is not None:
        usage["prompt_tokens_details"] = {"cached_tokens": cached_tokens}
    return json.dumps({"id": "x", "choices": [], "usage": usage})


def vllm_final_chunk_dict(prompt_tokens, completion_tokens, cached_tokens=None):
    return json.loads(vllm_final_chunk(prompt_tokens, completion_tokens, cached_tokens))


print("== 1. Concurrency limit ==")
f = make_filter(max_concurrent_requests=2)
meta = {"chat_id": "c0", "message_id": "m1"}
for i in range(2):
    run_inlet(f, {"messages": [{"role": "user", "content": "hi"}]}, metadata={"chat_id": "c0", "message_id": f"m{i}"})
blocked = None
try:
    run_inlet(f, {"messages": [{"role": "user", "content": "hi"}]}, metadata={"chat_id": "c0", "message_id": "m2"})
    blocked = False
except Exception as e:
    blocked = "Concurrency limit reached" in str(e)
check("3rd concurrent request blocked", blocked)
check(
    "no credits recorded when limits disabled",
    not [k for k in f._get_redis().keys() if k.endswith(":credits")],
)
# key format: reconstructible member {chat}:{message}
key = "oi-rate-limiter:glm-5.3:u1:inflight"
check(
    "in-flight members are chat:message ids",
    sorted(f._get_redis().zrange(key, 0, -1)) == ["c0:m0", "c0:m1"],
    f._get_redis().zrange(key, 0, -1),
)

print("== 2. Credit limit blocking via stream usage ==")
f = make_filter(
    sliding_window_credits=1000,
)
run_inlet(f, {"stream": True, "messages": []})
out = run_stream(f, vllm_final_chunk_dict(prompt_tokens=400, completion_tokens=100))
check("stream returns dict chunk unchanged", out.get("usage", {}).get("prompt_tokens") == 400)
run_inlet(f, {"stream": True, "messages": []})
run_stream(f, vllm_final_chunk_dict(prompt_tokens=400, completion_tokens=100))
blocked = None
try:
    run_inlet(f, {"stream": True, "messages": []})
    blocked = False
except Exception as e:
    blocked = "Credit limit exceeded" in str(e)
check("blocked after exceeding credit budget", blocked)

print("== 3. Weights: prefill/decode differential pricing ==")
f = make_filter(
    sliding_window_credits=10000, input_weight=1.0, output_weight=4.0,
)
run_inlet(f, {"stream": True, "messages": []})
run_stream(f, vllm_final_chunk_dict(prompt_tokens=1000, completion_tokens=500))
key = [k for k in f._get_redis().keys() if k.endswith(":credits")][0]
members = f._get_redis().zrange(key, 0, -1)
# 1000*1.0 + 500*4.0 = 3000
check("weighted charge = 3000", members[0].rsplit(":", 1)[1] == "3000", members[0])

print("== 4. Cache-hit discount ==")
f = make_filter(
    sliding_window_credits=10000, input_weight=1.0, cached_input_weight=0.2,
    output_weight=1.0,
)
run_inlet(f, {"stream": True, "messages": []})
run_stream(
    f, vllm_final_chunk_dict(prompt_tokens=5000, completion_tokens=1000, cached_tokens=4200)
)
key = [k for k in f._get_redis().keys() if k.endswith(":credits")][0]
members = f._get_redis().zrange(key, 0, -1)
# uncached 800*1.0 + cached 4200*0.2 + 1000*1.0 = 2640
check(
    "cache-discounted charge = 2640",
    members[0].rsplit(":", 1)[1] == "2640",
    members[0],
)
# member keeps the raw split for reporting
check(
    "member stores raw split",
    members[0].rsplit(":", 6)[2:] == ["800", "4200", "1000", "2640"],
    members[0],
)

print("== 5. No cached_tokens reported -> full prefill charged ==")
f = make_filter(
    sliding_window_credits=10000, cached_input_weight=0.0,
)
run_inlet(f, {"stream": True, "messages": []})
run_stream(f, vllm_final_chunk_dict(prompt_tokens=5000, completion_tokens=1000))
key = [k for k in f._get_redis().keys() if k.endswith(":credits")][0]
members = f._get_redis().zrange(key, 0, -1)
check(
    "missing cached_tokens charges full prefill",
    members[0].rsplit(":", 1)[1] == str(5000 + 1000),
    members[0],
)

print("== 6. Raw SSE line (API path) ==")
f = make_filter(
    sliding_window_credits=10000,
)
line = f"data: {vllm_final_chunk(prompt_tokens=300, completion_tokens=50)}\n\n".encode()
out = run_stream(f, line)
check("bytes returned unchanged", out == line)
key = [k for k in f._get_redis().keys() if k.endswith(":credits")][0]
check("usage recorded from raw SSE", f._get_redis().zcard(key) == 1)

# str not bytes
line_str = f"data: {vllm_final_chunk(prompt_tokens=300, completion_tokens=50)}\n\n"
out = run_stream(f, line_str)
check("str returned unchanged", out == line_str)

# [DONE] and non-data lines pass through
out = run_stream(f, b"data: [DONE]\n\n")
check("[DONE] passes through", out == b"data: [DONE]\n\n")

print("== 7. Normalized / Ollama usage formats ==")
f = make_filter(
    sliding_window_credits=10000,
)
run_stream(f, {"usage": {"input_tokens": 100, "output_tokens": 20, "total_tokens": 120}})
run_stream(
    f,
    (
        "data: "
        + json.dumps(
            {
                "choices": [],
                "usage": {"prompt_eval_count": 40, "eval_count": 10},
            }
        )
        + "\n\n"
    ).encode(),
)
key = [k for k in f._get_redis().keys() if k.endswith(":credits")][0]
members = f._get_redis().zrange(key, 0, -1)
check("two records recorded", len(members) == 2, members)

print("== 8. Outlet: web UI non-streaming turn ==")
f = make_filter(
    sliding_window_credits=10000,
)
out = run_outlet(
    f,
    {
        "model": "glm-5.3",
        "chat_id": "c1",
        "id": "m1",
        "messages": [
            {"id": "a", "role": "user", "content": "hi"},
            {
                "id": "m1",
                "role": "assistant",
                "content": "hello",
                "usage": {"input_tokens": 200, "output_tokens": 30},
            },
        ],
    },
)
key = [k for k in f._get_redis().keys() if k.endswith(":credits")][0]
check("outlet records assistant usage", f._get_redis().zcard(key) == 1)

print("== 9. Outlet dedup against stream (done marker) ==")
f = make_filter(
    sliding_window_credits=10000,
)
meta = {"chat_id": "c1", "message_id": "m1"}
run_stream(f, vllm_final_chunk_dict(prompt_tokens=100, completion_tokens=20), metadata=meta)
run_outlet(
    f,
    {
        "model": "glm-5.3",
        "chat_id": "c1",
        "id": "m1",
        "messages": [
            {
                "id": "m1",
                "role": "assistant",
                "content": "x",
                "usage": {"input_tokens": 100, "output_tokens": 20},
            }
        ],
    },
)
key = [k for k in f._get_redis().keys() if k.endswith(":credits")][0]
check("stream + outlet = single record", f._get_redis().zcard(key) == 1)

print("== 10. Non-streaming API estimate ==")
f = make_filter(
    sliding_window_credits=100000, chars_per_token=3.0,
)
body = {"messages": [{"role": "user", "content": "a" * 300}]}
run_inlet(f, body, metadata={})  # no session_id/chat_id -> API client
key = [k for k in f._get_redis().keys() if k.endswith(":credits")][0]
members = f._get_redis().zrange(key, 0, -1)
check(
    "estimate = 100 credits",
    len(members) == 1 and members[0].rsplit(":", 1)[1] == "100",
    members,
)
# web UI call (session_id present) is not estimated
f2 = make_filter(
    sliding_window_credits=100000, chars_per_token=3.0,
)
run_inlet(f2, {"messages": [{"role": "user", "content": "a" * 300}]},
          metadata={"session_id": "s1", "chat_id": "c1"})
check(
    "web UI non-streaming not estimated",
    not [k for k in f2._get_redis().keys() if k.endswith(":credits")],
)

print("== 11. max_tokens capping ==")
f = make_filter(
    sliding_window_credits=1000, output_weight=4.0, cap_max_tokens_to_budget=True,
)
run_inlet(f, {"stream": True, "messages": [], "max_tokens": 400})
run_stream(f, vllm_final_chunk_dict(prompt_tokens=100, completion_tokens=100))
body = {"stream": True, "messages": [], "max_tokens": 400}
run_inlet(f, body)
# used 100*1 + 100*4 = 500 credits; remaining 500; 500/4 = 125
check("max_tokens clamped to remaining/output_weight", body["max_tokens"] == 125, body)

f3 = make_filter(
    sliding_window_credits=1000, output_weight=4.0, cap_max_tokens_to_budget=True,
)
body = {"stream": True, "messages": []}  # no max_tokens at all
run_inlet(f3, body)
check("max_tokens injected when absent", body.get("max_tokens") == 250, body)

print("== 12. Shared namespace, per-instance weights ==")
shared_server = fakeredis.FakeServer()
fa = Filter()
fa._redis = fakeredis.FakeStrictRedis(server=shared_server, decode_responses=True)
fa.valves = fa.Valves(
    sliding_window_credits=1000, rate_limiter_id="pool", input_weight=1.0,
    output_weight=1.0,
)
fb = Filter()
fb._redis = fakeredis.FakeStrictRedis(server=shared_server, decode_responses=True)
fb.valves = fb.Valves(
    sliding_window_credits=1000, rate_limiter_id="pool", input_weight=1.0,
    output_weight=3.0,
)
# model A writes 100 in + 100 out = 200 charged
run_stream(
    fa, vllm_final_chunk_dict(prompt_tokens=100, completion_tokens=100),
    model={"id": "model-a", "info": {}},
)
# model B writes 100 in + 100 out = 100 + 300 = 400 charged
run_stream(
    fb, vllm_final_chunk_dict(prompt_tokens=100, completion_tokens=100),
    model={"id": "model-b", "info": {}},
)
key = f"oi-rate-limiter:pool:u1:credits"
members = fa._get_redis().zrange(key, 0, -1)
charges = sorted(m.rsplit(":", 1)[1] for m in members)
check("both models write to shared key", len(members) == 2, members)
check("per-instance weights baked into charge", charges == ["200", "400"], charges)

# B's check against the shared budget sees 600 total
limited, wait, used, limit = fb.credit_limited("u1", "model-b")
check("shared sum correct across instances", used == 600 and not limited, used)

print("== 13. Multi-worker consistency (shared redis state) ==")
# fa/fb above already share one redis; verify both see the same count
check(
    "all workers read the same records",
    fa._sum_credits(key, 0, "+inf") == fb._sum_credits(key, 0, "+inf") == 600,
)

print("== 14. Admin bypass and enabled_for_admins ==")
f = make_filter(
    sliding_window_credits=10, enabled_for_admins=False,
)
run_stream(
    f, vllm_final_chunk_dict(prompt_tokens=100, completion_tokens=100),
    u=user("admin1", role="admin"),
)
run_inlet(
    f, {"stream": True, "messages": []},
    u=user("admin1", role="admin"),
)
check(
    "admin not recorded when enabled_for_admins=False",
    not [k for k in f._get_redis().keys() if k.endswith(":credits")],
)

f = make_filter(
    sliding_window_credits=1000,
)
run_stream(
    f, vllm_final_chunk_dict(prompt_tokens=100, completion_tokens=100),
    u=user("admin2", role="admin"),
)
check(
    "admin recorded when enabled_for_admins=True (default)",
    [k for k in f._get_redis().keys() if k.endswith(":credits")],
)

# Admin bypass on concurrency: no admission, no release, no tracking.
f = make_filter(max_concurrent_requests=1, enabled_for_admins=False)
for i in range(3):
    run_inlet(
        f, {"messages": []},
        u=user("admin3", role="admin"),
        metadata={"chat_id": "ca", "message_id": f"m{i}"},
    )
check(
    "admin bypass: no in-flight slots taken",
    not [k for k in f._get_redis().keys() if k.endswith(":inflight")],
)

print("== 15. stream_options injection ==")
f = make_filter(
)
body = {"stream": True, "messages": []}
run_inlet(f, body)
check("injects stream_options", body.get("stream_options") == {"include_usage": True})
body = {"stream": True, "messages": [], "stream_options": {"include_usage": False}}
run_inlet(f, body)
check(
    "client stream_options preserved",
    body["stream_options"] == {"include_usage": False},
    body,
)
body = {"stream": False, "messages": []}
run_inlet(f, body)
check("no injection when not streaming", "stream_options" not in body)

print("== 16. All credit limits disabled -> no recording overhead ==")
f = make_filter(
)
run_stream(f, vllm_final_chunk_dict(prompt_tokens=100, completion_tokens=100))
check(
    "no records when no credit limit set",
    not [k for k in f._get_redis().keys() if k.endswith(":credits")],
)

print("== 17. In-flight key format (bare API call) ==")
f = make_filter()
run_inlet(f, {"messages": []})  # no chat/message ids -> uuid fallback member
key = "oi-rate-limiter:glm-5.3:u1:inflight"
members = f._get_redis().zrange(key, 0, -1)
check(
    "uuid fallback member with timestamp",
    len(members) == 1 and members[0].split(":")[1].strip() != ""
    and len(members[0].split(":")) == 2,
    members,
)
check("in-flight key has TTL", 0 < f._get_redis().ttl(key) <= 5 * 60, f._get_redis().ttl(key))

print("== 18. Concurrency enforced independently of credits ==")
f = make_filter(
    max_concurrent_requests=1,
    sliding_window_credits=100000,
)
# first request in flight (not completed): the slot must block the second
run_inlet(f, {"messages": []})
blocked = None
try:
    run_inlet(f, {"messages": []})
    blocked = False
except Exception as e:
    blocked = "Concurrency limit reached" in str(e)
check("concurrency limit enforced with credits on", blocked)

print("== 18b. Slot released when usage records via stream ==")
f = make_filter(max_concurrent_requests=1, sliding_window_credits=100000)
meta = {"chat_id": "c1", "message_id": "m1"}
run_inlet(f, {"stream": True, "messages": []}, metadata=meta)
key = "oi-rate-limiter:glm-5.3:u1:inflight"
check("slot held during flight", f._get_redis().zcard(key) == 1)
run_stream(f, vllm_final_chunk_dict(prompt_tokens=100, completion_tokens=50), metadata=meta)
check("slot freed after usage records", f._get_redis().zcard(key) == 0)
# user can immediately run another request
run_inlet(f, {"stream": True, "messages": []}, metadata={"chat_id": "c1", "message_id": "m2"})
check("new request admitted after release", f._get_redis().zcard(key) == 1)

print("== 18c. Slot released via outlet (non-streaming turn) ==")
f = make_filter(max_concurrent_requests=1, sliding_window_credits=100000)
run_inlet(f, {"messages": []}, metadata={"chat_id": "c2", "message_id": "m1"})
key = "oi-rate-limiter:glm-5.3:u1:inflight"
check("slot held before outlet", f._get_redis().zcard(key) == 1)
run_outlet(
    f,
    {
        "model": "glm-5.3",
        "chat_id": "c2",
        "id": "m1",
        "messages": [
            {
                "id": "m1",
                "role": "assistant",
                "content": "x",
                "usage": {"input_tokens": 100, "output_tokens": 20},
            }
        ],
    },
)
check("outlet frees the slot", f._get_redis().zcard(key) == 0)

print("== 18d. Marker dedup: stream then outlet releases once ==")
f = make_filter(max_concurrent_requests=1, sliding_window_credits=100000)
meta = {"chat_id": "c3", "message_id": "m1"}
run_inlet(f, {"stream": True, "messages": []}, metadata=meta)
run_stream(f, vllm_final_chunk_dict(prompt_tokens=100, completion_tokens=20), metadata=meta)
# m1's slot was released by the stream hook; a second request is now in flight
run_inlet(f, {"stream": True, "messages": []}, metadata={"chat_id": "c3", "message_id": "m2"})
key = "oi-rate-limiter:glm-5.3:u1:inflight"
check("m2 slot held after m1 completed", f._get_redis().zcard(key) == 1)
# outlet for the already-recorded m1 must NOT release again (marker set):
# without the marker it would FIFO-release m2's slot
run_outlet(
    f,
    {
        "model": "glm-5.3",
        "chat_id": "c3",
        "id": "m1",
        "messages": [
            {"id": "m1", "role": "assistant", "content": "x",
             "usage": {"input_tokens": 100, "output_tokens": 20}}
        ],
    },
)
check("outlet after stream-accounted turn does not release m2", f._get_redis().zcard(key) == 1)

print("== 18e. Liveness refresh keeps a long stream's slot alive ==")
f = make_filter(max_concurrent_requests=1, in_flight_ttl_minutes=5)
meta = {"chat_id": "c4", "message_id": "m1"}
run_inlet(f, {"stream": True, "messages": []}, metadata=meta)
key = "oi-rate-limiter:glm-5.3:u1:inflight"
# age the member past the TTL, then send a chunk to refresh it
old = time.time() - 6 * 60
f._get_redis().zadd(key, {"c4:m1": old}, xx=True)
f._inflight_refresh.clear()  # defeat the 60s throttle for the test
run_stream(f, {"id": "x", "choices": [{"delta": {"content": "hello"}}]}, metadata=meta)
score_after = f._get_redis().zscore(key, "c4:m1")
check("chunk bumps the slot score past the silence window", score_after is not None and score_after > old + 5 * 60, score_after)
# XX guard: refreshing an absent member must not create one
f._get_redis().delete(key)
f._inflight_refresh.clear()
run_stream(f, {"id": "x", "choices": [{"delta": {"content": "hi"}}]}, metadata=meta)
check("XX guard: refresh does not create a missing slot", not f._get_redis().exists(key))

print("== 18f. Silence expiry: stale slot stops counting ==")
f = make_filter(max_concurrent_requests=1, in_flight_ttl_minutes=5)
run_inlet(f, {"messages": []}, metadata={"chat_id": "c5", "message_id": "m1"})
key = "oi-rate-limiter:glm-5.3:u1:inflight"
# the request died silently: backdate its score beyond the TTL
old = time.time() - 6 * 60
f._get_redis().zadd(key, {"c5:m1": old}, xx=True)
blocked = None
try:
    run_inlet(f, {"messages": []}, metadata={"chat_id": "c5", "message_id": "m2"})
    blocked = False
except Exception as e:
    blocked = "Concurrency limit reached" in str(e)
check("stale slot pruned, new request admitted", not blocked)

print("== 18g. Bare API call: FIFO fallback release ==")
f = make_filter(max_concurrent_requests=2, sliding_window_credits=100000)
# two API calls with no chat/message identity -> uuid members
run_inlet(f, {"stream": True, "messages": []})
run_inlet(f, {"stream": True, "messages": []})
key = "oi-rate-limiter:glm-5.3:u1:inflight"
check("two uuid slots", f._get_redis().zcard(key) == 2)
# usage records without reconstructible identity -> FIFO release of the oldest
run_stream(f, vllm_final_chunk_dict(prompt_tokens=100, completion_tokens=10))
check("FIFO release frees one uuid slot", f._get_redis().zcard(key) == 1)

print("== 18h. Users and namespaces independent ==")
f = make_filter(max_concurrent_requests=1)
run_inlet(f, {"messages": []}, metadata={"chat_id": "c6", "message_id": "m1"})  # u1 in glm-5.3
# same user, other model namespace -> separate slot pool
run_inlet(f, {"messages": []}, model={"id": "qwen-3.5", "info": {}}, metadata={"chat_id": "c6", "message_id": "m2"})
check(
    "namespaces have separate in-flight sets",
    f._get_redis().zcard("oi-rate-limiter:glm-5.3:u1:inflight") == 1
    and f._get_redis().zcard("oi-rate-limiter:qwen-3.5:u1:inflight") == 1,
)
# other user, same namespace -> not blocked by u1's slot
run_inlet(f, {"messages": []}, u=user("u2"), metadata={"chat_id": "c7", "message_id": "m3"})
check("other user unaffected", f._get_redis().zcard("oi-rate-limiter:glm-5.3:u2:inflight") == 1)
# u1 is now blocked in glm-5.3
blocked = None
try:
    run_inlet(f, {"messages": []}, metadata={"chat_id": "c6", "message_id": "m9"})
    blocked = False
except Exception:
    blocked = True
check("original user still capped", blocked)

print("== 18i. Shared rate_limiter_id shares slots ==")
f = make_filter(max_concurrent_requests=1, rate_limiter_id="shared-pool")
run_inlet(f, {"messages": []}, model={"id": "glm-5.3", "info": {}}, metadata={"chat_id": "c8", "message_id": "m1"})
blocked = None
try:
    # different model id, same rate_limiter_id -> same namespace -> same pool
    run_inlet(f, {"messages": []}, model={"id": "qwen-3.5", "info": {}}, metadata={"chat_id": "c8", "message_id": "m2"})
    blocked = False
except Exception:
    blocked = True
check("shared namespace enforces one combined pool", blocked)
check(
    "shared namespace in-flight key",
    f._get_redis().zcard("oi-rate-limiter:shared-pool:u1:inflight") == 1,
)

print("== 19. Concurrent burst: holds block at the door ==")
# User with empty history fires many simultaneous streaming requests whose
# combined actual cost would exceed the budget. With holds, request k sees
# the k-1 pending estimates and gets blocked before generating.
f = make_filter(
    sliding_window_credits=600, chars_per_token=1.0,  # 300-char prompt = 300 est
)
body = {"stream": True, "messages": [{"role": "user", "content": "a" * 300}]}
admitted = 0
for i in range(10):
    try:
        run_inlet(f, dict(body))
        admitted += 1
    except Exception:
        break
check("burst admitted only twice (600 budget / 300 per hold)", admitted == 2, admitted)

# Each request completes and records its exact usage: 200 in + 100 out = 300
for i in range(admitted):
    run_stream(f, vllm_final_chunk_dict(prompt_tokens=200, completion_tokens=100))
# Redis removes a zset key when its last member is deleted, so a fully
# released hold set means the key is gone entirely.
check(
    "all holds released after usage records",
    not [k for k in f._get_redis().keys() if k.endswith(":holds")],
)
credits_key = [k for k in f._get_redis().keys() if k.endswith(":credits")][0]
members = f._get_redis().zrange(credits_key, 0, -1)
check("exact usage replaced estimates", len(members) == 2, members)

print("== 20. Hold charges estimate, not double ==")
# With all requests completed, the recorded total is the exact charge only.
charges = sorted(m.rsplit(":", 1)[1] for m in members)
check("net charge = exact usage (300 x 2)", charges == ["300", "300"], charges)

print("== 21. Abandoned request: hold expires via TTL ==")
f = make_filter(
    sliding_window_credits=600, chars_per_token=1.0, hold_ttl_minutes=15,
)
run_inlet(f, {"stream": True, "messages": [{"role": "user", "content": "a" * 300}]})
# request dies without reporting usage: TTL removes the hold
holds_key = "oi-rate-limiter:glm-5.3:u1:holds"
ttl = f._get_redis().ttl(holds_key)
check("hold key has TTL", 0 < ttl <= 15 * 60, ttl)
limited, wait, used, lim = f.credit_limited("u1", "glm-5.3")
check("hold counts toward usage while live", used == 300, used)
f._get_redis().delete(holds_key)  # simulate expiry
limited, wait, used, lim = f.credit_limited("u1", "glm-5.3")
check("expired hold stops counting", used == 0, used)

print("== 22. Output reserve in hold ==")
f = make_filter(
    sliding_window_credits=10000, chars_per_token=1.0,
    hold_output_reserve_tokens=100, output_weight=4.0,
)
run_inlet(f, {"stream": True, "messages": [{"role": "user", "content": "a" * 100}]})
members = f._get_redis().zrange("oi-rate-limiter:glm-5.3:u1:holds", 0, -1)
# 100 in x 1.0 + 100 out-reserve x 4.0 = 500
check("hold includes output reserve", members[0].rsplit(":", 1)[1] == "500", members)

print("== 23. Session window (z.ai 5-hour style) ==")
f = make_filter(
    credits_per_session=500, session_window_minutes=300,
    sliding_window_credits=None, chars_per_token=1.0,
)
run_inlet(f, {"stream": True, "messages": [{"role": "user", "content": "a" * 300}]})
run_stream(f, vllm_final_chunk_dict(prompt_tokens=300, completion_tokens=100))  # 400
limited, wait, used, lim = f.credit_limited("u1", "glm-5.3")
check("session window counts usage", used == 400 and not limited, (used, lim))
run_inlet(f, {"stream": True, "messages": [{"role": "user", "content": "a" * 100}]})
run_stream(f, vllm_final_chunk_dict(prompt_tokens=100, completion_tokens=0))  # +100
limited, wait, used, lim = f.credit_limited("u1", "glm-5.3")
check("session limit blocks at 500", limited and used == 500 and lim == 500, (limited, used, lim))
check("wait is bounded by the 5h window", wait is not None and wait <= 300 * 60, wait)

print("== 24. Holds disabled -> original post-hoc behavior ==")
f = make_filter(
    sliding_window_credits=600, chars_per_token=1.0, hold_credits_at_inlet=False,
)
body = {"stream": True, "messages": [{"role": "user", "content": "a" * 300}]}
admitted = 0
for i in range(10):
    try:
        run_inlet(f, dict(body))
        admitted += 1
    except Exception:
        break
check(
    "all burst admitted when holds off",
    admitted == 10 and not [k for k in f._get_redis().keys() if k.endswith(":holds")],
    admitted,
)

print("== 25. max_tokens cap sees holds too ==")
f = make_filter(
    sliding_window_credits=1000, chars_per_token=1.0,
    cap_max_tokens_to_budget=True, hold_output_reserve_tokens=0,
)
body1 = {"stream": True, "messages": [{"role": "user", "content": "a" * 800}]}
run_inlet(f, body1)  # hold 800
body2 = {"stream": True, "messages": [{"role": "user", "content": "a" * 100}]}
run_inlet(f, body2)
# remaining = 1000 - 800 - 100 = 100 -> max_tokens = 100
check("max_tokens cap accounts for sibling holds", body2.get("max_tokens") == 100, body2)

print("== 26. Default valves equal z.ai GLM Coding Plan Lite ==")
v = Filter.Valves()
check("credits_per_session default 2000", v.credits_per_session == 2000, v.credits_per_session)
check("session_window_minutes default 300", v.session_window_minutes == 300, v.session_window_minutes)
check("sliding_window_credits default 10000", v.sliding_window_credits == 10000, v.sliding_window_credits)
check("credit_window_minutes default 10080 (7d)", v.credit_window_minutes == 10080, v.credit_window_minutes)
check("input_weight default 6.9/10000", abs(v.input_weight - 0.00069) < 1e-12, v.input_weight)
check(
    "cached_input_weight default 1.7/10000",
    abs(v.cached_input_weight - 0.00017) < 1e-12,
    v.cached_input_weight,
)
check("output_weight default 24/10000", abs(v.output_weight - 0.0024) < 1e-12, v.output_weight)

# A full z.ai-style request charges the same credits z.ai would.
f = make_filter()  # base has neutral weights; restore z.ai defaults explicitly
f.valves = Filter.Valves()
f2 = make_filter()
f2.valves.input_weight = 0.00069
f2.valves.cached_input_weight = 0.00017
f2.valves.output_weight = 0.0024
f2.valves.credits_per_session = 2000
run_stream(f2, vllm_final_chunk_dict(prompt_tokens=100000, completion_tokens=5000, cached_tokens=90000))
key = [k for k in f2._get_redis().keys() if k.endswith(":credits")][0]
members = f2._get_redis().zrange(key, 0, -1)
# z.ai: (10000*6.9 + 90000*1.7 + 5000*24)/10000 = 34.2 credits — the
# fractional ledger keeps the true value (the old integer rounding
# recorded 34, undercharging by 0.2)
check(
    "z.ai formula reproduced: 34.2 credits",
    members[0].rsplit(":", 1)[1] == "34.2",
    members[0],
)

print("== 27. Usage status display ==")
f = make_filter(
    credits_per_session=2000, sliding_window_credits=10000,
    show_usage_status=True, chars_per_token=1.0,
)
emitter = TestEventEmitter()
run_inlet(f, {"stream": True, "messages": [{"role": "user", "content": "a" * 300}]}, emitter=emitter)
status_events = [e for e in emitter.events if e.get("type") == "status"]
check("status emitted at inlet", len(status_events) == 1, emitter.events)
desc = status_events[0]["data"]["description"] if status_events else ""
# Status shows where the user stands before this request's hold is placed.
check(
    "first request status starts at zero",
    desc == "Credits: 0/2000 session (0.0%) - 0/10K week (0.0%)",
    desc,
)
check("status event marked done", status_events and status_events[0]["data"].get("done") is True)

# after usage records, a new inlet shows updated numbers
run_stream(f, vllm_final_chunk_dict(prompt_tokens=200, completion_tokens=100))  # 300 exact
emitter2 = TestEventEmitter()
run_inlet(f, {"stream": True, "messages": []}, emitter=emitter2)
desc2 = [e for e in emitter2.events if e.get("type") == "status"][0]["data"]["description"]
check(
    "status reflects recorded usage after completion",
    desc2 == "Credits: 300/2000 session (15.0%) - 300/10K week (3.0%)",
    desc2,
)

print("== 28. Status off by default / per-user opt-out ==")
f = make_filter(
    credits_per_session=2000,
)
emitter = TestEventEmitter()
run_inlet(f, {"stream": True, "messages": []}, emitter=emitter)
check(
    "no status when valve off",
    not [e for e in emitter.events if e.get("type") == "status"],
    emitter.events,
)

f = make_filter(
    credits_per_session=2000, show_usage_status=True,
)
# user valve opt-out: __user__["valves"].show_usage_status = False
class UserValvesStub:
    show_usage_status = False

emitter = TestEventEmitter()
run_inlet(
    f, {"stream": True, "messages": []}, emitter=emitter,
    u={"id": "u1", "role": "user", "valves": UserValvesStub()},
)
check(
    "user opt-out suppresses status",
    not [e for e in emitter.events if e.get("type") == "status"],
    emitter.events,
)

# outlet emits refreshed status after recording
f = make_filter(
    credits_per_session=2000, show_usage_status=True,
)
run_stream(f, vllm_final_chunk_dict(prompt_tokens=100, completion_tokens=50))  # 150
emitter = TestEventEmitter()
asyncio.run(
    f.outlet(
        {
            "model": "glm-5.3",
            "chat_id": "c9",
            "id": "m9",
            "messages": [
                {
                    "id": "m9",
                    "role": "assistant",
                    "content": "x",
                    "usage": {"input_tokens": 100, "output_tokens": 50},
                }
            ],
        },
        __event_emitter__=emitter,
        __user__=user(),
        __model__=MODEL,
    )
)
statuses = [e for e in emitter.events if e.get("type") == "status"]
check("outlet emits refreshed status", len(statuses) == 1, emitter.events)
if statuses:
    check(
        "outlet status includes new record",
        "300/2000 session" in statuses[0]["data"]["description"],
        statuses[0]["data"]["description"],
    )

# large numbers format as K
f = make_filter(
    credits_per_session=2000, show_usage_status=True, chars_per_token=1.0,
)
run_stream(f, vllm_final_chunk_dict(prompt_tokens=1500, completion_tokens=0))
msg = f._usage_status_message("u1", "glm-5.3")
check("integer formatting below 10K", msg.startswith("Credits: 1500/2000 session (75.0%)"), msg)

print("== 29. Live status ticks during generation ==")
f = make_filter(
    credits_per_session=2000, show_usage_status=True, chars_per_token=1.0,
    status_interval_seconds=0,  # emit on every chunk for testing
)
meta = {"chat_id": "c1", "message_id": "m1"}
emitter = TestEventEmitter()

# inlet: hold 100
run_inlet(f, {"stream": True, "messages": [{"role": "user", "content": "a" * 100}]}, emitter=emitter, metadata=meta)

# stream several content deltas
for i in range(3):
    run_stream(
        f,
        {"id": "x", "choices": [{"delta": {"content": "b" * 50}}]},
        metadata=meta,
        emitter=emitter,
    )
# each tick: 50/100/150 chars -> 50/100/150 est tokens x weight 1
statuses = [e for e in emitter.events if e.get("type") == "status"]
live = [s for s in statuses if s["data"].get("done") is False]
check("live statuses emitted during stream", len(live) == 3, [s["data"]["description"] for s in live])
if len(live) == 3:
    check(
        "live count grows with generated content",
        live[0]["data"]["description"] == "Credits: 150/2000 session (7.5%)"
        and live[2]["data"]["description"] == "Credits: 250/2000 session (12.5%)",
        [s["data"]["description"] for s in live],
    )

# final chunk records usage and clears live state
run_stream(f, vllm_final_chunk_dict(prompt_tokens=100, completion_tokens=150), metadata=meta, emitter=emitter)
run_stream(f, {"id": "x", "choices": [{"delta": {"content": "b" * 10}}]}, metadata=meta, emitter=emitter)
statuses_after = [e for e in emitter.events if e.get("type") == "status"]
live_after = [s for s in statuses_after if s["data"].get("done") is False]
# After recording 250 exact, the live counter restarts: 250 recorded + 10 new
check(
    "live counter reset after usage records (new turn counts from 0)",
    live_after and live_after[-1]["data"]["description"] == "Credits: 260/2000 session (13.0%)",
    [s["data"]["description"] for s in live_after],
)

print("== 30. Outlet refreshes status after streaming turn ==")
f = make_filter(
    credits_per_session=2000, show_usage_status=True,
)
meta = {"chat_id": "c2", "message_id": "m2"}
run_inlet(f, {"stream": True, "messages": []}, metadata=meta)
run_stream(f, vllm_final_chunk_dict(prompt_tokens=100, completion_tokens=50), metadata=meta)  # 150 recorded
emitter = TestEventEmitter()
asyncio.run(
    f.outlet(
        {
            "model": "glm-5.3",
            "chat_id": "c2",
            "id": "m2",
            "messages": [
                {
                    "id": "m2",
                    "role": "assistant",
                    "content": "x",
                    "usage": {"input_tokens": 100, "output_tokens": 50},
                }
            ],
        },
        __event_emitter__=emitter,
        __user__=user(),
        __model__=MODEL,
    )
)
statuses = [e for e in emitter.events if e.get("type") == "status"]
check("outlet emits status even when turn already recorded via stream", len(statuses) == 1, emitter.events)
if statuses:
    check(
        "refreshed status shows recorded usage",
        statuses[0]["data"]["description"] == "Credits: 150/2000 session (7.5%)",
        statuses[0]["data"]["description"],
    )

print("== 31. Fractional credit accounting (no rounding to zero) ==")
# A short prompt under default-style weights costs a fraction of a credit;
# it must be recorded, not rounded away.
f = make_filter(
    sliding_window_credits=10000,
    input_weight=0.00069, cached_input_weight=0.00017, output_weight=0.0024,
)
# "Are you there?" shaped request: 100 fresh in, 0 cached, 30 out
run_inlet(f, {"stream": True, "messages": []})
run_stream(f, vllm_final_chunk_dict(prompt_tokens=100, completion_tokens=30))
key = [k for k in f._get_redis().keys() if k.endswith(":credits")][0]
member = f._get_redis().zrange(key, 0, -1)[0]
# 100*0.00069 + 30*0.0024 = 0.141 credits
check(
    "short prompt records fractional charge 0.141",
    member.rsplit(":", 1)[1] == "0.141",
    member,
)
# repeat 8 times: total 8 x 0.141 = 1.128, accumulated exactly
for i in range(7):
    run_inlet(f, {"stream": True, "messages": []})
    run_stream(f, vllm_final_chunk_dict(prompt_tokens=100, completion_tokens=30))
used, _ = f._used_with_holds("u1", "glm-5.3", time.time(), 7 * 86400)
check("fractional charges accumulate (1.128 not 0)", abs(used - 1.128) < 1e-9, used)

# legacy integer records still parse
f._get_redis().zadd(key, {f"{time.time()}:{uuid.uuid4().hex}:1:0:1:5": time.time()})
used2, _ = f._used_with_holds("u1", "glm-5.3", time.time(), 7 * 86400)
check("legacy integer records coexist (6.128)", abs(used2 - 6.128) < 1e-9, used2)

# a burst of zero-cost-under-old-rounding prompts exhausts a small budget
f2 = make_filter(
    sliding_window_credits=1,
    input_weight=0.00069, cached_input_weight=0.00017, output_weight=0.0024,
)
blocked = None
for i in range(10):
    try:
        run_inlet(f2, {"stream": True, "messages": []})
        run_stream(f2, vllm_final_chunk_dict(prompt_tokens=100, completion_tokens=30))
    except Exception as e:
        blocked = "Credit limit exceeded" in str(e)
        break
check("short-prompt spam now exhausts a 1-credit budget", blocked is not None and blocked, blocked)

print("== 32. Status unit dropdown (credits / milli-credits / raw) ==")
f = make_filter(
    credits_per_session=2000, sliding_window_credits=10000,
    input_weight=0.00069, cached_input_weight=0.00017, output_weight=0.0024,
    show_usage_status=True,
)
# one short prompt: 0.141 credits
run_inlet(f, {"stream": True, "messages": []})
run_stream(f, vllm_final_chunk_dict(prompt_tokens=100, completion_tokens=30))

# default (credits, 2dp below 100)
msg = f._usage_status_message("u1", "glm-5.3")
check(
    "default unit: 2dp below 100",
    msg == "Credits: 0.14/2000 session (0.0%) - 0.14/10K week (0.0%)",
    msg,
)

# milli-credits
msg = f._usage_status_message("u1", "glm-5.3", unit=_mod.StatusUnit.MILLI_CREDITS)
check(
    "milli-credit unit shows thousandths",
    msg == "Credits: 141 mC/2M mC session (0.0%) - 141 mC/10M mC week (0.0%)",
    msg,
)

# raw
msg = f._usage_status_message("u1", "glm-5.3", unit=_mod.StatusUnit.RAW)
check(
    "raw unit shows unrounded value",
    msg == "Credits: 0.141/2000 session (0.0%) - 0.141/10000 week (0.0%)",
    msg,
)

# whole-credit formatting at scale: +500000 in, +100000 out -> +585 credits
run_stream(f, vllm_final_chunk_dict(prompt_tokens=500000, completion_tokens=100000))
msg = f._usage_status_message("u1", "glm-5.3")
check(
    "credits unit: integers at 100+",
    msg.startswith("Credits: 585/2000 session ("),
    msg,
)

# user valve override: milli-credits for this user only
class UserValvesStub2:
    show_usage_status = True
    status_unit = _mod.StatusUnit.MILLI_CREDITS

emitter = TestEventEmitter()
run_inlet(
    f, {"stream": True, "messages": []}, emitter=emitter,
    u={"id": "u1", "role": "user", "valves": UserValvesStub2()},
)
statuses = [e for e in emitter.events if e.get("type") == "status"]
if statuses:
    check(
        "user valve overrides status unit",
        "mC" in statuses[0]["data"]["description"],
        statuses[0]["data"]["description"],
    )

# unparseable user valve falls back to the filter valve
class UserValvesStub3:
    show_usage_status = True
    status_unit = "bogus"

emitter = TestEventEmitter()
run_inlet(
    f, {"stream": True, "messages": []}, emitter=emitter,
    u={"id": "u1", "role": "user", "valves": UserValvesStub3()},
)
statuses = [e for e in emitter.events if e.get("type") == "status"]
if statuses:
    check(
        "bogus user valve falls back to filter valve",
        "mC" not in statuses[0]["data"]["description"],
        statuses[0]["data"]["description"],
    )

print()
print(f"RESULTS: {PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
