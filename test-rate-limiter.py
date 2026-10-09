"""Tests for rate-limiter.py simulating the Open WebUI filter runtime.

Covers: request limits, credit budgets, weight math, cache-hit discount,
stream-hook formats (dict / raw SSE bytes), outlet dedup via done markers,
non-streaming input estimation, max_tokens capping, shared namespaces with
per-instance weights, and multi-worker consistency via fakeredis shared state.

Run: python3 test-rate-limiter.py
"""

import asyncio
import importlib.util
import json
import os
import sys

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
    f.valves = f.Valves(**valves)
    return f


def user(uid="u1", role="user"):
    return {"id": uid, "role": role, "email": "x@y.z"}


MODEL = {"id": "glm-5.3", "info": {}}


def run_inlet(f, body, metadata=None, model=MODEL, u=None):
    return f.inlet(
        body,
        TestEventEmitter(),
        __user__=u or user(),
        __model__=model,
        __metadata__=metadata or {},
    )


def run_stream(f, chunk, metadata=None, model=MODEL, u=None):
    return asyncio.run(
        f.stream(
            chunk, __user__=u or user(), __model__=model, __metadata__=metadata or {}
        )
    )


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


print("== 1. Request-count limits (unchanged behavior) ==")
f = make_filter(requests_per_minute=2)
for i in range(2):
    run_inlet(f, {"messages": [{"role": "user", "content": "hi"}]})
blocked = None
try:
    run_inlet(f, {"messages": [{"role": "user", "content": "hi"}]})
    blocked = False
except Exception as e:
    blocked = True
check("3rd request blocked per-minute", blocked)
check(
    "no credits recorded when limits disabled",
    not [k for k in f._get_redis().keys() if k.endswith(":credits")],
)

print("== 2. Credit limit blocking via stream usage ==")
f = make_filter(
    requests_per_minute=None,
    requests_per_hour=None,
    sliding_window_limit=None,
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
    requests_per_minute=None, requests_per_hour=None, sliding_window_limit=None,
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
    requests_per_minute=None, requests_per_hour=None, sliding_window_limit=None,
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
    requests_per_minute=None, requests_per_hour=None, sliding_window_limit=None,
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
    requests_per_minute=None, requests_per_hour=None, sliding_window_limit=None,
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
    requests_per_minute=None, requests_per_hour=None, sliding_window_limit=None,
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
    requests_per_minute=None, requests_per_hour=None, sliding_window_limit=None,
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
    requests_per_minute=None, requests_per_hour=None, sliding_window_limit=None,
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
    requests_per_minute=None, requests_per_hour=None, sliding_window_limit=None,
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
    requests_per_minute=None, requests_per_hour=None, sliding_window_limit=None,
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
    requests_per_minute=None, requests_per_hour=None, sliding_window_limit=None,
    sliding_window_credits=1000, output_weight=4.0, cap_max_tokens_to_budget=True,
)
run_inlet(f, {"stream": True, "messages": [], "max_tokens": 400})
run_stream(f, vllm_final_chunk_dict(prompt_tokens=100, completion_tokens=100))
body = {"stream": True, "messages": [], "max_tokens": 400}
run_inlet(f, body)
# used 100*1 + 100*4 = 500 credits; remaining 500; 500/4 = 125
check("max_tokens clamped to remaining/output_weight", body["max_tokens"] == 125, body)

f3 = make_filter(
    requests_per_minute=None, requests_per_hour=None, sliding_window_limit=None,
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
    requests_per_minute=None, requests_per_hour=None, sliding_window_limit=None,
    sliding_window_credits=1000, rate_limiter_id="pool", input_weight=1.0,
    output_weight=1.0,
)
fb = Filter()
fb._redis = fakeredis.FakeStrictRedis(server=shared_server, decode_responses=True)
fb.valves = fb.Valves(
    requests_per_minute=None, requests_per_hour=None, sliding_window_limit=None,
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
    requests_per_minute=None, requests_per_hour=None, sliding_window_limit=None,
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
    requests_per_minute=None, requests_per_hour=None, sliding_window_limit=None,
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

print("== 15. stream_options injection ==")
f = make_filter(
    requests_per_minute=None, requests_per_hour=None, sliding_window_limit=None,
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
    requests_per_minute=None, requests_per_hour=None, sliding_window_limit=None,
)
run_stream(f, vllm_final_chunk_dict(prompt_tokens=100, completion_tokens=100))
check(
    "no records when no credit limit set",
    not [k for k in f._get_redis().keys() if k.endswith(":credits")],
)

print("== 17. Request keys unchanged from previous version ==")
f = make_filter()
run_inlet(f, {"messages": []})
keys = f._get_redis().keys()
check(
    "request key format backward compatible",
    any(k == "oi-rate-limiter:glm-5.3:u1" for k in keys),
    keys,
)

print("== 18. Credits do not affect request limits and vice versa ==")
f = make_filter(
    requests_per_minute=None, requests_per_hour=None, sliding_window_limit=1,
    sliding_window_credits=100000,
)
run_inlet(f, {"messages": []})
run_stream(f, vllm_final_chunk_dict(prompt_tokens=99999, completion_tokens=99999))
blocked = None
try:
    run_inlet(f, {"messages": []})
    blocked = False
except Exception as e:
    blocked = "Rate limit" in str(e)
check("request limit still enforced independently", blocked)

print()
print(f"RESULTS: {PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
