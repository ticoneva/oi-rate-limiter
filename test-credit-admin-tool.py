"""Tests for credit-admin-tool.py simulating the Open WebUI tool runtime.

Run: python3 test-credit-admin-tool.py
"""

import asyncio
import importlib.util
import os
import sys
import time
import uuid

import fakeredis

_spec = importlib.util.spec_from_file_location(
    "credit_admin_tool",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "credit-admin-tool.py"),
)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
Tools = _mod.Tools

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


def make_tool():
    t = Tools()
    t._redis = fakeredis.FakeStrictRedis(decode_responses=True)
    return t


def admin():
    return {"id": "admin-id", "role": "admin", "email": "a@b.c", "name": "Admin"}


def pleb():
    return {"id": "u1", "role": "user", "email": "x@y.z", "name": "Pleb"}


def add_record(t, ns, user_id, uncached, cached, out, charged, age_secs=10):
    """Write a record in the filter's exact member format."""
    key = f"oi-rate-limiter:{ns}:{user_id}:credits"
    ts = time.time() - age_secs
    member = f"{ts}:{uuid.uuid4().hex}:{uncached}:{cached}:{out}:{charged}"
    t._get_redis().zadd(key, {member: ts})


print("== 1. Admin gating ==")
t = make_tool()
out = t.query_credit_usage(user="u1", __user__=pleb())
check("non-admin blocked from query", out.startswith("Error:"))
out = t.clear_credit_usage(user="u1", confirm=True, __user__=pleb())
check("non-admin blocked from clear", out.startswith("Error:"))
out = t.query_credit_usage(user="nobody", __user__=admin())
check("admin allowed", not out.startswith("Error"))

print("== 2. Query across namespaces ==")
t = make_tool()
add_record(t, "glm-5.3", "u1", 10000, 90000, 5000, 34)
add_record(t, "glm-5.3", "u1", 500, 0, 200, 30, age_secs=3)  # young record
add_record(t, "qwen-3.5", "u1", 1000, 0, 100, 10)
out = t.query_credit_usage(user="u1", __user__=admin())
check("query lists both namespaces", "[glm-5.3]" in out and "[qwen-3.5]" in out, out)
check("total charged summed", "total charged: 64 credits" in out, out)
check("token split reported", "100500 input (90000 cached, 90% hit rate)" in out, out)
check("session window separates old/new", "session (5h): 64 credits over 2 requests" in out, out)
check("week window same", "week: 64 credits over 2 requests" in out, out)

# namespace filter
out = t.query_credit_usage(user="u1", namespace="qwen-3.5", __user__=admin())
check("namespace filter isolates", "[qwen-3.5]" in out and "glm-5.3" not in out, out)

# no records
out = t.query_credit_usage(user="stranger", __user__=admin())
check("no records message", "No credit records found" in out, out)

print("== 3. Old records fall out of windows ==")
t = make_tool()
add_record(t, "glm-5.3", "u1", 100, 0, 0, 7, age_secs=6 * 3600)  # 6h old
out = t.query_credit_usage(user="u1", __user__=admin())
check("6h-old record outside 5h session", "session (5h): 0 credits over 0 requests" in out, out)
check("6h-old record inside week", "week: 7 credits over 1 requests" in out, out)
check("all-time keeps it", "all time: 7 credits over 1 requests" in out, out)

print("== 4. Holds and request keys ==")
t = make_tool()
r = t._get_redis()
add_record(t, "glm-5.3", "u1", 10, 0, 5, 3)
r.zadd(f"oi-rate-limiter:glm-5.3:u1:holds", {f"{time.time()}:{uuid.uuid4().hex}:50": time.time()})
r.zadd(f"oi-rate-limiter:glm-5.3:u1", {f"{time.time()}:{uuid.uuid4().hex}": time.time()})
r.set(f"oi-rate-limiter:glm-5.3:u1:done:c:m", "1", ex=900)

print("== 5. Clear requires confirm ==")
out = t.clear_credit_usage(user="u1", confirm=False, __user__=admin())
check("clear refused without confirm", "No changes made" in out, out)
check("records still present", t._get_redis().exists("oi-rate-limiter:glm-5.3:u1:credits"))

print("== 6. Clear scoped to one namespace ==")
add_record(t, "qwen-3.5", "u1", 1, 0, 1, 2)
out = t.clear_credit_usage(user="u1", namespace="glm-5.3", confirm=True, __user__=admin())
check("clear reports success", "Cleared usage" in out, out)
check("glm credits gone", not t._get_redis().exists("oi-rate-limiter:glm-5.3:u1:credits"))
check("glm holds gone", not t._get_redis().exists("oi-rate-limiter:glm-5.3:u1:holds"))
check("glm request log gone", not t._get_redis().exists("oi-rate-limiter:glm-5.3:u1"))
check("done markers survive (scoped)", t._get_redis().exists("oi-rate-limiter:glm-5.3:u1:done:c:m"))
check("qwen credits survive", t._get_redis().exists("oi-rate-limiter:qwen-3.5:u1:credits"))

print("== 7. Clear all namespaces ==")
out = t.clear_credit_usage(user="u1", confirm=True, __user__=admin())
check("all clear succeeds", "Cleared usage" in out, out)
check("qwen credits gone", not t._get_redis().exists("oi-rate-limiter:qwen-3.5:u1:credits"))
check("request keys swept when un-namespaced", not [k for k in t._get_redis().keys("oi-rate-limiter:*") if k.split(":")[-1] == "u1"])

print("== 8. Redis errors surface as strings ==")
t = make_tool()
t._redis = None

class BrokenRedis:
    def __getattr__(self, name):
        raise ConnectionError("redis down")

t._redis = BrokenRedis()
out = t.query_credit_usage(user="u1", __user__=admin())
check("query error returned as message, not exception", isinstance(out, str), out)

print()
print(f"RESULTS: {PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
