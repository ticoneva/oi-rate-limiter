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


def run(coro):
    """Run a tool call to completion (tools are async since v0.2.0)."""
    return asyncio.run(coro)


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


print("== 1. Permission gating ==")
t = make_tool()
out = run(t.query_credit_usage(user="somebody-else", __user__=pleb()))
check(
    "non-admin querying someone else is refused",
    out.startswith("Error: you can only view your own"),
    out,
)
out = run(t.clear_credit_usage(user="u1", confirm=True, __user__=pleb()))
check("non-admin blocked from clear", out.startswith("Error: this tool is restricted"))
out = run(t.adjust_credit_usage(user="u1", amount=50, confirm=True, __user__=pleb()))
check("non-admin blocked from adjust", out.startswith("Error: this tool is restricted"))
out = run(t.query_credit_usage(user="nobody", __user__=admin()))
check("admin allowed", not out.startswith("Error"))

print("== 2. Query across namespaces ==")
t = make_tool()
add_record(t, "glm-5.3", "u1", 10000, 90000, 5000, 34)
add_record(t, "glm-5.3", "u1", 500, 0, 200, 30, age_secs=3)  # young record
add_record(t, "qwen-3.5", "u1", 1000, 0, 100, 10)
out = run(t.query_credit_usage(user="u1", __user__=admin()))
check("query lists both namespaces", "[glm-5.3]" in out and "[qwen-3.5]" in out, out)
check("total charged summed", "total charged: 64 credits" in out, out)
check("token split reported", "100500 input (90000 cached, 90% hit rate)" in out, out)
check("session window separates old/new", "session (5h): 64 credits of 2000 budget (3.2%) over 2 requests" in out, out)
check("week window same", "week: 64 credits of 10000 budget (0.6%) over 2 requests" in out, out)

# namespace filter
out = run(t.query_credit_usage(user="u1", namespace="qwen-3.5", __user__=admin()))
check("namespace filter isolates", "[qwen-3.5]" in out and "glm-5.3" not in out, out)

# no records
out = run(t.query_credit_usage(user="stranger", __user__=admin()))
check("no records message", "No credit records found" in out, out)

print("== 3. Old records fall out of windows ==")
t = make_tool()
add_record(t, "glm-5.3", "u1", 100, 0, 0, 7, age_secs=6 * 3600)  # 6h old
out = run(t.query_credit_usage(user="u1", __user__=admin()))
check("6h-old record outside 5h session", "session (5h): 0 credits of 2000 budget (0.0%) over 0 requests" in out, out)
check("6h-old record inside week", "week: 7 credits of 10000 budget (0.1%) over 1 requests" in out, out)
check("all-time keeps it", "all time: 7 credits over 1 requests" in out, out)

print("== 3b. Window budgets in usage reports ==")
t = make_tool()
add_record(t, "glm-5.3", "u1", 10000, 90000, 5000, 34)
add_record(t, "glm-5.3", "u1", 500, 0, 200, 30, age_secs=3)
out = run(t.query_credit_usage(user="u1", __user__=admin()))
check(
    "default budgets shown",
    "session (5h): 64 credits of 2000 budget (3.2%) over 2 requests" in out
    and "week: 64 credits of 10000 budget (0.6%) over 2 requests" in out,
    out,
)
check("all-time has no budget line", "all time: 64 credits over 2 requests" in out, out)

# custom budgets reflect the valves
t.valves.credits_per_session = 100
t.valves.sliding_window_credits = 640
out = run(t.query_credit_usage(user="u1", __user__=admin()))
check(
    "custom budgets reported",
    "session (5h): 64 credits of 100 budget (64.0%) over 2 requests" in out
    and "week: 64 credits of 640 budget (10.0%) over 2 requests" in out,
    out,
)

# None disables a budget; the window still reports usage
t.valves.credits_per_session = None
out = run(t.query_credit_usage(user="u1", __user__=admin()))
check(
    "None budget drops only the budget phrase",
    "session (5h): 64 credits over 2 requests" in out
    and "week: 64 credits of 640 budget" in out,
    out,
)

print("== 4. Holds and in-flight keys ==")
t = make_tool()
r = t._get_redis()
add_record(t, "glm-5.3", "u1", 10, 0, 5, 3)
r.zadd(f"oi-rate-limiter:glm-5.3:u1:holds", {f"{time.time()}:{uuid.uuid4().hex}:50": time.time()})
r.zadd(f"oi-rate-limiter:glm-5.3:u1:inflight", {f"c1:m1": time.time()})
r.set(f"oi-rate-limiter:glm-5.3:u1:done:c:m", "1", ex=900)

print("== 5. Clear requires confirm ==")
out = run(t.clear_credit_usage(user="u1", confirm=False, __user__=admin()))
check("clear refused without confirm", "No changes made" in out, out)
check("records still present", t._get_redis().exists("oi-rate-limiter:glm-5.3:u1:credits"))

print("== 6. Clear scoped to one namespace ==")
add_record(t, "qwen-3.5", "u1", 1, 0, 1, 2)
out = run(t.clear_credit_usage(user="u1", namespace="glm-5.3", confirm=True, __user__=admin()))
check("clear reports success", "Cleared usage" in out, out)
check("glm credits gone", not t._get_redis().exists("oi-rate-limiter:glm-5.3:u1:credits"))
check("glm holds gone", not t._get_redis().exists("oi-rate-limiter:glm-5.3:u1:holds"))
check("glm in-flight gone", not t._get_redis().exists("oi-rate-limiter:glm-5.3:u1:inflight"))
check("done markers survive (scoped)", t._get_redis().exists("oi-rate-limiter:glm-5.3:u1:done:c:m"))
check("qwen credits survive", t._get_redis().exists("oi-rate-limiter:qwen-3.5:u1:credits"))

print("== 7. Clear all namespaces ==")
out = run(t.clear_credit_usage(user="u1", confirm=True, __user__=admin()))
check("all clear succeeds", "Cleared usage" in out, out)
check("qwen credits gone", not t._get_redis().exists("oi-rate-limiter:qwen-3.5:u1:credits"))
check(
    "credits/holds/inflight swept when un-namespaced",
    not [
        k
        for k in t._get_redis().keys("oi-rate-limiter:*:u1:*")
        if not ":done:" in k
    ],
)

print("== 8. Redis errors surface as strings ==")
t = make_tool()
t._redis = None

class BrokenRedis:
    def __getattr__(self, name):
        raise ConnectionError("redis down")

t._redis = BrokenRedis()
out = run(t.query_credit_usage(user="u1", __user__=admin()))
check("query error returned as message, not exception", isinstance(out, str), out)

print("== 8b. Non-admin self-query ==")
t = make_tool()
add_record(t, "glm-5.3", "u1", 1000, 0, 100, 42)
out = run(t.query_credit_usage(user="", __user__=pleb()))
check(
    "empty user means self",
    "Credit usage for Pleb <x@y.z>" in out and "total charged: 42 credits" in out,
    out,
)
out = run(t.query_credit_usage(user="u1", __user__=pleb()))
check("own id allowed for non-admin", "total charged: 42 credits" in out, out)
out = run(t.query_credit_usage(user="", __user__={"id": "stranger", "role": "user", "name": "S", "email": "s@s.s"}))
check("stranger sees no records message", "No credit records found" in out, out)
# ambiguity errors must not leak candidate emails to non-admins
out = run(t.query_credit_usage(user="someone else", __user__=pleb()))
check(
    "non-admin denied without leaking existence",
    out.startswith("Error: you can only view your own"),
    out,
)

print("== 8c. Adjust credits (admin) ==")
t = make_tool()
add_record(t, "glm-5.3", "u1", 1000, 0, 100, 30)
add_record(t, "qwen-3.5", "u1", 500, 0, 50, 20)

out = run(t.adjust_credit_usage(user="u1", amount=100, confirm=False, __user__=admin()))
check("adjust refused without confirm", "No changes made" in out, out)
check("ledger untouched", "total charged: 30 credits" in run(t.query_credit_usage(user="u1", namespace="glm-5.3", __user__=admin())))

out = run(t.adjust_credit_usage(user="u1", amount=0, confirm=True, __user__=admin()))
check("zero amount no-op", "No changes made" in out, out)

out = run(t.adjust_credit_usage(user="u1", amount="abc", confirm=True, __user__=admin()))
check("non-integer amount rejected", out.startswith("Error:"), out)

# grant on one namespace
out = run(t.adjust_credit_usage(user="u1", amount=100, reason="promotional grant", namespace="glm-5.3", confirm=True, __user__=admin()))
check("grant reports success", "Adjusted credits" in out and "+100" in out and "refund" in out, out)
out = run(t.query_credit_usage(user="u1", namespace="glm-5.3", __user__=admin()))
check("grant visible in ledger", "total charged: 130 credits" in out, out)
check("adjustment line in report", "manual adjustments: 1, net +100 credits" in out, out)
check("request count excludes adjustment", "week: 130 credits of 10000 budget (1.3%) over 1 requests" in out, out)
out = run(t.query_credit_usage(user="u1", namespace="qwen-3.5", __user__=admin()))
check("other namespace untouched", "total charged: 20 credits" in out, out)

# charge (negative) across all namespaces
out = run(t.adjust_credit_usage(user="u1", amount=-30, reason="penalty: abuse", confirm=True, __user__=admin()))
check("charge reports success", "-30" in out and "charge" in out, out)
out = run(t.query_credit_usage(user="u1", namespace="qwen-3.5", __user__=admin()))
check("un-namespaced charge hits all namespaces", "total charged: -10 credits" in out, out)

# reason sanitization: colons would corrupt the member format
out = run(t.adjust_credit_usage(user="u1", amount=5, reason="weird: reason! with stuff", confirm=True, __user__=admin()))
check("sanitized reason accepted", "Adjusted credits" in out, out)
out = run(t.query_credit_usage(user="u1", namespace="qwen-3.5", __user__=admin()))
check("sanitized record parses fine", "manual adjustments: 2, net -25 credits" in out, out)

# adjusting a user with no ledger fails cleanly
out = run(t.adjust_credit_usage(user="stranger", amount=10, confirm=True, __user__=admin()))
check("no ledger -> clean error", "No credit records found" in out, out)

# the filter's own summing logic sees adjustments: parse member from redis
member = t._get_redis().zrange("oi-rate-limiter:qwen-3.5:u1:credits", 0, -1)[-1]
parts = member.split(":")
check("member format matches filter expectation", len(parts) == 6 and parts[1].startswith("adj-"), member)
check(
    "filter-style sum includes adjustment",
    sum(int(float(m.rsplit(":", 1)[1])) for m in t._get_redis().zrange("oi-rate-limiter:qwen-3.5:u1:credits", 0, -1)) == -5,
)

print("== 9. User resolution via Open WebUI (mocked Users) ==")


class FakeUser:
    def __init__(self, id, name, email):
        self.id, self.name, self.email = id, name, email


class _FakeUsersBase:
    """Shared exact ID/email lookups and ilike get_users search, mimicking
    open_webui.models.users.Users; subclasses choose sync or async style
    (Open WebUI made the repository async in v0.9.0)."""

    def __init__(self, users):
        self._users = users

    def _by_id(self, q):
        return next((u for u in self._users if u.id == q), None)

    def _by_email(self, q):
        return next((u for u in self._users if u.email.lower() == q.lower()), None)

    def _search(self, query):
        q = (query or "").lower()
        hits = [
            u
            for u in self._users
            if q in u.name.lower() or q in u.email.lower()
        ]
        return {"users": hits, "total": len(hits)}


class FakeUsersSync(_FakeUsersBase):
    """Pre-0.9 style: plain sync methods (v0.8.10)."""

    def get_user_by_id(self, q):
        return self._by_id(q)

    def get_user_by_email(self, q):
        return self._by_email(q)

    def get_users(self, filter=None):
        return self._search((filter or {}).get("query"))


class FakeUsersAsync(_FakeUsersBase):
    """0.9+ style: async methods returning coroutines."""

    async def get_user_by_id(self, q):
        return self._by_id(q)

    async def get_user_by_email(self, q):
        return self._by_email(q)

    async def get_users(self, filter=None):
        return self._search((filter or {}).get("query"))


for style, FakeUsers in (("sync (v0.8)", FakeUsersSync), ("async (v0.9+)", FakeUsersAsync)):
    print(f"-- resolver against {style} Users API --")

    LLM = FakeUser("uuid-llm", "llm", "llm@cuhk.edu.hk")
    BOT = FakeUser("uuid-bot", "llm-bot", "bot@cuhk.edu.hk")
    _mod.Users = FakeUsers(
        [LLM, BOT, FakeUser("uuid-x", "Someone Else", "x@cuhk.edu.hk")]
    )

    t = make_tool()
    add_record(t, "test-limit", "uuid-llm", 1000, 0, 100, 10)
    out = run(t.query_credit_usage(user="llm", namespace="test-limit", __user__=admin()))
    check("name query finds records under UUID", "total charged: 10 credits" in out, out)
    check("resolved display shows name and email", "llm <llm@cuhk.edu.hk>" in out, out)

    out = run(t.query_credit_usage(user="uuid-llm", namespace="test-limit", __user__=admin()))
    check("id query resolves", "llm <llm@cuhk.edu.hk>" in out, out)

    out = run(t.query_credit_usage(user="bot@cuhk.edu.hk", __user__=admin()))
    check(
        "email query resolves (no records for this one)",
        "No credit records found for llm-bot <bot@cuhk.edu.hk>" in out,
        out,
    )

    # exact name match must win over substring siblings ("llm" vs "llm-bot")
    t = make_tool()
    add_record(t, "test-limit", "uuid-llm", 500, 0, 50, 5)
    add_record(t, "test-limit", "uuid-bot", 999, 0, 99, 99)
    out = run(t.query_credit_usage(user="LLM", namespace="test-limit", __user__=admin()))
    check(
        "exact name match is case-insensitive and beats substring sibling",
        "total charged: 5 credits" in out and "999" not in out,
        out,
    )

    # ambiguous name: two substring matches, no exact match -> error, no guess
    _mod.Users = FakeUsers(
        [BOT, FakeUser("uuid-llm2", "llm-2", "l2@cuhk.edu.hk")]
    )
    t = make_tool()
    add_record(t, "test-limit", "uuid-bot", 100, 0, 10, 1)
    out = run(t.query_credit_usage(user="llm", __user__=admin()))
    check(
        "ambiguous name returns error listing candidates",
        out.startswith("Error:") and "llm-bot" in out and "llm-2" in out,
        out,
    )
    out = run(t.clear_credit_usage(user="llm", confirm=True, __user__=admin()))
    check("ambiguous name blocks clear", out.startswith("Error:"), out)
    check(
        "records untouched on ambiguity",
        t._get_redis().exists("oi-rate-limiter:test-limit:uuid-bot:credits"),
    )

    # backend raising must surface as an error string, not silent fallback
    class ExplodingUsers(_FakeUsersBase):
        def get_user_by_id(self, q):
            raise RuntimeError("db down")

        def get_user_by_email(self, q):
            raise RuntimeError("db down")

        def get_users(self, filter=None):
            raise RuntimeError("db down")

    if style.startswith("sync"):
        _mod.Users = ExplodingUsers([])
        t = make_tool()
        out = run(t.query_credit_usage(user="llm", __user__=admin()))
        check(
            "backend error surfaces as Error string (no silent raw-ID fallback)",
            out.startswith("Error:") and "db down" in out,
            out,
        )

_mod.Users = None  # restore for any later sections

print()
print(f"RESULTS: {PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
