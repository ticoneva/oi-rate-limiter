"""
title: Credit Usage Admin Tool
author: Assistant
author_url: https://github.com/ticoneva/oi-rate-limiter
version: 0.4.0
license: MIT

Admin-only companion to the Rate and Credit Limit Filter. Queries a user's
credit consumption (per rate-limiter namespace), adjusts it with manual
credit grants/charges, and clears their credit records. Reads the same
Redis keys the filter writes (oi-rate-limiter:{namespace}:{user}:credits),
so this tool must point at the same redis_url and share the namespaces you
want to inspect.

Permissions: admins may query, adjust, and clear anyone's usage; everyone
else may query only their own (an empty user parameter means "me"). All
modifications require confirm=true. Non-admins never see other users'
names or emails in errors.

User resolution calls Open WebUI's user repository and handles both the
pre-0.9 sync API and the 0.9+ async API; lookup failures are reported as
error strings rather than silently falling back to a raw-ID Redis query.
"""

import inspect
import re
import time
from typing import Optional, Tuple
from pydantic import BaseModel, Field

# Resolve a user by email/name in addition to raw ID, when Open WebUI's
# user model is importable (i.e. when running inside Open WebUI).
try:
    from open_webui.models.users import Users
except ImportError:
    Users = None


class UserLookupError(ValueError):
    """A user query matched several users, or the Open WebUI user backend
    raised while resolving — both are surfaced to the caller instead of
    silently falling back to treating the query as a raw user ID."""


def _display(resolved: dict) -> str:
    """'Name <email>' when both are known, else whichever we have."""
    name, email = resolved.get("name", ""), resolved.get("email", "")
    if name and email and name != email:
        return f"{name} <{email}>"
    return name or email or resolved.get("id", "?")


class Tools:
    class Valves(BaseModel):
        redis_url: str = Field(
            default="redis://redis-valkey:6379/0",
            description="Redis URL — must match the rate limiter filter's redis_url.",
        )
        session_window_minutes: int = Field(
            default=300,
            description="Session window to report against; must match the filter's valve for exact percentages.",
        )
        credit_window_minutes: int = Field(
            default=10080,
            description="Weekly window to report against; must match the filter's valve for exact percentages.",
        )

    def __init__(self):
        self.valves = self.Valves()
        self._redis: Optional["redis.Redis"] = None

    def _get_redis(self):
        if self._redis is None:
            import redis as _redis

            self._redis = _redis.from_url(self.valves.redis_url, decode_responses=True)
        return self._redis

    async def _resolve_user(self, query: str) -> Optional[dict]:
        """Accept a raw user ID, email, or name; return the Open WebUI user
        dict when resolvable, else None. Raises UserLookupError on
        ambiguous name matches or backend errors, so the caller can report
        them instead of silently querying Redis with a bad user ID."""
        if Users is None:
            return None

        async def run(fn, *args, **kwargs):
            """Call a Users method, handling Open WebUI's async transition:
            v0.9.0+ made the user repository async, while v0.8.x is sync.
            A sync call against an async method returns a bare coroutine
            (then fails on attribute access), so detect and await it."""
            result = fn(*args, **kwargs)
            if inspect.iscoroutine(result):
                result = await result
            return result

        # 1. Exact ID
        try:
            user = await run(Users.get_user_by_id, query)
            if user:
                return {"id": user.id, "name": user.name, "email": user.email}
        except UserLookupError:
            raise
        except Exception as e:
            raise UserLookupError(
                f"looking up user ID '{query}' in Open WebUI failed: {e!r}"
            )
        # 2. Exact email
        try:
            user = await run(Users.get_user_by_email, query)
            if user:
                return {"id": user.id, "name": user.name, "email": user.email}
        except UserLookupError:
            raise
        except Exception as e:
            raise UserLookupError(
                f"looking up email '{query}' in Open WebUI failed: {e!r}"
            )
        # 3. Name/email search via get_users (ilike substring over name
        # and email, case-insensitive). An exact name match wins over
        # substring hits so "llm" resolves even if "llm-bot" also exists;
        # otherwise multiple matches are an error listing the candidates.
        try:
            result = await run(Users.get_users, filter={"query": query})
            users = result.get("users", []) if isinstance(result, dict) else []
            matches = [u for u in users if u.name.lower() == query.lower()] or users
            if len(matches) == 1:
                u = matches[0]
                return {"id": u.id, "name": u.name, "email": u.email}
            if len(matches) > 1:
                names = ", ".join(f"{u.name} <{u.email}>" for u in matches[:10])
                raise UserLookupError(
                    f"'{query}' matches {len(matches)} users: {names}. "
                    "Use the exact email or user ID."
                )
        except UserLookupError:
            raise
        except Exception as e:
            raise UserLookupError(
                f"searching users for '{query}' in Open WebUI failed: {e!r}"
            )
        return None

    @staticmethod
    def _member_usage(member: str):
        """Parse '{ts}:{uuid}:{uncached_in}:{cached_in}:{out}:{charged}'.
        Returns (ts, uncached, cached, out, charged, is_adjustment) or None.
        Manual adjustments written by adjust_credit_usage use a
        'adj-{reason}' uuid segment and 0 token counts."""
        parts = member.split(":")
        if len(parts) != 6:
            return None
        try:
            return (
                float(parts[0]),
                int(parts[2]),
                int(parts[3]),
                int(parts[4]),
                int(parts[5]),
                parts[1].startswith("adj-"),
            )
        except ValueError:
            return None

    @staticmethod
    def _require_admin(__user__: Optional[dict]) -> Optional[str]:
        """Return an error string if the caller is not an admin."""
        if not __user__ or __user__.get("role") != "admin":
            return "Error: this tool is restricted to administrators."
        return None

    @staticmethod
    def _caller_is_admin(__user__: Optional[dict]) -> bool:
        """Open WebUI passes __user__ as a flat model_dump() dict with
        id/name/email/role at the top level (verified v0.8.10 and v0.11.0)."""
        if not __user__:
            return False
        return __user__.get("role") == "admin"

    async def _target_user(
        self, user: str, __user__: Optional[dict]
    ) -> Tuple[Optional[dict], Optional[str]]:
        """Resolve which account an operation targets and how to display it.

        Returns (resolved, error). Admins may target anyone (empty `user`
        means themselves); non-admins are pinned to their own account — the
        `user` parameter is ignored entirely, so a non-admin cannot read or
        modify another account by naming it. Non-admins also never see
        candidate names/emails from ambiguity errors."""
        if self._caller_is_admin(__user__):
            if not user:
                caller = __user__ or {}
                resolved = {
                    "id": caller.get("id", ""),
                    "name": caller.get("name", ""),
                    "email": caller.get("email", ""),
                }
                if not resolved["id"]:
                    return None, "Error: could not determine your account."
                return resolved, None
            try:
                resolved = await self._resolve_user(user)
            except UserLookupError as e:
                return None, f"Error: {e}"
            if resolved is not None:
                return resolved, None
            if Users is not None:
                # The user backend is importable but found no match — a
                # typo'd name would otherwise silently query the wrong
                # (nonexistent) Redis key.
                return None, (
                    f"Error: no Open WebUI user found for '{user}'. "
                    "Use an exact ID, email, or unique name."
                )
            # open_webui not importable (module renamed? running outside
            # Open WebUI?) — best effort: treat the query as a raw user ID.
            return {"id": user, "name": user, "email": ""}, None
        # Non-admin: always self
        caller = __user__ or {}
        if not caller.get("id"):
            return None, "Error: could not determine your account."
        if user and user.strip() and user.strip() != caller.get("id"):
            # Don't confirm whether the named user exists, or reveal their
            # name/email — just refuse.
            return None, (
                "Error: you can only view your own credit usage. "
                "Leave the user parameter empty to see your own."
            )
        resolved = {
            "id": caller.get("id", ""),
            "name": caller.get("name", ""),
            "email": caller.get("email", ""),
        }
        return resolved, None

    def _namespaces_for_user(self, r, user_id: str):
        """All namespaces that have credit records for this user."""
        found = []
        for key in r.scan_iter(match=f"oi-rate-limiter:*:{user_id}:credits"):
            # oi-rate-limiter:{ns...}:{user_id}:credits — user_id is the
            # second-to-last segment; ns may itself contain ':' if a
            # rate_limiter_id does, so split from both ends.
            body = key[len("oi-rate-limiter:") : -len(":credits")]
            ns = body.rsplit(":", 1)[0]
            found.append((ns, key))
        return found

    async def query_credit_usage(
        self,
        user: str = "",
        namespace: str = "",
        __user__: Optional[dict] = None,
    ) -> str:
        """
        Query a user's credit usage recorded by the rate limiter filter.
        Admins may query anyone; everyone else sees only their own usage —
        leave user empty to mean yourself.

        :param user: Open WebUI user ID, email, or name (admin-only; must match uniquely). Non-admins: leave empty to see your own usage.
        :param namespace: Rate limiter namespace (model ID or rate_limiter_id). Leave empty to report all namespaces the user has records in.
        :return: Formatted usage summary per namespace and window.
        """
        resolved, err = await self._target_user(user, __user__)
        if err:
            return err

        try:
            return await self._query_impl(resolved, namespace)
        except Exception as e:
            return f"Error querying credit usage: {e}"

    async def _query_impl(self, resolved: dict, namespace: str) -> str:
        r = self._get_redis()
        user_id = resolved["id"]
        display = _display(resolved)

        now = time.time()
        windows = [
            ("session (5h)", self.valves.session_window_minutes * 60),
            ("week", self.valves.credit_window_minutes * 60),
            ("all time", None),
        ]

        namespaces = self._namespaces_for_user(r, user_id)
        if namespace:
            namespaces = [(ns, k) for ns, k in namespaces if ns == namespace.strip()]
        if not namespaces:
            hint = (
                " Try querying without a namespace to see where this user has records."
                if namespace
                else ""
            )
            return f"No credit records found for {display}.{hint}"
        if not namespace:
            namespaces.sort(key=lambda x: x[0])

        lines = [f"Credit usage for {display}:"]
        for ns, key in namespaces:
            members = r.zrange(key, 0, -1, withscores=True)
            parsed = [self._member_usage(m) for m, _ in members]
            parsed = [p for p in parsed if p is not None]
            if not parsed:
                lines.append(f"\n[{ns}] no parseable records")
                continue

            total_charged = sum(p[4] for p in parsed)
            total_in = sum(p[1] + p[2] for p in parsed)
            total_cached = sum(p[2] for p in parsed)
            total_out = sum(p[3] for p in parsed)
            cache_rate = (total_cached / total_in * 100) if total_in else 0
            adjustments = [p for p in parsed if p[5]]
            requests = [p for p in parsed if not p[5]]

            lines.append(f"\n[{ns}]")
            lines.append(
                f"  records: {len(parsed)}, total charged: {total_charged} credits"
            )
            lines.append(
                f"  tokens: {total_in} input ({total_cached} cached, "
                f"{cache_rate:.0f}% hit rate), {total_out} output"
            )
            if adjustments:
                net = sum(p[4] for p in adjustments)
                lines.append(
                    f"  manual adjustments: {len(adjustments)}, "
                    f"net {net:+d} credits (included in the totals above)"
                )
            for label, win_secs in windows:
                if win_secs is None:
                    subset = parsed
                    reqs = requests
                else:
                    subset = [p for p in parsed if p[0] > now - win_secs]
                    reqs = [p for p in requests if p[0] > now - win_secs]
                charged = sum(p[4] for p in subset)
                lines.append(
                    f"  {label}: {charged} credits over {len(reqs)} requests"
                )

        return "\n".join(lines)

    async def clear_credit_usage(
        self,
        user: str,
        namespace: str = "",
        confirm: bool = False,
        __user__: Optional[dict] = None,
    ) -> str:
        """
        Clear a user's recorded credit usage (and any pending holds and
        in-flight slots). The user's budget and concurrency slots reset
        immediately for the cleared namespaces.
        Admin-only.

        :param user: Open WebUI user ID, email, or name (must match uniquely).
        :param namespace: Rate limiter namespace to clear. Leave empty to clear ALL namespaces for the user.
        :param confirm: Must be True for the deletion to happen; protects against accidental model-triggered calls.
        :return: Summary of what was cleared.
        """
        denied = self._require_admin(__user__)
        if denied:
            return denied
        if not confirm:
            return (
                "No changes made — call again with confirm=true to actually delete "
                "credit records. This resets the user's budget to full."
            )

        resolved, err = await self._target_user(user, __user__)
        if err:
            return err

        try:
            return await self._clear_impl(resolved, namespace)
        except Exception as e:
            return f"Error clearing credit usage: {e}"

    async def _clear_impl(self, resolved: dict, namespace: str) -> str:
        r = self._get_redis()
        user_id = resolved["id"]
        display = _display(resolved)

        targets = self._namespaces_for_user(r, user_id)
        if namespace:
            targets = [(ns, k) for ns, k in targets if ns == namespace.strip()]
        if not targets:
            return f"No credit records found for {display}"
        if not namespace:
            # An un-namespaced clear also sweeps hold and in-flight keys, so
            # credit holds and the concurrency limit do not keep blocking a
            # cleared user (in-flight slots are the escape hatch for leaked
            # slots from cancelled requests).
            hold_keys = list(r.scan_iter(match=f"oi-rate-limiter:*:{user_id}:holds"))
            inflight_keys = list(
                r.scan_iter(match=f"oi-rate-limiter:*:{user_id}:inflight")
            )
        else:
            hold_keys = [f"oi-rate-limiter:{namespace.strip()}:{user_id}:holds"]
            inflight_keys = [
                f"oi-rate-limiter:{namespace.strip()}:{user_id}:inflight"
            ]

        deleted_credits = 0
        for _, key in targets:
            deleted_credits += r.delete(key)
        deleted_holds = sum(r.delete(k) for k in hold_keys)
        deleted_inflight = sum(r.delete(k) for k in inflight_keys)

        cleared_ns = ", ".join(ns for ns, _ in targets)
        parts = [
            f"Cleared usage for {display}.",
            f"Namespaces: {cleared_ns}",
            f"Deleted keys: {deleted_credits} credit, {deleted_holds} hold, {deleted_inflight} in-flight",
        ]
        return ". ".join(parts)

    async def adjust_credit_usage(
        self,
        user: str,
        amount: int,
        reason: str = "",
        namespace: str = "",
        confirm: bool = False,
        __user__: Optional[dict] = None,
    ) -> str:
        """
        Grant or deduct credits for a user by writing a manual adjustment
        record into the rate limiter's credit ledger. Positive amounts add
        budget headroom (refund), negative amounts charge credits (penalty).
        The next filter check reflects it immediately — no restart needed.
        Admin-only.

        :param user: Open WebUI user ID, email, or name (must match uniquely).
        :param amount: Signed credit delta — positive grants, negative charges.
        :param reason: Short note recorded with the entry (e.g. "promotional grant"); kept verbatim in the ledger and shown in usage reports.
        :param namespace: Rate limiter namespace to adjust. Leave empty to adjust ALL namespaces the user has records in.
        :param confirm: Must be True for the change to happen; protects against accidental model-triggered calls.
        :return: Summary of the adjustment.
        """
        denied = self._require_admin(__user__)
        if denied:
            return denied
        try:
            amount = int(amount)
        except (TypeError, ValueError):
            return f"Error: amount must be an integer number of credits."
        if amount == 0:
            return "No changes made — amount is 0."
        if not confirm:
            return (
                "No changes made — call again with confirm=true to apply the "
                f"adjustment of {amount:+d} credits. This immediately changes "
                "the user's remaining budget."
            )

        resolved, err = await self._target_user(user, __user__)
        if err:
            return err

        try:
            return await self._adjust_impl(resolved, amount, reason, namespace)
        except Exception as e:
            return f"Error adjusting credit usage: {e}"

    async def _adjust_impl(
        self, resolved: dict, amount: int, reason: str, namespace: str
    ) -> str:
        r = self._get_redis()
        user_id = resolved["id"]
        display = _display(resolved)

        # Match the filter's ledger format exactly so its summing logic
        # sees the adjustment: '{ts}:{uuid}:{uncached}:{cached}:{out}:{charged}'.
        # The 'adj-' prefix marks manual adjustments in reports; the reason
        # rides along in the uuid segment. Colons would break parsing for
        # both the filter and this tool, so strip them.
        safe_reason = re.sub(r"[^0-9A-Za-z_.-]", "_", reason.strip())[:60]
        ts = time.time()
        member = f"{ts}:adj-{safe_reason or 'manual'}:0:0:0:{amount}"

        targets = self._namespaces_for_user(r, user_id)
        if namespace:
            targets = [(ns, k) for ns, k in targets if ns == namespace.strip()]
        if not targets:
            hint = (
                " Try clearing the namespace or granting without a namespace."
                if namespace
                else ""
            )
            return (
                f"No credit records found for {display} — nothing to adjust. "
                f"An adjustment needs an existing ledger to write into.{hint}"
            )

        with r.pipeline() as pipe:
            for _, key in targets:
                pipe.zadd(key, {member: ts})
                # Keep the ledger alive at least as long as the filter's
                # longest window, mirroring its retention logic.
                retention = max(
                    self.valves.credit_window_minutes,
                    self.valves.session_window_minutes,
                ) * 60
                pipe.expire(key, retention)
            pipe.execute()

        cleared_ns = ", ".join(ns for ns, _ in targets)
        return (
            f"Adjusted credits for {display} by {amount:+d} "
            f"({'refund' if amount > 0 else 'charge'}, reason: "
            f"{safe_reason or 'manual'}). Namespaces: {cleared_ns}. "
            "The change is live immediately."
        )
