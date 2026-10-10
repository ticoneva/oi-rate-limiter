"""
title: Credit Usage Admin Tool
author: Assistant
author_url: https://github.com/ticoneva/oi-rate-limiter
version: 0.1.0
license: MIT

Admin-only companion to the Rate and Credit Limit Filter. Queries a user's
credit consumption (per rate-limiter namespace) and clears their credit
records. Reads the same Redis keys the filter writes
(oi-rate-limiter:{namespace}:{user}:credits), so this tool must point at
the same redis_url and share the namespaces you want to inspect.
"""

import time
from typing import Optional
from pydantic import BaseModel, Field

# Resolve a user by email/name in addition to raw ID, when Open WebUI's
# user model is importable (i.e. when running inside Open WebUI).
try:
    from open_webui.models.users import Users
except ImportError:
    Users = None


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

    def _resolve_user(self, query: str) -> Optional[dict]:
        """Accept a raw user ID, email, or name; return the Open WebUI user
        dict when resolvable, else None."""
        if Users is None:
            return None
        try:
            user = Users.get_user_by_id(query)
            if user:
                return {"id": user.id, "name": user.name, "email": user.email}
        except Exception:
            pass
        try:
            user = Users.get_user_by_email(query) if hasattr(Users, "get_user_by_email") else None
            if user:
                return {"id": user.id, "name": user.name, "email": user.email}
        except Exception:
            pass
        return None

    @staticmethod
    def _member_usage(member: str):
        """Parse '{ts}:{uuid}:{uncached_in}:{cached_in}:{out}:{charged}'.
        Returns (ts, uncached, cached, out, charged) or None."""
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
            )
        except ValueError:
            return None

    @staticmethod
    def _require_admin(__user__: Optional[dict]) -> Optional[str]:
        """Return an error string if the caller is not an admin."""
        if not __user__ or __user__.get("role") != "admin":
            return "Error: this tool is restricted to administrators."
        return None

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

    def query_credit_usage(
        self,
        user: str,
        namespace: str = "",
        __user__: Optional[dict] = None,
    ) -> str:
        """
        Query a user's credit usage recorded by the rate limiter filter.

        :param user: Open WebUI user ID, email, or name.
        :param namespace: Rate limiter namespace (model ID or rate_limiter_id). Leave empty to report all namespaces the user has records in.
        :return: Formatted usage summary per namespace and window.
        """
        denied = self._require_admin(__user__)
        if denied:
            return denied

        try:
            return self._query_impl(user, namespace)
        except Exception as e:
            return f"Error querying credit usage: {e}"

    def _query_impl(self, user: str, namespace: str) -> str:
        r = self._get_redis()
        resolved = self._resolve_user(user)
        if resolved:
            user_id, display = resolved["id"], f"{resolved['name']} <{resolved['email']}>"
        else:
            user_id, display = user, user

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
            return f"No credit records found for {display}"
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

            lines.append(f"\n[{ns}]")
            lines.append(
                f"  records: {len(parsed)}, total charged: {total_charged} credits"
            )
            lines.append(
                f"  tokens: {total_in} input ({total_cached} cached, "
                f"{cache_rate:.0f}% hit rate), {total_out} output"
            )
            for label, win_secs in windows:
                if win_secs is None:
                    subset = parsed
                else:
                    subset = [p for p in parsed if p[0] > now - win_secs]
                charged = sum(p[4] for p in subset)
                lines.append(
                    f"  {label}: {charged} credits over {len(subset)} requests"
                )

        return "\n".join(lines)

    def clear_credit_usage(
        self,
        user: str,
        namespace: str = "",
        confirm: bool = False,
        __user__: Optional[dict] = None,
    ) -> str:
        """
        Clear a user's recorded credit usage (and any pending holds). The
        user's budget resets immediately for the cleared namespaces.

        :param user: Open WebUI user ID, email, or name.
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

        try:
            return self._clear_impl(user, namespace)
        except Exception as e:
            return f"Error clearing credit usage: {e}"

    def _clear_impl(self, user: str, namespace: str) -> str:
        r = self._get_redis()
        resolved = self._resolve_user(user)
        if resolved:
            user_id, display = resolved["id"], f"{resolved['name']} <{resolved['email']}>"
        else:
            user_id, display = user, user

        targets = self._namespaces_for_user(r, user_id)
        if namespace:
            targets = [(ns, k) for ns, k in targets if ns == namespace.strip()]
        if not targets:
            return f"No credit records found for {display}"
        if not namespace:
            # An un-namespaced clear also sweeps hold and request keys, so
            # request-count limits do not keep blocking a cleared user.
            hold_keys = list(r.scan_iter(match=f"oi-rate-limiter:*:{user_id}:holds"))
            req_keys = [
                k
                for k in r.scan_iter(match=f"oi-rate-limiter:*:{user_id}")
                if not k.endswith((":credits", ":holds", ":done"))
            ]
        else:
            hold_keys = [f"oi-rate-limiter:{namespace.strip()}:{user_id}:holds"]
            req_keys = [f"oi-rate-limiter:{namespace.strip()}:{user_id}"]

        deleted_credits = 0
        for _, key in targets:
            deleted_credits += r.delete(key)
        deleted_holds = sum(r.delete(k) for k in hold_keys)
        deleted_requests = sum(r.delete(k) for k in req_keys)

        cleared_ns = ", ".join(ns for ns, _ in targets)
        parts = [
            f"Cleared usage for {display}.",
            f"Namespaces: {cleared_ns}",
            f"Deleted keys: {deleted_credits} credit, {deleted_holds} hold, {deleted_requests} request-log",
        ]
        return ". ".join(parts)
