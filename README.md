# Open WebUI Rate Limiter

A filter for [Open WebUI](https://github.com/open-webui/open-webui) that enforces per-user request rate limits.

## Why Redis?

Open WebUI can run with **multiple workers**, each handling requests independently. A rate limiter that stores counts in memory (e.g. a Python `dict`) only knows about the requests it has personally seen. The same user hitting two different workers would never exceed the limit, even though they've made twice as many requests.

This rate limiter uses **Redis sorted sets** as a shared, centralized counter so all workers agree on how many requests each user has made, and limits are enforced consistently regardless of which worker processes a request.

## Configuration

| Setting | Default | Description |
|---|---|---|
| `requests_per_minute` | 10 | Max requests per minute per user |
| `requests_per_hour` | 50 | Max requests per hour per user |
| `sliding_window_limit` | 100 | Max requests per sliding window |
| `sliding_window_minutes` | 180 | Sliding window duration in minutes |
| `enabled_for_admins` | True | Whether admins are also rate-limited |
| `redis_url` | `redis://redis-valkey:6379/0` | Redis connection URL |
| `rate_limiter_id` | (empty) | Namespace for this limiter; leave empty to use the model ID, or set the same ID on multiple instances to share tracking globally |

## Installation

1. Install the `redis` Python package:

```bash
pip install redis
```

2. Place `rate-limiter.py` in the Open WebUI filters directory.

3. Set `redis_url` to match your Redis/Valkey instance.

## License

MIT