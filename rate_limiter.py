import threading
import time


class RateLimiter:
    """
    Thread-safe token-bucket rate limiter.

    Share ONE instance across all your ThreadPoolExecutor workers.
    No matter how many threads you spin up (15, 50, 100...), the TOTAL
    request rate hitting the government server never exceeds `rate`
    requests/second.

    On a 429 / 503 (rate-limited by the server), call `.penalize()`.
    It pauses every thread for `penalty_seconds` AND permanently
    (until reset) drops the allowed rate by `slowdown_factor`, so the
    scraper self-heals into a safer speed instead of hammering the
    server again immediately.
    """

    def __init__(self, rate=8.0, burst=8, min_rate=0.5):
        self.rate = rate                # tokens added per second
        self.capacity = burst           # max tokens the bucket can hold
        self.tokens = burst
        self.lock = threading.Lock()
        self.last_refill = time.monotonic()
        self._penalty_until = 0.0
        self._min_rate = min_rate       # never throttle down below this

    def acquire(self):
        """Blocks the calling thread until it is safe to fire one request."""
        while True:
            with self.lock:
                now = time.monotonic()
                if now < self._penalty_until:
                    wait = self._penalty_until - now
                else:
                    elapsed = now - self.last_refill
                    self.last_refill = now
                    self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
                    if self.tokens >= 1:
                        self.tokens -= 1
                        return
                    wait = (1 - self.tokens) / self.rate
            time.sleep(wait)

    def penalize(self, penalty_seconds=15, slowdown_factor=0.5):
        with self.lock:
            self._penalty_until = max(self._penalty_until, time.monotonic() + penalty_seconds)
            self.rate = max(self._min_rate, self.rate * slowdown_factor)
            print(f"[RATE-LIMIT] server pushed back -> slowing to {self.rate:.2f} req/s, "
                  f"pausing {penalty_seconds}s")

    def recover(self, amount=1.2, ceiling=None):
        """Optional: call periodically (e.g. every N successful requests) to
        slowly climb the rate back up after a period of clean responses."""
        with self.lock:
            new_rate = self.rate * amount
            if ceiling:
                new_rate = min(new_rate, ceiling)
            self.rate = new_rate
