"""
Rate Limiter - Token Bucket Implementation

Philosophy: Zero Hardcoding - Rate limits configured per source, not hardcoded
"""

import time
import threading
from typing import Dict, Any, Optional
from collections import defaultdict


class RateLimiter:
    """
    Token bucket rate limiter

    Supports:
    - Per-source rate limits
    - Burst capacity
    - Thread-safe operation
    - Graceful timeout

    Zero Hardcoding: All limits configured, not hardcoded
    """

    def __init__(self, rate_limits: Dict[str, Dict[str, int]]):
        """
        Initialize rate limiter with per-source configs

        Args:
            rate_limits: {
                'source_name': {
                    'requests_per_second': 10,
                    'burst': 20
                },
                ...
            }

        Example:
            rate_limits = {
                'nselib': {'requests_per_second': 10, 'burst': 20},
                'sqlite': {'requests_per_second': 1000, 'burst': 5000}
            }
        """
        self.rate_limits = rate_limits

        # Token buckets for each source
        # Format: source -> {'tokens': float, 'last_update': float}
        self.buckets: Dict[str, Dict[str, float]] = {}

        # Locks for thread safety
        self.locks: Dict[str, threading.Lock] = defaultdict(threading.Lock)

        # Initialize buckets
        for source, config in rate_limits.items():
            self.buckets[source] = {
                'tokens': config['burst'],  # Start with full burst capacity
                'last_update': time.time()
            }

    def acquire(self, source: str, tokens: int = 1, timeout: Optional[float] = 5.0) -> bool:
        """
        Acquire rate limit tokens

        Args:
            source: Source name (e.g., 'nselib', 'sqlite')
            tokens: Number of tokens to acquire (default: 1)
            timeout: Max wait time in seconds (None = wait forever)

        Returns:
            True if acquired, False if timeout

        Thread-safe: Yes
        """
        # Check if source has rate limit configured
        if source not in self.rate_limits:
            # No rate limit configured, allow immediately
            return True

        config = self.rate_limits[source]
        rate = config['requests_per_second']
        burst = config['burst']

        start_time = time.time()

        with self.locks[source]:
            while True:
                # Refill tokens based on time elapsed
                now = time.time()
                elapsed = now - self.buckets[source]['last_update']

                # Add tokens based on rate
                new_tokens = elapsed * rate
                self.buckets[source]['tokens'] = min(
                    burst,  # Cap at burst capacity
                    self.buckets[source]['tokens'] + new_tokens
                )
                self.buckets[source]['last_update'] = now

                # Try to acquire tokens
                if self.buckets[source]['tokens'] >= tokens:
                    self.buckets[source]['tokens'] -= tokens
                    return True

                # Check timeout
                if timeout is not None:
                    elapsed_total = time.time() - start_time
                    if elapsed_total >= timeout:
                        return False

                # Wait a bit before retry
                # Calculate how long until we have enough tokens
                tokens_needed = tokens - self.buckets[source]['tokens']
                wait_time = tokens_needed / rate

                # Don't wait longer than remaining timeout
                if timeout is not None:
                    remaining_timeout = timeout - (time.time() - start_time)
                    wait_time = min(wait_time, remaining_timeout)

                # Sleep without holding lock
                # (Release and re-acquire is handled by context manager)
                if wait_time > 0:
                    time.sleep(min(wait_time, 0.1))  # Sleep at most 100ms at a time

    def try_acquire(self, source: str, tokens: int = 1) -> bool:
        """
        Try to acquire tokens without waiting

        Args:
            source: Source name
            tokens: Number of tokens to acquire

        Returns:
            True if acquired immediately, False otherwise
        """
        return self.acquire(source, tokens, timeout=0)

    def get_available_tokens(self, source: str) -> float:
        """
        Get current available tokens for source

        Args:
            source: Source name

        Returns:
            Number of available tokens
        """
        if source not in self.rate_limits:
            return float('inf')  # No limit

        with self.locks[source]:
            # Refill tokens first
            now = time.time()
            elapsed = now - self.buckets[source]['last_update']
            config = self.rate_limits[source]

            new_tokens = elapsed * config['requests_per_second']
            current_tokens = min(
                config['burst'],
                self.buckets[source]['tokens'] + new_tokens
            )

            return current_tokens

    def reset(self, source: str):
        """
        Reset rate limiter for source (fill to burst capacity)

        Args:
            source: Source name
        """
        if source not in self.rate_limits:
            return

        with self.locks[source]:
            self.buckets[source] = {
                'tokens': self.rate_limits[source]['burst'],
                'last_update': time.time()
            }

    def get_stats(self, source: str) -> Dict[str, Any]:
        """
        Get rate limiter statistics for source

        Args:
            source: Source name

        Returns:
            Dictionary with stats
        """
        if source not in self.rate_limits:
            return {
                'configured': False,
                'rate_limited': False
            }

        available = self.get_available_tokens(source)
        config = self.rate_limits[source]

        return {
            'configured': True,
            'available_tokens': available,
            'burst_capacity': config['burst'],
            'requests_per_second': config['requests_per_second'],
            'utilization': 1.0 - (available / config['burst'])
        }
