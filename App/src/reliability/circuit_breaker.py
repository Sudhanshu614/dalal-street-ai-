"""
Circuit Breaker - Stop hammering failing sources

Philosophy: Zero Hardcoding - Circuit params configured per source, not hardcoded
"""

import time
import threading
from typing import Dict, Any, Optional
from collections import defaultdict, deque
from enum import Enum


class CircuitState(Enum):
    """Circuit breaker states"""
    CLOSED = "CLOSED"        # Normal operation, requests allowed
    OPEN = "OPEN"            # Too many failures, requests blocked
    HALF_OPEN = "HALF_OPEN"  # Testing if source recovered


class CircuitBreaker:
    """
    Circuit breaker pattern implementation

    States:
    - CLOSED: Normal operation, track failure rate
    - OPEN: Too many failures, block requests
    - HALF_OPEN: Allow test requests to check recovery

    Transitions:
    CLOSED --[failure_threshold]--> OPEN
    OPEN --[timeout]--> HALF_OPEN
    HALF_OPEN --[success_threshold]--> CLOSED
    HALF_OPEN --[1 failure]--> OPEN

    Zero Hardcoding: All thresholds configured, not hardcoded
    """

    def __init__(self, configs: Dict[str, Dict[str, Any]]):
        """
        Initialize circuit breaker with per-source configs

        Args:
            configs: {
                'source_name': {
                    'failure_threshold': 5,      # Open after 5 consecutive failures
                    'success_threshold': 2,      # Close after 2 successes in half-open
                    'timeout': 60.0,             # Try half-open after 60s
                    'window_size': 100           # Track last 100 requests
                },
                ...
            }

        Example:
            configs = {
                'nselib': {
                    'failure_threshold': 5,
                    'success_threshold': 2,
                    'timeout': 60.0,
                    'window_size': 50
                },
                'sqlite': {
                    'failure_threshold': 10,
                    'success_threshold': 1,
                    'timeout': 5.0,
                    'window_size': 100
                }
            }
        """
        self.configs = configs

        # Default config for sources without explicit config
        self.default_config = {
            'failure_threshold': 5,
            'success_threshold': 2,
            'timeout': 30.0,
            'window_size': 50
        }

        # Circuit state for each source
        self.states: Dict[str, CircuitState] = defaultdict(lambda: CircuitState.CLOSED)

        # Failure/success windows (sliding window of results)
        # Format: source -> deque of (timestamp, success:bool)
        self.windows: Dict[str, deque] = {}

        # When circuit opened (for timeout tracking)
        self.open_times: Dict[str, float] = {}

        # Success count in half-open state
        self.half_open_successes: Dict[str, int] = defaultdict(int)

        # Locks for thread safety
        self.locks: Dict[str, threading.Lock] = defaultdict(threading.Lock)

        # Initialize windows
        for source in configs.keys():
            self.windows[source] = deque(maxlen=configs[source]['window_size'])

    def allow_request(self, source: str) -> bool:
        """
        Check if request is allowed (circuit not open)

        Args:
            source: Source name

        Returns:
            True if request allowed, False if circuit open
        """
        if source not in self.configs:
            # No circuit breaker configured, allow
            return True

        with self.locks[source]:
            state = self._get_state(source)

            if state == CircuitState.CLOSED:
                # Normal operation
                return True

            elif state == CircuitState.OPEN:
                # Check if timeout expired (transition to half-open)
                config = self.configs.get(source, self.default_config)
                if source in self.open_times:
                    elapsed = time.time() - self.open_times[source]
                    if elapsed >= config['timeout']:
                        # Transition to half-open
                        self.states[source] = CircuitState.HALF_OPEN
                        self.half_open_successes[source] = 0
                        return True
                return False

            elif state == CircuitState.HALF_OPEN:
                # Allow test request
                return True

            return False

    def record_success(self, source: str):
        """
        Record successful request

        Args:
            source: Source name
        """
        if source not in self.configs:
            return

        with self.locks[source]:
            # Initialize window if needed
            if source not in self.windows:
                config = self.configs.get(source, self.default_config)
                self.windows[source] = deque(maxlen=config['window_size'])

            # Record success
            self.windows[source].append((time.time(), True))

            state = self._get_state(source)

            if state == CircuitState.HALF_OPEN:
                # Track successes in half-open
                self.half_open_successes[source] += 1
                config = self.configs.get(source, self.default_config)

                if self.half_open_successes[source] >= config['success_threshold']:
                    # Enough successes, close circuit
                    self.states[source] = CircuitState.CLOSED
                    self.half_open_successes[source] = 0

    def record_failure(self, source: str):
        """
        Record failed request, may open circuit

        Args:
            source: Source name
        """
        if source not in self.configs:
            return

        with self.locks[source]:
            # Initialize window if needed
            if source not in self.windows:
                config = self.configs.get(source, self.default_config)
                self.windows[source] = deque(maxlen=config['window_size'])

            # Record failure
            self.windows[source].append((time.time(), False))

            state = self._get_state(source)

            if state == CircuitState.CLOSED:
                # Check if should open
                config = self.configs.get(source, self.default_config)
                consecutive_failures = self._get_consecutive_failures(source)

                if consecutive_failures >= config['failure_threshold']:
                    # Open circuit
                    self.states[source] = CircuitState.OPEN
                    self.open_times[source] = time.time()

            elif state == CircuitState.HALF_OPEN:
                # Failure in half-open, reopen circuit
                self.states[source] = CircuitState.OPEN
                self.open_times[source] = time.time()
                self.half_open_successes[source] = 0

    def get_state(self, source: str) -> str:
        """
        Get current circuit state

        Args:
            source: Source name

        Returns:
            State: 'CLOSED', 'OPEN', or 'HALF_OPEN'
        """
        if source not in self.configs:
            return CircuitState.CLOSED.value

        with self.locks[source]:
            return self._get_state(source).value

    def _get_state(self, source: str) -> CircuitState:
        """
        Internal get state (without lock)

        Args:
            source: Source name

        Returns:
            CircuitState enum
        """
        return self.states.get(source, CircuitState.CLOSED)

    def _get_consecutive_failures(self, source: str) -> int:
        """
        Get consecutive failures from end of window

        Args:
            source: Source name

        Returns:
            Number of consecutive failures
        """
        if source not in self.windows:
            return 0

        window = self.windows[source]
        if not window:
            return 0

        # Count failures from end
        consecutive = 0
        for _, success in reversed(window):
            if success:
                break
            consecutive += 1

        return consecutive

    def reset(self, source: str):
        """
        Reset circuit breaker for source

        Args:
            source: Source name
        """
        if source not in self.configs:
            return

        with self.locks[source]:
            self.states[source] = CircuitState.CLOSED
            if source in self.windows:
                self.windows[source].clear()
            if source in self.open_times:
                del self.open_times[source]
            self.half_open_successes[source] = 0

    def get_stats(self, source: str) -> Dict[str, Any]:
        """
        Get circuit breaker statistics for source

        Args:
            source: Source name

        Returns:
            Dictionary with stats
        """
        if source not in self.configs:
            return {
                'configured': False,
                'state': 'CLOSED'
            }

        with self.locks[source]:
            config = self.configs.get(source, self.default_config)
            window = self.windows.get(source, deque())

            total_requests = len(window)
            failures = sum(1 for _, success in window if not success)
            successes = total_requests - failures
            consecutive_failures = self._get_consecutive_failures(source)

            state = self._get_state(source)

            stats = {
                'configured': True,
                'state': state.value,
                'total_requests': total_requests,
                'failures': failures,
                'successes': successes,
                'consecutive_failures': consecutive_failures,
                'failure_threshold': config['failure_threshold'],
                'success_threshold': config['success_threshold'],
                'timeout': config['timeout']
            }

            if state == CircuitState.OPEN and source in self.open_times:
                elapsed = time.time() - self.open_times[source]
                stats['time_until_half_open'] = max(0, config['timeout'] - elapsed)

            if state == CircuitState.HALF_OPEN:
                stats['half_open_successes'] = self.half_open_successes.get(source, 0)

            return stats
