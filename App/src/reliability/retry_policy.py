"""
Retry Policy - Exponential Backoff Implementation

Philosophy: Zero Hardcoding - Retry params configured per source, not hardcoded
"""

import time
import random
from typing import Dict, Any, Optional


class RetryPolicy:
    """
    Exponential backoff retry policy

    Features:
    - Exponential backoff (delay = base * exponential_base ^ attempt)
    - Max delay cap
    - Jitter to prevent thundering herd
    - Per-source configuration
    - Retryable vs non-retryable error classification

    Zero Hardcoding: All retry parameters configured, not hardcoded
    """

    def __init__(self, retry_configs: Dict[str, Dict[str, Any]]):
        """
        Initialize retry policy with per-source configs

        Args:
            retry_configs: {
                'source_name': {
                    'max_retries': 3,
                    'base_delay': 0.1,
                    'max_delay': 5.0,
                    'exponential_base': 2,
                    'jitter': True
                },
                ...
            }

        Example:
            retry_configs = {
                'nselib': {
                    'max_retries': 3,
                    'base_delay': 0.5,
                    'max_delay': 10.0,
                    'exponential_base': 2,
                    'jitter': True
                },
                'sqlite': {
                    'max_retries': 1,
                    'base_delay': 0.01,
                    'max_delay': 0.1
                }
            }
        """
        self.retry_configs = retry_configs

        # Default config for sources without explicit config
        self.default_config = {
            'max_retries': 2,
            'base_delay': 0.1,
            'max_delay': 5.0,
            'exponential_base': 2,
            'jitter': False
        }

    def get_max_retries(self, source: str) -> int:
        """
        Get max retries for source

        Args:
            source: Source name

        Returns:
            Max retry attempts
        """
        config = self.retry_configs.get(source, self.default_config)
        return config['max_retries']

    def should_retry(self, source: str, attempt: int, error: Exception) -> bool:
        """
        Determine if should retry based on error type and attempt count

        Args:
            source: Source name
            attempt: Current attempt number (0-indexed)
            error: Exception that occurred

        Returns:
            True if should retry, False otherwise
        """
        # Check if exceeded max retries
        max_retries = self.get_max_retries(source)
        if attempt >= max_retries:
            return False

        # Check if error is retryable
        return self._is_retryable_error(error)

    def get_delay(self, source: str, attempt: int) -> float:
        """
        Calculate retry delay with exponential backoff + optional jitter

        Args:
            source: Source name
            attempt: Current attempt number (0-indexed)

        Returns:
            Delay in seconds before next retry
        """
        config = self.retry_configs.get(source, self.default_config)

        # Calculate exponential backoff
        delay = config['base_delay'] * (config['exponential_base'] ** attempt)

        # Cap at max_delay
        delay = min(delay, config['max_delay'])

        # Add jitter if enabled
        if config.get('jitter', False):
            # Add ±25% random jitter
            jitter_range = delay * 0.25
            delay += random.uniform(-jitter_range, jitter_range)
            delay = max(0, delay)  # Ensure non-negative

        return delay

    def _is_retryable_error(self, error: Exception) -> bool:
        """
        Classify error as retryable or not

        Retryable errors (transient):
        - ConnectionError, TimeoutError
        - Temporary network issues
        - Rate limit exceeded (wait and retry)
        - Server temporarily unavailable (5xx)

        Non-retryable errors (permanent):
        - ValueError, TypeError (bad parameters)
        - Authentication errors (401, 403)
        - Not found (404)
        - Bad request (400)

        Args:
            error: Exception to classify

        Returns:
            True if retryable, False otherwise
        """
        error_type = type(error).__name__
        error_message = str(error).lower()

        # Retryable error types
        retryable_types = [
            'ConnectionError',
            'TimeoutError',
            'Timeout',
            'ConnectTimeout',
            'ReadTimeout',
            'HTTPError',  # Could be 5xx (retryable)
        ]

        if error_type in retryable_types:
            # For HTTPError, check status code
            if error_type == 'HTTPError':
                # Check if 5xx (server error) - retryable
                # vs 4xx (client error) - not retryable
                if hasattr(error, 'response'):
                    status_code = getattr(error.response, 'status_code', None)
                    if status_code:
                        return 500 <= status_code < 600  # 5xx errors are retryable
            return True

        # Retryable error messages
        retryable_messages = [
            'connection reset',
            'connection refused',
            'connection timeout',
            'temporarily unavailable',
            'rate limit',
            'too many requests',
            'service unavailable',
            'gateway timeout',
            'network error'
        ]

        for msg in retryable_messages:
            if msg in error_message:
                return True

        # Non-retryable by default
        return False

    def execute_with_retry(self, source: str, func, *args, **kwargs):
        """
        Execute function with retry logic

        Args:
            source: Source name
            func: Function to execute
            *args, **kwargs: Arguments to pass to function

        Returns:
            Result of function call

        Raises:
            Last exception if all retries exhausted
        """
        last_error = None

        for attempt in range(self.get_max_retries(source) + 1):
            try:
                # Try to execute function
                return func(*args, **kwargs)

            except Exception as e:
                last_error = e

                # Check if should retry
                if not self.should_retry(source, attempt, e):
                    # Not retryable or max retries reached
                    raise

                # Calculate delay
                if attempt < self.get_max_retries(source):
                    delay = self.get_delay(source, attempt)
                    time.sleep(delay)

        # All retries exhausted
        if last_error:
            raise last_error
        else:
            raise RuntimeError(f"Retry exhausted for source '{source}'")

    def get_stats(self, source: str) -> Dict[str, Any]:
        """
        Get retry policy statistics for source

        Args:
            source: Source name

        Returns:
            Dictionary with config info
        """
        config = self.retry_configs.get(source, self.default_config)

        return {
            'max_retries': config['max_retries'],
            'base_delay': config['base_delay'],
            'max_delay': config['max_delay'],
            'exponential_base': config['exponential_base'],
            'jitter': config.get('jitter', False)
        }
