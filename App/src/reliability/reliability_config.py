"""
Reliability Configuration - Zero Hardcoding

All reliability parameters are configured here, not hardcoded in logic.

Configuration includes:
- Rate limits per source
- Retry policies per source
- Circuit breaker thresholds per source

To add a new source: Add configuration dictionary, NO code changes needed
"""

from typing import Dict, Any


# ============================================================================
# RATE LIMITING CONFIGURATION
# ============================================================================

RATE_LIMITS: Dict[str, Dict[str, int]] = {
    # SQLite - Local database, high throughput
    'sqlite': {
        'requests_per_second': 1000,  # Very fast local queries
        'burst': 5000                 # Allow large bursts
    },

    # CSV - Local files, moderate throughput
    'csv': {
        'requests_per_second': 500,   # File I/O limits
        'burst': 2000
    },

    # nselib - External API, rate limited
    'nselib': {
        'requests_per_second': 10,    # Conservative limit
        'burst': 20                   # Small burst capacity
    },

    # jugaad-data - External API, rate limited
    'jugaad': {
        'requests_per_second': 5,     # More conservative
        'burst': 10
    }
}


# ============================================================================
# RETRY POLICY CONFIGURATION
# ============================================================================

RETRY_CONFIGS: Dict[str, Dict[str, Any]] = {
    # SQLite - Local database, fast fail
    'sqlite': {
        'max_retries': 1,             # Retry once
        'base_delay': 0.01,           # 10ms initial delay
        'max_delay': 0.1,             # 100ms max delay
        'exponential_base': 2,
        'jitter': False               # No need for jitter on local DB
    },

    # CSV - Local files, moderate retries
    'csv': {
        'max_retries': 2,
        'base_delay': 0.05,           # 50ms initial delay
        'max_delay': 1.0,             # 1s max delay
        'exponential_base': 2,
        'jitter': False
    },

    # nselib - External API, aggressive retries
    'nselib': {
        'max_retries': 3,
        'base_delay': 0.5,            # 500ms initial delay
        'max_delay': 10.0,            # 10s max delay
        'exponential_base': 2,
        'jitter': True                # Prevent thundering herd
    },

    # jugaad-data - External API, aggressive retries
    'jugaad': {
        'max_retries': 3,
        'base_delay': 0.5,
        'max_delay': 10.0,
        'exponential_base': 2,
        'jitter': True
    }
}


# ============================================================================
# CIRCUIT BREAKER CONFIGURATION
# ============================================================================

CIRCUIT_BREAKER_CONFIGS: Dict[str, Dict[str, Any]] = {
    # SQLite - Local database, rarely fails
    'sqlite': {
        'failure_threshold': 10,      # Open after 10 consecutive failures
        'success_threshold': 2,       # Close after 2 successes in half-open
        'timeout': 5.0,               # Try half-open after 5s
        'window_size': 100            # Track last 100 requests
    },

    # CSV - Local files, moderate tolerance
    'csv': {
        'failure_threshold': 8,
        'success_threshold': 2,
        'timeout': 10.0,
        'window_size': 100
    },

    # nselib - External API, sensitive to failures
    'nselib': {
        'failure_threshold': 5,       # Open after 5 failures (APIs are fragile)
        'success_threshold': 2,
        'timeout': 60.0,              # Wait 1 minute before retry
        'window_size': 50
    },

    # jugaad-data - External API, sensitive to failures
    'jugaad': {
        'failure_threshold': 5,
        'success_threshold': 2,
        'timeout': 60.0,
        'window_size': 50
    }
}


# ============================================================================
# CONNECTION POOL CONFIGURATION
# ============================================================================

CONNECTION_POOL_CONFIGS: Dict[str, Dict[str, Any]] = {
    # SQLite - Pool connections for performance
    'sqlite': {
        'min_size': 2,                # Minimum connections
        'max_size': 10,               # Maximum connections
        'max_idle_time': 300,         # 5 minutes idle timeout
        'max_lifetime': 3600,         # 1 hour max lifetime
        'health_check_interval': 60   # Health check every minute
    }
}


# ============================================================================
# TIMEOUT CONFIGURATION
# ============================================================================

TIMEOUT_CONFIGS: Dict[str, Dict[str, float]] = {
    # SQLite - Fast local queries
    'sqlite': {
        'default': 1.0,               # 1s default timeout
        'complex_query': 5.0          # 5s for complex queries
    },

    # CSV - File I/O
    'csv': {
        'default': 2.0,
        'large_file': 10.0
    },

    # nselib - External API
    'nselib': {
        'default': 10.0,
        'historical': 30.0,           # Historical data can be slow
        'live': 5.0
    },

    # jugaad-data - External API
    'jugaad': {
        'default': 10.0,
        'historical': 30.0,
        'live': 5.0
    }
}


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_rate_limit_config(source: str) -> Dict[str, int]:
    """
    Get rate limit config for source

    Args:
        source: Source name

    Returns:
        Rate limit config or empty dict if not configured
    """
    return RATE_LIMITS.get(source, {})


def get_retry_config(source: str) -> Dict[str, Any]:
    """
    Get retry config for source

    Args:
        source: Source name

    Returns:
        Retry config or default if not configured
    """
    default = {
        'max_retries': 2,
        'base_delay': 0.1,
        'max_delay': 5.0,
        'exponential_base': 2,
        'jitter': False
    }
    return RETRY_CONFIGS.get(source, default)


def get_circuit_breaker_config(source: str) -> Dict[str, Any]:
    """
    Get circuit breaker config for source

    Args:
        source: Source name

    Returns:
        Circuit breaker config or default if not configured
    """
    default = {
        'failure_threshold': 5,
        'success_threshold': 2,
        'timeout': 30.0,
        'window_size': 50
    }
    return CIRCUIT_BREAKER_CONFIGS.get(source, default)


def get_timeout_config(source: str, query_type: str = 'default') -> float:
    """
    Get timeout for source and query type

    Args:
        source: Source name
        query_type: Query type (default, historical, live, etc.)

    Returns:
        Timeout in seconds
    """
    source_config = TIMEOUT_CONFIGS.get(source, {})
    return source_config.get(query_type, source_config.get('default', 10.0))


# ============================================================================
# VALIDATION
# ============================================================================

def validate_configs():
    """
    Validate all reliability configurations

    Checks:
    - All required fields present
    - Positive numeric values
    - Reasonable ranges

    Raises:
        ValueError: If configuration invalid
    """
    # Validate rate limits
    for source, config in RATE_LIMITS.items():
        if config['requests_per_second'] <= 0:
            raise ValueError(f"Rate limit for {source} must be positive")
        if config['burst'] <= 0:
            raise ValueError(f"Burst capacity for {source} must be positive")
        if config['burst'] < config['requests_per_second']:
            raise ValueError(f"Burst capacity for {source} should be >= requests_per_second")

    # Validate retry configs
    for source, config in RETRY_CONFIGS.items():
        if config['max_retries'] < 0:
            raise ValueError(f"Max retries for {source} must be non-negative")
        if config['base_delay'] <= 0:
            raise ValueError(f"Base delay for {source} must be positive")
        if config['max_delay'] <= config['base_delay']:
            raise ValueError(f"Max delay for {source} must be > base delay")

    # Validate circuit breaker configs
    for source, config in CIRCUIT_BREAKER_CONFIGS.items():
        if config['failure_threshold'] <= 0:
            raise ValueError(f"Failure threshold for {source} must be positive")
        if config['success_threshold'] <= 0:
            raise ValueError(f"Success threshold for {source} must be positive")
        if config['timeout'] <= 0:
            raise ValueError(f"Timeout for {source} must be positive")
        if config['window_size'] <= 0:
            raise ValueError(f"Window size for {source} must be positive")

    # Validate timeouts
    for source, config in TIMEOUT_CONFIGS.items():
        for query_type, timeout in config.items():
            if timeout <= 0:
                raise ValueError(f"Timeout for {source}.{query_type} must be positive")


# Run validation on import
validate_configs()
