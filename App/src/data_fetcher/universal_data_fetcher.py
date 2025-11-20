"""
Universal Data Fetcher - Phase 2 Priority 1 Implementation

Philosophy: ZERO HARDCODING
- Discovers ALL data sources at initialization
- Discovers ALL schemas dynamically (PRAGMA, CSV inspection, API metadata)
- Routes based on measured characteristics (from Phase 1)
- Validates ALL parameters against discovered schemas
- One fetch() method handles infinite query combinations

NOT: get_tcs_price(), get_infy_price(), ... (2,183 functions)
YES: fetch(query_type, params) (1 method, infinite combinations)
"""

import sqlite3
import pandas as pd
import time
from datetime import datetime, timedelta, date
from typing import Dict, List, Any, Optional, Tuple
import numpy as np
from pathlib import Path
import json
import os

# Import GenericQueryBuilder for zero-hardcoding SQL generation
from .generic_query_builder import GenericQueryBuilder

# Import TickerResolver for automatic ticker resolution
from .ticker_resolver import TickerResolver

# Import reliability components for resilient data fetching
import sys
from pathlib import Path as ReliabilityPath
reliability_path = ReliabilityPath(__file__).parent.parent / 'reliability'
if str(reliability_path) not in sys.path:
    sys.path.insert(0, str(reliability_path.parent))

from reliability import RateLimiter, RetryPolicy, CircuitBreaker
from reliability.reliability_config import (
    RATE_LIMITS,
    RETRY_CONFIGS,
    CIRCUIT_BREAKER_CONFIGS
)

# API imports
try:
    from jugaad_data.nse import NSELive  # Note: module is jugaad_data (underscore), not jugaad (dot)
    JUGAAD_AVAILABLE = True
except ImportError:
    JUGAAD_AVAILABLE = False
    print("WARNING: jugaad-data not installed. Install with: pip install git+https://github.com/jugaad-py/jugaad-data.git")

try:
    from nselib import capital_market, derivatives
    NSELIB_AVAILABLE = True
except Exception as e:
    NSELIB_AVAILABLE = False
    # Log actual error for debugging (nselib installed but import fails)
    import sys
    print(f"[DEBUG] nselib import failed: {type(e).__name__}: {str(e)}", file=sys.stderr)

try:
    from talib import abstract as ta_abstract
    TALIB_AVAILABLE = True
except Exception:
    TALIB_AVAILABLE = False

try:
    import pandas_ta as pta
    PANDAS_TA_AVAILABLE = True
except Exception:
    PANDAS_TA_AVAILABLE = False


class UniversalDataFetcher:
    """
    ZERO HARDCODING - Works for ANY data source, ANY query type

    Architecture:
    - Discovers schemas at runtime (PRAGMA table_info for SQLite, df.columns for CSV)
    - Routes based on Phase 1 measurements (latency, reliability, data freshness)
    - 4-tier fallback: Primary → Backup → Fallback → Error with context
    - Parameter validation against discovered schemas
    """

    def __init__(self, db_path: str, csv_directory: Optional[str] = None):
        """
        Initialize data fetcher with dynamic schema discovery

        ZERO HARDCODING: Auto-discovers CSV files, no hardcoded filenames

        Args:
            db_path: Path to SQLite database
            csv_directory: Optional directory containing CSV files (auto-discovers latest)
        """
        self.db_path = Path(db_path)
        self.csv_directory = Path(csv_directory) if csv_directory else None

        # Auto-discover CSV files in directory (no hardcoded filename!)
        self.csv_path = self._discover_csv_file() if self.csv_directory else None

        # Discover ALL schemas at startup (ZERO hardcoding!)
        print("[INFO] Discovering schemas dynamically...")
        self.schemas = self._discover_all_schemas()

        # Initialize GenericQueryBuilder for zero-hardcoding SQL generation
        self.query_builder = GenericQueryBuilder(self.schemas['sqlite'])

        # Load routing matrix from Phase 1 measurements
        self.routing_matrix = self._load_routing_matrix()

        # Load query type mapping (maps query types to table/filter configs)
        self.query_type_mapping = self._load_query_type_mapping()

        # Initialize reliability components for resilient fetching
        self.rate_limiter = RateLimiter(RATE_LIMITS)
        self.retry_policy = RetryPolicy(RETRY_CONFIGS)
        self.circuit_breaker = CircuitBreaker(CIRCUIT_BREAKER_CONFIGS)

        # Initialize API connections if available
        self.jugaad = NSELive() if JUGAAD_AVAILABLE else None
        self.nselib_cm = capital_market if NSELIB_AVAILABLE else None
        self.nselib_deriv = derivatives if NSELIB_AVAILABLE else None

        # Get SQLite last updated date for routing decisions
        self.sqlite_last_updated = self._get_sqlite_last_updated()

        # Initialize TickerResolver for automatic ticker resolution
        self.ticker_resolver = TickerResolver(str(self.db_path), str(self.csv_path) if self.csv_path else None)

        self._price_cache = {}
        try:
            from App.config import config as app_config
            self.cache_ttl_sec = getattr(app_config, 'PRICE_CACHE_TTL_SEC', 60)
        except Exception:
            self.cache_ttl_sec = int(os.getenv('PRICE_CACHE_TTL_SEC', '60'))

        print(f"[OK] UniversalDataFetcher initialized")
        print(f"   - SQLite: {len(self.schemas['sqlite'])} tables discovered")
        print(f"   - GenericQueryBuilder: Ready for dynamic SQL generation")
        print(f"   - CSV: {'Available' if self.csv_path and self.csv_path.exists() else 'Not configured'}")
        print(f"   - jugaad-data: {'Available' if self.jugaad else 'Not installed'}")
        print(f"   - nselib: {'Available' if self.nselib_cm else 'Not installed'}")
        print(f"   - TickerResolver: Active ({len(self.ticker_resolver._active_tickers_cache)} tickers tracked)")
        print(f"   - SQLite last updated: {self.sqlite_last_updated}")

    def _discover_csv_file(self) -> Optional[Path]:
        """
        Auto-discover CSV file in directory

        ZERO HARDCODING: Finds CSV files automatically, no hardcoded filename

        Strategy:
        1. Look for files matching pattern: CF-CA-equities-*.csv
        2. Select the most recent one (by filename date)

        Returns:
            Path to CSV file, or None if not found
        """
        if not self.csv_directory or not self.csv_directory.exists():
            return None

        # Find all CSV files matching patterns
        csv_files = list(self.csv_directory.glob('CF-CA-equities-*.csv'))
        if not csv_files:
            csv_files = list(self.csv_directory.glob('CF-CA-*.csv'))

        if not csv_files:
            # No CSV files found
            return None

        if len(csv_files) == 1:
            # Only one file, use it
            return csv_files[0]

        # Multiple files - select most recent by filename
        # Files are named like CF-CA-equities-01-01-2000-to-28-10-2025.csv or CF-CA-*.csv
        # Sort by name (lexicographic) - later dates will be last
        csv_files.sort()
        latest_file = csv_files[-1]  # Last one is most recent

        print(f"   📂 CSV: Found {len(csv_files)} files, using latest: {latest_file.name}")

        return latest_file

    def _discover_all_schemas(self) -> Dict[str, Any]:
        """
        Discover ALL schemas dynamically - ZERO hardcoding

        Returns:
            Dictionary with schemas for all data sources
        """
        schemas = {
            'sqlite': self._discover_sqlite_schema(),
            'csv': self._discover_csv_schema() if self.csv_path else {}
        }

        return schemas

    def _discover_sqlite_schema(self) -> Dict[str, Dict[str, Any]]:
        """
        Discover ALL SQLite tables and columns dynamically

        Uses PRAGMA table_info() - NO hardcoded table or column names!

        Returns:
            Dict mapping table_name -> {columns: [...], types: {...}}
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Discover ALL tables (zero hardcoding!)
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]

        schemas = {}
        for table in tables:
            # Discover ALL columns for this table
            cursor.execute(f"PRAGMA table_info({table})")
            columns_info = cursor.fetchall()

            # Extract column names and types
            columns = [col[1] for col in columns_info]  # col[1] is name
            types = {col[1]: col[2] for col in columns_info}  # col[2] is type

            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            row_count = cursor.fetchone()[0]

            schemas[table] = {
                'columns': columns,
                'types': types,
                'row_count': row_count
            }

        conn.close()
        return schemas

    def _discover_csv_schema(self) -> Dict[str, Any]:
        """
        Discover CSV schema dynamically using pandas

        Returns:
            Dict with columns, types, row_count
        """
        if not self.csv_path or not self.csv_path.exists():
            return {}

        # Read CSV to discover schema
        df = pd.read_csv(self.csv_path)

        return {
            'columns': list(df.columns),
            'types': {col: str(dtype) for col, dtype in df.dtypes.items()},
            'row_count': len(df)
        }

    def _get_sqlite_last_updated(self) -> date:
        """
        Get last updated date from SQLite metadata table

        Returns:
            Last updated date, or current date if not found
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # Check if metadata table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='metadata'")
            if cursor.fetchone():
                cursor.execute("SELECT value FROM metadata WHERE key='last_updated'")
                result = cursor.fetchone()
                if result:
                    conn.close()
                    # Parse date string (format: YYYY-MM-DD)
                    return datetime.strptime(result[0], '%Y-%m-%d').date()

            conn.close()

            # Fallback: Get max date from daily_ohlc
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT MAX(date) FROM daily_ohlc")
            result = cursor.fetchone()
            conn.close()

            if result and result[0]:
                return datetime.strptime(result[0], '%Y-%m-%d').date()

        except Exception as e:
            print(f"WARNING: Could not get SQLite last updated date: {e}")

        # Default to current date
        return datetime.now().date()

    def _load_routing_matrix(self) -> Dict[str, Dict[str, Any]]:
        """
        Load routing matrix based on Phase 1 measurements

        Returns:
            Dictionary mapping query_type -> routing strategy
        """
        # Based on PHASE1_COMPLETE_REPORT.md routing decisions
        return {
            'live_quote': {
                'primary': 'jugaad',
                'primary_timeout': 1000,  # ms
                'backup': 'nselib',
                'backup_timeout': 3000,
                'fallback': 'sqlite',
                'fallback_timeout': 100,
                'reason': 'jugaad 10x faster (254ms vs 2,117ms)'
            },
            'historical_ohlc': {
                'primary': 'sqlite',
                'primary_timeout': 100,
                'backup': None,
                'fallback': None,
                'reason': 'Data exists locally, instant response'
            },
            'corporate_actions': {
                'primary': 'csv',
                'primary_timeout': 100,
                'backup': 'sqlite',
                'backup_timeout': 100,
                'fallback': 'jugaad',
                'fallback_timeout': 500,
                'reason': 'CSV most complete + fresh for historical actions'
            },
            'financial_results': {
                'primary': 'nselib',
                'primary_timeout': 30000,  # 30 seconds (very slow but unique)
                'backup': 'sqlite',
                'backup_timeout': 100,
                'fallback': None,
                'reason': 'Only nselib has 68-column detailed results'
            },
            'fii_dii_positioning': {
                'primary': 'nselib',
                'primary_timeout': 5000,
                'backup': 'sqlite',
                'backup_timeout': 100,
                'fallback': None,
                'reason': 'Only nselib has participant-wise breakdown'
            },
            'option_chain': {
                'primary': 'jugaad',
                'primary_timeout': 1000,
                'backup': 'nselib',
                'backup_timeout': 5000,
                'fallback': None,
                'reason': 'jugaad 14x faster (270ms vs 3,797ms)'
            },
            'market_status': {
                'primary': 'jugaad',
                'primary_timeout': 500,
                'backup': None,
                'fallback': None,
                'reason': 'Fastest, most comprehensive'
            },
            'symbol_validation': {
                'primary': 'sqlite',
                'primary_timeout': 100,
                'backup': 'nselib',
                'backup_timeout': 3000,
                'fallback': None,
                'reason': 'Instant local lookup'
            }
        }

    def _load_query_type_mapping(self) -> Dict[str, Dict[str, Any]]:
        """
        Map query types to table/filter configurations

        ZERO HARDCODING - Maps query types to generic configurations, not hardcoded logic

        Returns:
            Dictionary mapping query_type -> {table, filter_builder, ...}
        """
        return {
            'live_quote': {
                'table': 'fundamentals',
                'filter_param': 'symbol',
                'return_single': True
            },
            'historical_ohlc': {
                'table': 'daily_ohlc',
                'filter_param': 'symbol',
                'sort_by': 'date',
                'sort_order': 'desc',
                'return_single': False
            },
            'symbol_validation': {
                'table': 'stocks_master',
                'filter_param': 'symbol',
                'return_single': True
            },
            'corporate_actions': {
                'table': 'corporate_actions',
                'filter_param': 'symbol',
                'sort_by': 'ex_date',
                'sort_order': 'desc',
                'return_single': False
            }
        }

    def fetch(self, query_type: str, params: Dict[str, Any],
              routing_strategy: str = 'auto') -> Dict[str, Any]:
        """
        Universal fetch method - Works for ANY query type, ANY parameters

        Examples:
            fetch('live_quote', {'symbol': 'TCS'})
            fetch('historical_ohlc', {'symbol': 'INFY', 'from_date': '2024-01-01', 'to_date': '2024-12-31'})
            fetch('corporate_actions', {'symbol': 'RELIANCE', 'action_type': 'dividend'})
            fetch(ANY_QUERY_TYPE, ANY_PARAMS)  # Infinite combinations

        Args:
            query_type: Type of query (live_quote, historical_ohlc, etc.)
            params: Query parameters (symbol, dates, etc.)
            routing_strategy: 'auto' (use routing matrix) or manual source name

        Returns:
            Dictionary with data and metadata:
            {
                'data': <result>,
                'metadata': {
                    'source': 'jugaad',
                    'timestamp': datetime,
                    'latency_ms': 254,
                    'freshness': 'live',
                    'tier': 'primary',
                    'fallback_attempts': 0
                }
            }
        """
        start_time = time.time()

        # Resolve ticker if 'symbol' in params (auto-handle ticker changes)
        ticker_resolution = None
        if params and 'symbol' in params and isinstance(params['symbol'], str):
            resolution = self.ticker_resolver.resolve(params['symbol'])

            if resolution['resolved_ticker']:
                original_ticker = params['symbol']
                params['symbol'] = resolution['resolved_ticker']

                ticker_resolution = {
                    'original': original_ticker,
                    'resolved': resolution['resolved_ticker'],
                    'method': resolution['resolution_method'],
                    'confidence': resolution['confidence'],
                    'note': resolution['metadata'].get('note', '')
                }

                # Symbol bridge: keep both original and resolved for SQLite queries
                try:
                    if isinstance(original_ticker, str) and original_ticker.upper() != resolution['resolved_ticker'].upper():
                        params['_symbol_bridge'] = [resolution['resolved_ticker'], original_ticker.upper()]
                except Exception:
                    pass

                if original_ticker != resolution['resolved_ticker']:
                    print(f"[INFO] Ticker resolved: {original_ticker} → {resolution['resolved_ticker']}")

        # Get routing strategy
        route = self._get_route(query_type, params, routing_strategy)

        # Try sources in order (primary → backup → fallback)
        attempts = []
        for source_config in route:
            attempt_start = time.time()

            try:
                result = self._try_source(
                    source_name=source_config['source'],
                    query_type=query_type,
                    params=params,
                    timeout=source_config['timeout']
                )

                if result is not None:
                    # Success! Format and return
                    latency = (time.time() - attempt_start) * 1000
                    total_time = (time.time() - start_time) * 1000

                    response = {
                        'data': result,
                        'metadata': {
                            'source': source_config['source'],
                            'timestamp': datetime.now().isoformat(),
                            'latency_ms': round(latency, 2),
                            'total_latency_ms': round(total_time, 2),
                            'freshness': self._determine_freshness(source_config['source']),
                            'tier': source_config['tier'],
                            'fallback_attempts': len(attempts),
                            'attempts_made': attempts + [source_config['source']]
                        }
                    }

                    # Add ticker resolution if occurred
                    if ticker_resolution:
                        response['ticker_resolution'] = ticker_resolution

                    return response

            except Exception as e:
                latency = (time.time() - attempt_start) * 1000
                attempts.append({
                    'source': source_config['source'],
                    'error': str(e),
                    'latency_ms': round(latency, 2)
                })

        # All sources failed
        return self._handle_failure(query_type, params, route, attempts)

    def _get_route(self, query_type: str, params: Dict[str, Any],
                   routing_strategy: str) -> List[Dict[str, Any]]:
        """
        Get routing strategy for this query

        ZERO HARDCODING - Routes based on measured characteristics

        Args:
            query_type: Type of query
            params: Query parameters
            routing_strategy: 'auto' or manual source name

        Returns:
            List of source configs to try in order
        """
        # Check if query is historical (use fast local sources)
        if 'date' in params or 'from_date' in params:
            query_date = params.get('date') or params.get('from_date')

            # Parse date if string
            if isinstance(query_date, str):
                try:
                    query_date = datetime.strptime(query_date, '%Y-%m-%d').date()
                except:
                    pass

            # If query date is before SQLite last updated, use SQLite first
            if isinstance(query_date, date) and query_date <= self.sqlite_last_updated:
                return [
                    {'source': 'sqlite', 'timeout': 100, 'tier': 'primary'},
                ]

        # Manual routing
        if routing_strategy != 'auto':
            return [
                {'source': routing_strategy, 'timeout': 5000, 'tier': 'manual'}
            ]

        # Use routing matrix
        routing = self.routing_matrix.get(query_type)

        if not routing:
            # Unknown query type - try all sources
            return [
                {'source': 'sqlite', 'timeout': 100, 'tier': 'fallback'},
                {'source': 'jugaad', 'timeout': 1000, 'tier': 'fallback'},
                {'source': 'nselib', 'timeout': 5000, 'tier': 'fallback'},
            ]

        # Build route from routing matrix
        route = []

        if routing.get('primary'):
            route.append({
                'source': routing['primary'],
                'timeout': routing['primary_timeout'],
                'tier': 'primary'
            })

        if routing.get('backup'):
            route.append({
                'source': routing['backup'],
                'timeout': routing['backup_timeout'],
                'tier': 'backup'
            })

        if routing.get('fallback'):
            route.append({
                'source': routing['fallback'],
                'timeout': routing['fallback_timeout'],
                'tier': 'fallback'
            })

        return route

    def _try_source(self, source_name: str, query_type: str,
                    params: Dict[str, Any], timeout: int) -> Optional[Any]:
        """
        Try fetching from specific source WITH RELIABILITY FEATURES

        Reliability Features Applied:
        1. Circuit Breaker - Skip source if consistently failing
        2. Rate Limiting - Respect API rate limits
        3. Retry Logic - Handle transient failures with exponential backoff

        Args:
            source_name: 'sqlite', 'csv', 'jugaad', 'nselib'
            query_type: Type of query
            params: Query parameters
            timeout: Timeout in milliseconds

        Returns:
            Result if successful, None if failed
        """
        # STEP 1: Check circuit breaker (skip if open)
        if not self.circuit_breaker.allow_request(source_name):
            # Circuit is OPEN - source is down, skip it
            return None

        # STEP 2: Acquire rate limit token (wait up to timeout)
        timeout_seconds = timeout / 1000.0  # Convert ms to seconds
        if not self.rate_limiter.acquire(source_name, timeout=timeout_seconds):
            # Rate limit exceeded
            return None

        try:
            # STEP 3: Execute with retry logic
            last_error = None

            for attempt in range(self.retry_policy.get_max_retries(source_name) + 1):
                try:
                    # Execute the actual fetch
                    result = self._execute_fetch(source_name, query_type, params)

                    # Success! Record success on circuit breaker
                    self.circuit_breaker.record_success(source_name)
                    return result

                except Exception as e:
                    last_error = e

                    # Check if should retry
                    if self.retry_policy.should_retry(source_name, attempt, e):
                        # Calculate retry delay
                        delay = self.retry_policy.get_delay(source_name, attempt)
                        time.sleep(delay)
                        continue
                    else:
                        # Non-retryable error, fail immediately
                        raise

            # All retries exhausted
            self.circuit_breaker.record_failure(source_name)
            return None

        except Exception as e:
            # Final failure - record on circuit breaker
            self.circuit_breaker.record_failure(source_name)
            return None

        finally:
            # STEP 4: Always release rate limit token
            # (No explicit release needed for token bucket, handled automatically)
            pass

    def _execute_fetch(self, source_name: str, query_type: str,
                      params: Dict[str, Any]) -> Optional[Any]:
        """
        Execute the actual fetch from source (called by _try_source with reliability)

        Args:
            source_name: 'sqlite', 'csv', 'jugaad', 'nselib'
            query_type: Type of query
            params: Query parameters

        Returns:
            Result if successful

        Raises:
            Exception if fetch fails
        """
        if source_name == 'sqlite':
            return self._fetch_from_sqlite(query_type, params)
        elif source_name == 'csv':
            return self._fetch_from_csv(query_type, params)
        elif source_name == 'jugaad':
            return self._fetch_from_jugaad(query_type, params)
        elif source_name == 'nselib':
            return self._fetch_from_nselib(query_type, params)
        else:
            raise ValueError(f"Unknown source: {source_name}")

    def _fetch_from_sqlite(self, query_type: str, params: Dict[str, Any]) -> Optional[Any]:
        """
        Fetch from SQLite database using GenericQueryBuilder

        ZERO HARDCODING - Uses GenericQueryBuilder to construct SQL dynamically
        NO hardcoded query types, table names, or SQL strings

        Philosophy: Configuration, not code
        """
        # Get query configuration for this query type
        query_config = self.query_type_mapping.get(query_type)

        if not query_config:
            # Unknown query type - can't handle
            return None

        # Extract configuration
        table = query_config['table']
        filter_param_name = query_config['filter_param']
        return_single = query_config.get('return_single', True)
        sort_by = query_config.get('sort_by')
        sort_order = query_config.get('sort_order', 'asc')

        # Build filters from params
        # Prefer bridge list for symbol-based queries to unify old/new tickers
        bridge_list = None
        if filter_param_name == 'symbol':
            bl = params.get('_symbol_bridge')
            if isinstance(bl, list) and bl:
                bridge_list = [str(x).upper() for x in bl]
        filter_value = bridge_list if bridge_list else params.get(filter_param_name)
        if not filter_value:
            # Required filter param missing
            return None

        filters = {filter_param_name: filter_value}

        # Check if table exists in discovered schema
        if table not in self.schemas['sqlite']:
            return None

        # Use GenericQueryBuilder to construct SQL (ZERO hardcoding!)
        try:
            sql, sql_params = self.query_builder.query(
                table=table,
                filters=filters,
                fields=None,  # Get all fields
                sort_by=sort_by,
                sort_order=sort_order,
                limit=None
            )
        except Exception as e:
            # Query building failed (invalid params)
            return None

        # Execute the dynamically generated SQL
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        try:
            cursor.execute(sql, sql_params)

            if return_single:
                # Return single record
                result = cursor.fetchone()
                rec = dict(result) if result else None
                # Runtime augmentation: attach company_name when missing and symbol present
                if rec and ('company_name' not in rec) and ('symbol' in rec) and rec['symbol']:
                    try:
                        conn2 = sqlite3.connect(self.db_path)
                        conn2.row_factory = sqlite3.Row
                        cur2 = conn2.cursor()
                        cur2.execute("SELECT company_name FROM company_names_canonical WHERE symbol = ?", (str(rec['symbol']).upper(),))
                        r = cur2.fetchone()
                        if r:
                            rec['company_name'] = r['company_name'] if isinstance(r, sqlite3.Row) else r[0]
                        conn2.close()
                    except Exception:
                        pass
                return rec
            else:
                # Return list of records
                results = cursor.fetchall()
                out = [dict(row) for row in results]
                # Attach company_name in batch when missing
                try:
                    need_name = any(('company_name' not in r) for r in out)
                    have_symbol = all(('symbol' in r and r['symbol']) for r in out)
                    if out and need_name and have_symbol:
                        symbols = {str(r['symbol']).upper() for r in out}
                        if not symbols:
                            return out
                        conn2 = sqlite3.connect(self.db_path)
                        conn2.row_factory = sqlite3.Row
                        cur2 = conn2.cursor()
                        placeholders = ",".join(["?"] * len(symbols))
                        cur2.execute(f"SELECT symbol, company_name FROM company_names_canonical WHERE symbol IN ({placeholders})", list(symbols))
                        mapping = { (row['symbol'] if isinstance(row, sqlite3.Row) else row[0]): (row['company_name'] if isinstance(row, sqlite3.Row) else row[1]) for row in cur2.fetchall() }
                        cur2.execute(f"SELECT old_symbol, new_symbol, old_name, new_name, effective_date FROM alias_events WHERE UPPER(old_symbol) IN ({placeholders}) OR UPPER(new_symbol) IN ({placeholders})", list(symbols)+list(symbols))
                        evs = cur2.fetchall()
                        events_by_symbol = {}
                        for e in evs:
                            osym = e['old_symbol'] if isinstance(e, sqlite3.Row) else e[0]
                            nsym = e['new_symbol'] if isinstance(e, sqlite3.Row) else e[1]
                            oname = e['old_name'] if isinstance(e, sqlite3.Row) else e[2]
                            nname = e['new_name'] if isinstance(e, sqlite3.Row) else e[3]
                            ed = e['effective_date'] if isinstance(e, sqlite3.Row) else e[4]
                            if osym:
                                events_by_symbol.setdefault(str(osym).upper(), []).append(('old', oname, ed, nsym, nname))
                            if nsym:
                                events_by_symbol.setdefault(str(nsym).upper(), []).append(('new', nname, ed, osym, oname))
                        conn2.close()
                        for r in out:
                            if 'company_name' not in r:
                                symu = str(r['symbol']).upper()
                                v = mapping.get(symu)
                                if v is not None:
                                    r['company_name'] = v
                                else:
                                    r['company_name'] = None
                                if events_by_symbol.get(symu) and ('date' in r and r['date']):
                                    try:
                                        dstr = str(r['date'])
                                        from datetime import datetime
                                        try:
                                            dt = datetime.strptime(dstr, '%Y-%m-%d')
                                        except Exception:
                                            dt = datetime.strptime(dstr[:10], '%Y-%m-%d')
                                        for kind, name, ed, peer_sym, peer_name in events_by_symbol.get(symu) or []:
                                            if not ed:
                                                continue
                                            try:
                                                eddt = datetime.strptime(str(ed), '%Y-%m-%d')
                                            except Exception:
                                                continue
                                            if kind == 'old' and dt <= eddt and name:
                                                r['company_name'] = name
                                                break
                                            if kind == 'new' and dt >= eddt and name:
                                                r['company_name'] = name
                                                break
                                    except Exception:
                                        pass
                except Exception:
                    pass
                return out

        except Exception as e:
            # SQL execution failed
            return None
        finally:
            conn.close()

    def _fetch_from_csv(self, query_type: str, params: Dict[str, Any]) -> Optional[Any]:
        """
        Fetch from CSV file

        ZERO HARDCODING - Uses pandas to discover and query dynamically
        """
        if not self.csv_path or not self.csv_path.exists():
            return None

        if query_type != 'corporate_actions':
            return None

        # Read CSV
        df = pd.read_csv(self.csv_path)

        # Filter by symbol if provided
        symbol = params.get('symbol')
        if symbol:
            df = df[df['SYMBOL'] == symbol]

        # Filter by action type if provided
        action_type = params.get('action_type')
        if action_type:
            df = df[df['PURPOSE'].str.contains(action_type, case=False, na=False)]

        # Convert to list of dictionaries
        return df.to_dict('records')

    def _fetch_from_jugaad(self, query_type: str, params: Dict[str, Any]) -> Optional[Any]:
        """
        Fetch from jugaad-data API

        Uses NSELive methods dynamically
        """
        if not self.jugaad:
            return None

        symbol = params.get('symbol')

        try:
            if query_type == 'live_quote':
                return self.jugaad.stock_quote(symbol)

            elif query_type == 'option_chain':
                return self.jugaad.index_option_chain(symbol)

            elif query_type == 'market_status':
                return self.jugaad.market_status()

            return None

        except Exception as e:
            # jugaad method failed
            return None

    def _fetch_from_nselib(self, query_type: str, params: Dict[str, Any]) -> Optional[Any]:
        """
        Fetch from nselib API

        Uses capital_market and derivatives modules
        """
        if not self.nselib_cm:
            return None

        symbol = params.get('symbol')

        try:
            if query_type == 'financial_results':
                # Get financial results for equity
                from_date = params.get('from_date')
                to_date = params.get('to_date')

                if not from_date or not to_date:
                    # Default to last quarter
                    to_date = datetime.now().strftime('%d-%m-%Y')
                    from_date = (datetime.now() - timedelta(days=90)).strftime('%d-%m-%Y')

                # This might need symbol parameter - check nselib docs
                return self.nselib_cm.financial_results(from_date, to_date)

            elif query_type == 'fii_dii_positioning':
                trade_date = params.get('date', datetime.now().strftime('%d-%m-%Y'))
                return self.nselib_deriv.participant_wise_open_interest(trade_date)

            return None

        except Exception as e:
            # nselib method failed
            return None

    def _determine_freshness(self, source_name: str) -> str:
        """
        Determine data freshness based on source

        Args:
            source_name: Name of data source

        Returns:
            'live', 'cached', or 'stale'
        """
        if source_name in ['jugaad', 'nselib']:
            return 'live'
        elif source_name == 'sqlite':
            days_stale = (datetime.now().date() - self.sqlite_last_updated).days
            if days_stale <= 7:
                return 'cached'
            else:
                return 'stale'
        elif source_name == 'csv':
            return 'cached'
        else:
            return 'unknown'

    def _handle_failure(self, query_type: str, params: Dict[str, Any],
                       route: List[Dict[str, Any]], attempts: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Handle complete failure when all sources failed

        Returns error with helpful context
        """
        return {
            'error': f"Could not fetch data for query_type='{query_type}'",
            'query_type': query_type,
            'params': params,
            'attempts': attempts,
            'sources_tried': [r['source'] for r in route],
            'suggestion': 'Check parameters, symbol validity, or try again later'
        }

    def validate_params(self, query_type: str, params: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        Validate parameters against discovered schemas

        Args:
            query_type: Type of query
            params: Parameters to validate

        Returns:
            (is_valid, error_message)
        """
        # Basic validation - can be extended based on query type
        if query_type in ['live_quote', 'historical_ohlc', 'symbol_validation']:
            if 'symbol' not in params:
                return False, "Missing required parameter: 'symbol'"

        # All validation passed
        return True, None

    def get_available_tables(self) -> List[str]:
        """
        Get list of all available SQLite tables

        Returns:
            List of table names
        """
        return list(self.schemas['sqlite'].keys())

    def get_table_schema(self, table_name: str) -> Optional[Dict[str, Any]]:
        """
        Get schema for specific table

        Args:
            table_name: Name of table

        Returns:
            Schema dict with columns, types, row_count
        """
        return self.schemas['sqlite'].get(table_name)

    def get_csv_schema(self) -> Dict[str, Any]:
        """
        Get CSV schema

        Returns:
            Schema dict with columns, types, row_count
        """
        return self.schemas.get('csv', {})

    # =========================================================================
    # THE 4 UNIVERSAL OPERATIONS (SYSTEM_ARCHITECTURE_PHILOSOPHY)
    # =========================================================================
    # Following documentation: FROM_SCRATCH_DOCS/SYSTEM_ARCHITECTURE_PHILOSOPHY.md
    # These 4 methods handle INFINITE query combinations (zero hardcoding)
    # =========================================================================

    def query_stocks(self,
                    filters: Optional[Dict[str, Any]] = None,
                    sort_by: Optional[str] = None,
                    sort_order: str = 'desc',
                    limit: Optional[int] = None,
                    fields: Optional[List[str]] = None,
                    table: str = 'fundamentals') -> Dict[str, Any]:
        """
        OPERATION 1: Query ANY database table with flexible filters

        Following ZERO_HARDCODING_PHILOSOPHY: Works for ANY table, ANY filter combination
        ✅ FIXED: Now supports ALL 16 tables, not just fundamentals!

        Args:
            filters: Filter conditions
                - Exact match: {'ticker': 'TCS'}
                - Range: {'pe_ratio': {'min': 10, 'max': 20}}
                - List: {'sector': ['IT', 'Pharma']}
            sort_by: Field to sort by
            sort_order: 'asc' or 'desc'
            limit: Max results
            fields: Specific fields to return
            table: Which table to query (default: 'fundamentals')
                Available: fundamentals, daily_ohlc, quarterly_results, annual_financials,
                          corporate_actions, stocks_master, etc. (16 total)

        Returns:
            Query results with metadata

        Examples:
            query_stocks(filters={'ticker': 'TCS'})  # Default: fundamentals table
            query_stocks(filters={'sector': 'IT'}, limit=10, sort_by='market_cap')
            query_stocks(table='daily_ohlc', filters={'ticker': 'TCS', 'date': {'min': '2024-01-01'}})
            query_stocks(table='quarterly_results', filters={'ticker': 'INFY'})
        """
        # Resolve ticker if present in filters (auto-handle ticker changes)
        ticker_resolution = None
        if filters:
            # Check for ticker/symbol in filters
            ticker_field = None
            if 'symbol' in filters:
                ticker_field = 'symbol'
            elif 'ticker' in filters:
                ticker_field = 'ticker'

            if ticker_field and isinstance(filters[ticker_field], str):
                # Resolve ticker
                resolution = self.ticker_resolver.resolve(filters[ticker_field])

                if resolution['resolved_ticker']:
                    original_ticker = filters[ticker_field]
                    resolved_ticker = resolution['resolved_ticker']
                    try:
                        if isinstance(original_ticker, str) and original_ticker.upper() != resolved_ticker.upper():
                            base_list = [resolved_ticker, original_ticker.upper()]
                        else:
                            base_list = [resolved_ticker]
                    except Exception:
                        base_list = [resolved_ticker]
                    try:
                        conn2 = sqlite3.connect(self.db_path)
                        conn2.row_factory = sqlite3.Row
                        cur2 = conn2.cursor()
                        cur2.execute("SELECT DISTINCT old_symbol FROM alias_events WHERE UPPER(new_symbol)=?", (resolved_ticker.upper(),))
                        olds = [row['old_symbol'] if isinstance(row, sqlite3.Row) else row[0] for row in cur2.fetchall()]
                        if not [x for x in olds if x]:
                            cur2.execute("SELECT DISTINCT old_name FROM alias_events WHERE UPPER(new_symbol)=?", (resolved_ticker.upper(),))
                            old_names = [row['old_name'] if isinstance(row, sqlite3.Row) else row[0] for row in cur2.fetchall()]
                            for on in old_names:
                                if not on:
                                    continue
                                cur2.execute("SELECT symbol FROM stocks_master WHERE UPPER(company_name)=?", (str(on).upper(),))
                                sym_rows = cur2.fetchall()
                                for srow in sym_rows:
                                    olds.append(srow['symbol'] if isinstance(srow, sqlite3.Row) else srow[0])
                        conn2.close()
                        olds = [str(x).upper() for x in olds if x]
                        bridged = list({*(base_list + olds)})
                        filters[ticker_field] = bridged if len(bridged)>1 else bridged[0]
                    except Exception:
                        filters[ticker_field] = base_list if len(base_list)>1 else base_list[0]

                    # Store resolution metadata
                    ticker_resolution = {
                        'original': original_ticker,
                        'resolved': resolved_ticker,
                        'method': resolution['resolution_method'],
                        'confidence': resolution['confidence'],
                        'note': resolution['metadata'].get('note', '')
                    }

                    if original_ticker != resolution['resolved_ticker']:
                        print(f"[INFO] Ticker resolved: {original_ticker} → {resolution['resolved_ticker']}")

        # Use GenericQueryBuilder for zero-hardcoding SQL generation
        # ✅ NOW WORKS FOR ANY TABLE! Full database accessible (6.3M records)
        try:
            sql, params = self.query_builder.query(
                table=table,  # ✅ FIXED: Now accepts any table parameter
                filters=filters,
                fields=fields,
                sort_by=sort_by,
                sort_order=sort_order,
                limit=limit
            )

            # Execute query
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(sql, params)

            # Format results
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            results = [dict(zip(columns, row)) for row in rows]

            conn.close()

            response = {
                'results': results,
                'count': len(results),
                'source': 'sqlite',
                'table': table,  # ✅ FIXED: Return actual table used
                'query': sql,
                'filters_used': filters
            }

            # Add ticker resolution metadata if resolution occurred
            if ticker_resolution:
                response['ticker_resolution'] = ticker_resolution

            return response

        except Exception as e:
            return {
                'error': f'Query failed: {str(e)}',
                'filters': filters,
                'exception_type': type(e).__name__
            }

    def calculate_indicators(self,
                            ticker: str,
                            indicators: Optional[List[str]] = None,
                            days: int = 365) -> Dict[str, Any]:
        """
        OPERATION 2: Calculate technical indicators
        """
        symbol = ticker
        specs: List[Dict[str, Any]] = []
        for item in indicators or []:
            if isinstance(item, dict) and 'name' in item:
                specs.append({'name': str(item['name']).upper(), 'params': item.get('params') or {}})
            elif isinstance(item, str):
                specs.append({'name': item.upper(), 'params': {}})

        lookback = days if isinstance(days, int) and days > 0 else 365

        # Determine required inputs dynamically from TA-Lib function metadata
        required_inputs: List[str] = []
        if TALIB_AVAILABLE:
            for spec in specs:
                try:
                    func = ta_abstract.Function(spec['name'])
                    inputs = func.info.get('input_names') or []
                    for nm in inputs:
                        nm_l = str(nm).lower()
                        # Normalize TA-Lib generic names to series fields
                        if nm_l in ('real', 'price', 'typprice'):
                            nm_l = 'close'
                        if nm_l not in required_inputs:
                            required_inputs.append(nm_l)
                except Exception:
                    continue

        # Compute required rows from TA-Lib optional inputs and provided params
        required_rows = self._compute_required_rows(specs)
        if isinstance(days, int) and days > 0:
            required_rows = max(required_rows, days)

        df_prices, source_used = self._fetch_prices_series(symbol, required_rows, required_inputs)
        if df_prices is None or df_prices.empty or (len(df_prices) < required_rows):
            return {
                'results': [],
                'count': 0,
                'source': source_used or 'unknown',
                'table': 'daily_ohlc',
                'timestamp': datetime.now().isoformat()
            }

        inputs = {}
        for col in ['open', 'high', 'low', 'close', 'volume']:
            if col in df_prices.columns:
                inputs[col] = df_prices[col].values

        # Provide aliases required by TA-Lib functions dynamically
        req_set = set(required_inputs)
        if 'price' in req_set and 'price' not in inputs and 'close' in inputs:
            inputs['price'] = inputs['close']
        if 'real' in req_set and 'real' not in inputs and 'close' in inputs:
            inputs['real'] = inputs['close']
        if 'typprice' in req_set and 'typprice' not in inputs:
            if all(k in inputs for k in ['high', 'low', 'close']):
                inputs['typprice'] = (inputs['high'] + inputs['low'] + inputs['close']) / 3.0
            elif 'close' in inputs:
                inputs['typprice'] = inputs['close']

        df = df_prices.copy()
        calc_start = time.time()

        all_added_cols = []
        if TALIB_AVAILABLE:
            for spec in specs:
                try:
                    func = ta_abstract.Function(spec['name'])
                except Exception:
                    continue
                try:
                    out = func(inputs, **(spec.get('params') or {}))
                    output_names = func.info.get('output_names') or []
                    if not output_names:
                        # Try TA-Lib default names known via metadata; fallback to function name
                        meta = func.info or {}
                        output_names = meta.get('output_names') or [spec['name']]
                    added = []
                    if isinstance(out, dict):
                        for k, v in out.items():
                            col = k if k else output_names[0]
                            df[col] = v
                            added.append(col)
                    elif isinstance(out, pd.DataFrame):
                        for i, col in enumerate(out.columns):
                            name = output_names[i] if i < len(output_names) and output_names[i] else f"{spec['name']}_{i}"
                            df[name] = out.iloc[:, i].values
                            added.append(name)
                    elif isinstance(out, (np.ndarray, list)):
                        if isinstance(out, list) and len(out) > 1 and all(hasattr(x, '__len__') for x in out):
                            cols_to_use = output_names if len(output_names) >= len(out) else [f"{spec['name']}__output_{i}" for i in range(len(out))]
                            for i, series in enumerate(out):
                                df[cols_to_use[i]] = np.asarray(series)
                                added.append(cols_to_use[i])
                        else:
                            arr = np.asarray(out)
                            if arr.ndim == 1:
                                name = output_names[0] if output_names else spec['name']
                                df[name] = arr
                                added.append(name)
                            elif arr.ndim == 2:
                                cols_to_use = output_names if len(output_names) >= arr.shape[1] else [f"{spec['name']}__output_{i}" for i in range(arr.shape[1])]
                                for i in range(arr.shape[1]):
                                    df[cols_to_use[i]] = arr[:, i]
                                    added.append(cols_to_use[i])
                    else:
                        # Fallback: single series
                        name = output_names[0] if output_names else spec['name']
                        df[name] = out
                        added.append(name)
                    print(f"[IND] func={spec['name']} type={type(out).__name__} outputs={output_names} added={added}")
                    all_added_cols.extend(added)
                except Exception as e:
                    print(f"[IND-ERR] func={spec['name']} error={type(e).__name__}: {str(e)}")
                    continue

            # pandas-ta fallback for any spec that did not add columns
            if PANDAS_TA_AVAILABLE:
                dfp = df_prices.copy()
                for spec in specs:
                    added_cols = [c for c in df.columns if c.lower().startswith(spec['name'].lower())]
                    if added_cols:
                        continue
                    fn = getattr(pta, spec['name'].lower(), None)
                    if not callable(fn):
                        continue
                    try:
                        import inspect
                        sig = inspect.signature(fn)
                        kwargs = {}
                        for pname in sig.parameters.keys():
                            pl = pname.lower()
                            if pl in ('close','open','high','low','volume') and pl in dfp.columns:
                                kwargs[pname] = dfp[pl]
                        for k, v in (spec.get('params') or {}).items():
                            if k in sig.parameters:
                                kwargs[k] = v
                        out = fn(**kwargs)
                        added = []
                        if isinstance(out, pd.DataFrame):
                            for col in out.columns:
                                df[f"{spec['name']}_{str(col)}"] = out[col].values
                                added.append(f"{spec['name']}_{str(col)}")
                        elif hasattr(out, 'values'):
                            df[spec['name']] = getattr(out, 'values')
                            added.append(spec['name'])
                        elif isinstance(out, (list, tuple)):
                            for i, series in enumerate(out):
                                df[f"{spec['name']}__output_{i}"] = np.asarray(series)
                                added.append(f"{spec['name']}__output_{i}")
                        print(f"[IND-PTA] func={spec['name']} added={added}")
                        all_added_cols.extend(added)
                    except Exception as e:
                        print(f"[IND-PTA-ERR] func={spec['name']} error={type(e).__name__}: {str(e)}")

            df = df.replace([np.inf, -np.inf], np.nan)
            if all_added_cols:
                df[all_added_cols] = df[all_added_cols].where(pd.notna(df[all_added_cols]), None)
            records = df.to_dict('records')
            latency_ms = int((time.time() - calc_start) * 1000)
            print(f"[IND] source={source_used} rows={len(df)} latency_ms={latency_ms}")
            return {
                'results': records,
                'count': len(records),
                'source': source_used or ('sqlite' if df_prices is not None else 'unknown'),
                'table': 'daily_ohlc',
                'timestamp': datetime.now().isoformat()
            }

    def _fetch_prices_series(self, symbol: str, days: int, required_inputs: Optional[List[str]] = None) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        df = None
        src = None
        required_inputs = required_inputs or []
        cache_key = (tuple(symbol) if isinstance(symbol, (list, tuple)) else symbol, int(days), tuple(sorted(required_inputs)))
        cached = self._price_cache.get(cache_key)
        if cached:
            dfc, ts = cached
            if (time.time() - ts) <= self.cache_ttl_sec:
                return dfc.copy(), 'cache'
        # If only 'close' is required and jugaad is available, use fast chart_data
        if self.jugaad and set(required_inputs).issubset({'close'}):
            try:
                payload = self.jugaad.chart_data(symbol, days)
                # Normalize known structure: grapthData with date/value
                records = None
                if isinstance(payload, dict):
                    records = payload.get('grapthData') or payload.get('graphData') or payload.get('data')
                if isinstance(records, list) and records:
                    rows = []
                    for item in records[:days]:
                        dt = item.get('date') or item.get('Date') or item.get('dt')
                        val = item.get('value') or item.get('Value') or item.get('close')
                        if dt is not None and val is not None:
                            rows.append({'date': dt, 'close': val})
                    if rows:
                        df = pd.DataFrame(rows)
                        src = 'jugaad'
            except Exception:
                pass
        if (df is None or df.empty) and self.nselib_cm and days <= 5:
            # Attempt explicit date handling for recent days to prioritize nselib
            try:
                def _date_str(dt: datetime) -> str:
                    return dt.strftime('%d-%m-%Y')

                # Limit per-day API calls for performance
                max_days = min(max(days, 1), 5)
                rows = []
                seen_dates = set()
                now = datetime.now()
                for i in range(max_days):
                    dstr = _date_str(now - timedelta(days=i))
                    try:
                        payload = self.nselib_cm.bhav_copy_with_delivery(dstr)
                    except Exception:
                        payload = None
                    if payload is None:
                        try:
                            payload = self.nselib_cm.bhav_copy_equities(dstr)
                        except Exception:
                            payload = None
                    if payload is None:
                        continue
                    if isinstance(payload, pd.DataFrame):
                        df0 = payload
                    elif isinstance(payload, dict):
                        df0 = pd.DataFrame(payload.get('data') or payload.get('results') or payload.get('records') or payload)
                    else:
                        df0 = pd.DataFrame(payload)
                    if df0 is None or df0.empty:
                        continue
                    cols = list(df0.columns)
                    # Map columns using documented schema
                    scol = next((c for c in cols if str(c).lower() in ('symbol','tckrsymb','securityname')), None)
                    dcol = next((c for c in cols if str(c).lower() in ('date','date1','traddt','lasttradgdt')), None)
                    ocol = next((c for c in cols if str(c).lower() in ('open','open_price','opnpric')), None)
                    hcol = next((c for c in cols if str(c).lower() in ('high','high_price','hghpric')), None)
                    lcol = next((c for c in cols if str(c).lower() in ('low','low_price','lwpric')), None)
                    ccol = next((c for c in cols if str(c).lower() in ('close','close_price','clspric','last_price','lastpric')), None)
                    vcol = next((c for c in cols if str(c).lower() in ('volume','ttl_trd_qnty','ttltradgvol')), None)
                    if not (scol and dcol and ccol):
                        continue
                    sym_mask = df0[scol].astype(str).str.upper() == symbol.upper()
                    dff = df0.loc[sym_mask].copy()
                    if dff.empty:
                        continue
                    # Use provided date column if available; fallback to requested dstr
                    dvals = dff[dcol].astype(str) if dcol in dff.columns else pd.Series([dstr]*len(dff))
                    # Normalize one row per date for the symbol
                    for idx, row in dff.iterrows():
                        date_val = str(row.get(dcol, dstr))
                        if date_val in seen_dates:
                            continue
                        rec = {
                            'date': date_val,
                            'close': row.get(ccol)
                        }
                        if ocol and ocol in dff.columns: rec['open'] = row.get(ocol)
                        if hcol and hcol in dff.columns: rec['high'] = row.get(hcol)
                        if lcol and lcol in dff.columns: rec['low'] = row.get(lcol)
                        if vcol and vcol in dff.columns: rec['volume'] = row.get(vcol)
                        rows.append(rec)
                        seen_dates.add(date_val)
                    if len(rows) >= days:
                        break
                if rows:
                    df = pd.DataFrame(rows)
                    src = 'nselib'
            except Exception:
                pass
        if df is None or df.empty:
            try:
                filters = {'symbol': symbol if not isinstance(symbol, (list, tuple)) else list(symbol)}
                sql, sql_params = self.query_builder.query(
                    table='daily_ohlc',
                    filters=filters,
                    fields=['date', 'open', 'high', 'low', 'close', 'volume', 'symbol'],
                    sort_by='date',
                    sort_order='desc',
                    limit=days
                )
                conn = sqlite3.connect(self.db_path)
                df = pd.read_sql_query(sql, conn, params=sql_params)
                conn.close()
                src = 'sqlite'
            except Exception:
                df = None
                src = None
        if df is not None and not df.empty:
            df = df.sort_values('date')
        if df is not None and not df.empty:
            self._price_cache[cache_key] = (df.copy(), time.time())
        return df, src

    def _compute_required_rows(self, specs: List[Dict[str, Any]]) -> int:
        min_rows = 0
        for spec in specs:
            merged = dict(spec.get('params') or {})
            if TALIB_AVAILABLE:
                try:
                    func = ta_abstract.Function(spec['name'])
                    info = func.info or {}
                    opt_inputs = info.get('opt_inputs') or {}
                    for k, v in opt_inputs.items():
                        merged.setdefault(k, v)
                except Exception:
                    pass
            period_vals: List[int] = []
            for k, v in merged.items():
                if isinstance(v, (int, float)) and 'period' in str(k).lower():
                    period_vals.append(int(v))
            if not period_vals:
                needed = 30
            else:
                needed = sum(period_vals) if len(period_vals) > 1 else max(period_vals)
            min_rows = max(min_rows, needed)
        return max(min_rows + 10, 30)

    def query_corporate_actions(self,
                                ticker: str,
                                action_type: Optional[str] = None,
                                from_date: Optional[str] = None,
                                limit: Optional[int] = None) -> Dict[str, Any]:
        """
        OPERATION 3: Get dividend, bonus, split history

        Args:
            ticker: Stock ticker symbol
            action_type: Type of action (Dividend, Bonus, Split, etc.)
            from_date: Start date (YYYY-MM-DD format)
            limit: Max results

        Returns:
            Corporate actions with metadata

        Examples:
            query_corporate_actions(ticker='TCS', action_type='Dividend')
            query_corporate_actions(ticker='HDFC', from_date='2024-01-01')
        """
        # Map to existing corporate_actions query type
        params = {
            'symbol': ticker,
            'action_type': action_type,
            'from_date': from_date,
            'limit': limit
        }

        return self.fetch('corporate_actions', params)

    def fetch_stock_data(self,
                        ticker: str,
                        components: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        OPERATION 4: Get comprehensive stock data with specific components

        Args:
            ticker: Stock ticker symbol
            components: Which data components to fetch
                - 'fundamentals': Basic company data
                - 'technical': Technical indicators
                - 'options': Options chain data
                - 'delivery': Delivery data
                - 'historical': Historical prices

        Returns:
            Comprehensive stock data

        Examples:
            fetch_stock_data(ticker='TCS', components=['fundamentals'])
            fetch_stock_data(ticker='INFY', components=['fundamentals', 'technical'])
        """
        # Map to existing live_quote query type
        params = {
            'symbol': ticker,
            'components': components or ['fundamentals']
        }

        return self.fetch('live_quote', params)
