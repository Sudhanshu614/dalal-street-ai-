"""
Ticker Resolution System - Dynamic Multi-Tier Strategy

Resolves old/changed ticker symbols to current active tickers using:
- stock_aliases table (name changes)
- CF-CA CSV (demergers)
- stocks_master table (active tickers)

NO HARDCODED MAPPINGS - all correlations discovered at runtime

Reference: FROM_SCRATCH_DOCS/TICKER_RESOLUTION_STRATEGY.md
Philosophy: Zero hardcoding, dynamic correlation
"""

import sqlite3
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from difflib import get_close_matches
import pandas as pd
import re


class TickerResolver:
    """
    Multi-tier ticker resolution using existing data sources

    Resolution Strategy:
    - Tier 1: Direct match (ticker exists in stocks_master with is_active=1)
    - Tier 2: Demerger correlation (stock_aliases + CF-CA + timeline matching)
    - Tier 3: Fuzzy name matching (company_name in stocks_master)
    - Tier 4: Not found (return suggestions)

    NO manual mappings - everything discovered at runtime
    """

    def __init__(self, db_path: str, csv_path: Optional[str] = None):
        """
        Initialize resolver with database and CSV paths

        Args:
            db_path: Path to SQLite database
            csv_path: Path to CF-CA CSV file (optional, for demerger correlation)
        """
        self.db_path = db_path
        self.csv_path = csv_path
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row

        # Load CF-CA data if available
        self.cf_ca_data = None
        if csv_path:
            try:
                self.cf_ca_data = pd.read_csv(csv_path)
            except Exception as e:
                print(f"[WARN] Could not load CF-CA CSV: {e}")
        # Load ETF symbols for detection
        self._etf_symbols = self._load_etf_symbols()
        # Build ETF normalization and alias maps (similar to indices)
        self._normalized_etf_map = {self._normalize_etf_symbol(s): s for s in self._etf_symbols}
        self._etf_alias_map = self._build_etf_alias_map(self._etf_symbols)

        # Cache active tickers for fast lookup
        self._active_tickers_cache = self._load_active_tickers()
        # Index names cache and alias map
        self._index_names = self._load_index_names()
        self._index_alias_map = self._build_index_alias_map(self._index_names)
        self._index_names = self._load_index_names()
        self._normalized_index_map = {self._normalize_index_name(n): n for n in self._index_names}

    def _load_active_tickers(self) -> set:
        """Load set of currently active ticker symbols from stocks_master"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT symbol FROM stocks_master WHERE is_active = 1")
        return {row['symbol'] for row in cursor.fetchall()}

    def _load_etf_symbols(self) -> set:
        """
        Load ETF symbols from database
        
        ETFs are stored in market_etfs table with a '-EQ' suffix (e.g., 'NIFTYBEES-EQ'),
        but users query them without the suffix ('NIFTYBEES'). This method loads the
        symbols and normalizes them to the user-facing format.
        
        Architecture Notes:
        - market_etfs.index_name:  'NIFTYBEES-EQ' (with suffix)
        - daily_ohlc.symbol:       'NIFTYBEES' (without suffix)
        - User queries:            'NIFTYBEES' (without suffix)
        - jugaad.stock_quote:      expects 'NIFTYBEES' (without suffix)
        
        Returns:
            Set of ETF ticker symbols in user-facing format (e.g., 'NIFTYBEES')
        """
        cursor = self.conn.cursor()
        try:
            # Load all ETF symbols from market_etfs and strip the -EQ suffix
            cursor.execute("SELECT DISTINCT index_name FROM market_etfs")
            
            etf_symbols = set()
            for row in cursor.fetchall():
                symbol_with_suffix = row['index_name'] or ''
                # Remove -EQ suffix to get user-facing format
                if symbol_with_suffix.endswith('-EQ'):
                    base_symbol = symbol_with_suffix[:-3]  # Remove last 3 chars ('-EQ')
                    if base_symbol:
                        etf_symbols.add(base_symbol)
            
            # Validate we actually loaded ETFs
            if not etf_symbols:
                print("[WARN] TickerResolver: No ETF symbols loaded - check database schema")
            else:
                print(f"[INFO] TickerResolver: Loaded {len(etf_symbols)} ETF symbols")
            
            return etf_symbols
            
        except Exception as e:
            print(f"[ERROR] TickerResolver: Failed to load ETF symbols: {e}")
            return set()

    def _load_index_names(self) -> List[str]:
        try:
            c = self.conn.cursor()
            c.execute("SELECT DISTINCT index_name FROM market_indices")
            rows = c.fetchall()
            out = []
            for r in rows:
                v = r['index_name'] if isinstance(r, sqlite3.Row) else r[0]
                if v:
                    out.append(str(v).strip())
            return out
        except Exception:
            return []

    def _normalize_index_name(self, s: str) -> str:
        x = str(s or '').upper()
        x = re.sub(r"[^A-Z0-9]", "", x)
        x = x.replace("NIFTYFIFTY", "NIFTY50")
        x = x.replace("NIFTYONEHUNDRED", "NIFTY100")
        x = x.replace("BANKNIFTY", "NIFTYBANK")
        x = x.replace("NIFTYBANK", "NIFTYBANK")
        if x == "NIFTY":
            x = "NIFTY50"
        return x

    def _normalize_etf_symbol(self, s: str) -> str:
        """
        Normalize ETF symbol for matching
        
        Handles common variations:
        - Spaces and hyphens: 'NIFTY BEES' -> 'NIFTYBEES'
        - Missing suffix 'S': 'NIFTYBEE' -> 'NIFTYBEES'
        - Case variations
        
        Args:
            s: ETF symbol to normalize
            
        Returns:
            Normalized symbol
        """
        x = str(s or '').upper().strip()
        # Remove all non-alphanumeric characters (spaces, hyphens, etc.)
        x = re.sub(r"[^A-Z0-9]", "", x)
        
        # Common normalizations for ETF patterns
        # Handle missing 'S' at end for BEES ETFs
        if x.endswith('BEE') and not x.endswith('BEES'):
            x = x + 'S'
        
        return x

    def _build_etf_alias_map(self, etf_symbols: set) -> Dict[str, List[str]]:
        """
        Build alias map for ETFs
        
        Maps normalized forms to list of actual ETF symbols.
        Handles common user query patterns.
        
        Args:
            etf_symbols: Set of actual ETF symbols
            
        Returns:
            Dict mapping normalized keys to lists of actual symbols
        """
        alias_map = {}
        
        for symbol in etf_symbols:
            normalized = self._normalize_etf_symbol(symbol)
            
            if normalized not in alias_map:
                alias_map[normalized] = []
            
            alias_map[normalized].append(symbol)
        
        return alias_map

    def resolve(self, ticker: str) -> Dict[str, Any]:
        """
        Resolve ticker using multi-tier strategy

        Args:
            ticker: Stock ticker symbol to resolve

        Returns:
            {
                'resolved_ticker': str or None,
                'original_ticker': str,
                'resolution_method': str,
                'confidence': int (0-100),
                'metadata': dict
            }
        """
        ticker = ticker.upper().strip()

        # Tier 1: Check if ticker is currently active (no resolution needed)
        if ticker in self._active_tickers_cache:
            return {
                'resolved_ticker': ticker,
                'original_ticker': ticker,
                'resolution_method': 'direct',
                'confidence': 100,
                'metadata': {'status': 'active', 'note': 'Ticker is currently active'},
                'entity_type': 'stock'
            }
        # Tier 1b: Check if ticker is an ETF (direct match)
        if ticker in getattr(self, '_etf_symbols', set()):
            return {
                'resolved_ticker': ticker,
                'original_ticker': ticker,
                'resolution_method': 'etf_direct',
                'confidence': 100,
                'metadata': {'status': 'active', 'note': 'ETF ticker'},
                'entity_type': 'etf'
            }
        
        # Tier 1c: Check if ticker is an ETF (normalized match)
        # Handles: 'NIFTY BEES' -> 'NIFTYBEES', 'NIFTYBEE' -> 'NIFTYBEES'
        normalized_ticker = self._normalize_etf_symbol(ticker)
        if normalized_ticker in getattr(self, '_normalized_etf_map', {}):
            actual_symbol = self._normalized_etf_map[normalized_ticker]
            return {
                'resolved_ticker': actual_symbol,
                'original_ticker': ticker,
                'resolution_method': 'etf_normalized',
                'confidence': 98,
                'metadata': {'normalized_from': ticker, 'note': 'ETF symbol normalized'},
                'entity_type': 'etf'
            }

        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT symbol, is_active FROM stocks_master WHERE UPPER(symbol) = ?", (ticker,))
            r = cursor.fetchone()
            if r and str(r['symbol']).upper() in self._active_tickers_cache:
                sym = str(r['symbol']).upper()
                return {
                    'resolved_ticker': sym,
                    'original_ticker': ticker,
                    'resolution_method': 'direct_lookup',
                    'confidence': 99,
                    'metadata': {'status': 'active'},
                    'entity_type': 'stock'
                }
        except Exception:
            pass

        # Fuzzy matching for stocks and ETFs
        if '%' not in ticker and '*' not in ticker:
            # Try stock symbol fuzzy match
            try:
                active = list(self._active_tickers_cache)
                matches = get_close_matches(ticker, active, n=1, cutoff=0.86)
                if matches:
                    m = matches[0]
                    from difflib import SequenceMatcher
                    sim = SequenceMatcher(None, ticker, m).ratio()
                    conf = int(sim*100)
                    return {
                        'resolved_ticker': m,
                        'original_ticker': ticker,
                        'resolution_method': 'symbol_fuzzy',
                        'confidence': conf,
                        'metadata': {'matched_symbol': m, 'similarity_score': sim},
                        'entity_type': 'stock'
                    }
            except Exception:
                pass
            
            # Try ETF symbol fuzzy match
            try:
                etf_symbols_list = list(getattr(self, '_etf_symbols', set()))
                if etf_symbols_list:
                    etf_matches = get_close_matches(ticker, etf_symbols_list, n=1, cutoff=0.86)
                    if etf_matches:
                        matched_etf = etf_matches[0]
                        from difflib import SequenceMatcher
                        sim = SequenceMatcher(None, ticker, matched_etf).ratio()
                        conf = int(sim*100)
                        return {
                            'resolved_ticker': matched_etf,
                            'original_ticker': ticker,
                            'resolution_method': 'etf_fuzzy',
                            'confidence': conf,
                            'metadata': {'matched_symbol': matched_etf, 'similarity_score': sim},
                            'entity_type': 'etf'
                        }
            except Exception:
                pass

        # Index resolution attempt
        idx = self._resolve_index(ticker)
        if idx:
            return idx

        # Tier 2: Fuzzy name matching (company_name in stocks_master)
        fuzzy_result = self._resolve_via_fuzzy_matching(ticker)
        if fuzzy_result and fuzzy_result['confidence'] >= 90:
            return fuzzy_result

        # Tier 3: Alias lineage traversal
        try:
            def clean(s):
                x = str(s or '')
                x = re.sub(r"\s+(Ltd\.?|Limited|Private|Pvt\.?|Corporation|Corp\.?|Inc\.?)$", "", x, flags=re.IGNORECASE)
                x = re.sub(r"[&()\[\].,]", " ", x)
                x = " ".join(x.split())
                return x.strip().upper()
            cursor = self.conn.cursor()
            cursor.execute(
                """
                SELECT new_symbol, new_name, effective_date, confidence, notes
                FROM alias_events
                WHERE old_symbol = ?
                ORDER BY effective_date DESC, id DESC
                """,
                (ticker,)
            )
            rows = cursor.fetchall()
            for row in rows:
                ns = row['new_symbol'] if isinstance(row, sqlite3.Row) else row[0]
                if ns and ns.upper() in self._active_tickers_cache:
                    conf = row['confidence'] if isinstance(row, sqlite3.Row) else row[3]
                    reason = row['notes'] if isinstance(row, sqlite3.Row) else (row[4] if len(row) > 4 else None)
                    # Prefer canonical company name for display
                    disp_name = None
                    try:
                        c2 = self.conn.cursor()
                        c2.execute("SELECT company_name FROM company_names_canonical WHERE symbol = ?", (ns.upper(),))
                        rr = c2.fetchone()
                        if rr:
                            disp_name = rr['company_name'] if isinstance(rr, sqlite3.Row) else rr[0]
                    except Exception:
                        pass
                    return {
                        'resolved_ticker': ns.upper(),
                        'original_ticker': ticker,
                        'resolution_method': 'alias_lineage',
                        'confidence': int(round(float(conf or 0)*100)),
                        'metadata': {
                            'effective_date': row['effective_date'] if isinstance(row, sqlite3.Row) else row[2],
                            'new_name': (disp_name or (row['new_name'] if isinstance(row, sqlite3.Row) else row[1])),
                            'reason': reason
                        },
                        'entity_type': 'stock'
                    }
            cursor.execute("SELECT company_name FROM stocks_master WHERE symbol = ?", (ticker,))
            r = cursor.fetchone()
            if r:
                cname = r['company_name'] if isinstance(r, sqlite3.Row) else r[0]
                c = clean(cname)
                cursor.execute(
                    """
                    SELECT new_symbol, new_name, effective_date, confidence, notes
                    FROM alias_events
                    WHERE UPPER(old_name) LIKE ? OR UPPER(new_name) LIKE ?
                    ORDER BY effective_date DESC, id DESC
                    """,
                    (f"%{c}%", f"%{c}%")
                )
                rows = cursor.fetchall()
                for row in rows:
                    ns = row['new_symbol'] if isinstance(row, sqlite3.Row) else row[0]
                    if ns and ns.upper() in self._active_tickers_cache:
                        conf = row['confidence'] if isinstance(row, sqlite3.Row) else row[3]
                        reason = row['notes'] if isinstance(row, sqlite3.Row) else (row[4] if len(row) > 4 else None)
                        # Prefer canonical company name for display
                        disp_name = None
                        try:
                            c2 = self.conn.cursor()
                            c2.execute("SELECT company_name FROM company_names_canonical WHERE symbol = ?", (ns.upper(),))
                            rr = c2.fetchone()
                            if rr:
                                disp_name = rr['company_name'] if isinstance(rr, sqlite3.Row) else rr[0]
                        except Exception:
                            pass
                        return {
                            'resolved_ticker': ns.upper(),
                            'original_ticker': ticker,
                            'resolution_method': 'alias_lineage',
                            'confidence': int(round(float(conf or 0)*100)),
                            'metadata': {
                                'effective_date': row['effective_date'] if isinstance(row, sqlite3.Row) else row[2],
                                'new_name': (disp_name or (row['new_name'] if isinstance(row, sqlite3.Row) else row[1])),
                                'reason': reason
                            },
                            'entity_type': 'stock'
                        }
            tc = clean(ticker)
            cursor.execute(
                """
                SELECT new_symbol, new_name, effective_date, confidence
                FROM alias_events
                WHERE UPPER(old_name) LIKE ? OR UPPER(new_name) LIKE ?
                ORDER BY effective_date DESC, id DESC
                """,
                (f"%{tc}%", f"%{tc}%")
            )
            rows = cursor.fetchall()
            for row in rows:
                ns = row['new_symbol'] if isinstance(row, sqlite3.Row) else row[0]
                if ns and ns.upper() in self._active_tickers_cache:
                    conf = row['confidence'] if isinstance(row, sqlite3.Row) else row[3]
                    return {
                        'resolved_ticker': ns.upper(),
                        'original_ticker': ticker,
                        'resolution_method': 'alias_lineage',
                        'confidence': int(round(float(conf or 0)*100)),
                        'metadata': {
                            'effective_date': row['effective_date'] if isinstance(row, sqlite3.Row) else row[2],
                            'new_name': row['new_name'] if isinstance(row, sqlite3.Row) else row[1]
                        },
                        'entity_type': 'stock'
                    }
        except Exception:
            pass

        # Tier 4: Demerger correlation (stock_aliases + CF-CA + timeline)
        demerger_result = self._resolve_via_demerger_correlation(ticker)
        if demerger_result:
            return demerger_result

        # Tier 5: Alias-only fallback when other signals are not found
        alias_only = self._resolve_via_alias_only(ticker)
        if alias_only:
            return alias_only

        # Tier 4: Not found - provide suggestions
        suggestions = self._get_similar_tickers(ticker)
        last_seen = self._check_historical_existence(ticker)

        return {
            'resolved_ticker': None,
            'original_ticker': ticker,
            'resolution_method': 'not_found',
            'confidence': 0,
            'metadata': {
                'suggestions': suggestions,
                'last_seen': last_seen
            },
            'entity_type': 'unknown'
        }



    def _resolve_index(self, text: str) -> Optional[Dict[str, Any]]:
        q = str(text or '').strip()
        if not q:
            return None
        q_upper = q.upper()
        try:
            for name in self._index_names:
                if q_upper == str(name).upper():
                    return {
                        'resolved_ticker': None,
                        'original_ticker': q,
                        'resolution_method': 'index_direct',
                        'confidence': 100,
                        'metadata': {'index_name': name},
                        'entity_type': 'index',
                        'resolved_index_name': name
                    }
        except Exception:
            pass
        norm_q = self._normalize_index_name(q_upper)
        try:
            tgt = self._normalized_index_map.get(norm_q)
            if tgt:
                return {
                    'resolved_ticker': None,
                    'original_ticker': q,
                    'resolution_method': 'index_normalized',
                    'confidence': 95,
                    'metadata': {'index_name': tgt},
                    'entity_type': 'index',
                    'resolved_index_name': tgt
                }
        except Exception:
            pass
        try:
            matches = get_close_matches(q_upper, [str(n).upper() for n in self._index_names], n=1, cutoff=0.8)
            if matches:
                m = matches[0]
                from difflib import SequenceMatcher
                sim = SequenceMatcher(None, q_upper, m).ratio()
                orig = None
                for n in self._index_names:
                    if str(n).upper() == m:
                        orig = n
                        break
                if orig:
                    return {
                        'resolved_ticker': None,
                        'original_ticker': q,
                        'resolution_method': 'index_fuzzy',
                        'confidence': int(sim*100),
                        'metadata': {'index_name': orig, 'similarity_score': sim},
                        'entity_type': 'index',
                        'resolved_index_name': orig
                    }
        except Exception:
            pass
        return None

    def _resolve_via_demerger_correlation(self, ticker: str) -> Optional[Dict[str, Any]]:
        """
        Resolve ticker by correlating:
        1. Name changes in stock_aliases table
        2. Demergers in CF-CA CSV
        3. Timeline proximity (within 30 days)

        Returns resolved ticker if correlation found, None otherwise
        """
        if self.cf_ca_data is None:
            return None

        try:
            # Step 1: Find name changes for companies matching ticker pattern
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT old_name, new_name, nse_symbol, change_date, confidence
                FROM stock_aliases
                WHERE old_name LIKE ? OR new_name LIKE ?
                ORDER BY confidence DESC
                LIMIT 5
            """, (f"%{ticker}%", f"%{ticker}%"))

            name_changes = cursor.fetchall()

            if not name_changes:
                return None

            # Step 2: For each name change, check if demerger occurred around same time
            for name_change in name_changes:
                new_name = name_change['new_name']
                change_date_str = name_change['change_date']
                nse_symbol = name_change['nse_symbol']

                if not change_date_str:
                    continue

                try:
                    change_date = datetime.strptime(change_date_str, '%Y-%m-%d')
                except:
                    try:
                        change_date = datetime.strptime(change_date_str, '%d-%b-%Y')
                    except:
                        continue

                date_min = (change_date - timedelta(days=30)).strftime('%d-%b-%Y')
                date_max = (change_date + timedelta(days=30)).strftime('%d-%b-%Y')

                company_keywords = new_name.split()[:3]
                df = self.cf_ca_data
                purposes = ['Demerger', 'Scheme of Arrangement', 'Amalgamation', 'Merger']
                mask_purpose = df['PURPOSE'].str.contains('|'.join(purposes), case=False, na=False)
                mask_company = df['COMPANY NAME'].str.contains('|'.join(company_keywords), case=False, na=False)
                mask_symbol = False
                if nse_symbol:
                    mask_symbol = (df['SYMBOL'].astype(str).str.upper() == str(nse_symbol).upper())
                try:
                    ex_dates = pd.to_datetime(df['EX-DATE'], errors='coerce', format='%d-%b-%Y')
                except Exception:
                    ex_dates = pd.to_datetime(df['EX-DATE'], errors='coerce')
                df = df.assign(__ex_date=ex_dates)
                window_mask = (df['__ex_date'] >= pd.to_datetime(date_min)) & (df['__ex_date'] <= pd.to_datetime(date_max))
                demergers = df[mask_purpose & (mask_company | mask_symbol) & window_mask]
                if demergers.empty:
                    date_min2 = (change_date - timedelta(days=365)).strftime('%d-%b-%Y')
                    date_max2 = (change_date + timedelta(days=365)).strftime('%d-%b-%Y')
                    window_mask2 = (df['__ex_date'] >= pd.to_datetime(date_min2)) & (df['__ex_date'] <= pd.to_datetime(date_max2))
                    demergers = df[mask_purpose & (mask_company | mask_symbol) & window_mask2]

                if not demergers.empty:
                    row = demergers.sort_values('__ex_date').iloc[0]
                    resolved_symbol = row['SYMBOL']
                    if resolved_symbol in self._active_tickers_cache:
                        ex_date = row['__ex_date']
                        days_apart = abs((change_date - ex_date.to_pydatetime()).days)
                        if days_apart <= 7:
                            confidence = 95
                        elif days_apart <= 30:
                            confidence = 90
                        elif days_apart <= 180:
                            confidence = 80
                        elif days_apart <= 365:
                            confidence = 70
                        else:
                            confidence = 60
                        return {
                            'resolved_ticker': resolved_symbol,
                            'original_ticker': ticker,
                            'resolution_method': 'demerger_correlation',
                            'confidence': confidence,
                            'metadata': {
                                'old_company_name': name_change['old_name'],
                                'new_company_name': new_name,
                                'change_date': change_date_str,
                                'demerger_date': row['EX-DATE'],
                                'note': f"Ticker changed from {ticker} to {resolved_symbol}",
                                'reason': str(row['PURPOSE'])
                            }
                        }

            return None

        except Exception as e:
            print(f"[WARN] Demerger correlation failed: {e}")
            return None

    def _resolve_via_alias_only(self, ticker: str) -> Optional[Dict[str, Any]]:
        """
        Resolve using stock_aliases table alone when CF-CA correlation is not available.

        Strategy:
        - Find alias rows where old_name/new_name contains the input
        - Prefer rows with a non-empty nse_symbol that is active in stocks_master
        - Use stored confidence (0-1) scaled to 0-100; default to 80 if missing
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                """
                SELECT old_name, new_name, nse_symbol, change_date, confidence
                FROM stock_aliases
                WHERE old_name LIKE ? OR new_name LIKE ?
                ORDER BY confidence DESC
                LIMIT 5
                """,
                (f"%{ticker}%", f"%{ticker}%")
            )
            rows = cursor.fetchall()
            if not rows:
                return None

            for row in rows:
                sym = (row[2] if isinstance(row, tuple) else row['nse_symbol'])
                if not sym:
                    continue
                sym_up = str(sym).upper()
                if sym_up in self._active_tickers_cache:
                    from difflib import SequenceMatcher
                    def clean_txt(s):
                        x = str(s or '')
                        x = re.sub(r"\s+(Ltd\.?|Limited|Private|Pvt\.?|Corporation|Corp\.?|Inc\.?)$", "", x, flags=re.IGNORECASE)
                        x = re.sub(r"[&()\[\].,]", " ", x)
                        x = " ".join(x.split())
                        return x.strip().upper()
                    t_clean = clean_txt(ticker)
                    old_name = row[0] if isinstance(row, tuple) else row['old_name']
                    new_name = row[1] if isinstance(row, tuple) else row['new_name']
                    oc = clean_txt(old_name)
                    nc = clean_txt(new_name)
                    sim_old = SequenceMatcher(None, t_clean, oc).ratio() if oc else 0.0
                    sim_new = SequenceMatcher(None, t_clean, nc).ratio() if nc else 0.0
                    sim_max = max(sim_old, sim_new)
                    if t_clean == sym_up or sim_max >= 0.88:
                        conf_val = (row[4] if isinstance(row, tuple) else row['confidence'])
                        try:
                            confidence = int(round(float(conf_val) * 100))
                        except Exception:
                            confidence = 80
                        change_date = row[3] if isinstance(row, tuple) else row['change_date']
                        return {
                            'resolved_ticker': sym_up,
                            'original_ticker': ticker,
                            'resolution_method': 'alias_only',
                            'confidence': confidence,
                            'metadata': {
                                'old_company_name': old_name,
                                'new_company_name': new_name,
                                'change_date': change_date,
                                'note': f"Resolved via stock_aliases without CF-CA correlation",
                                'reason': None
                            }
                        }
        except Exception:
            return None
        return None

    def _resolve_via_fuzzy_matching(self, ticker: str) -> Optional[Dict[str, Any]]:
        """
        Resolve ticker by fuzzy matching on company_name in stocks_master

        Examples:
        - "TATAMOTORS" → "Tata Motors" → TCS (if similarity > 85%)
        - "Tata Consultancy Services" → TCS
        """
        cursor = self.conn.cursor()

        # Get all active companies
        cursor.execute("""
            SELECT symbol, company_name
            FROM stocks_master
            WHERE is_active = 1
        """)

        companies = cursor.fetchall()

        # Try fuzzy matching on company names
        company_names = [row['company_name'] for row in companies if row['company_name']]
        matches = get_close_matches(ticker, company_names, n=1, cutoff=0.7)

        if matches:
            matched_name = matches[0]

            # Get symbol for matched company
            cursor.execute("""
                SELECT symbol FROM stocks_master
                WHERE company_name = ? AND is_active = 1
            """, (matched_name,))

            result = cursor.fetchone()
            if result:
                # Calculate similarity score
                from difflib import SequenceMatcher
                similarity = SequenceMatcher(None, ticker.lower(), matched_name.lower()).ratio()
                confidence = int(similarity * 100)

                return {
                    'resolved_ticker': result['symbol'],
                    'original_ticker': ticker,
                    'resolution_method': 'fuzzy_name_match',
                    'confidence': confidence,
                    'metadata': {
                        'matched_company_name': matched_name,
                        'similarity_score': similarity,
                        'note': f"Matched '{ticker}' to company name '{matched_name}'"
                    }
                }
            else:
                from difflib import SequenceMatcher
                similarity = SequenceMatcher(None, ticker.lower(), matched_name.lower()).ratio()
                cursor.execute("SELECT old_symbol, new_symbol, old_name, new_name, confidence FROM alias_events WHERE UPPER(old_name) = ? ORDER BY effective_date DESC, id DESC", (matched_name.upper(),))
                ev = cursor.fetchone()
                if ev:
                    ns = ev['new_symbol'] if isinstance(ev, sqlite3.Row) else ev[1]
                    if ns and ns.upper() in self._active_tickers_cache:
                        conf = ev['confidence'] if isinstance(ev, sqlite3.Row) else ev[4]
                        return {
                            'resolved_ticker': ns.upper(),
                            'original_ticker': ticker,
                            'resolution_method': 'alias_lineage',
                            'confidence': int(round(min(1.0, float(conf or 0)) * 100)),
                            'metadata': {
                                'matched_company_name': matched_name,
                                'similarity_score': similarity
                            }
                        }

        return None

    def _get_similar_tickers(self, ticker: str, limit: int = 5) -> List[str]:
        """Get similar ticker suggestions using difflib"""
        matches = get_close_matches(ticker, self._active_tickers_cache, n=limit, cutoff=0.6)
        return matches

    def _check_historical_existence(self, ticker: str) -> Optional[str]:
        """Check if ticker existed in the past (in daily_ohlc historical data)"""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT MAX(date) as last_seen
            FROM daily_ohlc
            WHERE symbol = ?
        """, (ticker,))

        result = cursor.fetchone()
        if result and result['last_seen']:
            return result['last_seen']

        return None

    def refresh_cache(self):
        self._active_tickers_cache = self._load_active_tickers()
        self._index_names = self._load_index_names()
        self._index_alias_map = self._build_index_alias_map(self._index_names)
        print(f"[INFO] Ticker resolver cache refreshed: {len(self._active_tickers_cache)} active tickers; {len(self._index_names)} indices")

    def _load_index_names(self) -> List[str]:
        try:
            c = self.conn.cursor()
            c.execute("SELECT DISTINCT index_name FROM market_indices")
            rows = c.fetchall()
            out = []
            for row in rows:
                nm = row['index_name'] if isinstance(row, sqlite3.Row) else row[0]
                if nm:
                    out.append(str(nm).strip())
            return out
        except Exception:
            return []

    def _normalize_index_key(self, s: str) -> str:
        return re.sub(r"[^A-Z0-9]", "", str(s or "").upper())

    def _build_index_alias_map(self, names: List[str]) -> Dict[str, List[str]]:
        m: Dict[str, List[str]] = {}
        def _add(key: str, val: str):
            if not key:
                return
            arr = m.get(key)
            if arr is None:
                m[key] = [val]
            else:
                if val not in arr:
                    arr.append(val)
        for name in names:
            n = str(name or "").strip()
            if not n:
                continue
            k = self._normalize_index_key(n)
            _add(k, n)
            u = n.upper()
            _add(u.replace(" ", ""), n)
            if u.startswith("NIFTY"):
                parts = [p for p in u.split() if p]
                if len(parts) >= 2:
                    x = parts[1]
                    alias = (x[:4] if len(x) >= 4 else x) + "NIFTY"
                    _add(alias, n)
                if len(parts) >= 3:
                    x2 = parts[1]
                    alias2 = (x2[:3] if len(x2) >= 3 else x2) + "NIFTY"
                    _add(alias2, n)
            if ("50" in u) and ("NIFTY" in u):
                _add("NIFTY", n)
        return m

    def resolve_index(self, text: str) -> Optional[Dict[str, Any]]:
        s = str(text or "").strip()
        if not s:
            return None
        k = self._normalize_index_key(s)
        cands = self._index_alias_map.get(k) or []
        if cands:
            q_tokens = [t for t in re.split(r"[^A-Z]+", k) if t]
            def score(name: str) -> int:
                u = self._normalize_index_key(name)
                n_tokens = [t for t in re.split(r"[^A-Z]+", u) if t]
                qs = set(q_tokens)
                ns = [t for t in n_tokens if t not in ("NIFTY", "INDEX", "INDICES")]
                match = 0
                for qt in qs:
                    for nt in ns:
                        if nt == qt or (len(qt) >= 3 and nt.startswith(qt)):
                            match += 1
                            break
                extra = max(len(ns) - match, 0)
                return match * 2 - extra
            best = sorted(cands, key=lambda nm: (score(nm), -len(nm)), reverse=True)[0]
            return {
                'resolved_index_name': best,
                'original_ticker': text,
                'resolution_method': 'index_alias',
                'confidence': 95,
                'metadata': {'type': 'index'},
                'entity_type': 'index'
            }
        from difflib import get_close_matches, SequenceMatcher
        keys = list(self._index_alias_map.keys())
        matches = get_close_matches(k, keys, n=1, cutoff=0.8)
        if matches:
            mk = matches[0]
            cands2 = self._index_alias_map.get(mk) or []
            name = cands2[0] if cands2 else None
            sim = SequenceMatcher(None, k, mk).ratio()
            if name:
                return {
                    'resolved_index_name': name,
                    'original_ticker': text,
                    'resolution_method': 'index_fuzzy',
                    'confidence': int(sim * 100),
                    'metadata': {'type': 'index'},
                    'entity_type': 'index'
                }
        return None

    def resolve_any(self, text: str) -> Dict[str, Any]:
        r = self.resolve(text)
        if r.get('resolved_ticker'):
            return r
        ir = self.resolve_index(text)
        if ir:
            return ir
        return r

    def __del__(self):
        """Close database connection"""
        if hasattr(self, 'conn'):
            self.conn.close()


# Convenience function for single-ticker resolution
def resolve_ticker(ticker: str, db_path: str, csv_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Resolve a single ticker symbol

    Args:
        ticker: Stock ticker symbol
        db_path: Path to SQLite database
        csv_path: Path to CF-CA CSV (optional)

    Returns:
        Resolution result dictionary

    Example:
        >>> result = resolve_ticker('TATAMOTORS', 'Database/stock_market_new.db', 'Database/CF-CA-*.csv')
        >>> print(result['resolved_ticker'])  # 'TMPV'
    """
    resolver = TickerResolver(db_path, csv_path)
    return resolver.resolve(ticker)
