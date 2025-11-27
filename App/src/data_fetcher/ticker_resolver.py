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

    def _normalize_company_name(self, name):
        if not name:
            return ""
        
        name = name.upper().strip()
        name = name.replace('&', ' AND ')
        name = re.sub(r"[\.,\-/()'\"]+", " ", name)
        name = re.sub(r"\s+", " ", name).strip()
        
        # Common suffixes to remove (in order of specificity)
        suffixes = [
            ' LIMITED',
            ' LTD.',
            ' LTD',
            ' PRIVATE LIMITED',
            ' PVT. LTD.',
            ' PVT LTD',
            ' PVT.',
            ' PVT',
            ' CORPORATION',
            ' CORP.',
            ' CORP',
            ' INCORPORATED',
            ' INC.',
            ' INC',
            ' COMPANY',
            ' CO.',
            ' CO',
        ]
        
        for suffix in suffixes:
            if name.endswith(suffix):
                name = name[:-len(suffix)].strip()
                break
        
        return name

    def _normalize_tokens(self, text: str) -> List[str]:
        s = self._normalize_company_name(text)
        toks = [t for t in re.split(r"\s+", s) if t]
        stop = {
            'LIMITED','LTD','PRIVATE','PVT','COMPANY','CO','IND','INDUSTRIES','BANK','PLC'
        }
        out = []
        for t in toks:
            u = t.strip()
            if not u:
                continue
            if u in stop:
                continue
            if len(u) == 1:
                continue
            out.append(u)
        return out

    def _token_similarity(self, a: List[str], b: List[str]) -> float:
        if not a or not b:
            return 0.0
        aset = set(a)
        bset = set(b)
        match = 0
        for qt in aset:
            if qt in bset:
                match += 1
                continue
            for nt in bset:
                if len(qt) >= 3 and nt.startswith(qt):
                    match += 1
                    break
        return (match * 2) / (len(aset) + len(bset))

    def _fuzzy_match_active(self, query):
        """
        Fuzzy match query against active ticker symbols and company names.
        Uses suffix normalization for better matching (e.g., "TATA MOTORS" matches "TATA MOTORS LIMITED").
        
        Args:
            query: Search string (already uppercased)
            
        Returns:
            dict with {symbol, name, confidence} if match found, else None
        """
        import difflib
        
        query_upper = query.upper()
        
        # Normalize query for comparison
        query_normalized = self._normalize_company_name(query_upper)
        
        # First try exact symbol match
        if query_upper in self._active_tickers_cache:
            # Query DB to get company name
            cur = self.conn.cursor()
            cur.execute("SELECT company_name FROM stocks_master WHERE symbol = ?", (query_upper,))
            result = cur.fetchone()
            company_name = result['company_name'] if result else query_upper
            
            return {
                'symbol': query_upper,
                'name': company_name,
                'confidence': 100
            }
        
        # Try fuzzy symbol match
        symbols = list(self._active_tickers_cache)
        close_symbols = difflib.get_close_matches(query_upper, symbols, n=1, cutoff=0.85)
        if close_symbols:
            best_symbol = close_symbols[0]
            confidence = difflib.SequenceMatcher(None, query_upper, best_symbol).ratio() * 100
            
            # Get company name
            cur = self.conn.cursor()
            cur.execute("SELECT company_name FROM stocks_master WHERE symbol = ?", (best_symbol,))
            result = cur.fetchone()
            company_name = result['company_name'] if result else best_symbol
            
            return {
                'symbol': best_symbol,
                'name': company_name,
                'confidence': min(int(confidence), 99)  # Cap at 99 for fuzzy matches
            }
        
        # Try fuzzy company name match with token-based normalization
        cur = self.conn.cursor()
        cur.execute("""
            SELECT symbol, company_name 
            FROM stocks_master 
            WHERE is_active = 1
        """)
        
        matches = []
        for row in cur.fetchall():
            name_upper = row['company_name'].upper() if row['company_name'] else ''
            if name_upper:
                q_tokens = self._normalize_tokens(query_upper)
                n_tokens = self._normalize_tokens(name_upper)
                similarity = self._token_similarity(q_tokens, n_tokens)
                if similarity >= 0.85:
                    matches.append((name_upper, row['symbol'], similarity))
        
        if matches:
            # Sort by similarity score (highest first), then by name length (shorter first for tie-breaking)
            # This ensures "TATA MOTORS LIMITED" (shorter, exact match) beats 
            # "TATA MOTORS PASSENGER VEHICLES LIMITED" (longer, less exact)
            matches.sort(key=lambda x: (-x[2], len(x[0])))
            
            best_match = matches[0]
            best_name = best_match[0]
            best_symbol = best_match[1]
            confidence = best_match[2] * 100
            
            return {
                'symbol': best_symbol,
                'name': best_name,
                'confidence': min(int(confidence), 98)  # Cap at 98 for name matches
            }
        
        return None

    def resolve(self, ticker: str) -> Dict[str, Any]:
        """
        Resolve ticker using NSE authoritative data
        
        Resolution order (7 tiers):
        1. Active stock (direct match)
        2. Symbol change (NSE authoritative - 100% confidence)
        3. Name change (NSE authoritative - 100% confidence)
        4. Fuzzy name match (fallback - 85%+ confidence)
        5. Delisted (explicit)
        6. Demerger (from corporate_events)
        7. Not found (with suggestions)
        
        Args:
            ticker: Stock ticker symbol to resolve

        Returns:
            {
                'resolved_ticker': str or None,
                'original_ticker': str,
                'resolution_method': str,
                'confidence': int (0-100),
                'metadata': dict,
                'entity_type': str
            }
        """
        # Pre-process input
        original_input = ticker
        ticker = ticker.upper().strip()
        ticker = re.sub(r"\.", "", ticker)
        
        # Handle "Demerger of X" or "X Demerger"
        demerger_keywords = ['DEMERGER OF', 'DEMERGER']
        for kw in demerger_keywords:
            if kw in ticker:
                ticker = ticker.replace(kw, '').strip()
        
        # Handle common suffixes
        suffixes = [' IND.', ' IND', ' LTD.', ' LTD', ' LIMITED', ' PVT', ' PRIVATE']
        for s in suffixes:
            if ticker.endswith(s):
                ticker = ticker[:-len(s)].strip()

        # Tier 1: Check if ticker is currently active (no resolution needed)
        if ticker in self._active_tickers_cache:
            return {
                'resolved_ticker': ticker,
                'original_ticker': original_input,
                'resolution_method': 'direct',
                'confidence': 100,
                'metadata': {'status': 'active', 'note': 'Ticker is currently active'},
                'entity_type': 'stock'
            }
        
        # Tier 1b: Check if ticker is an ETF (direct match)
        if ticker in getattr(self, '_etf_symbols', set()):
            return {
                'resolved_ticker': ticker,
                'original_ticker': original_input,
                'resolution_method': 'etf_direct',
                'confidence': 100,
                'metadata': {'status': 'active', 'note': 'ETF ticker'},
                'entity_type': 'etf'
            }
        
        # Tier 1c: Check if ticker is an ETF (normalized match)
        normalized_ticker = self._normalize_etf_symbol(ticker)
        if normalized_ticker in getattr(self, '_normalized_etf_map', {}):
            actual_symbol = self._normalized_etf_map[normalized_ticker]
            return {
                'resolved_ticker': actual_symbol,
                'original_ticker': original_input,
                'resolution_method': 'etf_normalized',
                'confidence': 98,
                'metadata': {'normalized_from': ticker, 'note': 'ETF symbol normalized'},
                'entity_type': 'etf'
            }

        # Tier 1d: Index resolution - MOVED TO AFTER FUZZY MATCH (see after Tier 2.5)

        cur = self.conn.cursor()
        
        # Tier 2: Symbol change (Recursive A -> B -> C)
        # We loop to follow the chain of changes
        current_symbol = ticker
        chain = []
        max_hops = 5
        
        for _ in range(max_hops):
            try:
                cur.execute("""
                    SELECT new_symbol, change_date 
                    FROM symbol_change_events 
                    WHERE old_symbol = ?
                    ORDER BY change_date DESC LIMIT 1
                """, (current_symbol,))
                result = cur.fetchone()
                
                if result:
                    new_sym = result['new_symbol'] if isinstance(result, sqlite3.Row) else result[0]
                    change_date = result['change_date'] if isinstance(result, sqlite3.Row) else result[1]
                    
                    if new_sym:
                        chain.append((current_symbol, new_sym, change_date))
                        current_symbol = new_sym
                    else:
                        break
                else:
                    break
            except Exception:
                break
        
        if chain:
            # We found at least one change. 'current_symbol' is the final one.
            final_symbol = current_symbol
            is_active = final_symbol.upper() in self._active_tickers_cache
            
            # Construct chain string for metadata
            chain_str = " -> ".join([c[0] for c in chain] + [final_symbol])
            
            return {
                'resolved_ticker': final_symbol.upper(),
                'original_ticker': original_input,
                'resolution_method': 'symbol_change',
                'confidence': 100,
                'metadata': {
                    'change_chain': chain_str,
                    'final_change_date': chain[-1][2],
                    'note': 'NSE authoritative symbol change (recursive)',
                    'status': 'active' if is_active else 'inactive'
                },
                'entity_type': 'stock'
            }
        
        # Tier 2.5: High-confidence Fuzzy Match on Active Tickers (Pre-Name Change)
        # This prevents "Tata Motors" -> "TMPV" (Name Change) when "TMCV" exists
        # We prioritize an existing ACTIVE stock over a historical name change if the name is very similar
        # Thanks to suffix normalization, "TATA MOTORS" matches "TATA MOTORS LIMITED" with 98% confidence
        fuzzy_match = self._fuzzy_match_active(ticker)
        if fuzzy_match and fuzzy_match['confidence'] >= 85:  # Safe threshold thanks to normalization
             return {
                'resolved_ticker': fuzzy_match['symbol'],
                'original_ticker': original_input,
                'resolution_method': 'fuzzy_match_high_conf',
                'confidence': fuzzy_match['confidence'],
                'metadata': {'match_name': fuzzy_match['name'], 'note': 'High confidence fuzzy match prioritized'},
                'entity_type': 'stock'
            }
        
        # Tier 2.6: Index resolution (after stock fuzzy match to avoid false positives)
        # Moved here from Tier 1d to ensure stocks like "Jio Financial Services" don't match to "Nifty Financial Services" index
        idx = self.resolve_index(ticker)
        if idx:
            return idx

        # Tier 3: Name change (100% authoritative NSE data)
        
        # Tier 3: Name change (100% authoritative NSE data)
        try:
            cur.execute("""
                SELECT symbol, new_name, change_date
                FROM name_change_events
                WHERE old_name LIKE ? OR new_name LIKE ?
                ORDER BY change_date DESC LIMIT 1
            """, (f"%{ticker}%", f"%{ticker}%"))
            result = cur.fetchone()
            
            if result:
                sym = result['symbol'] if isinstance(result, sqlite3.Row) else result[0]
                if sym:
                    is_active = sym.upper() in self._active_tickers_cache
                    
                    # If the name change result is inactive, but we have a decent fuzzy match (e.g. >80%),
                    # prefer the fuzzy match.
                    if not is_active and fuzzy_match and fuzzy_match['confidence'] > 80:
                         return {
                            'resolved_ticker': fuzzy_match['symbol'],
                            'original_ticker': original_input,
                            'resolution_method': 'fuzzy_match_fallback',
                            'confidence': fuzzy_match['confidence'],
                            'metadata': {'match_name': fuzzy_match['name'], 'note': 'Fuzzy match preferred over inactive name change'},
                            'entity_type': 'stock'
                        }

                    return {
                        'resolved_ticker': sym.upper(),
                        'original_ticker': ticker,
                        'resolution_method': 'name_change',
                        'confidence': 100,
                        'metadata': {
                            'new_name': result['new_name'] if isinstance(result, sqlite3.Row) else result[1],
                            'change_date': result['change_date'] if isinstance(result, sqlite3.Row) else result[2],
                            'note': 'NSE authoritative name change',
                            'status': 'active' if is_active else 'inactive'
                        },
                        'entity_type': 'stock'
                    }
            else:
                nc_fuzzy = self._fuzzy_match_name_change(original_input)
                if nc_fuzzy and nc_fuzzy.get('confidence', 0) >= 75:
                    return {
                        'resolved_ticker': nc_fuzzy['symbol'],
                        'original_ticker': original_input,
                        'resolution_method': 'name_change_fuzzy',
                        'confidence': nc_fuzzy['confidence'],
                        'metadata': {'match_name': nc_fuzzy['name']},
                        'entity_type': 'stock'
                    }
        except Exception as e:
            pass
        
        # Tier 4: Delisted (explicit)
        try:
            cur.execute("""
                SELECT symbol, last_traded_date, delisting_reason
                FROM delisting_events
                WHERE symbol = ?
            """, (ticker,))
            result = cur.fetchone()
            
            if result:
                return {
                    'resolved_ticker': None,
                    'original_ticker': ticker,
                    'resolution_method': 'delisted',
                    'confidence': 100,
                    'metadata': {
                        'status': 'DELISTED',
                        'last_traded_date': result['last_traded_date'] if isinstance(result, sqlite3.Row) else result[1],
                        'reason': result['delisting_reason'] if isinstance(result, sqlite3.Row) else result[2]
                    },
                    'entity_type': 'stock'
                }
        except Exception as e:
            pass
        
        # Tier 5: Demerger (from corporate_events table)
        try:
            cur.execute("""
                SELECT symbol, ex_date, purpose
                FROM corporate_events
                WHERE event_type = 'DEMERGER'
                AND (symbol = ? OR purpose LIKE ?)
                ORDER BY ex_date DESC
                LIMIT 1
            """, (ticker, f"%{ticker}%"))
            result = cur.fetchone()
            
            if result:
                parent_symbol = result['symbol'] if isinstance(result, sqlite3.Row) else result[0]
                ex_date = result['ex_date'] if isinstance(result, sqlite3.Row) else result[1]
                
                from datetime import datetime, timedelta
                try:
                    ex_dt = datetime.strptime(str(ex_date), '%d-%b-%Y')
                except Exception:
                    try:
                        ex_dt = datetime.strptime(str(ex_date), '%Y-%m-%d')
                    except Exception:
                        ex_dt = None
                
                children: List[str] = []
                if ex_dt:
                    try:
                        cur.execute("""
                            SELECT symbol, listing_date
                            FROM stocks_master
                            WHERE listing_date IS NOT NULL
                        """)
                        rows = cur.fetchall()
                        for r in rows:
                            sym = r['symbol'] if isinstance(r, sqlite3.Row) else r[0]
                            ld = r['listing_date'] if isinstance(r, sqlite3.Row) else r[1]
                            if not ld:
                                continue
                            # Parse listing_date stored as 'DD-MMM-YYYY'
                            try:
                                ld_dt = datetime.strptime(str(ld), '%d-%b-%Y')
                            except Exception:
                                try:
                                    ld_dt = datetime.strptime(str(ld), '%Y-%m-%d')
                                except Exception:
                                    continue
                            if abs((ld_dt - ex_dt).days) <= 30:
                                children.append(str(sym).upper())
                    except Exception:
                        pass
                
                if len(children) == 1:
                    return {
                        'resolved_ticker': children[0],
                        'original_ticker': ticker,
                        'resolution_method': 'demerger_single_child',
                        'confidence': 85,
                        'metadata': {
                            'parent_symbol': parent_symbol,
                            'demerger_date': ex_date
                        },
                        'entity_type': 'stock'
                    }
                elif len(children) > 1:
                    return {
                        'resolved_ticker': None,
                        'original_ticker': ticker,
                        'resolution_method': 'demerger_multiple_children',
                        'confidence': 75,
                        'metadata': {
                            'parent_symbol': parent_symbol,
                            'children': children,
                            'note': 'Demerger created multiple entities - please specify which one'
                        },
                        'entity_type': 'stock'
                    }
        except Exception:
            pass
        
        # Tier 6: Not found - provide suggestions
        suggestions = self._get_similar_tickers(ticker)
        return {
            'resolved_ticker': None,
            'original_ticker': ticker,
            'resolution_method': 'not_found',
            'confidence': 0,
            'metadata': {
                'suggestions': suggestions
            },
            'entity_type': 'unknown'
        }

    def _fuzzy_match_name_change(self, query: str) -> Optional[Dict[str, Any]]:
        import difflib
        q = str(query or "").upper().strip()
        qn = self._normalize_company_name(q)
        cur = self.conn.cursor()
        try:
            cur.execute("""
                SELECT symbol, old_name, new_name
                FROM name_change_events
            """)
            rows = cur.fetchall()
        except Exception:
            return None
        best = None
        best_score = 0.0
        for r in rows:
            sym = r['symbol'] if isinstance(r, sqlite3.Row) else r[0]
            oldn = r['old_name'] if isinstance(r, sqlite3.Row) else r[1]
            newn = r['new_name'] if isinstance(r, sqlite3.Row) else r[2]
            for nm in (oldn, newn):
                if not nm:
                    continue
                nn = self._normalize_company_name(str(nm).upper())
                score = difflib.SequenceMatcher(None, qn, nn).ratio()
                if score > best_score:
                    best_score = score
                    best = {'symbol': str(sym).upper(), 'name': nm, 'confidence': int(score * 100)}
        return best

    def _get_similar_tickers(self, query: str) -> List[Dict[str, Any]]:
        import difflib
        q = str(query or "").upper().strip()
        qn = self._normalize_company_name(q)
        cur = self.conn.cursor()
        cur.execute("""
            SELECT symbol, company_name
            FROM stocks_master
            WHERE is_active = 1
        """)
        rows = cur.fetchall()
        items: List[Dict[str, Any]] = []
        for row in rows:
            sym = row['symbol'] if isinstance(row, sqlite3.Row) else row[0]
            name = row['company_name'] if isinstance(row, sqlite3.Row) else row[1]
            su = str(sym or "").upper()
            nu = self._normalize_company_name(str(name or ""))
            s_sim = difflib.SequenceMatcher(None, q, su).ratio()
            n_sim = difflib.SequenceMatcher(None, qn, nu).ratio()
            sim = max(s_sim, n_sim)
            items.append({
                'symbol': su,
                'name': str(name or ""),
                'confidence': int(sim * 100)
            })
        items.sort(key=lambda x: (-x['confidence'], len(x['name'])))
        return items[:5]



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
