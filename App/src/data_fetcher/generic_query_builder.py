"""
Generic Query Builder - Zero Hardcoding Implementation

Philosophy: Build SQL dynamically from parameters, validate against discovered schemas
NO hardcoded table names, NO hardcoded field names, NO hardcoded query patterns
"""

from typing import Dict, List, Any, Optional, Tuple
import sqlite3


class GenericQueryBuilder:
    """
    ZERO HARDCODING - Builds SQL queries dynamically for ANY table, ANY filters

    Examples:
        query('stocks', filters={'symbol': 'TCS'})
        query('stocks', filters={'sector': 'IT', 'pe_ratio': {'max': 15}}, sort_by='market_cap', limit=10)
        query(ANY_TABLE, ANY_FILTERS)  # Infinite combinations
    """

    def __init__(self, schemas: Dict[str, Dict[str, Any]]):
        """
        Initialize with discovered schemas

        Args:
            schemas: Dictionary mapping table_name -> schema_info
        """
        self.schemas = schemas

    def query(self,
              table: str,
              filters: Optional[Dict[str, Any]] = None,
              fields: Optional[List[str]] = None,
              sort_by: Optional[str] = None,
              sort_order: str = 'desc',
              limit: Optional[int] = None) -> Tuple[str, List[Any]]:
        """
        Build SQL query dynamically from parameters

        ZERO HARDCODING - Works for ANY valid parameter combination

        Args:
            table: Table name (validated against schema)
            filters: Filter conditions (validated against schema)
            fields: Fields to select (validated against schema, default: all)
            sort_by: Sort field (validated against schema)
            sort_order: 'asc' or 'desc'
            limit: Result limit

        Returns:
            (sql_query, params) tuple ready for execution

        Examples:
            # Simple query
            query('fundamentals', filters={'symbol': 'TCS'})
            # Output: ("SELECT * FROM fundamentals WHERE symbol = ?", ['TCS'])

            # Complex query
            query('fundamentals',
                  filters={'sector': 'IT', 'pe_ratio': {'max': 15}, 'roe': {'min': 20}},
                  sort_by='market_cap',
                  limit=10)
            # Output: ("SELECT * FROM fundamentals WHERE sector = ? AND pe_ratio <= ? AND roe >= ? ORDER BY market_cap DESC LIMIT 10",
            #          ['IT', 15, 20])
        """
        # Validate table exists
        if table not in self.schemas:
            available = list(self.schemas.keys())
            raise ValueError(f"Unknown table: '{table}'. Available tables: {available}")

        # Build SELECT clause
        select_str = self._build_select_clause(table, fields)

        # Build WHERE clause
        where_str, params = self._build_where_clause(table, filters)

        # Build ORDER BY clause
        order_str = self._build_order_clause(table, sort_by, sort_order)

        # Build LIMIT clause
        limit_str = self._build_limit_clause(limit)

        # Combine into final SQL
        sql_parts = [f"SELECT {select_str}", f"FROM {table}"]
        if where_str:
            sql_parts.append(where_str)
        if order_str:
            sql_parts.append(order_str)
        if limit_str:
            sql_parts.append(limit_str)

        sql = " ".join(sql_parts)
        return sql, params

    def _build_select_clause(self, table: str, fields: Optional[List[str]]) -> str:
        """
        Build SELECT clause dynamically

        If fields specified, validate against schema
        If no fields, use * (all fields)
        """
        if fields is None:
            return '*'

        # Validate all fields exist in table
        invalid = [f for f in fields if f not in self.schemas[table]['columns']]
        if invalid:
            available = self.schemas[table]['columns']
            raise ValueError(
                f"Invalid fields for table '{table}': {invalid}. "
                f"Available fields: {available}"
            )

        return ','.join(fields)

    def _build_where_clause(self, table: str, filters: Optional[Dict[str, Any]]) -> Tuple[str, List[Any]]:
        """
        Build WHERE clause dynamically from filters

        Supports:
        - Exact match: {'sector': 'IT'} → WHERE sector = 'IT'
        - Range: {'pe_ratio': {'max': 15}} → WHERE pe_ratio <= 15
        - Range: {'roe': {'min': 20}} → WHERE roe >= 20
        - Both: {'roe': {'min': 20, 'max': 50}} → WHERE roe >= 20 AND roe <= 50
        - IN clause: {'sector': ['IT', 'Banking']} → WHERE sector IN ('IT', 'Banking')
        """
        if not filters:
            return '', []

        where_clauses = []
        params = []

        for field, condition in filters.items():
            # Validate field exists in table
            if field not in self.schemas[table]['columns']:
                available = self.schemas[table]['columns']
                raise ValueError(
                    f"Invalid field '{field}' for table '{table}'. "
                    f"Available fields: {available}"
                )

            # Handle different condition types
            if isinstance(condition, dict):
                # Range query: {'min': X, 'max': Y}
                if 'min' in condition:
                    where_clauses.append(f"{field} >= ?")
                    params.append(condition['min'])
                if 'max' in condition:
                    where_clauses.append(f"{field} <= ?")
                    params.append(condition['max'])

            elif isinstance(condition, list):
                # IN query: ['IT', 'Banking']
                placeholders = ','.join(['?' for _ in condition])
                where_clauses.append(f"{field} IN ({placeholders})")
                params.extend(condition)

            else:
                # Exact match
                where_clauses.append(f"{field} = ?")
                params.append(condition)

        if where_clauses:
            where_str = f"WHERE {' AND '.join(where_clauses)}"
        else:
            where_str = ''

        return where_str, params

    def _build_order_clause(self, table: str, sort_by: Optional[str], sort_order: str) -> str:
        """
        Build ORDER BY clause dynamically

        Validates sort_by field exists in table
        """
        if not sort_by:
            return ''

        # Validate sort_by field exists
        if sort_by not in self.schemas[table]['columns']:
            cols = self.schemas[table]['columns']
            sbl = sort_by.lower()
            if sbl in ('date','dt','timestamp'):
                cands = [c for c in cols if 'date' in c.lower() or c.lower() in ('dt','timestamp')]
                if 'date' in cands:
                    sort_by = 'date'
                else:
                    end_date = [c for c in cands if c.lower().endswith('_date')]
                    sort_by = end_date[0] if end_date else (cands[0] if cands else '')
            else:
                sort_by = ''
            if not sort_by:
                return ''

        # Validate sort_order
        if sort_order.lower() not in ['asc', 'desc']:
            raise ValueError(f"Invalid sort_order: '{sort_order}'. Must be 'asc' or 'desc'")

        return f"ORDER BY {sort_by} {sort_order.upper()}"

    def _build_limit_clause(self, limit: Optional[int]) -> str:
        """
        Build LIMIT clause

        Validates limit is positive integer
        """
        if limit is None:
            return ''

        if not isinstance(limit, int) or limit <= 0:
            raise ValueError(f"Invalid limit: {limit}. Must be positive integer")

        return f"LIMIT {limit}"
