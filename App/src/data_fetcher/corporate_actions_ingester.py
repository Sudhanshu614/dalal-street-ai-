import sqlite3
from pathlib import Path
from typing import Optional, Dict, Any
import pandas as pd


class CorporateActionsIngester:
    def __init__(self, db_path: str, csv_path: Optional[str] = None):
        self.db_path = str(Path(db_path).resolve())
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self.csv_path = csv_path or self._discover_csv()
        self.schema = self._discover_table_schema('corporate_actions')

    def _discover_table_schema(self, table: str) -> Dict[str, Any]:
        cur = self.conn.cursor()
        cur.execute(f"PRAGMA table_info({table})")
        cols = [row[1] for row in cur.fetchall()]
        return {"columns": cols}

    def _discover_csv(self) -> str:
        base = Path(self.db_path).parent
        candidates = list(base.glob('CF-CA-*.csv'))
        if not candidates:
            raise FileNotFoundError('CF-CA CSV not found')
        candidates.sort()
        return str(candidates[-1].resolve())

    def _normalize_date(self, s: Any) -> Optional[str]:
        if s is None:
            return None
        try:
            return pd.to_datetime(s, format='%d-%b-%Y', errors='coerce').date().isoformat()
        except Exception:
            dt = pd.to_datetime(s, errors='coerce')
            return dt.date().isoformat() if pd.notnull(dt) else None

    def ingest(self, limit: Optional[int] = None) -> Dict[str, Any]:
        df = pd.read_csv(self.csv_path)
        df.columns = df.columns.str.strip()
        required = ['SYMBOL', 'PURPOSE', 'EX-DATE']
        for r in required:
            if r not in df.columns:
                raise ValueError('CSV missing required columns')

        cur = self.conn.cursor()
        count_inserted = 0
        rows = df.itertuples(index=False)
        processed = 0
        for row in rows:
            if limit is not None and processed >= limit:
                break
            processed += 1
            symbol = getattr(row, 'SYMBOL', None)
            purpose = getattr(row, 'PURPOSE', None)
            ex_date = self._normalize_date(getattr(row, 'EX-DATE', None))
            record_date = self._normalize_date(getattr(row, 'RECORD DATE', None) if hasattr(row, 'RECORD_DATE') is False else getattr(row, 'RECORD_DATE', None))
            bc_start = self._normalize_date(getattr(row, 'BOOK CLOSURE START DATE', None) if hasattr(row, 'BOOK_CLOSURE_START_DATE') is False else getattr(row, 'BOOK_CLOSURE_START_DATE', None))
            bc_end = self._normalize_date(getattr(row, 'BOOK CLOSURE END DATE', None) if hasattr(row, 'BOOK_CLOSURE_END_DATE') is False else getattr(row, 'BOOK_CLOSURE_END_DATE', None))
            face_value = getattr(row, 'FACE VALUE', None)
            if symbol is None or purpose is None or ex_date is None:
                continue
            cur.execute(
                "SELECT 1 FROM corporate_actions WHERE symbol=? AND action_type=? AND ex_date=? LIMIT 1",
                (str(symbol).upper().strip(), str(purpose).strip(), ex_date)
            )
            exists = cur.fetchone() is not None
            if exists:
                continue
            cols = self.schema['columns']
            vals: Dict[str, Any] = {}
            if 'symbol' in cols:
                vals['symbol'] = str(symbol).upper().strip()
            if 'action_type' in cols:
                vals['action_type'] = str(purpose).strip()
            if 'subject' in cols:
                vals['subject'] = str(purpose).strip()
            if 'ex_date' in cols:
                vals['ex_date'] = ex_date
            if 'record_date' in cols:
                vals['record_date'] = record_date
            if 'bc_start_date' in cols:
                vals['bc_start_date'] = bc_start
            if 'bc_end_date' in cols:
                vals['bc_end_date'] = bc_end
            if 'face_value' in cols:
                vals['face_value'] = face_value
            if 'data_source' in cols:
                vals['data_source'] = 'cf_ca_csv'
            keys = ','.join(vals.keys())
            placeholders = ','.join(['?'] * len(vals))
            cur.execute(
                f"INSERT INTO corporate_actions ({keys}) VALUES ({placeholders})",
                list(vals.values())
            )
            count_inserted += 1

        self.conn.commit()
        try:
            cur.execute(
                """
                INSERT INTO download_log (table_name, symbol, status, records_added, error_message, timestamp)
                VALUES (?, NULL, ?, ?, NULL, CURRENT_TIMESTAMP)
                """,
                (
                    'corporate_actions',
                    'success',
                    count_inserted
                )
            )
            self.conn.commit()
        except Exception:
            pass

        return {
            'csv_path': self.csv_path,
            'inserted': count_inserted,
            'processed': processed
        }

    def close(self):
        self.conn.close()