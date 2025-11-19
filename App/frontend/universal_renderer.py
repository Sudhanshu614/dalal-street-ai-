import streamlit as st
import pandas as pd
from datetime import datetime
from io import StringIO
import re

# Display toggles
SHOW_FOOTER = False

def _natural_label(token: str) -> str:
    # Preserve ALL_CAPS acronyms and ticker-like tokens (2–6 uppercase letters)
    if re.fullmatch(r'[A-Z]{2,6}', token):
        return token
    # Convert snake_case or camelCase to Title Case with spaces for non-acronyms
    s = re.sub(r'_+', ' ', token)
    s = re.sub(r'([a-z])([A-Z])', r"\1 \2", s)
    s = s.strip()
    return s.title()

def _sanitize_text(text: str) -> str:
    t = (text or "")
    # Strip fenced code blocks
    t = re.sub(r"```[\s\S]*?```", "", t)
    t = t.replace('\u00A0', ' ').replace('\xa0', ' ')
    t = t.replace('‑','-').replace('–','-').replace('—','-')
    # Ensure emoji markers start new lines anywhere in narrative
    try:
        t = re.sub(r"(?<!\n)(?:\s*)(📌|🔍|📊|📈|📝|📎|📋|📍|📣)", r"\n\n\1", t)
    except Exception:
        pass
    lines = t.split('\n')
    out = []
    header_re = re.compile(r'^\s*#{1,6}\s+')
    html_head_re = re.compile(r'<\s*h[1-6][^>]*>([\s\S]*?)<\s*/\s*h[1-6]\s*>', re.IGNORECASE)
    html_wrap_re = re.compile(r'<\s*(span|div|p)[^>]*>([\s\S]*?)<\s*/\s*\1\s*>', re.IGNORECASE)
    in_key_insights = False
    for line in lines:
        s = line.strip()
        if not s:
            out.append(s)
            continue
        if s.lower().startswith('this is educational information only'):
            continue
        if any(x in s for x in ['🕒','📅','📈']):
            continue
        if '\t' in s:
            # Drop tab-separated lines from narrative; tables are rendered separately
            continue
        if header_re.match(s):
            s = header_re.sub('', s).strip()
            if not s:
                continue
        if '<' in s and '>' in s:
            try:
                s = html_head_re.sub(lambda m: (m.group(1) or '').strip(), s)
                s = html_wrap_re.sub(lambda m: (m.group(2) or '').strip(), s)
                s = re.sub(r'\s*style=\"[^\"]*\"', '', s, flags=re.IGNORECASE)
            except Exception:
                pass
        # Remove obvious technical artifacts in-line
        if re.search(r"\b(SELECT|FROM|JOIN|WHERE|ORDER\s+BY|INSERT|UPDATE|DELETE)\b", s, re.IGNORECASE):
            continue
        s = re.sub(r'`+', '', s)
        s = re.sub(r'https?://localhost:\d+/\S+', '', s)
        s = re.sub(r'/api/\S+', '', s)
        s = re.sub(r"\b(query_stocks|calculate_indicators|fetch_stock_data)\b", '', s)
        # Transform technical identifiers to natural labels
        def repl_ident(m):
            tok = m.group(0)
            # Preserve ALL_CAPS acronyms/tickers; transform only other identifiers
            if tok.isupper():
                return tok
            if '_' in tok or re.search(r'[a-z][A-Z]', tok):
                return _natural_label(tok)
            return tok
        s = re.sub(r"\b[A-Za-z0-9_]{3,}\b", repl_ident, s)
        # Emoji-start lines → force a separate line (no bullets)
        if re.match(r"^(📌|🔍|📊|📈|📝|📎|📋|📍|📣)\s*", s):
            out.append(s)
            continue
        # Key Insights block → enforce bullet points per line
        if s.lower().startswith('key insights') or s.startswith('🔍 Key Insights'):
            in_key_insights = True
            out.append('**Key Insights:**')
            continue
        if in_key_insights:
            if not s:
                out.append('')
                in_key_insights = False
                continue
            # Emoji or any text line becomes a bullet
            out.append(f"- {s}")
            # Keep in insights mode until a blank line or a non-text separator
            continue
        out.append(s)
    s = '\n'.join(out)
    try:
        s = re.sub(r"\n{3,}", "\n\n", s)
    except Exception:
        pass
    return s.rstrip()

def _extract_tab_blocks(text: str) -> list:
    lines = (text or '').split('\n')
    blocks = []
    cur = []
    for ln in lines:
        s = (ln or '').strip()
        if '\t' in s:
            cur.append(s)
        else:
            if cur:
                blocks.append(cur)
                cur = []
    if cur:
        blocks.append(cur)
    return blocks

def _normalize_row(row: dict) -> dict:
    r = dict(row or {})
    # Pattern-driven semantic mapping: add canonical keys when matches are found
    for k in list(r.keys()):
        kl = str(k).lower()
        val = r.get(k)
        if val is None:
            continue
        if re.search(r"(?i)^(last|close)$|close|last\s*price", kl) and 'close' not in r:
            r['close'] = val
        elif re.search(r"(?i)^(open|open[_\s]?price)$", kl) and 'open' not in r:
            r['open'] = val
        elif re.search(r"(?i)^(high|high[_\s]?price)$", kl) and 'high' not in r:
            r['high'] = val
        elif re.search(r"(?i)^(low|low[_\s]?price)$", kl) and 'low' not in r:
            r['low'] = val
        elif re.search(r"(?i)^(prev.*close|previous.*close|prvs.*|previous\s*close)$", kl) and 'previous_close' not in r:
            r['previous_close'] = val
        elif re.search(r"(?i)^(p\s?change|per.*change|percent.*change|change\s?%)$", kl) and 'change_percent' not in r:
            r['change_percent'] = val
    return r

def _detect_dataset(raw: dict) -> str:
    if isinstance(raw, dict):
        tags = raw.get('tags')
        if isinstance(tags, list) and tags:
            if any('time_series' in str(t) for t in tags):
                return 'time_series'
            if any('corporate_actions' in str(t) for t in tags):
                return 'corporate_actions'
            if any('live_quote' in str(t) for t in tags):
                return 'single_stock'
    recs = (raw or {}).get('results') or []
    if recs:
        cols = set((recs[0] or {}).keys())
        if any('date' in str(k).lower() for k in cols):
            return 'time_series'
        if 'action_type' in cols or 'ex_date' in cols:
            return 'corporate_actions'
        if len(recs) == 1:
            return 'single_stock'
        return 'multiple_stocks'
    return 'unknown'

def _column_semantics(cols: list) -> list:
    # Prefer common time-series semantics first if present, else keep original order
    priority = ['date','open','high','low','close','volume','vwap','rsi','macd','macd_signal']
    present = [c for c in priority if c in cols]
    if present:
        return present + [c for c in cols if c not in present]
    return list(cols)

def _choose_identifier_column(df: pd.DataFrame) -> int:
    # Pick the first non-numeric column with highest token diversity
    best_idx = 0
    best_score = -1
    for idx, col in enumerate(df.columns):
        s = df[col]
        try:
            if pd.api.types.is_numeric_dtype(s):
                continue
            uniques = s.astype(str).nunique(dropna=True)
            avg_len = s.astype(str).map(len).mean()
            score = uniques + (avg_len or 0)
            if score > best_score:
                best_score = score
                best_idx = idx
        except Exception:
            continue
    return best_idx

def _format_date_str(s: str) -> str:
    try:
        d = datetime.fromisoformat(str(s).replace('Z','').split(' ')[0])
        return d.strftime('%d %b %Y')
    except Exception:
        try:
            d = datetime.strptime(str(s)[:10], '%Y-%m-%d')
            return d.strftime('%d %b %Y')
        except Exception:
            return str(s)

def _compute_footer(raw: dict, metadata: dict) -> tuple:
    interval = metadata.get('interval') or ''
    recs = (raw or {}).get('results') or []
    table = (raw or {}).get('table') or ''
    tags = (raw or {}).get('tags') or []
    if not interval:
        if table == 'daily_ohlc':
            interval = '1 day'
        else:
            if recs and any(any('date' in str(k).lower() for k in r.keys()) for r in recs):
                interval = '1 day'
    dmin = None
    dmax = None
    for r in recs:
        for k, v in (r or {}).items():
            if isinstance(v, str) and any(x in str(k).lower() for x in ['date','timestamp']):
                try:
                    d = datetime.fromisoformat(v.replace('Z','').split(' ')[0])
                except Exception:
                    try:
                        d = datetime.strptime(v[:10], '%Y-%m-%d')
                    except Exception:
                        d = None
                if d:
                    dmin = d if dmin is None or d < dmin else dmin
                    dmax = d if dmax is None or d > dmax else dmax
                break
    if dmin and dmax:
        dr = (_format_date_str(dmin), _format_date_str(dmax))
        date_range = dr[0] if dr[0] == dr[1] else f"{dr[0]} – {dr[1]}"
    elif dmax:
        date_range = _format_date_str(dmax)
    else:
        # If live quote without explicit dates, use today's date
        if any('live_quote' in str(t) for t in tags):
            date_range = datetime.now().strftime('%d %b %Y')
            interval = interval or '1 day'
        else:
            date_range = 'Date not available'
    return interval or 'N/A', date_range

def _logic_line(has_dates: bool, total_rows: int) -> str:
    if has_dates and total_rows > 1:
        return "Historical data formatted with Indian date and currency conventions"
    if has_dates and total_rows == 1:
        return "Single-day snapshot formatted with Indian date and currency conventions"
    return "Results formatted for clarity using Indian date and currency conventions"

def render_response(envelope: dict):
    if not envelope:
        st.warning("No response received from backend.")
        return
    text = envelope.get('response') or ''
    sanitized = _sanitize_text(text)
    tab_blocks = _extract_tab_blocks(text)
    render_narrative = True
    try:
        render_narrative = bool((envelope.get('metadata') or {}).get('render_narrative', True))
    except Exception:
        render_narrative = True
    if render_narrative:
        st.markdown(sanitized)
    rendered = False
    dfs = []
    for block in tab_blocks:
        if not block:
            continue
        cols_count = block[0].count('\t') + 1
        if cols_count < 2:
            continue
        try:
            df = pd.read_csv(StringIO('\n'.join(block)), sep='\t')
            dcol = next((c for c in df.columns if 'date' in c.lower()), None)
            if dcol:
                try:
                    df[dcol] = pd.to_datetime(df[dcol].astype(str), errors='coerce').dt.strftime('%d %b %Y')
                except Exception:
                    pass
            # Reorder columns to put identifier first
            id_idx = _choose_identifier_column(df)
            if id_idx != 0:
                cols = list(df.columns)
                id_col = cols[id_idx]
                new_cols = [id_col] + [c for c in cols if c != id_col]
                df = df[new_cols]
            df = df.reset_index(drop=True)
            # Adaptive numeric formatting without field lists
            def fmt_val(v):
                try:
                    if v is None:
                        return ''
                    if isinstance(v, (int,)):
                        return f"{v}"
                    fv = float(v)
                    if abs(fv) >= 1:
                        s = f"{fv:.2f}"
                    else:
                        s = f"{fv:.4f}"
                    return s.rstrip('0').rstrip('.')
                except Exception:
                    return v
            for c in df.columns:
                try:
                    if pd.api.types.is_numeric_dtype(df[c]):
                        df[c] = df[c].map(fmt_val)
                    else:
                        # Attempt numeric mapping for string numerics
                        df[c] = df[c].map(fmt_val)
                except Exception:
                    pass
            styler = df.style.hide(axis='index')
            html = styler.to_html()
            try:
                html = re.sub(r'<\s*style[^>]*>[\s\S]*?<\s*/\s*style\s*>', '', html, flags=re.IGNORECASE)
                # Add a stable class to the table for scoped CSS
                html = re.sub(r'<table', '<table class="rendered-table"', html, count=1)
            except Exception:
                pass
            css = (
                "<style>"
                ".rendered-table{width:max-content;}"
                ".rendered-table th,.rendered-table td{padding:6px 10px;}"
                ".rendered-table th{font-weight:600;white-space:nowrap;}"
                ".rendered-table td:first-child,.rendered-table th:first-child{font-weight:600;white-space:normal;}"
                ".rendered-table td:not(:first-child){white-space:nowrap;}"
                "</style>"
            )
            output = css + '<div class="rendered-table-container" style="overflow-x:auto;">' + html + '</div>'
            st.markdown(output, unsafe_allow_html=True)
            dfs.append(df)
            rendered = True
        except Exception:
            continue
    if rendered and SHOW_FOOTER:
        st.markdown('---')
        meta = envelope.get('metadata') or {}
        interval = meta.get('interval') or ''
        # Compute date range from rendered tables
        dmin = None
        dmax = None
        has_dates = False
        total_rows = 0
        unique_dates = set()
        for df in dfs:
            dcol = next((c for c in df.columns if 'date' in c.lower()), None)
            if dcol:
                try:
                    vals = pd.to_datetime(df[dcol].astype(str), errors='coerce')
                    vals = vals.dropna()
                    if not vals.empty:
                        vmin = vals.min()
                        vmax = vals.max()
                        dmin = vmin if dmin is None or vmin < dmin else dmin
                        dmax = vmax if dmax is None or vmax > dmax else dmax
                        has_dates = True
                        unique_dates.update(list(vals.dt.date))
                except Exception:
                    pass
            try:
                total_rows += len(df)
            except Exception:
                pass
        # Only show interval when more than one unique date exists
        if not interval:
            interval = '1 day' if len(unique_dates) > 1 else ''
        if dmin and dmax:
            s1 = dmin.strftime('%d %b %Y')
            s2 = dmax.strftime('%d %b %Y')
            date_range = s1 if s1 == s2 else f"{s1} – {s2}"
        elif dmax:
            date_range = dmax.strftime('%d %b %Y')
        else:
            date_range = 'Date not available'
        if SHOW_FOOTER:
            if interval:
                st.markdown(f"🕒 Candle Interval Used: {interval}")
            if date_range and date_range != 'Date not available':
                st.markdown(f"📅 Data Range: {date_range}")
            st.markdown(f"📈 Logic Used: {_logic_line(has_dates, total_rows)}")
SYNONYMS = {
    'close': ['close', 'last_price', 'lastPrice', 'finalPrice'],
    'company_name': ['company_name', 'companyName', 'name', 'symbolDesc'],
    'macd_signal': ['macd_signal', 'macdSignal', 'macdsignal'],
    'change_percent': ['change_percent', 'pChange', 'perChange', 'percentChange'],
    'previous_close': ['previous_close', 'prevClose', 'PrvsClsgPric', 'previousClose'],
    'open': ['open', 'open_price', 'Open', 'OPEN_PRICE'],
    'high': ['high', 'high_price', 'High', 'HIGH_PRICE'],
    'low': ['low', 'low_price', 'Low', 'LOW_PRICE'],
}
