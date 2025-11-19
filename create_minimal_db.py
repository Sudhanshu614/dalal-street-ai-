import sqlite3
import os

# Path to create minimal database
db_path = r"e:\Dalal Street Trae\App\database\stock_market_minimal.db"

# Remove if exists
if os.path.exists(db_path):
    os.remove(db_path)

# Create database
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Create stocks_master table (minimal version)
cursor.execute("""
CREATE TABLE IF NOT EXISTS stocks_master (
    symbol TEXT PRIMARY KEY,
    company_name TEXT,
    sector TEXT,
    industry TEXT,
    isin TEXT,
    is_active INTEGER DEFAULT 1,
    series TEXT DEFAULT 'EQ',
    date_of_listing TEXT,
    face_value REAL,
    market_lot INTEGER,
    last_updated TEXT
)
""")

# Create daily_ohlc table (minimal version)
cursor.execute("""
CREATE TABLE IF NOT EXISTS daily_ohlc (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    date TEXT NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    prev_close REAL,
    volume INTEGER,
    value REAL,
    trades INTEGER,
    deliverable_qty INTEGER,
    deliverable_percent REAL,
    spread_h_l REAL,
    spread_c_o REAL,
    UNIQUE(symbol, date)
)
""")

# Create fundamentals table (minimal version)
cursor.execute("""
CREATE TABLE IF NOT EXISTS fundamentals (
    symbol TEXT PRIMARY KEY,
    market_cap REAL,
    pe_ratio REAL,
    pb_ratio REAL,
    dividend_yield REAL,
    roe REAL,
    debt_to_equity REAL,
    current_ratio REAL,
    book_value REAL,
    face_value REAL,
    eps REAL,
    industry_pe REAL,
    week_high_52 REAL,
    week_low_52 REAL,
    last_updated TEXT
)
""")

# Create corporate_actions table
cursor.execute("""
CREATE TABLE IF NOT EXISTS corporate_actions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    company_name TEXT,
    ex_date TEXT,
    purpose TEXT,
    record_date TEXT,
    bc_start_date TEXT,
    bc_end_date TEXT,
    nd_start_date TEXT,
    nd_end_date TEXT,
    actual_payment_date TEXT,
    UNIQUE(symbol, ex_date, purpose)
)
""")

# Insert a few sample stocks so the database isn't empty
sample_stocks = [
    ('TCS', 'Tata Consultancy Services Ltd', 'Information Technology', 'IT Services & Consulting', 'INE467B01029', 1),
    ('INFY', 'Infosys Ltd', 'Information Technology', 'IT Services & Consulting', 'INE009A01021', 1),
    ('RELIANCE', 'Reliance Industries Ltd', 'Energy', 'Refineries', 'INE002A01018', 1),
    ('HDFCBANK', 'HDFC Bank Ltd', 'Financial Services', 'Banks', 'INE040A01034', 1),
    ('ICICIBANK', 'ICICI Bank Ltd', 'Financial Services', 'Banks', 'INE090A01021', 1),
]

cursor.executemany("""
    INSERT OR IGNORE INTO stocks_master (symbol, company_name, sector, industry, isin, is_active)
    VALUES (?, ?, ?, ?, ?, ?)
""", sample_stocks)

conn.commit()
conn.close()

print(f"✅ Minimal database created at: {db_path}")
print(f"📊 Contains {len(sample_stocks)} sample stocks")
print(f"📦 File size: {os.path.getsize(db_path) / 1024:.2f} KB")