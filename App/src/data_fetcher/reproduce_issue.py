import sys
import os
import time
import pandas as pd
import concurrent.futures

# Add App to path
current_dir = os.path.dirname(os.path.abspath(__file__))
app_root = os.path.abspath(os.path.join(current_dir, '..', '..', '..'))
if app_root not in sys.path:
    sys.path.insert(0, app_root)

from App.src.data_fetcher.universal_data_fetcher import UniversalDataFetcher

def run_stress_test():
    db_path = r"e:\Dalal Street Trae\App\database\stock_market_new.db"
    print(f"DB Path: {db_path}")
    
    if not os.path.exists(db_path):
        print("DB file not found!")
        return

    udf = UniversalDataFetcher(db_path)
    
    # Test Configuration
    tickers = ['TCS', 'INFY', 'RELIANCE', 'HDFCBANK', 'SBIN']
    indicators = [
        {'name': 'RSI', 'params': {'timeperiod': 14}},
        {'name': 'MACD', 'params': {'fastperiod': 12, 'slowperiod': 26, 'signalperiod': 9}},
        {'name': 'BBANDS', 'params': {'timeperiod': 20, 'nbdevup': 2, 'nbdevdn': 2}},
        {'name': 'SMA', 'params': {'timeperiod': 50}},
        {'name': 'EMA', 'params': {'timeperiod': 20}},
        {'name': 'STOCH', 'params': {}}
    ]
    
    print(f"\nStarting Stress Test...")
    print(f"Tickers: {tickers}")
    print(f"Indicators: {[i['name'] for i in indicators]}")
    
    start_total = time.time()
    
    for ticker in tickers:
        print(f"\nProcessing {ticker}...")
        t_start = time.time()
        try:
            # Fetch with all indicators
            result = udf.calculate_indicators(ticker, indicators, days=365)
            
            # Validation
            if not result.get('results'):
                print(f"❌ {ticker}: No results returned")
                continue
                
            df = pd.DataFrame(result['results'])
            cols = df.columns.tolist()
            
            # Check for expected columns
            missing = []
            
            found_cols = []
            for ind in indicators:
                name = ind['name']
                # Check if any column starts with the indicator name
                related = [c for c in cols if c.startswith(name)]
                if not related:
                    missing.append(name)
                else:
                    found_cols.extend(related)
            
            if missing:
                print(f"❌ {ticker}: Missing columns for {missing}")
            else:
                print(f"✅ {ticker}: Success ({len(df)} rows). Found columns: {found_cols}")
                
            print(f"   Time: {time.time() - t_start:.2f}s")
            
        except Exception as e:
            print(f"❌ {ticker}: Error - {e}")

    print(f"\nTotal Stress Test Time: {time.time() - start_total:.2f}s")

if __name__ == "__main__":
    run_stress_test()
