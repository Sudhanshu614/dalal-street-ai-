import sys
import os
import sqlite3
import pandas as pd
import numpy as np
try:
    from talib import abstract as ta_abstract
    print("TA-Lib available")
except ImportError:
    print("TA-Lib NOT available")
    sys.exit(0)

def test_column_names():
    # Simulate what UniversalDataFetcher does
    specs = [{'name': 'RSI', 'params': {}}, {'name': 'SMA', 'params': {}}]
    
    # Mock data
    df = pd.DataFrame({
        'close': np.random.rand(100) * 100,
        'open': np.random.rand(100) * 100,
        'high': np.random.rand(100) * 100,
        'low': np.random.rand(100) * 100,
        'volume': np.random.rand(100) * 1000
    })
    
    inputs = {
        'close': df['close'].values,
        'open': df['open'].values,
        'high': df['high'].values,
        'low': df['low'].values,
        'volume': df['volume'].values,
        'price': df['close'].values,
        'real': df['close'].values
    }
    
    print("\n--- Testing Column Naming ---")
    for spec in specs:
        func = ta_abstract.Function(spec['name'])
        out = func(inputs, **(spec.get('params') or {}))
        output_names = func.info.get('output_names') or []
        print(f"Indicator: {spec['name']}")
        print(f"Output names: {output_names}")
        
        # NEW LOGIC: Use indicator name directly or as prefix
        if isinstance(out, (np.ndarray, list)):
            arr = np.asarray(out)
            if arr.ndim == 1:
                name = spec['name']
                print(f"Assigned column name: {name}")
                if name in df.columns:
                    print(f"WARNING: Column '{name}' already exists! Overwriting.")
                df[name] = arr
            else:
                # Handle multi-output
                pass

    print("\nFinal columns:", df.columns.tolist())

if __name__ == "__main__":
    test_column_names()
