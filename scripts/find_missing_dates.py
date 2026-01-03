import pandas as pd
from datetime import timedelta

# Load data
try:
    df = pd.read_csv('data/binance_sep_dec28_2025_partial_minute_data.csv')
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    print("Checking for missing data ranges...\n")

    for symbol in df['symbol'].unique():
        print(f"--- {symbol} ---")
        symbol_df = df[df['symbol'] == symbol].sort_values('timestamp')
        
        # Calculate time difference between consecutive rows
        symbol_df['time_diff'] = symbol_df['timestamp'].diff()
        
        # Find gaps larger than 1 minute
        gaps = symbol_df[symbol_df['time_diff'] > timedelta(minutes=1)]
        
        if len(gaps) == 0:
            print("No gaps found (continuous data).")
        else:
            print(f"Found {len(gaps)} gaps:")
            for idx, row in gaps.iterrows():
                gap_end = row['timestamp']
                gap_duration = row['time_diff']
                gap_start = gap_end - gap_duration
                
                # The missing range is explicitly from (last_seen + 1min) to (current - 1min)
                missing_start = gap_start + timedelta(minutes=1)
                missing_end = gap_end - timedelta(minutes=1)
                
                print(f"  Missing: {missing_start} to {missing_end} | Duration: {gap_duration - timedelta(minutes=1)}")
        print("\n")

except FileNotFoundError:
    print("Error: Data file not found at data/binance_sep_dec28_2025_partial_minute_data.csv")
except Exception as e:
    print(f"An error occurred: {e}")



