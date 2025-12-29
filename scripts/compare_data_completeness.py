import pandas as pd
from datetime import timedelta
import os

# Find all CSV files in data directory
data_dir = 'data/'
csv_files = [
    'binance_ranges_1_12_minute_data.csv',
    'binance_sep_dec28_2025_partial_minute_data.csv',
    'binance_parallel_data.csv',
    'binance_dec25_minute_data.csv'
]

results = []

for filename in csv_files:
    filepath = os.path.join(data_dir, filename)
    
    if not os.path.exists(filepath):
        print(f"Skipping {filename} (not found)")
        continue
    
    print(f"\n{'='*70}")
    print(f"Analyzing: {filename}")
    print('='*70)
    
    try:
        # Load data
        df = pd.read_csv(filepath)
        
        if 'timestamp' not in df.columns:
            print(f"  ⚠ No 'timestamp' column found")
            continue
            
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Get basic stats
        file_size_mb = os.path.getsize(filepath) / (1024**2)
        total_rows = len(df)
        
        # Check if has symbol column
        if 'symbol' in df.columns:
            symbols = df['symbol'].unique()
            print(f"  Symbols: {', '.join(symbols)}")
            
            # Analyze each symbol
            for symbol in symbols:
                symbol_df = df[df['symbol'] == symbol].sort_values('timestamp')
                
                start = symbol_df['timestamp'].min()
                end = symbol_df['timestamp'].max()
                expected_minutes = int((end - start).total_seconds() / 60) + 1
                actual_minutes = len(symbol_df)
                completeness = (actual_minutes / expected_minutes) * 100
                
                # Count gaps
                symbol_df['time_diff'] = symbol_df['timestamp'].diff()
                gaps = symbol_df[symbol_df['time_diff'] > timedelta(minutes=1)]
                
                print(f"\n  {symbol}:")
                print(f"    Date range: {start} to {end}")
                print(f"    Duration: {(end - start).days} days")
                print(f"    Rows: {actual_minutes:,}")
                print(f"    Completeness: {completeness:.2f}%")
                print(f"    Gaps: {len(gaps)}")
                
                results.append({
                    'file': filename,
                    'symbol': symbol,
                    'size_mb': file_size_mb,
                    'rows': actual_minutes,
                    'start': start,
                    'end': end,
                    'days': (end - start).days,
                    'completeness': completeness,
                    'gaps': len(gaps)
                })
        else:
            # No symbol column
            start = df['timestamp'].min()
            end = df['timestamp'].max()
            expected_minutes = int((end - start).total_seconds() / 60) + 1
            actual_minutes = len(df)
            completeness = (actual_minutes / expected_minutes) * 100
            
            df['time_diff'] = df['timestamp'].diff()
            gaps = df[df['time_diff'] > timedelta(minutes=1)]
            
            print(f"  Date range: {start} to {end}")
            print(f"  Duration: {(end - start).days} days")
            print(f"  Rows: {actual_minutes:,}")
            print(f"  Completeness: {completeness:.2f}%")
            print(f"  Gaps: {len(gaps)}")
            
            results.append({
                'file': filename,
                'symbol': 'N/A',
                'size_mb': file_size_mb,
                'rows': actual_minutes,
                'start': start,
                'end': end,
                'days': (end - start).days,
                'completeness': completeness,
                'gaps': len(gaps)
            })
            
    except Exception as e:
        print(f"  ✗ Error: {e}")

# Summary
print(f"\n\n{'='*70}")
print("SUMMARY - RANKED BY COMPLETENESS")
print('='*70)

results_df = pd.DataFrame(results)
if len(results_df) > 0:
    results_df = results_df.sort_values('completeness', ascending=False)
    
    for idx, row in results_df.iterrows():
        print(f"\n{row['file']}")
        if row['symbol'] != 'N/A':
            print(f"  Symbol: {row['symbol']}")
        print(f"  Size: {row['size_mb']:.1f} MB")
        print(f"  Rows: {row['rows']:,}")
        print(f"  Period: {row['days']} days ({row['start'].date()} to {row['end'].date()})")
        print(f"  Completeness: {row['completeness']:.2f}%")
        print(f"  Gaps: {row['gaps']}")
        
    print(f"\n{'='*70}")
    print("RECOMMENDATION:")
    best = results_df.iloc[0]
    print(f"Most complete file: {best['file']}")
    print(f"  Completeness: {best['completeness']:.2f}%")
    print(f"  Coverage: {best['days']} days with {best['gaps']} gaps")
    print('='*70)



