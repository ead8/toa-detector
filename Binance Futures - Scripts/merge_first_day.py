import os
import pandas as pd
from datetime import datetime, timedelta
import glob

def merge_first_day_candles():
    """
    Merge all CSV files and extract only the first day's candles.
    Keep only: date/time, coin name, OHLCV columns.
    """
    
    # Get all CSV files in the data directory
    csv_files = glob.glob("data/*.csv")
    print(f"Found {len(csv_files)} CSV files")
    
    # List to store all first day dataframes
    all_first_day_data = []
    
    for csv_file in csv_files:
        try:
            # Extract coin name from filename
            coin_name = os.path.basename(csv_file).replace('.csv', '')
            
            # Read the CSV file
            df = pd.read_csv(csv_file)
            
            # Convert open_time to datetime
            df['open_time'] = pd.to_datetime(df['open_time'])
            
            # Get the first day (24 hours from the first candle)
            first_candle_time = df['open_time'].iloc[0]
            first_day_end = first_candle_time + timedelta(days=1)
            
            # Filter for first day only
            first_day_df = df[df['open_time'] < first_day_end].copy()
            
            if len(first_day_df) > 0:
                # Keep only the columns we want
                result_df = first_day_df[['open_time', 'open', 'high', 'low', 'close', 'volume']].copy()
                
                # Add coin name column
                result_df['coin'] = coin_name
                
                # Reorder columns: date/time, coin, OHLCV
                result_df = result_df[['open_time', 'coin', 'open', 'high', 'low', 'close', 'volume']]
                
                all_first_day_data.append(result_df)
                print(f"Processed {coin_name}: {len(first_day_df)} first-day candles")
            else:
                print(f"No first day data for {coin_name}")
                
        except Exception as e:
            print(f"Error processing {csv_file}: {e}")
    
    if all_first_day_data:
        # Concatenate all dataframes
        merged_df = pd.concat(all_first_day_data, ignore_index=True)
        
        # Sort by date/time and coin
        merged_df = merged_df.sort_values(['open_time', 'coin'])
        
        # Save to new CSV file
        output_file = "data/first_day_all_coins.csv"
        merged_df.to_csv(output_file, index=False)
        
        print(f"\nMerged data saved to: {output_file}")
        print(f"Total records: {len(merged_df)}")
        print(f"Unique coins: {merged_df['coin'].nunique()}")
        print(f"Date range: {merged_df['open_time'].min()} to {merged_df['open_time'].max()}")
        
        # Show sample of the data
        print("\nSample of merged data:")
        print(merged_df.head(10))
        
        return merged_df
    else:
        print("No data to merge!")
        return None

if __name__ == "__main__":
    merge_first_day_candles() 