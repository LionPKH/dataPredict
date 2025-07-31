#!/usr/bin/env python3
"""
Script to clean speed_interpolated_improved.csv by removing rows where ax, ay, az or speed are null/empty
"""

import pandas as pd
import os

def clean_null_values():
    input_file = 'output_cleaned/speed_interpolated_improved.csv'
    
    # Check if file exists
    if not os.path.exists(input_file):
        print(f"Error: File {input_file} not found")
        return
    
    # Read the CSV file
    print(f"Reading {input_file}...")
    df = pd.read_csv(input_file)
    
    print(f"Original number of rows: {len(df)}")
    
    # Count rows with null values in the specified columns
    null_mask = df[['x_accel', 'y_accel', 'z_accel', 'speed']].isnull().any(axis=1)
    null_count = null_mask.sum()
    
    print(f"Rows with null values in x_accel, y_accel, z_accel, or speed: {null_count}")
    
    # Remove rows where x_accel, y_accel, z_accel, or speed are null
    df_cleaned = df.dropna(subset=['x_accel', 'y_accel', 'z_accel', 'speed'])
    
    print(f"Number of rows after cleaning: {len(df_cleaned)}")
    print(f"Removed {len(df) - len(df_cleaned)} rows")
    
    # Save the cleaned data to output/final_merged_data.csv
    output_file = 'output/final_merged_data.csv'
    df_cleaned.to_csv(output_file, index=False)
    print(f"Cleaned data saved to {output_file}")

if __name__ == "__main__":
    clean_null_values()