#!/usr/bin/env python3
import pandas as pd
import sys

def check_excel_columns(file_path):
    """Check column structure of Excel file"""
    try:
        df = pd.read_excel(file_path)
        print(f"File: {file_path}")
        print(f"Total rows: {len(df)}")
        print(f"Total columns: {len(df.columns)}")
        print("\nColumn names:")
        for i, col in enumerate(df.columns, 1):
            print(f"{i:2d}. '{col}'")
        
        print("\nFirst 3 rows:")
        print(df.head(3).to_string(max_cols=8))
        
        return df.columns.tolist()
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return None

if __name__ == "__main__":
    # Check the uploaded portfolio file
    file_path = "temp_uploads/portfolio_data.xlsx"
    columns = check_excel_columns(file_path)
    
    if columns:
        print(f"\nColumns found: {columns}")