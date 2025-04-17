import pandas as pd
import os

excel_files = [
    'factsheet_testdata.xlsx',
    'returns_testdata.xlsx',
    'mutual_portfolio_testdata.xlsx',
    'navtimeseries_testdata.xlsx'
]

for file_name in excel_files:
    file_path = os.path.join('attached_assets', file_name)
    print(f"\n\n=== {file_name} ===")
    try:
        df = pd.read_excel(file_path)
        print(f"Columns: {df.columns.tolist()}")
        print(f"Number of rows: {len(df)}")
        print("First 3 rows:")
        print(df.head(3))
    except Exception as e:
        print(f"Error reading {file_name}: {str(e)}")