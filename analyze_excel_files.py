import os
import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def analyze_excel_file(file_path):
    """
    Analyze an Excel file to understand its structure
    
    Args:
        file_path (str): Path to the Excel file
        
    Returns:
        dict: Information about the Excel file structure
    """
    try:
        # Read Excel file
        excel_data = pd.read_excel(file_path)
        
        # Get basic information
        info = {
            'file_name': os.path.basename(file_path),
            'num_rows': len(excel_data),
            'num_columns': len(excel_data.columns),
            'columns': list(excel_data.columns),
            'sample_data': excel_data.head(3).to_dict('records')
        }
        
        logger.info(f"Successfully analyzed {file_path}")
        return info
    except Exception as e:
        logger.error(f"Error analyzing {file_path}: {e}")
        return None

def analyze_all_excel_files():
    """
    Analyze all Excel files in the attached_assets directory
    """
    excel_files = [
        'attached_assets/factsheet_testdata.xlsx',
        'attached_assets/mutual_portfolio_testdata.xlsx',
        'attached_assets/navtimeseries_testdata.xlsx',
        'attached_assets/returns_testdata.xlsx'
    ]
    
    results = {}
    
    for file_path in excel_files:
        info = analyze_excel_file(file_path)
        if info:
            results[file_path] = info
    
    return results

if __name__ == "__main__":
    logger.info("Starting Excel file analysis...")
    results = analyze_all_excel_files()
    
    # Print results in a readable format
    for file_path, info in results.items():
        print(f"\n{'=' * 80}")
        print(f"File: {info['file_name']}")
        print(f"{'=' * 80}")
        print(f"Number of rows: {info['num_rows']}")
        print(f"Number of columns: {info['num_columns']}")
        print("\nColumns:")
        for col in info['columns']:
            print(f"  - {col}")
        
        print("\nSample data (first 3 rows):")
        for i, row in enumerate(info['sample_data']):
            print(f"\nRow {i+1}:")
            for col, val in row.items():
                print(f"  {col}: {val}")