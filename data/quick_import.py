#!/usr/bin/env python3
"""
Quick import script that only imports factsheet data for the first 2 entries.
This is useful for testing when full data import is too time-consuming.
"""

import os
import sys
import logging
import pandas as pd
from datetime import datetime

# Add parent directory to path to enable imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from setup_db import create_app, db
from new_models_updated import Fund, FundFactSheet

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def parse_date(date_value):
    """Parse date values from various formats"""
    if pd.isna(date_value):
        return None
        
    if isinstance(date_value, datetime):
        return date_value.date()
        
    try:
        return datetime.strptime(str(date_value), '%Y-%m-%d').date()
    except ValueError:
        try:
            return datetime.strptime(str(date_value), '%d-%m-%Y').date()
        except ValueError:
            logger.warning(f"Could not parse date: {date_value}")
            return None

def import_minimal_data():
    """Import just the first 2 funds from factsheet data"""
    app = create_app()
    
    with app.app_context():
        # Define file paths
        root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        factsheet_file = os.path.join(root_dir, 'attached_assets', 'factsheet_testdata.xlsx')
        
        logger.info(f"Importing minimal data from {factsheet_file}")
        
        try:
            # Read Excel file
            df = pd.read_excel(factsheet_file)
            logger.info(f"Found {len(df)} records in factsheet data, using first 2")
            
            # Only use first 2 records
            df = df.head(2)
            
            # Get ISINs to import
            isins = df['ISIN'].tolist()
            logger.info(f"Importing funds with ISINs: {isins}")
            
            # Clear existing data for these ISINs
            FundFactSheet.query.filter(FundFactSheet.isin.in_(isins)).delete(synchronize_session=False)
            Fund.query.filter(Fund.isin.in_(isins)).delete(synchronize_session=False)
            db.session.commit()
            
            # Import funds and factsheets
            for _, row in df.iterrows():
                isin = row['ISIN']
                scheme_name = row['Scheme Name']
                fund_type = row['Type']
                fund_subtype = row['Subtype']
                amc_name = scheme_name.split(' ')[0]
                
                # Create fund
                fund = Fund(
                    isin=isin,
                    scheme_name=scheme_name,
                    fund_type=fund_type,
                    fund_subtype=fund_subtype,
                    amc_name=amc_name
                )
                db.session.add(fund)
                
                # Parse launch date
                launch_date = parse_date(row['Launch Date'])
                
                # Create factsheet
                factsheet = FundFactSheet(
                    isin=isin,
                    fund_manager=row['Fund Manager(s)'] if not pd.isna(row['Fund Manager(s)']) else None,
                    aum=row['AUM (₹ Cr)'] if not pd.isna(row['AUM (₹ Cr)']) else None,
                    expense_ratio=row['Expense Ratio'] if not pd.isna(row['Expense Ratio']) else None,
                    launch_date=launch_date,
                    exit_load=str(row['Exit Load']) if not pd.isna(row['Exit Load']) else None
                )
                db.session.add(factsheet)
            
            # Commit changes
            db.session.commit()
            logger.info(f"Successfully imported {len(df)} funds and factsheets")
            
            # Verify import
            funds = Fund.query.all()
            logger.info(f"Total funds in database: {len(funds)}")
            for fund in funds:
                logger.info(f"Fund: {fund.isin} - {fund.scheme_name}")
                
            return True
            
        except Exception as e:
            db.session.rollback()
            logger.error(f"Error importing minimal data: {e}")
            return False

if __name__ == "__main__":
    success = import_minimal_data()
    if success:
        logger.info("Quick import completed successfully")
    else:
        logger.error("Quick import failed")
        sys.exit(1)