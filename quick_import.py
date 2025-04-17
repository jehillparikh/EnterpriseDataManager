import pandas as pd
import os
import logging
from datetime import datetime
from setup_db import create_app, db
from new_models_updated import Fund, FundFactSheet, FundReturns

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = create_app()

def import_fund_data():
    """Import funds with their factsheets and returns in a single transaction"""
    logger.info("Starting quick import...")
    factsheet_path = os.path.join('attached_assets', 'factsheet_testdata.xlsx')
    returns_path = os.path.join('attached_assets', 'returns_testdata.xlsx')
    
    # Read data files
    try:
        factsheet_df = pd.read_excel(factsheet_path)
        returns_df = pd.read_excel(returns_path)
        logger.info(f"Read {len(factsheet_df)} factsheet records and {len(returns_df)} returns records")
    except Exception as e:
        logger.error(f"Error reading Excel files: {e}")
        return
    
    # Create a returns lookup dictionary for faster access
    returns_lookup = {row['ISIN']: row for _, row in returns_df.iterrows()}
    
    with app.app_context():
        # Start a transaction
        try:
            # Process each fund from factsheet data
            funds_created = 0
            factsheets_created = 0
            returns_created = 0
            
            for _, row in factsheet_df.iterrows():
                isin = row['ISIN']
                
                # Skip if already exists
                fund = Fund.query.filter_by(isin=isin).first()
                if not fund:
                    # Extract AMC name from scheme name
                    scheme_name = row['Scheme Name']
                    amc_name = scheme_name.split(' ')[0]
                    
                    # Create fund
                    fund = Fund(
                        isin=isin,
                        scheme_name=scheme_name,
                        fund_type=row['Type'],
                        fund_subtype=row['Subtype'],
                        amc_name=amc_name
                    )
                    db.session.add(fund)
                    funds_created += 1
                
                # Create or skip factsheet
                factsheet = FundFactSheet.query.filter_by(isin=isin).first()
                if not factsheet:
                    # Process launch date
                    launch_date = None
                    if not pd.isna(row['Launch Date']):
                        if isinstance(row['Launch Date'], datetime):
                            launch_date = row['Launch Date'].date()
                        else:
                            try:
                                launch_date = datetime.strptime(str(row['Launch Date']), '%Y-%m-%d').date()
                            except ValueError:
                                pass
                    
                    factsheet = FundFactSheet(
                        isin=isin,
                        fund_manager=row['Fund Manager(s)'] if not pd.isna(row['Fund Manager(s)']) else None,
                        aum=row['AUM (₹ Cr)'] if not pd.isna(row['AUM (₹ Cr)']) else None,
                        expense_ratio=row['Expense Ratio'] if not pd.isna(row['Expense Ratio']) else None,
                        launch_date=launch_date,
                        exit_load=row['Exit Load'] if not pd.isna(row['Exit Load']) else None
                    )
                    db.session.add(factsheet)
                    factsheets_created += 1
                
                # Create or skip returns
                if isin in returns_lookup:
                    returns_row = returns_lookup[isin]
                    returns = FundReturns.query.filter_by(isin=isin).first()
                    
                    if not returns:
                        returns = FundReturns(
                            isin=isin,
                            return_1m=returns_row['1M Return'] if not pd.isna(returns_row['1M Return']) else None,
                            return_3m=returns_row['3M Return'] if not pd.isna(returns_row['3M Return']) else None,
                            return_6m=returns_row['6M Return'] if not pd.isna(returns_row['6M Return']) else None,
                            return_ytd=returns_row['YTD Return'] if not pd.isna(returns_row['YTD Return']) else None,
                            return_1y=returns_row['1Y Return'] if not pd.isna(returns_row['1Y Return']) else None,
                            return_3y=returns_row['3Y Return'] if not pd.isna(returns_row['3Y Return']) else None,
                            return_5y=returns_row['5Y Return'] if not pd.isna(returns_row['5Y Return']) else None
                        )
                        db.session.add(returns)
                        returns_created += 1
            
            # Commit all changes
            db.session.commit()
            logger.info(f"Import complete: {funds_created} funds, {factsheets_created} factsheets, {returns_created} returns")
            
            # Quick verification
            fund_count = Fund.query.count()
            factsheet_count = FundFactSheet.query.count()
            returns_count = FundReturns.query.count()
            
            logger.info(f"Database contains: {fund_count} funds, {factsheet_count} factsheets, {returns_count} returns")
            
        except Exception as e:
            db.session.rollback()
            logger.error(f"Error during import: {e}")
            return False
    
    return True

if __name__ == "__main__":
    import_fund_data()