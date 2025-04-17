import pandas as pd
import os
import logging
from datetime import datetime
from setup_db import create_app, db
from new_models_updated import Fund, FundFactSheet, FundReturns

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = create_app()

def import_factsheet_data():
    """Import data from factsheet_testdata.xlsx"""
    logger.info("Importing factsheet data...")
    file_path = os.path.join('attached_assets', 'factsheet_testdata.xlsx')
    
    df = pd.read_excel(file_path)
    
    with app.app_context():
        for _, row in df.iterrows():
            # Create fund if it doesn't exist
            isin = row['ISIN']
            fund = Fund.query.filter_by(isin=isin).first()
            
            if not fund:
                fund_type = row['Type']
                fund_subtype = row['Subtype']
                scheme_name = row['Scheme Name']
                
                # Extract AMC name from scheme name
                amc_name = scheme_name.split(' ')[0]  # This assumes the first word is the AMC name
                
                fund = Fund(
                    isin=isin,
                    scheme_name=scheme_name,
                    fund_type=fund_type,
                    fund_subtype=fund_subtype,
                    amc_name=amc_name
                )
                db.session.add(fund)
                
            # Create or update factsheet
            factsheet = FundFactSheet.query.filter_by(isin=isin).first()
            
            launch_date = None
            if not pd.isna(row['Launch Date']):
                if isinstance(row['Launch Date'], datetime):
                    launch_date = row['Launch Date'].date()
                else:
                    try:
                        launch_date = datetime.strptime(str(row['Launch Date']), '%d-%m-%Y').date()
                    except ValueError:
                        try:
                            launch_date = datetime.strptime(str(row['Launch Date']), '%Y-%m-%d').date()
                        except ValueError:
                            logger.warning(f"Could not parse launch date for {isin}: {row['Launch Date']}")
            
            if not factsheet:
                factsheet = FundFactSheet(
                    isin=isin,
                    fund_manager=row['Fund Manager(s)'] if not pd.isna(row['Fund Manager(s)']) else None,
                    aum=row['AUM (₹ Cr)'] if not pd.isna(row['AUM (₹ Cr)']) else None,
                    expense_ratio=row['Expense Ratio'] if not pd.isna(row['Expense Ratio']) else None,
                    launch_date=launch_date,
                    exit_load=row['Exit Load'] if not pd.isna(row['Exit Load']) else None
                )
                db.session.add(factsheet)
            else:
                factsheet.fund_manager = row['Fund Manager(s)'] if not pd.isna(row['Fund Manager(s)']) else factsheet.fund_manager
                factsheet.aum = row['AUM (₹ Cr)'] if not pd.isna(row['AUM (₹ Cr)']) else factsheet.aum
                factsheet.expense_ratio = row['Expense Ratio'] if not pd.isna(row['Expense Ratio']) else factsheet.expense_ratio
                factsheet.launch_date = launch_date if launch_date else factsheet.launch_date
                factsheet.exit_load = row['Exit Load'] if not pd.isna(row['Exit Load']) else factsheet.exit_load
        
        db.session.commit()
        logger.info(f"Imported {df.shape[0]} factsheet records")
        
        # Display what we imported
        funds = Fund.query.all()
        logger.info(f"Total funds in database: {len(funds)}")
        for fund in funds[:5]:  # Show first 5 only
            logger.info(f"Fund: {fund.isin} - {fund.scheme_name}")
            
        factsheets = FundFactSheet.query.all()
        logger.info(f"Total factsheets in database: {len(factsheets)}")

def import_returns_data():
    """Import data from returns_testdata.xlsx"""
    logger.info("Importing returns data...")
    file_path = os.path.join('attached_assets', 'returns_testdata.xlsx')
    
    df = pd.read_excel(file_path)
    
    with app.app_context():
        for _, row in df.iterrows():
            isin = row['ISIN']
            
            # Skip if fund doesn't exist (should have been created during factsheet import)
            fund = Fund.query.filter_by(isin=isin).first()
            if not fund:
                logger.warning(f"Skipping returns for {isin} - fund does not exist")
                continue
            
            # Create or update returns
            returns = FundReturns.query.filter_by(isin=isin).first()
            
            if not returns:
                returns = FundReturns(
                    isin=isin,
                    return_1m=row['1M Return'] if not pd.isna(row['1M Return']) else None,
                    return_3m=row['3M Return'] if not pd.isna(row['3M Return']) else None,
                    return_6m=row['6M Return'] if not pd.isna(row['6M Return']) else None,
                    return_ytd=row['YTD Return'] if not pd.isna(row['YTD Return']) else None,
                    return_1y=row['1Y Return'] if not pd.isna(row['1Y Return']) else None,
                    return_3y=row['3Y Return'] if not pd.isna(row['3Y Return']) else None,
                    return_5y=row['5Y Return'] if not pd.isna(row['5Y Return']) else None
                )
                db.session.add(returns)
            else:
                returns.return_1m = row['1M Return'] if not pd.isna(row['1M Return']) else returns.return_1m
                returns.return_3m = row['3M Return'] if not pd.isna(row['3M Return']) else returns.return_3m
                returns.return_6m = row['6M Return'] if not pd.isna(row['6M Return']) else returns.return_6m
                returns.return_ytd = row['YTD Return'] if not pd.isna(row['YTD Return']) else returns.return_ytd
                returns.return_1y = row['1Y Return'] if not pd.isna(row['1Y Return']) else returns.return_1y
                returns.return_3y = row['3Y Return'] if not pd.isna(row['3Y Return']) else returns.return_3y
                returns.return_5y = row['5Y Return'] if not pd.isna(row['5Y Return']) else returns.return_5y
        
        db.session.commit()
        logger.info(f"Imported {df.shape[0]} returns records")
        
        # Display what we imported
        returns = FundReturns.query.all()
        logger.info(f"Total returns records in database: {len(returns)}")
        for record in returns[:5]:  # Show first 5 only
            logger.info(f"Returns for {record.isin}: 1M={record.return_1m}, 1Y={record.return_1y}")

def main():
    # Import data in order: factsheets, returns
    import_factsheet_data()
    import_returns_data()
    
    logger.info("Basic data import complete")

if __name__ == "__main__":
    main()