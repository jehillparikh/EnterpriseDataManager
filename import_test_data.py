import pandas as pd
import os
from datetime import datetime
from setup_db import create_app, db
from new_models_updated import Fund, FundFactSheet, FundReturns, PortfolioHolding, NavHistory

app = create_app()

def import_factsheet_data():
    """Import data from factsheet_testdata.xlsx"""
    print("Importing factsheet data...")
    file_path = os.path.join('attached_assets', 'factsheet_testdata.xlsx')
    
    df = pd.read_excel(file_path)
    
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
                        print(f"Could not parse launch date for {isin}: {row['Launch Date']}")
        
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
    print(f"Imported {df.shape[0]} factsheet records")

def import_returns_data():
    """Import data from returns_testdata.xlsx"""
    print("Importing returns data...")
    file_path = os.path.join('attached_assets', 'returns_testdata.xlsx')
    
    df = pd.read_excel(file_path)
    
    for _, row in df.iterrows():
        isin = row['ISIN']
        
        # Skip if fund doesn't exist (should have been created during factsheet import)
        fund = Fund.query.filter_by(isin=isin).first()
        if not fund:
            print(f"Skipping returns for {isin} - fund does not exist")
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
    print(f"Imported {df.shape[0]} returns records")

def import_portfolio_data():
    """Import data from mutual_portfolio_testdata.xlsx"""
    print("Importing portfolio data...")
    file_path = os.path.join('attached_assets', 'mutual_portfolio_testdata.xlsx')
    
    # This file is large, so read it in chunks
    chunk_size = 100
    for chunk in pd.read_excel(file_path, chunksize=chunk_size):
        for _, row in chunk.iterrows():
            isin = row['ISIN']
            
            # Skip if fund doesn't exist (should have been created during factsheet import)
            fund = Fund.query.filter_by(isin=isin).first()
            if not fund:
                # Create fund if it doesn't exist
                fund = Fund(
                    isin=isin,
                    scheme_name=row['Fund Name'],
                    fund_type='Unknown',  # Cannot determine from this data
                    amc_name=row['Fund Name'].split(' ')[0]  # Assume first word is AMC name
                )
                db.session.add(fund)
                db.session.commit()
                print(f"Created fund {isin} - {row['Fund Name']}")
            
            # Create portfolio holding
            holding = PortfolioHolding(
                isin=isin,
                instrument_name=row['Name Of the Instrument'] if not pd.isna(row['Name Of the Instrument']) else 'Unknown',
                percentage_to_nav=row['% to NAV'] if not pd.isna(row['% to NAV']) else 0,
                instrument_type=row['Type'] if not pd.isna(row['Type']) else 'Unknown',
                coupon=row['Coupon (%)'] if not pd.isna(row['Coupon (%)']) else None,
                sector=row['Sector'] if not pd.isna(row['Sector']) else None,
                quantity=row['Quantity'] if not pd.isna(row['Quantity']) else None,
                value=row['Value'] if not pd.isna(row['Value']) else None,
                yield_value=row['Yield'] if not pd.isna(row['Yield']) else None
            )
            db.session.add(holding)
        
        db.session.commit()
    
    print("Imported portfolio holdings")

def import_nav_data():
    """Import data from navtimeseries_testdata.xlsx"""
    print("Importing NAV data...")
    file_path = os.path.join('attached_assets', 'navtimeseries_testdata.xlsx')
    
    # This file is large, so read it in chunks
    chunk_size = 500
    count = 0
    
    for chunk in pd.read_excel(file_path, chunksize=chunk_size):
        for _, row in chunk.iterrows():
            isin = row['ISIN']
            
            # Skip if fund doesn't exist (should have been created during factsheet import)
            fund = Fund.query.filter_by(isin=isin).first()
            if not fund:
                # Create fund if it doesn't exist
                fund = Fund(
                    isin=isin,
                    scheme_name=row['Scheme Name'],
                    fund_type='Unknown',  # Cannot determine from this data
                    amc_name=row['Scheme Name'].split(' ')[0]  # Assume first word is AMC name
                )
                db.session.add(fund)
                db.session.commit()
                print(f"Created fund {isin} - {row['Scheme Name']}")
            
            # Convert date if needed
            nav_date = None
            if not pd.isna(row['Date']):
                if isinstance(row['Date'], datetime):
                    nav_date = row['Date'].date()
                else:
                    try:
                        nav_date = datetime.strptime(str(row['Date']), '%Y-%m-%d').date()
                    except ValueError:
                        print(f"Could not parse date for {isin}: {row['Date']}")
                        continue
            
            if not nav_date:
                continue
            
            # Check if NAV entry already exists
            existing_nav = NavHistory.query.filter_by(isin=isin, date=nav_date).first()
            if existing_nav:
                existing_nav.nav = row['Net Asset Value']
            else:
                # Create NAV entry
                nav_entry = NavHistory(
                    isin=isin,
                    date=nav_date,
                    nav=row['Net Asset Value']
                )
                db.session.add(nav_entry)
            
            count += 1
            
            # Commit every 1000 records to avoid memory issues
            if count % 1000 == 0:
                db.session.commit()
                print(f"Imported {count} NAV records so far...")
    
    db.session.commit()
    print(f"Imported {count} NAV records total")

def main():
    with app.app_context():
        # Drop all tables and recreate
        print("Recreating database tables...")
        db.drop_all()
        db.create_all()
        
        # Import data in order: factsheets, returns, portfolio, NAV
        import_factsheet_data()
        import_returns_data()
        import_portfolio_data()
        import_nav_data()
        
        print("Data import complete")

if __name__ == "__main__":
    main()