import pandas as pd
import os
import logging
from datetime import datetime
from setup_db import create_app, db
from new_models_updated import Fund, PortfolioHolding, NavHistory

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = create_app()

def import_portfolio_data():
    """Import portfolio holdings data from mutual_portfolio_testdata.xlsx"""
    logger.info("Importing portfolio holdings data...")
    file_path = os.path.join('attached_assets', 'mutual_portfolio_testdata.xlsx')
    
    try:
        df = pd.read_excel(file_path)
        logger.info(f"Read {len(df)} portfolio holding records")
        
        with app.app_context():
            holdings_created = 0
            
            for _, row in df.iterrows():
                isin = row['ISIN']
                
                # Skip if fund doesn't exist
                fund = Fund.query.filter_by(isin=isin).first()
                if not fund:
                    # Check if this is our test fund
                    if isin == 'TEST00000001':
                        fund = Fund.query.filter_by(isin='TEST00000001').first()
                        if fund:
                            isin = 'TEST00000001'
                        else:
                            logger.warning(f"Skipping portfolio for {isin} - fund does not exist")
                            continue
                    else:
                        logger.warning(f"Skipping portfolio for {isin} - fund does not exist")
                        continue
                
                # Create portfolio holding
                holding = PortfolioHolding(
                    isin=isin,
                    instrument_isin=None,  # Not available in test data
                    coupon=row['Coupon (%)'] if not pd.isna(row['Coupon (%)']) else None,
                    instrument_name=row['Name Of the Instrument'],
                    sector=row['Sector'] if not pd.isna(row['Sector']) else None,
                    quantity=row['Quantity'] if not pd.isna(row['Quantity']) else None,
                    value=row['Value'] if not pd.isna(row['Value']) else None,
                    percentage_to_nav=row['% to NAV'] if not pd.isna(row['% to NAV']) else 0.0,
                    yield_value=row['Yield'] if not pd.isna(row['Yield']) else None,
                    instrument_type=row['Type'] if not pd.isna(row['Type']) else 'Other'
                )
                db.session.add(holding)
                holdings_created += 1
                
                # Commit every 50 records to avoid large transactions
                if holdings_created % 50 == 0:
                    db.session.commit()
                    logger.info(f"Committed {holdings_created} holdings so far")
            
            # Final commit
            db.session.commit()
            logger.info(f"Imported {holdings_created} portfolio holding records")
            
            # Basic verification
            holdings_count = PortfolioHolding.query.count()
            logger.info(f"Total portfolio holdings in database: {holdings_count}")
            
        return True
    except Exception as e:
        logger.error(f"Error importing portfolio data: {e}")
        return False

def import_nav_data():
    """Import NAV history data from navtimeseries_testdata.xlsx"""
    logger.info("Importing NAV history data...")
    file_path = os.path.join('attached_assets', 'navtimeseries_testdata.xlsx')
    
    try:
        df = pd.read_excel(file_path)
        logger.info(f"Read {len(df)} NAV history records")
        
        with app.app_context():
            navs_created = 0
            
            for _, row in df.iterrows():
                isin = row['ISIN']
                
                # Skip if fund doesn't exist
                fund = Fund.query.filter_by(isin=isin).first()
                if not fund:
                    # Check if this is our test fund
                    if isin == 'TEST00000001':
                        fund = Fund.query.filter_by(isin='TEST00000001').first()
                        if fund:
                            isin = 'TEST00000001'
                        else:
                            logger.warning(f"Skipping NAV for {isin} - fund does not exist")
                            continue
                    else:
                        logger.warning(f"Skipping NAV for {isin} - fund does not exist")
                        continue
                
                # Parse date
                nav_date = None
                if not pd.isna(row['Date']):
                    if isinstance(row['Date'], datetime):
                        nav_date = row['Date'].date()
                    else:
                        try:
                            nav_date = datetime.strptime(str(row['Date']), '%Y-%m-%d').date()
                        except ValueError:
                            try:
                                nav_date = datetime.strptime(str(row['Date']), '%d-%m-%Y').date()
                            except ValueError:
                                logger.warning(f"Could not parse date for NAV record: {row['Date']}")
                                continue
                else:
                    logger.warning("Skipping NAV record with missing date")
                    continue
                
                # Skip if NAV is missing
                if pd.isna(row['Net Asset Value']):
                    logger.warning(f"Skipping NAV record with missing value for {isin} on {nav_date}")
                    continue
                
                # Create NAV history entry
                nav_history = NavHistory(
                    isin=isin,
                    date=nav_date,
                    nav=float(row['Net Asset Value'])
                )
                db.session.add(nav_history)
                navs_created += 1
                
                # Commit every 100 records to avoid large transactions
                if navs_created % 100 == 0:
                    db.session.commit()
                    logger.info(f"Committed {navs_created} NAV records so far")
            
            # Final commit
            db.session.commit()
            logger.info(f"Imported {navs_created} NAV history records")
            
            # Basic verification
            nav_count = NavHistory.query.count()
            logger.info(f"Total NAV history records in database: {nav_count}")
            
        return True
    except Exception as e:
        logger.error(f"Error importing NAV data: {e}")
        return False

def main():
    """Run both imports"""
    portfolio_success = import_portfolio_data()
    nav_success = import_nav_data()
    
    if portfolio_success and nav_success:
        logger.info("All data imported successfully")
    else:
        logger.warning("Some imports failed - check logs for details")

if __name__ == "__main__":
    main()