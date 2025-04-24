import os
import pandas as pd
import logging
from datetime import datetime
from sqlalchemy.exc import SQLAlchemyError
from setup_db import create_app, db
from new_models_updated import Fund, FundFactSheet, FundReturns, PortfolioHolding, NavHistory

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ExcelDataImporter:
    """
    Class to import data from Excel files into the database
    """
    def __init__(self):
        """Initialize the importer with a Flask app context"""
        self.app = create_app()
        self.app_context = self.app.app_context()
        self.app_context.push()
        
        # Create tables if they don't exist
        db.create_all()
        
        # Excel file paths
        self.factsheet_path = 'attached_assets/factsheet_testdata.xlsx'
        self.portfolio_path = 'attached_assets/mutual_portfolio_testdata.xlsx'
        self.nav_path = 'attached_assets/navtimeseries_testdata.xlsx'
        self.returns_path = 'attached_assets/returns_testdata.xlsx'
    
    def close(self):
        """Clean up resources"""
        self.app_context.pop()
    
    def import_factsheet_data(self):
        """Import fund and factsheet data from factsheet_testdata.xlsx"""
        try:
            logger.info(f"Importing data from {self.factsheet_path}")
            df = pd.read_excel(self.factsheet_path)
            
            count = 0
            for _, row in df.iterrows():
                # Extract data
                isin = row['ISIN']
                scheme_name = row['Scheme Name']
                fund_type = row['Type']
                fund_subtype = row['Subtype']
                amc_name = scheme_name.split()[0]  # Assume first word is AMC name
                
                # Create or update Fund
                fund = Fund.query.filter_by(isin=isin).first()
                if not fund:
                    fund = Fund(
                        isin=isin,
                        scheme_name=scheme_name,
                        fund_type=fund_type,
                        fund_subtype=fund_subtype,
                        amc_name=amc_name
                    )
                    db.session.add(fund)
                else:
                    fund.scheme_name = scheme_name
                    fund.fund_type = fund_type
                    fund.fund_subtype = fund_subtype
                    fund.amc_name = amc_name
                
                # Create or update FundFactSheet
                factsheet = FundFactSheet.query.filter_by(isin=isin).first()
                
                # Convert launch date to date object if it's not None
                launch_date = row['Launch Date']
                if isinstance(launch_date, str):
                    try:
                        launch_date = datetime.strptime(launch_date, '%Y-%m-%d').date()
                    except ValueError:
                        launch_date = None
                elif hasattr(launch_date, 'date'):
                    launch_date = launch_date.date()
                
                if not factsheet:
                    factsheet = FundFactSheet(
                        isin=isin,
                        fund_manager=row['Fund Manager(s)'],
                        aum=row['AUM (₹ Cr)'],
                        expense_ratio=row['Expense Ratio'],
                        launch_date=launch_date,
                        exit_load=str(row['Exit Load'])
                    )
                    db.session.add(factsheet)
                else:
                    factsheet.fund_manager = row['Fund Manager(s)']
                    factsheet.aum = row['AUM (₹ Cr)']
                    factsheet.expense_ratio = row['Expense Ratio']
                    factsheet.launch_date = launch_date
                    factsheet.exit_load = str(row['Exit Load'])
                
                count += 1
                
                # Commit every 100 records to avoid large transactions
                if count % 100 == 0:
                    db.session.commit()
                    logger.info(f"Committed {count} records so far")
            
            # Final commit for remaining records
            db.session.commit()
            logger.info(f"Successfully imported {count} factsheet records")
            
            return count
        except Exception as e:
            db.session.rollback()
            logger.error(f"Error importing factsheet data: {e}")
            raise
    
    def import_returns_data(self):
        """Import returns data from returns_testdata.xlsx"""
        try:
            logger.info(f"Importing data from {self.returns_path}")
            df = pd.read_excel(self.returns_path)
            
            count = 0
            for _, row in df.iterrows():
                # Extract data
                isin = row['ISIN']
                
                # Check if fund exists
                fund = Fund.query.filter_by(isin=isin).first()
                if not fund:
                    logger.warning(f"Fund with ISIN {isin} not found, skipping returns import")
                    continue
                
                # Create or update FundReturns
                returns = FundReturns.query.filter_by(isin=isin).first()
                if not returns:
                    returns = FundReturns(
                        isin=isin,
                        return_1m=row['1M Return'],
                        return_3m=row['3M Return'],
                        return_6m=row['6M Return'],
                        return_ytd=row['YTD Return'],
                        return_1y=row['1Y Return'],
                        return_3y=row['3Y Return'],
                        return_5y=row['5Y Return']
                    )
                    db.session.add(returns)
                else:
                    returns.return_1m = row['1M Return']
                    returns.return_3m = row['3M Return']
                    returns.return_6m = row['6M Return']
                    returns.return_ytd = row['YTD Return']
                    returns.return_1y = row['1Y Return']
                    returns.return_3y = row['3Y Return']
                    returns.return_5y = row['5Y Return']
                
                count += 1
                
                # Commit every 100 records to avoid large transactions
                if count % 100 == 0:
                    db.session.commit()
                    logger.info(f"Committed {count} records so far")
            
            # Final commit for remaining records
            db.session.commit()
            logger.info(f"Successfully imported {count} returns records")
            
            return count
        except Exception as e:
            db.session.rollback()
            logger.error(f"Error importing returns data: {e}")
            raise
    
    def import_portfolio_data(self):
        """Import portfolio holdings data from mutual_portfolio_testdata.xlsx"""
        try:
            logger.info(f"Importing data from {self.portfolio_path}")
            df = pd.read_excel(self.portfolio_path)
            
            # Clear existing portfolio holdings to avoid duplicates
            count = 0
            for _, row in df.iterrows():
                # Extract data
                isin = row['ISIN']
                
                # Check if fund exists (based on Fund Name in portfolio data)
                fund_name = row['Fund Name']
                fund = Fund.query.filter(Fund.scheme_name.like(f"%{fund_name}%")).first()
                
                if not fund:
                    logger.warning(f"Fund with name '{fund_name}' not found, skipping portfolio import")
                    continue
                
                # Create PortfolioHolding
                holding = PortfolioHolding(
                    isin=fund.isin,  # Use the fund ISIN, not the instrument ISIN
                    instrument_isin=row['ISIN'],  # This is the instrument ISIN
                    coupon=row['Coupon (%)'] if not pd.isna(row['Coupon (%)']) else None,
                    instrument_name=row['Name Of the Instrument'],
                    sector=row['Sector'],
                    quantity=row['Quantity'] if not pd.isna(row['Quantity']) else None,
                    value=row['Value'] if not pd.isna(row['Value']) else None,
                    percentage_to_nav=row['% to NAV'],
                    yield_value=row['Yield'] if not pd.isna(row['Yield']) else None,
                    instrument_type=row['Type']
                )
                db.session.add(holding)
                
                count += 1
                
                # Commit every 100 records to avoid large transactions
                if count % 100 == 0:
                    db.session.commit()
                    logger.info(f"Committed {count} records so far")
            
            # Final commit for remaining records
            db.session.commit()
            logger.info(f"Successfully imported {count} portfolio holding records")
            
            return count
        except Exception as e:
            db.session.rollback()
            logger.error(f"Error importing portfolio data: {e}")
            raise
    
    def import_nav_data(self):
        """Import NAV history data from navtimeseries_testdata.xlsx"""
        try:
            logger.info(f"Importing data from {self.nav_path}")
            df = pd.read_excel(self.nav_path)
            
            count = 0
            batch_size = 1000  # Process in batches to handle large files efficiently
            for i, chunk in enumerate(pd.read_excel(self.nav_path, chunksize=batch_size)):
                for _, row in chunk.iterrows():
                    # Extract data
                    isin = row['ISIN']
                    
                    # Check if fund exists
                    fund = Fund.query.filter_by(isin=isin).first()
                    if not fund:
                        # Try to find by scheme name as a fallback
                        scheme_name = row['Scheme Name']
                        fund = Fund.query.filter(Fund.scheme_name.like(f"%{scheme_name}%")).first()
                        
                        if not fund:
                            logger.warning(f"Fund with ISIN {isin} or name '{scheme_name}' not found, skipping NAV import")
                            continue
                    
                    # Convert date to date object
                    nav_date = row['Date']
                    if isinstance(nav_date, str):
                        try:
                            nav_date = datetime.strptime(nav_date, '%Y-%m-%d').date()
                        except ValueError:
                            logger.warning(f"Invalid date format: {nav_date}, skipping")
                            continue
                    elif hasattr(nav_date, 'date'):
                        nav_date = nav_date.date()
                    
                    # Check if NAV entry already exists
                    existing_nav = NavHistory.query.filter_by(
                        isin=fund.isin, 
                        date=nav_date
                    ).first()
                    
                    if not existing_nav:
                        # Create NavHistory
                        nav_history = NavHistory(
                            isin=fund.isin,
                            date=nav_date,
                            nav=row['Net Asset Value']
                        )
                        db.session.add(nav_history)
                        
                        count += 1
                    
                    # Commit every 1000 records to avoid large transactions
                    if count % 1000 == 0:
                        db.session.commit()
                        logger.info(f"Committed {count} records so far")
                
                db.session.commit()
                logger.info(f"Processed batch {i+1}, total records so far: {count}")
            
            # Final commit for remaining records
            db.session.commit()
            logger.info(f"Successfully imported {count} NAV history records")
            
            return count
        except Exception as e:
            db.session.rollback()
            logger.error(f"Error importing NAV data: {e}")
            raise
    
    def import_all(self):
        """Import all data from Excel files"""
        try:
            # Import in the correct order: first funds, then related data
            factsheet_count = self.import_factsheet_data()
            returns_count = self.import_returns_data()
            portfolio_count = self.import_portfolio_data()
            nav_count = self.import_nav_data()
            
            return {
                'factsheet': factsheet_count,
                'returns': returns_count,
                'portfolio': portfolio_count,
                'nav': nav_count
            }
        except Exception as e:
            logger.error(f"Error during import process: {e}")
            raise
        finally:
            self.close()


if __name__ == "__main__":
    try:
        logger.info("Starting Excel data import process")
        importer = ExcelDataImporter()
        results = importer.import_all()
        
        logger.info(f"Import summary:")
        logger.info(f"- Factsheet records: {results['factsheet']}")
        logger.info(f"- Returns records: {results['returns']}")
        logger.info(f"- Portfolio holding records: {results['portfolio']}")
        logger.info(f"- NAV history records: {results['nav']}")
        
        logger.info("Import process completed successfully")
    except Exception as e:
        logger.error(f"Import process failed: {e}")