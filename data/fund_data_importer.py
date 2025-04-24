import pandas as pd
import os
import logging
import sys
from datetime import datetime

# Add parent directory to path to enable imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from setup_db import create_app, db
from new_models_updated import Fund, FundFactSheet, FundReturns, PortfolioHolding, NavHistory

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class FundDataImporter:
    """
    Class for importing mutual fund data from Excel files into the database.
    Uses ISIN as the primary key for all fund data.
    
    Handles:
    - Fund basic data and factsheets (factsheet_testdata.xlsx)
    - Fund returns data (returns_testdata.xlsx)
    - Fund portfolio holdings (mutual_portfolio_testdata.xlsx)
    - Fund NAV history (navtimeseries_testdata.xlsx)
    """
    
    def __init__(self):
        """Initialize the FundDataImporter with a Flask app context"""
        self.app = create_app()
        self.app_context = self.app.app_context()
        self.app_context.push()
        
        # Define file paths
        self.excel_dir = 'attached_assets'
        self.factsheet_file = os.path.join(self.excel_dir, 'factsheet_testdata.xlsx')
        self.returns_file = os.path.join(self.excel_dir, 'returns_testdata.xlsx')
        self.portfolio_file = os.path.join(self.excel_dir, 'mutual_portfolio_testdata.xlsx')
        self.nav_file = os.path.join(self.excel_dir, 'navtimeseries_testdata.xlsx')
        
    def __del__(self):
        """Clean up resources when the object is destroyed"""
        try:
            self.app_context.pop()
        except:
            pass
    
    def _parse_date(self, date_value):
        """
        Parse date values from various formats
        
        Args:
            date_value: The date value to parse
            
        Returns:
            Date object or None if parsing fails
        """
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
    
    def import_factsheet_data(self, clear_existing=False):
        """
        Import fund and factsheet data from factsheet_testdata.xlsx
        
        Args:
            clear_existing (bool): Whether to clear existing data before import
            
        Returns:
            dict: Statistics about the import operation
        """
        logger.info(f"Importing factsheet data from {self.factsheet_file}")
        
        try:
            # Read Excel file
            df = pd.read_excel(self.factsheet_file)
            logger.info(f"Found {len(df)} records in factsheet data")
            
            if clear_existing and len(df) > 0:
                # Get list of ISINs to clear
                isins = df['ISIN'].unique().tolist()
                
                # Delete existing factsheets for these ISINs
                FundFactSheet.query.filter(FundFactSheet.isin.in_(isins)).delete(synchronize_session=False)
                
                # Delete existing funds for these ISINs
                Fund.query.filter(Fund.isin.in_(isins)).delete(synchronize_session=False)
                
                db.session.commit()
                logger.info(f"Cleared existing fund and factsheet data for {len(isins)} ISINs")
            
            # Track statistics
            stats = {
                'funds_created': 0,
                'funds_updated': 0,
                'factsheets_created': 0,
                'factsheets_updated': 0
            }
            
            # Process each row
            for _, row in df.iterrows():
                isin = row['ISIN']
                scheme_name = row['Scheme Name']
                fund_type = row['Type']
                fund_subtype = row['Subtype']
                
                # Extract AMC name from scheme name (assume first word is AMC)
                amc_name = scheme_name.split(' ')[0]
                
                # Create or update fund
                fund = Fund.query.filter_by(isin=isin).first()
                
                if not fund:
                    # Create new fund
                    fund = Fund(
                        isin=isin,
                        scheme_name=scheme_name,
                        fund_type=fund_type,
                        fund_subtype=fund_subtype,
                        amc_name=amc_name
                    )
                    db.session.add(fund)
                    stats['funds_created'] += 1
                else:
                    # Update existing fund
                    fund.scheme_name = scheme_name
                    fund.fund_type = fund_type
                    fund.fund_subtype = fund_subtype
                    fund.amc_name = amc_name
                    stats['funds_updated'] += 1
                
                # Parse launch date
                launch_date = self._parse_date(row['Launch Date'])
                
                # Create or update factsheet
                factsheet = FundFactSheet.query.filter_by(isin=isin).first()
                
                if not factsheet:
                    # Create new factsheet
                    factsheet = FundFactSheet(
                        isin=isin,
                        fund_manager=row['Fund Manager(s)'] if not pd.isna(row['Fund Manager(s)']) else None,
                        aum=row['AUM (₹ Cr)'] if not pd.isna(row['AUM (₹ Cr)']) else None,
                        expense_ratio=row['Expense Ratio'] if not pd.isna(row['Expense Ratio']) else None,
                        launch_date=launch_date,
                        exit_load=str(row['Exit Load']) if not pd.isna(row['Exit Load']) else None
                    )
                    db.session.add(factsheet)
                    stats['factsheets_created'] += 1
                else:
                    # Update existing factsheet
                    factsheet.fund_manager = row['Fund Manager(s)'] if not pd.isna(row['Fund Manager(s)']) else factsheet.fund_manager
                    factsheet.aum = row['AUM (₹ Cr)'] if not pd.isna(row['AUM (₹ Cr)']) else factsheet.aum
                    factsheet.expense_ratio = row['Expense Ratio'] if not pd.isna(row['Expense Ratio']) else factsheet.expense_ratio
                    factsheet.launch_date = launch_date if launch_date else factsheet.launch_date
                    factsheet.exit_load = str(row['Exit Load']) if not pd.isna(row['Exit Load']) else factsheet.exit_load
                    stats['factsheets_updated'] += 1
            
            # Commit all changes
            db.session.commit()
            logger.info(f"Factsheet import completed: {stats}")
            
            return stats
            
        except Exception as e:
            db.session.rollback()
            logger.error(f"Error importing factsheet data: {e}")
            raise
    
    def import_returns_data(self, clear_existing=False):
        """
        Import fund returns data from returns_testdata.xlsx
        
        Args:
            clear_existing (bool): Whether to clear existing data before import
            
        Returns:
            dict: Statistics about the import operation
        """
        logger.info(f"Importing returns data from {self.returns_file}")
        
        try:
            # Read Excel file
            df = pd.read_excel(self.returns_file)
            logger.info(f"Found {len(df)} records in returns data")
            
            if clear_existing and len(df) > 0:
                # Get list of ISINs to clear
                isins = df['ISIN'].unique().tolist()
                
                # Delete existing returns for these ISINs
                FundReturns.query.filter(FundReturns.isin.in_(isins)).delete(synchronize_session=False)
                
                db.session.commit()
                logger.info(f"Cleared existing returns data for {len(isins)} ISINs")
            
            # Track statistics
            stats = {
                'returns_created': 0,
                'returns_updated': 0,
                'funds_not_found': 0
            }
            
            # Process each row
            for _, row in df.iterrows():
                isin = row['ISIN']
                
                # Skip if fund doesn't exist
                fund = Fund.query.filter_by(isin=isin).first()
                if not fund:
                    logger.warning(f"Skipping returns for {isin}: Fund not found in database")
                    stats['funds_not_found'] += 1
                    continue
                
                # Create or update returns
                returns = FundReturns.query.filter_by(isin=isin).first()
                
                if not returns:
                    # Create new returns record
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
                    stats['returns_created'] += 1
                else:
                    # Update existing returns record
                    returns.return_1m = row['1M Return'] if not pd.isna(row['1M Return']) else returns.return_1m
                    returns.return_3m = row['3M Return'] if not pd.isna(row['3M Return']) else returns.return_3m
                    returns.return_6m = row['6M Return'] if not pd.isna(row['6M Return']) else returns.return_6m
                    returns.return_ytd = row['YTD Return'] if not pd.isna(row['YTD Return']) else returns.return_ytd
                    returns.return_1y = row['1Y Return'] if not pd.isna(row['1Y Return']) else returns.return_1y
                    returns.return_3y = row['3Y Return'] if not pd.isna(row['3Y Return']) else returns.return_3y
                    returns.return_5y = row['5Y Return'] if not pd.isna(row['5Y Return']) else returns.return_5y
                    stats['returns_updated'] += 1
            
            # Commit all changes
            db.session.commit()
            logger.info(f"Returns import completed: {stats}")
            
            return stats
            
        except Exception as e:
            db.session.rollback()
            logger.error(f"Error importing returns data: {e}")
            raise
    
    def import_portfolio_data(self, clear_existing=False):
        """
        Import fund portfolio holdings from mutual_portfolio_testdata.xlsx
        
        Args:
            clear_existing (bool): Whether to clear existing data before import
            
        Returns:
            dict: Statistics about the import operation
        """
        logger.info(f"Importing portfolio data from {self.portfolio_file}")
        
        try:
            # Read Excel file
            df = pd.read_excel(self.portfolio_file)
            logger.info(f"Found {len(df)} records in portfolio data")
            
            # Get unique fund names for mapping
            fund_names = df['Fund Name'].unique().tolist()
            
            # Track statistics
            stats = {
                'holdings_created': 0,
                'funds_matched': 0,
                'funds_not_found': 0
            }
            
            # Clear existing holdings if requested
            if clear_existing:
                # Get all funds with names matching the ones in the Excel file
                funds_to_clear = Fund.query.filter(
                    Fund.scheme_name.in_([f"%{name}%" for name in fund_names])
                ).all()
                
                fund_isins = [fund.isin for fund in funds_to_clear]
                
                if fund_isins:
                    # Delete holdings for these funds
                    deleted = PortfolioHolding.query.filter(
                        PortfolioHolding.isin.in_(fund_isins)
                    ).delete(synchronize_session=False)
                    
                    db.session.commit()
                    logger.info(f"Cleared {deleted} existing portfolio holdings for {len(fund_isins)} funds")
            
            # Create a mapping between fund names in Excel and actual fund ISINs in the database
            fund_name_to_isin = {}
            for fund_name in fund_names:
                # Find matching fund by scheme name
                fund = Fund.query.filter(Fund.scheme_name.like(f"%{fund_name}%")).first()
                if fund:
                    fund_name_to_isin[fund_name] = fund.isin
                    stats['funds_matched'] += 1
                else:
                    stats['funds_not_found'] += 1
                    logger.warning(f"No matching fund found for '{fund_name}'")
            
            # Process each row
            for _, row in df.iterrows():
                fund_name = row['Fund Name']
                
                # Skip if fund not found in our mapping
                if fund_name not in fund_name_to_isin:
                    continue
                
                # Get the fund ISIN from our mapping
                fund_isin = fund_name_to_isin[fund_name]
                
                # Create portfolio holding
                holding = PortfolioHolding(
                    isin=fund_isin,
                    instrument_isin=row['ISIN'] if not pd.isna(row['ISIN']) else None,  # This is the security ISIN, not the fund ISIN
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
                stats['holdings_created'] += 1
                
                # Commit every 100 records to avoid large transactions
                if stats['holdings_created'] % 100 == 0:
                    db.session.commit()
                    logger.info(f"Committed {stats['holdings_created']} portfolio holdings so far")
            
            # Final commit
            db.session.commit()
            logger.info(f"Portfolio import completed: {stats}")
            
            return stats
            
        except Exception as e:
            db.session.rollback()
            logger.error(f"Error importing portfolio data: {e}")
            raise
    
    def import_nav_data(self, clear_existing=False, batch_size=1000):
        """
        Import fund NAV history from navtimeseries_testdata.xlsx
        
        Args:
            clear_existing (bool): Whether to clear existing data before import
            batch_size (int): Number of records to process in each batch
            
        Returns:
            dict: Statistics about the import operation
        """
        logger.info(f"Importing NAV data from {self.nav_file}")
        
        try:
            # Track statistics
            stats = {
                'nav_created': 0,
                'nav_skipped': 0,
                'funds_not_found': 0,
                'invalid_dates': 0
            }
            
            # Read the file in batches to handle large files efficiently
            reader = pd.read_excel(self.nav_file, chunksize=batch_size)
            
            # Get a mapping of ISINs to funds for efficient lookups
            all_funds = Fund.query.all()
            isin_to_fund = {fund.isin: fund for fund in all_funds}
            
            # Also create a mapping from fund name fragments to ISIN for fuzzy matching
            name_to_isin = {}
            for fund in all_funds:
                name_fragments = fund.scheme_name.lower().split()
                for fragment in name_fragments:
                    if len(fragment) > 3:  # Only use meaningful words
                        if fragment not in name_to_isin:
                            name_to_isin[fragment] = []
                        name_to_isin[fragment].append(fund.isin)
            
            # Process each batch
            batch_num = 0
            for chunk in reader:
                batch_num += 1
                logger.info(f"Processing batch {batch_num} with {len(chunk)} records")
                
                # Clear existing NAV data for ISINs in this chunk if requested
                if clear_existing and batch_num == 1:
                    # Get unique ISINs in the first batch
                    batch_isins = chunk['ISIN'].unique().tolist()
                    
                    # Get existing ISINs that match
                    existing_isins = [isin for isin in batch_isins if isin in isin_to_fund]
                    
                    if existing_isins:
                        # Delete NAV history for these ISINs
                        deleted = NavHistory.query.filter(
                            NavHistory.isin.in_(existing_isins)
                        ).delete(synchronize_session=False)
                        
                        db.session.commit()
                        logger.info(f"Cleared {deleted} existing NAV records for {len(existing_isins)} funds")
                
                # Process each row in the batch
                for _, row in chunk.iterrows():
                    excel_isin = row['ISIN']
                    scheme_name = row['Scheme Name'].lower() if not pd.isna(row['Scheme Name']) else ""
                    
                    # Find the fund ISIN - try direct match first
                    fund_isin = None
                    if excel_isin in isin_to_fund:
                        fund_isin = excel_isin
                    else:
                        # Try to match by name fragments
                        matching_isins = []
                        for word in scheme_name.split():
                            if len(word) > 3 and word in name_to_isin:
                                matching_isins.extend(name_to_isin[word])
                        
                        if matching_isins:
                            # Use the most frequent match
                            from collections import Counter
                            fund_isin = Counter(matching_isins).most_common(1)[0][0]
                    
                    if not fund_isin:
                        stats['funds_not_found'] += 1
                        if stats['funds_not_found'] <= 10:  # Limit log spam
                            logger.warning(f"No matching fund found for NAV record with ISIN {excel_isin} and name '{scheme_name}'")
                        continue
                    
                    # Parse date
                    nav_date = self._parse_date(row['Date'])
                    if not nav_date:
                        stats['invalid_dates'] += 1
                        continue
                    
                    # Skip if NAV value is missing
                    if pd.isna(row['Net Asset Value']):
                        stats['nav_skipped'] += 1
                        continue
                    
                    # Create NAV history entry
                    nav_history = NavHistory(
                        isin=fund_isin,
                        date=nav_date,
                        nav=float(row['Net Asset Value'])
                    )
                    db.session.add(nav_history)
                    stats['nav_created'] += 1
                    
                    # Commit every 1000 records to avoid large transactions
                    if stats['nav_created'] % 1000 == 0:
                        db.session.commit()
                        logger.info(f"Committed {stats['nav_created']} NAV records so far")
                
                # Commit at the end of each batch
                db.session.commit()
                logger.info(f"Processed batch {batch_num}, total NAV records so far: {stats['nav_created']}")
            
            # Final commit
            db.session.commit()
            logger.info(f"NAV import completed: {stats}")
            
            return stats
            
        except Exception as e:
            db.session.rollback()
            logger.error(f"Error importing NAV data: {e}")
            raise
    
    def import_all_data(self, clear_existing=False):
        """
        Import all fund data from Excel files
        
        Args:
            clear_existing (bool): Whether to clear existing data before import
            
        Returns:
            dict: Statistics about the import operations
        """
        logger.info("Starting import of all mutual fund data")
        
        try:
            # Import in the correct order: funds and factsheets first, then related data
            factsheet_stats = self.import_factsheet_data(clear_existing)
            returns_stats = self.import_returns_data(clear_existing)
            portfolio_stats = self.import_portfolio_data(clear_existing)
            nav_stats = self.import_nav_data(clear_existing)
            
            # Collect overall statistics
            stats = {
                'factsheet': factsheet_stats,
                'returns': returns_stats,
                'portfolio': portfolio_stats,
                'nav': nav_stats
            }
            
            # Print summary
            logger.info("===== Fund Data Import Summary =====")
            logger.info(f"Funds created: {factsheet_stats['funds_created']}")
            logger.info(f"Factsheets created: {factsheet_stats['factsheets_created']}")
            logger.info(f"Returns created: {returns_stats['returns_created']}")
            logger.info(f"Portfolio holdings created: {portfolio_stats['holdings_created']}")
            logger.info(f"NAV history records created: {nav_stats['nav_created']}")
            logger.info("==================================")
            
            return stats
            
        except Exception as e:
            logger.error(f"Error during complete import process: {e}")
            raise


# Command-line interface
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Import mutual fund data from Excel files")
    parser.add_argument('--factsheet', action='store_true', help='Import factsheet data only')
    parser.add_argument('--returns', action='store_true', help='Import returns data only')
    parser.add_argument('--portfolio', action='store_true', help='Import portfolio data only')
    parser.add_argument('--nav', action='store_true', help='Import NAV history data only')
    parser.add_argument('--all', action='store_true', help='Import all data')
    parser.add_argument('--clear', action='store_true', help='Clear existing data before import')
    
    args = parser.parse_args()
    
    # Default to importing all if no specific import is requested
    if not any([args.factsheet, args.returns, args.portfolio, args.nav, args.all]):
        args.all = True
    
    importer = FundDataImporter()
    
    try:
        if args.all:
            importer.import_all_data(args.clear)
        else:
            if args.factsheet:
                importer.import_factsheet_data(args.clear)
            if args.returns:
                importer.import_returns_data(args.clear)
            if args.portfolio:
                importer.import_portfolio_data(args.clear)
            if args.nav:
                importer.import_nav_data(args.clear)
    except Exception as e:
        logger.error(f"Import process failed: {e}")