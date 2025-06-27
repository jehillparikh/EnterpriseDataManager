import pandas as pd
import os
import logging
import sys
from datetime import datetime

# Add parent directory to path to enable imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from setup_db import create_app, db
from models import Fund, FundFactSheet, FundReturns, FundHolding, NavHistory

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
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
        """Initialize the FundDataImporter - uses existing Flask app context"""
        # Define file paths - use absolute paths to ensure we find the files
        root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.excel_dir = os.path.join(root_dir, 'attached_assets')
        self.factsheet_file = os.path.join(self.excel_dir,
                                           'factsheet_testdata.xlsx')
        self.returns_file = os.path.join(self.excel_dir,
                                         'returns_testdata.xlsx')
        self.portfolio_file = os.path.join(self.excel_dir,
                                           'mutual_portfolio_testdata.xlsx')
        self.nav_file = os.path.join(self.excel_dir,
                                     'navtimeseries_testdata.xlsx')

        # Log the file paths
        logger.info(f"Excel directory: {self.excel_dir}")
        logger.info(f"Factsheet file: {self.factsheet_file}")
        logger.info(f"Returns file: {self.returns_file}")
        logger.info(f"Portfolio file: {self.portfolio_file}")
        logger.info(f"NAV file: {self.nav_file}")

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
            df.dropna(subset=['ISIN'])
            logger.info(f"Found {len(df)} records in factsheet data")

            if clear_existing and len(df) > 0:
                # Get list of ISINs to clear
                isins = df['ISIN'].unique().tolist()

                # Delete existing factsheets for these ISINs
                FundFactSheet.query.filter(
                    FundFactSheet.isin.in_(isins)).delete(
                        synchronize_session=False)

                # Delete existing funds for these ISINs
                Fund.query.filter(
                    Fund.isin.in_(isins)).delete(synchronize_session=False)

                db.session.commit()
                logger.info(
                    f"Cleared existing fund and factsheet data for {len(isins)} ISINs"
                )

            # Track statistics
            stats = {
                'funds_created': 0,
                'funds_updated': 0,
                'factsheets_created': 0,
                'factsheets_updated': 0
            }

            # Process each row
            for index, row in df.iterrows():
                try:
                    isin = str(row['ISIN']).strip()
                    scheme_name = str(row['Scheme Name']).strip()
                    fund_type = str(row['Type']).strip()
                    fund_subtype = str(row['Subtype']).strip() if not pd.isna(
                        row['Subtype']) else None

                    # Skip if ISIN is not valid
                    if pd.isna(row['ISIN']) or not isin:
                        logger.warning(
                            f"Skipping row {index+1} with invalid ISIN: {row['ISIN']}"
                        )
                        continue

                    # Get AMC name from the AMC column
                    amc_name = str(row['AMC']).strip() if not pd.isna(
                        row['AMC']) else 'Unknown'

                    # Create or update fund
                    fund = Fund.query.filter_by(isin=isin).first()

                except Exception as e:
                    logger.error(f"Error processing row {index+1}: {e}")
                    continue

                try:
                    if not fund:
                        # Create new fund
                        fund = Fund(isin=isin,
                                    scheme_name=scheme_name,
                                    fund_type=fund_type,
                                    fund_subtype=fund_subtype,
                                    amc_name=amc_name)
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
                    factsheet = FundFactSheet.query.filter_by(
                        isin=isin).first()

                    if not factsheet:
                        # Create new factsheet
                        factsheet = FundFactSheet(
                            isin=isin,
                            fund_manager=str(row['Fund Manager(s)']).strip()
                            if not pd.isna(row['Fund Manager(s)']) else None,
                            aum=float(row['AUM (₹ Cr)'])
                            if not pd.isna(row['AUM (₹ Cr)']) else None,
                            expense_ratio=float(row['Expense Ratio'])
                            if not pd.isna(row['Expense Ratio']) else None,
                            launch_date=launch_date,
                            exit_load=str(row['Exit Load']).strip()
                            if not pd.isna(row['Exit Load']) else None)
                        db.session.add(factsheet)
                        stats['factsheets_created'] += 1
                    else:
                        # Update existing factsheet
                        if not pd.isna(row['Fund Manager(s)']):
                            factsheet.fund_manager = str(
                                row['Fund Manager(s)']).strip()
                        if not pd.isna(row['AUM (₹ Cr)']):
                            factsheet.aum = float(row['AUM (₹ Cr)'])
                        if not pd.isna(row['Expense Ratio']):
                            factsheet.expense_ratio = float(
                                row['Expense Ratio'])
                        if launch_date:
                            factsheet.launch_date = launch_date
                        if not pd.isna(row['Exit Load']):
                            factsheet.exit_load = str(row['Exit Load']).strip()
                        stats['factsheets_updated'] += 1

                    # Commit every 100 records to avoid large transactions
                    if (index + 1) % 100 == 0:
                        logger.info(f"Processed {index + 1} records")
                        db.session.commit()
                        logger.info(f"Processed {index + 1} records")

                except Exception as e:
                    logger.error(
                        f"Error creating fund/factsheet for ISIN {isin}: {e}")
                    db.session.rollback()
                    continue

            logger.info(f"Factsheet committing to database now!!!!")
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
                FundReturns.query.filter(FundReturns.isin.in_(isins)).delete(
                    synchronize_session=False)

                db.session.commit()
                logger.info(
                    f"Cleared existing returns data for {len(isins)} ISINs")

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
                    logger.warning(
                        f"Skipping returns for {isin}: Fund not found in database"
                    )
                    stats['funds_not_found'] += 1
                    continue

                # Create or update returns
                returns = FundReturns.query.filter_by(isin=isin).first()

                if not returns:
                    # Create new returns record
                    returns = FundReturns(
                        isin=isin,
                        return_1m=row['1M Return']
                        if not pd.isna(row['1M Return']) else None,
                        return_3m=row['3M Return']
                        if not pd.isna(row['3M Return']) else None,
                        return_6m=row['6M Return']
                        if not pd.isna(row['6M Return']) else None,
                        return_ytd=row['YTD Return']
                        if not pd.isna(row['YTD Return']) else None,
                        return_1y=row['1Y Return']
                        if not pd.isna(row['1Y Return']) else None,
                        return_3y=row['3Y Return']
                        if not pd.isna(row['3Y Return']) else None,
                        return_5y=row['5Y Return']
                        if not pd.isna(row['5Y Return']) else None)
                    db.session.add(returns)
                    stats['returns_created'] += 1
                else:
                    # Update existing returns record
                    returns.return_1m = row['1M Return'] if not pd.isna(
                        row['1M Return']) else returns.return_1m
                    returns.return_3m = row['3M Return'] if not pd.isna(
                        row['3M Return']) else returns.return_3m
                    returns.return_6m = row['6M Return'] if not pd.isna(
                        row['6M Return']) else returns.return_6m
                    returns.return_ytd = row['YTD Return'] if not pd.isna(
                        row['YTD Return']) else returns.return_ytd
                    returns.return_1y = row['1Y Return'] if not pd.isna(
                        row['1Y Return']) else returns.return_1y
                    returns.return_3y = row['3Y Return'] if not pd.isna(
                        row['3Y Return']) else returns.return_3y
                    returns.return_5y = row['5Y Return'] if not pd.isna(
                        row['5Y Return']) else returns.return_5y
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
        Import fund portfolio holdings using Scheme ISIN for fund linking
        Expected columns: Name of Instrument, ISIN, Coupon, Industry, Quantity, 
        Market Value, % to Net Assets, Yield, Type, AMC, Scheme Name, Scheme ISIN
        
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

            # Map actual column names to expected names
            actual_columns = df.columns.tolist()
            logger.info(f"Portfolio file columns: {actual_columns}")
            
            # Create column mapping for the actual file structure
            column_mapping = {
                'Name Of the Instrument': 'Name of Instrument',
                'Fund Name': 'Scheme Name',
                '% to NAV': '% to Net Assets',
                'Coupon (%)': 'Coupon',
                'Sector': 'Industry',
                'Value': 'Market Value',
                'Type': 'Type',
                'Yield': 'Yield'
            }
            
            # Rename columns to match expected names
            df = df.rename(columns=column_mapping)
            logger.info(f"Columns after mapping: {df.columns.tolist()}")
            
            # For this file format, we need to add a default Scheme ISIN since it's not provided
            # We'll use a placeholder that gets mapped to actual fund ISINs later
            if 'Scheme ISIN' not in df.columns:
                df['Scheme ISIN'] = 'INF179K01830'  # Default ISIN for portfolio holdings
                logger.info("Added default Scheme ISIN for portfolio holdings")
            
            # Check required columns are now present
            required_columns = ['Name of Instrument', 'ISIN']
            missing_columns = [
                col for col in required_columns if col not in df.columns
            ]
            if missing_columns:
                logger.error(f"Required columns: {required_columns}")
                logger.error(f"Available columns: {df.columns.tolist()}")
                raise ValueError(
                    f"Missing required columns: {missing_columns}")

            # Get unique scheme ISINs for mapping and filter out invalid values
            raw_scheme_isins = df['Scheme ISIN'].unique().tolist()

            # Filter out invalid ISINs (NaN, empty, non-string, or not 12 characters)
            valid_scheme_isins = []
            for isin in raw_scheme_isins:
                # Skip NaN values
                if pd.isna(isin):
                    continue
                # Convert to string and strip whitespace
                isin_str = str(isin).strip()
                # Skip empty, dash, or invalid values
                if not isin_str or isin_str in ('-', 'nan', 'None'):
                    continue
                # Only include 12-character ISINs
                if len(isin_str) == 12:
                    valid_scheme_isins.append(isin_str)
            
            scheme_isins = valid_scheme_isins
            logger.info(f"Filtered {len(raw_scheme_isins)} ISINs down to {len(scheme_isins)} valid ISINs")

            # Track statistics
            stats = {
                'holdings_created': 0,
                'funds_matched': 0,
                'funds_not_found': 0,
                'rows_processed': 0
            }

            # Clear existing holdings if requested
            if clear_existing and scheme_isins:
                # Delete existing holdings for these scheme ISINs
                deleted = FundHolding.query.filter(
                    FundHolding.isin.in_(scheme_isins)).delete(
                        synchronize_session=False)

                db.session.commit()
                logger.info(
                    f"Cleared {deleted} existing portfolio holdings for {len(scheme_isins)} funds"
                )

            # Verify which scheme ISINs exist in our Fund table
            existing_funds = Fund.query.filter(
                Fund.isin.in_(scheme_isins)).all()
            existing_isins = {fund.isin for fund in existing_funds}

            logger.info(
                f"Found {len(existing_isins)} matching funds out of {len(scheme_isins)} scheme ISINs"
            )

            # Helper function to safely convert to float
            def safe_float(val):
                if pd.isna(val):
                    return None
                try:
                    # Remove any non-numeric characters except decimal points and minus signs
                    if isinstance(val, str):
                        # Replace @ symbols and other invalid chars with empty string
                        cleaned = ''.join(c for c in val
                                          if c.isdigit() or c in '.-')
                        if not cleaned or cleaned in ['.', '-', '.-']:
                            return None
                        return float(cleaned)
                    return float(val)
                except (ValueError, TypeError):
                    return None

            # Process each row
            for _, row in df.iterrows():
                stats['rows_processed'] += 1
                scheme_isin = row['Scheme ISIN']

                # Skip if Scheme ISIN is NaN or invalid
                if pd.isna(scheme_isin) or not scheme_isin:
                    stats['funds_not_found'] += 1
                    logger.warning(
                        f"Skipping row with invalid/empty Scheme ISIN")
                    continue

                scheme_isin = str(scheme_isin).strip()

                # Skip if ISIN is not 12 characters (invalid format)
                if len(scheme_isin) != 12:
                    stats['funds_not_found'] += 1
                    logger.warning(
                        f"Skipping row with invalid Scheme ISIN format: {scheme_isin}"
                    )
                    continue

                # Skip if fund not found in our database
                if scheme_isin not in existing_isins:
                    if scheme_isin not in [
                            isin for isin in scheme_isins
                            if isin in existing_isins
                    ]:
                        stats['funds_not_found'] += 1
                        logger.warning(
                            f"Fund with Scheme ISIN {scheme_isin} not found in database"
                        )
                    continue

                stats['funds_matched'] += 1

                # Create portfolio holding with new column mapping
                try:
                    holding = FundHolding(
                        isin=scheme_isin,  # Use Scheme ISIN to link to fund
                        instrument_isin=str(row['ISIN']).strip()
                        if not pd.isna(row['ISIN']) else None,
                        coupon=safe_float(row.get('Coupon', None)),
                        instrument_name=str(row['Name of Instrument']).strip(),
                        sector=str(row.get('Industry', '')).strip()
                        if not pd.isna(row.get('Industry', None)) else None,
                        quantity=safe_float(row.get('Quantity', None)),
                        value=safe_float(row.get('Market Value', None)),
                        percentage_to_nav=safe_float(
                            row.get('% to Net Assets', None)) or 0.0,
                        yield_value=safe_float(row.get('Yield', None)),
                        instrument_type=str(row.get('Type', 'Other')).strip()
                        if not pd.isna(row.get('Type', None)) else 'Other',
                        amc_name=str(row.get('AMC', '')).strip()
                        if not pd.isna(row.get('AMC', None)) else None,
                        scheme_name=str(row.get('Scheme Name', '')).strip()
                        if not pd.isna(row.get('Scheme Name', None)) else None)

                    db.session.add(holding)
                    stats['holdings_created'] += 1

                except Exception as e:
                    logger.error(
                        f"Error creating portfolio holding for Scheme ISIN {scheme_isin}: {e}"
                    )
                    continue

                # Commit every 100 records to avoid large transactions
                if stats['holdings_created'] % 100 == 0:
                    db.session.commit()
                    logger.info(
                        f"Committed {stats['holdings_created']} portfolio holdings so far"
                    )

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
                logger.info(
                    f"Processing batch {batch_num} with {len(chunk)} records")

                # Clear existing NAV data for ISINs in this chunk if requested
                if clear_existing and batch_num == 1:
                    # Get unique ISINs in the first batch
                    batch_isins = chunk['ISIN'].unique().tolist()

                    # Get existing ISINs that match
                    existing_isins = [
                        isin for isin in batch_isins if isin in isin_to_fund
                    ]

                    if existing_isins:
                        # Delete NAV history for these ISINs
                        deleted = NavHistory.query.filter(
                            NavHistory.isin.in_(existing_isins)).delete(
                                synchronize_session=False)

                        db.session.commit()
                        logger.info(
                            f"Cleared {deleted} existing NAV records for {len(existing_isins)} funds"
                        )

                # Process each row in the batch
                for _, row in chunk.iterrows():
                    excel_isin = row['ISIN']
                    scheme_name = row['Scheme Name'].lower() if not pd.isna(
                        row['Scheme Name']) else ""

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
                            fund_isin = Counter(matching_isins).most_common(
                                1)[0][0]

                    if not fund_isin:
                        stats['funds_not_found'] += 1
                        if stats['funds_not_found'] <= 10:  # Limit log spam
                            logger.warning(
                                f"No matching fund found for NAV record with ISIN {excel_isin} and name '{scheme_name}'"
                            )
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
                    nav_history = NavHistory(isin=fund_isin,
                                             date=nav_date,
                                             nav=float(row['Net Asset Value']))
                    db.session.add(nav_history)
                    stats['nav_created'] += 1

                    # Commit every 1000 records to avoid large transactions
                    if stats['nav_created'] % 1000 == 0:
                        db.session.commit()
                        logger.info(
                            f"Committed {stats['nav_created']} NAV records so far"
                        )

                # Commit at the end of each batch
                db.session.commit()
                logger.info(
                    f"Processed batch {batch_num}, total NAV records so far: {stats['nav_created']}"
                )

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
            logger.info(
                f"Factsheets created: {factsheet_stats['factsheets_created']}")
            logger.info(f"Returns created: {returns_stats['returns_created']}")
            logger.info(
                f"Portfolio holdings created: {portfolio_stats['holdings_created']}"
            )
            logger.info(
                f"NAV history records created: {nav_stats['nav_created']}")
            logger.info("==================================")

            return stats

        except Exception as e:
            logger.error(f"Error during complete import process: {e}")
            raise


# Command-line interface
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Import mutual fund data from Excel files")
    parser.add_argument('--factsheet',
                        action='store_true',
                        help='Import factsheet data only')
    parser.add_argument('--returns',
                        action='store_true',
                        help='Import returns data only')
    parser.add_argument('--portfolio',
                        action='store_true',
                        help='Import portfolio data only')
    parser.add_argument('--nav',
                        action='store_true',
                        help='Import NAV history data only')
    parser.add_argument('--all', action='store_true', help='Import all data')
    parser.add_argument('--clear',
                        action='store_true',
                        help='Clear existing data before import')

    args = parser.parse_args()

    # Default to importing all if no specific import is requested
    if not any(
        [args.factsheet, args.returns, args.portfolio, args.nav, args.all]):
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
