import pandas as pd
import os
import logging
import sys
from datetime import datetime

# Add parent directory to path to enable imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from setup_db import db
from models import Fund, FundFactSheet, FundReturns, FundHolding, NavHistory

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class FundDataImporter:
    """
    Class for importing mutual fund data from Excel files into the database.
    Uses ISIN as the primary key for all fund data.
    
    Handles:
    - Fund basic data and factsheets
    - Fund returns data
    - Fund portfolio holdings
    - Fund NAV history
    """
    
    def __init__(self):
        """Initialize the FundDataImporter"""
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
    
    def import_factsheet_data(self, df, clear_existing=False, batch_size=1000):
        """
        Import fund and factsheet data from DataFrame with batch processing
        
        Args:
            df: DataFrame containing factsheet data
            clear_existing (bool): Whether to clear existing data before import
            batch_size (int): Number of records to process in each batch
            
        Returns:
            dict: Statistics about the import operation
        """
        logger.info(f"Importing factsheet data with {len(df)} records")

        try:
            df = df.dropna(subset=['ISIN'])
            logger.info(f"{len(df)} valid ISINs after cleaning")

            if clear_existing and not df.empty:
                isins = df['ISIN'].unique().tolist()
                FundFactSheet.query.filter(FundFactSheet.isin.in_(isins)).delete(synchronize_session=False)
                Fund.query.filter(Fund.isin.in_(isins)).delete(synchronize_session=False)
                db.session.commit()
                logger.info(f"Cleared existing records for {len(isins)} ISINs")

            stats = {
                'funds_created': 0,
                'factsheets_created': 0,
                'total_rows_processed': len(df),
                'batches_processed': 0
            }

            fund_records, factsheet_records = [], []
            for idx, row in df.iterrows():
                try:
                    isin = str(row.get('ISIN', '')).strip()
                    if not isin or isin.lower() == 'nan':
                        continue

                    fund_data = {
                        'isin': isin,
                        'scheme_name': str(row.get('Scheme Name', '')).strip(),
                        'fund_type': str(row.get('Fund Type', row.get('Type', ''))).strip(),
                        'fund_subtype': str(row.get('Fund Sub Type', row.get('Subtype', ''))).strip() if not pd.isna(row.get('Fund Sub Type', row.get('Subtype'))) else None,
                        'amc_name': str(row.get('AMC Name', row.get('AMC', ''))).strip(),
                    }

                    factsheet_data = {
                        'isin': isin,
                        'fund_manager': str(row.get('Fund Manager', row.get('Fund Manager(s)', ''))).strip() if not pd.isna(row.get('Fund Manager', row.get('Fund Manager(s)'))) else None,
                        'aum': float(row.get('AUM', row.get('AUM (₹ Cr)', 0))) if not pd.isna(row.get('AUM', row.get('AUM (₹ Cr)'))) else None,
                        'expense_ratio': float(row.get('Expense Ratio', 0)) if not pd.isna(row.get('Expense Ratio')) else None,
                        'launch_date': self._parse_date(row.get('Launch Date')),
                        'exit_load': str(row.get('Exit Load', '')).strip() if not pd.isna(row.get('Exit Load')) else None,
                    }

                    fund_records.append(fund_data)
                    factsheet_records.append(factsheet_data)

                    # Bulk insert in batches
                    if len(fund_records) >= batch_size:
                        db.session.bulk_insert_mappings(Fund.__mapper__, fund_records)
                        db.session.bulk_insert_mappings(FundFactSheet.__mapper__, factsheet_records)
                        db.session.commit()
                        stats['funds_created'] += len(fund_records)
                        stats['factsheets_created'] += len(factsheet_records)
                        stats['batches_processed'] += 1
                        fund_records.clear()
                        factsheet_records.clear()
                        logger.info(f"Batch {stats['batches_processed']} committed")

                except Exception as e:
                    logger.error(f"Error processing row {idx+1}: {e}")
                    continue

            # Insert remaining
            if fund_records:
                db.session.bulk_insert_mappings(Fund.__mapper__, fund_records)
                db.session.bulk_insert_mappings(FundFactSheet.__mapper__, factsheet_records)
                db.session.commit()
                stats['funds_created'] += len(fund_records)
                stats['factsheets_created'] += len(factsheet_records)
                stats['batches_processed'] += 1

            logger.info(f"Factsheet import completed: {stats}")
            return stats
            
        except Exception as e:
            db.session.rollback()
            logger.error(f"Error importing factsheet data: {e}")
            raise
    
    def import_returns_data(self, df, clear_existing=False):
        """
        Import fund returns data from DataFrame
        
        Args:
            df: DataFrame containing returns data
            clear_existing (bool): Whether to clear existing data before import
            
        Returns:
            dict: Statistics about the import operation
        """
        logger.info(f"Importing returns data with {len(df)} records")
        
        try:
            # Clean data
            df = df.dropna(subset=['ISIN'])
            
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
                'funds_not_found': 0,
                'total_rows_processed': len(df)
            }
            
            # Process each row
            for _, row in df.iterrows():
                isin = str(row['ISIN']).strip()
                
                if not isin or isin.lower() == 'nan':
                    continue
                
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
                    returns = FundReturns()
                    returns.isin = isin
                    returns.return_1m = float(row.get('1M Return', 0)) if not pd.isna(row.get('1M Return')) else None
                    returns.return_3m = float(row.get('3M Return', 0)) if not pd.isna(row.get('3M Return')) else None
                    returns.return_6m = float(row.get('6M Return', 0)) if not pd.isna(row.get('6M Return')) else None
                    returns.return_ytd = float(row.get('YTD Return', 0)) if not pd.isna(row.get('YTD Return')) else None
                    returns.return_1y = float(row.get('1Y Return', 0)) if not pd.isna(row.get('1Y Return')) else None
                    returns.return_3y = float(row.get('3Y Return', 0)) if not pd.isna(row.get('3Y Return')) else None
                    returns.return_5y = float(row.get('5Y Return', 0)) if not pd.isna(row.get('5Y Return')) else None
                    db.session.add(returns)
                    stats['returns_created'] += 1
                else:
                    # Update existing returns record
                    returns.return_1m = float(row.get('1M Return', 0)) if not pd.isna(row.get('1M Return')) else returns.return_1m
                    returns.return_3m = float(row.get('3M Return', 0)) if not pd.isna(row.get('3M Return')) else returns.return_3m
                    returns.return_6m = float(row.get('6M Return', 0)) if not pd.isna(row.get('6M Return')) else returns.return_6m
                    returns.return_ytd = float(row.get('YTD Return', 0)) if not pd.isna(row.get('YTD Return')) else returns.return_ytd
                    returns.return_1y = float(row.get('1Y Return', 0)) if not pd.isna(row.get('1Y Return')) else returns.return_1y
                    returns.return_3y = float(row.get('3Y Return', 0)) if not pd.isna(row.get('3Y Return')) else returns.return_3y
                    returns.return_5y = float(row.get('5Y Return', 0)) if not pd.isna(row.get('5Y Return')) else returns.return_5y
                    stats['returns_updated'] += 1
            
            # Commit all changes
            db.session.commit()
            logger.info(f"Returns import completed: {stats}")
            
            return stats
            
        except Exception as e:
            db.session.rollback()
            logger.error(f"Error importing returns data: {e}")
            raise
    
    def import_holdings_data(self, df, clear_existing=False):
        """
        Import fund holdings data from DataFrame
        
        Args:
            df: DataFrame containing holdings data
            clear_existing (bool): Whether to clear existing data before import
            
        Returns:
            dict: Statistics about the import operation
        """
        logger.info(f"Importing holdings data with {len(df)} records")
        
        try:
            if clear_existing and len(df) > 0:
                # Clear all existing holdings
                FundHolding.query.delete()
                db.session.commit()
                logger.info("Cleared existing holdings data")
            
            # Track statistics
            stats = {
                'holdings_created': 0,
                'rows_skipped_invalid_isin': 0,
                'rows_skipped_no_fund': 0,
                'total_rows_processed': len(df)
            }
            
            # Process each row
            for _, row in df.iterrows():
                try:
                    scheme_isin = str(row.get('Scheme ISIN', '')).strip()
                    
                    # Skip if Scheme ISIN is invalid, empty, NaN, or contains invalid characters
                    if (not scheme_isin or 
                        scheme_isin.lower() == 'nan' or 
                        scheme_isin == '' or 
                        scheme_isin == '-' or 
                        scheme_isin == 'None' or
                        pd.isna(row.get('Scheme ISIN')) or
                        len(scheme_isin) < 8):  # ISIN should be at least 8 characters
                        logger.warning(f"Skipping row with invalid Scheme ISIN: '{scheme_isin}'")
                        stats['rows_skipped_invalid_isin'] += 1
                        continue
                    
                    # Check if the fund exists in database
                    fund_exists = Fund.query.filter_by(isin=scheme_isin).first()
                    if not fund_exists:
                        logger.warning(f"Skipping holding for non-existent fund ISIN: '{scheme_isin}'")
                        stats['rows_skipped_no_fund'] += 1
                        continue
                    
                    holding = FundHolding()
                    holding.isin = scheme_isin
                    holding.instrument_isin = str(row.get('ISIN', '')) if not pd.isna(row.get('ISIN')) else None
                    holding.instrument_name = str(row.get('Name of Instrument', ''))
                    holding.sector = str(row.get('Industry', '')) if not pd.isna(row.get('Industry')) else None
                    holding.quantity = float(row.get('Quantity', 0)) if not pd.isna(row.get('Quantity')) else None
                    holding.value = float(row.get('Market Value', 0)) if not pd.isna(row.get('Market Value')) else None
                    holding.percentage_to_nav = float(row.get('% to Net Assets', 0)) if not pd.isna(row.get('% to Net Assets')) else 0
                    holding.yield_value = float(row.get('Yield', 0)) if not pd.isna(row.get('Yield')) else None
                    holding.instrument_type = str(row.get('Type', ''))
                    holding.coupon = float(row.get('Coupon', 0)) if not pd.isna(row.get('Coupon')) else None
                    holding.amc_name = str(row.get('AMC', '')) if not pd.isna(row.get('AMC')) else None
                    holding.scheme_name = str(row.get('Scheme Name', '')) if not pd.isna(row.get('Scheme Name')) else None
                    
                    db.session.add(holding)
                    stats['holdings_created'] += 1
                    
                except Exception as e:
                    logger.error(f"Error processing holding row: {e}")
                    continue
            
            # Commit all changes
            db.session.commit()
            logger.info(f"Holdings import completed: {stats}")
            
            return stats
            
        except Exception as e:
            db.session.rollback()
            logger.error(f"Error importing holdings data: {e}")
            raise
    
    def import_nav_data(self, df, clear_existing=False, batch_size=1000):
        """
        Import NAV data from DataFrame
        
        Args:
            df: DataFrame containing NAV data
            clear_existing (bool): Whether to clear existing data before import
            batch_size (int): Number of records to process in each batch
            
        Returns:
            dict: Statistics about the import operation
        """
        logger.info(f"Importing NAV data with {len(df)} records")
        
        try:
            if clear_existing and len(df) > 0:
                # Clear all existing NAV data
                NavHistory.query.delete()
                db.session.commit()
                logger.info("Cleared existing NAV data")
            
            # Track statistics
            stats = {
                'nav_records_created': 0,
                'total_rows_processed': len(df),
                'batch_size_used': batch_size
            }
            
            batch_count = 0
            
            # Process each row
            for _, row in df.iterrows():
                try:
                    isin = str(row.get('ISIN', '')).strip()
                    if not isin or isin.lower() == 'nan':
                        continue
                    
                    # Parse date
                    date_str = str(row.get('Date', ''))
                    if pd.notna(row.get('Date')):
                        if isinstance(row.get('Date'), datetime):
                            nav_date = row.get('Date').date()
                        else:
                            nav_date = pd.to_datetime(date_str).date()
                    else:
                        continue
                    
                    nav_value = float(row.get('NAV', 0)) if pd.notna(row.get('NAV')) else None
                    if nav_value is None:
                        continue
                    
                    nav_record = NavHistory()
                    nav_record.isin = isin
                    nav_record.date = nav_date
                    nav_record.nav = nav_value
                    
                    db.session.add(nav_record)
                    stats['nav_records_created'] += 1
                    batch_count += 1
                    
                    # Commit in batches
                    if batch_count >= batch_size:
                        db.session.commit()
                        batch_count = 0
                        logger.info(f"Committed batch of {batch_size} NAV records")
                        
                except Exception as e:
                    logger.error(f"Error processing NAV row: {e}")
                    continue
            
            # Commit remaining records
            if batch_count > 0:
                db.session.commit()
            
            logger.info(f"NAV import completed: {stats}")
            
            return stats
            
        except Exception as e:
            db.session.rollback()
            logger.error(f"Error importing NAV data: {e}")
            raise