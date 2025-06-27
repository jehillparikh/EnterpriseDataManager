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
    
    def import_factsheet_data(self, df, clear_existing=False):
        """
        Import fund and factsheet data from DataFrame
        
        Args:
            df: DataFrame containing factsheet data
            clear_existing (bool): Whether to clear existing data before import
            
        Returns:
            dict: Statistics about the import operation
        """
        logger.info(f"Importing factsheet data with {len(df)} records")
        
        try:
            # Clean data
            df = df.dropna(subset=['ISIN'])
            logger.info(f"Found {len(df)} records with valid ISIN in factsheet data")
            
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
                'factsheets_updated': 0,
                'total_rows_processed': len(df)
            }
            
            # Process each row
            for index, row in df.iterrows():
                try:
                    isin = str(row['ISIN']).strip()
                    
                    # Skip if ISIN is not valid
                    if pd.isna(row['ISIN']) or not isin or isin.lower() == 'nan':
                        logger.warning(f"Skipping row {index+1} with invalid ISIN: {row['ISIN']}")
                        continue
                    
                    scheme_name = str(row.get('Scheme Name', '')).strip()
                    fund_type = str(row.get('Fund Type', row.get('Type', ''))).strip()
                    fund_subtype = str(row.get('Fund Sub Type', row.get('Subtype', ''))).strip() if not pd.isna(row.get('Fund Sub Type', row.get('Subtype'))) else None
                    amc_name = str(row.get('AMC Name', row.get('AMC', ''))).strip()
                    
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
                    launch_date = self._parse_date(row.get('Launch Date'))
                    
                    # Create or update factsheet
                    factsheet = FundFactSheet.query.filter_by(isin=isin).first()
                    
                    if not factsheet:
                        # Create new factsheet
                        factsheet = FundFactSheet(
                            isin=isin,
                            fund_manager=str(row.get('Fund Manager', row.get('Fund Manager(s)', ''))).strip() if not pd.isna(row.get('Fund Manager', row.get('Fund Manager(s)'))) else None,
                            aum=float(row.get('AUM', row.get('AUM (₹ Cr)', 0))) if not pd.isna(row.get('AUM', row.get('AUM (₹ Cr)'))) else None,
                            expense_ratio=float(row.get('Expense Ratio', 0)) if not pd.isna(row.get('Expense Ratio')) else None,
                            launch_date=launch_date,
                            exit_load=str(row.get('Exit Load', '')).strip() if not pd.isna(row.get('Exit Load')) else None
                        )
                        db.session.add(factsheet)
                        stats['factsheets_created'] += 1
                    else:
                        # Update existing factsheet
                        if not pd.isna(row.get('Fund Manager', row.get('Fund Manager(s)'))):
                            factsheet.fund_manager = str(row.get('Fund Manager', row.get('Fund Manager(s)', ''))).strip()
                        if not pd.isna(row.get('AUM', row.get('AUM (₹ Cr)'))):
                            factsheet.aum = float(row.get('AUM', row.get('AUM (₹ Cr)', 0)))
                        if not pd.isna(row.get('Expense Ratio')):
                            factsheet.expense_ratio = float(row.get('Expense Ratio'))
                        if launch_date:
                            factsheet.launch_date = launch_date
                        if not pd.isna(row.get('Exit Load')):
                            factsheet.exit_load = str(row.get('Exit Load', '')).strip()
                        stats['factsheets_updated'] += 1
                    
                    # Commit every 100 records to avoid large transactions
                    if (index + 1) % 100 == 0:
                        db.session.commit()
                        logger.info(f"Processed {index + 1} records")
                        
                except Exception as e:
                    logger.error(f"Error processing row {index+1}: {e}")
                    db.session.rollback()
                    continue
            
            # Commit all changes
            db.session.commit()
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
                    returns = FundReturns(
                        isin=isin,
                        return_1m=float(row.get('1M Return', 0)) if not pd.isna(row.get('1M Return')) else None,
                        return_3m=float(row.get('3M Return', 0)) if not pd.isna(row.get('3M Return')) else None,
                        return_6m=float(row.get('6M Return', 0)) if not pd.isna(row.get('6M Return')) else None,
                        return_ytd=float(row.get('YTD Return', 0)) if not pd.isna(row.get('YTD Return')) else None,
                        return_1y=float(row.get('1Y Return', 0)) if not pd.isna(row.get('1Y Return')) else None,
                        return_3y=float(row.get('3Y Return', 0)) if not pd.isna(row.get('3Y Return')) else None,
                        return_5y=float(row.get('5Y Return', 0)) if not pd.isna(row.get('5Y Return')) else None
                    )
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
                'total_rows_processed': len(df)
            }
            
            # Process each row
            for _, row in df.iterrows():
                try:
                    scheme_isin = str(row.get('Scheme ISIN', '')).strip()
                    if not scheme_isin or scheme_isin.lower() == 'nan':
                        continue
                    
                    holding = FundHolding(
                        isin=scheme_isin,
                        instrument_isin=str(row.get('ISIN', '')) if not pd.isna(row.get('ISIN')) else None,
                        instrument_name=str(row.get('Name of Instrument', '')),
                        sector=str(row.get('Industry', '')) if not pd.isna(row.get('Industry')) else None,
                        quantity=float(row.get('Quantity', 0)) if not pd.isna(row.get('Quantity')) else None,
                        value=float(row.get('Market Value', 0)) if not pd.isna(row.get('Market Value')) else None,
                        percentage_to_nav=float(row.get('% to Net Assets', 0)) if not pd.isna(row.get('% to Net Assets')) else 0,
                        yield_value=float(row.get('Yield', 0)) if not pd.isna(row.get('Yield')) else None,
                        instrument_type=str(row.get('Type', '')),
                        coupon=float(row.get('Coupon', 0)) if not pd.isna(row.get('Coupon')) else None,
                        amc_name=str(row.get('AMC', '')) if not pd.isna(row.get('AMC')) else None,
                        scheme_name=str(row.get('Scheme Name', '')) if not pd.isna(row.get('Scheme Name')) else None
                    )
                    
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
                    
                    nav_record = NavHistory(
                        isin=isin,
                        date=nav_date,
                        nav=nav_value
                    )
                    
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