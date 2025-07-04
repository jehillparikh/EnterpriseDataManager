import pandas as pd
import os
import logging
import sys
from datetime import datetime

# Add parent directory to path to enable imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from setup_db import db
from models import Fund, FundFactSheet, FundReturns, FundHolding, NavHistory, BSEScheme

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
    - Fund basic data and factsheets
    - Fund returns data
    - Fund portfolio holdings
    - Fund NAV history
    - BSE scheme data
    """

    def __init__(self):
        """Initialize the FundDataImporter"""
        self.existing_isins = self.get_existing_isins()

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

    def get_existing_isins(self):
        """Fetches all valid ISINs from the mf_fund table."""
        try:
            result = db.session.execute(text("SELECT isin FROM mf_fund"))
            return {row[0] for row in result}
        except Exception as e:
            logger.error(f"Error fetching ISINs from mf_fund: {e}")
            return set()

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
        logger.info(
            f"Importing factsheet data with {len(df)} records using bulk upsert strategy"
        )

        try:
            df = df.dropna(subset=['ISIN'])
            logger.info(f"{len(df)} valid ISINs after cleaning")

            if clear_existing:
                # Clear ALL existing factsheet and fund data
                factsheet_count = FundFactSheet.query.count()
                fund_count = Fund.query.count()

                FundFactSheet.query.delete()
                Fund.query.delete()
                db.session.commit()
                logger.info(
                    f"Cleared ALL existing data: {factsheet_count} factsheets and {fund_count} funds"
                )

            stats = {
                'funds_processed': 0,
                'factsheets_processed': 0,
                'total_rows_processed': len(df),
                'batches_processed': 0
            }

            # Process records in batches using bulk upsert strategy
            for batch_start in range(0, len(df), batch_size):
                batch_end = min(batch_start + batch_size, len(df))
                batch_df = df.iloc[batch_start:batch_end]
                current_batch = (batch_start // batch_size) + 1
                total_batches = (len(df) + batch_size - 1) // batch_size

                logger.info(
                    f"Processing batch {current_batch}/{total_batches} (rows {batch_start+1}-{batch_end})"
                )

                fund_records = []
                factsheet_records = []

                for idx, row in batch_df.iterrows():
                    try:
                        isin = str(row.get('ISIN', '')).strip()
                        if not isin or isin.lower() == 'nan':
                            logger.warning(
                                f"row {idx+1} with NO ISIN: '{isin}'")

                            continue

                        isin = str(row.get('ISIN', '')).strip()
                        if not isin or isin.lower() in ['nan', 'none', '-']:
                            continue

                        # Extract fund data - using new column structure
                        scheme_name = str(row.get('Scheme Name', '')).strip()
                        scheme_type = str(row.get(
                            'Scheme Type', '')).strip() if not pd.isna(
                                row.get('Scheme Type')) else None
                        sub_category = str(row.get(
                            'Scheme Sub Category', '')).strip() if not pd.isna(
                                row.get('Scheme Sub Category')) else None

                        #fund_type = str(row.get('Scheme Type', row.get('Fund Type', row.get('Type', '')))).strip()
                        #fund_subtype = str(row.get('sub_category', row.get('Fund Sub Type', row.get('Subtype', '')))).strip() if not pd.isna(row.get('Sub Category', row.get('Fund Sub Type', row.get('Subtype')))) else None
                        amc_name = str(row.get('AMC', row.get('AMC Name',
                                                              ''))).strip()

                        fund_record = {
                            'isin': isin,
                            'scheme_name': scheme_name,
                            'fund_type': scheme_type,
                            'fund_subtype': sub_category,
                            'amc_name': amc_name
                        }
                        fund_records.append(fund_record)

                        # Extract enhanced factsheet data - supporting new column structure
                        # Core fund information
                        factsheet_scheme_name = str(row.get(
                            'Scheme Name', '')).strip() if not pd.isna(
                                row.get('Scheme Name')) else None
                        plan = str(row.get('Plan', '')).strip() if not pd.isna(
                            row.get('Plan')) else None
                        amc = str(row.get('AMC', '')).strip() if not pd.isna(
                            row.get('AMC')) else None

                        # Financial details
                        expense_ratio = float(row.get(
                            'Expense Ratio', 0)) if not pd.isna(
                                row.get('Expense Ratio')) else None
                        minimum_lumpsum = float(row.get(
                            'Minimum Lumpsum', 0)) if not pd.isna(
                                row.get('Minimum Lumpsum')) else None
                        minimum_sip = float(row.get(
                            'Minimum SIP', 0)) if not pd.isna(
                                row.get('Minimum SIP')) else None

                        # Investment terms
                        lock_in = str(row.get('Lock-in',
                                              '')).strip() if not pd.isna(
                                                  row.get('Lock-in')) else None
                        exit_load = str(row.get(
                            'Exit Load', '')).strip() if not pd.isna(
                                row.get('Exit Load')) else None

                        # Management and risk
                        fund_manager = str(row.get(
                            'Fund Manager', '')).strip() if not pd.isna(
                                row.get('Fund Manager')) else None
                        benchmark = str(row.get(
                            'Benchmark', '')).strip() if not pd.isna(
                                row.get('Benchmark')) else None
                        sebi_risk_category = str(
                            row.get(
                                'SEBI Risk Category',
                                '')).strip() if not pd.isna(
                                    row.get('SEBI Risk Category')) else None

                        # Legacy fields for backward compatibility
                        launch_date = self._parse_date(row.get('Launch Date'))

                        factsheet_record = {
                            'isin': isin,
                            # Enhanced fields from new column structure
                            'scheme_name': factsheet_scheme_name,
                            'scheme_type': scheme_type,
                            'sub_category': sub_category,
                            'plan': plan,
                            'amc': amc,
                            'expense_ratio': expense_ratio,
                            'minimum_lumpsum': minimum_lumpsum,
                            'minimum_sip': minimum_sip,
                            'lock_in': lock_in,
                            'exit_load': exit_load,
                            'fund_manager': fund_manager,
                            'benchmark': benchmark,
                            'sebi_risk_category': sebi_risk_category,
                            # Legacy fields for backward compatibility
                            'launch_date': launch_date
                        }
                        factsheet_records.append(factsheet_record)

                    except Exception as e:
                        logger.error(f"Error processing row {idx+1}: {e}")
                        continue

                # Bulk upsert funds using PostgreSQL ON CONFLICT
                if fund_records:
                    from sqlalchemy.dialects.postgresql import insert
                    from datetime import datetime

                    # Add timestamps to records
                    for record in fund_records:
                        record['created_at'] = datetime.utcnow()
                        record['updated_at'] = datetime.utcnow()

                    stmt = insert(Fund.__table__).values(fund_records)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=['isin'],
                        set_=dict(scheme_name=stmt.excluded.scheme_name,
                                  fund_type=stmt.excluded.fund_type,
                                  fund_subtype=stmt.excluded.fund_subtype,
                                  amc_name=stmt.excluded.amc_name,
                                  updated_at=stmt.excluded.updated_at))
                    db.session.execute(stmt)
                    stats['funds_processed'] += len(fund_records)

                # Bulk upsert factsheets using PostgreSQL ON CONFLICT
                if factsheet_records:
                    from sqlalchemy.dialects.postgresql import insert
                    from datetime import datetime

                    # Add timestamps to records
                    for record in factsheet_records:
                        record['last_updated'] = datetime.utcnow()

                    stmt = insert(
                        FundFactSheet.__table__).values(factsheet_records)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=['isin'],
                        set_=dict(
                            # Standardized Excel columns
                            scheme_name=stmt.excluded.scheme_name,
                            scheme_type=stmt.excluded.scheme_type,
                            sub_category=stmt.excluded.sub_category,
                            plan=stmt.excluded.plan,
                            amc=stmt.excluded.amc,
                            expense_ratio=stmt.excluded.expense_ratio,
                            minimum_lumpsum=stmt.excluded.minimum_lumpsum,
                            minimum_sip=stmt.excluded.minimum_sip,
                            lock_in=stmt.excluded.lock_in,
                            exit_load=stmt.excluded.exit_load,
                            fund_manager=stmt.excluded.fund_manager,
                            benchmark=stmt.excluded.benchmark,
                            sebi_risk_category=stmt.excluded.
                            sebi_risk_category,
                            # Legacy fields
                            launch_date=stmt.excluded.launch_date,
                            last_updated=stmt.excluded.last_updated))
                    db.session.execute(stmt)
                    stats['factsheets_processed'] += len(factsheet_records)

                # Commit batch
                db.session.commit()
                stats['batches_processed'] += 1
                logger.info(
                    f"Completed batch {current_batch}/{total_batches} - {len(fund_records)} funds, {len(factsheet_records)} factsheets"
                )

            logger.info(f"Bulk factsheet upsert completed: {stats}")
            return stats

        except Exception as e:
            db.session.rollback()
            logger.error(f"Error importing factsheet data: {e}")
            raise

    def import_returns_data(self, df, clear_existing=False):
        """
        Import fund returns data from DataFrame using bulk upsert strategy
        
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
                FundReturns.query.filter(FundReturns.isin.in_(isins)).delete(
                    synchronize_session=False)

                db.session.commit()
                logger.info(
                    f"Cleared existing returns data for {len(isins)} ISINs")

            # Track statistics
            stats = {
                'returns_created': 0,
                'returns_updated': 0,
                'funds_not_found': 0,
                'total_rows_processed': len(df)
            }

            # Get all valid fund ISINs for validation
            valid_fund_isins = set(
                fund.isin
                for fund in Fund.query.with_entities(Fund.isin).all())

            # Prepare records for bulk upsert
            returns_records = []

            for _, row in df.iterrows():
                isin = str(row['ISIN']).strip()

                if not isin or isin.lower() == 'nan':
                    continue

                # Skip if fund doesn't exist
                if isin not in valid_fund_isins:
                    logger.warning(
                        f"Skipping returns for {isin}: Fund not found in database"
                    )
                    stats['funds_not_found'] += 1
                    continue

                # Create returns record
                returns_record = {
                    'isin':
                    isin,
                    'return_1m':
                    float(row.get('1M Return', 0))
                    if not pd.isna(row.get('1M Return')) else None,
                    'return_3m':
                    float(row.get('3M Return', 0))
                    if not pd.isna(row.get('3M Return')) else None,
                    'return_6m':
                    float(row.get('6M Return', 0))
                    if not pd.isna(row.get('6M Return')) else None,
                    'return_ytd':
                    float(row.get('YTD Return', 0))
                    if not pd.isna(row.get('YTD Return')) else None,
                    'return_1y':
                    float(row.get('1Y Return', 0))
                    if not pd.isna(row.get('1Y Return')) else None,
                    'return_3y':
                    float(row.get('3Y Return', 0))
                    if not pd.isna(row.get('3Y Return')) else None,
                    'return_5y':
                    float(row.get('5Y Return', 0))
                    if not pd.isna(row.get('5Y Return')) else None
                }
                returns_records.append(returns_record)

            # Bulk upsert returns using PostgreSQL
            if returns_records:
                from sqlalchemy.dialects.postgresql import insert

                stmt = insert(FundReturns.__table__).values(returns_records)
                stmt = stmt.on_conflict_do_update(
                    index_elements=['isin'],
                    set_=dict(return_1m=stmt.excluded.return_1m,
                              return_3m=stmt.excluded.return_3m,
                              return_6m=stmt.excluded.return_6m,
                              return_ytd=stmt.excluded.return_ytd,
                              return_1y=stmt.excluded.return_1y,
                              return_3y=stmt.excluded.return_3y,
                              return_5y=stmt.excluded.return_5y))
                db.session.execute(stmt)
                stats['returns_created'] = len(returns_records)

            # Commit all changes
            db.session.commit()
            logger.info(f"Returns import completed: {stats}")

            return stats

        except Exception as e:
            db.session.rollback()
            logger.error(f"Error importing returns data: {e}")
            raise

    def import_holdings_data(self, df, clear_existing=False, batch_size=1000):
        """
        Import fund holdings data from DataFrame using bulk insert strategy
        
        Args:
            df: DataFrame containing holdings data with columns:
                Name of Instrument, ISIN, Coupon, Industry, Quantity, 
                Market Value, % to Net Assets, Yield, Type, Scheme ISIN
            clear_existing (bool): Whether to clear existing data before import
            batch_size (int): Number of records to process in each batch
            
        Returns:
            dict: Statistics about the import operation
        """
        logger.info(
            f"Importing holdings data with {len(df)} records using bulk upsert strategy"
        )

        try:
            if clear_existing and len(df) > 0:
                # Clear all existing holdings
                FundHolding.query.delete()
                db.session.commit()
                logger.info("Cleared existing holdings data")

            # Track statistics
            stats = {
                'holdings_processed': 0,
                'rows_skipped_invalid_isin': 0,
                'rows_skipped_no_fund': 0,
                'total_rows_processed': len(df),
                'batches_processed': 0
            }

            # Get all valid fund ISINs for validation
            valid_fund_isins = set(
                fund.isin
                for fund in Fund.query.with_entities(Fund.isin).all())

            #valid_fund_isins=self.existing_isins TO DO check if this is faster

            # Process data in batches
            total_batches = (len(df) + batch_size - 1) // batch_size

            for batch_num in range(total_batches):
                start_idx = batch_num * batch_size
                end_idx = min(start_idx + batch_size, len(df))
                batch_df = df.iloc[start_idx:end_idx]

                logger.info(
                    f"Processing batch {batch_num + 1}/{total_batches} (rows {start_idx + 1}-{end_idx})"
                )

                holdings_records = []

                for idx, row in batch_df.iterrows():
                    try:
                        scheme_isin = str(row.get('Scheme ISIN', '')).strip()

                        instrument_isin = str(row.get('ISIN')).strip()

                        # Skip if Scheme ISIN is invalid, empty, NaN, or contains invalid characters
                        if (not scheme_isin or scheme_isin.lower() == 'nan'
                                or scheme_isin == '' or scheme_isin == '-'
                                or scheme_isin == 'None'
                                or pd.isna(row.get('Scheme ISIN'))
                                or len(scheme_isin) < 8 or len(scheme_isin)
                                > 12):  # ISIN should be at least 8 characters
                            logger.warning(
                                f"Skipping row {idx+1} with invalid Scheme ISIN: '{scheme_isin}'"
                            )
                            stats['rows_skipped_invalid_isin'] += 1
                            continue

                        if len(instrument_isin) > 12 or len(
                                instrument_isin
                        ) < 8 or instrument_isin.lower(
                        ) == 'nan' or instrument_isin == '' or instrument_isin == '-' or instrument_isin == 'None':
                            logger.warning(
                                f"Skipping holding for non-valid instrutment ISIN: '{instrument_isin}'"
                            )
                            continue

                        # Check if the fund exists in database
                        if scheme_isin not in valid_fund_isins:
                            logger.warning(
                                f"Skipping holding for non-existent fund ISIN: '{scheme_isin}'"
                            )
                            stats['rows_skipped_no_fund'] += 1
                            continue

                        # Create holding record
                        holding_record = {
                            'isin':
                            scheme_isin,
                            'instrument_isin':
                            str(row.get('ISIN', '')).strip(),
                            'instrument_name':
                            str(row.get('Name of Instrument', '')).strip(),
                            'sector':
                            str(row.get('Industry', '')).strip()
                            if not pd.isna(row.get('Industry')) else None,
                            'quantity':
                            float(row.get('Quantity', 0))
                            if not pd.isna(row.get('Quantity')) else None,
                            'value':
                            float(row.get('Market Value', 0))
                            if not pd.isna(row.get('Market Value')) else None,
                            'percentage_to_nav':
                            float(row.get('% to Net Assets', 0))
                            if not pd.isna(row.get('% to Net Assets')) else 0,
                            'yield_value':
                            float(row.get('Yield', 0))
                            if not pd.isna(row.get('Yield')) else None,
                            'instrument_type':
                            str(row.get('Type', '')).strip(),
                            'coupon':
                            float(row.get('Coupon', 0))
                            if not pd.isna(row.get('Coupon')) else None
                        }
                        holdings_records.append(holding_record)

                    except Exception as e:
                        logger.error(
                            f"Error processing holding row {idx+1}: {e}")
                        continue

                logger.info(f"commiting stats of the batch records")

                # Bulk insert holdings using simple INSERT
                if holdings_records:
                    # Create FundHolding objects for bulk insert
                    holdings_objects = [
                        FundHolding(**record) for record in holdings_records
                    ]

                    # Use bulk insert without conflict resolution
                    db.session.bulk_save_objects(holdings_objects)
                    stats['holdings_processed'] += len(holdings_records)

                stats['batches_processed'] += 1

            # Commit all changes
            db.session.commit()
            logger.info(f"Holdings bulk import completed: {stats}")

            return stats

        except Exception as e:
            db.session.rollback()
            logger.error(f"Error importing holdings data: {e}")
            raise

    def import_nav_data(self, df, clear_existing=False, batch_size=1000):
        """
        Import NAV data from DataFrame using bulk upsert strategy
        
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
                'batch_size_used': batch_size,
                'missing_funds_skipped': 0
            }

            batch_count = 0

            # Process data in batches
            total_batches = (len(df) + batch_size - 1) // batch_size

            for batch_num in range(total_batches):
                start_idx = batch_num * batch_size
                end_idx = min(start_idx + batch_size, len(df))
                batch_df = df.iloc[start_idx:end_idx]

                logger.info(
                    f"Processing NAV batch {batch_num + 1}/{total_batches} (rows {start_idx + 1}-{end_idx})"
                )

                nav_records = []

                for _, row in batch_df.iterrows():
                    try:
                        isin = str(row.get('ISIN', '')).strip()
                        if not isin or isin.lower() == 'nan' or len(isin) < 8:
                            continue

                        if isin not in self.existing_isins:
                            stats['missing_funds_skipped'] += 1
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

                        nav_value = float(row.get('NAV', 0)) if pd.notna(
                            row.get('NAV')) else None
                        if nav_value is None:
                            continue

                        nav_record = {
                            'isin': isin,
                            'date': nav_date,
                            'nav': nav_value
                        }
                        nav_records.append(nav_record)

                    except Exception as e:
                        logger.error(f"Error processing NAV row: {e}")
                        continue

                # Bulk upsert NAV records using PostgreSQL
                if nav_records:
                    from sqlalchemy.dialects.postgresql import insert

                    stmt = insert(NavHistory.__table__).values(nav_records)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=['isin', 'date'],
                        set_=dict(nav=stmt.excluded.nav))
                    db.session.execute(stmt)
                    stats['nav_records_created'] += len(nav_records)

                # Commit batch
                db.session.commit()

            logger.info(f"NAV import completed: {stats}")

            return stats

        except Exception as e:
            db.session.rollback()
            logger.error(f"Error importing NAV data: {e}")
            raise

    def import_bse_scheme_data(self,
                               df,
                               clear_existing=False,
                               batch_size=1000):
        """
        Import BSE scheme data from DataFrame using bulk upsert strategy
        
        Args:
            df: DataFrame containing BSE scheme data
            clear_existing (bool): Whether to clear existing data before import
            batch_size (int): Number of records to process in each batch
            
        Returns:
            dict: Statistics about the import operation
        """
        try:
            logger.info(f"Starting BSE scheme import with {len(df)} rows")

            # Clear existing data if requested
            if clear_existing:
                deleted_count = BSEScheme.query.count()
                db.session.query(BSEScheme).delete()
                db.session.commit()
                logger.info(
                    f"Cleared {deleted_count} existing BSE scheme records")

            # Track statistics
            stats = {
                'schemes_created': 0,
                'schemes_updated': 0,
                'total_rows_processed': len(df),
                'rows_skipped': 0,
                'batch_size_used': batch_size,
                'errors': []
            }

            batch_count = 0

            # Column mapping for BSE scheme data
            column_mapping = {
                'Unique No': 'unique_no',
                'Scheme Code': 'scheme_code',
                'RTA Scheme Code': 'rta_scheme_code',
                'AMC Scheme Code': 'amc_scheme_code',
                'ISIN': 'isin',
                'AMC Code': 'amc_code',
                'Scheme Type': 'scheme_type',
                'Scheme Plan': 'scheme_plan',
                'Scheme Name': 'scheme_name',
                'Purchase Allowed': 'purchase_allowed',
                'Purchase Transaction mode': 'purchase_transaction_mode',
                'Minimum Purchase Amount': 'minimum_purchase_amount',
                'Additional Purchase Amount': 'additional_purchase_amount',
                'Maximum Purchase Amount': 'maximum_purchase_amount',
                'Purchase Amount Multiplier': 'purchase_amount_multiplier',
                'Purchase Cutoff Time': 'purchase_cutoff_time',
                'Redemption Allowed': 'redemption_allowed',
                'Redemption Transaction Mode': 'redemption_transaction_mode',
                'Minimum Redemption Qty': 'minimum_redemption_qty',
                'Redemption Qty Multiplier': 'redemption_qty_multiplier',
                'Maximum Redemption Qty': 'maximum_redemption_qty',
                'Redemption Amount - Minimum': 'redemption_amount_minimum',
                'Redemption Amount – Maximum': 'redemption_amount_maximum',
                'Redemption Amount Multiple': 'redemption_amount_multiple',
                'Redemption Cut off Time': 'redemption_cutoff_time',
                'RTA Agent Code': 'rta_agent_code',
                'AMC Active Flag': 'amc_active_flag',
                'Dividend Reinvestment Flag': 'dividend_reinvestment_flag',
                'SIP FLAG': 'sip_flag',
                'STP FLAG': 'stp_flag',
                'SWP Flag': 'swp_flag',
                'Switch FLAG': 'switch_flag',
                'SETTLEMENT TYPE': 'settlement_type',
                'AMC_IND': 'amc_ind',
                'Face Value': 'face_value',
                'Start Date': 'start_date',
                'End Date': 'end_date',
                'Exit Load Flag': 'exit_load_flag',
                'Exit Load': 'exit_load',
                'Lock-in Period Flag': 'lockin_period_flag',
                'Lock-in Period': 'lockin_period',
                'Channel Partner Code': 'channel_partner_code',
                'ReOpening Date': 'reopening_date'
            }

            # Process data in batches
            total_batches = (len(df) + batch_size - 1) // batch_size

            for batch_num in range(total_batches):
                start_idx = batch_num * batch_size
                end_idx = min(start_idx + batch_size, len(df))
                batch_df = df.iloc[start_idx:end_idx]

                logger.info(
                    f"Processing BSE batch {batch_num + 1}/{total_batches} (rows {start_idx + 1}-{end_idx})"
                )

                scheme_records = []

                for index, row in batch_df.iterrows():
                    try:
                        # Check if required fields are present
                        unique_no = row.get('Unique No')
                        scheme_code = row.get('Scheme Code')
                        isin = row.get('ISIN')

                        if pd.isna(unique_no) or pd.isna(
                                scheme_code) or pd.isna(isin):
                            stats['rows_skipped'] += 1
                            logger.warning(
                                f"Skipping row {index}: Missing required fields"
                            )
                            continue

                        # Create scheme record
                        scheme_record = {
                            'unique_no':
                            int(unique_no),
                            'scheme_code':
                            str(row.get('Scheme Code', '')),
                            'rta_scheme_code':
                            str(row.get('RTA Scheme Code', '')),
                            'amc_scheme_code':
                            str(row.get('AMC Scheme Code', '')),
                            'isin':
                            str(row.get('ISIN', '')),
                            'amc_code':
                            str(row.get('AMC Code', '')),
                            'scheme_type':
                            str(row.get('Scheme Type', '')),
                            'scheme_plan':
                            str(row.get('Scheme Plan', '')),
                            'scheme_name':
                            str(row.get('Scheme Name', '')),
                            'purchase_allowed':
                            str(row.get('Purchase Allowed', 'N')),
                            'purchase_transaction_mode':
                            str(row.get('Purchase Transaction mode', '')),
                            'minimum_purchase_amount':
                            float(row.get('Minimum Purchase Amount', 0)),
                            'additional_purchase_amount':
                            float(row.get('Additional Purchase Amount', 0)),
                            'maximum_purchase_amount':
                            float(row.get('Maximum Purchase Amount', 0)),
                            'purchase_amount_multiplier':
                            float(row.get('Purchase Amount Multiplier', 0)),
                            'purchase_cutoff_time':
                            str(row.get('Purchase Cutoff Time', '')),
                            'redemption_allowed':
                            str(row.get('Redemption Allowed', 'N')),
                            'redemption_transaction_mode':
                            str(row.get('Redemption Transaction Mode', '')),
                            'minimum_redemption_qty':
                            float(row.get('Minimum Redemption Qty', 0)),
                            'redemption_qty_multiplier':
                            float(row.get('Redemption Qty Multiplier', 0)),
                            'maximum_redemption_qty':
                            float(row.get('Maximum Redemption Qty', 0)),
                            'redemption_amount_minimum':
                            float(row.get('Redemption Amount - Minimum', 0)),
                            'redemption_amount_maximum':
                            float(row.get('Redemption Amount – Maximum', 0)),
                            'redemption_amount_multiple':
                            float(row.get('Redemption Amount Multiple', 0)),
                            'redemption_cutoff_time':
                            str(row.get('Redemption Cut off Time', '')),
                            'rta_agent_code':
                            str(row.get('RTA Agent Code', '')),
                            'amc_active_flag':
                            int(row.get('AMC Active Flag', 0)),
                            'dividend_reinvestment_flag':
                            str(row.get('Dividend Reinvestment Flag', 'N')),
                            'sip_flag':
                            str(row.get('SIP FLAG', 'N')),
                            'stp_flag':
                            str(row.get('STP FLAG', 'N')),
                            'swp_flag':
                            str(row.get('SWP Flag', 'N')),
                            'switch_flag':
                            str(row.get('Switch FLAG', 'N')),
                            'settlement_type':
                            str(row.get('SETTLEMENT TYPE', '')),
                            'amc_ind':
                            float(row.get('AMC_IND')) if pd.notna(
                                row.get('AMC_IND')) else None,
                            'face_value':
                            float(row.get('Face Value', 0)),
                            'start_date':
                            self._parse_date(row.get('Start Date')),
                            'end_date':
                            self._parse_date(row.get('End Date')),
                            'reopening_date':
                            self._parse_date(row.get('ReOpening Date'))
                            if pd.notna(row.get('ReOpening Date')) else None,
                            'exit_load_flag':
                            str(row.get('Exit Load Flag')) if pd.notna(
                                row.get('Exit Load Flag')) else None,
                            'exit_load':
                            str(row.get('Exit Load', '')),
                            'lockin_period_flag':
                            str(row.get('Lock-in Period Flag')) if pd.notna(
                                row.get('Lock-in Period Flag')) else None,
                            'lockin_period':
                            float(row.get('Lock-in Period')) if pd.notna(
                                row.get('Lock-in Period')) else None,
                            'channel_partner_code':
                            str(row.get('Channel Partner Code', ''))
                        }
                        scheme_records.append(scheme_record)

                    except Exception as e:
                        error_msg = f"Error processing row {index}: {str(e)}"
                        logger.error(error_msg)
                        stats['errors'].append(error_msg)
                        stats['rows_skipped'] += 1
                        continue

                # Bulk upsert BSE schemes using PostgreSQL
                if scheme_records:
                    from sqlalchemy.dialects.postgresql import insert

                    stmt = insert(BSEScheme.__table__).values(scheme_records)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=['unique_no'],
                        set_=dict(
                            scheme_code=stmt.excluded.scheme_code,
                            rta_scheme_code=stmt.excluded.rta_scheme_code,
                            amc_scheme_code=stmt.excluded.amc_scheme_code,
                            isin=stmt.excluded.isin,
                            amc_code=stmt.excluded.amc_code,
                            scheme_type=stmt.excluded.scheme_type,
                            scheme_plan=stmt.excluded.scheme_plan,
                            scheme_name=stmt.excluded.scheme_name,
                            purchase_allowed=stmt.excluded.purchase_allowed,
                            purchase_transaction_mode=stmt.excluded.
                            purchase_transaction_mode,
                            minimum_purchase_amount=stmt.excluded.
                            minimum_purchase_amount,
                            additional_purchase_amount=stmt.excluded.
                            additional_purchase_amount,
                            maximum_purchase_amount=stmt.excluded.
                            maximum_purchase_amount,
                            purchase_amount_multiplier=stmt.excluded.
                            purchase_amount_multiplier,
                            purchase_cutoff_time=stmt.excluded.
                            purchase_cutoff_time,
                            redemption_allowed=stmt.excluded.
                            redemption_allowed,
                            redemption_transaction_mode=stmt.excluded.
                            redemption_transaction_mode,
                            minimum_redemption_qty=stmt.excluded.
                            minimum_redemption_qty,
                            redemption_qty_multiplier=stmt.excluded.
                            redemption_qty_multiplier,
                            maximum_redemption_qty=stmt.excluded.
                            maximum_redemption_qty,
                            redemption_amount_minimum=stmt.excluded.
                            redemption_amount_minimum,
                            redemption_amount_maximum=stmt.excluded.
                            redemption_amount_maximum,
                            redemption_amount_multiple=stmt.excluded.
                            redemption_amount_multiple,
                            redemption_cutoff_time=stmt.excluded.
                            redemption_cutoff_time,
                            rta_agent_code=stmt.excluded.rta_agent_code,
                            amc_active_flag=stmt.excluded.amc_active_flag,
                            dividend_reinvestment_flag=stmt.excluded.
                            dividend_reinvestment_flag,
                            sip_flag=stmt.excluded.sip_flag,
                            stp_flag=stmt.excluded.stp_flag,
                            swp_flag=stmt.excluded.swp_flag,
                            switch_flag=stmt.excluded.switch_flag,
                            settlement_type=stmt.excluded.settlement_type,
                            amc_ind=stmt.excluded.amc_ind,
                            face_value=stmt.excluded.face_value,
                            start_date=stmt.excluded.start_date,
                            end_date=stmt.excluded.end_date,
                            reopening_date=stmt.excluded.reopening_date,
                            exit_load_flag=stmt.excluded.exit_load_flag,
                            exit_load=stmt.excluded.exit_load,
                            lockin_period_flag=stmt.excluded.
                            lockin_period_flag,
                            lockin_period=stmt.excluded.lockin_period,
                            channel_partner_code=stmt.excluded.
                            channel_partner_code))
                    db.session.execute(stmt)
                    stats['schemes_created'] += len(scheme_records)

                # Commit batch
                db.session.commit()

            logger.info(f"BSE scheme import completed: {stats}")

            return stats

        except Exception as e:
            db.session.rollback()
            logger.error(f"Error importing BSE scheme data: {e}")
            raise
