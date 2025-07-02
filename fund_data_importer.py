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
                            continue

                        # Extract fund data - using new column structure
                        scheme_name = str(row.get('Scheme Name', '')).strip()
                        fund_type = str(row.get('Scheme Type', row.get('Fund Type', row.get('Type', '')))).strip()
                        fund_subtype = str(row.get('Sub Category', row.get('Fund Sub Type', row.get('Subtype', '')))).strip() if not pd.isna(row.get('Sub Category', row.get('Fund Sub Type', row.get('Subtype')))) else None
                        amc_name = str(row.get('AMC', row.get('AMC Name', ''))).strip()

                        fund_record = {
                            'isin': isin,
                            'scheme_name': scheme_name,
                            'fund_type': fund_type,
                            'fund_subtype': fund_subtype,
                            'amc_name': amc_name
                        }
                        fund_records.append(fund_record)

                        # Extract enhanced factsheet data - supporting new column structure
                        # Core fund information
                        factsheet_scheme_name = str(row.get('Scheme Name', '')).strip() if not pd.isna(row.get('Scheme Name')) else None
                        scheme_type = str(row.get('Scheme Type', '')).strip() if not pd.isna(row.get('Scheme Type')) else None
                        sub_category = str(row.get('Sub Category', '')).strip() if not pd.isna(row.get('Sub Category')) else None
                        plan = str(row.get('Plan', '')).strip() if not pd.isna(row.get('Plan')) else None
                        amc = str(row.get('AMC', '')).strip() if not pd.isna(row.get('AMC')) else None
                        
                        # Financial details
                        expense_ratio = float(row.get('Expense Ratio', 0)) if not pd.isna(row.get('Expense Ratio')) else None
                        minimum_lumpsum = float(row.get('Minimum Lumpsum', 0)) if not pd.isna(row.get('Minimum Lumpsum')) else None
                        minimum_sip = float(row.get('Minimum SIP', 0)) if not pd.isna(row.get('Minimum SIP')) else None
                        
                        # Investment terms
                        lock_in = str(row.get('Lock-in', '')).strip() if not pd.isna(row.get('Lock-in')) else None
                        exit_load = str(row.get('Exit Load', '')).strip() if not pd.isna(row.get('Exit Load')) else None
                        
                        # Management and risk
                        fund_manager = str(row.get('Fund Manager', '')).strip() if not pd.isna(row.get('Fund Manager')) else None
                        benchmark = str(row.get('Benchmark', '')).strip() if not pd.isna(row.get('Benchmark')) else None
                        sebi_risk_category = str(row.get('SEBI Risk Category', '')).strip() if not pd.isna(row.get('SEBI Risk Category')) else None
                        
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
                            sebi_risk_category=stmt.excluded.sebi_risk_category,
                            # Legacy fields
                            launch_date=stmt.excluded.launch_date,
                            last_updated=stmt.excluded.last_updated
                        ))
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

            # Process each row
            for _, row in df.iterrows():
                isin = str(row['ISIN']).strip()

                if not isin or isin.lower() == 'nan':
                    continue

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
                    returns = FundReturns()
                    returns.isin = isin
                    returns.return_1m = float(row.get(
                        '1M Return',
                        0)) if not pd.isna(row.get('1M Return')) else None
                    returns.return_3m = float(row.get(
                        '3M Return',
                        0)) if not pd.isna(row.get('3M Return')) else None
                    returns.return_6m = float(row.get(
                        '6M Return',
                        0)) if not pd.isna(row.get('6M Return')) else None
                    returns.return_ytd = float(row.get(
                        'YTD Return',
                        0)) if not pd.isna(row.get('YTD Return')) else None
                    returns.return_1y = float(row.get(
                        '1Y Return',
                        0)) if not pd.isna(row.get('1Y Return')) else None
                    returns.return_3y = float(row.get(
                        '3Y Return',
                        0)) if not pd.isna(row.get('3Y Return')) else None
                    returns.return_5y = float(row.get(
                        '5Y Return',
                        0)) if not pd.isna(row.get('5Y Return')) else None
                    db.session.add(returns)
                    stats['returns_created'] += 1
                else:
                    # Update existing returns record
                    returns.return_1m = float(
                        row.get('1M Return', 0)) if not pd.isna(
                            row.get('1M Return')) else returns.return_1m
                    returns.return_3m = float(
                        row.get('3M Return', 0)) if not pd.isna(
                            row.get('3M Return')) else returns.return_3m
                    returns.return_6m = float(
                        row.get('6M Return', 0)) if not pd.isna(
                            row.get('6M Return')) else returns.return_6m
                    returns.return_ytd = float(
                        row.get('YTD Return', 0)) if not pd.isna(
                            row.get('YTD Return')) else returns.return_ytd
                    returns.return_1y = float(
                        row.get('1Y Return', 0)) if not pd.isna(
                            row.get('1Y Return')) else returns.return_1y
                    returns.return_3y = float(
                        row.get('3Y Return', 0)) if not pd.isna(
                            row.get('3Y Return')) else returns.return_3y
                    returns.return_5y = float(
                        row.get('5Y Return', 0)) if not pd.isna(
                            row.get('5Y Return')) else returns.return_5y
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
                    if (not scheme_isin or scheme_isin.lower() == 'nan'
                            or scheme_isin == '' or scheme_isin == '-'
                            or scheme_isin == 'None'
                            or pd.isna(row.get('Scheme ISIN'))
                            or len(scheme_isin)
                            < 8):  # ISIN should be at least 8 characters
                        logger.warning(
                            f"Skipping row with invalid Scheme ISIN: '{scheme_isin}'"
                        )
                        stats['rows_skipped_invalid_isin'] += 1
                        continue

                    # Check if the fund exists in database
                    fund_exists = Fund.query.filter_by(
                        isin=scheme_isin).first()
                    if not fund_exists:
                        logger.warning(
                            f"Skipping holding for non-existent fund ISIN: '{scheme_isin}'"
                        )
                        stats['rows_skipped_no_fund'] += 1
                        continue

                    holding = FundHolding()
                    holding.isin = scheme_isin
                    holding.instrument_isin = str(row.get(
                        'ISIN', '')) if not pd.isna(row.get('ISIN')) else None
                    holding.instrument_name = str(
                        row.get('Name of Instrument', ''))
                    holding.sector = str(row.get(
                        'Industry',
                        '')) if not pd.isna(row.get('Industry')) else None
                    holding.quantity = float(row.get(
                        'Quantity',
                        0)) if not pd.isna(row.get('Quantity')) else None
                    holding.value = float(row.get(
                        'Market Value',
                        0)) if not pd.isna(row.get('Market Value')) else None
                    holding.percentage_to_nav = float(
                        row.get('% to Net Assets', 0)) if not pd.isna(
                            row.get('% to Net Assets')) else 0
                    holding.yield_value = float(row.get(
                        'Yield', 0)) if not pd.isna(row.get('Yield')) else None
                    holding.instrument_type = str(row.get('Type', ''))
                    holding.coupon = float(
                        row.get('Coupon',
                                0)) if not pd.isna(row.get('Coupon')) else None
                    holding.amc_name = str(row.get(
                        'AMC', '')) if not pd.isna(row.get('AMC')) else None
                    holding.scheme_name = str(row.get(
                        'Scheme Name',
                        '')) if not pd.isna(row.get('Scheme Name')) else None

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

                    nav_value = float(row.get('NAV', 0)) if pd.notna(
                        row.get('NAV')) else None
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
                        logger.info(
                            f"Committed batch of {batch_size} NAV records")

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

    def import_bse_scheme_data(self,
                               df,
                               clear_existing=False,
                               batch_size=1000):
        """
        Import BSE scheme data from DataFrame
        
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

            # Process each row
            for index, row in df.iterrows():
                try:
                    # Check if required fields are present
                    unique_no = row.get('Unique No')
                    scheme_code = row.get('Scheme Code')
                    isin = row.get('ISIN')

                    if pd.isna(unique_no) or pd.isna(scheme_code) or pd.isna(
                            isin):
                        stats['rows_skipped'] += 1
                        logger.warning(
                            f"Skipping row {index}: Missing required fields")
                        continue

                    # Check if scheme already exists
                    existing_scheme = BSEScheme.query.filter_by(
                        unique_no=int(unique_no)).first()

                    if existing_scheme:
                        scheme = existing_scheme
                        is_update = True
                    else:
                        scheme = BSEScheme()
                        is_update = False

                    # Set all fields
                    scheme.unique_no = int(unique_no)
                    scheme.scheme_code = str(row.get('Scheme Code', ''))
                    scheme.rta_scheme_code = str(row.get(
                        'RTA Scheme Code', ''))
                    scheme.amc_scheme_code = str(row.get(
                        'AMC Scheme Code', ''))
                    scheme.isin = str(row.get('ISIN', ''))
                    scheme.amc_code = str(row.get('AMC Code', ''))
                    scheme.scheme_type = str(row.get('Scheme Type', ''))
                    scheme.scheme_plan = str(row.get('Scheme Plan', ''))
                    scheme.scheme_name = str(row.get('Scheme Name', ''))

                    # Purchase parameters
                    scheme.purchase_allowed = str(
                        row.get('Purchase Allowed', 'N'))
                    scheme.purchase_transaction_mode = str(
                        row.get('Purchase Transaction mode', ''))
                    scheme.minimum_purchase_amount = float(
                        row.get('Minimum Purchase Amount', 0))
                    scheme.additional_purchase_amount = float(
                        row.get('Additional Purchase Amount', 0))
                    scheme.maximum_purchase_amount = float(
                        row.get('Maximum Purchase Amount', 0))
                    scheme.purchase_amount_multiplier = float(
                        row.get('Purchase Amount Multiplier', 0))
                    scheme.purchase_cutoff_time = str(
                        row.get('Purchase Cutoff Time', ''))

                    # Redemption parameters
                    scheme.redemption_allowed = str(
                        row.get('Redemption Allowed', 'N'))
                    scheme.redemption_transaction_mode = str(
                        row.get('Redemption Transaction Mode', ''))
                    scheme.minimum_redemption_qty = float(
                        row.get('Minimum Redemption Qty', 0))
                    scheme.redemption_qty_multiplier = float(
                        row.get('Redemption Qty Multiplier', 0))
                    scheme.maximum_redemption_qty = float(
                        row.get('Maximum Redemption Qty', 0))
                    scheme.redemption_amount_minimum = float(
                        row.get('Redemption Amount - Minimum', 0))
                    scheme.redemption_amount_maximum = float(
                        row.get('Redemption Amount – Maximum', 0))
                    scheme.redemption_amount_multiple = float(
                        row.get('Redemption Amount Multiple', 0))
                    scheme.redemption_cutoff_time = str(
                        row.get('Redemption Cut off Time', ''))

                    # Operational details
                    scheme.rta_agent_code = str(row.get('RTA Agent Code', ''))
                    scheme.amc_active_flag = int(row.get('AMC Active Flag', 0))
                    scheme.dividend_reinvestment_flag = str(
                        row.get('Dividend Reinvestment Flag', 'N'))

                    # Transaction flags
                    scheme.sip_flag = str(row.get('SIP FLAG', 'N'))
                    scheme.stp_flag = str(row.get('STP FLAG', 'N'))
                    scheme.swp_flag = str(row.get('SWP Flag', 'N'))
                    scheme.switch_flag = str(row.get('Switch FLAG', 'N'))

                    # Settlement and operational parameters
                    scheme.settlement_type = str(row.get(
                        'SETTLEMENT TYPE', ''))
                    amc_ind_val = row.get('AMC_IND')
                    scheme.amc_ind = float(amc_ind_val) if pd.notna(
                        amc_ind_val) else None
                    scheme.face_value = float(row.get('Face Value', 0))

                    # Date fields
                    scheme.start_date = self._parse_date(row.get('Start Date'))
                    scheme.end_date = self._parse_date(row.get('End Date'))
                    reopening_date = row.get('ReOpening Date')
                    scheme.reopening_date = self._parse_date(
                        reopening_date) if pd.notna(reopening_date) else None

                    # Exit load and lock-in details
                    exit_load_flag = row.get('Exit Load Flag')
                    scheme.exit_load_flag = str(exit_load_flag) if pd.notna(
                        exit_load_flag) else None
                    scheme.exit_load = str(row.get('Exit Load', ''))
                    lockin_flag = row.get('Lock-in Period Flag')
                    scheme.lockin_period_flag = str(lockin_flag) if pd.notna(
                        lockin_flag) else None
                    lockin_period = row.get('Lock-in Period')
                    scheme.lockin_period = float(lockin_period) if pd.notna(
                        lockin_period) else None

                    # Channel and distribution
                    scheme.channel_partner_code = str(
                        row.get('Channel Partner Code', ''))

                    if not is_update:
                        db.session.add(scheme)
                        stats['schemes_created'] += 1
                    else:
                        stats['schemes_updated'] += 1

                    batch_count += 1

                    # Commit in batches
                    if batch_count >= batch_size:
                        db.session.commit()
                        batch_count = 0
                        logger.info(
                            f"Committed batch of {batch_size} BSE scheme records"
                        )

                except Exception as e:
                    error_msg = f"Error processing row {index}: {str(e)}"
                    logger.error(error_msg)
                    stats['errors'].append(error_msg)
                    stats['rows_skipped'] += 1
                    continue

            # Commit remaining records
            if batch_count > 0:
                db.session.commit()

            logger.info(f"BSE scheme import completed: {stats}")

            return stats

        except Exception as e:
            db.session.rollback()
            logger.error(f"Error importing BSE scheme data: {e}")
            raise
