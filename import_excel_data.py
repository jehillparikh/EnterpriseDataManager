import os
import pandas as pd
import logging
from datetime import datetime
from sqlalchemy.exc import SQLAlchemyError
from setup_db import create_app, db
from new_models_updated import Fund, FundFactSheet, FundReturns, PortfolioHolding, NavHistory
from sqlalchemy import create_engine
import math

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


            def safe_float(val):
                # treat None, non-numbers, or NaN all as 0.0
                if val is None or (isinstance(val, float) and math.isnan(val)):
                    return 0.0
                try:
                    return float(val)
                except (ValueError, TypeError):
                    return 0.0
            
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
                """
                
                
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
                """
                # Create PortfolioHolding
                holding = PortfolioHolding(
                    isin=fund.isin,  # Use the fund ISIN, not the instrument ISIN
                    instrument_isin=row['ISIN'],
                    coupon=safe_float(row.get('Coupon (%)')),
                    instrument_name=row.get('Name Of the Instrument'),
                    sector=row.get('Sector'),
                    quantity=safe_float(row.get('Quantity')),
                    value=safe_float(row.get('Value')),
                    percentage_to_nav=safe_float(row.get('% to NAV')),
                    yield_value=safe_float(row.get('Yield')),
                    instrument_type=row.get('Type')
                )
                
                db.session.add(holding)
                
                count += 1
                
                # Commit every 100 records to avoid large transactions
                if count % 200 == 0:
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




    def import_portfolio_data_batch(excel_path: str, batch_size: int = 500):
        """
        Batch-import portfolio holdings from Excel into PortfolioHolding table.
        Inserts new rows and updates existing, keyed by (isin, instrument_isin). TODO not test
        """
        # 1) Read entire sheet
        df = pd.read_excel(excel_path, keep_default_na=False)

        # 2) Normalize column names to match your model attributes
        df = df.rename(columns={
            'ISIN': 'instrument_isin',
            'Fund ISIN': 'isin',               # if you have Fund ISIN column
            'Fund Name': 'fund_name',          # optional, for logging
            'Coupon (%)': 'coupon',
            'Name Of the Instrument': 'instrument_name',
            'Sector': 'sector',
            'Quantity': 'quantity',
            'Value': 'value',
            '% to NAV': 'percentage_to_nav',
            'Yield': 'yield_value',
            'Type': 'instrument_type',
        })

        total_inserts = 0
        total_updates = 0

        engine = db.engine  # or create_engine(db_url)
        n = len(df)
        chunks = math.ceil(n / batch_size)

        for chunk_idx in range(chunks):
            start = chunk_idx * batch_size
            end = start + batch_size
            sub = df.iloc[start:end]

            # --- Fetch Funds in one query by isin ---
            isins = sub['isin'].dropna().unique().tolist()
            funds = Fund.query.filter(Fund.isin.in_(isins)).all()
            fund_map = {f.isin: f for f in funds}

            # --- Fetch existing holdings in one query by (isin, instrument_isin) ---
            keys = sub.apply(lambda r: (r['isin'], r['instrument_isin']), axis=1).unique().tolist()
            existing = PortfolioHolding.query.filter(
                tuple_(PortfolioHolding.isin, PortfolioHolding.instrument_isin).in_(keys)
            ).all()
            exist_map = {(h.isin, h.instrument_isin): h.id for h in existing}

            inserts = []
            updates = []

            # --- Prepare mappings ---
            for row in sub.to_dict(orient='records'):
                isin = row['isin']
                instr = row['instrument_isin']
                if isin not in fund_map:
                    # skip if fund not found
                    continue

                pk = exist_map.get((isin, instr))
                mapping = {
                    'isin': isin,
                    'instrument_isin': instr,
                    'coupon': _safe_float(row['coupon']),
                    'instrument_name': row['instrument_name'],
                    'sector': row['sector'],
                    'quantity': _safe_float(row['quantity']),
                    'value': _safe_float(row['value']),
                    'percentage_to_nav': _safe_float(row['percentage_to_nav']),
                    'yield_value': _safe_float(row['yield_value']),
                    'instrument_type': row['instrument_type'],
                }

                if pk:
                    # existing → update mapping must include primary key
                    mapping['id'] = pk
                    updates.append(mapping)
                else:
                    # new → insert mapping
                    inserts.append(mapping)

            # --- Bulk write ---
            with Session(engine) as sess:
                if inserts:
                    sess.bulk_insert_mappings(PortfolioHolding, inserts)
                    total_inserts += len(inserts)
                if updates:
                    sess.bulk_update_mappings(PortfolioHolding, updates)
                    total_updates += len(updates)
                sess.commit()

        print(f"Done: {total_inserts} inserted, {total_updates} updated.")
        return total_inserts, total_updates

    def safe_float(x):
        try:
            return float(x)
        except Exception:
            return 0

    def import_nav_data_pandas(self,table_name='nav_history'):
        """
        Efficiently import NAV data from Excel using pandas and SQLAlchemy.

        Args:
            nav_path (str): Path to the Excel file.
            db_uri (str): SQLAlchemy-compatible database URI.
            table_name (str): Name of the table to write to.
        """
        try:
            # Read Excel file in chunks
            batch_size = 1000
            engine = create_engine(db.engine.url)
            print(f"✅ Starting NAV import from {self.nav_path} into `{table_name}`")
            print(f"✅ Database URI: {db.engine.url}")

            df = pd.read_excel(self.nav_path)
            batch_size = 1000
            total_rows=0
            for i in range(0, len(df), batch_size):
                chunk = df.iloc[i:i+batch_size]
            
                # Optional: clean/transform the chunk
                chunk = chunk.dropna(subset=['ISIN', 'Date', 'Net Asset Value'])
                chunk['Date'] = pd.to_datetime(chunk['Date']).dt.date

                # Rename columns if necessary to match DB schema
                chunk = chunk.rename(columns={
                    'ISIN': 'isin',
                    'Net Asset Value': 'nav',
                    'Scheme Name': 'scheme_name',
                    'Date': 'date'
                })

                # Bulk insert
                chunk.to_sql(name=table_name, con=engine, if_exists='append', index=False, method='multi')
                total_rows += len(chunk)

                print(f"Inserted {len(chunk)} records...")

            print(f"✅ Finished importing {total_rows} records into `{table_name}`")

        except Exception as e:
            print(f"❌ Error during NAV import: {e}")
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

    def import_or_update_nav_data(self):
        """
        Import NAV history data from Excel.
        - Inserts new NAV entries if not present.
        - Updates existing NAV values if ISIN + Date match.
        """
        try:


            df = pd.read_excel(self.nav_path)
            batch_size = 1000
            total_processed=0
            for i in range(0, len(df), batch_size):
                chunk = df.iloc[i:i+batch_size]
                for _, row in chunk.iterrows():
                    isin = row.get("ISIN")
                    nav_value = row.get("Net Asset Value")
                    scheme_name = row.get("Scheme Name")
                    nav_date = row.get("Date")

                    if pd.isna(isin) or pd.isna(nav_value) or pd.isna(nav_date):
                        continue

                    # Parse date
                    if isinstance(nav_date, str):
                        try:
                            nav_date = datetime.strptime(nav_date, "%Y-%m-%d").date()
                        except ValueError:
                            logger.warning(f"Skipping invalid date: {nav_date}")
                            continue
                    elif hasattr(nav_date, "date"):
                        nav_date = nav_date.date()

                    # Find fund by ISIN or scheme name
                    fund = Fund.query.filter_by(isin=isin).first()
                    if not fund and scheme_name:
                        fund = Fund.query.filter(Fund.scheme_name.ilike(f"%{scheme_name}%")).first()

                    if not fund:
                        logger.warning(f"Fund not found for ISIN: {isin} or Scheme: {scheme_name}")
                        continue

                    # Check existing NAV entry
                    nav_entry = NavHistory.query.filter_by(isin=fund.isin, date=nav_date).first()

                    if nav_entry:
                        # Update NAV if changed
                        if nav_entry.nav != nav_value:
                            nav_entry.nav = nav_value
                    else:
                        # Insert new NAV entry
                        nav_entry = NavHistory(
                            isin=fund.isin,
                            date=nav_date,
                            nav=nav_value
                        )
                        db.session.add(nav_entry)

                    total_processed += 1

                db.session.commit()
                logger.info(f"Processed batch {i+1}, total records updated/inserted so far: {total_processed}")

            logger.info(f"✅ NAV import complete. Total records processed: {total_processed}")
            return total_processed

        except Exception as e:
            db.session.rollback()
            logger.error(f"❌ Error during NAV import: {e}")
            raise
    
    def import_all(self):
        """Import all data from Excel files"""
        try:
            # Import in the correct order: first funds, then related data
            factsheet_count = self.import_factsheet_data()
            returns_count = self.import_returns_data()
            portfolio_count = self.import_portfolio_data()
            nav_count = self.import_nav_data_pandas()
            
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