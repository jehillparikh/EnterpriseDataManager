import os
import logging
from setup_db import create_app, db
from sqlalchemy import inspect, text

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = create_app()

def check_tables():
    """Check tables in database"""
    with app.app_context():
        # Get inspector
        inspector = inspect(db.engine)
        
        # Get tables
        tables = inspector.get_table_names()
        logger.info(f"Tables in database: {tables}")
        
        # Check for fund-related tables
        fund_tables = [t for t in tables if 'fund' in t.lower()]
        logger.info(f"Fund-related tables: {fund_tables}")
        
        # Check if any required new tables exist
        required_tables = ['mf_fund', 'mf_factsheet', 'mf_returns', 'mf_portfolio_holdings', 'mf_nav_history']
        logger.info(f"Required tables: {required_tables}")
        for table in required_tables:
            if table in tables:
                logger.info(f"Table {table} exists")
                # Get columns for this table
                columns = inspector.get_columns(table)
                column_names = [c['name'] for c in columns]
                logger.info(f"Columns in {table}: {column_names}")
            else:
                logger.warning(f"Table {table} does not exist")
                
        # Check foreign key constraints
        for table in tables:
            fks = inspector.get_foreign_keys(table)
            if fks:
                logger.info(f"Foreign keys in {table}: {fks}")

def execute_sql(sql):
    """Execute SQL statement"""
    with app.app_context():
        result = db.session.execute(text(sql))
        if sql.lower().startswith('select'):
            # Fetchall for SELECT statements
            return result.fetchall()
        else:
            # Commit for other statements
            db.session.commit()
            return None

if __name__ == "__main__":
    check_tables()