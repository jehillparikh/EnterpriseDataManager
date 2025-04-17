import logging
from setup_db import create_app, db
from new_models_updated import Fund, FundFactSheet, FundReturns
from datetime import date

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = create_app()

def insert_test_fund():
    """Insert a single test fund with factsheet and returns data"""
    with app.app_context():
        try:
            # Check if test fund already exists
            test_isin = "TEST123456789"
            fund = Fund.query.filter_by(isin=test_isin).first()
            
            if not fund:
                # Create test fund
                fund = Fund(
                    isin=test_isin,
                    scheme_name="Test HDFC Balanced Advantage Fund",
                    fund_type="Hybrid",
                    fund_subtype="Balanced Advantage",
                    amc_name="HDFC"
                )
                db.session.add(fund)
                logger.info("Created test fund")
                
                # Create factsheet
                factsheet = FundFactSheet(
                    isin=test_isin,
                    fund_manager="Test Manager",
                    aum=10000.0,
                    expense_ratio=0.01,
                    launch_date=date(2020, 1, 1),
                    exit_load="1% for less than 1 year"
                )
                db.session.add(factsheet)
                logger.info("Created test factsheet")
                
                # Create returns
                returns = FundReturns(
                    isin=test_isin,
                    return_1m=1.5,
                    return_3m=4.2,
                    return_6m=8.7,
                    return_ytd=5.3,
                    return_1y=12.1,
                    return_3y=36.5,
                    return_5y=65.8
                )
                db.session.add(returns)
                logger.info("Created test returns")
                
                # Commit changes
                db.session.commit()
                logger.info("Test fund data committed to database")
            else:
                logger.info("Test fund already exists, skipping")
                
            return True
            
        except Exception as e:
            db.session.rollback()
            logger.error(f"Error creating test fund: {e}")
            return False

if __name__ == "__main__":
    insert_test_fund()