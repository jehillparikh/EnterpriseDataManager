import pandas as pd
import os
import logging
from datetime import datetime, timedelta
from setup_db import create_app, db
from new_models_updated import Fund, PortfolioHolding, NavHistory

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = create_app()

def import_test_holdings():
    """Import sample portfolio holdings for the test fund"""
    logger.info("Creating sample portfolio holdings for test fund...")
    
    with app.app_context():
        # Check if test fund exists
        test_isin = "TEST00000001"
        fund = Fund.query.filter_by(isin=test_isin).first()
        
        if not fund:
            logger.error(f"Test fund {test_isin} does not exist")
            return False
        
        # Delete existing holdings if any
        existing = PortfolioHolding.query.filter_by(isin=test_isin).all()
        if existing:
            for holding in existing:
                db.session.delete(holding)
            db.session.commit()
            logger.info(f"Deleted {len(existing)} existing holdings for test fund")
        
        # Create sample holdings
        holdings = [
            PortfolioHolding(
                isin=test_isin,
                instrument_name="HDFC Bank Ltd",
                sector="Financial Services",
                quantity=12500,
                value=2500000.0,
                percentage_to_nav=12.5,
                instrument_type="Equity"
            ),
            PortfolioHolding(
                isin=test_isin,
                instrument_name="Reliance Industries Ltd",
                sector="Energy",
                quantity=8000,
                value=2000000.0,
                percentage_to_nav=10.0,
                instrument_type="Equity"
            ),
            PortfolioHolding(
                isin=test_isin,
                instrument_name="Infosys Ltd",
                sector="Information Technology",
                quantity=15000,
                value=1800000.0,
                percentage_to_nav=9.0,
                instrument_type="Equity"
            ),
            PortfolioHolding(
                isin=test_isin,
                instrument_name="TCS Ltd",
                sector="Information Technology",
                quantity=6000,
                value=1500000.0,
                percentage_to_nav=7.5,
                instrument_type="Equity"
            ),
            PortfolioHolding(
                isin=test_isin,
                instrument_name="Government of India 6.54% 2032",
                sector="Government Securities",
                coupon=6.54,
                value=1200000.0,
                percentage_to_nav=6.0,
                yield_value=6.2,
                instrument_type="Debt"
            ),
            PortfolioHolding(
                isin=test_isin,
                instrument_name="NABARD 7.35% 2026",
                sector="Financial Services",
                coupon=7.35,
                value=800000.0,
                percentage_to_nav=4.0,
                yield_value=7.1,
                instrument_type="Debt"
            ),
            PortfolioHolding(
                isin=test_isin,
                instrument_name="Cash & Cash Equivalents",
                sector="Cash",
                value=2000000.0,
                percentage_to_nav=10.0,
                instrument_type="Cash"
            )
        ]
        
        for holding in holdings:
            db.session.add(holding)
        
        db.session.commit()
        logger.info(f"Created {len(holdings)} sample holdings for test fund")
        
        return True

def import_test_nav_history():
    """Import sample NAV history for the test fund"""
    logger.info("Creating sample NAV history for test fund...")
    
    with app.app_context():
        # Check if test fund exists
        test_isin = "TEST00000001"
        fund = Fund.query.filter_by(isin=test_isin).first()
        
        if not fund:
            logger.error(f"Test fund {test_isin} does not exist")
            return False
        
        # Delete existing NAV history if any
        existing = NavHistory.query.filter_by(isin=test_isin).all()
        if existing:
            for nav in existing:
                db.session.delete(nav)
            db.session.commit()
            logger.info(f"Deleted {len(existing)} existing NAV records for test fund")
        
        # Create sample NAV history - 365 days
        base_nav = 100.0
        base_date = datetime.now().date() - timedelta(days=365)
        
        nav_records = []
        for day_offset in range(366):
            # Skip weekends
            curr_date = base_date + timedelta(days=day_offset)
            if curr_date.weekday() >= 5:  # 5 is Saturday, 6 is Sunday
                continue
                
            # Calculate a NAV with some randomness and slight upward trend
            day_factor = 1.0 + (day_offset * 0.0001)  # Slight upward trend
            nav_value = base_nav * day_factor
            
            # Add oscillation to make it look realistic
            if day_offset % 7 < 3:
                nav_value += (nav_value * 0.005)  # +0.5%
            else:
                nav_value -= (nav_value * 0.003)  # -0.3%
            
            nav_records.append(
                NavHistory(
                    isin=test_isin,
                    date=curr_date,
                    nav=round(nav_value, 4)
                )
            )
        
        for nav in nav_records:
            db.session.add(nav)
        
        db.session.commit()
        logger.info(f"Created {len(nav_records)} sample NAV records for test fund")
        
        return True

def main():
    """Run both imports for test data"""
    holdings_success = import_test_holdings()
    nav_success = import_test_nav_history()
    
    if holdings_success and nav_success:
        logger.info("Test data imported successfully")
    else:
        logger.warning("Some test data imports failed - check logs for details")

if __name__ == "__main__":
    main()