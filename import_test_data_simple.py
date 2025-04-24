"""
Simple script to import test data for the Mutual Fund API
This script creates a single test fund with all related data
"""

import os
import datetime
from app_updated import app
from setup_db import db
from new_models_updated import Fund, FundFactSheet, FundReturns, PortfolioHolding, NavHistory

def create_test_fund():
    """Create a test fund with all related data"""
    print("Creating test fund...")
    
    # Check if the fund already exists
    test_fund = Fund.query.filter_by(isin="TEST00000001").first()
    if test_fund:
        print("Test fund already exists, skipping creation")
        return test_fund
    
    # Create a test fund
    test_fund = Fund(
        isin="TEST00000001",
        scheme_name="Test HDFC Balanced Advantage Fund",
        fund_type="Hybrid",
        fund_subtype="Balanced Advantage",
        amc_name="HDFC"
    )
    
    try:
        db.session.add(test_fund)
        db.session.commit()
        print(f"Created fund: {test_fund.scheme_name}")
        return test_fund
    except Exception as e:
        db.session.rollback()
        print(f"Error creating fund: {e}")
        return None

def create_test_factsheet(fund):
    """Create a test factsheet for the fund"""
    print("Creating test factsheet...")
    
    # Check if factsheet already exists
    factsheet = FundFactSheet.query.filter_by(isin=fund.isin).first()
    if factsheet:
        print("Test factsheet already exists, skipping creation")
        return factsheet
    
    # Create a test factsheet
    factsheet = FundFactSheet(
        isin=fund.isin,
        fund_manager="Test Manager",
        aum=10000.0,  # 10000 crores
        expense_ratio=0.01,  # 1%
        launch_date=datetime.date(2020, 1, 1),
        exit_load="1% for less than 1 year"
    )
    
    try:
        db.session.add(factsheet)
        db.session.commit()
        print(f"Created factsheet for {fund.scheme_name}")
        return factsheet
    except Exception as e:
        db.session.rollback()
        print(f"Error creating factsheet: {e}")
        return None

def create_test_returns(fund):
    """Create test returns for the fund"""
    print("Creating test returns...")
    
    # Check if returns already exist
    returns = FundReturns.query.filter_by(isin=fund.isin).first()
    if returns:
        print("Test returns already exist, skipping creation")
        return returns
    
    # Create test returns
    returns = FundReturns(
        isin=fund.isin,
        return_1m=1.5,
        return_3m=4.2,
        return_6m=8.7,
        return_ytd=5.3,
        return_1y=12.1,
        return_3y=36.5,
        return_5y=65.8
    )
    
    try:
        db.session.add(returns)
        db.session.commit()
        print(f"Created returns for {fund.scheme_name}")
        return returns
    except Exception as e:
        db.session.rollback()
        print(f"Error creating returns: {e}")
        return None

def create_test_holdings(fund):
    """Create test portfolio holdings for the fund"""
    print("Creating test holdings...")
    
    # Check if holdings already exist
    existing_holdings = PortfolioHolding.query.filter_by(isin=fund.isin).first()
    if existing_holdings:
        print("Test holdings already exist, skipping creation")
        return
    
    # Create test holdings
    holdings = [
        PortfolioHolding(
            isin=fund.isin,
            instrument_name="HDFC Bank Ltd",
            instrument_type="Equity",
            sector="Financial Services",
            percentage_to_nav=8.5,
            quantity=10000.0,
            value=2000000.0,
            instrument_isin="INE040A01034"
        ),
        PortfolioHolding(
            isin=fund.isin,
            instrument_name="Reliance Industries Ltd",
            instrument_type="Equity",
            sector="Energy",
            percentage_to_nav=7.2,
            quantity=8000.0,
            value=1800000.0,
            instrument_isin="INE002A01018"
        ),
        PortfolioHolding(
            isin=fund.isin,
            instrument_name="Infosys Ltd",
            instrument_type="Equity",
            sector="Information Technology",
            percentage_to_nav=6.8,
            quantity=7500.0,
            value=1700000.0,
            instrument_isin="INE009A01021"
        ),
        PortfolioHolding(
            isin=fund.isin,
            instrument_name="ICICI Bank Ltd",
            instrument_type="Equity",
            sector="Financial Services",
            percentage_to_nav=5.9,
            quantity=6500.0,
            value=1500000.0,
            instrument_isin="INE090A01021"
        ),
        PortfolioHolding(
            isin=fund.isin,
            instrument_name="Tata Consultancy Services Ltd",
            instrument_type="Equity",
            sector="Information Technology",
            percentage_to_nav=5.2,
            quantity=5500.0,
            value=1300000.0,
            instrument_isin="INE467B01029"
        ),
        PortfolioHolding(
            isin=fund.isin,
            instrument_name="Government Securities 7.26% 2032",
            instrument_type="Debt",
            sector="Government Securities",
            percentage_to_nav=10.0,
            coupon=7.26,
            yield_value=6.8,
            value=2500000.0,
            instrument_isin="IN0020320037"
        ),
        PortfolioHolding(
            isin=fund.isin,
            instrument_name="Cash and Cash Equivalents",
            instrument_type="Cash",
            sector="Cash",
            percentage_to_nav=10.0,
            value=2500000.0
        )
    ]
    
    try:
        for holding in holdings:
            db.session.add(holding)
        db.session.commit()
        print(f"Created {len(holdings)} portfolio holdings for {fund.scheme_name}")
    except Exception as e:
        db.session.rollback()
        print(f"Error creating holdings: {e}")

def create_test_nav_history(fund):
    """Create test NAV history for the fund"""
    print("Creating test NAV history...")
    
    # Check if NAV history already exists
    existing_nav = NavHistory.query.filter_by(isin=fund.isin).first()
    if existing_nav:
        print("Test NAV history already exists, skipping creation")
        return
    
    # Create 30 days of NAV history
    today = datetime.date.today()
    nav_value = 100.0
    
    nav_entries = []
    for i in range(30):
        date = today - datetime.timedelta(days=i)
        # Add some slight variation to NAV
        if i > 0:
            # Random daily change between -0.5% and +0.5%
            change = (((i * 13) % 10) - 5) / 1000
            nav_value = nav_value * (1 + change)
        
        nav_entries.append(
            NavHistory(
                isin=fund.isin,
                date=date,
                nav=round(nav_value, 4)
            )
        )
    
    try:
        for nav in nav_entries:
            db.session.add(nav)
        db.session.commit()
        print(f"Created {len(nav_entries)} NAV history entries for {fund.scheme_name}")
    except Exception as e:
        db.session.rollback()
        print(f"Error creating NAV history: {e}")

def main():
    """Main function to import all test data"""
    with app.app_context():
        # Create test fund
        fund = create_test_fund()
        if fund:
            # Create related data
            create_test_factsheet(fund)
            create_test_returns(fund)
            create_test_holdings(fund)
            create_test_nav_history(fund)
            print("Test data import completed successfully")
        else:
            print("Failed to create test fund, aborting")

if __name__ == "__main__":
    main()