#!/usr/bin/env python3
"""
Quick script to add a single test fund to the database.
"""

from setup_db import create_app, db
from new_models_updated import Fund, FundFactSheet

def add_test_fund():
    """Add a test fund to the database"""
    app = create_app()
    
    with app.app_context():
        # Create a test fund
        test_fund = Fund(
            isin='INF200K01MA5',
            scheme_name='Test Equity Fund',
            fund_type='Equity',
            fund_subtype='Large Cap',
            amc_name='TestAMC'
        )
        
        # Create a test factsheet
        test_factsheet = FundFactSheet(
            isin='INF200K01MA5',
            fund_manager='Test Manager',
            aum=1000.0,
            expense_ratio=1.2,
            exit_load='1% if redeemed within 1 year'
        )
        
        # Add to session
        db.session.add(test_fund)
        db.session.add(test_factsheet)
        
        # Commit transaction
        db.session.commit()
        
        print(f"Successfully added test fund with ISIN {test_fund.isin}")
        
        # Verify it was added
        funds = Fund.query.all()
        print(f"Total funds in database: {len(funds)}")
        for fund in funds:
            print(f"Fund: {fund.isin} - {fund.scheme_name}")

if __name__ == "__main__":
    add_test_fund()