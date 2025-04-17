import os
import logging
from flask import render_template
from setup_db import create_app, db
from fund_api import init_fund_api

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def init_app():
    """Initialize the Flask application with our components"""
    # Create Flask application
    app = create_app()
    
    # Create database tables
    with app.app_context():
        # Import models after app context is established
        from new_models_updated import Fund, FundFactSheet, FundReturns, PortfolioHolding, NavHistory
        
        # Instead of dropping tables, we'll try to create them if they don't exist
        # This allows us to work with existing tables that might have dependencies
        try:
            db.create_all()
            logger.info("Database tables created successfully")
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            # If there's an error, we'll attempt to handle it without dropping tables
    
    # Register API routes
    app = init_fund_api(app)
    logger.info("API routes registered successfully")
    
    return app

# Create Flask application
app = init_app()

@app.route('/')
def index():
    """Homepage route"""
    return render_template('api_test.html')

if __name__ == '__main__':
    # Run the Flask application
    app.run(host='0.0.0.0', port=5000, debug=True)