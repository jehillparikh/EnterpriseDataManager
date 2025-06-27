import os
import logging
from flask import Flask, render_template, send_file, Response
from setup_db import create_app, db
from fund_api import init_fund_api
from upload_handler import init_upload_routes
import config

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
        from models import Fund, FundFactSheet, FundReturns, FundHolding, NavHistory
        
        try:
            # Create all tables
            db.create_all()
            logger.info("Database tables created successfully")
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
    
    # Register API routes
    app = init_fund_api(app)
    logger.info("API routes registered successfully")
    
    # Register upload routes
    init_upload_routes(app)
    logger.info("Upload routes registered successfully")
    
    return app

# Create Flask application
app = init_app()

@app.route('/')
def index():
    """Homepage route"""
    return render_template('main_dashboard.html')


if __name__ == '__main__':
    # Run the Flask application
    app.run(host=config.HOST, port=config.PORT, debug=config.DEBUG)