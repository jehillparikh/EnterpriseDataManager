import os
import logging
from flask import Flask, render_template, send_file, Response
from setup_db import create_app, db
from fund_api import init_fund_api
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
            db.create_all()
            logger.info("Database tables created successfully")
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
    
    # Register API routes
    app = init_fund_api(app)
    logger.info("API routes registered successfully")
    
    return app

# Create Flask application
app = init_app()

@app.route('/')
def index():
    """Homepage route"""
    return "<h1>Mutual Fund API</h1><p>API is running. Use /api/funds to access fund data.</p>"


if __name__ == '__main__':
    # Run the Flask application
    app.run(host=config.HOST, port=config.PORT, debug=config.DEBUG)