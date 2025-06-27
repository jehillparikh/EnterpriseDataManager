import os
import logging
from flask import Flask, render_template, send_file, Response
from setup_db import create_app, db
from fund_api import init_fund_api
from temp_upload_handler import init_upload_routes
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
        from new_models_updated import Fund, FundFactSheet, FundReturns, FundHolding, NavHistory
        
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

@app.route('/docs')
def documentation():
    """Documentation page route"""
    return render_template('docs.html')

@app.route('/readme-content')
def readme_content():
    """Serve the README.md content"""
    try:
        with open('README.md', 'r') as file:
            content = file.read()
        return Response(content, mimetype='text/plain')
    except Exception as e:
        logger.error(f"Error reading README.md: {e}")
        return Response("# Error\nCould not load API documentation.", mimetype='text/plain')

if __name__ == '__main__':
    # Run the Flask application
    app.run(host=config.HOST, port=config.PORT, debug=config.DEBUG)