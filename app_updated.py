import os
import logging
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
    return """
    <html>
    <head>
        <title>Mutual Fund API</title>
        <link rel="stylesheet" href="https://cdn.replit.com/agent/bootstrap-agent-dark-theme.min.css">
    </head>
    <body class="container mt-5">
        <div class="row">
            <div class="col-md-8 offset-md-2">
                <div class="card">
                    <div class="card-header bg-primary text-white">
                        <h2>Mutual Fund API</h2>
                    </div>
                    <div class="card-body">
                        <h4 class="mb-4">Available Endpoints:</h4>
                        
                        <div class="list-group">
                            <a href="/api/funds" class="list-group-item list-group-item-action d-flex justify-content-between align-items-center">
                                GET /api/funds
                                <span class="badge bg-primary">List all funds</span>
                            </a>
                            <a href="/api/funds?amc_name=HDFC" class="list-group-item list-group-item-action d-flex justify-content-between align-items-center">
                                GET /api/funds?amc_name=HDFC
                                <span class="badge bg-primary">Filter by AMC</span>
                            </a>
                            <a href="/api/funds?fund_type=Equity" class="list-group-item list-group-item-action d-flex justify-content-between align-items-center">
                                GET /api/funds?fund_type=Equity
                                <span class="badge bg-primary">Filter by type</span>
                            </a>
                            <a href="/api/funds/INF179K01830" class="list-group-item list-group-item-action d-flex justify-content-between align-items-center">
                                GET /api/funds/{isin}
                                <span class="badge bg-primary">Fund details</span>
                            </a>
                            <a href="/api/funds/INF179K01830/factsheet" class="list-group-item list-group-item-action d-flex justify-content-between align-items-center">
                                GET /api/funds/{isin}/factsheet
                                <span class="badge bg-primary">Factsheet</span>
                            </a>
                            <a href="/api/funds/INF179K01830/returns" class="list-group-item list-group-item-action d-flex justify-content-between align-items-center">
                                GET /api/funds/{isin}/returns
                                <span class="badge bg-primary">Returns</span>
                            </a>
                            <a href="/api/funds/INF179K01830/holdings" class="list-group-item list-group-item-action d-flex justify-content-between align-items-center">
                                GET /api/funds/{isin}/holdings
                                <span class="badge bg-primary">Holdings</span>
                            </a>
                            <a href="/api/funds/INF179K01830/nav" class="list-group-item list-group-item-action d-flex justify-content-between align-items-center">
                                GET /api/funds/{isin}/nav
                                <span class="badge bg-primary">NAV History</span>
                            </a>
                            <a href="/api/funds/INF179K01830/all" class="list-group-item list-group-item-action d-flex justify-content-between align-items-center">
                                GET /api/funds/{isin}/all
                                <span class="badge bg-primary">All fund data</span>
                            </a>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </body>
    </html>
    """

if __name__ == '__main__':
    # Run the Flask application
    app.run(host='0.0.0.0', port=5000, debug=True)