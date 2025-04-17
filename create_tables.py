import logging
from setup_db import create_app, db
from sqlalchemy import text

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = create_app()

def drop_and_create_tables():
    with app.app_context():
        # Import our models
        from new_models_updated import Fund, FundFactSheet, FundReturns, PortfolioHolding, NavHistory
        
        # First, try to create tables directly
        logger.info("Attempting to create tables directly...")
        try:
            db.create_all()
            logger.info("Tables created successfully")
            return
        except Exception as e:
            logger.warning(f"Could not create tables directly: {e}")
        
        # If direct creation fails, try using raw SQL to create tables in proper order
        logger.info("Creating tables using raw SQL...")
        
        # First, try to drop tables if they exist (in reverse dependency order)
        statements = [
            "DROP TABLE IF EXISTS mf_nav_history;",
            "DROP TABLE IF EXISTS mf_portfolio_holdings;",
            "DROP TABLE IF EXISTS mf_returns;",
            "DROP TABLE IF EXISTS mf_factsheet;",
            "DROP TABLE IF EXISTS mf_fund;"
        ]
        
        # Then create tables in dependency order
        statements.extend([
            """
            CREATE TABLE IF NOT EXISTS mf_fund (
                isin VARCHAR(12) PRIMARY KEY,
                scheme_name VARCHAR(255) NOT NULL,
                fund_type VARCHAR(50) NOT NULL,
                fund_subtype VARCHAR(50),
                amc_name VARCHAR(100) NOT NULL,
                created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS mf_factsheet (
                isin VARCHAR(12) PRIMARY KEY REFERENCES mf_fund(isin) ON DELETE CASCADE,
                fund_manager VARCHAR(255),
                aum FLOAT,
                expense_ratio FLOAT,
                launch_date DATE,
                exit_load VARCHAR(255),
                last_updated TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS mf_returns (
                isin VARCHAR(12) PRIMARY KEY REFERENCES mf_fund(isin) ON DELETE CASCADE,
                return_1m FLOAT,
                return_3m FLOAT,
                return_6m FLOAT,
                return_ytd FLOAT,
                return_1y FLOAT,
                return_3y FLOAT,
                return_5y FLOAT,
                last_updated TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                CHECK (return_1m >= -100),
                CHECK (return_3m >= -100),
                CHECK (return_6m >= -100),
                CHECK (return_ytd >= -100),
                CHECK (return_1y >= -100),
                CHECK (return_3y >= -100),
                CHECK (return_5y >= -100)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS mf_portfolio_holdings (
                id SERIAL PRIMARY KEY,
                isin VARCHAR(12) REFERENCES mf_fund(isin) ON DELETE CASCADE NOT NULL,
                instrument_isin VARCHAR(12),
                coupon FLOAT,
                instrument_name VARCHAR(255) NOT NULL,
                sector VARCHAR(100),
                quantity FLOAT,
                value FLOAT,
                percentage_to_nav FLOAT NOT NULL,
                yield_value FLOAT,
                instrument_type VARCHAR(50) NOT NULL,
                last_updated TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                CHECK (percentage_to_nav >= 0),
                CHECK (percentage_to_nav <= 100)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS mf_nav_history (
                id SERIAL PRIMARY KEY,
                isin VARCHAR(12) REFERENCES mf_fund(isin) ON DELETE CASCADE NOT NULL,
                date DATE NOT NULL,
                nav FLOAT NOT NULL,
                CHECK (nav >= 0)
            );
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_fund_amc_name ON mf_fund(amc_name);
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_fund_type ON mf_fund(fund_type);
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_holdings_fund_isin ON mf_portfolio_holdings(isin);
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_holdings_sector ON mf_portfolio_holdings(sector);
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_holdings_type ON mf_portfolio_holdings(instrument_type);
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_nav_history_isin_date ON mf_nav_history(isin, date);
            """
        ])
        
        # Execute each statement
        for statement in statements:
            try:
                db.session.execute(text(statement))
                db.session.commit()
                logger.info(f"Executed: {statement[:50]}...")
            except Exception as e:
                db.session.rollback()
                logger.error(f"Error executing SQL: {e}")
                logger.error(f"Statement: {statement}")
        
        logger.info("Finished creating tables")

if __name__ == "__main__":
    drop_and_create_tables()