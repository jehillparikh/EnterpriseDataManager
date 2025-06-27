import os
import logging
from flask import Flask
from sqlalchemy import MetaData
from sqlalchemy.orm import DeclarativeBase, registry
from flask_sqlalchemy import SQLAlchemy
import config

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Define naming convention for constraints to help with migrations
convention = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s"
}

# Create base metadata with naming convention
metadata = MetaData(naming_convention=convention)
mapper_registry = registry(metadata=metadata)

# Create SQLAlchemy base class
class Base(DeclarativeBase):
    metadata = metadata

# Create SQLAlchemy extension
db = SQLAlchemy(model_class=Base)

def create_app():
    """
    Create Flask application
    
    Returns:
        Flask: The configured Flask application
    """
    # Create Flask application
    app = Flask(__name__)
    
    # Configure database - prioritize Google Cloud SQL
    database_uri = os.environ.get('GOOGLE_CLOUD_DATABASE_URL')
    if database_uri:
        logger.info("Attempting to use Google Cloud SQL database")
        # Check if the connection string has the correct PostgreSQL format
        try:
            # Test if the URL can be parsed properly
            from sqlalchemy.engine import make_url
            make_url(database_uri)
            logger.info("Google Cloud SQL connection string format is valid")
        except Exception as e:
            logger.error(f"Invalid Google Cloud database URL format: {e}")
            logger.error("The connection string should be in format: postgresql://username:password@host:port/database")
            logger.error("For Google Cloud SQL, use the public IP address, not the connection name")
            logger.info("Falling back to default database connection")
            database_uri = None
    
    if not database_uri:
        # Fallback to other database URLs
        database_uri = os.environ.get('DATABASE_URL') or os.environ.get('SQLALCHEMY_DATABASE_URI')
        if database_uri:
            logger.info("Using fallback database connection")
        else:
            logger.error("No valid database connection string found!")
            raise ValueError("Database connection string required")
    
    # Set this in the environment in case any other modules need it
    os.environ['SQLALCHEMY_DATABASE_URI'] = database_uri
    
    # Configure SQLAlchemy - this is what Flask-SQLAlchemy looks for
    app.config['SQLALCHEMY_DATABASE_URI'] = database_uri
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
        "pool_recycle": 300,
        "pool_pre_ping": True,
    }
    
    # Set a secret key for the application
    app.secret_key = config.SECRET_KEY
    
    # Initialize database
    db.init_app(app)
    
    return app