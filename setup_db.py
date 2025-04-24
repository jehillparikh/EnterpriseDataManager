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
    
    # Configure database
    database_uri = os.environ.get('SQLALCHEMY_DATABASE_URI')
    if not database_uri:
        logger.error("SQLALCHEMY_DATABASE_URI environment variable not set!")
        logger.warning("Using fallback connection string for development only")
        # For local development only, should never be used in production
        database_uri = "postgresql://userdatabase_740c_user:VswYN2reYmzvjgZ5QMkNugBPxYvzTe08@dpg-cvn3uaemcj7s73c3a8vg-a.singapore-postgres.render.com/userdatabase_740c"
    
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