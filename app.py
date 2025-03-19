import os
import logging

from flask import Flask
from api import setup_routes

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Create Flask application
app = Flask(__name__)
app.secret_key = os.environ.get("SESSION_SECRET", "dev-secret-key")

def setup_api(app):
    """
    Configure API routes for the application
    """
    setup_routes(app)
    logger.info("API routes configured successfully")

logger.info("Flask application created")
