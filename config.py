"""
Configuration settings for the Mutual Fund API application
"""
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database configuration
SQLALCHEMY_DATABASE_URI = os.environ.get('SQLALCHEMY_DATABASE_URI')

# Application configuration
DEBUG = os.environ.get('DEBUG', 'False').lower() in ('true', '1', 't')
SECRET_KEY = os.environ.get('SECRET_KEY', 'mutual-fund-api-secret-key')

# API configuration
API_VERSION = '1.0.0'
API_BASE_URL = '/api'

# Server configuration
HOST = '0.0.0.0'
PORT = int(os.environ.get('PORT', 5000))


# SQLonfig.py
class SQLConfig:
  DB_USER = os.environ["DB_USER"]
  DB_PASSWORD = os.environ["DB_PASSWORD"]
  DB_HOST = os.environ["DB_HOST"]
  DB_PORT = os.environ["DB_PORT"]
  DB_NAME = os.environ["DB_NAME"]

  @classmethod
  def get_database_uri(cls) -> str:
    """Constructs and returns the PostgreSQL URI."""
    return f"postgresql://{cls.DB_USER}:{cls.DB_PASSWORD}@{cls.DB_HOST}:{cls.DB_PORT}/{cls.DB_NAME}"
