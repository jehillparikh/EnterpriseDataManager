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
  """Configuration for PostgreSQL database connection."""

  DB_USER = os.getenv("DB_USER", "postgres").strip()
  DB_PASSWORD = os.getenv("DB_PASSWORD", "mutual%40fund%40pro12").strip()
  DB_HOST = os.getenv("DB_HOST", "34.57.196.130").strip()
  DB_PORT = os.getenv("DB_PORT", "5432").strip()
  DB_NAME = os.getenv("DB_NAME", "mutualfundpro").strip()

  @classmethod
  def get_database_uri(cls) -> str:
    """Constructs and returns the PostgreSQL URI."""
    return f"postgresql://{cls.DB_USER}:{cls.DB_PASSWORD}@{cls.DB_HOST}:{cls.DB_PORT}/{cls.DB_NAME}"
