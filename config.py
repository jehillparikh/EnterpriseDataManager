"""
Configuration settings for the Mutual Fund API application
"""
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database configuration
DATABASE_URL = os.environ.get('DATABASE_URL')

# Application configuration
DEBUG = os.environ.get('DEBUG', 'False').lower() in ('true', '1', 't')
SECRET_KEY = os.environ.get('SECRET_KEY', 'mutual-fund-api-secret-key')

# API configuration
API_VERSION = '1.0.0'
API_BASE_URL = '/api'

# Server configuration
HOST = '0.0.0.0'
PORT = int(os.environ.get('PORT', 5000))