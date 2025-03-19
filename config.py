import os

# Database configuration
DATABASE_URL = os.environ.get('DATABASE_URL')

# Secret key for JWT
JWT_SECRET_KEY = os.environ.get('JWT_SECRET_KEY', 'your-secret-key-here')
JWT_EXPIRY_HOURS = 24 * 30  # 30 days

# Hyperverge API Configuration
HYPERVERGE_APP_ID = os.environ.get('HYPERVERGE_APP_ID', '')
HYPERVERGE_APP_KEY = os.environ.get('HYPERVERGE_APP_KEY', '')
HYPERVERGE_BASE_URL = 'https://ind-docs.hyperverge.co/v2.0'

# BSE Star API Configuration
BSE_STAR_API_KEY = os.environ.get('BSE_STAR_API_KEY', '')
BSE_STAR_BASE_URL = 'https://www.bsestarmf.in/RptApi/api'