import os
import logging
from flask import Blueprint, request, jsonify
from werkzeug.utils import secure_filename
import pandas as pd
from models import Fund, FundFactSheet, FundReturns, FundHolding, NavHistory
from setup_db import db

# Configure logging
logger = logging.getLogger(__name__)

# Create blueprint
upload_bp = Blueprint('upload', __name__)

# Allowed file extensions
ALLOWED_EXTENSIONS = {'xlsx', 'xls'}

def allowed_file(filename):
    """Check if file extension is allowed"""
    if not filename:
        return False
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@upload_bp.route('/api/upload', methods=['POST'])
def upload_file():
    """Upload and process Excel file"""
    try:
        # Check if file is present
        if 'file' not in request.files:
            return jsonify({'error': 'No file provided'}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
        
        if not allowed_file(file.filename):
            return jsonify({'error': 'Invalid file type. Only .xlsx and .xls files are allowed'}), 400
        
        # Process the file
        try:
            df = pd.read_excel(file)
            logger.info(f"Successfully read Excel file with {len(df)} rows and {len(df.columns)} columns")
            
            # Return basic file information
            return jsonify({
                'message': f'File uploaded successfully. Found {len(df)} rows and {len(df.columns)} columns.',
                'filename': secure_filename(file.filename or 'unknown'),
                'rows': len(df),
                'columns': len(df.columns),
                'column_names': df.columns.tolist()
            }), 200
            
        except Exception as e:
            logger.error(f"Error processing Excel file: {e}")
            return jsonify({'error': f'Error processing Excel file: {str(e)}'}), 400
            
    except Exception as e:
        logger.error(f"Error in upload_file: {e}")
        return jsonify({'error': f'Upload failed: {str(e)}'}), 500

def init_upload_routes(app):
    """Initialize upload routes"""
    app.register_blueprint(upload_bp)
    logger.info("Upload routes registered")