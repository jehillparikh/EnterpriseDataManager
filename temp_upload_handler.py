import os
import logging
import shutil
from flask import Blueprint, request, jsonify, render_template
from werkzeug.utils import secure_filename
from data.fund_data_importer import FundDataImporter

# Configure logging
logger = logging.getLogger(__name__)

# Create blueprint
upload_bp = Blueprint('upload', __name__)

# Allowed file extensions
ALLOWED_EXTENSIONS = {'xlsx', 'xls'}

# Temp upload directory
TEMP_UPLOAD_DIR = 'temp_uploads'

# Ensure temp directory exists
os.makedirs(TEMP_UPLOAD_DIR, exist_ok=True)

def allowed_file(filename):
    """Check if file extension is allowed"""
    if not filename:
        return False
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@upload_bp.route('/upload')
def upload_page():
    """Display the file upload page"""
    return render_template('main_dashboard.html')

@upload_bp.route('/api/upload/factsheet', methods=['POST'])
def upload_factsheet():
    """Upload factsheet file to temp storage"""
    return store_file('factsheet', 'factsheet_data.xlsx')

@upload_bp.route('/api/upload/returns', methods=['POST'])
def upload_returns():
    """Upload returns file to temp storage"""
    return store_file('returns', 'returns_data.xlsx')

@upload_bp.route('/api/upload/portfolio', methods=['POST'])
def upload_portfolio():
    """Upload portfolio file to temp storage"""
    return store_file('portfolio', 'portfolio_data.xlsx')

@upload_bp.route('/api/upload/nav', methods=['POST'])
def upload_nav():
    """Upload NAV file to temp storage"""
    return store_file('nav', 'nav_data.xlsx')

@upload_bp.route('/api/upload/submit', methods=['POST'])
def submit_to_database():
    """Process all uploaded files and import to database"""
    return process_all_files()

@upload_bp.route('/api/upload/status', methods=['GET'])
def upload_status():
    """Check status of uploaded files"""
    return get_upload_status()

@upload_bp.route('/api/upload/clear', methods=['POST'])
def clear_uploads():
    """Clear all uploaded files"""
    return clear_temp_files()

def store_file(data_type, temp_filename):
    """Store uploaded file in temp directory"""
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    
    file = request.files['file']
    if not file or file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    
    if not allowed_file(file.filename):
        return jsonify({'error': 'Invalid file type. Only .xlsx and .xls files are allowed'}), 400
    
    try:
        # Save file to temp directory
        temp_file_path = os.path.join(TEMP_UPLOAD_DIR, temp_filename)
        file.save(temp_file_path)
        logger.info(f"Stored {data_type} file: {temp_filename}")
        
        return jsonify({
            'message': f'{data_type.title()} file uploaded successfully',
            'filename': temp_filename,
            'size': f"{(os.path.getsize(temp_file_path) / 1024 / 1024):.2f} MB"
        }), 200
        
    except Exception as e:
        logger.error(f"Error storing {data_type} file: {e}")
        return jsonify({'error': str(e)}), 500

def process_all_files():
    """Process all uploaded files and import to database"""
    try:
        # Get options from form
        clear_existing = request.form.get('clear_existing', 'false').lower() == 'true'
        batch_size = int(request.form.get('batch_size', 1000))
        
        # Create importer
        importer = FundDataImporter()
        
        # Set file paths for available files
        factsheet_path = os.path.join(TEMP_UPLOAD_DIR, 'factsheet_data.xlsx')
        returns_path = os.path.join(TEMP_UPLOAD_DIR, 'returns_data.xlsx')
        portfolio_path = os.path.join(TEMP_UPLOAD_DIR, 'portfolio_data.xlsx')
        nav_path = os.path.join(TEMP_UPLOAD_DIR, 'nav_data.xlsx')
        
        results = {}
        
        # Import factsheet data if available
        if os.path.exists(factsheet_path):
            importer.factsheet_file = factsheet_path
            results['factsheet'] = importer.import_factsheet_data(clear_existing=clear_existing)
            logger.info("Factsheet data imported successfully")
        
        # Import returns data if available
        if os.path.exists(returns_path):
            importer.returns_file = returns_path
            results['returns'] = importer.import_returns_data(clear_existing=clear_existing)
            logger.info("Returns data imported successfully")
        
        # Import portfolio data if available
        if os.path.exists(portfolio_path):
            importer.portfolio_file = portfolio_path
            results['portfolio'] = importer.import_portfolio_data(clear_existing=clear_existing)
            logger.info("Portfolio data imported successfully")
        
        # Import NAV data if available
        if os.path.exists(nav_path):
            importer.nav_file = nav_path
            results['nav'] = importer.import_nav_data(clear_existing=clear_existing, batch_size=batch_size)
            logger.info("NAV data imported successfully")
        
        if not results:
            return jsonify({'error': 'No files found to process'}), 400
        
        return jsonify({
            'message': 'All data imported successfully',
            'results': results,
            'processed_files': list(results.keys())
        }), 200
        
    except Exception as e:
        logger.error(f"Error processing files: {e}")
        return jsonify({'error': str(e)}), 500

def get_upload_status():
    """Get status of uploaded files"""
    files = {}
    
    file_types = {
        'factsheet_data.xlsx': 'factsheet',
        'returns_data.xlsx': 'returns', 
        'portfolio_data.xlsx': 'portfolio',
        'nav_data.xlsx': 'nav'
    }
    
    for filename, file_type in file_types.items():
        file_path = os.path.join(TEMP_UPLOAD_DIR, filename)
        if os.path.exists(file_path):
            file_size = os.path.getsize(file_path)
            files[file_type] = {
                'filename': filename,
                'size': f"{(file_size / 1024 / 1024):.2f} MB",
                'uploaded': True
            }
        else:
            files[file_type] = {
                'filename': filename,
                'uploaded': False
            }
    
    return jsonify({
        'files': files,
        'ready_to_submit': any(f['uploaded'] for f in files.values())
    })

def clear_temp_files():
    """Clear all temporary files"""
    try:
        if os.path.exists(TEMP_UPLOAD_DIR):
            shutil.rmtree(TEMP_UPLOAD_DIR)
            os.makedirs(TEMP_UPLOAD_DIR, exist_ok=True)
            logger.info("Cleared all temporary files")
        
        return jsonify({'message': 'All temporary files cleared successfully'}), 200
        
    except Exception as e:
        logger.error(f"Error clearing temp files: {e}")
        return jsonify({'error': str(e)}), 500

def init_upload_routes(app):
    """Initialize upload routes"""
    app.register_blueprint(upload_bp)
    logger.info("Upload routes registered")