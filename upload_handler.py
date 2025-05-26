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
    return render_template('upload.html')

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

def handle_upload(data_type, temp_filename):
    """Handle file upload and data import"""
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    
    file = request.files['file']
    if not file or file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    
    if not allowed_file(file.filename):
        return jsonify({'error': 'Invalid file type. Only .xlsx and .xls files are allowed'}), 400
    
    # Create temporary directory
    temp_dir = tempfile.mkdtemp()
    temp_file_path = os.path.join(temp_dir, temp_filename)
    
    try:
        # Save uploaded file temporarily
        file.save(temp_file_path)
        logger.info(f"Saved {data_type} file to {temp_file_path}")
        
        # Create importer and set file path
        importer = FundDataImporter()
        
        # Get options from form
        clear_existing = request.form.get('clear_existing', 'false').lower() == 'true'
        
        # Import data based on type
        if data_type == 'factsheet':
            importer.factsheet_file = temp_file_path
            stats = importer.import_factsheet_data(clear_existing=clear_existing)
        elif data_type == 'returns':
            importer.returns_file = temp_file_path
            stats = importer.import_returns_data(clear_existing=clear_existing)
        elif data_type == 'portfolio':
            importer.portfolio_file = temp_file_path
            stats = importer.import_portfolio_data(clear_existing=clear_existing)
        elif data_type == 'nav':
            importer.nav_file = temp_file_path
            batch_size = int(request.form.get('batch_size', 1000))
            stats = importer.import_nav_data(clear_existing=clear_existing, batch_size=batch_size)
        else:
            return jsonify({'error': 'Invalid data type'}), 400
        
        return jsonify({
            'message': f'{data_type.title()} data imported successfully',
            'stats': stats
        }), 200
        
    except Exception as e:
        logger.error(f"Error importing {data_type} data: {e}")
        return jsonify({'error': str(e)}), 500
        
    finally:
        # Clean up temporary files
        try:
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)
            os.rmdir(temp_dir)
            logger.info("Cleaned up temporary files")
        except Exception as e:
            logger.warning(f"Error cleaning up temporary files: {e}")

def init_upload_routes(app):
    """Initialize upload routes"""
    app.register_blueprint(upload_bp)
    logger.info("Upload routes registered")