import os
import tempfile
import logging
from flask import Blueprint, request, jsonify, render_template
from werkzeug.utils import secure_filename
from data.fund_data_importer import FundDataImporter

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Create blueprint
upload_api = Blueprint('upload_api', __name__)

# Allowed file extensions
ALLOWED_EXTENSIONS = {'xlsx', 'xls'}

def allowed_file(filename):
    """Check if file extension is allowed"""
    if not filename:
        return False
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@upload_api.route('/upload', methods=['GET'])
def upload_page():
    """Display the file upload page"""
    return render_template('upload.html')

@upload_api.route('/api/upload/factsheet', methods=['POST'])
def upload_factsheet():
    """Upload and import factsheet data"""
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    
    if not allowed_file(file.filename):
        return jsonify({'error': 'Invalid file type. Only .xlsx and .xls files are allowed'}), 400
    
    # Create temporary directory
    temp_dir = tempfile.mkdtemp()
    temp_file_path = None
    
    try:
        # Save uploaded file temporarily
        temp_file_path = os.path.join(temp_dir, 'factsheet_data.xlsx')
        file.save(temp_file_path)
        
        logger.info(f"Saved factsheet file to {temp_file_path}")
        
        # Create importer with custom file path
        importer = FundDataImporter()
        importer.factsheet_file = temp_file_path
        
        # Import data
        clear_existing = request.form.get('clear_existing', 'false').lower() == 'true'
        stats = importer.import_factsheet_data(clear_existing=clear_existing)
        
        return jsonify({
            'message': 'Factsheet data imported successfully',
            'stats': stats
        }), 200
        
    except Exception as e:
        logger.error(f"Error importing factsheet data: {e}")
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

@upload_api.route('/api/upload/returns', methods=['POST'])
def upload_returns():
    """Upload and import returns data"""
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    
    if not allowed_file(file.filename):
        return jsonify({'error': 'Invalid file type. Only .xlsx and .xls files are allowed'}), 400
    
    # Create temporary directory
    temp_dir = tempfile.mkdtemp()
    
    try:
        # Save uploaded file temporarily
        filename = secure_filename(file.filename)
        temp_file_path = os.path.join(temp_dir, 'returns_data.xlsx')
        file.save(temp_file_path)
        
        logger.info(f"Saved returns file to {temp_file_path}")
        
        # Create importer with custom file path
        importer = FundDataImporter()
        importer.returns_file = temp_file_path
        
        # Import data
        clear_existing = request.form.get('clear_existing', 'false').lower() == 'true'
        stats = importer.import_returns_data(clear_existing=clear_existing)
        
        return jsonify({
            'message': 'Returns data imported successfully',
            'stats': stats
        }), 200
        
    except Exception as e:
        logger.error(f"Error importing returns data: {e}")
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

@upload_api.route('/api/upload/portfolio', methods=['POST'])
def upload_portfolio():
    """Upload and import portfolio holdings data"""
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    
    if not allowed_file(file.filename):
        return jsonify({'error': 'Invalid file type. Only .xlsx and .xls files are allowed'}), 400
    
    # Create temporary directory
    temp_dir = tempfile.mkdtemp()
    
    try:
        # Save uploaded file temporarily
        filename = secure_filename(file.filename)
        temp_file_path = os.path.join(temp_dir, 'portfolio_data.xlsx')
        file.save(temp_file_path)
        
        logger.info(f"Saved portfolio file to {temp_file_path}")
        
        # Create importer with custom file path
        importer = FundDataImporter()
        importer.portfolio_file = temp_file_path
        
        # Import data
        clear_existing = request.form.get('clear_existing', 'false').lower() == 'true'
        stats = importer.import_portfolio_data(clear_existing=clear_existing)
        
        return jsonify({
            'message': 'Portfolio data imported successfully',
            'stats': stats
        }), 200
        
    except Exception as e:
        logger.error(f"Error importing portfolio data: {e}")
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

@upload_api.route('/api/upload/nav', methods=['POST'])
def upload_nav():
    """Upload and import NAV history data"""
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    
    if not allowed_file(file.filename):
        return jsonify({'error': 'Invalid file type. Only .xlsx and .xls files are allowed'}), 400
    
    # Create temporary directory
    temp_dir = tempfile.mkdtemp()
    
    try:
        # Save uploaded file temporarily
        filename = secure_filename(file.filename)
        temp_file_path = os.path.join(temp_dir, 'nav_data.xlsx')
        file.save(temp_file_path)
        
        logger.info(f"Saved NAV file to {temp_file_path}")
        
        # Create importer with custom file path
        importer = FundDataImporter()
        importer.nav_file = temp_file_path
        
        # Import data
        clear_existing = request.form.get('clear_existing', 'false').lower() == 'true'
        batch_size = int(request.form.get('batch_size', 1000))
        stats = importer.import_nav_data(clear_existing=clear_existing, batch_size=batch_size)
        
        return jsonify({
            'message': 'NAV data imported successfully',
            'stats': stats
        }), 200
        
    except Exception as e:
        logger.error(f"Error importing NAV data: {e}")
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

def init_upload_api(app):
    """Initialize upload API routes"""
    app.register_blueprint(upload_api)
    logger.info("Upload API routes registered")
    
    return app