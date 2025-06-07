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

# Factsheet Flow
@upload_bp.route('/api/upload/factsheet', methods=['POST'])
def upload_factsheet():
    """Upload factsheet file to temp storage"""
    return store_file('factsheet', 'factsheet_data.xlsx')

@upload_bp.route('/api/upload/factsheet/submit', methods=['POST'])
def submit_factsheet():
    """Process factsheet data only"""
    try:
        # Start import in background and return immediately
        import threading
        
        def background_import():
            try:
                from flask import current_app
                from setup_db import create_app, db
                
                # Create new app instance for background processing
                app = create_app()
                with app.app_context():
                    # Import models within app context
                    from data.fund_data_importer import FundDataImporter
                    
                    # Set up file paths for importer
                    import os
                    temp_dir = os.path.join(os.getcwd(), 'temp_uploads')
                    factsheet_path = os.path.join(temp_dir, 'factsheet_data.xlsx')
                    
                    if not os.path.exists(factsheet_path):
                        logger.error("No factsheet file found in temp directory")
                        return
                    
                    # Process factsheet files
                    importer = FundDataImporter()
                    importer.factsheet_file = factsheet_path
                    results = importer.import_factsheet_data(clear_existing=False)
                    logger.info(f"Background factsheet import completed: {results}")
                    
            except Exception as e:
                logger.error(f"Background import failed: {e}")
                import traceback
                logger.error(f"Traceback: {traceback.format_exc()}")
        
        # Start background thread
        thread = threading.Thread(target=background_import)
        thread.daemon = True
        thread.start()
        
        return jsonify({
            'status': 'started',
            'message': 'Import started in background. Processing 877 records. Check console logs for progress.'
        })
        
    except Exception as e:
        logger.error(f"Error starting factsheet import: {e}")
        return jsonify({'error': str(e)}), 500

# Portfolio Flow  
@upload_bp.route('/api/upload/portfolio', methods=['POST'])
def upload_portfolio():
    """Upload portfolio file to temp storage"""
    return store_file('portfolio', 'portfolio_data.xlsx')

@upload_bp.route('/api/upload/portfolio/submit', methods=['POST'])
def submit_portfolio():
    """Process portfolio data only"""
    try:
        import threading
        
        def background_import():
            try:
                from setup_db import create_app, db
                
                app = create_app()
                with app.app_context():
                    from data.fund_data_importer import FundDataImporter
                    
                    importer = FundDataImporter()
                    results = importer.import_portfolio_data()
                    logger.info(f"Background portfolio import completed: {results}")
                    
            except Exception as e:
                logger.error(f"Background portfolio import failed: {e}")
                import traceback
                logger.error(f"Traceback: {traceback.format_exc()}")
        
        thread = threading.Thread(target=background_import)
        thread.daemon = True
        thread.start()
        
        return jsonify({
            'status': 'started',
            'message': 'Portfolio import started in background. Check console logs for progress.'
        })
        
    except Exception as e:
        logger.error(f"Error starting portfolio import: {e}")
        return jsonify({'error': str(e)}), 500

# NAV and Returns Flow
@upload_bp.route('/api/upload/returns', methods=['POST'])
def upload_returns():
    """Upload returns file to temp storage"""
    return store_file('returns', 'returns_data.xlsx')

@upload_bp.route('/api/upload/nav', methods=['POST'])
def upload_nav():
    """Upload NAV file to temp storage"""
    return store_file('nav', 'nav_data.xlsx')

@upload_bp.route('/api/upload/nav-returns/submit', methods=['POST'])
def submit_nav_returns():
    """Process NAV and returns data together"""
    try:
        import threading
        
        def background_import():
            try:
                from setup_db import create_app, db
                
                app = create_app()
                with app.app_context():
                    from data.fund_data_importer import FundDataImporter
                    
                    importer = FundDataImporter()
                    # Import both returns and NAV data
                    returns_results = importer.import_returns_data()
                    nav_results = importer.import_nav_data()
                    logger.info(f"Background NAV/returns import completed - Returns: {returns_results}, NAV: {nav_results}")
                    
            except Exception as e:
                logger.error(f"Background NAV/returns import failed: {e}")
                import traceback
                logger.error(f"Traceback: {traceback.format_exc()}")
        
        thread = threading.Thread(target=background_import)
        thread.daemon = True
        thread.start()
        
        return jsonify({
            'status': 'started',
            'message': 'NAV and returns import started in background. Check console logs for progress.'
        })
        
    except Exception as e:
        logger.error(f"Error starting NAV/returns import: {e}")
        return jsonify({'error': str(e)}), 500

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

def process_factsheet_files():
    """Process factsheet data only"""
    try:
        clear_existing = request.form.get('clear_existing', 'false').lower() == 'true'
        
        factsheet_path = os.path.join(TEMP_UPLOAD_DIR, 'factsheet_data.xlsx')
        
        if not os.path.exists(factsheet_path):
            return jsonify({'error': 'No factsheet file found. Please upload a factsheet file first.'}), 400
        
        # Create importer
        importer = FundDataImporter()
        importer.factsheet_file = factsheet_path
        
        # Import factsheet data
        results = importer.import_factsheet_data(clear_existing=clear_existing)
        logger.info("Factsheet data imported successfully")
        
        return jsonify({
            'message': 'Factsheet data imported successfully',
            'results': {'factsheet': results},
            'processed_files': ['factsheet']
        }), 200
        
    except Exception as e:
        logger.error(f"Error processing factsheet: {e}")
        return jsonify({'error': str(e)}), 500

def process_portfolio_files():
    """Process portfolio data only"""
    try:
        clear_existing = request.form.get('clear_existing', 'false').lower() == 'true'
        batch_size = int(request.form.get('batch_size', 1000))
        
        portfolio_path = os.path.join(TEMP_UPLOAD_DIR, 'portfolio_data.xlsx')
        
        if not os.path.exists(portfolio_path):
            return jsonify({'error': 'No portfolio file found. Please upload a portfolio file first.'}), 400
        
        # Create importer
        importer = FundDataImporter()
        importer.portfolio_file = portfolio_path
        
        # Import portfolio data
        results = importer.import_portfolio_data(clear_existing=clear_existing)
        logger.info("Portfolio data imported successfully")
        
        return jsonify({
            'message': 'Portfolio data imported successfully',
            'results': {'portfolio': results},
            'processed_files': ['portfolio']
        }), 200
        
    except Exception as e:
        logger.error(f"Error processing portfolio: {e}")
        return jsonify({'error': str(e)}), 500

def process_nav_returns_files():
    """Process NAV and returns data together"""
    try:
        clear_existing = request.form.get('clear_existing', 'false').lower() == 'true'
        batch_size = int(request.form.get('batch_size', 1000))
        
        returns_path = os.path.join(TEMP_UPLOAD_DIR, 'returns_data.xlsx')
        nav_path = os.path.join(TEMP_UPLOAD_DIR, 'nav_data.xlsx')
        
        # Check if at least one file exists
        has_returns = os.path.exists(returns_path)
        has_nav = os.path.exists(nav_path)
        
        if not has_returns and not has_nav:
            return jsonify({'error': 'No NAV or returns files found. Please upload at least one file first.'}), 400
        
        # Create importer
        importer = FundDataImporter()
        results = {}
        
        # Import returns data if available
        if has_returns:
            importer.returns_file = returns_path
            results['returns'] = importer.import_returns_data(clear_existing=clear_existing)
            logger.info("Returns data imported successfully")
        
        # Import NAV data if available
        if has_nav:
            importer.nav_file = nav_path
            results['nav'] = importer.import_nav_data(clear_existing=clear_existing, batch_size=batch_size)
            logger.info("NAV data imported successfully")
        
        return jsonify({
            'message': 'NAV and returns data imported successfully',
            'results': results,
            'processed_files': list(results.keys())
        }), 200
        
    except Exception as e:
        logger.error(f"Error processing NAV/returns: {e}")
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