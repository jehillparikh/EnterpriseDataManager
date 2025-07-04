import os
import logging
import tempfile
from flask import Blueprint, request, jsonify
from werkzeug.utils import secure_filename
import pandas as pd
from fund_data_importer import FundDataImporter

# Configure logging
logger = logging.getLogger(__name__)

# Create blueprint
upload_bp = Blueprint('upload', __name__)

# Allowed file extensions
ALLOWED_EXTENSIONS = {'xlsx', 'xls', 'csv'}


def allowed_file(filename):
    """Check if file extension is allowed"""
    if not filename:
        return False
    return '.' in filename and filename.rsplit(
        '.', 1)[1].lower() in ALLOWED_EXTENSIONS


def read_bse_csv(file):
    """Read BSE scheme CSV file with specific formatting"""
    # Save uploaded file to temporary location
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
    file.save(temp_file.name)
    temp_file.close()

    try:
        # Read CSV with BSE-specific parameters
        df = pd.read_csv(temp_file.name,
                         sep='|',
                         encoding='utf-8',
                         dtype={'Exit Load': str})
        if 'Unnamed: 43' in df.columns:
            df = df.drop(columns=['Unnamed: 43'])
        return df
    finally:
        # Clean up temporary file
        os.unlink(temp_file.name)


@upload_bp.route('/api/upload', methods=['POST'])
def upload_file():
    """Upload and process Excel file directly to database"""
    try:
        # Check if file is present
        if 'file' not in request.files:
            return jsonify({'error': 'No file provided'}), 400

        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400

        if not allowed_file(file.filename):
            return jsonify({
                'error':
                'Invalid file type. Only .xlsx, .xls, and .csv files are allowed'
            }), 400

        # Get upload parameters
        file_type = request.form.get('file_type', '')
        clear_existing = request.form.get('clear_existing',
                                          'false').lower() == 'true'
        batch_size = int(request.form.get('batch_size', 1000))

        if not file_type:
            return jsonify({'error': 'File type not specified'}), 400

        # Process the file
        try:
            logger.info(f"Reading file: {file.filename}")

            # Read file based on extension
            if file.filename:
                file_extension = file.filename.rsplit('.', 1)[1].lower()
                if file_extension == 'csv':
                    # Special handling for BSE scheme CSV files
                    if file_type == 'bse_scheme':
                        df = read_bse_csv(file)
                    else:
                        # Save to temp file for regular CSV processing
                        temp_file = tempfile.NamedTemporaryFile(delete=False,
                                                                suffix='.csv')
                        file.save(temp_file.name)
                        temp_file.close()
                        try:
                            df = pd.read_csv(temp_file.name)
                        finally:
                            os.unlink(temp_file.name)
                else:
                    df = pd.read_excel(file)
            else:
                return jsonify({'error': 'Invalid filename'}), 400

            logger.info(
                f"Successfully read file with {len(df)} rows and {len(df.columns)} columns"
            )

            # Import data to database based on file type
            stats = {}
            logger.info(f"Processing file type: {file_type}")

            # Initialize the data importer
            importer = FundDataImporter()

            if file_type == 'factsheet':
                stats = importer.import_factsheet_data(df, clear_existing)
            elif file_type == 'holdings':
                stats = importer.import_holdings_data(df, clear_existing, batch_size)
            elif file_type == 'returns':
                stats = importer.import_returns_data(df, clear_existing)
            elif file_type == 'nav':
                stats = importer.import_nav_data(df, clear_existing,
                                                 batch_size)
            elif file_type == 'bse_scheme':
                stats = importer.import_bse_scheme_data(
                    df, clear_existing, batch_size)
            else:
                logger.error(f"Unsupported file type: {file_type}")
                return jsonify(
                    {'error': f'Unsupported file type: {file_type}'}), 400

            logger.info(f"Import completed successfully with stats: {stats}")

            response_data = {
                'message': f'{file_type.title()} data imported successfully.',
                'filename': secure_filename(file.filename or 'unknown'),
                'rows': len(df),
                'columns': len(df.columns),
                'stats': stats
            }
            logger.info(f"Returning response: {response_data}")
            return jsonify(response_data), 200

        except pd.errors.EmptyDataError:
            error_msg = "Excel file is empty or contains no data"
            logger.error(error_msg)
            return jsonify({'error': error_msg}), 400
        except Exception as e:
            error_msg = f"Error processing Excel file: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return jsonify({'error': error_msg}), 400

    except Exception as e:
        logger.error(f"Error in upload_file: {e}")
        return jsonify({'error': f'Upload failed: {str(e)}'}), 500


@upload_bp.route('/api/upload/clear-temp', methods=['POST'])
def clear_temp_folder():
    """Clear temporary files"""
    try:
        import tempfile
        import shutil
        import glob

        temp_dir = tempfile.gettempdir()
        pattern = os.path.join(temp_dir, '*_*_*.xlsx')
        pattern2 = os.path.join(temp_dir, '*_*_*.xls')

        pattern3 = os.path.join(temp_dir, '*_*_*.csv')

        files_removed = 0

        # Remove xlsx files
        for file_path in glob.glob(pattern):
            try:
                os.remove(file_path)
                files_removed += 1
                logger.info(f"Removed temp file: {file_path}")
            except Exception as e:
                logger.error(f"Error removing {file_path}: {e}")

        # Remove xls files
        for file_path in glob.glob(pattern2):
            try:
                os.remove(file_path)
                files_removed += 1
                logger.info(f"Removed temp file: {file_path}")
            except Exception as e:
                logger.error(f"Error removing {file_path}: {e}")

        for file_path in glob.glob(pattern3):
            try:
                os.remove(file_path)
                files_removed += 1
                logger.info(f"Removed temp file: {file_path}")
            except Exception as e:
                logger.error(f"Error removing {file_path}: {e}")

        return jsonify({
            'message':
            f'Temp folder cleared successfully. Removed {files_removed} files.',
            'files_removed': files_removed
        }), 200

    except Exception as e:
        logger.error(f"Error clearing temp folder: {e}")
        return jsonify({'error': f'Error clearing temp folder: {str(e)}'}), 500


def init_upload_routes(app):
    """Initialize upload routes"""
    app.register_blueprint(upload_bp)
    logger.info("Upload routes registered")
