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
    """Upload and process Excel file directly to database"""
    try:
        # Check if file is present
        if 'file' not in request.files:
            return jsonify({'error': 'No file provided'}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
        
        if not allowed_file(file.filename):
            return jsonify({'error': 'Invalid file type. Only .xlsx and .xls files are allowed'}), 400
        
        # Get upload parameters
        file_type = request.form.get('file_type', '')
        clear_existing = request.form.get('clear_existing', 'false').lower() == 'true'
        batch_size = int(request.form.get('batch_size', 1000))
        
        if not file_type:
            return jsonify({'error': 'File type not specified'}), 400
        
        # Process the file
        try:
            df = pd.read_excel(file)
            logger.info(f"Successfully read Excel file with {len(df)} rows and {len(df.columns)} columns")
            
            # Import data to database based on file type
            stats = {}
            
            if file_type == 'factsheet':
                stats = import_factsheet_data(df, clear_existing)
            elif file_type == 'holdings':
                stats = import_holdings_data(df, clear_existing)
            elif file_type == 'returns':
                stats = import_returns_data(df, clear_existing)
            elif file_type == 'nav':
                stats = import_nav_data(df, clear_existing, batch_size)
            else:
                return jsonify({'error': f'Unsupported file type: {file_type}'}), 400
            
            return jsonify({
                'message': f'{file_type.title()} data imported successfully.',
                'filename': secure_filename(file.filename or 'unknown'),
                'rows': len(df),
                'columns': len(df.columns),
                'stats': stats
            }), 200
            
        except Exception as e:
            logger.error(f"Error processing Excel file: {e}")
            return jsonify({'error': f'Error processing Excel file: {str(e)}'}), 400
            
    except Exception as e:
        logger.error(f"Error in upload_file: {e}")
        return jsonify({'error': f'Upload failed: {str(e)}'}), 500


def import_factsheet_data(df, clear_existing):
    """Import factsheet data to database"""
    if clear_existing:
        FundFactSheet.query.delete()
        Fund.query.delete()
        db.session.commit()
        logger.info("Cleared existing factsheet and fund data")
    
    funds_created = 0
    factsheets_created = 0
    
    for _, row in df.iterrows():
        try:
            # Create or update Fund record
            isin = str(row.get('ISIN', '')).strip()
            if not isin or isin.lower() == 'nan':
                continue
                
            fund = Fund.query.filter_by(isin=isin).first()
            if not fund:
                fund = Fund(
                    isin=isin,
                    scheme_name=str(row.get('Scheme Name', '')),
                    fund_type=str(row.get('Fund Type', '')),
                    fund_subtype=str(row.get('Fund Sub Type', '')),
                    amc_name=str(row.get('AMC Name', ''))
                )
                db.session.add(fund)
                funds_created += 1
            
            # Create or update FundFactSheet record
            factsheet = FundFactSheet.query.filter_by(isin=isin).first()
            if not factsheet:
                factsheet = FundFactSheet(isin=isin)
                factsheets_created += 1
            
            # Update factsheet fields
            factsheet.fund_manager = str(row.get('Fund Manager', '')) if pd.notna(row.get('Fund Manager')) else None
            factsheet.aum = float(row.get('AUM', 0)) if pd.notna(row.get('AUM')) else None
            factsheet.expense_ratio = float(row.get('Expense Ratio', 0)) if pd.notna(row.get('Expense Ratio')) else None
            factsheet.exit_load = str(row.get('Exit Load', '')) if pd.notna(row.get('Exit Load')) else None
            
            if factsheets_created > 0:
                db.session.add(factsheet)
                
        except Exception as e:
            logger.error(f"Error processing factsheet row: {e}")
            continue
    
    db.session.commit()
    return {
        'funds_created': funds_created,
        'factsheets_created': factsheets_created,
        'total_rows_processed': len(df)
    }


def import_holdings_data(df, clear_existing):
    """Import holdings data to database"""
    if clear_existing:
        FundHolding.query.delete()
        db.session.commit()
        logger.info("Cleared existing holdings data")
    
    holdings_created = 0
    
    for _, row in df.iterrows():
        try:
            scheme_isin = str(row.get('Scheme ISIN', '')).strip()
            if not scheme_isin or scheme_isin.lower() == 'nan':
                continue
            
            holding = FundHolding(
                isin=scheme_isin,
                instrument_isin=str(row.get('ISIN', '')) if pd.notna(row.get('ISIN')) else None,
                instrument_name=str(row.get('Name of Instrument', '')),
                sector=str(row.get('Industry', '')) if pd.notna(row.get('Industry')) else None,
                quantity=float(row.get('Quantity', 0)) if pd.notna(row.get('Quantity')) else None,
                value=float(row.get('Market Value', 0)) if pd.notna(row.get('Market Value')) else None,
                percentage_to_nav=float(row.get('% to Net Assets', 0)) if pd.notna(row.get('% to Net Assets')) else 0,
                yield_value=float(row.get('Yield', 0)) if pd.notna(row.get('Yield')) else None,
                instrument_type=str(row.get('Type', '')),
                coupon=float(row.get('Coupon', 0)) if pd.notna(row.get('Coupon')) else None,
                amc_name=str(row.get('AMC', '')) if pd.notna(row.get('AMC')) else None,
                scheme_name=str(row.get('Scheme Name', '')) if pd.notna(row.get('Scheme Name')) else None
            )
            
            db.session.add(holding)
            holdings_created += 1
            
        except Exception as e:
            logger.error(f"Error processing holding row: {e}")
            continue
    
    db.session.commit()
    return {
        'holdings_created': holdings_created,
        'total_rows_processed': len(df)
    }


def import_returns_data(df, clear_existing):
    """Import returns data to database"""
    if clear_existing:
        FundReturns.query.delete()
        db.session.commit()
        logger.info("Cleared existing returns data")
    
    returns_created = 0
    
    for _, row in df.iterrows():
        try:
            isin = str(row.get('ISIN', '')).strip()
            if not isin or isin.lower() == 'nan':
                continue
            
            returns_record = FundReturns.query.filter_by(isin=isin).first()
            if not returns_record:
                returns_record = FundReturns(isin=isin)
                returns_created += 1
            
            # Update returns fields
            returns_record.return_1m = float(row.get('1M Return', 0)) if pd.notna(row.get('1M Return')) else None
            returns_record.return_3m = float(row.get('3M Return', 0)) if pd.notna(row.get('3M Return')) else None
            returns_record.return_6m = float(row.get('6M Return', 0)) if pd.notna(row.get('6M Return')) else None
            returns_record.return_ytd = float(row.get('YTD Return', 0)) if pd.notna(row.get('YTD Return')) else None
            returns_record.return_1y = float(row.get('1Y Return', 0)) if pd.notna(row.get('1Y Return')) else None
            returns_record.return_3y = float(row.get('3Y Return', 0)) if pd.notna(row.get('3Y Return')) else None
            returns_record.return_5y = float(row.get('5Y Return', 0)) if pd.notna(row.get('5Y Return')) else None
            
            if returns_created > 0:
                db.session.add(returns_record)
                
        except Exception as e:
            logger.error(f"Error processing returns row: {e}")
            continue
    
    db.session.commit()
    return {
        'returns_created': returns_created,
        'total_rows_processed': len(df)
    }


def import_nav_data(df, clear_existing, batch_size):
    """Import NAV data to database"""
    from datetime import datetime
    
    if clear_existing:
        NavHistory.query.delete()
        db.session.commit()
        logger.info("Cleared existing NAV data")
    
    nav_records_created = 0
    batch_count = 0
    
    for _, row in df.iterrows():
        try:
            isin = str(row.get('ISIN', '')).strip()
            if not isin or isin.lower() == 'nan':
                continue
            
            # Parse date
            date_str = str(row.get('Date', ''))
            if pd.notna(row.get('Date')):
                if isinstance(row.get('Date'), datetime):
                    nav_date = row.get('Date').date()
                else:
                    nav_date = pd.to_datetime(date_str).date()
            else:
                continue
            
            nav_value = float(row.get('NAV', 0)) if pd.notna(row.get('NAV')) else None
            if nav_value is None:
                continue
            
            nav_record = NavHistory(
                isin=isin,
                date=nav_date,
                nav=nav_value
            )
            
            db.session.add(nav_record)
            nav_records_created += 1
            batch_count += 1
            
            # Commit in batches
            if batch_count >= batch_size:
                db.session.commit()
                batch_count = 0
                logger.info(f"Committed batch of {batch_size} NAV records")
                
        except Exception as e:
            logger.error(f"Error processing NAV row: {e}")
            continue
    
    # Commit remaining records
    if batch_count > 0:
        db.session.commit()
    
    return {
        'nav_records_created': nav_records_created,
        'total_rows_processed': len(df),
        'batch_size_used': batch_size
    }

def init_upload_routes(app):
    """Initialize upload routes"""
    app.register_blueprint(upload_bp)
    logger.info("Upload routes registered")