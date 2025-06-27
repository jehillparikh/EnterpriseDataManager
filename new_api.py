from flask import Blueprint, jsonify, request
from new_services import (
    FundService, FactSheetService, ReturnsService, 
    PortfolioService, NavHistoryService,
    ResourceNotFoundError, ValidationError, 
    UniqueConstraintError, DatabaseError
)
from marshmallow import Schema, fields, validate, ValidationError as SchemaValidationError
from datetime import datetime
import logging

api_bp = Blueprint('api', __name__)

# Schema definitions
class FundSchema(Schema):
    """Schema for fund validation"""
    isin = fields.String(required=True, validate=validate.Length(equal=12))
    scheme_name = fields.String(required=True)
    fund_type = fields.String(required=True)
    fund_subtype = fields.String()
    amc_name = fields.String(required=True)

class FactSheetSchema(Schema):
    """Schema for factsheet validation"""
    isin = fields.String(required=True, validate=validate.Length(equal=12))
    fund_manager = fields.String()
    aum = fields.Float(validate=validate.Range(min=0))
    expense_ratio = fields.Float(validate=validate.Range(min=0, max=10))
    launch_date = fields.Date()
    exit_load = fields.String()

class ReturnsSchema(Schema):
    """Schema for returns validation"""
    isin = fields.String(required=True, validate=validate.Length(equal=12))
    return_1m = fields.Float(validate=validate.Range(min=-100))
    return_3m = fields.Float(validate=validate.Range(min=-100))
    return_6m = fields.Float(validate=validate.Range(min=-100))
    return_ytd = fields.Float(validate=validate.Range(min=-100))
    return_1y = fields.Float(validate=validate.Range(min=-100))
    return_3y = fields.Float(validate=validate.Range(min=-100))
    return_5y = fields.Float(validate=validate.Range(min=-100))

class FundHoldingSchema(Schema):
    """Schema for portfolio holding validation"""
    isin = fields.String(required=True, validate=validate.Length(equal=12))
    instrument_isin = fields.String(validate=validate.Length(equal=12))
    coupon = fields.Float(validate=validate.Range(min=0))
    instrument_name = fields.String(required=True)
    sector = fields.String()
    quantity = fields.Float(validate=validate.Range(min=0))
    value = fields.Float(validate=validate.Range(min=0))
    percentage_to_nav = fields.Float(required=True, validate=validate.Range(min=0, max=100))
    yield_value = fields.Float(validate=validate.Range(min=0))
    instrument_type = fields.String(required=True)

class NavHistorySchema(Schema):
    """Schema for NAV history validation"""
    isin = fields.String(required=True, validate=validate.Length(equal=12))
    date = fields.Date(required=True)
    nav = fields.Float(required=True, validate=validate.Range(min=0))

# Instantiate schemas
fund_schema = FundSchema()
factsheet_schema = FactSheetSchema()
returns_schema = ReturnsSchema()
fund_holding_schema = FundHoldingSchema()
nav_history_schema = NavHistorySchema()

# Global error handler
@api_bp.errorhandler(Exception)
def handle_error(error):
    """Global error handler for API routes"""
    logging.error(f"API error: {str(error)}")
    
    if isinstance(error, ResourceNotFoundError):
        return jsonify({"error": str(error)}), 404
    elif isinstance(error, UniqueConstraintError):
        return jsonify({"error": str(error)}), 409
    elif isinstance(error, ValidationError):
        return jsonify({"error": str(error)}), 400
    elif isinstance(error, SchemaValidationError):
        return jsonify({"error": error.messages}), 400
    elif isinstance(error, DatabaseError):
        return jsonify({"error": str(error)}), 500
    else:
        return jsonify({"error": "Internal server error"}), 500

# Fund routes
@api_bp.route('/funds', methods=['GET'])
def get_all_funds():
    """Get all funds"""
    funds = FundService.get_all_funds()
    
    result = []
    for fund in funds:
        result.append({
            "isin": fund.isin,
            "scheme_name": fund.scheme_name,
            "fund_type": fund.fund_type,
            "fund_subtype": fund.fund_subtype,
            "amc_name": fund.amc_name
        })
    
    return jsonify(result), 200

@api_bp.route('/funds', methods=['POST'])
def create_fund():
    """Create a new fund"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No input data provided"}), 400
        
        validated_data = fund_schema.load(data)
        
        fund = FundService.create_fund(
            isin=validated_data['isin'],
            scheme_name=validated_data['scheme_name'],
            fund_type=validated_data['fund_type'],
            amc_name=validated_data['amc_name'],
            fund_subtype=validated_data.get('fund_subtype')
        )
        
        return jsonify({
            "message": "Fund created successfully",
            "fund": {
                "isin": fund.isin,
                "scheme_name": fund.scheme_name,
                "fund_type": fund.fund_type,
                "fund_subtype": fund.fund_subtype,
                "amc_name": fund.amc_name
            }
        }), 201
    except SchemaValidationError as e:
        return jsonify({"error": e.messages}), 400

@api_bp.route('/funds/<string:isin>', methods=['GET'])
def get_fund(isin):
    """Get a fund by ISIN"""
    fund = FundService.get_fund(isin)
    
    return jsonify({
        "isin": fund.isin,
        "scheme_name": fund.scheme_name,
        "fund_type": fund.fund_type,
        "fund_subtype": fund.fund_subtype,
        "amc_name": fund.amc_name,
        "created_at": fund.created_at.isoformat() if fund.created_at else None,
        "updated_at": fund.updated_at.isoformat() if fund.updated_at else None
    }), 200

@api_bp.route('/funds/<string:isin>', methods=['DELETE'])
def delete_fund(isin):
    """Delete a fund by ISIN"""
    FundService.delete_fund(isin)
    
    return jsonify({
        "message": "Fund deleted successfully"
    }), 200

@api_bp.route('/funds/amc/<string:amc_name>', methods=['GET'])
def get_funds_by_amc(amc_name):
    """Get all funds for an AMC"""
    funds = FundService.get_funds_by_amc(amc_name)
    
    result = []
    for fund in funds:
        result.append({
            "isin": fund.isin,
            "scheme_name": fund.scheme_name,
            "fund_type": fund.fund_type,
            "fund_subtype": fund.fund_subtype,
            "amc_name": fund.amc_name
        })
    
    return jsonify(result), 200

@api_bp.route('/funds/type/<string:fund_type>', methods=['GET'])
def get_funds_by_type(fund_type):
    """Get all funds of a specific type"""
    funds = FundService.get_funds_by_type(fund_type)
    
    result = []
    for fund in funds:
        result.append({
            "isin": fund.isin,
            "scheme_name": fund.scheme_name,
            "fund_type": fund.fund_type,
            "fund_subtype": fund.fund_subtype,
            "amc_name": fund.amc_name
        })
    
    return jsonify(result), 200

# Factsheet routes
@api_bp.route('/funds/<string:isin>/factsheet', methods=['GET'])
def get_factsheet(isin):
    """Get a factsheet for a fund"""
    factsheet = FactSheetService.get_factsheet(isin)
    
    return jsonify({
        "isin": factsheet.isin,
        "fund_manager": factsheet.fund_manager,
        "aum": factsheet.aum,
        "expense_ratio": factsheet.expense_ratio,
        "launch_date": factsheet.launch_date.isoformat() if factsheet.launch_date else None,
        "exit_load": factsheet.exit_load,
        "last_updated": factsheet.last_updated.isoformat() if factsheet.last_updated else None
    }), 200

@api_bp.route('/funds/<string:isin>/factsheet', methods=['POST'])
def create_factsheet(isin):
    """Create a factsheet for a fund"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No input data provided"}), 400
        
        # Add isin to data before validation
        data['isin'] = isin
        validated_data = factsheet_schema.load(data)
        
        factsheet = FactSheetService.create_factsheet(
            isin=validated_data['isin'],
            fund_manager=validated_data.get('fund_manager'),
            aum=validated_data.get('aum'),
            expense_ratio=validated_data.get('expense_ratio'),
            launch_date=validated_data.get('launch_date'),
            exit_load=validated_data.get('exit_load')
        )
        
        return jsonify({
            "message": "Factsheet created successfully",
            "factsheet": {
                "isin": factsheet.isin,
                "fund_manager": factsheet.fund_manager,
                "aum": factsheet.aum,
                "expense_ratio": factsheet.expense_ratio,
                "launch_date": factsheet.launch_date.isoformat() if factsheet.launch_date else None,
                "exit_load": factsheet.exit_load,
                "last_updated": factsheet.last_updated.isoformat() if factsheet.last_updated else None
            }
        }), 201
    except SchemaValidationError as e:
        return jsonify({"error": e.messages}), 400

@api_bp.route('/funds/<string:isin>/factsheet', methods=['PUT', 'PATCH'])
def update_factsheet(isin):
    """Update a factsheet for a fund"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No input data provided"}), 400
        
        # Add isin to data before validation
        data['isin'] = isin
        validated_data = factsheet_schema.load(data, partial=True)
        
        # Remove isin from kwargs for update
        validated_data.pop('isin', None)
        
        factsheet = FactSheetService.update_factsheet(isin, **validated_data)
        
        return jsonify({
            "message": "Factsheet updated successfully",
            "factsheet": {
                "isin": factsheet.isin,
                "fund_manager": factsheet.fund_manager,
                "aum": factsheet.aum,
                "expense_ratio": factsheet.expense_ratio,
                "launch_date": factsheet.launch_date.isoformat() if factsheet.launch_date else None,
                "exit_load": factsheet.exit_load,
                "last_updated": factsheet.last_updated.isoformat() if factsheet.last_updated else None
            }
        }), 200
    except SchemaValidationError as e:
        return jsonify({"error": e.messages}), 400

@api_bp.route('/funds/<string:isin>/factsheet', methods=['DELETE'])
def delete_factsheet(isin):
    """Delete a factsheet for a fund"""
    FactSheetService.delete_factsheet(isin)
    
    return jsonify({
        "message": "Factsheet deleted successfully"
    }), 200

# Returns routes
@api_bp.route('/funds/<string:isin>/returns', methods=['GET'])
def get_returns(isin):
    """Get returns data for a fund"""
    returns = ReturnsService.get_returns(isin)
    
    return jsonify({
        "isin": returns.isin,
        "return_1m": returns.return_1m,
        "return_3m": returns.return_3m,
        "return_6m": returns.return_6m,
        "return_ytd": returns.return_ytd,
        "return_1y": returns.return_1y,
        "return_3y": returns.return_3y,
        "return_5y": returns.return_5y,
        "last_updated": returns.last_updated.isoformat() if returns.last_updated else None
    }), 200

@api_bp.route('/funds/<string:isin>/returns', methods=['POST'])
def create_returns(isin):
    """Create returns data for a fund"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No input data provided"}), 400
        
        # Add isin to data before validation
        data['isin'] = isin
        validated_data = returns_schema.load(data)
        
        returns = ReturnsService.create_returns(
            isin=validated_data['isin'],
            return_1m=validated_data.get('return_1m'),
            return_3m=validated_data.get('return_3m'),
            return_6m=validated_data.get('return_6m'),
            return_ytd=validated_data.get('return_ytd'),
            return_1y=validated_data.get('return_1y'),
            return_3y=validated_data.get('return_3y'),
            return_5y=validated_data.get('return_5y')
        )
        
        return jsonify({
            "message": "Returns data created successfully",
            "returns": {
                "isin": returns.isin,
                "return_1m": returns.return_1m,
                "return_3m": returns.return_3m,
                "return_6m": returns.return_6m,
                "return_ytd": returns.return_ytd,
                "return_1y": returns.return_1y,
                "return_3y": returns.return_3y,
                "return_5y": returns.return_5y,
                "last_updated": returns.last_updated.isoformat() if returns.last_updated else None
            }
        }), 201
    except SchemaValidationError as e:
        return jsonify({"error": e.messages}), 400

@api_bp.route('/funds/<string:isin>/returns', methods=['PUT', 'PATCH'])
def update_returns(isin):
    """Update returns data for a fund"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No input data provided"}), 400
        
        # Add isin to data before validation
        data['isin'] = isin
        validated_data = returns_schema.load(data, partial=True)
        
        # Remove isin from kwargs for update
        validated_data.pop('isin', None)
        
        returns = ReturnsService.update_returns(isin, **validated_data)
        
        return jsonify({
            "message": "Returns data updated successfully",
            "returns": {
                "isin": returns.isin,
                "return_1m": returns.return_1m,
                "return_3m": returns.return_3m,
                "return_6m": returns.return_6m,
                "return_ytd": returns.return_ytd,
                "return_1y": returns.return_1y,
                "return_3y": returns.return_3y,
                "return_5y": returns.return_5y,
                "last_updated": returns.last_updated.isoformat() if returns.last_updated else None
            }
        }), 200
    except SchemaValidationError as e:
        return jsonify({"error": e.messages}), 400

@api_bp.route('/funds/<string:isin>/returns', methods=['DELETE'])
def delete_returns(isin):
    """Delete returns data for a fund"""
    ReturnsService.delete_returns(isin)
    
    return jsonify({
        "message": "Returns data deleted successfully"
    }), 200

# Portfolio holdings routes
@api_bp.route('/funds/<string:isin>/holdings', methods=['GET'])
def get_holdings(isin):
    """Get all holdings for a fund"""
    holdings = PortfolioService.get_holdings_by_fund(isin)
    
    result = []
    for holding in holdings:
        result.append({
            "id": holding.id,
            "isin": holding.isin,
            "instrument_isin": holding.instrument_isin,
            "coupon": holding.coupon,
            "instrument_name": holding.instrument_name,
            "sector": holding.sector,
            "quantity": holding.quantity,
            "value": holding.value,
            "percentage_to_nav": holding.percentage_to_nav,
            "yield_value": holding.yield_value,
            "instrument_type": holding.instrument_type,
            "last_updated": holding.last_updated.isoformat() if holding.last_updated else None
        })
    
    return jsonify(result), 200

@api_bp.route('/funds/<string:isin>/holdings', methods=['POST'])
def create_holding(isin):
    """Create a portfolio holding for a fund"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No input data provided"}), 400
        
        # Add isin to data before validation
        data['isin'] = isin
        validated_data = portfolio_holding_schema.load(data)
        
        holding = PortfolioService.create_holding(
            isin=validated_data['isin'],
            instrument_name=validated_data['instrument_name'],
            percentage_to_nav=validated_data['percentage_to_nav'],
            instrument_type=validated_data['instrument_type'],
            instrument_isin=validated_data.get('instrument_isin'),
            coupon=validated_data.get('coupon'),
            sector=validated_data.get('sector'),
            quantity=validated_data.get('quantity'),
            value=validated_data.get('value'),
            yield_value=validated_data.get('yield_value')
        )
        
        return jsonify({
            "message": "Portfolio holding created successfully",
            "holding": {
                "id": holding.id,
                "isin": holding.isin,
                "instrument_isin": holding.instrument_isin,
                "coupon": holding.coupon,
                "instrument_name": holding.instrument_name,
                "sector": holding.sector,
                "quantity": holding.quantity,
                "value": holding.value,
                "percentage_to_nav": holding.percentage_to_nav,
                "yield_value": holding.yield_value,
                "instrument_type": holding.instrument_type,
                "last_updated": holding.last_updated.isoformat() if holding.last_updated else None
            }
        }), 201
    except SchemaValidationError as e:
        return jsonify({"error": e.messages}), 400

@api_bp.route('/holdings/<int:holding_id>', methods=['GET'])
def get_holding(holding_id):
    """Get a portfolio holding by ID"""
    holding = PortfolioService.get_holding(holding_id)
    
    return jsonify({
        "id": holding.id,
        "isin": holding.isin,
        "instrument_isin": holding.instrument_isin,
        "coupon": holding.coupon,
        "instrument_name": holding.instrument_name,
        "sector": holding.sector,
        "quantity": holding.quantity,
        "value": holding.value,
        "percentage_to_nav": holding.percentage_to_nav,
        "yield_value": holding.yield_value,
        "instrument_type": holding.instrument_type,
        "last_updated": holding.last_updated.isoformat() if holding.last_updated else None
    }), 200

@api_bp.route('/holdings/<int:holding_id>', methods=['PUT', 'PATCH'])
def update_holding(holding_id):
    """Update a portfolio holding"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No input data provided"}), 400
        
        validated_data = portfolio_holding_schema.load(data, partial=True)
        
        # Remove isin from kwargs for update
        validated_data.pop('isin', None)
        
        holding = PortfolioService.update_holding(holding_id, **validated_data)
        
        return jsonify({
            "message": "Portfolio holding updated successfully",
            "holding": {
                "id": holding.id,
                "isin": holding.isin,
                "instrument_isin": holding.instrument_isin,
                "coupon": holding.coupon,
                "instrument_name": holding.instrument_name,
                "sector": holding.sector,
                "quantity": holding.quantity,
                "value": holding.value,
                "percentage_to_nav": holding.percentage_to_nav,
                "yield_value": holding.yield_value,
                "instrument_type": holding.instrument_type,
                "last_updated": holding.last_updated.isoformat() if holding.last_updated else None
            }
        }), 200
    except SchemaValidationError as e:
        return jsonify({"error": e.messages}), 400

@api_bp.route('/holdings/<int:holding_id>', methods=['DELETE'])
def delete_holding(holding_id):
    """Delete a portfolio holding"""
    PortfolioService.delete_holding(holding_id)
    
    return jsonify({
        "message": "Portfolio holding deleted successfully"
    }), 200

@api_bp.route('/funds/<string:isin>/holdings', methods=['DELETE'])
def delete_all_holdings(isin):
    """Delete all holdings for a fund"""
    PortfolioService.delete_all_holdings(isin)
    
    return jsonify({
        "message": "All portfolio holdings deleted successfully"
    }), 200

# NAV history routes
@api_bp.route('/funds/<string:isin>/nav', methods=['GET'])
def get_nav_history(isin):
    """Get NAV history for a fund"""
    start_date_str = request.args.get('start_date')
    end_date_str = request.args.get('end_date')
    
    start_date = None
    end_date = None
    
    if start_date_str:
        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
        except ValueError:
            return jsonify({"error": "Invalid start_date format. Use YYYY-MM-DD"}), 400
    
    if end_date_str:
        try:
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
        except ValueError:
            return jsonify({"error": "Invalid end_date format. Use YYYY-MM-DD"}), 400
    
    nav_history = NavHistoryService.get_nav_history(isin, start_date, end_date)
    
    result = []
    for nav_entry in nav_history:
        result.append({
            "id": nav_entry.id,
            "isin": nav_entry.isin,
            "date": nav_entry.date.isoformat() if nav_entry.date else None,
            "nav": nav_entry.nav
        })
    
    return jsonify(result), 200

@api_bp.route('/funds/<string:isin>/nav', methods=['POST'])
def create_nav(isin):
    """Create a NAV history entry for a fund"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No input data provided"}), 400
        
        # Add isin to data before validation
        data['isin'] = isin
        validated_data = nav_history_schema.load(data)
        
        nav_entry = NavHistoryService.create_nav(
            isin=validated_data['isin'],
            date=validated_data['date'],
            nav=validated_data['nav']
        )
        
        return jsonify({
            "message": "NAV history entry created successfully",
            "nav": {
                "id": nav_entry.id,
                "isin": nav_entry.isin,
                "date": nav_entry.date.isoformat() if nav_entry.date else None,
                "nav": nav_entry.nav
            }
        }), 201
    except SchemaValidationError as e:
        return jsonify({"error": e.messages}), 400

@api_bp.route('/nav/<int:nav_id>', methods=['GET'])
def get_nav(nav_id):
    """Get a NAV history entry by ID"""
    nav_entry = NavHistoryService.get_nav(nav_id)
    
    return jsonify({
        "id": nav_entry.id,
        "isin": nav_entry.isin,
        "date": nav_entry.date.isoformat() if nav_entry.date else None,
        "nav": nav_entry.nav
    }), 200

@api_bp.route('/nav/<int:nav_id>', methods=['PUT', 'PATCH'])
def update_nav(nav_id):
    """Update a NAV history entry"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No input data provided"}), 400
        
        if 'nav' not in data:
            return jsonify({"error": "NAV value is required"}), 400
        
        nav_entry = NavHistoryService.update_nav(nav_id, data['nav'])
        
        return jsonify({
            "message": "NAV history entry updated successfully",
            "nav": {
                "id": nav_entry.id,
                "isin": nav_entry.isin,
                "date": nav_entry.date.isoformat() if nav_entry.date else None,
                "nav": nav_entry.nav
            }
        }), 200
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

@api_bp.route('/nav/<int:nav_id>', methods=['DELETE'])
def delete_nav(nav_id):
    """Delete a NAV history entry"""
    NavHistoryService.delete_nav(nav_id)
    
    return jsonify({
        "message": "NAV history entry deleted successfully"
    }), 200

@api_bp.route('/funds/<string:isin>/nav', methods=['DELETE'])
def delete_nav_history(isin):
    """Delete all NAV history for a fund"""
    NavHistoryService.delete_nav_history(isin)
    
    return jsonify({
        "message": "All NAV history deleted successfully"
    }), 200

def setup_routes(app):
    """Register blueprint with the Flask application"""
    app.register_blueprint(api_bp, url_prefix='/api')
    logging.info("API routes registered successfully")