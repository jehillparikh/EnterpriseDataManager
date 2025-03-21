from flask import Blueprint, request, jsonify, make_response
from werkzeug.security import generate_password_hash
from functools import wraps
import jwt
import datetime
import os
import logging

from models import db, UserInfo
from marshmallow import ValidationError as SchemaValidationError
from services import (
    UserService, KycService, BankService, FundService, PortfolioService,
    ResourceNotFoundError, ValidationError, UniqueConstraintError, AuthenticationError, DatabaseError
)
from schemas import (
    user_registration_schema, user_login_schema, user_update_schema,
    kyc_detail_schema, bank_repo_schema, branch_repo_schema, bank_detail_schema, mandate_schema,
    amc_schema, fund_schema, fund_scheme_schema, fund_scheme_detail_schema,
    mutual_fund_schema, user_portfolio_schema, fund_factsheet_schema, returns_schema, fund_holding_schema
)

logger = logging.getLogger(__name__)

# Create Blueprint for API routes
api_bp = Blueprint('api', __name__, url_prefix='/api')

# Secret key for JWT
JWT_SECRET_KEY = os.environ.get("JWT_SECRET_KEY", "dev_secret_key")

# Authentication middleware
def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = None
        
        # Get token from Authorization header
        auth_header = request.headers.get('Authorization')
        if auth_header and auth_header.startswith('Bearer '):
            token = auth_header.split(' ')[1]
        
        if not token:
            return jsonify({'message': 'Token is missing!'}), 401
        
        try:
            # Decode token
            print(f"DEBUG - Decoding token: {token}")
            data = jwt.decode(token, JWT_SECRET_KEY, algorithms=["HS256"])
            print(f"DEBUG - Token data: {data}")
            # Convert sub (string) back to int for database lookup
            user_id = int(data['sub'])
            current_user = UserService.get_user(user_id)
            print(f"DEBUG - Found user: {current_user.id}")
        except jwt.ExpiredSignatureError as e:
            print(f"DEBUG - Token expired: {str(e)}")
            return jsonify({'message': 'Token has expired!'}), 401
        except jwt.InvalidTokenError as e:
            print(f"DEBUG - Invalid token: {str(e)}")
            return jsonify({'message': 'Invalid token!'}), 401
        except ResourceNotFoundError as e:
            print(f"DEBUG - User not found: {str(e)}")
            return jsonify({'message': 'User not found!'}), 401
        except Exception as e:
            print(f"DEBUG - Unexpected error: {str(e)}")
            return jsonify({'message': f'Error processing token: {str(e)}'}), 401
        
        return f(current_user, *args, **kwargs)
    
    return decorated

# Error handler for API routes
@api_bp.errorhandler(Exception)
def handle_error(error):
    """Global error handler for API routes"""
    logger.error(f"API error: {str(error)}")
    
    if isinstance(error, ResourceNotFoundError):
        return jsonify({"error": str(error)}), 404
    elif isinstance(error, (ValidationError, UniqueConstraintError, SchemaValidationError, AuthenticationError)):
        return jsonify({"error": str(error)}), 400
    elif isinstance(error, DatabaseError):
        return jsonify({"error": str(error)}), 500
    else:
        return jsonify({"error": "Internal server error"}), 500

# User Registration and Authentication Routes
@api_bp.route('/auth/register', methods=['POST'])
def register():
    """Register a new user"""
    try:
        # Validate request data
        data = request.get_json()
        if not data:
            return jsonify({"error": "No input data provided"}), 400
            
        validated_data = user_registration_schema.load(data)
        
        # Register user
        user = UserService.register_user(
            email=validated_data['email'],
            mobile_number=validated_data['mobile_number'],
            password=validated_data['password']
        )
        
        # Return user data (without password)
        return jsonify({
            "message": "User registered successfully",
            "user": {
                "id": user.id,
                "email": user.email,
                "mobile_number": user.mobile_number
            }
        }), 201
    except SchemaValidationError as e:
        return jsonify({"error": e.messages}), 400

@api_bp.route('/auth/login', methods=['POST'])
def login():
    """Authenticate a user and return a token"""
    try:
        # Validate request data
        data = request.get_json()
        if not data:
            return jsonify({"error": "No input data provided"}), 400
            
        validated_data = user_login_schema.load(data)
        
        # Authenticate user
        user = UserService.authenticate_user(
            email=validated_data['email'],
            password=validated_data['password']
        )
        
        # Generate token
        token = jwt.encode(
            {
                'sub': str(user.id),  # Convert ID to string for JWT compliance
                'iat': datetime.datetime.utcnow(),
                'exp': datetime.datetime.utcnow() + datetime.timedelta(hours=24)
            },
            JWT_SECRET_KEY,
            algorithm="HS256"
        )
        
        return jsonify({
            "message": "Login successful",
            "token": token,
            "user": {
                "id": user.id,
                "email": user.email,
                "mobile_number": user.mobile_number
            }
        }), 200
    except (SchemaValidationError, AuthenticationError) as e:
        return jsonify({"error": str(e)}), 401

# User Management Routes
@api_bp.route('/users/profile', methods=['GET'])
@token_required
def get_user_profile(current_user):
    """Get the profile of the authenticated user"""
    return jsonify({
        "id": current_user.id,
        "email": current_user.email,
        "mobile_number": current_user.mobile_number,
        "created_at": current_user.created_at.isoformat() if current_user.created_at else None,
        "updated_at": current_user.updated_at.isoformat() if current_user.updated_at else None
    }), 200

@api_bp.route('/users/profile', methods=['PUT', 'PATCH'])
@token_required
def update_user_profile(current_user):
    """Update the profile of the authenticated user"""
    try:
        # Validate request data
        data = request.get_json()
        if not data:
            return jsonify({"error": "No input data provided"}), 400
            
        validated_data = user_update_schema.load(data)
        
        # Update user
        user = UserService.update_user(
            user_id=current_user.id,
            email=validated_data.get('email'),
            mobile_number=validated_data.get('mobile_number'),
            password=validated_data.get('password')
        )
        
        return jsonify({
            "message": "Profile updated successfully",
            "user": {
                "id": user.id,
                "email": user.email,
                "mobile_number": user.mobile_number,
                "updated_at": user.updated_at.isoformat() if user.updated_at else None
            }
        }), 200
    except SchemaValidationError as e:
        return jsonify({"error": e.messages}), 400

@api_bp.route('/users/profile', methods=['DELETE'])
@token_required
def delete_user_profile(current_user):
    """Delete the authenticated user's account"""
    try:
        UserService.delete_user(current_user.id)
        return jsonify({"message": "Account deleted successfully"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# KYC Routes
@api_bp.route('/users/kyc', methods=['POST'])
@token_required
def create_kyc(current_user):
    """Create KYC details for the authenticated user"""
    try:
        # Validate request data
        data = request.get_json()
        if not data:
            return jsonify({"error": "No input data provided"}), 400
            
        validated_data = kyc_detail_schema.load(data)
        
        # Create KYC details
        kyc = KycService.create_kyc(
            user_id=current_user.id,
            pan=validated_data['pan'],
            tax_status=validated_data.get('tax_status', '01'),
            occ_code=validated_data.get('occ_code', '02'),
            first_name=validated_data['first_name'],
            middle_name=validated_data.get('middle_name'),
            last_name=validated_data['last_name'],
            dob=validated_data['dob'],
            gender=validated_data['gender'],
            address=validated_data['address'],
            city=validated_data['city'],
            state=validated_data['state'],
            pincode=validated_data['pincode'],
            phone=validated_data.get('phone'),
            income_slab=validated_data['income_slab']
        )
        
        return jsonify({
            "message": "KYC details added successfully",
            "kyc": {
                "id": kyc.id,
                "user_id": kyc.user_id,
                "pan": kyc.pan,
                "first_name": kyc.first_name,
                "last_name": kyc.last_name
            }
        }), 201
    except SchemaValidationError as e:
        return jsonify({"error": e.messages}), 400

@api_bp.route('/users/kyc', methods=['GET'])
@token_required
def get_kyc(current_user):
    """Get KYC details of the authenticated user"""
    try:
        kyc = KycService.get_kyc(current_user.id)
        
        return jsonify({
            "id": kyc.id,
            "user_id": kyc.user_id,
            "pan": kyc.pan,
            "tax_status": kyc.tax_status,
            "occ_code": kyc.occ_code,
            "first_name": kyc.first_name,
            "middle_name": kyc.middle_name,
            "last_name": kyc.last_name,
            "dob": kyc.dob,
            "gender": kyc.gender,
            "address": kyc.address,
            "city": kyc.city,
            "state": kyc.state,
            "pincode": kyc.pincode,
            "phone": kyc.phone,
            "income_slab": kyc.income_slab
        }), 200
    except ResourceNotFoundError:
        return jsonify({"error": "KYC details not found"}), 404

@api_bp.route('/users/kyc', methods=['PUT', 'PATCH'])
@token_required
def update_kyc(current_user):
    """Update KYC details of the authenticated user"""
    try:
        # Validate request data
        data = request.get_json()
        if not data:
            return jsonify({"error": "No input data provided"}), 400
            
        validated_data = kyc_detail_schema.load(data, partial=True)
        
        # Update KYC details
        kyc = KycService.update_kyc(current_user.id, **validated_data)
        
        return jsonify({
            "message": "KYC details updated successfully",
            "kyc": {
                "id": kyc.id,
                "user_id": kyc.user_id,
                "pan": kyc.pan,
                "first_name": kyc.first_name,
                "last_name": kyc.last_name
            }
        }), 200
    except SchemaValidationError as e:
        return jsonify({"error": e.messages}), 400
    except ResourceNotFoundError:
        return jsonify({"error": "KYC details not found"}), 404

# Bank Routes
@api_bp.route('/banks', methods=['GET'])
@token_required
def get_banks(current_user):
    """Get all banks"""
    banks = BankService.get_all_banks()
    
    return jsonify([{
        "id": bank.id,
        "name": bank.name
    } for bank in banks]), 200

@api_bp.route('/banks/<int:bank_id>/branches', methods=['GET'])
@token_required
def get_branches(current_user, bank_id):
    """Get all branches for a bank"""
    try:
        branches = BankService.get_branches_by_bank(bank_id)
        
        return jsonify([{
            "id": branch.id,
            "bank_id": branch.bank_id,
            "branch_name": branch.branch_name,
            "branch_city": branch.branch_city,
            "branch_address": branch.branch_address,
            "ifsc_code": branch.ifsc_code,
            "micr_code": branch.micr_code
        } for branch in branches]), 200
    except ResourceNotFoundError:
        return jsonify({"error": f"Bank with ID {bank_id} not found"}), 404

@api_bp.route('/users/bank-details', methods=['POST'])
@token_required
def add_bank_detail(current_user):
    """Add bank details for the authenticated user"""
    try:
        # Validate request data
        data = request.get_json()
        if not data:
            return jsonify({"error": "No input data provided"}), 400
            
        validated_data = bank_detail_schema.load(data)
        
        # Create bank details
        bank_detail = BankService.create_bank_detail(
            user_id=current_user.id,
            branch_id=validated_data['branch_id'],
            account_number=validated_data['account_number'],
            account_type_bse=validated_data['account_type_bse']
        )
        
        return jsonify({
            "message": "Bank details added successfully",
            "bank_detail": {
                "id": bank_detail.id,
                "user_id": bank_detail.user_id,
                "branch_id": bank_detail.branch_id,
                "account_number": bank_detail.account_number,
                "account_type_bse": bank_detail.account_type_bse
            }
        }), 201
    except SchemaValidationError as e:
        return jsonify({"error": e.messages}), 400

@api_bp.route('/users/bank-details', methods=['GET'])
@token_required
def get_bank_details(current_user):
    """Get all bank details for the authenticated user"""
    try:
        bank_details = BankService.get_bank_details(current_user.id)
        
        return jsonify([{
            "id": detail.id,
            "user_id": detail.user_id,
            "branch_id": detail.branch_id,
            "account_number": detail.account_number,
            "account_type_bse": detail.account_type_bse
        } for detail in bank_details]), 200
    except ResourceNotFoundError:
        return jsonify({"error": "Bank details not found"}), 404

# AMC Routes
@api_bp.route('/amcs', methods=['GET'])
def get_amcs():
    """Get all unique AMCs from funds"""
    # Get unique AMC entries from funds
    funds = Fund.query.with_entities(
        Fund.amc_name, 
        Fund.amc_short_name,
        Fund.fund_code,
        Fund.bse_code,
        Fund.active
    ).distinct().all()
    
    return jsonify([{
        "name": fund.amc_name,
        "short_name": fund.amc_short_name,
        "fund_code": fund.fund_code,
        "bse_code": fund.bse_code,
        "active": fund.active
    } for fund in funds]), 200

@api_bp.route('/amcs', methods=['POST'])
@token_required
def create_amc(current_user):
    """Create a new AMC"""
    try:
        # Validate request data
        data = request.get_json()
        if not data:
            return jsonify({"error": "No input data provided"}), 400
            
        validated_data = amc_schema.load(data)
        
        # Create AMC
        amc = FundService.create_amc(
            name=validated_data['name'],
            short_name=validated_data['short_name'],
            fund_code=validated_data.get('fund_code'),
            bse_code=validated_data.get('bse_code'),
            active=validated_data.get('active', True)
        )
        
        return jsonify({
            "message": "AMC created successfully",
            "amc": {
                "id": amc.id,
                "name": amc.name,
                "short_name": amc.short_name,
                "fund_code": amc.fund_code,
                "bse_code": amc.bse_code,
                "active": amc.active
            }
        }), 201
    except SchemaValidationError as e:
        return jsonify({"error": e.messages}), 400

@api_bp.route('/amcs/<string:amc_name>', methods=['GET'])
def get_amc(amc_name):
    """Get an AMC by name"""
    try:
        funds = FundService.get_funds_by_amc_name(amc_name)
        
        # Return first fund's AMC details since AMCs are now part of the Fund model
        if funds:
            fund = funds[0]
            return jsonify({
                "amc_name": fund.amc_name,
                "amc_short_name": fund.amc_short_name,
                "fund_code": fund.fund_code,
                "bse_code": fund.bse_code,
                "active": fund.active
            }), 200
        else:
            raise ResourceNotFoundError(f"No funds found for AMC {amc_name}")
    except ResourceNotFoundError:
        return jsonify({"error": f"AMC with name {amc_name} not found"}), 404

# Fund Routes
@api_bp.route('/funds', methods=['POST'])
@token_required
def create_fund_direct(current_user):
    """Create a new fund directly"""
    try:
        # Validate request data
        data = request.get_json()
        if not data:
            return jsonify({"error": "No input data provided"}), 400
            
        validated_data = data
        
        # Create fund
        fund = FundService.create_fund(
            name=validated_data['name'],
            amc_name=validated_data['amc_name'],
            amc_short_name=validated_data['amc_short_name'],
            short_name=validated_data.get('short_name'),
            fund_code=validated_data.get('fund_code'),
            rta_code=validated_data.get('rta_code'),
            bse_code=validated_data.get('bse_code'),
            fund_type=validated_data.get('fund_type'),
            fund_category=validated_data.get('fund_category'),
            active=validated_data.get('active', True),
            direct=validated_data.get('direct', False)
        )
        
        return jsonify({
            "id": fund.id,
            "name": fund.name,
            "amc_name": fund.amc_name,
            "amc_short_name": fund.amc_short_name,
            "short_name": fund.short_name,
            "fund_code": fund.fund_code,
            "rta_code": fund.rta_code,
            "bse_code": fund.bse_code,
            "fund_type": fund.fund_type,
            "fund_category": fund.fund_category,
            "active": fund.active,
            "direct": fund.direct
        }), 201
    except SchemaValidationError as e:
        return jsonify({"error": e.messages}), 400
    except UniqueConstraintError as e:
        return jsonify({"error": str(e)}), 409
    except Exception as e:
        return jsonify({"error": f"Error creating fund: {str(e)}"}), 500

@api_bp.route('/amcs/<string:amc_name>/funds', methods=['GET'])
def get_funds_by_amc(amc_name):
    """Get all funds for an AMC by name"""
    try:
        funds = FundService.get_funds_by_amc_name(amc_name)
        
        return jsonify([{
            "id": fund.id,
            "name": fund.name,
            "short_name": fund.short_name,
            "amc_name": fund.amc_name,
            "amc_short_name": fund.amc_short_name,
            "rta_code": fund.rta_code,
            "bse_code": fund.bse_code,
            "fund_type": fund.fund_type,
            "fund_category": fund.fund_category,
            "active": fund.active,
            "direct": fund.direct
        } for fund in funds]), 200
    except ResourceNotFoundError:
        return jsonify({"error": f"AMC with name {amc_name} not found"}), 404
        
@api_bp.route('/amcs/<string:amc_name>/funds', methods=['POST'])
@token_required
def create_fund(current_user, amc_name):
    """Create a new fund under an AMC by name"""
    try:
        # Validate request data
        data = request.get_json()
        if not data:
            return jsonify({"error": "No input data provided"}), 400
            
        # Extract AMC details from the request or use the path parameter
        amc_name = data.get('amc_name', amc_name)
        amc_short_name = data.get('amc_short_name', '')
        validated_data = data
        
        # Create fund
        fund = FundService.create_fund(
            name=validated_data['name'],
            amc_name=amc_name,
            amc_short_name=amc_short_name,
            short_name=validated_data.get('short_name'),
            fund_code=validated_data.get('fund_code'),
            rta_code=validated_data.get('rta_code'),
            bse_code=validated_data.get('bse_code'),
            fund_type=validated_data.get('fund_type'),
            fund_category=validated_data.get('fund_category'),
            active=validated_data.get('active', True),
            direct=validated_data.get('direct', False)
        )
        
        return jsonify({
            "message": "Fund created successfully",
            "fund": {
                "id": fund.id,
                "name": fund.name,
                "amc_name": fund.amc_name,
                "amc_short_name": fund.amc_short_name,
                "short_name": fund.short_name,
                "rta_code": fund.rta_code,
                "bse_code": fund.bse_code,
                "fund_type": fund.fund_type,
                "fund_category": fund.fund_category,
                "active": fund.active,
                "direct": fund.direct
            }
        }), 201
    except SchemaValidationError as e:
        return jsonify({"error": e.messages}), 400
    except ResourceNotFoundError as e:
        return jsonify({"error": str(e)}), 404

@api_bp.route('/funds/<int:fund_id>', methods=['GET'])
def get_fund(fund_id):
    """Get a fund by ID"""
    try:
        fund = FundService.get_fund(fund_id)
        
        return jsonify({
            "id": fund.id,
            "name": fund.name,
            "amc_name": fund.amc_name,
            "amc_short_name": fund.amc_short_name,
            "short_name": fund.short_name,
            "rta_code": fund.rta_code,
            "bse_code": fund.bse_code,
            "fund_type": fund.fund_type,
            "fund_category": fund.fund_category,
            "active": fund.active,
            "direct": fund.direct
        }), 200
    except ResourceNotFoundError:
        return jsonify({"error": f"Fund with ID {fund_id} not found"}), 404

@api_bp.route('/funds/<int:fund_id>/schemes', methods=['GET'])
def get_schemes_by_fund(fund_id):
    """Get all schemes for a fund"""
    try:
        schemes = FundService.get_schemes_by_fund(fund_id)
        
        return jsonify([{
            "id": scheme.id,
            "fund_id": scheme.fund_id,
            "scheme_code": scheme.scheme_code,
            "plan": scheme.plan,
            "option": scheme.option,
            "bse_code": scheme.bse_code
        } for scheme in schemes]), 200
    except ResourceNotFoundError:
        return jsonify({"error": f"Fund with ID {fund_id} not found"}), 404
        
@api_bp.route('/funds/<int:fund_id>/schemes', methods=['POST'])
@token_required
def create_fund_scheme(current_user, fund_id):
    """Create a new fund scheme under a fund"""
    try:
        # Validate request data
        data = request.get_json()
        if not data:
            return jsonify({"error": "No input data provided"}), 400
            
        # Add fund_id to the data before validation
        data['fund_id'] = fund_id
        validated_data = fund_scheme_schema.load(data)
        
        # Create fund scheme
        scheme = FundService.create_fund_scheme(
            fund_id=validated_data['fund_id'],
            scheme_code=validated_data['scheme_code'],
            plan=validated_data['plan'],
            option=validated_data.get('option'),
            bse_code=validated_data.get('bse_code')
        )
        
        return jsonify({
            "message": "Fund scheme created successfully",
            "scheme": {
                "id": scheme.id,
                "fund_id": scheme.fund_id,
                "scheme_code": scheme.scheme_code,
                "plan": scheme.plan,
                "option": scheme.option,
                "bse_code": scheme.bse_code
            }
        }), 201
    except SchemaValidationError as e:
        return jsonify({"error": e.messages}), 400
    except ResourceNotFoundError as e:
        return jsonify({"error": str(e)}), 404

@api_bp.route('/schemes/<int:scheme_id>', methods=['GET'])
def get_fund_scheme(scheme_id):
    """Get a fund scheme by ID"""
    try:
        scheme = FundService.get_fund_scheme(scheme_id)
        
        response_data = {
            "id": scheme.id,
            "fund_id": scheme.fund_id,
            "scheme_code": scheme.scheme_code,
            "plan": scheme.plan,
            "option": scheme.option,
            "bse_code": scheme.bse_code
        }
        
        # Try to get scheme details if available
        try:
            details = FundService.get_fund_scheme_detail(scheme_id)
            response_data["details"] = {
                "nav": details.nav,
                "expense_ratio": details.expense_ratio,
                "fund_manager": details.fund_manager,
                "aum": details.aum,
                "risk_level": details.risk_level,
                "benchmark": details.benchmark
            }
        except ResourceNotFoundError:
            pass
            
        return jsonify(response_data), 200
    except ResourceNotFoundError:
        return jsonify({"error": f"Scheme with ID {scheme_id} not found"}), 404
        
@api_bp.route('/schemes/<int:scheme_id>/details', methods=['POST'])
@token_required
def create_scheme_details(current_user, scheme_id):
    """Create details for a fund scheme"""
    try:
        # Validate request data
        data = request.get_json()
        if not data:
            return jsonify({"error": "No input data provided"}), 400
            
        # Add scheme_id to the data before validation
        data['scheme_id'] = scheme_id
        validated_data = fund_scheme_detail_schema.load(data)
        
        # Create fund scheme details
        details = FundService.create_fund_scheme_detail(
            scheme_id=validated_data['scheme_id'],
            nav=validated_data['nav'],
            expense_ratio=validated_data.get('expense_ratio'),
            fund_manager=validated_data.get('fund_manager'),
            aum=validated_data.get('aum'),
            risk_level=validated_data.get('risk_level'),
            benchmark=validated_data.get('benchmark')
        )
        
        return jsonify({
            "message": "Fund scheme details created successfully",
            "details": {
                "id": details.id,
                "scheme_id": details.scheme_id,
                "nav": details.nav,
                "expense_ratio": details.expense_ratio,
                "fund_manager": details.fund_manager,
                "aum": details.aum,
                "risk_level": details.risk_level,
                "benchmark": details.benchmark
            }
        }), 201
    except SchemaValidationError as e:
        return jsonify({"error": e.messages}), 400
    except ResourceNotFoundError as e:
        return jsonify({"error": str(e)}), 404
    except UniqueConstraintError as e:
        return jsonify({"error": str(e)}), 409


# Fund Factsheet Routes
@api_bp.route('/schemes/<int:scheme_id>/factsheet', methods=['POST'])
@token_required
def create_fund_factsheet(current_user, scheme_id):
    """Create a factsheet for a fund scheme"""
    try:
        # Validate request data
        data = request.get_json()
        if not data:
            return jsonify({"error": "No input data provided"}), 400
            
        # Add scheme_id to the data before validation
        data['scheme_id'] = scheme_id
        validated_data = fund_factsheet_schema.load(data)
        
        # Create fund factsheet
        factsheet = FundService.create_fund_factsheet(
            scheme_id=validated_data['scheme_id'],
            fund_manager=validated_data.get('fund_manager'),
            fund_house=validated_data.get('fund_house'),
            inception_date=validated_data.get('inception_date'),
            expense_ratio=validated_data.get('expense_ratio'),
            benchmark_index=validated_data.get('benchmark_index'),
            category=validated_data.get('category'),
            risk_level=validated_data.get('risk_level'),
            aum=validated_data.get('aum'),
            exit_load=validated_data.get('exit_load'),
            holdings_count=validated_data.get('holdings_count')
        )
        
        return jsonify({
            "message": "Fund factsheet created successfully",
            "factsheet": {
                "id": factsheet.id,
                "scheme_id": factsheet.scheme_id,
                "fund_manager": factsheet.fund_manager,
                "fund_house": factsheet.fund_house,
                "inception_date": factsheet.inception_date.isoformat() if factsheet.inception_date else None,
                "expense_ratio": factsheet.expense_ratio,
                "benchmark_index": factsheet.benchmark_index,
                "category": factsheet.category,
                "risk_level": factsheet.risk_level,
                "aum": factsheet.aum,
                "exit_load": factsheet.exit_load,
                "holdings_count": factsheet.holdings_count,
                "last_updated": factsheet.last_updated.isoformat() if factsheet.last_updated else None
            }
        }), 201
    except SchemaValidationError as e:
        return jsonify({"error": e.messages}), 400
    except ResourceNotFoundError as e:
        return jsonify({"error": str(e)}), 404
    except UniqueConstraintError as e:
        return jsonify({"error": str(e)}), 409


@api_bp.route('/schemes/<int:scheme_id>/factsheet', methods=['GET'])
def get_fund_factsheet(scheme_id):
    """Get a factsheet for a fund scheme"""
    try:
        factsheet = FundService.get_fund_factsheet(scheme_id)
        
        return jsonify({
            "id": factsheet.id,
            "scheme_id": factsheet.scheme_id,
            "fund_manager": factsheet.fund_manager,
            "fund_house": factsheet.fund_house,
            "inception_date": factsheet.inception_date.isoformat() if factsheet.inception_date else None,
            "expense_ratio": factsheet.expense_ratio,
            "benchmark_index": factsheet.benchmark_index,
            "category": factsheet.category,
            "risk_level": factsheet.risk_level,
            "aum": factsheet.aum,
            "exit_load": factsheet.exit_load,
            "holdings_count": factsheet.holdings_count,
            "last_updated": factsheet.last_updated.isoformat() if factsheet.last_updated else None
        }), 200
    except ResourceNotFoundError as e:
        return jsonify({"error": str(e)}), 404


@api_bp.route('/schemes/<int:scheme_id>/factsheet', methods=['PUT', 'PATCH'])
@token_required
def update_fund_factsheet(current_user, scheme_id):
    """Update a factsheet for a fund scheme"""
    try:
        # Validate request data
        data = request.get_json()
        if not data:
            return jsonify({"error": "No input data provided"}), 400
            
        validated_data = fund_factsheet_schema.load(data, partial=True)
        
        # Update fund factsheet
        factsheet = FundService.update_fund_factsheet(
            scheme_id=scheme_id,
            **validated_data
        )
        
        return jsonify({
            "message": "Fund factsheet updated successfully",
            "factsheet": {
                "id": factsheet.id,
                "scheme_id": factsheet.scheme_id,
                "fund_manager": factsheet.fund_manager,
                "fund_house": factsheet.fund_house,
                "inception_date": factsheet.inception_date.isoformat() if factsheet.inception_date else None,
                "expense_ratio": factsheet.expense_ratio,
                "benchmark_index": factsheet.benchmark_index,
                "category": factsheet.category,
                "risk_level": factsheet.risk_level,
                "aum": factsheet.aum,
                "exit_load": factsheet.exit_load,
                "holdings_count": factsheet.holdings_count,
                "last_updated": factsheet.last_updated.isoformat() if factsheet.last_updated else None
            }
        }), 200
    except SchemaValidationError as e:
        return jsonify({"error": e.messages}), 400
    except ResourceNotFoundError as e:
        return jsonify({"error": str(e)}), 404


@api_bp.route('/schemes/<int:scheme_id>/factsheet', methods=['DELETE'])
@token_required
def delete_fund_factsheet(current_user, scheme_id):
    """Delete a factsheet for a fund scheme"""
    try:
        FundService.delete_fund_factsheet(scheme_id)
        return jsonify({"message": "Fund factsheet deleted successfully"}), 200
    except ResourceNotFoundError as e:
        return jsonify({"error": str(e)}), 404


# Fund Returns Routes
@api_bp.route('/schemes/<int:scheme_id>/returns', methods=['POST'])
@token_required
def create_fund_returns(current_user, scheme_id):
    """Create returns data for a fund scheme"""
    try:
        # Validate request data
        data = request.get_json()
        if not data:
            return jsonify({"error": "No input data provided"}), 400
            
        # Add scheme_id to the data before validation
        data['scheme_id'] = scheme_id
        validated_data = returns_schema.load(data)
        
        # Create fund returns
        returns = FundService.create_fund_returns(
            scheme_id=validated_data['scheme_id'],
            date=validated_data['date'],
            scheme_code=validated_data['scheme_code'],
            return_1m=validated_data.get('return_1m'),
            return_3m=validated_data.get('return_3m'),
            return_6m=validated_data.get('return_6m'),
            return_ytd=validated_data.get('return_ytd'),
            return_1y=validated_data.get('return_1y'),
            return_3y=validated_data.get('return_3y'),
            return_5y=validated_data.get('return_5y')
        )
        
        return jsonify({
            "message": "Fund returns created successfully",
            "returns": {
                "id": returns.id,
                "scheme_id": returns.scheme_id,
                "date": returns.date.isoformat() if returns.date else None,
                "scheme_code": returns.scheme_code,
                "return_1m": returns.return_1m,
                "return_3m": returns.return_3m,
                "return_6m": returns.return_6m,
                "return_ytd": returns.return_ytd,
                "return_1y": returns.return_1y,
                "return_3y": returns.return_3y,
                "return_5y": returns.return_5y
            }
        }), 201
    except SchemaValidationError as e:
        return jsonify({"error": e.messages}), 400
    except ResourceNotFoundError as e:
        return jsonify({"error": str(e)}), 404
    except UniqueConstraintError as e:
        return jsonify({"error": str(e)}), 409


@api_bp.route('/schemes/<int:scheme_id>/returns', methods=['GET'])
def get_fund_returns(scheme_id):
    """Get returns data for a fund scheme"""
    try:
        date_param = request.args.get('date')
        date = None
        if date_param:
            try:
                date = datetime.datetime.strptime(date_param, '%Y-%m-%d').date()
            except ValueError:
                return jsonify({"error": "Invalid date format. Use YYYY-MM-DD"}), 400
        
        returns_data = FundService.get_fund_returns(scheme_id, date)
        
        if isinstance(returns_data, list):
            result = []
            for returns in returns_data:
                result.append({
                    "id": returns.id,
                    "scheme_id": returns.scheme_id,
                    "date": returns.date.isoformat() if returns.date else None,
                    "scheme_code": returns.scheme_code,
                    "return_1m": returns.return_1m,
                    "return_3m": returns.return_3m,
                    "return_6m": returns.return_6m,
                    "return_ytd": returns.return_ytd,
                    "return_1y": returns.return_1y,
                    "return_3y": returns.return_3y,
                    "return_5y": returns.return_5y
                })
        else:
            result = {
                "id": returns_data.id,
                "scheme_id": returns_data.scheme_id,
                "date": returns_data.date.isoformat() if returns_data.date else None,
                "scheme_code": returns_data.scheme_code,
                "return_1m": returns_data.return_1m,
                "return_3m": returns_data.return_3m,
                "return_6m": returns_data.return_6m,
                "return_ytd": returns_data.return_ytd,
                "return_1y": returns_data.return_1y,
                "return_3y": returns_data.return_3y,
                "return_5y": returns_data.return_5y
            }
            
        return jsonify(result), 200
    except ResourceNotFoundError as e:
        return jsonify({"error": str(e)}), 404


@api_bp.route('/schemes/<int:scheme_id>/returns/<date>', methods=['PUT', 'PATCH'])
@token_required
def update_fund_returns(current_user, scheme_id, date):
    """Update returns data for a fund scheme"""
    try:
        # Convert date string to date object
        try:
            date_obj = datetime.datetime.strptime(date, '%Y-%m-%d').date()
        except ValueError:
            return jsonify({"error": "Invalid date format. Use YYYY-MM-DD"}), 400
        
        # Validate request data
        data = request.get_json()
        if not data:
            return jsonify({"error": "No input data provided"}), 400
            
        validated_data = returns_schema.load(data, partial=True)
        
        # Update fund returns
        returns = FundService.update_fund_returns(
            scheme_id=scheme_id,
            date=date_obj,
            **validated_data
        )
        
        return jsonify({
            "message": "Fund returns updated successfully",
            "returns": {
                "id": returns.id,
                "scheme_id": returns.scheme_id,
                "date": returns.date.isoformat() if returns.date else None,
                "scheme_code": returns.scheme_code,
                "return_1m": returns.return_1m,
                "return_3m": returns.return_3m,
                "return_6m": returns.return_6m,
                "return_ytd": returns.return_ytd,
                "return_1y": returns.return_1y,
                "return_3y": returns.return_3y,
                "return_5y": returns.return_5y
            }
        }), 200
    except SchemaValidationError as e:
        return jsonify({"error": e.messages}), 400
    except ResourceNotFoundError as e:
        return jsonify({"error": str(e)}), 404


@api_bp.route('/schemes/<int:scheme_id>/returns/<date>', methods=['DELETE'])
@token_required
def delete_fund_returns(current_user, scheme_id, date):
    """Delete returns data for a fund scheme"""
    try:
        # Convert date string to date object
        try:
            date_obj = datetime.datetime.strptime(date, '%Y-%m-%d').date()
        except ValueError:
            return jsonify({"error": "Invalid date format. Use YYYY-MM-DD"}), 400
            
        FundService.delete_fund_returns(scheme_id, date_obj)
        return jsonify({"message": "Fund returns deleted successfully"}), 200
    except ResourceNotFoundError as e:
        return jsonify({"error": str(e)}), 404


# Portfolio Routes
@api_bp.route('/users/portfolio', methods=['GET'])
@token_required
def get_user_portfolio(current_user):
    """Get portfolio entries for the authenticated user"""
    try:
        portfolio_entries = PortfolioService.get_user_portfolio(current_user.id)
        
        return jsonify([{
            "id": entry.id,
            "user_id": entry.user_id,
            "scheme_id": entry.scheme_id,
            "scheme_code": entry.scheme_code,
            "units": entry.units,
            "purchase_nav": entry.purchase_nav,
            "current_nav": entry.current_nav,
            "invested_amount": entry.invested_amount,
            "current_value": entry.current_value,
            "date_invested": entry.date_invested.isoformat() if entry.date_invested else None,
            "last_updated": entry.last_updated.isoformat() if entry.last_updated else None
        } for entry in portfolio_entries]), 200
    except ResourceNotFoundError:
        return jsonify({"error": "User portfolio not found"}), 404

@api_bp.route('/users/portfolio', methods=['POST'])
@token_required
def add_to_portfolio(current_user):
    """Add a portfolio entry for the authenticated user"""
    try:
        # Validate request data
        data = request.get_json()
        if not data:
            return jsonify({"error": "No input data provided"}), 400
            
        validated_data = user_portfolio_schema.load(data)
        
        # Create portfolio entry
        portfolio_entry = PortfolioService.create_portfolio(
            user_id=current_user.id,
            scheme_id=validated_data['scheme_id'],
            scheme_code=validated_data['scheme_code'],
            units=validated_data['units'],
            purchase_nav=validated_data['purchase_nav'],
            invested_amount=validated_data['invested_amount'],
            date_invested=validated_data['date_invested'],
            current_nav=validated_data.get('current_nav'),
            current_value=validated_data.get('current_value')
        )
        
        # Update holdings
        PortfolioService.update_holdings(
            user_id=current_user.id,
            scheme_id=validated_data['scheme_id'],
            new_units=validated_data['units'],
            nav=validated_data['purchase_nav'],
            invested_amount=validated_data['invested_amount']
        )
        
        return jsonify({
            "message": "Portfolio entry added successfully",
            "portfolio": {
                "id": portfolio_entry.id,
                "user_id": portfolio_entry.user_id,
                "scheme_id": portfolio_entry.scheme_id,
                "scheme_code": portfolio_entry.scheme_code,
                "units": portfolio_entry.units,
                "purchase_nav": portfolio_entry.purchase_nav,
                "invested_amount": portfolio_entry.invested_amount,
                "date_invested": portfolio_entry.date_invested.isoformat() if portfolio_entry.date_invested else None
            }
        }), 201
    except SchemaValidationError as e:
        return jsonify({"error": e.messages}), 400

@api_bp.route('/users/portfolio/<int:portfolio_id>', methods=['PUT', 'PATCH'])
@token_required
def update_portfolio_entry(current_user, portfolio_id):
    """Update a portfolio entry"""
    try:
        # Get portfolio entry to ensure it belongs to current user
        portfolio_entry = PortfolioService.get_portfolio(portfolio_id)
        if portfolio_entry.user_id != current_user.id:
            return jsonify({"error": "Unauthorized access to portfolio entry"}), 403
            
        # Validate request data
        data = request.get_json()
        if not data:
            return jsonify({"error": "No input data provided"}), 400
            
        # Update portfolio entry
        updated_entry = PortfolioService.update_portfolio(
            portfolio_id=portfolio_id,
            units=data.get('units'),
            current_nav=data.get('current_nav'),
            current_value=data.get('current_value')
        )
        
        return jsonify({
            "message": "Portfolio entry updated successfully",
            "portfolio": {
                "id": updated_entry.id,
                "user_id": updated_entry.user_id,
                "scheme_id": updated_entry.scheme_id,
                "scheme_code": updated_entry.scheme_code,
                "units": updated_entry.units,
                "purchase_nav": updated_entry.purchase_nav,
                "current_nav": updated_entry.current_nav,
                "invested_amount": updated_entry.invested_amount,
                "current_value": updated_entry.current_value,
                "last_updated": updated_entry.last_updated.isoformat() if updated_entry.last_updated else None
            }
        }), 200
    except ResourceNotFoundError:
        return jsonify({"error": f"Portfolio entry with ID {portfolio_id} not found"}), 404

@api_bp.route('/users/portfolio/<int:portfolio_id>', methods=['DELETE'])
@token_required
def delete_portfolio_entry(current_user, portfolio_id):
    """Delete a portfolio entry"""
    try:
        # Get portfolio entry to ensure it belongs to current user
        portfolio_entry = PortfolioService.get_portfolio(portfolio_id)
        if portfolio_entry.user_id != current_user.id:
            return jsonify({"error": "Unauthorized access to portfolio entry"}), 403
            
        # Delete portfolio entry
        PortfolioService.delete_portfolio(portfolio_id)
        
        return jsonify({"message": "Portfolio entry deleted successfully"}), 200
    except ResourceNotFoundError:
        return jsonify({"error": f"Portfolio entry with ID {portfolio_id} not found"}), 404

# Setup function to register blueprint
# Fund Holdings Routes
@api_bp.route('/schemes/<int:scheme_id>/holdings', methods=['POST'])
@token_required
def create_fund_holding(current_user, scheme_id):
    """Create a new fund holding for a scheme"""
    try:
        # Validate request data
        data = request.get_json()
        if not data:
            return jsonify({"error": "No input data provided"}), 400
            
        validated_data = fund_holding_schema.load(data)
        
        # Create fund holding
        holding = FundService.create_fund_holding(
            scheme_id=scheme_id,
            security_name=validated_data['security_name'],
            asset_type=validated_data['asset_type'],
            weightage=validated_data['weightage'],
            isin=validated_data.get('isin'),
            sector=validated_data.get('sector'),
            holding_value=validated_data.get('holding_value')
        )
        
        return jsonify({
            "message": "Fund holding created successfully",
            "holding": {
                "id": holding.id,
                "scheme_id": holding.scheme_id,
                "security_name": holding.security_name,
                "isin": holding.isin,
                "sector": holding.sector,
                "asset_type": holding.asset_type,
                "weightage": holding.weightage,
                "holding_value": holding.holding_value,
                "last_updated": holding.last_updated.isoformat() if holding.last_updated else None
            }
        }), 201
    except SchemaValidationError as e:
        return jsonify({"error": e.messages}), 400
    except ResourceNotFoundError:
        return jsonify({"error": f"Fund scheme with ID {scheme_id} not found"}), 404

@api_bp.route('/schemes/<int:scheme_id>/holdings', methods=['GET'])
def get_fund_holdings(scheme_id):
    """Get all holdings for a fund scheme"""
    try:
        holdings = FundService.get_fund_holdings(scheme_id)
        
        return jsonify([{
            "id": holding.id,
            "scheme_id": holding.scheme_id,
            "security_name": holding.security_name,
            "isin": holding.isin,
            "sector": holding.sector,
            "asset_type": holding.asset_type,
            "weightage": holding.weightage,
            "holding_value": holding.holding_value,
            "last_updated": holding.last_updated.isoformat() if holding.last_updated else None
        } for holding in holdings]), 200
    except ResourceNotFoundError:
        return jsonify({"error": f"Fund scheme with ID {scheme_id} not found"}), 404

@api_bp.route('/holdings/<int:holding_id>', methods=['GET'])
def get_fund_holding(holding_id):
    """Get a specific fund holding by ID"""
    try:
        holding = FundService.get_fund_holding(holding_id)
        
        return jsonify({
            "id": holding.id,
            "scheme_id": holding.scheme_id,
            "security_name": holding.security_name,
            "isin": holding.isin,
            "sector": holding.sector,
            "asset_type": holding.asset_type,
            "weightage": holding.weightage,
            "holding_value": holding.holding_value,
            "last_updated": holding.last_updated.isoformat() if holding.last_updated else None
        }), 200
    except ResourceNotFoundError:
        return jsonify({"error": f"Fund holding with ID {holding_id} not found"}), 404

@api_bp.route('/holdings/<int:holding_id>', methods=['PUT', 'PATCH'])
@token_required
def update_fund_holding(current_user, holding_id):
    """Update a fund holding"""
    try:
        # Validate request data
        data = request.get_json()
        if not data:
            return jsonify({"error": "No input data provided"}), 400
            
        validated_data = fund_holding_schema.load(data, partial=True)
        
        # Update fund holding
        holding = FundService.update_fund_holding(
            holding_id=holding_id,
            **validated_data
        )
        
        return jsonify({
            "message": "Fund holding updated successfully",
            "holding": {
                "id": holding.id,
                "scheme_id": holding.scheme_id,
                "security_name": holding.security_name,
                "isin": holding.isin,
                "sector": holding.sector,
                "asset_type": holding.asset_type,
                "weightage": holding.weightage,
                "holding_value": holding.holding_value,
                "last_updated": holding.last_updated.isoformat() if holding.last_updated else None
            }
        }), 200
    except SchemaValidationError as e:
        return jsonify({"error": e.messages}), 400
    except ResourceNotFoundError:
        return jsonify({"error": f"Fund holding with ID {holding_id} not found"}), 404

@api_bp.route('/holdings/<int:holding_id>', methods=['DELETE'])
@token_required
def delete_fund_holding(current_user, holding_id):
    """Delete a fund holding"""
    try:
        FundService.delete_fund_holding(holding_id)
        return jsonify({"message": "Fund holding deleted successfully"}), 200
    except ResourceNotFoundError:
        return jsonify({"error": f"Fund holding with ID {holding_id} not found"}), 404

def setup_routes(app):
    """Register blueprints with the Flask application"""
    app.register_blueprint(api_bp)
    
    # Register error handlers
    @app.errorhandler(404)
    def not_found_error(error):
        if request.path.startswith('/api/'):
            return jsonify({"error": "Resource not found"}), 404
        return "<h1>404 Not Found</h1>", 404
    
    @app.errorhandler(500)
    def internal_error(error):
        if request.path.startswith('/api/'):
            return jsonify({"error": "Internal server error"}), 500
        return "<h1>500 Internal Server Error</h1>", 500