from marshmallow import Schema, fields, validate, ValidationError, validates_schema
import re
from models import PAN_REGEX, DOB_REGEX, PINCODE_REGEX, PHONE_REGEX, ACCOUNT_NUMBER_REGEX, IFSC_CODE_REGEX, MICR_CODE_REGEX

# User Management Schemas
class UserRegistrationSchema(Schema):
    """Schema for user registration"""
    email = fields.Email(required=True, error_messages={"required": "Email is required."})
    mobile_number = fields.String(required=True, validate=validate.Regexp(PHONE_REGEX), 
                                  error_messages={"required": "Mobile number is required."})
    password = fields.String(required=True, validate=validate.Length(min=8), 
                             error_messages={"required": "Password is required."})


class UserLoginSchema(Schema):
    """Schema for user login"""
    email = fields.Email(required=True, error_messages={"required": "Email is required."})
    password = fields.String(required=True, error_messages={"required": "Password is required."})


class UserUpdateSchema(Schema):
    """Schema for updating user details"""
    email = fields.Email()
    mobile_number = fields.String(validate=validate.Regexp(PHONE_REGEX))
    password = fields.String(validate=validate.Length(min=8))


class KycDetailSchema(Schema):
    """Schema for KYC detail validation"""
    pan = fields.String(required=True, validate=validate.Regexp(PAN_REGEX), 
                        error_messages={"required": "PAN is required."})
    tax_status = fields.String(default='01')
    occ_code = fields.String(default='02')
    first_name = fields.String(required=True, validate=validate.Length(min=1, max=70), 
                               error_messages={"required": "First name is required."})
    middle_name = fields.String(validate=validate.Length(max=70))
    last_name = fields.String(required=True, validate=validate.Length(min=1, max=70), 
                              error_messages={"required": "Last name is required."})
    dob = fields.String(required=True, validate=validate.Regexp(DOB_REGEX), 
                        error_messages={"required": "Date of birth is required."})
    gender = fields.String(required=True, validate=validate.OneOf(['M', 'F']), 
                           error_messages={"required": "Gender is required."})
    address = fields.String(required=True, validate=validate.Length(min=5, max=120), 
                            error_messages={"required": "Address is required."})
    city = fields.String(required=True, validate=validate.Length(min=2, max=35), 
                         error_messages={"required": "City is required."})
    state = fields.String(required=True, validate=validate.Length(2), 
                          error_messages={"required": "State is required."})
    pincode = fields.String(required=True, validate=validate.Regexp(PINCODE_REGEX), 
                            error_messages={"required": "Pincode is required."})
    phone = fields.String(validate=validate.Regexp(PHONE_REGEX))
    income_slab = fields.Integer(required=True, validate=validate.OneOf([31, 32, 33, 34, 35, 36]), 
                                 error_messages={"required": "Income slab is required."})


class BankRepoSchema(Schema):
    """Schema for bank validation"""
    name = fields.String(required=True, validate=validate.Length(min=2, max=100), 
                         error_messages={"required": "Bank name is required."})


class BranchRepoSchema(Schema):
    """Schema for branch validation"""
    bank_id = fields.Integer(required=True, error_messages={"required": "Bank ID is required."})
    branch_name = fields.String(required=True, validate=validate.Length(min=2, max=100), 
                                error_messages={"required": "Branch name is required."})
    branch_city = fields.String(required=True, validate=validate.Length(min=2, max=35), 
                                error_messages={"required": "Branch city is required."})
    branch_address = fields.String(validate=validate.Length(max=250))
    ifsc_code = fields.String(required=True, validate=validate.Regexp(IFSC_CODE_REGEX), 
                              error_messages={"required": "IFSC code is required."})
    micr_code = fields.String(validate=validate.Regexp(MICR_CODE_REGEX))


class BankDetailSchema(Schema):
    """Schema for bank detail validation"""
    user_id = fields.Integer(required=True, error_messages={"required": "User ID is required."})
    branch_id = fields.Integer(error_messages={"required": "Branch ID is required."})
    account_number = fields.String(required=True, validate=validate.Regexp(ACCOUNT_NUMBER_REGEX), 
                                   error_messages={"required": "Account number is required."})
    account_type_bse = fields.String(required=True, error_messages={"required": "Account type is required."})


class MandateSchema(Schema):
    """Schema for mandate validation"""
    id = fields.String(required=True, validate=validate.Length(min=1, max=10), 
                       error_messages={"required": "Mandate ID is required."})
    user_id = fields.Integer(required=True, error_messages={"required": "User ID is required."})
    bank_id = fields.Integer(required=True, error_messages={"required": "Bank ID is required."})
    status = fields.String(default='0', validate=validate.OneOf(['0', '1', '2', '3', '4', '5', '6', '7']))
    amount = fields.Float()


# Fund Management Schemas
class AmcSchema(Schema):
    """Schema for AMC validation"""
    name = fields.String(required=True, validate=validate.Length(min=2, max=100), 
                         error_messages={"required": "AMC name is required."})
    short_name = fields.String(required=True, validate=validate.Length(min=1, max=20), 
                               error_messages={"required": "Short name is required."})
    fund_code = fields.String(validate=validate.Length(max=10))
    bse_code = fields.String(validate=validate.Length(max=10))
    active = fields.Boolean(default=True)


class FundSchema(Schema):
    """Schema for fund validation"""
    name = fields.String(required=True, validate=validate.Length(min=2, max=100), 
                         error_messages={"required": "Fund name is required."})
    short_name = fields.String(validate=validate.Length(max=20))
    amc_id = fields.Integer(required=True, error_messages={"required": "AMC ID is required."})
    rta_code = fields.String(validate=validate.Length(max=10))
    bse_code = fields.String(validate=validate.Length(max=10))
    active = fields.Boolean(default=True)
    direct = fields.Boolean(default=False)


class FundSchemeSchema(Schema):
    """Schema for fund scheme validation"""
    fund_id = fields.Integer(required=True, error_messages={"required": "Fund ID is required."})
    scheme_code = fields.String(required=True, validate=validate.Length(min=1, max=10), 
                                error_messages={"required": "Scheme code is required."})
    plan = fields.String(required=True, validate=validate.Length(min=1, max=10), 
                         error_messages={"required": "Plan is required."})
    option = fields.String(validate=validate.Length(max=10))
    bse_code = fields.String(validate=validate.Length(max=10))


class FundSchemeDetailSchema(Schema):
    """Schema for fund scheme detail validation"""
    scheme_id = fields.Integer(required=True, error_messages={"required": "Scheme ID is required."})
    nav = fields.Float(required=True, error_messages={"required": "NAV is required."})
    expense_ratio = fields.Float()
    fund_manager = fields.String(validate=validate.Length(max=100))
    aum = fields.Float()
    risk_level = fields.String(validate=validate.Length(max=10))
    benchmark = fields.String(validate=validate.Length(max=100))


class MutualFundSchema(Schema):
    """Schema for mutual fund validation"""
    amc = fields.String(required=True, error_messages={"required": "AMC is required."})
    code = fields.Integer(required=True, error_messages={"required": "Code is required."})
    scheme_name = fields.String(required=True, error_messages={"required": "Scheme name is required."})
    scheme_type = fields.String(required=True, error_messages={"required": "Scheme type is required."})
    scheme_category = fields.String(required=True, error_messages={"required": "Scheme category is required."})
    scheme_nav_name = fields.String()
    scheme_minimum_amount = fields.Integer()
    launch_date = fields.Date()
    closure_date = fields.Date()
    isin_div_payout_growth = fields.String()
    isin_div_reinvestment = fields.String()


class UserPortfolioSchema(Schema):
    """Schema for user portfolio validation"""
    user_id = fields.Integer(required=True, error_messages={"required": "User ID is required."})
    scheme_id = fields.Integer(required=True, error_messages={"required": "Scheme ID is required."})
    scheme_code = fields.String(required=True, error_messages={"required": "Scheme code is required."})
    units = fields.Float(required=True, validate=validate.Range(min=0), 
                         error_messages={"required": "Units is required."})
    purchase_nav = fields.Float(required=True, error_messages={"required": "Purchase NAV is required."})
    current_nav = fields.Float()
    invested_amount = fields.Float(required=True, validate=validate.Range(min=0), 
                                   error_messages={"required": "Invested amount is required."})
    current_value = fields.Float()
    date_invested = fields.Date(required=True, error_messages={"required": "Date invested is required."})


class FundFactSheetSchema(Schema):
    """Schema for fund factsheet validation"""
    scheme_id = fields.Integer(required=True, error_messages={"required": "Scheme ID is required."})
    fund_manager = fields.String(validate=validate.Length(max=255))
    fund_house = fields.String(required=True, validate=validate.Length(min=2, max=255),
                               error_messages={"required": "Fund house is required."})
    inception_date = fields.Date()
    expense_ratio = fields.Float(validate=validate.Range(min=0, max=10))
    benchmark_index = fields.String(validate=validate.Length(max=255))
    category = fields.String(required=True, validate=validate.Length(min=2, max=100),
                             error_messages={"required": "Category is required."})
    risk_level = fields.String(validate=validate.OneOf(["Low", "Moderate", "High"]))
    aum = fields.Float(validate=validate.Range(min=0))
    exit_load = fields.String(validate=validate.Length(max=50))
    holdings_count = fields.Integer(validate=validate.Range(min=0))


class ReturnsSchema(Schema):
    """Schema for returns validation"""
    scheme_id = fields.Integer(required=True, error_messages={"required": "Scheme ID is required."})
    date = fields.Date(required=True, error_messages={"required": "Date is required."})
    return_1m = fields.Float(validate=validate.Range(min=-100))
    return_3m = fields.Float(validate=validate.Range(min=-100))
    return_6m = fields.Float(validate=validate.Range(min=-100))
    return_ytd = fields.Float(validate=validate.Range(min=-100))
    return_1y = fields.Float(validate=validate.Range(min=-100))
    return_3y = fields.Float(validate=validate.Range(min=-100))
    return_5y = fields.Float(validate=validate.Range(min=-100))
    scheme_code = fields.String(required=True, error_messages={"required": "Scheme code is required."})


# Create schema instances
user_registration_schema = UserRegistrationSchema()
user_login_schema = UserLoginSchema()
user_update_schema = UserUpdateSchema()
kyc_detail_schema = KycDetailSchema()
bank_repo_schema = BankRepoSchema()
branch_repo_schema = BranchRepoSchema()
bank_detail_schema = BankDetailSchema()
mandate_schema = MandateSchema()

amc_schema = AmcSchema()
fund_schema = FundSchema()
fund_scheme_schema = FundSchemeSchema()
fund_scheme_detail_schema = FundSchemeDetailSchema()
mutual_fund_schema = MutualFundSchema()
user_portfolio_schema = UserPortfolioSchema()
fund_factsheet_schema = FundFactSheetSchema()
returns_schema = ReturnsSchema()