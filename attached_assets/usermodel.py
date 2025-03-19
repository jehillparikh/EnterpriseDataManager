# Define the models
from datetime import datetime
import re
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import CheckConstraint, Enum, Integer, Float
from sqlalchemy.orm import relationship

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://username:password@localhost:5432/your_db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)


# Validators (Regex Patterns)
PAN_REGEX = r'^[A-Za-z]{5}[0-9]{4}[A-Za-z]{1}$'
DOB_REGEX = r'^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
PINCODE_REGEX = r'^\d{6}$'
PHONE_REGEX = r'^\d{10}$'
ACCOUNT_NUMBER_REGEX = r'^\d{9,16}$'
IFSC_CODE_REGEX = r'^[A-Za-z]{4}0[A-Za-z0-9]{6}$'
MICR_CODE_REGEX = r'^\d{9}$'

# Models

class UserInfo(db.Model):
    """
    Internal User table.
    """
    __tablename__ = 'info'
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(255), unique=True, nullable=False)
    mobile_number = db.Column(db.String(15), unique=True, nullable=False)  # Added mobile number field

    __table_args__ = (
        db.Index('idx_user_email', 'email'),  # Optimized lookup
        db.Index('idx_user_mobile', 'mobile_number')  # Optimized lookup for mobile number
    )


class KycDetail(db.Model):
    """
    Stores KYC details of a user.
    """
    __tablename__ = 'kyc_detail'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('info.id'), nullable=False, unique=True)

    # Enums
    OCCUPATION_CHOICES = {'01': 'Business', '02': 'Services'}
    GENDER_CHOICES = {'M': 'Male', 'F': 'Female'}
    STATE_CHOICES = {
        'AN': 'Andaman & Nicobar', 'AP': 'Andhra Pradesh', 'AR': 'Arunachal Pradesh', 'AS': 'Assam', 'BR': 'Bihar',
        'CH': 'Chandigarh', 'CG': 'Chhattisgarh', 'DL': 'Delhi', 'GA': 'Goa', 'GJ': 'Gujarat', 'HR': 'Haryana',
        'HP': 'Himachal Pradesh', 'JK': 'Jammu & Kashmir', 'JH': 'Jharkhand', 'KA': 'Karnataka', 'KL': 'Kerala',
        'MP': 'Madhya Pradesh', 'MH': 'Maharashtra', 'MN': 'Manipur', 'ML': 'Meghalaya', 'MZ': 'Mizoram',
        'NL': 'Nagaland', 'OD': 'Odisha', 'PB': 'Punjab', 'RJ': 'Rajasthan', 'SK': 'Sikkim', 'TN': 'Tamil Nadu',
        'TG': 'Telangana', 'TR': 'Tripura', 'UP': 'Uttar Pradesh', 'UT': 'Uttarakhand', 'WB': 'West Bengal'
    }
    INCOME_SLAB_CHOICES = {31: 'Below 1 Lakh', 32: '> 1 <=5 Lacs', 33: '>5 <=10 Lacs', 34: '>10 <= 25 Lacs', 
                           35: '> 25 Lacs < = 1 Crore', 36: 'Above 1 Crore'}

    # Fields
    pan = db.Column(db.String(10), nullable=False)
    tax_status = db.Column(db.String(2), default='01')
    occ_code = db.Column(db.String(2), default='02')
    first_name = db.Column(db.String(70), nullable=False)
    middle_name = db.Column(db.String(70), nullable=True)
    last_name = db.Column(db.String(70), nullable=False)
    dob = db.Column(db.String(10), nullable=False)
    gender = db.Column(db.String(1), nullable=False)
    address = db.Column(db.String(120), nullable=False)
    city = db.Column(db.String(35), nullable=False)
    state = db.Column(db.String(2), nullable=False)
    pincode = db.Column(db.String(6), nullable=False)
    phone = db.Column(db.String(10), nullable=True)
    income_slab = db.Column(db.Integer, nullable=False)

    # Relationships
    user = relationship("Info", backref="kyc_detail")


class BankRepo(db.Model):
    """
    Repository of bank names for user's bank details.
    """
    __tablename__ = 'bank_repo'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), unique=True, nullable=False)


class BranchRepo(db.Model):
    """
    Repository of branch details for user's bank details.
    """
    __tablename__ = 'branch_repo'
    id = db.Column(db.Integer, primary_key=True)
    bank_id = db.Column(db.Integer, db.ForeignKey('bank_repo.id'), nullable=False)
    branch_name = db.Column(db.String(100), nullable=False)
    branch_city = db.Column(db.String(35), nullable=False)
    branch_address = db.Column(db.String(250), nullable=True)
    ifsc_code = db.Column(db.String(11), unique=True, nullable=False)
    micr_code = db.Column(db.String(9), nullable=True)

    bank = relationship("BankRepo", backref="branches")


class BankDetail(db.Model):
    """
    Stores bank details of a user.
    """
    __tablename__ = 'bank_detail'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('info.id'), nullable=False)
    branch_id = db.Column(db.Integer, db.ForeignKey('branch_repo.id'), nullable=True)
    account_number = db.Column(db.String(20), nullable=False)
    account_type_bse = db.Column(db.String(2), nullable=False)

    # Relationships
    user = relationship("UserInfo", backref="bank_details")
    branch = relationship("BranchRepo", backref="bank_details")


class Mandate(db.Model):
    """
    Stores mandates registered in BSE for a user.
    """
    __tablename__ = 'mandate'
    id = db.Column(db.String(10), primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('info.id'), nullable=False)
    bank_id = db.Column(db.Integer, db.ForeignKey('bank_detail.id'), nullable=False)

    # Enums
    STATUS_CHOICES = {
        '0': 'Created', '1': 'Cancelled', '2': 'Registered in BSE', '3': 'Form submitted to BSE',
        '4': 'Received by BSE', '5': 'Accepted by BSE', '6': 'Rejected by BSE', '7': 'Exhausted'
    }

    # Fields
    status = db.Column(db.String(1), nullable=False, default='0')
    amount = db.Column(db.Float, nullable=True)

    # Relationships
    user = relationship("UserInfo", backref="mandates")
    bank = relationship("BankDetail", backref="mandates")


class MFHoldings(db.Model):
    """
    Tracks accumulated holdings per user per scheme.
    """
    __tablename__ = 'mf_holdings'
    id = db.Column(Integer, primary_key=True)
    user_id = db.Column(Integer, ForeignKey('info.id'), nullable=False)
    scheme_id = db.Column(Integer, ForeignKey('fund_scheme.id'), nullable=False)
    units_held = db.Column(Float, nullable=False, default=0)  # Total units held
    average_nav = db.Column(Float, nullable=False, default=0)  # Average purchase NAV
    invested_amount = db.Column(Float, nullable=False, default=0)  # Total amount invested
    current_value = db.Column(Float, nullable=True)  # Current value based on latest NAV
    last_updated = db.Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    user = relationship("UserInfo", backref="mf_holdings")
    scheme = relationship("FundScheme", backref="mf_holdings")

    __table_args__ = (
        Index('idx_user_scheme_holdings', 'user_id', 'scheme_id'),
        CheckConstraint('units_held >= 0', name='check_units_held'),
    )


class Portfolio(db.Model):
    """
    User portfolio containing mutual fund holdings.
    """
    __tablename__ = 'portfolio'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)  # Reference to user
    scheme_id = db.Column(db.Integer, db.ForeignKey('fund_scheme.id'), nullable=False)  # Reference to mutual fund scheme
    scheme_code = db.Column(db.String(20), nullable=False)  # Scheme code
    units = db.Column(db.Float, nullable=False)  # Number of units held
    purchase_nav = db.Column(db.Float, nullable=False)  # NAV at purchase time
    current_nav = db.Column(db.Float, nullable=True)  # Latest NAV
    invested_amount = db.Column(db.Float, nullable=False)  # Total amount invested
    current_value = db.Column(db.Float, nullable=True)  # Current portfolio value
    date_invested = db.Column(db.Date, nullable=False)  # Date of investment
    last_updated = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)  # Auto-update timestamp

    user = relationship("UserInfo", backref="portfolio")
    scheme = relationship("FundScheme", backref="portfolio")

    __table_args__ = (
        db.Index('idx_user_scheme', 'user_id', 'scheme_id'),  # Optimize user portfolio lookups
        db.CheckConstraint('units >= 0', name='check_units'),  # No negative units
        db.CheckConstraint('invested_amount >= 0', name='check_invested_amount'),  # No negative investments
    )
