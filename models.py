from setup_db import db
from datetime import datetime
from sqlalchemy import Index, CheckConstraint

class Fund(db.Model):
    """
    Mutual Fund model with ISIN as primary key
    """
    __tablename__ = 'mf_fund'
    
    isin = db.Column(db.String(12), primary_key=True)
    scheme_name = db.Column(db.String(255), nullable=False)
    fund_type = db.Column(db.String(50), nullable=False)  # Type (equity, debt, hybrid)
    fund_subtype = db.Column(db.String(100), nullable=True)  # Subtype
    amc_name = db.Column(db.String(100), nullable=False)  # Fund house name
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships defined later with backref
    
    __table_args__ = (
        Index('idx_fund_amc_name', 'amc_name'),  # Optimize AMC lookups
        Index('idx_fund_type', 'fund_type'),  # Optimize fund type lookups
    )


class FundFactSheet(db.Model):
    """
    Factsheet information for a mutual fund
    """
    __tablename__ = 'mf_factsheet'
    
    isin = db.Column(db.String(12), db.ForeignKey('mf_fund.isin'), primary_key=True)
    fund_manager = db.Column(db.String(255), nullable=True)
    aum = db.Column(db.Float, nullable=True)  # Assets Under Management in Crores
    expense_ratio = db.Column(db.Float, nullable=True)  # Expense Ratio percentage
    launch_date = db.Column(db.Date, nullable=True)  # Launch date of the fund
    exit_load = db.Column(db.String(255), nullable=True)  # Exit load details
    last_updated = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationship to Fund
    fund = db.relationship("Fund", backref="factsheet")


class FundReturns(db.Model):
    """
    Returns data for a mutual fund
    """
    __tablename__ = 'mf_returns'
    
    isin = db.Column(db.String(12), db.ForeignKey('mf_fund.isin'), primary_key=True)
    return_1m = db.Column(db.Float, nullable=True)  # 1-month return percentage
    return_3m = db.Column(db.Float, nullable=True)  # 3-month return percentage
    return_6m = db.Column(db.Float, nullable=True)  # 6-month return percentage
    return_ytd = db.Column(db.Float, nullable=True)  # Year-to-date return percentage
    return_1y = db.Column(db.Float, nullable=True)  # 1-year return percentage
    return_3y = db.Column(db.Float, nullable=True)  # 3-year return percentage
    return_5y = db.Column(db.Float, nullable=True)  # 5-year return percentage
    last_updated = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationship to Fund
    fund = db.relationship("Fund", backref="returns")
    
    __table_args__ = (
        CheckConstraint('return_1m >= -100', name='check_return_1m'),
        CheckConstraint('return_3m >= -100', name='check_return_3m'),
        CheckConstraint('return_6m >= -100', name='check_return_6m'),
        CheckConstraint('return_ytd >= -100', name='check_return_ytd'),
        CheckConstraint('return_1y >= -100', name='check_return_1y'),
        CheckConstraint('return_3y >= -100', name='check_return_3y'),
        CheckConstraint('return_5y >= -100', name='check_return_5y'),
    )


class FundHolding(db.Model):
    """
    Holdings/investments within a mutual fund
    Expected columns: Name of Instrument, ISIN, Coupon, Industry, Quantity, 
    Market Value, % to Net Assets, Yield, Type, AMC, Scheme Name, Scheme ISIN
    """
    __tablename__ = 'mf_fund_holdings'
    
    id = db.Column(db.Integer, primary_key=True)
    isin = db.Column(db.String(12), db.ForeignKey('mf_fund.isin'), nullable=False)  # Scheme ISIN
    instrument_isin = db.Column(db.String(12), nullable=True)  # ISIN of the instrument
    coupon = db.Column(db.Float, nullable=True)  # Coupon percentage for debt instruments
    instrument_name = db.Column(db.String(255), nullable=False)  # Name of Instrument
    sector = db.Column(db.String(100), nullable=True)  # Industry classification
    quantity = db.Column(db.Float, nullable=True)  # Quantity held
    value = db.Column(db.Float, nullable=True)  # Market Value in INR
    percentage_to_nav = db.Column(db.Float, nullable=False)  # % to Net Assets
    yield_value = db.Column(db.Float, nullable=True)  # Yield percentage
    instrument_type = db.Column(db.String(50), nullable=False)  # Type of instrument
    amc_name = db.Column(db.String(100), nullable=True)  # AMC name from upload
    scheme_name = db.Column(db.String(255), nullable=True)  # Scheme Name from upload
    last_updated = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationship to Fund
    fund = db.relationship("Fund", backref="fund_holdings")
    
    __table_args__ = (
        CheckConstraint('percentage_to_nav >= 0', name='check_percentage_to_nav'),
        CheckConstraint('percentage_to_nav <= 100', name='check_percentage_to_nav_upper'),
    )


class NavHistory(db.Model):
    """
    NAV history for a mutual fund
    """
    __tablename__ = 'mf_nav_history'
    
    id = db.Column(db.Integer, primary_key=True)
    isin = db.Column(db.String(12), db.ForeignKey('mf_fund.isin'), nullable=False)
    date = db.Column(db.Date, nullable=False)  # Date of NAV
    nav = db.Column(db.Float, nullable=False)  # NAV value
    
    # Relationship to Fund
    fund = db.relationship("Fund", backref="nav_history")
    
    __table_args__ = (
        CheckConstraint('nav >= 0', name='check_nav'),
    )


class FundRating(db.Model):
    """
    Fund ratings from various rating agencies
    """
    __tablename__ = 'mf_fund_ratings'
    
    id = db.Column(db.Integer, primary_key=True)
    isin = db.Column(db.String(12), db.ForeignKey('mf_fund.isin'), nullable=False)
    rating_agency = db.Column(db.String(50), nullable=False)  # CRISIL, Morningstar, Value Research, etc.
    rating_category = db.Column(db.String(50), nullable=False)  # Overall, Risk, Return, Expense, etc.
    rating_value = db.Column(db.String(10), nullable=False)  # 5 Star, AAA, High, etc.
    rating_numeric = db.Column(db.Float, nullable=True)  # Numeric equivalent (1-5 for stars, 1-10 for scores)
    rating_date = db.Column(db.Date, nullable=False)  # Date when rating was assigned
    rating_outlook = db.Column(db.String(20), nullable=True)  # Positive, Negative, Stable, Under Review
    rating_description = db.Column(db.Text, nullable=True)  # Additional rating commentary
    is_current = db.Column(db.Boolean, default=True)  # Flag to mark current vs historical ratings
    devmani_recommended = db.Column(db.Boolean, default=False)  # Devmani recommendation flag
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationship to Fund
    fund = db.relationship("Fund", backref="fund_ratings")
    
    __table_args__ = (
        Index('idx_rating_agency_category', 'rating_agency', 'rating_category'),
        Index('idx_rating_current', 'is_current'),
        Index('idx_rating_date', 'rating_date'),
        Index('idx_devmani_recommended', 'devmani_recommended'),
        CheckConstraint('rating_numeric >= 0', name='check_rating_numeric_positive'),
        CheckConstraint('rating_numeric <= 10', name='check_rating_numeric_max'),
    )