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


class FundAnalytics(db.Model):
    """
    Advanced analytics and metrics for mutual funds
    """
    __tablename__ = 'mf_fund_analytics'
    
    id = db.Column(db.Integer, primary_key=True)
    isin = db.Column(db.String(12), db.ForeignKey('mf_fund.isin'), nullable=False)
    
    # Risk Metrics
    beta = db.Column(db.Float, nullable=True)  # Beta coefficient vs benchmark
    alpha = db.Column(db.Float, nullable=True)  # Alpha (excess return vs benchmark)
    standard_deviation = db.Column(db.Float, nullable=True)  # Volatility measure
    sharpe_ratio = db.Column(db.Float, nullable=True)  # Risk-adjusted return
    sortino_ratio = db.Column(db.Float, nullable=True)  # Downside risk-adjusted return
    treynor_ratio = db.Column(db.Float, nullable=True)  # Return per unit of systematic risk
    information_ratio = db.Column(db.Float, nullable=True)  # Active return vs tracking error
    
    # Performance Metrics
    tracking_error = db.Column(db.Float, nullable=True)  # Standard deviation of excess returns
    r_squared = db.Column(db.Float, nullable=True)  # Correlation with benchmark (0-100)
    maximum_drawdown = db.Column(db.Float, nullable=True)  # Largest peak-to-trough decline
    calmar_ratio = db.Column(db.Float, nullable=True)  # Annual return / max drawdown
    
    # Market Timing Metrics
    up_capture_ratio = db.Column(db.Float, nullable=True)  # Performance in rising markets
    down_capture_ratio = db.Column(db.Float, nullable=True)  # Performance in falling markets
    
    # Advanced Metrics
    var_95 = db.Column(db.Float, nullable=True)  # Value at Risk (95% confidence)
    var_99 = db.Column(db.Float, nullable=True)  # Value at Risk (99% confidence)
    skewness = db.Column(db.Float, nullable=True)  # Return distribution asymmetry
    kurtosis = db.Column(db.Float, nullable=True)  # Return distribution tail risk
    
    # Reference Data
    benchmark_index = db.Column(db.String(50), nullable=True)  # Primary benchmark
    calculation_period = db.Column(db.String(20), nullable=True)  # Period for calculations (1Y, 3Y, 5Y)
    calculation_date = db.Column(db.Date, nullable=False)  # Date of calculation
    
    # Metadata
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationship to Fund
    fund = db.relationship("Fund", backref="fund_analytics")
    
    __table_args__ = (
        Index('idx_analytics_calculation_date', 'calculation_date'),
        Index('idx_analytics_period', 'calculation_period'),
        Index('idx_analytics_benchmark', 'benchmark_index'),
        CheckConstraint('r_squared >= 0 AND r_squared <= 100', name='check_r_squared_range'),
        CheckConstraint('sharpe_ratio >= -10 AND sharpe_ratio <= 10', name='check_sharpe_ratio_range'),
        CheckConstraint('beta >= 0', name='check_beta_positive'),
    )


class FundStatistics(db.Model):
    """
    Statistical data and portfolio composition metrics for mutual funds
    """
    __tablename__ = 'mf_fund_statistics'
    
    id = db.Column(db.Integer, primary_key=True)
    isin = db.Column(db.String(12), db.ForeignKey('mf_fund.isin'), nullable=False)
    
    # Portfolio Composition
    total_holdings = db.Column(db.Integer, nullable=True)  # Number of holdings
    top_10_holdings_percentage = db.Column(db.Float, nullable=True)  # % in top 10 holdings
    equity_percentage = db.Column(db.Float, nullable=True)  # Equity allocation %
    debt_percentage = db.Column(db.Float, nullable=True)  # Debt allocation %
    cash_percentage = db.Column(db.Float, nullable=True)  # Cash and equivalents %
    other_percentage = db.Column(db.Float, nullable=True)  # Other investments %
    
    # Market Cap Distribution
    large_cap_percentage = db.Column(db.Float, nullable=True)  # Large cap allocation
    mid_cap_percentage = db.Column(db.Float, nullable=True)  # Mid cap allocation
    small_cap_percentage = db.Column(db.Float, nullable=True)  # Small cap allocation
    
    # Sector Concentration
    top_sector_name = db.Column(db.String(100), nullable=True)  # Highest weighted sector
    top_sector_percentage = db.Column(db.Float, nullable=True)  # Top sector weight
    sector_concentration_ratio = db.Column(db.Float, nullable=True)  # Top 3 sectors combined %
    
    # Credit Quality (for debt funds)
    aaa_percentage = db.Column(db.Float, nullable=True)  # AAA rated securities
    aa_percentage = db.Column(db.Float, nullable=True)  # AA rated securities
    a_percentage = db.Column(db.Float, nullable=True)  # A rated securities
    below_a_percentage = db.Column(db.Float, nullable=True)  # Below A rated securities
    unrated_percentage = db.Column(db.Float, nullable=True)  # Unrated securities
    
    # Duration Metrics (for debt funds)
    average_maturity = db.Column(db.Float, nullable=True)  # Average maturity in years
    modified_duration = db.Column(db.Float, nullable=True)  # Interest rate sensitivity
    yield_to_maturity = db.Column(db.Float, nullable=True)  # Portfolio YTM
    
    # Flow Statistics
    monthly_inflow = db.Column(db.Float, nullable=True)  # Last month inflow (crores)
    monthly_outflow = db.Column(db.Float, nullable=True)  # Last month outflow (crores)
    net_flow = db.Column(db.Float, nullable=True)  # Net flow (inflow - outflow)
    quarterly_flow = db.Column(db.Float, nullable=True)  # Last quarter net flow
    yearly_flow = db.Column(db.Float, nullable=True)  # Last year net flow
    
    # Turnover Metrics
    portfolio_turnover_ratio = db.Column(db.Float, nullable=True)  # Annual turnover %
    
    # Reference Data
    statistics_date = db.Column(db.Date, nullable=False)  # Date of statistics
    data_source = db.Column(db.String(50), nullable=True)  # Data provider
    
    # Metadata
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationship to Fund
    fund = db.relationship("Fund", backref="fund_statistics")
    
    __table_args__ = (
        Index('idx_statistics_date', 'statistics_date'),
        Index('idx_statistics_source', 'data_source'),
        CheckConstraint('equity_percentage >= 0 AND equity_percentage <= 100', name='check_equity_percentage'),
        CheckConstraint('debt_percentage >= 0 AND debt_percentage <= 100', name='check_debt_percentage'),
        CheckConstraint('cash_percentage >= 0 AND cash_percentage <= 100', name='check_cash_percentage'),
        CheckConstraint('top_10_holdings_percentage >= 0 AND top_10_holdings_percentage <= 100', name='check_top_10_percentage'),
        CheckConstraint('portfolio_turnover_ratio >= 0', name='check_turnover_positive'),
    )