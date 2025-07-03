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
    fund_type = db.Column(db.String(50),
                          nullable=False)  # Type (equity, debt, hybrid)
    fund_subtype = db.Column(db.String(100), nullable=True)  # Subtype
    amc_name = db.Column(db.String(100), nullable=False)  # Fund house name
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime,
                           default=datetime.utcnow,
                           onupdate=datetime.utcnow)

    # Relationships defined later with backref

    __table_args__ = (
        Index('idx_fund_amc_name', 'amc_name'),  # Optimize AMC lookups
        Index('idx_fund_type', 'fund_type'),  # Optimize fund type lookups
    )


class FundFactSheet(db.Model):
    """
    Enhanced factsheet information for a mutual fund
    Supports Excel columns: Scheme Name, Scheme Type, Sub Category, Plan, 
    Expense Ratio, AMC, Benchmark, Minimum Lumpsum, Lock-in, Exit Load, 
    Fund Manager, Minimum SIP, SEBI Risk Category, ISIN
    """
    __tablename__ = 'mf_factsheet'

    isin = db.Column(db.String(12),
                     db.ForeignKey('mf_fund.isin'),
                     primary_key=True)

    # Core fund information
    scheme_name = db.Column(db.String(255),
                            nullable=True)  # From 'Scheme Name'
    scheme_type = db.Column(db.String(100),
                            nullable=True)  # From 'Scheme Type'
    sub_category = db.Column(db.String(100),
                             nullable=True)  # From 'Sub Category'
    plan = db.Column(db.String(50),
                     nullable=True)  # From 'Plan' (Growth/Dividend)
    amc = db.Column(db.String(100),
                    nullable=True)  # From 'AMC' (Asset Management Company)

    # Financial details
    expense_ratio = db.Column(db.Float,
                              nullable=True)  # From 'Expense Ratio' percentage
    minimum_lumpsum = db.Column(db.Float,
                                nullable=True)  # From 'Minimum Lumpsum'
    minimum_sip = db.Column(db.Float, nullable=True)  # From 'Minimum SIP'

    # Investment terms
    lock_in = db.Column(db.String(100), nullable=True)  # From 'Lock-in' period
    exit_load = db.Column(db.String(255),
                          nullable=True)  # From 'Exit Load' details

    # Management and risk
    fund_manager = db.Column(db.String(255),
                             nullable=True)  # From 'Fund Manager'
    benchmark = db.Column(db.String(255), nullable=True)  # From 'Benchmark'
    sebi_risk_category = db.Column(db.String(50),
                                   nullable=True)  # From 'SEBI Risk Category'

    # Legacy fields (for backward compatibility)
    launch_date = db.Column(db.Date, nullable=True)  # Launch date of the fund

    # Metadata
    last_updated = db.Column(db.DateTime,
                             default=datetime.utcnow,
                             onupdate=datetime.utcnow)

    # Relationship to Fund
    fund = db.relationship("Fund", backref="factsheet")

    # Index for common searches
    __table_args__ = (
        Index('idx_factsheet_scheme_type', 'scheme_type'),
        Index('idx_factsheet_sub_category', 'sub_category'),
        Index('idx_factsheet_amc', 'amc'),
        Index('idx_factsheet_sebi_risk', 'sebi_risk_category'),
    )


class FundReturns(db.Model):
    """
    Returns data for a mutual fund
    """
    __tablename__ = 'mf_returns'

    isin = db.Column(db.String(12),
                     db.ForeignKey('mf_fund.isin'),
                     primary_key=True)
    return_1m = db.Column(db.Float, nullable=True)  # 1-month return percentage
    return_3m = db.Column(db.Float, nullable=True)  # 3-month return percentage
    return_6m = db.Column(db.Float, nullable=True)  # 6-month return percentage
    return_ytd = db.Column(db.Float,
                           nullable=True)  # Year-to-date return percentage
    return_1y = db.Column(db.Float, nullable=True)  # 1-year return percentage
    return_3y = db.Column(db.Float, nullable=True)  # 3-year return percentage
    return_5y = db.Column(db.Float, nullable=True)  # 5-year return percentage
    last_updated = db.Column(db.DateTime,
                             default=datetime.utcnow,
                             onupdate=datetime.utcnow)

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
    isin = db.Column(db.String(12),
                     db.ForeignKey('mf_fund.isin'),
                     nullable=False)  # Scheme ISIN
    instrument_isin = db.Column(db.String(12),
                                nullable=True)  # ISIN of the instrument
    coupon = db.Column(db.Float,
                       nullable=True)  # Coupon percentage for debt instruments
    instrument_name = db.Column(db.String(255),
                                nullable=False)  # Name of Instrument
    sector = db.Column(db.String(255),
                       nullable=True)  # Industry classification
    quantity = db.Column(db.Float, nullable=True)  # Quantity held
    value = db.Column(db.Float, nullable=True)  # Market Value in INR
    percentage_to_nav = db.Column(db.Float, nullable=False)  # % to Net Assets
    yield_value = db.Column(db.Float, nullable=True)  # Yield percentage
    instrument_type = db.Column(db.String(100),
                                nullable=False)  # Type of instrument
    amc_name = db.Column(db.String(255), nullable=True)  # AMC name from upload
    scheme_name = db.Column(db.String(255),
                            nullable=True)  # Scheme Name from upload
    last_updated = db.Column(db.DateTime,
                             default=datetime.utcnow,
                             onupdate=datetime.utcnow)

    # Relationship to Fund
    fund = db.relationship("Fund", backref="fund_holdings")

    __table_args__ = (
        CheckConstraint('percentage_to_nav >= 0',
                        name='check_percentage_to_nav'),
        CheckConstraint('percentage_to_nav <= 100',
                        name='check_percentage_to_nav_upper'),
    )


class NavHistory(db.Model):
    """
    NAV history for a mutual fund
    """
    __tablename__ = 'mf_nav_history'

    id = db.Column(db.Integer, primary_key=True)
    isin = db.Column(db.String(12),
                     db.ForeignKey('mf_fund.isin'),
                     nullable=False)
    date = db.Column(db.Date, nullable=False)  # Date of NAV
    nav = db.Column(db.Float, nullable=False)  # NAV value

    # Relationship to Fund
    fund = db.relationship("Fund", backref="nav_history")

    __table_args__ = (CheckConstraint('nav >= 0', name='check_nav'), )


class FundRating(db.Model):
    """
    Fund ratings from various rating agencies
    """
    __tablename__ = 'mf_fund_ratings'

    id = db.Column(db.Integer, primary_key=True)
    isin = db.Column(db.String(12),
                     db.ForeignKey('mf_fund.isin'),
                     nullable=False)
    rating_agency = db.Column(
        db.String(50),
        nullable=False)  # CRISIL, Morningstar, Value Research, etc.
    rating_category = db.Column(
        db.String(50), nullable=False)  # Overall, Risk, Return, Expense, etc.
    rating_value = db.Column(db.String(10),
                             nullable=False)  # 5 Star, AAA, High, etc.
    rating_numeric = db.Column(
        db.Float,
        nullable=True)  # Numeric equivalent (1-5 for stars, 1-10 for scores)
    rating_date = db.Column(db.Date,
                            nullable=False)  # Date when rating was assigned
    rating_outlook = db.Column(
        db.String(20),
        nullable=True)  # Positive, Negative, Stable, Under Review
    rating_description = db.Column(
        db.Text, nullable=True)  # Additional rating commentary
    is_current = db.Column(
        db.Boolean, default=True)  # Flag to mark current vs historical ratings
    recommended = db.Column(db.Boolean,
                            default=False)  # Devmani recommendation flag
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime,
                           default=datetime.utcnow,
                           onupdate=datetime.utcnow)

    # Relationship to Fund
    fund = db.relationship("Fund", backref="fund_ratings")

    __table_args__ = (
        Index('idx_rating_agency_category', 'rating_agency',
              'rating_category'),
        Index('idx_rating_current', 'is_current'),
        Index('idx_rating_date', 'rating_date'),
        Index('idx_devmani_recommended', 'recommended'),
        CheckConstraint('rating_numeric >= 0',
                        name='check_rating_numeric_positive'),
        CheckConstraint('rating_numeric <= 10',
                        name='check_rating_numeric_max'),
    )


class FundAnalytics(db.Model):
    """
    Advanced analytics and metrics for mutual funds
    """
    __tablename__ = 'mf_fund_analytics'

    id = db.Column(db.Integer, primary_key=True)
    isin = db.Column(db.String(12),
                     db.ForeignKey('mf_fund.isin'),
                     nullable=False)

    # Risk Metrics
    beta = db.Column(db.Float, nullable=True)  # Beta coefficient vs benchmark
    alpha = db.Column(db.Float,
                      nullable=True)  # Alpha (excess return vs benchmark)
    standard_deviation = db.Column(db.Float,
                                   nullable=True)  # Volatility measure
    sharpe_ratio = db.Column(db.Float, nullable=True)  # Risk-adjusted return
    sortino_ratio = db.Column(db.Float,
                              nullable=True)  # Downside risk-adjusted return
    treynor_ratio = db.Column(
        db.Float, nullable=True)  # Return per unit of systematic risk
    information_ratio = db.Column(
        db.Float, nullable=True)  # Active return vs tracking error

    # Performance Metrics
    tracking_error = db.Column(
        db.Float, nullable=True)  # Standard deviation of excess returns
    r_squared = db.Column(db.Float,
                          nullable=True)  # Correlation with benchmark (0-100)
    maximum_drawdown = db.Column(
        db.Float, nullable=True)  # Largest peak-to-trough decline
    calmar_ratio = db.Column(db.Float,
                             nullable=True)  # Annual return / max drawdown

    # Market Timing Metrics
    up_capture_ratio = db.Column(
        db.Float, nullable=True)  # Performance in rising markets
    down_capture_ratio = db.Column(
        db.Float, nullable=True)  # Performance in falling markets

    # Advanced Metrics
    var_95 = db.Column(db.Float,
                       nullable=True)  # Value at Risk (95% confidence)
    var_99 = db.Column(db.Float,
                       nullable=True)  # Value at Risk (99% confidence)
    skewness = db.Column(db.Float,
                         nullable=True)  # Return distribution asymmetry
    kurtosis = db.Column(db.Float,
                         nullable=True)  # Return distribution tail risk

    # Reference Data
    benchmark_index = db.Column(db.String(50),
                                nullable=True)  # Primary benchmark
    calculation_period = db.Column(
        db.String(20), nullable=True)  # Period for calculations (1Y, 3Y, 5Y)
    calculation_date = db.Column(db.Date,
                                 nullable=False)  # Date of calculation

    # Metadata
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime,
                           default=datetime.utcnow,
                           onupdate=datetime.utcnow)

    # Relationship to Fund
    fund = db.relationship("Fund", backref="fund_analytics")

    __table_args__ = (
        Index('idx_analytics_calculation_date', 'calculation_date'),
        Index('idx_analytics_period', 'calculation_period'),
        Index('idx_analytics_benchmark', 'benchmark_index'),
        CheckConstraint('r_squared >= 0 AND r_squared <= 100',
                        name='check_r_squared_range'),
        CheckConstraint('sharpe_ratio >= -10 AND sharpe_ratio <= 10',
                        name='check_sharpe_ratio_range'),
        CheckConstraint('beta >= 0', name='check_beta_positive'),
    )


class FundStatistics(db.Model):
    """
    Statistical data and portfolio composition metrics for mutual funds
    """
    __tablename__ = 'mf_fund_statistics'

    id = db.Column(db.Integer, primary_key=True)
    isin = db.Column(db.String(12),
                     db.ForeignKey('mf_fund.isin'),
                     nullable=False)

    # Portfolio Composition
    total_holdings = db.Column(db.Integer, nullable=True)  # Number of holdings
    top_10_holdings_percentage = db.Column(
        db.Float, nullable=True)  # % in top 10 holdings
    equity_percentage = db.Column(db.Float,
                                  nullable=True)  # Equity allocation %
    debt_percentage = db.Column(db.Float, nullable=True)  # Debt allocation %
    cash_percentage = db.Column(db.Float,
                                nullable=True)  # Cash and equivalents %
    other_percentage = db.Column(db.Float,
                                 nullable=True)  # Other investments %

    # Market Cap Distribution
    large_cap_percentage = db.Column(db.Float,
                                     nullable=True)  # Large cap allocation
    mid_cap_percentage = db.Column(db.Float,
                                   nullable=True)  # Mid cap allocation
    small_cap_percentage = db.Column(db.Float,
                                     nullable=True)  # Small cap allocation

    # Sector Concentration
    top_sector_name = db.Column(db.String(100),
                                nullable=True)  # Highest weighted sector
    top_sector_percentage = db.Column(db.Float,
                                      nullable=True)  # Top sector weight
    sector_concentration_ratio = db.Column(
        db.Float, nullable=True)  # Top 3 sectors combined %

    # Credit Quality (for debt funds)
    aaa_percentage = db.Column(db.Float, nullable=True)  # AAA rated securities
    aa_percentage = db.Column(db.Float, nullable=True)  # AA rated securities
    a_percentage = db.Column(db.Float, nullable=True)  # A rated securities
    below_a_percentage = db.Column(db.Float,
                                   nullable=True)  # Below A rated securities
    unrated_percentage = db.Column(db.Float,
                                   nullable=True)  # Unrated securities

    # Duration Metrics (for debt funds)
    average_maturity = db.Column(db.Float,
                                 nullable=True)  # Average maturity in years
    modified_duration = db.Column(db.Float,
                                  nullable=True)  # Interest rate sensitivity
    yield_to_maturity = db.Column(db.Float, nullable=True)  # Portfolio YTM

    # Flow Statistics
    monthly_inflow = db.Column(db.Float,
                               nullable=True)  # Last month inflow (crores)
    monthly_outflow = db.Column(db.Float,
                                nullable=True)  # Last month outflow (crores)
    net_flow = db.Column(db.Float,
                         nullable=True)  # Net flow (inflow - outflow)
    quarterly_flow = db.Column(db.Float,
                               nullable=True)  # Last quarter net flow
    yearly_flow = db.Column(db.Float, nullable=True)  # Last year net flow

    # Turnover Metrics
    portfolio_turnover_ratio = db.Column(db.Float,
                                         nullable=True)  # Annual turnover %

    # Reference Data
    statistics_date = db.Column(db.Date, nullable=False)  # Date of statistics
    data_source = db.Column(db.String(50), nullable=True)  # Data provider

    # Metadata
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime,
                           default=datetime.utcnow,
                           onupdate=datetime.utcnow)

    # Relationship to Fund
    fund = db.relationship("Fund", backref="fund_statistics")

    __table_args__ = (
        Index('idx_statistics_date', 'statistics_date'),
        Index('idx_statistics_source', 'data_source'),
        CheckConstraint('equity_percentage >= 0 AND equity_percentage <= 100',
                        name='check_equity_percentage'),
        CheckConstraint('debt_percentage >= 0 AND debt_percentage <= 100',
                        name='check_debt_percentage'),
        CheckConstraint('cash_percentage >= 0 AND cash_percentage <= 100',
                        name='check_cash_percentage'),
        CheckConstraint(
            'top_10_holdings_percentage >= 0 AND top_10_holdings_percentage <= 100',
            name='check_top_10_percentage'),
        CheckConstraint('portfolio_turnover_ratio >= 0',
                        name='check_turnover_positive'),
    )


class FundCodeLookup(db.Model):
    """
    Code mapping table for mutual funds across different systems
    Maps ISIN, AMFI Code, BSE Code, and other identifier codes
    """
    __tablename__ = 'mf_fund_code_lookup'

    id = db.Column(db.Integer, primary_key=True)
    isin = db.Column(db.String(12),
                     db.ForeignKey('mf_fund.isin'),
                     nullable=False)

    # Standard Codes
    amfi_code = db.Column(db.String(10), nullable=True,
                          unique=True)  # AMFI scheme code
    bse_code = db.Column(db.String(10), nullable=True,
                         unique=True)  # BSE scheme code
    nse_code = db.Column(db.String(10), nullable=True,
                         unique=True)  # NSE scheme code

    # Exchange Specific Codes
    bse_symbol = db.Column(db.String(20), nullable=True)  # BSE trading symbol
    nse_symbol = db.Column(db.String(20), nullable=True)  # NSE trading symbol

    # Regulatory Codes
    sebi_code = db.Column(db.String(15),
                          nullable=True)  # SEBI registration code
    rta_code = db.Column(db.String(15),
                         nullable=True)  # Registrar & Transfer Agent code

    # Third Party Codes
    morningstar_id = db.Column(db.String(20),
                               nullable=True)  # Morningstar identifier
    valueresearch_id = db.Column(db.String(20),
                                 nullable=True)  # Value Research identifier
    crisil_code = db.Column(db.String(15), nullable=True)  # CRISIL code

    # Alternative Identifiers
    scheme_code = db.Column(db.String(20),
                            nullable=True)  # Internal scheme code
    old_isin = db.Column(db.String(12),
                         nullable=True)  # Previous ISIN if changed

    # Status and Metadata
    is_active = db.Column(db.Boolean,
                          default=True)  # Whether codes are currently active
    verification_status = db.Column(
        db.String(20), default='pending')  # verified, pending, failed
    verification_date = db.Column(db.Date,
                                  nullable=True)  # Date of last verification

    # Source Information
    data_source = db.Column(db.String(50),
                            nullable=True)  # Source of code mapping
    last_updated = db.Column(db.DateTime,
                             default=datetime.utcnow,
                             onupdate=datetime.utcnow)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    # Relationship to Fund
    fund = db.relationship("Fund", backref="code_lookup")

    __table_args__ = (
        Index('idx_code_amfi', 'amfi_code'),
        Index('idx_code_bse', 'bse_code'),
        Index('idx_code_nse', 'nse_code'),
        Index('idx_code_sebi', 'sebi_code'),
        Index('idx_code_active', 'is_active'),
        Index('idx_code_verification', 'verification_status'),
        Index('idx_code_morningstar', 'morningstar_id'),
        Index('idx_code_valueresearch', 'valueresearch_id'),
    )


class BSEScheme(db.Model):
    """
    BSE Scheme details with comprehensive transaction and operational parameters
    Contains all BSE-specific scheme information for trading and operations
    """
    __tablename__ = 'mf_bse_scheme'

    # Primary identifiers
    id = db.Column(db.Integer, primary_key=True)
    unique_no = db.Column(db.Integer, nullable=False, unique=True, index=True)
    scheme_code = db.Column(db.String(20), nullable=False, index=True)
    rta_scheme_code = db.Column(db.String(20), nullable=False)
    amc_scheme_code = db.Column(db.String(20), nullable=False)
    isin = db.Column(db.String(12), nullable=False, index=True)
    amc_code = db.Column(db.String(10), nullable=False, index=True)

    # Scheme basic information
    scheme_type = db.Column(db.String(50), nullable=False)
    scheme_plan = db.Column(db.String(50), nullable=False)
    scheme_name = db.Column(db.String(255), nullable=False)

    # Purchase parameters
    purchase_allowed = db.Column(db.String(10), nullable=False)  # Y/N
    purchase_transaction_mode = db.Column(db.String(50), nullable=False)
    minimum_purchase_amount = db.Column(db.Numeric(15, 2), nullable=False)
    additional_purchase_amount = db.Column(db.Numeric(15, 2), nullable=False)
    maximum_purchase_amount = db.Column(db.Numeric(15, 2), nullable=False)
    purchase_amount_multiplier = db.Column(db.Numeric(15, 2), nullable=False)
    purchase_cutoff_time = db.Column(db.String(20), nullable=False)

    # Redemption parameters
    redemption_allowed = db.Column(db.String(10), nullable=False)  # Y/N
    redemption_transaction_mode = db.Column(db.String(50), nullable=False)
    minimum_redemption_qty = db.Column(db.Numeric(15, 4), nullable=False)
    redemption_qty_multiplier = db.Column(db.Numeric(15, 4), nullable=False)
    maximum_redemption_qty = db.Column(db.Numeric(15, 4), nullable=False)
    redemption_amount_minimum = db.Column(db.Numeric(15, 2), nullable=False)
    redemption_amount_maximum = db.Column(db.Numeric(15, 2), nullable=False)
    redemption_amount_multiple = db.Column(db.Numeric(15, 2), nullable=False)
    redemption_cutoff_time = db.Column(db.String(20), nullable=False)

    # Operational details
    rta_agent_code = db.Column(db.String(20), nullable=False)
    amc_active_flag = db.Column(db.Integer, nullable=False)  # 0/1
    dividend_reinvestment_flag = db.Column(db.String(10),
                                           nullable=False)  # Y/N

    # Transaction flags
    sip_flag = db.Column(db.String(10), nullable=False)  # Y/N
    stp_flag = db.Column(db.String(10), nullable=False)  # Y/N
    swp_flag = db.Column(db.String(10), nullable=False)  # Y/N
    switch_flag = db.Column(db.String(10), nullable=False)  # Y/N

    # Settlement and operational parameters
    settlement_type = db.Column(db.String(20), nullable=False)
    amc_ind = db.Column(db.Numeric(10, 2), nullable=True)  # Mostly null values
    face_value = db.Column(db.Numeric(10, 2), nullable=False)

    # Date fields
    start_date = db.Column(db.Date, nullable=False)
    end_date = db.Column(db.Date, nullable=False)
    reopening_date = db.Column(db.Date, nullable=True)  # Many null values

    # Exit load and lock-in details
    exit_load_flag = db.Column(db.String(10), nullable=True)  # Y/N, some nulls
    exit_load = db.Column(db.String(255),
                          nullable=False)  # Can contain complex text
    lockin_period_flag = db.Column(db.String(10),
                                   nullable=True)  # Y/N, some nulls
    lockin_period = db.Column(db.Numeric(10, 0),
                              nullable=True)  # Days, some nulls

    # Channel and distribution
    channel_partner_code = db.Column(db.String(20), nullable=False)

    # Timestamps for tracking
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime,
                           default=datetime.utcnow,
                           onupdate=datetime.utcnow)

    # Indexes for performance
    __table_args__ = (
        db.Index('idx_bse_scheme_isin_amc', 'isin', 'amc_code'),
        db.Index('idx_bse_scheme_type_plan', 'scheme_type', 'scheme_plan'),
        db.Index('idx_bse_scheme_active', 'amc_active_flag'),
        db.Index('idx_bse_scheme_purchase_allowed', 'purchase_allowed'),
        db.Index('idx_bse_scheme_dates', 'start_date', 'end_date'),
    )

    def __repr__(self):
        return f'<BSEScheme {self.scheme_code}: {self.scheme_name[:50]}>'

    def is_active(self):
        """Check if scheme is currently active"""
        today = datetime.utcnow().date()
        return (self.amc_active_flag == 1
                and self.start_date <= today <= self.end_date)

    def is_purchase_allowed(self):
        """Check if purchase is allowed"""
        return self.purchase_allowed.upper() == 'Y' and self.is_active()

    def is_redemption_allowed(self):
        """Check if redemption is allowed"""
        return self.redemption_allowed.upper() == 'Y' and self.is_active()

    def has_sip(self):
        """Check if SIP is available"""
        return self.sip_flag.upper() == 'Y'

    def has_stp(self):
        """Check if STP is available"""
        return self.stp_flag.upper() == 'Y'

    def has_swp(self):
        """Check if SWP is available"""
        return self.swp_flag.upper() == 'Y'

    def has_switch(self):
        """Check if switch is available"""
        return self.switch_flag.upper() == 'Y'

    def has_exit_load(self):
        """Check if exit load is applicable"""
        return (self.exit_load_flag and self.exit_load_flag.upper() == 'Y'
                and self.exit_load.strip() not in ['', 'NIL', 'nil', 'Nil'])

    def has_lockin_period(self):
        """Check if lock-in period is applicable"""
        return (self.lockin_period_flag
                and self.lockin_period_flag.upper() == 'Y'
                and self.lockin_period and self.lockin_period > 0)

    def to_dict(self):
        """Convert model to dictionary for API responses"""
        return {
            'unique_no':
            self.unique_no,
            'scheme_code':
            self.scheme_code,
            'rta_scheme_code':
            self.rta_scheme_code,
            'amc_scheme_code':
            self.amc_scheme_code,
            'isin':
            self.isin,
            'amc_code':
            self.amc_code,
            'scheme_type':
            self.scheme_type,
            'scheme_plan':
            self.scheme_plan,
            'scheme_name':
            self.scheme_name,
            'purchase_allowed':
            self.purchase_allowed,
            'purchase_transaction_mode':
            self.purchase_transaction_mode,
            'minimum_purchase_amount':
            float(self.minimum_purchase_amount),
            'additional_purchase_amount':
            float(self.additional_purchase_amount),
            'maximum_purchase_amount':
            float(self.maximum_purchase_amount),
            'purchase_amount_multiplier':
            float(self.purchase_amount_multiplier),
            'purchase_cutoff_time':
            self.purchase_cutoff_time,
            'redemption_allowed':
            self.redemption_allowed,
            'redemption_transaction_mode':
            self.redemption_transaction_mode,
            'minimum_redemption_qty':
            float(self.minimum_redemption_qty),
            'redemption_qty_multiplier':
            float(self.redemption_qty_multiplier),
            'maximum_redemption_qty':
            float(self.maximum_redemption_qty),
            'redemption_amount_minimum':
            float(self.redemption_amount_minimum),
            'redemption_amount_maximum':
            float(self.redemption_amount_maximum),
            'redemption_amount_multiple':
            float(self.redemption_amount_multiple),
            'redemption_cutoff_time':
            self.redemption_cutoff_time,
            'rta_agent_code':
            self.rta_agent_code,
            'amc_active_flag':
            self.amc_active_flag,
            'dividend_reinvestment_flag':
            self.dividend_reinvestment_flag,
            'sip_flag':
            self.sip_flag,
            'stp_flag':
            self.stp_flag,
            'swp_flag':
            self.swp_flag,
            'switch_flag':
            self.switch_flag,
            'settlement_type':
            self.settlement_type,
            'amc_ind':
            float(self.amc_ind) if self.amc_ind else None,
            'face_value':
            float(self.face_value),
            'start_date':
            self.start_date.isoformat(),
            'end_date':
            self.end_date.isoformat(),
            'reopening_date':
            self.reopening_date.isoformat() if self.reopening_date else None,
            'exit_load_flag':
            self.exit_load_flag,
            'exit_load':
            self.exit_load,
            'lockin_period_flag':
            self.lockin_period_flag,
            'lockin_period':
            float(self.lockin_period) if self.lockin_period else None,
            'channel_partner_code':
            self.channel_partner_code,
            'is_active':
            self.is_active(),
            'purchase_allowed_status':
            self.is_purchase_allowed(),
            'redemption_allowed_status':
            self.is_redemption_allowed(),
            'has_sip':
            self.has_sip(),
            'has_stp':
            self.has_stp(),
            'has_swp':
            self.has_swp(),
            'has_switch':
            self.has_switch(),
            'has_exit_load':
            self.has_exit_load(),
            'has_lockin_period':
            self.has_lockin_period()
        }
