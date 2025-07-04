from flask import Blueprint, jsonify, request
from models import Fund, FundFactSheet, FundReturns, FundHolding, NavHistory, BSEScheme
from setup_db import db
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Create blueprint
fund_api = Blueprint('fund_api', __name__)


@fund_api.route('/api/funds', methods=['GET'])
def get_funds():
    """Get all funds or filter by AMC or fund type"""
    try:
        # Get query parameters
        amc_name = request.args.get('amc_name')
        fund_type = request.args.get('fund_type')

        # Base query
        query = Fund.query

        # Only include funds that have NAV, Holdings, and Returns
        query = query.filter(
            db.session.query(
                NavHistory.isin).filter(NavHistory.isin == Fund.isin).exists(),
            db.session.query(FundHolding.isin).filter(
                FundHolding.isin == Fund.isin).exists(),
            db.session.query(FundReturns.isin).filter(
                FundReturns.isin == Fund.isin).exists(),
            db.session.query(FundFactSheet.isin).filter(
                FundFactSheet.isin == Fund.isin).exists())

        # Apply filters if provided
        if amc_name:
            query = query.filter(Fund.amc_name.ilike(f'%{amc_name}%'))
        if fund_type:
            query = query.filter(Fund.fund_type == fund_type)

        # Execute query with pagination
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 20, type=int)
        paginated_funds = query.paginate(page=page,
                                         per_page=per_page,
                                         error_out=False)

        # Format response
        funds = []
        for fund in paginated_funds.items:
            funds.append({
                'isin': fund.isin,
                'scheme_name': fund.scheme_name,
                'fund_type': fund.fund_type,
                'fund_subtype': fund.fund_subtype,
                'amc_name': fund.amc_name
            })

        # Add pagination metadata
        response = {
            'funds': funds,
            'pagination': {
                'total_items': paginated_funds.total,
                'total_pages': paginated_funds.pages,
                'current_page': paginated_funds.page,
                'per_page': paginated_funds.per_page
            }
        }

        return jsonify(response), 200
    except Exception as e:
        logger.error(f"Error getting funds: {e}")
        return jsonify({'error': str(e)}), 500


@fund_api.route('/api/funds/<isin>', methods=['GET'])
def get_fund(isin):
    """Get a fund by ISIN"""
    try:
        # Get fund
        fund = Fund.query.filter_by(isin=isin).first()
        if not fund:
            return jsonify({'error': f'Fund with ISIN {isin} not found'}), 404

        # Format response
        response = {
            'isin': fund.isin,
            'scheme_name': fund.scheme_name,
            'fund_type': fund.fund_type,
            'fund_subtype': fund.fund_subtype,
            'amc_name': fund.amc_name,
            'created_at':
            fund.created_at.isoformat() if fund.created_at else None,
            'updated_at':
            fund.updated_at.isoformat() if fund.updated_at else None
        }

        return jsonify(response), 200
    except Exception as e:
        logger.error(f"Error getting fund {isin}: {e}")
        return jsonify({'error': str(e)}), 500


@fund_api.route('/api/funds/<isin>/factsheet', methods=['GET'])
def get_fund_factsheet(isin):
    """Get a fund's factsheet"""
    try:
        # Get fund
        fund = Fund.query.filter_by(isin=isin).first()
        if not fund:
            return jsonify({'error': f'Fund with ISIN {isin} not found'}), 404

        # Get factsheet
        factsheet = FundFactSheet.query.filter_by(isin=isin).first()
        if not factsheet:
            return jsonify(
                {'error':
                 f'Factsheet for fund with ISIN {isin} not found'}), 404

        # Format enhanced response with all factsheet fields
        response = {
            'isin':
            factsheet.isin,
            # Core fund information
            'scheme_name':
            factsheet.scheme_name,
            'scheme_type':
            factsheet.scheme_type,
            'sub_category':
            factsheet.sub_category,
            'plan':
            factsheet.plan,
            'amc':
            factsheet.amc,
            # Financial details
            'expense_ratio':
            factsheet.expense_ratio,
            'minimum_lumpsum':
            factsheet.minimum_lumpsum,
            'minimum_sip':
            factsheet.minimum_sip,
            # Investment terms
            'lock_in':
            factsheet.lock_in,
            'exit_load':
            factsheet.exit_load,
            # Management and risk
            'fund_manager':
            factsheet.fund_manager,
            'benchmark':
            factsheet.benchmark,
            'sebi_risk_category':
            factsheet.sebi_risk_category,
            # Legacy fields
            'launch_date':
            factsheet.launch_date.isoformat()
            if factsheet.launch_date else None,
            'last_updated':
            factsheet.last_updated.isoformat()
            if factsheet.last_updated else None
        }

        return jsonify(response), 200
    except Exception as e:
        logger.error(f"Error getting factsheet for fund {isin}: {e}")
        return jsonify({'error': str(e)}), 500


@fund_api.route('/api/funds/<isin>/returns', methods=['GET'])
def get_fund_returns(isin):
    """Get a fund's returns"""
    try:
        # Get fund
        fund = Fund.query.filter_by(isin=isin).first()
        if not fund:
            return jsonify({'error': f'Fund with ISIN {isin} not found'}), 404

        # Get returns
        returns = FundReturns.query.filter_by(isin=isin).first()
        if not returns:
            return jsonify(
                {'error': f'Returns for fund with ISIN {isin} not found'}), 404

        # Format response
        response = {
            'isin':
            returns.isin,
            'return_1m':
            returns.return_1m,
            'return_3m':
            returns.return_3m,
            'return_6m':
            returns.return_6m,
            'return_ytd':
            returns.return_ytd,
            'return_1y':
            returns.return_1y,
            'return_3y':
            returns.return_3y,
            'return_5y':
            returns.return_5y,
            'last_updated':
            returns.last_updated.isoformat() if returns.last_updated else None
        }

        return jsonify(response), 200
    except Exception as e:
        logger.error(f"Error getting returns for fund {isin}: {e}")
        return jsonify({'error': str(e)}), 500


@fund_api.route('/api/funds/<isin>/holdings', methods=['GET'])
def get_fund_holdings(isin):
    """Get a fund's portfolio holdings"""
    try:
        # Get fund
        fund = Fund.query.filter_by(isin=isin).first()
        if not fund:
            return jsonify({'error': f'Fund with ISIN {isin} not found'}), 404

        # Get holdings with pagination
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 20, type=int)

        paginated_holdings = FundHolding.query.filter_by(isin=isin).paginate(
            page=page, per_page=per_page, error_out=False)

        if paginated_holdings.total == 0:
            return jsonify({
                'holdings': [],
                'pagination': {
                    'total_items': 0,
                    'total_pages': 0,
                    'current_page': page,
                    'per_page': per_page
                }
            }), 200

        # Format response
        holdings = []
        for holding in paginated_holdings.items:
            holdings.append({
                'id':
                holding.id,
                'instrument_name':
                holding.instrument_name,
                'instrument_isin':
                holding.instrument_isin,
                'instrument_type':
                holding.instrument_type,
                'sector':
                holding.sector,
                'percentage_to_nav':
                holding.percentage_to_nav,
                'quantity':
                holding.quantity,
                'value':
                holding.value,
                'coupon':
                holding.coupon,
                'yield_value':
                holding.yield_value,
                'last_updated':
                holding.last_updated.isoformat()
                if holding.last_updated else None
            })

        # Add pagination metadata
        response = {
            'holdings': holdings,
            'pagination': {
                'total_items': paginated_holdings.total,
                'total_pages': paginated_holdings.pages,
                'current_page': paginated_holdings.page,
                'per_page': paginated_holdings.per_page
            }
        }

        return jsonify(response), 200
    except Exception as e:
        logger.error(f"Error getting holdings for fund {isin}: {e}")
        return jsonify({'error': str(e)}), 500


@fund_api.route('/api/funds/<isin>/nav', methods=['GET'])
def get_fund_nav(isin):
    """Get a fund's NAV history"""
    try:
        # Get fund
        fund = Fund.query.filter_by(isin=isin).first()
        if not fund:
            return jsonify({'error': f'Fund with ISIN {isin} not found'}), 404

        # Get date range parameters
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')

        # Base query
        query = NavHistory.query.filter_by(isin=isin)

        # Apply date filters if provided
        if start_date:
            query = query.filter(NavHistory.date >= start_date)
        if end_date:
            query = query.filter(NavHistory.date <= end_date)

        # Order by date
        query = query.order_by(NavHistory.date.desc())

        # Paginate
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 30, type=int)
        paginated_nav = query.paginate(page=page,
                                       per_page=per_page,
                                       error_out=False)

        if paginated_nav.total == 0:
            return jsonify({
                'nav_history': [],
                'pagination': {
                    'total_items': 0,
                    'total_pages': 0,
                    'current_page': page,
                    'per_page': per_page
                }
            }), 200

        # Format response
        nav_history = []
        for nav in paginated_nav.items:
            nav_history.append({
                'id': nav.id,
                'date': nav.date.isoformat(),
                'nav': nav.nav
            })

        # Add pagination metadata
        response = {
            'nav_history': nav_history,
            'pagination': {
                'total_items': paginated_nav.total,
                'total_pages': paginated_nav.pages,
                'current_page': paginated_nav.page,
                'per_page': paginated_nav.per_page
            }
        }

        return jsonify(response), 200
    except Exception as e:
        logger.error(f"Error getting NAV history for fund {isin}: {e}")
        return jsonify({'error': str(e)}), 500


@fund_api.route('/api/funds/<isin>/all', methods=['GET'])
def get_fund_all(isin):
    """Get all fund data including factsheet, returns, and most recent NAV"""
    try:
        # Get fund
        fund = Fund.query.filter_by(isin=isin).first()
        if not fund:
            return jsonify({'error': f'Fund with ISIN {isin} not found'}), 404

        # Get factsheet
        factsheet = FundFactSheet.query.filter_by(isin=isin).first()
        factsheet_data = None
        if factsheet:
            factsheet_data = {
                # Core fund information
                'scheme_name':
                factsheet.scheme_name,
                'scheme_type':
                factsheet.scheme_type,
                'sub_category':
                factsheet.sub_category,
                'plan':
                factsheet.plan,
                'amc':
                factsheet.amc,
                # Financial details
                'expense_ratio':
                factsheet.expense_ratio,
                'minimum_lumpsum':
                factsheet.minimum_lumpsum,
                'minimum_sip':
                factsheet.minimum_sip,
                # Investment terms
                'lock_in':
                factsheet.lock_in,
                'exit_load':
                factsheet.exit_load,
                # Management and risk
                'fund_manager':
                factsheet.fund_manager,
                'benchmark':
                factsheet.benchmark,
                'sebi_risk_category':
                factsheet.sebi_risk_category,
                # Legacy fields
                'launch_date':
                factsheet.launch_date.isoformat()
                if factsheet.launch_date else None,
                'last_updated':
                factsheet.last_updated.isoformat()
                if factsheet.last_updated else None
            }

        # Get returns
        returns = FundReturns.query.filter_by(isin=isin).first()
        returns_data = None
        if returns:
            returns_data = {
                'return_1m':
                returns.return_1m,
                'return_3m':
                returns.return_3m,
                'return_6m':
                returns.return_6m,
                'return_ytd':
                returns.return_ytd,
                'return_1y':
                returns.return_1y,
                'return_3y':
                returns.return_3y,
                'return_5y':
                returns.return_5y,
                'last_updated':
                returns.last_updated.isoformat()
                if returns.last_updated else None
            }

        # Get most recent NAV
        most_recent_nav = NavHistory.query.filter_by(isin=isin).order_by(
            NavHistory.date.desc()).first()
        nav_data = None
        if most_recent_nav:
            nav_data = {
                'date': most_recent_nav.date.isoformat(),
                'nav': most_recent_nav.nav
            }

        # Format complete response
        response = {
            'isin': fund.isin,
            'scheme_name': fund.scheme_name,
            'fund_type': fund.fund_type,
            'fund_subtype': fund.fund_subtype,
            'amc_name': fund.amc_name,
            'created_at':
            fund.created_at.isoformat() if fund.created_at else None,
            'updated_at':
            fund.updated_at.isoformat() if fund.updated_at else None,
            'factsheet': factsheet_data,
            'returns': returns_data,
            'latest_nav': nav_data
        }

        return jsonify(response), 200
    except Exception as e:
        logger.error(f"Error getting all data for fund {isin}: {e}")
        return jsonify({'error': str(e)}), 500


@fund_api.route('/api/funds/<isin>/complete', methods=['GET'])
def get_fund_complete(isin):
    """Get comprehensive fund data including factsheet, returns, latest NAV, portfolio holdings, and sector analysis"""
    try:
        # Get fund
        fund = Fund.query.filter_by(isin=isin).first()
        if not fund:
            return jsonify({'error': f'Fund with ISIN {isin} not found'}), 404

        # Get factsheet
        factsheet = FundFactSheet.query.filter_by(isin=isin).first()
        factsheet_data = None
        if factsheet:
            factsheet_data = {
                # Core fund information
                'scheme_name':
                factsheet.scheme_name,
                'scheme_type':
                factsheet.scheme_type,
                'sub_category':
                factsheet.sub_category,
                'plan':
                factsheet.plan,
                'amc':
                factsheet.amc,
                # Financial details
                'expense_ratio':
                factsheet.expense_ratio,
                'minimum_lumpsum':
                factsheet.minimum_lumpsum,
                'minimum_sip':
                factsheet.minimum_sip,
                # Investment terms
                'lock_in':
                factsheet.lock_in,
                'exit_load':
                factsheet.exit_load,
                # Management and risk
                'fund_manager':
                factsheet.fund_manager,
                'benchmark':
                factsheet.benchmark,
                'sebi_risk_category':
                factsheet.sebi_risk_category,
                # Legacy fields
                'launch_date':
                factsheet.launch_date.isoformat()
                if factsheet.launch_date else None,
                'last_updated':
                factsheet.last_updated.isoformat()
                if factsheet.last_updated else None
            }

        # Get returns
        returns = FundReturns.query.filter_by(isin=isin).first()
        returns_data = None
        if returns:
            returns_data = {
                'return_1m':
                returns.return_1m,
                'return_3m':
                returns.return_3m,
                'return_6m':
                returns.return_6m,
                'return_ytd':
                returns.return_ytd,
                'return_1y':
                returns.return_1y,
                'return_3y':
                returns.return_3y,
                'return_5y':
                returns.return_5y,
                'last_updated':
                returns.last_updated.isoformat()
                if returns.last_updated else None
            }

        # Get NAV history (last 30 days)
        nav_history = NavHistory.query.filter_by(isin=isin).order_by(
            NavHistory.date.desc()).limit(30).all()
        nav_history_data = []
        latest_nav = None

        if nav_history:
            for nav in nav_history:
                nav_history_data.append({
                    'date': nav.date.isoformat(),
                    'nav': nav.nav
                })

            # First one is the most recent
            if nav_history_data:
                latest_nav = nav_history_data[0]

        # Get portfolio holdings
        holdings = FundHolding.query.filter_by(isin=isin).all()
        holdings_data = []

        # Prepare data for sector analysis
        sector_map = {}
        equity_allocation = 0
        debt_allocation = 0
        cash_allocation = 0
        other_allocation = 0

        if holdings:
            for holding in holdings:
                # Add to holdings list
                holdings_data.append({
                    'instrument_name': holding.instrument_name,
                    'instrument_type': holding.instrument_type,
                    'sector': holding.sector,
                    'percentage_to_nav': holding.percentage_to_nav,
                    'quantity': holding.quantity,
                    'value': holding.value,
                    'coupon': holding.coupon,
                    'yield_value': holding.yield_value
                })

                # Track sector allocation
                if holding.sector:
                    if holding.sector in sector_map:
                        sector_map[holding.sector] += holding.percentage_to_nav
                    else:
                        sector_map[holding.sector] = holding.percentage_to_nav

                # Track asset class allocation
                if holding.instrument_type == 'Equity or related':
                    equity_allocation += holding.percentage_to_nav
                elif holding.instrument_type == 'Debt':
                    debt_allocation += holding.percentage_to_nav
                elif holding.instrument_type == 'Cash':
                    cash_allocation += holding.percentage_to_nav
                else:
                    other_allocation += holding.percentage_to_nav

        # Create top sectors list
        top_sectors = []
        for sector, allocation in sorted(sector_map.items(),
                                         key=lambda x: x[1],
                                         reverse=True):
            top_sectors.append({'sector': sector, 'allocation': allocation})

        # Add asset allocation breakdown
        asset_allocation = {
            'equity': equity_allocation,
            'debt': debt_allocation,
            'cash': cash_allocation,
            'other': other_allocation
        }

        # Format complete response
        response = {
            'fund': {
                'isin':
                fund.isin,
                'scheme_name':
                fund.scheme_name,
                'fund_type':
                fund.fund_type,
                'fund_subtype':
                fund.fund_subtype,
                'amc_name':
                fund.amc_name,
                'created_at':
                fund.created_at.isoformat() if fund.created_at else None,
                'updated_at':
                fund.updated_at.isoformat() if fund.updated_at else None
            },
            'factsheet': factsheet_data,
            'returns': returns_data,
            'latest_nav': latest_nav,
            'nav_history': nav_history_data,
            'holdings': holdings_data,
            'analysis': {
                'top_sectors': top_sectors,
                'asset_allocation': asset_allocation
            }
        }

        return jsonify(response), 200
    except Exception as e:
        logger.error(f"Error getting complete data for fund {isin}: {e}")
        return jsonify({'error': str(e)}), 500


# BSE Scheme API Endpoints


@fund_api.route('/api/bse-schemes', methods=['GET'])
def get_bse_schemes():
    """Get BSE schemes with filtering options"""
    try:
        # Get query parameters
        scheme_name = request.args.get('scheme_name')
        amc_code = request.args.get('amc_code')
        isin = request.args.get('isin')
        scheme_type = request.args.get('scheme_type')
        active_only = request.args.get('active_only',
                                       'false').lower() == 'true'
        purchase_allowed = request.args.get('purchase_allowed',
                                            'false').lower() == 'true'

        # Base query
        query = BSEScheme.query

        # Apply filters
        if scheme_name:
            query = query.filter(
                BSEScheme.scheme_name.ilike(f'%{scheme_name}%'))
        if amc_code:
            query = query.filter(BSEScheme.amc_code == amc_code)
        if isin:
            query = query.filter(BSEScheme.isin == isin)
        if scheme_type:
            query = query.filter(
                BSEScheme.scheme_type.ilike(f'%{scheme_type}%'))
        if active_only:
            query = query.filter(BSEScheme.amc_active_flag == 1)
        if purchase_allowed:
            query = query.filter(BSEScheme.purchase_allowed == 'Y')

        # Pagination
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 20, type=int)
        paginated_schemes = query.paginate(page=page,
                                           per_page=per_page,
                                           error_out=False)

        # Format response
        schemes = []
        for scheme in paginated_schemes.items:
            schemes.append({
                'unique_no': scheme.unique_no,
                'scheme_code': scheme.scheme_code,
                'scheme_name': scheme.scheme_name,
                'isin': scheme.isin,
                'amc_code': scheme.amc_code,
                'scheme_type': scheme.scheme_type,
                'scheme_plan': scheme.scheme_plan,
                'purchase_allowed': scheme.purchase_allowed,
                'redemption_allowed': scheme.redemption_allowed,
                'amc_active_flag': scheme.amc_active_flag,
                'sip_flag': scheme.sip_flag,
                'stp_flag': scheme.stp_flag,
                'swp_flag': scheme.swp_flag,
                'switch_flag': scheme.switch_flag,
                'minimum_purchase_amount': scheme.minimum_purchase_amount,
                'minimum_redemption_qty': scheme.minimum_redemption_qty,
                'exit_load_flag': scheme.exit_load_flag,
                'lockin_period_flag': scheme.lockin_period_flag
            })

        response = {
            'schemes': schemes,
            'pagination': {
                'total_items': paginated_schemes.total,
                'total_pages': paginated_schemes.pages,
                'current_page': paginated_schemes.page,
                'per_page': paginated_schemes.per_page,
                'has_next': paginated_schemes.has_next,
                'has_prev': paginated_schemes.has_prev
            }
        }

        return jsonify(response), 200
    except Exception as e:
        logger.error(f"Error getting BSE schemes: {e}")
        return jsonify({'error': str(e)}), 500


@fund_api.route('/api/bse-schemes/<int:unique_no>', methods=['GET'])
def get_bse_scheme(unique_no):
    """Get BSE scheme by unique number"""
    try:
        scheme = BSEScheme.query.filter_by(unique_no=unique_no).first()

        if not scheme:
            return jsonify({'error': 'BSE scheme not found'}), 404

        # Format complete scheme data
        scheme_data = {
            'unique_no': scheme.unique_no,
            'scheme_code': scheme.scheme_code,
            'rta_scheme_code': scheme.rta_scheme_code,
            'amc_scheme_code': scheme.amc_scheme_code,
            'isin': scheme.isin,
            'amc_code': scheme.amc_code,
            'scheme_type': scheme.scheme_type,
            'scheme_plan': scheme.scheme_plan,
            'scheme_name': scheme.scheme_name,
            'purchase_details': {
                'purchase_allowed': scheme.purchase_allowed,
                'purchase_transaction_mode': scheme.purchase_transaction_mode,
                'minimum_purchase_amount': scheme.minimum_purchase_amount,
                'additional_purchase_amount':
                scheme.additional_purchase_amount,
                'maximum_purchase_amount': scheme.maximum_purchase_amount,
                'purchase_amount_multiplier':
                scheme.purchase_amount_multiplier,
                'purchase_cutoff_time': scheme.purchase_cutoff_time
            },
            'redemption_details': {
                'redemption_allowed': scheme.redemption_allowed,
                'redemption_transaction_mode':
                scheme.redemption_transaction_mode,
                'minimum_redemption_qty': scheme.minimum_redemption_qty,
                'redemption_qty_multiplier': scheme.redemption_qty_multiplier,
                'maximum_redemption_qty': scheme.maximum_redemption_qty,
                'redemption_amount_minimum': scheme.redemption_amount_minimum,
                'redemption_amount_maximum': scheme.redemption_amount_maximum,
                'redemption_amount_multiple':
                scheme.redemption_amount_multiple,
                'redemption_cutoff_time': scheme.redemption_cutoff_time
            },
            'operational_details': {
                'rta_agent_code': scheme.rta_agent_code,
                'amc_active_flag': scheme.amc_active_flag,
                'dividend_reinvestment_flag':
                scheme.dividend_reinvestment_flag,
                'settlement_type': scheme.settlement_type,
                'amc_ind': scheme.amc_ind,
                'face_value': scheme.face_value,
                'channel_partner_code': scheme.channel_partner_code
            },
            'transaction_flags': {
                'sip_flag': scheme.sip_flag,
                'stp_flag': scheme.stp_flag,
                'swp_flag': scheme.swp_flag,
                'switch_flag': scheme.switch_flag
            },
            'dates': {
                'start_date':
                scheme.start_date.isoformat() if scheme.start_date else None,
                'end_date':
                scheme.end_date.isoformat() if scheme.end_date else None,
                'reopening_date':
                scheme.reopening_date.isoformat()
                if scheme.reopening_date else None
            },
            'exit_load_details': {
                'exit_load_flag': scheme.exit_load_flag,
                'exit_load': scheme.exit_load
            },
            'lockin_details': {
                'lockin_period_flag': scheme.lockin_period_flag,
                'lockin_period': scheme.lockin_period
            }
        }

        return jsonify(scheme_data), 200
    except Exception as e:
        logger.error(f"Error getting BSE scheme {unique_no}: {e}")
        return jsonify({'error': str(e)}), 500


@fund_api.route('/api/bse-schemes/by-isin/<isin>', methods=['GET'])
def get_bse_scheme_by_isin(isin):
    """Get BSE scheme by ISIN"""
    try:
        scheme = BSEScheme.query.filter_by(isin=isin).first()

        if not scheme:
            return jsonify({'error':
                            'BSE scheme not found for this ISIN'}), 404

        # Return the same detailed format as get_bse_scheme
        return get_bse_scheme(scheme.unique_no)
    except Exception as e:
        logger.error(f"Error getting BSE scheme by ISIN {isin}: {e}")
        return jsonify({'error': str(e)}), 500


@fund_api.route('/api/bse-schemes/transaction-flags', methods=['GET'])
def get_bse_transaction_flags():
    """Get BSE schemes with transaction flags summary"""
    try:
        schemes = BSEScheme.query.filter_by(amc_active_flag=1).all()

        # Count schemes by transaction type
        sip_enabled = sum(1 for s in schemes if s.sip_flag == 'Y')
        stp_enabled = sum(1 for s in schemes if s.stp_flag == 'Y')
        swp_enabled = sum(1 for s in schemes if s.swp_flag == 'Y')
        switch_enabled = sum(1 for s in schemes if s.switch_flag == 'Y')
        purchase_allowed = sum(1 for s in schemes if s.purchase_allowed == 'Y')
        redemption_allowed = sum(1 for s in schemes
                                 if s.redemption_allowed == 'Y')

        summary = {
            'total_active_schemes': len(schemes),
            'transaction_flags': {
                'sip_enabled': sip_enabled,
                'stp_enabled': stp_enabled,
                'swp_enabled': swp_enabled,
                'switch_enabled': switch_enabled,
                'purchase_allowed': purchase_allowed,
                'redemption_allowed': redemption_allowed
            }
        }

        return jsonify(summary), 200
    except Exception as e:
        logger.error(f"Error getting BSE transaction flags: {e}")
        return jsonify({'error': str(e)}), 500


def init_fund_api(app):
    """Initialize fund API routes"""
    app.register_blueprint(fund_api)
    logger.info("Fund API routes registered")

    return app
