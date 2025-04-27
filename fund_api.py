from flask import Blueprint, jsonify, request
from new_models_updated import Fund, FundFactSheet, FundReturns, PortfolioHolding, NavHistory
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
        
        # Apply filters if provided
        if amc_name:
            query = query.filter(Fund.amc_name.ilike(f'%{amc_name}%'))
        if fund_type:
            query = query.filter(Fund.fund_type == fund_type)
        
        # Execute query with pagination
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 20, type=int)
        paginated_funds = query.paginate(page=page, per_page=per_page, error_out=False)
        
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
            'created_at': fund.created_at.isoformat() if fund.created_at else None,
            'updated_at': fund.updated_at.isoformat() if fund.updated_at else None
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
            return jsonify({'error': f'Factsheet for fund with ISIN {isin} not found'}), 404
        
        # Format response
        response = {
            'isin': factsheet.isin,
            'fund_manager': factsheet.fund_manager,
            'aum': factsheet.aum,
            'expense_ratio': factsheet.expense_ratio,
            'launch_date': factsheet.launch_date.isoformat() if factsheet.launch_date else None,
            'exit_load': factsheet.exit_load,
            'last_updated': factsheet.last_updated.isoformat() if factsheet.last_updated else None
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
            return jsonify({'error': f'Returns for fund with ISIN {isin} not found'}), 404
        
        # Format response
        response = {
            'isin': returns.isin,
            'return_1m': returns.return_1m,
            'return_3m': returns.return_3m,
            'return_6m': returns.return_6m,
            'return_ytd': returns.return_ytd,
            'return_1y': returns.return_1y,
            'return_3y': returns.return_3y,
            'return_5y': returns.return_5y,
            'last_updated': returns.last_updated.isoformat() if returns.last_updated else None
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
        
        paginated_holdings = PortfolioHolding.query.filter_by(isin=isin).paginate(
            page=page, per_page=per_page, error_out=False
        )
        
        if paginated_holdings.total == 0:
            return jsonify({'holdings': [], 'pagination': {'total_items': 0, 'total_pages': 0, 'current_page': page, 'per_page': per_page}}), 200
        
        # Format response
        holdings = []
        for holding in paginated_holdings.items:
            holdings.append({
                'id': holding.id,
                'instrument_name': holding.instrument_name,
                'instrument_isin': holding.instrument_isin,
                'instrument_type': holding.instrument_type,
                'sector': holding.sector,
                'percentage_to_nav': holding.percentage_to_nav,
                'quantity': holding.quantity,
                'value': holding.value,
                'coupon': holding.coupon,
                'yield_value': holding.yield_value,
                'last_updated': holding.last_updated.isoformat() if holding.last_updated else None
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
        paginated_nav = query.paginate(page=page, per_page=per_page, error_out=False)
        
        if paginated_nav.total == 0:
            return jsonify({'nav_history': [], 'pagination': {'total_items': 0, 'total_pages': 0, 'current_page': page, 'per_page': per_page}}), 200
        
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
                'fund_manager': factsheet.fund_manager,
                'aum': factsheet.aum,
                'expense_ratio': factsheet.expense_ratio,
                'launch_date': factsheet.launch_date.isoformat() if factsheet.launch_date else None,
                'exit_load': factsheet.exit_load,
                'last_updated': factsheet.last_updated.isoformat() if factsheet.last_updated else None
            }
        
        # Get returns
        returns = FundReturns.query.filter_by(isin=isin).first()
        returns_data = None
        if returns:
            returns_data = {
                'return_1m': returns.return_1m,
                'return_3m': returns.return_3m,
                'return_6m': returns.return_6m,
                'return_ytd': returns.return_ytd,
                'return_1y': returns.return_1y,
                'return_3y': returns.return_3y,
                'return_5y': returns.return_5y,
                'last_updated': returns.last_updated.isoformat() if returns.last_updated else None
            }
        
        # Get most recent NAV
        most_recent_nav = NavHistory.query.filter_by(isin=isin).order_by(NavHistory.date.desc()).first()
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
            'created_at': fund.created_at.isoformat() if fund.created_at else None,
            'updated_at': fund.updated_at.isoformat() if fund.updated_at else None,
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
                'fund_manager': factsheet.fund_manager,
                'aum': factsheet.aum,
                'expense_ratio': factsheet.expense_ratio,
                'launch_date': factsheet.launch_date.isoformat() if factsheet.launch_date else None,
                'exit_load': factsheet.exit_load,
                'last_updated': factsheet.last_updated.isoformat() if factsheet.last_updated else None
            }
        
        # Get returns
        returns = FundReturns.query.filter_by(isin=isin).first()
        returns_data = None
        if returns:
            returns_data = {
                'return_1m': returns.return_1m,
                'return_3m': returns.return_3m,
                'return_6m': returns.return_6m,
                'return_ytd': returns.return_ytd,
                'return_1y': returns.return_1y,
                'return_3y': returns.return_3y,
                'return_5y': returns.return_5y,
                'last_updated': returns.last_updated.isoformat() if returns.last_updated else None
            }
        
        # Get NAV history (last 30 days)
        nav_history = NavHistory.query.filter_by(isin=isin).order_by(NavHistory.date.desc()).limit(30).all()
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
        holdings = PortfolioHolding.query.filter_by(isin=isin).all()
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
        for sector, allocation in sorted(sector_map.items(), key=lambda x: x[1], reverse=True):
            top_sectors.append({
                'sector': sector,
                'allocation': allocation
            })
        
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
                'isin': fund.isin,
                'scheme_name': fund.scheme_name,
                'fund_type': fund.fund_type,
                'fund_subtype': fund.fund_subtype,
                'amc_name': fund.amc_name,
                'created_at': fund.created_at.isoformat() if fund.created_at else None,
                'updated_at': fund.updated_at.isoformat() if fund.updated_at else None
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

def init_fund_api(app):
    """Initialize fund API routes"""
    app.register_blueprint(fund_api)
    logger.info("Fund API routes registered")
    
    return app