from setup_db import db
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from new_models_updated import Fund, FundFactSheet, FundReturns, FundHolding, NavHistory
from datetime import datetime

class DatabaseError(Exception):
    """Base exception for database operations"""
    pass

class ResourceNotFoundError(DatabaseError):
    """Exception for when a resource is not found"""
    pass

class ValidationError(DatabaseError):
    """Exception for validation errors"""
    pass

class UniqueConstraintError(ValidationError):
    """Exception for unique constraint violations"""
    pass

class FundService:
    """Service for Fund-related operations"""
    
    @staticmethod
    def create_fund(isin, scheme_name, fund_type, amc_name, fund_subtype=None):
        """
        Create a new fund
        
        Args:
            isin (str): ISIN code
            scheme_name (str): Fund scheme name
            fund_type (str): Type of fund (Equity, Debt, Hybrid)
            amc_name (str): AMC/Fund house name
            fund_subtype (str, optional): Subtype of fund
            
        Returns:
            Fund: The created fund
            
        Raises:
            UniqueConstraintError: If ISIN already exists
        """
        try:
            fund = Fund(
                isin=isin,
                scheme_name=scheme_name,
                fund_type=fund_type,
                fund_subtype=fund_subtype,
                amc_name=amc_name
            )
            db.session.add(fund)
            db.session.commit()
            return fund
        except IntegrityError:
            db.session.rollback()
            raise UniqueConstraintError(f"Fund with ISIN {isin} already exists")
        except SQLAlchemyError as e:
            db.session.rollback()
            raise DatabaseError(f"Error creating fund: {str(e)}")
    
    @staticmethod
    def get_fund(isin):
        """
        Get a fund by ISIN
        
        Args:
            isin (str): ISIN code
            
        Returns:
            Fund: The requested fund
            
        Raises:
            ResourceNotFoundError: If fund does not exist
        """
        fund = Fund.query.filter_by(isin=isin).first()
        if not fund:
            raise ResourceNotFoundError(f"Fund with ISIN {isin} not found")
        return fund
    
    @staticmethod
    def get_all_funds():
        """
        Get all funds
        
        Returns:
            list: List of all funds
        """
        return Fund.query.all()
    
    @staticmethod
    def get_funds_by_amc(amc_name):
        """
        Get all funds for an AMC
        
        Args:
            amc_name (str): AMC name
            
        Returns:
            list: List of funds
        """
        return Fund.query.filter_by(amc_name=amc_name).all()
    
    @staticmethod
    def get_funds_by_type(fund_type):
        """
        Get all funds of a specific type
        
        Args:
            fund_type (str): Fund type (Equity, Debt, Hybrid)
            
        Returns:
            list: List of funds
        """
        return Fund.query.filter_by(fund_type=fund_type).all()

    @staticmethod
    def delete_fund(isin):
        """
        Delete a fund
        
        Args:
            isin (str): ISIN code
            
        Raises:
            ResourceNotFoundError: If fund does not exist
        """
        fund = FundService.get_fund(isin)
        db.session.delete(fund)
        db.session.commit()

class FactSheetService:
    """Service for Fund Factsheet operations"""
    
    @staticmethod
    def create_factsheet(isin, fund_manager=None, aum=None, expense_ratio=None, 
                        launch_date=None, exit_load=None):
        """
        Create a factsheet for a fund
        
        Args:
            isin (str): ISIN code
            fund_manager (str, optional): Fund manager name
            aum (float, optional): Assets Under Management in Crores
            expense_ratio (float, optional): Expense Ratio percentage
            launch_date (datetime.date, optional): Launch date
            exit_load (str, optional): Exit load details
            
        Returns:
            FundFactSheet: The created factsheet
            
        Raises:
            ResourceNotFoundError: If fund does not exist
            UniqueConstraintError: If factsheet already exists
        """
        # Ensure fund exists
        FundService.get_fund(isin)
        
        try:
            factsheet = FundFactSheet(
                isin=isin,
                fund_manager=fund_manager,
                aum=aum,
                expense_ratio=expense_ratio,
                launch_date=launch_date,
                exit_load=exit_load
            )
            db.session.add(factsheet)
            db.session.commit()
            return factsheet
        except IntegrityError:
            db.session.rollback()
            raise UniqueConstraintError(f"Factsheet for fund with ISIN {isin} already exists")
        except SQLAlchemyError as e:
            db.session.rollback()
            raise DatabaseError(f"Error creating factsheet: {str(e)}")
    
    @staticmethod
    def get_factsheet(isin):
        """
        Get a factsheet by fund ISIN
        
        Args:
            isin (str): ISIN code
            
        Returns:
            FundFactSheet: The requested factsheet
            
        Raises:
            ResourceNotFoundError: If factsheet does not exist
        """
        factsheet = FundFactSheet.query.filter_by(isin=isin).first()
        if not factsheet:
            raise ResourceNotFoundError(f"Factsheet for fund with ISIN {isin} not found")
        return factsheet
    
    @staticmethod
    def update_factsheet(isin, **kwargs):
        """
        Update a factsheet
        
        Args:
            isin (str): ISIN code
            **kwargs: Fields to update
            
        Returns:
            FundFactSheet: The updated factsheet
            
        Raises:
            ResourceNotFoundError: If factsheet does not exist
        """
        factsheet = FactSheetService.get_factsheet(isin)
        
        for key, value in kwargs.items():
            if hasattr(factsheet, key):
                setattr(factsheet, key, value)
        
        db.session.commit()
        return factsheet
    
    @staticmethod
    def delete_factsheet(isin):
        """
        Delete a factsheet
        
        Args:
            isin (str): ISIN code
            
        Raises:
            ResourceNotFoundError: If factsheet does not exist
        """
        factsheet = FactSheetService.get_factsheet(isin)
        db.session.delete(factsheet)
        db.session.commit()

class ReturnsService:
    """Service for Fund Returns operations"""
    
    @staticmethod
    def create_returns(isin, return_1m=None, return_3m=None, return_6m=None, 
                      return_ytd=None, return_1y=None, return_3y=None, return_5y=None):
        """
        Create returns data for a fund
        
        Args:
            isin (str): ISIN code
            return_1m (float, optional): 1-month return percentage
            return_3m (float, optional): 3-month return percentage
            return_6m (float, optional): 6-month return percentage
            return_ytd (float, optional): Year-to-date return percentage
            return_1y (float, optional): 1-year return percentage
            return_3y (float, optional): 3-year return percentage
            return_5y (float, optional): 5-year return percentage
            
        Returns:
            FundReturns: The created returns data
            
        Raises:
            ResourceNotFoundError: If fund does not exist
            UniqueConstraintError: If returns data already exists
        """
        # Ensure fund exists
        FundService.get_fund(isin)
        
        try:
            returns = FundReturns(
                isin=isin,
                return_1m=return_1m,
                return_3m=return_3m,
                return_6m=return_6m,
                return_ytd=return_ytd,
                return_1y=return_1y,
                return_3y=return_3y,
                return_5y=return_5y
            )
            db.session.add(returns)
            db.session.commit()
            return returns
        except IntegrityError:
            db.session.rollback()
            raise UniqueConstraintError(f"Returns data for fund with ISIN {isin} already exists")
        except SQLAlchemyError as e:
            db.session.rollback()
            raise DatabaseError(f"Error creating returns data: {str(e)}")
    
    @staticmethod
    def get_returns(isin):
        """
        Get returns data by fund ISIN
        
        Args:
            isin (str): ISIN code
            
        Returns:
            FundReturns: The requested returns data
            
        Raises:
            ResourceNotFoundError: If returns data does not exist
        """
        returns = FundReturns.query.filter_by(isin=isin).first()
        if not returns:
            raise ResourceNotFoundError(f"Returns data for fund with ISIN {isin} not found")
        return returns
    
    @staticmethod
    def update_returns(isin, **kwargs):
        """
        Update returns data
        
        Args:
            isin (str): ISIN code
            **kwargs: Fields to update
            
        Returns:
            FundReturns: The updated returns data
            
        Raises:
            ResourceNotFoundError: If returns data does not exist
        """
        returns = ReturnsService.get_returns(isin)
        
        for key, value in kwargs.items():
            if hasattr(returns, key):
                setattr(returns, key, value)
        
        db.session.commit()
        return returns
    
    @staticmethod
    def delete_returns(isin):
        """
        Delete returns data
        
        Args:
            isin (str): ISIN code
            
        Raises:
            ResourceNotFoundError: If returns data does not exist
        """
        returns = ReturnsService.get_returns(isin)
        db.session.delete(returns)
        db.session.commit()

class PortfolioService:
    """Service for Portfolio Holdings operations"""
    
    @staticmethod
    def create_holding(isin, instrument_name, percentage_to_nav, instrument_type, 
                      instrument_isin=None, coupon=None, sector=None, quantity=None, 
                      value=None, yield_value=None):
        """
        Create a portfolio holding for a fund
        
        Args:
            isin (str): Fund ISIN code
            instrument_name (str): Name of the instrument
            percentage_to_nav (float): Percentage allocation to NAV
            instrument_type (str): Type of instrument (Equity, Debt, etc)
            instrument_isin (str, optional): ISIN of the instrument
            coupon (float, optional): Coupon percentage for debt instruments
            sector (str, optional): Sector of the instrument
            quantity (float, optional): Quantity held
            value (float, optional): Value in INR
            yield_value (float, optional): Yield percentage
            
        Returns:
            FundHolding: The created holding
            
        Raises:
            ResourceNotFoundError: If fund does not exist
            ValidationError: If required fields are missing
        """
        # Ensure fund exists
        FundService.get_fund(isin)
        
        try:
            holding = PortfolioHolding(
                isin=isin,
                instrument_isin=instrument_isin,
                coupon=coupon,
                instrument_name=instrument_name,
                sector=sector,
                quantity=quantity,
                value=value,
                percentage_to_nav=percentage_to_nav,
                yield_value=yield_value,
                instrument_type=instrument_type
            )
            db.session.add(holding)
            db.session.commit()
            return holding
        except SQLAlchemyError as e:
            db.session.rollback()
            raise DatabaseError(f"Error creating portfolio holding: {str(e)}")
    
    @staticmethod
    def get_holding(holding_id):
        """
        Get a portfolio holding by ID
        
        Args:
            holding_id (int): Holding ID
            
        Returns:
            PortfolioHolding: The requested holding
            
        Raises:
            ResourceNotFoundError: If holding does not exist
        """
        holding = PortfolioHolding.query.get(holding_id)
        if not holding:
            raise ResourceNotFoundError(f"Portfolio holding with ID {holding_id} not found")
        return holding
    
    @staticmethod
    def get_holdings_by_fund(isin):
        """
        Get all holdings for a fund
        
        Args:
            isin (str): ISIN code
            
        Returns:
            list: List of holdings
            
        Raises:
            ResourceNotFoundError: If fund does not exist
        """
        # Ensure fund exists
        FundService.get_fund(isin)
        
        return PortfolioHolding.query.filter_by(isin=isin).all()
    
    @staticmethod
    def update_holding(holding_id, **kwargs):
        """
        Update a portfolio holding
        
        Args:
            holding_id (int): Holding ID
            **kwargs: Fields to update
            
        Returns:
            PortfolioHolding: The updated holding
            
        Raises:
            ResourceNotFoundError: If holding does not exist
        """
        holding = PortfolioService.get_holding(holding_id)
        
        for key, value in kwargs.items():
            if hasattr(holding, key):
                setattr(holding, key, value)
        
        db.session.commit()
        return holding
    
    @staticmethod
    def delete_holding(holding_id):
        """
        Delete a portfolio holding
        
        Args:
            holding_id (int): Holding ID
            
        Raises:
            ResourceNotFoundError: If holding does not exist
        """
        holding = PortfolioService.get_holding(holding_id)
        db.session.delete(holding)
        db.session.commit()
    
    @staticmethod
    def delete_all_holdings(isin):
        """
        Delete all holdings for a fund
        
        Args:
            isin (str): ISIN code
            
        Raises:
            ResourceNotFoundError: If fund does not exist
        """
        # Ensure fund exists
        FundService.get_fund(isin)
        
        PortfolioHolding.query.filter_by(isin=isin).delete()
        db.session.commit()

class NavHistoryService:
    """Service for NAV History operations"""
    
    @staticmethod
    def create_nav(isin, date, nav):
        """
        Create a NAV history entry for a fund
        
        Args:
            isin (str): ISIN code
            date (datetime.date): Date of NAV
            nav (float): NAV value
            
        Returns:
            NavHistory: The created NAV history entry
            
        Raises:
            ResourceNotFoundError: If fund does not exist
            UniqueConstraintError: If NAV entry already exists for the date
        """
        # Ensure fund exists
        FundService.get_fund(isin)
        
        # Check for existing NAV entry for the date
        existing_nav = NavHistory.query.filter_by(isin=isin, date=date).first()
        if existing_nav:
            raise UniqueConstraintError(f"NAV entry for fund with ISIN {isin} on date {date} already exists")
        
        try:
            nav_entry = NavHistory(
                isin=isin,
                date=date,
                nav=nav
            )
            db.session.add(nav_entry)
            db.session.commit()
            return nav_entry
        except SQLAlchemyError as e:
            db.session.rollback()
            raise DatabaseError(f"Error creating NAV history entry: {str(e)}")
    
    @staticmethod
    def get_nav(nav_id):
        """
        Get a NAV history entry by ID
        
        Args:
            nav_id (int): NAV entry ID
            
        Returns:
            NavHistory: The requested NAV history entry
            
        Raises:
            ResourceNotFoundError: If NAV entry does not exist
        """
        nav_entry = NavHistory.query.get(nav_id)
        if not nav_entry:
            raise ResourceNotFoundError(f"NAV history entry with ID {nav_id} not found")
        return nav_entry
    
    @staticmethod
    def get_nav_by_date(isin, date):
        """
        Get a NAV history entry by fund ISIN and date
        
        Args:
            isin (str): ISIN code
            date (datetime.date): Date of NAV
            
        Returns:
            NavHistory: The requested NAV history entry
            
        Raises:
            ResourceNotFoundError: If NAV entry does not exist
        """
        nav_entry = NavHistory.query.filter_by(isin=isin, date=date).first()
        if not nav_entry:
            raise ResourceNotFoundError(f"NAV history entry for fund with ISIN {isin} on date {date} not found")
        return nav_entry
    
    @staticmethod
    def get_nav_history(isin, start_date=None, end_date=None):
        """
        Get NAV history for a fund within a date range
        
        Args:
            isin (str): ISIN code
            start_date (datetime.date, optional): Start date of range
            end_date (datetime.date, optional): End date of range
            
        Returns:
            list: List of NAV history entries
            
        Raises:
            ResourceNotFoundError: If fund does not exist
        """
        # Ensure fund exists
        FundService.get_fund(isin)
        
        query = NavHistory.query.filter_by(isin=isin)
        
        if start_date:
            query = query.filter(NavHistory.date >= start_date)
        
        if end_date:
            query = query.filter(NavHistory.date <= end_date)
        
        return query.order_by(NavHistory.date).all()
    
    @staticmethod
    def update_nav(nav_id, nav):
        """
        Update a NAV history entry
        
        Args:
            nav_id (int): NAV entry ID
            nav (float): New NAV value
            
        Returns:
            NavHistory: The updated NAV history entry
            
        Raises:
            ResourceNotFoundError: If NAV entry does not exist
        """
        nav_entry = NavHistoryService.get_nav(nav_id)
        nav_entry.nav = nav
        db.session.commit()
        return nav_entry
    
    @staticmethod
    def delete_nav(nav_id):
        """
        Delete a NAV history entry
        
        Args:
            nav_id (int): NAV entry ID
            
        Raises:
            ResourceNotFoundError: If NAV entry does not exist
        """
        nav_entry = NavHistoryService.get_nav(nav_id)
        db.session.delete(nav_entry)
        db.session.commit()
    
    @staticmethod
    def delete_nav_history(isin):
        """
        Delete all NAV history for a fund
        
        Args:
            isin (str): ISIN code
            
        Raises:
            ResourceNotFoundError: If fund does not exist
        """
        # Ensure fund exists
        FundService.get_fund(isin)
        
        NavHistory.query.filter_by(isin=isin).delete()
        db.session.commit()