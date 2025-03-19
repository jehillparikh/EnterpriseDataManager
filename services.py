import logging
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime
from models import (
    db, UserInfo, KycDetail, BankRepo, BranchRepo, BankDetail, Mandate,
    Amc, Fund, FundScheme, FundSchemeDetail, MutualFund, UserPortfolio, MFHoldings
)

logger = logging.getLogger(__name__)

# Custom Exceptions
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

class AuthenticationError(ValidationError):
    """Exception for authentication failures"""
    pass

# User Services
class UserService:
    """Service for User-related operations"""
    
    @staticmethod
    def register_user(email, mobile_number, password):
        """
        Register a new user
        
        Args:
            email (str): Email address
            mobile_number (str): Mobile number
            password (str): Plaintext password to be hashed
            
        Returns:
            UserInfo: The created user
            
        Raises:
            UniqueConstraintError: If email or mobile number already exists
        """
        # Check if email or mobile number already exists
        existing_email = UserInfo.query.filter_by(email=email).first()
        if existing_email:
            raise UniqueConstraintError("Email already registered")
            
        existing_mobile = UserInfo.query.filter_by(mobile_number=mobile_number).first()
        if existing_mobile:
            raise UniqueConstraintError("Mobile number already registered")
        
        # Hash the password
        password_hash = generate_password_hash(password)
        
        # Create and store the user
        new_user = UserInfo(
            email=email,
            mobile_number=mobile_number,
            password_hash=password_hash
        )
        
        try:
            db.session.add(new_user)
            db.session.commit()
            logger.info(f"User created: {new_user.id}")
            return new_user
        except Exception as e:
            db.session.rollback()
            logger.error(f"Error creating user: {str(e)}")
            raise DatabaseError(f"Error creating user: {str(e)}")
    
    @staticmethod
    def authenticate_user(email, password):
        """
        Authenticate a user with email and password
        
        Args:
            email (str): User email
            password (str): Plaintext password
            
        Returns:
            UserInfo: The authenticated user
            
        Raises:
            AuthenticationError: If authentication fails
        """
        user = UserInfo.query.filter_by(email=email).first()
        
        if not user or not check_password_hash(user.password_hash, password):
            raise AuthenticationError("Invalid email or password")
        
        return user
    
    @staticmethod
    def get_user(user_id):
        """
        Get a user by ID
        
        Args:
            user_id (int): User ID
            
        Returns:
            UserInfo: The requested user
            
        Raises:
            ResourceNotFoundError: If user does not exist
        """
        user = UserInfo.query.get(user_id)
        if not user:
            raise ResourceNotFoundError(f"User with ID {user_id} not found")
        return user
    
    @staticmethod
    def get_all_users():
        """
        Get all users
        
        Returns:
            list: List of all users
        """
        return UserInfo.query.all()
    
    @staticmethod
    def update_user(user_id, email=None, mobile_number=None, password=None):
        """
        Update a user
        
        Args:
            user_id (int): User ID
            email (str, optional): New email
            mobile_number (str, optional): New mobile number
            password (str, optional): New password
            
        Returns:
            UserInfo: The updated user
            
        Raises:
            ResourceNotFoundError: If user does not exist
            UniqueConstraintError: If new email or mobile number already exists
        """
        user = UserService.get_user(user_id)
        
        # Check email uniqueness if changing
        if email and email != user.email:
            existing_email = UserInfo.query.filter_by(email=email).first()
            if existing_email:
                raise UniqueConstraintError("Email already registered")
            user.email = email
            
        # Check mobile number uniqueness if changing
        if mobile_number and mobile_number != user.mobile_number:
            existing_mobile = UserInfo.query.filter_by(mobile_number=mobile_number).first()
            if existing_mobile:
                raise UniqueConstraintError("Mobile number already registered")
            user.mobile_number = mobile_number
            
        # Update password if provided
        if password:
            user.password_hash = generate_password_hash(password)
            
        user.updated_at = datetime.utcnow()
            
        try:
            db.session.commit()
            logger.info(f"User updated: {user.id}")
            return user
        except Exception as e:
            db.session.rollback()
            logger.error(f"Error updating user: {str(e)}")
            raise DatabaseError(f"Error updating user: {str(e)}")
    
    @staticmethod
    def delete_user(user_id):
        """
        Delete a user
        
        Args:
            user_id (int): User ID
            
        Raises:
            ResourceNotFoundError: If user does not exist
        """
        user = UserService.get_user(user_id)
        
        try:
            db.session.delete(user)
            db.session.commit()
            logger.info(f"User deleted: {user_id}")
        except Exception as e:
            db.session.rollback()
            logger.error(f"Error deleting user: {str(e)}")
            raise DatabaseError(f"Error deleting user: {str(e)}")

# KYC Services
class KycService:
    """Service for KYC-related operations"""
    
    @staticmethod
    def create_kyc(user_id, pan, tax_status, occ_code, first_name, middle_name, last_name, 
                  dob, gender, address, city, state, pincode, phone, income_slab):
        """
        Create KYC details for a user
        
        Args:
            user_id (int): User ID
            pan (str): PAN number
            tax_status (str): Tax status code
            occ_code (str): Occupation code
            first_name (str): First name
            middle_name (str, optional): Middle name
            last_name (str): Last name
            dob (str): Date of birth
            gender (str): Gender code
            address (str): Address
            city (str): City
            state (str): State code
            pincode (str): Pincode
            phone (str, optional): Phone number
            income_slab (int): Income slab code
            
        Returns:
            KycDetail: The created KYC details
            
        Raises:
            ResourceNotFoundError: If user does not exist
            UniqueConstraintError: If KYC details already exist for the user
        """
        # Verify user exists
        user = UserService.get_user(user_id)
        
        # Check if KYC details already exist
        existing_kyc = KycDetail.query.filter_by(user_id=user_id).first()
        if existing_kyc:
            raise UniqueConstraintError(f"KYC details already exist for user {user_id}")
            
        # Create KYC details
        kyc = KycDetail(
            user_id=user_id,
            pan=pan,
            tax_status=tax_status,
            occ_code=occ_code,
            first_name=first_name,
            middle_name=middle_name,
            last_name=last_name,
            dob=dob,
            gender=gender,
            address=address,
            city=city,
            state=state,
            pincode=pincode,
            phone=phone,
            income_slab=income_slab
        )
        
        try:
            db.session.add(kyc)
            db.session.commit()
            logger.info(f"KYC details created for user: {user_id}")
            return kyc
        except Exception as e:
            db.session.rollback()
            logger.error(f"Error creating KYC details: {str(e)}")
            raise DatabaseError(f"Error creating KYC details: {str(e)}")
    
    @staticmethod
    def get_kyc(user_id):
        """
        Get KYC details for a user
        
        Args:
            user_id (int): User ID
            
        Returns:
            KycDetail: The requested KYC details
            
        Raises:
            ResourceNotFoundError: If KYC details do not exist
        """
        kyc = KycDetail.query.filter_by(user_id=user_id).first()
        if not kyc:
            raise ResourceNotFoundError(f"KYC details not found for user {user_id}")
        return kyc
    
    @staticmethod
    def update_kyc(user_id, **kwargs):
        """
        Update KYC details
        
        Args:
            user_id (int): User ID
            **kwargs: KYC fields to update
            
        Returns:
            KycDetail: The updated KYC details
            
        Raises:
            ResourceNotFoundError: If KYC details do not exist
        """
        kyc = KycService.get_kyc(user_id)
        
        # Update fields
        for key, value in kwargs.items():
            if hasattr(kyc, key) and value is not None:
                setattr(kyc, key, value)
        
        try:
            db.session.commit()
            logger.info(f"KYC details updated for user: {user_id}")
            return kyc
        except Exception as e:
            db.session.rollback()
            logger.error(f"Error updating KYC details: {str(e)}")
            raise DatabaseError(f"Error updating KYC details: {str(e)}")
            
    @staticmethod
    def verify_kyc_hyperverge(user_id, pan_image_path, selfie_image_path=None, video_path=None):
        """
        Verify KYC using Hyperverge API
        
        Args:
            user_id (int): User ID
            pan_image_path (str): Path to PAN card image
            selfie_image_path (str, optional): Path to selfie image
            video_path (str, optional): Path to liveness verification video
            
        Returns:
            dict: Verification result
            
        Raises:
            ResourceNotFoundError: If user does not exist
        """
        from hyperverge_service import HypervergeService
        
        # Verify user exists
        user = UserService.get_user(user_id)
        
        # Verify PAN card
        pan_result = HypervergeService.verify_id_card(pan_image_path, id_type="PAN")
        
        # Initialize results
        verification_results = {
            "pan_verification": pan_result,
            "face_match": None,
            "liveness": None,
            "success": pan_result.get("success", False)
        }
        
        # If PAN verification succeeded and selfie is provided, verify face match
        if pan_result.get("success", False) and selfie_image_path:
            face_match_result = HypervergeService.verify_face_match(selfie_image_path, pan_image_path)
            verification_results["face_match"] = face_match_result
            verification_results["success"] = verification_results["success"] and face_match_result.get("success", False)
        
        # If video is provided, verify liveness
        if video_path:
            liveness_result = HypervergeService.verify_liveness(video_path)
            verification_results["liveness"] = liveness_result
            verification_results["success"] = verification_results["success"] and liveness_result.get("success", False)
        
        # Extract PAN details if verification successful
        if verification_results["success"] and "result" in pan_result:
            try:
                # Extract details from PAN card
                pan_details = pan_result["result"]
                
                # Check if we already have KYC for this user
                existing_kyc = KycDetail.query.filter_by(user_id=user_id).first()
                
                if existing_kyc:
                    # Update existing KYC with PAN details
                    kyc_update = {}
                    
                    if "pan" in pan_details:
                        kyc_update["pan"] = pan_details["pan"]
                    if "name" in pan_details:
                        # Split name into parts (simplified)
                        name_parts = pan_details["name"].split(" ")
                        if len(name_parts) > 0:
                            kyc_update["first_name"] = name_parts[0]
                        if len(name_parts) > 1:
                            kyc_update["last_name"] = name_parts[-1]
                        if len(name_parts) > 2:
                            kyc_update["middle_name"] = " ".join(name_parts[1:-1])
                    if "dob" in pan_details:
                        kyc_update["dob"] = pan_details["dob"]
                    
                    KycService.update_kyc(user_id, **kyc_update)
                    verification_results["kyc_updated"] = True
                else:
                    # Skip creating a new KYC record here as we need more details
                    verification_results["kyc_created"] = False
                    verification_results["message"] = "PAN verification successful, but full KYC details needed"
            except Exception as e:
                logger.error(f"Error processing PAN details: {str(e)}")
                verification_results["processing_error"] = str(e)
        
        return verification_results
    
    @staticmethod
    def register_with_bse_star(user_id):
        """
        Register a user with BSE Star
        
        Args:
            user_id (int): User ID
            
        Returns:
            dict: Registration result
            
        Raises:
            ResourceNotFoundError: If user or KYC details do not exist
        """
        from bse_star_service import BseStarService
        
        # Verify user exists
        user = UserService.get_user(user_id)
        
        # Get KYC details
        kyc = KycService.get_kyc(user_id)
        
        # Prepare full name
        full_name_parts = [kyc.first_name]
        if kyc.middle_name:
            full_name_parts.append(kyc.middle_name)
        full_name_parts.append(kyc.last_name)
        full_name = " ".join(full_name_parts)
        
        # Register client with BSE Star
        try:
            result = BseStarService.register_client(
                pan=kyc.pan,
                full_name=full_name,
                dob=kyc.dob,
                mobile=user.mobile_number,
                email=user.email,
                address=kyc.address,
                city=kyc.city,
                state=kyc.state,
                pincode=kyc.pincode
            )
            
            return result
        except Exception as e:
            logger.error(f"Error registering with BSE Star: {str(e)}")
            return {"success": False, "error": str(e)}

# Bank Services
class BankService:
    """Service for bank-related operations"""
    
    @staticmethod
    def create_bank(name):
        """
        Create a new bank
        
        Args:
            name (str): Bank name
            
        Returns:
            BankRepo: The created bank
            
        Raises:
            UniqueConstraintError: If bank name already exists
        """
        existing_bank = BankRepo.query.filter_by(name=name).first()
        if existing_bank:
            raise UniqueConstraintError(f"Bank with name '{name}' already exists")
            
        bank = BankRepo(name=name)
        
        try:
            db.session.add(bank)
            db.session.commit()
            logger.info(f"Bank created: {bank.id}")
            return bank
        except Exception as e:
            db.session.rollback()
            logger.error(f"Error creating bank: {str(e)}")
            raise DatabaseError(f"Error creating bank: {str(e)}")
    
    @staticmethod
    def get_bank(bank_id):
        """
        Get a bank by ID
        
        Args:
            bank_id (int): Bank ID
            
        Returns:
            BankRepo: The requested bank
            
        Raises:
            ResourceNotFoundError: If bank does not exist
        """
        bank = BankRepo.query.get(bank_id)
        if not bank:
            raise ResourceNotFoundError(f"Bank with ID {bank_id} not found")
        return bank
    
    @staticmethod
    def get_all_banks():
        """
        Get all banks
        
        Returns:
            list: List of all banks
        """
        return BankRepo.query.all()
    
    @staticmethod
    def create_branch(bank_id, branch_name, branch_city, branch_address, ifsc_code, micr_code=None):
        """
        Create a new branch
        
        Args:
            bank_id (int): Bank ID
            branch_name (str): Branch name
            branch_city (str): Branch city
            branch_address (str): Branch address
            ifsc_code (str): IFSC code
            micr_code (str, optional): MICR code
            
        Returns:
            BranchRepo: The created branch
            
        Raises:
            ResourceNotFoundError: If bank does not exist
            UniqueConstraintError: If IFSC code already exists
        """
        # Verify bank exists
        bank = BankService.get_bank(bank_id)
        
        # Check if IFSC code already exists
        existing_branch = BranchRepo.query.filter_by(ifsc_code=ifsc_code).first()
        if existing_branch:
            raise UniqueConstraintError(f"Branch with IFSC code '{ifsc_code}' already exists")
            
        branch = BranchRepo(
            bank_id=bank_id,
            branch_name=branch_name,
            branch_city=branch_city,
            branch_address=branch_address,
            ifsc_code=ifsc_code,
            micr_code=micr_code
        )
        
        try:
            db.session.add(branch)
            db.session.commit()
            logger.info(f"Branch created: {branch.id}")
            return branch
        except Exception as e:
            db.session.rollback()
            logger.error(f"Error creating branch: {str(e)}")
            raise DatabaseError(f"Error creating branch: {str(e)}")
    
    @staticmethod
    def get_branch(branch_id):
        """
        Get a branch by ID
        
        Args:
            branch_id (int): Branch ID
            
        Returns:
            BranchRepo: The requested branch
            
        Raises:
            ResourceNotFoundError: If branch does not exist
        """
        branch = BranchRepo.query.get(branch_id)
        if not branch:
            raise ResourceNotFoundError(f"Branch with ID {branch_id} not found")
        return branch
    
    @staticmethod
    def get_branches_by_bank(bank_id):
        """
        Get all branches for a bank
        
        Args:
            bank_id (int): Bank ID
            
        Returns:
            list: List of branches
            
        Raises:
            ResourceNotFoundError: If bank does not exist
        """
        # Verify bank exists
        bank = BankService.get_bank(bank_id)
        
        return BranchRepo.query.filter_by(bank_id=bank_id).all()
    
    @staticmethod
    def create_bank_detail(user_id, branch_id, account_number, account_type_bse):
        """
        Create bank details for a user
        
        Args:
            user_id (int): User ID
            branch_id (int): Branch ID
            account_number (str): Account number
            account_type_bse (str): Account type code
            
        Returns:
            BankDetail: The created bank details
            
        Raises:
            ResourceNotFoundError: If user or branch does not exist
        """
        # Verify user exists
        user = UserService.get_user(user_id)
        
        # Verify branch exists
        branch = BankService.get_branch(branch_id)
        
        bank_detail = BankDetail(
            user_id=user_id,
            branch_id=branch_id,
            account_number=account_number,
            account_type_bse=account_type_bse
        )
        
        try:
            db.session.add(bank_detail)
            db.session.commit()
            logger.info(f"Bank details created for user: {user_id}")
            return bank_detail
        except Exception as e:
            db.session.rollback()
            logger.error(f"Error creating bank details: {str(e)}")
            raise DatabaseError(f"Error creating bank details: {str(e)}")
    
    @staticmethod
    def get_bank_details(user_id):
        """
        Get all bank details for a user
        
        Args:
            user_id (int): User ID
            
        Returns:
            list: List of bank details
            
        Raises:
            ResourceNotFoundError: If user does not exist
        """
        # Verify user exists
        user = UserService.get_user(user_id)
        
        return BankDetail.query.filter_by(user_id=user_id).all()
    
    @staticmethod
    def get_bank_detail(bank_detail_id):
        """
        Get bank detail by ID
        
        Args:
            bank_detail_id (int): Bank detail ID
            
        Returns:
            BankDetail: The requested bank detail
            
        Raises:
            ResourceNotFoundError: If bank detail does not exist
        """
        bank_detail = BankDetail.query.get(bank_detail_id)
        if not bank_detail:
            raise ResourceNotFoundError(f"Bank detail with ID {bank_detail_id} not found")
        return bank_detail

# Fund Services
class FundService:
    """Service for Fund-related operations"""
    
    @staticmethod
    def create_amc(name, short_name, fund_code=None, bse_code=None, active=True):
        """
        Create a new AMC
        
        Args:
            name (str): AMC name
            short_name (str): Short name
            fund_code (str, optional): Fund code
            bse_code (str, optional): BSE code
            active (bool, optional): Active status
            
        Returns:
            Amc: The created AMC
            
        Raises:
            UniqueConstraintError: If AMC name already exists
        """
        existing_amc = Amc.query.filter_by(name=name).first()
        if existing_amc:
            raise UniqueConstraintError(f"AMC with name '{name}' already exists")
            
        amc = Amc(
            name=name,
            short_name=short_name,
            fund_code=fund_code,
            bse_code=bse_code,
            active=active
        )
        
        try:
            db.session.add(amc)
            db.session.commit()
            logger.info(f"AMC created: {amc.id}")
            return amc
        except Exception as e:
            db.session.rollback()
            logger.error(f"Error creating AMC: {str(e)}")
            raise DatabaseError(f"Error creating AMC: {str(e)}")
    
    @staticmethod
    def get_amc(amc_id):
        """
        Get an AMC by ID
        
        Args:
            amc_id (int): AMC ID
            
        Returns:
            Amc: The requested AMC
            
        Raises:
            ResourceNotFoundError: If AMC does not exist
        """
        amc = Amc.query.get(amc_id)
        if not amc:
            raise ResourceNotFoundError(f"AMC with ID {amc_id} not found")
        return amc
    
    @staticmethod
    def get_all_amcs():
        """
        Get all AMCs
        
        Returns:
            list: List of all AMCs
        """
        return Amc.query.all()
    
    @staticmethod
    def create_fund(name, amc_id, short_name=None, rta_code=None, bse_code=None, active=True, direct=False):
        """
        Create a new fund
        
        Args:
            name (str): Fund name
            amc_id (int): AMC ID
            short_name (str, optional): Short name
            rta_code (str, optional): RTA code
            bse_code (str, optional): BSE code
            active (bool, optional): Active status
            direct (bool, optional): Direct plan indicator
            
        Returns:
            Fund: The created fund
            
        Raises:
            ResourceNotFoundError: If AMC does not exist
        """
        # Verify AMC exists
        amc = FundService.get_amc(amc_id)
        
        fund = Fund(
            name=name,
            amc_id=amc_id,
            short_name=short_name,
            rta_code=rta_code,
            bse_code=bse_code,
            active=active,
            direct=direct
        )
        
        try:
            db.session.add(fund)
            db.session.commit()
            logger.info(f"Fund created: {fund.id}")
            return fund
        except Exception as e:
            db.session.rollback()
            logger.error(f"Error creating fund: {str(e)}")
            raise DatabaseError(f"Error creating fund: {str(e)}")
    
    @staticmethod
    def get_fund(fund_id):
        """
        Get a fund by ID
        
        Args:
            fund_id (int): Fund ID
            
        Returns:
            Fund: The requested fund
            
        Raises:
            ResourceNotFoundError: If fund does not exist
        """
        fund = Fund.query.get(fund_id)
        if not fund:
            raise ResourceNotFoundError(f"Fund with ID {fund_id} not found")
        return fund
    
    @staticmethod
    def get_funds_by_amc(amc_id):
        """
        Get all funds for an AMC
        
        Args:
            amc_id (int): AMC ID
            
        Returns:
            list: List of funds
            
        Raises:
            ResourceNotFoundError: If AMC does not exist
        """
        # Verify AMC exists
        amc = FundService.get_amc(amc_id)
        
        return Fund.query.filter_by(amc_id=amc_id).all()
    
    @staticmethod
    def create_fund_scheme(fund_id, scheme_code, plan, option=None, bse_code=None):
        """
        Create a new fund scheme
        
        Args:
            fund_id (int): Fund ID
            scheme_code (str): Scheme code
            plan (str): Plan code
            option (str, optional): Option code
            bse_code (str, optional): BSE code
            
        Returns:
            FundScheme: The created fund scheme
            
        Raises:
            ResourceNotFoundError: If fund does not exist
        """
        # Verify fund exists
        fund = FundService.get_fund(fund_id)
        
        scheme = FundScheme(
            fund_id=fund_id,
            scheme_code=scheme_code,
            plan=plan,
            option=option,
            bse_code=bse_code
        )
        
        try:
            db.session.add(scheme)
            db.session.commit()
            logger.info(f"Fund scheme created: {scheme.id}")
            return scheme
        except Exception as e:
            db.session.rollback()
            logger.error(f"Error creating fund scheme: {str(e)}")
            raise DatabaseError(f"Error creating fund scheme: {str(e)}")
    
    @staticmethod
    def get_fund_scheme(scheme_id):
        """
        Get a fund scheme by ID
        
        Args:
            scheme_id (int): Scheme ID
            
        Returns:
            FundScheme: The requested fund scheme
            
        Raises:
            ResourceNotFoundError: If fund scheme does not exist
        """
        scheme = FundScheme.query.get(scheme_id)
        if not scheme:
            raise ResourceNotFoundError(f"Fund scheme with ID {scheme_id} not found")
        return scheme
    
    @staticmethod
    def get_schemes_by_fund(fund_id):
        """
        Get all schemes for a fund
        
        Args:
            fund_id (int): Fund ID
            
        Returns:
            list: List of fund schemes
            
        Raises:
            ResourceNotFoundError: If fund does not exist
        """
        # Verify fund exists
        fund = FundService.get_fund(fund_id)
        
        return FundScheme.query.filter_by(fund_id=fund_id).all()
    
    @staticmethod
    def create_fund_scheme_detail(scheme_id, nav, expense_ratio=None, fund_manager=None, aum=None, risk_level=None, benchmark=None):
        """
        Create details for a fund scheme
        
        Args:
            scheme_id (int): Scheme ID
            nav (float): NAV
            expense_ratio (float, optional): Expense ratio
            fund_manager (str, optional): Fund manager
            aum (float, optional): AUM
            risk_level (str, optional): Risk level
            benchmark (str, optional): Benchmark
            
        Returns:
            FundSchemeDetail: The created fund scheme detail
            
        Raises:
            ResourceNotFoundError: If fund scheme does not exist
            UniqueConstraintError: If details already exist for the scheme
        """
        # Verify scheme exists
        scheme = FundService.get_fund_scheme(scheme_id)
        
        # Check if details already exist
        existing_detail = FundSchemeDetail.query.filter_by(scheme_id=scheme_id).first()
        if existing_detail:
            raise UniqueConstraintError(f"Details already exist for scheme {scheme_id}")
            
        detail = FundSchemeDetail(
            scheme_id=scheme_id,
            nav=nav,
            expense_ratio=expense_ratio,
            fund_manager=fund_manager,
            aum=aum,
            risk_level=risk_level,
            benchmark=benchmark
        )
        
        try:
            db.session.add(detail)
            db.session.commit()
            logger.info(f"Fund scheme detail created for scheme: {scheme_id}")
            return detail
        except Exception as e:
            db.session.rollback()
            logger.error(f"Error creating fund scheme detail: {str(e)}")
            raise DatabaseError(f"Error creating fund scheme detail: {str(e)}")
    
    @staticmethod
    def get_fund_scheme_detail(scheme_id):
        """
        Get details for a fund scheme
        
        Args:
            scheme_id (int): Scheme ID
            
        Returns:
            FundSchemeDetail: The requested fund scheme detail
            
        Raises:
            ResourceNotFoundError: If fund scheme detail does not exist
        """
        detail = FundSchemeDetail.query.filter_by(scheme_id=scheme_id).first()
        if not detail:
            raise ResourceNotFoundError(f"Details for scheme {scheme_id} not found")
        return detail

# Portfolio Services
class PortfolioService:
    """Service for Portfolio-related operations"""
    
    @staticmethod
    def create_portfolio(user_id, scheme_id, scheme_code, units, purchase_nav, invested_amount, date_invested, current_nav=None, current_value=None):
        """
        Create a portfolio entry for a user
        
        Args:
            user_id (int): User ID
            scheme_id (int): Scheme ID
            scheme_code (str): Scheme code
            units (float): Number of units
            purchase_nav (float): Purchase NAV
            invested_amount (float): Invested amount
            date_invested (date): Date invested
            current_nav (float, optional): Current NAV
            current_value (float, optional): Current value
            
        Returns:
            UserPortfolio: The created portfolio entry
            
        Raises:
            ResourceNotFoundError: If user or fund scheme does not exist
        """
        # Verify user exists
        user = UserService.get_user(user_id)
        
        # Verify scheme exists
        scheme = FundService.get_fund_scheme(scheme_id)
        
        portfolio = UserPortfolio(
            user_id=user_id,
            scheme_id=scheme_id,
            scheme_code=scheme_code,
            units=units,
            purchase_nav=purchase_nav,
            current_nav=current_nav,
            invested_amount=invested_amount,
            current_value=current_value,
            date_invested=date_invested
        )
        
        try:
            db.session.add(portfolio)
            db.session.commit()
            logger.info(f"Portfolio entry created for user: {user_id}")
            return portfolio
        except Exception as e:
            db.session.rollback()
            logger.error(f"Error creating portfolio entry: {str(e)}")
            raise DatabaseError(f"Error creating portfolio entry: {str(e)}")
    
    @staticmethod
    def get_portfolio(portfolio_id):
        """
        Get a portfolio entry by ID
        
        Args:
            portfolio_id (int): Portfolio entry ID
            
        Returns:
            UserPortfolio: The requested portfolio entry
            
        Raises:
            ResourceNotFoundError: If portfolio entry does not exist
        """
        portfolio = UserPortfolio.query.get(portfolio_id)
        if not portfolio:
            raise ResourceNotFoundError(f"Portfolio entry with ID {portfolio_id} not found")
        return portfolio
    
    @staticmethod
    def get_user_portfolio(user_id):
        """
        Get all portfolio entries for a user
        
        Args:
            user_id (int): User ID
            
        Returns:
            list: List of portfolio entries
            
        Raises:
            ResourceNotFoundError: If user does not exist
        """
        # Verify user exists
        user = UserService.get_user(user_id)
        
        return UserPortfolio.query.filter_by(user_id=user_id).all()
    
    @staticmethod
    def update_portfolio(portfolio_id, units=None, current_nav=None, current_value=None):
        """
        Update a portfolio entry
        
        Args:
            portfolio_id (int): Portfolio entry ID
            units (float, optional): New units value
            current_nav (float, optional): New current NAV
            current_value (float, optional): New current value
            
        Returns:
            UserPortfolio: The updated portfolio entry
            
        Raises:
            ResourceNotFoundError: If portfolio entry does not exist
        """
        portfolio = PortfolioService.get_portfolio(portfolio_id)
        
        if units is not None:
            portfolio.units = units
        if current_nav is not None:
            portfolio.current_nav = current_nav
        if current_value is not None:
            portfolio.current_value = current_value
            
        portfolio.last_updated = datetime.utcnow()
        
        try:
            db.session.commit()
            logger.info(f"Portfolio entry updated: {portfolio_id}")
            return portfolio
        except Exception as e:
            db.session.rollback()
            logger.error(f"Error updating portfolio entry: {str(e)}")
            raise DatabaseError(f"Error updating portfolio entry: {str(e)}")
    
    @staticmethod
    def delete_portfolio(portfolio_id):
        """
        Delete a portfolio entry
        
        Args:
            portfolio_id (int): Portfolio entry ID
            
        Raises:
            ResourceNotFoundError: If portfolio entry does not exist
        """
        portfolio = PortfolioService.get_portfolio(portfolio_id)
        
        try:
            db.session.delete(portfolio)
            db.session.commit()
            logger.info(f"Portfolio entry deleted: {portfolio_id}")
        except Exception as e:
            db.session.rollback()
            logger.error(f"Error deleting portfolio entry: {str(e)}")
            raise DatabaseError(f"Error deleting portfolio entry: {str(e)}")
    
    @staticmethod
    def update_holdings(user_id, scheme_id, new_units, nav, invested_amount):
        """
        Update holdings for a user and scheme
        
        Args:
            user_id (int): User ID
            scheme_id (int): Scheme ID
            new_units (float): New units to add/subtract
            nav (float): NAV for the transaction
            invested_amount (float): Amount invested/redeemed
            
        Returns:
            MFHoldings: The updated holdings
            
        Raises:
            ResourceNotFoundError: If user or scheme does not exist
        """
        # Verify user and scheme exist
        user = UserService.get_user(user_id)
        scheme = FundService.get_fund_scheme(scheme_id)
        
        # Get existing holdings or create new one
        holdings = MFHoldings.query.filter_by(user_id=user_id, scheme_id=scheme_id).first()
        
        if not holdings:
            # If new units is negative (redemption), can't proceed
            if new_units < 0:
                raise ValidationError("Cannot redeem units from non-existent holdings")
                
            holdings = MFHoldings(
                user_id=user_id,
                scheme_id=scheme_id,
                units_held=new_units,
                average_nav=nav,
                invested_amount=invested_amount,
                current_value=new_units * nav
            )
            db.session.add(holdings)
        else:
            # Update existing holdings
            old_units = holdings.units_held
            old_invested = holdings.invested_amount
            
            # Calculate new totals
            total_units = old_units + new_units
            
            # Ensure we don't go below zero units
            if total_units < 0:
                raise ValidationError("Redemption would result in negative units")
                
            total_invested = old_invested + invested_amount
            
            # Update holdings
            holdings.units_held = total_units
            
            # Recalculate average NAV only for purchases
            if new_units > 0:
                holdings.average_nav = total_invested / total_units if total_units > 0 else 0
                
            holdings.invested_amount = total_invested
            holdings.current_value = total_units * nav
            holdings.last_updated = datetime.utcnow()
        
        try:
            db.session.commit()
            logger.info(f"Holdings updated for user {user_id}, scheme {scheme_id}")
            return holdings
        except Exception as e:
            db.session.rollback()
            logger.error(f"Error updating holdings: {str(e)}")
            raise DatabaseError(f"Error updating holdings: {str(e)}")