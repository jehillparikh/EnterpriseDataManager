import requests
import logging
import json
from datetime import datetime
from config import BSE_STAR_API_KEY, BSE_STAR_BASE_URL

logger = logging.getLogger(__name__)

class BseStarService:
    """Service for interacting with BSE Star APIs"""
    
    @staticmethod
    def get_auth_header():
        """
        Generate the authentication header
        
        Returns:
            dict: The authentication header
        """
        return {
            "Authorization": f"Bearer {BSE_STAR_API_KEY}",
            "Content-Type": "application/json"
        }
    
    @staticmethod
    def register_client(pan, full_name, dob, mobile, email, address, city, state, pincode):
        """
        Register a client with BSE Star
        
        Args:
            pan (str): PAN number
            full_name (str): Full name
            dob (str): Date of birth in format YYYY-MM-DD
            mobile (str): Mobile number
            email (str): Email address
            address (str): Address
            city (str): City
            state (str): State
            pincode (str): Pincode
            
        Returns:
            dict: Registration result
        """
        url = f"{BSE_STAR_BASE_URL}/RegisterClient"
        headers = BseStarService.get_auth_header()
        
        # Split full name into parts
        name_parts = full_name.split(' ')
        first_name = name_parts[0]
        last_name = name_parts[-1] if len(name_parts) > 1 else ""
        middle_name = ' '.join(name_parts[1:-1]) if len(name_parts) > 2 else ""
        
        # Format date as required by BSE
        try:
            dob_date = datetime.strptime(dob, "%Y-%m-%d")
            dob_formatted = dob_date.strftime("%d/%m/%Y")
        except Exception as e:
            logger.error(f"Error formatting date: {str(e)}")
            dob_formatted = dob  # Use as is if format fails
        
        data = {
            "PAN": pan,
            "FirstName": first_name,
            "MiddleName": middle_name,
            "LastName": last_name,
            "DOB": dob_formatted,
            "MobileNumber": mobile,
            "Email": email,
            "Address": address,
            "City": city,
            "State": state,
            "PinCode": pincode,
            "TaxStatus": "01",  # Individual
            "OccupationCode": "02",  # Service
            "Gender": "M"  # Default to Male, should be updated based on input
        }
        
        try:
            response = requests.post(url, headers=headers, data=json.dumps(data))
            return response.json()
        except Exception as e:
            logger.error(f"Error in register_client: {str(e)}")
            return {"success": False, "error": str(e)}
    
    @staticmethod
    def register_bank_account(client_code, account_number, ifsc_code, account_type="SB"):
        """
        Register a bank account for a client
        
        Args:
            client_code (str): Client code from BSE
            account_number (str): Bank account number
            ifsc_code (str): IFSC code
            account_type (str): Account type (SB for Savings, CA for Current)
            
        Returns:
            dict: Registration result
        """
        url = f"{BSE_STAR_BASE_URL}/RegisterBankAccount"
        headers = BseStarService.get_auth_header()
        
        data = {
            "ClientCode": client_code,
            "AccountNumber": account_number,
            "IFSCCode": ifsc_code,
            "AccountType": account_type
        }
        
        try:
            response = requests.post(url, headers=headers, data=json.dumps(data))
            return response.json()
        except Exception as e:
            logger.error(f"Error in register_bank_account: {str(e)}")
            return {"success": False, "error": str(e)}
    
    @staticmethod
    def register_mandate(client_code, bank_account_number, amount, start_date, end_date):
        """
        Register a mandate for a client
        
        Args:
            client_code (str): Client code from BSE
            bank_account_number (str): Bank account number
            amount (float): Mandate amount
            start_date (str): Start date in format YYYY-MM-DD
            end_date (str): End date in format YYYY-MM-DD
            
        Returns:
            dict: Registration result
        """
        url = f"{BSE_STAR_BASE_URL}/RegisterMandate"
        headers = BseStarService.get_auth_header()
        
        # Format dates as required by BSE
        try:
            start_date_obj = datetime.strptime(start_date, "%Y-%m-%d")
            end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
            start_date_formatted = start_date_obj.strftime("%d/%m/%Y")
            end_date_formatted = end_date_obj.strftime("%d/%m/%Y")
        except Exception as e:
            logger.error(f"Error formatting dates: {str(e)}")
            start_date_formatted = start_date
            end_date_formatted = end_date
        
        data = {
            "ClientCode": client_code,
            "BankAccountNumber": bank_account_number,
            "Amount": amount,
            "StartDate": start_date_formatted,
            "EndDate": end_date_formatted
        }
        
        try:
            response = requests.post(url, headers=headers, data=json.dumps(data))
            return response.json()
        except Exception as e:
            logger.error(f"Error in register_mandate: {str(e)}")
            return {"success": False, "error": str(e)}
    
    @staticmethod
    def purchase_transaction(client_code, scheme_code, amount, mandate_id=None):
        """
        Execute a purchase transaction
        
        Args:
            client_code (str): Client code from BSE
            scheme_code (str): Scheme code
            amount (float): Purchase amount
            mandate_id (str, optional): Mandate ID for SIP
            
        Returns:
            dict: Transaction result
        """
        url = f"{BSE_STAR_BASE_URL}/PurchaseTransaction"
        headers = BseStarService.get_auth_header()
        
        data = {
            "ClientCode": client_code,
            "SchemeCode": scheme_code,
            "Amount": amount,
            "PaymentMode": "M" if mandate_id else "N",  # M for Mandate, N for Net Banking
        }
        
        if mandate_id:
            data["MandateID"] = mandate_id
        
        try:
            response = requests.post(url, headers=headers, data=json.dumps(data))
            return response.json()
        except Exception as e:
            logger.error(f"Error in purchase_transaction: {str(e)}")
            return {"success": False, "error": str(e)}
    
    @staticmethod
    def redemption_transaction(client_code, scheme_code, units=None, amount=None, all_units=False):
        """
        Execute a redemption transaction
        
        Args:
            client_code (str): Client code from BSE
            scheme_code (str): Scheme code
            units (float, optional): Units to redeem
            amount (float, optional): Amount to redeem
            all_units (bool, optional): Whether to redeem all units
            
        Returns:
            dict: Transaction result
        """
        url = f"{BSE_STAR_BASE_URL}/RedemptionTransaction"
        headers = BseStarService.get_auth_header()
        
        data = {
            "ClientCode": client_code,
            "SchemeCode": scheme_code,
        }
        
        if all_units:
            data["AllUnits"] = "Y"
        elif units is not None:
            data["Units"] = units
        elif amount is not None:
            data["Amount"] = amount
        else:
            return {"success": False, "error": "One of units, amount, or all_units must be specified"}
        
        try:
            response = requests.post(url, headers=headers, data=json.dumps(data))
            return response.json()
        except Exception as e:
            logger.error(f"Error in redemption_transaction: {str(e)}")
            return {"success": False, "error": str(e)}