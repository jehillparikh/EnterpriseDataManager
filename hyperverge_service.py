import os
import requests
import base64
import json
import logging
from datetime import datetime
from config import HYPERVERGE_APP_ID, HYPERVERGE_APP_KEY, HYPERVERGE_BASE_URL

logger = logging.getLogger(__name__)

class HypervergeService:
    """Service for interacting with Hyperverge APIs for KYC verification"""
    
    @staticmethod
    def get_auth_header():
        """
        Generate the base64 encoded authentication header
        
        Returns:
            dict: The authentication header
        """
        auth_string = f"{HYPERVERGE_APP_ID}:{HYPERVERGE_APP_KEY}"
        auth_encoded = base64.b64encode(auth_string.encode()).decode()
        return {"Authorization": f"Basic {auth_encoded}"}
    
    @staticmethod
    def verify_id_card(id_image_path, id_type="PAN"):
        """
        Verify an ID card (PAN, Aadhaar, etc.)
        
        Args:
            id_image_path (str): Path to the ID image file
            id_type (str): Type of ID card (PAN, AADHAAR, etc.)
            
        Returns:
            dict: Verification result
        """
        url = f"{HYPERVERGE_BASE_URL}/readKYC"
        headers = HypervergeService.get_auth_header()
        
        # Check if file exists
        if not os.path.exists(id_image_path):
            logger.error(f"File not found: {id_image_path}")
            return {"success": False, "error": "File not found"}
        
        try:
            with open(id_image_path, 'rb') as image_file:
                files = {'image': image_file}
                params = {'type': id_type}
                
                response = requests.post(url, headers=headers, files=files, params=params)
                return response.json()
        except Exception as e:
            logger.error(f"Error in verify_id_card: {str(e)}")
            return {"success": False, "error": str(e)}
    
    @staticmethod
    def verify_face_match(selfie_image_path, id_image_path):
        """
        Verify if the face in the selfie matches the face in the ID card
        
        Args:
            selfie_image_path (str): Path to the selfie image file
            id_image_path (str): Path to the ID image file
            
        Returns:
            dict: Face match result
        """
        url = f"{HYPERVERGE_BASE_URL}/matchFaces"
        headers = HypervergeService.get_auth_header()
        
        # Check if files exist
        if not os.path.exists(selfie_image_path):
            logger.error(f"File not found: {selfie_image_path}")
            return {"success": False, "error": "Selfie file not found"}
        
        if not os.path.exists(id_image_path):
            logger.error(f"File not found: {id_image_path}")
            return {"success": False, "error": "ID file not found"}
        
        try:
            with open(selfie_image_path, 'rb') as selfie_file, open(id_image_path, 'rb') as id_file:
                files = {
                    'image1': selfie_file,
                    'image2': id_file
                }
                
                response = requests.post(url, headers=headers, files=files)
                return response.json()
        except Exception as e:
            logger.error(f"Error in verify_face_match: {str(e)}")
            return {"success": False, "error": str(e)}
    
    @staticmethod
    def verify_liveness(video_path):
        """
        Verify liveness using a short video
        
        Args:
            video_path (str): Path to the video file
            
        Returns:
            dict: Liveness verification result
        """
        url = f"{HYPERVERGE_BASE_URL}/checkLiveness"
        headers = HypervergeService.get_auth_header()
        
        # Check if file exists
        if not os.path.exists(video_path):
            logger.error(f"File not found: {video_path}")
            return {"success": False, "error": "Video file not found"}
        
        try:
            with open(video_path, 'rb') as video_file:
                files = {'video': video_file}
                
                response = requests.post(url, headers=headers, files=files)
                return response.json()
        except Exception as e:
            logger.error(f"Error in verify_liveness: {str(e)}")
            return {"success": False, "error": str(e)}