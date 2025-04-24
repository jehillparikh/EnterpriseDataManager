#!/usr/bin/env python3
"""
Script to import all mutual fund data from Excel files into the database.
This is a convenience wrapper around the FundDataImporter class.
"""

import logging
import sys
import os

# Add path to parent directory for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.fund_data_importer import FundDataImporter

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logger.info("Starting mutual fund data import process")
    
    try:
        # Create importer
        importer = FundDataImporter()
        
        # Import all data
        stats = importer.import_all_data(clear_existing=True)
        
        logger.info("Import process completed successfully")
        logger.info("===== Summary =====")
        logger.info(f"Funds: {stats['factsheet']['funds_created']} created, {stats['factsheet']['funds_updated']} updated")
        logger.info(f"Factsheets: {stats['factsheet']['factsheets_created']} created, {stats['factsheet']['factsheets_updated']} updated")
        logger.info(f"Returns: {stats['returns']['returns_created']} created, {stats['returns']['returns_updated']} updated")
        logger.info(f"Portfolio holdings: {stats['portfolio']['holdings_created']} created")
        logger.info(f"NAV records: {stats['nav']['nav_created']} created")
        logger.info("==================")
        
    except Exception as e:
        logger.error(f"Import process failed: {e}")
        sys.exit(1)
    
    logger.info("Done")
    sys.exit(0)