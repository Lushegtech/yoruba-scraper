#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import logging
import os
import sys
from scrape import GlosbeYorubaScraper

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("verify_csv_data.log")
    ]
)
logger = logging.getLogger("verify_csv_data")

def main():
    """
    Utility to verify and fix data quality issues in CSV files.
    This performs more thorough validation and cleaning than just fixing spacing.
    """
    parser = argparse.ArgumentParser(description="Verify and fix data quality in Yoruba CSV files")
    parser.add_argument("--file", "-f", help="Path to a specific CSV file to fix")
    parser.add_argument("--dir", "-d", default="./scraped_data", help="Base directory to search for CSV files")
    parser.add_argument("--report-only", "-r", action="store_true", help="Only report issues without fixing them")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        print("Initializing Yoruba CSV verifier...")
        
        # Create the scraper with the specified base folder
        scraper = GlosbeYorubaScraper(base_folder=args.dir)
        
        # Verify and fix the CSV data
        if args.report_only:
            print("Running in report-only mode, will not make changes to files")
            # TODO: Implement report-only functionality
            print("Report-only mode not yet implemented")
            return
            
        # Process the files
        files_processed, entries_fixed = scraper.verify_and_fix_csv_data(args.file)
        
        print(f"Verification complete. Processed {files_processed} files, fixed {entries_fixed} entries.")
        print("See verify_csv_data.log for details.")
        
    except Exception as e:
        import traceback
        logger.error(f"Error verifying CSV data: {str(e)}")
        traceback.print_exc()
        print(f"ERROR: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 