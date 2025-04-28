#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import re
import logging
import types
import importlib
import inspect

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("yoruba_scraper_fixed.log")
    ]
)
logger = logging.getLogger("yoruba_scraper_fix")

# Make a backup of the original file
def backup_file(filename):
    """Create a backup of the file if it doesn't exist"""
    backup = f"{filename}.bak"
    if not os.path.exists(backup):
        import shutil
        shutil.copy2(filename, backup)
        logger.info(f"Created backup of {filename} at {backup}")
    return backup

# Function to fix spacing issues in Yoruba text
def fix_yoruba_spacing(text):
    """Fix spacing issues in Yoruba text."""
    if not isinstance(text, str):
        return text
    
    # Fix common patterns where 'à' is joined to subsequent word
    text = re.sub(r'(^|\s|\(|")à(?=[a-zàáèéìíòóùúẹọṣ])', r'\1à ', text)
    
    # Fix specific starting pattern for "À bá"
    text = re.sub(r'(?:^|\s)À(?:bá|ba)ti', r'À bá ti', text)
    
    # Fix auxiliary verb spacing issues
    text = re.sub(r'([áàńḿ])([a-zàáèéìíòóùúẹọṣ][a-zàáèéìíòóùúẹọṣ]+)', r'\1 \2', text)
    
    # Fix specific particles and pronouns that are commonly misjoined
    text = re.sub(r'(wọ́n|won|kí|ki|tó|to|ìyẹn|iyen|yìí|yii|èyí|eyi|bàá|baa)([a-zàáèéìíòóùúẹọṣ][a-zàáèéìíòóùúẹọṣ]+)', r'\1 \2', text)
    
    # Fix cases where 'á' follows a word and should be separated
    text = re.sub(r'([a-zàáèéìíòóùúẹọṣ][a-zàáèéìíòóùúẹọṣ]+)(á[a-zàáèéìíòóùúẹọṣ])', r'\1 \2', text)
    
    # Fix specific verb combinations (ti + something)
    text = re.sub(r'(ti)(tu|yan|fi|lo|gbà|pa|mọ̀)', r'\1 \2', text)
    
    # Fix 'bá' plus following word
    text = re.sub(r'(bá)(ti|pa|fi|gbà|jẹ́|ṣe)', r'\1 \2', text)
    
    # Fix final spacing issues
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text

# Function to fix spacing issues in English text
def fix_english_spacing(text):
    """Fix spacing issues in English text."""
    if not isinstance(text, str):
        return text
        
    # Add spaces between lowercase and uppercase letters (except for known acronyms)
    text = re.sub(r'([a-z])([A-Z])', r'\1 \2', text)
    
    # Fix specific patterns we've observed in the data
    text = re.sub(r'beenput(to)?death', r'been put to death', text)
    text = re.sub(r'putto(death)', r'put to \1', text)
    text = re.sub(r'beenputto', r'been put to', text)
    
    # Fix joined "been" + verb
    past_participlesAfterBeen = ["released", "put", "used", "confined", "blessed", "left"]
    for pp in past_participlesAfterBeen:
        text = text.replace(f"been{pp}", f"been {pp}")
    
    # Fix final spacing issues
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text

# Create the wrapper functions that handle 'self' if presented
def fix_yoruba_spacing_wrapper(*args):
    """Wrapper function to handle both free function and method calls"""
    if len(args) == 1:
        return fix_yoruba_spacing(args[0])
    elif len(args) == 2:  # Method call with self
        return fix_yoruba_spacing(args[1])
    else:
        raise TypeError(f"fix_yoruba_spacing_wrapper expected 1 or 2 arguments, got {len(args)}")

def fix_english_spacing_wrapper(*args):
    """Wrapper function to handle both free function and method calls"""
    if len(args) == 1:
        return fix_english_spacing(args[0])
    elif len(args) == 2:  # Method call with self
        return fix_english_spacing(args[1])
    else:
        raise TypeError(f"fix_english_spacing_wrapper expected 1 or 2 arguments, got {len(args)}")

# Monkey patch the ExampleSentenceExtractor class
def patch_scrape_module():
    """Patch the scrape module to use our wrapper functions"""
    try:
        # Import the module
        import scrape
        
        # Backup the scrape.py file
        backup_file("scrape.py")
        
        # Add our functions to the module namespace
        scrape.fix_yoruba_spacing = fix_yoruba_spacing_wrapper
        scrape.fix_english_spacing = fix_english_spacing_wrapper
        
        # Monkey patch the ExampleSentenceExtractor class
        if hasattr(scrape, 'ExampleSentenceExtractor'):
            logger.info("Patching ExampleSentenceExtractor class")
            
            # Add the methods to the class
            scrape.ExampleSentenceExtractor._fix_yoruba_spacing = fix_yoruba_spacing_wrapper
            scrape.ExampleSentenceExtractor._fix_english_spacing = fix_english_spacing_wrapper
            
            # Patch the clean_example_text method if needed
            if hasattr(scrape.ExampleSentenceExtractor, 'clean_example_text'):
                original_method = scrape.ExampleSentenceExtractor.clean_example_text
                
                # Define a wrapper method that catches any spacing method errors
                def clean_example_text_wrapper(self, text):
                    try:
                        return original_method(self, text)
                    except (AttributeError, TypeError) as e:
                        # If there's a spacing method error, use our functions
                        if not text:
                            return ""
                        
                        # Do basic cleanup
                        text = re.sub(r'\s+', ' ', text).strip()
                        
                        # Check if it's Yoruba text
                        has_yoruba_diacritics = bool(re.search(r'[àáèéìíòóùúẹọṣ]', text))
                        
                        # Apply appropriate spacing fix
                        if has_yoruba_diacritics:
                            text = fix_yoruba_spacing(text)
                        else:
                            text = fix_english_spacing(text)
                            
                        return text.strip()
                
                # Replace the original method with our wrapped version
                scrape.ExampleSentenceExtractor.clean_example_text = clean_example_text_wrapper
                logger.info("Patched clean_example_text method")
        
        # Return the patched module
        return scrape
        
    except Exception as e:
        logger.error(f"Error patching scrape module: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

# Main function to run the scraper
def main():
    """Main function to patch the module and run the scraper"""
    logger.info("Starting the yoruba-scraper script with patches")
    
    # Patch the module
    scrape_module = patch_scrape_module()
    
    if not scrape_module:
        logger.error("Failed to patch the scrape module")
        return 1
        
    # Get the GlosbeYorubaScraper class
    GlosbeYorubaScraper = getattr(scrape_module, 'GlosbeYorubaScraper', None)
    
    if not GlosbeYorubaScraper:
        logger.error("Could not find GlosbeYorubaScraper class in the module")
        return 1
        
    # Create the scraper instance
    logger.info("Creating GlosbeYorubaScraper instance")
    scraper = GlosbeYorubaScraper()
    
    # Run the scraper
    logger.info("Running the scraper")
    try:
        scraper.run()
        return 0
    except Exception as e:
        logger.error(f"Error running the scraper: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main()) 