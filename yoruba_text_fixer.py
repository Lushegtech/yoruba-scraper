#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import logging
import os
import sys
import pandas as pd
import traceback

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("yoruba_text_fixer.log")
    ]
)
logger = logging.getLogger("yoruba_text_fixer")

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
    
    # Fix 'ká' plus following word
    text = re.sub(r'(ká)(ní|sì|ti)', r'\1 \2', text)
    
    # Fix 'kò' plus following word 
    text = re.sub(r'(kò)(ké|ní|fi|sì)', r'\1 \2', text)
    
    # Fix common incorrect word formations
    text = re.sub(r'nià', r'ni à', text)
    text = re.sub(r'láti', r'lá ti', text)
    text = re.sub(r'síbẹ̀', r'sí bẹ̀', text)
    
    # Fix spacing after "Àmọ́"
    text = re.sub(r'(Àmọ́|àmọ́)([a-zàáèéìíòóùúẹọṣ])', r'\1 \2', text)
    
    # Fix spacing for "pe à bá" construction
    text = re.sub(r'peà(bá|ba)', r'pe à \1', text)
    
    # Fix for "à bá" construction
    text = re.sub(r'à(bá|ba)ti', r'à \1 ti', text)
    
    # Fix final spacing issues
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text

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
    text = re.sub(r'Thismancould', r'This man could', text)
    text = re.sub(r'releasedifhe', r'released if he', text)
    
    # Fix joined "been" + verb
    past_participlesAfterBeen = ["released", "put", "used", "confined", "blessed", "left", "prevented", "corrected", "heeded", "supplied", "saved"]
    for pp in past_participlesAfterBeen:
        text = text.replace(f"been{pp}", f"been {pp}")
    
    # Fix main verb + preposition/conjunction
    main_verbs = ["released", "explained", "provided", "put", "had", "made", "took", "gave", "left", "used", "kept", "come", "saved"]
    prepositions = ["if", "when", "as", "by", "to", "for", "with", "on", "in", "at", "from"]
    for verb in main_verbs:
        for prep in prepositions:
            text = text.replace(f"{verb}{prep}", f"{verb} {prep}")
    
    # Fix modal verbs + other verb forms
    modal_verbs = ["could", "would", "should", "might", "will", "can"]
    verb_forms = ["have", "be", "not", "get", "make", "take", "give", "find"]
    for modal in modal_verbs:
        for verb in verb_forms:
            text = text.replace(f"{modal}{verb}", f"{modal} {verb}")
            
    # Fix specific common joined words
    specific_fixes = [
        ('couldhave', 'could have'),
        ('wouldhave', 'would have'),
        ('shouldhave', 'should have'),
        ('havebeen', 'have been'),
        ('hasbeen', 'has been'),
        ('willbe', 'will be'),
        ('wouldbe', 'would be'),
        ('hecould', 'he could'),
        ('hewas', 'he was'),
        ('hehas', 'he has'),
        ('inhim', 'in him'),
        ('itis', 'it is'),
        ('thatit', 'that it'),
        ('thanit', 'than it'),
        ('withplenty', 'with plenty'),
    ]
    
    for bad, good in specific_fixes:
        text = text.replace(bad, good)
    
    # Fix spacing around punctuation
    text = re.sub(r'\s+([.,;:!?])', r'\1', text)
    text = re.sub(r'([.,;:!?])([A-Za-z])', r'\1 \2', text)
    
    # Fix final spacing issues
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text

def extract_clean_translation(text):
    """Clean translation text that may have formatting issues"""
    if not text or not isinstance(text, str):
        return text
        
    # Remove any HTML tags that might be present
    text = re.sub(r'<[^>]+>', '', text)
    
    # Replace various types of quotes with regular ones
    text = text.replace(''', "'").replace(''', "'").replace('"', '"').replace('"', '"')
    
    # Normalize whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text

# If this module is run directly, provide command-line functionality to fix CSV files
if __name__ == "__main__":
    import argparse
    
    # Set up command line arguments
    parser = argparse.ArgumentParser(description='Fix spacing issues in Yoruba and English text in CSV files.')
    parser.add_argument('--file', type=str, help='Specific CSV file to process')
    parser.add_argument('--dir', type=str, default='./scraped_data', help='Directory containing CSV files to process')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging')
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    print("Initializing Yoruba text fixer...")
    
    # Process files based on command-line arguments
    if args.file:
        from fix_csv_data import verify_and_fix_csv
        files_processed, entries_fixed = 1, verify_and_fix_csv(args.file)
    else:
        from fix_csv_data import process_csv_files
        files_processed, entries_fixed = process_csv_files(args.dir)
    
    print(f"Verification complete. Processed {files_processed} files, fixed {entries_fixed} entries.")
    print("See yoruba_text_fixer.log for details.") 