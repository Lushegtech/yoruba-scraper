#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import logging
import os
import sys
import re
import shutil
import pandas as pd
import traceback

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("fix_csv_data.log")
    ]
)
logger = logging.getLogger("fix_csv_data")

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

def verify_and_fix_csv(csv_file_path):
    """Verify and fix a single CSV file"""
    try:
        logger.info(f"Processing {csv_file_path}")
        
        # Read the CSV file
        df = pd.read_csv(csv_file_path, encoding='utf-8')
        original_df = df.copy()
        
        # Identify column types to know what kind of data we're dealing with
        has_translations = 'translation' in df.columns
        has_examples = 'yoruba_text' in df.columns and 'english_text' in df.columns
        
        if has_translations:
            # Handle translation CSV
            logger.info(f"Fixing translations in {csv_file_path}")
            df['translation'] = df['translation'].apply(
                lambda x: extract_clean_translation(x) if isinstance(x, str) else x
            )
        
        if has_examples:
            # Handle examples CSV
            logger.info(f"Fixing examples in {csv_file_path}")
            
            # Clean yoruba text
            df['yoruba_text'] = df['yoruba_text'].apply(
                lambda x: fix_yoruba_spacing(x) if isinstance(x, str) else x
            )
            
            # Clean english text
            df['english_text'] = df['english_text'].apply(
                lambda x: fix_english_spacing(x) if isinstance(x, str) else x
            )
        
        # Count changes made
        changes_made = 0
        for i in range(len(df)):
            row_changed = False
            for col in df.columns:
                if df.iloc[i][col] != original_df.iloc[i][col]:
                    row_changed = True
                    break
            if row_changed:
                changes_made += 1
        
        if changes_made > 0:
            # Create backup of original file
            backup_path = f"{csv_file_path}.bak"
            if not os.path.exists(backup_path):
                shutil.copy2(csv_file_path, backup_path)
                logger.info(f"Created backup at {backup_path}")
            
            # Save the updated data
            df.to_csv(csv_file_path, index=False, encoding='utf-8')
            logger.info(f"Fixed {changes_made} entries in {csv_file_path}")
            
            return changes_made
        else:
            logger.info(f"No issues found in {csv_file_path}")
            return 0
            
    except Exception as e:
        logger.error(f"Error processing {csv_file_path}: {str(e)}")
        traceback.print_exc()
        return 0

def process_csv_files(base_folder, specific_file=None):
    """Process CSV files in the base folder or a specific file"""
    files_processed = 0
    entries_fixed = 0
    
    # Handle single file or all files
    csv_files = []
    if specific_file and os.path.exists(specific_file):
        csv_files = [specific_file]
    else:
        # Find all CSV files in base folder
        for root, _, files in os.walk(base_folder):
            for file in files:
                if file.endswith('.csv'):
                    csv_files.append(os.path.join(root, file))
    
    logger.info(f"Found {len(csv_files)} CSV files to process")
    
    # Process each file
    for file_path in csv_files:
        changes = verify_and_fix_csv(file_path)
        entries_fixed += changes
        files_processed += 1
    
    logger.info(f"Completed verification and fixing of {files_processed} CSV files, {entries_fixed} entries fixed")
    return files_processed, entries_fixed

def main():
    """
    Utility to verify and fix data quality issues in CSV files.
    This performs thorough validation and cleaning.
    """
    parser = argparse.ArgumentParser(description="Verify and fix data quality in Yoruba CSV files")
    parser.add_argument("--file", "-f", help="Path to a specific CSV file to fix")
    parser.add_argument("--dir", "-d", default="./scraped_data", help="Base directory to search for CSV files")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        print("Initializing Yoruba CSV fixer...")
        
        # Process the files
        files_processed, entries_fixed = process_csv_files(args.dir, args.file)
        
        print(f"Verification complete. Processed {files_processed} files, fixed {entries_fixed} entries.")
        print("See fix_csv_data.log for details.")
        
    except Exception as e:
        logger.error(f"Error fixing CSV data: {str(e)}")
        traceback.print_exc()
        print(f"ERROR: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 