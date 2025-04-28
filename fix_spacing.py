#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import pandas as pd
import logging
import argparse
from pathlib import Path
import shutil

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("spacing_fix_log.txt")
    ]
)
logger = logging.getLogger("spacing_fix")

def _fix_yoruba_spacing(text):
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
    
    # Fix "ẹ̀" plus following word
    text = re.sub(r'(ẹ̀)([a-zàáèéìíòóùúẹọṣ][a-zàáèéìíòóùúẹọṣ]+)', r'\1 \2', text)
    
    # Fix common incorrect word formations
    text = re.sub(r'nià', r'ni à', text)
    text = re.sub(r'láti', r'lá ti', text)
    text = re.sub(r'síbẹ̀', r'sí bẹ̀', text)
    text = re.sub(r'walá', r'wa lá', text)
    text = re.sub(r'lọ́wọ́', r'lọ́ wọ́', text)
    
    # Fix À pattern at the beginning of sentences
    text = re.sub(r'^À', r'À ', text)
    text = re.sub(r'([\.\?!]\s*)À', r'\1À ', text)
    
    # Fix for "Bí ... bá" construction which is commonly joined incorrectly
    text = re.sub(r'(Bí|bí)([a-zàáèéìíòóùúẹọṣ][a-zàáèéìíòóùúẹọṣ]+)', r'\1 \2', text)
    
    # Fix for "ká ní" and similar constructions
    text = re.sub(r'ká(ní)', r'ká \1', text)
    
    # Fix spacing between peà and bá
    text = re.sub(r'peà(bá|ba)', r'pe à \1', text)
    
    # Fix for "à bá" construction
    text = re.sub(r'à(bá|ba)ti', r'à \1 ti', text)
    
    # Fix for "à wọn" construction
    text = re.sub(r'à(wọn)', r'à \1', text)
    
    # Fix spacing after "Àmọ́"
    text = re.sub(r'(Àmọ́|àmọ́)([a-zàáèéìíòóùúẹọṣ])', r'\1 \2', text)
    
    # Fix spacing for "lá wọ́jú"
    text = re.sub(r'lá(wọ́jú)', r'lá \1', text)
    
    # Fix spacing for comma-separated constructions
    text = re.sub(r',([a-zàáèéìíòóùúẹọṣ])', r', \1', text)
    
    # Fix spacing for the "dá ra" pattern
    text = re.sub(r'dá(ra)', r'dá \1', text)
    
    return text

def _fix_english_spacing(text):
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
    
    # Fix pronoun + preposition
    pronouns = ["him", "her", "it", "them", "us", "you", "we", "they", "he", "she"]
    for pron in pronouns:
        for prep in prepositions:
            text = text.replace(f"{pron}{prep}", f"{pron} {prep}")
            
    # Fix specific cases where 'we' is connected
    text = re.sub(r'(If|if)we', r'\1 we', text)
    text = re.sub(r'(If|if)no', r'\1 no', text)
    text = re.sub(r'andwe', r'and we', text)
    
    # Fix determiner + noun
    determiners = ["This", "That", "The", "A", "An", "His", "Her", "Our", "Their", "Its"]
    nouns = ["man", "woman", "child", "person", "people", "life", "time", "day", "world", "house", "best"]
    for det in determiners:
        for noun in nouns:
            text = text.replace(f"{det}{noun}", f"{det} {noun}")
    
    # Fix compound constructions with "to"
    compounds = [("put", "to", "death"), ("have", "to", "be"), ("need", "to", "go"), ("had", "to", "obey")]
    for a, b, c in compounds:
        text = text.replace(f"{a}{b}{c}", f"{a} {b} {c}")
    
    # Fix "many of mankind's" type constructions
    text = text.replace("manyof", "many of")
    text = text.replace("mankind'smistakes", "mankind's mistakes")
    text = text.replace("ofmankind", "of mankind")
    text = text.replace("mankind's", "mankind's ")  # Add space after possessive
    
    # Fix common joined phrases we've observed
    text = re.sub(r'we\'?(re)?sorry', r"we're sorry", text)
    text = re.sub(r'wouldn\'?t(have)?', r"wouldn't have", text)
    text = re.sub(r'didn\'?t(come)?', r"didn't come", text)
    
    # Fix "from among" construction
    text = text.replace("fromamong", "from among")
    
    # These specific fixes needed for our dataset
    specific_fixes = [
        ('couldhave', 'could have'),
        ('couldhavebeen', 'could have been'),
        ('wouldhave', 'would have'),
        ('wouldhavereturned', 'would have returned'),
        ('wouldhaveused', 'would have used'),
        ('shouldhave', 'should have'),
        ('havebeen', 'have been'),
        ('becomejust', 'become just'),
        ('beenconfined', 'been confined'),
        ('beenexpelled', 'been expelled'),
        ('beencorrected', 'been corrected'),
        ('beenleft', 'been left'),
        ('beensaved', 'been saved'),
        ('havebeen', 'have been'),
        ('mightmanyof', 'might many of'),
        ('willbe', 'will be'),
        ('willtake', 'will take'),
        ('hadto', 'had to'),
        ('itup', 'it up'),
        ('withplenty', 'with plenty'),
        ('toextend', 'to extend'),
        ('theylearn', 'they learn'),
        ('inhim', 'in him'),
        ('uswith', 'us with'),
        ('hewas', 'he was'),
        ('hecould', 'he could'),
        ('shewas', 'she was'),
        ('Theyno', 'They no'),
        ('andspiritual', 'and spiritual'),
        ('betheirs', 'be theirs'),
        ('Whydoes', 'Why does'),
        ('willunity', 'will unity'),
        ('thathecould', 'that he could'),
        ('youwouldhave', 'you would have'),
    ]
    
    for old, new in specific_fixes:
        text = text.replace(old, new)
    
    # Process words with missing spaces around punctuation
    text = re.sub(r'([.,;:!?])([A-Za-z])', r'\1 \2', text)
    
    # Fix "will be" and similar constructs
    modal_be = [("will", "be"), ("would", "be"), ("could", "be"), ("should", "be")]
    for modal, be in modal_be:
        text = text.replace(f"{modal}{be}", f"{modal} {be}")
    
    # Fix "with" compounds
    with_compounds = [("us", "with"), ("you", "with"), ("them", "with")]
    for a, b in with_compounds:
        text = text.replace(f"{a}{b}", f"{a} {b}")
    
    # Fix "and" compounds
    and_compounds = [
        ("spiritual", "and"), ("emotional", "and"), ("physical", "and"), 
        ("mental", "and"), ("moral", "and"), ("social", "and")
    ]
    for a, b in and_compounds:
        text = text.replace(f"{a}{b}", f"{a} {b}")
    
    # Fix "he/she/it" compounds
    that_compounds = [
        ("that", "he"), ("that", "she"), ("that", "it"), ("that", "they"),
        ("when", "he"), ("when", "she"), ("when", "it"), ("when", "they"),
        ("if", "he"), ("if", "she"), ("if", "it"), ("if", "they"),
        ("because", "he"), ("because", "she"), ("because", "it"), ("because", "they")
    ]
    for a, b in that_compounds:
        text = text.replace(f"{a}{b}", f"{a} {b}")
            
    # Final pass to catch any remaining issues
    text = re.sub(r'([a-z]{3,})([A-Z][a-z])', r'\1 \2', text)
    
    return text

def _fix_spacing_in_csv(file_path):
    """Fix spacing issues in a CSV file."""
    try:
        print(f"Processing file: {file_path}")
        logger.info(f"Fixing spacing issues in {file_path}")
        
        # Check if file exists
        if not os.path.exists(file_path):
            logger.error(f"File does not exist: {file_path}")
            return 0, 0
            
        # Read the CSV file
        df = pd.read_csv(file_path, encoding='utf-8')
        
        # Check if the required columns exist
        yoruba_fixed_count = 0
        if 'yoruba_text' in df.columns:
            # Apply spacing fix to all Yoruba text
            original_yoruba = df['yoruba_text'].copy()
            df['yoruba_text'] = df['yoruba_text'].apply(lambda x: _fix_yoruba_spacing(x) if isinstance(x, str) else x)
            
            # Count rows that were fixed
            yoruba_fixed_count = sum(original_yoruba != df['yoruba_text'])
            logger.info(f"Fixed {yoruba_fixed_count} Yoruba rows")
            print(f"Fixed {yoruba_fixed_count} Yoruba rows")
        else:
            logger.warning(f"No 'yoruba_text' column found in {file_path}")
        
        # Check for English text
        english_fixed_count = 0
        if 'english_text' in df.columns:
            # Apply spacing fix to all English text
            original_english = df['english_text'].copy()
            df['english_text'] = df['english_text'].apply(lambda x: _fix_english_spacing(x) if isinstance(x, str) else x)
            
            # Count rows that were fixed
            english_fixed_count = sum(original_english != df['english_text'])
            logger.info(f"Fixed {english_fixed_count} English rows")
            print(f"Fixed {english_fixed_count} English rows")
        else:
            logger.warning(f"No 'english_text' column found in {file_path}")

        # Create a backup of the original file
        backup_file = f"{file_path}.bak"
        shutil.copy2(file_path, backup_file)
        logger.info(f"Created backup at {backup_file}")
        
        # Save the updated CSV
        df.to_csv(file_path, index=False, encoding='utf-8')
        logger.info(f"Fixed spacing in {file_path}: {yoruba_fixed_count} Yoruba rows, {english_fixed_count} English rows")
        
        return yoruba_fixed_count, english_fixed_count
    except Exception as e:
        logger.error(f"Error fixing spacing in {file_path}: {e}")
        print(f"Error fixing spacing in {file_path}: {e}")
        return 0, 0

def fix_spacing_in_existing_csv(csv_file_path=None, base_folder="./scraped_data"):
    """Fix spacing issues in existing CSV files."""
    logger.info("Starting to fix spacing in CSV files")
    
    # Variables to track progress
    total_fixed_yoruba = 0
    total_fixed_english = 0
    csv_files_processed = 0
    
    # Check if a specific CSV file path is provided and exists
    if csv_file_path:
        print(f"Checking specific file: {csv_file_path}")
        if os.path.exists(csv_file_path):
            logger.info(f"Processing specific CSV file: {csv_file_path}")
            yoruba_fixed, english_fixed = _fix_spacing_in_csv(csv_file_path)
            total_fixed_yoruba += yoruba_fixed
            total_fixed_english += english_fixed
            csv_files_processed += 1
        else:
            print(f"File does not exist: {csv_file_path}")
            logger.error(f"File does not exist: {csv_file_path}")
    else:
        # If no specific file, process all CSV files in the base folder
        print(f"No specific CSV file provided, processing all CSV files in {base_folder}")
        logger.info(f"No specific CSV file provided, processing all CSV files in {base_folder}")
        
        # Make sure the base folder exists
        if not os.path.exists(base_folder):
            print(f"Base folder does not exist: {base_folder}")
            logger.error(f"Base folder does not exist: {base_folder}")
            return 0, 0, 0
            
        # Find all CSV files in the base folder and its subdirectories
        csv_files = []
        for root, _, files in os.walk(base_folder):
            for file in files:
                if file.endswith('.csv'):
                    csv_files.append(os.path.join(root, file))
        
        print(f"Found {len(csv_files)} CSV files to process")
        logger.info(f"Found {len(csv_files)} CSV files to process")
        
        # Process each CSV file
        for csv_file in csv_files:
            yoruba_fixed, english_fixed = _fix_spacing_in_csv(csv_file)
            total_fixed_yoruba += yoruba_fixed
            total_fixed_english += english_fixed
            csv_files_processed += 1
    
    logger.info(f"Finished processing {csv_files_processed} CSV files")
    logger.info(f"Total Yoruba rows fixed: {total_fixed_yoruba}")
    logger.info(f"Total English rows fixed: {total_fixed_english}")
    
    return csv_files_processed, total_fixed_yoruba, total_fixed_english

def main():
    """Main entry point of the script."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Fix spacing issues in Yoruba and English text in CSV files")
    parser.add_argument("--file", "-f", help="Path to a specific CSV file to process")
    parser.add_argument("--dir", "-d", default="./scraped_data", help="Base directory to search for CSV files")
    
    args = parser.parse_args()
    
    # Run the spacing fix
    csv_files_processed, total_fixed_yoruba, total_fixed_english = fix_spacing_in_existing_csv(
        csv_file_path=args.file, 
        base_folder=args.dir
    )
    
    print(f"Processed {csv_files_processed} CSV files")
    print(f"Fixed {total_fixed_yoruba} Yoruba rows")
    print(f"Fixed {total_fixed_english} English rows")

if __name__ == "__main__":
    main() 