#!/usr/bin/env python3
"""
Fix messy translations in CSV files from the Yoruba scraper.
This script cleans up contaminated translations by:
1. Removing source URLs from translation text
2. Separating merged examples from translations
3. Fixing missing spaces between words
4. Removing duplicate content
5. Removing duplicate entries
"""

import os
import re
import pandas as pd
import logging
import shutil
from tqdm import tqdm
import argparse

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("translation_fix_log.txt", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def clean_translation(text):
    """Clean a translation by removing contaminated data"""
    if not text or not isinstance(text, str):
        return text
    
    # Extract the main translation text before any contamination
    # First try to separate by common source markers
    source_markers = ['en.wiktionary.org', 'wiki', 'pronoun', 'verb', 'noun', 'adjective', 'adverb']
    
    # Remove URLs
    text = re.sub(r'https?://\S+', '', text)
    
    # First attempt: Remove wiki source markers
    for marker in source_markers:
        if marker in text:
            parts = text.split(marker)
            text = parts[0].strip()
            break
    
    # Second attempt: Look for Yoruba text contamination
    if re.search(r'[àáèéìíòóùúẹọṣ]', text):
        # Find the Yoruba sentence by looking for diacritics
        yoruba_match = re.search(r'([A-Za-z\s]+)([àáèéìíòóùúẹọṣ])', text)
        if yoruba_match:
            # Take the part before Yoruba text starts
            text = yoruba_match.group(1).strip()
        else:
            # Try to split at first diacritic
            segments = re.split(r'([^a-zA-Z0-9\s.,;:!?-])', text)
            for i, segment in enumerate(segments):
                if re.search(r'[àáèéìíòóùúẹọṣ]', segment):
                    # We've found Yoruba text, take the part before it
                    text = ''.join(segments[:i]).strip()
                    break
    
    # Third attempt: Look for contaminated part of speech info
    if "pronoun" in text or "verb" in text or "noun" in text:
        matches = re.search(r'^([^(]+?)(?:pronoun|verb|noun|adjective|adverb)', text)
        if matches:
            text = matches.group(1).strip()
    
    # Fix word spacing issues
    # Add space between lowercase and uppercase letters
    text = re.sub(r'([a-z])([A-Z])', r'\1 \2', text)
    
    # Fix auxiliary + verb combinations
    for aux in ['would', 'could', 'should', 'will', 'have', 'has', 'had']:
        for verb in ['have', 'been', 'used', 'put', 'not', 'come', 'go']:
            text = text.replace(f"{aux}{verb}", f"{aux} {verb}")
    
    # Fix joinedwords issues
    common_patterns = [
        (r'thankswiki', r'thanks'),
        (r'would haveused', r'would have used'),
        (r'would haveÀ bá tilò óWe would haveused it', r'would have'),
        (r'First-person plural subject pronoun: we', r'we'),
        (r'First-person plural subject', r'we'),
        (r'been putto', r'been put to'),
        (r'beenput', r'been put'),
        (r'putto', r'put to'),
        (r'releasedif', r'released if'),
        (r'shewas', r'she was'),
        (r'hewas', r'he was'),
        (r'theyare', r'they are'),
        (r'hepronoun', r'he'),
        (r'shepronoun', r'she'),
        (r'theypronoun', r'they'),
    ]
    
    for pattern, replacement in common_patterns:
        text = text.replace(pattern, replacement)
    
    # Further clean up by removing anything after the main translation
    if " " in text:
        # Look for suspicious long text with examples
        if len(text) > 25:
            # Try to find the core translation based on length
            words = text.split()
            if len(words) > 4:
                # If many words, see if we can extract a clear translation phrase
                common_trans = ['we give thanks', 'we would have', 'he', 'she', 'they', 'will', 'shall']
                for trans in common_trans:
                    if text.lower().startswith(trans.lower()):
                        return trans
                
                # If we're here, we need to make a best guess on what's the real translation
                # Use the first 3-4 words or up to first punctuation
                punct_match = re.search(r'^([^.,:;!?]+)', text)
                if punct_match:
                    return punct_match.group(1).strip()
                else:
                    # Use the first few words if reasonable
                    return ' '.join(words[:min(4, len(words))]).strip()
    
    # Fix Yoruba letter contamination (common in our dataset)
    text = re.sub(r'À\s*b.*$', '', text)  # Remove "À b" and everything after it
    
    # Final cleanup
    text = re.sub(r'\s+', ' ', text).strip()
    text = re.sub(r'[.,;:!?\s]+$', '', text)
    
    return text

def clean_part_of_speech(text):
    """Clean part of speech information"""
    if not text or not isinstance(text, str):
        return text
    
    # Extract just the basic part of speech
    pos_types = ['noun', 'verb', 'pronoun', 'adjective', 'adverb', 'preposition', 'conjunction', 'interjection', 'phrase', 'auxiliary verb']
    
    text = text.lower()
    for pos in pos_types:
        if pos in text:
            return pos
    
    return text

def fix_translations_csv(file_path):
    """Fix translations CSV file by cleaning messy translations"""
    logging.info(f"Processing translations file: {file_path}")
    
    # Check if file exists
    if not os.path.exists(file_path):
        logging.warning(f"File not found: {file_path}")
        return False
    
    # Create backup
    backup_path = file_path + '.bak'
    if not os.path.exists(backup_path):
        shutil.copy(file_path, backup_path)
        logging.info(f"Created backup at {backup_path}")
    
    try:
        # Read CSV file
        logging.info(f"Reading file {file_path}")
        df = pd.read_csv(file_path, encoding='utf-8')
        
        # Log column names for debugging
        logging.info(f"Columns in file: {', '.join(df.columns.tolist())}")
        
        original_row_count = len(df)
        logging.info(f"Original row count: {original_row_count}")
        
        # Ensure required columns exist
        if 'translation' not in df.columns:
            logging.error(f"File {file_path} does not have a 'translation' column")
            logging.info(f"Available columns: {', '.join(df.columns)}")
            return False
        
        if 'word_id' not in df.columns:
            logging.warning(f"File {file_path} does not have a 'word_id' column for duplicate detection")
            has_word_id = False
        else:
            has_word_id = True
        
        # Clean translations
        original_translations = df['translation'].copy()
        df['translation'] = df['translation'].apply(clean_translation)
        
        # Log some examples of fixed translations
        for i, (orig, fixed) in enumerate(zip(original_translations.iloc[:5], df['translation'].iloc[:5])):
            if orig != fixed:
                logging.info(f"Example fix {i+1}: '{orig}' -> '{fixed}'")
        
        # Clean part of speech
        if 'part_of_speech' in df.columns:
            df['part_of_speech'] = df['part_of_speech'].apply(clean_part_of_speech)
        
        # Get count of changed translations
        changes_mask = original_translations != df['translation'].iloc[:len(original_translations)]
        translation_changes = changes_mask.sum()
        logging.info(f"Fixed {translation_changes} translations in {file_path}")
        
        # Remove duplicates (first exact duplicates)
        before_dedup = len(df)
        df = df.drop_duplicates()
        exact_dups_removed = before_dedup - len(df)
        logging.info(f"Removed {exact_dups_removed} exact duplicate rows")
        
        # Then remove duplicates based on word_id and translation
        if has_word_id:
            before_dedup_by_content = len(df)
            df = df.drop_duplicates(subset=['word_id', 'translation'], keep='first')
            content_dups_removed = before_dedup_by_content - len(df)
            logging.info(f"Removed {content_dups_removed} duplicate word/translation pairs")
        
        # Calculate total removals
        total_removed = original_row_count - len(df)
        logging.info(f"Total rows removed: {total_removed} ({len(df)} rows remaining)")
        
        # Write updated CSV file
        logging.info(f"Writing cleaned data back to {file_path}")
        df.to_csv(file_path, index=False, encoding='utf-8')
        
        return translation_changes > 0 or total_removed > 0
    
    except Exception as e:
        logging.error(f"Error processing {file_path}: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        return False

def clean_example_text(text):
    """Clean an example text from contaminants and fix spacing issues"""
    if not text or not isinstance(text, str):
        return text
    
    # Fix Yoruba spacing issues
    # Add space after common auxiliary verbs and particles
    text = re.sub(r'(À|Á|à|á|ń|kí|wọ́n)(\S)', r'\1 \2', text)
    
    # Fix incorrectly joined words
    text = re.sub(r'nià', r'ni à', text)
    text = re.sub(r'sílẹ̀ká', r'sílẹ̀ ká', text)
    
    # Ensure consistent spacing
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text

def clean_english_example(text):
    """Clean an English example text and fix spacing issues"""
    if not text or not isinstance(text, str):
        return text
    
    # Fix spacing between words
    # Add space between lowercase and uppercase letters (common in run-together words)
    text = re.sub(r'([a-z])([A-Z])', r'\1 \2', text)
    
    # Fix auxiliary verbs + past participles
    for aux in ['could', 'would', 'should', 'might', 'must', 'will', 'shall']:
        for verb in ['have', 'be', 'go', 'do']:
            text = text.replace(f"{aux}{verb}", f"{aux} {verb}")
    
    # Fix common compound words
    common_patterns = [
        (r'beenreleased', r'been released'),
        (r'havebeen', r'have been'),
        (r'beenleft', r'been left'),
        (r'beenput', r'been put'),
        (r'putto', r'put to'),
        (r'releasedif', r'released if'),
        (r'unanswered:', r'unanswered: '),
        (r'manwas', r'man was'),
        (r'mankind\'smistakes', r'mankind\'s mistakes'),
        (r'Thismancould', r'This man could'),
        (r'mancould', r'man could'),
        (r'ofmankind', r'of mankind'),
        (r'wouldreturned', r'would returned'),
        (r'wouldgone', r'would gone'),
        (r'deathfor', r'death for'),
        (r'hecould', r'he could'),
        (r'hecannot', r'he cannot'),
        (r'shecannot', r'she cannot'),
        (r'itis', r'it is'),
        (r'Godhas', r'God has'),
        (r'ifhe', r'if he'),
        (r'ifthey', r'if they'),
        (r'wasno', r'was no'),
        (r'herejoined', r'he rejoined'),
        (r'puttowork', r'put to work'),
        (r'beenputtodeath', r'been put to death'),
        (r'beenused', r'been used'),
        (r'\s+,', r','),
        (r'\s+\.', r'.'),
        (r'\s+:', r':'),
    ]
    
    for pattern, replacement in common_patterns:
        text = text.replace(pattern, replacement)
    
    # Ensure consistent spacing
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text

def fix_examples_csv(file_path):
    """Fix examples CSV file by cleaning texts"""
    logging.info(f"Processing examples file: {file_path}")
    
    # Check if file exists
    if not os.path.exists(file_path):
        logging.warning(f"File not found: {file_path}")
        return False
    
    # Create backup
    backup_path = file_path + '.bak'
    if not os.path.exists(backup_path):
        shutil.copy(file_path, backup_path)
        logging.info(f"Created backup at {backup_path}")
    
    try:
        # Open the file and check for newline issues
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Look for truncated lines
        truncated_lines = len(re.findall(r',[^,\n]*$', content.replace('\r\n', '\n')))
        if truncated_lines > 0:
            logging.warning(f"Found {truncated_lines} truncated lines in the file")
            # Try to fix truncated CSV with pandas
            content = re.sub(r',[^,\n]*$', ',""', content.replace('\r\n', '\n'))
            # Write the fixed content
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            logging.info(f"Attempted to fix truncated CSV format")
        
        # Read CSV file with explicit handling of line breaks in quoted fields
        logging.info(f"Reading file {file_path}")
        df = pd.read_csv(file_path, encoding='utf-8', lineterminator='\n', quotechar='"', escapechar='\\')
        
        # Log column names for debugging
        logging.info(f"Columns in file: {', '.join(df.columns.tolist())}")
        
        original_row_count = len(df)
        logging.info(f"Original row count: {original_row_count}")
        
        yoruba_fixes = 0
        english_fixes = 0
        
        # Ensure required columns exist
        if 'yoruba_text' not in df.columns:
            logging.warning(f"File {file_path} does not have a 'yoruba_text' column")
        else:
            # Fix any newlines in yoruba_text
            df['yoruba_text'] = df['yoruba_text'].apply(lambda x: x.replace('\n', ' ').replace('\r', '') if isinstance(x, str) else x)
            
            # Clean Yoruba examples
            original_yoruba = df['yoruba_text'].copy()
            df['yoruba_text'] = df['yoruba_text'].apply(clean_example_text)
            
            # Count Yoruba fixes
            yoruba_changes = original_yoruba != df['yoruba_text']
            yoruba_fixes = yoruba_changes.sum()
            logging.info(f"Fixed {yoruba_fixes} Yoruba examples in {file_path}")
            
            # Log some example fixes
            if yoruba_fixes > 0:
                for i, (orig, fixed) in enumerate(zip(original_yoruba[yoruba_changes][:3], df.loc[yoruba_changes, 'yoruba_text'][:3])):
                    logging.info(f"Example Yoruba fix {i+1}: '{orig}' -> '{fixed}'")
        
        if 'english_text' not in df.columns:
            logging.warning(f"File {file_path} does not have an 'english_text' column")
        else:
            # Fix any newlines in english_text
            df['english_text'] = df['english_text'].apply(lambda x: x.replace('\n', ' ').replace('\r', '') if isinstance(x, str) else x)
            
            # Clean English examples
            original_english = df['english_text'].copy()
            df['english_text'] = df['english_text'].apply(clean_english_example)
            
            # Count English fixes
            english_changes = original_english != df['english_text']
            english_fixes = english_changes.sum()
            logging.info(f"Fixed {english_fixes} English examples in {file_path}")
            
            # Log some example fixes
            if english_fixes > 0:
                for i, (orig, fixed) in enumerate(zip(original_english[english_changes][:3], df.loc[english_changes, 'english_text'][:3])):
                    logging.info(f"Example English fix {i+1}: '{orig}' -> '{fixed}'")
        
        # Remove duplicates
        before_dedup = len(df)
        df = df.drop_duplicates()
        exact_dups_removed = before_dedup - len(df)
        logging.info(f"Removed {exact_dups_removed} exact duplicate rows")
        
        # Calculate total removals
        total_removed = original_row_count - len(df)
        logging.info(f"Total rows removed: {total_removed} ({len(df)} rows remaining)")
        
        # Write updated CSV file
        logging.info(f"Writing cleaned data back to {file_path}")
        df.to_csv(file_path, index=False, encoding='utf-8', quoting=1)  # QUOTE_ALL to prevent issues
        
        return yoruba_fixes > 0 or english_fixes > 0 or total_removed > 0
    
    except Exception as e:
        logging.error(f"Error processing {file_path}: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        return False

def process_directory(directory_path):
    """Process all relevant CSV files in a directory and its subdirectories"""
    fixed_files = 0
    processed_files = 0
    
    # Find all translations CSV files
    translations_files = []
    examples_files = []
    for root, _, files in os.walk(directory_path):
        for file in files:
            if file.endswith('_translations.csv'):
                translations_files.append(os.path.join(root, file))
            elif file.endswith('_examples.csv'):
                examples_files.append(os.path.join(root, file))
    
    logging.info(f"Found {len(translations_files)} translations CSV files to process")
    logging.info(f"Found {len(examples_files)} examples CSV files to process")
    
    # Process translation files
    for file_path in tqdm(translations_files, desc="Processing translations files"):
        processed_files += 1
        if fix_translations_csv(file_path):
            fixed_files += 1
    
    # Process examples files
    for file_path in tqdm(examples_files, desc="Processing examples files"):
        processed_files += 1
        if fix_examples_csv(file_path):
            fixed_files += 1
    
    logging.info(f"Processed {processed_files} files, fixed {fixed_files} files")
    return fixed_files

def main():
    """Main entry point for the script"""
    parser = argparse.ArgumentParser(description='Fix messy translations in CSV files')
    parser.add_argument('--dir', '-d', default='scraped_data', help='Directory to process (default: scraped_data)')
    parser.add_argument('--file', '-f', help='Process a specific file instead of a directory')
    parser.add_argument('--examples', '-e', action='store_true', help='Fix examples files instead of translations')
    args = parser.parse_args()
    
    if args.file:
        logging.info(f"Processing specific file: {args.file}")
        if args.examples or args.file.endswith('_examples.csv'):
            fix_examples_csv(args.file)
        else:
            fix_translations_csv(args.file)
    else:
        logging.info(f"Processing directory: {args.dir}")
        process_directory(args.dir)
    
    logging.info("Fix process complete!")

if __name__ == "__main__":
    main() 