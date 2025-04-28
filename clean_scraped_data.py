#!/usr/bin/env python3
"""
Clean Scraped Data for Yoruba Dictionary Project
------------------------------------------------
This script provides a comprehensive solution for cleaning up messy data from web scraping.
It handles both translation CSVs and example CSVs and fixes common issues like:

1. Removing contaminated data from translations (URLs, metadata, etc.)
2. Fixing spacing issues in both Yoruba and English text
3. Removing duplicate entries
4. Cleaning part of speech information
5. Processing an entire directory of files at once

Usage:
  python clean_scraped_data.py --dir scraped_data   # Process all files in directory
  python clean_scraped_data.py --file path/to/file.csv  # Process a specific file
"""

import os
import re
import pandas as pd
import logging
import shutil
import argparse
from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("cleaning_log.txt", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

class DataCleaner:
    """Main class for cleaning scraped data files"""
    
    def __init__(self, backup=True, accuracy_check=False):
        """Initialize the cleaner"""
        self.backup = backup
        self.accuracy_check = accuracy_check
        self.stats = {
            'files_processed': 0,
            'files_changed': 0,
            'translations_fixed': 0,
            'yoruba_examples_fixed': 0,
            'english_examples_fixed': 0,
            'duplicates_removed': 0,
            'suspect_translations': 0,
            'suspect_examples': 0
        }
        self.suspect_entries = []
    
    def clean_translation(self, text):
        """Clean a translation by removing contaminated data"""
        if not text or not isinstance(text, str):
            return text
        
        # Extract the main translation text before any contamination
        original_text = text
        
        # Remove URLs
        text = re.sub(r'https?://\S+', '', text)
        
        # Remove email addresses
        text = re.sub(r'\[email protected\]', '', text)
        
        # Remove various source markers
        source_markers = [
            'proper', 'Hei NER', 'Heidelberg', 'Named Entity', 'Resource', 
            'Dbnary', 'wiki', 'adjective', 'noun', 'verb', 'conjunction', 
            'phrase', 'interjection'
        ]
        
        # First pass: Try to extract clean text before source markers
        for marker in source_markers:
            if marker in text:
                parts = text.split(marker, 1)
                text = parts[0].strip()
                break
        
        # Second pass: Remove part of speech embedded in words
        common_embedded_pos = ['adjective', 'noun', 'verb', 'conjunction', 'interjection', 'ad']
        for pos in common_embedded_pos:
            text = re.sub(f"{pos}([A-Z])", f" \\1", text)  # Separate POS + capital letter
            text = text.replace(f"{pos}", " ")  # Remove embedded POS
        
        # Remove property and description markers
        text = re.sub(r'proper\s+\w+', '', text)
        
        # Remove description text in parentheses
        text = re.sub(r'\([^)]*\)', '', text)
        
        # Remove anything after a phrase description
        if "A " in text and len(text) > 30:
            parts = text.split("A ", 1)
            text = parts[0].strip()
        
        # Remove any parts that look like metadata or identifiers
        text = re.sub(r'[A-Z][A-Za-z]+ (is|in|that|when|where|which|a|an|the|to|of|on)\s', '', text)
        
        # Fix incorrectly joined words
        # Add space between lowercase and uppercase letters
        text = re.sub(r'([a-z])([A-Z])', r'\1 \2', text)
        
        # Fix common compound words
        common_patterns = [
            ('orangeadjective', 'orange'),
            ('chiefadjective', 'chief'),
            ('bossadjective', 'boss'),
            ('accidentaladjective', 'accidental'),
            ('yesinterjection', 'yes'),
            ('asconjunction', 'as'),
            ('whetherconjunction', 'whether'),
            ('perhapsad', 'perhaps'),
            ('maybead', 'maybe'),
            ('perchancead', 'perchance'),
            ('howad', 'how'),
        ]
        
        for pattern, replacement in common_patterns:
            text = text.replace(pattern, replacement)
        
        # Clean up extra spaces and punctuation
        text = re.sub(r'\s+', ' ', text).strip()
        text = re.sub(r'[.,;:!?\s]+$', '', text)
        
        # Filter out single letter or invalid translations
        if len(text) == 1 and text in "mnlw":
            text = original_text  # Restore original if it looks suspiciously short
        
        return text
    
    def clean_part_of_speech(self, text):
        """Clean part of speech information"""
        if not text or not isinstance(text, str):
            return text
        
        # Extract just the basic part of speech
        pos_types = ['noun', 'verb', 'pronoun', 'adjective', 'adverb', 
                     'preposition', 'conjunction', 'interjection', 'phrase', 
                     'auxiliary verb']
        
        text = text.lower()
        for pos in pos_types:
            if pos in text:
                return pos
        
        return text
    
    def clean_yoruba_example(self, text):
        """Clean a Yoruba example text"""
        if not text or not isinstance(text, str):
            return text
        
        # Fix Yoruba spacing issues
        # Add space after common auxiliary verbs and particles
        text = re.sub(r'(À|Á|à|á|ń|kí|wọ́n)(\S)', r'\1 \2', text)
        
        # Fix incorrectly joined words
        text = re.sub(r'nià', r'ni à', text)
        text = re.sub(r'sílẹ̀ká', r'sílẹ̀ ká', text)
        text = re.sub(r'tifi', r'ti fi', text)
        text = re.sub(r'tiló', r'ti ló', text)
        text = re.sub(r'tilẹ̀', r'ti lẹ̀', text)
        
        # Fix newlines and ensure consistent spacing
        text = text.replace('\n', ' ').replace('\r', '')
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
    
    def clean_english_example(self, text):
        """Clean an English example text"""
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
            ('beenreleased', 'been released'),
            ('havebeen', 'have been'),
            ('beenleft', 'been left'),
            ('beenput', 'been put'),
            ('putto', 'put to'),
            ('releasedif', 'released if'),
            ('unanswered:', 'unanswered: '),
            ('manwas', 'man was'),
            ('mankind\'smistakes', 'mankind\'s mistakes'),
            ('Thismancould', 'This man could'),
            ('mancould', 'man could'),
            ('ofmankind', 'of mankind'),
            ('wouldreturned', 'would returned'),
            ('wouldgone', 'would gone'),
            ('deathfor', 'death for'),
            ('hecould', 'he could'),
            ('hecannot', 'he cannot'),
            ('shecannot', 'she cannot'),
            ('itis', 'it is'),
            ('Godhas', 'God has'),
            ('ifhe', 'if he'),
            ('ifthey', 'if they'),
            ('wasno', 'was no'),
            ('herejoined', 'he rejoined'),
            ('puttowork', 'put to work'),
            ('beenputtodeath', 'been put to death'),
            ('beenused', 'been used'),
        ]
        
        for pattern, replacement in common_patterns:
            text = text.replace(pattern, replacement)
        
        # Fix spacing around punctuation
        text = re.sub(r'\s+([,.;:!?])', r'\1', text)
        
        # Fix newlines and ensure consistent spacing
        text = text.replace('\n', ' ').replace('\r', '')
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
    
    def fix_csv_format(self, file_path):
        """Fix formatting issues in a CSV file that might cause parsing problems"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Fix header issues
            if '\"score\n' in content:
                content = content.replace('\"score\n', '\"score\"\n')
                logging.info(f"Fixed missing quote in header in {file_path}")
            
            # Fix missing commas in header
            if '"source"' in content and '"score' in content and ',"score' not in content:
                content = content.replace('"source"', '"source",')
                logging.info(f"Fixed missing comma in header in {file_path}")
            
            # Write fixed content
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            
            return True
        except Exception as e:
            logging.error(f"Error fixing CSV format: {str(e)}")
            return False
    
    def check_translation_accuracy(self, row, file_path):
        """
        Check if a translation is potentially inaccurate
        Returns True if the translation seems suspect
        """
        is_suspect = False
        reason = ""
        
        # Skip if we don't have the necessary columns
        if 'translation' not in row:
            return False
        
        translation = row['translation']
        if not isinstance(translation, str):
            return False
        
        # Check for suspiciously short translations
        if len(translation.strip()) < 2 and translation.strip() not in ["I", "a"]:
            is_suspect = True
            reason = "Very short translation"
        
        # Check for technical/database terms unlikely to be accurate translations
        tech_terms = ['null', 'undefined', 'nan', 'none', 'N/A', 'NaN']
        if translation.lower() in tech_terms:
            is_suspect = True
            reason = "Likely a database placeholder"
        
        # Check for translations that contain non-English characters (except common accents)
        non_english_pattern = r'[^\x00-\x7F\áàäâéèëêíìïîóòöôúùüûñçÁÀÄÂÉÈËÊÍÌÏÎÓÒÖÔÚÙÜÛÑÇ]'
        if re.search(non_english_pattern, translation):
            # Exclude Yoruba characters so these aren't flagged
            if not re.search(r'[àáèéìíòóùúẹọṣ]', translation):  
                is_suspect = True
                reason = "Contains non-English/non-Yoruba characters"
        
        # Check for translations that have strange formatting
        if re.search(r'^\W+$', translation) or re.search(r'_{2,}', translation):
            is_suspect = True
            reason = "Strange formatting or special characters only"
        
        # Check for capitalization patterns that suggest metadata
        if re.match(r'^[A-Z][a-z]+[A-Z]', translation):  # CamelCase likely metadata
            is_suspect = True
            reason = "CamelCase pattern suggests metadata"
        
        # Check for code-like patterns
        if re.search(r'[<>{}\[\]\\\/]', translation):
            is_suspect = True
            reason = "Contains code-like characters"
        
        # If suspect, log it
        if is_suspect and self.accuracy_check:
            word_id = row.get('word_id', 'unknown')
            confidence = row.get('confidence', 'unknown')
            entry_id = row.get('id', 'unknown')
            
            self.suspect_entries.append({
                'file': file_path,
                'id': entry_id,
                'word_id': word_id,
                'translation': translation,
                'confidence': confidence,
                'reason': reason
            })
            self.stats['suspect_translations'] += 1
        
        return is_suspect
    
    def check_example_accuracy(self, row, file_path):
        """
        Check if an example is potentially inaccurate
        Returns True if the example seems suspect
        """
        is_suspect = False
        reason = ""
        
        # Skip if we don't have the necessary columns
        if 'yoruba_text' not in row or 'english_text' not in row:
            return False
        
        yoruba = row.get('yoruba_text', '')
        english = row.get('english_text', '')
        
        if not isinstance(yoruba, str) or not isinstance(english, str):
            return False
        
        # Check for missing content in either field
        if not yoruba.strip() and english.strip():
            is_suspect = True
            reason = "Missing Yoruba text"
        elif yoruba.strip() and not english.strip():
            is_suspect = True
            reason = "Missing English text"
        
        # Check for suspiciously short content
        if len(yoruba.strip()) < 5 and yoruba.strip():
            is_suspect = True
            reason = "Very short Yoruba text"
        elif len(english.strip()) < 5 and english.strip():
            is_suspect = True
            reason = "Very short English text"
        
        # Check if English text contains Yoruba markers
        yoruba_markers = ['à', 'á', 'è', 'é', 'ì', 'í', 'ò', 'ó', 'ù', 'ú', 'ẹ', 'ọ', 'ṣ']
        if any(marker in english for marker in yoruba_markers):
            is_suspect = True
            reason = "English text contains Yoruba characters"
        
        # Check length ratio (if one is much longer than the other, might be mismatch)
        if yoruba.strip() and english.strip():
            len_ratio = len(yoruba) / len(english) if len(english) > 0 else 999
            if len_ratio > 3 or len_ratio < 0.33:
                is_suspect = True
                reason = f"Unusual length ratio ({len_ratio:.1f})"
        
        # If suspect, log it
        if is_suspect and self.accuracy_check:
            translation_id = row.get('translation_id', 'unknown')
            word_id = row.get('word_id', 'unknown')
            entry_id = row.get('id', 'unknown')
            
            self.suspect_entries.append({
                'file': file_path,
                'id': entry_id,
                'translation_id': translation_id,
                'word_id': word_id,
                'yoruba_text': yoruba,
                'english_text': english,
                'reason': reason
            })
            self.stats['suspect_examples'] += 1
        
        return is_suspect
    
    def save_suspect_entries(self, output_file="suspect_entries.csv"):
        """Save suspect entries to a CSV file for review"""
        if not self.suspect_entries:
            logging.info("No suspect entries to save")
            return
        
        # Create DataFrame from suspect entries
        df = pd.DataFrame(self.suspect_entries)
        
        # Save to CSV
        df.to_csv(output_file, index=False, encoding='utf-8')
        logging.info(f"Saved {len(self.suspect_entries)} suspect entries to {output_file}")
    
    def fix_translations_csv(self, file_path):
        """Fix a translations CSV file"""
        logging.info(f"Processing translations file: {file_path}")
        
        # Check if file exists
        if not os.path.exists(file_path):
            logging.warning(f"File not found: {file_path}")
            return False
        
        # Create backup if needed
        if self.backup:
            backup_path = file_path + '.bak'
            if not os.path.exists(backup_path):
                shutil.copy(file_path, backup_path)
                logging.info(f"Created backup at {backup_path}")
        
        # First fix any CSV format issues
        self.fix_csv_format(file_path)
        
        try:
            # Read CSV file
            logging.info(f"Reading file {file_path}")
            df = pd.read_csv(file_path, encoding='utf-8', on_bad_lines='skip')
            
            # Log column names for debugging
            logging.info(f"Columns in file: {', '.join(df.columns.tolist())}")
            
            original_row_count = len(df)
            logging.info(f"Original row count: {original_row_count}")
            
            # Ensure required columns exist
            if 'translation' not in df.columns:
                logging.error(f"File {file_path} does not have a 'translation' column")
                logging.info(f"Available columns: {', '.join(df.columns)}")
                return False
            
            # Clean translations
            original_translations = df['translation'].copy()
            df['translation'] = df['translation'].apply(self.clean_translation)
            
            # Log some examples of fixed translations
            for i, (orig, fixed) in enumerate(zip(original_translations.iloc[:3], df['translation'].iloc[:3])):
                if orig != fixed:
                    logging.info(f"Example fix {i+1}: '{orig}' -> '{fixed}'")
            
            # Clean part of speech
            if 'part_of_speech' in df.columns:
                df['part_of_speech'] = df['part_of_speech'].apply(self.clean_part_of_speech)
            
            # Get count of changed translations
            changes_mask = original_translations != df['translation']
            translation_changes = changes_mask.sum()
            logging.info(f"Fixed {translation_changes} translations in {file_path}")
            self.stats['translations_fixed'] += translation_changes
            
            # Check for accuracy issues
            if self.accuracy_check:
                logging.info(f"Checking for potentially inaccurate translations in {file_path}")
                for _, row in df.iterrows():
                    self.check_translation_accuracy(row, file_path)
            
            # Remove duplicates
            before_dedup = len(df)
            df = df.drop_duplicates()
            
            # Then remove duplicates based on word_id and translation
            if 'word_id' in df.columns:
                df = df.drop_duplicates(subset=['word_id', 'translation'], keep='first')
            
            # Calculate duplicates removed
            dups_removed = before_dedup - len(df)
            logging.info(f"Removed {dups_removed} duplicate entries in {file_path}")
            self.stats['duplicates_removed'] += dups_removed
            
            # Write updated CSV file
            logging.info(f"Writing cleaned data back to {file_path}")
            df.to_csv(file_path, index=False, encoding='utf-8')
            
            self.stats['files_processed'] += 1
            if translation_changes > 0 or dups_removed > 0:
                self.stats['files_changed'] += 1
            
            return translation_changes > 0 or dups_removed > 0
        
        except Exception as e:
            logging.error(f"Error processing {file_path}: {str(e)}")
            import traceback
            logging.error(traceback.format_exc())
            return False
    
    def fix_examples_csv(self, file_path):
        """Fix examples CSV file"""
        logging.info(f"Processing examples file: {file_path}")
        
        # Check if file exists
        if not os.path.exists(file_path):
            logging.warning(f"File not found: {file_path}")
            return False
        
        # Create backup if needed
        if self.backup:
            backup_path = file_path + '.bak'
            if not os.path.exists(backup_path):
                shutil.copy(file_path, backup_path)
                logging.info(f"Created backup at {backup_path}")
        
        # First fix any CSV format issues
        self.fix_csv_format(file_path)
        
        try:
            # Read CSV file
            logging.info(f"Reading file {file_path}")
            df = pd.read_csv(file_path, encoding='utf-8', on_bad_lines='skip')
            
            # Log column names for debugging
            logging.info(f"Columns in file: {', '.join(df.columns.tolist())}")
            
            original_row_count = len(df)
            logging.info(f"Original row count: {original_row_count}")
            
            yoruba_fixes = 0
            english_fixes = 0
            
            # Process Yoruba examples
            if 'yoruba_text' in df.columns:
                # Fix any newlines in yoruba_text
                original_yoruba = df['yoruba_text'].copy()
                df['yoruba_text'] = df['yoruba_text'].apply(self.clean_yoruba_example)
                
                # Count Yoruba fixes
                yoruba_changes = original_yoruba != df['yoruba_text']
                yoruba_fixes = yoruba_changes.sum()
                logging.info(f"Fixed {yoruba_fixes} Yoruba examples in {file_path}")
                self.stats['yoruba_examples_fixed'] += yoruba_fixes
                
                # Log some example fixes
                if yoruba_fixes > 0:
                    for i, (orig, fixed) in enumerate(zip(original_yoruba[yoruba_changes][:3], df.loc[yoruba_changes, 'yoruba_text'][:3])):
                        logging.info(f"Example Yoruba fix {i+1}: '{orig}' -> '{fixed}'")
            
            # Process English examples
            if 'english_text' in df.columns:
                # Fix any newlines in english_text
                original_english = df['english_text'].copy()
                df['english_text'] = df['english_text'].apply(self.clean_english_example)
                
                # Count English fixes
                english_changes = original_english != df['english_text']
                english_fixes = english_changes.sum()
                logging.info(f"Fixed {english_fixes} English examples in {file_path}")
                self.stats['english_examples_fixed'] += english_fixes
                
                # Log some example fixes
                if english_fixes > 0:
                    for i, (orig, fixed) in enumerate(zip(original_english[english_changes][:3], df.loc[english_changes, 'english_text'][:3])):
                        logging.info(f"Example English fix {i+1}: '{orig}' -> '{fixed}'")
            
            # Check for accuracy issues
            if self.accuracy_check:
                logging.info(f"Checking for potentially inaccurate examples in {file_path}")
                for _, row in df.iterrows():
                    self.check_example_accuracy(row, file_path)
            
            # Remove duplicates
            before_dedup = len(df)
            df = df.drop_duplicates()
            dups_removed = before_dedup - len(df)
            logging.info(f"Removed {dups_removed} duplicate rows in {file_path}")
            self.stats['duplicates_removed'] += dups_removed
            
            # Write updated CSV file
            logging.info(f"Writing cleaned data back to {file_path}")
            df.to_csv(file_path, index=False, encoding='utf-8')
            
            self.stats['files_processed'] += 1
            if yoruba_fixes > 0 or english_fixes > 0 or dups_removed > 0:
                self.stats['files_changed'] += 1
            
            return yoruba_fixes > 0 or english_fixes > 0 or dups_removed > 0
        
        except Exception as e:
            logging.error(f"Error processing {file_path}: {str(e)}")
            import traceback
            logging.error(traceback.format_exc())
            return False
    
    def process_file(self, file_path):
        """Process a single file based on its type"""
        # Determine file type based on name
        if file_path.endswith('_translations.csv'):
            return self.fix_translations_csv(file_path)
        elif file_path.endswith('_examples.csv'):
            return self.fix_examples_csv(file_path)
        else:
            logging.warning(f"Unknown file type: {file_path}")
            return False
    
    def process_directory(self, directory_path):
        """Process all CSV files in a directory and subdirectories"""
        # Find all CSV files
        csv_files = []
        for root, _, files in os.walk(directory_path):
            for file in files:
                if file.endswith('.csv') and not file.endswith('.bak'):
                    csv_files.append(os.path.join(root, file))
        
        logging.info(f"Found {len(csv_files)} CSV files to process")
        
        # Process each file
        for file_path in tqdm(csv_files, desc="Processing files"):
            self.process_file(file_path)
        
        return self.stats
    
    def report_stats(self):
        """Report the statistics of the cleaning process"""
        logging.info("=== Cleaning Summary ===")
        logging.info(f"Files processed: {self.stats['files_processed']}")
        logging.info(f"Files changed: {self.stats['files_changed']}")
        logging.info(f"Translations fixed: {self.stats['translations_fixed']}")
        logging.info(f"Yoruba examples fixed: {self.stats['yoruba_examples_fixed']}")
        logging.info(f"English examples fixed: {self.stats['english_examples_fixed']}")
        logging.info(f"Duplicates removed: {self.stats['duplicates_removed']}")
        
        if self.accuracy_check:
            logging.info(f"Suspect translations: {self.stats['suspect_translations']}")
            logging.info(f"Suspect examples: {self.stats['suspect_examples']}")
            if self.suspect_entries:
                logging.info(f"See suspect_entries.csv for details on {len(self.suspect_entries)} suspect entries")
        
        logging.info("======================")

def main():
    """Main entry point for the script"""
    parser = argparse.ArgumentParser(description='Clean up scraped data files')
    parser.add_argument('--dir', '-d', help='Directory to process (recursively)')
    parser.add_argument('--file', '-f', help='Process a specific file')
    parser.add_argument('--no-backup', action='store_true', help='Skip creating backup files')
    parser.add_argument('--check-accuracy', '-c', action='store_true', 
                        help='Check for potentially inaccurate translations')
    parser.add_argument('--output', '-o', default='suspect_entries.csv', 
                        help='Output file for suspect entries (default: suspect_entries.csv)')
    args = parser.parse_args()
    
    cleaner = DataCleaner(backup=not args.no_backup, accuracy_check=args.check_accuracy)
    
    if args.file:
        logging.info(f"Processing specific file: {args.file}")
        cleaner.process_file(args.file)
    elif args.dir:
        logging.info(f"Processing directory: {args.dir}")
        cleaner.process_directory(args.dir)
    else:
        logging.info("No file or directory specified. Using 'scraped_data' directory.")
        cleaner.process_directory('scraped_data')
    
    cleaner.report_stats()
    
    # Save suspect entries if accuracy check was enabled
    if args.check_accuracy:
        cleaner.save_suspect_entries(args.output)
    
    logging.info("Cleaning process complete!")

if __name__ == "__main__":
    main() 