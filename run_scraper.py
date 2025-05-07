#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import re
import logging
import types
import argparse

# Set up our own fix_yoruba_spacing and fix_english_spacing functions
def _fix_yoruba_spacing_impl(self, text):
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

def _fix_english_spacing_impl(self, text):
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

# The main processing function (optional)
def process_text(text):
    """Process and fix text spacing issues"""
    has_yoruba_diacritics = bool(re.search(r'[àáèéìíòóùúẹọṣ]', text))
    
    if has_yoruba_diacritics:
        # Apply Yoruba-specific fixes
        return _fix_yoruba_spacing_impl(None, text)
    else:
        # Apply English-specific fixes
        return _fix_english_spacing_impl(None, text)

# Monkey patch the ExampleSentenceExtractor class
def monkey_patch_extractor():
    """Add the fix_yoruba_spacing and fix_english_spacing methods to the ExampleSentenceExtractor class"""
    from scrape import ExampleSentenceExtractor
    
    # Check if the methods already exist
    if not hasattr(ExampleSentenceExtractor, '_fix_yoruba_spacing'):
        ExampleSentenceExtractor._fix_yoruba_spacing = _fix_yoruba_spacing_impl
        print("Added _fix_yoruba_spacing method to ExampleSentenceExtractor")
    
    if not hasattr(ExampleSentenceExtractor, '_fix_english_spacing'):
        ExampleSentenceExtractor._fix_english_spacing = _fix_english_spacing_impl
        print("Added _fix_english_spacing method to ExampleSentenceExtractor")
    
    return True

def run_scraper_with_alphabet(start_alphabet=None, end_alphabet=None):
    """Run the scraper with optional start and end alphabet filters"""
    # Add our spacing fix methods to the ExampleSentenceExtractor class
    monkey_patch_extractor()
    
    # Now import and run the scraper
    from scrape import GlosbeYorubaScraper
    
    # Create a scraper instance
    scraper = GlosbeYorubaScraper()
    
    # Get the word files 
    word_files = scraper.get_word_files()
    
    # If no alphabets specified, process all
    if not word_files:
        print("No word files found.")
        return
        
    # Filter alphabets if specified
    if start_alphabet:
        # Confirm start_alphabet exists
        if start_alphabet not in word_files:
            print(f"Start alphabet '{start_alphabet}' not found in word files.")
            print(f"Available alphabets: {', '.join(sorted(word_files.keys()))}")
            return
            
    if end_alphabet:
        # Confirm end_alphabet exists
        if end_alphabet not in word_files:
            print(f"End alphabet '{end_alphabet}' not found in word files.")
            print(f"Available alphabets: {', '.join(sorted(word_files.keys()))}")
            return
            
    # Sort alphabets to process them in order
    alphabet_keys = sorted(word_files.keys())
    
    # Filter the alphabets to process based on start and end
    filtered_alphabets = {}
    processing = False if start_alphabet else True
    
    for alpha in alphabet_keys:
        # Start processing when we reach start_alphabet
        if alpha == start_alphabet:
            processing = True
        
        # Add this alphabet if we're in processing mode
        if processing:
            filtered_alphabets[alpha] = word_files[alpha]
        
        # Stop after processing end_alphabet
        if alpha == end_alphabet:
            processing = False
            
    # Display which alphabets will be processed
    print(f"Will process alphabets: {', '.join(sorted(filtered_alphabets.keys()))}")
    
    # Now process only the selected alphabets
    total_words = 0
    total_processed = 0
    total_failed = 0
    
    for alphabet, files in filtered_alphabets.items():
        print(f"Processing alphabet: {alphabet}")
        alphabet_folder = os.path.join(scraper.json_folder, alphabet)
        os.makedirs(alphabet_folder, exist_ok=True)
        
        alphabet_csv_folder = os.path.join(scraper.csv_folder, alphabet)
        os.makedirs(alphabet_csv_folder, exist_ok=True)
        
        for word_file in files:
            result = scraper.process_file(word_file, alphabet)
            if result:
                total_words += result
                total_processed += result
                
    print(f"Completed processing {len(filtered_alphabets)} alphabets")
    print(f"Total words: {total_words}")
    print(f"Successfully processed: {total_processed}")
    print(f"Failed: {total_failed}")
    
    # Generate combined CSV
    scraper.generate_combined_csv()
    
    return {
        "total_words": total_words,
        "processed": total_processed,
        "failed": total_failed
    }

# Main function
if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Run the Yoruba Scraper with specific alphabet ranges')
    parser.add_argument('--start', help='Alphabet to start processing from (e.g., "a")')
    parser.add_argument('--end', help='Alphabet to end processing at (inclusive)')
    parser.add_argument('--alphabet', help='Process only a single alphabet')
    
    args = parser.parse_args()
    
    # If single alphabet is specified, use it for both start and end
    if args.alphabet:
        start_alphabet = args.alphabet
        end_alphabet = args.alphabet
    else:
        start_alphabet = args.start
        end_alphabet = args.end
    
    # Run the scraper with the specified alphabets
    run_scraper_with_alphabet(start_alphabet, end_alphabet) 