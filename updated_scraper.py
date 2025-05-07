import os
import time
import json
import requests
import csv
from bs4 import BeautifulSoup
from urllib.parse import quote
import re
from concurrent.futures import ThreadPoolExecutor
import logging
import random
from tqdm import tqdm
import pandas as pd
import shutil
import traceback
import sys
from in_progress_tracker import WordTracker

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scraper_log.txt", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def captcha_detected(response_text):
    """
    Check if the response text contains a CAPTCHA message.
    Returns True if a CAPTCHA is detected; otherwise False.
    """
    captcha_keywords = [
        "Glosbe is intended to provide",
        "solve the CAPTCHA query"
    ]
    return any(keyword in response_text for keyword in captcha_keywords)

class GlosbeYorubaScraper:
    """Class for scraping Yoruba words from Glosbe"""
    
    def __init__(self, base_folder="./scraped_data"):
        """Initialize the scraper"""
        self.base_folder = base_folder
        self.debug_folder = os.path.join(base_folder, "debug_html")
        self.json_folder = os.path.join(base_folder, "json")
        self.csv_folder = os.path.join(base_folder, "csv")
        
        # Create output directories
        os.makedirs(self.debug_folder, exist_ok=True)
        os.makedirs(self.json_folder, exist_ok=True)
        os.makedirs(self.csv_folder, exist_ok=True)
        
        # For progress tracking
        self.total_words = 0
        self.processed_words = 0
        self.failed_words = 0
        
        # Initialize the tracker for in-progress words
        self.word_tracker = WordTracker(base_folder)
        
        # Initialize the HTML fetcher and extractors
        self.html_fetcher = HtmlFetcher(self.debug_folder)
        self.word_extractor = WordInfoExtractor()
        self.translation_extractor = TranslationExtractor()
        self.example_extractor = ExampleSentenceExtractor()
        
        # Set delays to avoid rate limiting
        self.min_delay = 2.0
        self.max_delay = 5.0
        
        # Load cached examples if available
        self.cached_examples = {}
        self.cached_examples_file = os.path.join(base_folder, "cached_examples.json")
        if os.path.exists(self.cached_examples_file):
            try:
                with open(self.cached_examples_file, "r", encoding="utf-8") as f:
                    self.cached_examples = json.load(f)
                logging.info(f"Loaded {len(self.cached_examples)} cached examples")
            except Exception as e:
                logging.error(f"Error loading cached examples: {str(e)}")

    def process_file(self, word_file, alphabet):
        """Process a single word file for a specific alphabet"""
        if not os.path.exists(word_file):
            logging.error(f"Word file not found: {word_file}")
            return 0
            
        # Create alphabet directory if it doesn't exist
        alphabet_folder = os.path.join(self.json_folder, alphabet)
        os.makedirs(alphabet_folder, exist_ok=True)
        
        alphabet_csv_folder = os.path.join(self.csv_folder, alphabet)
        os.makedirs(alphabet_csv_folder, exist_ok=True)
        
        # Load the words from the file
        words = []
        with open(word_file, "r", encoding="utf-8") as f:
            for line in f:
                word = line.strip()
                if word and not self.word_tracker.is_processed(word):
                    words.append(word)
                    
        # Remove duplicates
        words = list(set(words))
        logging.info(f"Found {len(words)} unique words in file")
        logging.info(f"After deduplication: {len(words)} words to process")
        
        if not words:
            logging.info(f"No new words to process in {word_file}")
            return 0
            
        # Process the words with a progress bar
        processed_count = 0
        with tqdm(total=len(words), desc=f"Processing words in {os.path.basename(word_file)}") as pbar:
            for word in words:
                try:
                    # Skip already processed words
                    if self.word_tracker.is_processed(word):
                        pbar.update(1)
                        continue
                        
                    # Mark as in-progress
                    self.word_tracker.mark_in_progress(word)
                    
                    # Scrape the word
                    result = self.scrape_word(word)
                    
                    if result["status"] == "success":
                        # Save to JSON
                        word_json_path = os.path.join(alphabet_folder, f"{word}.json")
                        with open(word_json_path, "w", encoding="utf-8") as f:
                            json.dump(result, f, indent=2, ensure_ascii=False)
                            
                        # Mark as processed
                        self.word_tracker.mark_processed(word)
                        processed_count += 1
                    else:
                        logging.error(f"Failed to scrape word: {word}, error: {result['error']}")
                        self.word_tracker.remove_in_progress(word)
                        
                except Exception as e:
                    logging.error(f"Error processing word {word}: {str(e)}")
                    traceback.print_exc()
                    self.word_tracker.remove_in_progress(word)
                    
                # Add a delay to avoid hitting rate limits
                time.sleep(random.uniform(self.min_delay, self.max_delay))
                
                pbar.update(1)
                
        # Generate CSV for this alphabet
        try:
            self.generate_alphabet_csv(alphabet)
        except Exception as e:
            logging.error(f"Error generating CSV for alphabet {alphabet}: {str(e)}")
            traceback.print_exc()
            
        return processed_count
    
    def generate_alphabet_csv(self, alphabet):
        """Generate CSV files for a specific alphabet"""
        alphabet_folder = os.path.join(self.json_folder, alphabet)
        alphabet_csv_folder = os.path.join(self.csv_folder, alphabet)
        
        if not os.path.exists(alphabet_folder):
            logging.warning(f"Alphabet folder {alphabet_folder} does not exist")
            return
            
        # Get all JSON files for this alphabet
        json_files = [os.path.join(alphabet_folder, f) for f in os.listdir(alphabet_folder) if f.endswith(".json")]
        
        if not json_files:
            logging.warning(f"No JSON files found in {alphabet_folder}")
            return
            
        # Prepare CSV files
        words_csv_path = os.path.join(alphabet_csv_folder, f"{alphabet}_words.csv")
        translations_csv_path = os.path.join(alphabet_csv_folder, f"{alphabet}_translations.csv")
        examples_csv_path = os.path.join(alphabet_csv_folder, f"{alphabet}_examples.csv")
        
        # Process each JSON file and write to CSV
        with open(words_csv_path, "w", encoding="utf-8", newline="") as words_file, \
             open(translations_csv_path, "w", encoding="utf-8", newline="") as translations_file, \
             open(examples_csv_path, "w", encoding="utf-8", newline="") as examples_file:
             
            words_writer = csv.writer(words_file)
            translations_writer = csv.writer(translations_file)
            examples_writer = csv.writer(examples_file)
            
            # Write headers
            words_writer.writerow(["word", "url", "scrape_time", "status", "error", "translation", "part_of_speech", "pronunciation"])
            translations_writer.writerow(["word", "translation", "part_of_speech", "confidence"])
            examples_writer.writerow(["word", "yoruba", "english", "source", "confidence", "is_jw_reference"])
            
            for json_file in json_files:
                try:
                    with open(json_file, "r", encoding="utf-8") as f:
                        data = json.load(f)
                        
                    # Write word data
                    words_writer.writerow([
                        data.get("word", ""),
                        data.get("url", ""),
                        data.get("scrape_time", ""),
                        data.get("status", ""),
                        data.get("error", ""),
                        data.get("translation", ""),
                        data.get("part_of_speech", ""),
                        data.get("pronunciation", "")
                    ])
                    
                    # Write translations
                    for translation in data.get("translations", []):
                        translations_writer.writerow([
                            data.get("word", ""),
                            translation.get("translation", ""),
                            translation.get("part_of_speech", ""),
                            translation.get("confidence", "")
                        ])
                        
                    # Write examples
                    for example in data.get("examples", []):
                        examples_writer.writerow([
                            data.get("word", ""),
                            example.get("yoruba", ""),
                            example.get("english", ""),
                            example.get("source", ""),
                            example.get("confidence", ""),
                            example.get("is_jw_reference", "")
                        ])
                        
                except Exception as e:
                    logging.error(f"Error processing JSON file {json_file}: {str(e)}")
                    
        logging.info(f"Generated CSV files for alphabet {alphabet}")
    
    def run(self, start_alphabet=None, end_alphabet=None):
        """Run the scraper for all words or specific alphabet range"""
        # Get word files
        word_files = self.get_word_files()
        
        if not word_files:
            logging.error("No word files found")
            return {"status": "error", "message": "No word files found"}
            
        # Filter alphabets to process if specified
        alphabets_to_process = {}
        
        if start_alphabet or end_alphabet:
            # Sort alphabets
            alphabet_keys = sorted(word_files.keys())
            processing = False if start_alphabet else True
            
            for alpha in alphabet_keys:
                # Start processing when we reach start_alphabet
                if alpha == start_alphabet:
                    processing = True
                
                # Add this alphabet if we're in processing mode
                if processing:
                    alphabets_to_process[alpha] = word_files[alpha]
                
                # Stop after processing end_alphabet
                if alpha == end_alphabet:
                    processing = False
        else:
            # Process all alphabets
            alphabets_to_process = word_files
            
        # Display which alphabets will be processed
        logging.info(f"Will process alphabets: {', '.join(sorted(alphabets_to_process.keys()))}")
            
        # Process selected alphabets
        total_processed = 0
        
        for alphabet, files in alphabets_to_process.items():
            logging.info(f"Processing alphabet: {alphabet}")
            alphabet_folder = os.path.join(self.json_folder, alphabet)
            os.makedirs(alphabet_folder, exist_ok=True)
            
            alphabet_csv_folder = os.path.join(self.csv_folder, alphabet)
            os.makedirs(alphabet_csv_folder, exist_ok=True)
            
            for word_file in files:
                processed = self.process_file(word_file, alphabet)
                total_processed += processed
                
        # Generate combined CSV
        self.generate_combined_csv()
        
        return {
            "status": "success",
            "alphabets_processed": len(alphabets_to_process),
            "total_processed": total_processed
        }

# Main function
if __name__ == "__main__":
    import argparse
    
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
    
    # Create and run the scraper
    try:
        scraper = GlosbeYorubaScraper()
        result = scraper.run(start_alphabet, end_alphabet)
        print(f"Scraper completed: {result}")
    except Exception as e:
        logging.error(f"Error running scraper: {str(e)}")
        traceback.print_exc() 