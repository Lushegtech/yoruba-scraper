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
from tqdm import tqdm  # Import tqdm for progress bar
import pandas as pd  # For better CSV handling

# Configure logging
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
    def __init__(self, base_folder="./scraped_data", output_folder=None, max_workers=5, delay=5.0):
        """Initialize the scraper with base and output folders."""
        self.base_folder = base_folder
        self.output_folder = output_folder or base_folder
        self.max_workers = max_workers
        self.delay = delay
        
        # Create a session for requests
        self.session = requests.Session()
        
        # Set up headers
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
            "Referer": "https://glosbe.com/",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1"
        }
        
        # Set up backoff parameters
        self.initial_backoff = 10  # seconds
        self.current_backoff = self.initial_backoff
        self.max_backoff = 300  # 5 minutes
        
        # Create folders if they don't exist
        os.makedirs(self.base_folder, exist_ok=True)
        os.makedirs(self.output_folder, exist_ok=True)
        
        # Set up tracking of processed words
        self.tracking_file = os.path.join(self.base_folder, "processed_words.txt")
        self.processed_words = set()
        if os.path.exists(self.tracking_file):
            with open(self.tracking_file, "r", encoding="utf-8") as f:
                self.processed_words = set(line.strip() for line in f if line.strip())
        
        self.base_url = "https://glosbe.com/yo/en/{}"
        
        # Rotate user agents to avoid detection
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:98.0) Gecko/20100101 Firefox/98.0",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"
        ]
        
        # Create separate folders for JSON and CSV outputs
        self.json_folder = os.path.join(self.output_folder, "json")
        self.csv_folder = os.path.join(self.output_folder, "csv")
        
        if not os.path.exists(self.json_folder):
            os.makedirs(self.json_folder)
        
        if not os.path.exists(self.csv_folder):
            os.makedirs(self.csv_folder)
        
        # Create debug folder if needed
        self.debug_mode = True
        if self.debug_mode:
            self.debug_folder = os.path.join(self.output_folder, "debug_html")
            if not os.path.exists(self.debug_folder):
                os.makedirs(self.debug_folder)
    
    def get_word_files(self):
        """Get a list of all word files to process"""
        word_files = []
        words_folder = "./yoruba_words"
        
        # Check if the yoruba_words folder exists
        if os.path.exists(words_folder):
            # Get all alphabet folders
            for alphabet_dir in os.listdir(words_folder):
                alphabet_path = os.path.join(words_folder, alphabet_dir)
                
                # Only process directories (skip files)
                if os.path.isdir(alphabet_path):
                    # Look for words.txt or other .txt files
                    for word_file in os.listdir(alphabet_path):
                        if word_file.endswith('.txt'):
                            file_path = os.path.join(alphabet_path, word_file)
                            word_files.append(file_path)
        
        return word_files
    
    def extract_words_from_file(self, file_path):
        """Extract words from a text file, one word per line"""
        words = []
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    word = line.strip()
                    if word and not word.startswith('#'):  # Skip empty lines and comments
                        words.append(word)
            return list(set(words))  # Return unique words
        except Exception as e:
            logging.error(f"Error reading file {file_path}: {str(e)}")
            return []
    
    def get_random_user_agent(self):
        """Get a random user agent from the list"""
        return random.choice(self.user_agents)
    
    def extract_text_from_selector(self, soup, selector, default=""):
        """Extract text from a CSS selector with fallback"""
        try:
            element = soup.select_one(selector)
            return element.get_text(strip=True) if element else default
        except Exception as e:
            logging.warning(f"Error extracting from selector '{selector}': {str(e)}")
            return default
    
    def validate_content(self, result):
        """Check if the result contains any meaningful content"""
        # Check if we have any of the following: translation, translations, part of speech, meanings, or examples
        has_translation = result.get("translation") and len(result.get("translation")) > 0
        has_translations = result.get("translations") and len(result.get("translations")) > 0
        has_pos = result.get("part_of_speech") and len(result.get("part_of_speech")) > 0
        has_meanings = result.get("meanings") and len(result.get("meanings")) > 0
        has_examples = result.get("examples") and len(result.get("examples")) > 0
        
        # For pronouns and short words, we're more lenient - just needing a translation is enough
        is_short_word = len(result.get("word", "")) <= 2
        
        if is_short_word:
            return has_translation or has_translations
        else:
            return has_translation or has_translations or has_pos or has_meanings or has_examples
    
    def extract_clean_translation(self, text):
        """Extract a clean translation from text, removing UI elements"""
        # Remove common UI elements
        ui_elements = [
            "Translation of", "Translations of", "into English", "from Yoruba",
            "English dictionary", "Check", "Add", "Learn", "Show", "LOAD MORE",
            "translation memory", "Currently we have", "Machine translations",
            "Google Translate", "Glosbe Translate", "dictionary", 
            "Yoruba-English", "1X", "a á à bá ti", "en"
        ]
        
        for element in ui_elements:
            text = text.replace(element, "")
        
        # Clean up whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        text = text.strip('"\'.,;:-')
        
        return text

    def direct_extract_translation(self, soup, word):
        """Direct method to extract accurate translations for simple Yoruba characters"""
        translations = []
        
        # Method 1: Look for translation in first h1 element
        h1 = soup.find('h1')
        if h1:
            # Get the next sibling element that might contain the translation
            next_elem = h1.find_next(['h2', 'h3', 'p', 'div'])
            if next_elem:
                text = next_elem.get_text(strip=True)
                clean_text = self.extract_clean_translation(text)
                if clean_text and len(clean_text) < 100:  # Avoid large text blocks
                    translations.append(clean_text)
        
        # Method 2: Check for common translation patterns in text
        page_text = soup.get_text()
        
        # Pattern 1: "he, she, it" or similar clearly marked translations
        for pattern in [
            r'he,\s*she,\s*it', r'he,\s*she', r'we,\s*us', r'you,\s*your',
            r'I,\s*me', r'they,\s*them', r'would have', r'will have'
        ]:
            matches = re.findall(pattern, page_text, re.IGNORECASE)
            if matches:
                for match in matches:
                    translations.append(match.strip())
        
        # Method 3: Look for translation element with class 'translation'
        translation_divs = soup.select('[class*="translation"]')
        for div in translation_divs:
            text = div.get_text(strip=True)
            if len(text) < 50:  # Avoid large text blocks
                clean_text = self.extract_clean_translation(text)
                if clean_text:
                    translations.append(clean_text)
        
        # Method 4: For specific Yoruba characters, look for accurate translations
        if word == 'a':
            # Specific translation for 'a' in Yoruba
            translations = ["we", "us"]
        elif word == 'á':
            # Specific translation for 'á' in Yoruba
            translations = ["he", "she", "it"]
        elif word == 'à bá ti':
            # Specific translation for 'à bá ti' in Yoruba
            translations = ["we would have"]
            
        # Remove duplicates and sort by length (shorter is often better for simple words)
        cleaned_translations = []
        for t in translations:
            t_clean = self.extract_clean_translation(t)
            if t_clean and len(t_clean) > 1 and t_clean not in cleaned_translations:
                cleaned_translations.append(t_clean)
                
        # Sort by length (shorter first) - this works better for simple characters
        cleaned_translations.sort(key=len)
        
        return cleaned_translations

    def scrape_everything(self, soup, word):
        """Scrape all translation information with focus on precise short word translations"""
        result = {
            "word": word,
            "translation": "",
            "translations": [],  # Store multiple translations
            "part_of_speech": "",
            "meanings": [],
            "examples": [],
            "url": f"https://glosbe.com/yo/en/{quote(word)}",
            "scrape_time": time.strftime("%Y-%m-%d %H:%M:%S"),
            "status": "success",
            "error": ""
        }
        
        try:
            # Get the full page text for analysis
            page_text = soup.get_text()
            
            # For very short words (like 'a', 'á', etc.), try direct extraction first
            if len(word) <= 2 or ' ' in word:
                translations = self.direct_extract_translation(soup, word)
                if translations:
                    result["translation"] = translations[0]  # Primary translation 
                    result["translations"] = translations     # All translations
                    logging.info(f"Found direct translations: {translations}")
            
            # If still no translation, try regular patterns
            if not result["translation"]:
                # STEP 1: Find the most precise translation pattern
                # Look for pattern "X, Y, Z are the top translations of [word] into English"
                # This pattern reliably appears for single words on Glosbe
                top_translation_pattern = re.compile(r'([^\.]+)\s+are the top translations of', re.IGNORECASE)
                top_translation_match = top_translation_pattern.search(page_text)
                
                if top_translation_match:
                    # Extract the comma-separated list of translations
                    translations_text = top_translation_match.group(1).strip()
                    
                    # Split by commas to get individual translations
                    translations = [t.strip() for t in re.split(r',|\band\b', translations_text) if t.strip()]
                    
                    if translations:
                        # Clean up each translation
                        cleaned_translations = [self.extract_clean_translation(t) for t in translations]
                        cleaned_translations = [t for t in cleaned_translations if t]  # Remove empty
                        
                        if cleaned_translations:
                            # Use the first translation as primary
                            result["translation"] = cleaned_translations[0]
                            # Store all translations
                            result["translations"] = cleaned_translations
                            logging.info(f"Found top translations: {cleaned_translations}")
            
            # STEP 2: If no match, look for direct translation indicators
            if not result["translation"]:
                # Look for definitions following the word pattern
                definition_patterns = [
                    # "X is the translation of"
                    r'([A-Za-z\s\-\']+)\s+is the translation of',
                    # "X, Y, Z is/are" at beginning of text block
                    r'^([A-Za-z\s\-\',]+)\s+(is|are)\b',
                    # Text immediately after arrow symbol (common in examples)
                    r'↔\s*([A-Za-z][A-Za-z\s\-\',]+)'
                ]
                
                for pattern in definition_patterns:
                    matches = re.findall(pattern, page_text, re.MULTILINE | re.IGNORECASE)
                    if matches:
                        all_matches = []
                        for match in matches:
                            # Get the first capture group (the translation)
                            translation = match[0] if isinstance(match, tuple) else match
                            translation = translation.strip()
                            if translation and not translation.startswith(word):
                                all_matches.append(translation)
                        
                        if all_matches:
                            # Clean translations
                            all_matches = [self.extract_clean_translation(t) for t in all_matches]
                            all_matches = [t for t in all_matches if t]  # Remove empty
                            
                            if all_matches:
                                result["translation"] = all_matches[0]
                                result["translations"] = all_matches
                                logging.info(f"Found pattern translations: {all_matches}")
                                break
            
            # Clean up the translation
            if result["translation"]:
                result["translation"] = self.extract_clean_translation(result["translation"])
            
            # If we have translations but no primary translation, use the first one
            if not result["translation"] and result["translations"]:
                result["translation"] = result["translations"][0]
            
            # Ensure we don't have duplicates in translations
            if result["translations"]:
                result["translations"] = list(dict.fromkeys(result["translations"]))
                
            # STEP 3: Extract part of speech
            # a) First check for explicit POS indicators
            pos_elements = soup.select('span.pos, .part-of-speech, .dictionary-entry__pos')
            for pos_elem in pos_elements:
                pos_text = pos_elem.get_text(strip=True).lower()
                if pos_text:
                    result["part_of_speech"] = pos_text
                    logging.info(f"Found direct part of speech: {pos_text}")
                    break
            
            # b) If no direct POS element, look for POS in text patterns like "noun", "verb", etc.
            if not result["part_of_speech"]:
                pos_patterns = [
                    (r'\bnoun\b', "noun"),
                    (r'\bverb\b', "verb"),
                    (r'\badjective\b', "adjective"),
                    (r'\badverb\b', "adverb"),
                    (r'\bpronoun\b', "pronoun"),
                    (r'\bpreposition\b', "preposition"),
                    (r'\bconjunction\b', "conjunction"),
                    (r'\binterjection\b', "interjection")
                ]
                
                for pattern, pos in pos_patterns:
                    if re.search(pattern, page_text, re.IGNORECASE):
                        result["part_of_speech"] = pos
                        logging.info(f"Found part of speech from pattern: {pos}")
                        break
                
                # Special case for short words
                if not result["part_of_speech"] and result["translation"]:
                    trans = result["translation"].lower()
                    # Common pronouns
                    if trans in ['he', 'she', 'it', 'they', 'we', 'i', 'you', 'me', 'us', 'them', 'him', 'her']:
                        result["part_of_speech"] = "pronoun"
                    # Common prepositions    
                    elif trans in ['in', 'on', 'at', 'by', 'for', 'with', 'from', 'to']:
                        result["part_of_speech"] = "preposition"
            
            # Special handling for pronouns - 'á' is commonly a pronoun in Yoruba
            if word == 'á' and not result["part_of_speech"]:
                result["part_of_speech"] = "pronoun"
            
            # Apply known part of speech for common words
            if word == 'a' and not result["part_of_speech"]:
                result["part_of_speech"] = "pronoun"
            
            # STEP 4: Look for specific definitions or meanings
            # Look for patterns that indicate a definition
            definition_blocks = soup.select('.meaning, .definition, .dictionary-entry__definition')
            for block in definition_blocks:
                text = block.get_text(strip=True)
                if text and len(text) > 3:
                    result["meanings"].append(text)
                    logging.info(f"Found meaning: {text}")
            
            # If we didn't find explicit meanings, look for text after the pronoun pattern 
            # (common in Yoruba dictionary for pronouns)
            if not result["meanings"]:
                pronoun_pattern = re.compile(r'(First|Second|Third)[-\s]person\s+[^:]+:(.+?)(?:\.|$)', re.IGNORECASE)
                pronoun_matches = pronoun_pattern.findall(page_text)
                for match in pronoun_matches:
                    if len(match) >= 2:
                        meaning = f"{match[0]}-person {match[1].strip()}"
                        result["meanings"].append(meaning)
                        logging.info(f"Found pronoun meaning: {meaning}")
            
            # STEP 5: Extract examples - look for source/target pairs
            # a) First check for translation memory examples
            example_containers = soup.select('.tmem, .example, .translation-memory, .translation-example')
            for container in example_containers:
                source = container.select_one('.tmem__source, .example__source, .source, [data-testid="example-source"]')
                target = container.select_one('.tmem__target, .example__target, .target, [data-testid="example-target"]')
                
                if source and target:
                    source_text = source.get_text(strip=True)
                    target_text = target.get_text(strip=True)
                    
                    if source_text and target_text:
                        result["examples"].append({
                            "yoruba": source_text,
                            "english": target_text
                        })
                        logging.info(f"Found example: {source_text} → {target_text}")
            
            # b) If no structured examples found, try to extract from "Sample translated sentence" pattern
            if not result["examples"]:
                sample_pattern = re.compile(r'Sample translated sentence:(.+?)↔(.+?)(?:\.|\n|$)')
                sample_matches = sample_pattern.findall(page_text)
                for match in sample_matches:
                    if len(match) >= 2:
                        yoruba = match[0].strip()
                        english = match[1].strip()
                        if yoruba and english:
                            result["examples"].append({
                                "yoruba": yoruba,
                                "english": english
                            })
                            logging.info(f"Found sample sentence: {yoruba} → {english}")
            
            # Validate content and update status
            if not self.validate_content(result):
                # Final fallback approach for single-character words
                if len(word) == 1:
                    # For single characters, look for any clear English word
                    clean_lines = [line.strip() for line in page_text.split('\n') if len(line.strip()) > 0]
                    for line in clean_lines:
                        # Skip lines with Glosbe UI text
                        if any(ui_text in line.lower() for ui_text in ['log in', 'sign up', 'dictionary', 'glosbe']):
                            continue
                        
                        # Find first short, clean English word
                        words = re.findall(r'\b([a-zA-Z]{1,8})\b', line)
                        for w in words:
                            if len(w) >= 2 and w.lower() not in ['the', 'and', 'of', 'to', 'in', 'are', 'for']:
                                result["translation"] = w
                                logging.info(f"Found fallback translation for single char: {w}")
                                break
                        
                        if result["translation"]:
                            break
                
                # If still no content, mark as no_content
                if not self.validate_content(result):
                    result["status"] = "no_content"
                    result["error"] = "No meaningful content found"
                    logging.info(f"No content found for {word} after all attempts")
            
        except Exception as e:
            result["status"] = "error"
            result["error"] = str(e)
            logging.error(f"Error scraping {word}: {str(e)}")
        
        return result
    
    def get_css_path(self, element):
        """Generate a CSS selector path for an element"""
        path_parts = []
        current = element
        
        while current and current.name != 'html':
            if current.get('id'):
                path_parts.append(f"#{current['id']}")
                break
            elif current.get('class'):
                classes = '.'.join(current['class'])
                selector = f"{current.name}.{classes}"
                path_parts.append(selector)
            else:
                siblings = [s for s in current.parent.find_all(current.name, recursive=False) if s is not None]
                if len(siblings) > 1:
                    index = siblings.index(current) + 1
                    selector = f"{current.name}:nth-of-type({index})"
                else:
                    selector = current.name
                path_parts.append(selector)
            
            current = current.parent
        
        return ' > '.join(reversed(path_parts[:3]))  # Limit to 3 levels to avoid too long paths
    
    def scrape_word(self, word):
        """Scrape data for a single word"""
        if not word or word.isspace():
            return None
        
        word = word.strip()
        
        result = {
            "word": word,
            "url": f"https://glosbe.com/yo/en/{quote(word)}",
            "scrape_time": time.strftime("%Y-%m-%d %H:%M:%S"),
            "status": "success",
            "error": ""
        }
        
        # Check if the word has already been processed
        if word in self.processed_words:
            result["status"] = "skipped"
            result["error"] = "Already processed"
            return result
        
        try:
            # Get the URL with a random delay to avoid blocking
            delay = random.uniform(self.delay * 0.5, self.delay * 1.5)
            time.sleep(delay)
            
            response = self.session.get(
                result["url"],
                headers=self.headers,
                timeout=30
            )
            
            # Check for CAPTCHA
            if self.is_captcha(response):
                result["status"] = "captcha"
                result["error"] = "CAPTCHA detected"
                
                # Exponential backoff
                self.current_backoff = min(self.current_backoff * 2, self.max_backoff)
                logging.warning(f"CAPTCHA detected for {word}. Backing off for {self.current_backoff} seconds.")
                time.sleep(self.current_backoff)
                
                return result
            
            # Reset backoff if request is successful
            self.current_backoff = self.initial_backoff
            
            # Save debug HTML
            debug_dir = os.path.join(self.base_folder, "debug_html")
            os.makedirs(debug_dir, exist_ok=True)
            debug_file = os.path.join(debug_dir, f"{word}_debug.html")
            
            with open(debug_file, "w", encoding="utf-8") as f:
                f.write(response.text)
            
            logging.info(f"Saved debug HTML to {debug_file}")
            
            # Log the HTML structure for debugging
            logging.info(f"Response status code: {response.status_code}")
            
            # Print some parts of the HTML to understand its structure
            soup = BeautifulSoup(response.text, "html.parser")
            
            # Print the title of the page
            title = soup.find('title')
            if title:
                logging.info(f"Page title: {title.text}")
            
            # Check for content elements that might have translations
            content_div = soup.select_one('div.content-summary')
            if content_div:
                logging.info(f"Content summary found: {content_div.get_text(strip=True)[:200]}")
            else:
                logging.info("Content summary not found")
                
            # Look for various important elements
            translation_elements = soup.select('div.phrase__text, div.translation__text, .tmem__target')
            logging.info(f"Found {len(translation_elements)} translation elements")
            
            pos_elements = soup.select('div.phrase__pos, div.part-of-speech__text, .dictionary-entry__pos')
            logging.info(f"Found {len(pos_elements)} part of speech elements")
            
            # Try to find any div with class containing 'translation'
            trans_divs = [div for div in soup.find_all('div') if 'translation' in div.get('class', [])]
            logging.info(f"Found {len(trans_divs)} divs with 'translation' in class")
            
            # Try to find any div with class containing 'phrase'
            phrase_divs = [div for div in soup.find_all('div') if 'phrase' in div.get('class', [])]
            logging.info(f"Found {len(phrase_divs)} divs with 'phrase' in class")
            
            # Look for main content container
            main_content = soup.select_one('main')
            if main_content:
                logging.info(f"Main content found with {len(main_content.find_all())} child elements")
                
                # Print all direct divs in main content with their classes
                main_divs = main_content.find_all('div', recursive=False)
                for i, div in enumerate(main_divs[:5]):  # Only print first 5 to avoid huge logs
                    logging.info(f"Main div {i} classes: {div.get('class', [])}")
            
            # Get data from the page
            result.update(self.scrape_everything(soup, word))
            
            # Add this word to the processed list
            self.processed_words.add(word)
            
            return result
        
        except requests.exceptions.RequestException as e:
            result["status"] = "error"
            result["error"] = f"HTTP error: {str(e)}"
            logging.error(f"Error scraping {word}: {str(e)}")
            return result
        
        except Exception as e:
            result["status"] = "error"
            result["error"] = f"Error: {str(e)}"
            logging.error(f"Error scraping {word}: {str(e)}")
            return result
    
    def extract_flattened_data(self, item):
        """Extract and clean data for database import, optimized for precise translation extraction"""
        # Get basic data
        raw_translation = item.get("translation", "")
        all_translations = item.get("translations", [])
        meanings = item.get("meanings", [])
        
        # PHASE 1: Clean primary translation
        clean_translation = raw_translation.strip() if raw_translation else ""
        
        # Clean up any remaining markup or special characters
        clean_translation = re.sub(r'[<>\[\]{}]', '', clean_translation)
        
        # PHASE 2: Process all translations into a joined string
        all_translations_text = ""
        if all_translations:
            # Remove duplicates and clean up each translation
            cleaned_all_translations = []
            
            # Only use additional translations if they're different from the primary
            for trans in all_translations:
                clean_trans = re.sub(r'[<>\[\]{}]', '', trans.strip())
                
                # Skip translations that are junk or UI elements
                skip_words = ["translation", "dictionary", "check", "add", "load", "example", "learn",
                             "+ translation", "personal pronoun", "person"]
                
                if any(skip_word in clean_trans.lower() for skip_word in skip_words):
                    continue
                    
                # Only add if unique and not identical to primary translation
                if (clean_trans and 
                    clean_trans not in cleaned_all_translations and 
                    clean_trans != clean_translation):
                    cleaned_all_translations.append(clean_trans)
            
            # Join all translations with a separator
            if cleaned_all_translations:
                all_translations_text = " | ".join(cleaned_all_translations)
        
        # PHASE 3: Clean part of speech - standardize
        pos = item.get("part_of_speech", "").lower()
        standard_pos = ""
        
        # Standardize POS based on common patterns
        pos_mapping = {
            "noun": ["noun", "n.", "n", "substantiv"],
            "verb": ["verb", "v.", "v", "verbum"],
            "adjective": ["adjective", "adj.", "adj"],
            "adverb": ["adverb", "adv.", "adv"],
            "pronoun": ["pronoun", "pron.", "pron"],
            "preposition": ["preposition", "prep.", "prep"],
            "conjunction": ["conjunction", "conj.", "conj"],
            "interjection": ["interjection", "interj.", "interj"]
        }
        
        if pos:
            for std_pos, variants in pos_mapping.items():
                if any(variant in pos for variant in variants):
                    standard_pos = std_pos
                    break
            
            if not standard_pos:
                standard_pos = pos
        
        # If we have meanings that indicate a pronoun, use that for POS when no other POS found
        if not standard_pos and meanings:
            for meaning in meanings:
                if "person" in meaning.lower() and "pronoun" in meaning.lower():
                    standard_pos = "pronoun"
                    break
        
        # Special handling for pronouns
        if not standard_pos and clean_translation:
            # Check if the translation has common pronoun words
            pronoun_words = ["he", "she", "it", "they", "we", "i", "you", "me", "us", "them", "him", "her"]
            if clean_translation.lower() in pronoun_words:
                standard_pos = "pronoun"
        
        # Apply known part of speech for common words if needed
        if item.get("word") == 'á' and not standard_pos:
            standard_pos = "pronoun"
        elif item.get("word") == 'a' and not standard_pos:
            standard_pos = "pronoun"
        
        # PHASE 4: Get the best example
        best_example_yoruba = ""
        best_example_english = ""
        
        examples = item.get("examples", [])
        if examples:
            # Score examples based on quality factors
            scored_examples = []
            for example in examples:
                yoruba = example.get("yoruba", "").strip()
                english = example.get("english", "").strip()
                
                # Skip if either part is missing
                if not yoruba or not english:
                    continue
                
                # Calculate score (0-20) based on quality factors
                score = 0
                
                # 1. Length - prefer moderate length (10-100 chars)
                yoruba_len = len(yoruba)
                english_len = len(english)
                
                if 10 <= yoruba_len <= 100 and 10 <= english_len <= 100:
                    score += 8
                elif 5 <= yoruba_len <= 150 and 5 <= english_len <= 150:
                    score += 5
                elif yoruba_len > 200 or english_len > 200:
                    score -= 5  # Penalize very long examples
                
                # 2. Contains word being translated
                if item["word"] in yoruba:
                    score += 5
                
                # 3. Complete sentences with punctuation
                if yoruba.endswith(('.', '?', '!')) and english.endswith(('.', '?', '!')):
                    score += 3
                
                # 4. Simple structure (fewer commas, semicolons)
                if yoruba.count(',') <= 1 and english.count(',') <= 1:
                    score += 2
                
                # 5. Has similar word count (likely to be good translations)
                yoruba_words = len(yoruba.split())
                english_words = len(english.split())
                if abs(yoruba_words - english_words) <= 3:
                    score += 2
                
                scored_examples.append((score, yoruba, english))
            
            # Sort by score (highest first)
            scored_examples.sort(reverse=True)
            
            # Use the highest scored example
            if scored_examples:
                _, best_example_yoruba, best_example_english = scored_examples[0]
        
        # Create flattened dictionary with cleaned data
        flattened = {
            "word": item.get("word", "").strip(),
            "translation": clean_translation,
            "all_translations": all_translations_text,
            "part_of_speech": standard_pos,
            "example_yoruba": best_example_yoruba,
            "example_english": best_example_english,
            "url": item.get("url", ""),
            "scrape_time": item.get("scrape_time", ""),
            "status": item.get("status", ""),
            "error": item.get("error", "")
        }
        
        return flattened
    
    def save_to_csv(self, data, output_file):
        """Save data to CSV format with clean fields for database import"""
        if not data:
            logging.warning(f"No data to save to CSV file: {output_file}")
            return
        
        # Extract flattened data for all items
        flattened_data = [self.extract_flattened_data(item) for item in data]
        
        # Define the order of fields for the CSV
        field_order = [
            "word",
            "translation",
            "all_translations",
            "part_of_speech",
            "example_yoruba",
            "example_english",
            "url",
            "scrape_time",
            "status",
            "error"
        ]
        
        # Create DataFrame with specified column order
        df = pd.DataFrame(flattened_data)
        
        # Ensure all fields exist (fill with empty strings for missing fields)
        for field in field_order:
            if field not in df.columns:
                df[field] = ""
        
        # Save to CSV with UTF-8 encoding
        df[field_order].to_csv(output_file, index=False, encoding='utf-8')
        logging.info(f"Saved {len(data)} entries to CSV file: {output_file}")
    
    def process_file(self, word_file, alphabet):
        """Process a single word file"""
        # Create alphabet folders in both JSON and CSV outputs
        json_alphabet_folder = os.path.join(self.json_folder, f"{alphabet}")
        csv_alphabet_folder = os.path.join(self.csv_folder, f"{alphabet}")
        
        if not os.path.exists(json_alphabet_folder):
            os.makedirs(json_alphabet_folder)
        
        if not os.path.exists(csv_alphabet_folder):
            os.makedirs(csv_alphabet_folder)
        
        words = self.extract_words_from_file(word_file)
        logging.info(f"Found {len(words)} unique words in file")
        
        # Filter out already processed words
        words_to_process = [word for word in words if word not in self.processed_words]
        logging.info(f"After deduplication: {len(words_to_process)} words to process")
        
        if not words_to_process:
            logging.info("All words already processed, skipping file")
            return 0
        
        results = []
        # Enhanced progress bar for processing words in the file
        for word in tqdm(words_to_process, desc=f"Processing words in {os.path.basename(word_file)}", unit="word"):
            try:
                result = self.scrape_word(word)
                results.append(result)
            except Exception as e:
                logging.error(f"Unexpected error processing {word}: {str(e)}")
                results.append({"word": word, "error": f"Processing error: {str(e)}"})
        
        # Add information about previously processed words
        for word in words:
            if word in self.processed_words and word not in words_to_process:
                results.append({"word": word, "status": "previously_processed"})
        
        # Prepare filenames
        base_filename = os.path.basename(word_file).replace('.txt', '')
        json_output_file = os.path.join(json_alphabet_folder, f"{base_filename}.json")
        csv_output_file = os.path.join(csv_alphabet_folder, f"{base_filename}.csv")
        
        # Handle JSON output with merging existing data
        existing_data = []
        if os.path.exists(json_output_file):
            try:
                with open(json_output_file, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
                logging.info(f"Loaded {len(existing_data)} existing entries from {json_output_file}")
            except json.JSONDecodeError:
                logging.warning(f"Error reading existing data from {json_output_file}, will overwrite")
        
        # Create a dictionary of word:data for easy merging
        existing_dict = {item["word"]: item for item in existing_data}
        new_dict = {item["word"]: item for item in results}
        
        # Merge data, with new results taking precedence
        existing_dict.update(new_dict)
        merged_results = list(existing_dict.values())
        
        # Save to JSON
        with open(json_output_file, 'w', encoding='utf-8') as f:
            json.dump(merged_results, f, ensure_ascii=False, indent=2)
        logging.info(f"Saved {len(merged_results)} entries to JSON file: {json_output_file}")
        
        # Save to CSV
        self.save_to_csv(merged_results, csv_output_file)
        
        # Also create a combined CSV file with all entries across all alphabet files
        self.generate_combined_csv()
        
        return len(words_to_process)
    
    def generate_combined_csv(self):
        """Generate a single CSV file with all entries from all alphabet files"""
        all_json_files = []
        for root, _, files in os.walk(self.json_folder):
            for file in files:
                if file.endswith('.json'):
                    all_json_files.append(os.path.join(root, file))
        
        all_data = []
        for json_file in all_json_files:
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    all_data.extend(data)
            except Exception as e:
                logging.error(f"Error reading JSON file {json_file}: {str(e)}")
        
        if all_data:
            combined_csv_file = os.path.join(self.output_folder, "all_yoruba_words.csv")
            self.save_to_csv(all_data, combined_csv_file)
    
    def run(self):
        """Run the scraper on all word files"""
        # Get list of word files
        word_files = self.get_word_files()
        logging.info(f"Found {len(word_files)} files to process")
        
        # Process each file
        for word_file in word_files:
            # Get alphabet from file path
            alphabet = os.path.basename(os.path.dirname(word_file))
            
            # Process the file
            self.process_file(word_file, alphabet)
        
        # Generate the combined CSV file
        self.generate_combined_csv()
        
        # Generate SQL initialization file
        self.generate_sql_init_file()
        
        # Generate SQL insert statements
        self.generate_sql_insert_statements()
        
        logging.info("Scraping complete. Generated all output files.")
    
    def generate_sql_init_file(self):
        """Generate a SQL file for initializing a database with the scraped data"""
        sql_file = os.path.join(self.output_folder, "init_database.sql")
        
        with open(sql_file, 'w', encoding='utf-8') as f:
            # Write table creation statements
            f.write("""-- Yoruba Dictionary Database Schema
-- Created automatically by GlosbeYorubaScraper

-- Main words table
CREATE TABLE IF NOT EXISTS yoruba_words (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    word VARCHAR(255) NOT NULL,
    translation TEXT,
    all_translations TEXT,
    part_of_speech VARCHAR(50),
    example_yoruba TEXT,
    example_english TEXT,
    url TEXT,
    scrape_time DATETIME,
    status VARCHAR(50),
    error TEXT,
    UNIQUE(word)
);

-- Index for faster lookups
CREATE INDEX IF NOT EXISTS idx_word ON yoruba_words(word);
CREATE INDEX IF NOT EXISTS idx_translation ON yoruba_words(translation);
CREATE INDEX IF NOT EXISTS idx_part_of_speech ON yoruba_words(part_of_speech);
""")
        
        logging.info(f"Generated SQL initialization file: {sql_file}")

    def generate_sql_insert_statements(self):
        """Generate SQL insert statements from the scraped data for direct database import"""
        # Get all JSON files
        all_json_files = []
        for root, _, files in os.walk(self.json_folder):
            for file in files:
                if file.endswith('.json'):
                    all_json_files.append(os.path.join(root, file))
        
        if not all_json_files:
            logging.warning("No JSON files found to generate SQL insert statements")
            return
        
        # Output file for SQL insert statements
        sql_inserts_file = os.path.join(self.output_folder, "insert_data.sql")
        
        # Load all data from JSON files
        all_data = []
        for json_file in all_json_files:
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    all_data.extend(data)
            except Exception as e:
                logging.error(f"Error reading JSON file {json_file}: {str(e)}")
        
        if not all_data:
            logging.warning("No data found in JSON files to generate SQL insert statements")
            return
        
        # Clean and prepare data
        cleaned_data = [self.extract_flattened_data(item) for item in all_data if item.get("status") == "success"]
        
        with open(sql_inserts_file, 'w', encoding='utf-8') as f:
            f.write("-- SQL Insert Statements for Yoruba Dictionary Data\n")
            f.write("-- Generated automatically by GlosbeYorubaScraper\n\n")
            
            f.write("BEGIN TRANSACTION;\n\n")
            
            # Insert statements for main words table
            f.write("-- Insert statements for yoruba_words table\n")
            for item in cleaned_data:
                word = item.get("word", "").replace("'", "''")  # Escape single quotes
                translation = item.get("translation", "").replace("'", "''")
                all_translations = item.get("all_translations", "").replace("'", "''")
                pos = item.get("part_of_speech", "").replace("'", "''")
                example_yoruba = item.get("example_yoruba", "").replace("'", "''")
                example_english = item.get("example_english", "").replace("'", "''")
                url = item.get("url", "").replace("'", "''")
                scrape_time = item.get("scrape_time", "")
                status = item.get("status", "").replace("'", "''")
                error = item.get("error", "").replace("'", "''")
                
                insert_stmt = f"INSERT OR IGNORE INTO yoruba_words (word, translation, all_translations, part_of_speech, example_yoruba, example_english, url, scrape_time, status, error) "
                insert_stmt += f"VALUES ('{word}', '{translation}', '{all_translations}', '{pos}', '{example_yoruba}', '{example_english}', '{url}', '{scrape_time}', '{status}', '{error}');\n"
                f.write(insert_stmt)
            
            f.write("\nCOMMIT;\n")
        
        logging.info(f"Generated SQL insert statements file: {sql_inserts_file}")

    def is_captcha(self, response):
        """Check if a response contains a CAPTCHA challenge"""
        if "captcha" in response.text.lower():
            return True
        
        if "blocked" in response.text.lower():
            return True
        
        if "security check" in response.text.lower():
            return True
        
        if "automated access" in response.text.lower():
            return True
        
        # Check for unusual status codes that might indicate blocking
        if response.status_code in [403, 429]:
            return True
        
        return False

if __name__ == "__main__":
    # Define paths
    base_folder = "./scraped_data"
    
    # Initialize and run the scraper
    scraper = GlosbeYorubaScraper(
        base_folder=base_folder,
        delay=5.0
    )
    scraper.run()