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

class ExampleSentenceExtractor:
    """A specialized class for extracting and verifying example sentences from Glosbe"""
    
    def __init__(self, debug=False):
        self.debug = debug
        # Common patterns that indicate example sentences
        self.example_patterns = [
            # Updated patterns for translation examples
            ('.translation__example', '.source-text', '.target-text'),
            ('.example-pair', '.source', '.target'),
            ('.translation-memory-example', '.source', '.target'),
            ('.example__content', '.source', '.target'),
            # Additional patterns for better coverage
            ('.translation__item', '.source', '.target'),
            ('.translation-list__item', '.source', '.target'),
            ('.translation__translation', '.source', '.target'),
            ('.translation-item', '.source', '.target')
        ]
        
        # Text patterns that indicate examples (when HTML structure doesn't help)
        self.text_patterns = [
            r'Example sentences with "([^"]+)"[:\s]+(.+?)↔(.+?)(?=$|\n|<)',
            r'Sample translated sentence:(.+?)↔(.+?)(?=$|\n|<)',
            r'Example:(.+?)↔(.+?)(?=$|\n|<)',
            r'Translation examples:(.+?)↔(.+?)(?=$|\n|<)',
            r'([^\.]+\.)[\s]*↔[\s]*([^\.]+\.)',
            # Additional patterns for better coverage
            r'Usage:[\s]*([^→]+)→([^$\n<]+)',
            r'Context:[\s]*([^=]+)=([^$\n<]+)',
            r'"([^"]+)"\s*translates to\s*"([^"]+)"',
            r'([^:]+):\s*\(([^)]+)\)',
            # Patterns for short words and pronouns
            r'\b([^\.]{1,50})\s*[=→↔]\s*([^\.]{1,50})',
            r'([^:]+):\s*"([^"]+)"',
            r'•\s*([^•]+)\s*•\s*([^•]+)',
            r'[\[\(]([^\[\]]+)[\]\)]\s*=\s*[\[\(]([^\[\]]+)[\]\)]'
        ]
    
        # Common Yoruba words and patterns to validate examples
        self.yoruba_markers = [
            'mo', 'o', 'ó', 'wọn', 'won', 'a', 'ẹ', 'è', 'ni',
            'kò', 'ko', 'ṣe', 'se', 'ti', 'sì', 'si', 'yìí', 'yii',
            'ń', 'n', 'kí', 'ki', 'bí', 'bi', 'fún', 'fun'
        ]
        
        # Common English patterns to validate translations
        self.english_markers = [
            'the', 'a', 'an', 'is', 'are', 'was', 'were', 'will',
            'have', 'has', 'had', 'be', 'been', 'being',
            'I', 'you', 'he', 'she', 'it', 'we', 'they',
            'this', 'that', 'these', 'those'
        ]
    
    def is_jw_reference(self, yoruba, english):
        """Check if an example is likely from JW.org (contains Bible references)"""
        jw_patterns = [
            r'\(\s*[0-9]+\s*[A-Za-z]+\s*[0-9]+:[0-9]+',  # (John 3:16)
            r'Jehovah',
            r'Kingdom Hall',
            r'Bible',
            r'Scripture',
            r'Gospel',
            r'Psalm',
            r'Verse',
            r'Chapter'
        ]
        
        for pattern in jw_patterns:
            if (re.search(pattern, yoruba, re.IGNORECASE) or 
                re.search(pattern, english, re.IGNORECASE)):
                return True
            
        return False
    
    def is_valid_example(self, yoruba, english, word):
        """Validate if extracted example pair is legitimate"""
        # Both strings must be present and non-empty
        if not yoruba or not english:
            return False
            
        # Adjust length requirements for short words and pronouns
        is_short_word = len(word) <= 2
        min_length = 1 if is_short_word else 5
        max_length = 500
        
        if len(yoruba) < min_length or len(english) < min_length:
            return False
        if len(yoruba) > max_length or len(english) > max_length:
            return False
            
        # Check for complete sentences - more lenient for short words
        has_yoruba_sentence = bool(re.search(r'[.!?]$', yoruba))
        has_english_sentence = bool(re.search(r'[.!?]$', english))
            
        # Check for HTML fragments
        if re.search(r'</?[a-z]+>', yoruba) or re.search(r'</?[a-z]+>', english):
            return False
            
        # Check for UI elements
        ui_elements = [
            'glosbe', 'log in', 'sign up', 'click', 'next page',
            'show more', 'hide', 'loading', 'search', 'menu',
            'translation', 'dictionary', 'example'
        ]
        if any(ui in yoruba.lower() for ui in ui_elements) or any(ui in english.lower() for ui in ui_elements):
            return False
            
        # Calculate confidence score (0-100)
        score = 0
        
        # Complete sentences score higher, but not required for short words
        if has_yoruba_sentence and has_english_sentence:
            score += 30
        elif (has_yoruba_sentence or has_english_sentence) and not is_short_word:
            score += 15
            
        # Contains the word being looked up - higher score for exact match
        if word.lower() in yoruba.lower():
            score += 30  # Increased from 20
            
        # Similar length ratio between Yoruba and English - more lenient for short words
        length_ratio = min(len(yoruba), len(english)) / max(len(yoruba), len(english))
        if is_short_word:
            score += int(length_ratio * 15)  # Reduced weight for short words
        else:
            score += int(length_ratio * 25)
            
        # Proper capitalization - optional for short words
        if not is_short_word and re.match(r'^[A-Z]', yoruba) and re.match(r'^[A-Z]', english):
            score += 10
            
        # Language markers present
        has_yoruba_markers = any(marker in yoruba.lower() for marker in self.yoruba_markers)
        has_english_markers = any(marker in english.lower() for marker in self.english_markers)
        
        if has_yoruba_markers:
            score += 15  # Increased from 10
        if has_english_markers:
            score += 15  # Increased from 10
            
        # Quotation marks matching
        if yoruba.count('"') == english.count('"'):
            score += 5
            
        # Contains typical sentence structures - more lenient for short words
        if re.search(r'\b(mo|o|ó|wọn|won|a)\b', yoruba.lower()):
            score += 15  # Increased from 10
            
        # Penalize for potential issues - reduced penalties for short words
        if not is_short_word:
            if len(re.findall(r'[.!?]', yoruba)) != len(re.findall(r'[.!?]', english)):
                score -= 10  # Reduced from 15
                
            if abs(yoruba.count(',') - english.count(',')) > 2:
                score -= 5  # Reduced from 10
            
        # Lower threshold for short words and pronouns
        required_score = 30 if is_short_word else 60
        return score >= required_score  # More lenient threshold for short words
    
    def clean_example_text(self, text):
        """Clean up extracted example text"""
        if not text:
            return ""
            
        # Remove excess whitespace
        text = re.sub(r'\s+', ' ', text).strip()
            
        # Remove common UI artifacts
        text = re.sub(r'(\d+/\d+|Show all|Hide)', '', text)
            
        # Remove special markers
        text = re.sub(r'(↑|↓|→|←|↔)', '', text)
            
        # Remove email addresses and URLs
        text = re.sub(r'\S+@\S+\.\S+', '', text)
        text = re.sub(r'https?://\S+', '', text)
            
        # Clean up punctuation spacing
        text = re.sub(r'\s+([.,;:!?])', r'\1', text)
        text = re.sub(r'([.,;:!?])\s+', r'\1 ', text)
        
        # Fix common encoding issues
        text = text.replace('&amp;', '&')
        text = text.replace('&quot;', '"')
        text = text.replace('&apos;', "'")
        text = text.replace('&lt;', '<')
        text = text.replace('&gt;', '>')
            
        # Remove any remaining HTML entities
        text = re.sub(r'&[a-zA-Z]+;', '', text)
        
        # Normalize quotes
        text = re.sub(r'["""]', '"', text)
        text = re.sub(r'[\u2018\u2019\']', "'", text)  # Using unicode escapes for smart quotes
        
        # Ensure proper spacing after punctuation
        text = re.sub(r'([.,!?])([A-Za-z])', r'\1 \2', text)
            
        return text.strip()
    
    def extract_examples(self, soup, word):
        """Extract example sentences using multiple techniques"""
        examples = []
        
        # Technique 1: Extract from HTML structure using selectors
        # Updated selectors based on actual HTML structure
        example_selectors = [
            '.translation__example', '.example-pair',
            '.translation-memory-example', '.example__content',
            '.dict-example',  # Additional selectors
            '.translation-example',
            '.example-item',
            '.dict-example-item',
            '.translation-memory',
            '.tmem',
            '.example',
            '[data-example]',
            '.py-2.flex',  # New selector for the actual HTML structure
            '.odd\\:bg-slate-100'  # New selector for the actual HTML structure
        ]
        
        source_selectors = [
            '.yoruba', '.source', '.example__source', '.left', '.src', '[data-source]',
            '.w-1\\/2.dir-aware-pr-1', # New selector for the actual HTML structure
            'p[lang="yo"]' # New selector for the actual HTML structure
        ]
        
        target_selectors = [
            '.english', '.target', '.example__target', '.right', '.tgt', '[data-target]',
            '.w-1\\/2.dir-aware-pl-1', # New selector for the actual HTML structure
            '.w-1\\/2.px-1.ml-2'  # New selector for the actual HTML structure
        ]
        
        # First try the standard example containers
        for selector in example_selectors:
            containers = soup.select(selector)
            for container in containers:
                yoruba = None
                english = None
                
                # Try each possible source/target selector combination
                for src_sel in source_selectors:
                    yoruba_elem = container.select_one(src_sel)
                    if yoruba_elem:
                        yoruba = yoruba_elem.get_text(strip=True)
                        if yoruba:
                            break
                
                for tgt_sel in target_selectors:
                    english_elem = container.select_one(tgt_sel)
                    if english_elem:
                        english = english_elem.get_text(strip=True)
                        if english:
                            break
                
                # If not found through selectors, try direct children
                if not yoruba or not english:
                    children = list(container.children)
                    if len(children) >= 2:
                        potential_yoruba = children[0].get_text(strip=True) if hasattr(children[0], 'get_text') else str(children[0]).strip()
                        potential_english = children[1].get_text(strip=True) if hasattr(children[1], 'get_text') else str(children[1]).strip()
                        
                        if not yoruba and potential_yoruba:
                            yoruba = potential_yoruba
                        if not english and potential_english:
                            english = potential_english
                
                if yoruba and english:
                    yoruba_text = self.clean_example_text(yoruba)
                    english_text = self.clean_example_text(english)
                    
                    if self.is_valid_example(yoruba_text, english_text, word):
                        examples.append({
                            "yoruba": yoruba_text,
                            "english": english_text,
                            "source": "html",
                            "confidence": "high",
                            "is_jw_reference": self.is_jw_reference(yoruba_text, english_text)
                        })
        
        # Technique 1b: Try a more direct approach based on actual HTML structure
        # Look for the example containers in the translation memory section
        memory_examples = soup.select('#tmem_first_examples .odd\\:bg-slate-100, #tmem_first_examples .py-2.flex')
        for example in memory_examples:
            yoruba_elem = example.select_one('.w-1\\/2.dir-aware-pr-1, p[lang="yo"]')
            english_elem = example.select_one('.w-1\\/2.dir-aware-pl-1, .w-1\\/2.px-1.ml-2')
            
            if yoruba_elem and english_elem:
                yoruba = yoruba_elem.get_text(strip=True)
                english = english_elem.get_text(strip=True)
                
                yoruba_text = self.clean_example_text(yoruba)
                english_text = self.clean_example_text(english)
                
                # Use a more lenient validation for these examples
                if yoruba_text and english_text and word.lower() in yoruba_text.lower():
                    examples.append({
                        "yoruba": yoruba_text,
                        "english": english_text,
                        "source": "tmem",
                        "confidence": "high",
                        "is_jw_reference": self.is_jw_reference(yoruba_text, english_text)
                    })
        
        # Technique 2: Extract from page text using regex patterns
        html_text = str(soup)
        
        # Patterns to find example sentences
        example_patterns = [
            r'Example:(.+?)↔(.+?)(?=$|\n|<)',
            r'([^\.]+\.)[\s]*↔[\s]*([^\.]+\.)',
            r'yo">([^<]+)</p>\s*<p[^>]*>([^<]+)</p>',
            r'<p[^>]*>([^<]+)</p>\s*<p[^>]*>([^<]+)</p>',
            r'"example__content">([^<]+)<[^>]+>([^<]+)<',
            r'"([^"]+)" ↔ "([^"]+)"',
            r'([^">]+)" → "([^<]+)',
            r'<div[^>]*>([^<]*\b' + re.escape(word) + r'\b[^<]*)</div>\s*<div[^>]*>([^<]+)</div>'
        ]
        
        for pattern in example_patterns:
            matches = re.findall(pattern, html_text, re.IGNORECASE)
            for match in matches:
                if isinstance(match, tuple) and len(match) >= 2:
                    yoruba_text = self.clean_example_text(match[0])
                    english_text = self.clean_example_text(match[1])
                    
                    # Check if this contains our word and passes validation
                    if word.lower() in yoruba_text.lower() and self.is_valid_example(yoruba_text, english_text, word):
                        examples.append({
                            "yoruba": yoruba_text,
                            "english": english_text,
                            "source": "regex",
                            "confidence": "medium",
                            "is_jw_reference": self.is_jw_reference(yoruba_text, english_text)
                        })
        
        # Technique 3: Look for adjacent text blocks with language markers
        # For short words, use specialized extraction method
        if len(word) <= 2:
            short_word_examples = self.extract_short_word_examples(soup, word)
            examples.extend(short_word_examples)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_examples = []
        for example in examples:
            key = (example["yoruba"], example["english"])
            if key not in seen:
                seen.add(key)
                unique_examples.append(example)
        
        return unique_examples
        
    def extract_short_word_examples(self, soup, word):
        """Special extraction for short words like pronouns"""
        examples = []
        
        # Direct lookup for example pairs where one contains our word
        example_pairs = []
        
        # Look for examples in the translation memory section
        memory_section = soup.select_one('#tmem_first_examples')
        if memory_section:
            items = memory_section.select('.odd\\:bg-slate-100, .px-1')
            for item in items:
                yoruba_elem = item.select_one('.w-1\\/2.dir-aware-pr-1, [lang="yo"]')
                english_elem = item.select_one('.w-1\\/2.dir-aware-pl-1, .w-1\\/2.px-1.ml-2')
                
                if yoruba_elem and english_elem:
                    yoruba = yoruba_elem.get_text(strip=True)
                    english = english_elem.get_text(strip=True)
                    
                    if word.lower() in yoruba.lower():
                        example_pairs.append((yoruba, english))
        
        # Extract from translation details with examples
        detail_cards = soup.select('[id^="translation-details-card_"]')
        for card in detail_cards:
            example_divs = card.select('.translation__example')
            for example_div in example_divs:
                yoruba_elem = example_div.select_one('[lang="yo"], .w-1\\/2.dir-aware-pr-1')
                english_elem = example_div.select_one('.w-1\\/2.px-1.ml-2, .w-1\\/2.dir-aware-pl-1')
                
                if yoruba_elem and english_elem:
                    yoruba = yoruba_elem.get_text(strip=True)
                    english = english_elem.get_text(strip=True)
                    
                    if word.lower() in yoruba.lower():
                        example_pairs.append((yoruba, english))
        
        # Add the collected example pairs
        for yoruba, english in example_pairs:
            yoruba_text = self.clean_example_text(yoruba)
            english_text = self.clean_example_text(english)
            
            # Very lenient validation for short words
            if yoruba_text and english_text and len(yoruba_text) >= 5 and len(english_text) >= 5:
                examples.append({
                    "yoruba": yoruba_text,
                    "english": english_text,
                    "source": "short_word",
                    "confidence": "medium",  # Upgraded from low
                    "is_jw_reference": self.is_jw_reference(yoruba_text, english_text)
                })
        
        # Also check the regular old way as backup
        paragraphs = soup.find_all(['p', 'div', 'span'])
        for paragraph in paragraphs:
            text = paragraph.get_text(strip=True)
            if word.lower() in text.lower():
                # Look for examples in this paragraph
                sentences = re.split(r'[.!?]', text)
                for sentence in sentences:
                    if word.lower() in sentence.lower() and len(sentence) >= 10:
                        # Try to find a translation nearby
                        next_p = paragraph.find_next_sibling(['p', 'div', 'span'])
                        if next_p:
                            next_text = next_p.get_text(strip=True)
                            # Simple heuristic - if lengths are comparable, might be a translation
                            if 0.5 <= len(next_text) / len(sentence) <= 2:
                                examples.append({
                                    "yoruba": sentence.strip(),
                                    "english": next_text.strip(),
                                    "source": "short_word",
                                    "confidence": "low",
                                    "is_jw_reference": False
                                })
        
        return examples
        
    def is_valid_example(self, yoruba, english, word):
        """Validate if extracted example pair is legitimate with more lenient criteria"""
        # Both strings must be present and non-empty
        if not yoruba or not english:
            return False
            
        # Adjust length requirements for short words and pronouns
        is_short_word = len(word) <= 2
        min_length = 1 if is_short_word else 3  # More lenient minimum length
        max_length = 1000  # Increased maximum length
        
        if len(yoruba) < min_length or len(english) < min_length:
            return False
        if len(yoruba) > max_length or len(english) > max_length:
            return False
            
        # Check for HTML fragments - serious issue
        if re.search(r'</?[a-z]+>', yoruba) or re.search(r'</?[a-z]+>', english):
            return False
            
        # Check for UI elements - serious issue
        ui_elements = [
            'glosbe', 'log in', 'sign up', 'click', 'next page',
            'show more', 'hide', 'loading', 'search', 'menu',
            'translation', 'dictionary', 'example'
        ]
        if any(ui in yoruba.lower() for ui in ui_elements) or any(ui in english.lower() for ui in ui_elements):
            return False
            
        # Calculate confidence score (0-100)
        score = 0
        
        # Contains the word being looked up - important signal
        if word.lower() in yoruba.lower():
            score += 30
            
        # Give bonus points for JW references since they're common in the data
        if self.is_jw_reference(yoruba, english):
            score += 15
            
        # Similar length ratio between Yoruba and English - more lenient for short words
        length_ratio = min(len(yoruba), len(english)) / max(len(yoruba), len(english))
        if is_short_word:
            score += int(length_ratio * 15)  # Reduced weight for short words
        else:
            score += int(length_ratio * 25)
            
        # Language markers present - important signal
        has_yoruba_markers = any(marker in yoruba.lower() for marker in self.yoruba_markers)
        has_english_markers = any(marker in english.lower() for marker in self.english_markers)
        
        if has_yoruba_markers:
            score += 20  # Increased from 15
        if has_english_markers:
            score += 20  # Increased from 15
            
        # Contains typical sentence structures - good signal for short words
        if re.search(r'\b(mo|o|ó|wọn|won|a)\b', yoruba.lower()):
            score += 15
            
        # Lower threshold to capture more examples
        required_score = 15 if is_short_word else 30  # Much more lenient than before
        
        return score >= required_score
    
    def extract_examples_by_translation(self, soup, word, translations):
        """Extract examples and try to associate them with specific translations"""
        # First get all examples
        all_examples = self.extract_examples(soup, word)
        
        # Try to match examples to translations
        examples_by_translation = {}
        for translation in translations:
            examples_by_translation[translation] = []
        
        # General examples that don't match any translation
        general_examples = []
        
        for example in all_examples:
            english = example.get("english", "").lower()
            matched = False
            
            # Try to match with translations
            for translation in translations:
                # Simple matching - if translation appears in example
                if translation.lower() in english:
                    examples_by_translation[translation].append(example)
                    matched = True
                    break
            
            # If not matched to any translation, add to general examples
            if not matched:
                general_examples.append(example)
        
        # Return both matched examples and general examples
        return {
            "by_translation": examples_by_translation,
            "general": general_examples
        }

    def verify_example_pair(self, yoruba, english):
        # Initialize score
        score = 0

        # Check for semantic similarity using a simple heuristic
        if yoruba.lower() in english.lower() or english.lower() in yoruba.lower():
            score += 20  # Direct match bonus

        # Check for common translation errors
        if len(yoruba.split()) == len(english.split()):
            score += 10  # Length match bonus

        # Use regex to check for punctuation and structure
        if re.match(r'^[A-Z]', english) and english.endswith('.'):  # Proper sentence structure
            score += 10

        # Check for common words in both sentences
        common_words = set(yoruba.lower().split()) & set(english.lower().split())
        score += len(common_words) * 2  # Bonus for each common word

        # Set a threshold for acceptance
        threshold = 40
        is_valid = score >= threshold

        return is_valid

class PostgresExporter:
    """Class for exporting data to PostgreSQL format"""
    
    def __init__(self, output_folder):
        self.output_folder = output_folder
        
    def normalize_string(self, text):
        """Normalize and escape a string for PostgreSQL use"""
        if text is None:
            return "NULL"
        
        # Escape single quotes and backslashes
        normalized = text.replace("'", "''").replace("\\", "\\\\")
        
        # Convert tabs and newlines to spaces for better formatting
        normalized = normalized.replace("\n", " ").replace("\t", " ").replace("\r", " ")
        
        # Remove any control characters
        normalized = re.sub(r'[\x00-\x1F\x7F]', '', normalized)
        
        return f"'{normalized}'"
    
    def generate_schema(self):
        """Generate PostgreSQL schema optimized for the dictionary database"""
        schema = []
        
        # Add header comment
        schema.append("-- Yoruba Dictionary Database Schema")
        schema.append("-- Generated: " + time.strftime("%Y-%m-%d %H:%M:%S"))
        schema.append("-- This schema uses numeric IDs throughout for optimal database performance")
        schema.append("")
        
        # Create words table
        schema.append("-- Words table contains the base Yoruba words")
        schema.append("CREATE TABLE IF NOT EXISTS words (")
        schema.append("    id INTEGER PRIMARY KEY,")
        schema.append("    word VARCHAR(255) NOT NULL,")
        schema.append("    url TEXT,")
        schema.append("    scrape_time TIMESTAMP,")
        schema.append("    status VARCHAR(50),")
        schema.append("    error TEXT")
        schema.append(");")
        schema.append("")
        
        # Add indexes
        schema.append("CREATE INDEX IF NOT EXISTS idx_words_word ON words (word);")
        schema.append("")
        
        # Create translations table
        schema.append("-- Translations table contains translations for each word")
        schema.append("CREATE TABLE IF NOT EXISTS translations (")
        schema.append("    id INTEGER PRIMARY KEY,")
        schema.append("    word_id INTEGER NOT NULL,")
        schema.append("    translation TEXT NOT NULL,")
        schema.append("    part_of_speech VARCHAR(100),")
        schema.append("    confidence VARCHAR(50),")
        schema.append("    FOREIGN KEY (word_id) REFERENCES words (id)")
        schema.append(");")
        schema.append("")
        
        # Add indexes
        schema.append("CREATE INDEX IF NOT EXISTS idx_translations_word_id ON translations (word_id);")
        schema.append("CREATE INDEX IF NOT EXISTS idx_translations_translation ON translations (translation);")
        schema.append("")
        
        # Create examples table
        schema.append("-- Examples table contains example sentences for translations")
        schema.append("CREATE TABLE IF NOT EXISTS examples (")
        schema.append("    id INTEGER PRIMARY KEY,")
        schema.append("    translation_id INTEGER,")
        schema.append("    word_id INTEGER NOT NULL,")
        schema.append("    yoruba_text TEXT NOT NULL,")
        schema.append("    english_text TEXT NOT NULL,")
        schema.append("    is_jw_reference BOOLEAN DEFAULT FALSE,")
        schema.append("    confidence VARCHAR(50),")
        schema.append("    source VARCHAR(100),")
        schema.append("    score INTEGER,")
        schema.append("    FOREIGN KEY (translation_id) REFERENCES translations (id),")
        schema.append("    FOREIGN KEY (word_id) REFERENCES words (id)")
        schema.append(");")
        schema.append("")
        
        # Add indexes
        schema.append("CREATE INDEX IF NOT EXISTS idx_examples_translation_id ON examples (translation_id);")
        schema.append("CREATE INDEX IF NOT EXISTS idx_examples_word_id ON examples (word_id);")
        schema.append("")
        
        # Add full-text search capabilities
        schema.append("-- Add full-text search capabilities")
        schema.append("-- These are PostgreSQL-specific extensions for text search")
        schema.append("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
        schema.append("")
        schema.append("-- Create GIN indexes for faster text search")
        schema.append("CREATE INDEX IF NOT EXISTS idx_words_trgm ON words USING GIN (word gin_trgm_ops);")
        schema.append("CREATE INDEX IF NOT EXISTS idx_translations_trgm ON translations USING GIN (translation gin_trgm_ops);")
        schema.append("CREATE INDEX IF NOT EXISTS idx_examples_yoruba_trgm ON examples USING GIN (yoruba_text gin_trgm_ops);")
        schema.append("CREATE INDEX IF NOT EXISTS idx_examples_english_trgm ON examples USING GIN (english_text gin_trgm_ops);")
        schema.append("")
        
        return "\n".join(schema)
    
    def create_insert_statements(self, all_data):
        """Generate INSERT statements for the data"""
        inserts = []
        
        # Add header
        inserts.append("-- Data Import Statements")
        inserts.append("-- Generated: " + time.strftime("%Y-%m-%d %H:%M:%S"))
        inserts.append("")
        
        # Tracks unique IDs
        word_ids = set()
        translation_ids = set()
        example_ids = set()
        
        # Process all data
        for file_data in all_data:
            # Process words
            for word in file_data.get("words", []):
                # Skip if already processed
                if word["id"] in word_ids:
                    continue
                word_ids.add(word["id"])
                
                # Generate insert statement
                inserts.append(f"INSERT INTO words (id, word, url, scrape_time, status, error) VALUES (")
                inserts.append(f"    {word['id']},")
                inserts.append(f"    {self.normalize_string(word['word'])},")
                inserts.append(f"    {self.normalize_string(word.get('url', ''))},")
                inserts.append(f"    {self.normalize_string(word.get('scrape_time', ''))},")
                inserts.append(f"    {self.normalize_string(word.get('status', ''))},")
                inserts.append(f"    {self.normalize_string(word.get('error', ''))}")
                inserts.append(");")
                inserts.append("")
            
            # Process translations
            for trans in file_data.get("translations", []):
                # Skip if already processed
                if trans["id"] in translation_ids:
                    continue
                translation_ids.add(trans["id"])
                
                # Generate insert statement
                inserts.append(f"INSERT INTO translations (id, word_id, translation, part_of_speech, confidence) VALUES (")
                inserts.append(f"    {trans['id']},")
                inserts.append(f"    {trans['word_id']},")
                inserts.append(f"    {self.normalize_string(trans['translation'])},")
                inserts.append(f"    {self.normalize_string(trans.get('part_of_speech', ''))},")
                inserts.append(f"    {self.normalize_string(trans.get('confidence', ''))}")
                inserts.append(");")
                inserts.append("")
            
            # Process examples
            for example in file_data.get("examples", []):
                # Skip if already processed
                if example["id"] in example_ids:
                    continue
                example_ids.add(example["id"])
                
                # Handle NULL for translation_id if it doesn't exist
                translation_id = "NULL"
                if example.get("translation_id") is not None:
                    translation_id = example["translation_id"]
                
                # Generate insert statement
                inserts.append(f"INSERT INTO examples (id, translation_id, word_id, yoruba_text, english_text, is_jw_reference, confidence, source, score) VALUES (")
                inserts.append(f"    {example['id']},")
                inserts.append(f"    {translation_id},")
                inserts.append(f"    {example['word_id']},")
                inserts.append(f"    {self.normalize_string(example['yoruba_text'])},")
                inserts.append(f"    {self.normalize_string(example['english_text'])},")
                inserts.append(f"    {str(example.get('is_jw_reference', False)).lower()},")
                inserts.append(f"    {self.normalize_string(example.get('confidence', ''))},")
                inserts.append(f"    {self.normalize_string(example.get('source', ''))},")
                inserts.append(f"    {example.get('score', 0)}")
                inserts.append(");")
                inserts.append("")
        
        return "\n".join(inserts)
    
    def generate_postgres_export(self, all_data):
        """Generate a complete PostgreSQL export file"""
        # Create output folder if it doesn't exist
        os.makedirs(self.output_folder, exist_ok=True)
        
        # Generate schema file
        schema_file = os.path.join(self.output_folder, "yoruba_dictionary_schema.sql")
        with open(schema_file, "w", encoding="utf-8") as f:
            f.write(self.generate_schema())
        logging.info(f"Generated PostgreSQL schema file: {schema_file}")
        
        # Generate data file
        data_file = os.path.join(self.output_folder, "yoruba_dictionary_data.sql")
        with open(data_file, "w", encoding="utf-8") as f:
            f.write(self.create_insert_statements(all_data))
        logging.info(f"Generated PostgreSQL data file: {data_file}")
        
        # Generate combined file
        combined_file = os.path.join(self.output_folder, "yoruba_dictionary_complete.sql")
        with open(combined_file, "w", encoding="utf-8") as f:
            f.write(self.generate_schema())
            f.write("\n\n-- Beginning of data import\n\n")
            f.write(self.create_insert_statements(all_data))
        logging.info(f"Generated complete PostgreSQL export file: {combined_file}")
        
        return {
            "schema_file": schema_file,
            "data_file": data_file, 
            "combined_file": combined_file
        }

class DataVerifier:
    """Enhanced class to verify the quality and accuracy of scraped data"""
    
    def __init__(self, debug=False):
        self.debug = debug
        
        # Adjust minimum required scores for verification
        self.min_scores = {
            "translation": 50,  # Lowered from 70
            "example": 40,     # Lowered from 60
            "overall": 45      # Lowered from 65
        }
        
        # Known Yoruba words and their verified translations
        self.known_words = {
            # Basic pronouns
            "a": {"translations": ["we", "us"], "pos": "pronoun"},
            "á": {"translations": ["he", "she", "it"], "pos": "pronoun"},
            "mi": {"translations": ["I", "me", "my"], "pos": "pronoun"},
            "o": {"translations": ["you"], "pos": "pronoun"},
            "ẹ": {"translations": ["you (plural)"], "pos": "pronoun"},
            "wọn": {"translations": ["they", "them"], "pos": "pronoun"},
            
            # Common phrases
            "à bá ti": {"translations": ["we would have"], "pos": "phrase"},
            "a óò": {"translations": ["we will"], "pos": "phrase"},
            "a máa": {"translations": ["we will"], "pos": "phrase"},
            "a dúpẹ́": {"translations": ["we give thanks"], "pos": "phrase"},
            "A kú ọdún àjíǹde": {"translations": ["Happy Easter"], "pos": "phrase"},
            "a gba ọ̀rọ̀ àkọsílẹ̀ dúró": {"translations": ["we accept the written word"], "pos": "phrase"},
            "a ta": {"translations": ["we sell", "we sold"], "pos": "verb"}
        }
        
        # Yoruba language markers (common words, prefixes, suffixes)
        self.yoruba_markers = {
            "characters": ["ẹ", "ọ", "ṣ", "à", "á", "è", "é", "ì", "í", "ò", "ó", "ù", "ú"],
            "pronouns": ["mo", "o", "ó", "á", "a", "ẹ", "wọn", "mi"],
            "verbs": ["ní", "ti", "kò", "ṣe", "máa", "wá", "lọ", "jẹ", "bá"],
            "particles": ["ni", "kí", "bí", "tí", "sì", "fún"]
        }
        
        # English language patterns
        self.english_patterns = {
            "pronouns": ["i", "you", "he", "she", "it", "we", "they", "me", "us", "them"],
            "articles": ["the", "a", "an"],
            "auxiliaries": ["is", "are", "was", "were", "will", "would", "have", "has", "had"],
            "prepositions": ["in", "on", "at", "to", "for", "with", "by", "from"]
        }

    def verify_yoruba_text(self, text):
        """Verify if text contains valid Yoruba language patterns"""
        if not text:
            return False, 0
            
        score = 0
        text_lower = text.lower()
        words = text_lower.split()
        
        # Check for Yoruba characters (more lenient scoring)
        for char in self.yoruba_markers["characters"]:
            if char in text_lower:
                score += 15  # Increased from 10
                break
        
        # Check for Yoruba pronouns and particles (more lenient)
        for word in words:
            if word in self.yoruba_markers["pronouns"]:
                score += 20  # Increased from 15
            elif word in self.yoruba_markers["particles"]:
                score += 15  # Increased from 10
            elif word in self.yoruba_markers["verbs"]:
                score += 15  # Increased from 10
        
        # Penalize less for English patterns
        english_words = sum(1 for w in words if w in 
                          [item for sublist in self.english_patterns.values() for item in sublist])
        if english_words > 0:
            score -= english_words * 5  # Reduced penalty from 10
        
        return score >= 40, score  # Lowered threshold from 50

    def verify_english_text(self, text):
        """Verify if text contains valid English language patterns"""
        if not text:
            return False, 0
            
        score = 0
        text_lower = text.lower()
        words = text_lower.split()
        
        # More lenient scoring for English patterns
        for category, patterns in self.english_patterns.items():
            if any(pattern in words for pattern in patterns):
                score += 20  # Increased from 15
        
        # Bonus for proper capitalization and punctuation
        if text[0].isupper():
            score += 15  # Increased from 10
        
        if re.match(r'^[A-Z].*[.!?]$', text):
            score += 20  # Increased from 15
        
        # Less penalty for Yoruba characters
        yoruba_chars = sum(1 for char in self.yoruba_markers["characters"] if char in text_lower)
        if yoruba_chars > 0:
            score -= yoruba_chars * 5  # Reduced penalty from 10
        
        return score >= 40, score  # Lowered threshold from 50

    def verify_translation_pair(self, yoruba, english):
        """Verify if a translation pair is valid"""
        if not yoruba or not english:
            return False, 0
        
        score = 0
        
        # Verify Yoruba text with more weight
        yoruba_valid, yoruba_score = self.verify_yoruba_text(yoruba)
        if yoruba_valid:
            score += yoruba_score * 0.6  # Increased from 0.5
        
        # Verify English text
        english_valid, english_score = self.verify_english_text(english)
        if english_valid:
            score += english_score * 0.4  # Decreased from 0.5
        
        # More lenient length ratio
        yoruba_words = len(yoruba.split())
        english_words = len(english.split())
        ratio = min(yoruba_words, english_words) / max(yoruba_words, english_words)
        if ratio >= 0.3:  # More lenient ratio (was 0.5)
            score += ratio * 25  # Increased from 20
        
        return score >= self.min_scores["translation"], score

    def verify_example_pair(self, yoruba, english):
        """Verify if an example sentence pair is valid"""
        if not yoruba or not english:
            return False, 0
        
        # Get word length for threshold adjustment
        word_length = len(yoruba.split()[0])  # Length of first word
        is_short_word = word_length <= 2
        
        # Check for reasonable length ratio - more lenient for short words
        yoruba_words = len(yoruba.split())
        english_words = len(english.split())
        min_ratio = 0.3 if is_short_word else 0.5
        max_ratio = 3.0 if is_short_word else 2.0
        ratio = yoruba_words / english_words
        if not (min_ratio <= ratio <= max_ratio):
            return False, 0
        
        # Initialize score
        score = 0
        
        # Check for matching sentence structure - less strict for short words
        yoruba_ends_with_punct = bool(re.search(r'[.!?]$', yoruba))
        english_ends_with_punct = bool(re.search(r'[.!?]$', english))
        if yoruba_ends_with_punct and english_ends_with_punct:
            score += 20
        elif yoruba_ends_with_punct != english_ends_with_punct and not is_short_word:
            return False, 0
        
        # Check for matching quotes and parentheses - less strict for short words
        yoruba_quotes = len(re.findall(r'["""]', yoruba))
        english_quotes = len(re.findall(r'["""]', english))
        if yoruba_quotes == english_quotes:
            score += 10
        elif yoruba_quotes != english_quotes and not is_short_word:
            return False, 0
        
        # Verify basic sentence structure - optional for short words
        if re.match(r'^[A-Z]', english):  # Should start with capital letter
            score += 15
        elif not is_short_word:
            return False, 0
        
        # Check for common noise patterns
        noise_patterns = [
            r'^\s*\d+\s*$',  # Just numbers
            r'^\s*[a-z]\)\s*$',  # Just letter markers
            r'^\s*$',  # Empty or whitespace
            r'^Yoruba$',  # UI elements
            r'^English$',
            r'^Google Translate$',
            r'^Translation$',
            r'^Example$'
        ]
        
        for pattern in noise_patterns:
            if re.match(pattern, yoruba, re.IGNORECASE) or re.match(pattern, english, re.IGNORECASE):
                return False, 0
        
        # Check for reasonable length - more lenient for short words
        min_length = 2 if is_short_word else 5
        if len(yoruba) < min_length or len(english) < min_length:
            return False, 0
        if len(yoruba) > 500 or len(english) > 500:
            return False, 0
        
        # Check for HTML or UI elements
        if re.search(r'<[^>]+>', yoruba) or re.search(r'<[^>]+>', english):
            return False, 0
        
        # Check for UI elements
        ui_elements = ['click', 'button', 'menu', 'loading', 'search']
        if any(ui in yoruba.lower() for ui in ui_elements) or any(ui in english.lower() for ui in ui_elements):
            return False, 0
        
        # Add points for natural language patterns
        yoruba_patterns = [r'\b(ni|ti|si|ko|ṣe|wa|lo)\b']
        english_patterns = [r'\b(the|a|an|is|are|was|were)\b']
        
        for pattern in yoruba_patterns:
            if re.search(pattern, yoruba.lower()):
                score += 10
        
        for pattern in english_patterns:
            if re.search(pattern, english.lower()):
                score += 10
        
        # Check length ratio is reasonable - more lenient for short words
        ratio = min(len(yoruba), len(english)) / max(len(yoruba), len(english))
        score += int(ratio * 20)
        
        # Lower threshold for short words
        required_score = 40 if is_short_word else 60
        
        # Return True if score is high enough
        return score >= required_score, score
    
    def verify_result(self, result):
        """Verify the entire scrape result for a word"""
        word = result.get("word", "")
        verified_result = {
            "word": word,
            "translation": "",
            "translations": [],
            "part_of_speech": "",
            "examples": [],
            "example_yoruba": "",  # Add these fields
            "example_english": "", # Add these fields
            "url": result.get("url", ""),
            "scrape_time": result.get("scrape_time", ""),
            "status": "success",
            "error": "",
            "verification": {
                "translation_score": 0,
                "examples_score": 0,
                "quality_score": 0
            }
        }
        
        # Check if it's a known word first
        if word in self.known_words:
            known_data = self.known_words[word]
            verified_result.update({
                "translation": known_data["translations"][0],
                "translations": known_data["translations"],
                "part_of_speech": known_data["pos"],
                "verification": {
                    "translation_score": 100,
                    "examples_score": 0,
                    "quality_score": 80
                }
            })
            return verified_result
        
        # Verify translation
        translation = result.get("translation", "")
        translations = result.get("translations", [])
        
        if translation:
            valid, score = self.verify_translation_pair(word, translation)
            if valid:
                verified_result["translation"] = translation
                verified_result["verification"]["translation_score"] = score
            
            # Verify additional translations
            verified_translations = []
            for trans in translations:
                valid, score = self.verify_translation_pair(word, trans)
                if valid and trans != translation:
                    verified_translations.append(trans)
            verified_result["translations"] = verified_translations
        
        # Verify examples
        examples = result.get("examples", [])
        verified_examples = []
        total_example_score = 0
        
        for example in examples:
            if isinstance(example, dict):
                yoruba = example.get("yoruba", "")
                english = example.get("english", "")
                valid, score = self.verify_example_pair(yoruba, english)
                
                if valid:
                    verified_examples.append({
                        "yoruba": yoruba,
                        "english": english,
                        "score": score,
                        "is_jw_reference": example.get("is_jw_reference", False)
                    })
                    total_example_score += score
        
            verified_result["examples"] = verified_examples
        
        # Set the best example as the primary example
        if verified_examples:
            best_example = max(verified_examples, key=lambda x: x["score"])
            verified_result["example_yoruba"] = best_example["yoruba"]
            verified_result["example_english"] = best_example["english"]
            verified_result["verification"]["examples_score"] = total_example_score / len(verified_examples)
        
        # Calculate overall quality score
        quality_score = (
            verified_result["verification"]["translation_score"] * 0.6 +
            verified_result["verification"]["examples_score"] * 0.4
        )
        verified_result["verification"]["quality_score"] = int(quality_score)
        
        # Update status based on quality score
        if quality_score < self.min_scores["overall"]:
            verified_result["status"] = "verification_failed"
            verified_result["error"] = f"Verification failed with quality score {int(quality_score)}"
        
        return verified_result

    def clean_example_text(self, text):
        """Clean and normalize example text."""
        if not text or len(text.strip()) < 5:
            return None
        
        # Remove HTML tags if any remain
        text = re.sub(r'<[^>]+>', '', text)
        
        # Clean up whitespace
        text = ' '.join(text.split())
        
        # Ensure proper sentence ending
        if text and not text[-1] in '.!?':
            text = text + '.'
        
        # Ensure proper capitalization
        if text and text[0].islower():
            text = text[0].upper() + text[1:]
        
        # Remove any remaining noise patterns
        noise_patterns = [
            r'\[\d+\]',  # Reference numbers
            r'\(\s*\)',  # Empty parentheses
            r'^\s*\d+\.\s*',  # Leading numbers with dots
            r'^\s*[a-z]\)\s*',  # Leading letters with parentheses
        ]
        
        for pattern in noise_patterns:
            text = re.sub(pattern, '', text)
        
        text = text.strip()
        return text if len(text) >= 5 and len(text) <= 500 else None

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
        
        # Add the example extractor and data verifier
        self.example_extractor = ExampleSentenceExtractor(debug=self.debug_mode)
        self.data_verifier = DataVerifier(debug=self.debug_mode)
        
        # Dictionary of verified Yoruba-English translations
        # This will serve as our ground truth reference
        self.verified_translations = {
            # Basic pronouns
            "a": [
                {"translation": "we", "part_of_speech": "pronoun", "confidence": "high"},
                {"translation": "us", "part_of_speech": "pronoun", "confidence": "high"}
            ],
            "á": [
                {"translation": "he", "part_of_speech": "pronoun", "confidence": "high"},
                {"translation": "she", "part_of_speech": "pronoun", "confidence": "high"},
                {"translation": "it", "part_of_speech": "pronoun", "confidence": "high"},
                {"translation": "they", "part_of_speech": "pronoun", "confidence": "high"}
            ],
            "mi": [
                {"translation": "I", "part_of_speech": "pronoun", "confidence": "high"},
                {"translation": "me", "part_of_speech": "pronoun", "confidence": "high"},
                {"translation": "my", "part_of_speech": "pronoun", "confidence": "high"}
            ],
            "o": [
                {"translation": "you", "part_of_speech": "pronoun", "confidence": "high"}
            ],
            "ẹ": [
                {"translation": "you (plural)", "part_of_speech": "pronoun", "confidence": "high"}
            ],
            "wọn": [
                {"translation": "they", "part_of_speech": "pronoun", "confidence": "high"},
                {"translation": "them", "part_of_speech": "pronoun", "confidence": "high"}
            ],
            
            # Common phrases
            "à bá ti": [
                {"translation": "we would have", "part_of_speech": "phrase", "confidence": "high"}
            ],
            "a óò": [
                {"translation": "we will", "part_of_speech": "phrase", "confidence": "high"}
            ],
            "a máa": [
                {"translation": "we will", "part_of_speech": "phrase", "confidence": "high"}
            ],
            "a dúpẹ́": [
                {"translation": "we give thanks", "part_of_speech": "phrase", "confidence": "high"}
            ],
            "A kú ọdún àjíǹde": [
                {"translation": "Happy Easter", "part_of_speech": "phrase", "confidence": "high"}
            ],
            "a gba ọ̀rọ̀ àkọsílẹ̀ dúró": [
                {"translation": "we accept the written word", "part_of_speech": "phrase", "confidence": "high"}
            ],
            "a ta": [
                {"translation": "we sell", "part_of_speech": "verb", "confidence": "high"},
                {"translation": "we sold", "part_of_speech": "verb", "confidence": "high"}
            ]
        }
    
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
        """Completely overhauled method to extract clean, accurate translations"""
        if not text or len(text.strip()) < 1:
            return ""

        # First check if this is one of those combined translation examples
        # Pattern: "we would haveÀ bá tilò óWe would haveused it"
        # Where we need only "we would have"
        mixed_pattern = r'^([a-zA-Z\s]+[a-zA-Z])[À-ÿ]'
        mixed_match = re.match(mixed_pattern, text)
        if mixed_match:
            text = mixed_match.group(1).strip()

        # Remove "personal pronoun", "plural", etc. labels that appear with pronouns
        label_prefixes = [
            "personal pronoun", "plural", "singular", "first-person", "second-person",
            "third-person", "subject pronoun", "object pronoun", "possessive pronoun"
        ]
        
        for prefix in label_prefixes:
            if text.lower().startswith(prefix):
                text = text[len(prefix):].strip()
                # Remove leading colon or whitespace
                text = re.sub(r'^[:\s]+', '', text)
        
        # Another pattern: "pronoun: we"
        # Where we need only "we"
        label_pattern = r'^[a-z\s]+[:\.]\s*(.+)$'
        label_match = re.match(label_pattern, text, re.IGNORECASE)
        if label_match:
            text = label_match.group(1).strip()

        # Remove HTML-like artifacts 
        text = re.sub(r'<[^>]+>', '', text)
        text = re.sub(r'&[a-z]+;', '', text)
        
        # Fix common patterns like "hepronoun" -> "he"
        text = re.sub(r'([a-z]+)pronoun', r'\1', text, flags=re.IGNORECASE)
        text = re.sub(r'([a-z]+)noun', r'\1', text, flags=re.IGNORECASE)
        text = re.sub(r'([a-z]+)verb', r'\1', text, flags=re.IGNORECASE)
        text = re.sub(r'([a-z]+)adjective', r'\1', text, flags=re.IGNORECASE)
        text = re.sub(r'([a-z]+)person[A-Z]', r'\1', text)
        
        # Remove specific attribution sources
        attribution_sources = [
            r'en\.wiktionary\.org',
            r'wiki$',
            r'GlosbeResearch',
            r'MicrosoftLanguagePortal',
            r'ween\.wiktionary\.org',
            r'email\s*protected',
            r'\[email.*?\]',
            r'JW\.ORG',
            r'tatoeba\.org',
            r'glosbe\.com'
        ]
        
        for source in attribution_sources:
            text = re.sub(source, '', text, flags=re.IGNORECASE)

        # Remove category/definition prefixes
        prefixes = [
            r'^(noun|verb|pronoun|adjective|adverb|preposition|conjunction|interjection)(\s+|:)',
            r'^(First-person|Second-person|Third-person)(\s+|:)',
            r'^(personal pronoun|plural|singular)(\s+|:)',
            r'^(Phrase|Expression|Numeral|Cardinal|Ordinal)(\s+|:)',
            r'^\s*"(.+?)"\s*$',  # Extract content within quotes
            r'^[a-z]+\s+of\s+(.+)$'  # "translation of X" -> "X"
        ]
        
        for prefix in prefixes:
            match = re.match(prefix, text, re.IGNORECASE)
            if match and match.group(1):
                # Extract the content after the prefix
                if prefix.endswith(r'\s*$'):  # For quote pattern
                    text = match.group(1)
                else:
                    remaining = text[match.end():].strip()
                    if remaining:  # Only use if there's something after the prefix
                        text = remaining
                break

        # Remove parentheticals which often contain explanations
        text = re.sub(r'\([^)]+\)', '', text)
        text = re.sub(r'\[[^\]]+\]', '', text)
        
        # Extract the clean translation from mixed patterns
        # Pattern: "we (translation) Example sentence in Yoruba."
        split_patterns = [
            # Translation followed by capitalized sentence
            r'^([^\.]{1,20})\s+[A-Z][^\.]+\.',
            # Translation followed by Yoruba text (contains diacritics)
            r'^([a-zA-Z\s]{1,20})\s+[À-ÿ]',
            # Word followed by parenthetical example
            r'^([^\.]{1,20})\s+\([^\)]+\)',
            # Short phrase followed by longer explanation
            r'^([a-zA-Z\s]{1,20})\s+[a-z]+\s+[a-z]+'
        ]
        
        for pattern in split_patterns:
            match = re.match(pattern, text)
            if match and match.group(1):
                candidate = match.group(1).strip()
                if len(candidate) >= 1:
                    text = candidate
                    break

        # Clean up whitespace and punctuation
        text = re.sub(r'\s+', ' ', text)
        text = text.strip('.,;:()[]{}"\' \t\n\r')

        # Final validations
        if len(text) < 1 or len(text) > 50:  # Reasonable length for translations
            return ""

        # Check if it contains Yoruba characters (likely not a clean translation)
        if re.search(r'[À-ÿ]', text):
            # Try to extract just the English part
            english_only = re.match(r'^([a-zA-Z\s]+)[À-ÿ]', text)
            if english_only:
                text = english_only.group(1).strip()
            else:
                return ""  # Reject if we can't clean it

        # Final validation for noise words
        noise_words = {'translation', 'dictionary', 'check', 'add', 'load', 'example', 'learn',
                      'click', 'more', 'hide', 'show', 'search', 'meaning', 'definition'}
        
        if text.lower() in noise_words or len(text) < 2:
            return ""

        return text.strip()
        
    def identify_part_of_speech(self, text, cleaned_text):
        """Accurately determine part of speech including phrases and numerals"""
        if not text or not cleaned_text:
            return ""
            
        # Dictionary of pos patterns with explicit markers in original text
        pos_indicators = {
            "noun": [
                r"\bnoun\b", r"\bn\.\b", r"\bname\b", r"\bobject\b", r"\bthing\b",
                r"\bthe\s+\w+\b", r"\ba\s+\w+\b", r"\ban\s+\w+\b"
            ],
            "verb": [
                r"\bverb\b", r"\bv\.\b", r"\baction\b", r"\bto\s+\w+\b",
                r"\baction word\b", r"\bdoing word\b"
            ],
            "adjective": [
                r"\badjective\b", r"\badj\.\b", r"\bdescriptive\b", r"\bquality\b",
                r"\bvery\s+\w+\b", r"\breally\s+\w+\b"
            ],
            "adverb": [
                r"\badverb\b", r"\badv\.\b", r"\bmanner\b", r"\bhow\b",
                r"\b\w+ly\b"  # Words ending in 'ly' are often adverbs
            ],
            "pronoun": [
                r"\bpronoun\b", r"\bpron\.\b", r"\bpersonal\b", 
                r"\bI\b", r"\byou\b", r"\bhe\b", r"\bshe\b", r"\bit\b", r"\bwe\b", r"\bthey\b",
                r"\bme\b", r"\bhim\b", r"\bher\b", r"\bus\b", r"\bthem\b"
            ],
            "preposition": [
                r"\bpreposition\b", r"\bprep\.\b", r"\brelation\b",
                r"\bin\b", r"\bon\b", r"\bat\b", r"\bto\b", r"\bfrom\b"
            ],
            "conjunction": [
                r"\bconjunction\b", r"\bconj\.\b", r"\bconnecting\b",
                r"\band\b", r"\bor\b", r"\bbut\b", r"\bbecause\b"
            ],
            "interjection": [
                r"\binterjection\b", r"\binterj\.\b", r"\bexclamation\b",
                r"\boh\b", r"\bah\b", r"\bwow\b", r"\bhey\b"
            ],
            "phrase": [
                r"\bphrase\b", r"\bexpression\b", r"\bidiom\b", r"\bsaying\b",
                r"\bgreeting\b", r"\bproverb\b"
            ],
            "numeral": [
                r"\bnumeral\b", r"\bnum\.\b", r"\bnumber\b", r"\bcardinal\b", r"\bordinal\b",
                r"\bfirst\b", r"\bsecond\b", r"\bthird\b", r"\bone\b", r"\btwo\b", r"\bthree\b"
            ]
        }
        
        # First check for explicit POS indicators in original text
        for pos, patterns in pos_indicators.items():
            for pattern in patterns:
                if re.search(pattern, text, re.IGNORECASE):
                    return pos
                    
        # Next, analyze the cleaned translation text
        words = cleaned_text.lower().split()
        
        # Quick check for common pronouns
        common_pronouns = {"i", "me", "my", "mine", "you", "your", "yours", "he", "him", "his", 
                         "she", "her", "hers", "it", "its", "we", "us", "our", "ours", 
                         "they", "them", "their", "theirs", "this", "that", "these", "those"}
        
        if len(words) == 1 and words[0] in common_pronouns:
            return "pronoun"
            
        # Check for verb phrases - often start with "to"
        if len(words) > 1 and words[0] == "to" and words[1] not in {"the", "a", "an"}:
            return "verb"
            
        # Check for common word endings
        if len(words) == 1:
            word = words[0]
            # Common noun endings
            if word.endswith(("ness", "ity", "tion", "ment", "hood", "ship", "dom")):
                return "noun"
                
            # Common adjective endings
            if word.endswith(("ful", "ous", "ive", "al", "ic", "ible", "able", "ish", "less")):
                return "adjective"
                
            # Common adverb endings
            if word.endswith("ly") and not word.endswith("ply"):  # 'supply' isn't an adverb
                return "adverb"
                
        # Check for number words and ordinals - could be numerals
        numeral_words = {"one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten",
                       "eleven", "twelve", "thirteen", "fourteen", "fifteen", "sixteen", "seventeen",
                       "eighteen", "nineteen", "twenty", "thirty", "forty", "fifty", "hundred", "thousand",
                       "million", "billion", "first", "second", "third", "fourth", "fifth"}
                       
        if any(word in numeral_words for word in words):
            return "numeral"
            
        # Check for common verb forms
        common_verbs = {"make", "do", "have", "get", "take", "give", "find", "think", "see", "want", 
                       "come", "look", "use", "tell", "ask", "work", "seem", "feel", "try", "leave",
                       "call", "is", "are", "was", "were", "be", "been", "being", "go", "going"}
                       
        if len(words) == 1 and words[0] in common_verbs:
            return "verb"
            
        # Check for phrases - usually multiple words not caught by other rules
        if len(words) > 2:
            return "phrase"
            
        # Default case - if it has an article, likely a noun phrase
        if any(word in {"a", "an", "the"} for word in words):
            return "noun"
            
        # For short phrases of 2 words
        if len(words) == 2:
            # Adjective + Noun pattern
            if any(words[0].endswith(suffix) for suffix in ["ful", "ous", "ive", "al", "ic", "ed"]):
                return "phrase"
                
            # Default to phrase for any 2+ word combination not caught above
            return "phrase"
            
        # When all else fails, default to noun (most common case)
        return "noun"
        
    def scrape_word(self, word):
        """Scrape a single word from Glosbe."""
        try:
            # Get the page content
            url = self.base_url.format(quote(word))
            response = self.session.get(url, headers=self.headers)
            response.raise_for_status()
            
            # Parse with BeautifulSoup
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Save debug HTML if in debug mode
            if self.debug_mode:
                debug_dir = os.path.join(self.base_folder, "debug_html")
                os.makedirs(debug_dir, exist_ok=True)
                debug_file = os.path.join(debug_dir, f"{word.replace(' ', '_')}_debug.html")
                with open(debug_file, "w", encoding="utf-8") as f:
                    f.write(str(soup))
                logging.info(f"Saved debug HTML to {debug_file}")
            
            # Scrape everything from the page
            result = self.scrape_everything(word)
            
            # Ensure we return a dictionary
            if not isinstance(result, dict):
                return {
                    "word": word,
                    "status": "error",
                    "error": "Invalid result format",
                    "scrape_time": time.strftime("%Y-%m-%d %H:%M:%S")
                }
            
            return result
        
        except requests.exceptions.RequestException as e:
            logging.error(f"HTTP error scraping {word}: {str(e)}")
            return {
                "word": word,
                "status": "error",
                "error": f"HTTP error: {str(e)}",
                "scrape_time": time.strftime("%Y-%m-%d %H:%M:%S")
            }
        except Exception as e:
            logging.error(f"Error scraping {word}: {str(e)}")
            return {
                "word": word,
                "status": "error",
                "error": f"Error: {str(e)}",
                "scrape_time": time.strftime("%Y-%m-%d %H:%M:%S")
            }

    def scrape_everything(self, word):
        """Scrape translations and examples for a word"""
        result = {
            "word": word,
            "translations": [],
            "examples": [],
            "error": None,
            "url": f"https://glosbe.com/yo/en/{word}",
            "scrape_time": time.strftime("%Y-%m-%d %H:%M:%S"),
            "status": "success"
        }
        
        try:
            # FIRST: Check if we have verified translations for this word
            # If so, use them as our primary source
            if word in self.verified_translations:
                # Start with our verified translations - these are the most accurate
                translations = self.verified_translations[word]
                
                # Now try to get examples from the webpage
                
                # Get the webpage content for examples
                url = f"https://glosbe.com/yo/en/{word}"
                response = requests.get(url)
                response.raise_for_status()
                
                # Save debug HTML
                os.makedirs(self.debug_folder, exist_ok=True)
                safe_word = re.sub(r'[\\/:*?"<>|]', '_', word)  # Handle special characters
                debug_file = os.path.join(self.debug_folder, f"{safe_word}_debug.html")
                with open(debug_file, "w", encoding="utf-8") as f:
                    f.write(response.text)
                logging.info(f"Saved debug HTML to {debug_file}")
                
                # Parse HTML to get examples
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Extract examples only - we already have reliable translations
                all_examples = self.example_extractor.extract_examples(soup, word)
                result["examples"] = all_examples
                
                # Associate examples with our verified translations
                if all_examples and translations:
                    for example in all_examples:
                        english_text = example.get("english", "").lower()
                        matched = False
                        
                        for trans in translations:
                            translation_text = trans.get("translation", "").lower()
                            # Match if translation appears as a whole word in the example
                            if translation_text and (
                                re.search(r'\b' + re.escape(translation_text) + r'\b', english_text) or
                                # For phrase translations, allow partial matches
                                (len(translation_text.split()) > 1 and translation_text in english_text)
                            ):
                                # Create a copy of the example to avoid modifying the original
                                example_copy = example.copy()
                                if "examples" not in trans:
                                    trans["examples"] = []
                                trans["examples"].append(example_copy)
                                # Mark that this example is associated with a translation
                                example["associated_translation"] = translation_text
                                matched = True
                
                # Set the translations
                result["translations"] = translations
                
                # Find the best example if no associations were made
                if all_examples and not any(trans.get("examples") for trans in translations):
                    # Find the best example to use as the primary example
                    best_example = max(all_examples, key=lambda x: 
                        "high" if x.get("confidence") == "high" else 
                        "medium" if x.get("confidence") == "medium" else "low")
                    
                    # Add as primary example fields
                    result["example_yoruba"] = best_example.get("yoruba", "")
                    result["example_english"] = best_example.get("english", "")
                
                return result
            
            # If we don't have verified translations, proceed with regular extraction
            
            # Get the webpage content
            url = f"https://glosbe.com/yo/en/{word}"
            response = requests.get(url)
            response.raise_for_status()
            
            # Save debug HTML
            os.makedirs(self.debug_folder, exist_ok=True)
            safe_word = re.sub(r'[\\/:*?"<>|]', '_', word)  # Handle special characters
            debug_file = os.path.join(self.debug_folder, f"{safe_word}_debug.html")
            with open(debug_file, "w", encoding="utf-8") as f:
                f.write(response.text)
            logging.info(f"Saved debug HTML to {debug_file}")
            
            # Parse HTML
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract translations using multiple approaches for better accuracy
            translation_candidates = []
            
            # Approach 1: Direct translation elements
            translation_selectors = [
                '.translation__item',
                '.translation-list__item',
                '.translation__translation',
                '.translation-item',
                '.dict-item',
                '.translation-list > div',
                '.translation > div',
                '[data-translation]',
                '.translation-list .translation-item',
                '.dictionary-results .translation',
                '.translation--Lw0 .translation__translation--1hBZ',
                '.translation--Lw0 .translation__phrase--8lzX',
                '.translation-list .phrase',
                '.translation-list .text',
                '.translation-list .meaning',
                '.dictionary-meaning',
                '.dictionary-item',
                '.translation__item div',
                '.translation div',
                '.meanings li',
                '.meanings div',
                '.meaning__translation'
            ]
            
            for selector in translation_selectors:
                elements = soup.select(selector)
                for element in elements:
                    # Get the text and any potential part of speech indicator
                    trans_text = element.get_text(strip=True)
                    
                    # Look for explicit part of speech markers in parent or siblings
                    pos_indicator = ""
                    # Check parent element for POS indicator
                    parent = element.parent
                    if parent:
                        parent_text = parent.get_text()
                        pos_indicator = self.extract_pos_from_text(parent_text)
                    
                    # Check previous sibling for POS indicator
                    prev_sib = element.find_previous_sibling()
                    if prev_sib and not pos_indicator:
                        prev_text = prev_sib.get_text()
                        pos_indicator = self.extract_pos_from_text(prev_text)
                    
                    translation_candidates.append({
                        "text": trans_text,
                        "pos_hint": pos_indicator,
                        "source": "direct",
                        "confidence": "high"
                    })
            
            # Approach 2: Look for patterns in text that suggest translations
            # This is especially useful for pages that don't use the standard structure
            translation_patterns = [
                r'translated as "([^"]+)"',
                r'translated to "([^"]+)"',
                r'meaning is "([^"]+)"',
                r'meaning: "([^"]+)"',
                r'Translation:\s*(.+?)(?=\n|$)',
                r'Translate:\s*(.+?)(?=\n|$)',
                r'In English:\s*(.+?)(?=\n|$)',
                r'means "([^"]+)"',
                r'Definition:\s*(.+?)(?=\n|$)',
                r'Definition of[^:]*:\s*(.+?)(?=\n|$)',
                r'English:\s*(.+?)(?=\n|$)'
            ]
            
            page_text = soup.get_text()
            for pattern in translation_patterns:
                matches = re.finditer(pattern, page_text, re.IGNORECASE)
                for match in matches:
                    if match.group(1):
                        translation_candidates.append({
                            "text": match.group(1),
                            "pos_hint": "",
                            "source": "pattern",
                            "confidence": "medium"
                        })
            
            # Approach 3: Look for the first English word or phrase after the Yoruba word in titles/headings
            headings = soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'title'])
            for heading in headings:
                heading_text = heading.get_text(strip=True)
                if word in heading_text:
                    # Try to find English translation part after the Yoruba word
                    parts = heading_text.split(word, 1)
                    if len(parts) > 1 and parts[1]:
                        # Clean up any separators
                        english_part = re.sub(r'^[:\-–—\s]+', '', parts[1]).strip()
                        if english_part:
                            translation_candidates.append({
                                "text": english_part,
                                "pos_hint": "",
                                "source": "heading",
                                "confidence": "high"
                            })
            
            # Process all candidates to extract clean translations
            seen_translations = set()
            translations = []
            
            for candidate in translation_candidates:
                if not candidate["text"]:
                    continue
                    
                clean_trans = self.extract_clean_translation(candidate["text"])
                
                # Skip empty or too-short translations and duplicates
                if not clean_trans or clean_trans.lower() in seen_translations or len(clean_trans) < 2:
                    continue
                
                # Identify part of speech with the improved method
                pos = self.identify_part_of_speech(candidate["text"], clean_trans)
                
                # Use the POS hint if available and no POS was determined
                if not pos and candidate["pos_hint"]:
                    pos = candidate["pos_hint"]
                
                seen_translations.add(clean_trans.lower())
                translations.append({
                    "translation": clean_trans,
                    "part_of_speech": pos,
                    "confidence": candidate["confidence"],
                    "examples": []
                })
            
            # Add known translations for short words/pronouns if we don't have many
            if len(word) <= 2 or len(translations) < 2:
                known_translations = self.get_known_translations(word)
                if known_translations:
                    for known_trans in known_translations:
                        translation_text = known_trans.get("translation", "")
                        if translation_text and translation_text.lower() not in seen_translations:
                            seen_translations.add(translation_text.lower())
                            translations.append({
                                "translation": translation_text,
                                "part_of_speech": known_trans.get("part_of_speech", ""),
                                "confidence": "high",
                                "examples": []
                            })
            
            # Extract examples 
            all_examples = self.example_extractor.extract_examples(soup, word)
            
            # Add examples directly to result
            result["examples"] = all_examples
            
            # Associate examples with translations when possible
            if all_examples and translations:
                for example in all_examples:
                    english_text = example.get("english", "").lower()
                    matched = False
                    
                    for trans in translations:
                        translation_text = trans.get("translation", "").lower()
                        # Match if translation appears as a whole word in the example
                        if translation_text and (
                            re.search(r'\b' + re.escape(translation_text) + r'\b', english_text) or
                            # For phrase translations, allow partial matches
                            (len(translation_text.split()) > 1 and translation_text in english_text)
                        ):
                            trans["examples"].append(example)
                            # Mark that this example is associated with a translation
                            example["associated_translation"] = translation_text
                            matched = True
                            break
                    
                    # If not matched to any specific translation but has high confidence,
                    # add to all translations with similar part of speech
                    if not matched and example.get("confidence") == "high":
                        # Try to guess which translations this example might apply to
                        example_words = set(example.get("english", "").lower().split())
                        for trans in translations:
                            trans_words = set(trans.get("translation", "").lower().split())
                            # If there's word overlap or the translation is a single word in the example
                            if trans_words & example_words:
                                trans["examples"].append(example)
            
            result["translations"] = translations
            
            # Find the best example if no associations were made
            if all_examples and not any(trans.get("examples") for trans in translations):
                # Find the best example to use as the primary example
                best_example = max(all_examples, key=lambda x: 
                    "high" if x.get("confidence") == "high" else 
                    "medium" if x.get("confidence") == "medium" else "low")
                
                # Add as primary example fields
                result["example_yoruba"] = best_example.get("yoruba", "")
                result["example_english"] = best_example.get("english", "")
            
        except requests.RequestException as e:
            logging.error(f"Error fetching {word}: {str(e)}")
            result["error"] = f"Network error: {str(e)}"
            result["status"] = "error"
        except Exception as e:
            logging.error(f"Error processing {word}: {str(e)}")
            result["error"] = f"Processing error: {str(e)}"
            result["status"] = "error"
        
            return result
        
    def extract_pos_from_text(self, text):
        """Extract part of speech marker from text"""
        if not text:
            return ""
            
        # Common POS markers
        pos_patterns = {
            "noun": [r"\bnoun\b", r"\bn\.\b"],
            "verb": [r"\bverb\b", r"\bv\.\b"],
            "adjective": [r"\badjective\b", r"\badj\.\b"],
            "adverb": [r"\badverb\b", r"\badv\.\b"],
            "pronoun": [r"\bpronoun\b", r"\bpron\.\b"],
            "preposition": [r"\bpreposition\b", r"\bprep\.\b"],
            "conjunction": [r"\bconjunction\b", r"\bconj\.\b"],
            "interjection": [r"\binterjection\b", r"\binterj\.\b"],
            "phrase": [r"\bphrase\b", r"\bexpression\b", r"\bidiom\b"],
            "numeral": [r"\bnumeral\b", r"\bnum\.\b", r"\bnumber\b", r"\bcardinal\b", r"\bordinal\b"]
        }
            
        for pos, patterns in pos_patterns.items():
            for pattern in patterns:
                if re.search(pattern, text, re.IGNORECASE):
                    return pos
                    
        return ""
    
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
                # Modify the code to handle dictionary objects correctly
                if isinstance(trans, dict):
                    # Extract the translation text from the dictionary
                    trans_text = trans.get('translation', '')
                    clean_trans = re.sub(r'[<>\\[\\\]{}]', '', trans_text.strip())
                else:
                    clean_trans = re.sub(r'[<>\\[\\\]{}]', '', trans.strip())
                
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
        
        # PHASE 4: Get the best example
        example_yoruba = item.get("example_yoruba", "")
        example_english = item.get("example_english", "")
        
        # Create flattened dictionary with cleaned data
        flattened = {
            "word": item.get("word", "").strip(),
            "translation": clean_translation,
            "all_translations": all_translations_text,
            "part_of_speech": standard_pos,
            "example_yoruba": example_yoruba,
            "example_english": example_english,
            "url": item.get("url", ""),
            "scrape_time": item.get("scrape_time", ""),
            "status": item.get("status", ""),
            "error": item.get("error", "")
        }
        
        return flattened
    
    def save_to_csv(self, data, output_file):
        """Save data to CSV format with normalized structure optimized for PostgreSQL import"""
        if not data:
            logging.warning(f"No data to save to CSV file: {output_file}")
            return
        
        # Create separate CSV files for words, translations, and examples
        base_name = os.path.splitext(output_file)[0]
        words_file = f"{base_name}_words.csv"
        translations_file = f"{base_name}_translations.csv"
        examples_file = f"{base_name}_examples.csv"
        
        # Prepare data for each file
        words_data = []
        translations_data = []
        examples_data = []
        
        # Track the next available ID for each entity type
        next_word_id = 1
        next_translation_id = 1
        next_example_id = 1
        
        # Dictionary to track word IDs by word text to avoid duplicates
        word_id_map = {}
        
        for item in data:
            word_text = item["word"]
            
            # Check if this word was already processed
            if word_text in word_id_map:
                word_id = word_id_map[word_text]
            else:
                word_id = next_word_id
                next_word_id += 1
                word_id_map[word_text] = word_id
                
                # Words data
                words_data.append({
                    "id": word_id,
                    "word": word_text,
                    "url": item.get("url", ""),
                    "scrape_time": item.get("scrape_time", ""),
                    "status": item.get("status", ""),
                    "error": item.get("error", "")
                })
            
            # Process translations - add deduplication by normalized text
            seen_translations = set()
            # Map to track translation IDs for this word
            trans_id_map = {}
            
            for trans in item.get("translations", []):
                if not isinstance(trans, dict) or not trans.get("translation"):
                    continue
                
                # Normalize the translation text for deduplication check
                norm_translation = trans["translation"].lower().strip()
                
                # Skip if we've already seen this translation
                if norm_translation in seen_translations:
                    continue
                
                seen_translations.add(norm_translation)
                
                # Create a unique translation ID
                trans_id = next_translation_id
                next_translation_id += 1
                
                # Store the ID for example association
                trans_key = f"{word_text}:{norm_translation}"
                trans_id_map[trans_key] = trans_id
                
                # Translations data
                translations_data.append({
                    "id": trans_id,
                    "word_id": word_id,
                    "translation": trans["translation"],
                    "part_of_speech": trans.get("part_of_speech", ""),
                    "confidence": trans.get("confidence", "medium")
                })
                
                # Examples data from translation
                for example in trans.get("examples", []):
                    if not isinstance(example, dict):
                        continue
                    yoruba = example.get("yoruba", "")
                    english = example.get("english", "")
                    if not yoruba or not english:
                        continue
                    
                    example_id = next_example_id
                    next_example_id += 1
                    
                    examples_data.append({
                        "id": example_id,
                        "translation_id": trans_id,
                        "word_id": word_id,
                        "yoruba_text": yoruba,
                        "english_text": english,
                        "is_jw_reference": example.get("is_jw_reference", False),
                        "confidence": example.get("confidence", "medium"),
                        "source": example.get("source", "unknown"),
                        "score": example.get("score", 0)
                    })
            
            # Also include examples directly from the item
            for example in item.get("examples", []):
                if not isinstance(example, dict):
                    continue
                yoruba = example.get("yoruba", "")
                english = example.get("english", "")
                if not yoruba or not english:
                    continue
                
                # Skip examples already added via translations
                already_added = False
                for ex_data in examples_data:
                    if (ex_data["yoruba_text"] == yoruba and 
                        ex_data["english_text"] == english):
                        already_added = True
                        break
                
                if not already_added:
                    example_id = next_example_id
                    next_example_id += 1
                    
                    # Try to find a matching translation
                    translation_id = None
                    for trans_key, trans_id in trans_id_map.items():
                        trans_text = trans_key.split(":", 1)[1]
                        if trans_text in english.lower():
                            translation_id = trans_id
                            break
                    
                    examples_data.append({
                        "id": example_id,
                        "translation_id": translation_id,  # May be None if no matching translation
                        "word_id": word_id,  # Always associated with the word
                        "yoruba_text": yoruba,
                        "english_text": english,
                        "is_jw_reference": example.get("is_jw_reference", False),
                        "confidence": example.get("confidence", "medium"),
                        "source": example.get("source", "unknown"),
                        "score": example.get("score", 0)
                    })
        
        # Save to CSV files with proper encoding
        if words_data:
            pd.DataFrame(words_data).to_csv(words_file, index=False, encoding='utf-8')
            logging.info(f"Saved {len(words_data)} words to {words_file}")
        
        if translations_data:
            pd.DataFrame(translations_data).to_csv(translations_file, index=False, encoding='utf-8')
            logging.info(f"Saved {len(translations_data)} translations to {translations_file}")
        
        if examples_data:
            pd.DataFrame(examples_data).to_csv(examples_file, index=False, encoding='utf-8')
            logging.info(f"Saved {len(examples_data)} examples to {examples_file}")
        else:
            logging.warning(f"No examples to save to {examples_file}")
    
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
        
        # Generate PostgreSQL-specific exports
        self.generate_postgres_exports()
        
        logging.info("Scraping complete. Generated all output files.")
    
    def generate_sql_init_file(self):
        """Generate a SQL file for initializing a database with the scraped data"""
        sql_file = os.path.join(self.output_folder, "init_database.sql")
        
        with open(sql_file, 'w', encoding='utf-8') as f:
            f.write("""-- Yoruba Dictionary Database Schema
-- Created automatically by GlosbeYorubaScraper

-- Main words table
CREATE TABLE IF NOT EXISTS yoruba_words (
    id SERIAL PRIMARY KEY,
    word VARCHAR(255) NOT NULL,
    url TEXT,
    scrape_time TIMESTAMP,
    status VARCHAR(50),
    error TEXT,
    UNIQUE(word)
);

-- Create index for faster word lookups
CREATE INDEX idx_word ON yoruba_words(word);

-- Translations table for multiple translations per word
CREATE TABLE IF NOT EXISTS translations (
    id SERIAL PRIMARY KEY,
    word_id INTEGER REFERENCES yoruba_words(id),
    translation TEXT NOT NULL,
    part_of_speech VARCHAR(50),
    confidence SMALLINT,
    UNIQUE(word_id, translation, part_of_speech)
);

-- Create indexes for faster translation lookups
CREATE INDEX idx_translation ON translations(translation);
CREATE INDEX idx_word_id ON translations(word_id);

-- Examples table for storing examples linked to translations
CREATE TABLE IF NOT EXISTS examples (
    id SERIAL PRIMARY KEY,
    translation_id INTEGER REFERENCES translations(id),
    yoruba_text TEXT NOT NULL,
    english_text TEXT NOT NULL,
    is_jw_reference BOOLEAN DEFAULT FALSE,
    confidence SMALLINT,
    source VARCHAR(50),
    score INTEGER,
    UNIQUE(translation_id, yoruba_text, english_text)
);

-- Create indexes for faster example lookups
CREATE INDEX idx_translation_id ON examples(translation_id);
CREATE INDEX idx_yoruba_text ON examples(yoruba_text);
CREATE INDEX idx_english_text ON examples(english_text);

-- Create view for easy querying of complete word data
CREATE OR REPLACE VIEW word_details AS
SELECT 
    w.word,
    t.translation,
    t.part_of_speech,
    e.yoruba_text as example_yoruba,
    e.english_text as example_english,
    e.is_jw_reference,
    e.confidence as example_confidence,
    e.score as example_score
FROM yoruba_words w
LEFT JOIN translations t ON w.id = t.word_id
LEFT JOIN examples e ON t.id = e.translation_id;
""")
        
        logging.info(f"Generated SQL initialization file: {sql_file}")
        return sql_file

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
                verification_score = item.get("verification", {}).get("quality_score", 0)
                
                insert_stmt = f"INSERT OR IGNORE INTO yoruba_words (word, translation, all_translations, part_of_speech, example_yoruba, example_english, url, scrape_time, status, error, verification_score) "
                insert_stmt += f"VALUES ('{word}', '{translation}', '{all_translations}', '{pos}', '{example_yoruba}', '{example_english}', '{url}', '{scrape_time}', '{status}', '{error}', {verification_score});\n"
                f.write(insert_stmt)
            
            f.write("\nCOMMIT;\n")
        
        logging.info(f"Generated SQL insert statements file: {sql_inserts_file}")

    def generate_postgres_exports(self):
        """Generate PostgreSQL specific export files"""
        # Get all JSON files
        all_json_files = []
        for root, _, files in os.walk(self.json_folder):
            for file in files:
                if file.endswith('.json'):
                    all_json_files.append(os.path.join(root, file))
        
        # Load all data from JSON files
        all_data = []
        for json_file in all_json_files:
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    all_data.extend(data)
            except Exception as e:
                logging.error(f"Error reading JSON file {json_file}: {str(e)}")
        
        # Create PostgreSQL exporter and generate exports
        exporter = PostgresExporter(self.output_folder)
        export_files = exporter.generate_postgres_export(all_data)
        
        logging.info(f"PostgreSQL export complete. Schema: {export_files['schema_file']}, Data: {export_files['insert_file']}")

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

    def clean_example_text(self, text):
        """Clean and normalize example text."""
        if not text or len(text.strip()) < 5:
            return None
        
        # Remove HTML tags if any remain
        text = re.sub(r'<[^>]+>', '', text)
        
        # Clean up whitespace
        text = ' '.join(text.split())
        
        # Ensure proper sentence ending
        if text and not text[-1] in '.!?':
            text = text + '.'
        
        # Ensure proper capitalization
        if text and text[0].islower():
            text = text[0].upper() + text[1:]
        
        # Remove any remaining noise patterns
        noise_patterns = [
            r'\[\d+\]',  # Reference numbers
            r'\(\s*\)',  # Empty parentheses
            r'^\s*\d+\.\s*',  # Leading numbers with dots
            r'^\s*[a-z]\)\s*',  # Leading letters with parentheses
        ]
        
        for pattern in noise_patterns:
            text = re.sub(pattern, '', text)
        
        text = text.strip()
        return text if len(text) >= 5 and len(text) <= 500 else None

    def get_known_translations(self, word):
        """Get known translations for common short words and pronouns"""
        # First check our verified translations dictionary
        if word in self.verified_translations:
            return self.verified_translations[word]
                
        # If not found in verified translations, use the default definitions
        known_words = {
            # Basic pronouns
            "a": [{"translation": "we", "part_of_speech": "pronoun"},
                  {"translation": "us", "part_of_speech": "pronoun"}],
            "á": [{"translation": "he", "part_of_speech": "pronoun"},
                  {"translation": "she", "part_of_speech": "pronoun"},
                  {"translation": "it", "part_of_speech": "pronoun"},
                  {"translation": "they", "part_of_speech": "pronoun"}],  # Added "they" as translation
            "mi": [{"translation": "I", "part_of_speech": "pronoun"},
                   {"translation": "me", "part_of_speech": "pronoun"},
                   {"translation": "my", "part_of_speech": "pronoun"}],
            "o": [{"translation": "you", "part_of_speech": "pronoun"}],
            "ẹ": [{"translation": "you (plural)", "part_of_speech": "pronoun"}],
            "wọn": [{"translation": "they", "part_of_speech": "pronoun"},
                    {"translation": "them", "part_of_speech": "pronoun"}],
            
            # Common phrases
            "à bá ti": [{"translation": "we would have", "part_of_speech": "phrase"}],
            "a óò": [{"translation": "we will", "part_of_speech": "phrase"}],
            "a máa": [{"translation": "we will", "part_of_speech": "phrase"}],
            "a dúpẹ́": [{"translation": "we give thanks", "part_of_speech": "phrase"}],
            "A kú ọdún àjíǹde": [{"translation": "Happy Easter", "part_of_speech": "phrase"}],
            "a gba ọ̀rọ̀ àkọsílẹ̀ dúró": [{"translation": "we accept the written word", "part_of_speech": "phrase"}],
            "a ta": [{"translation": "we sell", "part_of_speech": "verb"},
                     {"translation": "we sold", "part_of_speech": "verb"}]
        }
        
        return known_words.get(word, [])

if __name__ == "__main__":
    # Define paths
    base_folder = "./scraped_data"
    
    # Initialize and run the scraper
    scraper = GlosbeYorubaScraper(
        base_folder=base_folder,
        delay=5.0
    )
    scraper.run()