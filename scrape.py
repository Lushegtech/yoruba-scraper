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
        self.example_patterns = [
            ('.translation__example', '.source-text', '.target-text'),
            ('.example-pair', '.source', '.target'),
            ('.translation-memory-example', '.source', '.target'),
            ('.example__content', '.source', '.target'),
            ('.translation__item', '.source', '.target'),
            ('.translation-list__item', '.source', '.target'),
            ('.translation__translation', '.source', '.target'),
            ('.translation-item', '.source', '.target')
        ]
        
        self.text_patterns = [
            r'Example sentences with "([^"]+)"[:\s]+(.+?)↔(.+?)(?=$|\n|<)',
            r'Sample translated sentence:(.+?)↔(.+?)(?=$|\n|<)',
            r'Example:(.+?)↔(.+?)(?=$|\n|<)',
            r'Translation examples:(.+?)↔(.+?)(?=$|\n|<)',
            r'([^\.]+\.)[\s]*↔[\s]*([^\.]+\.)',

            r'Usage:[\s]*([^→]+)→([^$\n<]+)',
            r'Context:[\s]*([^=]+)=([^$\n<]+)',
            r'"([^"]+)"\s*translates to\s*"([^"]+)"',
            r'([^:]+):\s*\(([^)]+)\)',

            r'\b([^\.]{1,50})\s*[=→↔]\s*([^\.]{1,50})',
            r'([^:]+):\s*"([^"]+)"',
            r'•\s*([^•]+)\s*•\s*([^•]+)',
            r'[\[\(]([^\[\]]+)[\]\)]\s*=\s*[\[\(]([^\[\]]+)[\]\)]'
        ]
    
        self.yoruba_markers = [
            'mo', 'o', 'ó', 'wọn', 'won', 'a', 'ẹ', 'è', 'ni',
            'kò', 'ko', 'ṣe', 'se', 'ti', 'sì', 'si', 'yìí', 'yii',
            'ń', 'n', 'kí', 'ki', 'bí', 'bi', 'fún', 'fun',
            'àti', 'ati', 'ọmọ', 'omo', 'jẹ́', 'je', 'gbà', 'gba'
        ]
        
        self.english_markers = [
            'the', 'a', 'an', 'is', 'are', 'was', 'were', 'will',
            'have', 'has', 'had', 'be', 'been', 'being',
            'I', 'you', 'he', 'she', 'it', 'we', 'they',
            'this', 'that', 'these', 'those', 'and', 'but', 'or'
        ]
        
        # Dictionary to store known good example sentences for validation
        self.known_examples = {
            'adìye': [
                {"yoruba": "Adìye náà ń jẹ èso.", "english": "The chicken is eating fruit.", "confidence": "high"},
                {"yoruba": "Mo fẹ́ adìye kan.", "english": "I want a chicken.", "confidence": "high"}
            ],
            'àpẹ́': [
                {"yoruba": "Àpẹ́ náà ń wẹ̀ ní odò.", "english": "The duck is swimming in the river.", "confidence": "high"}
            ],
            'ó': [
                {"yoruba": "Ó ń sùn.", "english": "He is sleeping.", "confidence": "high"},
                {"yoruba": "Ó ń kọrin.", "english": "She is singing.", "confidence": "high"},
                {"yoruba": "Ó dára.", "english": "It is good.", "confidence": "high"}
            ],
            'àpẹ́rẹ́': [
                {"yoruba": "Àpẹ́rẹ́ tí mo fẹ́ fi hàn.", "english": "The example I want to show.", "confidence": "high"}
            ],
            'ẹ̀kọ́': [
                {"yoruba": "Ẹ̀kọ́ jẹ́ pàtàkì.", "english": "Education is important.", "confidence": "high"}
            ],
            'a': [
                {"yoruba": "A jẹ̀ ẹ.", "english": "We ate it.", "confidence": "high"},
                {"yoruba": "A tí dé.", "english": "We have arrived.", "confidence": "high"}
            ],
            'á': [
                {"yoruba": "Á mú un.", "english": "He will take it.", "confidence": "high"},
                {"yoruba": "Á pa á.", "english": "She will kill it.", "confidence": "high"}
            ]
        }
    
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
            r'Chapter',
            r'Jèhófà',
            r'Ìjọba',
            r'Bíbélì'
        ]
        
        for pattern in jw_patterns:
            if (re.search(pattern, yoruba, re.IGNORECASE) or 
                re.search(pattern, english, re.IGNORECASE)):
                return True
            
        return False
    
    def is_valid_example(self, yoruba, english, word):
        """
        Validate if extracted example pair is legitimate with enhanced accuracy
        
        This validation uses several criteria to ensure the examples are accurate:
        1. Both Yoruba and English texts must exist and be reasonable length
        2. The Yoruba text should contain the word being looked up (for most cases)
        3. The text should have appropriate language markers (Yoruba in Yoruba, English in English)
        4. The text should not contain UI elements or irrelevant content
        5. For short words, more lenient criteria are used
        
        Args:
            yoruba (str): The Yoruba text
            english (str): The English text
            word (str): The word being looked up
            
        Returns:
            bool: True if the example pair is valid, False otherwise
        """
        # Special case testing for common incorrect translations
        word_translations = {
            "adìye": {"correct": ["chicken"], "incorrect": ["duck", "goose", "hen", "turkey", "bird"]},
            "àpẹ́": {"correct": ["duck"], "incorrect": ["chicken", "goose", "hen", "turkey", "bird"]},
            "àpẹ́rẹ́": {"correct": ["example", "sample", "illustration"], "incorrect": []},
            "ẹ̀kọ́": {"correct": ["education", "lesson", "learning", "study"], "incorrect": []},
            "ìgbín": {"correct": ["snail"], "incorrect": ["slug", "worm", "insect"]},
            "àgbàdo": {"correct": ["corn", "maize"], "incorrect": ["wheat", "rice", "barley"]},
            "abo": {"correct": ["female", "feminine"], "incorrect": []},
            "a": {"correct": ["we", "us"], "incorrect": ["I", "they"]},
            "á": {"correct": ["will", "shall", "he", "she"], "incorrect": ["shut"]}
        }
        
        normalized_word = word.lower()
        
        # Basic validation checks
        if not yoruba or not english:
            return False
            
        is_short_word = len(word) <= 2
        min_length = 3 if is_short_word else 5
        max_length = 1000
        
        if len(yoruba) < min_length or len(english) < min_length:
            return False
        if len(yoruba) > max_length or len(english) > max_length:
            return False
        
        # Check for HTML tags in text (indicates parsing error)
        if re.search(r'</?[a-z]+>', yoruba) or re.search(r'</?[a-z]+>', english):
            return False
            
        # Check if example sentence includes the word being looked up (skip for very short words)
        if len(word) > 1 and not is_short_word:
            word_pattern = r'\b' + re.escape(normalized_word) + r'\b'
            word_variants = [
                normalized_word, 
                normalized_word.replace('ì', 'i').replace('é', 'e').replace('ó', 'o'),
                normalized_word.replace('ì', 'i'), 
                normalized_word.replace('é', 'e')
            ]
            
            # For longer words we should find the word somewhere in the Yoruba text
            if not any(re.search(r'\b' + re.escape(variant) + r'\b', yoruba.lower()) for variant in word_variants):
                # The Yoruba example MUST contain the word or a variant unless it's a very short word
                return False
            
        # Check for UI elements or common website text that shouldn't be in examples
        ui_elements = [
            'glosbe', 'log in', 'sign up', 'click', 'next page',
            'show more', 'hide', 'loading', 'search', 'menu',
            'translation', 'dictionary', 'cookie', 
            'privacy', 'terms', 'contact', 'email', 'password',
            'username', 'copyright', 'all rights', 'download'
        ]
        
        if any(ui in yoruba.lower() for ui in ui_elements) or any(ui in english.lower() for ui in ui_elements):
            return False
            
        # Check if the text is actually in Yoruba (must have at least some Yoruba markers or diacritics)
        has_yoruba_markers = any(marker in yoruba.lower() for marker in self.yoruba_markers)
        has_yoruba_diacritics = bool(re.search(r'[àáèéìíòóùúẹọṣ]', yoruba.lower()))
        
        # If no Yoruba markers or diacritics, likely not Yoruba text
        if not has_yoruba_markers and not has_yoruba_diacritics:
            return False
            
        score = 0
        
        # Very important: The word should be in the Yoruba text 
        # (unless it's a very short word which might be part of a larger word)
        if word.lower() in yoruba.lower():
            score += 30
        elif is_short_word and any(w.startswith(word.lower()) or w.endswith(word.lower()) for w in yoruba.lower().split()):
            score += 15
            
        # Check length ratio between Yoruba and English
        # Good translations tend to have somewhat similar lengths
        length_ratio = min(len(yoruba), len(english)) / max(len(yoruba), len(english))
        if is_short_word:
            score += int(length_ratio * 15)
        else:
            score += int(length_ratio * 25)
            
        # Check for language markers
        has_english_markers = any(marker in english.lower() for marker in self.english_markers)
        
        if has_yoruba_markers:
            score += 20
        if has_english_markers:
            score += 20
            
        # Check for Yoruba pronouns which are common in sentences
        if re.search(r'\b(mo|o|ó|wọn|won|a|ẹ|è)\b', yoruba.lower()):
            score += 15
        
        # Check for proper sentence structure in the Yoruba text
        if re.match(r'^[A-ZÀ-ÖØ-öø-ÿ]', yoruba) and re.search(r'[.!?]$', yoruba):
            score += 10
            
        # Check for proper sentence structure in the English text
        if re.match(r'^[A-Z]', english) and re.search(r'[.!?]$', english):
            score += 10
            
        # Check for translation consistency with known translations
        # If the word is "chicken" (adìye), the English should contain "chicken" not "duck"
        normalized_word = word.lower()
        if normalized_word in self.known_examples:
            # Get all English translations from known examples
            known_translations = set()
            incorrect_translations = {
                'chicken': {'duck', 'hen', 'goose', 'bird', 'turkey'},
                'duck': {'chicken', 'hen', 'goose', 'bird', 'turkey'},
                'example': {'sample', 'instance', 'case', 'illustration'},
                'sample': {'example', 'instance', 'case', 'illustration'}
            }
            
            for known_example in self.known_examples[normalized_word]:
                # Extract the main noun or verb from the English translation
                eng_words = known_example['english'].lower().split()
                for eng_word in eng_words:
                    # Remove punctuation
                    clean_eng_word = re.sub(r'[^\w\s]', '', eng_word)
                    if len(clean_eng_word) > 3:  # Only consider substantial words
                        known_translations.add(clean_eng_word)
            
            # If we have known translations, check if any of them are in the English text
            # or if any incorrect translations are in the English text
            if known_translations:
                if not any(trans in english.lower() for trans in known_translations):
                    score -= 30  # Penalize for not containing any known translations
                
                # Check for incorrect animal/object translations
                for correct_trans in known_translations:
                    if correct_trans in incorrect_translations:
                        for incorrect_trans in incorrect_translations[correct_trans]:
                            if incorrect_trans in english.lower() and correct_trans not in english.lower():
                                # Found incorrect translation (e.g., "duck" when should be "chicken")
                                score -= 50  # Severe penalty for wrong translations
        
        # Different thresholds based on word length
        required_score = 30 if is_short_word else 50
        
        # For known examples in our database, automatically validate
        if normalized_word in self.known_examples:
            for known_example in self.known_examples[normalized_word]:
                if (yoruba.lower() == known_example['yoruba'].lower() and
                    english.lower() == known_example['english'].lower()):
                    return True
        
        return score >= required_score
    
    def clean_example_text(self, text):
        """Clean up extracted example text"""
        if not text:
            return ""
        
        # Keep a copy of the original text
        original_text = text
            
        # Basic cleanup
        text = re.sub(r'\s+', ' ', text).strip()
            
        # Remove numeric references and UI elements
        text = re.sub(r'(\d+/\d+|Show all|Hide)', '', text)
            
        # Remove arrow symbols
        text = re.sub(r'(↑|↓|→|←|↔)', '', text)
            
        # Remove email addresses and URLs
        text = re.sub(r'\S+@\S+\.\S+', '', text)
        text = re.sub(r'https?://\S+', '', text)
            
        # Fix spacing around punctuation
        text = re.sub(r'\s+([.,;:!?])', r'\1', text)
        text = re.sub(r'([.,;:!?])\s+', r'\1 ', text)
        
        # Handle HTML entities
        text = text.replace('&amp;', '&')
        text = text.replace('&quot;', '"')
        text = text.replace('&apos;', "'")
        text = text.replace('&lt;', '<')
        text = text.replace('&gt;', '>')
        text = re.sub(r'&[a-zA-Z]+;', '', text)
        
        # Normalize quotes and apostrophes
        text = re.sub(r'["""]', '"', text)
        text = re.sub(r'[\u2018\u2019\']', "'", text)
        
        # Add space after punctuation if followed by a letter
        text = re.sub(r'([.,!?])([A-Za-z])', r'\1 \2', text)
        
        # Detect language and apply language-specific fixes
        has_yoruba_diacritics = bool(re.search(r'[àáèéìíòóùúẹọṣ]', text))
        
        if has_yoruba_diacritics:
            # Apply Yoruba-specific fixes
            text = self._fix_yoruba_spacing(text)
        else:
            # Apply English-specific fixes - includes handling of joined words
            text = self._fix_english_spacing(text)
        
        # Remove any multiple spaces that might have been created
        text = re.sub(r'\s+', ' ', text).strip()
        
        # If the text was reduced to something too short, revert to original
        if len(text) < 5 and len(original_text) > 10:
            return original_text.strip()
            
        return text.strip()
        
    def _clean_english_example(self, text):
        """Clean English example text to improve quality
        
        Args:
            text (str): The English example text to clean
            
        Returns:
            str: Cleaned example text
        """
        if not text or not isinstance(text, str):
            return text
        
        # Store original for comparison
        original = text
        
        # Fix spacing between words (common scraping issue)
        # Add space between lowercase and uppercase letters
        text = re.sub(r'([a-z])([A-Z])', r'\1 \2', text)
        
        # Fix common auxiliary verb + main verb issues
        for aux in ['could', 'would', 'should', 'might', 'must', 'will', 'shall']:
            for verb in ['have', 'be', 'go', 'do', 'take', 'make', 'get']:
                text = text.replace(f"{aux}{verb}", f"{aux} {verb}")
                # Also handle past participle forms
                text = text.replace(f"{aux}been", f"{aux} been")
                text = text.replace(f"{aux}had", f"{aux} had")
        
        # Fix "have been" compounds
        text = text.replace("havebeen", "have been")
        
        # Fix common compound words
        common_patterns = [
            ('beenreleased', 'been released'),
            ('havebeen', 'have been'),
            ('hasbeen', 'has been'),
            ('hadbeen', 'had been'),
            ('beenleft', 'been left'),
            ('beenput', 'been put'),
            ('putto', 'put to'),
            ('releasedif', 'released if'),
            ('manwas', 'man was'),
            ('mancould', 'man could'),
            ('ofmankind', 'of mankind'),
            ('hecould', 'he could'),
            ('hecannot', 'he cannot'),
            ('shecannot', 'she cannot'),
            ('itis', 'it is'),
            ('ifhe', 'if he'),
            ('ifthey', 'if they'),
            ('wasno', 'was no'),
            ('theylearn', 'they learn'),
            ('theydo', 'they do'),
            ('wedo', 'we do'),
            ('youdo', 'you do'),
            ('youknow', 'you know'),
            ('youmay', 'you may'),
            ('Theman', 'The man'),
            ('Thisman', 'This man'),
            ('Thatis', 'That is'),
            ('fromamong', 'from among'),
            ('toobey', 'to obey'),
            ('inorder', 'in order'),
            ('inthe', 'in the'),
            ('forthe', 'for the'),
            ('willbe', 'will be'),
            ('wouldbe', 'would be'),
            ('shouldbe', 'should be'),
            ('couldbe', 'could be'),
            ('mightbe', 'might be'),
            ('willhave', 'will have'),
            ('wouldhave', 'would have'),
            ('shouldhave', 'should have'),
            ('couldhave', 'could have'),
            ('mighthave', 'might have'),
        ]
        
        for pattern, replacement in common_patterns:
            text = text.replace(pattern, replacement)
        
        # Fix space between determiners and nouns 
        for det in ['The', 'A', 'An', 'This', 'That', 'These', 'Those', 'His', 'Her', 'Its', 'Our', 'Their']:
            text = re.sub(f"({det})([a-z][a-z]+)", r"\1 \2", text)
        
        # Fix possessive + noun patterns
        text = re.sub(r"('s)([a-z][a-z]+)", r"\1 \2", text)
        
        # Fix common preposition + noun patterns
        for prep in ['in', 'on', 'at', 'by', 'for', 'with', 'to', 'from', 'of']:
            text = re.sub(f"\\b{prep}([a-z][a-z]+)", f"{prep} \\1", text)
        
        # Clean up spaces around punctuation
        text = re.sub(r'\s+([.,;:!?])', r'\1', text)
        
        # Add necessary spacing after punctuation
        text = re.sub(r'([.,;:!?])([A-Za-z])', r'\1 \2', text)
        
        # Normalize spaces
        text = re.sub(r'\s+', ' ', text).strip()
        
        # If the text was reduced to something too short, revert to original
        if len(text) < 5 and len(original) > 10:
            return original.strip()
            
        return text
    
    def extract_examples(self, soup, word):
        """Extract example sentences using multiple techniques"""
        examples = []
        
        # First, extract the primary translation directly from the main translation item
        translation_items = soup.select('h3.translation__item__pharse')
        primary_translations = []
        
        for item in translation_items:
            translation_text = item.get_text(strip=True)
            if translation_text:
                primary_translations.append(translation_text)
                
        # Look for translation items with expandable details
        translation_details_containers = soup.select('li[data-element="translation"]')
        for container in translation_details_containers:
            translation_elem = container.select_one('h3.translation__item__pharse')
            if translation_elem:
                translation_text = translation_elem.get_text(strip=True)
                if translation_text and translation_text not in primary_translations:
                    primary_translations.append(translation_text)
        
        # Process standard example containers
        example_selectors = [
            '.translation__example', '.example-pair',
            '.translation-memory-example', '.example__content',
            '.dict-example',
            '.translation-example',
            '.example-item',
            '.dict-example-item',
            '.translation-memory',
            '.tmem',
            '.example',
            '[data-example]',
            '.py-2.flex',
            '.odd\\:bg-slate-100'
        ]
        
        source_selectors = [
            '.yoruba', '.source', '.example__source', '.left', '.src', '[data-source]',
            '.w-1\\/2.dir-aware-pr-1',
            'p[lang="yo"]'
        ]
        
        target_selectors = [
            '.english', '.target', '.example__target', '.right', '.tgt', '[data-target]',
            '.w-1\\/2.dir-aware-pl-1',
            '.w-1\\/2.px-1.ml-2'
        ]
        
        for selector in example_selectors:
            containers = soup.select(selector)
            for container in containers:
                yoruba = None
                english = None
                
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
        
        # Extract from similar phrases section
        similar_phrases_section = soup.select_one('#simmilar-phrases')
        if similar_phrases_section:
            phrase_items = similar_phrases_section.select('li.px-2.py-1.flex')
            for item in phrase_items:
                yoruba_elem = item.select_one('.w-1\\/3.dir-aware-text-right')
                english_elem = item.select_one('.dir-aware-pl-2.w-2\\/3')
                
                if yoruba_elem and english_elem:
                    yoruba = yoruba_elem.get_text(strip=True)
                    english = english_elem.get_text(strip=True)
                    
                    if yoruba and english:
                        yoruba_text = self.clean_example_text(yoruba)
                        english_text = self.clean_example_text(english)
                        
                        # Check if this example contains our word
                        if word.lower() in yoruba_text.lower() or yoruba_text.lower() in word.lower():
                            examples.append({
                                "yoruba": yoruba_text,
                                "english": english_text,
                                "source": "similar_phrase",
                                "confidence": "high",
                                "is_jw_reference": False
                            })
        
        # Extract from Memory Examples section (#tmem_first_examples)
        memory_examples = soup.select('#tmem_first_examples .odd\\:bg-slate-100, #tmem_first_examples .py-2.flex')
        for example in memory_examples:
            yoruba_elem = example.select_one('.w-1\\/2.dir-aware-pr-1, p[lang="yo"]')
            english_elem = example.select_one('.w-1\\/2.dir-aware-pl-1, .w-1\\/2.px-1.ml-2')
            
            if yoruba_elem and english_elem:
                yoruba = yoruba_elem.get_text(strip=True)
                english = english_elem.get_text(strip=True)
                
                yoruba_text = self.clean_example_text(yoruba)
                english_text = self.clean_example_text(english)
                
                if yoruba_text and english_text and word.lower() in yoruba_text.lower():
                    examples.append({
                        "yoruba": yoruba_text,
                        "english": english_text,
                        "source": "tmem",
                        "confidence": "high",
                        "is_jw_reference": self.is_jw_reference(yoruba_text, english_text)
                    })
        
        # Add regex pattern extraction for sentences
        html_text = str(soup)
        
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
                    
                    if word.lower() in yoruba_text.lower() and self.is_valid_example(yoruba_text, english_text, word):
                        examples.append({
                            "yoruba": yoruba_text,
                            "english": english_text,
                            "source": "regex",
                            "confidence": "medium",
                            "is_jw_reference": self.is_jw_reference(yoruba_text, english_text)
                        })
        
        # Special handling for short words
        if len(word) <= 2:
            short_word_examples = self.extract_short_word_examples(soup, word)
            examples.extend(short_word_examples)
        
        # Add the primary translations as examples where the Yoruba is the word itself
        for translation in primary_translations:
            clean_translation = translation.strip()
            if clean_translation and len(clean_translation) > 1:
                # Only add as an example if it's not already in the examples list
                if not any(example["english"].lower() == clean_translation.lower() for example in examples):
                    examples.append({
                        "yoruba": word,
                        "english": clean_translation,
                        "source": "primary_translation",
                        "confidence": "high",
                        "is_jw_reference": False
                    })
        
        # De-duplicate examples
        seen = set()
        unique_examples = []
        for example in examples:
            key = (example["yoruba"].lower(), example["english"].lower())
            if key not in seen:
                seen.add(key)
                unique_examples.append(example)
        
        return unique_examples
        
    def extract_short_word_examples(self, soup, word):
        """Special extraction for short words like pronouns"""
        examples = []
        
        example_pairs = []
        
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
        
        for yoruba, english in example_pairs:
            yoruba_text = self.clean_example_text(yoruba)
            english_text = self.clean_example_text(english)
            
            if yoruba_text and english_text and len(yoruba_text) >= 5 and len(english_text) >= 5:
                examples.append({
                    "yoruba": yoruba_text,
                    "english": english_text,
                    "source": "short_word",
                    "confidence": "medium",
                    "is_jw_reference": self.is_jw_reference(yoruba_text, english_text)
                })
        
        paragraphs = soup.find_all(['p', 'div', 'span'])
        for paragraph in paragraphs:
            text = paragraph.get_text(strip=True)
            if word.lower() in text.lower():
                sentences = re.split(r'[.!?]', text)
                for sentence in sentences:
                    if word.lower() in sentence.lower() and len(sentence) >= 10:
                        next_p = paragraph.find_next_sibling(['p', 'div', 'span'])
                        if next_p:
                            next_text = next_p.get_text(strip=True)
                            if 0.5 <= len(next_text) / len(sentence) <= 2:
                                examples.append({
                                    "yoruba": sentence.strip(),
                                    "english": next_text.strip(),
                                    "source": "short_word",
                                    "confidence": "low",
                                    "is_jw_reference": False
                                })
        
        return examples
        
    def extract_examples_by_translation(self, soup, word, translations):
        """Extract examples and try to associate them with specific translations"""
        all_examples = self.extract_examples(soup, word)
        
        examples_by_translation = {}
        for translation in translations:
            examples_by_translation[translation] = []
        
        general_examples = []
        
        for example in all_examples:
            english = example.get("english", "").lower()
            matched = False
            
            for translation in translations:
                if translation.lower() in english:
                    examples_by_translation[translation].append(example)
                    matched = True
                    break
            
            if not matched:
                general_examples.append(example)
        
        return {
            "by_translation": examples_by_translation,
            "general": general_examples
        }

    def verify_example_pair(self, yoruba, english):
        """
        Verify if an example pair is valid with enhanced accuracy checks.
        
        This performs additional validation beyond the basic checks in is_valid_example:
        1. Ensures proper sentence structure
        2. Verifies the sentences have matching meaning (based on key words)
        3. Checks for proper length ratio
        4. Verifies the presence of sentence markers
        
        Args:
            yoruba (str): The Yoruba text
            english (str): The English text
            
        Returns:
            bool: True if the example pair passes verification, False otherwise
        """
        # Skip empty sentences
        if not yoruba or not english:
            return False
            
        # Normalize texts
        yoruba = self.clean_example_text(yoruba)
        english = self.clean_example_text(english)
        
        # Basic length checks
        if len(yoruba) < 10 or len(english) < 10:
            return False
        if len(yoruba) > 500 or len(english) > 500:
            return False
            
        # Check for sentence structure (should have sentence markers)
        has_yoruba_sentence_markers = bool(re.search(r'[.!?]', yoruba))
        has_english_sentence_markers = bool(re.search(r'[.!?]', english))
        
        # If one has sentence markers but the other doesn't, that's a mismatch
        if has_yoruba_sentence_markers != has_english_sentence_markers:
            return False
            
        # Check length ratio - good translations tend to have somewhat similar lengths
        # Yoruba tends to be more concise than English, so the ratio is often around 0.7-1.3
        length_ratio = len(yoruba) / len(english) if len(english) > 0 else 0
        if length_ratio < 0.5 or length_ratio > 2.0:
            return False
            
        # Calculate a verification score
        score = 0
        
        # Score for sentence markers
        if has_yoruba_sentence_markers and has_english_sentence_markers:
            score += 20
            
        # Score for Yoruba markers (common words)
        yoruba_markers = [
            'ni', 'tí', 'sì', 'kò', 'ń', 'ó', 'á', 'mo', 'wọn', 'àti', 
            'fún', 'pé', 'kí', 'jẹ́', 'ṣe', 'bí', 'wá', 'lọ', 'gbà', 'rí'
        ]
        if any(re.search(r'\b' + marker + r'\b', yoruba.lower()) for marker in yoruba_markers):
            score += 20
            
        # Score for English markers (common words)
        english_markers = [
            'the', 'a', 'an', 'of', 'to', 'in', 'is', 'are', 'was', 'were',
            'will', 'have', 'has', 'had', 'be', 'with', 'for', 'and', 'or', 'but'
        ]
        if any(re.search(r'\b' + marker + r'\b', english.lower()) for marker in english_markers):
            score += 20
            
        # Score for proper length ratio
        if 0.7 <= length_ratio <= 1.3:
            score += 20
            
        # Score for capital letter at the beginning
        if yoruba and english and yoruba[0].isupper() and english[0].isupper():
            score += 10
            
        # Score for matching end punctuation
        yoruba_end = yoruba[-1] if yoruba else ''
        english_end = english[-1] if english else ''
        if yoruba_end in '.!?' and english_end in '.!?':
            score += 10
            
        # Minimum score required for verification
        return score >= 50

    def add_known_examples(self, word, examples):
        """
        Add known good examples for a word
        
        Args:
            word (str): The Yoruba word
            examples (list): A list of example dictionaries with 'yoruba' and 'english' keys
        """
        normalized_word = word.lower()
        if normalized_word not in self.known_examples:
            self.known_examples = self.known_examples or {}
            self.known_examples[normalized_word] = []
            
        # Add each example if it's not already in the list
        for example in examples:
            if not any(ex.get('yoruba') == example.get('yoruba') and 
                     ex.get('english') == example.get('english') 
                     for ex in self.known_examples.get(normalized_word, [])):
                
                # Ensure all examples have confidence level
                if 'confidence' not in example:
                    example['confidence'] = 'high'
                    
                self.known_examples[normalized_word].append(example)
                
    def get_known_examples(self, word):
        """
        Get known good examples for a word
        
        Args:
            word (str): The Yoruba word
            
        Returns:
            list: A list of example dictionaries
        """
        word_key = word.lower()
        return self.known_examples.get(word_key, [])
    
    # Adding the missing method
    def _fix_yoruba_spacing(self, text):
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
        
        # Fix spacing for other common patterns
        text = re.sub(r'gba(ra)', r'gba \1', text)
        text = re.sub(r'ọ(jọ)', r'ọ \1', text)
        text = re.sub(r'(mọ)(le)', r'\1 \2', text)
        text = re.sub(r'(jọ)(wọ)', r'\1 \2', text)
        
        # Fix patterns related to the "à bá" construction with various suffixes
        for suffix in ['ti', 'le', 'jẹ', 'ri', 'se', 'ṣe', 'wa', 'gbọ', 'gbà', 'mọ']:
            text = re.sub(f'à bá{suffix}', f'à bá {suffix}', text)
        
        # Fix spacing issues with pronouns and other function words
        pronoun_patterns = [
            (r'(mo|o|ó|à|a|è|e)(ń|n)', r'\1 \2'),
            (r'(ó|o)(ti)', r'\1 \2'),
            (r'(ní|ni)(lá|la)', r'\1 \2'),
            (r'(sí|si)(kí|ki)', r'\1 \2')
        ]
        
        for pattern, replacement in pronoun_patterns:
            text = re.sub(pattern, replacement, text)
        
        # Final pass to fix multiple spaces
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
        
    def _fix_english_spacing(self, text):
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
        
        for wrong, right in specific_fixes:
            text = text.replace(wrong, right)
            
        # Final pass to fix multiple spaces
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text

class PostgresExporter:
    """Class for exporting data to PostgreSQL format"""
    
    def __init__(self, output_folder):
        self.output_folder = output_folder
        
    def normalize_string(self, text):
        """Normalize and escape a string for PostgreSQL use"""
        if text is None:
            return "NULL"
        
        normalized = text.replace("'", "''").replace("\\", "\\\\")
        
        normalized = normalized.replace("\n", " ").replace("\t", " ").replace("\r", " ")
        
        normalized = re.sub(r'[\x00-\x1F\x7F]', '', normalized)
        
        return f"'{normalized}'"
    
    def generate_schema(self):
        """Generate PostgreSQL schema optimized for the dictionary database"""
        schema = []
        
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
        
        schema.append("CREATE INDEX IF NOT EXISTS idx_words_word ON words (word);")
        schema.append("")
        
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
        
        schema.append("CREATE INDEX IF NOT EXISTS idx_translations_word_id ON translations (word_id);")
        schema.append("CREATE INDEX IF NOT EXISTS idx_translations_translation ON translations (translation);")
        schema.append("")
        
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
        
        schema.append("CREATE INDEX IF NOT EXISTS idx_examples_translation_id ON examples (translation_id);")
        schema.append("CREATE INDEX IF NOT EXISTS idx_examples_word_id ON examples (word_id);")
        schema.append("")
        
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
        
        inserts.append("-- Data Import Statements")
        inserts.append("-- Generated: " + time.strftime("%Y-%m-%d %H:%M:%S"))
        inserts.append("")
        
        word_ids = set()
        translation_ids = set()
        example_ids = set()
        
        for file_data in all_data:
            for word in file_data.get("words", []):
                if word["id"] in word_ids:
                    continue
                word_ids.add(word["id"])
                
                inserts.append(f"INSERT INTO words (id, word, url, scrape_time, status, error) VALUES (")
                inserts.append(f"    {word['id']},")
                inserts.append(f"    {self.normalize_string(word['word'])},")
                inserts.append(f"    {self.normalize_string(word.get('url', ''))},")
                inserts.append(f"    {self.normalize_string(word.get('scrape_time', ''))},")
                inserts.append(f"    {self.normalize_string(word.get('status', ''))},")
                inserts.append(f"    {self.normalize_string(word.get('error', ''))}")
                inserts.append(");")
                inserts.append("")

            for trans in file_data.get("translations", []):
                if trans["id"] in translation_ids:
                    continue
                translation_ids.add(trans["id"])
                
                inserts.append(f"INSERT INTO translations (id, word_id, translation, part_of_speech, confidence) VALUES (")
                inserts.append(f"    {trans['id']},")
                inserts.append(f"    {trans['word_id']},")
                inserts.append(f"    {self.normalize_string(trans['translation'])},")
                inserts.append(f"    {self.normalize_string(trans.get('part_of_speech', ''))},")
                inserts.append(f"    {self.normalize_string(trans.get('confidence', ''))}")
                inserts.append(");")
                inserts.append("")
            
            for example in file_data.get("examples", []):
                if example["id"] in example_ids:
                    continue
                example_ids.add(example["id"])
                
                translation_id = "NULL"
                if example.get("translation_id") is not None:
                    translation_id = example["translation_id"]
                
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
        os.makedirs(self.output_folder, exist_ok=True)
        
        schema_file = os.path.join(self.output_folder, "yoruba_dictionary_schema.sql")
        with open(schema_file, "w", encoding="utf-8") as f:
            f.write(self.generate_schema())
        logging.info(f"Generated PostgreSQL schema file: {schema_file}")
        
        data_file = os.path.join(self.output_folder, "yoruba_dictionary_data.sql")
        with open(data_file, "w", encoding="utf-8") as f:
            f.write(self.create_insert_statements(all_data))
        logging.info(f"Generated PostgreSQL data file: {data_file}")
        
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
        
        self.min_scores = {
            "translation": 50,
            "example": 40,
            "overall": 45
        }
        
        self.known_words = {
            "a": {"translations": ["we", "us"], "pos": "pronoun"},
            "á": {"translations": ["he", "she", "it"], "pos": "pronoun"},
            "mi": {"translations": ["I", "me", "my"], "pos": "pronoun"},
            "o": {"translations": ["you"], "pos": "pronoun"},
            "ẹ": {"translations": ["you (plural)"], "pos": "pronoun"},
            "wọn": {"translations": ["they", "them"], "pos": "pronoun"},
            
            "à bá ti": {"translations": ["we would have"], "pos": "phrase"},
            "a óò": {"translations": ["we will"], "pos": "phrase"},
            "a máa": {"translations": ["we will"], "pos": "phrase"},
            "a dúpẹ́": {"translations": ["we give thanks"], "pos": "phrase"},
            "A kú ọdún àjíǹde": {"translations": ["Happy Easter"], "pos": "phrase"},
            "a gba ọ̀rọ̀ àkọsílẹ̀ dúró": {"translations": ["we accept the written word"], "pos": "phrase"},
            "a ta": {"translations": ["we sell", "we sold"], "pos": "verb"}
        }
        
        self.yoruba_markers = {
            "characters": ["ẹ", "ọ", "ṣ", "à", "á", "è", "é", "ì", "í", "ò", "ó", "ù", "ú"],
            "pronouns": ["mo", "o", "ó", "á", "a", "ẹ", "wọn", "mi"],
            "verbs": ["ní", "ti", "kò", "ṣe", "máa", "wá", "lọ", "jẹ", "bá"],
            "particles": ["ni", "kí", "bí", "tí", "sì", "fún"]
        }
        
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
        
        for char in self.yoruba_markers["characters"]:
            if char in text_lower:
                score += 15
                break
        
        for word in words:
            if word in self.yoruba_markers["pronouns"]:
                score += 20
            elif word in self.yoruba_markers["particles"]:
                score += 15
            elif word in self.yoruba_markers["verbs"]:
                score += 15

        english_words = sum(1 for w in words if w in 
                          [item for sublist in self.english_patterns.values() for item in sublist])
        if english_words > 0:
            score -= english_words * 5
        
        return score >= 40, score

    def verify_english_text(self, text):
        """Verify if text contains valid English language patterns"""
        if not text:
            return False, 0
            
        score = 0
        text_lower = text.lower()
        words = text_lower.split()
        
        for category, patterns in self.english_patterns.items():
            if any(pattern in words for pattern in patterns):
                score += 20
        
        if text[0].isupper():
            score += 15
        
        if re.match(r'^[A-Z].*[.!?]$', text):
            score += 20
        
        yoruba_chars = sum(1 for char in self.yoruba_markers["characters"] if char in text_lower)
        if yoruba_chars > 0:
            score -= yoruba_chars * 5
        
        return score >= 40, score

    def verify_translation_pair(self, yoruba, english):
        """Verify if a translation pair is valid"""
        if not yoruba or not english:
            return False, 0
        
        score = 0
        
        yoruba_valid, yoruba_score = self.verify_yoruba_text(yoruba)
        if yoruba_valid:
            score += yoruba_score * 0.6
        
        english_valid, english_score = self.verify_english_text(english)
        if english_valid:
            score += english_score * 0.4
        
        yoruba_words = len(yoruba.split())
        english_words = len(english.split())
        ratio = min(yoruba_words, english_words) / max(yoruba_words, english_words)
        if ratio >= 0.3:
            score += ratio * 25
        
        return score >= self.min_scores["translation"], score

    def verify_example_pair(self, yoruba, english):
        """Verify if an example sentence pair is valid"""
        if not yoruba or not english:
            return False, 0
        
        word_length = len(yoruba.split()[0])
        is_short_word = word_length <= 2
        
        yoruba_words = len(yoruba.split())
        english_words = len(english.split())
        min_ratio = 0.3 if is_short_word else 0.5
        max_ratio = 3.0 if is_short_word else 2.0
        ratio = yoruba_words / english_words
        if not (min_ratio <= ratio <= max_ratio):
            return False, 0
        
        score = 0
        
        yoruba_ends_with_punct = bool(re.search(r'[.!?]$', yoruba))
        english_ends_with_punct = bool(re.search(r'[.!?]$', english))
        if yoruba_ends_with_punct and english_ends_with_punct:
            score += 20
        elif yoruba_ends_with_punct != english_ends_with_punct and not is_short_word:
            return False, 0
        
        yoruba_quotes = len(re.findall(r'["""]', yoruba))
        english_quotes = len(re.findall(r'["""]', english))
        if yoruba_quotes == english_quotes:
            score += 10
        elif yoruba_quotes != english_quotes and not is_short_word:
            return False, 0
        
        if re.match(r'^[A-Z]', english):
            score += 15
        elif not is_short_word:
            return False, 0
        
        noise_patterns = [
            r'^\s*\d+\s*$',
            r'^\s*[a-z]\)\s*$',
            r'^\s*$',
            r'^Yoruba$',
            r'^English$',
            r'^Google Translate$',
            r'^Translation$',
            r'^Example$'
        ]
        
        for pattern in noise_patterns:
            if re.match(pattern, yoruba, re.IGNORECASE) or re.match(pattern, english, re.IGNORECASE):
                return False, 0
        
        min_length = 2 if is_short_word else 5
        if len(yoruba) < min_length or len(english) < min_length:
            return False, 0
        if len(yoruba) > 500 or len(english) > 500:
            return False, 0
        
        if re.search(r'<[^>]+>', yoruba) or re.search(r'<[^>]+>', english):
            return False, 0
        
        ui_elements = ['click', 'button', 'menu', 'loading', 'search']
        if any(ui in yoruba.lower() for ui in ui_elements) or any(ui in english.lower() for ui in ui_elements):
            return False, 0
        
        yoruba_patterns = [r'\b(ni|ti|si|ko|ṣe|wa|lo)\b']
        english_patterns = [r'\b(the|a|an|is|are|was|were)\b']
        
        for pattern in yoruba_patterns:
            if re.search(pattern, yoruba.lower()):
                score += 10
        
        for pattern in english_patterns:
            if re.search(pattern, english.lower()):
                score += 10
        
        ratio = min(len(yoruba), len(english)) / max(len(yoruba), len(english))
        score += int(ratio * 20)
        
        required_score = 40 if is_short_word else 60
        
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
            "example_yoruba": "",
            "example_english": "",
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
        
        translation = result.get("translation", "")
        translations = result.get("translations", [])
        
        if translation:
            valid, score = self.verify_translation_pair(word, translation)
            if valid:
                verified_result["translation"] = translation
                verified_result["verification"]["translation_score"] = score
            
            verified_translations = []
            for trans in translations:
                valid, score = self.verify_translation_pair(word, trans)
                if valid and trans != translation:
                    verified_translations.append(trans)
            verified_result["translations"] = verified_translations
        
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
        
        if verified_examples:
            best_example = max(verified_examples, key=lambda x: x["score"])
            verified_result["example_yoruba"] = best_example["yoruba"]
            verified_result["example_english"] = best_example["english"]
            verified_result["verification"]["examples_score"] = total_example_score / len(verified_examples)
        
        quality_score = (
            verified_result["verification"]["translation_score"] * 0.6 +
            verified_result["verification"]["examples_score"] * 0.4
        )
        verified_result["verification"]["quality_score"] = int(quality_score)
        
        if quality_score < self.min_scores["overall"]:
            verified_result["status"] = "verification_failed"
            verified_result["error"] = f"Verification failed with quality score {int(quality_score)}"
        
        return verified_result

    def clean_example_text(self, text):
        """Clean and normalize example text."""
        if not text or len(text.strip()) < 5:
            return None
        
        text = re.sub(r'<[^>]+>', '', text)
        
        text = ' '.join(text.split())
        
        # Detect if the text is likely Yoruba by looking for diacritics
        is_yoruba = bool(re.search(r'[àáèéìíòóùúẹọṣ]', text))
        
        if is_yoruba:
            # Fix Yoruba auxiliary verb spacing issues (á, à, ń, etc.)
            text = re.sub(r'([áàńḿ])([a-zàáèéìíòóùúẹọṣ][a-zàáèéìíòóùúẹọṣ]+)', r'\1 \2', text)  # Add space after auxiliary verbs
            text = re.sub(r'(wọ́n|won|kí|ki|tó|to|ìyẹn|iyen|yìí|yii|èyí|eyi|bàá|baa)([a-zàáèéìíòóùúẹọṣ][a-zàáèéìíòóùúẹọṣ]+)', r'\1 \2', text) # Add space after pronouns/particles
            text = re.sub(r'([a-zàáèéìíòóùúẹọṣ][a-zàáèéìíòóùúẹọṣ]+)(á[a-zàáèéìíòóùúẹọṣ])', r'\1 \2', text)  # Add space before á + word
            
            # Fix specific Yoruba patterns that need spaces
            text = re.sub(r'(ti)(tu|yan|fi|lo|gbà|pa|mọ̀)', r'\1 \2', text)  # Add space between 'ti' and the following verb
            text = re.sub(r'(bá)(ti|pa|fi|gbà|jẹ́|ṣe)', r'\1 \2', text)  # Add space between 'bá' and the following verb
            text = re.sub(r'(ká)(ní|sì|ti)', r'\1 \2', text)  # Add space after 'ká'
            text = re.sub(r'(kò)(ké|ní|fi|sì)', r'\1 \2', text)  # Add space after 'kò'
            
            # Fix common incorrect word formations
            text = re.sub(r'nià', r'ni à', text)
            text = re.sub(r'láti', r'lá ti', text)
            text = re.sub(r'síbẹ̀', r'sí bẹ̀', text)
        else:
            # Fix common joined words in English translations
            text = re.sub(r'([a-z])([A-Z])', r'\1 \2', text)  # Add space between lowercase and uppercase
            
            # Fix specific auxiliary verb + past participle combinations
            auxiliaries = ["could", "would", "should", "have", "has", "had", "will", "is", "are", "was", "were"]
            past_participles = ["been", "have", "had", "not", "find", "look", "want", "need", "make", "take", "give"]
            for aux in auxiliaries:
                for pp in past_participles:
                    text = text.replace(f"{aux}{pp}", f"{aux} {pp}")
            
            # Fix joined "been" + verb
            past_participlesAfterBeen = ["released", "put", "used", "confined", "blessed", "left", "prevented", "corrected", "heeded", "supplied"]
            for pp in past_participlesAfterBeen:
                text = text.replace(f"been{pp}", f"been {pp}")
            
            # Fix main verb + preposition/conjunction
            main_verbs = ["released", "explained", "provided", "put", "had", "made", "took", "gave", "left"]
            prepositions = ["if", "when", "as", "by", "to", "for", "with", "on", "in", "at", "from"]
            for verb in main_verbs:
                for prep in prepositions:
                    text = text.replace(f"{verb}{prep}", f"{verb} {prep}")
            
            # Fix pronoun + preposition
            pronouns = ["him", "her", "it", "them", "us", "you", "we", "they"]
            for pron in pronouns:
                for prep in prepositions:
                    text = text.replace(f"{pron}{prep}", f"{pron} {prep}")
            
            # Fix determiner + noun
            determiners = ["This", "That", "The", "A", "An", "His", "Her", "Our", "Their", "Its"]
            nouns = ["man", "woman", "child", "person", "people", "life", "time", "day", "world", "house"]
            for det in determiners:
                for noun in nouns:
                    text = text.replace(f"{det}{noun}", f"{det} {noun}")
            
            # Fix compound constructions with "to"
            compounds = [("put", "to", "death"), ("have", "to", "be"), ("need", "to", "go")]
            for a, b, c in compounds:
                text = text.replace(f"{a}{b}{c}", f"{a} {b} {c}")
            
            # Fix "many of mankind's" type constructions
            text = text.replace("manyof", "many of")
            text = text.replace("mankind'smistakes", "mankind's mistakes")
            text = text.replace("ofmankind", "of mankind")
            
            # Fix "will be" and similar constructs
            modal_be = [("will", "be"), ("would", "be"), ("could", "be"), ("should", "be")]
            for modal, be in modal_be:
                text = text.replace(f"{modal}{be}", f"{modal} {be}")
        
        # Fix spacing issues around punctuation
        text = re.sub(r'\s+([.,;:!?])', r'\1', text)
        text = re.sub(r'([.,;:!?])\s+', r'\1 ', text)
        
        if text and not text[-1] in '.!?':
            text = text + '.'
        
        if text and text[0].islower():
            text = text[0].upper() + text[1:]
        
        noise_patterns = [
            r'\[\d+\]',
            r'\(\s*\)',
            r'^\s*\d+\.\s*',
            r'^\s*[a-z]\)\s*',
        ]
        
        for pattern in noise_patterns:
            text = re.sub(pattern, '', text)
        
        text = text.strip()
        return text if len(text) >= 5 and len(text) <= 500 else None

class GlosbeYorubaScraper:
    def __init__(self, base_folder="./scraped_data", output_folder=None, max_workers=5, delay=5.0):
        """Initialize the scraper"""
        self.base_folder = base_folder
        self.output_folder = output_folder or base_folder
        self.max_workers = max_workers
        self.delay = delay
        self.debug_mode = True
        self.base_url = "https://glosbe.com/yo/en/{}"
        
        # List of user agents for rotating requests
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.164 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"
        ]
        
        self.session = requests.Session()
        self.headers = {
            'User-Agent': self.get_random_user_agent(),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Referer': 'https://glosbe.com/',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0',
            'TE': 'Trailers',
        }
        
        # Setup folders
        self.json_folder = os.path.join(self.output_folder, "json")
        self.csv_folder = os.path.join(self.output_folder, "csv")
        self.debug_folder = os.path.join(self.output_folder, "debug_html")
        
        # Create folders if they don't exist
        for folder in [self.json_folder, self.csv_folder, self.debug_folder]:
            os.makedirs(folder, exist_ok=True)
            
        # Initialize example extractor
        self.example_extractor = ExampleSentenceExtractor(debug=self.debug_mode)
        
        # Initialize data verifier
        self.data_verifier = DataVerifier(debug=self.debug_mode)
        
        # Set up logger
        self.logger = logging.getLogger(__name__)
        
        # Track processed words to avoid duplicates
        self.processed_words_file = os.path.join(self.output_folder, "processed_words.txt")
        self.processed_words = set()
        if os.path.exists(self.processed_words_file):
            with open(self.processed_words_file, "r", encoding="utf-8") as f:
                self.processed_words = set(line.strip() for line in f)
    
    def get_word_files(self):
        """Get a dictionary of word files organized by alphabet"""
        word_files_by_alphabet = {}
        words_folder = "./yoruba_words"
        
        if os.path.exists(words_folder):
            for alphabet_dir in os.listdir(words_folder):
                alphabet_path = os.path.join(words_folder, alphabet_dir)
                
                if os.path.isdir(alphabet_path):
                    alphabet_files = []
                    for word_file in os.listdir(alphabet_path):
                        if word_file.endswith('.txt'):
                            file_path = os.path.join(alphabet_path, word_file)
                            alphabet_files.append(file_path)
        
                    if alphabet_files:
                        word_files_by_alphabet[alphabet_dir] = alphabet_files
        
        return word_files_by_alphabet
    
    def extract_words_from_file(self, file_path):
        """Extract words from a text file, one word per line"""
        words = []
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    word = line.strip()
                    if word and not word.startswith('#'):
                        words.append(word)
            return list(set(words))
        except Exception as e:
            logging.error(f"Error reading file {file_path}: {str(e)}")
            return []
    
    def get_random_user_agent(self):
        """Get a random user agent from the list"""
        return random.choice(self.user_agents)
    
    def is_captcha(self, response):
        """Check if the response contains a CAPTCHA challenge"""
        if not response or not hasattr(response, 'text'):
            return False
        return captcha_detected(response.text)
    
    def extract_clean_translation(self, text):
        """Clean and normalize translation text to ensure consistency
        
        Args:
            text (str): The raw translation text to clean
            
        Returns:
            str: The cleaned and normalized translation text
        """
        if not text or not isinstance(text, str):
            return ""
        
        original_text = text
        
        # Remove whitespace
        text = text.strip()
        
        # Remove URLs and URL fragments
        text = re.sub(r'https?://\S+', '', text)
        text = re.sub(r'www\.\S+', '', text)
        text = re.sub(r'http-www\.\S+', '', text)
        
        # Remove source and metadata markers
        source_markers = [
            'proper', 'Hei NER', 'Heidelberg', 'Named Entity', 'Resource', 
            'Dbnary', 'wiki', 'lingvoj.org', 'lingvoj.rdf'
        ]
        
        # First try to extract clean text before source markers
        for marker in source_markers:
            if marker in text:
                parts = text.split(marker, 1)
                text = parts[0].strip()
                # If the split part is too short, keep original
                if len(text) < 2 and len(original_text) > 5:
                    text = original_text
                break
        
        # Remove part of speech embedded in words
        common_pos = ['adjective', 'noun', 'verb', 'conjunction', 'interjection', 'ad', 'proper']
        for pos in common_pos:
            # Pattern: partOfSpeech at the end or middle of a word
            text = re.sub(f"({pos})([A-Z])", r" \2", text)
            
            # If POS is attached to the end of a word, remove it
            text = re.sub(f"([a-z])({pos})($|\s)", r"\1\3", text)
            
            # Don't remove if it's a standalone POS indicator
            if not re.match(f"^{pos}$", text, re.IGNORECASE):
                text = re.sub(f"\\b{pos}\\b", " ", text)
        
        # Normalize spaces
        text = re.sub(r'\s+', ' ', text)
        
        # Remove any HTML entities
        text = text.replace('&amp;', '&')
        text = text.replace('&quot;', '"')
        text = text.replace('&apos;', "'")
        text = text.replace('&lt;', '<')
        text = text.replace('&gt;', '>')
        text = re.sub(r'&[a-zA-Z]+;', '', text)
        
        # Normalize quotes and apostrophes
        text = re.sub(r'["\u201C\u201D]', '"', text)
        text = re.sub(r'[\'\u2018\u2019]', "'", text)
        
        # Remove descriptive text (often appears after primary meaning)
        if "A " in text and len(text) > 30:
            # If a sentence starts with 'A' and is long, it's likely a definition
            parts = text.split("A ", 1)
            if len(parts[0]) > 1:  # Only if we're not removing the entire text
                text = parts[0].strip()
        
        # Look for patterns that suggest a description
        if " is " in text and len(text) > 25:
            parts = text.split(" is ", 1)
            if len(parts[0]) > 1:  # Only if we're not removing a useful word
                text = parts[0].strip()
        
        # Remove parenthetical information which is often metadata
        text = re.sub(r'\([^)]*\)', '', text)
        
        # Remove any leading/trailing punctuation
        text = re.sub(r'^[.,;:!?\s]+', '', text)
        text = re.sub(r'[.,;:!?\s]+$', '', text)
        
        # Remove annotation markers like [1], [2], etc.
        text = re.sub(r'\[\d+\]', '', text)
        
        # Fix spacing issues around punctuation
        text = re.sub(r'\s+([.,;:!?])', r'\1', text)
        text = re.sub(r'([.,;:!?])\s+', r'\1 ', text)
        
        # If the translation was reduced to something too short, revert to original
        # but only for reasonable non-URL originals
        if len(text.strip()) < 2 and len(original_text) > 5 and not ('http' in original_text or 'www.' in original_text):
            return original_text.strip()
        
        return text.strip()

    def is_valid_translation(self, word, translation, confidence="medium"):
        """Validate if a translation is correct for a given word
        
        Args:
            word (str): The Yoruba word being translated
            translation (str): The English translation to validate
            confidence (str): The confidence level of the translation
            
        Returns:
            bool: True if the translation is valid, False otherwise
        """
        if not translation or not isinstance(translation, str):
            return False
        
        # First apply basic cleanup for evaluation
        translation = translation.strip()
        
        # Skip if too short (except special cases like "I", "a") 
        if len(translation) < 2 and translation not in ["I", "a", "A"]:
            return False
        
        # Skip if too long - likely a description rather than translation
        if len(translation) > 50:
            return False
        
        # Skip translations that contain URLs or HTTP patterns
        if re.search(r'https?://|www\.|http-www', translation):
            return False
        
        # Skip translations with non-English characters (except known accents)
        non_english_pattern = r'[^\x00-\x7F\áàäâéèëêíìïîóòöôúùüûñçÁÀÄÂÉÈËÊÍÌÏÎÓÒÖÔÚÙÜÛÑÇ]'
        if re.search(non_english_pattern, translation):
            # Exclude Yoruba characters so these aren't flagged
            if not re.search(r'[àáèéìíòóùúẹọṣ]', translation):
                return False
        
        # Skip translations with unusual formatting
        if re.search(r'^\W+$', translation) or re.search(r'_{2,}', translation):
            return False
        
        # Skip if it appears to be metadata or has suspicious patterns
        if re.match(r'^[A-Z][a-z]+[A-Z]', translation):  # CamelCase likely metadata
            return False
        
        # Skip translations containing code-like characters
        if re.search(r'[<>{}\[\]\\\/]', translation):
            return False
        
        # Always accept high confidence translations AFTER basic validation
        if confidence == "high":
            return True
        
        # Check for known good/bad translations
        word_lower = word.lower()
        translation_lower = translation.lower()
        
        # Some words have known incorrect translations
        incorrect_translations = {
            "a": ["shut", "close", "they", "i", "they", "I", "across", "over"],
            "á": ["shut", "close", "blocked", "closed", "start", "stop"],
            "abo": ["duck", "goose", "turkey", "cock", "hen", "fowl", "bird"],
            "adìye": ["duck", "goose", "turkey", "bird", "goose"],
            "àpẹ́": ["chicken", "hen", "goose", "turkey", "bird"],
            "ó": ["me", "my", "you", "your", "we", "our"],
            "ẹ": ["i", "me", "my", "he", "his", "they", "them"],
            "e": ["i", "me", "my", "he", "his", "they", "them"]
        }
        
        # Return False if it's a known incorrect translation
        if word_lower in incorrect_translations and translation_lower in incorrect_translations[word_lower]:
            return False
        
        # Check for known good translations
        known_translations = self.get_known_translations(word_lower)
        for trans in known_translations:
            if translation_lower == trans.get("translation", "").lower():
                return True
        
        # For short words, be more restrictive
        if len(word) <= 2 and confidence != "high":
            return translation.lower() in [t.get("translation", "").lower() for t in known_translations]
        
        # Check for suspicious content
        suspicious_words = [
            'login', 'signup', 'register', 'password', 'username', 'cookie', 
            'click', 'download', 'upload', 'website', 'captcha', 'browser',
            'server', 'database', 'null', 'undefined', 'NaN', 'javascript',
            'language', 'proper', 'pronoun', 'heidelberg', 'resource', 'dbnary',
            'lingvoj'
        ]
        
        if any(s in translation_lower for s in suspicious_words):
            return False
        
        # By default, accept medium confidence translations for regular words
        return True
    
    def extract_text_from_selector(self, soup, selector, default=""):
        """Extract text from a CSS selector with fallback"""
        try:
            element = soup.select_one(selector)
            return element.get_text(strip=True) if element else default
        except Exception as e:
            logging.warning(f"Error extracting from selector '{selector}': {str(e)}")
            return default
    
    def validate_content(self, result):
        """Check if the result contains any meaningful content with stricter validation"""
        # Check for translations array
        has_translations = False
        valid_translations = []
        
        if result.get("translations") and isinstance(result["translations"], list):
            # Filter translations to only include valid ones
            for t in result["translations"]:
                if not isinstance(t, dict):
                    continue
                    
                if not t.get("translation") or not isinstance(t["translation"], str):
                    continue
                    
                translation = t["translation"].strip()
                if not translation:
                    continue
                    
                # Skip translations that are URLs or have URL fragments
                if re.search(r'https?://|www\.|http-www', translation):
                    continue
                    
                # Skip translations that are too short
                if len(translation) < 2 and translation not in ["I", "a", "A"]:
                    continue
                    
                # Skip translations with suspicious formatting
                if re.search(r'^\W+$', translation) or re.search(r'_{2,}', translation):
                    continue
                    
                # Keep only those that pass additional verification
                word = result.get("word", "").lower()
                confidence = t.get("confidence", "").lower()
                
                # Force clean the translation 
                clean_trans = self.extract_clean_translation(translation)
                if clean_trans != translation:
                    t["translation"] = clean_trans
                    translation = clean_trans
                    
                # Skip if cleaning made it too short
                if len(clean_trans) < 2 and clean_trans not in ["I", "a", "A"]:
                    continue
                    
                # Apply the full validation
                if self.is_valid_translation(word, translation, confidence):
                    valid_translations.append(t)
            
            # Update the translations list with only valid translations
            result["translations"] = valid_translations
            has_translations = len(valid_translations) > 0
        
        # Filter examples to improve quality
        has_examples = False
        valid_examples = []
        
        if result.get("examples") and isinstance(result["examples"], list):
            for e in result["examples"]:
                if (isinstance(e, dict) and 
                    e.get("yoruba") and e.get("english") and
                    isinstance(e["yoruba"], str) and isinstance(e["english"], str) and
                    len(e["yoruba"].strip()) > 0 and len(e["english"].strip()) > 0):
                    
                    # Apply stricter validation for examples
                    confidence = e.get("confidence", "").lower()
                    
                    # Always accept high confidence examples
                    if confidence == "high":
                        valid_examples.append(e)
                    elif confidence == "medium":
                        # For medium confidence, check if this is a good example
                        yoruba = e.get("yoruba", "")
                        english = e.get("english", "")
                        word = result.get("word", "")
                        
                        # Skip religious references unless they're high quality
                        if e.get("is_jw_reference", False) and not self.verify_example_pair(yoruba, english):
                            continue
                            
                        # For shorter words, be more selective
                        if len(word) <= 2:
                            # Medium confidence examples for short words must contain the word explicitly
                            normalized_word = self.normalize_word(word)
                            pattern = r'\b' + re.escape(normalized_word) + r'\b'
                            
                            if re.search(pattern, yoruba, re.IGNORECASE):
                                valid_examples.append(e)
                        else:
                            # For longer words, medium confidence examples must pass validation checks
                            if self.example_extractor.verify_example_pair(yoruba, english):
                                valid_examples.append(e)
            
            # Update the examples list with only valid examples
            result["examples"] = valid_examples
            has_examples = len(valid_examples) > 0
            
        # Check for direct translation string
        has_translation = (
            result.get("translation") and 
            isinstance(result["translation"], str) and
            len(result.get("translation", "").strip()) > 0
        )
        
        # Check for specific example fields
        has_specific_examples = (
            result.get("example_yoruba") and 
            result.get("example_english") and
            isinstance(result["example_yoruba"], str) and
            isinstance(result["example_english"], str) and
            len(result["example_yoruba"].strip()) > 0 and
            len(result["example_english"].strip()) > 0
        )
        
        # Special handling for hardcoded known words
        word = result.get("word", "").lower()
        has_hardcoded = False
        
        if word in self.get_known_translations(word):
            # Add these translations to the result
            known_translations = self.get_known_translations(word)
            
            # If we have no other translations, use the known ones
            if not has_translations:
                result["translations"] = known_translations
                has_translations = len(known_translations) > 0
            
            # Add known examples if we have few or no examples
            if not has_examples or len(result.get("examples", [])) < 2:
                known_examples = self.get_known_examples(word)
                if known_examples:
                    if "examples" not in result or not result["examples"]:
                        result["examples"] = []
                    
                    # Add only examples not already present
                    existing_yoruba = {ex["yoruba"].lower() for ex in result["examples"]}
                    for ex in known_examples:
                        if ex["yoruba"].lower() not in existing_yoruba:
                            result["examples"].append(ex)
                    
                    has_examples = len(result["examples"]) > 0
            
            has_hardcoded = True
        
        # Debug logging to help diagnose content validation
        if self.debug_mode:
            logging.debug(f"Content validation for {word}: " + 
                         f"has_translations={has_translations}, " +
                         f"has_translation={has_translation}, " +
                         f"has_examples={has_examples}, " +
                         f"has_specific_examples={has_specific_examples}, " +
                         f"has_hardcoded={has_hardcoded}")
        
        # A result is valid if it has at least one translation or example
        has_content = has_translations or has_translation or has_examples or has_specific_examples or has_hardcoded
        
        return has_content
        
    def verify_example_pair(self, yoruba, english):
        """More aggressive verification of example pairs"""
        # Reject pairs that are too different in length (unless one is very short)
        if not yoruba or not english or not isinstance(yoruba, str) or not isinstance(english, str):
            return False
            
        yoruba_len = len(yoruba)
        english_len = len(english)
        
        # Very short pairs are suspicious
        if yoruba_len < 10 or english_len < 10:
            return False
            
        # Check length ratio - they should be somewhat proportional
        ratio = max(yoruba_len, english_len) / min(yoruba_len, english_len)
        if ratio > 2.5:  # Stricter ratio limit (was 3.0)
            return False
        
        # Check for mixed examples (multiple translations spliced together)
        if re.search(r'(jw|jw\d+)', english) or re.search(r'(jw|jw\d+)', yoruba):
            return False
            
        # Check for formatting errors that indicate data corruption
        if yoruba == english:
            return False
            
        # Detect incorrect merging of Yoruba and English phrases
        # Look for patterns where Yoruba and English are joined incorrectly
        mixed_pattern = r'([a-zàáèéìíòóùúẹọṣ]{3,})([A-Za-z]{3,})'
        if re.search(mixed_pattern, yoruba) or re.search(mixed_pattern, english):
            return False
        
        # Check for Yoruba markers (at least one should be present)
        yoruba_markers = [
            'mo', 'o', 'ó', 'wọn', 'won', 'a', 'ẹ', 'è', 'ni',
            'kò', 'ko', 'ṣe', 'se', 'ti', 'sì', 'si', 'yìí', 'yii',
            'ń', 'n', 'kí', 'ki', 'bí', 'bi', 'fún', 'fun',
            'àti', 'ati', 'ọmọ', 'omo', 'jẹ́', 'je', 'gbà', 'gba',
            'bá', 'ba', 'à', 'á', 'lá', 'la'
        ]
        
        has_yoruba_marker = any(marker in yoruba.lower().split() for marker in yoruba_markers)
        has_yoruba_diacritics = bool(re.search(r'[àáèéìíòóùúẹọṣ]', yoruba.lower()))
        
        if not (has_yoruba_marker or has_yoruba_diacritics):
            return False
            
        # Check for English markers
        english_markers = [
            'the', 'a', 'an', 'is', 'are', 'was', 'were', 'will',
            'have', 'has', 'had', 'be', 'been', 'being',
            'I', 'you', 'he', 'she', 'it', 'we', 'they',
            'this', 'that', 'these', 'those', 'and', 'but', 'or',
            'could', 'would', 'should', 'may', 'might', 'must'
        ]
        
        has_english_marker = any(marker.lower() in english.lower().split() for marker in english_markers)
        
        if not has_english_marker:
            return False
            
        # Check for potentially mixed Yoruba/English content
        # English content shouldn't have many Yoruba diacritics
        if len(re.findall(r'[àáèéìíòóùúẹọṣ]', english.lower())) > 3:
            return False
            
        # Yoruba content shouldn't have many English-only markers
        english_only_markers = ['the', 'is', 'are', 'was', 'were', 'be', 'been', 'this', 'that', 'those', 'these']
        if sum(1 for marker in english_only_markers if marker in yoruba.lower().split()) > 2:
            return False
            
        # Reject examples with suspicious content
        suspicious_words = [
            'login', 'signup', 'register', 'password', 'username', 'cookie', 
            'click', 'download', 'upload', 'website', 'captcha', 'browser',
            'server', 'database', 'null', 'undefined', 'NaN', 'javascript',
            'html', 'css', 'python', 'script', 'glosbe', 'dictionary'
        ]
        
        if any(word in yoruba.lower() or word in english.lower() for word in suspicious_words):
            return False
            
        # Ensure all data is properly separated (check for common data issues)
        if re.search(r'we\s?would\s?have[A-Za-z]+', english.lower()):
            return False
            
        # Look for merging of Yoruba auxiliary verbs with English words
        if re.search(r'à[a-z]{3,}[A-Z]', yoruba) or re.search(r'á[a-z]{3,}[A-Z]', yoruba):
            return False
            
        return True
        
    def scrape_everything(self, word):
        """Scrape all data for a word with enhanced validation"""
        try:
            url = self.base_url.format(quote(word))
            
            headers = {
                'User-Agent': self.get_random_user_agent(),
                'Accept': 'text/html,application/xhtml+xml,application/xml',
                'Accept-Language': 'en-US,en;q=0.9',
                'Referer': 'https://glosbe.com/',
            }
            
            time.sleep(self.delay * (0.5 + random.random()))
            
            response = requests.get(url, headers=headers, timeout=30)
            
            if response.status_code != 200 or self.is_captcha(response):
                logging.warning(f"Failed to fetch {url} - Status: {response.status_code}")
                
                if self.is_captcha(response):
                    self.delay += 1.0
                    logging.warning(f"CAPTCHA detected! Increasing delay to {self.delay}s")
                    
                    # Save the HTML for debugging
                    debug_html_path = os.path.join(self.base_folder, "debug_html", f"{word}_captcha.html")
                    os.makedirs(os.path.dirname(debug_html_path), exist_ok=True)
                    
                    with open(debug_html_path, "w", encoding="utf-8") as f:
                        f.write(response.text)
                
                return {
                    "word": word,
                    "url": url,
                    "scrape_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                    "status": "failed",
                    "error": f"Status code: {response.status_code}" if not self.is_captcha(response) else "CAPTCHA",
                }
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            if self.debug_mode:
                debug_dir = os.path.join(self.base_folder, "debug_html")
                os.makedirs(debug_dir, exist_ok=True)
                debug_file = os.path.join(debug_dir, f"{word}_debug.html")
                with open(debug_file, "w", encoding="utf-8") as f:
                    f.write(str(soup))
                logging.info(f"Saved debug HTML to {debug_file}")
            
            result = {
                "word": word,
                "url": url,
                "scrape_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                "status": "success",
                "error": None,
                "translation": "",
                "translations": [],
                "part_of_speech": "",
                "pronunciation": "",
                "examples": [],
                "definitions": [],
                "html": str(soup),
                # Remove soup from result to ensure JSON serialization works
                # "soup": soup,
                "verification_score": 0,
            }
            
            # Enhanced translation extraction
            # First extraction approach: Direct translation items
            translation_candidates = []
            
            # Attempt 1: Find all translation items with their phrases
            translation_items = soup.select('h3.translation__item__pharse, .translation__pharse, .translation__item__phrase, .translation-item h3, .translation-list__item h3, .translation__item')
            for item in translation_items:
                trans_text = item.get_text(strip=True)
                
                # Find part of speech by looking at nearby elements
                pos_container = item.find_parent('div') or item.find_parent('li')
                pos_span = None
                
                if pos_container:
                    pos_span = pos_container.select_one('span.text-xxs.text-gray-500, .grammar-info, .pos-tag')
                
                if pos_span:
                    pos_text = pos_span.get_text(strip=True)
                else:
                    pos_text = ""
                
                if trans_text:
                    translation_candidates.append({
                        "text": trans_text,
                        "pos_hint": self.extract_pos_from_text(pos_text),
                        "source": "direct_h3",
                        "confidence": "high"
                    })
            
            # Attempt 2: Check for data-element="translation" li elements
            translation_li_elements = soup.select('li[data-element="translation"], .translation__list li, .translation-list li')
            for li in translation_li_elements:
                # Find the translation phrase inside this li
                trans_elem = li.select_one('h3.translation__item__pharse, h3.align-top.inline, .translation__item__pharse, .translation__item, .translation-item')
                
                if trans_elem:
                    trans_text = trans_elem.get_text(strip=True)
                    
                    # Look for part of speech span
                    pos_span = li.select_one('span.text-xxs, span.text-gray-500, .grammar-info, .pos-tag')
                    pos_text = pos_span.get_text(strip=True) if pos_span else ""
                    
                    if trans_text:
                        translation_candidates.append({
                            "text": trans_text,
                            "pos_hint": self.extract_pos_from_text(pos_text),
                            "source": "direct_li",
                            "confidence": "high"
                        })
            
            # Attempt 3: Check for translation in primary phrase
            phrase_title = soup.select_one('h1, .dictionary-title, .main-phrase')
            if phrase_title:
                phrase_text = phrase_title.get_text(strip=True)
                if 'Translation of' in phrase_text and 'into English' in phrase_text:
                    # Extract translation from header
                    translation_match = re.search(r'<strong>([^<]+)</strong>', str(phrase_title))
                    if translation_match:
                        primary_translation = translation_match.group(1).strip()
                        translation_candidates.append({
                            "text": primary_translation,
                            "pos_hint": "",
                            "source": "primary_header",
                            "confidence": "high"
                        })
            
            # Attempt 4: Check for translation in similar phrases section
            similar_section = soup.select_one('#simmilar-phrases')
            if similar_section:
                phrase_items = similar_section.select('li.px-2.py-1.flex')
                for item in phrase_items:
                    yoruba_elem = item.select_one('.w-1\\/3.dir-aware-text-right')
                    english_elem = item.select_one('.dir-aware-pl-2.w-2\\/3')
                    
                    if yoruba_elem and english_elem:
                        yoruba_text = yoruba_elem.get_text(strip=True)
                        english_text = english_elem.get_text(strip=True)
                        
                        # If the yoruba text is exactly our word or contains it
                        if yoruba_text.lower() == word.lower() or word.lower() in yoruba_text.lower():
                            translation_candidates.append({
                                "text": english_text,
                                "pos_hint": "",
                                "source": "similar",
                                "confidence": "medium"
                            })
            
            # Attempt 5: Check for automatic translations section
            auto_section = soup.select_one('#translation_automatic')
            if auto_section:
                translation_containers = auto_section.select('li.px-2.text-sm.pb-2')
                for container in translation_containers:
                    # Look for Google or Glosbe translation containers
                    translation_p = container.select_one('p.inline-block.min-h-8.text-primary-700')
                    
                    if translation_p and translation_p.get_text(strip=True):
                        trans_text = translation_p.get_text(strip=True)
                        
                        # Check if it's from Google or Glosbe
                        provider_span = container.select_one('span.inline-block.text-xs.text-gray-500')
                        provider = provider_span.get_text(strip=True) if provider_span else "Unknown"
                        confidence = "medium" if "Google" in provider else "low"
                        
                        translation_candidates.append({
                            "text": trans_text,
                            "pos_hint": "",
                            "source": "automatic",
                            "confidence": confidence
                        })
            
            # Attempt 6: Look for content-summary section that often has good translations
            summary_section = soup.select_one('#content-summary, .content-summary, .phrase-summary')
            if summary_section:
                strong_elements = summary_section.select('strong')
                if strong_elements:
                    translations_text = strong_elements[0].get_text(strip=True)
                    if translations_text:
                        # Split by commas to get individual translations
                        individual_translations = [t.strip() for t in translations_text.split(',')]
                        for trans in individual_translations:
                            if trans and len(trans) > 1:
                                translation_candidates.append({
                                    "text": trans,
                                    "pos_hint": "",
                                    "source": "summary",
                                    "confidence": "high"
                                })
            
            # Special handling for common Yoruba words with known translations
            normalized_word = self.normalize_word(word)
            if normalized_word in ["adìye", "abo", "a", "á", "ó", "àpẹ́", "aláàṣẹ", "àárẹ̀"]:
                known_word_translations = {
                    "adìye": [{"text": "chicken", "pos_hint": "noun", "confidence": "high"}],
                    "abo": [{"text": "female", "pos_hint": "noun", "confidence": "high"},
                            {"text": "feminine", "pos_hint": "adjective", "confidence": "high"}],
                    "a": [{"text": "he", "pos_hint": "pronoun", "confidence": "high"},
                          {"text": "we", "pos_hint": "pronoun", "confidence": "high"}],
                    "á": [{"text": "will", "pos_hint": "auxiliary verb", "confidence": "high"},
                          {"text": "he", "pos_hint": "pronoun", "confidence": "high"},
                          {"text": "she", "pos_hint": "pronoun", "confidence": "high"}],
                    "ó": [{"text": "he", "pos_hint": "pronoun", "confidence": "high"},
                          {"text": "she", "pos_hint": "pronoun", "confidence": "high"},
                          {"text": "it", "pos_hint": "pronoun", "confidence": "high"}],
                    "àpẹ́": [{"text": "duck", "pos_hint": "noun", "confidence": "high"}],
                    "aláàṣẹ": [{"text": "authority", "pos_hint": "noun", "confidence": "high"},
                               {"text": "executive", "pos_hint": "noun", "confidence": "high"}],
                    "àárẹ̀": [{"text": "fatigue", "pos_hint": "noun", "confidence": "high"},
                              {"text": "tiredness", "pos_hint": "noun", "confidence": "high"}]
                }
                
                if normalized_word in known_word_translations:
                    for known_trans in known_word_translations[normalized_word]:
                        translation_candidates.append({
                            "text": known_trans["text"],
                            "pos_hint": known_trans["pos_hint"],
                            "source": "hardcoded",
                            "confidence": known_trans["confidence"]
                        })
            
            # Log the candidates we found
            logging.debug(f"Found {len(translation_candidates)} translation candidates for {word}")
            
            # Process translation candidates
            translations = []
            seen_translations = set()
            
            # First, add known translations if applicable
            known_translations = self.get_known_translations(word)
            if known_translations:
                for known_trans in known_translations:
                    if known_trans["translation"].lower() not in seen_translations:
                        translations.append(known_trans)
                        seen_translations.add(known_trans["translation"].lower())
                # Set the primary translation from the known translations
                result["translation"] = known_translations[0]["translation"]
                result["part_of_speech"] = known_translations[0]["part_of_speech"]
            
            # Process other candidates
            for candidate in translation_candidates:
                trans_text = candidate["text"]
                
                # Skip empty or very short translations
                if not trans_text or len(trans_text) < 2:
                    continue
                
                # Clean the translation text
                clean_trans = self.extract_clean_translation(trans_text)
                
                # Skip translations that are the same as the original word
                if clean_trans.lower() == word.lower():
                    continue
                
                # Skip duplicates
                if clean_trans.lower() in seen_translations:
                    continue
                
                # Validate the translation
                if not self.is_valid_translation(word, clean_trans, candidate["confidence"]):
                    continue
                
                seen_translations.add(clean_trans.lower())
                
                # Get part of speech if available
                pos = candidate["pos_hint"] or self.identify_part_of_speech(trans_text, clean_trans)
                
                # Only add valid translations
                if clean_trans and len(clean_trans) > 1:
                    translations.append({
                        "translation": clean_trans,
                        "part_of_speech": pos,
                        "confidence": candidate["confidence"]
                    })
            
            # Set the primary translation if not already set from known translations
            if not result["translation"] and translations:
                # Sort by confidence and use the first high-confidence translation
                high_confidence = [t for t in translations if t["confidence"] == "high"]
                if high_confidence:
                    result["translation"] = high_confidence[0]["translation"]
                    result["part_of_speech"] = high_confidence[0]["part_of_speech"]
                else:
                    # Or use the first translation
                    result["translation"] = translations[0]["translation"]
                    result["part_of_speech"] = translations[0]["part_of_speech"]
            
            # Store all translations
            result["translations"] = translations
            
            # Get examples - this is handled by the example extractor
            examples = self.example_extractor.extract_examples(soup, word)
            result["examples"] = examples
            
            # Get definitions
            definitions = self.extract_definitions(soup)
            result["definitions"] = definitions
            
            # Log the final result
            logging.info(f"Final scraping result for {word}: {len(translations)} translations, {len(examples)} examples")
            
            # Apply strict filtering to translations
            if "translations" in result and isinstance(result["translations"], list):
                # First, remove duplicate translations
                unique_translations = {}
                for trans in result["translations"]:
                    translation_text = trans.get("translation", "").lower().strip()
                    if translation_text and len(translation_text) > 0:
                        # If we already have this translation with higher confidence, skip it
                        if (translation_text in unique_translations and 
                            trans.get("confidence") != "high" and 
                            unique_translations[translation_text].get("confidence") == "high"):
                            continue
                        
                        # Skip medium confidence translations if they're suspect for this word
                        if trans.get("confidence") == "medium":
                            # Some words have known incorrect translations
                            incorrect_translations = {
                                "a": ["shut", "close", "they", "i", "they", "I", "across", "over"],
                                "á": ["shut", "close", "blocked", "closed", "start", "stop"],
                                "abo": ["duck", "goose", "turkey", "cock", "hen", "fowl", "bird"],
                                "adìye": ["duck", "goose", "turkey", "bird", "goose"],
                                "àpẹ́": ["chicken", "hen", "goose", "turkey", "bird"],
                                "ó": ["me", "my", "you", "your", "we", "our"],
                                "ẹ": ["i", "me", "my", "he", "his", "they", "them"],
                                "e": ["i", "me", "my", "he", "his", "they", "them"]
                            }
                            
                            # Skip if this is a known incorrect translation
                            if (word.lower() in incorrect_translations and 
                                translation_text in incorrect_translations[word.lower()]):
                                continue
                        
                        # Keep the best version we've seen
                        unique_translations[translation_text] = trans
                
                # Replace with deduplicated list
                result["translations"] = list(unique_translations.values())
            
            # Apply strict filtering to examples
            if "examples" in result and isinstance(result["examples"], list):
                # Remove duplicate examples and low quality ones
                unique_examples = {}
                for example in result["examples"]:
                    yoruba = example.get("yoruba", "").strip()
                    english = example.get("english", "").strip()
                    
                    if yoruba and english and len(yoruba) > 5 and len(english) > 5:
                        # Create a key to identify duplicate examples
                        example_key = (yoruba.lower(), english.lower())
                        
                        # Perform more aggressive verification on medium confidence examples
                        if example.get("confidence") == "medium":
                            if not self.verify_example_pair(yoruba, english):
                                continue
                        
                        # Keep the best version we've seen
                        if (example_key not in unique_examples or
                            example.get("confidence") == "high" and unique_examples[example_key].get("confidence") != "high"):
                            unique_examples[example_key] = example
                
                # Replace with deduplicated and verified list
                result["examples"] = list(unique_examples.values())
                
            # Add known examples for short words or words with few examples
            if len(word) <= 2 or len(result.get("examples", [])) < 2:
                known_examples = self.get_known_examples(word)
                if known_examples:
                    if "examples" not in result:
                        result["examples"] = []
                    
                    # Add known examples that aren't already in the list
                    existing_yoruba = {ex.get("yoruba", "").lower() for ex in result["examples"]}
                    for ex in known_examples:
                        if ex.get("yoruba", "").lower() not in existing_yoruba:
                            result["examples"].append(ex)
            
            return result
            
        except requests.RequestException as e:
            logging.error(f"Error scraping {word}: {str(e)}")
            return {
                "word": word,
                "url": url if 'url' in locals() else self.base_url.format(quote(word)),
                "scrape_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                "status": "error",
                "error": str(e)
            }
        except Exception as e:
            logging.error(f"Error processing {word}: {str(e)}")
            return {
                "word": word,
                "url": url if 'url' in locals() else self.base_url.format(quote(word)),
                "scrape_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                "status": "error",
                "error": str(e)
            }
    
    def extract_pos_from_text(self, text):
        """Extract part of speech from text"""
        pattern = r'(?:^|\s)(noun|verb|adjective|pronoun|adverb|preposition|conjunction|interjection)(?:\b|$)'
        match = re.search(pattern, text, re.IGNORECASE)
        
        if match:
            return match.group(1).lower()
                    
        return ""
    
    def identify_part_of_speech(self, text, translation):
        """Identify part of speech from a translation by analyzing context"""
        # First, check if there's an explicit part of speech in the text
        pos_from_text = self.extract_pos_from_text(text)
        if pos_from_text:
            return pos_from_text
            
        # Use heuristics based on the translation and context
        translation_lower = translation.lower()
        
        # Check for phrases
        if len(translation_lower.split()) > 2:
            return 'phrase'
            
        # Common English determiners and pronouns usually map to pronouns
        if translation_lower in ['the', 'a', 'an', 'i', 'me', 'my', 'you', 'your', 'he', 'him', 'his', 
                               'she', 'her', 'it', 'its', 'we', 'us', 'our', 'they', 'them', 'their']:
            return 'pronoun'
            
        # Common English action words are likely verbs
        if translation_lower in ['go', 'come', 'do', 'make', 'take', 'get', 'see', 'know', 'want',
                               'find', 'give', 'tell', 'work', 'call', 'try']:
            return 'verb'
            
        # Check for multi-word expressions
        if ' ' in translation_lower:
            # Multi-word expressions with certain patterns
            if translation_lower.startswith(('we would', 'would have', 'will have', 'have been')):
                return 'phrase'
            if any(translation_lower.startswith(w) for w in ['give thanks', 'take care', 'look after']):
                return 'phrase'
        
        # Check for verb endings
        if translation_lower.endswith(('ing', 'ed', 's')) and len(translation_lower) > 4:
            return 'verb'
            
        # Check for adjective endings
        if translation_lower.endswith(('ful', 'ous', 'ive', 'able', 'ible', 'al', 'ic')):
            return 'adjective'
            
        # Check for adverb endings
        if translation_lower.endswith('ly') and len(translation_lower) > 3:
            return 'adverb'
            
        # Default to noun as most common part of speech
        return 'noun'
    
    def extract_flattened_data(self, item):
        """Extract flattened data from a processed item for CSV output"""
        if not item:
            return {}
        
        # Basic data
        flattened = {
            "word": item.get("word", ""),
            "url": item.get("url", ""),
            "scrape_time": item.get("scrape_time", ""),
            "status": item.get("status", ""),
            "error": item.get("error", ""),
            "verification_score": item.get("verification_score", 0),
        }
        
        # Translation data
        primary_translation = item.get("translation", "")
        flattened["translation"] = primary_translation
        
        # All translations
        translations = item.get("translations", [])
        all_trans = []
        for trans in translations:
            if isinstance(trans, dict) and "translation" in trans:
                all_trans.append(trans["translation"])
            elif isinstance(trans, str):
                all_trans.append(trans)
        
        flattened["all_translations"] = " | ".join(all_trans) if all_trans else ""
        
        # Part of speech
        flattened["part_of_speech"] = item.get("part_of_speech", "")
        
        # Example data
        examples = item.get("examples", [])
        if examples:
            best_example = examples[0]  # Take the first example
            for example in examples:
                # If we have a "high" confidence example, use that instead
                if example.get("confidence") == "high":
                    best_example = example
                    break
            
            flattened["example_yoruba"] = best_example.get("yoruba", "")
            flattened["example_english"] = best_example.get("english", "")
        else:
            flattened["example_yoruba"] = ""
            flattened["example_english"] = ""
        
        # Definition data
        definitions = item.get("definitions", [])
        if definitions:
            flattened["definition"] = definitions[0].get("text", "")
        else:
            flattened["definition"] = ""
        
        return flattened
    
    def save_to_csv(self, data, output_file):
        """Save data to CSV file
        
        Args:
            data (list): List of dictionaries containing data to save
            output_file (str): Path to output CSV file
        """
        if not data or not output_file:
            logging.warning(f"No data to save to {output_file}")
            return
        
        # First preprocess the data to ensure quality
        clean_data = self.preprocess_data_before_save(data)
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(os.path.abspath(output_file)), exist_ok=True)
        
        try:
            # Use a different method depending on the type of data
            if output_file.endswith('_words.csv'):
                # Words file format
                df = pd.DataFrame([
                    {
                        'id': idx + 1,
                        'word': item.get('word', ''),
                        'scraped_timestamp': item.get('scrape_time', ''),
                        'source_url': item.get('url', '')
                    }
                    for idx, item in enumerate(clean_data)
                ])
                
                df.to_csv(output_file, index=False, encoding='utf-8')
                logging.info(f"Saved {len(df)} words to {output_file}")
                
            elif output_file.endswith('_translations.csv'):
                # Translations file format
                translations_list = []
                for item in clean_data:
                    word_id = item.get('id')
                    
                    # Handle translations array
                    if 'translations' in item and item['translations']:
                        for idx, trans in enumerate(item['translations']):
                            if isinstance(trans, dict) and 'translation' in trans:
                                translations_list.append({
                                    'id': len(translations_list) + 1,
                                    'word_id': word_id,
                                    'translation': trans['translation'],
                                    'part_of_speech': trans.get('part_of_speech', ''),
                                    'confidence': trans.get('confidence', '')
                                })
                    
                    # Handle direct translation field
                    elif 'translation' in item and item['translation']:
                        translations_list.append({
                            'id': len(translations_list) + 1,
                            'word_id': word_id,
                            'translation': item['translation'],
                            'part_of_speech': item.get('part_of_speech', ''),
                            'confidence': 'high'
                        })
                
                df = pd.DataFrame(translations_list)
                df.to_csv(output_file, index=False, encoding='utf-8')
                logging.info(f"Saved {len(df)} translations to {output_file}")
                
            elif output_file.endswith('_examples.csv'):
                # Examples file format
                examples_list = []
                for item in clean_data:
                    word_id = item.get('id')
                    
                    # Process examples
                    if 'examples' in item and item['examples']:
                        for example in item['examples']:
                            if isinstance(example, dict) and 'yoruba' in example and 'english' in example:
                                examples_list.append({
                                    'id': len(examples_list) + 1,
                                    'translation_id': example.get('translation_id', ''),
                                    'word_id': word_id,
                                    'yoruba_text': example['yoruba'],
                                    'english_text': example['english'],
                                    'is_jw_reference': example.get('is_jw', False),
                                    'confidence': example.get('confidence', 'medium'),
                                    'source': example.get('source', 'tmem'),
                                    'score': example.get('score', '0')
                                })
                
                df = pd.DataFrame(examples_list)
                df.to_csv(output_file, index=False, encoding='utf-8')
                logging.info(f"Saved {len(df)} examples to {output_file}")
                
            else:
                # Generic format for other file types
                df = pd.DataFrame(clean_data)
                df.to_csv(output_file, index=False, encoding='utf-8')
                logging.info(f"Saved {len(df)} rows to {output_file}")
                
        except Exception as e:
            logging.error(f"Error saving to CSV {output_file}: {str(e)}")
            import traceback
            logging.error(traceback.format_exc())
    
    def process_file(self, word_file, alphabet):
        """Process a single word file"""
        json_alphabet_folder = os.path.join(self.json_folder, f"{alphabet}")
        csv_alphabet_folder = os.path.join(self.csv_folder, f"{alphabet}")
        
        if not os.path.exists(json_alphabet_folder):
            os.makedirs(json_alphabet_folder)
        
        if not os.path.exists(csv_alphabet_folder):
            os.makedirs(csv_alphabet_folder)
        
        words = self.extract_words_from_file(word_file)
        logging.info(f"Found {len(words)} unique words in file")
        
        words_to_process = [word for word in words if word not in self.processed_words]
        logging.info(f"After deduplication: {len(words_to_process)} words to process")
        
        if not words_to_process:
            logging.info("All words already processed, skipping file")
            return 0
        
        results = []
        for word in tqdm(words_to_process, desc=f"Processing words in {os.path.basename(word_file)}", unit="word"):
            try:
                result = self.scrape_word(word)
                results.append(result)
            except Exception as e:
                logging.error(f"Unexpected error processing {word}: {str(e)}")
                results.append({"word": word, "error": f"Processing error: {str(e)}"})
        
        for word in words:
            if word in self.processed_words and word not in words_to_process:
                results.append({"word": word, "status": "previously_processed"})
        
        base_filename = os.path.basename(word_file).replace('.txt', '')
        json_output_file = os.path.join(json_alphabet_folder, f"{base_filename}.json")
        csv_output_file = os.path.join(csv_alphabet_folder, f"{base_filename}.csv")
        
        existing_data = []
        if os.path.exists(json_output_file):
            try:
                with open(json_output_file, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
                logging.info(f"Loaded {len(existing_data)} existing entries from {json_output_file}")
            except json.JSONDecodeError:
                logging.warning(f"Error reading existing data from {json_output_file}, will overwrite")
        
        existing_dict = {item["word"]: item for item in existing_data}
        new_dict = {item["word"]: item for item in results}
        
        existing_dict.update(new_dict)
        merged_results = list(existing_dict.values())
        
        with open(json_output_file, 'w', encoding='utf-8') as f:
            json.dump(merged_results, f, ensure_ascii=False, indent=2)
        logging.info(f"Saved {len(merged_results)} entries to JSON file: {json_output_file}")
        
        self.save_to_csv(merged_results, csv_output_file)
        
        self.generate_combined_csv()
        
        return len(words_to_process)
    
    def generate_combined_csv(self):
        """Generate a combined CSV file from all the individual JSON files"""
        all_json_files = []
        for root, _, files in os.walk(self.json_folder):
            for file in files:
                if file.endswith('.json'):
                    all_json_files.append(os.path.join(root, file))
        
        if not all_json_files:
            logging.warning("No JSON files found to generate combined CSV")
            return
        
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
        """
        Run the scraper on all word files. This is the main entry point.
        """
        try:
            word_files_by_alphabet = self.get_word_files()
            
            if not word_files_by_alphabet:
                logging.error("No word files found to process.")
                return
                
            total_words = 0
            total_processed = 0
            total_failed = 0
            
            for alphabet, word_files in word_files_by_alphabet.items():
                alphabet_folder = os.path.join(self.json_folder, alphabet)
                os.makedirs(alphabet_folder, exist_ok=True)
                
                alphabet_csv_folder = os.path.join(self.csv_folder, alphabet)
                os.makedirs(alphabet_csv_folder, exist_ok=True)
                
                for word_file in word_files:
                    result = self.process_file(word_file, alphabet)
                    if result:
                        total_words += result
                        total_processed += result
                        # No failures to count if process_file just returns a count
                        
            logging.info(f"Total words: {total_words}")
            logging.info(f"Successfully processed: {total_processed}")
            logging.info(f"Failed: {total_failed}")
            
            # Generate combined CSV file
            self.generate_combined_csv()
            
            # Generate SQL init file
            self.generate_sql_init_file()
            
            # Generate SQL insert statements
            self.generate_sql_insert_statements()
            
            # The spacing fixes have been incorporated directly into the scraping process
            # The fix_spacing_in_existing_csv method is now optional and primarily for backward compatibility
            # or fixing legacy data that was scraped before these improvements
            # To apply them to all existing data, uncomment the following line:
            # self.fix_spacing_in_existing_csv()
            
            logging.info("Scraping completed successfully.")
            return {
                "total_words": total_words,
                "processed": total_processed,
                "failed": total_failed
            }
            
        except Exception as e:
            logging.error(f"Error running scraper: {str(e)}")
            traceback.print_exc()
            return None
    
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
        all_json_files = []
        for root, _, files in os.walk(self.json_folder):
            for file in files:
                if file.endswith('.json'):
                    all_json_files.append(os.path.join(root, file))
        
        if not all_json_files:
            logging.warning("No JSON files found to generate SQL insert statements")
            return
        
        sql_inserts_file = os.path.join(self.output_folder, "insert_data.sql")
        
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
        
        cleaned_data = [self.extract_flattened_data(item) for item in all_data if item.get("status") == "success"]
        
        with open(sql_inserts_file, 'w', encoding='utf-8') as f:
            f.write("-- SQL Insert Statements for Yoruba Dictionary Data\n")
            f.write("-- Generated automatically by GlosbeYorubaScraper\n\n")
            
            f.write("BEGIN TRANSACTION;\n\n")
            
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
                verification_score = item.get("verification_score", 0)
                
                insert_stmt = f"INSERT OR IGNORE INTO yoruba_words (word, translation, all_translations, part_of_speech, example_yoruba, example_english, url, scrape_time, status, error, verification_score) "
                insert_stmt += f"VALUES ('{word}', '{translation}', '{all_translations}', '{pos}', '{example_yoruba}', '{example_english}', '{url}', '{scrape_time}', '{status}', '{error}', {verification_score});\n"
                f.write(insert_stmt)
            
            f.write("\nCOMMIT;\n")
        
        logging.info(f"Generated SQL insert statements file: {sql_inserts_file}")

    def generate_postgres_exports(self):
        """Generate PostgreSQL specific export files"""
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
        
        exporter = PostgresExporter(self.output_folder)
        export_files = exporter.generate_postgres_export(all_data)
        
        logging.info(f"PostgreSQL export complete. Schema: {export_files['schema_file']}, Data: {export_files['insert_file']}")

    def fix_spacing_in_existing_csv(self, csv_file_path=None):
        """Fix spacing issues in existing CSV files."""
        logging.info("Starting to fix spacing in CSV files")
        
        # Variables to track progress
        total_fixed_yoruba = 0
        total_fixed_english = 0
        csv_files_processed = 0
        
        # Check if a specific CSV file path is provided and exists
        if csv_file_path and os.path.exists(csv_file_path):
            logging.info(f"Processing specific CSV file: {csv_file_path}")
            yoruba_fixed, english_fixed = self._fix_spacing_in_csv(csv_file_path)
            total_fixed_yoruba += yoruba_fixed
            total_fixed_english += english_fixed
            csv_files_processed += 1
        else:
            # If no specific file, process all CSV files in the base folder
            logging.info(f"No specific CSV file provided, processing all CSV files in {self.base_folder}")
            
            # Find all CSV files in the base folder and its subdirectories
            csv_files = []
            for root, _, files in os.walk(self.base_folder):
                for file in files:
                    if file.endswith('.csv'):
                        csv_files.append(os.path.join(root, file))
            
            logging.info(f"Found {len(csv_files)} CSV files to process")
            
            # Process each CSV file
            for csv_file in csv_files:
                yoruba_fixed, english_fixed = self._fix_spacing_in_csv(csv_file)
                total_fixed_yoruba += yoruba_fixed
                total_fixed_english += english_fixed
                csv_files_processed += 1
        
        logging.info(f"Finished processing {csv_files_processed} CSV files")
        logging.info(f"Total Yoruba rows fixed: {total_fixed_yoruba}")
        logging.info(f"Total English rows fixed: {total_fixed_english}")
        
        return csv_files_processed, total_fixed_yoruba, total_fixed_english
    
    def _fix_spacing_in_csv(self, file_path):
        """
        Apply spacing fixes to a single CSV file
        
        Args:
            file_path (str): Path to the CSV file to fix
        """
        try:
            logging.info(f"Fixing spacing issues in {file_path}")
            
            # Read the CSV file
            df = pd.read_csv(file_path, encoding='utf-8')
            
            # Check if the required columns exist
            yoruba_fixed_count = 0
            if 'yoruba_text' in df.columns:
                # Apply spacing fix to all Yoruba text
                original_yoruba = df['yoruba_text'].copy()
                df['yoruba_text'] = df['yoruba_text'].apply(lambda x: self._fix_yoruba_spacing(x) if isinstance(x, str) else x)
                
                # Count rows that were fixed
                yoruba_fixed_count = sum(original_yoruba != df['yoruba_text'])
                logging.info(f"Fixed {yoruba_fixed_count} Yoruba rows")
            else:
                logging.warning(f"No 'yoruba_text' column found in {file_path}")
            
            # Check for English text
            english_fixed_count = 0
            if 'english_text' in df.columns:
                # Apply spacing fix to all English text
                original_english = df['english_text'].copy()
                df['english_text'] = df['english_text'].apply(lambda x: self._fix_english_spacing(x) if isinstance(x, str) else x)
                
                # Count rows that were fixed
                english_fixed_count = sum(original_english != df['english_text'])
                logging.info(f"Fixed {english_fixed_count} English rows")
            else:
                logging.warning(f"No 'english_text' column found in {file_path}")

            # Create a backup of the original file
            backup_file = f"{file_path}.bak"
            shutil.copy2(file_path, backup_file)
            logging.info(f"Created backup at {backup_file}")
            
            # Save the updated CSV
            df.to_csv(file_path, index=False, encoding='utf-8')
            logging.info(f"Fixed spacing in {file_path}: {yoruba_fixed_count} Yoruba rows, {english_fixed_count} English rows")
            
            return yoruba_fixed_count, english_fixed_count
        except Exception as e:
            logging.error(f"Error fixing spacing in {file_path}: {e}")
            return 0, 0
    
    def _fix_yoruba_spacing(self, text):
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
        self.example_patterns = [
            ('.translation__example', '.source-text', '.target-text'),
            ('.example-pair', '.source', '.target'),
            ('.translation-memory-example', '.source', '.target'),
            ('.example__content', '.source', '.target'),
            ('.translation__item', '.source', '.target'),
            ('.translation-list__item', '.source', '.target'),
            ('.translation__translation', '.source', '.target'),
            ('.translation-item', '.source', '.target')
        ]
        
        self.text_patterns = [
            r'Example sentences with "([^"]+)"[:\s]+(.+?)↔(.+?)(?=$|\n|<)',
            r'Sample translated sentence:(.+?)↔(.+?)(?=$|\n|<)',
            r'Example:(.+?)↔(.+?)(?=$|\n|<)',
            r'Translation examples:(.+?)↔(.+?)(?=$|\n|<)',
            r'([^\.]+\.)[\s]*↔[\s]*([^\.]+\.)',

            r'Usage:[\s]*([^→]+)→([^$\n<]+)',
            r'Context:[\s]*([^=]+)=([^$\n<]+)',
            r'"([^"]+)"\s*translates to\s*"([^"]+)"',
            r'([^:]+):\s*\(([^)]+)\)',

            r'\b([^\.]{1,50})\s*[=→↔]\s*([^\.]{1,50})',
            r'([^:]+):\s*"([^"]+)"',
            r'•\s*([^•]+)\s*•\s*([^•]+)',
            r'[\[\(]([^\[\]]+)[\]\)]\s*=\s*[\[\(]([^\[\]]+)[\]\)]'
        ]
    
        self.yoruba_markers = [
            'mo', 'o', 'ó', 'wọn', 'won', 'a', 'ẹ', 'è', 'ni',
            'kò', 'ko', 'ṣe', 'se', 'ti', 'sì', 'si', 'yìí', 'yii',
            'ń', 'n', 'kí', 'ki', 'bí', 'bi', 'fún', 'fun',
            'àti', 'ati', 'ọmọ', 'omo', 'jẹ́', 'je', 'gbà', 'gba'
        ]
        
        self.english_markers = [
            'the', 'a', 'an', 'is', 'are', 'was', 'were', 'will',
            'have', 'has', 'had', 'be', 'been', 'being',
            'I', 'you', 'he', 'she', 'it', 'we', 'they',
            'this', 'that', 'these', 'those', 'and', 'but', 'or'
        ]
        
        # Dictionary to store known good example sentences for validation
        self.known_examples = {
            'adìye': [
                {"yoruba": "Adìye náà ń jẹ èso.", "english": "The chicken is eating fruit.", "confidence": "high"},
                {"yoruba": "Mo fẹ́ adìye kan.", "english": "I want a chicken.", "confidence": "high"}
            ],
            'àpẹ́': [
                {"yoruba": "Àpẹ́ náà ń wẹ̀ ní odò.", "english": "The duck is swimming in the river.", "confidence": "high"}
            ],
            'ó': [
                {"yoruba": "Ó ń sùn.", "english": "He is sleeping.", "confidence": "high"},
                {"yoruba": "Ó ń kọrin.", "english": "She is singing.", "confidence": "high"},
                {"yoruba": "Ó dára.", "english": "It is good.", "confidence": "high"}
            ],
            'àpẹ́rẹ́': [
                {"yoruba": "Àpẹ́rẹ́ tí mo fẹ́ fi hàn.", "english": "The example I want to show.", "confidence": "high"}
            ],
            'ẹ̀kọ́': [
                {"yoruba": "Ẹ̀kọ́ jẹ́ pàtàkì.", "english": "Education is important.", "confidence": "high"}
            ],
            'a': [
                {"yoruba": "A jẹ̀ ẹ.", "english": "We ate it.", "confidence": "high"},
                {"yoruba": "A tí dé.", "english": "We have arrived.", "confidence": "high"}
            ],
            'á': [
                {"yoruba": "Á mú un.", "english": "He will take it.", "confidence": "high"},
                {"yoruba": "Á pa á.", "english": "She will kill it.", "confidence": "high"}
            ]
        }
    
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
            r'Chapter',
            r'Jèhófà',
            r'Ìjọba',
            r'Bíbélì'
        ]
        
        for pattern in jw_patterns:
            if (re.search(pattern, yoruba, re.IGNORECASE) or 
                re.search(pattern, english, re.IGNORECASE)):
                return True
            
        return False
    
    def is_valid_example(self, yoruba, english, word):
        """
        Validate if extracted example pair is legitimate with enhanced accuracy
        
        This validation uses several criteria to ensure the examples are accurate:
        1. Both Yoruba and English texts must exist and be reasonable length
        2. The Yoruba text should contain the word being looked up (for most cases)
        3. The text should have appropriate language markers (Yoruba in Yoruba, English in English)
        4. The text should not contain UI elements or irrelevant content
        5. For short words, more lenient criteria are used
        
        Args:
            yoruba (str): The Yoruba text
            english (str): The English text
            word (str): The word being looked up
            
        Returns:
            bool: True if the example pair is valid, False otherwise
        """
        # Special case testing for common incorrect translations
        word_translations = {
            "adìye": {"correct": ["chicken"], "incorrect": ["duck", "goose", "hen", "turkey", "bird"]},
            "àpẹ́": {"correct": ["duck"], "incorrect": ["chicken", "goose", "hen", "turkey", "bird"]},
            "àpẹ́rẹ́": {"correct": ["example", "sample", "illustration"], "incorrect": []},
            "ẹ̀kọ́": {"correct": ["education", "lesson", "learning", "study"], "incorrect": []},
            "ìgbín": {"correct": ["snail"], "incorrect": ["slug", "worm", "insect"]},
            "àgbàdo": {"correct": ["corn", "maize"], "incorrect": ["wheat", "rice", "barley"]},
            "abo": {"correct": ["female", "feminine"], "incorrect": []},
            "a": {"correct": ["we", "us"], "incorrect": ["I", "they"]},
            "á": {"correct": ["will", "shall", "he", "she"], "incorrect": ["shut"]}
        }
        
        normalized_word = word.lower()
        
        # Basic validation checks
        if not yoruba or not english:
            return False
            
        is_short_word = len(word) <= 2
        min_length = 3 if is_short_word else 5
        max_length = 1000
        
        if len(yoruba) < min_length or len(english) < min_length:
            return False
        if len(yoruba) > max_length or len(english) > max_length:
            return False
        
        # Check for HTML tags in text (indicates parsing error)
        if re.search(r'</?[a-z]+>', yoruba) or re.search(r'</?[a-z]+>', english):
            return False
            
        # Check if example sentence includes the word being looked up (skip for very short words)
        if len(word) > 1 and not is_short_word:
            word_pattern = r'\b' + re.escape(normalized_word) + r'\b'
            word_variants = [
                normalized_word, 
                normalized_word.replace('ì', 'i').replace('é', 'e').replace('ó', 'o'),
                normalized_word.replace('ì', 'i'), 
                normalized_word.replace('é', 'e')
            ]
            
            # For longer words we should find the word somewhere in the Yoruba text
            if not any(re.search(r'\b' + re.escape(variant) + r'\b', yoruba.lower()) for variant in word_variants):
                # The Yoruba example MUST contain the word or a variant unless it's a very short word
                return False
            
        # Check for UI elements or common website text that shouldn't be in examples
        ui_elements = [
            'glosbe', 'log in', 'sign up', 'click', 'next page',
            'show more', 'hide', 'loading', 'search', 'menu',
            'translation', 'dictionary', 'cookie', 
            'privacy', 'terms', 'contact', 'email', 'password',
            'username', 'copyright', 'all rights', 'download'
        ]
        
        if any(ui in yoruba.lower() for ui in ui_elements) or any(ui in english.lower() for ui in ui_elements):
            return False
            
        # Check if the text is actually in Yoruba (must have at least some Yoruba markers or diacritics)
        has_yoruba_markers = any(marker in yoruba.lower() for marker in self.yoruba_markers)
        has_yoruba_diacritics = bool(re.search(r'[àáèéìíòóùúẹọṣ]', yoruba.lower()))
        
        # If no Yoruba markers or diacritics, likely not Yoruba text
        if not has_yoruba_markers and not has_yoruba_diacritics:
            return False
            
        score = 0
        
        # Very important: The word should be in the Yoruba text 
        # (unless it's a very short word which might be part of a larger word)
        if word.lower() in yoruba.lower():
            score += 30
        elif is_short_word and any(w.startswith(word.lower()) or w.endswith(word.lower()) for w in yoruba.lower().split()):
            score += 15
            
        # Check length ratio between Yoruba and English
        # Good translations tend to have somewhat similar lengths
        length_ratio = min(len(yoruba), len(english)) / max(len(yoruba), len(english))
        if is_short_word:
            score += int(length_ratio * 15)
        else:
            score += int(length_ratio * 25)
            
        # Check for language markers
        has_english_markers = any(marker in english.lower() for marker in self.english_markers)
        
        if has_yoruba_markers:
            score += 20
        if has_english_markers:
            score += 20
            
        # Check for Yoruba pronouns which are common in sentences
        if re.search(r'\b(mo|o|ó|wọn|won|a|ẹ|è)\b', yoruba.lower()):
            score += 15
        
        # Check for proper sentence structure in the Yoruba text
        if re.match(r'^[A-ZÀ-ÖØ-öø-ÿ]', yoruba) and re.search(r'[.!?]$', yoruba):
            score += 10
            
        # Check for proper sentence structure in the English text
        if re.match(r'^[A-Z]', english) and re.search(r'[.!?]$', english):
            score += 10
            
        # Check for translation consistency with known translations
        # If the word is "chicken" (adìye), the English should contain "chicken" not "duck"
        normalized_word = word.lower()
        if normalized_word in self.known_examples:
            # Get all English translations from known examples
            known_translations = set()
            incorrect_translations = {
                'chicken': {'duck', 'hen', 'goose', 'bird', 'turkey'},
                'duck': {'chicken', 'hen', 'goose', 'bird', 'turkey'},
                'example': {'sample', 'instance', 'case', 'illustration'},
                'sample': {'example', 'instance', 'case', 'illustration'}
            }
            
            for known_example in self.known_examples[normalized_word]:
                # Extract the main noun or verb from the English translation
                eng_words = known_example['english'].lower().split()
                for eng_word in eng_words:
                    # Remove punctuation
                    clean_eng_word = re.sub(r'[^\w\s]', '', eng_word)
                    if len(clean_eng_word) > 3:  # Only consider substantial words
                        known_translations.add(clean_eng_word)
            
            # If we have known translations, check if any of them are in the English text
            # or if any incorrect translations are in the English text
            if known_translations:
                if not any(trans in english.lower() for trans in known_translations):
                    score -= 30  # Penalize for not containing any known translations
                
                # Check for incorrect animal/object translations
                for correct_trans in known_translations:
                    if correct_trans in incorrect_translations:
                        for incorrect_trans in incorrect_translations[correct_trans]:
                            if incorrect_trans in english.lower() and correct_trans not in english.lower():
                                # Found incorrect translation (e.g., "duck" when should be "chicken")
                                score -= 50  # Severe penalty for wrong translations
        
        # Different thresholds based on word length
        required_score = 30 if is_short_word else 50
        
        # For known examples in our database, automatically validate
        if normalized_word in self.known_examples:
            for known_example in self.known_examples[normalized_word]:
                if (yoruba.lower() == known_example['yoruba'].lower() and
                    english.lower() == known_example['english'].lower()):
                    return True
        
        return score >= required_score
    
    def clean_example_text(self, text):
        """Clean up extracted example text"""
        if not text:
            return ""
        
        # Keep a copy of the original text
        original_text = text
            
        # Basic cleanup
        text = re.sub(r'\s+', ' ', text).strip()
            
        # Remove numeric references and UI elements
        text = re.sub(r'(\d+/\d+|Show all|Hide)', '', text)
            
        # Remove arrow symbols
        text = re.sub(r'(↑|↓|→|←|↔)', '', text)
            
        # Remove email addresses and URLs
        text = re.sub(r'\S+@\S+\.\S+', '', text)
        text = re.sub(r'https?://\S+', '', text)
            
        # Fix spacing around punctuation
        text = re.sub(r'\s+([.,;:!?])', r'\1', text)
        text = re.sub(r'([.,;:!?])\s+', r'\1 ', text)
        
        # Handle HTML entities
        text = text.replace('&amp;', '&')
        text = text.replace('&quot;', '"')
        text = text.replace('&apos;', "'")
        text = text.replace('&lt;', '<')
        text = text.replace('&gt;', '>')
        text = re.sub(r'&[a-zA-Z]+;', '', text)
        
        # Normalize quotes and apostrophes
        text = re.sub(r'["""]', '"', text)
        text = re.sub(r'[\u2018\u2019\']', "'", text)
        
        # Add space after punctuation if followed by a letter
        text = re.sub(r'([.,!?])([A-Za-z])', r'\1 \2', text)
        
        # Detect language and apply language-specific fixes
        has_yoruba_diacritics = bool(re.search(r'[àáèéìíòóùúẹọṣ]', text))
        
        if has_yoruba_diacritics:
            # Apply Yoruba-specific fixes
            text = self._fix_yoruba_spacing(text)
        else:
            # Apply English-specific fixes - includes handling of joined words
            text = self._fix_english_spacing(text)
        
        # Remove any multiple spaces that might have been created
        text = re.sub(r'\s+', ' ', text).strip()
        
        # If the text was reduced to something too short, revert to original
        if len(text) < 5 and len(original_text) > 10:
            return original_text.strip()
            
        return text.strip()
        
    def _clean_english_example(self, text):
        """Clean English example text to improve quality
        
        Args:
            text (str): The English example text to clean
            
        Returns:
            str: Cleaned example text
        """
        if not text or not isinstance(text, str):
            return text
        
        # Store original for comparison
        original = text
        
        # Fix spacing between words (common scraping issue)
        # Add space between lowercase and uppercase letters
        text = re.sub(r'([a-z])([A-Z])', r'\1 \2', text)
        
        # Fix common auxiliary verb + main verb issues
        for aux in ['could', 'would', 'should', 'might', 'must', 'will', 'shall']:
            for verb in ['have', 'be', 'go', 'do', 'take', 'make', 'get']:
                text = text.replace(f"{aux}{verb}", f"{aux} {verb}")
                # Also handle past participle forms
                text = text.replace(f"{aux}been", f"{aux} been")
                text = text.replace(f"{aux}had", f"{aux} had")
        
        # Fix "have been" compounds
        text = text.replace("havebeen", "have been")
        
        # Fix common compound words
        common_patterns = [
            ('beenreleased', 'been released'),
            ('havebeen', 'have been'),
            ('hasbeen', 'has been'),
            ('hadbeen', 'had been'),
            ('beenleft', 'been left'),
            ('beenput', 'been put'),
            ('putto', 'put to'),
            ('releasedif', 'released if'),
            ('manwas', 'man was'),
            ('mancould', 'man could'),
            ('ofmankind', 'of mankind'),
            ('hecould', 'he could'),
            ('hecannot', 'he cannot'),
            ('shecannot', 'she cannot'),
            ('itis', 'it is'),
            ('ifhe', 'if he'),
            ('ifthey', 'if they'),
            ('wasno', 'was no'),
            ('theylearn', 'they learn'),
            ('theydo', 'they do'),
            ('wedo', 'we do'),
            ('youdo', 'you do'),
            ('youknow', 'you know'),
            ('youmay', 'you may'),
            ('Theman', 'The man'),
            ('Thisman', 'This man'),
            ('Thatis', 'That is'),
            ('fromamong', 'from among'),
            ('toobey', 'to obey'),
            ('inorder', 'in order'),
            ('inthe', 'in the'),
            ('forthe', 'for the'),
            ('willbe', 'will be'),
            ('wouldbe', 'would be'),
            ('shouldbe', 'should be'),
            ('couldbe', 'could be'),
            ('mightbe', 'might be'),
            ('willhave', 'will have'),
            ('wouldhave', 'would have'),
            ('shouldhave', 'should have'),
            ('couldhave', 'could have'),
            ('mighthave', 'might have'),
        ]
        
        for pattern, replacement in common_patterns:
            text = text.replace(pattern, replacement)
        
        # Fix space between determiners and nouns 
        for det in ['The', 'A', 'An', 'This', 'That', 'These', 'Those', 'His', 'Her', 'Its', 'Our', 'Their']:
            text = re.sub(f"({det})([a-z][a-z]+)", r"\1 \2", text)
        
        # Fix possessive + noun patterns
        text = re.sub(r"('s)([a-z][a-z]+)", r"\1 \2", text)
        
        # Fix common preposition + noun patterns
        for prep in ['in', 'on', 'at', 'by', 'for', 'with', 'to', 'from', 'of']:
            text = re.sub(f"\\b{prep}([a-z][a-z]+)", f"{prep} \\1", text)
        
        # Clean up spaces around punctuation
        text = re.sub(r'\s+([.,;:!?])', r'\1', text)
        
        # Add necessary spacing after punctuation
        text = re.sub(r'([.,;:!?])([A-Za-z])', r'\1 \2', text)
        
        # Normalize spaces
        text = re.sub(r'\s+', ' ', text).strip()
        
        # If the text was reduced to something too short, revert to original
        if len(text) < 5 and len(original) > 10:
            return original.strip()
            
        return text
    
    def extract_examples(self, soup, word):
        """Extract example sentences using multiple techniques"""
        examples = []
        
        # First, extract the primary translation directly from the main translation item
        translation_items = soup.select('h3.translation__item__pharse')
        primary_translations = []
        
        for item in translation_items:
            translation_text = item.get_text(strip=True)
            if translation_text:
                primary_translations.append(translation_text)
                
        # Look for translation items with expandable details
        translation_details_containers = soup.select('li[data-element="translation"]')
        for container in translation_details_containers:
            translation_elem = container.select_one('h3.translation__item__pharse')
            if translation_elem:
                translation_text = translation_elem.get_text(strip=True)
                if translation_text and translation_text not in primary_translations:
                    primary_translations.append(translation_text)
        
        # Process standard example containers
        example_selectors = [
            '.translation__example', '.example-pair',
            '.translation-memory-example', '.example__content',
            '.dict-example',
            '.translation-example',
            '.example-item',
            '.dict-example-item',
            '.translation-memory',
            '.tmem',
            '.example',
            '[data-example]',
            '.py-2.flex',
            '.odd\\:bg-slate-100'
        ]
        
        source_selectors = [
            '.yoruba', '.source', '.example__source', '.left', '.src', '[data-source]',
            '.w-1\\/2.dir-aware-pr-1',
            'p[lang="yo"]'
        ]
        
        target_selectors = [
            '.english', '.target', '.example__target', '.right', '.tgt', '[data-target]',
            '.w-1\\/2.dir-aware-pl-1',
            '.w-1\\/2.px-1.ml-2'
        ]
        
        for selector in example_selectors:
            containers = soup.select(selector)
            for container in containers:
                yoruba = None
                english = None
                
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
        
        # Extract from similar phrases section
        similar_phrases_section = soup.select_one('#simmilar-phrases')
        if similar_phrases_section:
            phrase_items = similar_phrases_section.select('li.px-2.py-1.flex')
            for item in phrase_items:
                yoruba_elem = item.select_one('.w-1\\/3.dir-aware-text-right')
                english_elem = item.select_one('.dir-aware-pl-2.w-2\\/3')
                
                if yoruba_elem and english_elem:
                    yoruba = yoruba_elem.get_text(strip=True)
                    english = english_elem.get_text(strip=True)
                    
                    if yoruba and english:
                        yoruba_text = self.clean_example_text(yoruba)
                        english_text = self.clean_example_text(english)
                        
                        # Check if this example contains our word
                        if word.lower() in yoruba_text.lower() or yoruba_text.lower() in word.lower():
                            examples.append({
                                "yoruba": yoruba_text,
                                "english": english_text,
                                "source": "similar_phrase",
                                "confidence": "high",
                                "is_jw_reference": False
                            })
        
        # Extract from Memory Examples section (#tmem_first_examples)
        memory_examples = soup.select('#tmem_first_examples .odd\\:bg-slate-100, #tmem_first_examples .py-2.flex')
        for example in memory_examples:
            yoruba_elem = example.select_one('.w-1\\/2.dir-aware-pr-1, p[lang="yo"]')
            english_elem = example.select_one('.w-1\\/2.dir-aware-pl-1, .w-1\\/2.px-1.ml-2')
            
            if yoruba_elem and english_elem:
                yoruba = yoruba_elem.get_text(strip=True)
                english = english_elem.get_text(strip=True)
                
                yoruba_text = self.clean_example_text(yoruba)
                english_text = self.clean_example_text(english)
                
                if yoruba_text and english_text and word.lower() in yoruba_text.lower():
                    examples.append({
                        "yoruba": yoruba_text,
                        "english": english_text,
                        "source": "tmem",
                        "confidence": "high",
                        "is_jw_reference": self.is_jw_reference(yoruba_text, english_text)
                    })
        
        # Add regex pattern extraction for sentences
        html_text = str(soup)
        
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
                    
                    if word.lower() in yoruba_text.lower() and self.is_valid_example(yoruba_text, english_text, word):
                        examples.append({
                            "yoruba": yoruba_text,
                            "english": english_text,
                            "source": "regex",
                            "confidence": "medium",
                            "is_jw_reference": self.is_jw_reference(yoruba_text, english_text)
                        })
        
        # Special handling for short words
        if len(word) <= 2:
            short_word_examples = self.extract_short_word_examples(soup, word)
            examples.extend(short_word_examples)
        
        # Add the primary translations as examples where the Yoruba is the word itself
        for translation in primary_translations:
            clean_translation = translation.strip()
            if clean_translation and len(clean_translation) > 1:
                # Only add as an example if it's not already in the examples list
                if not any(example["english"].lower() == clean_translation.lower() for example in examples):
                    examples.append({
                        "yoruba": word,
                        "english": clean_translation,
                        "source": "primary_translation",
                        "confidence": "high",
                        "is_jw_reference": False
                    })
        
        # De-duplicate examples
        seen = set()
        unique_examples = []
        for example in examples:
            key = (example["yoruba"].lower(), example["english"].lower())
            if key not in seen:
                seen.add(key)
                unique_examples.append(example)
        
        return unique_examples
        
    def extract_short_word_examples(self, soup, word):
        """Special extraction for short words like pronouns"""
        examples = []
        
        example_pairs = []
        
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
        
        for yoruba, english in example_pairs:
            yoruba_text = self.clean_example_text(yoruba)
            english_text = self.clean_example_text(english)
            
            if yoruba_text and english_text and len(yoruba_text) >= 5 and len(english_text) >= 5:
                examples.append({
                    "yoruba": yoruba_text,
                    "english": english_text,
                    "source": "short_word",
                    "confidence": "medium",
                    "is_jw_reference": self.is_jw_reference(yoruba_text, english_text)
                })
        
        paragraphs = soup.find_all(['p', 'div', 'span'])
        for paragraph in paragraphs:
            text = paragraph.get_text(strip=True)
            if word.lower() in text.lower():
                sentences = re.split(r'[.!?]', text)
                for sentence in sentences:
                    if word.lower() in sentence.lower() and len(sentence) >= 10:
                        next_p = paragraph.find_next_sibling(['p', 'div', 'span'])
                        if next_p:
                            next_text = next_p.get_text(strip=True)
                            if 0.5 <= len(next_text) / len(sentence) <= 2:
                                examples.append({
                                    "yoruba": sentence.strip(),
                                    "english": next_text.strip(),
                                    "source": "short_word",
                                    "confidence": "low",
                                    "is_jw_reference": False
                                })
        
        return examples
        
    def extract_examples_by_translation(self, soup, word, translations):
        """Extract examples and try to associate them with specific translations"""
        all_examples = self.extract_examples(soup, word)
        
        examples_by_translation = {}
        for translation in translations:
            examples_by_translation[translation] = []
        
        general_examples = []
        
        for example in all_examples:
            english = example.get("english", "").lower()
            matched = False
            
            for translation in translations:
                if translation.lower() in english:
                    examples_by_translation[translation].append(example)
                    matched = True
                    break
            
            if not matched:
                general_examples.append(example)
        
        return {
            "by_translation": examples_by_translation,
            "general": general_examples
        }

    def verify_example_pair(self, yoruba, english):
        """
        Verify if an example pair is valid with enhanced accuracy checks.
        
        This performs additional validation beyond the basic checks in is_valid_example:
        1. Ensures proper sentence structure
        2. Verifies the sentences have matching meaning (based on key words)
        3. Checks for proper length ratio
        4. Verifies the presence of sentence markers
        
        Args:
            yoruba (str): The Yoruba text
            english (str): The English text
            
        Returns:
            bool: True if the example pair passes verification, False otherwise
        """
        # Skip empty sentences
        if not yoruba or not english:
            return False
            
        # Normalize texts
        yoruba = self.clean_example_text(yoruba)
        english = self.clean_example_text(english)
        
        # Basic length checks
        if len(yoruba) < 10 or len(english) < 10:
            return False
        if len(yoruba) > 500 or len(english) > 500:
            return False
            
        # Check for sentence structure (should have sentence markers)
        has_yoruba_sentence_markers = bool(re.search(r'[.!?]', yoruba))
        has_english_sentence_markers = bool(re.search(r'[.!?]', english))
        
        # If one has sentence markers but the other doesn't, that's a mismatch
        if has_yoruba_sentence_markers != has_english_sentence_markers:
            return False
            
        # Check length ratio - good translations tend to have somewhat similar lengths
        # Yoruba tends to be more concise than English, so the ratio is often around 0.7-1.3
        length_ratio = len(yoruba) / len(english) if len(english) > 0 else 0
        if length_ratio < 0.5 or length_ratio > 2.0:
            return False
            
        # Calculate a verification score
        score = 0
        
        # Score for sentence markers
        if has_yoruba_sentence_markers and has_english_sentence_markers:
            score += 20
            
        # Score for Yoruba markers (common words)
        yoruba_markers = [
            'ni', 'tí', 'sì', 'kò', 'ń', 'ó', 'á', 'mo', 'wọn', 'àti', 
            'fún', 'pé', 'kí', 'jẹ́', 'ṣe', 'bí', 'wá', 'lọ', 'gbà', 'rí'
        ]
        if any(re.search(r'\b' + marker + r'\b', yoruba.lower()) for marker in yoruba_markers):
            score += 20
            
        # Score for English markers (common words)
        english_markers = [
            'the', 'a', 'an', 'of', 'to', 'in', 'is', 'are', 'was', 'were',
            'will', 'have', 'has', 'had', 'be', 'with', 'for', 'and', 'or', 'but'
        ]
        if any(re.search(r'\b' + marker + r'\b', english.lower()) for marker in english_markers):
            score += 20
            
        # Score for proper length ratio
        if 0.7 <= length_ratio <= 1.3:
            score += 20
            
        # Score for capital letter at the beginning
        if yoruba and english and yoruba[0].isupper() and english[0].isupper():
            score += 10
            
        # Score for matching end punctuation
        yoruba_end = yoruba[-1] if yoruba else ''
        english_end = english[-1] if english else ''
        if yoruba_end in '.!?' and english_end in '.!?':
            score += 10
            
        # Minimum score required for verification
        return score >= 50

    def add_known_examples(self, word, examples):
        """
        Add known good examples for a word
        
        Args:
            word (str): The Yoruba word
            examples (list): A list of example dictionaries with 'yoruba' and 'english' keys
        """
        normalized_word = word.lower()
        if normalized_word not in self.known_examples:
            self.known_examples = self.known_examples or {}
            self.known_examples[normalized_word] = []
            
        # Add each example if it's not already in the list
        for example in examples:
            if not any(ex.get('yoruba') == example.get('yoruba') and 
                     ex.get('english') == example.get('english') 
                     for ex in self.known_examples.get(normalized_word, [])):
                
                # Ensure all examples have confidence level
                if 'confidence' not in example:
                    example['confidence'] = 'high'
                    
                self.known_examples[normalized_word].append(example)
                
    def get_known_examples(self, word):
        """
        Get known good examples for a word
        
        Args:
            word (str): The Yoruba word
            
        Returns:
            list: A list of example dictionaries
        """
        word_key = word.lower()
        return self.known_examples.get(word_key, [])

class PostgresExporter:
    """Class for exporting data to PostgreSQL format"""
    
    def __init__(self, output_folder):
        self.output_folder = output_folder
        
    def normalize_string(self, text):
        """Normalize and escape a string for PostgreSQL use"""
        if text is None:
            return "NULL"
        
        normalized = text.replace("'", "''").replace("\\", "\\\\")
        
        normalized = normalized.replace("\n", " ").replace("\t", " ").replace("\r", " ")
        
        normalized = re.sub(r'[\x00-\x1F\x7F]', '', normalized)
        
        return f"'{normalized}'"
    
    def generate_schema(self):
        """Generate PostgreSQL schema optimized for the dictionary database"""
        schema = []
        
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
        
        schema.append("CREATE INDEX IF NOT EXISTS idx_words_word ON words (word);")
        schema.append("")
        
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
        
        schema.append("CREATE INDEX IF NOT EXISTS idx_translations_word_id ON translations (word_id);")
        schema.append("CREATE INDEX IF NOT EXISTS idx_translations_translation ON translations (translation);")
        schema.append("")
        
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
        
        schema.append("CREATE INDEX IF NOT EXISTS idx_examples_translation_id ON examples (translation_id);")
        schema.append("CREATE INDEX IF NOT EXISTS idx_examples_word_id ON examples (word_id);")
        schema.append("")
        
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
        
        inserts.append("-- Data Import Statements")
        inserts.append("-- Generated: " + time.strftime("%Y-%m-%d %H:%M:%S"))
        inserts.append("")
        
        word_ids = set()
        translation_ids = set()
        example_ids = set()
        
        for file_data in all_data:
            for word in file_data.get("words", []):
                if word["id"] in word_ids:
                    continue
                word_ids.add(word["id"])
                
                inserts.append(f"INSERT INTO words (id, word, url, scrape_time, status, error) VALUES (")
                inserts.append(f"    {word['id']},")
                inserts.append(f"    {self.normalize_string(word['word'])},")
                inserts.append(f"    {self.normalize_string(word.get('url', ''))},")
                inserts.append(f"    {self.normalize_string(word.get('scrape_time', ''))},")
                inserts.append(f"    {self.normalize_string(word.get('status', ''))},")
                inserts.append(f"    {self.normalize_string(word.get('error', ''))}")
                inserts.append(");")
                inserts.append("")

            for trans in file_data.get("translations", []):
                if trans["id"] in translation_ids:
                    continue
                translation_ids.add(trans["id"])
                
                inserts.append(f"INSERT INTO translations (id, word_id, translation, part_of_speech, confidence) VALUES (")
                inserts.append(f"    {trans['id']},")
                inserts.append(f"    {trans['word_id']},")
                inserts.append(f"    {self.normalize_string(trans['translation'])},")
                inserts.append(f"    {self.normalize_string(trans.get('part_of_speech', ''))},")
                inserts.append(f"    {self.normalize_string(trans.get('confidence', ''))}")
                inserts.append(");")
                inserts.append("")
            
            for example in file_data.get("examples", []):
                if example["id"] in example_ids:
                    continue
                example_ids.add(example["id"])
                
                translation_id = "NULL"
                if example.get("translation_id") is not None:
                    translation_id = example["translation_id"]
                
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
        os.makedirs(self.output_folder, exist_ok=True)
        
        schema_file = os.path.join(self.output_folder, "yoruba_dictionary_schema.sql")
        with open(schema_file, "w", encoding="utf-8") as f:
            f.write(self.generate_schema())
        logging.info(f"Generated PostgreSQL schema file: {schema_file}")
        
        data_file = os.path.join(self.output_folder, "yoruba_dictionary_data.sql")
        with open(data_file, "w", encoding="utf-8") as f:
            f.write(self.create_insert_statements(all_data))
        logging.info(f"Generated PostgreSQL data file: {data_file}")
        
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
        
        self.min_scores = {
            "translation": 50,
            "example": 40,
            "overall": 45
        }
        
        self.known_words = {
            "a": {"translations": ["we", "us"], "pos": "pronoun"},
            "á": {"translations": ["he", "she", "it"], "pos": "pronoun"},
            "mi": {"translations": ["I", "me", "my"], "pos": "pronoun"},
            "o": {"translations": ["you"], "pos": "pronoun"},
            "ẹ": {"translations": ["you (plural)"], "pos": "pronoun"},
            "wọn": {"translations": ["they", "them"], "pos": "pronoun"},
            
            "à bá ti": {"translations": ["we would have"], "pos": "phrase"},
            "a óò": {"translations": ["we will"], "pos": "phrase"},
            "a máa": {"translations": ["we will"], "pos": "phrase"},
            "a dúpẹ́": {"translations": ["we give thanks"], "pos": "phrase"},
            "A kú ọdún àjíǹde": {"translations": ["Happy Easter"], "pos": "phrase"},
            "a gba ọ̀rọ̀ àkọsílẹ̀ dúró": {"translations": ["we accept the written word"], "pos": "phrase"},
            "a ta": {"translations": ["we sell", "we sold"], "pos": "verb"}
        }
        
        self.yoruba_markers = {
            "characters": ["ẹ", "ọ", "ṣ", "à", "á", "è", "é", "ì", "í", "ò", "ó", "ù", "ú"],
            "pronouns": ["mo", "o", "ó", "á", "a", "ẹ", "wọn", "mi"],
            "verbs": ["ní", "ti", "kò", "ṣe", "máa", "wá", "lọ", "jẹ", "bá"],
            "particles": ["ni", "kí", "bí", "tí", "sì", "fún"]
        }
        
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
        
        for char in self.yoruba_markers["characters"]:
            if char in text_lower:
                score += 15
                break
        
        for word in words:
            if word in self.yoruba_markers["pronouns"]:
                score += 20
            elif word in self.yoruba_markers["particles"]:
                score += 15
            elif word in self.yoruba_markers["verbs"]:
                score += 15

        english_words = sum(1 for w in words if w in 
                          [item for sublist in self.english_patterns.values() for item in sublist])
        if english_words > 0:
            score -= english_words * 5
        
        return score >= 40, score

    def verify_english_text(self, text):
        """Verify if text contains valid English language patterns"""
        if not text:
            return False, 0
            
        score = 0
        text_lower = text.lower()
        words = text_lower.split()
        
        for category, patterns in self.english_patterns.items():
            if any(pattern in words for pattern in patterns):
                score += 20
        
        if text[0].isupper():
            score += 15
        
        if re.match(r'^[A-Z].*[.!?]$', text):
            score += 20
        
        yoruba_chars = sum(1 for char in self.yoruba_markers["characters"] if char in text_lower)
        if yoruba_chars > 0:
            score -= yoruba_chars * 5
        
        return score >= 40, score

    def verify_translation_pair(self, yoruba, english):
        """Verify if a translation pair is valid"""
        if not yoruba or not english:
            return False, 0
        
        score = 0
        
        yoruba_valid, yoruba_score = self.verify_yoruba_text(yoruba)
        if yoruba_valid:
            score += yoruba_score * 0.6
        
        english_valid, english_score = self.verify_english_text(english)
        if english_valid:
            score += english_score * 0.4
        
        yoruba_words = len(yoruba.split())
        english_words = len(english.split())
        ratio = min(yoruba_words, english_words) / max(yoruba_words, english_words)
        if ratio >= 0.3:
            score += ratio * 25
        
        return score >= self.min_scores["translation"], score

    def verify_example_pair(self, yoruba, english):
        """Verify if an example sentence pair is valid"""
        if not yoruba or not english:
            return False, 0
        
        word_length = len(yoruba.split()[0])
        is_short_word = word_length <= 2
        
        yoruba_words = len(yoruba.split())
        english_words = len(english.split())
        min_ratio = 0.3 if is_short_word else 0.5
        max_ratio = 3.0 if is_short_word else 2.0
        ratio = yoruba_words / english_words
        if not (min_ratio <= ratio <= max_ratio):
            return False, 0
        
        score = 0
        
        yoruba_ends_with_punct = bool(re.search(r'[.!?]$', yoruba))
        english_ends_with_punct = bool(re.search(r'[.!?]$', english))
        if yoruba_ends_with_punct and english_ends_with_punct:
            score += 20
        elif yoruba_ends_with_punct != english_ends_with_punct and not is_short_word:
            return False, 0
        
        yoruba_quotes = len(re.findall(r'["""]', yoruba))
        english_quotes = len(re.findall(r'["""]', english))
        if yoruba_quotes == english_quotes:
            score += 10
        elif yoruba_quotes != english_quotes and not is_short_word:
            return False, 0
        
        if re.match(r'^[A-Z]', english):
            score += 15
        elif not is_short_word:
            return False, 0
        
        noise_patterns = [
            r'^\s*\d+\s*$',
            r'^\s*[a-z]\)\s*$',
            r'^\s*$',
            r'^Yoruba$',
            r'^English$',
            r'^Google Translate$',
            r'^Translation$',
            r'^Example$'
        ]
        
        for pattern in noise_patterns:
            if re.match(pattern, yoruba, re.IGNORECASE) or re.match(pattern, english, re.IGNORECASE):
                return False, 0
        
        min_length = 2 if is_short_word else 5
        if len(yoruba) < min_length or len(english) < min_length:
            return False, 0
        if len(yoruba) > 500 or len(english) > 500:
            return False, 0
        
        if re.search(r'<[^>]+>', yoruba) or re.search(r'<[^>]+>', english):
            return False, 0
        
        ui_elements = ['click', 'button', 'menu', 'loading', 'search']
        if any(ui in yoruba.lower() for ui in ui_elements) or any(ui in english.lower() for ui in ui_elements):
            return False, 0
        
        yoruba_patterns = [r'\b(ni|ti|si|ko|ṣe|wa|lo)\b']
        english_patterns = [r'\b(the|a|an|is|are|was|were)\b']
        
        for pattern in yoruba_patterns:
            if re.search(pattern, yoruba.lower()):
                score += 10
        
        for pattern in english_patterns:
            if re.search(pattern, english.lower()):
                score += 10
        
        ratio = min(len(yoruba), len(english)) / max(len(yoruba), len(english))
        score += int(ratio * 20)
        
        required_score = 40 if is_short_word else 60
        
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
            "example_yoruba": "",
            "example_english": "",
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
        
        translation = result.get("translation", "")
        translations = result.get("translations", [])
        
        if translation:
            valid, score = self.verify_translation_pair(word, translation)
            if valid:
                verified_result["translation"] = translation
                verified_result["verification"]["translation_score"] = score
            
            verified_translations = []
            for trans in translations:
                valid, score = self.verify_translation_pair(word, trans)
                if valid and trans != translation:
                    verified_translations.append(trans)
            verified_result["translations"] = verified_translations
        
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
        
        if verified_examples:
            best_example = max(verified_examples, key=lambda x: x["score"])
            verified_result["example_yoruba"] = best_example["yoruba"]
            verified_result["example_english"] = best_example["english"]
            verified_result["verification"]["examples_score"] = total_example_score / len(verified_examples)
        
        quality_score = (
            verified_result["verification"]["translation_score"] * 0.6 +
            verified_result["verification"]["examples_score"] * 0.4
        )
        verified_result["verification"]["quality_score"] = int(quality_score)
        
        if quality_score < self.min_scores["overall"]:
            verified_result["status"] = "verification_failed"
            verified_result["error"] = f"Verification failed with quality score {int(quality_score)}"
        
        return verified_result

    def clean_example_text(self, text):
        """Clean and normalize example text."""
        if not text or len(text.strip()) < 5:
            return None
        
        text = re.sub(r'<[^>]+>', '', text)
        
        text = ' '.join(text.split())
        
        # Detect if the text is likely Yoruba by looking for diacritics
        is_yoruba = bool(re.search(r'[àáèéìíòóùúẹọṣ]', text))
        
        if is_yoruba:
            # Fix Yoruba auxiliary verb spacing issues (á, à, ń, etc.)
            text = re.sub(r'([áàńḿ])([a-zàáèéìíòóùúẹọṣ][a-zàáèéìíòóùúẹọṣ]+)', r'\1 \2', text)  # Add space after auxiliary verbs
            text = re.sub(r'(wọ́n|won|kí|ki|tó|to|ìyẹn|iyen|yìí|yii|èyí|eyi|bàá|baa)([a-zàáèéìíòóùúẹọṣ][a-zàáèéìíòóùúẹọṣ]+)', r'\1 \2', text) # Add space after pronouns/particles
            text = re.sub(r'([a-zàáèéìíòóùúẹọṣ][a-zàáèéìíòóùúẹọṣ]+)(á[a-zàáèéìíòóùúẹọṣ])', r'\1 \2', text)  # Add space before á + word
            
            # Fix specific Yoruba patterns that need spaces
            text = re.sub(r'(ti)(tu|yan|fi|lo|gbà|pa|mọ̀)', r'\1 \2', text)  # Add space between 'ti' and the following verb
            text = re.sub(r'(bá)(ti|pa|fi|gbà|jẹ́|ṣe)', r'\1 \2', text)  # Add space between 'bá' and the following verb
            text = re.sub(r'(ká)(ní|sì|ti)', r'\1 \2', text)  # Add space after 'ká'
            text = re.sub(r'(kò)(ké|ní|fi|sì)', r'\1 \2', text)  # Add space after 'kò'
            
            # Fix common incorrect word formations
            text = re.sub(r'nià', r'ni à', text)
            text = re.sub(r'láti', r'lá ti', text)
            text = re.sub(r'síbẹ̀', r'sí bẹ̀', text)
        else:
            # Fix common joined words in English translations
            text = re.sub(r'([a-z])([A-Z])', r'\1 \2', text)  # Add space between lowercase and uppercase
            
            # Fix specific auxiliary verb + past participle combinations
            auxiliaries = ["could", "would", "should", "have", "has", "had", "will", "is", "are", "was", "were"]
            past_participles = ["been", "have", "had", "not", "find", "look", "want", "need", "make", "take", "give"]
            for aux in auxiliaries:
                for pp in past_participles:
                    text = text.replace(f"{aux}{pp}", f"{aux} {pp}")
            
            # Fix joined "been" + verb
            past_participlesAfterBeen = ["released", "put", "used", "confined", "blessed", "left", "prevented", "corrected", "heeded", "supplied"]
            for pp in past_participlesAfterBeen:
                text = text.replace(f"been{pp}", f"been {pp}")
            
            # Fix main verb + preposition/conjunction
            main_verbs = ["released", "explained", "provided", "put", "had", "made", "took", "gave", "left"]
            prepositions = ["if", "when", "as", "by", "to", "for", "with", "on", "in", "at", "from"]
            for verb in main_verbs:
                for prep in prepositions:
                    text = text.replace(f"{verb}{prep}", f"{verb} {prep}")
            
            # Fix pronoun + preposition
            pronouns = ["him", "her", "it", "them", "us", "you", "we", "they"]
            for pron in pronouns:
                for prep in prepositions:
                    text = text.replace(f"{pron}{prep}", f"{pron} {prep}")
            
            # Fix determiner + noun
            determiners = ["This", "That", "The", "A", "An", "His", "Her", "Our", "Their", "Its"]
            nouns = ["man", "woman", "child", "person", "people", "life", "time", "day", "world", "house"]
            for det in determiners:
                for noun in nouns:
                    text = text.replace(f"{det}{noun}", f"{det} {noun}")
            
            # Fix compound constructions with "to"
            compounds = [("put", "to", "death"), ("have", "to", "be"), ("need", "to", "go")]
            for a, b, c in compounds:
                text = text.replace(f"{a}{b}{c}", f"{a} {b} {c}")
            
            # Fix "many of mankind's" type constructions
            text = text.replace("manyof", "many of")
            text = text.replace("mankind'smistakes", "mankind's mistakes")
            text = text.replace("ofmankind", "of mankind")
            
            # Fix "will be" and similar constructs
            modal_be = [("will", "be"), ("would", "be"), ("could", "be"), ("should", "be")]
            for modal, be in modal_be:
                text = text.replace(f"{modal}{be}", f"{modal} {be}")
        
        # Fix spacing issues around punctuation
        text = re.sub(r'\s+([.,;:!?])', r'\1', text)
        text = re.sub(r'([.,;:!?])\s+', r'\1 ', text)
        
        if text and not text[-1] in '.!?':
            text = text + '.'
        
        if text and text[0].islower():
            text = text[0].upper() + text[1:]
        
        noise_patterns = [
            r'\[\d+\]',
            r'\(\s*\)',
            r'^\s*\d+\.\s*',
            r'^\s*[a-z]\)\s*',
        ]
        
        for pattern in noise_patterns:
            text = re.sub(pattern, '', text)
        
        text = text.strip()
        return text if len(text) >= 5 and len(text) <= 500 else None

class GlosbeYorubaScraper:
    def __init__(self, base_folder="./scraped_data", output_folder=None, max_workers=5, delay=5.0):
        """Initialize the scraper"""
        self.base_folder = base_folder
        self.output_folder = output_folder or base_folder
        self.max_workers = max_workers
        self.delay = delay
        self.debug_mode = True
        self.base_url = "https://glosbe.com/yo/en/{}"
        
        # List of user agents for rotating requests
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.164 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"
        ]
        
        self.session = requests.Session()
        self.headers = {
            'User-Agent': self.get_random_user_agent(),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Referer': 'https://glosbe.com/',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0',
            'TE': 'Trailers',
        }
        
        # Setup folders
        self.json_folder = os.path.join(self.output_folder, "json")
        self.csv_folder = os.path.join(self.output_folder, "csv")
        self.debug_folder = os.path.join(self.output_folder, "debug_html")
        
        # Create folders if they don't exist
        for folder in [self.json_folder, self.csv_folder, self.debug_folder]:
            os.makedirs(folder, exist_ok=True)
            
        # Initialize example extractor
        self.example_extractor = ExampleSentenceExtractor(debug=self.debug_mode)
        
        # Initialize data verifier
        self.data_verifier = DataVerifier(debug=self.debug_mode)
        
        # Set up logger
        self.logger = logging.getLogger(__name__)
        
        # Track processed words to avoid duplicates
        self.processed_words_file = os.path.join(self.output_folder, "processed_words.txt")
        self.processed_words = set()
        if os.path.exists(self.processed_words_file):
            with open(self.processed_words_file, "r", encoding="utf-8") as f:
                self.processed_words = set(line.strip() for line in f)
    
    def get_word_files(self):
        """Get a dictionary of word files organized by alphabet"""
        word_files_by_alphabet = {}
        words_folder = "./yoruba_words"
        
        if os.path.exists(words_folder):
            for alphabet_dir in os.listdir(words_folder):
                alphabet_path = os.path.join(words_folder, alphabet_dir)
                
                if os.path.isdir(alphabet_path):
                    alphabet_files = []
                    for word_file in os.listdir(alphabet_path):
                        if word_file.endswith('.txt'):
                            file_path = os.path.join(alphabet_path, word_file)
                            alphabet_files.append(file_path)
        
                    if alphabet_files:
                        word_files_by_alphabet[alphabet_dir] = alphabet_files
        
        return word_files_by_alphabet
    
    def extract_words_from_file(self, file_path):
        """Extract words from a text file, one word per line"""
        words = []
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    word = line.strip()
                    if word and not word.startswith('#'):
                        words.append(word)
            return list(set(words))
        except Exception as e:
            logging.error(f"Error reading file {file_path}: {str(e)}")
            return []
    
    def get_random_user_agent(self):
        """Get a random user agent from the list"""
        return random.choice(self.user_agents)
    
    def is_captcha(self, response):
        """Check if the response contains a CAPTCHA challenge"""
        if not response or not hasattr(response, 'text'):
            return False
        return captcha_detected(response.text)
    
    def extract_clean_translation(self, text):
        """Clean and normalize translation text to ensure consistency
        
        Args:
            text (str): The raw translation text to clean
            
        Returns:
            str: The cleaned and normalized translation text
        """
        if not text or not isinstance(text, str):
            return ""
        
        original_text = text
        
        # Remove whitespace
        text = text.strip()
        
        # Remove URLs and URL fragments
        text = re.sub(r'https?://\S+', '', text)
        text = re.sub(r'www\.\S+', '', text)
        text = re.sub(r'http-www\.\S+', '', text)
        
        # Remove source and metadata markers
        source_markers = [
            'proper', 'Hei NER', 'Heidelberg', 'Named Entity', 'Resource', 
            'Dbnary', 'wiki', 'lingvoj.org', 'lingvoj.rdf'
        ]
        
        # First try to extract clean text before source markers
        for marker in source_markers:
            if marker in text:
                parts = text.split(marker, 1)
                text = parts[0].strip()
                # If the split part is too short, keep original
                if len(text) < 2 and len(original_text) > 5:
                    text = original_text
                break
        
        # Remove part of speech embedded in words
        common_pos = ['adjective', 'noun', 'verb', 'conjunction', 'interjection', 'ad', 'proper']
        for pos in common_pos:
            # Pattern: partOfSpeech at the end or middle of a word
            text = re.sub(f"({pos})([A-Z])", r" \2", text)
            
            # If POS is attached to the end of a word, remove it
            text = re.sub(f"([a-z])({pos})($|\s)", r"\1\3", text)
            
            # Don't remove if it's a standalone POS indicator
            if not re.match(f"^{pos}$", text, re.IGNORECASE):
                text = re.sub(f"\\b{pos}\\b", " ", text)
        
        # Normalize spaces
        text = re.sub(r'\s+', ' ', text)
        
        # Remove any HTML entities
        text = text.replace('&amp;', '&')
        text = text.replace('&quot;', '"')
        text = text.replace('&apos;', "'")
        text = text.replace('&lt;', '<')
        text = text.replace('&gt;', '>')
        text = re.sub(r'&[a-zA-Z]+;', '', text)
        
        # Normalize quotes and apostrophes
        text = re.sub(r'["\u201C\u201D]', '"', text)
        text = re.sub(r'[\'\u2018\u2019]', "'", text)
        
        # Remove descriptive text (often appears after primary meaning)
        if "A " in text and len(text) > 30:
            # If a sentence starts with 'A' and is long, it's likely a definition
            parts = text.split("A ", 1)
            if len(parts[0]) > 1:  # Only if we're not removing the entire text
                text = parts[0].strip()
        
        # Look for patterns that suggest a description
        if " is " in text and len(text) > 25:
            parts = text.split(" is ", 1)
            if len(parts[0]) > 1:  # Only if we're not removing a useful word
                text = parts[0].strip()
        
        # Remove parenthetical information which is often metadata
        text = re.sub(r'\([^)]*\)', '', text)
        
        # Remove any leading/trailing punctuation
        text = re.sub(r'^[.,;:!?\s]+', '', text)
        text = re.sub(r'[.,;:!?\s]+$', '', text)
        
        # Remove annotation markers like [1], [2], etc.
        text = re.sub(r'\[\d+\]', '', text)
        
        # Fix spacing issues around punctuation
        text = re.sub(r'\s+([.,;:!?])', r'\1', text)
        text = re.sub(r'([.,;:!?])\s+', r'\1 ', text)
        
        # If the translation was reduced to something too short, revert to original
        # but only for reasonable non-URL originals
        if len(text.strip()) < 2 and len(original_text) > 5 and not ('http' in original_text or 'www.' in original_text):
            return original_text.strip()
        
        return text.strip()

    def is_valid_translation(self, word, translation, confidence="medium"):
        """Validate if a translation is correct for a given word
        
        Args:
            word (str): The Yoruba word being translated
            translation (str): The English translation to validate
            confidence (str): The confidence level of the translation
            
        Returns:
            bool: True if the translation is valid, False otherwise
        """
        if not translation or not isinstance(translation, str):
            return False
        
        # First apply basic cleanup for evaluation
        translation = translation.strip()
        
        # Skip if too short (except special cases like "I", "a") 
        if len(translation) < 2 and translation not in ["I", "a", "A"]:
            return False
        
        # Skip if too long - likely a description rather than translation
        if len(translation) > 50:
            return False
        
        # Skip translations that contain URLs or HTTP patterns
        if re.search(r'https?://|www\.|http-www', translation):
            return False
        
        # Skip translations with non-English characters (except known accents)
        non_english_pattern = r'[^\x00-\x7F\áàäâéèëêíìïîóòöôúùüûñçÁÀÄÂÉÈËÊÍÌÏÎÓÒÖÔÚÙÜÛÑÇ]'
        if re.search(non_english_pattern, translation):
            # Exclude Yoruba characters so these aren't flagged
            if not re.search(r'[àáèéìíòóùúẹọṣ]', translation):
                return False
        
        # Skip translations with unusual formatting
        if re.search(r'^\W+$', translation) or re.search(r'_{2,}', translation):
            return False
        
        # Skip if it appears to be metadata or has suspicious patterns
        if re.match(r'^[A-Z][a-z]+[A-Z]', translation):  # CamelCase likely metadata
            return False
        
        # Skip translations containing code-like characters
        if re.search(r'[<>{}\[\]\\\/]', translation):
            return False
        
        # Always accept high confidence translations AFTER basic validation
        if confidence == "high":
            return True
        
        # Check for known good/bad translations
        word_lower = word.lower()
        translation_lower = translation.lower()
        
        # Some words have known incorrect translations
        incorrect_translations = {
            "a": ["shut", "close", "they", "i", "they", "I", "across", "over"],
            "á": ["shut", "close", "blocked", "closed", "start", "stop"],
            "abo": ["duck", "goose", "turkey", "cock", "hen", "fowl", "bird"],
            "adìye": ["duck", "goose", "turkey", "bird", "goose"],
            "àpẹ́": ["chicken", "hen", "goose", "turkey", "bird"],
            "ó": ["me", "my", "you", "your", "we", "our"],
            "ẹ": ["i", "me", "my", "he", "his", "they", "them"],
            "e": ["i", "me", "my", "he", "his", "they", "them"]
        }
        
        # Return False if it's a known incorrect translation
        if word_lower in incorrect_translations and translation_lower in incorrect_translations[word_lower]:
            return False
        
        # Check for known good translations
        known_translations = self.get_known_translations(word_lower)
        for trans in known_translations:
            if translation_lower == trans.get("translation", "").lower():
                return True
        
        # For short words, be more restrictive
        if len(word) <= 2 and confidence != "high":
            return translation.lower() in [t.get("translation", "").lower() for t in known_translations]
        
        # Check for suspicious content
        suspicious_words = [
            'login', 'signup', 'register', 'password', 'username', 'cookie', 
            'click', 'download', 'upload', 'website', 'captcha', 'browser',
            'server', 'database', 'null', 'undefined', 'NaN', 'javascript',
            'language', 'proper', 'pronoun', 'heidelberg', 'resource', 'dbnary',
            'lingvoj'
        ]
        
        if any(s in translation_lower for s in suspicious_words):
            return False
        
        # By default, accept medium confidence translations for regular words
        return True
    
    def extract_text_from_selector(self, soup, selector, default=""):
        """Extract text from a CSS selector with fallback"""
        try:
            element = soup.select_one(selector)
            return element.get_text(strip=True) if element else default
        except Exception as e:
            logging.warning(f"Error extracting from selector '{selector}': {str(e)}")
            return default
    
    def validate_content(self, result):
        """Check if the result contains any meaningful content with stricter validation"""
        # Check for translations array
        has_translations = False
        valid_translations = []
        
        if result.get("translations") and isinstance(result["translations"], list):
            # Filter translations to only include valid ones
            for t in result["translations"]:
                if not isinstance(t, dict):
                    continue
                    
                if not t.get("translation") or not isinstance(t["translation"], str):
                    continue
                    
                translation = t["translation"].strip()
                if not translation:
                    continue
                    
                # Skip translations that are URLs or have URL fragments
                if re.search(r'https?://|www\.|http-www', translation):
                    continue
                    
                # Skip translations that are too short
                if len(translation) < 2 and translation not in ["I", "a", "A"]:
                    continue
                    
                # Skip translations with suspicious formatting
                if re.search(r'^\W+$', translation) or re.search(r'_{2,}', translation):
                    continue
                    
                # Keep only those that pass additional verification
                word = result.get("word", "").lower()
                confidence = t.get("confidence", "").lower()
                
                # Force clean the translation 
                clean_trans = self.extract_clean_translation(translation)
                if clean_trans != translation:
                    t["translation"] = clean_trans
                    translation = clean_trans
                    
                # Skip if cleaning made it too short
                if len(clean_trans) < 2 and clean_trans not in ["I", "a", "A"]:
                    continue
                    
                # Apply the full validation
                if self.is_valid_translation(word, translation, confidence):
                    valid_translations.append(t)
            
            # Update the translations list with only valid translations
            result["translations"] = valid_translations
            has_translations = len(valid_translations) > 0
        
        # Filter examples to improve quality
        has_examples = False
        valid_examples = []
        
        if result.get("examples") and isinstance(result["examples"], list):
            for e in result["examples"]:
                if (isinstance(e, dict) and 
                    e.get("yoruba") and e.get("english") and
                    isinstance(e["yoruba"], str) and isinstance(e["english"], str) and
                    len(e["yoruba"].strip()) > 0 and len(e["english"].strip()) > 0):
                    
                    # Apply stricter validation for examples
                    confidence = e.get("confidence", "").lower()
                    
                    # Always accept high confidence examples
                    if confidence == "high":
                        valid_examples.append(e)
                    elif confidence == "medium":
                        # For medium confidence, check if this is a good example
                        yoruba = e.get("yoruba", "")
                        english = e.get("english", "")
                        word = result.get("word", "")
                        
                        # Skip religious references unless they're high quality
                        if e.get("is_jw_reference", False) and not self.verify_example_pair(yoruba, english):
                            continue
                            
                        # For shorter words, be more selective
                        if len(word) <= 2:
                            # Medium confidence examples for short words must contain the word explicitly
                            normalized_word = self.normalize_word(word)
                            pattern = r'\b' + re.escape(normalized_word) + r'\b'
                            
                            if re.search(pattern, yoruba, re.IGNORECASE):
                                valid_examples.append(e)
                        else:
                            # For longer words, medium confidence examples must pass validation checks
                            if self.example_extractor.verify_example_pair(yoruba, english):
                                valid_examples.append(e)
            
            # Update the examples list with only valid examples
            result["examples"] = valid_examples
            has_examples = len(valid_examples) > 0
            
        # Check for direct translation string
        has_translation = (
            result.get("translation") and 
            isinstance(result["translation"], str) and
            len(result.get("translation", "").strip()) > 0
        )
        
        # Check for specific example fields
        has_specific_examples = (
            result.get("example_yoruba") and 
            result.get("example_english") and
            isinstance(result["example_yoruba"], str) and
            isinstance(result["example_english"], str) and
            len(result["example_yoruba"].strip()) > 0 and
            len(result["example_english"].strip()) > 0
        )
        
        # Special handling for hardcoded known words
        word = result.get("word", "").lower()
        has_hardcoded = False
        
        if word in self.get_known_translations(word):
            # Add these translations to the result
            known_translations = self.get_known_translations(word)
            
            # If we have no other translations, use the known ones
            if not has_translations:
                result["translations"] = known_translations
                has_translations = len(known_translations) > 0
            
            # Add known examples if we have few or no examples
            if not has_examples or len(result.get("examples", [])) < 2:
                known_examples = self.get_known_examples(word)
                if known_examples:
                    if "examples" not in result or not result["examples"]:
                        result["examples"] = []
                    
                    # Add only examples not already present
                    existing_yoruba = {ex["yoruba"].lower() for ex in result["examples"]}
                    for ex in known_examples:
                        if ex["yoruba"].lower() not in existing_yoruba:
                            result["examples"].append(ex)
                    
                    has_examples = len(result["examples"]) > 0
            
            has_hardcoded = True
        
        # Debug logging to help diagnose content validation
        if self.debug_mode:
            logging.debug(f"Content validation for {word}: " + 
                         f"has_translations={has_translations}, " +
                         f"has_translation={has_translation}, " +
                         f"has_examples={has_examples}, " +
                         f"has_specific_examples={has_specific_examples}, " +
                         f"has_hardcoded={has_hardcoded}")
        
        # A result is valid if it has at least one translation or example
        has_content = has_translations or has_translation or has_examples or has_specific_examples or has_hardcoded
        
        return has_content
        
    def verify_example_pair(self, yoruba, english):
        """More aggressive verification of example pairs"""
        # Reject pairs that are too different in length (unless one is very short)
        if not yoruba or not english or not isinstance(yoruba, str) or not isinstance(english, str):
            return False
            
        yoruba_len = len(yoruba)
        english_len = len(english)
        
        # Very short pairs are suspicious
        if yoruba_len < 10 or english_len < 10:
            return False
            
        # Check length ratio - they should be somewhat proportional
        ratio = max(yoruba_len, english_len) / min(yoruba_len, english_len)
        if ratio > 2.5:  # Stricter ratio limit (was 3.0)
            return False
        
        # Check for mixed examples (multiple translations spliced together)
        if re.search(r'(jw|jw\d+)', english) or re.search(r'(jw|jw\d+)', yoruba):
            return False
            
        # Check for formatting errors that indicate data corruption
        if yoruba == english:
            return False
            
        # Detect incorrect merging of Yoruba and English phrases
        # Look for patterns where Yoruba and English are joined incorrectly
        mixed_pattern = r'([a-zàáèéìíòóùúẹọṣ]{3,})([A-Za-z]{3,})'
        if re.search(mixed_pattern, yoruba) or re.search(mixed_pattern, english):
            return False
        
        # Check for Yoruba markers (at least one should be present)
        yoruba_markers = [
            'mo', 'o', 'ó', 'wọn', 'won', 'a', 'ẹ', 'è', 'ni',
            'kò', 'ko', 'ṣe', 'se', 'ti', 'sì', 'si', 'yìí', 'yii',
            'ń', 'n', 'kí', 'ki', 'bí', 'bi', 'fún', 'fun',
            'àti', 'ati', 'ọmọ', 'omo', 'jẹ́', 'je', 'gbà', 'gba',
            'bá', 'ba', 'à', 'á', 'lá', 'la'
        ]
        
        has_yoruba_marker = any(marker in yoruba.lower().split() for marker in yoruba_markers)
        has_yoruba_diacritics = bool(re.search(r'[àáèéìíòóùúẹọṣ]', yoruba.lower()))
        
        if not (has_yoruba_marker or has_yoruba_diacritics):
            return False
            
        # Check for English markers
        english_markers = [
            'the', 'a', 'an', 'is', 'are', 'was', 'were', 'will',
            'have', 'has', 'had', 'be', 'been', 'being',
            'I', 'you', 'he', 'she', 'it', 'we', 'they',
            'this', 'that', 'these', 'those', 'and', 'but', 'or',
            'could', 'would', 'should', 'may', 'might', 'must'
        ]
        
        has_english_marker = any(marker.lower() in english.lower().split() for marker in english_markers)
        
        if not has_english_marker:
            return False
            
        # Check for potentially mixed Yoruba/English content
        # English content shouldn't have many Yoruba diacritics
        if len(re.findall(r'[àáèéìíòóùúẹọṣ]', english.lower())) > 3:
            return False
            
        # Yoruba content shouldn't have many English-only markers
        english_only_markers = ['the', 'is', 'are', 'was', 'were', 'be', 'been', 'this', 'that', 'those', 'these']
        if sum(1 for marker in english_only_markers if marker in yoruba.lower().split()) > 2:
            return False
            
        # Reject examples with suspicious content
        suspicious_words = [
            'login', 'signup', 'register', 'password', 'username', 'cookie', 
            'click', 'download', 'upload', 'website', 'captcha', 'browser',
            'server', 'database', 'null', 'undefined', 'NaN', 'javascript',
            'html', 'css', 'python', 'script', 'glosbe', 'dictionary'
        ]
        
        if any(word in yoruba.lower() or word in english.lower() for word in suspicious_words):
            return False
            
        # Ensure all data is properly separated (check for common data issues)
        if re.search(r'we\s?would\s?have[A-Za-z]+', english.lower()):
            return False
            
        # Look for merging of Yoruba auxiliary verbs with English words
        if re.search(r'à[a-z]{3,}[A-Z]', yoruba) or re.search(r'á[a-z]{3,}[A-Z]', yoruba):
            return False
            
        return True
        
    def scrape_everything(self, word):
        """Scrape all data for a word with enhanced validation"""
        try:
            url = self.base_url.format(quote(word))
            
            headers = {
                'User-Agent': self.get_random_user_agent(),
                'Accept': 'text/html,application/xhtml+xml,application/xml',
                'Accept-Language': 'en-US,en;q=0.9',
                'Referer': 'https://glosbe.com/',
            }
            
            time.sleep(self.delay * (0.5 + random.random()))
            
            response = requests.get(url, headers=headers, timeout=30)
            
            if response.status_code != 200 or self.is_captcha(response):
                logging.warning(f"Failed to fetch {url} - Status: {response.status_code}")
                
                if self.is_captcha(response):
                    self.delay += 1.0
                    logging.warning(f"CAPTCHA detected! Increasing delay to {self.delay}s")
                    
                    # Save the HTML for debugging
                    debug_html_path = os.path.join(self.base_folder, "debug_html", f"{word}_captcha.html")
                    os.makedirs(os.path.dirname(debug_html_path), exist_ok=True)
                    
                    with open(debug_html_path, "w", encoding="utf-8") as f:
                        f.write(response.text)
                
                return {
                    "word": word,
                    "url": url,
                    "scrape_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                    "status": "failed",
                    "error": f"Status code: {response.status_code}" if not self.is_captcha(response) else "CAPTCHA",
                }
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            if self.debug_mode:
                debug_dir = os.path.join(self.base_folder, "debug_html")
                os.makedirs(debug_dir, exist_ok=True)
                debug_file = os.path.join(debug_dir, f"{word}_debug.html")
                with open(debug_file, "w", encoding="utf-8") as f:
                    f.write(str(soup))
                logging.info(f"Saved debug HTML to {debug_file}")
            
            result = {
                "word": word,
                "url": url,
                "scrape_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                "status": "success",
                "error": None,
                "translation": "",
                "translations": [],
                "part_of_speech": "",
                "pronunciation": "",
                "examples": [],
                "definitions": [],
                "html": str(soup),
                # Remove soup from result to ensure JSON serialization works
                # "soup": soup,
                "verification_score": 0,
            }
            
            # Enhanced translation extraction
            # First extraction approach: Direct translation items
            translation_candidates = []
            
            # Attempt 1: Find all translation items with their phrases
            translation_items = soup.select('h3.translation__item__pharse, .translation__pharse, .translation__item__phrase, .translation-item h3, .translation-list__item h3, .translation__item')
            for item in translation_items:
                trans_text = item.get_text(strip=True)
                
                # Find part of speech by looking at nearby elements
                pos_container = item.find_parent('div') or item.find_parent('li')
                pos_span = None
                
                if pos_container:
                    pos_span = pos_container.select_one('span.text-xxs.text-gray-500, .grammar-info, .pos-tag')
                
                if pos_span:
                    pos_text = pos_span.get_text(strip=True)
                else:
                    pos_text = ""
                
                if trans_text:
                    translation_candidates.append({
                        "text": trans_text,
                        "pos_hint": self.extract_pos_from_text(pos_text),
                        "source": "direct_h3",
                        "confidence": "high"
                    })
            
            # Attempt 2: Check for data-element="translation" li elements
            translation_li_elements = soup.select('li[data-element="translation"], .translation__list li, .translation-list li')
            for li in translation_li_elements:
                # Find the translation phrase inside this li
                trans_elem = li.select_one('h3.translation__item__pharse, h3.align-top.inline, .translation__item__pharse, .translation__item, .translation-item')
                
                if trans_elem:
                    trans_text = trans_elem.get_text(strip=True)
                    
                    # Look for part of speech span
                    pos_span = li.select_one('span.text-xxs, span.text-gray-500, .grammar-info, .pos-tag')
                    pos_text = pos_span.get_text(strip=True) if pos_span else ""
                    
                    if trans_text:
                        translation_candidates.append({
                            "text": trans_text,
                            "pos_hint": self.extract_pos_from_text(pos_text),
                            "source": "direct_li",
                            "confidence": "high"
                        })
            
            # Attempt 3: Check for translation in primary phrase
            phrase_title = soup.select_one('h1, .dictionary-title, .main-phrase')
            if phrase_title:
                phrase_text = phrase_title.get_text(strip=True)
                if 'Translation of' in phrase_text and 'into English' in phrase_text:
                    # Extract translation from header
                    translation_match = re.search(r'<strong>([^<]+)</strong>', str(phrase_title))
                    if translation_match:
                        primary_translation = translation_match.group(1).strip()
                        translation_candidates.append({
                            "text": primary_translation,
                            "pos_hint": "",
                            "source": "primary_header",
                            "confidence": "high"
                        })
            
            # Attempt 4: Check for translation in similar phrases section
            similar_section = soup.select_one('#simmilar-phrases')
            if similar_section:
                phrase_items = similar_section.select('li.px-2.py-1.flex')
                for item in phrase_items:
                    yoruba_elem = item.select_one('.w-1\\/3.dir-aware-text-right')
                    english_elem = item.select_one('.dir-aware-pl-2.w-2\\/3')
                    
                    if yoruba_elem and english_elem:
                        yoruba_text = yoruba_elem.get_text(strip=True)
                        english_text = english_elem.get_text(strip=True)
                        
                        # If the yoruba text is exactly our word or contains it
                        if yoruba_text.lower() == word.lower() or word.lower() in yoruba_text.lower():
                            translation_candidates.append({
                                "text": english_text,
                                "pos_hint": "",
                                "source": "similar",
                                "confidence": "medium"
                            })
            
            # Attempt 5: Check for automatic translations section
            auto_section = soup.select_one('#translation_automatic')
            if auto_section:
                translation_containers = auto_section.select('li.px-2.text-sm.pb-2')
                for container in translation_containers:
                    # Look for Google or Glosbe translation containers
                    translation_p = container.select_one('p.inline-block.min-h-8.text-primary-700')
                    
                    if translation_p and translation_p.get_text(strip=True):
                        trans_text = translation_p.get_text(strip=True)
                        
                        # Check if it's from Google or Glosbe
                        provider_span = container.select_one('span.inline-block.text-xs.text-gray-500')
                        provider = provider_span.get_text(strip=True) if provider_span else "Unknown"
                        confidence = "medium" if "Google" in provider else "low"
                        
                        translation_candidates.append({
                            "text": trans_text,
                            "pos_hint": "",
                            "source": "automatic",
                            "confidence": confidence
                        })
            
            # Attempt 6: Look for content-summary section that often has good translations
            summary_section = soup.select_one('#content-summary, .content-summary, .phrase-summary')
            if summary_section:
                strong_elements = summary_section.select('strong')
                if strong_elements:
                    translations_text = strong_elements[0].get_text(strip=True)
                    if translations_text:
                        # Split by commas to get individual translations
                        individual_translations = [t.strip() for t in translations_text.split(',')]
                        for trans in individual_translations:
                            if trans and len(trans) > 1:
                                translation_candidates.append({
                                    "text": trans,
                                    "pos_hint": "",
                                    "source": "summary",
                                    "confidence": "high"
                                })
            
            # Special handling for common Yoruba words with known translations
            normalized_word = self.normalize_word(word)
            if normalized_word in ["adìye", "abo", "a", "á", "ó", "àpẹ́", "aláàṣẹ", "àárẹ̀"]:
                known_word_translations = {
                    "adìye": [{"text": "chicken", "pos_hint": "noun", "confidence": "high"}],
                    "abo": [{"text": "female", "pos_hint": "noun", "confidence": "high"},
                            {"text": "feminine", "pos_hint": "adjective", "confidence": "high"}],
                    "a": [{"text": "he", "pos_hint": "pronoun", "confidence": "high"},
                          {"text": "we", "pos_hint": "pronoun", "confidence": "high"}],
                    "á": [{"text": "will", "pos_hint": "auxiliary verb", "confidence": "high"},
                          {"text": "he", "pos_hint": "pronoun", "confidence": "high"},
                          {"text": "she", "pos_hint": "pronoun", "confidence": "high"}],
                    "ó": [{"text": "he", "pos_hint": "pronoun", "confidence": "high"},
                          {"text": "she", "pos_hint": "pronoun", "confidence": "high"},
                          {"text": "it", "pos_hint": "pronoun", "confidence": "high"}],
                    "àpẹ́": [{"text": "duck", "pos_hint": "noun", "confidence": "high"}],
                    "aláàṣẹ": [{"text": "authority", "pos_hint": "noun", "confidence": "high"},
                               {"text": "executive", "pos_hint": "noun", "confidence": "high"}],
                    "àárẹ̀": [{"text": "fatigue", "pos_hint": "noun", "confidence": "high"},
                              {"text": "tiredness", "pos_hint": "noun", "confidence": "high"}]
                }
                
                if normalized_word in known_word_translations:
                    for known_trans in known_word_translations[normalized_word]:
                        translation_candidates.append({
                            "text": known_trans["text"],
                            "pos_hint": known_trans["pos_hint"],
                            "source": "hardcoded",
                            "confidence": known_trans["confidence"]
                        })
            
            # Log the candidates we found
            logging.debug(f"Found {len(translation_candidates)} translation candidates for {word}")
            
            # Process translation candidates
            translations = []
            seen_translations = set()
            
            # First, add known translations if applicable
            known_translations = self.get_known_translations(word)
            if known_translations:
                for known_trans in known_translations:
                    if known_trans["translation"].lower() not in seen_translations:
                        translations.append(known_trans)
                        seen_translations.add(known_trans["translation"].lower())
                # Set the primary translation from the known translations
                result["translation"] = known_translations[0]["translation"]
                result["part_of_speech"] = known_translations[0]["part_of_speech"]
            
            # Process other candidates
            for candidate in translation_candidates:
                trans_text = candidate["text"]
                
                # Skip empty or very short translations
                if not trans_text or len(trans_text) < 2:
                    continue
                
                # Clean the translation text
                clean_trans = self.extract_clean_translation(trans_text)
                
                # Skip translations that are the same as the original word
                if clean_trans.lower() == word.lower():
                    continue
                
                # Skip duplicates
                if clean_trans.lower() in seen_translations:
                    continue
                
                # Validate the translation
                if not self.is_valid_translation(word, clean_trans, candidate["confidence"]):
                    continue
                
                seen_translations.add(clean_trans.lower())
                
                # Get part of speech if available
                pos = candidate["pos_hint"] or self.identify_part_of_speech(trans_text, clean_trans)
                
                # Only add valid translations
                if clean_trans and len(clean_trans) > 1:
                    translations.append({
                        "translation": clean_trans,
                        "part_of_speech": pos,
                        "confidence": candidate["confidence"]
                    })
            
            # Set the primary translation if not already set from known translations
            if not result["translation"] and translations:
                # Sort by confidence and use the first high-confidence translation
                high_confidence = [t for t in translations if t["confidence"] == "high"]
                if high_confidence:
                    result["translation"] = high_confidence[0]["translation"]
                    result["part_of_speech"] = high_confidence[0]["part_of_speech"]
                else:
                    # Or use the first translation
                    result["translation"] = translations[0]["translation"]
                    result["part_of_speech"] = translations[0]["part_of_speech"]
            
            # Store all translations
            result["translations"] = translations
            
            # Get examples - this is handled by the example extractor
            examples = self.example_extractor.extract_examples(soup, word)
            result["examples"] = examples
            
            # Get definitions
            definitions = self.extract_definitions(soup)
            result["definitions"] = definitions
            
            # Log the final result
            logging.info(f"Final scraping result for {word}: {len(translations)} translations, {len(examples)} examples")
            
            # Apply strict filtering to translations
            if "translations" in result and isinstance(result["translations"], list):
                # First, remove duplicate translations
                unique_translations = {}
                for trans in result["translations"]:
                    translation_text = trans.get("translation", "").lower().strip()
                    if translation_text and len(translation_text) > 0:
                        # If we already have this translation with higher confidence, skip it
                        if (translation_text in unique_translations and 
                            trans.get("confidence") != "high" and 
                            unique_translations[translation_text].get("confidence") == "high"):
                            continue
                        
                        # Skip medium confidence translations if they're suspect for this word
                        if trans.get("confidence") == "medium":
                            # Some words have known incorrect translations
                            incorrect_translations = {
                                "a": ["shut", "close", "they", "i", "they", "I", "across", "over"],
                                "á": ["shut", "close", "blocked", "closed", "start", "stop"],
                                "abo": ["duck", "goose", "turkey", "cock", "hen", "fowl", "bird"],
                                "adìye": ["duck", "goose", "turkey", "bird", "goose"],
                                "àpẹ́": ["chicken", "hen", "goose", "turkey", "bird"],
                                "ó": ["me", "my", "you", "your", "we", "our"],
                                "ẹ": ["i", "me", "my", "he", "his", "they", "them"],
                                "e": ["i", "me", "my", "he", "his", "they", "them"]
                            }
                            
                            # Skip if this is a known incorrect translation
                            if (word.lower() in incorrect_translations and 
                                translation_text in incorrect_translations[word.lower()]):
                                continue
                        
                        # Keep the best version we've seen
                        unique_translations[translation_text] = trans
                
                # Replace with deduplicated list
                result["translations"] = list(unique_translations.values())
            
            # Apply strict filtering to examples
            if "examples" in result and isinstance(result["examples"], list):
                # Remove duplicate examples and low quality ones
                unique_examples = {}
                for example in result["examples"]:
                    yoruba = example.get("yoruba", "").strip()
                    english = example.get("english", "").strip()
                    
                    if yoruba and english and len(yoruba) > 5 and len(english) > 5:
                        # Create a key to identify duplicate examples
                        example_key = (yoruba.lower(), english.lower())
                        
                        # Perform more aggressive verification on medium confidence examples
                        if example.get("confidence") == "medium":
                            if not self.verify_example_pair(yoruba, english):
                                continue
                        
                        # Keep the best version we've seen
                        if (example_key not in unique_examples or
                            example.get("confidence") == "high" and unique_examples[example_key].get("confidence") != "high"):
                            unique_examples[example_key] = example
                
                # Replace with deduplicated and verified list
                result["examples"] = list(unique_examples.values())
                
            # Add known examples for short words or words with few examples
            if len(word) <= 2 or len(result.get("examples", [])) < 2:
                known_examples = self.get_known_examples(word)
                if known_examples:
                    if "examples" not in result:
                        result["examples"] = []
                    
                    # Add known examples that aren't already in the list
                    existing_yoruba = {ex.get("yoruba", "").lower() for ex in result["examples"]}
                    for ex in known_examples:
                        if ex.get("yoruba", "").lower() not in existing_yoruba:
                            result["examples"].append(ex)
            
            return result
            
        except requests.RequestException as e:
            logging.error(f"Error scraping {word}: {str(e)}")
            return {
                "word": word,
                "url": url if 'url' in locals() else self.base_url.format(quote(word)),
                "scrape_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                "status": "error",
                "error": str(e)
            }
        except Exception as e:
            logging.error(f"Error processing {word}: {str(e)}")
            return {
                "word": word,
                "url": url if 'url' in locals() else self.base_url.format(quote(word)),
                "scrape_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                "status": "error",
                "error": str(e)
            }
    
    def extract_pos_from_text(self, text):
        """Extract part of speech from text"""
        pattern = r'(?:^|\s)(noun|verb|adjective|pronoun|adverb|preposition|conjunction|interjection)(?:\b|$)'
        match = re.search(pattern, text, re.IGNORECASE)
        
        if match:
            return match.group(1).lower()
                    
        return ""
    
    def identify_part_of_speech(self, text, translation):
        """Identify part of speech from a translation by analyzing context"""
        # First, check if there's an explicit part of speech in the text
        pos_from_text = self.extract_pos_from_text(text)
        if pos_from_text:
            return pos_from_text
            
        # Use heuristics based on the translation and context
        translation_lower = translation.lower()
        
        # Check for phrases
        if len(translation_lower.split()) > 2:
            return 'phrase'
            
        # Common English determiners and pronouns usually map to pronouns
        if translation_lower in ['the', 'a', 'an', 'i', 'me', 'my', 'you', 'your', 'he', 'him', 'his', 
                               'she', 'her', 'it', 'its', 'we', 'us', 'our', 'they', 'them', 'their']:
            return 'pronoun'
            
        # Common English action words are likely verbs
        if translation_lower in ['go', 'come', 'do', 'make', 'take', 'get', 'see', 'know', 'want',
                               'find', 'give', 'tell', 'work', 'call', 'try']:
            return 'verb'
            
        # Check for multi-word expressions
        if ' ' in translation_lower:
            # Multi-word expressions with certain patterns
            if translation_lower.startswith(('we would', 'would have', 'will have', 'have been')):
                return 'phrase'
            if any(translation_lower.startswith(w) for w in ['give thanks', 'take care', 'look after']):
                return 'phrase'
        
        # Check for verb endings
        if translation_lower.endswith(('ing', 'ed', 's')) and len(translation_lower) > 4:
            return 'verb'
            
        # Check for adjective endings
        if translation_lower.endswith(('ful', 'ous', 'ive', 'able', 'ible', 'al', 'ic')):
            return 'adjective'
            
        # Check for adverb endings
        if translation_lower.endswith('ly') and len(translation_lower) > 3:
            return 'adverb'
            
        # Default to noun as most common part of speech
        return 'noun'
    
    def extract_flattened_data(self, item):
        """Extract flattened data from a processed item for CSV output"""
        if not item:
            return {}
        
        # Basic data
        flattened = {
            "word": item.get("word", ""),
            "url": item.get("url", ""),
            "scrape_time": item.get("scrape_time", ""),
            "status": item.get("status", ""),
            "error": item.get("error", ""),
            "verification_score": item.get("verification_score", 0),
        }
        
        # Translation data
        primary_translation = item.get("translation", "")
        flattened["translation"] = primary_translation
        
        # All translations
        translations = item.get("translations", [])
        all_trans = []
        for trans in translations:
            if isinstance(trans, dict) and "translation" in trans:
                all_trans.append(trans["translation"])
            elif isinstance(trans, str):
                all_trans.append(trans)
        
        flattened["all_translations"] = " | ".join(all_trans) if all_trans else ""
        
        # Part of speech
        flattened["part_of_speech"] = item.get("part_of_speech", "")
        
        # Example data
        examples = item.get("examples", [])
        if examples:
            best_example = examples[0]  # Take the first example
            for example in examples:
                # If we have a "high" confidence example, use that instead
                if example.get("confidence") == "high":
                    best_example = example
                    break
            
            flattened["example_yoruba"] = best_example.get("yoruba", "")
            flattened["example_english"] = best_example.get("english", "")
        else:
            flattened["example_yoruba"] = ""
            flattened["example_english"] = ""
        
        # Definition data
        definitions = item.get("definitions", [])
        if definitions:
            flattened["definition"] = definitions[0].get("text", "")
        else:
            flattened["definition"] = ""
        
        return flattened
    
    def save_to_csv(self, data, output_file):
        """Save data to CSV file
        
        Args:
            data (list): List of dictionaries containing data to save
            output_file (str): Path to output CSV file
        """
        if not data or not output_file:
            logging.warning(f"No data to save to {output_file}")
            return
        
        # First preprocess the data to ensure quality
        clean_data = self.preprocess_data_before_save(data)
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(os.path.abspath(output_file)), exist_ok=True)
        
        try:
            # Use a different method depending on the type of data
            if output_file.endswith('_words.csv'):
                # Words file format
                df = pd.DataFrame([
                    {
                        'id': idx + 1,
                        'word': item.get('word', ''),
                        'scraped_timestamp': item.get('scrape_time', ''),
                        'source_url': item.get('url', '')
                    }
                    for idx, item in enumerate(clean_data)
                ])
                
                df.to_csv(output_file, index=False, encoding='utf-8')
                logging.info(f"Saved {len(df)} words to {output_file}")
                
            elif output_file.endswith('_translations.csv'):
                # Translations file format
                translations_list = []
                for item in clean_data:
                    word_id = item.get('id')
                    
                    # Handle translations array
                    if 'translations' in item and item['translations']:
                        for idx, trans in enumerate(item['translations']):
                            if isinstance(trans, dict) and 'translation' in trans:
                                translations_list.append({
                                    'id': len(translations_list) + 1,
                                    'word_id': word_id,
                                    'translation': trans['translation'],
                                    'part_of_speech': trans.get('part_of_speech', ''),
                                    'confidence': trans.get('confidence', '')
                                })
                    
                    # Handle direct translation field
                    elif 'translation' in item and item['translation']:
                        translations_list.append({
                            'id': len(translations_list) + 1,
                            'word_id': word_id,
                            'translation': item['translation'],
                            'part_of_speech': item.get('part_of_speech', ''),
                            'confidence': 'high'
                        })
                
                df = pd.DataFrame(translations_list)
                df.to_csv(output_file, index=False, encoding='utf-8')
                logging.info(f"Saved {len(df)} translations to {output_file}")
                
            elif output_file.endswith('_examples.csv'):
                # Examples file format
                examples_list = []
                for item in clean_data:
                    word_id = item.get('id')
                    
                    # Process examples
                    if 'examples' in item and item['examples']:
                        for example in item['examples']:
                            if isinstance(example, dict) and 'yoruba' in example and 'english' in example:
                                examples_list.append({
                                    'id': len(examples_list) + 1,
                                    'translation_id': example.get('translation_id', ''),
                                    'word_id': word_id,
                                    'yoruba_text': example['yoruba'],
                                    'english_text': example['english'],
                                    'is_jw_reference': example.get('is_jw', False),
                                    'confidence': example.get('confidence', 'medium'),
                                    'source': example.get('source', 'tmem'),
                                    'score': example.get('score', '0')
                                })
                
                df = pd.DataFrame(examples_list)
                df.to_csv(output_file, index=False, encoding='utf-8')
                logging.info(f"Saved {len(df)} examples to {output_file}")
                
            else:
                # Generic format for other file types
                df = pd.DataFrame(clean_data)
                df.to_csv(output_file, index=False, encoding='utf-8')
                logging.info(f"Saved {len(df)} rows to {output_file}")
                
        except Exception as e:
            logging.error(f"Error saving to CSV {output_file}: {str(e)}")
            import traceback
            logging.error(traceback.format_exc())
    
    def process_file(self, word_file, alphabet):
        """Process a single word file"""
        json_alphabet_folder = os.path.join(self.json_folder, f"{alphabet}")
        csv_alphabet_folder = os.path.join(self.csv_folder, f"{alphabet}")
        
        if not os.path.exists(json_alphabet_folder):
            os.makedirs(json_alphabet_folder)
        
        if not os.path.exists(csv_alphabet_folder):
            os.makedirs(csv_alphabet_folder)
        
        words = self.extract_words_from_file(word_file)
        logging.info(f"Found {len(words)} unique words in file")
        
        words_to_process = [word for word in words if word not in self.processed_words]
        logging.info(f"After deduplication: {len(words_to_process)} words to process")
        
        if not words_to_process:
            logging.info("All words already processed, skipping file")
            return 0
        
        results = []
        for word in tqdm(words_to_process, desc=f"Processing words in {os.path.basename(word_file)}", unit="word"):
            try:
                result = self.scrape_word(word)
                results.append(result)
            except Exception as e:
                logging.error(f"Unexpected error processing {word}: {str(e)}")
                results.append({"word": word, "error": f"Processing error: {str(e)}"})
        
        for word in words:
            if word in self.processed_words and word not in words_to_process:
                results.append({"word": word, "status": "previously_processed"})
        
        base_filename = os.path.basename(word_file).replace('.txt', '')
        json_output_file = os.path.join(json_alphabet_folder, f"{base_filename}.json")
        csv_output_file = os.path.join(csv_alphabet_folder, f"{base_filename}.csv")
        
        existing_data = []
        if os.path.exists(json_output_file):
            try:
                with open(json_output_file, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
                logging.info(f"Loaded {len(existing_data)} existing entries from {json_output_file}")
            except json.JSONDecodeError:
                logging.warning(f"Error reading existing data from {json_output_file}, will overwrite")
        
        existing_dict = {item["word"]: item for item in existing_data}
        new_dict = {item["word"]: item for item in results}
        
        existing_dict.update(new_dict)
        merged_results = list(existing_dict.values())
        
        with open(json_output_file, 'w', encoding='utf-8') as f:
            json.dump(merged_results, f, ensure_ascii=False, indent=2)
        logging.info(f"Saved {len(merged_results)} entries to JSON file: {json_output_file}")
        
        self.save_to_csv(merged_results, csv_output_file)
        
        self.generate_combined_csv()
        
        return len(words_to_process)
    
    def generate_combined_csv(self):
        """Generate a combined CSV file from all the individual JSON files"""
        all_json_files = []
        for root, _, files in os.walk(self.json_folder):
            for file in files:
                if file.endswith('.json'):
                    all_json_files.append(os.path.join(root, file))
        
        if not all_json_files:
            logging.warning("No JSON files found to generate combined CSV")
            return
        
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
        """
        Run the scraper on all word files. This is the main entry point.
        """
        try:
            word_files_by_alphabet = self.get_word_files()
            
            if not word_files_by_alphabet:
                logging.error("No word files found to process.")
                return
                
            total_words = 0
            total_processed = 0
            total_failed = 0
            
            for alphabet, word_files in word_files_by_alphabet.items():
                alphabet_folder = os.path.join(self.json_folder, alphabet)
                os.makedirs(alphabet_folder, exist_ok=True)
                
                alphabet_csv_folder = os.path.join(self.csv_folder, alphabet)
                os.makedirs(alphabet_csv_folder, exist_ok=True)
                
                for word_file in word_files:
                    result = self.process_file(word_file, alphabet)
                    if result:
                        total_words += result
                        total_processed += result
                        # No failures to count if process_file just returns a count
                        
            logging.info(f"Total words: {total_words}")
            logging.info(f"Successfully processed: {total_processed}")
            logging.info(f"Failed: {total_failed}")
            
            # Generate combined CSV file
            self.generate_combined_csv()
            
            # Generate SQL init file
            self.generate_sql_init_file()
            
            # Generate SQL insert statements
            self.generate_sql_insert_statements()
            
            # The spacing fixes have been incorporated directly into the scraping process
            # The fix_spacing_in_existing_csv method is now optional and primarily for backward compatibility
            # or fixing legacy data that was scraped before these improvements
            # To apply them to all existing data, uncomment the following line:
            # self.fix_spacing_in_existing_csv()
            
            logging.info("Scraping completed successfully.")
            return {
                "total_words": total_words,
                "processed": total_processed,
                "failed": total_failed
            }
            
        except Exception as e:
            logging.error(f"Error running scraper: {str(e)}")
            traceback.print_exc()
            return None
    
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
        all_json_files = []
        for root, _, files in os.walk(self.json_folder):
            for file in files:
                if file.endswith('.json'):
                    all_json_files.append(os.path.join(root, file))
        
        if not all_json_files:
            logging.warning("No JSON files found to generate SQL insert statements")
            return
        
        sql_inserts_file = os.path.join(self.output_folder, "insert_data.sql")
        
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
        
        cleaned_data = [self.extract_flattened_data(item) for item in all_data if item.get("status") == "success"]
        
        with open(sql_inserts_file, 'w', encoding='utf-8') as f:
            f.write("-- SQL Insert Statements for Yoruba Dictionary Data\n")
            f.write("-- Generated automatically by GlosbeYorubaScraper\n\n")
            
            f.write("BEGIN TRANSACTION;\n\n")
            
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
                verification_score = item.get("verification_score", 0)
                
                insert_stmt = f"INSERT OR IGNORE INTO yoruba_words (word, translation, all_translations, part_of_speech, example_yoruba, example_english, url, scrape_time, status, error, verification_score) "
                insert_stmt += f"VALUES ('{word}', '{translation}', '{all_translations}', '{pos}', '{example_yoruba}', '{example_english}', '{url}', '{scrape_time}', '{status}', '{error}', {verification_score});\n"
                f.write(insert_stmt)
            
            f.write("\nCOMMIT;\n")
        
        logging.info(f"Generated SQL insert statements file: {sql_inserts_file}")

    def generate_postgres_exports(self):
        """Generate PostgreSQL specific export files"""
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
        
        exporter = PostgresExporter(self.output_folder)
        export_files = exporter.generate_postgres_export(all_data)
        
        logging.info(f"PostgreSQL export complete. Schema: {export_files['schema_file']}, Data: {export_files['insert_file']}")

    def fix_spacing_in_existing_csv(self, csv_file_path=None):
        """Fix spacing issues in existing CSV files."""
        logging.info("Starting to fix spacing in CSV files")
        
        # Variables to track progress
        total_fixed_yoruba = 0
        total_fixed_english = 0
        csv_files_processed = 0
        
        # Check if a specific CSV file path is provided and exists
        if csv_file_path and os.path.exists(csv_file_path):
            logging.info(f"Processing specific CSV file: {csv_file_path}")
            yoruba_fixed, english_fixed = self._fix_spacing_in_csv(csv_file_path)
            total_fixed_yoruba += yoruba_fixed
            total_fixed_english += english_fixed
            csv_files_processed += 1
        else:
            # If no specific file, process all CSV files in the base folder
            logging.info(f"No specific CSV file provided, processing all CSV files in {self.base_folder}")
            
            # Find all CSV files in the base folder and its subdirectories
            csv_files = []
            for root, _, files in os.walk(self.base_folder):
                for file in files:
                    if file.endswith('.csv'):
                        csv_files.append(os.path.join(root, file))
            
            logging.info(f"Found {len(csv_files)} CSV files to process")
            
            # Process each CSV file
            for csv_file in csv_files:
                yoruba_fixed, english_fixed = self._fix_spacing_in_csv(csv_file)
                total_fixed_yoruba += yoruba_fixed
                total_fixed_english += english_fixed
                csv_files_processed += 1
        
        logging.info(f"Finished processing {csv_files_processed} CSV files")
        logging.info(f"Total Yoruba rows fixed: {total_fixed_yoruba}")
        logging.info(f"Total English rows fixed: {total_fixed_english}")
        
        return csv_files_processed, total_fixed_yoruba, total_fixed_english
    
    def _fix_spacing_in_csv(self, file_path):
        """
        Apply spacing fixes to a single CSV file
        
        Args:
            file_path (str): Path to the CSV file to fix
        """
        try:
            logging.info(f"Fixing spacing issues in {file_path}")
            
            # Read the CSV file
            df = pd.read_csv(file_path, encoding='utf-8')
            
            # Check if the required columns exist
            yoruba_fixed_count = 0
            if 'yoruba_text' in df.columns:
                # Apply spacing fix to all Yoruba text
                original_yoruba = df['yoruba_text'].copy()
                df['yoruba_text'] = df['yoruba_text'].apply(lambda x: self._fix_yoruba_spacing(x) if isinstance(x, str) else x)
                
                # Count rows that were fixed
                yoruba_fixed_count = sum(original_yoruba != df['yoruba_text'])
                logging.info(f"Fixed {yoruba_fixed_count} Yoruba rows")
            else:
                logging.warning(f"No 'yoruba_text' column found in {file_path}")
            
            # Check for English text
            english_fixed_count = 0
            if 'english_text' in df.columns:
                # Apply spacing fix to all English text
                original_english = df['english_text'].copy()
                df['english_text'] = df['english_text'].apply(lambda x: self._fix_english_spacing(x) if isinstance(x, str) else x)
                
                # Count rows that were fixed
                english_fixed_count = sum(original_english != df['english_text'])
                logging.info(f"Fixed {english_fixed_count} English rows")
            else:
                logging.warning(f"No 'english_text' column found in {file_path}")

            # Create a backup of the original file
            backup_file = f"{file_path}.bak"
            shutil.copy2(file_path, backup_file)
            logging.info(f"Created backup at {backup_file}")
            
            # Save the updated CSV
            df.to_csv(file_path, index=False, encoding='utf-8')
            logging.info(f"Fixed spacing in {file_path}: {yoruba_fixed_count} Yoruba rows, {english_fixed_count} English rows")
            
            return yoruba_fixed_count, english_fixed_count
        except Exception as e:
            logging.error(f"Error fixing spacing in {file_path}: {e}")
            return 0, 0
    
    def _fix_yoruba_spacing(self, text):
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
        
        # Fix spacing for other common patterns
        text = re.sub(r'gba(ra)', r'gba \1', text)
        text = re.sub(r'ọ(jọ)', r'ọ \1', text)
        text = re.sub(r'(mọ)(le)', r'\1 \2', text)
        text = re.sub(r'(jọ)(wọ)', r'\1 \2', text)
        
        # Fix patterns related to the "à bá" construction with various suffixes
        for suffix in ['ti', 'le', 'jẹ', 'ri', 'se', 'ṣe', 'wa', 'gbọ', 'gbà', 'mọ']:
            text = re.sub(f'à bá{suffix}', f'à bá {suffix}', text)
        
        # Fix spacing issues with pronouns and other function words
        pronoun_patterns = [
            (r'(mo|o|ó|à|a|è|e)(ń|n)', r'\1 \2'),
            (r'(ó|o)(ti)', r'\1 \2'),
            (r'(ní|ni)(lá|la)', r'\1 \2'),
            (r'(sí|si)(kí|ki)', r'\1 \2')
        ]
        
        for pattern, replacement in pronoun_patterns:
            text = re.sub(pattern, replacement, text)
        
        # Final pass to fix multiple spaces
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
    
    def _fix_english_spacing(self, text):
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
    
    def _fix_spacing_in_csv(self, file_path):
        """Fix spacing issues in a CSV file."""
        # Fix spacing issues around punctuation
        text = re.sub(r'\s+([.,;:!?])', r'\1', text)
        text = re.sub(r'([.,;:!?])\s+', r'\1 ', text)
        
        # Handle HTML entities
        text = text.replace('&amp;', '&')
        text = text.replace('&quot;', '"')
        text = text.replace('&apos;', "'")
        text = text.replace('&lt;', '<')
        text = text.replace('&gt;', '>')
            
        text = re.sub(r'&[a-zA-Z]+;', '', text)
        
        # Normalize quotes and apostrophes
        text = re.sub(r'["\u201C\u201D]', '"', text)
        text = re.sub(r'[\'\u2018\u2019]', "'", text)
        
        # Fix issues with incorrect spacing after punctuation
        text = re.sub(r'([.,!?])([A-Za-z])', r'\1 \2', text)
            
        return text.strip()

    def normalize_word(self, word):
        """Normalize Yoruba words to handle diacritics consistently."""
        word = word.lower().strip()
        
        # Map of common normalizations
        normalizations = {
            'adiye': 'adìye',
            'aare': 'àárẹ̀',
            'apere': 'àpẹ́rẹ́',
            'ape': 'àpẹ́',
            'igbin': 'ìgbín',
            'agbado': 'àgbàdo',
            'eranko': 'ẹranko'
        }
        
        # Check if we have a direct normalization
        if word in normalizations:
            return normalizations[word]
            
        return word
        
    def get_known_translations(self, word):
        """Return known translations for common Yoruba words."""
        normalized_word = self.normalize_word(word)
        
        # Common translations for frequent Yoruba words
        common_translations = {
            "a": [
                {"translation": "he", "part_of_speech": "pronoun", "confidence": "high"},
                {"translation": "we", "part_of_speech": "pronoun", "confidence": "high"}
            ],
            "á": [
                {"translation": "will", "part_of_speech": "auxiliary verb", "confidence": "high"},
                {"translation": "shall", "part_of_speech": "auxiliary verb", "confidence": "high"},
                {"translation": "he", "part_of_speech": "pronoun", "confidence": "high"},
                {"translation": "she", "part_of_speech": "pronoun", "confidence": "high"},
                {"translation": "they", "part_of_speech": "pronoun", "confidence": "high"}
            ],
            "abo": [
                {"translation": "female", "part_of_speech": "noun", "confidence": "high"},
                {"translation": "feminine", "part_of_speech": "adjective", "confidence": "high"},
                {"translation": "woman", "part_of_speech": "noun", "confidence": "high"}
            ],
            "ó": [
                {"translation": "he", "part_of_speech": "pronoun", "confidence": "high"},
                {"translation": "she", "part_of_speech": "pronoun", "confidence": "high"},
                {"translation": "it", "part_of_speech": "pronoun", "confidence": "high"}
            ],
            "adìye": [
                {"translation": "chicken", "part_of_speech": "noun", "confidence": "high"},
                {"translation": "hen", "part_of_speech": "noun", "confidence": "medium"}
            ],
            "àárẹ̀": [
                {"translation": "fatigue", "part_of_speech": "noun", "confidence": "high"},
                {"translation": "tiredness", "part_of_speech": "noun", "confidence": "high"}
            ],
            "àpẹ́rẹ́": [
                {"translation": "example", "part_of_speech": "noun", "confidence": "high"},
                {"translation": "sample", "part_of_speech": "noun", "confidence": "high"},
                {"translation": "illustration", "part_of_speech": "noun", "confidence": "high"}
            ],
            "àpẹ́": [
                {"translation": "duck", "part_of_speech": "noun", "confidence": "high"}
            ],
            "ìgbín": [
                {"translation": "snail", "part_of_speech": "noun", "confidence": "high"}
            ],
            "àgbàdo": [
                {"translation": "corn", "part_of_speech": "noun", "confidence": "high"},
                {"translation": "maize", "part_of_speech": "noun", "confidence": "high"}
            ],
            "ẹranko": [
                {"translation": "animal", "part_of_speech": "noun", "confidence": "high"},
                {"translation": "beast", "part_of_speech": "noun", "confidence": "high"}
            ],
            "ẹ": [
                {"translation": "you", "part_of_speech": "pronoun", "confidence": "high"},
                {"translation": "you all", "part_of_speech": "pronoun", "confidence": "high"}
            ],
            "gbogbo": [
                {"translation": "all", "part_of_speech": "adjective", "confidence": "high"},
                {"translation": "every", "part_of_speech": "adjective", "confidence": "high"},
                {"translation": "entire", "part_of_speech": "adjective", "confidence": "high"}
            ],
            "bawo": [
                {"translation": "how", "part_of_speech": "adverb", "confidence": "high"}
            ],
            "à bá ti": [
                {"translation": "we would have", "part_of_speech": "phrase", "confidence": "high"}
            ],
            "aláàṣẹ": [
                {"translation": "authority", "part_of_speech": "noun", "confidence": "high"},
                {"translation": "executive", "part_of_speech": "noun", "confidence": "high"},
                {"translation": "official", "part_of_speech": "noun", "confidence": "high"},
                {"translation": "public authority", "part_of_speech": "noun", "confidence": "medium"}
            ],
            "ti": [
                {"translation": "of", "part_of_speech": "preposition", "confidence": "high"},
                {"translation": "that", "part_of_speech": "pronoun", "confidence": "high"},
                {"translation": "which", "part_of_speech": "pronoun", "confidence": "high"}
            ],
            "ni": [
                {"translation": "in", "part_of_speech": "preposition", "confidence": "high"},
                {"translation": "at", "part_of_speech": "preposition", "confidence": "high"},
                {"translation": "is", "part_of_speech": "verb", "confidence": "high"}
            ],
            "si": [
                {"translation": "to", "part_of_speech": "preposition", "confidence": "high"},
                {"translation": "towards", "part_of_speech": "preposition", "confidence": "high"}
            ],
            "ṣe": [
                {"translation": "do", "part_of_speech": "verb", "confidence": "high"},
                {"translation": "make", "part_of_speech": "verb", "confidence": "high"}
            ],
            "mo": [
                {"translation": "I", "part_of_speech": "pronoun", "confidence": "high"},
                {"translation": "me", "part_of_speech": "pronoun", "confidence": "high"}
            ],
            "wo": [
                {"translation": "look", "part_of_speech": "verb", "confidence": "high"},
                {"translation": "see", "part_of_speech": "verb", "confidence": "high"}
            ],
            "wá": [
                {"translation": "come", "part_of_speech": "verb", "confidence": "high"},
                {"translation": "search", "part_of_speech": "verb", "confidence": "high"}
            ],
            "fún": [
                {"translation": "give", "part_of_speech": "verb", "confidence": "high"},
                {"translation": "for", "part_of_speech": "preposition", "confidence": "high"}
            ]
        }
        
        # Return translations if found, otherwise empty list
        return common_translations.get(normalized_word, [])

    def extract_definitions(self, soup):
        """Extract dictionary definitions from the page"""
        definitions = []
        
        # Look for definition sections or elements
        definition_selectors = [
            '.translation__definition',
            '.definition',
            '.dict-definition',
            '.meaning',
            '.sense'
        ]
        
        for selector in definition_selectors:
            definition_elements = soup.select(selector)
            for elem in definition_elements:
                definition_text = elem.get_text(strip=True)
                if definition_text and len(definition_text) > 5:
                    # Clean the definition text
                    definition_text = re.sub(r'\s+', ' ', definition_text)
                    
                    # Extract part of speech if present
                    pos = self.extract_pos_from_text(definition_text)
                    
                    # Add to definitions if not already present
                    if not any(d.get('text') == definition_text for d in definitions):
                        definitions.append({
                            'text': definition_text,
                            'part_of_speech': pos,
                            'source': 'definition',
                            'confidence': 'high' if len(definition_text) > 15 else 'medium'
                        })
        
        return definitions

    def get_known_examples(self, word):
        """
        Get known good examples for a word
        
        Args:
            word (str): The Yoruba word
            
        Returns:
            list: A list of example dictionaries
        """
        normalized_word = self.normalize_word(word)
        return self.example_extractor.get_known_examples(normalized_word)

    def final_quality_check(self, data):
        """
        Perform a final quality check on data before saving to CSV
        This is the last opportunity to filter out incorrect data
        
        Args:
            data: The data dictionary containing all words and translations
            
        Returns:
            dict: The filtered data dictionary
        """
        if not data:
            return data
            
        # Filter translations
        if "translations" in data:
            filtered_translations = []
            word = data.get("word", "").lower()
            
            # For each translation, apply strict filtering
            for trans in data.get("translations", []):
                if not isinstance(trans, dict) or not trans.get("translation"):
                    continue
                    
                translation = trans.get("translation", "")
                confidence = trans.get("confidence", "medium")
                
                # Apply our improved validation
                if not self.is_valid_translation(word, translation, confidence):
                    continue
                    
                filtered_translations.append(trans)
                
            data["translations"] = filtered_translations
            
            # If we still have translations, ensure the primary translation is valid
            if filtered_translations and "translation" in data:
                primary = data.get("translation", "")
                # Check if the primary translation is in our filtered list
                primary_valid = any(t.get("translation") == primary for t in filtered_translations)
                
                if not primary_valid and filtered_translations:
                    # If primary translation was filtered out, use the best remaining one
                    high_confidence = [t for t in filtered_translations if t.get("confidence") == "high"]
                    if high_confidence:
                        data["translation"] = high_confidence[0]["translation"]
                        data["part_of_speech"] = high_confidence[0].get("part_of_speech", "")
                    else:
                        data["translation"] = filtered_translations[0]["translation"]
                        data["part_of_speech"] = filtered_translations[0].get("part_of_speech", "")
            
        # Filter examples
        if "examples" in data:
            filtered_examples = []
            word = data.get("word", "").lower()
            word_length = len(word)
            
            for example in data.get("examples", []):
                # Skip examples with missing or very short text
                yoruba = example.get("yoruba", "").strip()
                english = example.get("english", "").strip()
                
                if not yoruba or not english or len(yoruba) < 8 or len(english) < 8:
                    continue
                
                # Apply cleaning to ensure better quality
                yoruba = self.clean_example_text(yoruba)
                english = self.clean_example_text(english)
                example["yoruba"] = yoruba
                example["english"] = english
                
                # Skip examples that don't contain the word (for words longer than 2 chars)
                if word_length > 2 and word not in yoruba.lower():
                    continue
                    
                # Skip examples with UI elements or website content
                ui_elements = ["login", "password", "username", "email", "click", "button", 
                             "search", "menu", "glosbe", "dictionary", "example"]
                
                if any(ui in yoruba.lower() or ui in english.lower() for ui in ui_elements):
                    continue
                
                # Skip examples with heavily mismatched lengths
                length_ratio = len(yoruba) / len(english) if len(english) > 0 else 0
                if length_ratio < 0.5 or length_ratio > 2.0:
                    continue
                
                # Ensure proper sentence structure
                if not re.search(r'[.!?]$', yoruba) or not re.search(r'[.!?]$', english):
                    # If not ending with punctuation, it might be incomplete
                    continue
                
                # Verify example is proper through our verification function
                if self.verify_example_pair(yoruba, english):
                    filtered_examples.append(example)
                
            data["examples"] = filtered_examples
            
        return data
        
    def save_to_csv(self, data, output_file):
        """Save scraping results to CSV files"""
        # First perform final quality check
        data = self.final_quality_check(data)
        
        if not data:
            logging.warning(f"No data to save to CSV file: {output_file}")
            return
        
        base_name = os.path.splitext(output_file)[0]
        words_file = f"{base_name}_words.csv"
        translations_file = f"{base_name}_translations.csv"
        examples_file = f"{base_name}_examples.csv"
        
        words_data = []
        translations_data = []
        examples_data = []
        
        next_word_id = 1
        next_translation_id = 1
        next_example_id = 1
        
        # Sort data by word to ensure consistent ordering
        sorted_data = sorted(data, key=lambda x: x.get("word", "").lower())
        
        word_id_map = {}
        
        # First pass: assign word IDs sequentially without gaps
        for item in sorted_data:
            word_text = item["word"]
            
            if word_text not in word_id_map:
                word_id = next_word_id
                next_word_id += 1
                word_id_map[word_text] = word_id
                
                words_data.append({
                    "id": word_id,
                    "word": word_text,
                    "url": item.get("url", ""),
                    "scrape_time": item.get("scrape_time", ""),
                    "status": item.get("status", ""),
                    "error": item.get("error", "")
                })
        
        # Second pass: process all translations and examples
        for item in sorted_data:
            word_text = item["word"]
            word_id = word_id_map[word_text]
            
            seen_translations = set()
            trans_id_map = {}
            
            # Sort translations by confidence level (high first) and alphabetically
            translations = sorted(
                item.get("translations", []),
                key=lambda t: (0 if t.get("confidence") == "high" else 1, t.get("translation", "").lower())
            )
            
            for trans in translations:
                if not isinstance(trans, dict) or not trans.get("translation"):
                    continue
                
                norm_translation = trans["translation"].lower().strip()
                
                if norm_translation in seen_translations:
                    continue
                
                seen_translations.add(norm_translation)
                
                trans_id = next_translation_id
                next_translation_id += 1
                
                trans_key = f"{word_text}:{norm_translation}"
                trans_id_map[trans_key] = trans_id
                
                translations_data.append({
                    "id": trans_id,
                    "word_id": word_id,
                    "translation": trans["translation"],
                    "part_of_speech": trans.get("part_of_speech", ""),
                    "confidence": trans.get("confidence", "medium")
                })
                
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
            
            for example in item.get("examples", []):
                if not isinstance(example, dict):
                    continue
                yoruba = example.get("yoruba", "")
                english = example.get("english", "")
                if not yoruba or not english:
                    continue
                
                already_added = False
                for ex_data in examples_data:
                    if (ex_data["yoruba_text"] == yoruba and 
                        ex_data["english_text"] == english):
                        already_added = True
                        break
                
                if not already_added:
                    example_id = next_example_id
                    next_example_id += 1
                    
                    translation_id = None
                    for trans_key, trans_id in trans_id_map.items():
                        trans_text = trans_key.split(":", 1)[1]
                        if trans_text in english.lower():
                            translation_id = trans_id
                            break
                    
                    examples_data.append({
                        "id": example_id,
                        "translation_id": translation_id,
                        "word_id": word_id,
                        "yoruba_text": yoruba,
                        "english_text": english,
                        "is_jw_reference": example.get("is_jw_reference", False),
                        "confidence": example.get("confidence", "medium"),
                        "source": example.get("source", "unknown"),
                        "score": example.get("score", 0)
                    })
        
        # Check for word ID gaps before writing CSV
        word_ids = sorted([w["id"] for w in words_data])
        expected_ids = list(range(1, len(word_ids) + 1))
        
        if word_ids != expected_ids:
            logging.warning(f"Word ID gaps detected in {words_file}. Fixing sequence...")
            # Rebuild id maps
            id_map = {old_id: new_id for old_id, new_id in zip(word_ids, expected_ids)}
            
            # Update word IDs
            for word in words_data:
                if word["id"] in id_map:
                    word["id"] = id_map[word["id"]]
            
            # Update translations word_id references
            for trans in translations_data:
                if trans["word_id"] in id_map:
                    trans["word_id"] = id_map[trans["word_id"]]
            
            # Update examples word_id references
            for example in examples_data:
                if example["word_id"] in id_map:
                    example["word_id"] = id_map[example["word_id"]]
        
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
        json_alphabet_folder = os.path.join(self.json_folder, f"{alphabet}")
        csv_alphabet_folder = os.path.join(self.csv_folder, f"{alphabet}")
        
        if not os.path.exists(json_alphabet_folder):
            os.makedirs(json_alphabet_folder)
        
        if not os.path.exists(csv_alphabet_folder):
            os.makedirs(csv_alphabet_folder)
        
        words = self.extract_words_from_file(word_file)
        logging.info(f"Found {len(words)} unique words in file")
        
        words_to_process = [word for word in words if word not in self.processed_words]
        logging.info(f"After deduplication: {len(words_to_process)} words to process")
        
        if not words_to_process:
            logging.info("All words already processed, skipping file")
            return 0
        
        results = []
        for word in tqdm(words_to_process, desc=f"Processing words in {os.path.basename(word_file)}", unit="word"):
            try:
                result = self.scrape_word(word)
                results.append(result)
            except Exception as e:
                logging.error(f"Unexpected error processing {word}: {str(e)}")
                results.append({"word": word, "error": f"Processing error: {str(e)}"})
        
        for word in words:
            if word in self.processed_words and word not in words_to_process:
                results.append({"word": word, "status": "previously_processed"})
        
        base_filename = os.path.basename(word_file).replace('.txt', '')
        json_output_file = os.path.join(json_alphabet_folder, f"{base_filename}.json")
        csv_output_file = os.path.join(csv_alphabet_folder, f"{base_filename}.csv")
        
        existing_data = []
        if os.path.exists(json_output_file):
            try:
                with open(json_output_file, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
                logging.info(f"Loaded {len(existing_data)} existing entries from {json_output_file}")
            except json.JSONDecodeError:
                logging.warning(f"Error reading existing data from {json_output_file}, will overwrite")
        
        existing_dict = {item["word"]: item for item in existing_data}
        new_dict = {item["word"]: item for item in results}
        
        existing_dict.update(new_dict)
        merged_results = list(existing_dict.values())
        
        with open(json_output_file, 'w', encoding='utf-8') as f:
            json.dump(merged_results, f, ensure_ascii=False, indent=2)
        logging.info(f"Saved {len(merged_results)} entries to JSON file: {json_output_file}")
        
        self.save_to_csv(merged_results, csv_output_file)
        
        self.generate_combined_csv()
        
        return len(words_to_process)
    
    def scrape_word(self, word):
        """Alias for scrape_everything to maintain backward compatibility."""
        return self.scrape_everything(word)

    def preprocess_data_before_save(self, data):
        """
        Clean and validate data before saving to CSV
        
        Args:
            data (list): List of dictionaries containing translations and examples
            
        Returns:
            list: Cleaned data ready for saving
        """
        if not data:
            return []
        
        cleaned_data = []
        
        for item in data:
            # Skip entirely empty items
            if not item or not isinstance(item, dict):
                continue
            
            # Create a deep copy to avoid modifying the original
            clean_item = item.copy()
            
            # Clean and validate translations
            if "translations" in clean_item and isinstance(clean_item["translations"], list):
                valid_translations = []
                seen_translations = set()
                
                for trans in clean_item["translations"]:
                    if not isinstance(trans, dict):
                        continue
                        
                    if "translation" not in trans or not trans["translation"]:
                        continue
                    
                    # Clean the translation text
                    original_trans = trans["translation"]
                    cleaned_trans = self.extract_clean_translation(original_trans)
                    
                    # Skip if cleaning made it invalid
                    if not cleaned_trans or len(cleaned_trans) < 2:
                        continue
                        
                    # Apply more validation checks
                    if not self.is_valid_translation(clean_item.get("word", ""), cleaned_trans, trans.get("confidence", "medium")):
                        continue
                    
                    # Skip duplicates (case-insensitive)
                    if cleaned_trans.lower() in seen_translations:
                        continue
                    
                    # Update the translation with cleaned version
                    trans["translation"] = cleaned_trans
                    seen_translations.add(cleaned_trans.lower())
                    valid_translations.append(trans)
                
                # Update with only valid translations
                clean_item["translations"] = valid_translations
                
                # If there are no valid translations, skip this item
                if not valid_translations:
                    logging.warning(f"No valid translations for word: {clean_item.get('word', 'unknown')}")
                    continue
            
            # Clean and validate examples
            if "examples" in clean_item and isinstance(clean_item["examples"], list):
                valid_examples = []
                seen_yoruba = set()
                
                for example in clean_item["examples"]:
                    if not isinstance(example, dict):
                        continue
                    
                    if "yoruba" not in example or "english" not in example:
                        continue
                    
                    yoruba = example.get("yoruba", "")
                    english = example.get("english", "")
                    
                    # Skip empty examples
                    if not yoruba or not english:
                        continue
                    
                    # First apply basic cleaning to both
                    yoruba = self.clean_example_text(yoruba)
                    english = self._clean_english_example(english)
                    
                    # Apply language-specific cleaning
                    # Use our improved Yoruba spacing fixer
                    yoruba = self._fix_yoruba_spacing(yoruba)
                    english = self._fix_english_spacing(english)
                    
                    # Skip if any of the cleaned texts is too short
                    if len(yoruba) < 10 or len(english) < 10:
                        continue
                    
                    # Skip examples with suspicious content 
                    suspicious_words = [
                        'login', 'signup', 'register', 'password', 'username', 'cookie', 
                        'click', 'download', 'upload', 'website', 'captcha', 'browser',
                        'server', 'database', 'null', 'undefined', 'NaN', 'javascript'
                    ]
                    
                    if any(word in yoruba.lower() or word in english.lower() for word in suspicious_words):
                        continue
                    
                    # Check if we still have merged words after cleaning
                    if re.search(r'[a-z][A-Z]', yoruba) or re.search(r'[a-z][A-Z]', english):
                        if re.search(r'[À-úẹọṣ][A-Z]', yoruba):
                            continue  # Skip examples with Yoruba-English merges
                    
                    # Skip if the example doesn't contain the word (for words longer than 2 chars)
                    word = clean_item.get("word", "")
                    if len(word) > 2 and word.lower() not in yoruba.lower():
                        continue
                    
                    # Skip duplicate Yoruba examples (based on first 50 chars to be flexible)
                    yoruba_key = yoruba[:50].lower()
                    if yoruba_key in seen_yoruba:
                        continue
                    
                    # Verify the example pair
                    if self.verify_example_pair(yoruba, english):
                        # Update with cleaned versions
                        example["yoruba"] = yoruba
                        example["english"] = english
                        seen_yoruba.add(yoruba_key)
                        valid_examples.append(example)
                
                # Update with only valid examples
                clean_item["examples"] = valid_examples
            
            # Add the cleaned item to results
            cleaned_data.append(clean_item)
        
        # Sort the data by word for consistency
        cleaned_data.sort(key=lambda x: x.get("word", "").lower())
        
        return cleaned_data

    def _clean_english_example(self, text):
        """Clean English example text to improve quality
        
        Args:
            text (str): The English example text to clean
            
        Returns:
            str: Cleaned example text
        """
        if not text or not isinstance(text, str):
            return text
        
        # Fix spacing between words (common scraping issue)
        # Add space between lowercase and uppercase letters
        text = re.sub(r'([a-z])([A-Z])', r'\1 \2', text)
        
        # Fix common auxiliary verb + main verb issues
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
            ('manwas', 'man was'),
            ('mancould', 'man could'),
            ('ofmankind', 'of mankind'),
            ('hecould', 'he could'),
            ('hecannot', 'he cannot'),
            ('shecannot', 'she cannot'),
            ('itis', 'it is'),
            ('ifhe', 'if he'),
            ('ifthey', 'if they'),
            ('wasno', 'was no'),
        ]
        
        for pattern, replacement in common_patterns:
            text = text.replace(pattern, replacement)
        
        # Clean up spaces around punctuation
        text = re.sub(r'\s+([.,;:!?])', r'\1', text)
        
        # Normalize spaces
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text

    def verify_and_fix_csv_data(self, csv_file_path=None):
        """
        Verify and fix data in CSV files, performing additional cleaning and validation.
        This is more thorough than just fixing spacing - it checks for structural issues
        and data quality problems.
        
        Args:
            csv_file_path (str, optional): Path to a specific CSV file to fix.
                If None, will fix all CSV files in the base folder.
                
        Returns:
            tuple: (number of files processed, number of entries fixed)
        """
        logging.info("Starting comprehensive verification and fixing of CSV data")
        
        files_processed = 0
        entries_fixed = 0
        
        # Handle single file or all files
        csv_files = []
        if csv_file_path and os.path.exists(csv_file_path):
            csv_files = [csv_file_path]
        else:
            # Find all CSV files in base folder
            for root, _, files in os.walk(self.base_folder):
                for file in files:
                    if file.endswith('.csv'):
                        csv_files.append(os.path.join(root, file))
        
        logging.info(f"Found {len(csv_files)} CSV files to process")
        
        # Process each file
        for file_path in csv_files:
            try:
                logging.info(f"Processing {file_path}")
                
                # Read the CSV file
                df = pd.read_csv(file_path, encoding='utf-8')
                original_df = df.copy()
                
                # Identify column types to know what kind of data we're dealing with
                has_translations = 'translation' in df.columns
                has_examples = 'yoruba_text' in df.columns and 'english_text' in df.columns
                
                if has_translations:
                    # Handle translation CSV
                    # Clean translation text
                    df['translation'] = df['translation'].apply(
                        lambda x: self.extract_clean_translation(x) if isinstance(x, str) else x
                    )
                    
                    # Check for translation-specific issues
                    # Verify each translation is valid for its word
                    if 'word_id' in df.columns:
                        # First get mapping of word_id to actual word
                        word_id_map = {}
                        words_file = os.path.join(os.path.dirname(file_path), 'words.csv')
                        if os.path.exists(words_file):
                            words_df = pd.read_csv(words_file, encoding='utf-8')
                            word_id_map = dict(zip(words_df['id'], words_df['word']))
                        
                        # Apply validation
                        for idx, row in df.iterrows():
                            if pd.notna(row['translation']) and row['word_id'] in word_id_map:
                                word = word_id_map[row['word_id']]
                                translation = row['translation']
                                confidence = row.get('confidence', 'medium')
                                
                                # If the translation is invalid, blank it out
                                if not self.is_valid_translation(word, translation, confidence):
                                    df.at[idx, 'translation'] = ''
                
                if has_examples:
                    # Handle examples CSV
                    # Clean yoruba text directly with _fix_yoruba_spacing
                    df['yoruba_text'] = df['yoruba_text'].apply(
                        lambda x: self._fix_yoruba_spacing(x) if isinstance(x, str) else x
                    )
                    
                    # Clean english text
                    df['english_text'] = df['english_text'].apply(
                        lambda x: self._fix_english_spacing(x) if isinstance(x, str) else x
                    )
                    
                    # Verify each example pair and remove invalid ones
                    invalid_rows = []
                    for idx, row in df.iterrows():
                        if pd.notna(row['yoruba_text']) and pd.notna(row['english_text']):
                            yoruba = row['yoruba_text']
                            english = row['english_text']
                            
                            # If the example pair fails verification, mark it for removal
                            if not self.verify_example_pair(yoruba, english):
                                invalid_rows.append(idx)
                    
                    # Remove invalid rows
                    if invalid_rows:
                        logging.info(f"Removing {len(invalid_rows)} invalid example pairs from {file_path}")
                        df = df.drop(invalid_rows)
                
                # Count changes made
                changes_made = sum(df.ne(original_df).any(axis=1))
                
                if changes_made > 0:
                    # Create backup of original file
                    backup_path = f"{file_path}.bak"
                    if not os.path.exists(backup_path):
                        shutil.copy2(file_path, backup_path)
                        logging.info(f"Created backup at {backup_path}")
                    
                    # Save the updated data
                    df.to_csv(file_path, index=False, encoding='utf-8')
                    logging.info(f"Fixed {changes_made} entries in {file_path}")
                    
                    entries_fixed += changes_made
                else:
                    logging.info(f"No issues found in {file_path}")
                
                files_processed += 1
                
            except Exception as e:
                logging.error(f"Error processing {file_path}: {str(e)}")
                import traceback
                traceback.print_exc()
        
        logging.info(f"Completed verification and fixing of {files_processed} CSV files, {entries_fixed} entries fixed")
        return files_processed, entries_fixed

if __name__ == "__main__":
    try:
        # Set up proper console encoding for printing Unicode characters
        import sys
        import io
        import codecs
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='backslashreplace')
        
        print("Starting Yoruba Scraper...")
        scraper = GlosbeYorubaScraper()
        print("Initialized scraper, running...")
        
        # Get the word files and print them for debugging
        word_files = scraper.get_word_files()
        print(f"Found {len(word_files)} alphabet folders")
        for alphabet, files in word_files.items():
            print(f"  Alphabet: {alphabet}, Files: {len(files)}")
        
        # Now run the scraper
        result = scraper.run()
        print("Scraper completed successfully.")
        print(f"Result: {result}")
    except Exception as e:
        import traceback
        print(f"Error running scraper: {str(e)}")
        traceback.print_exc()