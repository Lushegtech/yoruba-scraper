#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Enhanced Translation Validator for Yoruba-English Dictionary

This module provides specialized validation for Yoruba-English translations.
It implements rigorous checks to detect incorrect or suspicious translations,
employing linguistic rules and reference data to ensure high accuracy.
"""

import re
import json
import logging
import os
from pathlib import Path
import csv
from difflib import SequenceMatcher

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("translation_validation.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("translation_validator")

class EnhancedTranslationValidator:
    """
    A specialized validator for Yoruba-English translations that detects
    incorrect or suspicious translations through multiple validation strategies.
    """
    
    def __init__(self, reference_file=None, known_words_dir="./yoruba_words"):
        """
        Initialize the validator with reference data.
        
        Args:
            reference_file: Optional CSV/JSON file with verified translations
            known_words_dir: Directory containing known Yoruba words
        """
        self.reference_file = reference_file
        self.known_words_dir = known_words_dir
        
        # Load reference translations
        self.reference_translations = {}
        if reference_file:
            self.reference_translations = self._load_reference_translations()
        
        # Load known Yoruba words
        self.known_words = self._load_known_words()
        
        # Dictionary of words with multiple meanings that need context
        self.ambiguous_words = {
            "ọkọ": {
                "meanings": ["husband", "vehicle", "car", "boat"],
                "context_clues": {
                    "husband": ["wife", "marriage", "wedding", "spouse"],
                    "vehicle": ["drive", "road", "transport", "passenger"],
                    "boat": ["water", "river", "sail", "fishing"]
                }
            },
            "ọmọ": {
                "meanings": ["child", "offspring", "young person"],
                "context_clues": {
                    "child": ["parent", "mother", "father", "baby", "infant"],
                    "offspring": ["birth", "descendant", "heir"],
                    "young person": ["youth", "young", "teenage"]
                }
            },
            "ilé": {
                "meanings": ["house", "home", "building", "residence"],
                "context_clues": {
                    "house": ["roof", "door", "window", "building"],
                    "home": ["family", "live", "reside", "dwelling"]
                }
            },
            "iṣẹ́": {
                "meanings": ["work", "job", "labor", "occupation", "task"],
                "context_clues": {
                    "work": ["office", "employment", "labor"],
                    "task": ["assignment", "duty", "responsibility"]
                }
            },
            "ẹnu": {
                "meanings": ["mouth", "opening", "entrance"],
                "context_clues": {
                    "mouth": ["tongue", "lips", "teeth", "speaking"],
                    "opening": ["door", "entry", "gateway", "access"]
                }
            }
        }
        
        # Common English translations with Yoruba equivalents
        # Used as a reference to check against
        self.common_translations = {
            "good": ["dára", "rere", "tó dára"],
            "bad": ["búburú", "burú", "burúkú"],
            "water": ["omi"],
            "food": ["oúnjẹ", "jíjẹ"],
            "money": ["owó", "owo"],
            "house": ["ilé", "ile"],
            "person": ["ènìyàn", "eniyan"],
            "time": ["àkókò", "akoko", "ìgbà", "igba"],
            "day": ["ọjọ́", "ojo"],
            "night": ["òru", "oru", "alẹ́", "ale"],
            "big": ["nlá", "nla", "tóbi", "tobi"],
            "small": ["kékeré", "kekere", "díẹ̀", "die"],
            "many": ["púpọ̀", "pupo", "ọ̀pọ̀", "opo"],
            "few": ["díẹ̀", "die", "mélòó", "meloo"],
            "all": ["gbogbo"],
            "go": ["lọ", "lo"],
            "come": ["wá", "wa"],
            "see": ["rí", "ri"],
            "hear": ["gbọ́", "gbo"],
            "speak": ["sọ̀rọ̀", "soro", "fọ̀", "fo"],
            "give": ["fún", "fun"],
            "take": ["mú", "mu", "gbà", "gba"],
            "eat": ["jẹ", "je", "jẹun", "jeun"],
            "drink": ["mu"],
            "sleep": ["sùn", "sun"],
            "live": ["gbé", "gbe"],
            "die": ["kú", "ku"],
            "know": ["mọ̀", "mo"],
            "think": ["rò", "ro"],
            "say": ["sọ", "so", "wí", "wi"],
            "do": ["ṣe", "se"],
            "make": ["ṣe", "se", "dá", "da"],
            "yesterday": ["àná", "ana"],
            "today": ["òní", "oni"],
            "tomorrow": ["ọ̀la", "ola"],
            "mother": ["ìyá", "iya"],
            "father": ["bàbá", "baba"],
            "child": ["ọmọ", "omo"],
            "man": ["ọkùnrin", "okunrin", "ọkọ́", "oko"],
            "woman": ["obìnrin", "obinrin"],
            "people": ["àwọn ènìyàn", "awon eniyan"],
        }
        
        # Initialize sets to track suspicious patterns
        self.suspicious_patterns = [
            # HTML or code fragments
            r'</?[a-z]+[^>]*>',
            # URLs
            r'https?://\S+',
            # Email-like patterns
            r'\S+@\S+\.\S+',
            # Unusual special characters
            r'[^\w\s.,;:!?()\'"-]',
            # Dictionary metadata
            r'\b(pl\.|n\.|v\.|adj\.|adv\.)\b',
            # Long numbers
            r'\d{4,}',
            # Mixed language text (English text with Yoruba characters)
            r'[a-zA-Z]+[àáèéìíòóùúẹọṣÀÁÈÉÌÍÒÓÙÚẸỌṢ]',
            # Suspiciously long words (likely joined)
            r'\b\w{20,}\b',
        ]
    
    def _load_reference_translations(self):
        """Load reference translations from file."""
        translations = {}
        if not os.path.exists(self.reference_file):
            logger.warning(f"Reference file {self.reference_file} not found.")
            return translations
            
        try:
            ext = os.path.splitext(self.reference_file)[1].lower()
            if ext == '.json':
                with open(self.reference_file, 'r', encoding='utf-8') as f:
                    translations = json.load(f)
            elif ext in ['.csv', '.tsv']:
                delimiter = ',' if ext == '.csv' else '\t'
                with open(self.reference_file, 'r', encoding='utf-8') as f:
                    reader = csv.reader(f, delimiter=delimiter)
                    header = next(reader)  # Skip header
                    
                    # Find the column indices
                    yoruba_idx = header.index('yoruba_word') if 'yoruba_word' in header else 0
                    english_idx = header.index('english_translation') if 'english_translation' in header else 1
                    
                    for row in reader:
                        if len(row) > max(yoruba_idx, english_idx):
                            yoruba_word = row[yoruba_idx].strip().lower()
                            english_trans = row[english_idx].strip().lower()
                            
                            if yoruba_word not in translations:
                                translations[yoruba_word] = []
                            
                            if english_trans and english_trans not in translations[yoruba_word]:
                                translations[yoruba_word].append(english_trans)
            else:
                logger.error(f"Unsupported file format: {ext}")
        except Exception as e:
            logger.error(f"Error loading reference translations: {str(e)}")
            
        logger.info(f"Loaded {len(translations)} reference translations")
        return translations
    
    def _load_known_words(self):
        """Load known Yoruba words from directory."""
        known_words = set()
        if not os.path.isdir(self.known_words_dir):
            logger.warning(f"Known words directory {self.known_words_dir} not found.")
            return known_words
            
        # Walk through the directory structure
        for root, dirs, files in os.walk(self.known_words_dir):
            for file in files:
                if file.endswith('.txt'):
                    try:
                        with open(os.path.join(root, file), 'r', encoding='utf-8') as f:
                            for line in f:
                                word = line.strip()
                                if word:
                                    known_words.add(word.lower())
                    except Exception as e:
                        logger.error(f"Error loading words from {file}: {str(e)}")
        
        logger.info(f"Loaded {len(known_words)} known Yoruba words")
        return known_words
    
    def verify_translation(self, yoruba_word, english_translation, context=None):
        """
        Verify if a translation is correct and accurate.
        
        Args:
            yoruba_word: The Yoruba word
            english_translation: The English translation to verify
            context: Optional context such as example sentences
            
        Returns:
            tuple: (is_valid, confidence, reason)
        """
        if not yoruba_word or not english_translation:
            return False, 0.0, "Missing word or translation"
            
        yoruba_word = yoruba_word.strip().lower()
        english_translation = english_translation.strip().lower()
        
        # Check if Yoruba word is known
        if yoruba_word not in self.known_words:
            logger.warning(f"Unknown Yoruba word: {yoruba_word}")
            # Not automatically invalid, but lower confidence
            confidence_modifier = 0.8
        else:
            confidence_modifier = 1.0
            
        # Check against reference translations if available
        if yoruba_word in self.reference_translations:
            reference_trans = [t.lower() for t in self.reference_translations[yoruba_word]]
            
            if english_translation in reference_trans:
                return True, 1.0 * confidence_modifier, "Exact match with reference translation"
                
            # Check for similarity with reference translations
            for ref_trans in reference_trans:
                similarity = self._similarity_score(english_translation, ref_trans)
                if similarity >= 0.8:
                    return True, similarity * confidence_modifier, f"Similar to reference translation ({similarity:.2f})"
        
        # Check for suspicious patterns in translation
        for pattern in self.suspicious_patterns:
            if re.search(pattern, english_translation):
                return False, 0.2 * confidence_modifier, f"Contains suspicious pattern: {pattern}"
        
        # Check for ambiguous words that need context
        if yoruba_word in self.ambiguous_words:
            meanings = self.ambiguous_words[yoruba_word]["meanings"]
            
            # If translation matches one of the possible meanings
            if english_translation in meanings:
                # If context is provided, check if it matches the expected context
                if context and any(clue in context.lower() for clue in 
                                  self.ambiguous_words[yoruba_word]["context_clues"].get(english_translation, [])):
                    return True, 0.9 * confidence_modifier, "Translation matches context"
                else:
                    # Without context or matching context clues, we can't be fully confident
                    return True, 0.7 * confidence_modifier, "Translation is one of multiple possible meanings"
            else:
                # Translation doesn't match any known meaning for this ambiguous word
                return False, 0.3 * confidence_modifier, f"Translation doesn't match any known meaning: {meanings}"
        
        # Check against common translations
        for eng, yoruba_list in self.common_translations.items():
            if english_translation == eng and yoruba_word not in yoruba_list:
                return False, 0.4 * confidence_modifier, f"Common English word doesn't match expected Yoruba words: {yoruba_list}"
            
            if yoruba_word in yoruba_list and english_translation != eng:
                # The translation might still be valid, but we should flag it for review
                return True, 0.5 * confidence_modifier, f"Translation differs from common reference: {eng}"
        
        # Length heuristic - if the translation is much longer or shorter than expected
        if len(english_translation) > 50:
            return False, 0.3 * confidence_modifier, "Translation suspiciously long"
            
        if len(english_translation) < 2:
            return False, 0.2 * confidence_modifier, "Translation suspiciously short"
            
        # Default to medium confidence if no other rules matched
        return True, 0.6 * confidence_modifier, "Passed basic validation checks"
    
    def verify_example_pair(self, yoruba_example, english_example):
        """
        Verify if an example sentence pair is valid and accurate.
        
        Args:
            yoruba_example: The Yoruba example sentence
            english_example: The English translation of the example
            
        Returns:
            tuple: (is_valid, confidence, reason)
        """
        if not yoruba_example or not english_example:
            return False, 0.0, "Missing example or translation"
            
        yoruba_example = yoruba_example.strip()
        english_example = english_example.strip()
        
        # Check for Yoruba orthographic markers (diacritics)
        if not re.search(r'[àáèéìíòóùúẹọṣ]', yoruba_example):
            return False, 0.3, "Yoruba example lacks expected diacritics"
            
        # Check for suspicious patterns
        for pattern in self.suspicious_patterns:
            if re.search(pattern, yoruba_example) or re.search(pattern, english_example):
                return False, 0.2, f"Contains suspicious pattern: {pattern}"
                
        # Check sentence structure 
        yoruba_words = yoruba_example.split()
        english_words = english_example.split()
        
        # Check reasonable length ratio (English usually longer than Yoruba)
        ratio = len(english_words) / len(yoruba_words) if len(yoruba_words) > 0 else 0
        if ratio < 0.5 or ratio > 3.0:
            return False, 0.4, f"Suspicious length ratio between Yoruba and English: {ratio:.2f}"
            
        # Check for overly short examples
        if len(yoruba_words) < 3 or len(english_words) < 3:
            return False, 0.5, "Example too short for reliable validation"
            
        # Extract words from Yoruba example and check if they're known
        valid_word_count = sum(1 for word in yoruba_words if self._clean_word(word) in self.known_words)
        valid_word_ratio = valid_word_count / len(yoruba_words) if len(yoruba_words) > 0 else 0
        
        if valid_word_ratio < 0.5 and len(yoruba_words) > 5:
            return False, 0.3, f"Only {valid_word_ratio:.2f} of Yoruba words recognized"
            
        # Default to medium-high confidence for examples that pass all checks
        return True, 0.8, "Example passed validation checks"
    
    def _clean_word(self, word):
        """Clean a word by removing punctuation and converting to lowercase."""
        return re.sub(r'[^\w\sàáèéìíòóùúẹọṣÀÁÈÉÌÍÒÓÙÚẸỌṢ]', '', word).lower()
    
    def _similarity_score(self, str1, str2):
        """Calculate string similarity score."""
        return SequenceMatcher(None, str1, str2).ratio()
    
    def identify_suspicious_translations(self, translations_data, output_file=None):
        """
        Process a collection of translations and identify suspicious ones.
        
        Args:
            translations_data: Dict mapping Yoruba words to English translations
            output_file: Optional file to save suspicious translations
            
        Returns:
            list: Suspicious translations with reasons
        """
        suspicious = []
        
        for yoruba_word, translation_info in translations_data.items():
            # Handle different possible data structures
            if isinstance(translation_info, list):
                translations = translation_info
                context = None
            elif isinstance(translation_info, dict):
                translations = translation_info.get("translations", [])
                if isinstance(translations, str):
                    translations = [translations]
                context = translation_info.get("example", "")
            else:
                translations = [translation_info]
                context = None
                
            for translation in translations:
                is_valid, confidence, reason = self.verify_translation(
                    yoruba_word, translation, context
                )
                
                if not is_valid or confidence < 0.7:
                    suspicious.append({
                        "yoruba_word": yoruba_word,
                        "translation": translation,
                        "context": context,
                        "valid": is_valid,
                        "confidence": confidence,
                        "reason": reason
                    })
        
        # Save suspicious translations if output file provided
        if output_file and suspicious:
            try:
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(suspicious, f, indent=2, ensure_ascii=False)
                logger.info(f"Saved {len(suspicious)} suspicious translations to {output_file}")
            except Exception as e:
                logger.error(f"Error saving suspicious translations: {str(e)}")
                
        return suspicious
    
    def generate_validation_report(self, data, output_file=None):
        """
        Generate a detailed validation report for a dataset.
        
        Args:
            data: Dictionary of Yoruba words and their translations
            output_file: Optional file to save the report
            
        Returns:
            dict: Report statistics
        """
        report = {
            "total_entries": 0,
            "valid_entries": 0,
            "suspicious_entries": 0,
            "confidence_levels": {
                "high": 0,    # 0.8-1.0
                "medium": 0,  # 0.5-0.79
                "low": 0      # <0.5
            },
            "common_issues": {},
            "suspicious_entries_list": []
        }
        
        for yoruba_word, translations in data.items():
            if isinstance(translations, str):
                translations = [translations]
            elif isinstance(translations, dict):
                translations = translations.get("translations", [])
                if isinstance(translations, str):
                    translations = [translations]
                    
            for translation in translations:
                report["total_entries"] += 1
                
                is_valid, confidence, reason = self.verify_translation(yoruba_word, translation)
                
                if is_valid:
                    report["valid_entries"] += 1
                else:
                    report["suspicious_entries"] += 1
                    report["suspicious_entries_list"].append({
                        "yoruba_word": yoruba_word,
                        "translation": translation,
                        "confidence": confidence,
                        "reason": reason
                    })
                
                # Track confidence levels
                if confidence >= 0.8:
                    report["confidence_levels"]["high"] += 1
                elif confidence >= 0.5:
                    report["confidence_levels"]["medium"] += 1
                else:
                    report["confidence_levels"]["low"] += 1
                
                # Track common issues
                if not is_valid or confidence < 0.7:
                    if reason not in report["common_issues"]:
                        report["common_issues"][reason] = 0
                    report["common_issues"][reason] += 1
        
        # Sort issues by frequency
        report["common_issues"] = dict(
            sorted(
                report["common_issues"].items(), 
                key=lambda x: x[1], 
                reverse=True
            )
        )
        
        # Calculate percentages
        if report["total_entries"] > 0:
            report["valid_percentage"] = round((report["valid_entries"] / report["total_entries"]) * 100, 2)
            report["suspicious_percentage"] = round((report["suspicious_entries"] / report["total_entries"]) * 100, 2)
            
            report["confidence_levels"]["high_percent"] = round(
                (report["confidence_levels"]["high"] / report["total_entries"]) * 100, 2
            )
            report["confidence_levels"]["medium_percent"] = round(
                (report["confidence_levels"]["medium"] / report["total_entries"]) * 100, 2
            )
            report["confidence_levels"]["low_percent"] = round(
                (report["confidence_levels"]["low"] / report["total_entries"]) * 100, 2
            )
        
        # Save report if output file provided
        if output_file:
            try:
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(report, f, indent=2, ensure_ascii=False)
                logger.info(f"Saved validation report to {output_file}")
            except Exception as e:
                logger.error(f"Error saving validation report: {str(e)}")
                
        return report

# Command-line interface for translation validation
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Validate Yoruba-English translations")
    parser.add_argument("--check", nargs=2, metavar=("YORUBA", "ENGLISH"), 
                        help="Check a single translation pair")
    parser.add_argument("--check-example", nargs=2, metavar=("YORUBA", "ENGLISH"), 
                        help="Check an example sentence pair")
    parser.add_argument("--file", help="Process translations from a JSON/CSV file")
    parser.add_argument("--output", help="Output file for reports or suspicious translations")
    parser.add_argument("--reference", help="Reference file with known correct translations")
    parser.add_argument("--words-dir", default="./yoruba_words", 
                        help="Directory containing known Yoruba words")
    
    args = parser.parse_args()
    
    # Initialize the validator
    validator = EnhancedTranslationValidator(
        reference_file=args.reference,
        known_words_dir=args.words_dir
    )
    
    if args.check:
        yoruba, english = args.check
        is_valid, confidence, reason = validator.verify_translation(yoruba, english)
        print(f"Translation: '{yoruba}' → '{english}'")
        print(f"Valid: {is_valid}, Confidence: {confidence:.2f}")
        print(f"Reason: {reason}")
    
    elif args.check_example:
        yoruba, english = args.check_example
        is_valid, confidence, reason = validator.verify_example_pair(yoruba, english)
        print(f"Example pair validation:")
        print(f"Yoruba: {yoruba}")
        print(f"English: {english}")
        print(f"Valid: {is_valid}, Confidence: {confidence:.2f}")
        print(f"Reason: {reason}")
    
    elif args.file:
        if not os.path.exists(args.file):
            print(f"Error: File {args.file} not found")
            sys.exit(1)
            
        try:
            with open(args.file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            print(f"Processing {len(data)} entries from {args.file}")
            
            # Generate validation report
            report = validator.generate_validation_report(data, args.output)
            
            # Print summary
            print("\nValidation Report Summary:")
            print(f"Total entries: {report['total_entries']}")
            print(f"Valid entries: {report['valid_entries']} ({report.get('valid_percentage', 0)}%)")
            print(f"Suspicious entries: {report['suspicious_entries']} ({report.get('suspicious_percentage', 0)}%)")
            
            print("\nConfidence levels:")
            print(f"High confidence: {report['confidence_levels']['high']} ({report['confidence_levels'].get('high_percent', 0)}%)")
            print(f"Medium confidence: {report['confidence_levels']['medium']} ({report['confidence_levels'].get('medium_percent', 0)}%)")
            print(f"Low confidence: {report['confidence_levels']['low']} ({report['confidence_levels'].get('low_percent', 0)}%)")
            
            print("\nTop 5 issues:")
            for i, (issue, count) in enumerate(list(report["common_issues"].items())[:5]):
                print(f"{i+1}. {issue}: {count} occurrences")
                
        except Exception as e:
            print(f"Error processing file: {str(e)}")
            import traceback
            traceback.print_exc()
    
    else:
        parser.print_help() 