#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Yoruba Language Validator

This module provides specialized validation functions for ensuring the accuracy 
and quality of Yoruba words, translations, and example sentences. It implements
rigorous checks for orthographic correctness, proper diacritics usage, and 
authentic translation validation.
"""

import re
import logging
import os
import json
from pathlib import Path
import unicodedata

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("yoruba_validation.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("yoruba_validator")

class YorubaLanguageValidator:
    """
    A specialized validator for Yoruba language data that ensures linguistic accuracy.
    Validates proper diacritics, orthographic rules, and authentic translations.
    """
    
    def __init__(self, reference_dir="./yoruba_words", known_words_file=None):
        """
        Initialize the validator with reference data.
        
        Args:
            reference_dir: Directory containing verified Yoruba word lists
            known_words_file: Optional file with verified word-translation pairs
        """
        self.reference_dir = reference_dir
        self.known_words_file = known_words_file
        
        # Load reference data
        self.reference_words = self._load_reference_words()
        self.known_translations = self._load_known_translations()
        
        # Common Yoruba diacritics and characters
        self.yoruba_diacritics = set('àáèéìíòóùúẹọṣ̀́ÀÁÈÉÌÍÒÓÙÚẸỌṢ')
        
        # Patterns for checking common orthographic issues
        self.invalid_patterns = [
            # Spaces between syllables that should be joined
            r'g\s+b[aeiouàáèéìíòóùúẹọṣ]',  # g ba should be gba
            # Words that are commonly misspelled
            r'\bágb[a|à]d[a|à]\b',  # should be àgbàdá
            r'\bn[i|í]t[o|ó]r[i|í]\b',  # should be nítòrí
        ]
        
        # Common translation errors to check for
        self.suspect_translations = {
            "ọkọ": ["husband", "car", "boat", "vehicle"],  # Context-dependent, needs careful checking
            "ìyá": ["mother", "suffering"],  # Can mean both but context matters
            "oko": ["farm", "hoe"],  # Often confused with ọkọ
            "ọmọ": ["child", "offspring"],
            "ilé": ["house", "home"],
        }
        
        # Load specific word patterns that need special attention
        self.special_attention_patterns = self._load_special_patterns()
    
    def _load_reference_words(self):
        """Load verified Yoruba words from reference directory."""
        reference_words = set()
        if not os.path.isdir(self.reference_dir):
            logger.warning(f"Reference directory {self.reference_dir} not found.")
            return reference_words
            
        for letter_dir in os.listdir(self.reference_dir):
            letter_path = os.path.join(self.reference_dir, letter_dir)
            if os.path.isdir(letter_path):
                for filename in os.listdir(letter_path):
                    if filename.endswith('.txt'):
                        file_path = os.path.join(letter_path, filename)
                        try:
                            with open(file_path, 'r', encoding='utf-8') as f:
                                for line in f:
                                    word = line.strip()
                                    if word:
                                        reference_words.add(word.lower())
                        except Exception as e:
                            logger.error(f"Error loading reference file {file_path}: {str(e)}")
        
        logger.info(f"Loaded {len(reference_words)} reference words")
        return reference_words
    
    def _load_known_translations(self):
        """Load verified word-translation pairs."""
        known_translations = {}
        if not self.known_words_file or not os.path.isfile(self.known_words_file):
            logger.info("No known translations file provided or file not found.")
            return known_translations
            
        try:
            with open(self.known_words_file, 'r', encoding='utf-8') as f:
                known_translations = json.load(f)
            logger.info(f"Loaded {len(known_translations)} known translations")
        except Exception as e:
            logger.error(f"Error loading known translations: {str(e)}")
            
        return known_translations
    
    def _load_special_patterns(self):
        """Load patterns that need special attention."""
        # These are patterns that require special handling or validation
        return {
            # Pattern: (regex, correct form, explanation)
            "tone_marks_on_consonants": (
                r'[bcdfghjklmnpqrstvwxz][̀́]', 
                None,
                "Tone marks should only appear on vowels in Yoruba"
            ),
            "incorrect_s_with_dot": (
                r's\.', 
                "ṣ",
                "The 's' with dot below should be 'ṣ' not 's.'"
            ),
            "incorrect_e_with_dot": (
                r'e\.', 
                "ẹ",
                "The 'e' with dot below should be 'ẹ' not 'e.'"
            ),
            "incorrect_o_with_dot": (
                r'o\.', 
                "ọ",
                "The 'o' with dot below should be 'ọ' not 'o.'"
            ),
        }
    
    def is_valid_yoruba_word(self, word):
        """
        Validate if a word follows Yoruba orthographic rules.
        
        Args:
            word: The Yoruba word to validate
            
        Returns:
            tuple: (is_valid, reason)
        """
        if not word or not isinstance(word, str):
            return False, "Word is empty or not a string"
            
        # Clean the word
        word = word.strip().lower()
        
        # Check if word exists in our reference list
        if word in self.reference_words:
            return True, "Word found in reference list"
            
        # Check if the word has any Yoruba diacritics
        has_yoruba_diacritics = any(char in self.yoruba_diacritics for char in word)
        
        # If word has no Yoruba diacritics, it might not be a Yoruba word
        if not has_yoruba_diacritics and len(word) > 3:
            # Some common Yoruba words don't have diacritics, so check length
            # This is a heuristic - longer words without diacritics are suspicious
            return False, "Word lacks expected Yoruba diacritics"
            
        # Check for invalid orthographic patterns
        for pattern in self.invalid_patterns:
            if re.search(pattern, word):
                return False, f"Word contains invalid pattern: {pattern}"
                
        # Check special patterns that need attention
        for pattern_name, (regex, correct, explanation) in self.special_attention_patterns.items():
            if re.search(regex, word):
                return False, explanation
                
        # Other orthographic rules
        
        # Check for incorrect syllable structure
        # Yoruba syllables are typically CV (consonant-vowel) or V (vowel)
        syllables = self._split_into_syllables(word)
        for syllable in syllables:
            if not self._is_valid_syllable(syllable):
                return False, f"Invalid syllable structure: {syllable}"
                
        return True, "Passed all validation checks"
    
    def _split_into_syllables(self, word):
        """Split a Yoruba word into syllables based on common patterns."""
        # This is a simplification - a proper implementation would be more complex
        # and would handle all Yoruba syllable patterns
        
        # Handle 'gb' as a single consonant
        word = word.replace('gb', 'G')
        
        syllables = []
        i = 0
        while i < len(word):
            # Find vowel (including those with diacritics)
            vowel_match = re.search(r'[aeiouàáèéìíòóùúẹọ]', word[i:])
            if not vowel_match:
                # No more vowels, add the rest as a syllable
                syllables.append(word[i:])
                break
                
            vowel_pos = vowel_match.start() + i
            
            # If the vowel is the first character or there's only one consonant before it
            if vowel_pos == i or vowel_pos == i + 1:
                syllables.append(word[i:vowel_pos+1])
                i = vowel_pos + 1
            else:
                # Multiple consonants - this is unusual in Yoruba
                # Take the last consonant with the vowel
                syllables.append(word[vowel_pos-1:vowel_pos+1])
                i = vowel_pos + 1
                
        # Restore 'gb'
        return [s.replace('G', 'gb') for s in syllables]
    
    def _is_valid_syllable(self, syllable):
        """Check if a syllable follows Yoruba patterns."""
        # Syllables in Yoruba are typically V or CV
        # (there are exceptions like 'gb' which would be treated as a single consonant)
        
        if len(syllable) == 1:
            # Single vowel syllable
            return syllable in 'aeiouàáèéìíòóùúẹọ'
            
        if len(syllable) == 2:
            # Consonant-Vowel syllable
            return (
                syllable[0] in 'bcdfghjklmnpqrstvwxzGBCDFGHJKLMNPQRSTVWXZ' and 
                syllable[1] in 'aeiouàáèéìíòóùúẹọAEIOUÀÁÈÉÌÍÒÓÙÚẸỌ'
            )
            
        # Check for syllables with diacritics which might appear as longer strings
        # due to Unicode representation
        normalized = unicodedata.normalize('NFD', syllable)
        if len(normalized) > len(syllable) and len(normalized) <= 3:
            # The syllable has combining diacritics
            base_char = normalized[0]
            if base_char in 'aeiouAEIOU':
                return True
            if len(normalized) >= 2:
                if base_char in 'bcdfghjklmnpqrstvwxzGBCDFGHJKLMNPQRSTVWXZ' and normalized[1] in 'aeiouAEIOU':
                    return True
                    
        # Special case for 'gb' as a single consonant
        if syllable.startswith('gb') and len(syllable) == 3 and syllable[2] in 'aeiouàáèéìíòóùúẹọ':
            return True
            
        return False
    
    def validate_translation(self, yoruba_word, english_translation):
        """
        Validate if a translation is likely correct.
        
        Args:
            yoruba_word: The Yoruba word
            english_translation: The English translation to validate
            
        Returns:
            tuple: (is_valid, confidence, reason)
        """
        if not yoruba_word or not english_translation:
            return False, 0, "Word or translation is empty"
            
        yoruba_word = yoruba_word.strip().lower()
        english_translation = english_translation.strip().lower()
        
        # Check against known translations
        if yoruba_word in self.known_translations:
            known_trans = [t.lower() for t in self.known_translations[yoruba_word]]
            if english_translation in known_trans:
                return True, 1.0, "Translation matches known reference"
            elif any(self._similar_enough(english_translation, kt) for kt in known_trans):
                return True, 0.8, "Translation is similar to known reference"
                
        # Check for suspicious translations
        if yoruba_word in self.suspect_translations:
            correct_options = self.suspect_translations[yoruba_word]
            if english_translation in [o.lower() for o in correct_options]:
                # Translation is in the list of possible translations, but context-dependent
                return True, 0.6, "Translation is plausible but context-dependent"
            else:
                # Translation not in the list of expected translations for this suspicious word
                return False, 0.3, f"Translation not in expected list for this word: {correct_options}"
                
        # Check for basic sanity (e.g., translation not too long or short compared to typical)
        if len(english_translation) < 2:
            return False, 0.2, "Translation suspiciously short"
            
        if len(english_translation) > 50:
            return False, 0.3, "Translation suspiciously long"
            
        # Check for common contaminants
        if re.search(r'(\{|\}|\[|\]|\(|\)|http|www|\.com)', english_translation):
            return False, 0.1, "Translation contains likely contamination"
            
        # By default, assume the translation might be valid but with medium confidence
        return True, 0.5, "Translation passed basic validation"
    
    def _similar_enough(self, str1, str2, threshold=0.8):
        """Determine if two strings are similar enough (simple implementation)."""
        # This is a simplified version - in a real implementation, you would use
        # a proper string similarity algorithm like Levenshtein distance or Jaccard similarity
        
        # Convert to lowercase and split into words
        words1 = set(str1.lower().split())
        words2 = set(str2.lower().split())
        
        # Calculate Jaccard similarity
        intersection = len(words1.intersection(words2))
        union = len(words1.union(words2))
        
        if union == 0:
            return False
            
        similarity = intersection / union
        return similarity >= threshold
    
    def validate_example(self, yoruba_text, english_text):
        """
        Validate if a Yoruba example and its English translation match.
        
        Args:
            yoruba_text: Yoruba example text
            english_text: English translation of the example
            
        Returns:
            tuple: (is_valid, confidence, reason)
        """
        if not yoruba_text or not english_text:
            return False, 0, "Example or translation is empty"
            
        yoruba_text = yoruba_text.strip()
        english_text = english_text.strip()
        
        # Check if the Yoruba text contains expected diacritics
        has_yoruba_diacritics = any(char in self.yoruba_diacritics for char in yoruba_text)
        if not has_yoruba_diacritics:
            return False, 0.2, "Yoruba example lacks expected diacritics"
            
        # Check for suspiciously short examples
        if len(yoruba_text) < 10 or len(english_text) < 10:
            return False, 0.3, "Example suspiciously short"
            
        # Check for reasonable length ratio between Yoruba and English
        # (English translations are typically longer than Yoruba text)
        ratio = len(english_text) / len(yoruba_text) if len(yoruba_text) > 0 else 0
        if ratio < 0.5 or ratio > 3.0:
            return False, 0.4, f"Suspicious length ratio between Yoruba and English: {ratio:.2f}"
            
        # Check for common contaminants in examples
        contaminant_patterns = [
            r'(\{|\}|\[|\]|<|>|\(|\)|http|www|\.com)',
            r'[A-Z]{2,}',  # Uppercase acronyms
            r'\d{4,}',     # Long numbers
        ]
        
        for pattern in contaminant_patterns:
            if re.search(pattern, yoruba_text) or re.search(pattern, english_text):
                return False, 0.2, f"Example contains likely contamination: {pattern}"
                
        # Check for programming code or HTML
        code_patterns = [r'function', r'class', r'const', r'var', r'<div', r'<span']
        for pattern in code_patterns:
            if re.search(pattern, yoruba_text) or re.search(pattern, english_text):
                return False, 0.1, "Example contains code-like content"
                
        # By default, assume the example is valid with medium confidence
        return True, 0.7, "Example passed basic validation"
    
    def fix_yoruba_spacing(self, text):
        """
        Apply Yoruba-specific spacing fixes.
        
        Args:
            text: Yoruba text to fix
            
        Returns:
            str: Fixed text
        """
        if not text or not isinstance(text, str):
            return text
            
        # Fix spacing for specific auxiliary verbs and particles
        text = re.sub(r'([áàńḿ])([a-zàáèéìíòóùúẹọṣ][a-zàáèéìíòóùúẹọṣ]+)', r'\1 \2', text)
        
        # Fix spacing after pronouns/particles
        pronouns = ['wọ́n', 'won', 'kí', 'ki', 'tó', 'to', 'ìyẹn', 'iyen', 'yìí', 'yii', 'èyí', 'eyi', 'bàá', 'baa']
        pattern = '|'.join([r'(' + p + r')' for p in pronouns])
        text = re.sub(pattern + r'([a-zàáèéìíòóùúẹọṣ][a-zàáèéìíòóùúẹọṣ]+)', r'\1 \2', text)
        
        # Fix common incorrect word formations
        text = re.sub(r'nià', r'ni à', text)
        text = re.sub(r'láti', r'lá ti', text)
        text = re.sub(r'síbẹ̀', r'sí bẹ̀', text)
        text = re.sub(r'walá', r'wa lá', text)
        text = re.sub(r'lọ́wọ́', r'lọ́ wọ́', text)
        
        # Fix for "Bí ... bá" construction which is commonly joined incorrectly
        text = re.sub(r'(Bí|bí)([a-zàáèéìíòóùúẹọṣ][a-zàáèéìíòóùúẹọṣ]+)', r'\1 \2', text)
        
        # Remove any multiple spaces that might have been created
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
    
    def fix_yoruba_diacritics(self, text):
        """
        Fix common diacritic errors in Yoruba text.
        
        Args:
            text: Text to fix
            
        Returns:
            str: Fixed text
        """
        if not text or not isinstance(text, str):
            return text
            
        # Replace Latin 's' with Yoruba 'ṣ' when appropriate
        text = re.sub(r's\.', 'ṣ', text)
        
        # Replace Latin 'e' with Yoruba 'ẹ' when appropriate
        text = re.sub(r'e\.', 'ẹ', text)
        
        # Replace Latin 'o' with Yoruba 'ọ' when appropriate
        text = re.sub(r'o\.', 'ọ', text)
        
        # Fix common misplaced diacritics (simplified)
        # A proper implementation would require more complex linguistic rules
        # For example, high tone after low tone in certain contexts
        
        return text

    def summarize_validation_results(self, results):
        """
        Summarize validation results for reporting.
        
        Args:
            results: List of validation results
            
        Returns:
            dict: Summary statistics
        """
        summary = {
            "total": len(results),
            "valid": sum(1 for r in results if r[0]),
            "invalid": sum(1 for r in results if not r[0]),
            "high_confidence": sum(1 for r in results if r[1] >= 0.8),
            "medium_confidence": sum(1 for r in results if 0.5 <= r[1] < 0.8),
            "low_confidence": sum(1 for r in results if r[1] < 0.5),
            "common_issues": {}
        }
        
        # Count common issues
        for _, _, reason in results:
            if reason not in summary["common_issues"]:
                summary["common_issues"][reason] = 0
            summary["common_issues"][reason] += 1
            
        # Sort issues by frequency
        summary["common_issues"] = dict(
            sorted(
                summary["common_issues"].items(),
                key=lambda x: x[1],
                reverse=True
            )
        )
        
        return summary

# Command-line interface for validation
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Validate Yoruba language data")
    parser.add_argument("--word", help="Validate a specific Yoruba word")
    parser.add_argument("--reference-dir", default="./yoruba_words", help="Directory containing reference word lists")
    parser.add_argument("--translation", help="English translation to validate (requires --word)")
    parser.add_argument("--example", nargs=2, metavar=("YORUBA", "ENGLISH"), help="Validate an example sentence pair")
    parser.add_argument("--fix-spacing", help="Fix spacing in provided Yoruba text")
    parser.add_argument("--fix-diacritics", help="Fix diacritics in provided Yoruba text")
    
    args = parser.parse_args()
    
    validator = YorubaLanguageValidator(reference_dir=args.reference_dir)
    
    if args.word:
        valid, reason = validator.is_valid_yoruba_word(args.word)
        print(f"Word validation: {'Valid' if valid else 'Invalid'} - {reason}")
        
        if args.translation:
            valid, confidence, reason = validator.validate_translation(args.word, args.translation)
            print(f"Translation validation: {'Valid' if valid else 'Invalid'} - Confidence: {confidence:.2f} - {reason}")
    
    if args.example:
        yoruba, english = args.example
        valid, confidence, reason = validator.validate_example(yoruba, english)
        print(f"Example validation: {'Valid' if valid else 'Invalid'} - Confidence: {confidence:.2f} - {reason}")
    
    if args.fix_spacing:
        fixed = validator.fix_yoruba_spacing(args.fix_spacing)
        print(f"Original: {args.fix_spacing}")
        print(f"Fixed:    {fixed}")
    
    if args.fix_diacritics:
        fixed = validator.fix_yoruba_diacritics(args.fix_diacritics)
        print(f"Original: {args.fix_diacritics}")
        print(f"Fixed:    {fixed}")
        
    # If no specific action was requested, show help
    if not any([args.word, args.example, args.fix_spacing, args.fix_diacritics]):
        parser.print_help() 