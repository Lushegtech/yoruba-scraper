#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Example Sentence Validator for Yoruba-English Dictionary

This module provides specialized validation for Yoruba-English example sentence pairs.
It implements rigorous checks to detect inaccurate, non-idiomatic, or machine-generated
example sentences, ensuring high quality for language learning and reference materials.
"""

import re
import json
import logging
import os
from pathlib import Path
from difflib import SequenceMatcher
import unicodedata

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("example_validation.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("example_validator")

class ExampleSentenceValidator:
    """
    A specialized validator for Yoruba-English example sentence pairs that detects
    inaccurate, non-idiomatic, or machine-generated examples through multiple validation strategies.
    """
    
    def __init__(self, reference_file=None, common_words_file=None):
        """
        Initialize the validator with reference data.
        
        Args:
            reference_file: Optional file with verified example pairs
            common_words_file: File with common Yoruba words for validation
        """
        self.reference_file = reference_file
        self.common_words_file = common_words_file
        
        # Load reference examples
        self.reference_examples = []
        if reference_file and os.path.exists(reference_file):
            self.reference_examples = self._load_reference_examples()
            
        # Load common Yoruba words
        self.common_words = set()
        if common_words_file and os.path.exists(common_words_file):
            self.common_words = self._load_common_words()
        
        # Yoruba diacritics and specific characters
        self.yoruba_chars = set('àáèéìíòóùúẹọṣÀÁÈÉÌÍÒÓÙÚẸỌṢ')
        
        # Regular expressions for detecting suspicious patterns
        self.suspicious_patterns = [
            # HTML or XML tags
            r'</?[a-z]+[^>]*>',
            # URLs
            r'https?://\S+',
            # Email-like patterns
            r'\S+@\S+\.\S+',
            # Unusual special characters
            r'[^\w\s.,;:!?()\'"-]',
            # Code-like patterns
            r'\bfunction\b|\bvar\b|\bconst\b|\breturn\b',
            # Long numbers
            r'\d{4,}',
            # Single character repetitions (likely noise)
            r'([a-zA-Z])\1{4,}',
            # Suspiciously long words
            r'\b\w{25,}\b',
        ]
        
        # Patterns that indicate machine translation
        self.machine_translation_indicators = [
            # Unnaturally formal or stilted phrases
            r'\bin accordance with\b',
            r'\butilize\b|\butilise\b',
            r'\bthe aforementioned\b',
            # Literal translations of idioms
            r'\bin the same breath\b',
            r'\bon the other hand\b',
            # Overly literal structure preservation
            r'\bthe Yoruba language\b',
            r'\bin Yoruba culture\b',
            # Nested subordinate clauses (typical of machine translation)
            r'\b(that|which) the .+ that\b',
            # Repeated translations
            r'(\b\w+\b)(\s+\1\b){2,}',
        ]
        
        # Yoruba grammar rules for validation
        self.yoruba_syntax_patterns = {
            # Subject-Verb pattern: In Yoruba, this is typically SVO
            "subject_verb": r'^[A-Z][^\s\.]+\s+[^\s\.]+',
            
            # Verb auxiliary pattern: In Yoruba, many auxiliaries come before the main verb
            "verb_auxiliary": r'\b(á|à|ń|ti|ó|kò|yóò|máa)\s+\w+\b',
            
            # Question formulation: Yoruba often keeps SVO order but adds question words
            "question": r'\b(kí|ta|báwo|èló|ṣé|níbo|kílódé)\b',
            
            # Negation pattern: "kò" precedes the verb in Yoruba
            "negation": r'\bkò\s+\w+\b',
        }
    
    def _load_reference_examples(self):
        """Load reference example pairs."""
        examples = []
        try:
            with open(self.reference_file, 'r', encoding='utf-8') as f:
                examples = json.load(f)
            logger.info(f"Loaded {len(examples)} reference examples")
        except Exception as e:
            logger.error(f"Error loading reference examples: {str(e)}")
        return examples
    
    def _load_common_words(self):
        """Load common Yoruba words."""
        words = set()
        try:
            with open(self.common_words_file, 'r', encoding='utf-8') as f:
                for line in f:
                    word = line.strip()
                    if word:
                        words.add(word.lower())
            logger.info(f"Loaded {len(words)} common Yoruba words")
        except Exception as e:
            logger.error(f"Error loading common words: {str(e)}")
        return words
    
    def validate_example_pair(self, yoruba_example, english_example, related_word=None):
        """
        Validate an example sentence pair.
        
        Args:
            yoruba_example: Yoruba example sentence
            english_example: English translation of the example
            related_word: The Yoruba word this example illustrates (optional)
            
        Returns:
            tuple: (is_valid, confidence, reasons)
        """
        if not yoruba_example or not english_example:
            return False, 0.0, ["Missing example or translation"]
        
        yoruba_example = yoruba_example.strip()
        english_example = english_example.strip()
        
        reasons = []
        confidence = 1.0  # Start with full confidence
        
        # Check for Yoruba diacritics (essential for proper Yoruba)
        if not any(char in self.yoruba_chars for char in yoruba_example):
            reasons.append("Yoruba text lacks essential diacritics")
            confidence *= 0.5
        
        # Check for suspicious patterns
        for pattern in self.suspicious_patterns:
            if re.search(pattern, yoruba_example):
                reasons.append(f"Yoruba example contains suspicious pattern: {pattern}")
                confidence *= 0.7
            if re.search(pattern, english_example):
                reasons.append(f"English example contains suspicious pattern: {pattern}")
                confidence *= 0.7
        
        # Check for machine translation indicators
        machine_translation_score = 0
        for pattern in self.machine_translation_indicators:
            if re.search(pattern, english_example, re.IGNORECASE):
                machine_translation_score += 1
                reasons.append(f"Example may be machine-translated (found '{pattern}')")
        
        if machine_translation_score > 0:
            confidence *= (1.0 - (0.1 * min(machine_translation_score, 5)))
        
        # Check length ratio (English usually longer than Yoruba in terms of words)
        yoruba_words = yoruba_example.split()
        english_words = english_example.split()
        
        if len(yoruba_words) == 0 or len(english_words) == 0:
            reasons.append("Example contains no words")
            confidence = 0.0
        else:
            ratio = len(english_words) / len(yoruba_words)
            if ratio < 0.5:
                reasons.append(f"English translation suspiciously short (ratio: {ratio:.2f})")
                confidence *= 0.7
            elif ratio > 3.0:
                reasons.append(f"English translation suspiciously long (ratio: {ratio:.2f})")
                confidence *= 0.8
        
        # Check for minimum length
        if len(yoruba_words) < 3:
            reasons.append("Yoruba example too short to be useful")
            confidence *= 0.8
        if len(english_words) < 3:
            reasons.append("English example too short to be useful")
            confidence *= 0.8
        
        # Check for related word in the example
        if related_word and related_word.strip():
            related_word = related_word.strip().lower()
            if related_word not in yoruba_example.lower():
                reasons.append(f"Related word '{related_word}' not found in Yoruba example")
                confidence *= 0.9
        
        # Check Yoruba syntax patterns
        yoruba_syntax_valid = False
        for pattern_name, pattern in self.yoruba_syntax_patterns.items():
            if re.search(pattern, yoruba_example):
                yoruba_syntax_valid = True
                break
        
        if not yoruba_syntax_valid and len(yoruba_words) > 3:
            reasons.append("Yoruba example may not follow typical syntax patterns")
            confidence *= 0.9
        
        # Check capitalization and punctuation
        if not yoruba_example[0].isupper():
            reasons.append("Yoruba example should start with a capital letter")
            confidence *= 0.95
            
        if not english_example[0].isupper():
            reasons.append("English example should start with a capital letter")
            confidence *= 0.95
            
        if not yoruba_example.endswith(('.', '!', '?')):
            reasons.append("Yoruba example lacks proper ending punctuation")
            confidence *= 0.95
            
        if not english_example.endswith(('.', '!', '?')):
            reasons.append("English example lacks proper ending punctuation")
            confidence *= 0.95
        
        # Validate against reference examples if available
        if self.reference_examples:
            best_match_score = 0
            for ref in self.reference_examples:
                ref_yoruba = ref.get("yoruba", "")
                ref_english = ref.get("english", "")
                
                if ref_yoruba and ref_english:
                    y_similarity = self._similarity_score(yoruba_example, ref_yoruba)
                    e_similarity = self._similarity_score(english_example, ref_english)
                    
                    # If both are very similar to a reference pair, it's likely valid
                    if y_similarity > 0.8 and e_similarity > 0.8:
                        best_match_score = max(best_match_score, min(y_similarity, e_similarity))
            
            if best_match_score > 0.9:
                reasons.append(f"Very similar to validated reference example ({best_match_score:.2f})")
                confidence = max(confidence, 0.95)  # Boost confidence based on reference match
            elif best_match_score > 0.8:
                reasons.append(f"Similar to validated reference example ({best_match_score:.2f})")
                confidence = max(confidence, 0.85)
        
        # Final validity determination
        is_valid = confidence >= 0.7
        
        return is_valid, confidence, reasons
    
    def validate_example_collection(self, examples_data, output_file=None):
        """
        Validate a collection of example sentence pairs.
        
        Args:
            examples_data: List of dictionaries with yoruba/english examples
            output_file: Optional file to save validation results
            
        Returns:
            dict: Validation results
        """
        results = {
            "total": len(examples_data),
            "valid": 0,
            "suspicious": 0,
            "invalid": 0,
            "examples": []
        }
        
        for idx, example in enumerate(examples_data):
            # Extract example pair from different possible formats
            if isinstance(example, dict):
                yoruba = example.get("yoruba", example.get("yoruba_example", ""))
                english = example.get("english", example.get("english_example", ""))
                word = example.get("word", example.get("yoruba_word", ""))
            elif isinstance(example, list) and len(example) >= 2:
                yoruba = example[0]
                english = example[1]
                word = example[2] if len(example) > 2 else ""
            else:
                logger.warning(f"Skipping example at index {idx}: invalid format")
                continue
            
            # Validate the pair
            is_valid, confidence, reasons = self.validate_example_pair(yoruba, english, word)
            
            # Categorize based on confidence
            if confidence >= 0.8:
                status = "valid"
                results["valid"] += 1
            elif confidence >= 0.5:
                status = "suspicious"
                results["suspicious"] += 1
            else:
                status = "invalid"
                results["invalid"] += 1
            
            # Add to results
            results["examples"].append({
                "yoruba": yoruba,
                "english": english,
                "word": word,
                "status": status,
                "confidence": confidence,
                "reasons": reasons
            })
        
        # Calculate percentages
        if results["total"] > 0:
            results["valid_percent"] = round((results["valid"] / results["total"]) * 100, 2)
            results["suspicious_percent"] = round((results["suspicious"] / results["total"]) * 100, 2)
            results["invalid_percent"] = round((results["invalid"] / results["total"]) * 100, 2)
        
        # Save to file if requested
        if output_file:
            try:
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(results, f, indent=2, ensure_ascii=False)
                logger.info(f"Saved validation results to {output_file}")
            except Exception as e:
                logger.error(f"Error saving validation results: {str(e)}")
        
        return results
    
    def fix_yoruba_example(self, example):
        """
        Fix common issues in Yoruba example sentences.
        
        Args:
            example: The Yoruba example to fix
            
        Returns:
            str: Fixed example
        """
        if not example:
            return example
        
        # Fix capitalization
        if example and not example[0].isupper():
            example = example[0].upper() + example[1:]
        
        # Fix ending punctuation
        if example and not example.endswith(('.', '!', '?')):
            example += '.'
        
        # Fix spacing issues
        example = self._fix_yoruba_spacing(example)
        
        # Normalize Unicode for consistent diacritics
        example = unicodedata.normalize('NFC', example)
        
        return example
    
    def fix_english_example(self, example):
        """
        Fix common issues in English example sentences.
        
        Args:
            example: The English example to fix
            
        Returns:
            str: Fixed example
        """
        if not example:
            return example
        
        # Fix capitalization
        if example and not example[0].isupper():
            example = example[0].upper() + example[1:]
        
        # Fix ending punctuation
        if example and not example.endswith(('.', '!', '?')):
            example += '.'
        
        # Fix common spacing issues
        example = re.sub(r'\s+', ' ', example).strip()
        
        # Fix spacing around punctuation
        example = re.sub(r'\s+([.,;:!?])', r'\1', example)
        
        return example
    
    def _fix_yoruba_spacing(self, text):
        """Fix Yoruba-specific spacing issues."""
        if not text:
            return text
        
        # Fix spacing for auxiliary verbs
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
        
        # Remove any multiple spaces that might have been created
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
    
    def _similarity_score(self, str1, str2):
        """Calculate similarity between two strings."""
        return SequenceMatcher(None, str1, str2).ratio()

# Command-line interface for example validation
if __name__ == "__main__":
    import argparse
    import sys
    
    parser = argparse.ArgumentParser(description="Validate Yoruba-English example sentences")
    parser.add_argument("--check", nargs=2, metavar=("YORUBA", "ENGLISH"), 
                        help="Check a single example pair")
    parser.add_argument("--file", help="Process examples from a JSON file")
    parser.add_argument("--output", help="Output file for validation results")
    parser.add_argument("--reference", help="Reference file with verified examples")
    parser.add_argument("--common-words", help="File with common Yoruba words")
    parser.add_argument("--fix", action="store_true", help="Fix common issues in examples")
    
    args = parser.parse_args()
    
    # Initialize the validator
    validator = ExampleSentenceValidator(
        reference_file=args.reference,
        common_words_file=args.common_words
    )
    
    if args.check:
        yoruba, english = args.check
        is_valid, confidence, reasons = validator.validate_example_pair(yoruba, english)
        
        print(f"Example pair validation:")
        print(f"Yoruba: {yoruba}")
        print(f"English: {english}")
        print(f"Valid: {is_valid}, Confidence: {confidence:.2f}")
        print("Reasons:")
        for reason in reasons:
            print(f"- {reason}")
        
        if args.fix and (not is_valid or confidence < 0.8):
            fixed_yoruba = validator.fix_yoruba_example(yoruba)
            fixed_english = validator.fix_english_example(english)
            
            if fixed_yoruba != yoruba or fixed_english != english:
                print("\nSuggested fixes:")
                if fixed_yoruba != yoruba:
                    print(f"Yoruba: {fixed_yoruba}")
                if fixed_english != english:
                    print(f"English: {fixed_english}")
    
    elif args.file:
        if not os.path.exists(args.file):
            print(f"Error: File {args.file} not found")
            sys.exit(1)
            
        try:
            with open(args.file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            print(f"Processing {len(data)} examples from {args.file}")
            
            # Validate the examples
            results = validator.validate_example_collection(data, args.output)
            
            # Print summary
            print("\nValidation Results Summary:")
            print(f"Total examples: {results['total']}")
            print(f"Valid: {results['valid']} ({results.get('valid_percent', 0)}%)")
            print(f"Suspicious: {results['suspicious']} ({results.get('suspicious_percent', 0)}%)")
            print(f"Invalid: {results['invalid']} ({results.get('invalid_percent', 0)}%)")
            
            # If fix option is enabled, also create a fixed version
            if args.fix:
                fixed_data = []
                for example in results["examples"]:
                    if example["status"] != "valid":
                        fixed_yoruba = validator.fix_yoruba_example(example["yoruba"])
                        fixed_english = validator.fix_english_example(example["english"])
                        
                        fixed_data.append({
                            "yoruba_original": example["yoruba"],
                            "english_original": example["english"],
                            "yoruba_fixed": fixed_yoruba,
                            "english_fixed": fixed_english,
                            "word": example["word"],
                            "confidence": example["confidence"],
                            "reasons": example["reasons"]
                        })
                
                if fixed_data:
                    fix_output = args.output.replace('.json', '_fixed.json') if args.output else 'fixed_examples.json'
                    with open(fix_output, 'w', encoding='utf-8') as f:
                        json.dump(fixed_data, f, indent=2, ensure_ascii=False)
                    print(f"\nSaved {len(fixed_data)} fixed examples to {fix_output}")
                
        except Exception as e:
            print(f"Error processing file: {str(e)}")
            import traceback
            traceback.print_exc()
    
    else:
        parser.print_help() 