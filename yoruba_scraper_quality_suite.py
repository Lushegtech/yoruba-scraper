#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Yoruba Scraper Quality Suite

This script coordinates all validation components to ensure the highest quality
Yoruba language data. It provides a comprehensive quality assurance workflow
that validates words, translations, and example sentences while fixing common issues.

Usage:
    python yoruba_scraper_quality_suite.py --validate-all
    python yoruba_scraper_quality_suite.py --validate-word "àdúrà"
    python yoruba_scraper_quality_suite.py --fix-data scraped_data/
"""

import os
import sys
import json
import logging
import argparse
from pathlib import Path
import csv
import pandas as pd
from tqdm import tqdm

# Import our validation modules
try:
    from yoruba_language_validator import YorubaLanguageValidator
    from enhanced_translation_validator import EnhancedTranslationValidator
    from example_sentence_validator import ExampleSentenceValidator
except ImportError:
    print("Error: Required validation modules not found.")
    print("Make sure yoruba_language_validator.py, enhanced_translation_validator.py, and example_sentence_validator.py are in the same directory.")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("quality_suite.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("quality_suite")

class YorubaScraperQualitySuite:
    """
    A comprehensive quality assurance suite for the Yoruba scraper.
    Coordinates all validation components to ensure high-quality data.
    """
    
    def __init__(self, base_folder="./scraped_data", reference_dir="./yoruba_words"):
        """
        Initialize the quality suite.
        
        Args:
            base_folder: Base folder containing scraped data
            reference_dir: Directory with reference Yoruba words
        """
        self.base_folder = base_folder
        self.reference_dir = reference_dir
        
        # Ensure directories exist
        os.makedirs(base_folder, exist_ok=True)
        
        # Initialize validation components
        self.word_validator = YorubaLanguageValidator(reference_dir=reference_dir)
        self.translation_validator = EnhancedTranslationValidator(
            known_words_dir=reference_dir
        )
        self.example_validator = ExampleSentenceValidator()
        
        # Output directories for validation results
        self.validation_dir = os.path.join(base_folder, "validation_results")
        os.makedirs(self.validation_dir, exist_ok=True)
        
        self.fixed_data_dir = os.path.join(base_folder, "fixed_data")
        os.makedirs(self.fixed_data_dir, exist_ok=True)
    
    def validate_word(self, word):
        """
        Validate a single Yoruba word and all its associated data.
        
        Args:
            word: Yoruba word to validate
            
        Returns:
            dict: Validation results
        """
        word = word.strip()
        logger.info(f"Validating word: {word}")
        
        # Initialize result structure
        result = {
            "word": word,
            "word_validation": {},
            "translations": [],
            "examples": []
        }
        
        # 1. Validate the word itself
        is_valid, reason = self.word_validator.is_valid_yoruba_word(word)
        result["word_validation"] = {
            "is_valid": is_valid,
            "reason": reason
        }
        
        # 2. Look for this word's data in the scraped data
        word_data = self._get_word_data(word)
        
        if word_data:
            # 3. Validate translations
            if "translations" in word_data:
                translations = word_data["translations"]
                if isinstance(translations, str):
                    translations = [translations]
                    
                for translation in translations:
                    is_valid, confidence, reason = self.translation_validator.verify_translation(
                        word, translation
                    )
                    result["translations"].append({
                        "translation": translation,
                        "is_valid": is_valid,
                        "confidence": confidence,
                        "reason": reason
                    })
            
            # 4. Validate examples
            if "examples" in word_data:
                examples = word_data["examples"]
                for example in examples:
                    if isinstance(example, dict):
                        yoruba = example.get("yoruba", "")
                        english = example.get("english", "")
                    elif isinstance(example, list) and len(example) >= 2:
                        yoruba = example[0]
                        english = example[1]
                    else:
                        continue
                        
                    is_valid, confidence, reasons = self.example_validator.validate_example_pair(
                        yoruba, english, word
                    )
                    result["examples"].append({
                        "yoruba": yoruba,
                        "english": english,
                        "is_valid": is_valid,
                        "confidence": confidence,
                        "reasons": reasons
                    })
        
        # Generate summary
        result["summary"] = {
            "word_valid": result["word_validation"]["is_valid"],
            "translations_total": len(result["translations"]),
            "translations_valid": sum(1 for t in result["translations"] if t["is_valid"]),
            "examples_total": len(result["examples"]),
            "examples_valid": sum(1 for e in result["examples"] if e["is_valid"])
        }
        
        # Overall quality score (0-100)
        score = 0
        if result["word_validation"]["is_valid"]:
            score += 30  # Word validation is worth 30%
            
        if result["translations_total"] > 0:
            trans_score = (result["summary"]["translations_valid"] / result["translations_total"]) * 40
            score += trans_score  # Translations worth 40%
            
        if result["examples_total"] > 0:
            example_score = (result["summary"]["examples_valid"] / result["examples_total"]) * 30
            score += example_score  # Examples worth 30%
            
        result["summary"]["quality_score"] = round(score, 2)
        
        return result
    
    def validate_all_data(self, output_file=None):
        """
        Validate all scraped data and generate a comprehensive report.
        
        Args:
            output_file: Optional file to save validation results
            
        Returns:
            dict: Validation summary
        """
        logger.info("Starting validation of all scraped data")
        
        # Get all scraped data files
        data_files = self._get_data_files()
        
        if not data_files:
            logger.warning("No data files found for validation")
            return {
                "status": "error",
                "message": "No data files found"
            }
        
        logger.info(f"Found {len(data_files)} data files to validate")
        
        # Initialize counters and result lists
        all_words = []
        valid_words = 0
        all_translations = []
        valid_translations = 0
        all_examples = []
        valid_examples = 0
        
        problematic_words = []
        problematic_translations = []
        problematic_examples = []
        
        # Process each file
        for file_path in tqdm(data_files, desc="Validating data files"):
            try:
                # Load data
                data = self._load_data_file(file_path)
                
                # Process each word
                for word, word_data in data.items():
                    # Validate word
                    is_valid, reason = self.word_validator.is_valid_yoruba_word(word)
                    all_words.append(word)
                    
                    if is_valid:
                        valid_words += 1
                    else:
                        problematic_words.append({
                            "word": word,
                            "reason": reason,
                            "file": file_path
                        })
                    
                    # Validate translations
                    if "translations" in word_data:
                        translations = word_data["translations"]
                        if isinstance(translations, str):
                            translations = [translations]
                            
                        for translation in translations:
                            all_translations.append((word, translation))
                            
                            is_valid, confidence, reason = self.translation_validator.verify_translation(
                                word, translation
                            )
                            
                            if is_valid and confidence >= 0.7:
                                valid_translations += 1
                            else:
                                problematic_translations.append({
                                    "word": word,
                                    "translation": translation,
                                    "confidence": confidence,
                                    "reason": reason,
                                    "file": file_path
                                })
                    
                    # Validate examples
                    if "examples" in word_data:
                        examples = word_data["examples"]
                        for example in examples:
                            if isinstance(example, dict):
                                yoruba = example.get("yoruba", "")
                                english = example.get("english", "")
                            elif isinstance(example, list) and len(example) >= 2:
                                yoruba = example[0]
                                english = example[1]
                            else:
                                continue
                                
                            all_examples.append((word, yoruba, english))
                            
                            is_valid, confidence, reasons = self.example_validator.validate_example_pair(
                                yoruba, english, word
                            )
                            
                            if is_valid and confidence >= 0.7:
                                valid_examples += 1
                            else:
                                problematic_examples.append({
                                    "word": word,
                                    "yoruba": yoruba,
                                    "english": english,
                                    "confidence": confidence,
                                    "reasons": reasons,
                                    "file": file_path
                                })
            
            except Exception as e:
                logger.error(f"Error processing file {file_path}: {str(e)}")
        
        # Generate summary
        summary = {
            "total_words": len(all_words),
            "valid_words": valid_words,
            "valid_words_percent": round((valid_words / len(all_words) * 100) if all_words else 0, 2),
            
            "total_translations": len(all_translations),
            "valid_translations": valid_translations,
            "valid_translations_percent": round((valid_translations / len(all_translations) * 100) if all_translations else 0, 2),
            
            "total_examples": len(all_examples),
            "valid_examples": valid_examples,
            "valid_examples_percent": round((valid_examples / len(all_examples) * 100) if all_examples else 0, 2),
            
            "problematic_words": problematic_words,
            "problematic_translations": problematic_translations,
            "problematic_examples": problematic_examples
        }
        
        # Calculate overall quality score
        word_score = summary["valid_words_percent"] * 0.3
        translation_score = summary["valid_translations_percent"] * 0.4
        example_score = summary["valid_examples_percent"] * 0.3
        
        summary["overall_quality_score"] = round(word_score + translation_score + example_score, 2)
        
        # Save results if requested
        if output_file:
            try:
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(summary, f, indent=2, ensure_ascii=False)
                logger.info(f"Saved validation summary to {output_file}")
            except Exception as e:
                logger.error(f"Error saving validation summary: {str(e)}")
        
        return summary
    
    def fix_data(self, input_path, output_path=None):
        """
        Fix data quality issues in scraped data.
        
        Args:
            input_path: Path to data file or directory to fix
            output_path: Optional path to save fixed data
            
        Returns:
            dict: Summary of fixes
        """
        if not output_path:
            if os.path.isfile(input_path):
                filename = os.path.basename(input_path)
                output_path = os.path.join(self.fixed_data_dir, filename)
            else:
                output_path = self.fixed_data_dir
        
        logger.info(f"Fixing data quality issues in {input_path}")
        logger.info(f"Fixed data will be saved to {output_path}")
        
        # Initialize counters
        fixes = {
            "total_files": 0,
            "words_fixed": 0,
            "translations_fixed": 0,
            "examples_fixed": 0,
            "details": []
        }
        
        # Process files
        if os.path.isfile(input_path):
            file_paths = [input_path]
        else:
            file_paths = []
            for root, _, files in os.walk(input_path):
                for file in files:
                    if file.endswith(('.json', '.csv')):
                        file_paths.append(os.path.join(root, file))
        
        for file_path in tqdm(file_paths, desc="Fixing data files"):
            try:
                # Load data
                data = self._load_data_file(file_path)
                fixes["total_files"] += 1
                
                # Create structure to track fixes in this file
                file_fixes = {
                    "file": file_path,
                    "words_fixed": [],
                    "translations_fixed": [],
                    "examples_fixed": []
                }
                
                # Process and fix each word entry
                fixed_data = {}
                for word, word_data in data.items():
                    # Fix the word itself if needed
                    fixed_word = word
                    is_valid, _ = self.word_validator.is_valid_yoruba_word(word)
                    
                    if not is_valid:
                        # Try to fix word using diacritics and spacing corrections
                        fixed_word = self.word_validator.fix_yoruba_spacing(word)
                        fixed_word = self.word_validator.fix_yoruba_diacritics(fixed_word)
                        
                        if fixed_word != word:
                            fixes["words_fixed"] += 1
                            file_fixes["words_fixed"].append({
                                "original": word,
                                "fixed": fixed_word
                            })
                    
                    # Create entry for fixed word
                    fixed_data[fixed_word] = {}
                    
                    # Fix translations if needed
                    if "translations" in word_data:
                        fixed_translations = []
                        translations = word_data["translations"]
                        
                        if isinstance(translations, str):
                            translations = [translations]
                            
                        for translation in translations:
                            is_valid, confidence, _ = self.translation_validator.verify_translation(
                                fixed_word, translation
                            )
                            
                            if not is_valid or confidence < 0.7:
                                # For translations, we can only do basic cleanup
                                fixed_translation = translation.strip()
                                fixed_translation = re.sub(r'\s+', ' ', fixed_translation)
                                
                                # Remove common contamination markers
                                fixed_translation = re.sub(r'<[^>]+>', '', fixed_translation)
                                fixed_translation = re.sub(r'\[\d+\]', '', fixed_translation)
                                
                                if fixed_translation != translation:
                                    fixes["translations_fixed"] += 1
                                    file_fixes["translations_fixed"].append({
                                        "word": fixed_word,
                                        "original": translation,
                                        "fixed": fixed_translation
                                    })
                                    translation = fixed_translation
                            
                            fixed_translations.append(translation)
                        
                        fixed_data[fixed_word]["translations"] = fixed_translations
                    
                    # Fix examples if needed
                    if "examples" in word_data:
                        fixed_examples = []
                        examples = word_data["examples"]
                        
                        for example in examples:
                            if isinstance(example, dict):
                                yoruba = example.get("yoruba", "")
                                english = example.get("english", "")
                            elif isinstance(example, list) and len(example) >= 2:
                                yoruba = example[0]
                                english = example[1]
                            else:
                                continue
                            
                            is_valid, confidence, _ = self.example_validator.validate_example_pair(
                                yoruba, english, fixed_word
                            )
                            
                            if not is_valid or confidence < 0.7:
                                # Apply fixes to examples
                                fixed_yoruba = self.example_validator.fix_yoruba_example(yoruba)
                                fixed_english = self.example_validator.fix_english_example(english)
                                
                                if fixed_yoruba != yoruba or fixed_english != english:
                                    fixes["examples_fixed"] += 1
                                    file_fixes["examples_fixed"].append({
                                        "word": fixed_word,
                                        "original_yoruba": yoruba,
                                        "fixed_yoruba": fixed_yoruba,
                                        "original_english": english,
                                        "fixed_english": fixed_english
                                    })
                                    yoruba = fixed_yoruba
                                    english = fixed_english
                            
                            fixed_examples.append({
                                "yoruba": yoruba,
                                "english": english
                            })
                        
                        fixed_data[fixed_word]["examples"] = fixed_examples
                    
                    # Copy any other fields from the original data
                    for key, value in word_data.items():
                        if key not in ["translations", "examples"]:
                            fixed_data[fixed_word][key] = value
                
                # Save fixed data
                if os.path.isfile(input_path):
                    output_file = output_path
                else:
                    rel_path = os.path.relpath(file_path, input_path)
                    output_file = os.path.join(output_path, rel_path)
                
                os.makedirs(os.path.dirname(output_file), exist_ok=True)
                
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(fixed_data, f, indent=2, ensure_ascii=False)
                
                fixes["details"].append(file_fixes)
                
            except Exception as e:
                logger.error(f"Error fixing file {file_path}: {str(e)}")
        
        # Save fix summary
        summary_file = os.path.join(output_path, "fix_summary.json")
        try:
            with open(summary_file, 'w', encoding='utf-8') as f:
                json.dump(fixes, f, indent=2)
            logger.info(f"Saved fix summary to {summary_file}")
        except Exception as e:
            logger.error(f"Error saving fix summary: {str(e)}")
        
        return fixes
    
    def _get_word_data(self, word):
        """Get data for a specific word from the scraped data."""
        # Check if there's a JSON file for this word
        word_file = os.path.join(self.base_folder, f"{word}.json")
        if os.path.isfile(word_file):
            try:
                with open(word_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception:
                pass
        
        # If not found as a single file, search in all data files
        for file_path in self._get_data_files():
            try:
                data = self._load_data_file(file_path)
                if word in data:
                    return data[word]
            except Exception:
                continue
        
        return None
    
    def _get_data_files(self):
        """Get all data files in the base folder."""
        data_files = []
        for root, _, files in os.walk(self.base_folder):
            for file in files:
                if file.endswith(('.json', '.csv')) and not file.startswith('.'):
                    data_files.append(os.path.join(root, file))
        return data_files
    
    def _load_data_file(self, file_path):
        """Load data from a file (supports JSON and CSV)."""
        if not os.path.isfile(file_path):
            return {}
        
        try:
            ext = os.path.splitext(file_path)[1].lower()
            
            if ext == '.json':
                with open(file_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            
            elif ext == '.csv':
                data = {}
                df = pd.read_csv(file_path, encoding='utf-8')
                
                # Try to determine the column names
                word_col = None
                for col in df.columns:
                    if 'word' in col.lower() or 'yoruba' in col.lower():
                        word_col = col
                        break
                
                if not word_col and len(df.columns) > 0:
                    word_col = df.columns[0]
                
                if word_col:
                    for _, row in df.iterrows():
                        word = str(row[word_col]).strip()
                        if word:
                            data[word] = {col: row[col] for col in df.columns if col != word_col}
                
                return data
            
            else:
                logger.warning(f"Unsupported file format: {ext}")
                return {}
                
        except Exception as e:
            logger.error(f"Error loading data from {file_path}: {str(e)}")
            return {}

# Helper function for importing required modules
def check_requirements():
    """Check if all required modules are available."""
    required_modules = ['pandas', 'tqdm']
    missing_modules = []
    
    for module in required_modules:
        try:
            __import__(module)
        except ImportError:
            missing_modules.append(module)
    
    if missing_modules:
        print(f"Missing required modules: {', '.join(missing_modules)}")
        print("Please install them using: pip install " + " ".join(missing_modules))
        return False
    
    return True

# Main CLI interface
def main():
    """Main function to run the quality suite from command line."""
    parser = argparse.ArgumentParser(description="Yoruba Scraper Quality Suite")
    
    # Main operation groups
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--validate-word", help="Validate a specific Yoruba word")
    group.add_argument("--validate-all", action="store_true", help="Validate all scraped data")
    group.add_argument("--fix-data", help="Fix data quality issues in specified file or directory")
    
    # Common options
    parser.add_argument("--base-folder", default="./scraped_data", help="Base folder containing scraped data")
    parser.add_argument("--reference-dir", default="./yoruba_words", help="Directory with reference Yoruba words")
    parser.add_argument("--output", help="Output file for results")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    
    args = parser.parse_args()
    
    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Check requirements
    if not check_requirements():
        return 1
    
    # Initialize quality suite
    suite = YorubaScraperQualitySuite(
        base_folder=args.base_folder,
        reference_dir=args.reference_dir
    )
    
    try:
        # Process requested operation
        if args.validate_word:
            word = args.validate_word
            result = suite.validate_word(word)
            
            # Save result if output specified
            if args.output:
                with open(args.output, 'w', encoding='utf-8') as f:
                    json.dump(result, f, indent=2, ensure_ascii=False)
            
            # Print summary
            print(f"\nValidation summary for '{word}':")
            print(f"Word valid: {result['word_validation']['is_valid']} - {result['word_validation']['reason']}")
            print(f"Translations: {result['summary']['translations_valid']}/{result['summary']['translations_total']} valid")
            print(f"Examples: {result['summary']['examples_valid']}/{result['summary']['examples_total']} valid")
            print(f"Overall quality score: {result['summary']['quality_score']}/100")
            
        elif args.validate_all:
            output_file = args.output or os.path.join(suite.validation_dir, "validation_summary.json")
            summary = suite.validate_all_data(output_file)
            
            # Print summary
            print("\nValidation summary for all data:")
            print(f"Words: {summary['valid_words']}/{summary['total_words']} valid ({summary['valid_words_percent']}%)")
            print(f"Translations: {summary['valid_translations']}/{summary['total_translations']} valid ({summary['valid_translations_percent']}%)")
            print(f"Examples: {summary['valid_examples']}/{summary['total_examples']} valid ({summary['valid_examples_percent']}%)")
            print(f"Overall quality score: {summary['overall_quality_score']}/100")
            print(f"Detailed report saved to: {output_file}")
            
        elif args.fix_data:
            output_path = args.output or suite.fixed_data_dir
            fixes = suite.fix_data(args.fix_data, output_path)
            
            # Print summary
            print("\nFix summary:")
            print(f"Files processed: {fixes['total_files']}")
            print(f"Words fixed: {fixes['words_fixed']}")
            print(f"Translations fixed: {fixes['translations_fixed']}")
            print(f"Examples fixed: {fixes['examples_fixed']}")
            print(f"Fixed data saved to: {output_path}")
    
    except Exception as e:
        logger.error(f"Error in quality suite: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main()) 