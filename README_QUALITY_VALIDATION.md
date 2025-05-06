# Yoruba Scraper Quality Validation System

This suite provides comprehensive quality validation tools for ensuring the highest accuracy and authenticity in Yoruba language data scraping. The validation framework includes specialized components for validating Yoruba words, translations, and example sentences.

## Overview

The quality validation system consists of several components:

1. **Yoruba Language Validator** - Validates Yoruba words against orthographic rules, proper diacritics usage, and reference word lists
2. **Enhanced Translation Validator** - Validates Yoruba-English translations for accuracy and authenticity 
3. **Example Sentence Validator** - Validates example sentences for naturalness, correctness, and proper translation
4. **Quality Suite** - Coordinates all validators in a unified workflow

## Installation

### Prerequisites

- Python 3.7+
- Required packages:
  - pandas
  - tqdm
  - numpy
  - difflib

### Setup

```bash
# Clone the repository
git clone https://github.com/yourusername/yoruba-scraper.git
cd yoruba-scraper

# Install required packages
pip install -r requirements.txt

# Run validation on your data
python yoruba_scraper_quality_suite.py --validate-all
```

## Usage

### Quality Suite (All-In-One)

The main script `yoruba_scraper_quality_suite.py` provides a unified interface to access all validation tools:

```bash
# Validate a specific word
python yoruba_scraper_quality_suite.py --validate-word "àdúrà"

# Validate all scraped data
python yoruba_scraper_quality_suite.py --validate-all

# Fix quality issues in data
python yoruba_scraper_quality_suite.py --fix-data ./scraped_data/
```

### Individual Validators

You can also use each validator component separately:

#### Word Validation

```bash
# Validate a Yoruba word
python yoruba_language_validator.py --word "àdúrà"

# Fix spacing issues in a word
python yoruba_language_validator.py --fix-spacing "nià"
```

#### Translation Validation

```bash
# Check a specific translation
python enhanced_translation_validator.py --check "àdúrà" "prayer"

# Validate translations from a file
python enhanced_translation_validator.py --file translations.json --output validation_report.json
```

#### Example Sentence Validation

```bash
# Check an example sentence pair
python example_sentence_validator.py --check "Àdúrà mi ni pé kí ọlọ́run bùkún ẹ." "My prayer is that God blesses you."

# Validate and fix examples in a file
python example_sentence_validator.py --file examples.json --fix
```

## Validation Rules

### Word Validation

- Checks for proper orthography including correct use of diacritics
- Validates against reference word lists
- Detects and fixes common typographical errors
- Ensures proper spacing between syllables

### Translation Validation

- Checks translations against reference dictionaries
- Detects context-specific translations for ambiguous words
- Ensures translations are not contaminated with metadata or markup
- Scores translations based on confidence level

### Example Sentence Validation

- Ensures natural, idiomatic Yoruba language usage
- Verifies proper translation of examples
- Detects machine-generated or unnatural examples
- Checks grammatical structure and syntax patterns

## Quality Scores

The validation system provides confidence scores from 0-1 for each validation:

- **0.0-0.5**: Invalid or highly suspicious
- **0.5-0.7**: Suspicious but potentially valid
- **0.7-0.9**: Valid with minor issues
- **0.9-1.0**: Highly valid

The overall quality score for each entry is the weighted average of:
- 30% Word validation
- 40% Translation validation
- 30% Example validation

## Output Files

The validation system produces structured output files:

- `validation_results/validation_summary.json` - Overall validation report
- `validation_results/suspicious_entries.json` - Entries requiring manual review
- `fixed_data/` - Directory containing fixed versions of problematic data

## Custom Reference Data

You can provide custom reference data to improve validation:

```bash
python yoruba_scraper_quality_suite.py --validate-all --reference-dir ./my_reference_data
```

## Contributing

Contributions to improve the validation system are welcome! Some areas that could be enhanced:

1. Additional Yoruba grammatical rule checks
2. Extended dictionaries of known words and translations
3. Advanced machine learning based detection of suspicious entries

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgements

This validation system builds upon research and resources from:
- [Yoruba Language Studies](https://yoruba.osu.edu/)
- [Yoruba Dictionary](https://www.yoruba.fi/dictionary)
- [Yoruba Orthography Guidelines](https://www.yorubalanguage.org/) 