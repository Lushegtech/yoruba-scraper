# Yoruba Scraper - Improved Translation Extractor

This repository contains scripts to improve the translation extraction process for the Yoruba-English dictionary scraper. The main focus is on correctly parsing debug HTML files to extract accurate translations.

## Background

The original scraper was saving HTML files in debug mode, but the translation extraction mechanism wasn't properly extracting all translations from these saved files. This improved extractor addresses that issue, ensuring that all available translations are correctly captured.

## Key Improvements

1. Prioritizes translations found in the page summary section (generally the most accurate)
2. Better captures part-of-speech information associated with translations
3. Improves extraction from multiple HTML elements including:
   - Direct translation elements (`h3.translation__item__pharse`)
   - List items with translation markup (`li[data-element="translation"]`)
   - Similar phrases section (`#simmilar-phrases`)
   - Automatic translations section (`#translation_automatic`)
4. Adds confidence levels to help prioritize the most accurate translations
5. Updates the existing scraped data with improved translations

## Files

- `improved_translation_extractor.py` - The main script providing enhanced translation extraction
- `test_improved_extractor.py` - A test script to verify extraction works correctly
- `verify_fix.py` - The original verification script (for reference)

## Usage

### Running the Improved Extractor

To process all debug HTML files and generate improved translations:

```bash
python improved_translation_extractor.py
```

This will:
1. Read all HTML files in the `scraped_data/debug_html/` directory
2. Extract translations using the improved method
3. Save results to `scraped_data/improved_translations.json`
4. Update the main scraped data file with the improved translations

### Testing a Specific Word

To test the extraction specifically for the word "adìye" (chicken):

```bash
python test_improved_extractor.py
```

This will show details about what elements are found in the HTML and what translations are extracted.

## Advanced Usage

You can also use the functions directly in your code:

```python
from improved_translation_extractor import process_debug_html_files, update_scraper_with_improved_translations

# Process debug files with custom paths
results = process_debug_html_files(
    input_dir="./custom_data_dir",
    output_file="./custom_output.json"
)

# Update scraped data with improved translations
update_scraper_with_improved_translations(
    translation_file="./improved_translations.json",
    output_folder="./updated_data.json"
)
```

## Future Improvements

Potential enhancements for the future:
- Integration with the main scraper to use this improved method in real-time
- Support for batch processing of specific words only
- Ability to merge results from multiple sources
- Web interface for manually reviewing and editing translations 