#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import re
import logging
import types

# Set up our own fix_yoruba_spacing and fix_english_spacing functions
def _fix_yoruba_spacing_impl(self, text):
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
    
    # Fix final spacing issues
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text

def _fix_english_spacing_impl(self, text):
    """Fix spacing issues in English text."""
    if not isinstance(text, str):
        return text
        
    # Add spaces between lowercase and uppercase letters (except for known acronyms)
    text = re.sub(r'([a-z])([A-Z])', r'\1 \2', text)
    
    # Fix specific patterns we've observed in the data
    text = re.sub(r'beenput(to)?death', r'been put to death', text)
    text = re.sub(r'putto(death)', r'put to \1', text)
    text = re.sub(r'beenputto', r'been put to', text)
    
    # Fix joined "been" + verb
    past_participlesAfterBeen = ["released", "put", "used", "confined", "blessed", "left"]
    for pp in past_participlesAfterBeen:
        text = text.replace(f"been{pp}", f"been {pp}")
    
    # Fix final spacing issues
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text

# The main processing function (optional)
def process_text(text):
    """Process and fix text spacing issues"""
    has_yoruba_diacritics = bool(re.search(r'[àáèéìíòóùúẹọṣ]', text))
    
    if has_yoruba_diacritics:
        # Apply Yoruba-specific fixes
        return _fix_yoruba_spacing_impl(None, text)
    else:
        # Apply English-specific fixes
        return _fix_english_spacing_impl(None, text)

# Monkey patch the ExampleSentenceExtractor class
def monkey_patch_extractor():
    """Add the fix_yoruba_spacing and fix_english_spacing methods to the ExampleSentenceExtractor class"""
    from scrape import ExampleSentenceExtractor
    
    # Check if the methods already exist
    if not hasattr(ExampleSentenceExtractor, '_fix_yoruba_spacing'):
        ExampleSentenceExtractor._fix_yoruba_spacing = _fix_yoruba_spacing_impl
        print("Added _fix_yoruba_spacing method to ExampleSentenceExtractor")
    
    if not hasattr(ExampleSentenceExtractor, '_fix_english_spacing'):
        ExampleSentenceExtractor._fix_english_spacing = _fix_english_spacing_impl
        print("Added _fix_english_spacing method to ExampleSentenceExtractor")
    
    return True

# Main function
if __name__ == "__main__":
    # Add our spacing fix methods to the ExampleSentenceExtractor class
    monkey_patch_extractor()
    
    # Now import and run the scraper
    from scrape import GlosbeYorubaScraper
    
    # Create a scraper instance
    scraper = GlosbeYorubaScraper()
    
    # Run the scraper
    scraper.run() 