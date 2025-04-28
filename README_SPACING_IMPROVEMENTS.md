# Yoruba Text Spacing Improvements

## Overview

This document describes the improvements made to the Yoruba scraper to handle spacing issues in both Yoruba and English text. Previously, spacing issues were corrected in a post-processing step, but now these fixes are integrated directly into the core scraping process.

## The Problem

Yoruba text extracted from web sources often contained incorrect spacing, particularly around:

1. **Auxiliary verbs** like "á", "à", "ń" incorrectly joined to adjacent words
2. **Particles and pronouns** like "wọ́n", "kí", "tó" incorrectly joined to following words
3. **Common word patterns** with missing spaces between elements

Similarly, English translations had spacing issues:

1. **Auxiliary verbs + past participles** (e.g., "couldhave" instead of "could have")
2. **Determiners + nouns** (e.g., "Thisman" instead of "This man")
3. **Words joined with no space** after punctuation

These spacing issues reduced readability and made the data less useful for language learning applications.

## The Solution

### Integrated Approach

We've improved the scraper by:

1. **Language Detection**: Automatically detecting whether text is Yoruba or English based on diacritics
2. **Language-Specific Fixes**: Applying different spacing rules for Yoruba and English text
3. **Integration Into Core Process**: Moving fixes from post-processing to the initial extraction phase

### Key Improvements

#### 1. Yoruba Text Handling

```python
# Fix Yoruba auxiliary verb spacing issues (á, à, ń, etc.)
text = re.sub(r'([áàńḿ])([a-zàáèéìíòóùúẹọṣ][a-zàáèéìíòóùúẹọṣ]+)', r'\1 \2', text)
text = re.sub(r'(wọ́n|won|kí|ki|tó|to|ìyẹn|iyen|yìí|yii|èyí|eyi|bàá|baa)([a-zàáèéìíòóùúẹọṣ][a-zàáèéìíòóùúẹọṣ]+)', r'\1 \2', text)
text = re.sub(r'([a-zàáèéìíòóùúẹọṣ][a-zàáèéìíòóùúẹọṣ]+)(á[a-zàáèéìíòóùúẹọṣ])', r'\1 \2', text)

# Fix specific Yoruba patterns that need spaces
text = re.sub(r'(ti)(tu|yan|fi|lo|gbà|pa|mọ̀)', r'\1 \2', text)
text = re.sub(r'(bá)(ti|pa|fi|gbà|jẹ́|ṣe)', r'\1 \2', text)
text = re.sub(r'(ká)(ní|sì|ti)', r'\1 \2', text)
text = re.sub(r'(kò)(ké|ní|fi|sì)', r'\1 \2', text)

# Fix common incorrect word formations
text = re.sub(r'nià', r'ni à', text)
text = re.sub(r'láti', r'lá ti', text)
text = re.sub(r'síbẹ̀', r'sí bẹ̀', text)
```

#### 2. English Text Handling

```python
# Fix common joined words in English translations
text = re.sub(r'([a-z])([A-Z])', r'\1 \2', text)

# Fix specific auxiliary verb + past participle combinations
auxiliaries = ["could", "would", "should", "have", "has", "had", "will", "is", "are", "was", "were"]
past_participles = ["been", "have", "had", "not", "find", "look", "want", "need", "make", "take", "give"]
for aux in auxiliaries:
    for pp in past_participles:
        text = text.replace(f"{aux}{pp}", f"{aux} {pp}")

# Fix "many of mankind's" type constructions
text = text.replace("manyof", "many of")
text = text.replace("mankind'smistakes", "mankind's mistakes")
text = text.replace("ofmankind", "of mankind")
```

## Implementation Details

We've updated three key methods in the codebase:

1. **ExampleSentenceExtractor.clean_example_text**: Used during the initial HTML extraction phase
2. **DataVerifier.clean_example_text**: Used during verification of extracted examples
3. **GlosbeYorubaScraper.clean_example_text**: Main implementation with all advanced fixes

The improvements are now fully integrated into the core scraping process, meaning:

- No need to run separate spacing-fix scripts after scraping
- Better data quality immediately upon extraction
- Consistent spacing across the entire dataset

## Migration

For existing datasets scraped before these improvements, you can still run the fix_spacing_in_existing_csv method:

```python
scraper = GlosbeYorubaScraper()
scraper.fix_spacing_in_existing_csv()
```

## Benefits

These improvements:

1. **Enhance readability** of both Yoruba and English text
2. **Improve grammatical accuracy** by correctly separating grammatical elements
3. **Increase data quality** at the point of extraction
4. **Reduce post-processing** needs

By incorporating these fixes directly into the core scraping process, we ensure consistent, high-quality text spacing throughout the entire dataset from the moment it's extracted. 