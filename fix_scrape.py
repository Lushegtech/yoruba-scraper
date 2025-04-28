#!/usr/bin/env python3
import re
import os
import sys

def fix_scrape_py():
    """Add the missing _fix_yoruba_spacing and _fix_english_spacing methods to the second instance 
    of the ExampleSentenceExtractor class."""
    
    # Read the file
    with open('scrape.py', 'r', encoding='utf-8') as file:
        content = file.read()
        
    # Find the second instance of ExampleSentenceExtractor 
    class_instances = list(re.finditer(r'class ExampleSentenceExtractor:', content))
    
    if len(class_instances) < 2:
        print(f"Expected at least 2 instances of ExampleSentenceExtractor, found {len(class_instances)}")
        return False
        
    # Get the second instance
    second_class_pos = class_instances[1].start()
    print(f"Found second ExampleSentenceExtractor at position {second_class_pos}")
    
    # Find the insert point - right before clean_example_text method in the second instance
    # Find all clean_example_text methods
    clean_methods = list(re.finditer(r'def clean_example_text\(self, text\):', content))
    
    # Find the one that's in the second class instance
    target_clean_method = None
    for method in clean_methods:
        if method.start() > second_class_pos and method.start() < second_class_pos + 5000:  # Reasonable distance
            target_clean_method = method
            break
            
    if not target_clean_method:
        print("Could not find clean_example_text method in the second ExampleSentenceExtractor")
        return False
        
    insert_pos = target_clean_method.start()
    print(f"Found insertion point at position {insert_pos}")
    
    # Check for existing methods to avoid duplication
    class_content_start = second_class_pos
    class_content_end = min(insert_pos + 5000, len(content))
    class_content = content[class_content_start:class_content_end]
    
    if "_fix_yoruba_spacing" in class_content:
        print("_fix_yoruba_spacing method already exists in the second class")
        return False
    
    # Get the indentation level from the clean_example_text method
    method_line = content[content.rfind('\n', 0, insert_pos) + 1:insert_pos].rstrip()
    indentation = ''
    for char in method_line:
        if char in ' \t':
            indentation += char
        else:
            break
    
    # Create the methods to insert with proper indentation
    methods_to_add = f"""
{indentation}def _fix_yoruba_spacing(self, text):
{indentation}    \"\"\"Fix spacing issues in Yoruba text.\"\"\"
{indentation}    if not isinstance(text, str):
{indentation}        return text
{indentation}    
{indentation}    # Fix common patterns where 'à' is joined to subsequent word
{indentation}    text = re.sub(r'(^|\\s|\\(|")à(?=[a-zàáèéìíòóùúẹọṣ])', r'\\1à ', text)
{indentation}    
{indentation}    # Fix specific starting pattern for "À bá"
{indentation}    text = re.sub(r'(?:^|\\s)À(?:bá|ba)ti', r'À bá ti', text)
{indentation}    
{indentation}    # Fix auxiliary verb spacing issues
{indentation}    text = re.sub(r'([áàńḿ])([a-zàáèéìíòóùúẹọṣ][a-zàáèéìíòóùúẹọṣ]+)', r'\\1 \\2', text)
{indentation}    
{indentation}    # Fix specific particles and pronouns that are commonly misjoined
{indentation}    text = re.sub(r'(wọ́n|won|kí|ki|tó|to|ìyẹn|iyen|yìí|yii|èyí|eyi|bàá|baa)([a-zàáèéìíòóùúẹọṣ][a-zàáèéìíòóùúẹọṣ]+)', r'\\1 \\2', text)
{indentation}    
{indentation}    # Fix cases where 'á' follows a word and should be separated
{indentation}    text = re.sub(r'([a-zàáèéìíòóùúẹọṣ][a-zàáèéìíòóùúẹọṣ]+)(á[a-zàáèéìíòóùúẹọṣ])', r'\\1 \\2', text)
{indentation}    
{indentation}    # Fix specific verb combinations (ti + something)
{indentation}    text = re.sub(r'(ti)(tu|yan|fi|lo|gbà|pa|mọ̀)', r'\\1 \\2', text)
{indentation}    
{indentation}    # Fix 'bá' plus following word
{indentation}    text = re.sub(r'(bá)(ti|pa|fi|gbà|jẹ́|ṣe)', r'\\1 \\2', text)
{indentation}    
{indentation}    # Fix final spacing issues
{indentation}    text = re.sub(r'\\s+', ' ', text).strip()
{indentation}    
{indentation}    return text
{indentation}
{indentation}def _fix_english_spacing(self, text):
{indentation}    \"\"\"Fix spacing issues in English text.\"\"\"
{indentation}    if not isinstance(text, str):
{indentation}        return text
{indentation}        
{indentation}    # Add spaces between lowercase and uppercase letters (except for known acronyms)
{indentation}    text = re.sub(r'([a-z])([A-Z])', r'\\1 \\2', text)
{indentation}    
{indentation}    # Fix specific patterns we've observed in the data
{indentation}    text = re.sub(r'beenput(to)?death', r'been put to death', text)
{indentation}    text = re.sub(r'putto(death)', r'put to \\1', text)
{indentation}    text = re.sub(r'beenputto', r'been put to', text)
{indentation}    
{indentation}    # Fix joined "been" + verb
{indentation}    past_participlesAfterBeen = ["released", "put", "used", "confined", "blessed", "left"]
{indentation}    for pp in past_participlesAfterBeen:
{indentation}        text = text.replace(f"been{{pp}}", f"been {{pp}}")
{indentation}    
{indentation}    # Fix main verb + preposition/conjunction
{indentation}    main_verbs = ["released", "explained", "provided", "put", "had", "made"]
{indentation}    prepositions = ["if", "when", "as", "by", "to", "for", "with"]
{indentation}    for verb in main_verbs:
{indentation}        for prep in prepositions:
{indentation}            text = text.replace(f"{{verb}}{{prep}}", f"{{verb}} {{prep}}")
{indentation}    
{indentation}    # Fix final spacing issues
{indentation}    text = re.sub(r'\\s+', ' ', text).strip()
{indentation}    
{indentation}    return text
"""

    # Create a backup of the original file
    backup_file = 'scrape.py.bak'
    if not os.path.exists(backup_file):
        with open(backup_file, 'w', encoding='utf-8') as file:
            file.write(content)
        print(f"Created backup at {backup_file}")
    
    # Insert the methods before the clean_example_text method
    new_content = content[:insert_pos] + methods_to_add + content[insert_pos:]
    
    # Update the file
    with open('scrape.py', 'w', encoding='utf-8') as file:
        file.write(new_content)
    
    print("Successfully added methods to ExampleSentenceExtractor")
    return True

if __name__ == "__main__":
    fix_scrape_py() 