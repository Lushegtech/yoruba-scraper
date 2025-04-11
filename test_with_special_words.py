from scrape import GlosbeYorubaScraper
import os
import json

def main():
    # Define paths
    base_folder = "./test_output"
    
    # Make sure the test folder exists
    os.makedirs(base_folder, exist_ok=True)
    
    # Initialize the scraper
    scraper = GlosbeYorubaScraper(
        base_folder=base_folder,
        delay=2.0  # Short delay for testing
    )
    
    # Test with specific Yoruba words that need special handling
    test_words = ['á', 'a', 'à bá ti', 'bawo', 'e', 'gbogbo']
    
    print("Testing improved translation extraction...")
    print("-" * 50)
    
    for word in test_words:
        print(f"\nScraping word: '{word}'")
        result = scraper.scrape_word(word)
        
        if result:
            # Format the translations nicely
            primary = result.get("translation", "")
            all_translations = result.get("translations", [])
            pos = result.get("part_of_speech", "")
            
            print(f"Primary translation: '{primary}'")
            print(f"Part of speech: {pos}")
            
            if all_translations:
                print("All translations:")
                for i, trans in enumerate(all_translations[:5], 1):  # Show max 5
                    print(f"  {i}. {trans}")
                
                if len(all_translations) > 5:
                    print(f"  ... ({len(all_translations)-5} more)")
            
            # Show examples
            examples = result.get("examples", [])
            if examples:
                print("Example:")
                example = examples[0]
                print(f"  Yoruba: {example.get('yoruba', '')[:100]}")
                print(f"  English: {example.get('english', '')[:100]}")
            
            # Show the flattened data
            flattened = scraper.extract_flattened_data(result)
            print("\nFlattened data (for CSV/database):")
            print(f"  Translation: '{flattened['translation']}'")
            print(f"  All translations: '{flattened['all_translations']}'")
            print(f"  Part of speech: '{flattened['part_of_speech']}'")
            
            print("-" * 50)
        else:
            print(f"No result for '{word}'")
            print("-" * 50)

if __name__ == "__main__":
    main() 