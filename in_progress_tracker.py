"""
Helper module for tracking in-progress and completed words in the Yoruba scraper.
This module helps the scraper resume from where it left off if interrupted.
"""

import os
import logging

class WordTracker:
    """Tracks which words have been processed and which are in-progress."""
    
    def __init__(self, output_folder="./scraped_data"):
        self.output_folder = output_folder
        
        # Track processed words to avoid duplicates
        self.processed_words_file = os.path.join(self.output_folder, "processed_words.txt")
        self.processed_words = set()
        
        # Create an in-progress words file to track words being currently processed
        self.in_progress_file = os.path.join(self.output_folder, "in_progress_words.txt")
        self.in_progress_words = set()
        
        # Load existing state if available
        self._load_processed_words()
        self._load_in_progress_words()
        
        # Ensure the output directory exists
        os.makedirs(self.output_folder, exist_ok=True)
    
    def _load_processed_words(self):
        """Load the list of processed words from the file."""
        if os.path.exists(self.processed_words_file):
            try:
                with open(self.processed_words_file, 'r', encoding='utf-8') as f:
                    self.processed_words = set(line.strip() for line in f if line.strip())
                logging.info(f"Loaded {len(self.processed_words)} processed words from {self.processed_words_file}")
            except Exception as e:
                logging.error(f"Error loading processed words: {e}")
                self.processed_words = set()
    
    def _load_in_progress_words(self):
        """Load the list of in-progress words from the file."""
        if os.path.exists(self.in_progress_file):
            try:
                with open(self.in_progress_file, 'r', encoding='utf-8') as f:
                    self.in_progress_words = set(line.strip() for line in f if line.strip())
                logging.info(f"Loaded {len(self.in_progress_words)} in-progress words from {self.in_progress_file}")
            except Exception as e:
                logging.error(f"Error loading in-progress words: {e}")
                self.in_progress_words = set()
    
    def _save_processed_words(self):
        """Save the current set of processed words to the file."""
        try:
            with open(self.processed_words_file, 'w', encoding='utf-8') as f:
                for word in sorted(self.processed_words):
                    f.write(f"{word}\n")
        except Exception as e:
            logging.error(f"Error saving processed words: {e}")
    
    def _save_in_progress_words(self):
        """Save the current set of in-progress words to the file."""
        try:
            with open(self.in_progress_file, 'w', encoding='utf-8') as f:
                for word in sorted(self.in_progress_words):
                    f.write(f"{word}\n")
        except Exception as e:
            logging.error(f"Error saving in-progress words: {e}")
    
    def is_processed(self, word):
        """Check if a word has already been processed."""
        return word in self.processed_words
    
    def is_in_progress(self, word):
        """Check if a word is currently marked as in-progress."""
        return word in self.in_progress_words
    
    def mark_processed(self, word):
        """Mark a word as processed and remove it from in-progress."""
        if word not in self.processed_words:
            self.processed_words.add(word)
            self._save_processed_words()
        
        # Remove from in-progress if it was there
        if word in self.in_progress_words:
            self.in_progress_words.remove(word)
            self._save_in_progress_words()
    
    def mark_in_progress(self, word):
        """Mark a word as in-progress if it's not already processed."""
        if word not in self.processed_words and word not in self.in_progress_words:
            self.in_progress_words.add(word)
            self._save_in_progress_words()
    
    def clean_up(self):
        """Clean up the in-progress file after successful completion."""
        if self.in_progress_words:
            self.in_progress_words.clear()
            self._save_in_progress_words()
            logging.info(f"Cleared in-progress words file {self.in_progress_file}")
    
    def get_all_tracked_words(self):
        """Get all words that are either processed or in-progress."""
        return self.processed_words.union(self.in_progress_words)
    
    def get_stats(self):
        """Get statistics about processed and in-progress words."""
        return {
            "processed": len(self.processed_words),
            "in_progress": len(self.in_progress_words),
            "total_tracked": len(self.get_all_tracked_words())
        } 