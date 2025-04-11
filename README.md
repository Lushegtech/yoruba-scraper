# Yoruba Dictionary Scraper

This script scrapes Yoruba words and their translations from Glosbe.com, including:
- Word
- Primary translation
- Pronunciation
- Part of speech
- Meanings/definitions
- Example sentences
- URLs
- Scrape time
- Status and error information

## Setup

1. Install Python 3.8 or higher
2. Install required packages:
```bash
pip install -r requirements.txt
```

3. Create the following folder structure:
```
yoruba_words/
  ├── a/
  │   └── words.txt
  ├── b/
  │   └── words.txt
  └── ... (other alphabet folders)
```

4. Add Yoruba words to the text files in each alphabet folder, one word per line.

## Usage

Run the scraper:
```bash
python scrape.py
```

The script will:
- Create necessary output folders
- Process words from each alphabet folder
- Save results in JSON and CSV formats
- Generate a combined CSV file
- Create a SQL initialization file

## Output Structure

```
scraped_data/
  ├── json/
  │   └── [alphabet]/
  │       └── [word_file].json
  ├── csv/
  │   └── [alphabet]/
  │       └── [word_file].csv
  ├── debug_html/
  │   └── [word]_debug.html
  ├── all_yoruba_words.csv
  ├── init_database.sql
  └── processed_words.txt
```

## Features

- Handles CAPTCHA detection and retries
- Uses random delays and user agents to avoid blocking
- Tracks processed words to avoid duplicates
- Saves debug HTML for troubleshooting
- Generates SQL schema for database setup
- Progress bar for monitoring scraping progress
- Error handling and logging 