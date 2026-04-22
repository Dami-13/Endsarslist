import json
import os
import logging
from datetime import datetime
from pathlib import Path
from supabase import create_client

SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

Path("logs").mkdir(exist_ok=True)
logging.basicConfig(
    filename="logs/error.log",
    level=logging.ERROR,
    format="%(asctime)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S"
)

def dump_json(data, filename="data/endsars_list.json"):
    Path("data").mkdir(exist_ok=True)
    with open(filename, 'w') as f:
        json.dump(data, f, indent=2)

def run_scraper():
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    victims_list = scrape_data()  # Assume this exists

    try:
        supabase.table('victims').insert(victims_list).execute()
    except Exception as e:
        source_url = getattr(e, 'url', 'unknown')
        logging.error("Supabase insert failed | source: %s | error: %s", source_url, str(e))
    finally:
        dump_json(victims_list)

if __name__ == "__main__":
    run_scraper()
