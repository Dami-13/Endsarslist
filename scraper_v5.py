#!/usr/bin/env python3
"""
EndSARSList â€” Scraper v5
=========================
Uses Google News RSS as the article discovery layer instead of
per-site search endpoints (which were broken/blocked).

Google News RSS is free, requires no API key, and works reliably.
Each query returns ~10 recent articles from across all Nigerian news sources.

Two modes:
  python scraper_v5.py            # daily mode â€” last 2 days
  python scraper_v5.py --backfill # backfill mode â€” last 10 years

Setup:
  pip install requests beautifulsoup4 supabase python-dotenv anthropic

Env vars:
  SUPABASE_URL
  SUPABASE_SERVICE_KEY
  ANTHROPIC_API_KEY
"""

import os, re, time, logging, sys, argparse, json
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Optional, List
from urllib.parse import quote_plus
from email.utils import parsedate_to_datetime

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import anthropic

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# â”€â”€ Clients â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
API_BASE_URL = os.environ.get("API_BASE_URL", "https://endsarslist-api.damidude.workers.dev").rstrip("/")
ADMIN_API_TOKEN = (
    os.environ.get("D1_ADMIN_TOKEN")
    or os.environ.get("SUPABASE_SERVICE_KEY")
    or os.environ.get("SUPABASE_KEY")
)
claude = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

# â”€â”€ Config â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
BACKFILL_YEARS  = 10
DAILY_DAYS      = 2
REQUEST_DELAY   = 1.5
TIMEOUT         = 15
CLAUDE_MODEL    = "claude-haiku-4-5-20251001"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

API_HEADERS = {
    "Content-Type": "application/json",
    "x-admin-token": ADMIN_API_TOKEN or "",
}

# â”€â”€ Google News search queries â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Each query targets Nigerian news specifically.
# Google News RSS returns ~10 results per query, refreshed every few hours.
SEARCH_QUERIES = [
    # â”€â”€ General Nigeria â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "arrested Nigeria",
    "detained Nigeria",
    "missing person Nigeria",
    "abducted Nigeria",
    "kidnapped Nigeria",
    "remanded custody Nigeria",
    "DSS custody Nigeria",
    "disappeared Nigeria",
    "has been missing Nigeria",
    "taken into custody Nigeria",
    "EndSARS arrested",
    "EndSARS missing",
    "EndSARS detained",
    "protester arrested Nigeria",
    "activist detained Nigeria",
    "journalist arrested Nigeria",
    "police arrest Nigeria",
    "arrested Nigeria human rights",
    "taken by soldiers Nigeria",
    "taken by police Nigeria",

    # â”€â”€ Lagos â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "taken Lagos",
    "missing person Lagos",
    "arrested Lagos",
    "kidnapped Lagos",
    "detained Lagos",
    "disappeared Lagos",

    # â”€â”€ Kano â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "taken Kano",
    "missing person Kano",
    "arrested Kano",
    "kidnapped Kano",
    "detained Kano",

    # â”€â”€ Ibadan â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "taken Ibadan",
    "missing person Ibadan",
    "arrested Ibadan",
    "kidnapped Ibadan",

    # â”€â”€ Abuja â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "taken Abuja",
    "missing person Abuja",
    "arrested Abuja",
    "detained Abuja FCT",
    "kidnapped Abuja",

    # â”€â”€ Port Harcourt â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "taken Port Harcourt",
    "missing person Port Harcourt",
    "arrested Port Harcourt",
    "kidnapped Port Harcourt",

    # â”€â”€ Benin City â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "taken Benin City",
    "missing person Benin City",
    "arrested Benin City",
    "kidnapped Benin City",

    # â”€â”€ Kaduna â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "taken Kaduna",
    "missing person Kaduna",
    "arrested Kaduna",
    "kidnapped Kaduna",

    # â”€â”€ Enugu â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "taken Enugu",
    "missing person Enugu",
    "arrested Enugu",
    "kidnapped Enugu",

    # â”€â”€ Aba â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "taken Aba Nigeria",
    "missing person Aba Nigeria",
    "arrested Aba Nigeria",
    "kidnapped Aba Nigeria",

    # â”€â”€ Maiduguri â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "taken Maiduguri",
    "missing person Maiduguri",
    "arrested Maiduguri",
    "kidnapped Maiduguri",

    # â”€â”€ States â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "arrested Rivers State",
    "kidnapped Rivers State",
    "missing Rivers State",
    "arrested Delta State",
    "kidnapped Delta State",
    "arrested Ogun State",
    "missing Ogun State",
    "arrested Oyo State",
    "missing Oyo State",
    "kidnapped Oyo State",
    "arrested Anambra",
    "missing Anambra",
    "kidnapped Anambra",
    "arrested Imo State",
    "missing Imo State",
    "arrested Edo State",
    "missing Edo State",
    "kidnapped Edo State",
    "arrested Borno",
    "missing Borno",
    "kidnapped Borno",
    "arrested Zamfara",
    "kidnapped Zamfara",
    "missing Zamfara",
    "arrested Sokoto",
    "kidnapped Sokoto",
    "arrested Kebbi",
    "kidnapped Kebbi",
    "arrested Katsina",
    "kidnapped Katsina",
    "missing Katsina",
    "arrested Bauchi",
    "kidnapped Bauchi",
    "arrested Gombe",
    "missing Gombe",
    "arrested Adamawa",
    "missing Adamawa",
    "arrested Taraba",
    "kidnapped Taraba",
    "arrested Benue",
    "kidnapped Benue",
    "missing Benue",
    "arrested Plateau",
    "kidnapped Plateau",
    "missing Plateau",
    "arrested Nasarawa",
    "arrested Niger State",
    "kidnapped Niger State",
    "arrested Kwara",
    "missing Kwara",
    "arrested Ekiti",
    "arrested Ondo State",
    "missing Ondo State",
    "arrested Osun",
    "missing Osun",
    "arrested Cross River",
    "missing Cross River",
    "arrested Akwa Ibom",
    "missing Akwa Ibom",
    "arrested Bayelsa",
    "missing Bayelsa",
    "kidnapped Bayelsa",
    "arrested Ebonyi",
    "missing Ebonyi",
    "arrested Abia",
    "missing Abia",
    "arrested Jigawa",
    "missing Jigawa",
    "arrested Yobe",
    "missing Yobe",
]

# Trusted Nigerian news domains â€” articles from other domains are skipped
TRUSTED_DOMAINS = {
    "saharareporters.com",
    "punchng.com",
    "vanguardngr.com",
    "premiumtimesng.com",
    "guardian.ng",
    "dailytrust.com",
    "thecable.ng",
    "thisdaylive.com",
    "dailypost.ng",
    "thenationonlineng.net",
    "legit.ng",
    "ripplesnigeria.com",
    "thewhistler.ng",
    "humanglemedia.com",
    "gazettengr.com",
    "fij.ng",
    "leadership.ng",
    "tribuneonlineng.com",
    "channelstv.com",
    "businessday.ng",
    "sunnewsonline.com",
    "ngrguardiannews.com",
    "naijanews.com",
    "informationng.com",
    "nairaland.com",
    "bbc.com",
    "aljazeera.com",
    "reuters.com",
    "apnews.com",
}

# â”€â”€ Data model â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
@dataclass
class ScrapedPerson:
    full_name: str
    source_url: str
    source_name: str
    record_type: str
    circumstances: str = ""
    last_seen_location: str = ""
    state: str = ""
    age: Optional[int] = None
    gender: str = "unknown"
    article_date: Optional[str] = None
    photo_url: str = ""
    charges: str = ""
    holding_location: str = ""
    arresting_authority: str = ""


# â”€â”€ HTTP helper â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def fetch(url: str) -> Optional[BeautifulSoup]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        return BeautifulSoup(r.text, "html.parser")
    except Exception as e:
        log.debug(f"fetch failed {url}: {e}")
        return None


def fetch_xml(url: str) -> Optional[BeautifulSoup]:
    """Fetch and parse XML/RSS feed."""
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        return BeautifulSoup(r.text, "xml")
    except Exception as e:
        log.debug(f"fetch_xml failed {url}: {e}")
        return None


# â”€â”€ Google News RSS â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def get_domain(url: str) -> str:
    """Extract domain from URL."""
    m = re.search(r'https?://(?:www\.)?([^/]+)', url)
    return m.group(1).lower() if m else ""


def google_news_urls(query: str, cutoff: datetime) -> List[tuple]:
    """
    Query Google News RSS for a search term.
    Returns list of (url, pub_date, source_name) tuples.
    Google News RSS: https://news.google.com/rss/search?q=QUERY&hl=en-NG&gl=NG&ceid=NG:en
    """
    rss_url = (
        f"https://news.google.com/rss/search?"
        f"q={quote_plus(query)}"
        f"&hl=en-NG&gl=NG&ceid=NG:en"
    )

    soup = fetch_xml(rss_url)
    if not soup:
        return []

    results = []
    for item in soup.find_all("item"):
        # In Google News RSS, <link> is a self-closing tag in XML mode.
        # The actual URL is stored as the text of <guid> or as the
        # NavigableString sibling of <link>. Try multiple approaches.
        url = ""

        # Approach 1: guid contains the Google News article URL
        guid = item.find("guid")
        if guid:
            url = guid.get_text().strip()

        # Approach 2: link tag text (works in some parsers)
        if not url:
            link = item.find("link")
            if link and link.string:
                url = link.string.strip()

        # Approach 3: next sibling of <link> tag (BeautifulSoup XML quirk)
        if not url:
            link = item.find("link")
            if link and link.next_sibling:
                candidate = str(link.next_sibling).strip()
                if candidate.startswith("http"):
                    url = candidate

        if not url:
            continue

        # Follow Google redirect to get the real article URL
        if "google.com" in url:
            try:
                r = requests.get(url, headers=HEADERS, timeout=TIMEOUT,
                                allow_redirects=True)
                url = r.url
            except:
                continue

        # Check domain is trusted
        domain = get_domain(url)
        if not any(trusted in domain for trusted in TRUSTED_DOMAINS):
            continue

        # Get publication date
        pub_date_str = ""
        pub_date_tag = item.find("pubDate")
        if pub_date_tag:
            pub_date_str = pub_date_tag.get_text().strip()

        pub_date = None
        if pub_date_str:
            try:
                pub_date = parsedate_to_datetime(pub_date_str).replace(tzinfo=None)
            except:
                pass

        # Check cutoff
        if pub_date and pub_date < cutoff:
            continue

        # Get source name
        source_tag = item.find("source")
        source_name = source_tag.get_text().strip() if source_tag else domain

        results.append((url, pub_date, source_name))

    return results


# â”€â”€ Batch deduplication â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def filter_already_scraped(urls: List[str]) -> List[str]:
    if not urls:
        return []
    try:
        r = requests.get(
            f"{API_BASE_URL}/api/admin/source-urls",
            headers=API_HEADERS,
            timeout=TIMEOUT,
        )
        r.raise_for_status()
        seen = set((r.json() or {}).get("urls", []))
        return [u for u in urls if u not in seen]
    except Exception as e:
        log.warning(f"Dedup check failed: {e}")
        return urls


# â”€â”€ Claude extraction â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
EXTRACT_PROMPT = """You are a data extraction assistant for a Nigerian human rights database tracking victims of police brutality, government repression, and the EndSARS movement.

Given a news article, extract information about people who are:
- Missing (disappeared, abducted, kidnapped, not found, whereabouts unknown)
- Arrested or detained (by police, DSS, military, EFCC, or any authority)

Return ONLY a JSON array. Each element is one person. If no relevant person found, return [].

Fields per person:
- full_name: string (MUST be a real human name with at least 2 words. NOT a job title, organisation, or place)
- record_type: "missing" | "arrested"
- age: number or null
- gender: "male" | "female" | "unknown"
- state: Nigerian state or city name, or ""
- circumstances: 1-2 sentence summary of what happened (max 300 chars)
- last_seen_location: where last seen (for missing persons) or ""
- arresting_authority: e.g. "Police", "DSS", "Army", "EFCC" (for arrested) or ""
- charges: what charged with (for arrested) or ""
- holding_location: where being held (for arrested) or ""

Rules:
- full_name must be a real person's name. REJECT: "Press Secretary", "Central Bank", "The Governor", "Police Officer"
- If only a group is mentioned with no individual names, return []
- Only include people clearly identified as missing or arrested/detained
- Do not invent or assume information not stated in the article

Return raw JSON array only. No markdown, no explanation, no preamble."""


def extract_with_claude(title: str, body: str, url: str) -> List[dict]:
    article_text = f"HEADLINE: {title}\n\nARTICLE:\n{body[:3000]}"
    try:
        msg = claude.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=1000,
            messages=[{"role": "user", "content": EXTRACT_PROMPT + "\n\n" + article_text}]
        )
        raw = msg.content[0].text.strip()
        raw = re.sub(r'^```json\s*', '', raw)
        raw = re.sub(r'\s*```$', '', raw)
        data = json.loads(raw)
        if isinstance(data, list):
            return data
    except json.JSONDecodeError as e:
        log.debug(f"Claude JSON parse error for {url}: {e}")
    except Exception as e:
        log.warning(f"Claude extraction failed for {url}: {e}")
    return []


# â”€â”€ Article scraping â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def scrape_article(url: str, source_name: str, pub_date: Optional[datetime]) -> List[ScrapedPerson]:
    soup = fetch(url)
    if not soup:
        return []

    title = ""
    for t in [soup.find("h1"), soup.find("title")]:
        if t:
            title = t.get_text(" ", strip=True)
            break

    body = ""
    for sel in ["article", ".entry-content", ".post-content", ".article-body",
                ".story-body", ".content", "main"]:
        el = soup.select_one(sel)
        if el:
            body = el.get_text(" ", strip=True)
            break
    if not body:
        body = soup.get_text(" ", strip=True)

    # Quick relevance pre-check before calling Claude
    combined_lower = (title + " " + body[:500]).lower()
    relevance_words = [
        "arrested", "detained", "missing", "abducted", "kidnapped",
        "disappeared", "remanded", "custody", "endsars", "whereabouts",
        "has been missing", "taken by", "taken into custody"
    ]
    if not any(w in combined_lower for w in relevance_words):
        return []

    # Get article date
    article_date = None
    if pub_date:
        article_date = pub_date.strftime("%Y-%m-%d")
    else:
        # Try to extract from page
        for tag in soup.find_all("time"):
            dt = tag.get("datetime") or tag.get_text()
            if dt:
                m = re.search(r'(\d{4}-\d{2}-\d{2})', dt)
                if m:
                    article_date = m.group(1)
                    break
        if not article_date:
            for prop in ["article:published_time", "datePublished"]:
                meta = soup.find("meta", property=prop) or soup.find("meta", attrs={"name": prop})
                if meta and meta.get("content"):
                    m = re.search(r'(\d{4}-\d{2}-\d{2})', meta["content"])
                    if m:
                        article_date = m.group(1)
                        break

    # Get photo
    photo_url = ""
    og = soup.find("meta", property="og:image")
    if og and og.get("content"):
        photo_url = og["content"]

    extracted = extract_with_claude(title, body, url)
    persons = []

    for item in extracted:
        name = (item.get("full_name") or "").strip()
        if not name or len(name.split()) < 2:
            continue
        rtype = item.get("record_type", "")
        if rtype not in ("missing", "arrested"):
            continue

        p = ScrapedPerson(
            full_name=name,
            source_url=url,
            source_name=source_name,
            record_type=rtype,
            circumstances=str(item.get("circumstances", ""))[:300],
            last_seen_location=str(item.get("last_seen_location", ""))[:150],
            state=str(item.get("state", ""))[:50],
            age=item.get("age") if isinstance(item.get("age"), int) else None,
            gender=item.get("gender", "unknown"),
            article_date=article_date,
            photo_url=photo_url,
            charges=str(item.get("charges", ""))[:200],
            holding_location=str(item.get("holding_location", ""))[:150],
            arresting_authority=str(item.get("arresting_authority", ""))[:100],
        )
        persons.append(p)
        log.info(f"  âœ“ [{rtype}] {name} ({article_date or 'no date'}) â€” {source_name}")

    return persons


# â”€â”€ Save to Supabase â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def save_person(person: ScrapedPerson) -> bool:
    try:
        if person.record_type == "arrested":
            row = {
                "full_name": person.full_name,
                "gender": person.gender,
                "age": person.age,
                "state": person.state or None,
                "location_arrested": person.last_seen_location or None,
                "arresting_authority": person.arresting_authority or None,
                "charges": person.charges or None,
                "holding_location": person.holding_location or None,
                "photo_url": person.photo_url or None,
                "circumstances": person.circumstances or None,
                "date_arrested": person.article_date,
                "source": "scraped",
                "source_name": person.source_name,
                "source_url": person.source_url,
                "status": "detained",
                "is_approved": True,
            }
            res = requests.post(
                f"{API_BASE_URL}/api/admin/arrested_persons",
                headers=API_HEADERS,
                json=row,
                timeout=TIMEOUT,
            )
        else:
            row = {
                "full_name": person.full_name,
                "gender": person.gender,
                "age": person.age,
                "state": person.state or None,
                "last_seen_location": person.last_seen_location or None,
                "circumstances": person.circumstances or None,
                "photo_url": person.photo_url or None,
                "date_missing": person.article_date,
                "source": "scraped",
                "source_name": person.source_name,
                "source_url": person.source_url,
                "status": "missing",
                "is_approved": True,
            }
            res = requests.post(
                f"{API_BASE_URL}/api/admin/missing_persons",
                headers=API_HEADERS,
                json=row,
                timeout=TIMEOUT,
            )
        res.raise_for_status()
        return True
    except Exception as e:
        response_text = getattr(locals().get("res", None), "text", str(e))
        if "duplicate" in str(e).lower() or "unique" in str(e).lower() or "ignore" in response_text.lower():
            log.debug(f"  Skip duplicate: {person.full_name}")
        else:
            log.error(f"  âœ— save failed {person.full_name}: {e}")
        return False


# â”€â”€ Main â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def run(backfill: bool = False):
    if backfill:
        cutoff = datetime.now() - timedelta(days=365 * BACKFILL_YEARS)
        log.info(f"=== BACKFILL MODE: going back to {cutoff.strftime('%Y-%m-%d')} ===")
    else:
        cutoff = datetime.now() - timedelta(days=DAILY_DAYS)
        log.info(f"=== DAILY MODE: last {DAILY_DAYS} days (cutoff: {cutoff.strftime('%Y-%m-%d')}) ===")

    start = datetime.utcnow()
    total_saved = 0
    total_checked = 0

    # â”€â”€ Collect all article URLs via Google News RSS â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    all_articles = {}  # url -> (pub_date, source_name)

    log.info(f"\nâ”€â”€ Querying Google News RSS ({len(SEARCH_QUERIES)} queries) â”€â”€")
    for query in SEARCH_QUERIES:
        log.info(f"  Searching: '{query}'")
        results = google_news_urls(query, cutoff)
        log.info(f"  Found {len(results)} results")
        for url, pub_date, source_name in results:
            if url not in all_articles:
                all_articles[url] = (pub_date, source_name)
        time.sleep(REQUEST_DELAY)

    log.info(f"\nâ”€â”€ Total unique URLs: {len(all_articles)} â”€â”€")

    # â”€â”€ Batch deduplication â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    all_urls = list(all_articles.keys())
    new_urls = filter_already_scraped(all_urls)
    log.info(f"â”€â”€ After dedup: {len(new_urls)} new articles to process â”€â”€\n")

    # â”€â”€ Scrape and extract â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    for url in new_urls:
        total_checked += 1
        pub_date, source_name = all_articles[url]
        try:
            persons = scrape_article(url, source_name, pub_date)
            for person in persons:
                if save_person(person):
                    total_saved += 1
        except Exception as e:
            log.error(f"Error on {url}: {e}")
        time.sleep(REQUEST_DELAY)

    # â”€â”€ Log run â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    duration = int((datetime.utcnow() - start).total_seconds())
    try:
        requests.post(
            f"{API_BASE_URL}/api/admin/scraper_runs",
            headers=API_HEADERS,
            json={
                "started_at": start.isoformat(),
                "records_found": total_saved,
                "status": "success",
                "notes": f"checked={total_checked}; duration={duration}s",
            },
            timeout=TIMEOUT,
        ).raise_for_status()
    except Exception as e:
        log.debug(f"Could not log run: {e}")

    log.info(f"\n=== Done. {total_saved} saved / {total_checked} checked / {duration}s ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--backfill", action="store_true")
    args = parser.parse_args()
    run(backfill=args.backfill)
