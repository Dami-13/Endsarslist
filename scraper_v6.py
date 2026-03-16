#!/usr/bin/env python3
"""
EndSARSList — Scraper v6
=========================
Uses each Nigerian news site's own RSS feed for article discovery.
No API keys, no search endpoints, no redirects — just clean XML feeds
that every site publishes publicly.

Two modes:
  python scraper_v6.py            # daily mode — last 2 days
  python scraper_v6.py --backfill # backfill mode — last 10 years

Setup:
  pip install requests beautifulsoup4 lxml supabase python-dotenv anthropic

Env vars:
  SUPABASE_URL
  SUPABASE_SERVICE_KEY
  ANTHROPIC_API_KEY
"""

import os, re, time, logging, argparse, json
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Optional, List
from email.utils import parsedate_to_datetime

import requests
from bs4 import BeautifulSoup
from supabase import create_client, Client
from dotenv import load_dotenv
import anthropic

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Clients ───────────────────────────────────────────────────────────────────
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
claude = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

# ── Config ────────────────────────────────────────────────────────────────────
DAILY_DAYS   = 2
REQUEST_DELAY = 1.5
TIMEOUT      = 20
CLAUDE_MODEL = "claude-haiku-4-5-20251001"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/rss+xml, application/xml, text/xml, */*",
    "Accept-Language": "en-US,en;q=0.9",
}

# ── RSS feeds ─────────────────────────────────────────────────────────────────
# Each site's main feed + any dedicated crime/news category feeds
RSS_FEEDS = [
    # Sahara Reporters
    ("Sahara Reporters",        "https://saharareporters.com/rss.xml"),

    # Punch Nigeria
    ("Punch Nigeria",           "https://punchng.com/feed/"),
    ("Punch Nigeria Crime",     "https://punchng.com/category/metro-plus/crime/feed/"),
    ("Punch Nigeria News",      "https://punchng.com/category/news/feed/"),

    # Vanguard
    ("Vanguard Nigeria",        "https://www.vanguardngr.com/feed/"),
    ("Vanguard Crime",          "https://www.vanguardngr.com/category/metro-crime/feed/"),
    ("Vanguard News",           "https://www.vanguardngr.com/category/news/feed/"),

    # Premium Times
    ("Premium Times",           "https://www.premiumtimesng.com/feed/"),
    ("Premium Times News",      "https://www.premiumtimesng.com/category/news/feed/"),

    # The Guardian Nigeria
    ("Guardian Nigeria",        "https://guardian.ng/feed/"),
    ("Guardian Nigeria News",   "https://guardian.ng/news/feed/"),

    # Daily Trust
    ("Daily Trust",             "https://dailytrust.com/feed/"),
    ("Daily Trust News",        "https://dailytrust.com/category/news/feed/"),

    # The Cable
    ("The Cable",               "https://www.thecable.ng/feed"),
    ("The Cable News",          "https://www.thecable.ng/category/news/feed"),

    # This Day Live
    ("This Day Live",           "https://www.thisdaylive.com/feed/"),

    # Daily Post
    ("Daily Post Nigeria",      "https://dailypost.ng/feed/"),
    ("Daily Post Crime",        "https://dailypost.ng/category/crime/feed/"),

    # The Nation
    ("The Nation Nigeria",      "https://thenationonlineng.net/feed/"),
    ("The Nation News",         "https://thenationonlineng.net/category/news/feed/"),

    # Legit.ng
    ("Legit.ng",                "https://www.legit.ng/rss/all.rss"),
    ("Legit.ng Crime",          "https://www.legit.ng/rss/nigeria-crime.rss"),

    # Channels TV
    ("Channels TV",             "https://www.channelstv.com/feed/"),
    ("Channels TV News",        "https://www.channelstv.com/category/news/feed/"),

    # HumAngle
    ("HumAngle",                "https://humanglemedia.com/feed/"),

    # Peoples Gazette
    ("Peoples Gazette",         "https://gazettengr.com/feed/"),

    # FIJ Nigeria
    ("FIJ Nigeria",             "https://fij.ng/feed/"),

    # Leadership
    ("Leadership Nigeria",      "https://leadership.ng/feed/"),

    # Nigerian Tribune
    ("Nigerian Tribune",        "https://tribuneonlineng.com/feed/"),

    # Ripples Nigeria
    ("Ripples Nigeria",         "https://www.ripplesnigeria.com/feed/"),

    # The Whistler
    ("The Whistler",            "https://thewhistler.ng/feed/"),

    # BusinessDay
    ("BusinessDay Nigeria",     "https://businessday.ng/feed/"),

    # The Sun Nigeria
    ("The Sun Nigeria",         "https://www.sunnewsonline.com/feed/"),

    # BBC Pidgin
    ("BBC Pidgin",              "https://feeds.bbci.co.uk/pidgin/rss.xml"),

    # Amnesty International Nigeria
    ("Amnesty Nigeria",         "https://www.amnesty.org/en/tag/nigeria/feed/"),
]

# Keywords to pre-filter articles before sending to Claude
# Article title or description must contain at least one of these
RELEVANCE_KEYWORDS = [
    "arrest", "detain", "missing", "abduct", "kidnap",
    "disappear", "remand", "custody", "taken", "whereabouts",
    "endsars", "end sars", "protester", "activist", "held",
    "nabbed", "apprehended", "locked up", "imprisoned",
    "gone missing", "last seen", "rescue", "freed", "released",
    "charged", "arraign",
]

# ── Data model ────────────────────────────────────────────────────────────────
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


# ── HTTP helpers ──────────────────────────────────────────────────────────────
def fetch_rss(url: str) -> Optional[BeautifulSoup]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        return BeautifulSoup(r.content, "xml")
    except Exception as e:
        log.debug(f"RSS fetch failed {url}: {e}")
        return None


def fetch_article(url: str) -> Optional[BeautifulSoup]:
    try:
        h = {**HEADERS, "Accept": "text/html,application/xhtml+xml,*/*"}
        r = requests.get(url, headers=h, timeout=TIMEOUT)
        r.raise_for_status()
        return BeautifulSoup(r.text, "html.parser")
    except Exception as e:
        log.debug(f"Article fetch failed {url}: {e}")
        return None


# ── RSS parsing ───────────────────────────────────────────────────────────────
def parse_rss_date(date_str: str) -> Optional[datetime]:
    if not date_str:
        return None
    try:
        return parsedate_to_datetime(date_str).replace(tzinfo=None)
    except:
        pass
    # Try ISO format
    m = re.search(r'(\d{4}-\d{2}-\d{2})', date_str)
    if m:
        try:
            return datetime.strptime(m.group(1), "%Y-%m-%d")
        except:
            pass
    return None


def get_rss_articles(feed_name: str, feed_url: str, cutoff: datetime) -> List[tuple]:
    """
    Parse an RSS feed and return list of (url, date, title, description) tuples
    for articles published after cutoff that contain relevance keywords.
    """
    soup = fetch_rss(feed_url)
    if not soup:
        log.debug(f"  Could not fetch {feed_name} RSS")
        return []

    articles = []
    items = soup.find_all("item")

    if not items:
        log.debug(f"  No items in {feed_name} RSS")
        return []

    for item in items:
        # Get URL
        url = ""
        link = item.find("link")
        if link:
            url = link.get_text().strip() or (link.next_sibling or "").strip()
        if not url:
            guid = item.find("guid")
            if guid:
                url = guid.get_text().strip()
        if not url or not url.startswith("http"):
            continue

        # Get title
        title_tag = item.find("title")
        title = title_tag.get_text().strip() if title_tag else ""

        # Get description/summary
        desc_tag = item.find("description") or item.find("summary")
        desc = BeautifulSoup(desc_tag.get_text(), "html.parser").get_text().strip()[:500] if desc_tag else ""

        # Get publication date
        pub_tag = item.find("pubDate") or item.find("published") or item.find("updated")
        pub_date = parse_rss_date(pub_tag.get_text().strip()) if pub_tag else None

        # Check cutoff — skip articles older than cutoff
        # In backfill mode cutoff is 10 years ago so almost everything passes
        if pub_date and pub_date < cutoff:
            continue

        # Pre-filter: check title + description for relevance keywords
        combined = (title + " " + desc).lower()
        if not any(kw in combined for kw in RELEVANCE_KEYWORDS):
            continue

        articles.append((url, pub_date, title, desc))

    return articles


# ── Batch deduplication ───────────────────────────────────────────────────────
def filter_already_scraped(urls: List[str]) -> List[str]:
    if not urls:
        return []
    try:
        r1 = supabase.table("arrested_persons").select("source_url").in_("source_url", urls).execute()
        r2 = supabase.table("missing_persons").select("source_url").in_("source_url", urls).execute()
        seen = {row["source_url"] for row in (r1.data or []) + (r2.data or [])}
        return [u for u in urls if u not in seen]
    except Exception as e:
        log.warning(f"Dedup check failed: {e}")
        return urls


# ── Claude extraction ─────────────────────────────────────────────────────────
EXTRACT_PROMPT = """You are a data extraction assistant for a Nigerian human rights database tracking victims of police brutality, government repression, and the EndSARS movement.

Given a news article, extract information about people who are:
- Missing (disappeared, abducted, kidnapped, not found, whereabouts unknown, taken)
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
- full_name must be a real person's name. REJECT: "Press Secretary", "Central Bank", "The Governor", "Police Officer", "The Suspect"
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


# ── Article scraping ──────────────────────────────────────────────────────────
def scrape_article(url: str, source_name: str, pub_date: Optional[datetime],
                   rss_title: str, rss_desc: str) -> List[ScrapedPerson]:

    # Try to get full article body
    soup = fetch_article(url)
    body = ""
    title = rss_title

    if soup:
        # Get title from page (more complete than RSS title)
        h1 = soup.find("h1")
        if h1:
            title = h1.get_text(" ", strip=True)

        # Get body
        for sel in ["article", ".entry-content", ".post-content",
                    ".article-body", ".story-body", ".content", "main"]:
            el = soup.select_one(sel)
            if el:
                body = el.get_text(" ", strip=True)
                break
        if not body:
            body = soup.get_text(" ", strip=True)[:5000]

    # Fall back to RSS description if article fetch failed
    if not body:
        body = rss_desc

    if not body and not title:
        return []

    # Get article date
    article_date = pub_date.strftime("%Y-%m-%d") if pub_date else None
    if not article_date and soup:
        for prop in ["article:published_time", "datePublished"]:
            meta = soup.find("meta", property=prop) or soup.find("meta", attrs={"name": prop})
            if meta and meta.get("content"):
                m = re.search(r'(\d{4}-\d{2}-\d{2})', meta["content"])
                if m:
                    article_date = m.group(1)
                    break

    # Get photo
    photo_url = ""
    if soup:
        og = soup.find("meta", property="og:image")
        if og and og.get("content"):
            photo_url = og["content"]

    # Extract with Claude
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
        log.info(f"  ✓ [{rtype}] {name} ({article_date or 'no date'}) — {source_name}")

    return persons


# ── Save to Supabase ──────────────────────────────────────────────────────────
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
            supabase.table("arrested_persons").insert(row).execute()
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
            supabase.table("missing_persons").insert(row).execute()
        return True
    except Exception as e:
        if "duplicate" in str(e).lower() or "unique" in str(e).lower():
            log.debug(f"  Skip duplicate: {person.full_name}")
        else:
            log.error(f"  ✗ save failed {person.full_name}: {e}")
        return False


# ── Main ──────────────────────────────────────────────────────────────────────
def run(backfill: bool = False):
    if backfill:
        cutoff = datetime.now() - timedelta(days=365 * 10)
        log.info(f"=== BACKFILL MODE: going back to {cutoff.strftime('%Y-%m-%d')} ===")
    else:
        cutoff = datetime.now() - timedelta(days=DAILY_DAYS)
        log.info(f"=== DAILY MODE: cutoff {cutoff.strftime('%Y-%m-%d')} ===")

    start = datetime.utcnow()
    total_saved = 0
    total_checked = 0

    # ── Collect articles from all RSS feeds ───────────────────────────────────
    all_articles = {}  # url -> (pub_date, source_name, title, desc)

    log.info(f"\n── Reading {len(RSS_FEEDS)} RSS feeds ──")
    for feed_name, feed_url in RSS_FEEDS:
        articles = get_rss_articles(feed_name, feed_url, cutoff)
        new_count = 0
        for url, pub_date, title, desc in articles:
            if url not in all_articles:
                all_articles[url] = (pub_date, feed_name, title, desc)
                new_count += 1
        if new_count:
            log.info(f"  {feed_name}: {new_count} relevant articles")
        time.sleep(0.5)

    log.info(f"\n── Total unique relevant articles: {len(all_articles)} ──")

    # ── Batch deduplication ───────────────────────────────────────────────────
    all_urls = list(all_articles.keys())
    new_urls = filter_already_scraped(all_urls)
    log.info(f"── After dedup: {len(new_urls)} new to process ──\n")

    # ── Process each article ──────────────────────────────────────────────────
    for url in new_urls:
        total_checked += 1
        pub_date, source_name, rss_title, rss_desc = all_articles[url]
        try:
            persons = scrape_article(url, source_name, pub_date, rss_title, rss_desc)
            for person in persons:
                if save_person(person):
                    total_saved += 1
        except Exception as e:
            log.error(f"Error on {url}: {e}")
        time.sleep(REQUEST_DELAY)

    # ── Log run ───────────────────────────────────────────────────────────────
    duration = int((datetime.utcnow() - start).total_seconds())
    try:
        supabase.table("scraper_runs").insert({
            "started_at": start.isoformat(),
            "records_found": total_saved,
            "status": "success",
        }).execute()
    except Exception as e:
        log.debug(f"Could not log run: {e}")

    log.info(f"\n=== Done. {total_saved} saved / {total_checked} checked / {duration}s ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--backfill", action="store_true")
    args = parser.parse_args()
    run(backfill=args.backfill)
