#!/usr/bin/env python3
"""
EndSARSList — Scraper v4
=========================
Improvements over v3:
  - Claude API for name/entity extraction (no more regex garbage)
  - Batch URL deduplication (one DB call instead of one per article)
  - Unknown-date articles no longer auto-pass cutoff
  - TikTok title search via unofficial API
  - Facebook public page scraping
  - Single workflow (delete the duplicate "EndSARSList Daily Scraper")
  - Backfill works correctly with proper date gating

Two modes:
  python scraper_v4.py            # daily mode — last 2 days
  python scraper_v4.py --backfill # backfill mode — last 10 years

Setup:
  pip install requests beautifulsoup4 supabase python-dotenv anthropic

Env vars:
  SUPABASE_URL
  SUPABASE_SERVICE_KEY
  ANTHROPIC_API_KEY         ← new
"""

import os, re, time, logging, sys, argparse, json
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Optional, List
from urllib.parse import quote_plus

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
BACKFILL_YEARS  = 10
DAILY_DAYS      = 2
MAX_PAGES_DAILY = 3
MAX_PAGES_BACK  = 50
REQUEST_DELAY   = 1.2   # seconds between HTTP requests
TIMEOUT         = 15
CLAUDE_MODEL    = "claude-haiku-4-5-20251001"   # fast + cheap for extraction

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

SEARCH_TERMS = [
    "arrested Nigeria",
    "detained Nigeria",
    "missing person Nigeria",
    "abducted Nigeria",
    "kidnapped Nigeria",
    "remanded custody Nigeria",
    "DSS custody Nigeria",
    "disappeared Nigeria",
    "EndSARS arrested",
    "EndSARS missing",
    "EndSARS detained",
    "protester arrested Nigeria",
    "activist detained Nigeria",
    "journalist arrested Nigeria",
]

NIGERIAN_STATES = [
    "Abia","Adamawa","Akwa Ibom","Anambra","Bauchi","Bayelsa","Benue","Borno",
    "Cross River","Delta","Ebonyi","Edo","Ekiti","Enugu","Gombe","Imo","Jigawa",
    "Kaduna","Kano","Katsina","Kebbi","Kogi","Kwara","Lagos","Nasarawa","Niger",
    "Ogun","Ondo","Osun","Oyo","Plateau","Rivers","Sokoto","Taraba","Yobe",
    "Zamfara","FCT","Abuja","Port Harcourt","Ibadan","Benin City",
]

# ── Sources ───────────────────────────────────────────────────────────────────
NEWS_SOURCES = [
    {
        "name": "Sahara Reporters",
        "base": "https://saharareporters.com",
        "search": "https://saharareporters.com/search/node/{query}?page={page}",
        "link_sel": "h3.title a, .search-result a",
        "next_sel": "a.pager-next",
        "date_sel": ".date-display-single, time, .field-name-post-date",
    },
    {
        "name": "Punch Nigeria",
        "base": "https://punchng.com",
        "search": "https://punchng.com/?s={query}&paged={page}",
        "link_sel": "h2.post-title a, h3.entry-title a, article h2 a",
        "next_sel": "a.next",
        "date_sel": "time, .post-date, .entry-date",
    },
    {
        "name": "Vanguard Nigeria",
        "base": "https://www.vanguardngr.com",
        "search": "https://www.vanguardngr.com/?s={query}&paged={page}",
        "link_sel": "h2.entry-title a, h3.entry-title a",
        "next_sel": "a.next",
        "date_sel": "time.entry-date, .post-date",
    },
    {
        "name": "Premium Times",
        "base": "https://www.premiumtimesng.com",
        "search": "https://www.premiumtimesng.com/?s={query}&paged={page}",
        "link_sel": "h2.entry-title a, h3 a",
        "next_sel": "a.next",
        "date_sel": "time, .post-date, .entry-date",
    },
    {
        "name": "The Guardian Nigeria",
        "base": "https://guardian.ng",
        "search": "https://guardian.ng/?s={query}&paged={page}",
        "link_sel": "h2.entry-title a, h3.entry-title a",
        "next_sel": "a.next",
        "date_sel": "time, .post-date",
    },
    {
        "name": "Daily Trust",
        "base": "https://dailytrust.com",
        "search": "https://dailytrust.com/?s={query}&paged={page}",
        "link_sel": "h2.entry-title a, h3 a",
        "next_sel": "a.next",
        "date_sel": "time, .post-date",
    },
    {
        "name": "The Cable",
        "base": "https://www.thecable.ng",
        "search": "https://www.thecable.ng/?s={query}&paged={page}",
        "link_sel": "h2.entry-title a, h3.entry-title a",
        "next_sel": "a.next",
        "date_sel": "time, .post-date",
    },
    {
        "name": "Daily Post Nigeria",
        "base": "https://dailypost.ng",
        "search": "https://dailypost.ng/?s={query}&paged={page}",
        "link_sel": "h2.entry-title a, h3.entry-title a",
        "next_sel": "a.next",
        "date_sel": "time, .post-date",
    },
    {
        "name": "The Nation Nigeria",
        "base": "https://thenationonlineng.net",
        "search": "https://thenationonlineng.net/?s={query}&paged={page}",
        "link_sel": "h2.entry-title a, h3.entry-title a",
        "next_sel": "a.next",
        "date_sel": "time, .post-date",
    },
    {
        "name": "Legit.ng",
        "base": "https://www.legit.ng",
        "search": "https://www.legit.ng/search/?q={query}&p={page}",
        "link_sel": "a.item__title, h2 a, h3 a",
        "next_sel": "a.next, .pagination a[rel=next]",
        "date_sel": "time, .date, .post-date",
    },
    {
        "name": "HumAngle",
        "base": "https://humanglemedia.com",
        "search": "https://humanglemedia.com/?s={query}&paged={page}",
        "link_sel": "h2.entry-title a, h3 a",
        "next_sel": "a.next",
        "date_sel": "time, .post-date",
    },
    {
        "name": "Peoples Gazette",
        "base": "https://gazettengr.com",
        "search": "https://gazettengr.com/?s={query}&paged={page}",
        "link_sel": "h2.entry-title a, h3 a",
        "next_sel": "a.next",
        "date_sel": "time, .post-date",
    },
    {
        "name": "FIJ Nigeria",
        "base": "https://fij.ng",
        "search": "https://fij.ng/?s={query}&paged={page}",
        "link_sel": "h2.entry-title a, h3 a",
        "next_sel": "a.next",
        "date_sel": "time, .post-date",
    },
    {
        "name": "Channels TV",
        "base": "https://www.channelstv.com",
        "search": "https://www.channelstv.com/?s={query}&paged={page}",
        "link_sel": "h2.entry-title a, h3 a",
        "next_sel": "a.next",
        "date_sel": "time, .post-date",
    },
    {
        "name": "Ripples Nigeria",
        "base": "https://www.ripplesnigeria.com",
        "search": "https://www.ripplesnigeria.com/?s={query}&paged={page}",
        "link_sel": "h2.entry-title a",
        "next_sel": "a.next",
        "date_sel": "time, .post-date",
    },
]

# ── Facebook public pages (no login needed) ───────────────────────────────────
# These are public advocacy/news pages, not private groups.
FACEBOOK_PAGES = [
    "SaharaReporters",
    "PunchNewspaper",
    "TheGuardianNigeria",
    "premiumtimesng",
    "Amnesty.Nigeria",
    "EndSARSMemorial",
]

# ── Data model ────────────────────────────────────────────────────────────────
@dataclass
class ScrapedPerson:
    full_name: str
    source_url: str
    source_name: str
    record_type: str          # "arrested" | "missing"
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


# ── HTTP helper ───────────────────────────────────────────────────────────────
def fetch(url: str, extra_headers: dict = None) -> Optional[BeautifulSoup]:
    try:
        h = {**HEADERS, **(extra_headers or {})}
        r = requests.get(url, headers=h, timeout=TIMEOUT)
        r.raise_for_status()
        return BeautifulSoup(r.text, "html.parser")
    except Exception as e:
        log.debug(f"fetch failed {url}: {e}")
        return None


# ── Batch deduplication (one DB call per batch) ───────────────────────────────
def filter_already_scraped(urls: List[str]) -> List[str]:
    """Return only URLs not yet in DB. One query per table instead of one per URL."""
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


# ── Date helpers ──────────────────────────────────────────────────────────────
def parse_date(text: str) -> Optional[str]:
    if not text:
        return None
    text = re.sub(r'\s+', ' ', text.strip())
    text = re.sub(r'(st|nd|rd|th),', ',', text)

    formats = [
        "%B %d, %Y", "%b %d, %Y", "%d %B %Y", "%d %b %Y",
        "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S", "%Y-%m-%d",
        "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y",
        "%A, %B %d, %Y", "%A, %d %B %Y",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(text[:30], fmt).strftime("%Y-%m-%d")
        except:
            pass

    m = re.search(r'(\d{4})-(\d{2})-(\d{2})', text)
    if m:
        return m.group(0)
    m = re.search(
        r'(\d{1,2})\s+(January|February|March|April|May|June|July|'
        r'August|September|October|November|December)\s+(\d{4})',
        text, re.IGNORECASE
    )
    if m:
        try:
            return datetime.strptime(f"{m.group(1)} {m.group(2)} {m.group(3)}", "%d %B %Y").strftime("%Y-%m-%d")
        except:
            pass
    return None


def extract_article_date(soup: BeautifulSoup, date_sel: str) -> Optional[str]:
    for tag in soup.find_all("time"):
        parsed = parse_date(tag.get("datetime") or tag.get("content") or tag.get_text())
        if parsed:
            return parsed
    for prop in ["article:published_time", "datePublished", "date"]:
        m = soup.find("meta", property=prop) or soup.find("meta", attrs={"name": prop})
        if m and m.get("content"):
            parsed = parse_date(m["content"])
            if parsed:
                return parsed
    for sel in date_sel.split(","):
        el = soup.select_one(sel.strip())
        if el:
            parsed = parse_date(el.get("datetime") or el.get_text())
            if parsed:
                return parsed
    return None


def is_within_cutoff(date_str: Optional[str], cutoff: datetime) -> bool:
    """
    FIX vs v3: unknown dates now FAIL the cutoff check (return False).
    We only include articles where we can confirm the date is recent enough.
    Exception: if cutoff is very old (backfill), we allow unknown dates through.
    """
    if not date_str:
        # Allow through in backfill (cutoff is years ago), reject in daily mode
        return cutoff < datetime.now() - timedelta(days=30)
    try:
        return datetime.strptime(date_str, "%Y-%m-%d") >= cutoff
    except:
        return False


# ── Claude extraction (replaces all regex name/entity logic) ──────────────────
EXTRACT_PROMPT = """You are a data extraction assistant for a Nigerian human rights database.

Given a news article, extract information about people who are:
- Missing (disappeared, abducted, kidnapped, not found)
- Arrested or detained (by police, DSS, military, EFCC, etc.)

Return ONLY a JSON array. Each element is one person. If no relevant person found, return [].

Fields per person:
- full_name: string (MUST be a real human name, 2+ words, NOT a job title or organisation)
- record_type: "missing" | "arrested"
- age: number or null
- gender: "male" | "female" | "unknown"
- state: Nigerian state/city or ""
- circumstances: 1-2 sentence summary of what happened (max 300 chars)
- last_seen_location: where they were last seen (missing only) or ""
- arresting_authority: who arrested them e.g. "Police", "DSS", "Army" (arrested only) or ""
- charges: what they are charged with (arrested only) or ""
- holding_location: where they are being held (arrested only) or ""

Rules:
- full_name must be a real person's name. Reject titles like "Press Secretary", "Central Bank", "The Governor".
- If the article is about a group arrest with no individual names, return [].
- Only include people clearly identified as missing or arrested/detained.
- Do not invent information not in the article.

Return raw JSON only, no markdown, no explanation."""


def extract_with_claude(title: str, body: str, url: str) -> List[dict]:
    """Use Claude Haiku to extract structured person data from article text."""
    article_text = f"HEADLINE: {title}\n\nARTICLE:\n{body[:3000]}"
    try:
        msg = claude.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=1000,
            messages=[
                {"role": "user", "content": EXTRACT_PROMPT + "\n\n" + article_text}
            ]
        )
        raw = msg.content[0].text.strip()
        # Strip any accidental markdown fences
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
def scrape_article(url: str, source: dict, cutoff: datetime) -> List[ScrapedPerson]:
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

    article_date = extract_article_date(soup, source.get("date_sel", "time"))

    if not is_within_cutoff(article_date, cutoff):
        log.debug(f"Skipping out-of-range article ({article_date}): {url}")
        return []

    # Quick relevance pre-check before paying for Claude
    combined_lower = (title + " " + body[:500]).lower()
    relevance_words = [
        "arrested","detained","missing","abducted","kidnapped",
        "disappeared","remanded","custody","endsars"
    ]
    if not any(w in combined_lower for w in relevance_words):
        return []

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
            source_name=source["name"],
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

    return persons


# ── Search pagination ─────────────────────────────────────────────────────────
def get_article_urls(source: dict, query: str, backfill: bool) -> List[str]:
    urls = []
    max_pages = MAX_PAGES_BACK if backfill else MAX_PAGES_DAILY

    for page in range(1, max_pages + 1):
        search_url = source["search"].format(query=quote_plus(query), page=page)
        soup = fetch(search_url)
        if not soup:
            break

        links = soup.select(source["link_sel"])
        if not links:
            break

        page_urls = []
        for a in links:
            href = a.get("href", "")
            if not href:
                continue
            if href.startswith("/"):
                href = source["base"] + href
            if not href.startswith("http"):
                continue
            if href == source["base"] or href in urls:
                continue
            page_urls.append(href)

        if not page_urls:
            break

        urls.extend(page_urls)
        log.debug(f"  {source['name']} '{query}' p{page}: {len(page_urls)} links")

        if not backfill:
            break
        if not soup.select_one(source.get("next_sel", "a.next")):
            break

        time.sleep(REQUEST_DELAY)

    return list(dict.fromkeys(urls))


# ── TikTok search (via unofficial scraper endpoint) ───────────────────────────
def search_tiktok(query: str, cutoff: datetime) -> List[ScrapedPerson]:
    """
    Uses TikTok's internal search API (no auth needed for public results).
    Extracts persons from video titles and descriptions only — no video content.
    """
    persons = []
    try:
        url = f"https://www.tiktok.com/api/search/general/full/?keyword={quote_plus(query)}&offset=0&count=20"
        headers = {
            **HEADERS,
            "Referer": "https://www.tiktok.com/",
        }
        r = requests.get(url, headers=headers, timeout=TIMEOUT)
        if r.status_code != 200:
            log.debug(f"TikTok search returned {r.status_code}")
            return []

        data = r.json()
        items = data.get("data", [])

        for item in items:
            video_info = item.get("item", {})
            desc = video_info.get("desc", "")
            create_time = video_info.get("createTime", 0)
            video_id = video_info.get("id", "")

            if not desc or not video_id:
                continue

            # Date check
            if create_time:
                article_date = datetime.fromtimestamp(create_time).strftime("%Y-%m-%d")
                if not is_within_cutoff(article_date, cutoff):
                    continue
            else:
                article_date = None

            video_url = f"https://www.tiktok.com/@{video_info.get('author', {}).get('uniqueId', 'unknown')}/video/{video_id}"

            relevance_words = ["missing","arrested","detained","kidnapped","abducted","disappeared","endsars"]
            if not any(w in desc.lower() for w in relevance_words):
                continue

            extracted = extract_with_claude(desc, desc, video_url)
            for item_data in extracted:
                name = (item_data.get("full_name") or "").strip()
                if not name or len(name.split()) < 2:
                    continue
                rtype = item_data.get("record_type", "")
                if rtype not in ("missing", "arrested"):
                    continue
                p = ScrapedPerson(
                    full_name=name,
                    source_url=video_url,
                    source_name="TikTok",
                    record_type=rtype,
                    circumstances=str(item_data.get("circumstances", ""))[:300],
                    state=str(item_data.get("state", ""))[:50],
                    age=item_data.get("age") if isinstance(item_data.get("age"), int) else None,
                    gender=item_data.get("gender", "unknown"),
                    article_date=article_date,
                )
                persons.append(p)

    except Exception as e:
        log.warning(f"TikTok search failed for '{query}': {e}")

    return persons


# ── Facebook public pages ─────────────────────────────────────────────────────
def scrape_facebook_page(page_name: str, cutoff: datetime) -> List[ScrapedPerson]:
    """
    Scrapes the public-facing Facebook page (mbasic.facebook.com).
    This works without login for fully public pages.
    Does NOT access private groups.
    """
    persons = []
    url = f"https://mbasic.facebook.com/{page_name}"
    soup = fetch(url, extra_headers={"Accept": "text/html"})
    if not soup:
        return []

    # mbasic FB shows posts as article or div blocks
    posts = soup.select("div[data-ft]") or soup.select("article") or []

    for post in posts[:20]:  # limit to 20 most recent
        text = post.get_text(" ", strip=True)
        if len(text) < 30:
            continue

        relevance_words = ["missing","arrested","detained","kidnapped","abducted","disappeared","endsars"]
        if not any(w in text.lower() for w in relevance_words):
            continue

        # Try to find post URL
        a = post.find("a", href=re.compile(r'/story\.php|/permalink/|\?story_fbid'))
        post_url = ""
        if a:
            href = a.get("href", "")
            post_url = "https://mbasic.facebook.com" + href if href.startswith("/") else href

        if not post_url:
            post_url = url

        extracted = extract_with_claude(text[:100], text, post_url)
        for item in extracted:
            name = (item.get("full_name") or "").strip()
            if not name or len(name.split()) < 2:
                continue
            rtype = item.get("record_type", "")
            if rtype not in ("missing", "arrested"):
                continue
            p = ScrapedPerson(
                full_name=name,
                source_url=post_url,
                source_name=f"Facebook/{page_name}",
                record_type=rtype,
                circumstances=str(item.get("circumstances", ""))[:300],
                state=str(item.get("state", ""))[:50],
                age=item.get("age") if isinstance(item.get("age"), int) else None,
                gender=item.get("gender", "unknown"),
            )
            persons.append(p)

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

        log.info(f"  ✓ [{person.record_type}] {person.full_name} ({person.article_date or 'no date'}) — {person.source_name}")
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
        cutoff = datetime.now() - timedelta(days=365 * BACKFILL_YEARS)
        log.info(f"=== BACKFILL MODE: going back to {cutoff.strftime('%Y-%m-%d')} ===")
    else:
        cutoff = datetime.now() - timedelta(days=DAILY_DAYS)
        log.info(f"=== DAILY MODE: last {DAILY_DAYS} days ===")

    start = datetime.utcnow()
    total_saved = 0
    total_checked = 0
    errors = 0

    # ── News sources ──────────────────────────────────────────────────────────
    for source in NEWS_SOURCES:
        log.info(f"\n── {source['name']} ──")
        source_saved = 0
        all_candidate_urls = []

        # Collect all URLs for this source first
        for query in SEARCH_TERMS:
            urls = get_article_urls(source, query, backfill)
            all_candidate_urls.extend(urls)
            time.sleep(REQUEST_DELAY * 2)

        # Batch dedup — one DB call instead of one per URL
        all_candidate_urls = list(dict.fromkeys(all_candidate_urls))
        new_urls = filter_already_scraped(all_candidate_urls)
        log.info(f"  {len(all_candidate_urls)} candidates → {len(new_urls)} new after dedup")

        for url in new_urls:
            total_checked += 1
            try:
                persons = scrape_article(url, source, cutoff)
                for person in persons:
                    if save_person(person):
                        source_saved += 1
                        total_saved += 1
            except Exception as e:
                log.error(f"  Error on {url}: {e}")
                errors += 1
            time.sleep(REQUEST_DELAY)

        log.info(f"  → {source['name']}: {source_saved} records saved")
        time.sleep(2)

    # ── TikTok ────────────────────────────────────────────────────────────────
    log.info("\n── TikTok ──")
    tiktok_terms = [
        "missing person Nigeria",
        "arrested Nigeria activist",
        "EndSARS missing",
        "kidnapped Nigeria",
    ]
    tiktok_urls_seen = set()
    for term in tiktok_terms:
        persons = search_tiktok(term, cutoff)
        for p in persons:
            if p.source_url not in tiktok_urls_seen:
                tiktok_urls_seen.add(p.source_url)
                if save_person(p):
                    total_saved += 1
        time.sleep(REQUEST_DELAY * 2)

    # ── Facebook public pages ─────────────────────────────────────────────────
    log.info("\n── Facebook Public Pages ──")
    fb_urls_seen = set()
    for page_name in FACEBOOK_PAGES:
        log.info(f"  Scraping fb/{page_name}")
        persons = scrape_facebook_page(page_name, cutoff)
        for p in persons:
            if p.source_url not in fb_urls_seen:
                fb_urls_seen.add(p.source_url)
                if save_person(p):
                    total_saved += 1
        time.sleep(REQUEST_DELAY * 3)

    # ── Log run ───────────────────────────────────────────────────────────────
    duration = (datetime.utcnow() - start).seconds
    try:
        supabase.table("scraper_runs").insert({
            "started_at": start.isoformat(),
            "duration_seconds": duration,
            "sources_checked": len(NEWS_SOURCES) + len(FACEBOOK_PAGES) + 1,
            "records_found": total_saved,
            "errors": errors,
            "status": "success",
        }).execute()
    except Exception as e:
        log.error(f"Could not log run: {e}")

    log.info(f"\n=== Done. {total_saved} saved / {total_checked} checked / {duration}s ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--backfill", action="store_true", help="Run backfill (10 years)")
    args = parser.parse_args()
    run(backfill=args.backfill)
