#!/usr/bin/env python3
"""
EndSARSList - Scraper v5
========================
Daily mode keeps the lightweight recent-news path.
Backfill mode adds archive discovery from trusted publisher sitemaps.
"""

import argparse
import gzip
import io
import json
import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import parse_qs, quote_plus, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def clean_env(name: str, default: str = "") -> str:
    value = os.environ.get(name, default)
    return value.strip() if isinstance(value, str) else default


API_BASE_URL = clean_env("API_BASE_URL", "https://endsarslist-api.damidude.workers.dev").rstrip("/")
ADMIN_API_TOKEN = (
    clean_env("D1_ADMIN_TOKEN")
    or clean_env("SUPABASE_SERVICE_KEY")
    or clean_env("SUPABASE_KEY")
)
OPENAI_API_KEY = clean_env("OPENAI_API_KEY")


BACKFILL_YEARS = 10
DAILY_DAYS = 2
REQUEST_DELAY = 1.5
TIMEOUT = 20
OPENAI_MODEL = clean_env("OPENAI_MODEL", "gpt-4o-2024-08-06")
ARTICLE_TEXT_LIMIT = 3000
BACKFILL_TEXT_LIMIT = 6000
BACKFILL_MAX_SITEMAPS = 40
BACKFILL_MAX_URLS_PER_DOMAIN = 80
BACKFILL_MAX_TOTAL_URLS = 1200

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

OPENAI_EXTRACTION_SCHEMA = {
    "name": "endsars_people",
    "strict": True,
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "people": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "full_name": {"type": "string"},
                        "record_type": {"type": "string", "enum": ["missing", "arrested", "not_applicable"]},
                        "outcome": {"type": "string", "enum": ["still_missing", "found_alive", "found_deceased", "detained", "released", "released_on_bail", "charged", "convicted", "acquitted", "deceased", "not_applicable"]},
                        "age": {"type": ["integer", "null"]},
                        "gender": {"type": "string", "enum": ["male", "female", "unknown"]},
                        "state": {"type": "string"},
                        "circumstances": {"type": "string"},
                        "last_seen_location": {"type": "string"},
                        "arresting_authority": {"type": "string"},
                        "charges": {"type": "string"},
                        "holding_location": {"type": "string"},
                    },
                    "required": [
                        "full_name",
                        "record_type",
                        "outcome",
                        "age",
                        "gender",
                        "state",
                        "circumstances",
                        "last_seen_location",
                        "arresting_authority",
                        "charges",
                        "holding_location",
                    ],
                },
            }
        },
        "required": ["people"],
    },
}

RELEVANCE_WORDS = [
    "arrested", "detained", "missing", "abducted", "kidnapped",
    "disappeared", "remanded", "custody", "endsars", "whereabouts",
    "has been missing", "taken by", "taken into custody", "apprehended",
    "nabbed", "rescued victim", "declared missing", "wanted person",
    "suspect", "suspects", "held hostage", "released after ransom",
]

URL_RELEVANCE_HINTS = (
    "arrest", "detain", "missing", "abduct", "kidnap", "remand",
    "custody", "endsars", "whereabout", "suspect", "victim",
    "hostage", "ransom", "rescue", "disappear",
)

SEARCH_QUERIES = [
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
    "police nab Nigeria",
    "suspect arrested Nigeria",
    "suspects arrested Nigeria",
    "apprehended Nigeria",
    "nabbed Nigeria",
    "court remands Nigeria",
    "remanded in prison custody Nigeria",
    "wanted person Nigeria",
    "declared missing Nigeria",
    "family seeks missing Nigeria",
    "kidnap victim rescued Nigeria",
    "rescued kidnapped victim Nigeria",
    "whereabouts unknown Nigeria",
    "taken away by police Nigeria",
    "arrested Nigeria human rights",
    "taken by soldiers Nigeria",
    "taken by police Nigeria",
    "taken Lagos",
    "missing person Lagos",
    "arrested Lagos",
    "suspect arrested Lagos",
    "kidnapped Lagos",
    "detained Lagos",
    "disappeared Lagos",
    "taken Kano",
    "missing person Kano",
    "arrested Kano",
    "kidnapped Kano",
    "detained Kano",
    "taken Ibadan",
    "missing person Ibadan",
    "arrested Ibadan",
    "kidnapped Ibadan",
    "taken Abuja",
    "missing person Abuja",
    "arrested Abuja",
    "remanded Abuja",
    "detained Abuja FCT",
    "kidnapped Abuja",
    "taken Port Harcourt",
    "missing person Port Harcourt",
    "arrested Port Harcourt",
    "suspects arrested Port Harcourt",
    "kidnapped Port Harcourt",
    "taken Benin City",
    "missing person Benin City",
    "arrested Benin City",
    "remanded Benin City",
    "kidnapped Benin City",
    "taken Kaduna",
    "missing person Kaduna",
    "arrested Kaduna",
    "suspects arrested Kaduna",
    "kidnapped Kaduna",
    "taken Enugu",
    "missing person Enugu",
    "arrested Enugu",
    "remanded Enugu",
    "kidnapped Enugu",
    "taken Aba Nigeria",
    "missing person Aba Nigeria",
    "arrested Aba Nigeria",
    "suspect arrested Aba Nigeria",
    "kidnapped Aba Nigeria",
    "taken Maiduguri",
    "missing person Maiduguri",
    "arrested Maiduguri",
    "remanded Maiduguri",
    "kidnapped Maiduguri",
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
    "remanded Yobe",
    "suspect arrested Rivers State",
    "remanded Rivers State",
    "suspect arrested Delta State",
    "remanded Ogun State",
    "suspects arrested Oyo State",
    "remanded Anambra",
    "suspect arrested Edo State",
    "kidnap victim rescued Kaduna",
    "victim rescued Plateau",
    "family reports missing Benue",
]

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
    "independent.ng",
    "pmnewsnigeria.com",
    "newtelegraphng.com",
    "blueprint.ng",
    "von.gov.ng",
    "peoplesgazette.com",
    "dailynigerian.com",
    "thetrentonline.com",
    "theinterceptng.com",
    "bbc.com",
    "aljazeera.com",
    "reuters.com",
    "apnews.com",
}

BACKFILL_SITEMAP_DOMAINS = [
    "punchng.com",
    "vanguardngr.com",
    "dailytrust.com",
    "premiumtimesng.com",
    "thecable.ng",
    "saharareporters.com",
    "dailypost.ng",
    "tribuneonlineng.com",
    "leadership.ng",
    "channelstv.com",
    "guardian.ng",
    "thisdaylive.com",
]

COMMON_SITEMAP_PATHS = (
    "/robots.txt",
    "/sitemap.xml",
    "/sitemap_index.xml",
    "/sitemap-news.xml",
    "/news-sitemap.xml",
    "/post-sitemap.xml",
    "/post-sitemap1.xml",
    "/category-sitemap.xml",
)


@dataclass
class ScrapedPerson:
    full_name: str
    source_url: str
    source_name: str
    record_type: str
    status: str = ""
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


def get_domain(url: str) -> str:
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    return host[4:] if host.startswith("www.") else host


def is_trusted_domain(url: str) -> bool:
    domain = get_domain(url)
    return any(domain == trusted or domain.endswith("." + trusted) for trusted in TRUSTED_DOMAINS)


def normalize_source_url(url: str) -> str:
    parsed = urlparse(url.strip())
    query = parse_qs(parsed.query)
    keep = []
    if "url" in query and "bing.com" in parsed.netloc:
        return normalize_source_url(query["url"][0])
    clean_parsed = parsed._replace(
        scheme=(parsed.scheme or "https").lower(),
        netloc=get_domain(url),
        query="",
        fragment="",
    )
    normalized = urlunparse(clean_parsed).rstrip("/")
    return normalized


def normalize_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def fetch_response(url: str) -> Optional[requests.Response]:
    try:
        response = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        response.raise_for_status()
        return response
    except Exception as e:
        log.debug(f"fetch failed {url}: {e}")
        return None


def fetch(url: str) -> Optional[BeautifulSoup]:
    response = fetch_response(url)
    if not response:
        return None
    return BeautifulSoup(response.text, "html.parser")


def fetch_xml(url: str) -> Optional[BeautifulSoup]:
    response = fetch_response(url)
    if not response:
        return None
    data = response.content
    if url.lower().endswith(".gz"):
        try:
            data = gzip.decompress(data)
        except OSError:
            try:
                data = gzip.GzipFile(fileobj=io.BytesIO(data)).read()
            except OSError:
                pass
    try:
        return BeautifulSoup(data, "xml")
    except Exception as e:
        log.debug(f"fetch_xml parse failed {url}: {e}")
        return None


def resolve_bing_link(url: str) -> str:
    parsed = urlparse(url)
    if "bing.com" not in parsed.netloc:
        return url
    return parse_qs(parsed.query).get("url", [""])[0] or url


def parse_pub_date(pub_date_str: str) -> Optional[datetime]:
    if not pub_date_str:
        return None
    try:
        return parsedate_to_datetime(pub_date_str).replace(tzinfo=None)
    except Exception:
        pass
    match = re.search(r"(\d{4}-\d{2}-\d{2})", pub_date_str)
    if match:
        try:
            return datetime.strptime(match.group(1), "%Y-%m-%d")
        except ValueError:
            return None
    return None


def year_scoped_queries(cutoff: datetime) -> List[str]:
    current_year = datetime.now().year
    years = list(range(cutoff.year, current_year + 1))
    seed_queries = [
        "missing person Nigeria",
        "kidnapped Nigeria",
        "abducted Nigeria",
        "arrested Nigeria",
        "detained Nigeria",
        "remanded custody Nigeria",
        "EndSARS arrested",
        "EndSARS missing",
        "kidnap victim rescued Nigeria",
    ]
    scoped = []
    for year in years:
        for query in seed_queries:
            scoped.append(f"{query} {year}")
    return scoped


def build_search_queries(backfill: bool, cutoff: datetime) -> List[str]:
    queries = list(SEARCH_QUERIES)
    if backfill:
        queries.extend(year_scoped_queries(cutoff))
    deduped = []
    seen = set()
    for query in queries:
        q = normalize_whitespace(query)
        if q.lower() in seen:
            continue
        seen.add(q.lower())
        deduped.append(q)
    return deduped


def bing_news_urls(query: str, cutoff: datetime) -> List[Tuple[str, Optional[datetime], str]]:
    rss_url = f"https://www.bing.com/news/search?q={quote_plus(query)}&format=rss"
    soup = fetch_xml(rss_url)
    if not soup:
        return []
    results = []
    for item in soup.find_all("item"):
        link = item.find("link")
        raw_url = resolve_bing_link(link.get_text().strip()) if link else ""
        url = normalize_source_url(raw_url)
        if not url or not is_trusted_domain(url):
            continue
        pub_date_tag = item.find("pubDate")
        pub_date = parse_pub_date(pub_date_tag.get_text().strip() if pub_date_tag else "")
        if pub_date and pub_date < cutoff:
            continue
        source_tag = item.find("News:Source") or item.find("source")
        source_name = normalize_whitespace(source_tag.get_text()) if source_tag else get_domain(url)
        results.append((url, pub_date, source_name))
    return results


def discover_sitemap_urls(domain: str) -> List[str]:
    found = []
    seen = set()

    robots_url = f"https://{domain}/robots.txt"
    response = fetch_response(robots_url)
    if response and response.text:
        for line in response.text.splitlines():
            if line.lower().startswith("sitemap:"):
                sitemap = normalize_whitespace(line.split(":", 1)[1])
                if sitemap and sitemap not in seen:
                    seen.add(sitemap)
                    found.append(sitemap)

    for path in COMMON_SITEMAP_PATHS[1:]:
        sitemap = f"https://{domain}{path}"
        if sitemap not in seen:
            seen.add(sitemap)
            found.append(sitemap)
    return found


def iter_sitemap_entries(root_urls: List[str], cutoff: datetime, domain: str) -> Dict[str, Tuple[Optional[datetime], str]]:
    queue = list(root_urls)
    seen_sitemaps: Set[str] = set()
    collected: Dict[str, Tuple[Optional[datetime], str]] = {}

    while queue and len(seen_sitemaps) < BACKFILL_MAX_SITEMAPS and len(collected) < BACKFILL_MAX_URLS_PER_DOMAIN:
        sitemap_url = queue.pop(0)
        if sitemap_url in seen_sitemaps:
            continue
        seen_sitemaps.add(sitemap_url)

        soup = fetch_xml(sitemap_url)
        if not soup:
            continue

        if soup.find("sitemapindex"):
            for child in soup.find_all("sitemap"):
                loc_tag = child.find("loc")
                if not loc_tag:
                    continue
                child_url = normalize_whitespace(loc_tag.get_text())
                if child_url and child_url not in seen_sitemaps and domain in child_url:
                    queue.append(child_url)
            continue

        for url_tag in soup.find_all("url"):
            loc_tag = url_tag.find("loc")
            if not loc_tag:
                continue
            raw_url = normalize_whitespace(loc_tag.get_text())
            url = normalize_source_url(raw_url)
            if not url or not is_trusted_domain(url):
                continue
            if not any(hint in url.lower() for hint in URL_RELEVANCE_HINTS):
                continue
            lastmod_tag = url_tag.find("lastmod")
            pub_date = parse_pub_date(lastmod_tag.get_text().strip() if lastmod_tag else "")
            if pub_date and pub_date < cutoff:
                continue
            collected[url] = (pub_date, domain)
            if len(collected) >= BACKFILL_MAX_URLS_PER_DOMAIN:
                break

    return collected


def discover_backfill_archive_urls(cutoff: datetime) -> Dict[str, Tuple[Optional[datetime], str]]:
    discovered: Dict[str, Tuple[Optional[datetime], str]] = {}
    for domain in BACKFILL_SITEMAP_DOMAINS:
        if len(discovered) >= BACKFILL_MAX_TOTAL_URLS:
            break
        log.info(f"  Scanning archive sitemaps: {domain}")
        sitemap_urls = discover_sitemap_urls(domain)
        domain_urls = iter_sitemap_entries(sitemap_urls, cutoff, domain)
        log.info(f"    Archive URLs kept: {len(domain_urls)}")
        for url, meta in domain_urls.items():
            if len(discovered) >= BACKFILL_MAX_TOTAL_URLS:
                break
            discovered[url] = meta
        time.sleep(0.5)
    return discovered


def filter_already_scraped(urls: List[str]) -> List[str]:
    if not urls:
        return []
    try:
        response = requests.get(
            f"{API_BASE_URL}/api/admin/source-urls",
            headers=API_HEADERS,
            timeout=TIMEOUT,
        )
        response.raise_for_status()
        seen = {normalize_source_url(url) for url in (response.json() or {}).get("urls", [])}
        return [url for url in urls if normalize_source_url(url) not in seen]
    except Exception as e:
        log.warning(f"Dedup check failed: {e}")
        return urls


def normalize_name(name: str) -> str:
    return normalize_whitespace(name).replace(" ,", ",")


NAME_TITLES = re.compile(
    r"^(mr|mrs|ms|miss|dr|rev|revd|fr|father|alhaji|alhaja|hajia|malam|mallam|chief|prof|pastor|hon|barr|engr|comrade|comr|sir|lady|oba|emir|imam|sheikh|cpl|sgt|capt|lt|col|gen|insp|asp|dsp|csp)\.?$"
)

MISSING_OUTCOMES = ("still_missing", "found_alive", "found_deceased")
ARRESTED_OUTCOMES = ("detained", "released", "released_on_bail", "charged", "convicted", "acquitted", "deceased")


def status_from_outcome(record_type: str, outcome: str) -> str:
    if record_type == "missing":
        return outcome if outcome in ("found_alive", "found_deceased") else "missing"
    return outcome if outcome in ARRESTED_OUTCOMES else "detained"


def name_key(name: str) -> str:
    tokens = re.sub(r"[^a-z\s'-]", " ", name.lower()).split()
    return " ".join(token for token in tokens if not NAME_TITLES.match(token))


# Within-run guard only. Cross-run duplicates (title variants, nearby dates, status updates)
# are handled by the Worker's admin API, which both scrapers save through.
def person_key(record_type: str, name: str, state: str, article_date: Optional[str]) -> Tuple[str, str, str, str]:
    return (
        record_type,
        name_key(normalize_name(name)),
        re.sub(r"\s+state$", "", normalize_whitespace(state).lower()),
        article_date or "",
    )


def load_existing_person_keys() -> Set[Tuple[str, str, str, str]]:
    seen: Set[Tuple[str, str, str, str]] = set()
    endpoints = [
        ("missing", f"{API_BASE_URL}/api/missing_persons?approved=true&limit=5000", "date_missing"),
        ("arrested", f"{API_BASE_URL}/api/arrested_persons?approved=true&limit=5000", "date_arrested"),
    ]
    for record_type, url, date_field in endpoints:
        try:
            response = requests.get(url, timeout=TIMEOUT)
            response.raise_for_status()
            rows = response.json() or []
            if isinstance(rows, dict):
                rows = rows.get("records") or rows.get("results") or []
            for row in rows:
                seen.add(
                    person_key(
                        record_type,
                        str(row.get("full_name", "")),
                        str(row.get("state", "")),
                        row.get(date_field),
                    )
                )
        except Exception as e:
            log.warning(f"Existing-record dedupe load failed for {record_type}: {e}")
    log.info(f"-- Existing person fingerprints loaded: {len(seen)} --")
    return seen


def should_keep_name(name: str) -> bool:
    if not name:
        return False
    lowered = name.lower()
    if "[" in lowered or "]" in lowered:
        return False
    banned = {
        "press secretary", "central bank", "the governor", "police officer",
        "spokesperson", "security operative", "kidnappers", "suspects",
        "victim", "unknown victim", "passenger", "resident", "driver",
        "john doe", "jane smith", "placeholder name",
    }
    banned_phrases = (
        "okonjo-iweala refutes",
        "oborevwori flags",
        "kwara apc chairman",
        "governor alia",
        "owode onirin",
        "borno women",
        "orphanage children",
        "five workers",
        "six suspected homosexuals",
        "hunter remanded",
        "tegbe as",
        "power minister",
        "detained nigerian tourists",
    )
    if lowered in banned:
        return False
    if any(phrase in lowered for phrase in banned_phrases):
        return False
    if lowered.startswith((
        "unknown ",
        "unidentified ",
        "unnamed ",
        "yet-to-be-identified ",
        "unyet-identified ",
    )):
        return False
    if re.search(r"\b(suspect|victim|driver|passenger|resident|man|woman|boy|girl|person|workers|women|children|tourists)\b", lowered):
        return False
    if re.search(r"\b(chairman|governor|flags|refutes|remanded|detained)\b", lowered):
        return False
    if re.search(r"\b(one|two|three|four|five|six|seven|eight|nine|ten)\b", lowered):
        return False
    if re.search(r"\d", lowered) or re.search(r"'s\b", lowered):
        return False
    if re.search(
        r"\b(govt|government|federal|ministry|commission|command|police|army|troops|navy|naf|dss|efcc|ndlea|nscdc|inec|apc|pdp|court|senate|assembly|council|faction|crash|members|individuals|unspecified|unnamed|unknown|unidentified|nationals|miners|cultists|suspected|students|pupils|schoolchildren|passengers|travellers|villagers|farmers|worshippers|residents|youths|abductees|hostages|family|families|staff|crew|corps|community|group|others|people|persons|victims|multiple|several|name)\b",
        lowered,
    ):
        return False
    tokens = [token.strip(".,:;!?()[]{}'\"") for token in name.split() if token.strip(".,:;!?()[]{}'\"")]
    if len(tokens) < 2 or len(tokens) > 5:
        return False
    if sum(1 for token in tokens if token.isupper() and len(token) > 1) > 2:
        return False
    if any(token and token[0].islower() for token in tokens):
        return False
    return len(name.split()) >= 2


EXTRACT_PROMPT = """You are a data extraction assistant for a Nigerian human rights database tracking victims of police brutality, government repression, and the EndSARS movement.

Given a news article, extract information about people who are:
- Missing (disappeared, abducted, kidnapped, not found, whereabouts unknown)
- Arrested or detained (by police, DSS, military, EFCC, or any authority)

Return ONLY valid JSON matching the schema. If no relevant person found, return {"people":[]}.

Fields per person:
- full_name: string (must be one real individual's name with at least 2 words)
- record_type: "missing" | "arrested" | "not_applicable"
- outcome: the person's situation as of the article (see below)
- age: number or null
- gender: "male" | "female" | "unknown"
- state: one of Nigeria's 36 states or "FCT", or "" if not stated or outside Nigeria
- circumstances: 1-2 sentence summary of what happened (max 300 chars)
- last_seen_location: where last seen (for missing persons) or ""
- arresting_authority: e.g. "Police", "DSS", "Army", "EFCC" (for arrested) or ""
- charges: what charged with (for arrested) or ""
- holding_location: where being held (for arrested) or ""

outcome values:
- for record_type "missing": "still_missing", "found_alive" (rescued, freed, escaped, released by kidnappers, returned home), or "found_deceased" (killed, died, body found)
- for record_type "arrested": "detained", "released", "released_on_bail", "charged", "convicted", "acquitted", or "deceased" (died in custody)
- for "not_applicable": "not_applicable"

Rules:
- REJECT job titles, organisations, agencies, or places as names: "Press Secretary", "Central Bank", "The Governor", "Police Officer"
- REJECT groups, counts and collectives: "42 Schoolchildren", "Unspecified Individuals", "Family Members", "Passengers", "Federal Govt", "Oyo Police"
- If only a group is mentioned with no individual names, return []
- Only include people who were themselves missing, abducted, arrested or detained, including named suspects, detainees, kidnapped people, rescued kidnap victims, and remanded defendants
- Use "not_applicable" for anyone else named in the story: police officers, spokespeople, officials, lawyers, judges, politicians, relatives, witnesses, or victims of a crime who were not abducted
- Someone killed in an attack who was never missing or abducted is "not_applicable"
- If a person is unnamed or only described as "unknown", "unidentified", "victim", or "suspect", do not include them
- Do not invent or assume information not stated in the article

Return raw JSON only. No markdown, no explanation, no preamble."""

EXTRACT_PROMPT_RETRY = """You are doing a second-pass extraction for a Nigerian missing-persons and detention tracker.

Read the whole article, including body paragraphs not mentioned in the headline, and extract every clearly named individual who was themselves:
- missing, abducted, kidnapped (including those later rescued or found dead), or whose whereabouts are unknown
- arrested, detained, remanded, nabbed, apprehended, or held in custody

Apply exactly the same fields, outcome values and rules as the first pass:
- record_type "not_applicable" for officials, police, spokespeople, lawyers, relatives, witnesses and anyone else merely mentioned
- outcome reflects the person's situation as of the article (e.g. "found_alive" if rescued, "found_deceased" if killed)
- require real individual names with at least 2 words; reject groups, counts, organisations and placeholders like "Unknown Suspect", "Unidentified Man", "Driver", or "Passenger"

Return raw JSON only using the same schema as before, wrapped as {"people":[...]}."""


def extract_openai_output_text(payload: dict) -> str:
    texts: List[str] = []
    for item in payload.get("output", []) or []:
        if item.get("type") != "message":
            continue
        for content in item.get("content", []) or []:
            if content.get("type") == "output_text" and content.get("text"):
                texts.append(content["text"])
    return "\n".join(texts).strip()


def extract_with_gpt(title: str, body: str, url: str, prompt: str, text_limit: int) -> List[dict]:
    if not OPENAI_API_KEY:
        log.warning("GPT extraction skipped: OPENAI_API_KEY is missing")
        return []
    article_text = f"HEADLINE: {title}\n\nARTICLE:\n{body[:text_limit]}"
    try:
        response = requests.post(
            "https://api.openai.com/v1/responses",
            headers={
                "Authorization": f"Bearer {OPENAI_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": OPENAI_MODEL,
                "input": prompt + "\n\n" + article_text,
                "max_output_tokens": 1400,
                "text": {
                    "format": {
                        "type": "json_schema",
                        "name": OPENAI_EXTRACTION_SCHEMA["name"],
                        "schema": OPENAI_EXTRACTION_SCHEMA["schema"],
                        "strict": OPENAI_EXTRACTION_SCHEMA["strict"],
                    }
                },
            },
            timeout=TIMEOUT,
        )
        response.raise_for_status()
        payload = response.json()
        raw = extract_openai_output_text(payload)
        if not raw:
            return []
        data = json.loads(raw)
        if isinstance(data, dict) and isinstance(data.get("people"), list):
            return data["people"]
    except json.JSONDecodeError as e:
        log.debug(f"GPT JSON parse error for {url}: {e}")
    except Exception as e:
        log.warning(f"GPT extraction failed for {url}: {e}")
    return []


def extract_article_date(soup: BeautifulSoup, pub_date: Optional[datetime]) -> Optional[str]:
    if pub_date:
        return pub_date.strftime("%Y-%m-%d")
    for tag in soup.find_all("time"):
        dt = tag.get("datetime") or tag.get_text()
        if not dt:
            continue
        match = re.search(r"(\d{4}-\d{2}-\d{2})", dt)
        if match:
            return match.group(1)
    for prop in ["article:published_time", "datePublished", "publish-date"]:
        meta = soup.find("meta", property=prop) or soup.find("meta", attrs={"name": prop})
        if meta and meta.get("content"):
            match = re.search(r"(\d{4}-\d{2}-\d{2})", meta["content"])
            if match:
                return match.group(1)
    return None


def scrape_article(
    url: str,
    source_name: str,
    pub_date: Optional[datetime],
    backfill: bool,
    existing_keys: Set[Tuple[str, str, str, str]],
) -> List[ScrapedPerson]:
    soup = fetch(url)
    if not soup:
        return []

    title = ""
    for tag in [soup.find("h1"), soup.find("title")]:
        if tag:
            title = tag.get_text(" ", strip=True)
            break

    body = ""
    for selector in ["article", ".entry-content", ".post-content", ".article-body", ".story-body", ".content", "main"]:
        element = soup.select_one(selector)
        if element:
            body = element.get_text(" ", strip=True)
            break
    if not body:
        body = soup.get_text(" ", strip=True)

    body_probe = body[:1200] if backfill else body[:500]
    combined_lower = f"{title} {body_probe}".lower()
    if not any(word in combined_lower for word in RELEVANCE_WORDS):
        return []

    article_date = extract_article_date(soup, pub_date)
    photo_url = ""
    og_image = soup.find("meta", property="og:image")
    if og_image and og_image.get("content"):
        photo_url = og_image["content"]

    text_limit = BACKFILL_TEXT_LIMIT if backfill else ARTICLE_TEXT_LIMIT
    extracted = extract_with_gpt(title, body, url, EXTRACT_PROMPT, text_limit)
    retry_extracted = extract_with_gpt(title, body, url, EXTRACT_PROMPT_RETRY, text_limit) if (backfill or len(extracted) <= 1) else []

    persons = []
    seen_people = set()
    for item in extracted + retry_extracted:
        name = normalize_name(item.get("full_name") or "")
        if not should_keep_name(name):
            continue
        record_type = item.get("record_type", "")
        if record_type not in ("missing", "arrested"):
            continue
        outcome = item.get("outcome", "")
        if outcome not in (MISSING_OUTCOMES if record_type == "missing" else ARRESTED_OUTCOMES):
            continue
        dedupe_key = person_key(
            record_type,
            name,
            str(item.get("state", "")),
            article_date,
        )
        if dedupe_key in seen_people or dedupe_key in existing_keys:
            continue
        seen_people.add(dedupe_key)
        person = ScrapedPerson(
            full_name=name,
            source_url=url,
            source_name=source_name,
            record_type=record_type,
            status=status_from_outcome(record_type, outcome),
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
        persons.append(person)
        log.info(f"  + [{record_type}/{person.status}] {name} ({article_date or 'no date'}) - {source_name}")
    return persons


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
                "status": person.status or "detained",
                "is_approved": True,
            }
            response = requests.post(
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
                "status": person.status or "missing",
                "date_found": person.article_date if person.status in ("found_alive", "found_deceased") else None,
                "found_notes": person.circumstances or None if person.status in ("found_alive", "found_deceased") else None,
                "is_approved": True,
            }
            response = requests.post(
                f"{API_BASE_URL}/api/admin/missing_persons",
                headers=API_HEADERS,
                json=row,
                timeout=TIMEOUT,
            )
        response.raise_for_status()
        # The admin API returns {"action": "inserted" | "duplicate" | "updated" | "rejected"} for scraped rows.
        try:
            action = (response.json() or {}).get("action", "inserted")
        except ValueError:
            action = "inserted"
        if action != "inserted":
            log.info(f"  = {action}: {person.full_name}")
        return action == "inserted"
    except Exception as e:
        response_text = getattr(locals().get("response", None), "text", str(e))
        if "duplicate" in str(e).lower() or "unique" in str(e).lower() or "ignore" in response_text.lower():
            log.debug(f"  Skip duplicate: {person.full_name}")
        else:
            log.error(f"  x save failed {person.full_name}: {e}")
        return False


def collect_recent_articles(cutoff: datetime, queries: List[str]) -> Dict[str, Tuple[Optional[datetime], str]]:
    articles: Dict[str, Tuple[Optional[datetime], str]] = {}
    log.info(f"\n-- Querying Bing News RSS ({len(queries)} queries) --")
    for query in queries:
        log.info(f"  Searching: '{query}'")
        results = bing_news_urls(query, cutoff)
        log.info(f"  Found {len(results)} results")
        for url, pub_date, source_name in results:
            if url not in articles:
                articles[url] = (pub_date, source_name)
        time.sleep(REQUEST_DELAY)
    return articles


def collect_backfill_articles(cutoff: datetime) -> Dict[str, Tuple[Optional[datetime], str]]:
    articles = collect_recent_articles(cutoff, build_search_queries(True, cutoff))
    archive_articles = discover_backfill_archive_urls(cutoff)
    for url, meta in archive_articles.items():
        articles.setdefault(url, meta)
    return articles


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

    all_articles = collect_backfill_articles(cutoff) if backfill else collect_recent_articles(cutoff, build_search_queries(False, cutoff))
    log.info(f"\n-- Total unique URLs discovered: {len(all_articles)} --")

    all_urls = list(all_articles.keys())
    new_urls = filter_already_scraped(all_urls)
    log.info(f"-- After dedup: {len(new_urls)} new articles to process --\n")
    existing_keys = load_existing_person_keys()

    for url in new_urls:
        total_checked += 1
        pub_date, source_name = all_articles[url]
        try:
            persons = scrape_article(url, source_name, pub_date, backfill, existing_keys)
            for person in persons:
                if save_person(person):
                    existing_keys.add(person_key(person.record_type, person.full_name, person.state, person.article_date))
                    total_saved += 1
        except Exception as e:
            log.error(f"Error on {url}: {e}")
        time.sleep(REQUEST_DELAY)

    duration = int((datetime.utcnow() - start).total_seconds())
    try:
        requests.post(
            f"{API_BASE_URL}/api/admin/scraper_runs",
            headers=API_HEADERS,
            json={
                "started_at": start.isoformat(),
                "records_found": total_saved,
                "status": "success",
                "notes": f"checked={total_checked}; duration={duration}s; backfill={backfill}",
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
