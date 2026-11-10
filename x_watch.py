#!/usr/bin/env python3
"""
EndSARSList - watch Nigerian news accounts on X for abductions, rescues and arrests
===================================================================================
Daily: searches recent posts from the accounts in x_watch_accounts.txt for abduction,
missing, rescue and arrest stories (including filter-dodging spellings like "abd¥cted"),
reads the post, any linked article and attached photos, extracts named people, and saves
them through the Worker admin API (name filter, dedupe, and status updates: a rescue
post marks a matching missing person found).

When a post adds new people or resolves existing ones, the account quote-posts it.
Quote posts are permitted automation on X; automated keyword-based replies are not.

  python x_watch.py --dry-run     # read and extract only; nothing saved or posted
  python x_watch.py               # save and quote-post (max MAX_QUOTES per run)
"""

import argparse
import json
import logging
import re
from typing import Dict, List

import requests

import x_accounts_import as base

log = logging.getLogger("x_watch")
LOOKBACK_HOURS = 26
MAX_QUOTES = 5
MAX_POSTS = 100
ARTICLE_LIMIT = 3500

# Accounts often disguise trigger words ("abd¥cted", "k+dnap", "cr+minal") to avoid filters.
TERMS = [
    "abducted", "abd¥cted", "abdµcted", "abduction", "abd¥ction",
    "kidnapped", "kidnap", "k+dnap", "k+dnapped", "kidn@pped", "k!dnapped",
    "missing", "m!ssing", "rescued", "rescue", "r€scued", "freed", "regain freedom",
    "arrested", "arrest", "arr€st", "detained", "remanded", "apprehended", "nabbed",
]

WATCH_PROMPT = base.EXTRACT_PROMPT.replace(
    "posts by Nigerian missing-person appeal accounts on X",
    "Nigerian news posts on X (with any linked article text and attached images)",
) + """

Extra rules for news posts:
- Names may appear only in the attached image or the linked article; read both.
- For rescue/release stories, list each named rescued person with outcome "found_alive" (or "found_deceased").
- For arrest stories, list each named arrested person with record_type "arrested".
- Ignore words written with symbols to dodge filters (treat "abd¥cted" as "abducted")."""


MAX_QUERY_LENGTH = 512  # X recent search limit


def search_query(accounts: List[str]) -> str:
    sources = " OR ".join(f"from:{a}" for a in accounts)
    terms = " OR ".join(f'"{t}"' if " " in t else t for t in TERMS)
    return f"({sources}) ({terms}) -is:retweet -is:reply"


def search_queries(accounts: List[str]) -> List[str]:
    """Split the watched accounts across as many queries as needed to stay under X's length limit."""
    queries, group = [], []
    for account in accounts:
        if group and len(search_query(group + [account])) > MAX_QUERY_LENGTH:
            queries.append(search_query(group))
            group = []
        group.append(account)
    if group:
        queries.append(search_query(group))
    return queries


def fetch_recent(accounts: List[str]) -> List[dict]:
    from datetime import datetime, timedelta, timezone
    start = (datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)).strftime("%Y-%m-%dT%H:%M:%SZ")
    posts = []
    for query in search_queries(accounts):
        posts.extend(fetch_query(query, start))
    log.info("search returned %s posts", len(posts))
    return posts


def fetch_query(query: str, start: str) -> List[dict]:
    page = base.x_get("/tweets/search/recent", {
        "query": query,
        "start_time": start,
        "max_results": MAX_POSTS,
        "tweet.fields": "created_at,author_id,note_tweet,entities,attachments",
        "expansions": "author_id,attachments.media_keys",
        "user.fields": "username",
        "media.fields": "type,url",
    })
    users = {u["id"]: u["username"] for u in page.get("includes", {}).get("users", [])}
    media = {m["media_key"]: m for m in page.get("includes", {}).get("media", [])}
    posts = []
    for tweet in page.get("data", []):
        keys = (tweet.get("attachments") or {}).get("media_keys", [])
        photos = [media[k]["url"] for k in keys if k in media and media[k].get("type") == "photo" and media[k].get("url")]
        links = [u.get("unwound_url") or u.get("expanded_url") for u in (tweet.get("entities") or {}).get("urls", [])]
        links = [l for l in links if l and not re.match(r"https?://(x|twitter)\.com/", l)]
        posts.append({
            "id": tweet["id"],
            "username": users.get(tweet.get("author_id"), "unknown"),
            "date": tweet.get("created_at", "")[:10],
            "text": base.PHONE_PATTERN.sub("[phone]", (tweet.get("note_tweet") or {}).get("text") or tweet.get("text", "")),
            "photos": photos,
            "photo": photos[0] if photos else "",
            "links": links,
        })
    return posts


def article_text(url: str) -> str:
    try:
        from bs4 import BeautifulSoup
        html = requests.get(url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}, timeout=20).text
        soup = BeautifulSoup(html, "html.parser")
        node = soup.find("article") or soup.find("main") or soup.body
        return base.PHONE_PATTERN.sub("[phone]", node.get_text(" ", strip=True))[:ARTICLE_LIMIT] if node else ""
    except Exception as error:
        log.info("article fetch failed for %s: %s", url, error)
        return ""


def extract_post(post: dict) -> List[dict]:
    article = "\n\n".join(article_text(link) for link in post["links"][:1])
    content = [{"type": "input_text", "text": f"{WATCH_PROMPT}\n\n[post_id {post['id']} | {post['date']} | @{post['username']}]\n{post['text']}"
                + (f"\n\nLinked article:\n{article}" if article else "")}]
    content += [{"type": "input_image", "image_url": url} for url in post["photos"][:2]]
    response = requests.post(
        "https://api.openai.com/v1/responses",
        headers={"Authorization": f"Bearer {base.OPENAI_API_KEY}", "Content-Type": "application/json"},
        json={"model": base.OPENAI_MODEL, "input": [{"role": "user", "content": content}], "max_output_tokens": 2000,
              "text": {"format": base.SCHEMA}},
        timeout=120,
    )
    response.raise_for_status()
    texts = [c["text"] for item in response.json().get("output", []) if item.get("type") == "message"
             for c in item.get("content", []) if c.get("type") == "output_text"]
    return json.loads("".join(texts) or '{"people": []}').get("people", [])


def join_names(names: List[str]) -> str:
    return names[0] if len(names) == 1 else ", ".join(names[:-1]) + " and " + names[-1]


def quote_text(added: List[dict], resolved: List[dict]) -> str:
    """Short, link-free quote post (X charges more for posts with links; the profile has the site)."""
    lines = []
    if added:
        missing = [r for r in added if r["kind"] == "missing"]
        arrested = [r for r in added if r["kind"] == "arrested"]
        if missing:
            lines.append(f"Added to the EndSARSList missing register: {join_names([r['full_name'] for r in missing])}.")
        if arrested:
            lines.append(f"Added to the EndSARSList arrests register: {join_names([r['full_name'] for r in arrested])}.")
    if resolved:
        lines.append(f"Update: {join_names([r['full_name'] for r in resolved])} now marked found/released on EndSARSList.")
    states = sorted({r["state"] for r in added + resolved if r.get("state") in base_states()})
    tags = ["#" + ("Abuja" if s == "FCT" else s.replace(" ", "")) for s in states[:2]] + ["#MissingPersons", "#EndSARS"]
    text = "\n\n".join(lines) + "\n\nFull list: link in bio\n" + " ".join(tags)
    return text if len(text) <= 280 else text[:277] + "..."


def base_states():
    return {"Abia", "Adamawa", "Akwa Ibom", "Anambra", "Bauchi", "Bayelsa", "Benue", "Borno", "Cross River", "Delta",
            "Ebonyi", "Edo", "Ekiti", "Enugu", "FCT", "Gombe", "Imo", "Jigawa", "Kaduna", "Kano", "Katsina", "Kebbi",
            "Kogi", "Kwara", "Lagos", "Nasarawa", "Niger", "Ogun", "Ondo", "Osun", "Oyo", "Plateau", "Rivers",
            "Sokoto", "Taraba", "Yobe", "Zamfara"}


def run(dry_run: bool):
    with open("x_watch_accounts.txt", encoding="utf-8") as handle:
        accounts = [line.strip().lstrip("@") for line in handle if line.strip() and not line.startswith("#")]
    posts = fetch_recent(accounts)
    preview, counts, quotes = [], {}, 0
    for post in posts:
        try:
            people = extract_post(post)
        except Exception as error:
            log.error("extraction failed for %s: %s", post["id"], error)
            continue
        records = []
        for person in people:
            valid = base.MISSING_OUTCOMES if person.get("record_type") == "missing" else base.ARRESTED_OUTCOMES
            if person.get("post_id") == post["id"] and person.get("record_type") in ("missing", "arrested") and person.get("outcome") in valid:
                records.append(base.to_record(person, post))
        preview.append({"post": f"https://x.com/{post['username']}/status/{post['id']}", "people": records})
        if dry_run or not records:
            continue
        added, resolved = [], []
        for record in records:
            action = base.save(dict(record))
            counts[action] = counts.get(action, 0) + 1
            (added if action == "inserted" else resolved if action == "updated" else []).append(record)
        if (added or resolved) and quotes < MAX_QUOTES:
            text = quote_text(added, resolved)
            response = base.oauth.post("https://api.x.com/2/tweets", json={"text": text, "quote_tweet_id": post["id"]}, timeout=30)
            if response.status_code in (200, 201):
                quotes += 1
                log.info("quoted %s: %s", post["id"], text.replace("\n", " | "))
            else:
                log.error("quote post failed %s: %s", response.status_code, response.text[:300])

    with open("x_watch_preview.json", "w", encoding="utf-8") as handle:
        json.dump({"accounts": accounts, "posts_read": len(posts), "results": preview}, handle, ensure_ascii=False, indent=1)
    log.info("done: posts=%s saved=%s quotes=%s dry_run=%s", len(posts), counts, quotes, dry_run)
    if not dry_run:
        requests.post(f"{base.API_BASE_URL}/api/admin/scraper_runs",
                      headers={"x-admin-token": base.ADMIN_API_TOKEN, "Content-Type": "application/json"},
                      json={"records_found": counts.get("inserted", 0), "status": "success",
                            "notes": f"x_watch posts={len(posts)}; quotes={quotes}; " + "; ".join(f"{k}={v}" for k, v in counts.items())},
                      timeout=30)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    run(parser.parse_args().dry_run)
