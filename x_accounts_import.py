#!/usr/bin/env python3
"""
EndSARSList - import names from missing-person accounts on X
=============================================================
Reads every post (up to X's 3,200 limit) from the accounts in x_accounts.txt,
extracts named missing/detained people with the same AI schema as the news
scraper, and saves them through the Worker admin API, which applies the name
filter, state normalisation, duplicate check and status updates.

  python x_accounts_import.py --dry-run   # extract only, write x_import_preview.json
  python x_accounts_import.py             # extract and save

Cost: X pay-per-use charges per post read ($0.005 at time of writing); reads of
the same post are not charged twice within the same UTC day, so a dry run
followed by a real run on the same day is billed once.
"""

import argparse
import json
import logging
import os
import re
import time
from typing import Dict, List

import requests
from requests_oauthlib import OAuth1Session

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

API_BASE_URL = os.environ.get("API_BASE_URL", "https://endsarslist-api.damidude.workers.dev").rstrip("/")
ADMIN_API_TOKEN = os.environ.get("D1_ADMIN_TOKEN", "").strip()
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-2024-08-06")
X_API = "https://api.x.com/2"
BATCH_SIZE = 15
TIMEOUT = 30

oauth = OAuth1Session(
    client_key=os.environ["X_CONSUMER_KEY"],
    client_secret=os.environ["X_CONSUMER_SECRET"],
    resource_owner_key=os.environ["X_ACCESS_TOKEN"],
    resource_owner_secret=os.environ["X_ACCESS_TOKEN_SECRET"],
)

PHONE_PATTERN = re.compile(r"(\+?234[\s-]?|0)[789][01]\d[\s-]?\d{3}[\s-]?\d{4}")

EXTRACT_PROMPT = """You extract people from posts by Nigerian missing-person appeal accounts on X, for a public database of missing and detained people.

Each post below has an id and a date. For every clearly named individual in a post who was themselves missing, abducted, kidnapped, arrested or detained, return one entry.

Fields per person:
- post_id: the id of the post the person appears in
- full_name: one real individual's name; a first name alone is fine if that is all the post gives (reject groups, placeholders, organisations)
- record_type: "missing" | "arrested" | "not_applicable"
- outcome: for missing: "still_missing", "found_alive" (found, rescued, released, returned home, reunited) or "found_deceased"; for arrested: "detained", "released", "released_on_bail", "charged", "convicted", "acquitted" or "deceased"; otherwise "not_applicable"
- age: number or null
- gender: "male" | "female" | "unknown"
- state: one of Nigeria's 36 states or "FCT", or "" if not stated
- last_seen_location: where last seen, or ""
- circumstances: 1-2 sentence summary (max 300 chars) of what happened
- arresting_authority, charges, holding_location: for arrested people, else ""

Rules:
- NEVER include phone numbers, home addresses or other contact details in any field
- Use "not_applicable" for anyone other than the missing/detained person (relatives, police, officials, people thanked)
- A post saying someone was found, released or has returned is outcome "found_alive"; if they were found dead, "found_deceased"
- Skip posts that are general advice, thanks, or not about a specific named person
- Do not invent information

Return {"people": [...]} only."""

SCHEMA = {
    "type": "json_schema",
    "name": "x_appeal_people",
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
                        "post_id": {"type": "string"},
                        "full_name": {"type": "string"},
                        "record_type": {"type": "string", "enum": ["missing", "arrested", "not_applicable"]},
                        "outcome": {"type": "string", "enum": ["still_missing", "found_alive", "found_deceased", "detained", "released", "released_on_bail", "charged", "convicted", "acquitted", "deceased", "not_applicable"]},
                        "age": {"type": ["integer", "null"]},
                        "gender": {"type": "string", "enum": ["male", "female", "unknown"]},
                        "state": {"type": "string"},
                        "last_seen_location": {"type": "string"},
                        "circumstances": {"type": "string"},
                        "arresting_authority": {"type": "string"},
                        "charges": {"type": "string"},
                        "holding_location": {"type": "string"},
                    },
                    "required": ["post_id", "full_name", "record_type", "outcome", "age", "gender", "state", "last_seen_location", "circumstances", "arresting_authority", "charges", "holding_location"],
                },
            }
        },
        "required": ["people"],
    },
}

MISSING_OUTCOMES = ("still_missing", "found_alive", "found_deceased")
ARRESTED_OUTCOMES = ("detained", "released", "released_on_bail", "charged", "convicted", "acquitted", "deceased")


def x_get(path: str, params: dict) -> dict:
    for _ in range(5):
        response = oauth.get(f"{X_API}{path}", params=params, timeout=TIMEOUT)
        if response.status_code == 429:
            reset = int(response.headers.get("x-rate-limit-reset", time.time() + 60))
            wait = max(5, min(900, reset - int(time.time()) + 2))
            log.info("rate limited, waiting %ss", wait)
            time.sleep(wait)
            continue
        if response.status_code != 200:
            raise RuntimeError(f"X API {path} returned {response.status_code}: {response.text[:300]}")
        return response.json()
    raise RuntimeError(f"X API {path}: still rate limited")


def fetch_posts(username: str) -> List[dict]:
    user = x_get(f"/users/by/username/{username}", {})["data"]
    posts, token = [], None
    while True:
        params = {
            "max_results": 100,
            "tweet.fields": "created_at,referenced_tweets,note_tweet,attachments",
            "expansions": "referenced_tweets.id,attachments.media_keys,referenced_tweets.id.attachments.media_keys",
            "media.fields": "type,url",
        }
        if token:
            params["pagination_token"] = token
        page = x_get(f"/users/{user['id']}/tweets", params)
        includes = page.get("includes", {})
        media = {m["media_key"]: m for m in includes.get("media", [])}
        refs = {t["id"]: t for t in includes.get("tweets", [])}

        def body(tweet: dict) -> str:
            return (tweet.get("note_tweet") or {}).get("text") or tweet.get("text", "")

        def photos(tweet: dict) -> List[str]:
            keys = (tweet.get("attachments") or {}).get("media_keys", [])
            return [media[k]["url"] for k in keys if k in media and media[k].get("type") == "photo" and media[k].get("url")]

        for tweet in page.get("data", []):
            text, images = body(tweet), photos(tweet)
            for ref in tweet.get("referenced_tweets", []) or []:
                original = refs.get(ref["id"])
                if not original:
                    continue
                if ref["type"] == "retweeted":
                    text = body(original)
                elif ref["type"] == "quoted":
                    text = f"{text}\n\nQuoted post: {body(original)}"
                images = images or photos(original)
            posts.append({
                "id": tweet["id"],
                "username": username,
                "date": tweet.get("created_at", "")[:10],
                "text": PHONE_PATTERN.sub("[phone]", text),
                "photo": images[0] if images else "",
            })
        token = (page.get("meta") or {}).get("next_token")
        if not token:
            break
    log.info("@%s: %s posts", username, len(posts))
    return posts


def extract(batch: List[dict]) -> List[dict]:
    listing = "\n\n".join(f"[post_id {p['id']} | {p['date']}]\n{p['text']}" for p in batch)
    response = requests.post(
        "https://api.openai.com/v1/responses",
        headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
        json={"model": OPENAI_MODEL, "input": f"{EXTRACT_PROMPT}\n\n{listing}", "max_output_tokens": 4000, "text": {"format": SCHEMA}},
        timeout=120,
    )
    response.raise_for_status()
    texts = [c["text"] for item in response.json().get("output", []) if item.get("type") == "message"
             for c in item.get("content", []) if c.get("type") == "output_text"]
    return json.loads("".join(texts) or '{"people": []}').get("people", [])


def to_record(person: dict, post: dict) -> Dict:
    record_type, outcome = person["record_type"], person["outcome"]
    circumstances = PHONE_PATTERN.sub("", person.get("circumstances", ""))[:300]
    common = {
        "full_name": person["full_name"].strip(),
        "gender": person.get("gender") or "unknown",
        "age": person.get("age"),
        "state": person.get("state") or None,
        "circumstances": circumstances or None,
        "photo_url": post["photo"] or None,
        "source": "scraped",
        "source_name": f"@{post['username']} on X",
        "source_url": f"https://x.com/{post['username']}/status/{post['id']}",
        "is_approved": True,
    }
    if record_type == "arrested":
        return {**common, "kind": "arrested", "date_arrested": post["date"] or None,
                "location_arrested": person.get("last_seen_location") or None,
                "arresting_authority": person.get("arresting_authority") or None,
                "charges": person.get("charges") or None,
                "holding_location": person.get("holding_location") or None,
                "status": outcome if outcome in ARRESTED_OUTCOMES else "detained"}
    found = outcome in ("found_alive", "found_deceased")
    return {**common, "kind": "missing", "date_missing": post["date"] or None,
            "last_seen_location": person.get("last_seen_location") or None,
            "status": outcome if found else "missing",
            "date_found": post["date"] if found else None,
            "found_notes": circumstances if found else None}


NAME_TITLES = re.compile(r"^(mr|mrs|ms|miss|dr|rev|revd|fr|father|alhaji|alhaja|hajia|malam|mallam|chief|prof|pastor|hon|barr|engr|comrade|comr|sir|lady|emir|imam|sheikh|major|cpl|sgt|capt|lt|col|gen|insp|asp|dsp|csp)\.?$")
RESOLVED = {"missing": 0, "found_alive": 1, "found_deceased": 2, "detained": 0, "charged": 1,
            "released_on_bail": 2, "released": 3, "acquitted": 3, "convicted": 3, "deceased": 4}


def name_tokens(name: str) -> List[str]:
    return [t for t in re.sub(r"[^a-z\s'-]", " ", name.lower()).split() if not NAME_TITLES.match(t)]


def same_name(a: str, b: str) -> bool:
    x, y = name_tokens(a), name_tokens(b)
    short, long = (x, set(y)) if len(x) <= len(y) else (y, set(x))
    return len(short) >= 2 and all(t in long for t in short)


def merge_repeats(records: List[dict]) -> List[dict]:
    """Appeal accounts repost the same person for months; merge each person's posts on one
    account into a single record: first appeal date, most resolved status, fullest details."""
    def account(r): return r["source_url"].split("/")[3]
    def when(r): return r.get("date_missing") or r.get("date_arrested") or ""

    records = sorted(records, key=when)
    parent = list(range(len(records)))

    def root(i):
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    for i in range(len(records)):
        for j in range(i + 1, len(records)):
            if account(records[i]) == account(records[j]) and same_name(records[i]["full_name"], records[j]["full_name"]):
                parent[root(j)] = root(i)
    groups: Dict[int, List[dict]] = {}
    for i, record in enumerate(records):
        groups.setdefault(root(i), []).append(record)

    merged = []
    for group in groups.values():
        missing_votes = sum(1 for r in group if r["kind"] == "missing")
        kind = "missing" if missing_votes >= len(group) - missing_votes else "arrested"
        of_kind = [r for r in group if r["kind"] == kind]
        best = sorted(of_kind, key=lambda r: (RESOLVED.get(r["status"], 0), when(r)))[-1]
        first = group[0]
        record = dict(first if first["kind"] == kind else of_kind[0])
        record["full_name"] = max((r["full_name"] for r in group), key=lambda n: len(name_tokens(n)))
        record["status"] = best["status"]
        for field in ("age", "state", "photo_url", "last_seen_location", "arresting_authority", "charges", "holding_location"):
            if not record.get(field):
                record[field] = next((r.get(field) for r in group if r.get(field)), None)
        record["circumstances"] = max((r.get("circumstances") or "" for r in group), key=len) or None
        if kind == "missing":
            found = best["status"] in ("found_alive", "found_deceased")
            record["date_found"] = when(best) if found else None
            record["found_notes"] = best.get("circumstances") if found else None
        merged.append(record)
    return merged


def save(record: dict) -> str:
    kind = record.pop("kind")
    response = requests.post(f"{API_BASE_URL}/api/admin/{kind}_persons",
                             headers={"x-admin-token": ADMIN_API_TOKEN, "Content-Type": "application/json"},
                             json=record, timeout=TIMEOUT)
    response.raise_for_status()
    return (response.json() or {}).get("action", "inserted")


def run(dry_run: bool):
    with open("x_accounts.txt", encoding="utf-8") as handle:
        accounts = [line.strip().lstrip("@") for line in handle if line.strip() and not line.startswith("#")]
    posts = [post for account in accounts for post in fetch_posts(account)]
    by_id = {post["id"]: post for post in posts}

    records = []
    for start in range(0, len(posts), BATCH_SIZE):
        batch = posts[start:start + BATCH_SIZE]
        try:
            people = extract(batch)
        except Exception as error:
            log.error("extraction failed for batch at %s: %s", start, error)
            continue
        for person in people:
            post = by_id.get(person.get("post_id"))
            valid = (MISSING_OUTCOMES if person.get("record_type") == "missing" else ARRESTED_OUTCOMES)
            if not post or person.get("record_type") not in ("missing", "arrested") or person.get("outcome") not in valid:
                continue
            records.append(to_record(person, post))
        log.info("extracted %s people from %s/%s posts", len(records), min(start + BATCH_SIZE, len(posts)), len(posts))

    mentions = len(records)
    records = merge_repeats(records)
    log.info("merged %s mentions into %s people", mentions, len(records))

    with open("x_import_preview.json", "w", encoding="utf-8") as handle:
        json.dump({"accounts": accounts, "posts_read": len(posts), "people": records}, handle, ensure_ascii=False, indent=1)
    if dry_run:
        log.info("dry run: %s people written to x_import_preview.json, nothing saved", len(records))
        return

    counts: Dict[str, int] = {}
    for record in records:
        try:
            action = save(dict(record))
        except Exception as error:
            action = "error"
            log.error("save failed for %s: %s", record["full_name"], error)
        counts[action] = counts.get(action, 0) + 1
    log.info("saved: %s", counts)
    requests.post(f"{API_BASE_URL}/api/admin/scraper_runs",
                  headers={"x-admin-token": ADMIN_API_TOKEN, "Content-Type": "application/json"},
                  json={"records_found": counts.get("inserted", 0), "status": "success",
                        "notes": f"x_import accounts={','.join(accounts)}; posts={len(posts)}; " + "; ".join(f"{k}={v}" for k, v in counts.items())},
                  timeout=TIMEOUT)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    run(parser.parse_args().dry_run)
