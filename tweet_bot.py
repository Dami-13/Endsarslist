#!/usr/bin/env python3
"""
EndSARSList Tweet Bot
=====================
Posts a summary thread to X roughly every 48 hours using the Cloudflare
Worker admin API as the backing store.
"""

import logging
import os
from datetime import datetime, timedelta

import requests
from dotenv import load_dotenv
from requests_oauthlib import OAuth1Session

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

API_BASE_URL = os.environ.get("API_BASE_URL", "https://endsarslist-api.damidude.workers.dev").rstrip("/")
ADMIN_API_TOKEN = (
    os.environ.get("D1_ADMIN_TOKEN")
    or os.environ.get("SUPABASE_SERVICE_KEY")
    or os.environ.get("SUPABASE_KEY")
)
API_HEADERS = {"x-admin-token": ADMIN_API_TOKEN or ""}

X_API_URL = "https://api.twitter.com/2/tweets"
LOOKBACK_DAYS = 2
MIN_HOURS_BETWEEN_TWEETS = 47

oauth = OAuth1Session(
    client_key=os.environ["X_CONSUMER_KEY"],
    client_secret=os.environ["X_CONSUMER_SECRET"],
    resource_owner_key=os.environ["X_ACCESS_TOKEN"],
    resource_owner_secret=os.environ["X_ACCESS_TOKEN_SECRET"],
)


def post_tweet(text: str, reply_to_id: str = None) -> str:
    payload = {"text": text}
    if reply_to_id:
        payload["reply"] = {"in_reply_to_tweet_id": reply_to_id}
    response = oauth.post(X_API_URL, json=payload)
    if response.status_code not in (200, 201):
        raise RuntimeError(f"Tweet failed {response.status_code}: {response.text}")
    tweet_id = response.json()["data"]["id"]
    log.info("tweeted: %s", text[:80])
    return tweet_id


def admin_get(path: str, **params):
    response = requests.get(
        f"{API_BASE_URL}{path}",
        headers=API_HEADERS,
        params=params,
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def admin_post(path: str, payload: dict):
    response = requests.post(
        f"{API_BASE_URL}{path}",
        headers={**API_HEADERS, "Content-Type": "application/json"},
        json=payload,
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def format_missing(row: dict) -> str:
    parts = [f"{row['full_name']}"]
    if row.get("age"):
        parts.append(f"Age {row['age']}")
    if row.get("state"):
        parts.append(row["state"])
    return " | ".join(parts)


def format_arrested(row: dict) -> str:
    parts = [f"{row['full_name']}"]
    if row.get("age"):
        parts.append(f"Age {row['age']}")
    if row.get("state"):
        parts.append(row["state"])
    if row.get("arresting_authority"):
        parts.append(f"Detained by {row['arresting_authority']}")
    return " | ".join(parts)


def latest_run():
    return admin_get("/api/admin/bot_runs", bot_name="tweet_bot")


def log_run(status: str, missing_count: int, arrested_count: int, total_records: int, tweet_id: str = None, notes: str = None):
    admin_post(
        "/api/admin/bot_runs",
        {
            "bot_name": "tweet_bot",
            "status": status,
            "missing_count": missing_count,
            "arrested_count": arrested_count,
            "total_records": total_records,
            "tweet_id": tweet_id,
            "notes": notes,
            "tweeted_at": datetime.utcnow().isoformat(),
        },
    )


def run():
    now = datetime.utcnow()
    previous = latest_run()
    fallback_since = now - timedelta(days=LOOKBACK_DAYS)

    if previous and previous.get("tweeted_at"):
        last_tweeted = datetime.fromisoformat(previous["tweeted_at"].replace("Z", "+00:00")).replace(tzinfo=None)
        if (now - last_tweeted).total_seconds() < MIN_HOURS_BETWEEN_TWEETS * 3600:
            log.info("last tweet is still within the 48-hour cadence window")
            return
        since = last_tweeted
    else:
        since = fallback_since

    recent = admin_get("/api/admin/recent", since=since.isoformat())
    missing = recent.get("missing", [])
    arrested = recent.get("arrested", [])
    total = len(missing) + len(arrested)
    date_str = now.strftime("%d %b %Y")

    if total == 0:
        tweet = (
            f"EndSARSList Update - {date_str}\n\n"
            "No new approved records were added since the last update.\n\n"
            "Submit reports or review existing cases at endsarslist.com"
        )
        tweet_id = post_tweet(tweet)
        log_run("success", 0, 0, 0, tweet_id=tweet_id, notes="heartbeat")
        return

    headline = (
        f"EndSARSList Update - {date_str}\n\n"
        f"{total} new record{'s' if total != 1 else ''} added since the last update"
    )
    if missing:
        headline += f"\nMissing: {len(missing)}"
    if arrested:
        headline += f"\nArrested/Detained: {len(arrested)}"
    headline += "\n\nendsarslist.com"

    lines = []
    if missing:
        lines.append(f"MISSING ({len(missing)}):")
        for row in missing[:3]:
            lines.append(format_missing(row))
    if arrested:
        if lines:
            lines.append("")
        lines.append(f"ARRESTED/DETAINED ({len(arrested)}):")
        for row in arrested[:3]:
            lines.append(format_arrested(row))
    lines.append("")
    lines.append(
        "If you have information about any of these individuals or want to submit a report:\n"
        "endsarslist.com\n\n"
        "#EndSARS #Nigeria #MissingPersons #HumanRights"
    )
    detail = "\n".join(lines)
    if len(detail) > 280:
        detail = detail[:277] + "..."

    tweet_id = post_tweet(headline)
    post_tweet(detail, reply_to_id=tweet_id)
    log_run("success", len(missing), len(arrested), total, tweet_id=tweet_id)


if __name__ == "__main__":
    run()
