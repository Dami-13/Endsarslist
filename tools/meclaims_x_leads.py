#!/usr/bin/env python3
"""Collect public X posts that may indicate a genuine airline compensation claim."""

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

from requests_oauthlib import OAuth1Session

SEARCH_URL = "https://api.x.com/2/tweets/search/recent"
OUTPUT = Path("artifacts/meclaims-x-leads.json")
QUERIES = [
    '("flight delayed" OR "flight cancelled" OR "missed connection") ("compensation" OR "claim" OR "refund") -is:retweet lang:en',
    '("lost baggage" OR "delayed baggage" OR "airline rejected") ("claim" OR "compensation" OR "expenses") -is:retweet lang:en',
]
HIGH_INTENT = (
    "my flight", "our flight", "airline rejected", "claim refused", "claim rejected",
    "still waiting", "lost baggage", "delayed baggage", "missed connection",
    "flight cancelled", "flight delayed", "compensation",
)
LOW_INTENT = ("news", "deal", "sale", "giveaway", "job", "hiring", "crypto")


def score(text):
    value = text.lower()
    points = sum(2 for phrase in HIGH_INTENT if phrase in value)
    points -= sum(3 for phrase in LOW_INTENT if phrase in value)
    if re.search(r"\b(ba|british airways|air peace|ryanair|easyjet|qatar|emirates|lufthansa|air france|klm|virgin)\b", value):
        points += 2
    if re.search(r"\b(london|heathrow|gatwick|manchester|lagos|abuja|accra|nairobi|doha|dubai)\b", value):
        points += 2
    return points


def main():
    oauth = OAuth1Session(
        client_key=os.environ["X_CONSUMER_KEY"],
        client_secret=os.environ["X_CONSUMER_SECRET"],
        resource_owner_key=os.environ["X_ACCESS_TOKEN"],
        resource_owner_secret=os.environ["X_ACCESS_TOKEN_SECRET"],
    )
    posts = {}
    for query in QUERIES:
        response = oauth.get(
            SEARCH_URL,
            params={
                "query": query,
                "max_results": 25,
                "tweet.fields": "created_at,author_id,lang,public_metrics",
                "expansions": "author_id",
                "user.fields": "username,name,verified",
            },
            timeout=30,
        )
        if response.status_code != 200:
            raise RuntimeError(f"X recent search failed ({response.status_code}): {response.text[:500]}")
        payload = response.json()
        users = {u["id"]: u for u in payload.get("includes", {}).get("users", [])}
        for post in payload.get("data", []):
            user = users.get(post.get("author_id"), {})
            text = post.get("text", "")
            item = {
                "id": post["id"],
                "created_at": post.get("created_at"),
                "text": text,
                "score": score(text),
                "username": user.get("username"),
                "display_name": user.get("name"),
                "verified": bool(user.get("verified")),
                "url": f"https://x.com/{user.get('username', 'i')}/status/{post['id']}",
                "public_metrics": post.get("public_metrics", {}),
            }
            posts[item["id"]] = item

    leads = sorted(posts.values(), key=lambda x: (x["score"], x.get("created_at") or ""), reverse=True)
    result = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "X recent search",
        "queries": QUERIES,
        "lead_count": len(leads),
        "high_intent_count": sum(1 for lead in leads if lead["score"] >= 4),
        "leads": leads,
    }
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(result, indent=2, ensure_ascii=True), encoding="utf-8")
    print(f"Collected {len(leads)} posts; {result['high_intent_count']} scored high intent.")


if __name__ == "__main__":
    main()
