#!/usr/bin/env python3
"""
Post one approved post to X from the EndSARSList account (via the X API, not the website).

  python post_to_x.py --text "Line one\\nLine two" [--reply-to POST_ID] [--quote POST_ID]

A literal \\n in --text becomes a line break (GitHub's workflow form is single-line).
"""

import argparse
import os

from requests_oauthlib import OAuth1Session


def post(text: str, reply_to: str = "", quote: str = "") -> str:
    oauth = OAuth1Session(
        client_key=os.environ["X_CONSUMER_KEY"],
        client_secret=os.environ["X_CONSUMER_SECRET"],
        resource_owner_key=os.environ["X_ACCESS_TOKEN"],
        resource_owner_secret=os.environ["X_ACCESS_TOKEN_SECRET"],
    )
    payload = {"text": text}
    if reply_to:
        payload["reply"] = {"in_reply_to_tweet_id": reply_to}
    if quote:
        payload["quote_tweet_id"] = quote
    response = oauth.post("https://api.x.com/2/tweets", json=payload, timeout=30)
    if response.status_code not in (200, 201):
        raise RuntimeError(f"Post failed {response.status_code}: {response.text}")
    return response.json()["data"]["id"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--text", required=True)
    parser.add_argument("--reply-to", default="")
    parser.add_argument("--quote", default="")
    args = parser.parse_args()
    text = args.text.replace("\\n", "\n").strip()
    if len(text) > 280:
        raise SystemExit(f"Text is {len(text)} characters; X allows 280.")
    post_id = post(text, args.reply_to.strip(), args.quote.strip())
    print(f"Posted https://x.com/i/status/{post_id}")
