"""Offline test for x_accounts_import.py: X and OpenAI are faked. Run: python -I tests/test_x_import.py"""
import os
import sys

for key in ("X_CONSUMER_KEY", "X_CONSUMER_SECRET", "X_ACCESS_TOKEN", "X_ACCESS_TOKEN_SECRET"):
    os.environ.setdefault(key, "test")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import x_accounts_import as xi  # noqa: E402

pages = {
    "/users/by/username/DMNI_NG": {"data": {"id": "42"}},
    "/users/42/tweets": {
        "data": [
            {"id": "1", "created_at": "2021-08-25T10:00:00Z", "text": "MISSING: Ada Okafor, 19, last seen Ikeja. Call 08031234567",
             "attachments": {"media_keys": ["m1"]}},
            {"id": "2", "created_at": "2021-08-26T10:00:00Z", "text": "RT @someone: truncated…",
             "referenced_tweets": [{"type": "retweeted", "id": "9"}]},
        ],
        "includes": {
            "media": [{"media_key": "m1", "type": "photo", "url": "https://pbs.twimg.com/media/ada.jpg"},
                      {"media_key": "m9", "type": "photo", "url": "https://pbs.twimg.com/media/tunde.jpg"}],
            "tweets": [{"id": "9", "text": "Tunde Bello has been found and reunited with family. Thank you all!",
                        "attachments": {"media_keys": ["m9"]}}],
        },
        "meta": {},
    },
}
xi.x_get = lambda path, params: pages[path]

posts = xi.fetch_posts("DMNI_NG")
assert len(posts) == 2
assert "[phone]" in posts[0]["text"] and "0803" not in posts[0]["text"], posts[0]["text"]
assert posts[0]["photo"].endswith("ada.jpg")
assert posts[1]["text"].startswith("Tunde Bello has been found"), "retweet should use the original's full text"
assert posts[1]["photo"].endswith("tunde.jpg"), "retweet should carry the original's photo"

missing = xi.to_record({"post_id": "1", "full_name": "Ada Okafor", "record_type": "missing", "outcome": "still_missing",
                        "age": 19, "gender": "female", "state": "Lagos", "last_seen_location": "Ikeja",
                        "circumstances": "Last seen in Ikeja. Call 0803 123 4567", "arresting_authority": "", "charges": "",
                        "holding_location": ""}, posts[0])
assert missing["kind"] == "missing" and missing["status"] == "missing" and missing["date_missing"] == "2021-08-25"
assert "0803" not in (missing["circumstances"] or ""), missing["circumstances"]
assert missing["source_url"] == "https://x.com/DMNI_NG/status/1" and missing["source_name"] == "@DMNI_NG on X"
assert missing["source"] == "scraped", "must go through the Worker's saveScraped checks"

found = xi.to_record({"post_id": "2", "full_name": "Tunde Bello", "record_type": "missing", "outcome": "found_alive",
                      "age": None, "gender": "male", "state": "", "last_seen_location": "", "circumstances": "Found and reunited.",
                      "arresting_authority": "", "charges": "", "holding_location": ""}, posts[1])
assert found["status"] == "found_alive" and found["date_found"] == "2021-08-26"

print("x import tests passed")
