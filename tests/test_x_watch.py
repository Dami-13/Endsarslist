"""Offline test for x_watch.py (X, OpenAI and the admin API faked). Run: python -I tests/test_x_watch.py"""
import json
import os
import sys

for key in ("X_CONSUMER_KEY", "X_CONSUMER_SECRET", "X_ACCESS_TOKEN", "X_ACCESS_TOKEN_SECRET"):
    os.environ.setdefault(key, "test")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
os.chdir(os.path.join(os.path.dirname(__file__), ".."))
import x_watch as xw  # noqa: E402

accounts = [l.strip() for l in open("x_watch_accounts.txt") if l.strip() and not l.startswith("#")]
queries = xw.search_queries(accounts)
assert all(len(q) <= 512 for q in queries), [len(q) for q in queries]
assert all(f"from:{a}" in " ".join(queries) for a in accounts), "every watched account must be searched"
assert "abd¥cted" in queries[0] and "k+dnap" in queries[0] and "-is:reply" in queries[0]

added = [{"kind": "missing", "full_name": "Bridget Okey", "state": "Enugu"},
         {"kind": "missing", "full_name": "Chukwubuike", "state": "Enugu"}]
resolved = [{"kind": "missing", "full_name": "Musa Bello", "state": "Kebbi"}]
text = xw.quote_text(added, resolved)
assert text.startswith("Added to the EndSARSList missing register: Bridget Okey and Chukwubuike."), text
assert "Update: Musa Bello now marked found/released" in text
assert "#Enugu" in text and "#Kebbi" in text and "http" not in text and "endsarslist.com" not in text
assert len(text) <= 280

# Full run with faked network: one abduction post (new names -> quote), one rescue post (update -> quote), one with no names.
posts = [
    {"id": "1", "username": "instablog9ja", "date": "2026-11-09", "text": "Mother and sons abd¥cted", "photos": [], "photo": "", "links": []},
    {"id": "2", "username": "instablog9ja", "date": "2026-11-10", "text": "Police rescue victims in Kebbi", "photos": ["https://pbs.twimg.com/a.jpg"], "photo": "https://pbs.twimg.com/a.jpg", "links": []},
    {"id": "3", "username": "PremiumTimesng", "date": "2026-11-10", "text": "Five unnamed victims rescued", "photos": [], "photo": "", "links": []},
]
people = {
    "1": [{"post_id": "1", "full_name": "Bridget Okey", "record_type": "missing", "outcome": "still_missing", "age": None, "gender": "female",
           "state": "Enugu", "last_seen_location": "", "circumstances": "Abducted while farming.", "arresting_authority": "", "charges": "", "holding_location": ""}],
    "2": [{"post_id": "2", "full_name": "Musa Bello", "record_type": "missing", "outcome": "found_alive", "age": None, "gender": "male",
           "state": "Kebbi", "last_seen_location": "", "circumstances": "Rescued by police.", "arresting_authority": "", "charges": "", "holding_location": ""}],
    "3": [],
}
saved, quoted = [], []
xw.fetch_recent = lambda accts: posts
xw.extract_post = lambda post: people[post["id"]]
xw.base.save = lambda record: (saved.append(record["full_name"]) or ("inserted" if record["full_name"] == "Bridget Okey" else "updated"))


class FakeResponse:
    status_code = 201
    text = ""


class FakeOAuth:
    def post(self, url, json=None, timeout=None):
        quoted.append(json)
        return FakeResponse()


xw.base.oauth = FakeOAuth()
xw.requests.post = lambda *a, **k: FakeResponse()
xw.run(dry_run=False)
assert saved == ["Bridget Okey", "Musa Bello"], saved
assert [q["quote_tweet_id"] for q in quoted] == ["1", "2"], quoted
assert quoted[0]["text"].startswith("Added to the EndSARSList missing register: Bridget Okey.")
assert quoted[1]["text"].startswith("Update: Musa Bello now marked found/released")

saved.clear(); quoted.clear()
xw.run(dry_run=True)
assert saved == [] and quoted == [], "dry run must not save or post"
preview = json.load(open("x_watch_preview.json", encoding="utf-8"))
assert preview["posts_read"] == 3
os.remove("x_watch_preview.json")
print("x watch tests passed")
