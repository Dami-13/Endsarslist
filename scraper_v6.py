import argparse

from scraper_v5 import run


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--backfill", action="store_true")
    args = parser.parse_args()
    run(backfill=args.backfill)
