"""
QualityDB Scheduler — runs all market scrapers automatically every day.

Scrapers run in sequence starting at SCHEDULE_HOUR (default 03:00 UTC).
Each scraper writes to the products table (market sources) or the
competitor_scores table (quality/sustainability sources).

Market scrapers (products table):
  heureka    — Heureka.cz      (Czech)
  amazon     — Amazon.de       (German/EU)
  zbozi      — Zbozi.cz        (Czech)
  geizhals   — Geizhals.at     (Austrian/DACH)
  coolblue   — Coolblue.nl     (Dutch/Belgian)
  bol        — Bol.com         (Dutch/Belgian)
  pricerunner — Pricerunner.dk (Danish/Scandinavian)
  digitec    — Digitec.ch      (Swiss)

Quality/sustainability scrapers (competitor_scores table) are run separately
via scrape_competitors.py (typically weekly, not daily).

Usage:
    python3 scraper/scheduler.py                  # run all market scrapers daily
    python3 scraper/scheduler.py --only heureka zbozi  # run subset

To run manually right now without waiting:
    python3 scraper/heureka_scraper.py
    python3 scraper/coolblue_scraper.py
    # etc.
"""

import argparse
import time
import datetime
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from scraper.config import SCHEDULE_HOUR, SCHEDULE_MINUTE

log = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# All market scrapers in run order (fastest/lightest first)
ALL_SCRAPERS = [
    "heureka",
    "zbozi",
    "amazon",
    "geizhals",
    "coolblue",
    "bol",
    "pricerunner",
    "digitec",
]


def _import_scraper(name: str):
    """Lazily import a scraper module and return its run_scraper function."""
    if name == "heureka":
        from scraper.heureka_scraper import run_scraper
    elif name == "zbozi":
        from scraper.zbozi_scraper import run_scraper
    elif name == "amazon":
        from scraper.amazon_scraper import run_scraper
    elif name == "geizhals":
        from scraper.geizhals_scraper import run_scraper
    elif name == "coolblue":
        from scraper.coolblue_scraper import run_scraper
    elif name == "bol":
        from scraper.bol_scraper import run_scraper
    elif name == "pricerunner":
        from scraper.pricerunner_scraper import run_scraper
    elif name == "digitec":
        from scraper.digitec_scraper import run_scraper
    else:
        raise ValueError(f"Unknown scraper: {name}")
    return run_scraper


def run_all(scrapers: list[str]) -> dict:
    """Run each scraper in sequence and return a summary dict."""
    results = {}
    for name in scrapers:
        log.info(f"  ── Starting {name} …")
        try:
            fn = _import_scraper(name)
            result = fn()
            added = result.get("total_added", result.get("rows", 0))
            results[name] = {"status": "ok", "added": added}
            log.info(f"  ✓ {name}: {added} new products added.")
        except Exception as e:
            results[name] = {"status": "error", "error": str(e)}
            log.error(f"  ✗ {name}: {e}")
        time.sleep(5)
    return results


def seconds_until_next_run(hour: int, minute: int) -> float:
    """Return seconds until the next occurrence of HH:MM today or tomorrow."""
    now    = datetime.datetime.now()
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now:
        target += datetime.timedelta(days=1)
    return (target - now).total_seconds()


def main():
    parser = argparse.ArgumentParser(description="QualityDB daily market scraper scheduler")
    parser.add_argument(
        "--only", nargs="+",
        choices=ALL_SCRAPERS,
        help="Run only these scrapers (space-separated)",
    )
    args = parser.parse_args()

    scrapers = args.only if args.only else ALL_SCRAPERS

    log.info("QualityDB Scheduler started.")
    log.info(f"Scrapers: {', '.join(scrapers)}")
    log.info(f"Daily scrape scheduled at {SCHEDULE_HOUR:02d}:{SCHEDULE_MINUTE:02d} UTC.")
    log.info("Press Ctrl+C to stop.\n")

    while True:
        wait     = seconds_until_next_run(SCHEDULE_HOUR, SCHEDULE_MINUTE)
        next_run = datetime.datetime.now() + datetime.timedelta(seconds=wait)
        log.info(
            f"Next scrape run at {next_run.strftime('%Y-%m-%d %H:%M:%S')} "
            f"(in {wait / 3600:.1f} hours)"
        )

        try:
            time.sleep(wait)
        except KeyboardInterrupt:
            log.info("Scheduler stopped by user.")
            break

        log.info("=" * 50)
        log.info(f"Starting scheduled scrape run — {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")
        log.info("=" * 50)

        results = run_all(scrapers)

        total_added = sum(r.get("added", 0) for r in results.values() if r["status"] == "ok")
        errors      = [n for n, r in results.items() if r["status"] == "error"]

        log.info(f"Run complete — {total_added} new products total.")
        if errors:
            log.warning(f"Errors in: {', '.join(errors)}")

        time.sleep(5)


if __name__ == "__main__":
    main()
