"""
Competitor data scraper — orchestrator.

Runs all competitor scrapers in sequence (safest first) and prints a summary.

Usage:
    python scrape_competitors.py                    # run all scrapers
    python scrape_competitors.py --only french_index ifixit
    python scrape_competitors.py --skip looria
    python scrape_competitors.py --summary-only     # just print DB stats
"""

import argparse
import sqlite3
import sys
import traceback
from datetime import datetime

# Add scraper dir to path so we can import modules from here
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scraper_competitors_config import DB_PATH
from scraper_competitors_db import init_table


def print_summary() -> None:
    """Print current competitor_scores table statistics."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    print("\n" + "="*60)
    print("COMPETITOR SCORES — DATABASE SUMMARY")
    print("="*60)

    try:
        rows = conn.execute("""
            SELECT
                source,
                COUNT(*) as n,
                ROUND(AVG(score_normalized), 1) as avg_score,
                ROUND(MIN(score_normalized), 1) as min_score,
                ROUND(MAX(score_normalized), 1) as max_score,
                MAX(scraped_at) as last_scraped
            FROM competitor_scores
            GROUP BY source
            ORDER BY source
        """).fetchall()

        if not rows:
            print("  (no data yet)")
        else:
            print(f"  {'Source':<16} {'Rows':>6} {'Avg':>6} {'Min':>6} {'Max':>6}  Last scraped")
            print(f"  {'-'*16} {'-'*6} {'-'*6} {'-'*6} {'-'*6}  {'-'*19}")
            for r in rows:
                print(f"  {r['source']:<16} {r['n']:>6} {r['avg_score']:>6} {r['min_score']:>6} {r['max_score']:>6}  {r['last_scraped']}")

        total = conn.execute("SELECT COUNT(*) FROM competitor_scores").fetchone()[0]
        print(f"\n  Total rows: {total}")

        # Top brands by average score
        print("\n  TOP BRANDS (avg score across all sources, min 3 records):")
        top_brands = conn.execute("""
            SELECT brand, COUNT(*) as n, ROUND(AVG(score_normalized), 1) as avg
            FROM competitor_scores
            WHERE brand IS NOT NULL AND brand != ''
            GROUP BY brand
            HAVING n >= 3
            ORDER BY avg DESC
            LIMIT 15
        """).fetchall()
        for br in top_brands:
            print(f"    {br['brand']:<25} {br['avg']:>6}  (n={br['n']})")

        # Cross-reference with IKOR brands
        print("\n  OVERLAP WITH IKOR PRODUCTS TABLE:")
        try:
            overlap = conn.execute("""
                SELECT cs.brand, cs.source, ROUND(cs.score_normalized, 1) as score, cs.canonical_category
                FROM competitor_scores cs
                WHERE cs.brand IN (
                    SELECT DISTINCT TRIM(brand) FROM products WHERE brand IS NOT NULL
                )
                ORDER BY cs.brand, cs.source
                LIMIT 30
            """).fetchall()
            if overlap:
                for row in overlap:
                    print(f"    {row['brand']:<20} {row['source']:<16} {row['score']:>6}  {row['canonical_category']}")
            else:
                print("    (no overlap yet — check brand name normalisation)")
        except sqlite3.OperationalError:
            print("    (products table not found or no brand column)")

    except sqlite3.OperationalError as e:
        print(f"  Error: {e}")
    finally:
        conn.close()
    print("="*60 + "\n")


def run_scraper(name: str) -> dict:
    """Import and run a single scraper. Returns result dict."""
    result = {"source": name, "rows": 0, "status": "ok", "error": None}
    start = datetime.now()

    try:
        if name == "french_index":
            from scraper_french_index import scrape
        elif name == "ifixit":
            from scraper_ifixit import scrape
        elif name == "bifl":
            from scraper_bifl import scrape
        elif name == "yale":
            from scraper_yale import scrape
        elif name == "looria":
            from scraper_looria import scrape
        elif name == "openrepair":
            from scraper_openrepair import scrape
        elif name == "eprel":
            from scraper_eprel import scrape
        else:
            result["status"] = "unknown"
            result["error"] = f"Unknown scraper: {name}"
            return result

        rows = scrape()
        result["rows"] = rows
    except Exception as e:
        result["status"] = "error"
        result["error"] = str(e)
        traceback.print_exc()
    finally:
        elapsed = (datetime.now() - start).total_seconds()
        result["elapsed_s"] = round(elapsed, 1)

    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Run competitor data scrapers")
    parser.add_argument(
        "--only", nargs="+",
        choices=["french_index", "ifixit", "bifl", "yale", "looria", "openrepair", "eprel"],
        help="Run only these scrapers",
    )
    parser.add_argument(
        "--skip", nargs="+",
        choices=["french_index", "ifixit", "bifl", "yale", "looria", "openrepair", "eprel"],
        help="Skip these scrapers",
    )
    parser.add_argument(
        "--summary-only", action="store_true",
        help="Only print DB summary, do not scrape",
    )
    args = parser.parse_args()

    # Ensure table exists
    init_table()

    if args.summary_only:
        print_summary()
        return

    # Determine which scrapers to run
    # Ordered by priority / reliability (safest first)
    # eprel: official EU REST API — no scraping, very reliable; run last as it is slow
    all_scrapers = ["french_index", "ifixit", "bifl", "yale", "looria", "openrepair", "eprel"]

    if args.only:
        scrapers = [s for s in all_scrapers if s in args.only]
    elif args.skip:
        scrapers = [s for s in all_scrapers if s not in args.skip]
    else:
        scrapers = all_scrapers

    print(f"\n{'='*60}")
    print(f"COMPETITOR SCRAPER — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"Running: {', '.join(scrapers)}")
    print(f"{'='*60}\n")

    results = []
    for scraper_name in scrapers:
        print(f"\n{'─'*40}")
        print(f"Starting: {scraper_name.upper()}")
        print(f"{'─'*40}")
        result = run_scraper(scraper_name)
        results.append(result)

    # Final summary
    print(f"\n{'='*60}")
    print("RUN SUMMARY")
    print(f"{'='*60}")
    for r in results:
        status_icon = "✓" if r["status"] == "ok" else "✗"
        elapsed = r.get("elapsed_s", "?")
        if r["status"] == "ok":
            print(f"  {status_icon} {r['source']:<16}  {r['rows']:>5} rows  {elapsed}s")
        else:
            print(f"  {status_icon} {r['source']:<16}  ERROR: {r['error']}")

    print_summary()


if __name__ == "__main__":
    main()
