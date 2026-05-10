"""
Open Repair Alliance scraper.

Data source: https://github.com/openrepair/data (CC BY 4.0)
Latest aggregate CSV with 305,000+ community repair events across Europe.

Each row = one repair attempt with brand, category, and outcome:
  Fixed | Repairable | End of life | Unknown

Score: fix_rate = (Fixed + Repairable) / (Fixed + Repairable + End_of_life)
→ score_normalized = fix_rate * 100 (0–100, higher = more repairable)

Minimum 20 events per brand/category required to avoid noise.
"""

import csv
import hashlib
import io

import requests

from scraper_competitors_config import REQUEST_TIMEOUT, USER_AGENT, canonical_category
from scraper_competitors_db import count_records, init_table, upsert_record

SOURCE = "openrepair"
DATA_URL = "https://raw.githubusercontent.com/openrepair/data/master/aggregated/202507/OpenRepairData_v0.3_aggregate_202507.csv"

# Category mapping from Open Repair Alliance → IKOR canonical
CATEGORY_MAP = {
    "Vacuum":                          "Vysavače",
    "Laptop":                          "Notebooky",
    "Mobile":                          "Smartphony",
    "Tablet":                          "Tablety",
    "Coffee maker":                    "Kávovary",
    "Washing machine":                 "Pračky",
    "Dishwasher":                      "Myčky",
    "Fridge":                          "Ledničky",
    "Flat screen":                     "Televize",
    "TV and gaming-related accessories": "Televize",
    "Printer/scanner":                 "Tiskárny",
    "Headphones":                      "Sluchátka",
    "Sewing machine":                  "Šicí stroje",
    "Food processor":                  "Kuchyňské spotřebiče",
    "Small kitchen item":              "Kuchyňské spotřebiče",
    "Toaster":                         "Kuchyňské spotřebiče",
    "Kettle":                          "Kuchyňské spotřebiče",
    "Iron":                            "Žehličky",
    "Power tool":                      "Nářadí",
    "Hair & beauty item":              "Osobní péče",
    "Large home electrical":           "Velké spotřebiče",
    "Small home electrical":           "Malé spotřebiče",
}

MIN_EVENTS = 20  # minimum repair events per brand/category to include


def _make_product_id(brand: str, category: str) -> str:
    key = f"{SOURCE}|{brand.lower()}|{category.lower()}"
    return hashlib.md5(key.encode()).hexdigest()[:16]


def scrape() -> int:
    """Download Open Repair Alliance aggregate CSV and compute fix rates per brand/category."""
    init_table()
    total = 0

    print(f"\n[openrepair] Downloading repair event data from GitHub...")
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT

    try:
        resp = session.get(DATA_URL, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
    except Exception as e:
        print(f"  [!] Download failed: {e}")
        return 0

    content = resp.content.decode("utf-8-sig", errors="replace")
    print(f"  Downloaded {len(content):,} bytes")

    # Parse and aggregate by brand + product_category
    reader = csv.DictReader(io.StringIO(content))

    # Aggregate counts
    from collections import defaultdict
    stats = defaultdict(lambda: {"Fixed": 0, "Repairable": 0, "End of life": 0, "Unknown": 0})

    for row in reader:
        brand = row.get("brand", "").strip()
        category = row.get("product_category", "").strip()
        status = row.get("repair_status", "").strip()

        if not brand or brand.lower() in ("", "unknown", "n/a", "-", "?", "various", "no brand"):
            continue
        if not category:
            continue

        key = (brand, category)
        if status in stats[key]:
            stats[key][status] += 1
        else:
            stats[key]["Unknown"] += 1

    print(f"  Found {len(stats):,} brand/category combinations")

    # Build records
    inserted = 0
    for (brand, category), counts in stats.items():
        fixed = counts["Fixed"]
        repairable = counts["Repairable"]
        eol = counts["End of life"]
        total_known = fixed + repairable + eol

        if total_known < MIN_EVENTS:
            continue

        # Fix rate: items that could be fixed (fixed + still repairable if parts found)
        # vs items that were end of life / irreparable
        fix_rate = (fixed + repairable) / total_known
        score = round(fix_rate * 100, 1)

        canon = CATEGORY_MAP.get(category) or canonical_category(category.lower())

        record = {
            "source":             SOURCE,
            "source_url":         DATA_URL,
            "product_name":       f"{brand} {category}",
            "brand":              brand,
            "model":              None,
            "product_category":   category,
            "canonical_category": canon,
            "raw_score":          round(fix_rate * 100, 1),
            "raw_score_min":      0.0,
            "raw_score_max":      100.0,
            "raw_score_label":    f"{score}% fixable ({total_known} events)",
            "score_normalized":   score,
            "sub_scores_json":    {
                "fixed": fixed,
                "repairable": repairable,
                "end_of_life": eol,
                "total_events": total_known,
                "fix_rate_pct": score,
            },
            "meta_json":          {
                "dataset": "OpenRepairData_v0.3_202507",
                "unknown_count": counts["Unknown"],
            },
            "source_product_id":  _make_product_id(brand, category),
        }

        upsert_record(record)
        inserted += 1
        total += 1

    print(f"  {inserted} brand/category combinations with ≥{MIN_EVENTS} events")
    print(f"[openrepair] Done — {total} rows. DB now has {count_records(SOURCE)} {SOURCE} records.")
    return total


if __name__ == "__main__":
    scrape()
