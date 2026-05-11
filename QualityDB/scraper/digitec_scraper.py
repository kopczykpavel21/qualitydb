"""
Digitec / Galaxus scraper — finds top-rated products and adds them to QualityDB.

Digitec.ch is Switzerland's largest electronics retailer.  Galaxus.ch is its
sister site covering a broader range of categories (both belong to the Digitec
Galaxus group).  Products carry verified buyer star ratings (1–5) with large
review volumes.

This scraper uses the public GraphQL API exposed by the Digitec/Galaxus
web application, which returns structured product data including ratings and
review counts — no HTML parsing required.

API endpoint:
  POST https://www.digitec.ch/api/graphql
  (same endpoint serves both Digitec and Galaxus data via productTypeId filter)

Normalisation:
  Stars (1–5) → recommend %: (stars / 5) × 100
  Prices are in CHF, stored as-is in Price_CZK column

Dependencies (already installed):
    pip3 install curl_cffi

Usage:
    python3 scraper/digitec_scraper.py
"""

import json
import time
import logging
import sqlite3
import os
import sys

try:
    from curl_cffi import requests
except ImportError:
    print("\n⚠  Missing dependencies. Please run:")
    print("    pip3 install curl_cffi\n")
    sys.exit(1)

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "products.db")

# ── Thresholds ────────────────────────────────────────────────────────────────
MIN_STARS     = 4.0    # Minimum average star rating (out of 5)
MIN_REVIEWS   = 10     # Minimum review count
STOP_BELOW    = 3.8    # Stop category when page avg drops below this
MAX_PAGES     = 6      # Max pages per category
PAGE_SIZE     = 24     # Products per GraphQL request
REQUEST_DELAY = 2.0    # Seconds between requests

# ── Category list ─────────────────────────────────────────────────────────────
# categoryId values are Digitec/Galaxus internal category identifiers.
# These can be confirmed by visiting the Digitec category pages and inspecting
# the GraphQL network request in browser DevTools.
CATEGORIES = [
    {"name": "TVs",              "categoryId": 76},
    {"name": "Smartphones",      "categoryId": 5},
    {"name": "Laptops",          "categoryId": 7},
    {"name": "Tablets",          "categoryId": 6},
    {"name": "Headphones",       "categoryId": 26},
    {"name": "Smartwatches",     "categoryId": 532},
    {"name": "Speakers",         "categoryId": 39},
    {"name": "Monitors",         "categoryId": 8},
    {"name": "Keyboards",        "categoryId": 28},
    {"name": "Mice",             "categoryId": 27},
    {"name": "SSD",              "categoryId": 22},
    {"name": "Coffee Machines",  "categoryId": 324},
    {"name": "Vacuum Cleaners",  "categoryId": 193},
    {"name": "Robot Vacuums",    "categoryId": 1380},
    {"name": "Washing Machines", "categoryId": 122},
    {"name": "Dishwashers",      "categoryId": 127},
    {"name": "Refrigerators",    "categoryId": 119},
    {"name": "Air Purifiers",    "categoryId": 1218},
]

GRAPHQL_URL = "https://www.digitec.ch/api/graphql"

# NOTE: The `productSearch` query field was removed from Digitec's GraphQL schema
# (verified May 2026 — returns "Cannot query field 'productSearch' on type 'Query'").
# To fix this scraper, open Digitec.ch in Chrome DevTools → Network → filter by
# "graphql" → find the product listing request and copy the operation name and
# query. Then replace PRODUCT_SEARCH_QUERY and the variables below.
PRODUCT_SEARCH_QUERY = """
query ProductSearch($categoryId: Int!, $offset: Int!, $limit: Int!) {
  productSearch(
    query: ""
    categoryId: $categoryId
    sortOrder: RATING_DESC
    offset: $offset
    limit: $limit
  ) {
    products {
      id
      name
      nameShort
      brandName
      averageRating
      totalRatings
      cheapestOffer {
        price {
          amountIncl
        }
      }
      canonicalUrl
    }
    totalProducts
  }
}
"""

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            os.path.join(os.path.dirname(__file__), "digitec_scraper.log"),
            encoding="utf-8",
        ),
    ],
)
log = logging.getLogger(__name__)

EXTRA_HEADERS = {
    "Accept-Language": "de-CH,de;q=0.9,en;q=0.8",
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Origin": "https://www.digitec.ch",
    "Referer": "https://www.digitec.ch/",
}


# ── API fetching ──────────────────────────────────────────────────────────────

def fetch_page(category_id: int, page: int, session) -> dict:
    """
    POST one GraphQL request for a category page.
    Returns parsed JSON data or empty dict on error.
    """
    offset = page * PAGE_SIZE
    payload = {
        "query": PRODUCT_SEARCH_QUERY,
        "variables": {
            "categoryId": category_id,
            "offset":     offset,
            "limit":      PAGE_SIZE,
        },
    }
    try:
        resp = session.post(
            GRAPHQL_URL,
            headers=EXTRA_HEADERS,
            json=payload,
            timeout=25,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        log.warning(f"  GraphQL request failed: {e}")
        return {}


def extract_products(data: dict) -> list[dict]:
    """Extract product list from GraphQL response."""
    try:
        items = data["data"]["productSearch"]["products"]
    except (KeyError, TypeError):
        return []

    products = []
    for item in items:
        name    = (item.get("nameShort") or item.get("name") or "").strip()
        brand   = (item.get("brandName") or "").strip()
        full_name = f"{brand} {name}".strip() if brand else name

        rating  = item.get("averageRating")     # float 1.0–5.0
        reviews = item.get("totalRatings", 0)   # integer

        price_chf = None
        try:
            price_chf = item["cheapestOffer"]["price"]["amountIncl"]
        except (KeyError, TypeError):
            pass

        url = item.get("canonicalUrl", "")
        if url and not url.startswith("http"):
            url = "https://www.digitec.ch" + url

        if full_name and rating is not None:
            products.append({
                "Name":              full_name,
                "ProductURL":        url,
                "Stars":             float(rating),
                "RecommendRate_pct": round((float(rating) / 5.0) * 100, 1),
                "ReviewsCount":      reviews or 0,
                "Price_CZK":         float(price_chf) if price_chf else None,
            })

    return products


# ── Database helpers ──────────────────────────────────────────────────────────

def load_existing_names(conn) -> set:
    rows = conn.execute("SELECT lower(Name) FROM products").fetchall()
    return {r[0] for r in rows}


def insert_products(conn, products: list[dict], category: str) -> int:
    existing = load_existing_names(conn)
    inserted = 0
    for p in products:
        key = p["Name"].lower()
        if key in existing:
            continue
        conn.execute(
            """INSERT INTO products
               (Name, Category, ProductURL, Price_CZK,
                RecommendRate_pct, ReviewsCount, source)
               VALUES (?,?,?,?,?,?,?)""",
            (
                p["Name"],
                category,
                p.get("ProductURL", ""),
                p.get("Price_CZK"),
                p.get("RecommendRate_pct"),
                p.get("ReviewsCount", 0),
                "digitec",
            ),
        )
        existing.add(key)
        inserted += 1
    conn.commit()
    return inserted


# ── Main scrape logic ─────────────────────────────────────────────────────────

def scrape_category(cat: dict, session, conn) -> int:
    category_id = cat["categoryId"]
    cat_name    = cat["name"]
    total       = 0

    log.info(f"── {cat_name}  (categoryId={category_id})")

    # First request to get total product count
    first_data  = fetch_page(category_id, 0, session)
    total_count = 0
    try:
        total_count = first_data["data"]["productSearch"]["totalProducts"]
    except (KeyError, TypeError):
        pass

    all_pages = range(MAX_PAGES)
    for page in all_pages:
        if page == 0:
            data = first_data
        else:
            data = fetch_page(category_id, page, session)

        products = extract_products(data)

        if not products:
            log.info("   Empty response — stopping.")
            break

        qualified = [
            p for p in products
            if p["Stars"] >= MIN_STARS and p["ReviewsCount"] >= MIN_REVIEWS
        ]

        lowest = min((p["Stars"] for p in products), default=5.0)

        added = insert_products(conn, qualified, cat_name)
        total += added
        log.info(
            f"   Page {page + 1} | Found {len(products)} | "
            f"Qualified: {len(qualified)} | New: {added} | Lowest ★: {lowest:.1f}"
        )

        # Stop when we've consumed all products or quality drops
        if (page + 1) * PAGE_SIZE >= total_count:
            log.info("   Reached end of category.")
            break
        if lowest < STOP_BELOW:
            log.info(f"   Stars dropped to {lowest:.1f} — stopping early.")
            break

        time.sleep(REQUEST_DELAY)

    return total


def run_scraper() -> dict:
    log.info("=" * 60)
    log.info("QualityDB Digitec/Galaxus Scraper — starting run")
    log.info("=" * 60)

    if not os.path.exists(DB_PATH):
        log.error(f"Database not found at {DB_PATH}.")
        return {"error": "database_not_found"}

    session = requests.Session(impersonate="chrome120")

    conn    = sqlite3.connect(DB_PATH)
    summary = {"categories_scraped": 0, "total_added": 0, "errors": []}

    for cat in CATEGORIES:
        try:
            added = scrape_category(cat, session, conn)
            summary["total_added"]        += added
            summary["categories_scraped"] += 1
        except Exception as e:
            log.error(f"Error scraping {cat['name']}: {e}")
            summary["errors"].append({"category": cat["name"], "error": str(e)})
        time.sleep(REQUEST_DELAY)

    conn.close()
    session.close()

    log.info("=" * 60)
    log.info(
        f"Run complete — {summary['total_added']} new products added "
        f"across {summary['categories_scraped']} categories."
    )
    log.info("=" * 60)
    return summary


if __name__ == "__main__":
    result = run_scraper()
    if result.get("errors"):
        print(f"\n⚠  {len(result['errors'])} category error(s) — check digitec_scraper.log")
    print(f"\n✓  Done. {result['total_added']} new products added to database.")
