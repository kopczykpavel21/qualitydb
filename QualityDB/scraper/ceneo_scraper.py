"""
Ceneo.pl scraper — finds top-rated products and adds them to QualityDB.

Ceneo.pl is Poland's largest price-comparison portal with verified buyer reviews.
Products carry star ratings (1–5 scale displayed as X/5) and review counts.

Scraping approach:
  - Category pages sorted by score (sort code 5 in URL)
  - 3 pages per category (~30 products/page)
  - No minimum rating threshold — all products stored (unrated products get NULL)
  - HTML parsed with BeautifulSoup from server-rendered pages
  - Prices are in PLN (zł), stored as-is in Price_CZK column
  - Warm-up homepage visit required for cookies (otherwise bot-blocked)

URL format:
  /{slug};0020-30-{offset}-0-5.htm
  where offset = (page - 1) * 30, sort code 5 = by score descending

Category slugs verified May 2026 by navigating ceneo.pl.

Dependencies (already installed):
    pip3 install curl_cffi beautifulsoup4

Usage:
    python3 scraper/ceneo_scraper.py
"""

import re
import time
import logging
import sqlite3
import os
import sys

try:
    from curl_cffi import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("\n⚠  Missing dependencies. Please run:")
    print("    pip3 install curl_cffi beautifulsoup4\n")
    sys.exit(1)

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "products.db")

# ── Settings ──────────────────────────────────────────────────────────────────
MAX_PAGES     = 3      # Pages per category (~30 products/page)
PAGE_SIZE     = 30     # Products per page (Ceneo default)
REQUEST_DELAY = 3.5    # Seconds between requests (Ceneo rate-limits aggressively)

BASE_URL = "https://www.ceneo.pl"

# ── Category list ─────────────────────────────────────────────────────────────
# Slugs verified May 2026 by navigating ceneo.pl.
# URL pattern: /{slug};0020-30-{offset}-0-5.htm  (sort=5 → by score desc)
CATEGORIES = [
    {"name": "TVs",              "slug": "Telewizory"},
    {"name": "Smartphones",      "slug": "Telefony_komorkowe"},
    {"name": "Laptops",          "slug": "Laptopy"},
    {"name": "Tablets",          "slug": "Tablety"},
    {"name": "Headphones",       "slug": "Sluchawki"},
    {"name": "Monitors",         "slug": "Monitory"},
    {"name": "Speakers",         "slug": "Glosniki_przenosne"},
    {"name": "Smartwatches",     "slug": "Smartwatche"},
    {"name": "Keyboards",        "slug": "Klawiatury"},
    {"name": "Washing Machines", "slug": "Pralki"},
    {"name": "Dishwashers",      "slug": "Zmywarki"},
    {"name": "Refrigerators",    "slug": "Lodowki"},
    {"name": "Coffee Machines",  "slug": "Ekspresy_do_kawy"},
    {"name": "Vacuum Cleaners",  "slug": "Odkurzacze"},
    {"name": "Robot Vacuums",    "slug": "Odkurzacze_automatyczne"},
    {"name": "SSD",              "slug": "Dyski_SSD"},
    {"name": "Air Purifiers",    "slug": "Oczyszczacze_powietrza"},
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            os.path.join(os.path.dirname(__file__), "ceneo_scraper.log"),
            encoding="utf-8",
        ),
    ],
)
log = logging.getLogger(__name__)

EXTRA_HEADERS = {
    "Accept-Language": "pl-PL,pl;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": BASE_URL,
}


# ── Parsing helpers ───────────────────────────────────────────────────────────

def _parse_score(text: str):
    """'4,8 / 5' → 4.8.  Returns None if no rating found."""
    m = re.search(r"(\d[,\.]\d)\s*/\s*5", text)
    return float(m.group(1).replace(",", ".")) if m else None


def _parse_reviews(text: str) -> int:
    """'5 297 opinii' → 5297.  '72 opinie' → 72."""
    m = re.search(r"([\d\s]+)\s*opini", text, re.IGNORECASE)
    if not m:
        return 0
    cleaned = re.sub(r"\s", "", m.group(1))
    return int(cleaned) if cleaned else 0


def _parse_price_pln(text: str):
    """'3 524,67 zł' → 3524.67."""
    m = re.search(r"([\d\s]+[,\.]\d{2})\s*zł", text)
    return float(re.sub(r"\s", "", m.group(1)).replace(",", ".")) if m else None


# ── Fetch & parse ─────────────────────────────────────────────────────────────

def fetch_page(slug: str, page: int, session) -> list:
    """Fetch one category page (sorted by score) and return list of product dicts."""
    offset = (page - 1) * PAGE_SIZE
    url = f"{BASE_URL}/{slug};0020-{PAGE_SIZE}-{offset}-0-5.htm"
    try:
        resp = session.get(url, headers=EXTRA_HEADERS, timeout=25)
        resp.raise_for_status()
    except Exception as e:
        log.warning(f"  Fetch failed: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    rows = soup.select(".cat-prod-row")
    if not rows:
        log.debug(f"  No .cat-prod-row elements found at {url}")
        return []

    products = []
    for row in rows:
        # Name via title attribute on the product link
        name_el = row.select_one("a[title]")
        if not name_el:
            continue
        name = name_el.get("title", "").strip()
        href = name_el.get("href", "")
        url_product = (BASE_URL + href) if href.startswith("/") else href

        # Full row text for regex extraction
        text = row.get_text(" ", strip=True)

        score    = _parse_score(text)
        pct      = round((score / 5.0) * 100, 1) if score else None
        reviews  = _parse_reviews(text)
        price    = _parse_price_pln(text)

        if name:
            products.append({
                "Name":              name,
                "ProductURL":        url_product,
                "Stars":             score,
                "RecommendRate_pct": pct,
                "ReviewsCount":      reviews,
                "Price_CZK":         price,
            })

    return products


# ── Database helpers ──────────────────────────────────────────────────────────

def load_existing_names(conn) -> set:
    rows = conn.execute("SELECT lower(Name) FROM products").fetchall()
    return {r[0] for r in rows}


def insert_products(conn, products: list, category: str) -> int:
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
                "ceneo.pl",
            ),
        )
        existing.add(key)
        inserted += 1
    conn.commit()
    return inserted


# ── Main scrape logic ─────────────────────────────────────────────────────────

def scrape_category(cat: dict, session, conn) -> int:
    slug     = cat["slug"]
    cat_name = cat["name"]
    total    = 0

    log.info(f"── {cat_name}  (/{slug})")

    for page in range(1, MAX_PAGES + 1):
        log.info(f"   Page {page}")
        products = fetch_page(slug, page, session)

        if not products:
            log.info("   Empty page — stopping.")
            break

        added  = insert_products(conn, products, cat_name)
        total += added
        log.info(f"   Found {len(products)} | New: {added}")

        time.sleep(REQUEST_DELAY)

    return total


# ── Orchestrator ──────────────────────────────────────────────────────────────

def run_scraper() -> dict:
    log.info("=" * 60)
    log.info("QualityDB Ceneo.pl Scraper — starting run")
    log.info("=" * 60)

    if not os.path.exists(DB_PATH):
        log.error(f"Database not found at {DB_PATH}.")
        return {"error": "database_not_found"}

    session = requests.Session(impersonate="chrome124")

    # Warm-up: visit homepage to obtain cookies and bypass bot check
    try:
        log.info("Warming up session (visiting ceneo.pl)…")
        session.get(BASE_URL + "/", headers=EXTRA_HEADERS, timeout=20)
        time.sleep(4.0)  # Longer cool-down after homepage to avoid bot detection
    except Exception as e:
        log.warning(f"Warm-up failed ({e}) — continuing anyway.")

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
        time.sleep(REQUEST_DELAY + 1.5)  # Extra pause between categories

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
        print(f"\n⚠  {len(result['errors'])} category error(s) — check ceneo_scraper.log")
    print(f"\n✓  Done. {result['total_added']} new products added to database.")
