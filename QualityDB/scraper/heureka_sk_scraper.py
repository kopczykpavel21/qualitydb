"""
Heureka.sk scraper — finds top-selling products and adds them to QualityDB.

Heureka.sk is Slovakia's largest price-comparison portal (sister site of
Heureka.cz, same Heureka Group).  Products carry a recommendation percentage
(0–100 %) derived from verified buyer reviews, identical in format to .cz.

Scraping approach:
  - Category pages sorted by rating (?sort=rating)
  - 3 pages per category (~24 products/page = ~72 products per category)
  - No minimum rating threshold — all top products are stored
  - HTML parsed with BeautifulSoup (server-side rendered)
  - Prices are in EUR, stored as-is in Price_CZK column
  - Uses curl_cffi Chrome TLS impersonation (same as Heureka.cz scraper)

Category URL format:
  https://{slug}.heureka.sk/?sort=rating         (page 1)
  https://{slug}.heureka.sk/?sort=rating&f={N}   (page N)

Category slugs verified May 2026 by browsing heureka.sk navigation.

Dependencies (already installed):
    pip3 install curl_cffi beautifulsoup4

Usage:
    python3 scraper/heureka_sk_scraper.py
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
MAX_PAGES     = 3      # Pages per category (~24 products/page)
REQUEST_DELAY = 2.0    # Seconds between requests

# ── Category list ─────────────────────────────────────────────────────────────
# Slugs verified May 2026 by navigating heureka.sk and testing each URL.
CATEGORIES = [
    {"name": "TVs",              "slug": "televizor"},
    {"name": "Smartphones",      "slug": "mobilne-telefony"},
    {"name": "Laptops",          "slug": "notebooky"},
    {"name": "Tablets",          "slug": "tablety"},
    {"name": "Headphones",       "slug": "sluchadla"},
    {"name": "Monitors",         "slug": "monitory"},
    {"name": "Speakers",         "slug": "bluetooth-reproduktory"},
    {"name": "Smartwatches",     "slug": "inteligentne-hodinky"},
    {"name": "Keyboards",        "slug": "klavesnice"},
    {"name": "Mice",             "slug": "mysi"},
    {"name": "Washing Machines", "slug": "pracky"},
    {"name": "Dishwashers",      "slug": "umyvacky-riadu"},
    {"name": "Refrigerators",    "slug": "chladnicky"},
    {"name": "Coffee Machines",  "slug": "kapsulove-kavovary"},
    {"name": "Vacuum Cleaners",  "slug": "vysavace"},
    {"name": "Robot Vacuums",    "slug": "roboticke-vysavace"},
]

BASE_DOMAIN = "heureka.sk"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            os.path.join(os.path.dirname(__file__), "heureka_sk_scraper.log"),
            encoding="utf-8",
        ),
    ],
)
log = logging.getLogger(__name__)

EXTRA_HEADERS = {
    "Accept-Language": "sk-SK,sk;q=0.9,en;q=0.8",
}


# ── Parsing helpers ───────────────────────────────────────────────────────────

def parse_rating(text: str):
    """'92 %' → 92.0,  '100%' → 100.0,  None if unparseable."""
    if not text:
        return None
    m = re.search(r"(\d+(?:[.,]\d+)?)\s*%", text)
    return float(m.group(1).replace(",", ".")) if m else None


def parse_reviews(text: str) -> int:
    """'2 recenzie' → 2,  '162 hodnotení' → 162."""
    if not text:
        return 0
    m = re.search(r"(\d[\d\s]*)", text)
    return int(re.sub(r"\s", "", m.group(1))) if m else 0


def parse_price(text: str):
    """'154,91 – 249,00 €' → 154.91  (takes the lower bound)."""
    if not text:
        return None
    m = re.search(r"(\d[\d\s]*)", text)
    return float(re.sub(r"\s", "", m.group(1)).replace(",", ".")) if m else None


# ── Page scraping ─────────────────────────────────────────────────────────────

def scrape_page(url: str, session) -> list:
    """Fetch one Heureka.sk listing page and return a list of product dicts."""
    try:
        resp = session.get(url, headers=EXTRA_HEADERS, timeout=20)
        resp.raise_for_status()
    except Exception as e:
        log.warning(f"  Request failed: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    cards = soup.select(".c-product")
    if not cards:
        log.debug(f"  No .c-product cards found at {url}")
        return []

    products = []
    for card in cards:
        # Name & URL
        name_el = card.select_one(".c-product__link")
        if not name_el:
            continue
        name        = name_el.get_text(strip=True)
        product_url = name_el.get("href", "")

        # Rating (percentage)
        rating_el = card.select_one(".c-rating-widget__value")
        rating    = parse_rating(rating_el.get_text() if rating_el else "")

        # Review count
        review_el = card.select_one(".c-star-rating__reviews, .c-product__review-count")
        reviews   = parse_reviews(review_el.get_text() if review_el else "")

        # Price (EUR lower bound)
        price_el = card.select_one(".c-product__price, [class*='price']")
        price    = parse_price(price_el.get_text() if price_el else "")

        if name:
            products.append({
                "Name":              name,
                "ProductURL":        product_url,
                "RecommendRate_pct": rating,
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
                "heureka.sk",
            ),
        )
        existing.add(key)
        inserted += 1
    conn.commit()
    return inserted


# ── Category scraper ──────────────────────────────────────────────────────────

def scrape_category(cat: dict, session, conn) -> int:
    slug     = cat["slug"]
    cat_name = cat["name"]
    total    = 0
    base_url = f"https://{slug}.{BASE_DOMAIN}"

    log.info(f"── {cat_name}  ({slug}.{BASE_DOMAIN})")

    for page in range(1, MAX_PAGES + 1):
        url = (f"{base_url}/?sort=rating" if page == 1
               else f"{base_url}/?sort=rating&f={page}")
        log.info(f"   Page {page}: {url}")

        products = scrape_page(url, session)
        if not products:
            log.info("   No products — stopping.")
            break

        added  = insert_products(conn, products, cat_name)
        total += added
        log.info(f"   Found {len(products)} | New: {added}")

        time.sleep(REQUEST_DELAY)

    return total


# ── Orchestrator ──────────────────────────────────────────────────────────────

def run_scraper() -> dict:
    log.info("=" * 60)
    log.info("QualityDB Heureka.sk Scraper — starting run")
    log.info("=" * 60)

    if not os.path.exists(DB_PATH):
        log.error(f"Database not found at {DB_PATH}.")
        return {"error": "database_not_found"}

    session = requests.Session(impersonate="chrome124")

    # Warm up session
    try:
        log.info("Warming up session (visiting heureka.sk)…")
        session.get("https://www.heureka.sk/", headers=EXTRA_HEADERS, timeout=20)
        time.sleep(1.5)
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
        print(f"\n⚠  {len(result['errors'])} category error(s) — check heureka_sk_scraper.log")
    print(f"\n✓  Done. {result['total_added']} new products added to database.")
