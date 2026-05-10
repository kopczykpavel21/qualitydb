"""
Pricerunner scraper — finds top-rated products and adds them to QualityDB.

Pricerunner is Scandinavia's largest price-comparison portal, operating in
Denmark (pricerunner.dk), Sweden (pricerunner.se), and Norway.  It carries
verified buyer ratings (1–5 stars) for hundreds of product categories.

This scraper targets the Danish site (pricerunner.dk) but the category slug
structure is identical across the Nordic markets; to switch to Sweden change
BASE_URL to "https://www.pricerunner.se".

Scraping approach:
  - Category pages sorted by best rated (?sortByPreset=BEST_RATED)
  - HTML parsed with BeautifulSoup (server-side rendered product cards)
  - Stars (1–5) converted to recommend %: (stars / 5) × 100
  - Prices are in DKK, stored as-is in Price_CZK column

Dependencies (already installed):
    pip3 install curl_cffi beautifulsoup4

Usage:
    python3 scraper/pricerunner_scraper.py
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

# ── Thresholds ────────────────────────────────────────────────────────────────
MIN_STARS     = 4.0    # Minimum average star rating (out of 5)
MIN_REVIEWS   = 10     # Minimum review count
STOP_BELOW    = 3.8    # Stop category when stars drop below this
MAX_PAGES     = 6      # Max pages per category
REQUEST_DELAY = 2.0    # Seconds between requests

# ── Category list ─────────────────────────────────────────────────────────────
# Pricerunner category URLs follow the pattern:
#   https://www.pricerunner.dk/cl/{id}-{slug}/?sortByPreset=BEST_RATED&page={N}
# The numeric prefix is the category ID used by Pricerunner internally.
CATEGORIES = [
    {"name": "TVs",              "slug": "33-Fladskjermsfjernsyn"},
    {"name": "Smartphones",      "slug": "1-Mobiltelefoner"},
    {"name": "Laptops",          "slug": "16-Barbaercomputere"},
    {"name": "Tablets",          "slug": "38-Tablets"},
    {"name": "Headphones",       "slug": "79-Hovedtelefoner"},
    {"name": "Smartwatches",     "slug": "574-Smartwatches"},
    {"name": "Speakers",         "slug": "80-Bluetooth-hojttalere"},
    {"name": "Monitors",         "slug": "37-Skaerme"},
    {"name": "Washing Machines", "slug": "58-Vaskemaskiner"},
    {"name": "Dishwashers",      "slug": "60-Opvaskemaskiner"},
    {"name": "Refrigerators",    "slug": "56-Koleskabe"},
    {"name": "Coffee Machines",  "slug": "97-Kaffemaskiner"},
    {"name": "Vacuum Cleaners",  "slug": "69-Stovsuger"},
    {"name": "Robot Vacuums",    "slug": "379-Robotstovsuger"},
    {"name": "SSD",              "slug": "1302-SSD-drev"},
    {"name": "Air Purifiers",    "slug": "1138-Luftrenere"},
]

BASE_URL = "https://www.pricerunner.dk"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            os.path.join(os.path.dirname(__file__), "pricerunner_scraper.log"),
            encoding="utf-8",
        ),
    ],
)
log = logging.getLogger(__name__)

EXTRA_HEADERS = {
    "Accept-Language": "da-DK,da;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.pricerunner.dk/",
}


# ── Parsing helpers ───────────────────────────────────────────────────────────

def _parse_stars(text: str):
    """'4,3 ud af 5' or '4.3' → 4.3.  Returns None if unparseable."""
    if not text:
        return None
    m = re.search(r"(\d)[,.](\d)", text)
    if m:
        return float(f"{m.group(1)}.{m.group(2)}")
    m = re.search(r"(\d+)", text)
    v = float(m.group(1)) if m else None
    return v if v and v <= 5 else None


def _parse_reviews(text: str) -> int:
    """'(1.234 anmeldelser)' → 1234."""
    if not text:
        return 0
    digits = re.sub(r"[^\d]", "", text)
    return int(digits) if digits else 0


def _parse_price_dkk(text: str):
    """'1.249 kr.' or '1249,00' → 1249.0 (stored in Price_CZK as DKK value)."""
    if not text:
        return None
    clean = re.sub(r"[^\d,.]", "", text).replace(",", ".")
    # Handle European thousand-separator: "1.249" means 1249
    parts = clean.split(".")
    if len(parts) == 2 and len(parts[1]) == 3:
        clean = "".join(parts)
    m = re.search(r"(\d+(?:\.\d+)?)", clean)
    return float(m.group(1)) if m else None


# ── Fetch & parse ─────────────────────────────────────────────────────────────

def fetch_page(slug: str, page: int, session) -> list[dict]:
    """Fetch one category page and return list of raw product dicts."""
    url = f"{BASE_URL}/cl/{slug}/?sortByPreset=BEST_RATED&page={page}"
    try:
        resp = session.get(url, headers=EXTRA_HEADERS, timeout=25)
        resp.raise_for_status()
    except Exception as e:
        log.warning(f"  Fetch failed: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    products = []

    # Pricerunner product items appear in elements with class "product-card"
    # or inside the product list container.
    items = (
        soup.select("[class*='ProductCard']")
        or soup.select("[class*='product-card']")
        or soup.select("[data-testid='product-card']")
        or soup.select("li[class*='product']")
    )

    if not items:
        log.debug("  No product items found — possibly last page or layout change.")
        return []

    for item in items:
        # ── Name ──────────────────────────────────────────────────────────────
        name_el = (
            item.select_one("a[class*='product-name']")
            or item.select_one("[class*='ProductTitle'] a")
            or item.select_one("[class*='product-title'] a")
            or item.select_one("h2 a")
            or item.select_one("h3 a")
            or item.select_one("a[href*='/pl/']")
        )
        name = name_el.get_text(strip=True) if name_el else ""
        href = name_el.get("href", "") if name_el else ""
        url_product = (BASE_URL + href) if href.startswith("/") else href

        # ── Stars ─────────────────────────────────────────────────────────────
        stars_el = (
            item.select_one("[aria-label*='ud af 5']")
            or item.select_one("[aria-label*='stjerner']")
            or item.select_one("[class*='RatingStars']")
            or item.select_one("[class*='rating']")
        )
        stars_text = ""
        if stars_el:
            stars_text = (
                stars_el.get("aria-label")
                or stars_el.get("title")
                or stars_el.get_text()
            )
        stars = _parse_stars(stars_text)

        # ── Reviews ───────────────────────────────────────────────────────────
        reviews_el = (
            item.select_one("[class*='review-count']")
            or item.select_one("[class*='RatingCount']")
            or item.select_one("[class*='anmeldelser']")
        )
        reviews = _parse_reviews(reviews_el.get_text() if reviews_el else "")

        # ── Price ─────────────────────────────────────────────────────────────
        price_el = (
            item.select_one("[class*='Price']")
            or item.select_one("[class*='price']")
        )
        price_dkk = _parse_price_dkk(price_el.get_text() if price_el else "")

        if name:
            products.append({
                "Name":              name,
                "ProductURL":        url_product,
                "Stars":             stars,
                "RecommendRate_pct": round((stars / 5.0) * 100, 1) if stars else None,
                "ReviewsCount":      reviews,
                "Price_CZK":         price_dkk,  # stored as DKK, label is a misnomer
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
                "pricerunner",
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

    log.info(f"── {cat_name}  (/cl/{slug}/)")

    for page in range(1, MAX_PAGES + 1):
        log.info(f"   Page {page}")
        products = fetch_page(slug, page, session)

        if not products:
            log.info("   Empty page — stopping.")
            break

        qualified = [
            p for p in products
            if p.get("Stars") is not None
            and p["Stars"] >= MIN_STARS
            and p["ReviewsCount"] >= MIN_REVIEWS
        ]

        stars_vals = [p["Stars"] for p in products if p.get("Stars")]
        lowest = min(stars_vals) if stars_vals else 5.0

        added = insert_products(conn, qualified, cat_name)
        total += added
        log.info(
            f"   Found {len(products)} | Qualified: {len(qualified)} | "
            f"New: {added} | Lowest ★: {lowest:.1f}"
        )

        if lowest < STOP_BELOW:
            log.info(f"   Stars dropped to {lowest:.1f} — stopping early.")
            break

        time.sleep(REQUEST_DELAY)

    return total


def run_scraper() -> dict:
    log.info("=" * 60)
    log.info("QualityDB Pricerunner.dk Scraper — starting run")
    log.info("=" * 60)

    if not os.path.exists(DB_PATH):
        log.error(f"Database not found at {DB_PATH}.")
        return {"error": "database_not_found"}

    session = requests.Session(impersonate="chrome120")

    try:
        log.info("Warming up session (visiting Pricerunner.dk)…")
        session.get(
            BASE_URL,
            headers={**EXTRA_HEADERS, "Accept": "text/html,*/*"},
            timeout=20,
        )
        time.sleep(2.0)
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
        print(f"\n⚠  {len(result['errors'])} category error(s) — check pricerunner_scraper.log")
    print(f"\n✓  Done. {result['total_added']} new products added to database.")
