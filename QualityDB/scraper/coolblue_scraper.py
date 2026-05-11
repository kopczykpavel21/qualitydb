"""
Coolblue.nl scraper — finds top-rated products and adds them to QualityDB.

Coolblue is the Netherlands' and Belgium's leading consumer-electronics
retailer, consistently ranked among Europe's highest-rated online shops for
customer service.  Products carry star ratings (1–5) and verified buyer
review counts displayed directly in category listing HTML.

Scraping approach:
  - Category listing pages sorted by review score (sort=review-score-desc)
  - HTML parsed with BeautifulSoup (server-side rendered, no heavy JS needed)
  - Stars (1–5) converted to recommend %: (stars / 5) × 100
  - Prices are in EUR, stored as-is in Price_CZK column

Dependencies (already installed):
    pip3 install curl_cffi beautifulsoup4

Usage:
    python3 scraper/coolblue_scraper.py
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
MAX_PAGES     = 6      # Max pages per category (24 products/page)
REQUEST_DELAY = 2.0    # Seconds between requests

# ── Category list ─────────────────────────────────────────────────────────────
# URL slug used in https://www.coolblue.nl/categorie/{slug}/
# Append ?sort=review-score-desc&offset={N}&limit=24 for sorted pagination.
CATEGORIES = [
    {"name": "TVs",              "slug": "televisies"},
    {"name": "Smartphones",      "slug": "smartphones"},
    {"name": "Laptops",          "slug": "laptops"},
    {"name": "Tablets",          "slug": "tablets"},
    {"name": "Headphones",       "slug": "hoofdtelefoons"},
    {"name": "Smartwatches",     "slug": "smartwatches"},
    {"name": "Speakers",         "slug": "draadloze-luidsprekers"},
    {"name": "Monitors",         "slug": "monitoren"},
    {"name": "Washing Machines", "slug": "wasmachines"},
    {"name": "Dishwashers",      "slug": "vaatwassers"},
    {"name": "Refrigerators",    "slug": "koelkasten"},
    {"name": "Coffee Machines",  "slug": "koffiezetapparaten"},
    {"name": "Vacuum Cleaners",  "slug": "stofzuigers"},
    {"name": "Robot Vacuums",    "slug": "robotstofzuigers"},
    {"name": "Air Purifiers",    "slug": "luchtreiniger"},
    {"name": "SSD",              "slug": "interne-ssds"},
    {"name": "Keyboards",        "slug": "toetsenborden"},
    {"name": "Mice",             "slug": "computermuizen"},
]

BASE_URL      = "https://www.coolblue.nl"
PAGE_SIZE     = 24

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            os.path.join(os.path.dirname(__file__), "coolblue_scraper.log"),
            encoding="utf-8",
        ),
    ],
)
log = logging.getLogger(__name__)

EXTRA_HEADERS = {
    "Accept-Language": "nl-NL,nl;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.coolblue.nl/",
}


# ── Parsing helpers ───────────────────────────────────────────────────────────

def _parse_stars(text: str):
    """'4,3 van 5' or '4.3 out of 5' or '4.3' → 4.3."""
    if not text:
        return None
    m = re.search(r"(\d)[,.](\d)", text)
    if m:
        return float(f"{m.group(1)}.{m.group(2)}")
    m = re.search(r"(\d+)", text)
    v = float(m.group(1)) if m else None
    return v if v and v <= 5 else None


def _parse_reviews(text: str) -> int:
    """'(1.234 recensies)' or '1234 beoordelingen' → 1234."""
    if not text:
        return 0
    raw = re.sub(r"[.,\s]", "", re.search(r"([\d.,\s]+)", text).group(1)) if re.search(r"[\d]", text) else ""
    return int(raw) if raw.isdigit() else 0


def _parse_price_eur(text: str):
    """'€ 249,99' or '249.99' → 249.99."""
    if not text:
        return None
    clean = text.replace("\xa0", "").replace(",", ".")
    m = re.search(r"(\d+\.\d+|\d+)", clean)
    return float(m.group(1)) if m else None


# ── Fetch & parse ─────────────────────────────────────────────────────────────

def fetch_page(slug: str, page: int, session) -> list[dict]:
    """Fetch one category page and return list of raw product dicts."""
    offset = (page - 1) * PAGE_SIZE
    url = (
        f"{BASE_URL}/categorie/{slug}/"
        f"?sort=review-score-desc&offset={offset}&limit={PAGE_SIZE}"
    )
    try:
        resp = session.get(url, headers=EXTRA_HEADERS, timeout=25)
        resp.raise_for_status()
    except Exception as e:
        log.warning(f"  Fetch failed: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    products = []

    # Coolblue product cards use class "product-card" or "js-product-card"
    items = (
        soup.select("div.product-card")
        or soup.select("[class*='product-card']")
        or soup.select("li[class*='product']")
    )

    if not items:
        # Check if this is a JS-rendered page (Coolblue uses React, no SSR for product lists)
        if resp.text and len(resp.text) > 10000 and "react" in resp.text.lower():
            log.warning(
                "Coolblue product listings are client-side rendered by React. "
                "Static HTML scraping cannot access the product data. "
                "Use Playwright for this scraper: pip install playwright"
            )
        else:
            log.debug("  No product cards found — possibly last page or layout change.")
        return []

    for item in items:
        # ── Name ──────────────────────────────────────────────────────────────
        name_el = (
            item.select_one("a.product-card__title")
            or item.select_one("[class*='product-card__title']")
            or item.select_one("h2 a")
            or item.select_one("h3 a")
            or item.select_one("a[href*='/product']")
        )
        name = name_el.get_text(strip=True) if name_el else ""
        href = name_el.get("href", "") if name_el else ""
        url_product = (BASE_URL + href) if href.startswith("/") else href

        # ── Stars ─────────────────────────────────────────────────────────────
        # Coolblue shows stars as aria-label="4,5 van 5" or in a title attribute
        stars_el = (
            item.select_one("[aria-label*='van 5']")
            or item.select_one("[class*='rating']")
            or item.select_one("[class*='star']")
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
            or item.select_one("[class*='recensies']")
            or item.select_one("[class*='beoordelingen']")
        )
        reviews = _parse_reviews(reviews_el.get_text() if reviews_el else "")

        # ── Price ─────────────────────────────────────────────────────────────
        price_el = (
            item.select_one("[class*='sales-price']")
            or item.select_one("[class*='product-card__price']")
            or item.select_one("[class*='price']")
        )
        price_eur = _parse_price_eur(price_el.get_text() if price_el else "")

        if name:
            products.append({
                "Name":              name,
                "ProductURL":        url_product,
                "Stars":             stars,
                "RecommendRate_pct": round((stars / 5.0) * 100, 1) if stars else None,
                "ReviewsCount":      reviews,
                "Price_CZK":         price_eur,
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
                "coolblue",
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

    log.info(f"── {cat_name}  (/categorie/{slug}/)")

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
    log.info("QualityDB Coolblue.nl Scraper — starting run")
    log.info("=" * 60)

    if not os.path.exists(DB_PATH):
        log.error(f"Database not found at {DB_PATH}.")
        return {"error": "database_not_found"}

    session = requests.Session(impersonate="chrome120")

    try:
        log.info("Warming up session (visiting Coolblue.nl)…")
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
        print(f"\n⚠  {len(result['errors'])} category error(s) — check coolblue_scraper.log")
    print(f"\n✓  Done. {result['total_added']} new products added to database.")
