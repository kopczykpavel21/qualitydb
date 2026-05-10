"""
Geizhals.at scraper — finds top-rated products and adds them to QualityDB.

Geizhals.at is Austria's largest price-comparison site, covering AT, DE, and
parts of the broader DACH market. Products carry star ratings (1–5) and
verified buyer review counts.

Scraping approach:
  - Category listing pages sorted by recommendation rate (sort=empf_desc)
  - HTML parsed with BeautifulSoup (server-side rendered content)
  - Stars converted to recommend %: (stars / 5) × 100
  - Prices are in EUR, stored as-is in Price_CZK column (EUR value)

Dependencies (already installed):
    pip3 install curl_cffi beautifulsoup4

Usage:
    python3 scraper/geizhals_scraper.py
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
MIN_STARS      = 4.0    # Minimum average star rating (out of 5)
MIN_REVIEWS    = 10     # Minimum number of buyer ratings
STOP_BELOW     = 3.8    # Stop category when stars drop below this
MAX_PAGES      = 6      # Max pages per category (30 products/page)
REQUEST_DELAY  = 2.5    # Seconds between requests (Geizhals is rate-sensitive)

# ── Category list ─────────────────────────────────────────────────────────────
# Geizhals category codes used in ?cat=CODE URL parameter.
# Sorted by recommendation: ?sort=empf_desc
CATEGORIES = [
    {"name": "TVs",              "slug": "tvall"},
    {"name": "Smartphones",      "slug": "mob"},
    {"name": "Laptops",          "slug": "nb"},
    {"name": "Tablets",          "slug": "tab"},
    {"name": "Headphones",       "slug": "headphoneall"},
    {"name": "Monitors",         "slug": "monitor"},
    {"name": "Smartwatches",     "slug": "smartwatch"},
    {"name": "Speakers",         "slug": "lspread"},
    {"name": "Washing Machines", "slug": "wm"},
    {"name": "Dishwashers",      "slug": "gspl"},
    {"name": "Refrigerators",    "slug": "kuehlschraenke"},
    {"name": "Coffee Machines",  "slug": "espresso"},
    {"name": "Vacuum Cleaners",  "slug": "staubsauger"},
    {"name": "Robot Vacuums",    "slug": "robotstaubsauger"},
    {"name": "SSD",              "slug": "ssd_intern"},
    {"name": "Keyboards",        "slug": "tastatur"},
    {"name": "Mice",             "slug": "maus"},
]

BASE_URL = "https://geizhals.at/"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            os.path.join(os.path.dirname(__file__), "geizhals_scraper.log"),
            encoding="utf-8",
        ),
    ],
)
log = logging.getLogger(__name__)

EXTRA_HEADERS = {
    "Accept-Language": "de-AT,de;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://geizhals.at/",
}


# ── Parsing helpers ───────────────────────────────────────────────────────────

def _parse_stars(text: str):
    """'4,3 von 5' or '4.3 / 5' → 4.3.  Returns None if unparseable."""
    if not text:
        return None
    m = re.search(r"(\d)[,.](\d)", text)
    if m:
        return float(f"{m.group(1)}.{m.group(2)}")
    m = re.search(r"(\d+(?:\.\d+)?)", text)
    return float(m.group(1)) if m else None


def _parse_reviews(text: str) -> int:
    """'(1.234 Bewertungen)' or '1234 Bewertungen' → 1234."""
    if not text:
        return 0
    m = re.search(r"([\d.,\s]+)", text)
    if not m:
        return 0
    raw = re.sub(r"[.,\s]", "", m.group(1))
    return int(raw) if raw.isdigit() else 0


def _parse_price_eur(text: str):
    """'€ 249,00' or '249.00 €' → 249.0."""
    if not text:
        return None
    clean = text.replace("\xa0", " ").replace(",", ".")
    m = re.search(r"(\d+\.\d+|\d+)", clean)
    return float(m.group(1)) if m else None


# ── Fetch & parse ─────────────────────────────────────────────────────────────

def fetch_page(slug: str, page: int, session) -> list[dict]:
    """Fetch one category page and return list of raw product dicts."""
    params = f"?cat={slug}&sort=empf_desc&pg={page}"
    url = BASE_URL + params
    try:
        resp = session.get(url, headers=EXTRA_HEADERS, timeout=25)
        resp.raise_for_status()
    except Exception as e:
        log.warning(f"  Fetch failed ({e})")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    products = []

    # Geizhals product items are contained in <article> tags or divs with
    # class "productlist-item".  We try both patterns for forward-compatibility.
    items = (
        soup.select("article.productlist-item")
        or soup.select("div.productlist-item")
        or soup.select("[class*='product-list__item']")
    )

    if not items:
        log.debug("  No product items found — possibly last page or layout change.")
        return []

    for item in items:
        # ── Name ──────────────────────────────────────────────────────────────
        name_el = (
            item.select_one("a.productlist-item__name")
            or item.select_one("[class*='item__name'] a")
            or item.select_one("h3 a")
            or item.select_one("h2 a")
        )
        name = name_el.get_text(strip=True) if name_el else ""
        href = name_el.get("href", "") if name_el else ""
        url_product = ("https://geizhals.at" + href) if href.startswith("/") else href

        # ── Stars ─────────────────────────────────────────────────────────────
        stars_el = (
            item.select_one("[class*='rating']")
            or item.select_one("[title*='Stern']")
            or item.select_one("[title*='stern']")
            or item.select_one("[aria-label*='Stern']")
        )
        stars_text = ""
        if stars_el:
            stars_text = stars_el.get("title") or stars_el.get("aria-label") or stars_el.get_text()
        stars = _parse_stars(stars_text)

        # ── Reviews ───────────────────────────────────────────────────────────
        reviews_el = item.select_one("[class*='review-count']") or item.select_one("[class*='bewertung']")
        reviews = _parse_reviews(reviews_el.get_text() if reviews_el else "")

        # ── Price ─────────────────────────────────────────────────────────────
        price_el = (
            item.select_one("[class*='price']")
            or item.select_one("[class*='Price']")
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
                "geizhals",
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

    log.info(f"── {cat_name}  (?cat={slug})")

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
    log.info("QualityDB Geizhals.at Scraper — starting run")
    log.info("=" * 60)

    if not os.path.exists(DB_PATH):
        log.error(f"Database not found at {DB_PATH}.")
        return {"error": "database_not_found"}

    session = requests.Session(impersonate="chrome120")

    # Warm-up visit to obtain cookies
    try:
        log.info("Warming up session (visiting Geizhals.at)…")
        session.get(BASE_URL, headers={**EXTRA_HEADERS, "Accept": "text/html,*/*"}, timeout=20)
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
        print(f"\n⚠  {len(result['errors'])} category error(s) — check geizhals_scraper.log")
    print(f"\n✓  Done. {result['total_added']} new products added to database.")
