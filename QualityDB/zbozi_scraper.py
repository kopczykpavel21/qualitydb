"""
Zbozi.cz scraper — finds top-rated products and adds them to QualityDB.

Zbozi.cz is Czech Republic's largest price comparison site (owned by Seznam.cz).
It aggregates products from hundreds of Czech shops and shows average ratings
collected from verified buyers — making it a great quality signal.

Dependencies (already installed):
    pip3 install curl_cffi beautifulsoup4

Usage:
    python3 scraper/zbozi_scraper.py          # run once manually
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
MIN_STARS     = 4.0    # Minimum star rating (out of 5)
MIN_REVIEWS   = 5      # Minimum review count
STOP_BELOW    = 3.8    # Stop category when stars drop below this
MAX_PAGES     = 8      # Max pages per category (approx 24 products/page)
REQUEST_DELAY = 2.0    # Seconds between requests

# ── Category URLs ─────────────────────────────────────────────────────────────
# Zbozi.cz category pages sorted by rating.
# URL pattern: /hledani/?q=KEYWORD&sort=rating  or  /kategorie/SLUG/?sort=rating
CATEGORIES = [
    # ── Audio ──────────────────────────────────────────────────────────────────
    {"name": "Headphones",          "url": "https://www.zbozi.cz/hledani/?q=sluchatka&sort=rating"},
    {"name": "Speakers",            "url": "https://www.zbozi.cz/hledani/?q=bluetooth+reproduktor&sort=rating"},

    # ── Wearables ──────────────────────────────────────────────────────────────
    {"name": "Smartwatches",        "url": "https://www.zbozi.cz/hledani/?q=chytre+hodinky&sort=rating"},

    # ── Home appliances ────────────────────────────────────────────────────────
    {"name": "Coffee Machines",     "url": "https://www.zbozi.cz/hledani/?q=kavovar&sort=rating"},
    {"name": "Vacuum Cleaners",     "url": "https://www.zbozi.cz/hledani/?q=vysavac&sort=rating"},
    {"name": "Robot Vacuums",       "url": "https://www.zbozi.cz/hledani/?q=roboticky+vysavac&sort=rating"},
    {"name": "Air Purifiers",       "url": "https://www.zbozi.cz/hledani/?q=cisticky+vzduchu&sort=rating"},
    {"name": "Kitchen Appliances",  "url": "https://www.zbozi.cz/hledani/?q=kuchynske+spotrebice&sort=rating"},

    # ── Computer peripherals ───────────────────────────────────────────────────
    {"name": "Mice",                "url": "https://www.zbozi.cz/hledani/?q=pocitacova+mys&sort=rating"},
    {"name": "Keyboards",           "url": "https://www.zbozi.cz/hledani/?q=klavesnice&sort=rating"},
    {"name": "SSD",                 "url": "https://www.zbozi.cz/hledani/?q=ssd+disk&sort=rating"},

    # ── TVs ────────────────────────────────────────────────────────────────────
    {"name": "TVs",                 "url": "https://www.zbozi.cz/hledani/?q=televize&sort=rating"},
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            os.path.join(os.path.dirname(__file__), "zbozi_scraper.log"),
            encoding="utf-8"
        )
    ]
)
log = logging.getLogger(__name__)

EXTRA_HEADERS = {
    "Accept-Language": "cs-CZ,cs;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Referer": "https://www.zbozi.cz/",
}


# ── Parsing helpers ───────────────────────────────────────────────────────────

def parse_stars(text: str):
    """
    Parse Zbozi.cz star ratings.
    'Hodnocení: 4.5 z 5' → 4.5
    '4,5 hvězdiček z 5'  → 4.5
    '90 %'               → 4.5 (converts percentage)
    """
    if not text:
        return None
    # Percentage → stars
    m = re.search(r"(\d+(?:[.,]\d+)?)\s*%", text)
    if m:
        pct = float(m.group(1).replace(",", "."))
        return round(pct / 20.0, 1)
    # Decimal star rating "4,5" or "4.5"
    m = re.search(r"(\d+)[,.](\d+)", text)
    if m:
        return float(f"{m.group(1)}.{m.group(2)}")
    # Whole number out of 5
    m = re.search(r"(\d+)\s*(?:z|/|hvězd)", text)
    if m:
        val = float(m.group(1))
        return val if val <= 5 else None
    return None


def parse_reviews(text: str) -> int:
    """
    '(123 hodnocení)' → 123
    '45 recenzí'      → 45
    '1 234 hodnocení' → 1234  (space as thousands separator in Czech)
    """
    if not text:
        return 0
    m = re.search(r"(\d[\d\s]*)", text)
    return int(re.sub(r"\s", "", m.group(1))) if m else 0


def parse_price(text: str):
    """'1 299 Kč' → 1299.0"""
    if not text:
        return None
    m = re.search(r"(\d[\d\s]*)", text)
    return float(re.sub(r"\s", "", m.group(1))) if m else None


# ── Session ───────────────────────────────────────────────────────────────────

def warm_up_session(session) -> bool:
    """Visit Zbozi.cz homepage to obtain session cookies."""
    try:
        log.info("Warming up session (visiting Zbozi.cz)…")
        resp = session.get("https://www.zbozi.cz/", headers=EXTRA_HEADERS, timeout=20)
        resp.raise_for_status()
        log.info(f"Session ready. Cookies: {len(session.cookies)}")
        time.sleep(1.5)
        return True
    except Exception as e:
        log.warning(f"Session warmup failed ({e}) — will try scraping anyway.")
        return False


# ── Page scraping ─────────────────────────────────────────────────────────────

def scrape_page(url: str, session) -> list:
    """Fetch one Zbozi.cz search/category page and return product dicts."""
    try:
        resp = session.get(url, headers=EXTRA_HEADERS, timeout=20)
        resp.raise_for_status()
    except Exception as e:
        log.warning(f"  Request failed: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")

    # Zbozi.cz product cards — try multiple selector patterns
    cards = (
        soup.select("article.b-product") or
        soup.select("div.b-product") or
        soup.select("[class*='b-product']") or
        soup.select("li.b-listing__item") or
        soup.select("[data-dot='product']") or
        soup.select("[class*='product-item']")
    )

    if not cards:
        log.debug(f"  No product cards found at {url}")
        log.debug(f"  Page title: {soup.title.string if soup.title else 'N/A'}")
        return []

    products = []
    for card in cards:
        # ── Name ──────────────────────────────────────────────────────────────
        name_el = (
            card.select_one("h2 a") or
            card.select_one("h3 a") or
            card.select_one("[class*='product-title'] a") or
            card.select_one("[class*='title'] a") or
            card.select_one("a[class*='name']")
        )
        if not name_el:
            continue
        name = name_el.get_text(strip=True)
        if not name:
            continue

        # ── URL ───────────────────────────────────────────────────────────────
        href = name_el.get("href", "")
        product_url = f"https://www.zbozi.cz{href}" if href.startswith("/") else href

        # ── Star rating ───────────────────────────────────────────────────────
        stars = None
        for sel in [
            "[class*='rating'][aria-label]",
            "[class*='stars'][aria-label]",
            "[class*='rating'][title]",
            "[itemprop='ratingValue']",
            "[class*='rating']",
            "[class*='stars']",
        ]:
            rating_el = card.select_one(sel)
            if not rating_el:
                continue
            for attr in ("aria-label", "title", "content"):
                val = rating_el.get(attr, "")
                stars = parse_stars(val)
                if stars is not None:
                    break
            if stars is None:
                stars = parse_stars(rating_el.get_text(strip=True))
            if stars is not None:
                break

        if stars is None:
            continue  # Skip unrated products

        # ── Review count ──────────────────────────────────────────────────────
        review_count = 0
        for el in card.find_all(["span", "a", "div", "p"]):
            text = el.get_text(strip=True).lower()
            if any(kw in text for kw in ["hodnocen", "recenz", "reviews"]):
                review_count = parse_reviews(el.get_text(strip=True))
                if review_count > 0:
                    break

        # ── Price ─────────────────────────────────────────────────────────────
        price_el = (
            card.select_one("[class*='price']") or
            card.select_one("[itemprop='price']")
        )
        price = parse_price(price_el.get_text(strip=True) if price_el else "")

        # Convert stars to recommend %
        recommend_pct = round((stars / 5.0) * 100, 1)

        products.append({
            "Name":              name,
            "ProductURL":        product_url,
            "AvgStarRating":     stars,
            "RecommendRate_pct": recommend_pct,
            "ReviewsCount":      review_count,
            "Price_CZK":         price,
        })

    return products


# ── Database helpers ──────────────────────────────────────────────────────────

def load_existing_names(conn: sqlite3.Connection) -> set:
    rows = conn.execute("SELECT lower(Name) FROM products").fetchall()
    return {r[0] for r in rows}


def insert_products(conn: sqlite3.Connection, products: list, category: str) -> int:
    existing = load_existing_names(conn)
    inserted = 0
    for p in products:
        key = p["Name"].lower()
        if key in existing:
            continue
        conn.execute(
            """INSERT INTO products
               (Name, Category, ProductURL, Price_CZK, AvgStarRating,
                RecommendRate_pct, ReviewsCount, source)
               VALUES (?,?,?,?,?,?,?,?)""",
            (
                p["Name"],
                category,
                p.get("ProductURL", ""),
                p.get("Price_CZK"),
                p.get("AvgStarRating"),
                p.get("RecommendRate_pct"),
                p.get("ReviewsCount", 0),
                "scraper",
            )
        )
        existing.add(key)
        inserted += 1
    conn.commit()
    return inserted


# ── Main scrape logic ─────────────────────────────────────────────────────────

def scrape_category(cat: dict, session, conn: sqlite3.Connection) -> int:
    base_url    = cat["url"]
    cat_name    = cat["name"]
    total_added = 0

    log.info(f"── {cat_name}  ({base_url})")

    for page in range(1, MAX_PAGES + 1):
        # Zbozi.cz pagination: &page=2, &page=3, ...
        url = base_url if page == 1 else f"{base_url}&page={page}"

        log.info(f"   Page {page}: {url}")
        products = scrape_page(url, session)

        if not products:
            log.info("   No products returned — stopping.")
            break

        qualified = [
            p for p in products
            if (p.get("AvgStarRating") or 0) >= MIN_STARS
            and p["ReviewsCount"] >= MIN_REVIEWS
        ]

        rated = [p["AvgStarRating"] for p in products if p.get("AvgStarRating") is not None]
        lowest_stars = min(rated) if rated else 5.0

        added = insert_products(conn, qualified, cat_name)
        total_added += added
        log.info(
            f"   Found {len(products)} | Qualified: {len(qualified)} | "
            f"New in DB: {added} | Lowest stars: {lowest_stars:.1f}"
        )

        if lowest_stars < STOP_BELOW:
            log.info(f"   Stars dropped to {lowest_stars:.1f} — stopping early.")
            break

        time.sleep(REQUEST_DELAY)

    return total_added


def run_scraper() -> dict:
    log.info("=" * 60)
    log.info("QualityDB Zbozi.cz Scraper — starting run")
    log.info("=" * 60)

    if not os.path.exists(DB_PATH):
        log.error(f"Database not found at {DB_PATH}. Run load_data.py first.")
        return {"error": "database_not_found"}

    session = requests.Session(impersonate="chrome120")
    warm_up_session(session)
    time.sleep(REQUEST_DELAY)

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
        print(f"\n⚠  {len(result['errors'])} category error(s) — check scraper/zbozi_scraper.log")
    print(f"\n✓  Done. {result['total_added']} new products added to database.")
