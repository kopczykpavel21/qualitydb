"""
Geizhals.at scraper — finds top-selling products and adds them to QualityDB.

Geizhals.at is Austria's largest price-comparison site, covering AT, DE, and
parts of the broader DACH market. Products carry star ratings (1–5) and
verified buyer review counts.

Scraping approach:
  - Playwright non-headless browser (required: Cloudflare JS challenge blocks curl)
  - Category listing pages sorted by topseller (sort=topseller), top 10 per category
  - No minimum star/review threshold — all top-sellers are stored
  - HTML parsed with BeautifulSoup from the rendered DOM
  - Stars (1–5) converted to recommend %: (stars / 5) × 100
  - Prices are in EUR, stored as-is in Price_CZK column

Dependencies:
    pip3 install playwright playwright-stealth
    playwright install chromium

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
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
    from playwright_stealth import Stealth
    from bs4 import BeautifulSoup
except ImportError:
    print("\n⚠  Missing dependencies. Please run:")
    print("    pip3 install playwright playwright-stealth beautifulsoup4")
    print("    playwright install chromium\n")
    sys.exit(1)

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "products.db")

# ── Settings ──────────────────────────────────────────────────────────────────
MAX_PAGES      = 3      # Pages of bestsellers per category (~25–30 products/page)
PAGE_WAIT_MS   = 3000   # ms to wait for page content after navigation

# ── Category list ─────────────────────────────────────────────────────────────
# Correct Geizhals category slugs (verified via browser DevTools, May 2026).
# URL pattern: https://geizhals.at/?cat={slug}&sort=empf_desc&pg={page}
CATEGORIES = [
    # Electronics (datatable layout)
    {"name": "TVs",              "slug": "tvlcd"},
    {"name": "Smartphones",      "slug": "umtsover"},
    {"name": "Smartwatches",     "slug": "uhrpm"},
    {"name": "Headphones",       "slug": "sphd"},
    # Computers & peripherals (productlist layout)
    {"name": "Laptops",          "slug": "nb"},
    {"name": "Tablets",          "slug": "nbtabl"},
    {"name": "Monitors",         "slug": "monlcd19wide"},
    {"name": "SSD",              "slug": "hdssd"},
    {"name": "Keyboards",        "slug": "kb"},
    {"name": "Mice",             "slug": "mouse"},
    # Audio
    {"name": "Speakers",         "slug": "hifipaspkr"},
    # Household appliances (datatable layout with h-prefix slugs)
    {"name": "Washing Machines", "slug": "hwaschf"},
    {"name": "Dishwashers",      "slug": "hgeschirr60"},
    {"name": "Refrigerators",    "slug": "hkuehlsch"},
    {"name": "Coffee Machines",  "slug": "hvollauto"},
    {"name": "Robot Vacuums",    "slug": "hausgrobot"},
    {"name": "Air Purifiers",    "slug": "hluft"},
]

BASE_URL = "https://geizhals.at"

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


# ── Parsing helpers ───────────────────────────────────────────────────────────

def _parse_stars(text: str):
    """'Bewertung: 4.8 von 5' → 4.8. Returns None if unparseable."""
    if not text:
        return None
    m = re.search(r"(\d)[,.](\d)", text)
    if m:
        return float(f"{m.group(1)}.{m.group(2)}")
    return None


def _parse_reviews(text: str) -> int:
    """'(123)', '74 Bewertungen', '5 Meinungen' → integer count."""
    if not text:
        return 0
    m = re.search(r"\((\d+)\)", text)
    if m:
        return int(m.group(1))
    # "74 Bewertungen" or "74 Meinungen"
    m = re.search(r"(\d+)\s*(?:Bewertung|Meinung|Review)", text, re.IGNORECASE)
    if m:
        return int(m.group(1))
    m = re.search(r"(\d+)", text)
    return int(m.group(1)) if m else 0


def _parse_price_eur(text: str):
    """'€ 1.612,44' or '1612.44' → 1612.44."""
    if not text:
        return None
    # Remove thousands separator (dot in German format), then replace comma decimal
    clean = text.replace("\xa0", " ").replace(".", "").replace(",", ".")
    m = re.search(r"(\d+\.\d+|\d+)", clean)
    return float(m.group(1)) if m else None


# ── Parse rendered HTML ───────────────────────────────────────────────────────

def _extract_from_datatable_row(row) -> dict:
    """Parse one product from the Svelte datatable layout (tr[data-row-index])."""
    name_el = row.select_one("a.product-name") or row.select_one("[class*='product-name'] a")
    if not name_el:
        return {}
    name = name_el.get_text(strip=True)
    href = name_el.get("href", "")
    url_product = (BASE_URL + href) if href.startswith("/") else href

    stars = None
    for el in row.select("span.visually-hidden, [class*='visually-hidden']"):
        text = el.get_text()
        if "ewertung" in text or "von 5" in text:
            stars = _parse_stars(text)
            if stars:
                break
    if not stars:
        for el in row.select("[class*='rating'], [aria-label*='Stern'], [aria-label*='von 5']"):
            stars = _parse_stars(el.get("aria-label", "") or el.get_text())
            if stars:
                break

    reviews = 0
    for el in row.select("[class*='rating'], [class*='review'], [class*='bewertung']"):
        text = el.get_text()
        if re.search(r"\(\d+\)", text):
            reviews = _parse_reviews(text)
            break

    price_el = (
        row.select_one(".gh_price")
        or row.select_one("[class*='price__primary']")
        or row.select_one("[class*='price']")
    )
    price_eur = _parse_price_eur(price_el.get_text() if price_el else "")

    if name and stars is not None:
        return {
            "Name":              name,
            "ProductURL":        url_product,
            "Stars":             stars,
            "RecommendRate_pct": round((stars / 5.0) * 100, 1),
            "ReviewsCount":      reviews,
            "Price_CZK":         price_eur,
        }
    return {}


def _extract_from_productlist_row(row) -> dict:
    """
    Parse one product from the Geizhals productlist layout
    (div.row.productlist__product).
    Used for laptops, monitors, and many appliance categories.
    """
    name_el = row.select_one("a.productlist__link")
    if not name_el:
        return {}
    name = name_el.get_text(strip=True)
    href = name_el.get("href", "")
    # hrefs are relative like "apple-macbook-air-...html"
    url_product = (BASE_URL + "/" + href.lstrip("./")) if href else ""

    # Rating cell (skip rows with 'not-enough')
    rating_cell = row.select_one(
        "[class*='productlist__rating']:not([class*='not-enough'])"
    )
    stars = None
    reviews = 0
    if rating_cell:
        vh = rating_cell.select_one("span.visually-hidden")
        rating_text = vh.get_text() if vh else rating_cell.get_text()
        stars = _parse_stars(rating_text)
        reviews = _parse_reviews(rating_cell.get_text())

    price_el = row.select_one("[class*='price']")
    price_eur = _parse_price_eur(price_el.get_text() if price_el else "")

    if name and stars is not None:
        return {
            "Name":              name,
            "ProductURL":        url_product,
            "Stars":             stars,
            "RecommendRate_pct": round((stars / 5.0) * 100, 1),
            "ReviewsCount":      reviews,
            "Price_CZK":         price_eur,
        }
    return {}


def parse_products(html: str) -> list:
    """
    Parse product rows from Geizhals rendered HTML.
    Handles three layouts:
      1. Svelte datatable — tr[data-row-index]  (TVs, phones, watches, appliances)
      2. Productlist rows — div.row.productlist__product  (laptops, monitors, SSDs)
      3. Gallery view — article.galleryview__item  (keyboards, mice, peripherals)
    """
    soup = BeautifulSoup(html, "html.parser")
    products = []

    # Layout 1: Svelte datatable
    rows = soup.select("tr[data-row-index]")
    if rows:
        for row in rows:
            p = _extract_from_datatable_row(row)
            if p:
                products.append(p)
        return products

    # Layout 2: Productlist
    rows = soup.select("div.row.productlist__product")
    if rows:
        for row in rows:
            p = _extract_from_productlist_row(row)
            if p:
                products.append(p)
        return products

    # Layout 3: Gallery view (keyboards, mice, some peripherals)
    items = soup.select("article.galleryview__item")
    if items:
        for item in items:
            name_el = item.select_one("a.galleryview__name-link")
            if not name_el:
                continue
            name = name_el.get_text(strip=True)
            href = name_el.get("href", "")
            url_product = (BASE_URL + "/" + href.lstrip("./")) if href else ""

            rating_link = item.select_one("a.galleryview__rating-link")
            stars = None
            reviews = 0
            if rating_link:
                vh = rating_link.select_one("span.visually-hidden")
                stars = _parse_stars(vh.get_text() if vh else rating_link.get_text())
                reviews = _parse_reviews(rating_link.get_text())

            price_el = item.select_one("a.galleryview__price-link")
            price_eur = _parse_price_eur(price_el.get_text() if price_el else "")

            if name and stars is not None:
                products.append({
                    "Name":              name,
                    "ProductURL":        url_product,
                    "Stars":             stars,
                    "RecommendRate_pct": round((stars / 5.0) * 100, 1),
                    "ReviewsCount":      reviews,
                    "Price_CZK":         price_eur,
                })
        return products

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
                "geizhals",
            ),
        )
        existing.add(key)
        inserted += 1
    conn.commit()
    return inserted


# ── Main scrape logic ─────────────────────────────────────────────────────────

def scrape_category(cat: dict, page_obj, conn) -> int:
    slug     = cat["slug"]
    cat_name = cat["name"]
    total    = 0

    log.info(f"── {cat_name}  (?cat={slug})")

    for page_num in range(1, MAX_PAGES + 1):
        url = f"{BASE_URL}/?cat={slug}&sort=topseller&pg={page_num}"
        log.info(f"   Page {page_num} — {url}")

        try:
            page_obj.goto(url, wait_until="domcontentloaded", timeout=30000)
            page_obj.wait_for_timeout(PAGE_WAIT_MS)
        except PlaywrightTimeout:
            log.warning(f"   Timeout on page {page_num} — stopping.")
            break

        html = page_obj.content()

        if "Just a moment" in html or "Sichere Verbindung" in html:
            log.error("   Blocked by Cloudflare — stopping.")
            break

        products = parse_products(html)

        if not products:
            log.info("   No products found — stopping.")
            break

        added = insert_products(conn, products, cat_name)
        total += added
        log.info(f"   Found {len(products)} | New: {added}")

        time.sleep(2.5)

    return total


def run_scraper() -> dict:
    log.info("=" * 60)
    log.info("QualityDB Geizhals.at Scraper (Playwright) — starting run")
    log.info("=" * 60)

    if not os.path.exists(DB_PATH):
        log.error(f"Database not found at {DB_PATH}.")
        return {"error": "database_not_found"}

    conn    = sqlite3.connect(DB_PATH)
    summary = {"categories_scraped": 0, "total_added": 0, "errors": []}

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False)
        context = browser.new_context(
            locale="de-AT",
            viewport={"width": 1280, "height": 900},
        )
        page = context.new_page()
        Stealth().apply_stealth_sync(page)

        # Warm-up: visit homepage to get cookies/Cloudflare clearance
        log.info("Warming up session (visiting Geizhals.at)…")
        try:
            page.goto(BASE_URL, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(3000)

            # Dismiss cookie banner if present
            for selector in [
                "button:has-text('Akzeptieren')",
                "button:has-text('Alle akzeptieren')",
                "[class*='cookie'] button",
                "#consent-accept",
            ]:
                try:
                    btn = page.query_selector(selector)
                    if btn and btn.is_visible():
                        btn.click()
                        page.wait_for_timeout(1000)
                        log.info(f"   Dismissed cookie banner ({selector})")
                        break
                except Exception:
                    pass
        except Exception as e:
            log.warning(f"Warm-up failed ({e}) — continuing anyway.")

        for cat in CATEGORIES:
            try:
                added = scrape_category(cat, page, conn)
                summary["total_added"]        += added
                summary["categories_scraped"] += 1
            except Exception as e:
                log.error(f"Error scraping {cat['name']}: {e}")
                summary["errors"].append({"category": cat["name"], "error": str(e)})
            time.sleep(2.5)

        browser.close()

    conn.close()

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
