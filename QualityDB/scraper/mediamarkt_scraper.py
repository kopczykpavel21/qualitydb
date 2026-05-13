"""
MediaMarkt.de scraper — Germany's largest electronics retailer.

MediaMarkt uses a persisted GraphQL API (CategoryV4 operation) for its product
listings.  The API is protected by Cloudflare + bot detection that requires a
real browser session, so this scraper uses Playwright non-headless + stealth.

Approach:
  - Navigate to each category URL with ?sortby=rating&page=N
  - Intercept the CategoryV4 GraphQL response via page.on('response', ...)
  - Extract rating, review count, title, URL from the intercepted JSON
  - Pages are 12 products each; stop when avg rating drops below threshold

Rating data location in GraphQL response:
  products[].cofrProductAggregate.cofrCoreFeature.reviewStatistics.averageOverallRating
  products[].cofrProductAggregate.cofrCoreFeature.reviewStatistics.totalReviewCount
  products[].productAggregate.product.title
  products[].productAggregate.product.url

Category URLs verified May 2026 (numeric suffix = PIM code suffix):
  fernseher-203       → TVs
  smartphones-handys-578 → Smartphones
  laptops-notebooks-362  → Laptops
  tablets-398         → Tablets
  kopfhoerer-7010     → Headphones (in-ear + over-ear)
  monitore-408        → Monitors
  waschmaschinen-3    → Washing Machines
  kuehlschraenke-33   → Refrigerators
  geschirrspueler-28  → Dishwashers
  kaffeevollautomaten-43 → Coffee Machines
  staubsauger-reiniger-87 → Vacuum Cleaners
  bluetooth-lautsprecher-7014 → Speakers
  luftreiniger-884    → Air Purifiers

Dependencies:
    pip3 install playwright playwright-stealth beautifulsoup4
    playwright install chromium

Usage:
    python3 scraper/mediamarkt_scraper.py
"""

import re
import time
import logging
import sqlite3
import os
import sys
import json

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
    from playwright_stealth import Stealth
except ImportError:
    print("\n⚠  Missing dependencies. Please run:")
    print("    pip3 install playwright playwright-stealth")
    print("    playwright install chromium\n")
    sys.exit(1)

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "products.db")

# ── Thresholds ────────────────────────────────────────────────────────────────
MIN_STARS   = 4.0   # Minimum average star rating (out of 5)
MIN_REVIEWS = 10    # Minimum number of buyer ratings
STOP_BELOW  = 3.8   # Stop category when page avg drops below this
MAX_PAGES   = 20    # Max pages per category (12 products/page)
PAGE_WAIT   = 4000  # ms to wait after navigation

BASE_URL = "https://www.mediamarkt.de"

# ── Categories ────────────────────────────────────────────────────────────────
# Verified by navigating site and capturing pimCode from GraphQL variables.
# pimCode pattern: CAT_DE_MM_{N} where N is the numeric suffix of the slug.
CATEGORIES = [
    {"name": "TVs",              "slug": "fernseher-203",              "label": "televisions"},
    {"name": "Smartphones",      "slug": "smartphones-handys-578",     "label": "smartphones"},
    {"name": "Laptops",          "slug": "laptops-notebooks-362",      "label": "laptops"},
    {"name": "Tablets",          "slug": "tablets-398",                "label": "tablets"},
    {"name": "Headphones",       "slug": "kopfhoerer-7010",            "label": "headphones"},
    {"name": "Monitors",         "slug": "monitore-408",               "label": "monitors"},
    {"name": "Washing Machines", "slug": "waschmaschinen-3",           "label": "washing machines"},
    {"name": "Refrigerators",    "slug": "kuehlschraenke-33",          "label": "refrigerators"},
    {"name": "Dishwashers",      "slug": "geschirrspueler-28",         "label": "dishwashers"},
    {"name": "Coffee Machines",  "slug": "kaffeevollautomaten-43",     "label": "coffee machines"},
    {"name": "Vacuum Cleaners",  "slug": "staubsauger-reiniger-87",    "label": "vacuums"},
    {"name": "Speakers",         "slug": "bluetooth-lautsprecher-7014","label": "speakers"},
    {"name": "Air Purifiers",    "slug": "luftreiniger-884",           "label": "air purifiers"},
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            os.path.join(os.path.dirname(__file__), "mediamarkt_scraper.log"),
            encoding="utf-8",
        ),
    ],
)
log = logging.getLogger(__name__)


# ── Product extraction ────────────────────────────────────────────────────────

def _extract_products(graphql_data: dict) -> list[dict]:
    """Extract products from a CategoryV4 GraphQL response dict."""
    cat     = graphql_data.get("data", {}).get("categoryV4", {})
    raw     = cat.get("products", [])
    products = []

    for item in raw:
        cofr    = item.get("cofrProductAggregate") or {}
        core    = cofr.get("cofrCoreFeature") or {}
        stats   = core.get("reviewStatistics") or {}
        agg     = item.get("productAggregate") or {}
        product = agg.get("product") or {}

        title   = (product.get("title") or "").strip()
        url_rel = product.get("url") or ""
        rating  = stats.get("averageOverallRating")
        reviews = stats.get("totalReviewCount", 0)

        if not title or rating is None:
            continue

        url_product = (BASE_URL + url_rel) if url_rel.startswith("/") else url_rel

        products.append({
            "Name":              title,
            "ProductURL":        url_product,
            "Stars":             rating,
            "RecommendRate_pct": round((rating / 5.0) * 100.0, 1),
            "ReviewsCount":      reviews,
        })

    return products


def _get_paging(graphql_data: dict) -> dict:
    cat = graphql_data.get("data", {}).get("categoryV4", {})
    return cat.get("paging", {})


# ── Database ──────────────────────────────────────────────────────────────────

def _load_existing(conn) -> set:
    rows = conn.execute("SELECT lower(Name) FROM products").fetchall()
    return {r[0] for r in rows}


def _insert(conn, products: list, category: str) -> int:
    existing = _load_existing(conn)
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
                None,
                p.get("RecommendRate_pct"),
                p.get("ReviewsCount", 0),
                "mediamarkt",
            ),
        )
        existing.add(key)
        inserted += 1
    conn.commit()
    return inserted


# ── Category scraper ──────────────────────────────────────────────────────────

def scrape_category(cat: dict, page_obj, conn) -> int:
    slug     = cat["slug"]
    cat_name = cat["name"]
    total    = 0
    max_page = MAX_PAGES

    log.info(f"── {cat_name}  (/de/category/{slug})")

    for page_num in range(1, MAX_PAGES + 1):
        captured = []

        def _on_response(resp):
            try:
                if "CategoryV4" in resp.url and "mediamarkt" in resp.url:
                    captured.append(resp.json())
            except Exception:
                pass

        page_obj.on("response", _on_response)

        url = f"{BASE_URL}/de/category/{slug}.html?sortby=rating&view=PRODUCTLIST&page={page_num}"
        log.info(f"   Page {page_num} — {url}")

        try:
            page_obj.goto(url, wait_until="domcontentloaded", timeout=30000)
            page_obj.wait_for_timeout(PAGE_WAIT)
        except PlaywrightTimeout:
            log.warning(f"   Timeout loading page {page_num} — stopping.")
            page_obj.remove_listener("response", _on_response)
            break

        page_obj.remove_listener("response", _on_response)

        if not captured:
            log.info("   No GraphQL response captured — stopping.")
            break

        gql_data = captured[-1]  # Use last captured (most recent page)
        products  = _extract_products(gql_data)
        paging    = _get_paging(gql_data)

        # Update max pages from API
        api_max = paging.get("maxPage", max_page)
        max_page = min(MAX_PAGES, api_max)

        if not products:
            log.info("   No products in response — stopping.")
            break

        qualified  = [p for p in products if p["Stars"] >= MIN_STARS and p["ReviewsCount"] >= MIN_REVIEWS]
        stars_vals = [p["Stars"] for p in products]
        avg_stars  = sum(stars_vals) / len(stars_vals) if stars_vals else 0

        added  = _insert(conn, qualified, cat_name)
        total += added
        log.info(
            f"   Found {len(products)} | Qualified: {len(qualified)} | "
            f"New: {added} | Avg ★: {avg_stars:.2f} | API maxPage: {api_max}"
        )

        if avg_stars < STOP_BELOW:
            log.info(f"   Avg rating {avg_stars:.2f} < {STOP_BELOW} — stopping early.")
            break

        if page_num >= max_page:
            log.info(f"   Reached max page {max_page}.")
            break

        time.sleep(1.5)

    return total


# ── Orchestrator ──────────────────────────────────────────────────────────────

def run_scraper() -> dict:
    log.info("=" * 60)
    log.info("QualityDB MediaMarkt.de Scraper (Playwright) — starting run")
    log.info("=" * 60)

    if not os.path.exists(DB_PATH):
        log.error(f"Database not found at {DB_PATH}.")
        return {"error": "database_not_found"}

    conn    = sqlite3.connect(DB_PATH)
    summary = {"categories_scraped": 0, "total_added": 0, "errors": []}

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False)
        context = browser.new_context(
            locale="de-DE",
            viewport={"width": 1280, "height": 900},
        )
        page = context.new_page()
        Stealth().apply_stealth_sync(page)

        # Warm-up: visit homepage and accept cookies
        log.info("Warming up session…")
        try:
            page.goto(BASE_URL, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(3000)

            for selector in [
                "[id*=accept]",
                "button:has-text('Alle akzeptieren')",
                "button:has-text('Akzeptieren')",
                "[class*=consent] button",
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
            time.sleep(2.0)

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
        print(f"\n⚠  {len(result['errors'])} category error(s) — check mediamarkt_scraper.log")
    print(f"\n✓  Done. {result['total_added']} new products added to database.")
