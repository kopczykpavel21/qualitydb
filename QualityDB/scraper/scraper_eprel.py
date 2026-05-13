"""
EPREL (European Product Registry for Energy Labelling) scraper.

EPREL is the official EU database of products covered by the energy labelling
regulation (EU) 2017/1369.  Manufacturers are legally required to register
all applicable products before placing them on the EU market.  The registry
contains energy efficiency class (A–G) for appliances, TVs, light sources, etc.

This scraper pulls A-class (and B-class) products via the EPREL public REST API,
derives a normalised 0–100 quality score from the energy efficiency class, and
stores the results in the competitor_scores table.

Normalisation (2021 EU energy label scale):
  A → 100   B → 85   C → 70   D → 55   E → 40   F → 25   G → 10

API endpoint (public, no auth required as of 2026):
  GET https://eprel.ec.europa.eu/api/products/{categoryName}
      ?_page=1&_limit=50&sort0=energyClass&order0=DESC

  Pagination: _page is 1-based.
  Sort order DESC puts A-class products first.

Category names discovered via Playwright network intercept (May 2026):
  electronicdisplays, washingmachines2019, dishwashers2019,
  refrigeratingappliances2019, airconditioners, tumbledryers20232534,
  washerdriers2019, lightsources, residentialventilationunits,
  localspaceheaters, spaceheaters, waterheaters, solidfuelboilers,
  rangehoods, ovens, smartphonestablets20231669

Dependencies (stdlib + curl_cffi for TLS fingerprinting):
    pip3 install curl_cffi

Usage:
    python3 scraper/scraper_eprel.py
    python3 scraper/scrape_competitors.py --only eprel
"""

import json
import time
import os
import sys
import uuid

try:
    from curl_cffi.requests import Session
    _HAS_CURL_CFFI = True
except ImportError:
    import urllib.request
    import urllib.error
    _HAS_CURL_CFFI = False

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scraper_competitors_config import (
    REQUEST_DELAY, REQUEST_TIMEOUT, canonical_category
)
from scraper_competitors_db import (
    count_records, get_checkpoint, init_table, save_checkpoint, upsert_record
)

SOURCE   = "eprel"
BASE_URL = "https://eprel.ec.europa.eu/api/products"
LIMIT    = 50     # products per page (EPREL max is 50)
DELAY    = max(REQUEST_DELAY, 1.5)

# Stop scraping a category once energy class drops below this.
# We only want high-efficiency (quality) products.
STOP_BELOW_CLASS = "C"   # stop when we see C or worse

# Energy efficiency class → normalised score (0–100)
# 2021 EU label: A = best, G = worst
ENERGY_CLASS_SCORE = {
    "A": 100, "B": 85, "C": 70, "D": 55, "E": 40, "F": 25, "G": 10,
    # Old pre-2021 labels (still in EPREL for legacy products)
    "A+++": 100, "A++": 92, "A+": 84,
    # Lowercase variants
    "a": 100, "b": 85, "c": 70, "d": 55, "e": 40, "f": 25, "g": 10,
    "a+++": 100, "a++": 92, "a+": 84,
}

# Minimum score to include a product (B and above = 85+)
MIN_SCORE = 85

# EPREL category names → canonical label for this DB
CATEGORIES = [
    # Major appliances
    {"eprel_name": "washingmachines2019",       "label": "washing machines"},
    {"eprel_name": "washerdriers2019",           "label": "washing machines"},
    {"eprel_name": "dishwashers2019",            "label": "dishwashers"},
    {"eprel_name": "refrigeratingappliances2019","label": "refrigerators"},
    {"eprel_name": "tumbledryers20232534",       "label": "tumble dryers"},
    # Displays
    {"eprel_name": "electronicdisplays",         "label": "televisions"},
    # Climate / heating
    {"eprel_name": "airconditioners",            "label": "air conditioners"},
    {"eprel_name": "spaceheaters",               "label": "space heaters"},
    {"eprel_name": "localspaceheaters",          "label": "space heaters"},
    {"eprel_name": "waterheaters",               "label": "water heaters"},
    # Other
    {"eprel_name": "lightsources",               "label": "light sources"},
    {"eprel_name": "residentialventilationunits","label": "ventilation units"},
    {"eprel_name": "rangehoods",                 "label": "range hoods"},
    {"eprel_name": "ovens",                      "label": "ovens"},
]

MAX_PAGES_PER_CATEGORY = 30   # up to 1 500 products per category


# ── HTTP helpers ──────────────────────────────────────────────────────────────

_HEADERS = {
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9,de;q=0.8",
    "Referer":         "https://eprel.ec.europa.eu/screen/product/electronicdisplays",
    "User-Agent":      (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
}


def _get(url: str, session=None) -> dict:
    """GET request; uses curl_cffi session if available, otherwise urllib."""
    if _HAS_CURL_CFFI and session is not None:
        try:
            r = session.get(url, headers=_HEADERS, timeout=REQUEST_TIMEOUT)
            if r.status_code == 200:
                return r.json()
            print(f"    [!] HTTP {r.status_code} for {url}")
            return {}
        except Exception as e:
            print(f"    [!] curl_cffi error: {e}")
            return {}
    else:
        req = urllib.request.Request(url, headers=_HEADERS)
        try:
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            print(f"    [!] HTTP {e.code} for {url}")
            return {}
        except Exception as e:
            print(f"    [!] Error fetching {url}: {e}")
            return {}


# ── Field extraction ──────────────────────────────────────────────────────────

def _energy_class(product: dict) -> str:
    """Return the primary energy class string from a product record."""
    # Some categories use energyClassSDR (displays), most use energyClass
    return (
        product.get("energyClass")
        or product.get("energyClassSDR")
        or ""
    ).strip()


def _brand(product: dict) -> str:
    return (
        product.get("supplierOrTrademark")
        or (product.get("organisation") or {}).get("organisationName", "")
        or product.get("brand")
        or ""
    ).strip()


def _model(product: dict) -> str:
    return (product.get("modelIdentifier") or "").strip()


def _product_id(product: dict) -> str:
    return str(
        product.get("eprelRegistrationNumber")
        or product.get("versionId")
        or product.get("id")
        or ""
    )


def _product_url(eprel_name: str, product: dict) -> str:
    pid = product.get("versionId") or product.get("eprelRegistrationNumber") or ""
    return f"https://eprel.ec.europa.eu/screen/product/{eprel_name}/{pid}"


# ── Per-category scrape ───────────────────────────────────────────────────────

def scrape_category(cat: dict, session=None) -> int:
    eprel_name = cat["eprel_name"]
    canon_cat  = canonical_category(cat["label"])
    checkpoint = get_checkpoint(SOURCE)
    seen_ids   = set(checkpoint.get(eprel_name, []))
    total      = 0

    print(f"\n  [{eprel_name}]  canonical → {canon_cat}")

    for page_num in range(1, MAX_PAGES_PER_CATEGORY + 1):
        url = (
            f"{BASE_URL}/{eprel_name}"
            f"?_page={page_num}&_limit={LIMIT}"
            f"&sort0=energyClass&order0=DESC"
        )
        data = _get(url, session=session)

        if not data:
            print(f"    Page {page_num}: empty response — stopping.")
            break

        items = data.get("hits") or data.get("items") or data.get("products") or []
        if not isinstance(items, list):
            print(f"    Page {page_num}: unexpected response shape — stopping.")
            break

        if not items:
            print(f"    Page {page_num}: no products — done.")
            break

        page_upserted = 0
        page_skipped_low = 0

        for product in items:
            prod_id = _product_id(product)
            if not prod_id or prod_id in seen_ids:
                continue

            raw_class = _energy_class(product)
            score = ENERGY_CLASS_SCORE.get(raw_class)

            if score is None or score < MIN_SCORE:
                page_skipped_low += 1
                continue

            brand = _brand(product)
            model = _model(product)
            name  = f"{brand} {model}".strip() or f"EPREL-{prod_id}"

            source_url = _product_url(eprel_name, product)

            # Sub-scores: any numerical efficiency fields
            sub_scores = {}
            for key in (
                "annualEnergyConsumption", "energyConsumption100Cycles",
                "ratedCapacity", "weightedEnergyConsumption",
                "annualElectricityConsumption", "powerOnModeSDR",
                "powerStandby",
            ):
                val = product.get(key)
                if val is not None:
                    sub_scores[key] = val

            meta = {
                "energy_class":        raw_class,
                "product_group":       eprel_name,
                "on_market_start":     product.get("onMarketStartDateTS"),
                "on_market_end":       product.get("onMarketEndDateTS"),
                "implementing_act":    product.get("implementingAct"),
            }

            upsert_record({
                "source":             SOURCE,
                "source_url":         source_url,
                "product_name":       name,
                "brand":              brand,
                "model":              model,
                "product_category":   eprel_name,
                "canonical_category": canon_cat,
                "raw_score":          score,
                "raw_score_min":      10,
                "raw_score_max":      100,
                "raw_score_label":    f"Energy Class {raw_class}",
                "score_normalized":   score,
                "sub_scores_json":    sub_scores,
                "meta_json":          meta,
                "source_product_id":  prod_id,
            })

            seen_ids.add(prod_id)
            page_upserted += 1
            total         += 1

        print(
            f"    Page {page_num}: {len(items)} returned, "
            f"{page_upserted} upserted, {page_skipped_low} below threshold"
        )

        # If every product on the page was below our quality threshold, stop
        if page_skipped_low == len(items) and page_upserted == 0:
            print(f"    All products below class threshold — stopping early.")
            break

        # Persist checkpoint after each page
        checkpoint[eprel_name] = list(seen_ids)
        save_checkpoint(SOURCE, checkpoint)

        if len(items) < LIMIT:
            print(f"    Last page reached.")
            break

        time.sleep(DELAY)

    return total


# ── Orchestrator ──────────────────────────────────────────────────────────────

def scrape() -> int:
    """Entry point called by scrape_competitors.py orchestrator."""
    init_table()
    print(f"\n[EPREL] Starting scrape — {len(CATEGORIES)} categories")
    print(f"[EPREL] curl_cffi available: {_HAS_CURL_CFFI}")

    session = None
    if _HAS_CURL_CFFI:
        from curl_cffi.requests import Session
        session = Session(impersonate="chrome124")

    grand_total = 0
    for cat in CATEGORIES:
        try:
            n = scrape_category(cat, session=session)
            grand_total += n
        except Exception as e:
            print(f"  [!] Error scraping {cat['eprel_name']}: {e}")
        time.sleep(DELAY)

    if session:
        session.close()

    print(f"\n[EPREL] Done — {grand_total} records upserted total.")
    print(f"[EPREL] Total in DB: {count_records(SOURCE)}")
    return grand_total


if __name__ == "__main__":
    total = scrape()
    print(f"\n✓  Done. {total} EPREL records upserted.")
