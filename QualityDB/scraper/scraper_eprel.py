"""
EPREL (European Product Registry for Energy Labelling) scraper.

EPREL is the official EU database of products covered by the energy labelling
regulation (EU) 2017/1369.  Manufacturers are legally required to register
all applicable products before placing them on the EU market.  The registry
contains energy efficiency class (A–G or A+/A++/A+++) for appliances,
TVs, light sources, heating, etc.

This scraper pulls products via the EPREL public REST API, derives a
normalised 0–100 quality score from the energy efficiency class, and stores
the results in the competitor_scores table.  The score can be used alongside
repairability (iFixit, French Index) and reliability (Yale) data to produce a
holistic sustainability rating for appliances.

Normalisation:
  A+++ → 100   A++ → 90   A+ → 80   A → 70
  B    → 60    C   → 50   D  → 40   E → 30   F → 20   G → 10

  (The 2021 EU rescaling removed A+++ classes for most categories —
   A on the new scale is equivalent to the old A+++ in energy terms.
   This scraper stores whatever class EPREL reports and normalises accordingly.)

API endpoint (public, no auth required):
  GET https://eprel.ec.europa.eu/api/product/{categoryInternalName}
      ?limit=50&offset=0

EPREL category internal names (as of 2025):
  Televisions, WashingMachines, WashingMachineDryers, Dishwashers,
  RefrigeratingAppliances, RefrigeratingAppliancesWithDirectSaleFunction,
  LightSources, SpaceHeaters, SolidFuelBoilers, WaterHeaters,
  LocalSpaceHeaters, AirConditioningUnits, VentilationUnits, Tyres

Dependencies (stdlib only — no extra installs):
    urllib.request  (used instead of curl_cffi to avoid fingerprinting overhead)

Usage:
    python3 scraper/scraper_eprel.py
    python3 scraper/scrape_competitors.py --only eprel
"""

import json
import time
import urllib.request
import urllib.error
import os
import sys
import re

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scraper_competitors_config import (
    REQUEST_DELAY, REQUEST_TIMEOUT, USER_AGENT, canonical_category
)
from scraper_competitors_db import (
    count_records, get_checkpoint, init_table, save_checkpoint, upsert_record
)

SOURCE   = "eprel"
BASE_URL = "https://eprel.ec.europa.eu/api/product"
LIMIT    = 50     # Max products per API call (EPREL hard limit is 50)
DELAY    = max(REQUEST_DELAY, 1.5)

# Energy efficiency class → normalised score (0–100)
ENERGY_CLASS_SCORE = {
    # New EU label (2021+): A through G
    "A": 100, "B": 85, "C": 70, "D": 55, "E": 40, "F": 25, "G": 10,
    # Old EU label (pre-2021): A+++ through G
    "A+++": 100, "A++": 90, "A+": 80,
    # Some EPREL entries use lowercase
    "a": 100, "b": 85, "c": 70, "d": 55, "e": 40, "f": 25, "g": 10,
    "a+++": 100, "a++": 90, "a+": 80,
}

# EPREL categories to scrape.  Each entry maps:
#   eprel_name → the category key used in the EPREL API URL
#   label      → human-readable label for the canonical_category lookup
CATEGORIES = [
    {"eprel_name": "Televisions",                  "label": "televisions"},
    {"eprel_name": "WashingMachines",               "label": "washing machines"},
    {"eprel_name": "WashingMachineDryers",          "label": "washing machines"},
    {"eprel_name": "Dishwashers",                   "label": "dishwashers"},
    {"eprel_name": "RefrigeratingAppliances",       "label": "refrigerators"},
    {"eprel_name": "AirConditioningUnits",          "label": "air conditioners"},
    {"eprel_name": "SpaceHeaters",                  "label": "space heaters"},
    {"eprel_name": "WaterHeaters",                  "label": "water heaters"},
    {"eprel_name": "LightSources",                  "label": "light sources"},
    {"eprel_name": "VentilationUnits",              "label": "ventilation units"},
]

MAX_PAGES_PER_CATEGORY = 20   # Up to 1000 products per category


def _get(url: str) -> dict:
    """Simple GET request using urllib (no external dependencies)."""
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        print(f"    [!] HTTP {e.code} for {url}")
        return {}
    except Exception as e:
        print(f"    [!] Error fetching {url}: {e}")
        return {}


def _energy_class_to_score(raw_class: str) -> float | None:
    """Convert an EPREL energy efficiency class string to a 0–100 score."""
    if not raw_class:
        return None
    cleaned = raw_class.strip()
    return ENERGY_CLASS_SCORE.get(cleaned)


def _extract_brand_model(product: dict) -> tuple[str, str]:
    """Extract brand and model from EPREL product dict."""
    brand = (
        product.get("supplierOrTrademark")
        or product.get("brand")
        or product.get("manufacturerName")
        or ""
    ).strip()
    model = (
        product.get("modelIdentifier")
        or product.get("model")
        or product.get("productName")
        or ""
    ).strip()
    return brand, model


def _product_name(brand: str, model: str, product: dict) -> str:
    """Build a display name from available fields."""
    if brand and model:
        return f"{brand} {model}"
    return (
        product.get("productName")
        or product.get("name")
        or f"{brand} {model}".strip()
        or "Unknown"
    )


def _make_product_id(product: dict) -> str:
    """Derive a stable unique ID from the EPREL registration number."""
    reg_number = (
        str(product.get("registrationNumber") or "")
        or str(product.get("id") or "")
        or str(product.get("eprelId") or "")
    )
    return reg_number or ""


def scrape_category(cat: dict) -> int:
    """Scrape all products in one EPREL category. Returns number of rows upserted."""
    eprel_name = cat["eprel_name"]
    canon_cat  = canonical_category(cat["label"])
    checkpoint = get_checkpoint(SOURCE)
    seen_ids   = set(checkpoint.get(eprel_name, []))
    total      = 0

    print(f"\n  [{eprel_name}]  canonical → {canon_cat}")

    for page in range(MAX_PAGES_PER_CATEGORY):
        offset = page * LIMIT
        url    = f"{BASE_URL}/{eprel_name}?limit={LIMIT}&offset={offset}"
        data   = _get(url)

        # EPREL returns either a list directly or an object with a "products" key
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            items = (
                data.get("products")
                or data.get("items")
                or data.get("data")
                or []
            )
            if not items and "registrationNumber" in data:
                # Single product returned
                items = [data]
        else:
            items = []

        if not items:
            print(f"    Page {page + 1}: empty — done.")
            break

        page_total = 0
        for product in items:
            prod_id = _make_product_id(product)
            if not prod_id or prod_id in seen_ids:
                continue

            # ── Energy class ─────────────────────────────────────────────────
            raw_class = (
                product.get("energyClass")
                or product.get("energyEfficiencyClass")
                or product.get("energyEfficiencyClassValue")
                or ""
            )
            score = _energy_class_to_score(raw_class)
            if score is None:
                # Skip products without a readable energy class
                continue

            # ── Identity ─────────────────────────────────────────────────────
            brand, model  = _extract_brand_model(product)
            product_name  = _product_name(brand, model, product)
            source_url    = (
                f"https://eprel.ec.europa.eu/screen/product/{eprel_name}/{prod_id}"
            )

            # ── Sub-scores ────────────────────────────────────────────────────
            # Collect any numerical efficiency sub-values available
            sub_scores: dict = {}
            for key in (
                "annualEnergyConsumption", "energyConsumption100Cycles",
                "ratedCapacity", "weightedEnergyConsumption",
                "annualElectricityConsumption",
            ):
                val = product.get(key)
                if val is not None:
                    sub_scores[key] = val

            # ── Meta ──────────────────────────────────────────────────────────
            meta = {
                "registration_number": prod_id,
                "energy_class":        raw_class,
                "on_market_start":     product.get("onMarketStart") or product.get("startDate"),
                "on_market_end":       product.get("onMarketEnd")   or product.get("endDate"),
                "product_type":        eprel_name,
            }

            upsert_record({
                "source":             SOURCE,
                "source_url":         source_url,
                "product_name":       product_name,
                "brand":              brand,
                "model":              model,
                "product_category":   eprel_name,
                "canonical_category": canon_cat,
                "raw_score":          list(ENERGY_CLASS_SCORE.keys()).index(
                                          raw_class.strip()
                                      ) if raw_class.strip() in ENERGY_CLASS_SCORE else None,
                "raw_score_min":      0,
                "raw_score_max":      len(ENERGY_CLASS_SCORE) - 1,
                "raw_score_label":    f"Energy Class {raw_class}",
                "score_normalized":   score,
                "sub_scores_json":    sub_scores,
                "meta_json":          meta,
                "source_product_id":  prod_id,
            })

            seen_ids.add(prod_id)
            page_total += 1
            total      += 1

        print(f"    Page {page + 1}: {len(items)} returned, {page_total} new upserted")

        # Persist checkpoint after each page so we can resume on failure
        checkpoint[eprel_name] = list(seen_ids)
        save_checkpoint(SOURCE, checkpoint)

        if len(items) < LIMIT:
            print(f"    Last page reached.")
            break

        time.sleep(DELAY)

    return total


def scrape() -> int:
    """Entry point called by scrape_competitors.py orchestrator."""
    init_table()
    print(f"\n[EPREL] Starting scrape — {len(CATEGORIES)} categories")
    grand_total = 0
    for cat in CATEGORIES:
        try:
            n = scrape_category(cat)
            grand_total += n
        except Exception as e:
            print(f"  [!] Error scraping {cat['eprel_name']}: {e}")
        time.sleep(DELAY)
    print(f"\n[EPREL] Done — {grand_total} records upserted total.")
    print(f"[EPREL] Total in DB: {count_records(SOURCE)}")
    return grand_total


if __name__ == "__main__":
    total = scrape()
    print(f"\n✓  Done. {total} EPREL records upserted.")
