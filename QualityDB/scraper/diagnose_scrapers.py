"""
Quick diagnostic — fetches one page from each new scraper target and reports:
  - HTTP status
  - Page title
  - Which CSS selectors matched
  - First 300 chars of body HTML (to identify structure)

Run this and paste the output so the scrapers can be fixed.

Usage:
    python3 scraper/diagnose_scrapers.py
"""

import re
import sys

try:
    from curl_cffi import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("pip3 install curl_cffi beautifulsoup4")
    sys.exit(1)

TESTS = [
    # ── Coolblue — try base URL without sort params ───────────────────────────
    {
        "name": "Coolblue (no sort)",
        "url": "https://www.coolblue.nl/categorie/televisies/",
        "headers": {"Accept-Language": "nl-NL,nl;q=0.9", "Accept": "text/html,*/*"},
    },
    # Try their search/browse JSON API
    {
        "name": "Coolblue JSON API",
        "url": "https://www.coolblue.nl/api/products?category=televisies&sort=reviewScore&limit=10",
        "headers": {"Accept-Language": "nl-NL,nl;q=0.9", "Accept": "application/json, */*"},
    },
    {
        "name": "Coolblue search API",
        "url": "https://www.coolblue.nl/zoeken?query=televisie&sorteren=recensiescore",
        "headers": {"Accept-Language": "nl-NL,nl;q=0.9", "Accept": "text/html,*/*"},
    },

    # ── Bol.com — try updated URL formats ────────────────────────────────────
    {
        "name": "Bol.com (new URL format)",
        "url": "https://www.bol.com/nl/nl/l/televisies/",
        "headers": {"Accept-Language": "nl-NL,nl;q=0.9", "Accept": "text/html,*/*"},
    },
    {
        "name": "Bol.com (sort param)",
        "url": "https://www.bol.com/nl/nl/l/televisies/?sortering=7",
        "headers": {"Accept-Language": "nl-NL,nl;q=0.9", "Accept": "text/html,*/*"},
    },

    # ── Pricerunner — find correct category URL format ────────────────────────
    {
        "name": "Pricerunner TV (no id prefix)",
        "url": "https://www.pricerunner.dk/cl/Fladskjermsfjernsyn-33/",
        "headers": {"Accept-Language": "da-DK,da;q=0.9", "Accept": "text/html,*/*"},
    },
    {
        "name": "Pricerunner TV (no sort)",
        "url": "https://www.pricerunner.dk/cl/33-TV/",
        "headers": {"Accept-Language": "da-DK,da;q=0.9", "Accept": "text/html,*/*"},
    },
    {
        "name": "Pricerunner search API",
        "url": "https://www.pricerunner.dk/search?q=tv&sortByPreset=BEST_RATED",
        "headers": {"Accept-Language": "da-DK,da;q=0.9", "Accept": "text/html,*/*"},
    },
    {
        "name": "Pricerunner public API",
        "url": "https://www.pricerunner.dk/public/v3/dk/category/1/products?limit=5&sortByPreset=BEST_RATED",
        "headers": {"Accept-Language": "da-DK,da;q=0.9", "Accept": "application/json, */*"},
    },

    # ── Geizhals — try without sort and with country prefix ──────────────────
    {
        "name": "Geizhals (no sort)",
        "url": "https://geizhals.at/?cat=tvall",
        "headers": {"Accept-Language": "de-AT,de;q=0.9", "Accept": "text/html,*/*"},
    },
    {
        "name": "Geizhals AT prefix",
        "url": "https://geizhals.at/at/?cat=tvall",
        "headers": {"Accept-Language": "de-AT,de;q=0.9", "Accept": "text/html,*/*"},
    },
    {
        "name": "Geizhals bestenliste",
        "url": "https://geizhals.at/bestenlisten/",
        "headers": {"Accept-Language": "de-AT,de;q=0.9", "Accept": "text/html,*/*"},
    },
    {
        "name": "Geizhals search",
        "url": "https://geizhals.at/?fs=fernseher&sort=empf_desc",
        "headers": {"Accept-Language": "de-AT,de;q=0.9", "Accept": "text/html,*/*"},
    },

    # ── Digitec — schema introspection to find correct query field ────────────
    {
        "name": "Digitec GraphQL introspect",
        "url": "https://www.digitec.ch/api/graphql",
        "method": "POST",
        "headers": {
            "Accept-Language": "de-CH,de;q=0.9",
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Origin": "https://www.digitec.ch",
            "Referer": "https://www.digitec.ch/",
        },
        "json": {
            "query": "{ __schema { queryType { fields { name description } } } }",
        },
    },
]

CANDIDATE_SELECTORS = [
    "article", "li[class*='product']", "div[class*='product']",
    "[data-test*='product']", "[class*='ProductCard']", "[class*='product-card']",
    "[class*='ProductItem']", "[class*='product-item']", "[class*='item']",
    "ul.products > li", ".search-results li",
]


def diagnose(test: dict, session) -> None:
    name   = test["name"]
    url    = test["url"]
    method = test.get("method", "GET")
    print(f"\n{'='*60}")
    print(f"  {name}")
    print(f"  {url}")
    print(f"{'='*60}")

    try:
        if method == "POST":
            resp = session.post(url, headers=test["headers"], json=test.get("json"), timeout=20)
        else:
            resp = session.get(url, headers=test["headers"], timeout=20)
        print(f"  Status : {resp.status_code}")
    except Exception as e:
        print(f"  FAILED : {e}")
        return

    if resp.status_code != 200:
        print(f"  Body   : {resp.text[:200]}")
        return

    # JSON response (API tests)
    ct = resp.headers.get("content-type", "")
    if "json" in ct:
        print(f"  JSON   : {resp.text[:400]}")
        return

    html = resp.text
    soup = BeautifulSoup(html, "html.parser")

    title = soup.title.string.strip() if soup.title else "(no title)"
    print(f"  Title  : {title}")

    # Check for common JS-only signals
    has_next_data  = "__NEXT_DATA__"  in html
    has_nuxt       = "__NUXT__"       in html
    has_react      = "data-reactroot" in html or "__REACT__" in html
    has_json_ld    = "application/ld+json" in html
    print(f"  SSR signals : Next.js={has_next_data}  Nuxt={has_nuxt}  React={has_react}  JSON-LD={has_json_ld}")

    # Try candidate selectors
    print("  Selectors that matched:")
    any_match = False
    for sel in CANDIDATE_SELECTORS:
        found = soup.select(sel)
        if found:
            print(f"    [{len(found):3d} items]  {sel}")
            any_match = True
    if not any_match:
        print("    (none matched)")

    # Print first meaningful chunk of body text
    body = soup.body
    if body:
        raw = re.sub(r"\s+", " ", body.get_text(" ", strip=True))
        print(f"  Body text  : {raw[:300]}")

    # Look for JSON data embedded in script tags
    scripts = soup.find_all("script", {"type": ["application/json", "application/ld+json"]})
    if scripts:
        print(f"  JSON scripts found: {len(scripts)}")
        for s in scripts[:2]:
            print(f"    {str(s)[:200]}")


def main():
    session = requests.Session(impersonate="chrome124")
    for test in TESTS:
        diagnose(test, session)
    session.close()
    print("\n[done]")


if __name__ == "__main__":
    main()
